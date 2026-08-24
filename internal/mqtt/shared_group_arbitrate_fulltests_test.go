//go:build mqttport_fulltests

// End-to-end coverage for F-077 on the CONNECT/resume path.
//
// Every test here has to reach the RESIDENT arm, for the reason
// shared_group_verify_fulltests_test.go gives: a fresh connect finds no cons entry, takes
// the join path, and would exercise none of the resume behaviour. So each connects,
// subscribes, disconnects WITHOUT a clean start so the session object survives, and only
// then reconnects.
//
// The oracle is the ARBITRATION ITSELF, observed at the "after-ownership-cas" boundary,
// which fires exactly once per ownership verdict. A raw record-write count cannot serve:
// a shared QoS 2 resume legitimately writes several times more than a plain one (the PUBREL
// consumer and the affine path both provision), so a differential against a plain resume
// measures those too. The boundary measures this change and nothing else.
//
// NOTHING HERE MAY RUN IN PARALLEL. Every seam these tests install —
// mqttTestSharedArbitratePauseV, mqttTestSessStoreResultHookV — is a PACKAGE-LEVEL atomic,
// so two concurrent tests observe each other's boundaries and each other's writes. An
// earlier revision of this file marked them t.Parallel() and all three failed for that
// reason alone, reporting creates and acceptances that belonged to a sibling test.
package mqtt

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testRecordStoreCounter counts session-record store attempts while installed.
type testRecordStoreCounter struct {
	mu sync.Mutex
	n  int
}

func (c *testRecordStoreCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func testCountSessionStores(t *testing.T) *testRecordStoreCounter {
	t.Helper()
	ctr := &testRecordStoreCounter{}
	h := func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		ctr.mu.Lock()
		ctr.n++
		ctr.mu.Unlock()
		return resp, err
	}
	mqttTestSessStoreResultHookV.Store(&h)
	t.Cleanup(func() { mqttTestSessStoreResultHookV.Store(nil) })
	return ctr
}

// testCountArbitrations counts "after-ownership-cas" firings, which is exactly one per
// ownership verdict.
// testSharedSilentRefusal resumes a session and reports whether ANY CONNACK arrived.
//
// A pre-CONNACK foreign verdict closes the connection SILENTLY — mqttNotifyDisplaced is
// gated on c.connAckWritten, which is false for every refusal inside the CONNECT — so there
// is no packet to read and no reason code to compare. "The socket closed without a CONNACK"
// is the observable, and a helper that fataled on EOF would report the correct behaviour as
// a test failure.
func testSharedSilentRefusal(t *testing.T, b *testQ2AffBroker, id string, expiry uint32) (net.Conn, bool) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(expiry),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	// A CONNACK is 4 bytes; anything at all means the CONNECT was answered.
	pt, _, ready := testMQTTReadPacketReady(r, 2*time.Second)
	if !ready {
		return c, false
	}
	if pt&mqttPacketMask == mqttPacketConnectAck {
		return c, true
	}
	t.Fatalf("a refused CONNECT answered with packet type 0x%x, which is not a CONNACK", pt)
	return c, false
}

func testCountArbitrations(t *testing.T) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	h := func(boundary string) {
		if boundary == "after-ownership-cas" {
			n.Add(1)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&h)
	t.Cleanup(func() { mqttTestSharedArbitratePauseV.Store(nil) })
	return &n
}

// TestMQTTSharedGroupOwnershipWriteRunsOnASharedResume is case 1: a resume that restores
// shared interest arbitrates exactly once, and a resume that restores none arbitrates not at
// all.
//
// Both halves are load-bearing. Without the shared half, deleting the arbitration passes
// (M1). Without the plain control, arbitrating on every CONNECT passes (M3) — and that
// mutation is not harmless: it would turn an ordinary plain-only reconnect into a session
// that can be refused by a contended record write it never needed to make.
func TestMQTTSharedGroupOwnershipWriteRunsOnASharedResume(t *testing.T) {
	t.Run("a shared resume arbitrates once", func(t *testing.T) {
		b := testQ2AffSetup(t, "ownshared")
		ca, _ := b.member(t, "ownsharedA", 300)
		b.bind(t, "ownsharedA")
		durable := b.groupDurable(t)
		testSharedResidentSession(t, b, "ownsharedA", durable)
		ca.Close()

		n := testCountArbitrations(t)
		c, _, reason, _ := testSharedResidentReconnect(t, b, "ownsharedA", 300)
		defer c.Close()
		if reason != mqttConnAckRCConnectionAccepted {
			t.Fatalf("the shared resume was refused (0x%x); the fixture proves nothing", reason)
		}
		if got := n.Load(); got != 1 {
			t.Fatalf("a shared resume arbitrated %d times, want exactly 1: 0 means the "+
				"ownership write is absent, more than 1 means it is per filter rather than "+
				"per restore", got)
		}
	})

	t.Run("a plain-only resume does not arbitrate", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{
			clientID:   "ownplainA",
			cleanStart: false,
			props:      mqttV5ConnPropsSessionExpiry(300),
		}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r1)
		codes := testMQTTSubV5(t, c1, r1, 1, []mqttV5SubFilter{{topic: "ownplain/topic", opts: 2}})
		if len(codes) != 1 || codes[0] != 2 {
			t.Fatalf("SUBACK %v", codes)
		}
		testMQTTFlush(t, c1, nil, r1)
		c1.Close()

		n := testCountArbitrations(t)
		c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
			clientID:   "ownplainA",
			cleanStart: false,
			props:      mqttV5ConnPropsSessionExpiry(300),
		}, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		_, reason, _ := testMQTTReadConnAckV5(t, r2)
		if reason != mqttConnAckRCConnectionAccepted {
			t.Fatalf("the plain resume was refused (0x%x); the fixture proves nothing", reason)
		}
		if got := n.Load(); got != 0 {
			t.Fatalf("a resume with no shared filter arbitrated %d times, want 0: the "+
				"trigger is restored SHARED interest, not any restore at all", got)
		}
	})
}

// TestMQTTSharedGroupOwnershipWriteRunsWithoutAnyCreate is case 2, and it is R1: the
// population a create-scoped trigger would have missed.
//
// Every durable is present and canonical, so the resume ADOPTS and creates nothing. The
// arbitration must still happen, because adopting a configuration is not the same as owning
// the membership it belongs to — that sentence is the whole finding, and
// docs/ARCHITECTURE.md assigns it to F-077 by name.
//
// Kills M2 (trigger the write only when a create happened).
func TestMQTTSharedGroupOwnershipWriteRunsWithoutAnyCreate(t *testing.T) {
	b := testQ2AffSetup(t, "ownnocreate")
	ca, _ := b.member(t, "ownnocreateA", 300)
	b.bind(t, "ownnocreateA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "ownnocreateA", durable)
	ca.Close()

	// The durable is deliberately LEFT ALONE, so the resume's classification finds it
	// present and canonical and the join arm is never entered.
	if ci, lerr := b.jsa.lookupConsumer(mqttStreamName, durable); lerr != nil || ci == nil {
		t.Fatalf("the group durable must exist for this fixture: %v", lerr)
	}

	var created atomic.Bool
	var arbitrated atomic.Int32
	h := func(boundary string) {
		switch {
		case strings.HasPrefix(boundary, "after-group-create:"),
			strings.HasPrefix(boundary, "after-group-verify:"):
			created.Store(true)
		case boundary == "after-ownership-cas":
			arbitrated.Add(1)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&h)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	c, _, reason, _ := testSharedResidentReconnect(t, b, "ownnocreateA", 300)
	defer c.Close()
	if reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("resume with a healthy group durable was refused (0x%x)", reason)
	}
	if created.Load() {
		t.Fatal("the resume CREATED a durable, so this fixture is exercising the join arm " +
			"and not R1 — the whole point is arbitration WITHOUT a create")
	}
	if got := arbitrated.Load(); got != 1 {
		t.Fatalf("a resume that restored shared interest without creating anything "+
			"arbitrated %d times, want 1: R1 is exactly the population a create-scoped "+
			"trigger misses", got)
	}
}

// TestMQTTSharedGroupForeignOwnershipRefusesTheResume is case 3: the ownership write is
// rejected 10071, so another broker owns the record and this connection must not serve.
//
// It is deliberately built on a CREATE (the durable is deleted out of band first, so the
// resume repairs it) rather than on a healthy resume, and for two reasons:
//
//   - the create gives the test a boundary to arm the injection AT. A healthy resume fires
//     no create boundary at all, so there is no point at which "the next record write is
//     the ownership write" is true, and an earlier revision of this fixture armed on a
//     boundary that never fired and silently asserted nothing.
//   - and it is what makes the durable-survival assertion meaningful. This broker has just
//     recreated a durable and then lost the arbitration; leaving it in place is the
//     deliberate disposition, because every available deletion is refuted — the
//     last-holder scan skips same-client-ID records, so after a takeover a blind delete
//     destroys the NEW owner's durable.
func TestMQTTSharedGroupForeignOwnershipRefusesTheResume(t *testing.T) {
	b := testQ2AffSetup(t, "ownforeign")
	ca, _ := b.member(t, "ownforeignA", 300)
	b.bind(t, "ownforeignA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "ownforeignA", durable)
	ca.Close()

	if _, err := b.jsa.deleteConsumer(mqttStreamName, durable, false); err != nil {
		t.Fatalf("deleting the group durable out of band: %v", err)
	}

	// Arm only once the restore has recreated the durable, so the accept save and every
	// write before it lands normally and the refusal below can only be the ownership
	// attempt's.
	var armed atomic.Bool
	var arbitrated atomic.Int32
	ph := func(boundary string) {
		switch {
		case strings.HasPrefix(boundary, "after-group-create:"):
			armed.Store(true)
		case boundary == "after-ownership-cas":
			arbitrated.Add(1)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&ph)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	var rejected atomic.Bool
	sh := func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if !armed.Load() || rejected.Load() {
			return resp, err
		}
		rejected.Store(true)
		return nil, ApiErrors[JSStreamWrongLastSequenceErrF]
	}
	mqttTestSessStoreResultHookV.Store(&sh)
	defer mqttTestSessStoreResultHookV.Store(nil)

	c, acked := testSharedSilentRefusal(t, b, "ownforeignA", 300)
	defer c.Close()
	if !rejected.Load() {
		t.Fatal("no record write was attempted after the group-durable create, so the " +
			"ownership write never ran and this fixture proves nothing")
	}
	if got := arbitrated.Load(); got != 1 {
		t.Fatalf("the restore arbitrated %d times, want 1", got)
	}
	// NO CONNACK AT ALL, success or failure. The disposal's DISCONNECT is suppressed
	// pre-CONNACK, so the whole client-visible outcome is a closed socket — the same signal
	// the reconciliation refusals give, and generalising it is its own roadmap item.
	if acked {
		t.Fatal("a resume whose ownership write was rejected 10071 was ANSWERED: this " +
			"broker would serve group deliveries under a membership the record no longer " +
			"carries, acknowledging messages that belong to the legitimate members")
	}
	// The durable must survive the foreign verdict.
	if ci, lerr := b.jsa.lookupConsumer(mqttStreamName, durable); lerr != nil || ci == nil {
		t.Fatalf("the group durable was deleted on the foreign verdict: the legitimate "+
			"owner may hold it, and this broker cannot tell which: %v", lerr)
	}
}

// TestMQTTSharedGroupRepairDoesNotDoubleChargeAndStillArbitrates is case 7.
//
// The repair recreates a durable this session object ALREADY holds the charge for, so it
// creates with charged == 0. Two things follow, and both are asserted:
//
//   - tmaxack must not move (M13: charging again leaves it permanently overcounted for the
//     life of the session);
//   - and the ownership write must still happen (M4: keying the trigger on the charge
//     silently skips arbitration for exactly the population that just recreated a durable).
func TestMQTTSharedGroupRepairDoesNotDoubleChargeAndStillArbitrates(t *testing.T) {
	b := testQ2AffSetup(t, "ownrepair")
	ca, _ := b.member(t, "ownrepairA", 300)
	b.bind(t, "ownrepairA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "ownrepairA", durable)

	sess := testMQTTGetClient(t, b.s, "ownrepairA").mqtt.sess
	sess.mu.Lock()
	beforeTmaxack := sess.tmaxack
	beforeCharge := sess.sharedCharged[durable]
	sess.mu.Unlock()
	if beforeCharge == 0 {
		t.Fatalf("the session holds no charge for %q, so a repair could not double-charge "+
			"and this fixture would prove nothing", durable)
	}
	ca.Close()

	if _, err := b.jsa.deleteConsumer(mqttStreamName, durable, false); err != nil {
		t.Fatalf("deleting the group durable out of band: %v", err)
	}

	var repaired atomic.Bool
	var arbitrated atomic.Int32
	ph := func(boundary string) {
		switch {
		case strings.HasPrefix(boundary, "after-group-create:"):
			repaired.Store(true)
		case boundary == "after-ownership-cas":
			arbitrated.Add(1)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&ph)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	c, _, reason, _ := testSharedResidentReconnect(t, b, "ownrepairA", 300)
	defer c.Close()
	if reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the repair resume was refused (0x%x)", reason)
	}
	if !repaired.Load() {
		t.Fatal("the resume did not enter the create arm, so no repair happened and the " +
			"charge/arbitration assertions below would pass vacuously")
	}
	if got := arbitrated.Load(); got != 1 {
		t.Fatalf("a zero-charge repair arbitrated %d times, want 1: the trigger must be "+
			"the interest it restored, never the charge it happened to add", got)
	}

	sess2 := testMQTTGetClient(t, b.s, "ownrepairA").mqtt.sess
	sess2.mu.Lock()
	afterTmaxack := sess2.tmaxack
	afterCharge := sess2.sharedCharged[durable]
	sess2.mu.Unlock()
	if afterCharge != beforeCharge {
		t.Fatalf("the repair changed the recorded charge for %q from %d to %d", durable, beforeCharge, afterCharge)
	}
	if afterTmaxack != beforeTmaxack {
		t.Fatalf("the repair double-charged tmaxack: %d -> %d. The session already held the "+
			"charge for this durable, so recreating it must add nothing.",
			beforeTmaxack, afterTmaxack)
	}
}

// TestMQTTSharedGroupPrePassRepairableDoesNotPoisonTheBatch drives the repairable-verdict
// rule through PRODUCTION classification, across more than one target, rather than by
// calling the predicates.
//
// The unit test beside this one asserts the predicates directly, which proves the rule and
// not its wiring. This proves the wiring: one absent durable among several must leave the
// others classified normally and the CONNECT restorable, where the pre-change rule would
// have stopped the batch at the absence and judged everything after it indeterminate —
// refusing a CONNECT whose other durables are perfectly healthy.
func TestMQTTSharedGroupPrePassRepairableDoesNotPoisonTheBatch(t *testing.T) {
	b := testQ2AffSetup(t, "prepassbatch")
	id := "prepassbatchA"

	// Three shared filters, so an absence in the first has later targets to poison.
	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r1)
	filters := []mqttV5SubFilter{
		{topic: "$share/" + b.group + "/prepassbatch/one", opts: 2},
		{topic: "$share/" + b.group + "/prepassbatch/two", opts: 2},
		{topic: "$share/" + b.group + "/prepassbatch/three", opts: 2},
	}
	if codes := testMQTTSubV5(t, c1, r1, 1, filters); len(codes) != 3 {
		t.Fatalf("SUBACK %v", codes)
	}
	testMQTTFlush(t, c1, nil, r1)

	b.bind(t, id) // populates b.asm / b.jsa, which the assertions below use
	sess := testMQTTGetClient(t, b.s, id).mqtt.sess
	sess.mu.Lock()
	var durables []string
	for _, cc := range sess.cons {
		if cc != nil && cc.DeliverGroup != _EMPTY_ {
			durables = append(durables, cc.Durable)
		}
	}
	sess.mu.Unlock()
	if len(durables) != 3 {
		t.Fatalf("expected three resident group durables, got %d", len(durables))
	}
	c1.Close()

	// TWO of the three are deleted out of band, and the count is deliberate rather than
	// incidental. The classification walks its targets in the order the filters produce,
	// which comes from a map iteration, so a fixture that deleted exactly one could put the
	// absence LAST — where a batch that wrongly stopped on it would poison nothing and the
	// mutation would have no observable effect at all. With two of three deleted, at least
	// one absence is guaranteed to have a target after it.
	if _, err := b.jsa.deleteConsumer(mqttStreamName, durables[0], false); err != nil {
		t.Fatalf("deleting one group durable: %v", err)
	}
	if _, err := b.jsa.deleteConsumer(mqttStreamName, durables[1], false); err != nil {
		t.Fatalf("deleting a second group durable: %v", err)
	}

	c2, _, reason, props := testSharedResidentReconnect(t, b, id, 300)
	defer c2.Close()
	if reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("absent durables among three refused the whole resume (0x%x, %v): a "+
			"repairable verdict must not stop the batch or poison its siblings",
			reason, props)
	}
	// All three exist afterwards: the absent one repaired, the healthy two untouched.
	for _, d := range durables {
		if ci, lerr := b.jsa.lookupConsumer(mqttStreamName, d); lerr != nil || ci == nil {
			t.Fatalf("durable %q is missing after the resume: %v", d, lerr)
		}
	}
}

// TestMQTTSharedGroupPostPassAbsenceRefuses is the other half of the asymmetry, driven
// through production: a durable deleted BETWEEN the two passes refuses the CONNECT.
//
// After the repair has run, an absence is no longer something this broker may create — it
// is one something is deleting underneath this CONNECT — so the second reading is terminal.
// The existing between-passes seam is what makes the window reachable at all.
func TestMQTTSharedGroupPostPassAbsenceRefuses(t *testing.T) {
	b := testQ2AffSetup(t, "postpassabs")
	ca, _ := b.member(t, "postpassabsA", 300)
	b.bind(t, "postpassabsA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "postpassabsA", durable)
	ca.Close()

	// Delete it once the pre-pass has already classified it present.
	b.asm.mu.Lock()
	b.asm.testSharedGroupBetweenPasses = func() {
		if _, err := b.jsa.deleteConsumer(mqttStreamName, durable, false); err != nil {
			t.Logf("deleting the group durable between the passes: %v", err)
		}
	}
	b.asm.mu.Unlock()
	defer func() {
		b.asm.mu.Lock()
		b.asm.testSharedGroupBetweenPasses = nil
		b.asm.mu.Unlock()
	}()

	c, _, reason, props := testSharedResidentReconnect(t, b, "postpassabsA", 300)
	defer c.Close()
	if reason == mqttConnAckRCConnectionAccepted {
		t.Fatal("a durable deleted between the two passes left the resume ACCEPTED: the " +
			"post-pass must treat absence as terminal, or the repair's own window becomes " +
			"a hole that swallows a durable being deleted underneath this CONNECT")
	}
	if props == nil || !strings.Contains(props.reasonString, "does not exist") {
		t.Fatalf("the refusal does not report the durable as absent: %+v", props)
	}
}

// TestMQTTSharedGroupTriggerRollsBackWithItsFilter pins the commit boundary: the
// arbitration trigger is committed only once a WHOLE filter succeeds.
//
// A multi-level shared filter owns two durables, and its level-up companion is installed
// after the main half. If the companion fails, the filter is rolled back — and the trigger
// must roll back with it, or a CONNECT that installed no surviving shared interest still
// performs an ownership write. The restore-exit seam fails the companion at exactly that
// point, which is the only way to reach the window.
func TestMQTTSharedGroupTriggerRollsBackWithItsFilter(t *testing.T) {
	b := testQ2AffSetup(t, "triggerrollback")
	id := "triggerrollbackA"

	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r1)
	// "foo/#" needs a level-up companion, so this filter owns two durables.
	if codes := testMQTTSubV5(t, c1, r1, 1, []mqttV5SubFilter{
		{topic: "$share/" + b.group + "/triggerrollback/#", opts: 2},
	}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK %v", codes)
	}
	testMQTTFlush(t, c1, nil, r1)
	b.bind(t, id) // populates b.asm, which the seam below installs onto
	c1.Close()

	// Fail the companion's consumer exit, after the main half has installed.
	b.asm.mu.Lock()
	b.asm.testRestoreFilterFail = func(exit, sid string) error {
		if exit == "consumer-fwc" {
			return errors.New("mqtt test: the level-up companion fails after its main half")
		}
		return nil
	}
	b.asm.mu.Unlock()
	defer func() {
		b.asm.mu.Lock()
		b.asm.testRestoreFilterFail = nil
		b.asm.mu.Unlock()
	}()

	n := testCountArbitrations(t)
	c2, _, reason, _ := testSharedResidentReconnect(t, b, id, 300)
	defer c2.Close()
	if reason == mqttConnAckRCConnectionAccepted {
		t.Fatal("the companion failure did not refuse the CONNECT, so the rollback window " +
			"was never entered")
	}
	if got := n.Load(); got != 0 {
		t.Fatalf("a CONNECT whose only shared filter rolled back arbitrated %d times, want "+
			"0: the trigger is committed at the WHOLE-FILTER boundary, so a filter that "+
			"fails after installing its main half must take the trigger back with it", got)
	}
}

// TestMQTTSharedGroupRepairAtCapacityStillRepairs pins the ceiling check to the charge it
// actually guards.
//
// A repair recreates a durable this session object was already charged for on an earlier
// connection, so it adds NOTHING to tmaxack. If the ceiling check still evaluated
// tmaxack + maxAckPending, a session sitting near the limit would be refused the repair on
// the strength of a charge already counted inside tmaxack — and that refusal is permanent
// for as long as the session stays near capacity, turning a recoverable absent durable back
// into the F-076 dead end this item exists to end.
//
// The session is put deliberately close to the ceiling: two consumers at 30000 leave 5535
// of the 65535 budget, so any recharge of 30000 would exceed it.
func TestMQTTSharedGroupRepairAtCapacityStillRepairs(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.MaxAckPending = 30000
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	id := "capacityA"
	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r1)
	// QoS 1, not 2: a QoS 2 filter additionally provisions this session's PUBREL consumer,
	// which takes its own MaxAckPending out of the same budget and would push the SUBSCRIBE
	// itself over the ceiling — leaving the fixture with no resident durable to repair.
	if codes := testMQTTSubV5(t, c1, r1, 1, []mqttV5SubFilter{
		{topic: "$share/capgrp/capacity/one", opts: 1},
		{topic: "$share/capgrp/capacity/two", opts: 1},
	}); len(codes) != 2 || codes[0] != 1 || codes[1] != 1 {
		t.Fatalf("SUBACK %v: both shared filters must be granted for this fixture to have "+
			"two charged group consumers", codes)
	}
	testMQTTFlush(t, c1, nil, r1)

	cl := testMQTTGetClient(t, s, id)
	sess, jsa := cl.mqtt.sess, cl.mqtt.sess.jsa
	sess.mu.Lock()
	tmaxack := sess.tmaxack
	var durable string
	for _, cc := range sess.cons {
		if cc != nil && cc.DeliverGroup != _EMPTY_ {
			durable = cc.Durable
			break
		}
	}
	sess.mu.Unlock()
	if durable == _EMPTY_ {
		t.Fatal("no resident group durable")
	}
	// The fixture is only meaningful if a RECHARGE would breach the ceiling.
	if tmaxack+int(o.MQTT.MaxAckPending) <= mqttMaxAckTotalLimit {
		t.Fatalf("tmaxack %d plus another %d is still inside the %d ceiling, so a "+
			"double-charging build would not be refused and this fixture proves nothing",
			tmaxack, o.MQTT.MaxAckPending, mqttMaxAckTotalLimit)
	}
	c1.Close()

	if _, err := jsa.deleteConsumer(mqttStreamName, durable, false); err != nil {
		t.Fatalf("deleting the group durable out of band: %v", err)
	}

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	_, reason, props := testMQTTReadConnAckV5(t, r2)
	if reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("a session near the max_ack_pending ceiling was refused the repair of a "+
			"durable it is ALREADY charged for (0x%x, %v): the ceiling check must move with "+
			"the charge it guards", reason, props)
	}
	if ci, lerr := jsa.lookupConsumer(mqttStreamName, durable); lerr != nil || ci == nil {
		t.Fatalf("the durable was not repaired: %v", lerr)
	}
}

// TestMQTTSharedGroupIndeterminateIsNeverRepaired pins the boundary of the repairable class
// on the side that matters most.
//
// The refusal itself is already covered, and it is NOT the discriminating observation: an
// indeterminate verdict is refused by the post-pass whatever the pre-pass admitted, so a
// build that widened repairability to every error still refuses this CONNECT and every
// refusal-only fixture passes against it.
//
// What such a build does wrong is CREATE. An indeterminate reading established nothing —
// the durable may be perfectly healthy and merely unreadable from here — so recreating on it
// turns one broker's transient error into a group-wide provisioning act
// (feedback_absence_is_not_a_fact). So the oracle is that no group-durable create is issued
// at all.
func TestMQTTSharedGroupIndeterminateIsNeverRepaired(t *testing.T) {
	b := testQ2AffSetup(t, "indetnorepair")
	ca, _ := b.member(t, "indetnorepairA", 300)
	b.bind(t, "indetnorepairA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "indetnorepairA", durable)
	ca.Close()

	var created atomic.Bool
	h := func(boundary string) {
		if strings.HasPrefix(boundary, "after-group-create:") {
			created.Store(true)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&h)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	restore := testSetSharedGroupLookupFail(b.asm, durable)
	c, _, reason, _ := testSharedResidentReconnect(t, b, "indetnorepairA", 300)
	restore()
	defer c.Close()

	if reason == mqttConnAckRCConnectionAccepted {
		t.Fatal("an indeterminate group-durable lookup was accepted")
	}
	if created.Load() {
		t.Fatal("an INDETERMINATE verdict reached the create arm: nothing established that " +
			"the durable is absent, so recreating it turns this broker's transient read " +
			"error into a group-wide provisioning act. Only an authoritative absence is " +
			"repairable.")
	}
}
