// Copyright 2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testMQTTSubV5NoSubAck writes a SUBSCRIBE and does NOT read a SUBACK, because these
// fixtures fail the packet on purpose and the server closes the connection instead of
// answering. Same wire encoding as testMQTTSubV5PropsEx, minus the read.
func testMQTTSubV5NoSubAck(t *testing.T, c net.Conn, pi uint16, subID int, filters []mqttV5SubFilter) {
	t.Helper()
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	// A nonzero subID sends a real Subscription Identifier property. Without one the
	// packet carries empty properties, sess.subIDs is never populated, and every
	// assertion about identifier restoration is vacuous — which is exactly what the
	// holistic review caught.
	if subID > 0 {
		props := newMQTTWriter(0)
		props.WriteByte(mqttPropSubscriptionID)
		props.WriteVarInt(subID)
		vh.WriteVarInt(props.Len())
		vh.Write(props.Bytes())
	} else {
		vh.WriteVarInt(0)
	}
	for _, f := range filters {
		vh.WriteBytes([]byte(f.topic))
		vh.WriteByte(f.opts)
	}
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}
}

// F-039. rollbackSubscribePacket's definite-failure arm restores sess.cons, the delivery
// subscriptions it closed, the register nonces it voided and the affine QoS 2 path it
// unwound — and used to forget sess.sharedSubs / rapSubs / subIDs. A shared filter's
// membership is persisted EAGERLY, per filter, before its group consumer exists, so the
// packet has already made a durable attempt the final save's failure did not undo. The
// next save from the session then rewrote the record WITHOUT the membership while the
// group durable was live, and no sweep reclaims a $MQTT_msgs group durable.
//
// Every fixture here is MQTT 5: in 3.1.1 "$share/..." is a literal plain filter and no
// group durable exists at all. Shared QoS 1, never 2 — a shared QoS 2 request is
// downgraded to 1 without the experimental flag, so a "QoS 2" fixture would silently
// test QoS 1.
//
// Every packet carries a PLAIN filter as well. The final save runs only when
// `needUpdate || convert()` holds: needUpdate is false for the shared filter because the
// eager write already stored its key at that value, and convert() is false because no
// QoS 0 downgrade is staged. A shared-only packet therefore produces no final save and
// no rollback at all, and the fixture would test nothing.

// testSharedRollbackObs is what the release hook sampled: the shared membership state at
// the moment the packet's record transaction was released.
type testSharedRollbackObs struct {
	sharedQoS   byte
	sharedFound bool
	rapFound    bool
	subID       int
	plainFound  bool
	consFound   bool
	// sampled is written by the hook's goroutine and read by the test body, so it is
	// atomic; the fields above are published before it and read only after it is true.
	sampled atomic.Bool
}

// testObserveSharedAtRollback arms the record-transaction release seam and returns the
// slot it fills.
//
// This is the observation point, and the choice is load-bearing. The failed packet closes
// the connection immediately, so a test body cannot look afterwards. Sampling inside the
// save hook is too EARLY — the rollback's own classification has not happened. Sampling
// at the close save is too LATE — it can land after the resolver has run. The release hook
// fires with sess.mu HELD and the fence still OPEN, so it observes exactly what the next
// holder of the fence will see, which is what turns "a definite failure is undone before
// the transaction is released" into an assertion rather than a race with the scheduler.
//
// The handler runs with sess.mu held, so it reads fields directly and never relocks.
func testObserveSharedAtRollback(t *testing.T, target **mqttSession, gate func() bool, sharedKey, plainKey, durable string) (*testSharedRollbackObs, func()) {
	t.Helper()
	obs := &testSharedRollbackObs{}
	var done atomic.Bool
	restore := setMQTTTestRecordTxReleaseHook(func(s *mqttSession) {
		// Only this test's session, only ONCE, and only the release that follows the
		// ROLLBACK's own save. Gating on "first release" alone would depend on timing: a
		// fixture with a preSub also runs F-078's post-packet ownership proof, which holds
		// a transaction of its own. gate() names the release we mean.
		if *target == nil || s != *target || !gate() || !done.CompareAndSwap(false, true) {
			return
		}
		obs.sharedQoS, obs.sharedFound = s.sharedSubs[sharedKey]
		_, obs.rapFound = s.rapSubs[sharedKey]
		obs.subID = s.subIDs[sharedKey]
		_, obs.plainFound = s.subs[plainKey]
		if durable != "" {
			for _, cc := range s.cons {
				if cc != nil && cc.Durable == durable {
					obs.consFound = true
				}
			}
		}
		obs.sampled.Store(true)
	})
	return obs, restore
}

// testSharedRollbackFixture runs one SUBSCRIBE whose final save and whose rollback save
// both fail DEFINITELY, which is the only arm that reaches readd().
//
// saveFailAt names the 1-based ordinals of persistRecord entries to refuse. A plain error
// from THIS hook is always saveDefinitelyFailed: it returns before uncertainty is armed,
// and classifySave depends on that transition. Producing an ambiguity needs the
// store-result seam instead, which is why T5 uses that one.
type testSharedRollbackFixture struct {
	clientID   string
	group      string
	topic      string
	plain      string
	sharedOpts byte
	plainOpts  byte
	props      []byte
	// subID, when nonzero, is sent as the failing packet's Subscription Identifier.
	subID int
	// preSubID, when nonzero, is the identifier the PRE-EXISTING subscription carries.
	preSubID int
	// preSub, when set, runs a first SUBSCRIBE that is allowed to succeed, so the failing
	// packet meets a PRE-EXISTING membership.
	preSub []mqttV5SubFilter
	// saveFailAt is consulted against the count of persistRecord entries for this session.
	saveFailAt map[int32]bool
}

func testRunSharedRollback(t *testing.T, o *Options, f testSharedRollbackFixture) (*testSharedRollbackObs, *Server, string) {
	t.Helper()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })

	topicSubj, err := mqttToNATSSubjectConversion([]byte(f.topic), false)
	if err != nil {
		t.Fatalf("convert topic %q: %v", f.topic, err)
	}
	_, _, durable := mqttSharedNames(f.group, string(topicSubj))
	sharedKey := "$share/" + f.group + "/" + f.topic

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: f.clientID, cleanStart: true, props: f.props}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	if len(f.preSub) > 0 {
		var pp []byte
		if f.preSubID > 0 {
			// testMQTTSubV5PropsEx writes this block VERBATIM, so it must carry its own
			// length prefix — passing a bare body makes the server read the first
			// property byte as the length and reject the packet.
			body := newMQTTWriter(0)
			body.WriteByte(mqttPropSubscriptionID)
			body.WriteVarInt(f.preSubID)
			w := newMQTTWriter(0)
			w.WriteVarInt(body.Len())
			w.Write(body.Bytes())
			pp = w.Bytes()
		}
		testMQTTSubV5Props(t, c, r, 1, pp, f.preSub)
	}

	asm := testMQTTGetAccountSessionManager(t, s, f.clientID)
	hash := string(getHash(f.clientID))
	asm.mu.RLock()
	sess := asm.sessByHash[hash]
	asm.mu.RUnlock()
	require_NotNil(t, sess)

	// The group durable must EXIST before the failing packet, or a later "gone" assertion
	// cannot tell a release from a create that never happened. Positive control.
	if len(f.preSub) > 0 && !testMQTTConsumerExists(t, s, f.clientID, durable) {
		t.Fatalf("precondition: group durable %q was never created, so the fixture tests nothing", durable)
	}

	plainSubj, err := mqttToNATSSubjectConversion([]byte(f.plain), false)
	if err != nil {
		t.Fatalf("convert plain filter %q: %v", f.plain, err)
	}
	plainKey := string(plainSubj)

	// The rollback's own save is ordinal 3 (measured; see the log below), so the release
	// that matters is the one taken while the counter is at 3 or beyond.
	var saves atomic.Int32
	obs, restore := testObserveSharedAtRollback(t, &sess, func() bool { return saves.Load() >= 3 },
		sharedKey, plainKey, durable)
	defer restore()

	restoreSave := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		n := saves.Add(1)
		// The ordinals are MEASURED, not assumed: every persistRecord entry for this
		// session is logged with its caller, so the fixture's failure points come from
		// the tree rather than from a guess about how many writes a packet makes.
		pc := make([]uintptr, 12)
		np := runtime.Callers(2, pc)
		frames := runtime.CallersFrames(pc[:np])
		var where []string
		for {
			fr, more := frames.Next()
			if fn := fr.Function; fn != "" {
				where = append(where, fmt.Sprintf("%s:%d", fn[strings.LastIndexByte(fn, '.')+1:], fr.Line))
			}
			if !more || len(where) >= 6 {
				break
			}
		}
		t.Logf("F-039 fixture: session record write #%d from %s (failing=%v)", n, strings.Join(where, " <- "), f.saveFailAt[n])
		if f.saveFailAt[n] {
			return errors.New("F-039 fixture: refusing this session record write")
		}
		return nil
	})
	defer restoreSave()

	testMQTTSubV5NoSubAck(t, c, 2, f.subID, []mqttV5SubFilter{
		{topic: sharedKey, opts: f.sharedOpts},
		{topic: f.plain, opts: f.plainOpts},
	})
	// A v5 server sends a DISCONNECT before it closes, so the close is only observable
	// after that packet has been read.
	rc, _ := testMQTTReadDisconnectV5(t, r)
	t.Logf("F-039 fixture: server DISCONNECT reason 0x%02x", rc)
	testMQTTExpectDisconnect(t, c)

	if !obs.sampled.Load() {
		t.Fatal("the record-transaction release seam never fired for this session, so nothing was observed — the fixture did not reach the rollback")
	}
	return obs, s, durable
}

// TestMQTTSubscribeRollbackRestoresSharedMembership is T1: the in-memory restoration
// itself. The plain key's ABSENCE is the control — the rollback really did revert the
// packet, so a passing shared assertion is not just "nothing happened".
func TestMQTTSubscribeRollbackRestoresSharedMembership(t *testing.T) {
	// NOT t.Parallel(). setMQTTTestSessSaveHook, setMQTTTestSessStoreResultHook and
	// setMQTTTestRecordTxReleaseHook are PROCESS-GLOBAL atomic pointers, so parallel
	// fixtures overwrite each other's arming and each one silently stops failing the save
	// it needs to fail — which looks like a passing packet, not like a broken fixture.
	o := testMQTTDefaultOptionsV5()
	obs, _, _ := testRunSharedRollback(t, o, testSharedRollbackFixture{
		clientID:   "f039-t1",
		group:      "g1",
		topic:      "foo",
		plain:      "bar",
		sharedOpts: 1,
		plainOpts:  1,
		props:      mqttV5PropsSessionExpiryReceiveMax(300, 0),
		saveFailAt: map[int32]bool{2: true, 3: true},
	})
	if !obs.sharedFound {
		t.Fatal("F-039: the shared membership was dropped by the rollback's definite arm")
	}
	if obs.sharedQoS != 1 {
		t.Fatalf("shared membership restored at QoS %d, want 1", obs.sharedQoS)
	}
	if !obs.consFound {
		t.Fatal("the group consumer entry was not restored, so the membership was restored without its config")
	}
	if obs.plainFound {
		t.Fatal("control failed: the plain filter survived the rollback, so this packet was not actually reverted")
	}
}

// TestMQTTSubscribeRollbackKeepsTheSharedMembershipInTheRecord is T2, and it is the
// assertion the "make the release walk both maps" remedy cannot satisfy: the evidence is
// the PERSISTED RECORD after the close, not the in-memory maps.
func TestMQTTSubscribeRollbackKeepsTheSharedMembershipInTheRecord(t *testing.T) {
	// NOT t.Parallel(). setMQTTTestSessSaveHook, setMQTTTestSessStoreResultHook and
	// setMQTTTestRecordTxReleaseHook are PROCESS-GLOBAL atomic pointers, so parallel
	// fixtures overwrite each other's arming and each one silently stops failing the save
	// it needs to fail — which looks like a passing packet, not like a broken fixture.
	o := testMQTTDefaultOptionsV5()
	obs, s, _ := testRunSharedRollback(t, o, testSharedRollbackFixture{
		clientID:   "f039-t2",
		group:      "g2",
		topic:      "foo",
		plain:      "bar",
		sharedOpts: 1,
		plainOpts:  1,
		props:      mqttV5PropsSessionExpiryReceiveMax(300, 0),
		saveFailAt: map[int32]bool{2: true, 3: true},
	})
	if !obs.sharedFound {
		t.Fatal("precondition: the in-memory restoration did not happen, so the record assertion below would be vacuous")
	}
	probe, _ := testSharedRollbackProbeASM(t, s, o)
	ps := testLoadPersistedSession(t, probe, "f039-t2")
	if _, ok := ps.SharedSubs["$share/g2/foo"]; !ok {
		t.Fatalf("F-039: the close save rewrote the record WITHOUT the shared membership while its group durable is live; record holds %v", ps.SharedSubs)
	}
}

// testSharedRollbackProbeASM connects a throwaway client purely to reach the account
// session manager: the fixture's own client is gone by the time the record is read.
func testSharedRollbackProbeASM(t *testing.T, s *Server, o *Options) (*mqttAccountSessionManager, string) {
	t.Helper()
	probeID := "f039-probe-" + nuid.Next()
	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: probeID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pc.Close() })
	testMQTTReadConnAckV5(t, pr)
	// The client ID comes back too: testMQTTConsumerExists reaches the account through a
	// LIVE client, and the fixture's own client is gone by the time anything is read.
	return testMQTTGetAccountSessionManager(t, s, probeID), probeID
}

// TestMQTTSubscribeRollbackReleasesTheSharedGroupDurable is T3, and it is the one that
// shows the leak is a leak rather than a bookkeeping detail. Session Expiry Interval 0 and
// a sole member, so the close TEARS DOWN the session: destroy derives the shared durables
// to release from sess.sharedSubs ALONE and skips every DeliverGroup entry in sess.cons,
// so a dropped membership means the durable is never named and never released — and
// nothing reclaims it, because $MQTT_msgs is interest-retained and the sweep route for a
// group durable on that stream is gone.
//
// The positive control is obs.consFound: the group consumer entry was resident at the
// rollback, so the durable existed and a later "gone" cannot mean "never created".
//
// The delete lands after the socket closes, so this polls rather than asserting once.
func TestMQTTSubscribeRollbackReleasesTheSharedGroupDurable(t *testing.T) {
	// NOT t.Parallel(): see the note in T1.
	o := testMQTTDefaultOptionsV5()
	obs, s, durable := testRunSharedRollback(t, o, testSharedRollbackFixture{
		clientID:   "f039-t3",
		group:      "g3",
		topic:      "foo",
		plain:      "bar",
		sharedOpts: 1,
		plainOpts:  1,
		// Expiry 0: the close destroys the session rather than stamping a disconnect.
		props:      mqttV5PropsSessionExpiryReceiveMax(0, 0),
		saveFailAt: map[int32]bool{2: true, 3: true},
	})
	if !obs.sharedFound {
		t.Fatal("F-039: the shared membership was dropped by the rollback's definite arm")
	}
	if !obs.consFound {
		t.Fatal("control failed: no group consumer entry was resident at the rollback, so the durable may never have existed and a 'gone' assertion below would prove nothing")
	}
	probe, probeID := testSharedRollbackProbeASM(t, s, o)
	_ = probe
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if testMQTTConsumerExists(t, s, probeID, durable) {
			return fmt.Errorf("group durable %q is still present: the teardown never named it, which is what a dropped shared membership causes", durable)
		}
		return nil
	})
}

// TestMQTTSubscribeRollbackCarriesSharedFilterOptions is T4. The negative half is what
// discriminates: rapSubs and subIDs are keyed by session key and carry PLAIN filters too,
// so a capture that copied the whole option map would leak the plain filter's identifier
// into the record. Asserting on the identifier VALUE would not discriminate, because the
// same identifier legitimately belongs to the shared filter's key — so the assertion is
// keyed by map ENTRY.
func TestMQTTSubscribeRollbackCarriesSharedFilterOptions(t *testing.T) {
	// NOT t.Parallel(). setMQTTTestSessSaveHook, setMQTTTestSessStoreResultHook and
	// setMQTTTestRecordTxReleaseHook are PROCESS-GLOBAL atomic pointers, so parallel
	// fixtures overwrite each other's arming and each one silently stops failing the save
	// it needs to fail — which looks like a passing packet, not like a broken fixture.
	o := testMQTTDefaultOptionsV5()
	// Retain As Published is bit 3 of the subscription options byte.
	const rap = byte(1) | 0x08
	obs, s, _ := testRunSharedRollback(t, o, testSharedRollbackFixture{
		clientID:   "f039-t4",
		group:      "g4",
		topic:      "foo",
		plain:      "bar",
		sharedOpts: rap,
		// The PLAIN filter carries Retain As Published TOO, and that is what makes the
		// negative half discriminate. With the plain filter at bare QoS 1 there is nothing
		// in rapSubs for an unfiltered capture to leak, so M4 passed and the assertion
		// proved nothing. Measured, not assumed.
		plainOpts: rap,
		// A REAL Subscription Identifier. A Subscription Identifier in a SUBSCRIBE applies
		// to every filter in the packet, so both keys get one — which is what lets the
		// negative half below discriminate for subIDs the same way RAP does.
		subID:      7,
		props:      mqttV5PropsSessionExpiryReceiveMax(300, 0),
		saveFailAt: map[int32]bool{2: true, 3: true},
	})
	if !obs.rapFound {
		t.Fatal("F-039: Retain As Published was dropped for the restored shared key")
	}
	if obs.subID != 7 {
		t.Fatalf("F-039: the Subscription Identifier was dropped for the restored shared key: got %d, want 7", obs.subID)
	}
	probe, _ := testSharedRollbackProbeASM(t, s, o)
	ps := testLoadPersistedSession(t, probe, "f039-t4")
	var sawShared, sawPlain bool
	for _, k := range ps.RetainAsPublished {
		if k == "$share/g4/foo" {
			sawShared = true
		}
		if k == "bar" {
			sawPlain = true
		}
	}
	if !sawShared {
		t.Fatalf("the record lost Retain As Published for the shared key; holds %v", ps.RetainAsPublished)
	}
	if sawPlain {
		t.Fatalf("the record gained an option entry for the PLAIN filter, whose subscription the rollback removed — the capture is not filtered by membership; holds %v", ps.RetainAsPublished)
	}
	// The identifier, keyed by map ENTRY in both directions. Asserting the VALUE alone
	// would not discriminate: the same identifier legitimately belongs to the shared key.
	if got, ok := ps.SubIDs["$share/g4/foo"]; !ok || got != 7 {
		t.Fatalf("the record lost the Subscription Identifier for the shared key: SubIDs=%v", ps.SubIDs)
	}
	if _, ok := ps.SubIDs["bar"]; ok {
		t.Fatalf("the record gained a Subscription Identifier for the PLAIN filter, whose subscription the rollback removed — the capture is not filtered by membership; SubIDs=%v", ps.SubIDs)
	}
}

// TestMQTTSubscribeRollbackClearsSharedFilterOptions is T4's CLEARING case, and it is the
// half an implementation that only ever ADDS would pass without. A pre-existing shared
// membership carries Retain As Published; the failing packet re-subscribes the same filter
// WITHOUT it. The attempted state is "no RAP", so the restoration must express the
// absence — both helpers clear as well as set — and the record must lose the option.
//
// The eager per-filter write still happens here even though the QoS is unchanged, because
// rapSubs changes and update() writes whenever a map changes.
func TestMQTTSubscribeRollbackClearsSharedFilterOptions(t *testing.T) {
	// NOT t.Parallel(): see the note in T1.
	o := testMQTTDefaultOptionsV5()
	const rap = byte(1) | 0x08
	obs, s, _ := testRunSharedRollback(t, o, testSharedRollbackFixture{
		clientID:   "f039-t4b",
		group:      "g4b",
		topic:      "foo",
		plain:      "bar",
		sharedOpts: 1, // no RAP this time: the packet CLEARS it
		plainOpts:  1,
		// And no Subscription Identifier, where the pre-existing subscription had one, so
		// the identifier is cleared too. Both options are cleared by the same packet
		// because both go through helpers that must clear as well as set.
		subID:      0,
		preSubID:   9,
		props:      mqttV5PropsSessionExpiryReceiveMax(300, 0),
		preSub:     []mqttV5SubFilter{{topic: "$share/g4b/foo", opts: rap}},
		saveFailAt: map[int32]bool{2: true, 3: true},
	})
	if !obs.sharedFound {
		t.Fatal("precondition: the shared membership was dropped, so the option assertion would be vacuous")
	}
	if obs.rapFound {
		t.Fatal("F-039 M5: Retain As Published was left SET for the restored shared key. The attempted state had it cleared, and a restoration that only ever adds cannot express that")
	}
	if obs.subID != 0 {
		t.Fatalf("F-039 M5: the Subscription Identifier was left at %d for the restored shared key. The attempted state cleared it, and a restoration that only ever adds cannot express that", obs.subID)
	}
	probe, _ := testSharedRollbackProbeASM(t, s, o)
	ps := testLoadPersistedSession(t, probe, "f039-t4b")
	for _, k := range ps.RetainAsPublished {
		if k == "$share/g4b/foo" {
			t.Fatalf("the record still carries Retain As Published for the shared key after a packet that cleared it; holds %v", ps.RetainAsPublished)
		}
	}
	if got, ok := ps.SubIDs["$share/g4b/foo"]; ok {
		t.Fatalf("the record still carries Subscription Identifier %d for the shared key after a packet that cleared it; SubIDs=%v", got, ps.SubIDs)
	}
}

// TestMQTTSubscribeRollbackAmbiguousKeepsTheRolledBackMembership is T5, the arm boundary,
// and it PASSES AGAINST TODAY'S CODE BY DESIGN. It is a control, not a regression test:
// its job is to fail against M3, the mutation that applies the restoration on the
// saveMaybeLanded arm too.
//
// An ambiguity cannot come from the save hook, which returns before uncertainty is armed.
// It comes from the store-result seam, which rewrites the OUTCOME of a record write —
// and a plain error from THAT hook is an ambiguity rather than a definite failure, which
// is exactly what is wanted here. Note the CAS/ambiguity verdict has to be delivered in
// the error RETURN VALUE: persistRecord does not re-derive it from resp after the hook.
func TestMQTTSubscribeRollbackAmbiguousKeepsTheRolledBackMembership(t *testing.T) {
	// NOT t.Parallel(): see the note in T1. Three process-global seams are armed here.
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID, sharedKey = "f039-t5", "$share/g5/foo"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true,
		props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessByHash[string(getHash(clientID))]
	asm.mu.RUnlock()
	require_NotNil(t, sess)

	// The shared filter is NEW in the failing packet, deliberately. A pre-established
	// membership at the same QoS makes the eager per-filter write save NOTHING (update()
	// only writes when a map changes), which shifts every ordinal and sends the fixture
	// into proveSharedMembershipOwnership instead of the rollback. That is how an earlier
	// revision of this test "detected M3" without ever reaching the arm it names.
	var saves atomic.Int32
	var reachedRollbackSave atomic.Bool
	var sharedAtRelease, uncertainAtRelease, sampled atomic.Bool

	restoreRelease := setMQTTTestRecordTxReleaseHook(func(hs *mqttSession) {
		if hs != sess || !sampled.CompareAndSwap(false, true) {
			return
		}
		_, ok := hs.sharedSubs[sharedKey]
		sharedAtRelease.Store(ok)
		// The ARM is sampled HERE, not in the test body. The readLoop resolves the
		// session after the packet, and resolution CLEARS seqUncertain — so a body-level
		// isSeqUncertain() reads false and the arm evidence is gone. Reading the field
		// directly is correct: this hook already holds sess.mu.
		uncertainAtRelease.Store(hs.seqUncertain)
	})
	defer restoreRelease()

	restoreSave := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		// #1 eager shared membership write, #2 the packet's final save, #3 the rollback's
		// own restore save. Measured, not assumed — see the ordinal log in the shared
		// fixture helper.
		switch saves.Add(1) {
		case 2:
			return errors.New("F-039 T5: refusing the packet's final save")
		case 3:
			// Let it through to the store, where the result hook makes it AMBIGUOUS.
			reachedRollbackSave.Store(true)
		}
		return nil
	})
	defer restoreSave()

	restoreResult := setMQTTTestSessStoreResultHook(func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		// Only the rollback's own write, identified by the save ordinal rather than by a
		// store count: the refused final save never reached a store at all, so the two
		// sequences do not correspond.
		if hs != sess || saves.Load() != 3 {
			return resp, err
		}
		// The verdict has to be in the ERROR RETURN VALUE. persistRecord does not
		// re-derive it from resp after this hook, and a plain error HERE is an ambiguity
		// rather than a definite failure — which is exactly the arm under test, and is
		// why the save hook cannot produce it.
		return nil, errors.New("F-039 T5: the rollback's own write is unresolved")
	})
	defer restoreResult()

	testMQTTSubV5NoSubAck(t, c, 2, 0, []mqttV5SubFilter{
		{topic: sharedKey, opts: 1},
		{topic: "bar", opts: 1},
	})
	rc, _ := testMQTTReadDisconnectV5(t, r)
	t.Logf("F-039 T5: server DISCONNECT reason 0x%02x, saves=%d", rc, saves.Load())
	testMQTTExpectDisconnect(t, c)

	// Three route assertions BEFORE the behavioural one. Without them this test passes
	// for a session that never rolled back and simply had no shared key.
	if got := saves.Load(); got < 3 {
		t.Fatalf("the fixture made only %d session record writes, so the rollback's own save was never reached", got)
	}
	if !reachedRollbackSave.Load() {
		t.Fatal("the rollback's restore save was never reached, so no ambiguity was injected")
	}
	if !sampled.Load() {
		t.Fatal("the record-transaction release seam never fired")
	}
	if !uncertainAtRelease.Load() {
		t.Fatal("the injected error did not arm uncertainty at the transaction's release, so this is the DEFINITE arm and not saveMaybeLanded — the fixture is not testing the arm it names")
	}

	if sharedAtRelease.Load() {
		t.Fatal("F-039 M3: the shared membership was restored on the AMBIGUOUS arm. The rolled-back record may be exactly what landed, and the resolver settles it by re-attempting those bytes — so the rolled-back view must stand")
	}
}

// TestMQTTSubscribeRollbackPreservesASucceededJoinWhenALaterOneIsCASRejected is T6: two
// shared filters in one packet where A's eager membership write SUCCEEDS and B's is
// CAS-REJECTED. processSubs records facts.recordCASRejected, restores B's prior value and
// CONTINUES, so the packet reaches its final save with A's key durably stored and B's not.
// A's membership must survive the rollback — the pre-packet snapshot would discard it, so
// M1 kills this — and B's must be back at its prior value (absent).
//
// This is the case that makes the invariant "the state as it stood before the
// compensation" rather than "the pre-packet snapshot": nothing local reverted A's durable
// write, and no local snapshot can capture what another writer did to the record.
//
// THE RECORD IS GENUINELY DISPLACED, by an INDEPENDENT unconditional write issued between
// A's write and B's. An earlier revision injected a wrong-last-sequence error at the store
// result instead, and the holistic review was right to reject that: rewriting a successful
// store's result cannot undo the store, so it exercised the rejected-arm continuation
// without ever moving the record. Here the displacement is real — the writer omits the
// JSExpectedLastSubjSeq header, so its append advances the subject's last sequence — and
// every rejection after it comes from the SERVER:
//
//	B's eager write, the packet's final save and the rollback's own save are all
//	CAS-rejected because sess.seq is now stale. Nothing is injected at all.
//
// A CAS rejection classifies as saveDefinitelyFailed, which is the arm that reaches
// readd(). It does NOT assert that F-078 arbitration ran, because it does not: an error
// from processSubs returns before it.
func TestMQTTSubscribeRollbackPreservesASucceededJoinWhenALaterOneIsCASRejected(t *testing.T) {
	// NOT t.Parallel(): see the note in T1.
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "f039-t6"
	const keyA, keyB = "$share/g6/aaa", "$share/g6/bbb"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true,
		props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	hash := string(getHash(clientID))
	asm.mu.RLock()
	sess := asm.sessByHash[hash]
	asm.mu.RUnlock()
	require_NotNil(t, sess)

	var saves atomic.Int32
	var aAtRelease, bAtRelease, sampled, displaced atomic.Bool

	restoreRelease := setMQTTTestRecordTxReleaseHook(func(hs *mqttSession) {
		// Ordinals: #1 A's eager write, #2 B's (rejected), #3 the packet's final save,
		// #4 the rollback's own. The release that matters is at 4 or beyond.
		if hs != sess || saves.Load() < 4 || !sampled.CompareAndSwap(false, true) {
			return
		}
		_, okA := hs.sharedSubs[keyA]
		_, okB := hs.sharedSubs[keyB]
		aAtRelease.Store(okA)
		bAtRelease.Store(okB)
	})
	defer restoreRelease()

	restoreSave := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		// The hook fires at persistRecord ENTRY, before it takes sess.mu and before its
		// CAS — which is the only window where a foreign write can make that CAS stale.
		if saves.Add(1) != 2 {
			return nil
		}
		// Read the record and append it again UNCONDITIONALLY: hdr = 0 means no
		// JSExpectedLastSubjSeq, so the store is not a compare-and-swap and simply
		// advances the subject's last sequence. Storing the same body keeps the record
		// semantically intact — the point is the sequence, not the contents.
		smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
		if err != nil {
			t.Errorf("T6 could not read the session record to displace it: %v", err)
			return nil
		}
		if _, err := asm.jsa.storeSessionMsg(asm.domainTk, hash, 0, smsg.Data); err != nil {
			t.Errorf("T6 could not displace the session record: %v", err)
			return nil
		}
		displaced.Store(true)
		// Return nil: B's write goes ahead and the SERVER rejects it.
		return nil
	})
	defer restoreSave()

	testMQTTSubV5NoSubAck(t, c, 2, 0, []mqttV5SubFilter{
		{topic: keyA, opts: 1},
		{topic: keyB, opts: 1},
		{topic: "bar", opts: 1},
	})
	rc, _ := testMQTTReadDisconnectV5(t, r)
	t.Logf("F-039 T6: server DISCONNECT reason 0x%02x, saves=%d", rc, saves.Load())
	testMQTTExpectDisconnect(t, c)

	// Route assertions first: without them this test passes for a packet that never
	// displaced anything and never rolled back.
	if !displaced.Load() {
		t.Fatal("the record was never displaced, so B's write was never genuinely CAS-rejected")
	}
	if got := saves.Load(); got < 4 {
		t.Fatalf("only %d session record writes: the rollback's own save was never reached", got)
	}
	if !sampled.Load() {
		t.Fatal("the record-transaction release seam never fired for the rollback")
	}

	if !aAtRelease.Load() {
		t.Fatalf("F-039: filter A's membership (%q) was discarded by the rollback, even though its eager write SUCCEEDED and nothing local reverted it", keyA)
	}
	if bAtRelease.Load() {
		t.Fatalf("filter B's membership (%q) is present, but its eager write was CAS-REJECTED and processSubs restored its prior value — the capture took a state that never existed", keyB)
	}
}

// TestMQTTSubscribeRollbackRestoresAnUpgradedPreExistingSharedKey is T7. The pre-existing
// membership is at QoS 0 and the failed packet upgrades it to QoS 1, so the assertion is
// on the CHANGED VALUE. Presence alone would pass against M1, because the pre-packet
// snapshot still carries the key — which is the whole trap this test exists to avoid.
//
// A QoS 0 member still joins the group consumer, so the durable underneath is real.
func TestMQTTSubscribeRollbackRestoresAnUpgradedPreExistingSharedKey(t *testing.T) {
	// NOT t.Parallel(). setMQTTTestSessSaveHook, setMQTTTestSessStoreResultHook and
	// setMQTTTestRecordTxReleaseHook are PROCESS-GLOBAL atomic pointers, so parallel
	// fixtures overwrite each other's arming and each one silently stops failing the save
	// it needs to fail — which looks like a passing packet, not like a broken fixture.
	o := testMQTTDefaultOptionsV5()
	obs, s, durable := testRunSharedRollback(t, o, testSharedRollbackFixture{
		clientID:   "f039-t7",
		group:      "g7",
		topic:      "foo",
		plain:      "bar",
		sharedOpts: 1,
		plainOpts:  1,
		props:      mqttV5PropsSessionExpiryReceiveMax(300, 0),
		preSub:     []mqttV5SubFilter{{topic: "$share/g7/foo", opts: 0}},
		saveFailAt: map[int32]bool{2: true, 3: true},
	})
	if !obs.sharedFound {
		t.Fatal("F-039: the shared membership was dropped by the rollback's definite arm")
	}
	if obs.sharedQoS != 1 {
		t.Fatalf("F-039: the restored membership is at QoS %d, not the attempted 1 — the pre-packet snapshot was left in place", obs.sharedQoS)
	}
	// The record is the durable evidence: assert the upgraded value survived into it, not
	// just into memory. The durable's own fate on a finite-expiry session is the expiry
	// path's business, not this test's.
	probe, _ := testSharedRollbackProbeASM(t, s, o)
	ps := testLoadPersistedSession(t, probe, "f039-t7")
	if qos, ok := ps.SharedSubs["$share/g7/foo"]; !ok || qos != 1 {
		t.Fatalf("F-039: the record does not carry the upgraded shared membership; SharedSubs=%v (durable %q)", ps.SharedSubs, durable)
	}
}
