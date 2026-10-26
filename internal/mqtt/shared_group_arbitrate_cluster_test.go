//go:build mqtt_fulltests

package mqtt

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// F-077's arbitration against a REAL second broker on replicated JetStream.
//
// Every other F-077 fixture injects the rejection. That proves the verdict handling but not
// the thing the finding is about: whether a genuine cross-broker takeover is actually
// observed by the restore. On a replicated `$MQTT_sess` the expected-last-subject-sequence
// check runs in the clustered pre-proposal path rather than as a single-writer store check,
// which is the mechanism a real deployment runs and the one that has to reject broker A.
//
// The name matches `make cluster-failover-tests`' regex, which this change extends for
// exactly this reason: a replicated test the target does not run is a test nobody runs.

// TestMQTTSharedGroupOwnershipArbitratedReplicated is the case the whole item exists for.
//
// The ordering is the load-bearing part, and two earlier designs of it could not
// discriminate anything:
//
//   - If B takes over BEFORE A's restore reaches its ownership write, A's accept save is
//     itself rejected and the fixture only re-proves F-029. The takeover therefore has to
//     land while A is mid-restore, which is what the create boundary is for.
//   - If B's takeover DELETES the group durable, A's F-076 post-pass observes the absence
//     and refuses before the ownership write ever runs — in a build with the write moved
//     or removed too, so the fixture passes for a reason that has nothing to do with this
//     change. So a SECOND, different-client member holds the group, and the durable stays
//     present and canonical throughout. A is refused by the ownership write or not at all.
func TestMQTTSharedGroupOwnershipArbitratedReplicated(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b0 := c.startBroker(t, 0)
	defer b0.cleanup()
	b1 := c.startBroker(t, 1)
	defer b1.cleanup()

	const (
		clientA = "sharedown-A"
		clientB = "sharedown-B"
		group   = "sharedowngrp"
		topic   = "sharedown/topic"
		filter  = "$share/" + group + "/" + topic
	)

	// The other member, on b1, holding the group for the whole test. It is a DIFFERENT
	// client ID, so nothing it does touches A's record — and its membership is what keeps
	// the group durable present and canonical while A's own membership is removed.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientB,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b1.opts.MQTT.Host, b1.opts.MQTT.Port)
	defer cb.Close()
	testMQTTReadConnAckV5(t, rb)
	if codes := testMQTTSubV5(t, cb, rb, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("member B SUBACK %v", codes)
	}
	testMQTTFlush(t, cb, nil, rb)

	// A joins the same group on b0, then leaves its connection so the session object stays
	// resident with the membership persisted.
	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientA,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b0.opts.MQTT.Host, b0.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, ra)
	if codes := testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("member A SUBACK %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)
	ca.Close()

	// A's resume must take the record-sourced join arm, so it CREATES — which gives the
	// takeover below a boundary to land on. Shared configs are never persisted, so a
	// session rebuilt from its record has no shared cons entry; here the object is still
	// resident, so the durable is deleted out of band to force the same arm through the
	// repair. Either way the create fires and the durable is immediately back, still held
	// by B.
	sessA := testMQTTGetClient(t, b0.s, clientA).mqtt.sess
	durable := mqttSharedNamesDurable(group, strings.ReplaceAll(topic, "/", "."), 1)
	if _, err := sessA.jsa.deleteConsumer(mqttStreamName, durable, false); err != nil {
		t.Fatalf("deleting the group durable out of band: %v", err)
	}

	// THE TAKEOVER, at the create boundary: A has recreated the durable and has not yet
	// written its ownership record. B's broker accepts a newer CONNECT for A's client ID,
	// which commits a newer record for that subject and moves the sequence A is about to
	// CAS against.
	// At "after-group-create", the LAST group-durable write of the restore and the last
	// boundary before the ownership CAS. Staging the takeover here is what gives the fixture
	// its discriminating power: a mutant ownership write placed AHEAD of the create has
	// already linearized when B takes over, so it is not rejected, A is CONNACKed, and the
	// test fails on the ordering it exists to prove.
	var sawCreate, tookOver atomic.Bool
	var arbitrated atomic.Int32
	h := func(boundary string) {
		switch {
		case strings.HasPrefix(boundary, "after-group-create:"):
			sawCreate.Store(true)
			if tookOver.CompareAndSwap(false, true) {
				// A clean start on the OTHER broker: it takes the session over and its
				// membership does not include A's shared filter.
				ct, rt := testMQTTConnectV5(t, &mqttV5ConnInfo{
					clientID:   clientA,
					cleanStart: true,
					props:      mqttV5ConnPropsSessionExpiry(300),
				}, b1.opts.MQTT.Host, b1.opts.MQTT.Port)
				testMQTTReadConnAckV5(t, rt)
				t.Cleanup(func() { ct.Close() })
			}
		case boundary == "after-ownership-cas":
			arbitrated.Add(1)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&h)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	// A resumes on b0. Its ownership write CASes against a sequence B has moved.
	cr, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientA,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b0.opts.MQTT.Host, b0.opts.MQTT.Port)
	defer cr.Close()

	pt, _, answered := testMQTTReadPacketReady(rr, 3*time.Second)

	if !sawCreate.Load() {
		t.Fatal("the restore never reached the group-durable create")
	}
	if !tookOver.Load() {
		t.Fatal("no takeover was staged, so this fixture proves nothing")
	}
	if got := arbitrated.Load(); got != 1 {
		t.Fatalf("A's restore arbitrated %d times, want 1: without the ownership write "+
			"there is nothing for B's takeover to be observed by", got)
	}
	// A must NOT be ACCEPTED. That is the invariant: an accepted resume would serve group
	// deliveries under a membership the record no longer carries and ACKNOWLEDGE them,
	// destroying them for the member that legitimately holds the group.
	//
	// A refusal now carries a REASON, where this test's earlier revision expected no packet at
	// all. That expectation was right at the time and said so; generalising the pre-CONNACK
	// refusal signal was a roadmap item and it has landed. A failure CONNACK accepts nothing:
	// mqttRecordConnAckWrittenLocked records the [MQTT-3.2.0-1] flag only for an accepted code,
	// so the displacement DISCONNECT stays gated exactly as before.
	// UNCONDITIONAL. An earlier revision of this assertion was guarded on `answered &&
	// pt == CONNACK`, which meant an EOF, a timeout, a non-CONNACK packet or a missing refusal
	// signal all skipped every check — so a mutation that removed the signal, or moved it after
	// the asynchronous disposal, survived. The holistic review caught that, and it refuted the
	// claim that re-aiming this test asserted strictly more than before. It does now.
	if !answered {
		t.Fatal("broker A received nothing at all after another broker took the session over: a " +
			"pre-CONNACK refusal must tell a v5 client why, and an automatic client cannot " +
			"distinguish silence from a network fault")
	}
	if pt&mqttPacketMask != mqttPacketConnectAck {
		t.Fatalf("broker A was answered with packet type 0x%x, want a CONNACK", pt)
	}
	sp, err := rr.readByte("connack flags")
	if err != nil {
		t.Fatalf("reading A's connack flags: %v", err)
	}
	reason, err := rr.readByte("connack reason")
	if err != nil {
		t.Fatalf("reading A's connack reason: %v", err)
	}
	// NOT ACCEPTED is the invariant: an accepted resume would serve group deliveries under a
	// membership the record no longer carries and ACKNOWLEDGE them, destroying them for the
	// member that legitimately holds the group.
	if reason == mqttConnAckRCConnectionAccepted {
		t.Fatal("broker A was ACCEPTED after another broker took the session over: it would " +
			"serve group deliveries under a membership the record no longer carries, and " +
			"ACKNOWLEDGE them, destroying them for the member that legitimately holds the group")
	}
	// The exact legal projection. 0x8E "Session taken over" is DISCONNECT-only ([3.2.2.2]), so a
	// takeover degrades to 0x83; sending 0x8E here would be a protocol violation.
	if reason != mqttReasonImplementationSpecificError {
		t.Fatalf("A's refusal carried reason %#x, want %#x: the disposal vocabulary is projected "+
			"onto a code a CONNACK may legally hold", reason, mqttReasonImplementationSpecificError)
	}
	if sp&0x1 != 0 {
		t.Fatalf("A's failure CONNACK (reason %#x) carried Session Present = 1, which "+
			"[MQTT-3.2.2-4] forbids", reason)
	}
	// And the connection is then CLOSED: the refusal is a refusal, not a demotion.
	testMQTTExpectDisconnect(t, cr)

	// The group durable survives: B holds the membership, and deleting it on A's foreign
	// verdict would break the legitimate member.
	if ci, lerr := sessA.jsa.lookupConsumer(mqttStreamName, durable); lerr != nil || ci == nil {
		t.Fatalf("the group durable is gone after A's foreign verdict, but B still holds "+
			"the membership: %v", lerr)
	}

	// And B still receives the group's deliveries. This is the positive oracle: the whole
	// point of refusing A is that the messages reach the member that owns the membership.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   "sharedown-pub",
		cleanStart: true,
	}, b1.opts.MQTT.Host, b1.opts.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, topic, []byte("for-B"))

	// Read B's delivery without the 3.1.1 exact-payload helper: a v5 PUBLISH carries a
	// property length the helper does not account for, so it reports the correct delivery
	// as a payload mismatch. What matters is that a PUBLISH for this topic carrying this
	// payload reaches B at all.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(rb, 2*time.Second)
		if !ready {
			continue
		}
		start := rb.pos
		body := string(rb.buf[start : start+pl])
		rb.pos = start + pl
		if pb&mqttPacketMask != mqttPacketPub {
			continue
		}
		if strings.Contains(body, topic) && strings.Contains(body, "for-B") {
			return
		}
	}
	t.Fatal("the group publish never reached the member that legitimately holds the " +
		"membership: refusing broker A is only correct if B still gets the message")
}
