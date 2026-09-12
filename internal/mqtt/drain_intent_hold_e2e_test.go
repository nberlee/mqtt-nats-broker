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
	"net"
	"strings"
	"testing"
	"time"
)

// Sentinel errors for the checkFor loops below, so a poll that never converges names what
// it was waiting for rather than printing an inline string.
var (
	errDrainTestIntentNotRecorded = errors.New("the past-PUBREC intent is not recorded yet")
	errDrainTestStillDraining     = errors.New("the drain has not retired yet")
)

// TestMQTTReSubscribeOverAnUnfinishedDrainedIntentIsRefused is F-094 end to end, and it is
// the test that has to fail on the DAMAGE rather than on the refusal.
//
// The window it occupies is real and is the only route to a drained past-PUBREC intent:
// the PUBREC must arrive AFTER the UNSUBSCRIBE, because mqttProcessPublishReceived untracks
// the PUBLISH entry at PUBREC and stageDrain builds its claim set from cpending — so a
// PUBREC first leaves the drain nothing to claim. The intent then lasts until the stored
// PUBREL is delivered back to this broker, which is what the delivery delay holds open.
//
// THE ASSERTION THAT DISCRIMINATES is not the SUBACK. A broker that refuses every
// re-SUBSCRIBE would satisfy that, so this test also asserts that the payload of the
// past-PUBREC exchange is never sent again — which is what actually goes wrong when
// adoption is allowed: the drain and its whole seqs map are dropped, the delivery callback
// no longer finds the durable in drainByDur, and the unacknowledged source is re-offered at
// AckWait on a FRESH packet identifier.
//
// AND IT CARRIES A POSITIVE DELIVERY CONTROL. "No PUBLISH arrived" is satisfied by a dead
// connection, a stalled transmission gate or an exhausted Receive Maximum just as well as
// by the fix, so the same connection must be shown delivering something else in the same
// window. Without that the negative assertion proves nothing.
func TestMQTTReSubscribeOverAnUnfinishedDrainedIntentIsRefused(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "heldintent/topic"
	const control = "heldintent/control"
	conn, sess, pi := testDrainSetup(t, s, o, "heldintent", topic, 2)
	defer conn.c.Close()

	// The control filter is subscribed BEFORE the drain exists, so it is an ordinary live
	// consumer throughout and its delivery says nothing about the drained one.
	testMQTTSub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: control, qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// UNSUBSCRIBE first: the delivery is on the wire and unacknowledged, so it drains.
	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %d", n)
	}
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}

	// Hold the PUBREL delivery, which is what finalizes the drain. Without this the
	// callback always wins the race and the intent is gone before a re-SUBSCRIBE can be
	// sent, so the window this test is about would be unreachable.
	release := make(chan struct{})
	restoreDelay := setMQTTTestPubRelDeliveryDelay(func() { <-release })
	defer restoreDelay()

	// Now past PUBREC: the intent is persisted and the PUBREL is stored, but its delivery
	// back to this broker is stalled, so the source claim is still held.
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if !testDrainIntentRecorded(sess) {
			return errDrainTestIntentNotRecorded
		}
		return nil
	})

	// The refusal. A held filter is answered with a failure code for that filter.
	testMQTTSub(t, 3, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 2}},
		[]byte{mqttSubAckFailure})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// The drain, its durable and its intent are all exactly where they were.
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("a refused re-SUBSCRIBE must leave the drain in place, got %d", n)
	}
	if !testDrainIntentRecorded(sess) {
		t.Fatal("a refused re-SUBSCRIBE must leave the past-PUBREC intent in place")
	}
	if n := testConsumerCount(sess); n != 1 {
		t.Fatalf("only the control filter's consumer may be live, got %d: a refused filter "+
			"must not provision a second durable alongside its drain (I7)", n)
	}

	// Release the PUBREL immediately, and deliberately so: the stall only had to outlive
	// the re-SUBSCRIBE above. Holding it past AckWait would make JetStream re-offer the
	// PUBREL record and turn this into a duplicate-PUBREL test, which is a different
	// subject. The drain finalizes on PUBREL DELIVERY — before the client's PUBCOMP — so
	// it retires here.
	close(release)
	testMQTTReadPIPacket(mqttPacketPubRel, t, conn.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
	restoreDelay()

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return errDrainTestStillDraining
		}
		return nil
	})

	// THE DAMAGE ASSERTION, paired with its control, and placed AFTER the exchange has
	// completed because that is where the two builds diverge. With the fix the drain was
	// HELD across the re-SUBSCRIBE, so its intent was never erased and this schedule
	// produces no second copy. With adoption allowed, the re-SUBSCRIBE above turned that
	// durable into a live consumer whose source was never acknowledged, and it is re-offered
	// at AckWait as a fresh PUBLISH of the payload this client already answered.
	//
	// SCOPE: evidence about the schedule this test exercises, NOT a claim that retirement
	// excludes every re-offer. Retirement's own exclusion — the delivery barrier and the
	// confirmed disposal — is pinned by drain_disposal_test.go, and what it still does not
	// exclude (an indeterminate delete, and a fork sibling rebuilding the drain) is stated
	// in docs/CONFORMANCE.md.
	//
	// Publishing on the control filter proves the connection is alive and delivering in
	// this very window, so "nothing else arrived" is a fact about the drained source and
	// not about a dead socket, a stalled gate or an exhausted Receive Maximum.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "heldintentpub2", cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, control, 1, []byte("control"))
	cpi := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, control, mqttPubQos1, []byte("control"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, cpi)

	// Past AckWait, so any re-offer of the drained source would have been transmitted.
	testMQTTExpectNothingFor(t, conn.r, 3*time.Second)

	// And the hold is RELEASED: the same filter now subscribes normally. This is the half
	// that keeps the refusal from being a permanent availability loss.
	testMQTTSub(t, 4, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 2}}, []byte{2})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// Counted from JETSTREAM, not from session state. testConsumerCount returns
	// len(sess.cons), which an orphaned durable passes trivially — and an orphaned durable
	// on this filter is precisely what a wrong retirement would leave. The question is how
	// many consumers actually exist for the filter's subject.
	if n := testStreamConsumersForFilter(t, sess, topic); n != 1 {
		t.Fatalf("expected exactly ONE JetStream consumer on %q after the drain retired and "+
			"the filter was re-subscribed, got %d: a second one is the retired drain's "+
			"durable left behind beside the fresh consumer", topic, n)
	}

	// A fresh publication arrives, and it carries the "after" payload rather than a second
	// copy of the drained one — which is the damage assertion restated on the live path.
	testMQTTPublish(t, pc, pr, 2, false, false, topic, 2, []byte("after"))
	npi := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, topic, mqttPubQoS2, []byte("after"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, npi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, conn.r, npi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, conn.c, npi)
}

// TestMQTTWildcardReSubscribeRefusedByACompanionIntent drives the per-FILTER preflight in
// processSubs, which the unit tests reach only by looping over the predicate by hand.
//
// It also has to discriminate the mutation that removes that preflight, and the SUBACK
// alone cannot: with the outer check gone the per-sid refusal in processJSConsumer still
// fails the filter. What separates them is the MAIN drain — a per-sid walk adopts it before
// reaching the companion, and the per-filter failure cleanup does not put an adopted drain
// back, so the next save drops its obligations and its durable identity together.
func TestMQTTWildcardReSubscribeRefusedByACompanionIntent(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A multi-level wildcard owns a main sid AND a level-up companion, and each gets its
	// own consumer and its own drain.
	const filter = "wild/#"
	const deep = "wild/a/b"
	const levelUp = "wild"

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "wildheld"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "wildheldpub", cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// One delivery on each sid, both left unacknowledged so both drain.
	testMQTTPublish(t, pc, pr, 2, false, false, deep, 1, []byte("deep"))
	deepPI := testMQTTCheckPubMsgNoAck(t, c, r, deep, mqttPubQoS2, []byte("deep"))
	testMQTTPublish(t, pc, pr, 2, false, false, levelUp, 2, []byte("levelup"))
	upPI := testMQTTCheckPubMsgNoAck(t, c, r, levelUp, mqttPubQoS2, []byte("levelup"))

	sess := testMQTTResidentSession(t, s, "wildheld")

	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: filter}})
	testMQTTFlush(t, c, nil, r)
	if n := testDrainCount(sess); n != 2 {
		t.Fatalf("a multi-level wildcard must drain BOTH sids, got %d", n)
	}

	release := make(chan struct{})
	restoreDelay := setMQTTTestPubRelDeliveryDelay(func() { <-release })
	defer restoreDelay()

	// Take ONE of them past PUBREC. The other stays a PUBLISH-phase claim, so its sid is
	// not held and would be adopted by a per-sid walk.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, upPI)
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if !testDrainIntentRecorded(sess) {
			return errDrainTestIntentNotRecorded
		}
		return nil
	})

	testMQTTSub(t, 3, c, r, []*mqttFilter{{filter: filter, qos: 2}}, []byte{mqttSubAckFailure})
	testMQTTFlush(t, c, nil, r)

	// The discriminating assertion: BOTH drains survive. Without the per-filter preflight
	// the unheld one is adopted before the held one refuses, and nothing puts it back.
	if n := testDrainCount(sess); n != 2 {
		t.Fatalf("the whole FILTER must be refused with nothing adopted, got %d drain(s): a "+
			"per-sid refusal adopts the unheld sid first and the failure cleanup does not "+
			"restore it, so the next save drops its obligations and its durable identity", n)
	}
	if n := testConsumerCount(sess); n != 0 {
		t.Fatalf("a refused filter must install no consumer, got %d", n)
	}

	// Let both exchanges finish so the test leaves nothing behind.
	close(release)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, upPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, upPI)
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, deepPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, deepPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, deepPI)
	testMQTTFlush(t, c, nil, r)
}

// TestMQTTQoS0ReSubscribeOverAHeldDrainIsUnchanged keeps the negative direction honest at
// the protocol level: a QoS 0 re-SUBSCRIBE never adopts, so it has nothing to erase and
// must not be refused.
func TestMQTTQoS0ReSubscribeOverAHeldDrainIsUnchanged(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "heldqos0/topic"
	conn, sess, pi := testDrainSetup(t, s, o, "heldqos0", topic, 2)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	release := make(chan struct{})
	restoreDelay := setMQTTTestPubRelDeliveryDelay(func() { <-release })
	defer restoreDelay()

	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if !testDrainIntentRecorded(sess) {
			return errDrainTestIntentNotRecorded
		}
		return nil
	})

	// QoS 0 is GRANTED over a held drain: it provisions a raw subscription and leaves the
	// drain draining independently.
	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("a QoS 0 re-SUBSCRIBE must leave the drain alone, got %d", n)
	}
	if !testDrainIntentRecorded(sess) {
		t.Fatal("a QoS 0 re-SUBSCRIBE must not disturb the past-PUBREC intent")
	}

	close(release)
	testMQTTReadPIPacket(mqttPacketPubRel, t, conn.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
}

// TestMQTTAdoptedDrainMustNotRedeliverAPastPubRecPayload is the DAMAGE test, and it exists
// separately from the refusal test for a reason the mutation run made concrete.
//
// The refusal test asserts `SUBACK 0x80` before it can reach any damage assertion, so under
// a mutation that removes the hold it fails at the SUBACK and the damage is never exercised.
// A test that only ever fails on the refusal proves nothing a broker refusing everything
// would not also satisfy (feedback_mutation_checks_must_discriminate).
//
// So this one deliberately does NOT assert the SUBACK's value. It subscribes, takes whatever
// answer the broker gives, and then asserts the thing that actually matters: the payload of
// an exchange this client has already answered is never sent again ON THIS SCHEDULE. With
// the hold, the re-SUBSCRIBE is refused so the drain's intent is never erased. Without it,
// the re-SUBSCRIBE adopts the drain, its durable becomes an ordinary live consumer whose
// source was never acknowledged, and AckWait re-offers that source as a fresh PUBLISH on a
// new packet identifier.
//
// SCOPE: the negative assertion is evidence for the exercised schedule only. It is NOT a
// claim that retirement disposes of the consumer or excludes every re-offer — retirement
// keeps HEAD's semantics there, which is a stated residual and its own roadmap item.
func TestMQTTAdoptedDrainMustNotRedeliverAPastPubRecPayload(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "damage/topic"
	const control = "damage/control"
	conn, sess, pi := testDrainSetup(t, s, o, "damage", topic, 2)
	defer conn.c.Close()

	testMQTTSub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: control, qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}

	release := make(chan struct{})
	restoreDelay := setMQTTTestPubRelDeliveryDelay(func() { <-release })
	defer restoreDelay()

	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if !testDrainIntentRecorded(sess) {
			return errDrainTestIntentNotRecorded
		}
		return nil
	})

	// The re-SUBSCRIBE, with its answer deliberately UNASSERTED. Both `0x80` and a granted
	// QoS 2 let this test continue to the assertion that discriminates.
	testMQTTSendSubPacketNoCheck(t, 3, conn.c, []*mqttFilter{{filter: topic, qos: 2}})
	testMQTTReadSubAckAnyCode(t, conn.r, 1)

	// Complete the exchange either way: with the hold the drain finalizes and retires; with
	// adoption there is no drain and the PUBREL forwards on the ordinary path.
	close(release)
	testMQTTReadPIPacket(mqttPacketPubRel, t, conn.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
	restoreDelay()

	// The positive delivery control: the connection is alive and delivering right now, so
	// the absence below is a fact about the drained source and not about the socket.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "damagepub2", cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, control, 1, []byte("control"))
	cpi := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, control, mqttPubQos1, []byte("control"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, cpi)

	// THE ASSERTION. Well past AckWait, so a re-offer of the drained source would have been
	// transmitted by now.
	testMQTTExpectNothingFor(t, conn.r, 3*time.Second)
}

// testMQTTSendSubPacketNoCheck writes a SUBSCRIBE without asserting anything about the
// SUBACK, so a caller can assert on what the broker DOES rather than on what it answers.
func testMQTTSendSubPacketNoCheck(t testing.TB, pi uint16, c net.Conn, filters []*mqttFilter) {
	t.Helper()
	w := newMQTTWriter(0)
	pkLen := 2
	for _, f := range filters {
		pkLen += 2 + len(f.filter) + 1
	}
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for _, f := range filters {
		w.WriteBytes([]byte(f.filter))
		w.WriteByte(f.qos)
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("error writing SUBSCRIBE: %v", err)
	}
}

// testMQTTReadSubAckAnyCode consumes a SUBACK and accepts ANY return code, which is what
// lets the caller reach an assertion the return code would otherwise short-circuit.
func testMQTTReadSubAckAnyCode(t testing.TB, r *mqttReader, filters int) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketSubAck {
		t.Fatalf("expected SUBACK, got packet type %x", pt)
	}
	if _, err := r.readUint16("packet identifier"); err != nil {
		t.Fatalf("reading the SUBACK identifier: %v", err)
	}
	for i := 0; i < filters; i++ {
		if _, err := r.readByte("return code"); err != nil {
			t.Fatalf("reading SUBACK return code %d: %v", i, err)
		}
	}
	_ = pl
}

// testStreamConsumersForFilter counts the $MQTT_msgs consumers whose filter subject is the
// one this topic maps to, asked of JETSTREAM rather than of sess.cons.
//
// The distinction is the point: len(sess.cons) counts what the session believes it has, so
// a durable left behind by a wrong teardown — which is exactly the residue a retirement bug
// produces — is invisible to it.
func testStreamConsumersForFilter(t testing.TB, sess *mqttSession, topic string) int {
	t.Helper()
	subj, err := mqttTopicToNATSPubSubject([]byte(topic))
	if err != nil {
		t.Fatalf("converting topic %q: %v", topic, err)
	}
	want := mqttStreamSubjectPrefix + string(subj)
	cons, err := sess.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	n := 0
	for _, ci := range cons {
		if ci != nil && ci.Config.FilterSubject == want {
			n++
		}
	}
	return n
}

// TestMQTTRestoredIntentWhoseRecreateFailedHoldsItsFilter is the RECOVERY schedule, and it
// is a different route from the live-connection tests above.
//
// Those hold the window open with a delivery delay on a PUBREL that was stored successfully.
// This one is the case the intent exists for: the store never landed, so the persisted
// intent is the ONLY evidence the exchange is past PUBREC, and recovery must recreate the
// PUBREL from it. While that recreate keeps failing the drain's durable stays in the resume
// barrier's skip set, source interest is withheld, and the intent is unfinished across
// reconnects — so the filter must stay held rather than being adoptable by a client that
// simply re-subscribes.
//
// It also pins the release: once the recreate succeeds on a LATER resume, the exchange
// completes, the drain retires and the same filter subscribes normally.
func TestMQTTRestoredIntentWhoseRecreateFailedHoldsItsFilter(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "recreatefail/topic"
	conn, sess, pi := testDrainSetup(t, s, o, "recreatefail", topic, 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}

	// Fail ONLY the PUBREL store. The intent is written first, so it lands; the record does
	// not. The store failure closes the connection, which is what testDrainWaitConnClosed
	// synchronises on -- the intent flag is set inside the record transaction, so observing
	// it directly would race the save.
	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			return errors.New("test: PUBREL store refused")
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	testDrainWaitConnClosed(t, conn.r)
	if !testDrainIntentRecorded(sess) {
		t.Fatal("the intent must have been persisted before the failing store")
	}
	conn.c.Close()

	// Reconnect with the hook STILL installed, so recovery's recreate fails too and the
	// intent is still unfinished. Both the initial store and the recreate go through
	// storeMsg, so one hook covers both.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "recreatefail", cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	sess2 := testMQTTResidentSession(t, s, "recreatefail")
	if !testDrainIntentRecorded(sess2) {
		t.Fatal("the restored session must still carry the unfinished intent")
	}

	// THE REFUSAL, on a session whose drain came back from the RECORD rather than from a
	// live UNSUBSCRIBE in this process.
	testMQTTSub(t, 2, c2, r2, []*mqttFilter{{filter: topic, qos: 2}}, []byte{mqttSubAckFailure})
	testMQTTFlush(t, c2, nil, r2)
	if n := testDrainCount(sess2); n != 1 {
		t.Fatalf("a refused re-SUBSCRIBE must leave the restored drain in place, got %d", n)
	}
	if !testDrainIntentRecorded(sess2) {
		t.Fatal("a refused re-SUBSCRIBE must leave the restored intent in place")
	}

	// Releasing the hook mid-connection reruns no BARRIER: this reconnect's resume work is
	// already finished, and the drain's recreation of the PUBREL needs its own resume. It
	// does NOT follow that no PUBREL can appear before then — L1's owed materialization,
	// enrolled when the intent was recorded, rebuilds the record from the slot once stores
	// work again (F-098), and admission may transmit it on THIS connection.
	//
	// Nothing below depends on which path produces it. The client does not answer here, so
	// an unacknowledged PUBREL is re-sent on the next resume either way, and the third
	// connection's PUBREL-then-PUBCOMP assertions hold whether the record was materialized
	// now or recreated by that resume.
	restore()
	testMQTTDisconnect(t, c2, nil)
	c2.Close()

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: "recreatefail", cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)

	// Now the recreate succeeds: the PUBREL arrives under the ORIGINAL identifier, because
	// that is the exchange the client is holding open.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r3, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c3, pi)
	testMQTTFlush(t, c3, nil, r3)

	sess3 := testMQTTResidentSession(t, s, "recreatefail")
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess3); n != 0 {
			return errDrainTestStillDraining
		}
		return nil
	})

	// And the hold is released: the filter subscribes normally again.
	testMQTTSub(t, 3, c3, r3, []*mqttFilter{{filter: topic, qos: 2}}, []byte{2})
	testMQTTFlush(t, c3, nil, r3)

	// Counted from JETSTREAM: exactly one consumer on the filter, so the retired drain's
	// durable was not left behind beside the fresh one.
	if n := testStreamConsumersForFilter(t, sess3, topic); n != 1 {
		t.Fatalf("expected exactly ONE JetStream consumer on %q after recovery, got %d", topic, n)
	}

	// THE POSITIVE DELIVERY CONTROL, and without it the silence below proves nothing: a
	// connection that answers PUBREL and SUBACK but never restores usable source delivery
	// would pass an ending that only asserts quiet. A fresh publication must actually
	// arrive, and it must be the NEW payload.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "recreatefailpub2", cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 2, false, false, topic, 9, []byte("after-recovery"))
	npi := testMQTTCheckPubMsgNoAck(t, c3, r3, topic, mqttPubQoS2, []byte("after-recovery"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c3, npi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r3, npi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c3, npi)

	// Only NOW is silence meaningful: delivery demonstrably works on this filter, so the
	// absence of the ORIGINAL payload is a fact about the past-PUBREC exchange.
	testMQTTExpectNothingFor(t, r3, 3*time.Second)
}
