// Copyright 2026 The mqtt-nats-broker Authors
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

//go:build mqtt_fulltests

package mqtt

import (
	"strconv"
	"testing"
	"time"
)

// End-to-end shared-group lifecycle tests (F-105), driven through real MQTT clients so that
// the whole join → serve → leave → retire protocol — J0/J1/J2/J3/J4 and the release ladder —
// runs, and a mutation anywhere in processSubs, acquireSharedFilter or the release paths kills
// the test. The oracle is the group record and the acquisition ledger, read back through the
// production decoders, together with the consumer's existence.

// waitHolders polls the group record until it serves wantInc with wantHolders, or fails.
// nil wantLive asserts the group serves nobody (live == null).
func waitHolders(t *testing.T, b *testSharedBroker, what string, wantInc, wantHolders uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		g := b.loadGroup(t)
		if g.status == mqttOutboundValid && g.rec.Live != nil &&
			g.rec.Live.Inc == wantInc && g.rec.Live.Holders == wantHolders {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: group record never reached live{inc:%d,holders:%d}: %+v (status %v)",
				what, wantInc, wantHolders, groupString(b.loadGroup(t)), b.loadGroup(t).status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func groupString(g mqttSharedGroupLoad) string {
	if g.rec == nil {
		return "<nil>"
	}
	return "live=" + itoaLive(g.rec.Live) + " retiring=" + itoaU64s(g.rec.Retiring)
}

func itoaLive(l *mqttSharedLive) string {
	if l == nil {
		return "null"
	}
	return "{inc:" + itoaU(l.Inc) + ",holders:" + itoaU(l.Holders) + "}"
}
func itoaU(u uint64) string { return strconv.FormatUint(u, 10) }
func itoaU64s(v []uint64) string {
	out := "["
	for i, k := range v {
		if i > 0 {
			out += " "
		}
		out += itoaU(k)
	}
	return out + "]"
}

// waitRetired polls until the group serves nobody and its consumer is deleted.
func waitRetired(t *testing.T, b *testSharedBroker, what, durable string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		g := b.loadGroup(t)
		liveGone := g.status == mqttOutboundValid && g.rec.Live == nil
		_, lerr := b.jsa.lookupConsumer(mqttStreamName, durable)
		consGone := lerr != nil
		if liveGone && consGone && len(g.rec.Retiring) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: group not fully retired: %s, consumer %q present=%v",
				what, groupString(b.loadGroup(t)), durable, lerr == nil)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSharedE2EJoinServeLeaveRetire is the happy path end to end: two members join one group,
// a publish load-balances to one of them, one leaves and the group stays live for the other,
// the last leaves and the incarnation retires with its consumer deleted.
func TestSharedE2EJoinServeLeaveRetire(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "e2ehappy")
	ca, ra := b.member(t, "happyA", 300)
	defer ca.Close()
	b.bind(t, "happyA")
	durable := b.groupDurable(t)

	// One holder after the first join.
	waitHolders(t, b, "after A joins", 1, 1)
	l := b.loadLedger(t, "happyA", testMQTTGetClient(t, b.s, "happyA").mqtt.sess.gen)
	if l.status != mqttOutboundValid || len(l.rec.Acqs) != 1 {
		t.Fatalf("A's ledger should list one acquisition: %+v (status %v)", l.rec, l.status)
	}

	cb, rb := b.member(t, "happyB", 300)
	defer cb.Close()
	waitHolders(t, b, "after B joins", 1, 2)

	// B unsubscribes: the group stays live for A, the consumer survives.
	if codes := testMQTTUnsubV5(t, cb, rb, 2, []string{"$share/" + b.group + "/" + b.topic}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("B UNSUBACK = %v", codes)
	}
	waitHolders(t, b, "after B leaves", 1, 1)
	if _, lerr := b.jsa.lookupConsumer(mqttStreamName, durable); lerr != nil {
		t.Fatalf("the group consumer was deleted while A still holds it: %v", lerr)
	}

	// A unsubscribes: last holder gone, the incarnation retires and its consumer is deleted.
	if codes := testMQTTUnsubV5(t, ca, ra, 3, []string{"$share/" + b.group + "/" + b.topic}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("A UNSUBACK = %v", codes)
	}
	waitRetired(t, b, "after A leaves", durable)
}

// TestSharedE2ELevelUpHoldsTwoAcquisitions is T9: a "foo/#" filter acquires on its own group
// AND on the level-up parent, and a plain "foo" filter beside it shares the parent consumer.
// Unsubscribing the wildcard releases BOTH of its acquisitions; the parent stays live for the
// plain filter, and unsubscribing that retires it.
func TestSharedE2ELevelUpHoldsTwoAcquisitions(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "e2elevelup")
	// The wildcard member. Its topic filter is "<name>/topic/#", whose NATS subject part is
	// "<name>.topic.>" and whose level-up parent is "<name>.topic".
	wildTopic := b.topic + "/#"
	cw, rw := b.memberFilter(t, "wildA", 300, wildTopic)
	defer cw.Close()
	b.bind(t, "wildA")

	subj, err := mqttTopicToNATSPubSubject([]byte(b.topic))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	parentKey := mqttSharedGroupKey(b.group, string(subj))
	_, _, parentDur := mqttSharedNames(b.group, string(subj), 1)

	// The parent group carries the wildcard's level-up acquisition: one holder.
	parentLoad := func() mqttSharedGroupLoad { return b.jsa.loadSharedGroup(b.asm.sessStreamEpoch, parentKey) }
	deadline := time.Now().Add(5 * time.Second)
	for parentLoad().status != mqttOutboundValid || parentLoad().rec.Live == nil || parentLoad().rec.Live.Holders != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("the parent group never reached one holder from the wildcard's level-up acquisition: %+v", groupString(parentLoad()))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A plain member of the parent filter: parent holders 2.
	cp, rp := b.memberFilter(t, "plainA", 300, b.topic)
	defer cp.Close()
	deadline = time.Now().Add(5 * time.Second)
	for parentLoad().rec.Live == nil || parentLoad().rec.Live.Holders != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the parent group never reached two holders: %+v", groupString(parentLoad()))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The wildcard unsubscribes: BOTH its acquisitions release. Its own group retires; the
	// parent goes back to the plain member's one holder, its consumer surviving.
	if codes := testMQTTUnsubV5(t, cw, rw, 2, []string{"$share/" + b.group + "/" + wildTopic}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("wildcard UNSUBACK = %v", codes)
	}
	deadline = time.Now().Add(5 * time.Second)
	for parentLoad().rec.Live == nil || parentLoad().rec.Live.Holders != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("the wildcard's level-up acquisition never released (parent holders != 1): %+v", groupString(parentLoad()))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, lerr := b.jsa.lookupConsumer(mqttStreamName, parentDur); lerr != nil {
		t.Fatalf("the parent consumer was deleted while the plain member still holds it: %v", lerr)
	}

	// The plain member leaves: the parent retires.
	if codes := testMQTTUnsubV5(t, cp, rp, 2, []string{"$share/" + b.group + "/" + b.topic}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("plain UNSUBACK = %v", codes)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		g := parentLoad()
		if g.rec.Live == nil && len(g.rec.Retiring) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the parent group never retired: %+v", groupString(g))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSharedE2EReSubscribeReusesTheAcquisition is T13: re-SUBSCRIBE with a QoS/option change
// does NOT acquire again — the ledger keeps one entry and the group one holder — and the final
// UNSUBSCRIBE retires the group.
func TestSharedE2EReSubscribeReusesTheAcquisition(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "e2ereuse")
	c, r := b.member(t, "reuseA", 300)
	defer c.Close()
	b.bind(t, "reuseA")
	durable := b.groupDurable(t)
	gen := testMQTTGetClient(t, b.s, "reuseA").mqtt.sess.gen
	waitHolders(t, b, "after join", 1, 1)

	acq0 := b.loadLedger(t, "reuseA", gen).rec.Acqs[0].Acq

	// Re-SUBSCRIBE the same filter several times, changing options. None may acquire.
	for i := 0; i < 5; i++ {
		filter := "$share/" + b.group + "/" + b.topic
		if codes := testMQTTSubV5(t, c, r, uint16(10+i), []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
			t.Fatalf("re-SUBSCRIBE %d SUBACK = %v", i, codes)
		}
		testMQTTFlush(t, c, nil, r)
	}
	l := b.loadLedger(t, "reuseA", gen)
	if l.status != mqttOutboundValid || len(l.rec.Acqs) != 1 {
		t.Fatalf("re-SUBSCRIBE acquired anew: ledger lists %d acquisitions, want 1", len(l.rec.Acqs))
	}
	if l.rec.Acqs[0].Acq != acq0 {
		t.Fatalf("re-SUBSCRIBE minted a fresh acquisition nonce; the reference must be reused")
	}
	g := b.loadGroup(t)
	if g.rec.Live == nil || g.rec.Live.Holders != 1 {
		t.Fatalf("re-SUBSCRIBE moved the holder count: %+v", groupString(g))
	}

	// The final UNSUBSCRIBE retires the group.
	if codes := testMQTTUnsubV5(t, c, r, 99, []string{"$share/" + b.group + "/" + b.topic}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("UNSUBACK = %v", codes)
	}
	waitRetired(t, b, "after unsubscribe", durable)
}

// TestSharedE2EOldRecordShapeIsRefused is T7: a session record whose shared_subs carry the
// pre-transfer numeric QoS is a shape this broker no longer writes and is refused at CONNECT.
func TestSharedE2EOldRecordShapeIsRefused(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "e2eoldshape")
	// A member, to create the account manager and the streams.
	c0, _ := b.member(t, "shapeprobe", 300)
	c0.Close()
	b.bind(t, "shapeprobe")

	// Plant a record for another client whose SharedSubs value is a bare number (the old
	// shape), which the membership decoder refuses.
	old := []byte(`{"ID":"oldshapeclient","SharedSubs":{"$share/g/foo":1}}`)
	testPlantSessionRecord(t, b.asm, "oldshapeclient", old)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "oldshapeclient", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c.Close()
	// The refusal is pre-CONNACK: it may arrive as a failure CONNACK or as a connection
	// close (EOF). Either is a refusal; an ACCEPTED CONNACK is the only failure.
	pt, _, ready := testMQTTReadPacketReady(r, 2*time.Second)
	if !ready {
		return // connection closed with no CONNACK — refused
	}
	if pt&mqttPacketMask != mqttPacketConnectAck {
		return // not a CONNACK at all — refused
	}
	if _, err := r.readByte("connack flags"); err != nil {
		return
	}
	reason, err := r.readByte("connack reason")
	if err != nil {
		return
	}
	if reason == mqttConnAckRCConnectionAccepted {
		t.Fatalf("a session record carrying the pre-transfer numeric shared_subs shape was RESUMED; it must be refused")
	}
}

// testPlantSessionRecord stores raw bytes on another session's $MQTT_sess subject, derived from
// a client-ID hash exactly as production does.
func testPlantSessionRecord(t *testing.T, asm *mqttAccountSessionManager, ownerID string, data []byte) {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + asm.domainTk + getHash(ownerID)
	if _, err := asm.jsa.storeMsg(subject, 0, data); err != nil {
		t.Fatalf("planting the session record of %q: %v", ownerID, err)
	}
}
