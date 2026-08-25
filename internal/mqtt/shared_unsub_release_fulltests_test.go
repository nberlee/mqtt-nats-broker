//go:build mqttport_fulltests

package mqtt

import (
	"strings"
	"testing"
	"time"
)

// testSessionConsumerNames lists the durables a resident session currently holds in cons.
func testSessionConsumerNames(sess *mqttSession) map[string]struct{} {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	out := make(map[string]struct{}, len(sess.cons))
	for _, cfg := range sess.cons {
		if cfg != nil {
			out[cfg.Durable] = struct{}{}
		}
	}
	return out
}

// TestUnsubTearsDownAPlainDollarShareFilter is T12, and it drives a REAL packet pair end to
// end because the thing it guards is the DISPATCH, not a helper.
//
// "$share.foo" is a plain topic filter, not a shared subscription: the shared form requires
// the literal prefix "$share/" in MQTT syntax, and a 3.1.1 client has no shared subscriptions
// at all. But mqttToNATSSubjectConversion encodes a literal '.' as "//", so its sid is
// "$share//foo" — which satisfies HasPrefix(sid, "$share/"). A lexical guard on the plain
// teardown path therefore strands an ordinary QoS 1 consumer, un-purged and undeleted, on a
// filter that has nothing to do with shared subscriptions.
//
// No pending deliveries are seeded: with started deliveries an UNSUBSCRIBE stages a DRAIN
// instead of tearing the consumer down (stageDrain), so "the consumer is gone" would be the
// wrong assertion rather than a weaker one.
func TestUnsubTearsDownAPlainDollarShareFilter(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "plaindollar", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share.foo", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, "plaindollar")
	before := testSessionConsumerNames(sess)
	if len(before) == 0 {
		t.Fatalf("the fixture never created a plain consumer for $share.foo, so it proves nothing")
	}
	// The premise this test exists for: the sid really does carry the "$share/" prefix.
	sess.mu.Lock()
	var sids []string
	for sid := range sess.cons {
		sids = append(sids, sid)
	}
	sess.mu.Unlock()
	prefixed := false
	for _, sid := range sids {
		if strings.HasPrefix(sid, string(mqttSharedSubPrefix)) {
			prefixed = true
		}
	}
	if !prefixed {
		t.Fatalf("the fixture's premise failed: no sid in %v carries the %q prefix", sids, mqttSharedSubPrefix)
	}

	if codes := testMQTTUnsubV5(t, c, r, 2, []string{"$share.foo"}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("UNSUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	if after := testSessionConsumerNames(sess); len(after) != 0 {
		t.Fatalf("the plain $share.foo consumer(s) %v survived the UNSUBSCRIBE", after)
	}
}

// TestUnsubSharedDispatchKeepsAForeignHeldConsumer drives a REAL shared UNSUBSCRIBE packet
// through mqttProcessUnsubs, which is the only route that exercises commitUnsub's `f.shared`
// dispatch and its sharedKeys construction.
//
// Every other test in this area calls releaseUnsubSharedTargets directly, so none of them
// would notice the caller ceasing to route shared filters into it — and a shared config
// reaching the plain teardown arm issues an UNCONDITIONAL CONSUMER.DELETE, destroying the
// interest-retained backlog of every other member of the group. A foreign holder is planted
// so the correct answer is unambiguous: this session's own state goes, the group consumer
// stays.
func TestUnsubSharedDispatchKeepsAForeignHeldConsumer(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "shareddispatch", cleanStart: true},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/g/dispatch", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, "shareddispatch")
	target := mqttSharedGroupNamesForKey("$share/g/dispatch")[0]

	asm := testMQTTGetAccountSessionManager(t, s, "shareddispatch")
	// Another member of the same group, so the last-holder scan answers HELD and the only
	// correct outcome is "discharge locally, withhold the delete".
	testPlantSessionRecordJSON(t, asm, "otherdispatch", &mqttPersistedSession{
		ID:         "otherdispatch",
		SharedSubs: map[string]byte{"$share/g/dispatch": 1},
	})

	if codes := testMQTTUnsubV5(t, c, r, 2, []string{"$share/g/dispatch"}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("UNSUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	// This session's own state is discharged whatever the scan said.
	sess.mu.Lock()
	_, charged := sess.sharedCharged[target.durable]
	_, pending := sess.cpending[target.durable]
	stillCons := false
	for _, cfg := range sess.cons {
		if cfg != nil && cfg.Durable == target.durable {
			stillCons = true
		}
	}
	sess.mu.Unlock()
	if charged || pending || stillCons {
		t.Fatalf("the session kept its own shared state after UNSUBSCRIBE (charge=%v pending=%v cons=%v)",
			charged, pending, stillCons)
	}

	// The group consumer is another member's business and must survive. Held over a window:
	// deleteConsumer is fire-and-forget, so one immediate look would pass on timing alone.
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		cons, err := asm.jsa.consumerList(mqttStreamName)
		if err != nil {
			t.Fatalf("listing consumers: %v", err)
		}
		found := false
		for _, ci := range cons {
			if ci != nil && ci.Name == target.durable {
				found = true
			}
		}
		if !found {
			t.Fatalf("the foreign-held group consumer %q was DELETED by the UNSUBSCRIBE", target.durable)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
