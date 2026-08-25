// Unit-level proofs for the restore gate (sess_restore_gate.go).
//
// These are the parts of F-050's mechanism that are decidable without a broker:
// the verdict's three transitions and what each one says to a waiter, the failure
// aggregation, and the invocation-scoped charge refund. The end-to-end properties
// — nothing precedes the CONNACK, the refusal preserves the session, the Will is
// not published — need a real connection and live in
// sess_restore_gate_e2e_test.go under the mqttport_fulltests tag.

package mqtt

import (
	"testing"
	"time"
)

func TestRestoreGateVerdictTransitions(t *testing.T) {
	// A session with no armed verdict is open: this is every path that is not a
	// CONNECT restore, and it must cost a delivery nothing.
	t.Run("unarmed is open", func(t *testing.T) {
		sess := &mqttSession{}
		got := sess.awaitTransmissionGates(nil)
		if got != mqttTransmissionOpen {
			t.Fatalf("unarmed verdict = %v, want open", got)
		}
		if sess.restoreGateBlocksQoS0() {
			t.Fatal("unarmed verdict blocks QoS 0")
		}
	})

	// Armed and not yet resolved: QoS 0 is dropped immediately rather than parked,
	// because it has no redelivery to wait for.
	t.Run("armed blocks QoS 0 without waiting", func(t *testing.T) {
		sess := &mqttSession{}
		sess.armRestoreGate()
		if !sess.restoreGateBlocksQoS0() {
			t.Fatal("armed verdict does not block QoS 0")
		}
	})

	// The success transition. The store must be visible to a waiter released by the
	// close — that ordering is the whole reason opened is written first — so this
	// asserts the value a waiter actually reads, not merely that it woke.
	t.Run("open releases waiters for transmission", func(t *testing.T) {
		sess := &mqttSession{}
		sess.armRestoreGate()
		got := make(chan mqttTransmissionVerdict, 1)
		go func() { got <- sess.awaitTransmissionGates(nil) }()
		// The waiter is either parked or about to park; either way the open below
		// is what it must observe.
		sess.openRestoreGate()
		select {
		case v := <-got:
			if v != mqttTransmissionOpen {
				t.Fatalf("verdict after open = %v, want open", v)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter never woke after open")
		}
		if sess.restoreGateBlocksQoS0() {
			t.Fatal("opened verdict still blocks QoS 0")
		}
	})

	// The refusal transition, and the property that took two review rounds to get
	// right: a waiter is WOKEN — so it can record itself as never transmitted
	// rather than waiting out its whole bound — and is still not allowed out.
	t.Run("abandon wakes waiters without allowing transmission", func(t *testing.T) {
		sess := &mqttSession{}
		sess.armRestoreGate()
		got := make(chan mqttTransmissionVerdict, 1)
		go func() { got <- sess.awaitTransmissionGates(nil) }()
		sess.abandonRestoreGate()
		select {
		case v := <-got:
			if v != mqttTransmissionWithheld {
				t.Fatalf("verdict after abandon = %v, want withheld", v)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter never woke after abandon")
		}
	})

	// A delivery arriving AFTER the abandon and before the client detaches must be
	// blocked too, which is why the abandon does not clear the pointer. Clearing it
	// would read as "no restore in progress" and transmit on a connection that was
	// never acknowledged.
	t.Run("abandon keeps blocking a late delivery", func(t *testing.T) {
		sess := &mqttSession{}
		sess.armRestoreGate()
		sess.abandonRestoreGate()
		got := sess.awaitTransmissionGates(nil)
		if got != mqttTransmissionWithheld {
			t.Fatalf("late verdict = %v, want withheld", got)
		}
		if !sess.restoreGateBlocksQoS0() {
			t.Fatal("abandoned verdict does not block a late QoS 0 delivery")
		}
	})

	// Arming is unconditional on every accepted CONNECT, so a session object that a
	// refusal left holding an abandoned verdict is usable again on the next one.
	// Without that, a reused object's later live subscriptions would sit behind a
	// dead verdict forever.
	t.Run("re-arming replaces an abandoned verdict", func(t *testing.T) {
		sess := &mqttSession{}
		sess.armRestoreGate()
		sess.abandonRestoreGate()
		sess.armRestoreGate()
		sess.openRestoreGate()
		got := sess.awaitTransmissionGates(nil)
		if got != mqttTransmissionOpen {
			t.Fatalf("verdict after re-arm and open = %v, want open", got)
		}
		if sess.restoreGateBlocksQoS0() {
			t.Fatal("re-armed and opened verdict still blocks QoS 0")
		}
	})
}

// Both seams must restore the PREVIOUS pointer, not nil. A setter that clears
// instead of restoring silently disarms an outer test's hook, and the failure lands
// on that outer test rather than on the one that broke it.
func TestRestoreGateSeamsRestoreTheirPredecessor(t *testing.T) {
	var outer, inner []string
	restoreOuter := setMQTTTestRestoreGateBlocked(func(kind string) { outer = append(outer, kind) })
	restoreInner := setMQTTTestRestoreGateBlocked(func(kind string) { inner = append(inner, kind) })
	mqttTestRestoreGateBlockedHook("qos0")
	restoreInner()
	mqttTestRestoreGateBlockedHook("qos12")
	restoreOuter()
	mqttTestRestoreGateBlockedHook("pubrel")
	if len(inner) != 1 || inner[0] != "qos0" {
		t.Fatalf("inner blocked-hook saw %v, want [qos0]", inner)
	}
	if len(outer) != 1 || outer[0] != "qos12" {
		t.Fatalf("outer blocked-hook saw %v, want [qos12] (the inner restore must not clear it)", outer)
	}

	var at []string
	restorePause := setMQTTTestRestorePause(func(a string) { at = append(at, a) })
	mqttTestRestorePauseHook("before-connack")
	restorePause()
	mqttTestRestorePauseHook("before-open")
	if len(at) != 1 || at[0] != "before-connack" {
		t.Fatalf("pause hook saw %v, want [before-connack]", at)
	}
}

func TestFirstUnrestoredFilter(t *testing.T) {
	ok := func(topic string) *mqttFilter { return &mqttFilter{filter: topic, qos: 1} }
	bad := func(topic string) *mqttFilter {
		return &mqttFilter{filter: topic, qos: mqttSubAckFailure, reasonStr: "boom"}
	}

	if f := mqttFirstUnrestoredFilter(nil); f != nil {
		t.Fatalf("empty filter list reported %q unrestored", f.filter)
	}
	if f := mqttFirstUnrestoredFilter([]*mqttFilter{ok("a"), ok("b")}); f != nil {
		t.Fatalf("all-restored list reported %q unrestored", f.filter)
	}
	// The FIRST failure is what the CONNECT reports, so the refusal names a filter
	// the client can act on rather than an arbitrary one.
	if f := mqttFirstUnrestoredFilter([]*mqttFilter{ok("a"), bad("b"), bad("c")}); f == nil || f.filter != "b" {
		t.Fatalf("first unrestored = %v, want b", f)
	}
	// A v5 error reason above the 3.1.1 failure byte counts too: processSubs writes
	// whichever the path produced into the same field.
	v5 := &mqttFilter{filter: "d", qos: mqttReasonImplementationSpecificError}
	if f := mqttFirstUnrestoredFilter([]*mqttFilter{ok("a"), v5}); f == nil || f.filter != "d" {
		t.Fatalf("v5 reason code not read as unrestored: %v", f)
	}
	// A nil entry is skipped rather than panicking: the restore path builds the
	// slice itself, but this function is the CONNECT's only reader of it and a
	// crash here would take the connection down for a bookkeeping slip.
	if f := mqttFirstUnrestoredFilter([]*mqttFilter{nil, ok("a")}); f != nil {
		t.Fatalf("nil entry reported unrestored: %v", f)
	}
}

// The refund has to be identified by the AMOUNT THIS INVOCATION CHARGED, never by
// the durable. Refunding by durable erases a charge an earlier connection made —
// the resident session keeps both the durable and its cons entry, so the next
// resume does not recharge, and tmaxack stays permanently short.
func TestRefundSharedChargesIsInvocationScoped(t *testing.T) {
	newSess := func() *mqttSession {
		return &mqttSession{
			tmaxack:       1000,
			sharedCharged: map[string]int{"dur-earlier": 1000},
		}
	}

	t.Run("this invocation's charge is refunded", func(t *testing.T) {
		sess := newSess()
		sess.sharedCharged["dur-now"] = 64
		sess.tmaxack += 64
		charges := appendSharedCharge(nil, "dur-now", 64)
		sess.refundSharedCharges(false, charges)
		if got := sess.tmaxack; got != 1000 {
			t.Fatalf("tmaxack = %d, want 1000", got)
		}
		if _, ok := sess.sharedCharged["dur-now"]; ok {
			t.Fatal("this invocation's charge was not released")
		}
		if got := sess.sharedCharged["dur-earlier"]; got != 1000 {
			t.Fatalf("earlier charge = %d, want 1000", got)
		}
	})

	t.Run("a reused entry charges nothing and refunds nothing", func(t *testing.T) {
		sess := newSess()
		// processSharedJSConsumer reports 0 when it reused a resident cons entry, and
		// appendSharedCharge drops it — so the earlier connection's charge is never a
		// refund candidate in the first place.
		charges := appendSharedCharge(nil, "dur-earlier", 0)
		if len(charges) != 0 {
			t.Fatalf("a zero charge was recorded: %v", charges)
		}
		sess.refundSharedCharges(false, charges)
		if got := sess.tmaxack; got != 1000 {
			t.Fatalf("tmaxack = %d, want 1000", got)
		}
		if got := sess.sharedCharged["dur-earlier"]; got != 1000 {
			t.Fatalf("earlier charge = %d, want 1000", got)
		}
	})

	t.Run("an amount that no longer matches is left alone", func(t *testing.T) {
		// Something else re-charged the durable between the join and the failure, so
		// this filter's amount is no longer what is recorded. Refunding the recorded
		// value would take away somebody else's charge; refunding ours would leave
		// the map disagreeing with tmaxack. Leave it: a stale over-count is bounded
		// by the session's life, an under-count is not recoverable at all.
		sess := newSess()
		sess.sharedCharged["dur-now"] = 128
		sess.tmaxack += 128
		sess.refundSharedCharges(false, appendSharedCharge(nil, "dur-now", 64))
		if got := sess.tmaxack; got != 1128 {
			t.Fatalf("tmaxack = %d, want 1128 (unchanged)", got)
		}
		if got := sess.sharedCharged["dur-now"]; got != 128 {
			t.Fatalf("dur-now = %d, want 128 (unchanged)", got)
		}
	})

	t.Run("a SUBSCRIBE refunds nothing here", func(t *testing.T) {
		// fromSubProto disposes of the consumer through releaseSharedConsumer, which
		// refunds as part of the release. Refunding here as well would double it.
		sess := newSess()
		sess.sharedCharged["dur-now"] = 64
		sess.tmaxack += 64
		sess.refundSharedCharges(true, appendSharedCharge(nil, "dur-now", 64))
		if got := sess.tmaxack; got != 1064 {
			t.Fatalf("tmaxack = %d, want 1064 (unchanged)", got)
		}
	})
}
