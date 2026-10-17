// Copyright 2025 The mqtt-nats-broker Authors

package mqtt

import (
	"testing"
	"time"
)

// The Will Delay Interval runs from the CLOSE, not from whenever a teardown path
// gets round to consuming the Will.
//
// What the defect actually was, in observable terms: mqttHandleWill computed the
// deadline from time.Now() at CONSUMPTION, and between the close and that moment sit
// the session gate, a clean-session discard and two JetStream round trips. So a Will
// consumed late fired late — and, the sharper harm, the window in which a reconnect
// can still suppress it was WIDER than the client configured. The suppression
// predicate itself was never wrong: mqttAcceptFactQualifies rejects an acceptance
// dated at or after FireAt, and its own comment states the rule ("a reconnect after
// the delay has elapsed does not [suppress], because the Will was already owed").
// Its INPUT was wrong.
//
// These fixtures pin the anchor at the four places it is written, read, cleared and
// restored, because a value that is only correct at its producer is one restore away
// from being wrong again.

// mqttWillFireAt's contract, stated as a test because three call sites depend on it
// and one of them (the deferred producer) cannot easily be reached end to end.
func TestWillFireAtMeasuresFromTheCloseAndCapsAtExpiry(t *testing.T) {
	will := &mqttWill{props: &mqttProperties{willDelay: 30}}
	closeAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	// Deliberately far from the close, so a helper that used it as the anchor is
	// caught rather than coincidentally right.
	now := closeAt.Add(9 * time.Second)

	t.Run("a nonzero anchor is the origin, not now", func(t *testing.T) {
		got := mqttWillFireAt(will, 3600, closeAt.UnixNano(), now)
		want := closeAt.Add(30 * time.Second).UnixNano()
		if got != want {
			t.Fatalf("FireAt %v, want %v: the delay must run from the close, not from the consumption that followed it",
				time.Unix(0, got), time.Unix(0, want))
		}
	})

	t.Run("a zero anchor means the caller's own action is the close", func(t *testing.T) {
		// The ARMED case: nothing observed the close separately, so this consumption
		// IS it. Zero has exactly one meaning to the helper; an OWED zero is an
		// invariant violation its callers classify before calling.
		got := mqttWillFireAt(will, 3600, 0, now)
		want := now.Add(30 * time.Second).UnixNano()
		if got != want {
			t.Fatalf("FireAt %v, want %v", time.Unix(0, got), time.Unix(0, want))
		}
	})

	t.Run("the Session Expiry Interval caps the delay", func(t *testing.T) {
		// Spec5 [3.1.3.2.2]: publish at the delay or at the session's end, whichever
		// is first. The cap applies to the anchored deadline, not to a fresh now.
		got := mqttWillFireAt(will, 5, closeAt.UnixNano(), now)
		want := closeAt.Add(5 * time.Second).UnixNano()
		if got != want {
			t.Fatalf("FireAt %v, want %v: the expiry must cap the delay", time.Unix(0, got), time.Unix(0, want))
		}
	})

	t.Run("mqttSessionNeverExpire never lowers the delay", func(t *testing.T) {
		got := mqttWillFireAt(will, mqttSessionNeverExpire, closeAt.UnixNano(), now)
		want := closeAt.Add(30 * time.Second).UnixNano()
		if got != want {
			t.Fatalf("FireAt %v, want %v: the max uint32 is not a shorter deadline", time.Unix(0, got), time.Unix(0, want))
		}
	})
}

// The anchor is written exactly when the Will becomes owed, on BOTH arms, and the
// second observation of one close does not overwrite the first.
func TestCloseObservationIsStampedWhenTheWillIsOwed(t *testing.T) {
	newArmed := func(delay uint32, expiry uint32) *client {
		c := &client{}
		c.mqtt = &mqtt{proto: mqttProtoLevel5, cp: &mqttConnectProto{}, cid: "anchor"}
		c.mqtt.cp.will = &mqttWill{topic: []byte("a/w"), props: &mqttProperties{willDelay: delay}}
		c.mqtt.willDisposition = mqttWillArmed
		c.mqtt.sessionExpiry = expiry
		return c
	}

	t.Run("the delayed arm stamps it", func(t *testing.T) {
		c := newArmed(30, 3600)
		before := time.Now().UnixNano()
		c.mqttOweWill(3600)
		after := time.Now().UnixNano()
		if c.mqtt.willDisposition != mqttWillOwedDelayed {
			t.Fatalf("disposition %v, want owed-delayed: this fixture is not on the arm it names", c.mqtt.willDisposition)
		}
		if c.mqtt.closeObservedAt < before || c.mqtt.closeObservedAt > after {
			t.Fatalf("closeObservedAt %d outside [%d, %d]", c.mqtt.closeObservedAt, before, after)
		}
	})

	t.Run("the owed-now arm stamps it too", func(t *testing.T) {
		// Stamped even where nothing reads it today. Stamping only where the value is
		// currently consumed would make its reach something every later reader has to
		// re-derive, which is how a field like this goes stale.
		c := newArmed(0, 3600)
		c.mqttOweWill(3600)
		if c.mqtt.willDisposition != mqttWillOwedNow {
			t.Fatalf("disposition %v, want owed-now", c.mqtt.willDisposition)
		}
		if c.mqtt.closeObservedAt == 0 {
			t.Fatal("the owed-now arm left no close observation")
		}
	})

	t.Run("a second observation of one close does not move the anchor", func(t *testing.T) {
		// mqttOweWill runs twice for a graceful DISCONNECT. The no-op-unless-armed
		// guard is what makes the FIRST observation the one that counts; without it the
		// second call would push the deadline later, which is the defect wearing a
		// different hat.
		c := newArmed(30, 3600)
		c.mqttOweWill(3600)
		first := c.mqtt.closeObservedAt
		time.Sleep(2 * time.Millisecond)
		c.mqttOweWill(3600)
		if c.mqtt.closeObservedAt != first {
			t.Fatalf("closeObservedAt moved from %d to %d on a second observation of the same close",
				first, c.mqtt.closeObservedAt)
		}
	})

	t.Run("taking the Will consumes the anchor with it", func(t *testing.T) {
		// The Will, the disposition it was consumed in, and the close it is measured
		// from are one fact. A caller that could take the Will without the anchor would
		// have to re-derive the deadline's origin.
		c := newArmed(30, 3600)
		c.mqttOweWill(3600)
		stamped := c.mqtt.closeObservedAt
		will, disp, closeAt := c.mqttTakeWillLocked()
		if will == nil || disp != mqttWillOwedDelayed {
			t.Fatalf("take returned will=%v disposition=%v", will != nil, disp)
		}
		if closeAt != stamped {
			t.Fatalf("take returned anchor %d, want the stamped %d", closeAt, stamped)
		}
		if c.mqtt.closeObservedAt != 0 {
			t.Fatalf("the anchor survived consumption as %d: a later Will on this connection would inherit a stale close",
				c.mqtt.closeObservedAt)
		}
	})

	t.Run("discarding the Will clears the anchor", func(t *testing.T) {
		// A discarded Will has no deadline left to anchor, and leaving the value set
		// would hand a stale close to whatever comes next.
		c := newArmed(30, 3600)
		c.mqttOweWill(3600)
		if c.mqtt.closeObservedAt == 0 {
			t.Fatal("fixture: nothing was stamped, so clearing it proves nothing")
		}
		c.mqttDiscardWillLocked()
		if c.mqtt.closeObservedAt != 0 {
			t.Fatalf("closeObservedAt %d survived a discard", c.mqtt.closeObservedAt)
		}
	})
}

// An OWED Will with no close observation is an INVARIANT FAILURE, and the
// disposition is degraded rather than correct.
//
// This is not a "refusal" and it does not preserve suppression entitlement — an
// earlier draft claimed both and neither is true. With a true close at t0, a delay
// D, and a consumption at t0+k, anchoring at the consumption moves the boundary
// LATER, so an acceptance in [t0+D, t0+k) is wrongly allowed to suppress; anchoring
// earlier would fail to suppress one in [t0+k, t0+D). The true boundary cannot be
// reconstructed from a state that never recorded it, so what the code owes is to
// enqueue rather than lose the Will, to say so loudly, and not to claim the
// boundary held.
//
// The zero is classified by the CALLERS, not inside mqttWillFireAt: an armed zero
// and an owed zero are identical to that helper, and conflating them is what would
// silently re-anchor an owed Will instead of diagnosing it.
func TestAnOwedWillWithNoCloseObservationIsAnInvariantFailureNotAReanchor(t *testing.T) {
	will := &mqttWill{props: &mqttProperties{willDelay: 30}}
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

	// What the producers do with a zero anchor on an OWED Will: enqueue it DUE NOW.
	// NOT now + D. The disposition already says the Will is owed, so granting a fresh
	// full delay would hand back a suppression window it has already spent, on top of
	// the one already lost. Marking it due is the smaller error and the only one that
	// cannot suppress a Will the client is owed.
	//
	// The producers compute this directly rather than through mqttWillFireAt, because
	// that helper's zero means "armed, so now is the anchor" and adds the delay — which
	// is right for an armed Will and wrong for this one.
	dueNow := now.UnixNano()
	viaHelper := mqttWillFireAt(will, 3600, 0, now)
	if viaHelper == dueNow {
		t.Fatal("fixture: the helper's armed reading and the owed-zero disposition coincide here, so " +
			"this test cannot tell a producer that confused them from one that did not")
	}
	if viaHelper <= dueNow {
		t.Fatalf("the helper's armed reading %v is not later than due-now %v", time.Unix(0, viaHelper), time.Unix(0, dueNow))
	}

	// The boundary error goes BOTH ways, and which way depends on how much of the
	// delay had already elapsed. Asserted as two concrete cases rather than described,
	// because an earlier draft of this test asserted only "later" and was simply wrong
	// for the common one.
	//
	// (a) Little of the delay elapsed: due-now is EARLIER than the true deadline, so
	// an acceptance in [dueNow, trueFireAt) that SHOULD have suppressed the Will no
	// longer can. The Will fires early.
	closeRecent := now.Add(-6 * time.Second)
	if trueFireAt := closeRecent.Add(30 * time.Second); !time.Unix(0, dueNow).Before(trueFireAt) {
		t.Fatalf("with only 6s of a 30s delay elapsed, due-now %v should precede the true deadline %v",
			time.Unix(0, dueNow), trueFireAt)
	}

	// (b) The delay had already run out: due-now is LATER than the true deadline, so
	// an acceptance in [trueFireAt, dueNow) that should NOT have suppressed the Will
	// still can. The Will can be wrongly cancelled.
	closeOld := now.Add(-90 * time.Second)
	if trueFireAt := closeOld.Add(30 * time.Second); !time.Unix(0, dueNow).After(trueFireAt) {
		t.Fatalf("with a 90s-old close and a 30s delay, due-now %v should follow the true deadline %v",
			time.Unix(0, dueNow), trueFireAt)
	}

	// What due-now DOES buy over a restarted delay, in case (b): it is strictly closer
	// to the truth. That is the whole reason the producers compute it directly instead
	// of passing now into the helper.
	closeOldFireAt := closeOld.Add(30 * time.Second)
	if time.Unix(0, viaHelper).Sub(closeOldFireAt) <= time.Unix(0, dueNow).Sub(closeOldFireAt) {
		t.Fatal("due-now must be strictly closer to the true deadline than a restarted delay would be, " +
			"or the choice between them is not the one this disposition claims to make")
	}
}
