package mqtt

import (
	"errors"
	"fmt"
	"testing"
)

// Unit tests for the resume reconciliation phase's bounded batch verification
// (sess_resume_reconcile.go). The end-to-end behavior — including the
// instance-scoped lookup hook on a real CONNECT — is covered in
// sess_resume_reconcile_e2e_test.go.

// The helper walks the deduplicated list in batches of at most `bound`, and
// classifies each durable present/absent from a completed lookup.
func TestMQTTVerifyConsumersBatchedBounds(t *testing.T) {
	t.Parallel()
	durables := []string{"a", "b", "c", "d", "e"}
	var batches [][]string
	lookup := func(durs []string) ([]*ConsumerInfo, []bool, error) {
		batches = append(batches, durs)
		infos := make([]*ConsumerInfo, len(durs))
		oks := make([]bool, len(durs))
		for i, d := range durs {
			oks[i] = true
			if d != "c" { // "c" is authoritatively absent
				infos[i] = &ConsumerInfo{Name: d}
			}
		}
		return infos, oks, nil
	}

	present, err := mqttVerifyConsumersBatched(durables, 2, lookup)
	if err != nil {
		t.Fatalf("mqttVerifyConsumersBatched: %v", err)
	}
	if len(batches) != 3 || len(batches[0]) != 2 || len(batches[1]) != 2 || len(batches[2]) != 1 {
		t.Fatalf("batches = %v, want sizes [2 2 1]", batches)
	}
	for _, d := range durables {
		want := d != "c"
		if present[d] != want {
			t.Fatalf("present[%q] = %v, want %v", d, present[d], want)
		}
	}
}

// Any transport failure, unreadable entry or short answer is INDETERMINATE and
// fails the WHOLE verification: absence must be authoritative before anything
// is recreated over it, and presence before a session resumes trusting it.
func TestMQTTVerifyConsumersBatchedIndeterminate(t *testing.T) {
	t.Parallel()
	durables := []string{"a", "b", "c", "d", "e"}

	for _, tc := range []struct {
		name   string
		lookup func(durs []string) ([]*ConsumerInfo, []bool, error)
	}{
		{"a chosen batch fails", func(durs []string) ([]*ConsumerInfo, []bool, error) {
			// The SECOND batch's transport fails; the first answered fine.
			if durs[0] == "c" {
				return nil, nil, errors.New("simulated batch outage")
			}
			infos := make([]*ConsumerInfo, len(durs))
			oks := make([]bool, len(durs))
			for i := range durs {
				infos[i], oks[i] = &ConsumerInfo{Name: durs[i]}, true
			}
			return infos, oks, nil
		}},
		{"one entry unreadable", func(durs []string) ([]*ConsumerInfo, []bool, error) {
			infos := make([]*ConsumerInfo, len(durs))
			oks := make([]bool, len(durs))
			for i, d := range durs {
				if d == "d" {
					continue // no response for this durable: not ok
				}
				infos[i], oks[i] = &ConsumerInfo{Name: d}, true
			}
			return infos, oks, nil
		}},
		{"short answer", func(durs []string) ([]*ConsumerInfo, []bool, error) {
			return make([]*ConsumerInfo, len(durs)-1), make([]bool, len(durs)-1), nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if present, err := mqttVerifyConsumersBatched(durables, 2, tc.lookup); err == nil {
				t.Fatalf("expected an indeterminate failure, got %v", present)
			}
		})
	}
}

// A degenerate bound never panics and never skips a durable.
func TestMQTTVerifyConsumersBatchedDegenerateBound(t *testing.T) {
	t.Parallel()
	durables := []string{"a", "b", "c"}
	calls := 0
	lookup := func(durs []string) ([]*ConsumerInfo, []bool, error) {
		calls++
		if len(durs) != 1 {
			return nil, nil, fmt.Errorf("batch of %d with bound clamped to 1", len(durs))
		}
		return []*ConsumerInfo{{Name: durs[0]}}, []bool{true}, nil
	}
	present, err := mqttVerifyConsumersBatched(durables, 0, lookup)
	if err != nil {
		t.Fatalf("mqttVerifyConsumersBatched: %v", err)
	}
	if calls != 3 || len(present) != 3 {
		t.Fatalf("calls=%d present=%v, want every durable resolved one at a time", calls, present)
	}
}

// A landed lifetime grant retargets only the teardown THIS connection parked.
//
// Review finding: a deferred disconnect carries no owner identity, and one
// parked by an earlier connection can outlive its resolution — resolveUncertainNow
// classifies a write as ours without discharging a parked action, after which
// the periodic pass and the next CONNECT both skip resolution because the
// session is no longer uncertain. Retargeting whatever is parked would then
// rewrite a previous connection's teardown terms with this connection's.
//
// Mutation check: drop the `cur != prior` guard and the pre-existing action
// below is rewritten.
func TestMQTTRetargetDeferredLifetimeIsScopedToItsOwnAction(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "retarget"}

	stale := &mqttDeferredDisconnect{disconnectedAt: 1000, expiryInterval: mqttSessionNeverExpire}
	sess.deferredDisconnect = stale

	// A grant attempted while `stale` was parked must not touch it, however its
	// own write is later classified.
	sess.retargetDeferredLifetime(stale, 0)
	if stale.expiryInterval != mqttSessionNeverExpire {
		t.Fatalf("a teardown parked by an earlier connection was retargeted to %d; it is not this grant's to rewrite",
			stale.expiryInterval)
	}

	// The close of the granting connection parks its own action, which the same
	// grant then legitimately retargets once its write is proven landed.
	fresh := &mqttDeferredDisconnect{disconnectedAt: 2000, expiryInterval: mqttSessionNeverExpire}
	sess.deferredDisconnect = fresh
	sess.retargetDeferredLifetime(stale, 0)
	if fresh.expiryInterval != 0 {
		t.Fatalf("this connection's parked teardown was not retargeted: expiryInterval=%d, want the granted 0",
			fresh.expiryInterval)
	}

	// Nothing parked at all is simply a no-op.
	sess.deferredDisconnect = nil
	sess.retargetDeferredLifetime(nil, 0)
}

// The ungated park is atomic with its uncertainty check.
//
// Review finding: checking isSeqUncertain and then parking were two steps, so a
// gate-holding resolver could clear the uncertainty and run its final discharge
// scan between them — and an action parked after that scan is stranded forever,
// because the session is no longer uncertain and neither the periodic pass nor
// the next CONNECT resolves it again.
//
// Mutation check: park without requiring uncertainty inside the critical
// section and the resolved-session case below parks a stranded action.
func TestMQTTDeferDisconnectIfUncertainIsAtomic(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "atomic-park"}
	action := &mqttDeferredDisconnect{disconnectedAt: 1, expiryInterval: 300}

	// Resolved session: the ungated park must refuse and say why.
	parked, stillUncertain := sess.deferDisconnectIfUncertain(action, true)
	if parked || stillUncertain {
		t.Fatalf("parked=%v stillUncertain=%v on a resolved session; the action would be stranded", parked, stillUncertain)
	}
	if sess.deferredDisconnect != nil {
		t.Fatal("an action was parked on a resolved session")
	}

	// Uncertain session: the same call parks and detaches.
	sess.seqUncertain = true
	sess.c = &client{}
	parked, stillUncertain = sess.deferDisconnectIfUncertain(action, true)
	if !parked || !stillUncertain {
		t.Fatalf("parked=%v stillUncertain=%v on an uncertain session, want true/true", parked, stillUncertain)
	}
	if sess.deferredDisconnect != action || sess.c != nil {
		t.Fatal("the park did not record the action and detach the client")
	}

	// First action wins; a second is refused but reported as handled.
	parked, stillUncertain = sess.deferDisconnectIfUncertain(&mqttDeferredDisconnect{}, true)
	if parked || !stillUncertain {
		t.Fatalf("parked=%v stillUncertain=%v with an action already parked, want false/true", parked, stillUncertain)
	}
	if sess.deferredDisconnect != action {
		t.Fatal("the first action's deadlines did not win")
	}

	// The gated caller skips the uncertainty requirement: it holds the gate, so
	// nothing can resolve concurrently, and it may park a resolved-looking
	// session mid-transition.
	sess2 := &mqttSession{id: "atomic-park-gated"}
	if parked, _ := sess2.deferDisconnectIfUncertain(action, false); !parked {
		t.Fatal("the gated form must park without requiring uncertainty")
	}
}
