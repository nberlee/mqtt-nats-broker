//go:build mqtt_fulltests

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

package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// Every way a Will leaves its connection maps to one anchor action (issue #133, plan 5.3), and
// the claimant decides a record from the anchor's suppression history. These tests drive the
// producers that own each action and read the anchor they leave.

// testAnchorRemovalRequested reports whether a removal of ref is owed on asm right now.
// dropWillAnchor records the removal before it returns, and the retry loop forgets it only once
// the anchor is gone, so "not requested and still standing" proves no removal was ever asked for.
func testAnchorRemovalRequested(asm *mqttAccountSessionManager, clientID string, ref *mqttWillAnchorRef) bool {
	asm.mu.Lock()
	defer asm.mu.Unlock()
	_, ok := asm.anchorRemovals[mqttAnchorRemoval{clientID: clientID, ref: *ref}]
	return ok
}

// testAnchorStanding is the anchor ref names as it stands in the client's authority.
func testAnchorStanding(t *testing.T, asm *mqttAccountSessionManager, clientID string, ref *mqttWillAnchorRef) (mqttWillAnchor, bool) {
	t.Helper()
	for _, a := range testWillAnchorsOf(t, asm, clientID) {
		if a.Ord == ref.Ord {
			return a, true
		}
	}
	return mqttWillAnchor{}, false
}

// testAwaitAnchorGone waits for ref's removal to land.
func testAwaitAnchorGone(t *testing.T, asm *mqttAccountSessionManager, clientID string, ref *mqttWillAnchorRef, why string) {
	t.Helper()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if _, ok := testAnchorStanding(t, asm, clientID, ref); ok {
			return fmt.Errorf("%s: Will anchor %d of %q still stands", why, ref.Ord, clientID)
		}
		return nil
	})
}

// testAnchorDecider is an account manager whose claimant decides from life, read as the
// authority's lifecycle, without a server.
func testAnchorDecider(life *mqttLifecycleState) *mqttAccountSessionManager {
	as := &mqttAccountSessionManager{}
	as.testAnchorEvidenceLoad = func(string) mqttAuthorityLoad {
		return mqttAuthorityLoad{status: mqttOutboundValid, seq: 1,
			rec: &mqttOutboundAuthority{ID: "decided", Rev: 1, Life: life}}
	}
	return as
}

// testAcceptAt is life after an acceptance at time at, ordinal ord, checked against the
// boundary's rule for the anchors an acceptance installs.
func testAcceptAt(t *testing.T, pl *mqttLifecycleState, ord uint64, at int64, delayCapable bool) (*mqttLifecycleState, *mqttWillAnchor) {
	t.Helper()
	anchors, next, own, err := mqttAcceptAnchors(pl, ord, at, delayCapable)
	if err != nil {
		t.Fatal(err)
	}
	nl := *pl
	nl.Anchors, nl.NextAnchorOrd = anchors, next
	nl.Accept = mqttAcceptance{Ord: ord, At: at}
	if cerr := mqttCheckAcceptAnchors(pl, &nl); cerr != nil {
		t.Fatalf("the acceptance at %d installs anchors its boundary refuses: %v", at, cerr)
	}
	return &nl, own
}

// testOweAt owes anchor ord of life with fireAt through the anchor transition, checked against
// its boundary rule, and returns the lifecycle it installs.
func testOweAt(t *testing.T, life *mqttLifecycleState, ord uint64, fireAt int64) *mqttLifecycleState {
	t.Helper()
	prev := &mqttOutboundAuthority{ID: "decided", Epoch: testEpoch, Rev: 4, Txn: "txn-a", OwnerTxn: "txn-a",
		State: mqttAuthorityClosed, Life: life}
	tr, err := mqttComposeAnchorTransition(prev, 1, "txn-owe", ord, mqttAnchorOwe, fireAt)
	if err != nil {
		t.Fatal(err)
	}
	if rerr := mqttCheckAnchorTransitionRules(tr); rerr != nil {
		t.Fatalf("owing anchor %d was refused by its boundary rule: %v", ord, rerr)
	}
	return tr.Auth.Life
}

// testDecide is the claimant's verdict on the Will anchor ord of life protects, firing at fireAt.
func testDecide(t *testing.T, life *mqttLifecycleState, ord uint64, fireAt int64) mqttAnchorVerdict {
	t.Helper()
	willID, err := mqttWillToken(life.L, ord)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := testAnchorDecider(life).decideFromAnchor(&mqttWillOutbox{ClientID: "decided", WillID: willID, FireAt: fireAt})
	return v
}

// T-minAt (issue #133): a Will firing at 100, followed by acceptances at 120, 80 and 130, is
// suppressed: the anchor keeps the EARLIEST later acceptance, and 80 came before the fire time
// even though the latest did not. Acceptances only after the fire time leave it to publish.
// Mutation M12: minAt set to the latest acceptance's time (will_anchor.go, mqttAcceptAnchors,
// `if a.MinAt == 0 || at < a.MinAt {` replaced by `if true {`).
func TestTheEarliestLaterAcceptanceSuppressesAWill(t *testing.T) {
	for _, tc := range []struct {
		name  string
		later []int64
		want  mqttAnchorVerdict
	}{
		{"an acceptance before the fire time among later ones", []int64{120, 80, 130}, mqttAnchorCancel},
		{"acceptances only after the fire time", []int64{120, 130}, mqttAnchorPublish},
	} {
		t.Run(tc.name, func(t *testing.T) {
			life, own := testAcceptAt(t, &mqttLifecycleState{L: 1, NextAnchorOrd: 1}, 0, 50, true)
			life = testOweAt(t, life, own.Ord, 100)
			for i, at := range tc.later {
				life, _ = testAcceptAt(t, life, uint64(i+1), at, false)
			}
			if got := testDecide(t, life, own.Ord, 100); got != tc.want {
				t.Fatalf("acceptances at %v after a Will firing at 100: verdict %d, want %d (anchor %+v)",
					tc.later, got, tc.want, life.Anchors)
			}
		})
	}
}

// T-straddle (issue #133): the old connection closes with its Will firing at 100, and its tail
// pauses before owing the anchor; acceptances at 80 and then 120 commit meanwhile; the tail
// resumes and owes the anchor. The Will is suppressed: the ARMED anchor already held minAt 80,
// and owing it keeps that history. Mutations M12 (as T-minAt) and M33: the owe initializing minAt
// from the current acceptance only (will_anchor.go, mqttComposeAnchorTransition, mqttAnchorOwe
// arm: add `nl.Anchors[i].MinAt = prev.Life.Accept.At`, killed here by the boundary rule and by
// the verdict).
func TestAnAcceptanceWhileTheTailIsPausedStillSuppresses(t *testing.T) {
	life, own := testAcceptAt(t, &mqttLifecycleState{L: 1, NextAnchorOrd: 1}, 0, 50, true)
	life, _ = testAcceptAt(t, life, 1, 80, false)
	life, _ = testAcceptAt(t, life, 2, 120, false)
	if i, ok := mqttAnchorAt(life.Anchors, own.Ord); !ok || life.Anchors[i].State != mqttAnchorArmed || life.Anchors[i].MinAt != 80 {
		t.Fatalf("the armed anchor does not hold the earliest acceptance while its tail is paused: %+v", life.Anchors)
	}
	life = testOweAt(t, life, own.Ord, 100)
	if got := testDecide(t, life, own.Ord, 100); got != mqttAnchorCancel {
		t.Fatalf("a Will firing at 100 after an acceptance at 80 that straddled its paused tail: verdict %d, want cancel (anchor %+v)",
			got, life.Anchors)
	}
}

// T-will-exits, abandonment row (issue #133): an abandonment removes exactly the anchors of the
// pending acceptance it abandons, in both of its forms, and an older OWED anchor stays.
// Mutation M29 (abandonment row): the anchor filter dropped (accept_abandon.go,
// mqttAbandonedLife, and teardown_claim.go, mqttClaimedLife: `if p := prev.Pending; p != nil {
// ... }` deleted, one run each).
func TestAnAbandonmentRemovesOnlyItsOwnAcceptancesAnchor(t *testing.T) {
	older := mqttWillAnchor{Ord: 1, AcceptOrd: 1, State: mqttAnchorOwed, FireAt: 9, MinAt: 5}
	own := mqttWillAnchor{Ord: 2, AcceptOrd: 2, State: mqttAnchorArmed}
	life := func(kind mqttPendingAcceptKind) *mqttLifecycleState {
		return &mqttLifecycleState{L: 1, NextGenOrd: 1, NextAcceptOrd: 3, NextAnchorOrd: 3,
			Accept:  mqttAcceptance{Ord: 1, At: 5, Tenure: "tenure-1", Nonce: "nonce-1"},
			Pending: &mqttPendingAcceptance{Kind: kind, AcceptOrd: 2, Nonce: "nonce-2", At: 7},
			Anchors: []mqttWillAnchor{older, own}}
	}
	check := func(form string, got []mqttWillAnchor) {
		t.Helper()
		if len(got) != 1 || got[0] != older {
			t.Fatalf("the %s abandonment leaves anchors %+v, want only the older owed %+v", form, got, older)
		}
	}
	resume := life(mqttPendingResume)
	resume.Pending.Prior = &mqttPriorLifetime{ExpiryInterval: 60}
	nl := mqttAbandonedLife(resume)
	check("resume", nl.Anchors)

	gen, err := mqttGenToken(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	claimed := mqttClaimedLife(&mqttOutboundAuthority{Gen: gen, Life: life(mqttPendingFresh)}, mqttTeardownAbandon, "nonce-t", "wid-t")
	check("fresh", claimed.Anchors)
}

// testWillExitEnv is a producer env whose connection holds an OWED-able anchor, with the outbox
// store and the fail-safe under the test's control.
type testWillExitEnv struct {
	srv       *Server
	asm       *mqttAccountSessionManager
	account   string
	clientID  string
	c         *client
	ref       *mqttWillAnchorRef
	failSafes atomic.Int64
	stores    atomic.Int64
	landedSeq atomic.Uint64
}

// testNewWillExitEnv builds the env. land: the first create lands and every acknowledgement is
// lost; otherwise every store fails without effect. failSafe is what the fail-safe returns.
func testNewWillExitEnv(t *testing.T, name string, land bool, failSafe error) *testWillExitEnv {
	t.Helper()
	srv, asm, account := testDelayedRetryEnv(t)
	setServiceAccount(srv, account, true)
	e := &testWillExitEnv{srv: srv, asm: asm, account: account, clientID: "will-exit-" + name}
	asm.mu.Lock()
	asm.testEnqueueStore = func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		e.stores.Add(1)
		if land && e.landedSeq.Load() == 0 {
			seq, err := asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
			if err != nil {
				return 0, err
			}
			e.landedSeq.Store(seq)
		}
		return 0, errors.New("injected lost ack")
	}
	asm.testPublishWill = func(*mqttWillOutbox) error {
		e.failSafes.Add(1)
		return failSafe
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testEnqueueStore, asm.testPublishWill = nil, nil
		asm.mu.Unlock()
	})
	e.c = testWillProducerClient(t, srv, asm, account, e.clientID, "exit/"+name, 600)
	e.ref = e.c.mqtt.willAnchor
	return e
}

// close runs the ordinary producer over the connection's owed delayed Will.
func (e *testWillExitEnv) close(originSeq uint64) {
	e.c.mqttOweWill(3600)
	e.srv.mqttHandleWill(e.c, nil, mqttNewSessGen(), originSeq, e.asm.sessStreamEpoch)
}

// testBlockWillReads makes every outbox re-read indeterminate until the returned func unblocks it.
func testBlockWillReads(t *testing.T) func() {
	t.Helper()
	var blocked atomic.Bool
	blocked.Store(true)
	restore := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttWillQStreamName && blocked.Load() {
			return errors.New("injected indeterminate re-read")
		}
		return nil
	})
	t.Cleanup(restore)
	return func() { blocked.Store(false) }
}

// T-will-exits (issue #133): each producer exit whose action is a removal or a discharge asks
// for it, and the anchor goes:
//
//   - an unauthorized delayed Will is consumed and never enqueued;
//   - a validation refusal (nothing was sent) whose fail-safe publishes, and one whose fail-safe
//     fails;
//   - an unconfirmed enqueue whose create landed, after the fail-safe published: the anchor is
//     discharged, and a claimant deciding the landed record after the discharge cancels it.
//
// Mutation M29, one run per row: will_disposition.go, mqttHandleWill, the else arm's
// `anchorAsm.dropWillAnchor(anchorCID, anchor)` deleted (unauthorized row); will_delayed_retry.go,
// mqttAnchorActionFor, mqttWillEnqueuePublishedInstead / mqttWillEnqueueTerminalFailed /
// mqttWillEnqueueUnconfirmedPublished moved to the mqttWillAnchorKeep arm (one row each).
func TestEveryWillExitThatEndsTheWillReleasesItsAnchor(t *testing.T) {
	t.Run("an unauthorized delayed Will", func(t *testing.T) {
		e := testNewWillExitEnv(t, "unauthorized", false, nil)
		e.c.willAuthorized = false
		e.close(1)
		testAwaitAnchorGone(t, e.asm, e.clientID, e.ref, "an unauthorized Will is never enqueued")
		if n := e.stores.Load() + e.failSafes.Load(); n != 0 {
			t.Fatalf("an unauthorized delayed Will reached the outbox or the fail-safe %d time(s)", n)
		}
	})
	for _, tc := range []struct {
		name     string
		failSafe error
	}{
		{"a validation refusal whose fail-safe publishes", nil},
		{"a validation refusal whose fail-safe fails", errors.New("injected fail-safe failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewWillExitEnv(t, "invalid", false, tc.failSafe)
			// A zero origin sequence is refused by the record's writer validation: nothing is sent.
			e.close(0)
			if n := e.stores.Load(); n != 0 {
				t.Fatalf("fixture: a record the writer refuses reached the store %d time(s)", n)
			}
			if n := e.failSafes.Load(); n != 1 {
				t.Fatalf("fixture: the writer-side refusal ran the fail-safe %d time(s), want 1", n)
			}
			testAwaitAnchorGone(t, e.asm, e.clientID, e.ref, "a Will the writer refused can never fire")
		})
	}
	t.Run("an unconfirmed enqueue after the fail-safe published", func(t *testing.T) {
		e := testNewWillExitEnv(t, "unconfirmed-published", true, nil)
		unblock := testBlockWillReads(t)
		e.close(1)
		if e.landedSeq.Load() == 0 || e.failSafes.Load() != 1 {
			t.Fatalf("fixture: landed seq %d, fail-safes %d; want a landed create and one published fail-safe",
				e.landedSeq.Load(), e.failSafes.Load())
		}
		testAwaitAnchorGone(t, e.asm, e.clientID, e.ref, "a Will the fail-safe published is discharged")
		unblock()
		rec, _, err := e.asm.jsa.loadWillOutbox(e.asm.domainTk, mqttWillQSubject(e.asm.domainTk, getHash(e.clientID), e.ref.willID()))
		if err != nil || rec == nil {
			t.Fatalf("fixture: the landed record reads %v, %v", rec, err)
		}
		if v, _ := e.asm.decideFromAnchor(rec); v != mqttAnchorCancel {
			t.Fatalf("the landed record of a Will the fail-safe already published was decided %d, want cancel", v)
		}
	})
}

// T-will-exits, pre-CONNACK refusal row (issue #133): a CONNECT refused before CONNACK removes the
// arriving connection's OWN anchor, and an older OWED anchor of the same lifecycle stays.
// Mutation M29 (refusal row): sess_resume_reconcile.go, mqttAbortAcceptedConnect,
// `asm.dropWillAnchor(cid, anchor)` deleted.
func TestARefusedConnectRemovesOnlyItsOwnAnchor(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)
	const clientID = "refused-own-anchor"
	older := testAcceptWithWillAnchor(t, asm, clientID)
	if err := asm.runAnchorTransition(clientID, *older, mqttAnchorOwe, time.Now().Add(time.Hour).UnixNano(), nil); err != nil {
		t.Fatal(err)
	}
	c := testWillProducerClient(t, srv, asm, account, clientID, "refused/will", 600)
	own := c.mqtt.willAnchor
	if own.L != older.L || own.Ord == older.Ord {
		t.Fatalf("fixture: the arriving anchor %+v is not a second anchor of the older one's lifecycle %+v", own, older)
	}
	c.mqttAbortAcceptedConnect()
	testAwaitAnchorGone(t, asm, clientID, own, "the refused connection can never serve")
	if a, ok := testAnchorStanding(t, asm, clientID, older); !ok || a.State != mqttAnchorOwed {
		t.Fatalf("the refusal changed the older owed anchor: %+v (standing %v)", a, ok)
	}
}

// T-possibly-landed (issue #133): the create lands, its acknowledgement and every readback fail,
// and the fail-safe fails before any effect, so the Will is retained possibly landed. The arms
// TestAPossiblyLandedWillIsResolvedNeverRepublished does not cover:
//
//   - (b) the record publishes and is deleted (its anchor discharged first, as the outbox
//     terminal path does): the retry finds the record absent and the anchor discharged, and ends
//     without writing it again;
//   - (c) the retention cap refuses the entry: the anchor stays OWED with no removal asked for,
//     and the landed record is still the anchor's to publish;
//   - (d) while every store and readback stays unresolved through the retry driver's passes, the
//     fail-safe is never invoked again.
//
// Mutations: (b) will_outbox.go, enqueueDelayedWill, the `case existing == nil:` arm of the
// resolution deleted; (c) M32b, will_delayed_retry.go, releaseUnretainedDelayedWill,
// `o.identity.PossiblyLanded ||` dropped from the early return, and M29b, mqttAnchorActionFor
// returning mqttWillAnchorDischarge for mqttWillEnqueueUnconfirmedAbandoned; (d) M32,
// enqueueDelayedWill, the `if identity.PossiblyLanded { ... return
// mqttWillEnqueueUnconfirmedAbandoned, err }` block before the fail-safe deleted.
func TestAPossiblyLandedWillIsSettledByItsRecordAndNeverByTheFailSafe(t *testing.T) {
	failSafeRefused := errMQTTWillDestinationUnavailable
	// held snapshots the retained entries BY VALUE between passes: a pass re-attempts an entry
	// with the queue unlocked and updates its identity under dischargeMu alone.
	held := func(srv *Server) []mqttOwedDelayedWill {
		q := &srv.owedDelayedWills
		q.dischargeMu.Lock()
		defer q.dischargeMu.Unlock()
		q.mu.Lock()
		defer q.mu.Unlock()
		out := make([]mqttOwedDelayedWill, 0, len(q.owed))
		for _, o := range q.owed {
			out = append(out, *o)
		}
		return out
	}
	subjectOf := func(e *testWillExitEnv) string {
		return mqttWillQSubject(e.asm.domainTk, getHash(e.clientID), e.ref.willID())
	}
	retained := func(t *testing.T, e *testWillExitEnv) {
		t.Helper()
		if e.landedSeq.Load() == 0 || e.failSafes.Load() != 1 {
			t.Fatalf("fixture: landed seq %d, fail-safes %d; want a landed create and one failed fail-safe",
				e.landedSeq.Load(), e.failSafes.Load())
		}
		if h := held(e.srv); len(h) != 1 || !h[0].identity.PossiblyLanded {
			t.Fatalf("fixture: the Will is not retained possibly landed (%d held)", len(h))
		}
	}

	t.Run("(b) a record that published and was deleted ends the retry", func(t *testing.T) {
		e := testNewWillExitEnv(t, "landed-deleted", true, failSafeRefused)
		unblock := testBlockWillReads(t)
		e.close(1)
		retained(t, e)
		// The outbox terminal path, in its order: the anchor discharged, then the record deleted.
		if err := e.asm.runAnchorTransition(e.clientID, *e.ref, mqttAnchorRemove, 0, nil); err != nil {
			t.Fatal(err)
		}
		if err := e.asm.jsa.deleteMsg(mqttWillQStreamName, e.landedSeq.Load(), true); err != nil {
			t.Fatal(err)
		}
		unblock()
		checkFor(t, 15*time.Second, 50*time.Millisecond, func() error {
			if n := len(held(e.srv)); n != 0 {
				return fmt.Errorf("%d entries still held: the retry never found its Will settled", n)
			}
			return nil
		})
		if rec, _, err := e.asm.jsa.loadWillOutbox(e.asm.domainTk, subjectOf(e)); err != nil || rec != nil {
			t.Fatalf("the settled Will's subject reads %+v, %v; want absent", rec, err)
		}
		if n := e.failSafes.Load(); n != 1 {
			t.Fatalf("the fail-safe ran %d times; a resolution never takes it", n)
		}
	})

	t.Run("(c) a retention refused at the cap keeps the anchor owed", func(t *testing.T) {
		e := testNewWillExitEnv(t, "landed-capped", true, failSafeRefused)
		q := &e.srv.owedDelayedWills
		q.mu.Lock()
		q.bytes = mqttDelayedWillMaxBytes
		q.mu.Unlock()
		t.Cleanup(func() {
			q.mu.Lock()
			q.bytes = 0
			q.mu.Unlock()
		})
		unblock := testBlockWillReads(t)
		e.close(1)
		if e.landedSeq.Load() == 0 || e.failSafes.Load() != 1 {
			t.Fatalf("fixture: landed seq %d, fail-safes %d", e.landedSeq.Load(), e.failSafes.Load())
		}
		if h := held(e.srv); len(h) != 0 {
			t.Fatalf("fixture: the cap did not refuse the retention (%d held)", len(h))
		}
		if testAnchorRemovalRequested(e.asm, e.clientID, e.ref) {
			t.Fatal("a possibly-landed Will dropped at the cap asked for its anchor's removal")
		}
		if a, ok := testAnchorStanding(t, e.asm, e.clientID, e.ref); !ok || a.State != mqttAnchorOwed {
			t.Fatalf("a possibly-landed Will dropped at the cap left its anchor %+v (standing %v), want owed", a, ok)
		}
		unblock()
		rec, _, err := e.asm.jsa.loadWillOutbox(e.asm.domainTk, subjectOf(e))
		if err != nil || rec == nil || rec.State != mqttWillPending {
			t.Fatalf("fixture: the landed record reads %+v, %v", rec, err)
		}
		if v, _ := e.asm.decideFromAnchor(rec); v != mqttAnchorPublish {
			t.Fatalf("the landed record of a Will dropped at the cap was decided %d, want publish", v)
		}
	})

	t.Run("(d) an unresolvable Will never takes the fail-safe again", func(t *testing.T) {
		e := testNewWillExitEnv(t, "landed-unresolved", true, failSafeRefused)
		testBlockWillReads(t)
		e.close(1)
		retained(t, e)
		// Three resolutions past the producer's own attempts: the insertion's pass and at least
		// two of the retry driver's.
		storesBefore := e.stores.Load()
		checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
			if n := e.stores.Load() - storesBefore; n < 3*mqttWillEnqueueAttempts {
				return fmt.Errorf("the retry driver re-sent %d creates, want three resolutions", n)
			}
			return nil
		})
		if n := e.failSafes.Load(); n != 1 {
			t.Fatalf("an unresolvable possibly-landed Will took the fail-safe %d times, want only the producer's one", n)
		}
		if h := held(e.srv); len(h) != 1 || !h[0].identity.PossiblyLanded {
			t.Fatalf("an unresolvable possibly-landed Will left retention (%d held)", len(h))
		}
		if a, ok := testAnchorStanding(t, e.asm, e.clientID, e.ref); !ok || a.State != mqttAnchorOwed {
			t.Fatalf("an unresolvable possibly-landed Will left its anchor %+v (standing %v), want owed", a, ok)
		}
	})
}
