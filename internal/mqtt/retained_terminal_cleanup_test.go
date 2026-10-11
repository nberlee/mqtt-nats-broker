//go:build mqttport_fulltests

package mqtt

import (
	"testing"
	"time"
)

// A retained terminal slot's cleanup is owned until it completes (WP-A round 1, consult 81).
// These tests drive real PUBREC, PUBREL and PUBCOMP handling while the source reads Unknown, so
// PUBCOMP retains the `pubrel_terminal` slot and enrolls its owner, and then settle the source
// through one production route with no client packet and no resume. None of them drives the
// terminal finalizer by hand: the owner is the only thing that may finish the slot.

const testRetainedNeverOnItsOwn = time.Hour

func testRetainedCleanupPacing(e *testInflightEnv, delay func(int) time.Duration) {
	e.sess.mu.Lock()
	e.sess.testRetainedCleanupDelay = delay
	e.sess.mu.Unlock()
}

func testRetainedSourceState(e *testInflightEnv, st *mqttInflightSourceState) {
	e.asm.mu.Lock()
	defer e.asm.mu.Unlock()
	if st == nil {
		e.asm.testInflightSourceState = nil
		return
	}
	s := *st
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return s }
}

func testRetainedOwner(e *testInflightEnv) *mqttRetainedCleanup {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	for _, w := range e.sess.retainedCleanups {
		return w
	}
	return nil
}

func testRetainedOwnerAttempts(e *testInflightEnv) int {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	for _, w := range e.sess.retainedCleanups {
		return w.attempt
	}
	return -1
}

// testOrdinaryOffered is testUnitCOfferedDisposal without the disposal: an admitted, offered
// QoS 2 delivery whose transmission intent has been consumed.
func testOrdinaryOffered(t *testing.T) *testAbandonFix {
	t.Helper()
	e := testNewInflightEnv(t, "wpa1-offered")
	testReadyForRealPubRelDelivery(t, e)
	e.c.mqtt.proto = mqttProtoLevel5
	dur := e.sess.idHash + "_wpa1"
	seq, reply := e.publishOfferedSource(t, dur)
	pi := e.trackAdmitted(t, dur, seq, 2, reply)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	r, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || r == nil {
		t.Fatal("missing admitted source register")
	}
	ml := e.asm.jsa.loadOccupantMarkerLoad(subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, subject, ml.seq, true); err != nil {
		t.Fatal(err)
	}
	return &testAbandonFix{testWithdrawFix: &testWithdrawFix{
		e: e, pi: pi, subject: subject, gen: e.sess.gen, lin: r.Lin, nonce: r.Nonce, sseq: seq,
	}, reply: reply}
}

// testRetainedAfterPubComp completes the client side of the exchange while the source reads
// Unknown, and returns once PUBCOMP has retained the terminal slot under an enrolled owner.
func testRetainedAfterPubComp(t *testing.T, f *testAbandonFix) mqttInflightSlotRef {
	t.Helper()
	e := f.e
	unknown := mqttInflightSourceUnknown
	testRetainedSourceState(e, &unknown)
	if err := e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "successful PUBREC materializes", func() bool {
		r, _ := f.register(t)
		return r != nil && r.State == mqttInflightPubRelReady && r.PubRelStreamSeq != 0
	})
	return testRetainedFromReady(t, f)
}

// testRetainedFromReady runs the client's PUBREL and PUBCOMP over a `pubrel_ready` slot.
func testRetainedFromReady(t *testing.T, f *testAbandonFix) mqttInflightSlotRef {
	t.Helper()
	e := f.e
	unknown := mqttInflightSourceUnknown
	testRetainedSourceState(e, &unknown)
	r, _ := f.register(t)
	testDeliverPubRelCorrelated(t, e, f.pi,
		testInflightAckSubject(r.PubRelDurable, r.PubRelStreamSeq, 1), r.Nonce, r.Dur, r.Seq)
	e.s.finishOwedActions()
	testCustodyEventually(t, "PUBREL action drains", func() bool {
		cur, _ := f.register(t)
		return cur != nil && cur.ActionToken == ""
	})
	e.c.mqttProcessPubComp(f.pi)
	e.s.finishOwedTerminals()
	testCustodyEventually(t, "PUBCOMP retains the terminal slot under an owner", func() bool {
		cur, _ := f.register(t)
		return cur != nil && cur.State == mqttInflightPubRelTerminal && testRetainedOwner(e) != nil
	})
	if testCustodyMints(e.sess, f.pi) == f.pi {
		t.Fatal("a retained terminal slot's identifier is allocatable")
	}
	cur, seq := f.register(t)
	return *mqttSlotRefFromRecord(f.subject, seq, cur)
}

func testRetainedConverged(t *testing.T, f *testAbandonFix, what string) {
	t.Helper()
	testCustodyEventually(t, what, func() bool {
		r, _ := f.register(t)
		return r == nil && testRetainedOwner(f.e) == nil && testCustodyMints(f.e.sess, f.pi) == f.pi
	})
}

// The WP-A round 1 schedule: the disposal owns the source of an abandoned exchange, PUBCOMP
// retains the slot while that source is unsettled, and the owner's own retries are pushed out
// of reach. The disposal's confirmed settlement is the only thing left that can finish it.
//
// Mutation: drop the wake in (*mqttDisposal).settleSource. The slot, its owner and the
// identifier's exclusion all stay.
func TestRetainedTerminalCleanupWokenByDisposalSettlement(t *testing.T) {
	// The unit C history: the disposal's abandonment landed while the source read Unknown,
	// then a real PUBREC recorded and materialized its PUBREL. Worker dispatch is closed in
	// this fixture, so a woken owner reaches the server's owed pass and nothing else runs it.
	f, _ := testC76History(t, "ready")
	e := f.e
	e.sess.mu.Lock()
	var w *mqttDisposal
	for _, d := range e.sess.disposals {
		w = d
	}
	e.sess.testRetainedCleanupDelay = func(int) time.Duration { return testRetainedNeverOnItsOwn }
	e.sess.mu.Unlock()
	if w == nil {
		t.Fatal("fixture: the disposal must still own the source")
	}
	testRetainedFromReady(t, f)

	testRetainedSourceState(e, nil)
	w.step()
	e.sess.mu.Lock()
	owned := e.sess.disposalCurrentLocked(w)
	e.sess.mu.Unlock()
	if owned || !w.settled {
		t.Fatal("the disposal did not settle its source")
	}
	testCustodyEventually(t, "the woken owner is handed to the owed pass", func() bool {
		e.s.owedRetainedMu.Lock()
		defer e.s.owedRetainedMu.Unlock()
		return len(e.s.owedRetained) == 1
	})
	e.s.finishOwedRetainedCleanups()
	// WP4: no dispatcher work starts after the shutdown gate closes; the fixture's
	// closeWillDispatch is a setup shortcut, not shutdown. The identifier's release is the custody
	// dispatcher's, so the gate reopens before convergence is asserted (ruling R-impl-3).
	testReopenDispatchForCustody(t, e)
	testRetainedConverged(t, f, "the disposal's settlement finishes the retained slot")
}

// The ordinary retired-source route: a retirement owns the source, and clearing it on a
// confirmed acknowledgement is the settlement the retained slot waits on.
//
// Mutation: drop the wake in clearRetiredSourceMatching.
func TestRetainedTerminalCleanupWokenByRetiredSourceClear(t *testing.T) {
	f := testOrdinaryOffered(t)
	e := f.e
	testRetainedCleanupPacing(e, func(int) time.Duration { return testRetainedNeverOnItsOwn })
	ref := testRetainedAfterPubComp(t, f)
	key := mqttDeferredKey{ref.dur, ref.sseq}
	e.sess.mu.Lock()
	if e.sess.srcAckOwed == nil {
		e.sess.srcAckOwed = make(map[mqttDeferredKey]*mqttRetiredSource)
	}
	e.sess.srcAckOwed[key] = &mqttRetiredSource{pi: f.pi, regSeq: ref.seq, nonce: ref.nonce}
	e.sess.mu.Unlock()

	testRetainedSourceState(e, nil)
	e.sess.clearRetiredSource(e.c, ref.dur, ref.sseq)
	testRetainedConverged(t, f, "the retirement's settlement finishes the retained slot")
}

// resolveRetiredSource reads the source Complete and drops the retirement: the same handoff.
//
// Mutation: drop the wake in resolveRetiredSource.
func TestRetainedTerminalCleanupWokenByResolvedRetirement(t *testing.T) {
	f := testOrdinaryOffered(t)
	e := f.e
	testRetainedCleanupPacing(e, func(int) time.Duration { return testRetainedNeverOnItsOwn })
	ref := testRetainedAfterPubComp(t, f)
	key := mqttDeferredKey{ref.dur, ref.sseq}
	e.sess.mu.Lock()
	if e.sess.srcAckOwed == nil {
		e.sess.srcAckOwed = make(map[mqttDeferredKey]*mqttRetiredSource)
	}
	e.sess.srcAckOwed[key] = &mqttRetiredSource{pi: f.pi, regSeq: ref.seq, nonce: ref.nonce}
	e.sess.mu.Unlock()

	testRetainedSourceState(e, nil)
	if !e.sess.resolveRetiredSource(e.c, key) {
		t.Fatal("a source acknowledged at PUBREC did not resolve Complete")
	}
	testRetainedConverged(t, f, "the resolved retirement finishes the retained slot")
}

// No settlement event at all (the cold Complete-then-Unknown shape): the owner's own retries
// carry the obligation well past the thirty attempts that used to retire it.
//
// Mutation: restore a lifetime cutoff at 30 attempts in runRetainedCleanup.
func TestRetainedTerminalCleanupOutlivesTheFormerAttemptLimit(t *testing.T) {
	f := testOrdinaryOffered(t)
	e := f.e
	testRetainedCleanupPacing(e, func(int) time.Duration { return time.Millisecond })
	testRetainedAfterPubComp(t, f)
	testCustodyEventually(t, "the owner retries past the former limit", func() bool {
		return testRetainedOwnerAttempts(e) > 40
	})
	testRetainedSourceState(e, nil)
	testRetainedConverged(t, f, "the owner converges once the source reads settled")
}

// A retained arm that ran late — it observed the source live, then the owner finished the
// slot — re-installs the exclusion over an empty subject. The owner it enrolls lifts it.
//
// Mutation: remove the first-read-absent lift in finalizeTerminalSlotObserved.
func TestRetainedTerminalCleanupLiftsALateArmsExclusion(t *testing.T) {
	f := testOrdinaryOffered(t)
	e := f.e
	testRetainedCleanupPacing(e, func(int) time.Duration { return 10 * time.Millisecond })
	ref := testRetainedAfterPubComp(t, f)
	testRetainedSourceState(e, nil)
	testRetainedConverged(t, f, "the owner finishes the slot")

	e.sess.releaseRetainedTerminalReservation(e.c, ref)
	testRetainedConverged(t, f, "the late arm's exclusion is lifted over the empty subject")
}

// Source settlement never manufactures a PUBCOMP: an owner enrolled over a slot the client has
// not completed ends without touching it.
//
// Mutation: drop the not-`pubrel_terminal` exit in attemptRetainedCleanup. The owner then
// retries forever over a live exchange.
func TestRetainedTerminalCleanupNeverFinishesAnUncompletedExchange(t *testing.T) {
	f, _ := testC76History(t, "ready")
	e := f.e
	testRetainedCleanupPacing(e, func(int) time.Duration { return time.Millisecond })
	r, seq := f.register(t)
	ref := *mqttSlotRefFromRecord(f.subject, seq, r)
	e.sess.mu.Lock()
	w := e.sess.enrollRetainedTerminalCleanupLocked(e.c, ref)
	if w == nil {
		e.sess.mu.Unlock()
		t.Fatal("enrollment refused a current-generation slot")
	}
	e.sess.scheduleRetainedCleanupLocked(w, 0)
	e.sess.mu.Unlock()
	// Dispatch is closed in this fixture: the attempt runs from the owed pass.
	testCustodyEventually(t, "the owner is handed to the owed pass", func() bool {
		e.s.owedRetainedMu.Lock()
		defer e.s.owedRetainedMu.Unlock()
		return len(e.s.owedRetained) == 1
	})
	e.s.finishOwedRetainedCleanups()
	if testRetainedOwner(e) != nil {
		t.Fatal("the owner stayed over an exchange the client has not completed")
	}
	if after, afterSeq := f.register(t); after == nil || *after != *r || afterSeq != seq {
		t.Fatal("the owner changed an exchange the client has not completed")
	}
}

// The drain's retained arm: PUBCOMP arrived while the PUBREL action was live, so the exchange
// ended pending drain, and the holder's own drain later found the source unsettled. That arm
// holds no reservation, and it must still leave the slot an owner.
//
// Mutation: drop the enrollment in settleDrainedTerminal's retained arm. No owner exists, so
// the disposal's settlement has nothing to wake and the slot stands.
func TestRetainedTerminalCleanupOwnedAfterTheHoldersDrain(t *testing.T) {
	f, act := testC76History(t, "pending-drain")
	e := f.e
	e.sess.mu.Lock()
	var w *mqttDisposal
	for _, d := range e.sess.disposals {
		w = d
	}
	e.sess.testRetainedCleanupDelay = func(int) time.Duration { return testRetainedNeverOnItsOwn }
	e.sess.mu.Unlock()
	if w == nil || act == nil {
		t.Fatal("fixture: a live PUBREL action and the source-owning disposal are required")
	}
	e.sess.startActionDrain(e.c, act)
	cur, _ := f.register(t)
	if cur == nil || cur.State != mqttInflightPubRelTerminal || cur.ActionToken != "" {
		t.Fatal("fixture: the drain must leave a retained, token-free terminal slot")
	}
	if testRetainedOwner(e) == nil {
		t.Fatal("the drain retained the terminal slot without an owner")
	}
	if testCustodyMints(e.sess, f.pi) == f.pi {
		t.Fatal("the drain's retained slot left its identifier allocatable")
	}

	testRetainedSourceState(e, nil)
	w.step()
	testCustodyEventually(t, "the woken owner is handed to the owed pass", func() bool {
		e.s.owedRetainedMu.Lock()
		defer e.s.owedRetainedMu.Unlock()
		return len(e.s.owedRetained) == 1
	})
	e.s.finishOwedRetainedCleanups()
	// WP4: no dispatcher work starts after the shutdown gate closes; the fixture's
	// closeWillDispatch is a setup shortcut, not shutdown. The identifier's release is the custody
	// dispatcher's, so the gate reopens before convergence is asserted (ruling R-impl-3).
	testReopenDispatchForCustody(t, e)
	testRetainedConverged(t, f, "the disposal's settlement finishes the drain's retained slot")
}
