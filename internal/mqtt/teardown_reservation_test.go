// Copyright 2025 Nico Berlee
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"errors"
	"testing"

	"github.com/nats-io/nuid"
)

// F-101, route 2. A consumer teardown establishes what the TEARDOWN owes; it never
// establishes that the CLIENT completed. These tests pin the two gates that follow from
// that — the purge's and the void staging's — plus the send fence that keeps a staged
// verdict true, and the reservation an open send section gets.
//
// Every fixture builds its possibly-attempted state by RUNNING A REAL SEND SECTION
// (testInflightEnv.transmit), never by setting the transmitted flag by hand: the durable
// half is what recovery reads, and a memory flag beside an intact marker is a state no
// production path produces.

// mints reports whether bumpPI will hand out pi again, which is the whole question a
// reservation answers.
func testInflightMints(t *testing.T, sess *mqttSession, pi uint16) bool {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	prev := sess.last_pi
	sess.last_pi = pi - 1
	got := sess.bumpPI()
	sess.last_pi = prev
	return got == pi
}

// A POSSIBLY-ATTEMPTED entry survives a consumer teardown's purge: the identifier stays
// reserved and its Receive Maximum charge stands. Only the source mapping goes with the
// consumer being destroyed.
//
// A purge UNIT test, deliberately: the production teardown always STAGES before it purges,
// and staging is left out here so that a mutation of the staging gate cannot reach this
// test. The possibly-attempted state comes from a completed send section, not from staging.
//
// Mutation check: remove the purge's provenUnsentReleasableLocked gate (HEAD's behaviour)
// and the reservation and charge assertions fail.
func TestMQTTTeardownPurgeReservesAPossiblyAttemptedEntry(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-purge-reserve")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.transmit(t, pi, dur, sseq, true)

	if _, transmitted, _, _ := e.entryState(pi); !transmitted {
		t.Fatalf("the fixture must be POSSIBLY ATTEMPTED before the teardown, or it tests the other population")
	}

	e.sess.mu.Lock()
	regs, cancels := e.sess.purgeConsumerPendingLocked(dur)
	_, stillTracked := e.sess.pendingPublish[pi]
	charge := e.sess.qos12InflightForCap()
	_, mappingLeft := e.sess.cpending[dur]
	e.sess.mu.Unlock()

	if !stillTracked {
		t.Fatal("a possibly-attempted entry must be RESERVED by the purge: the client may hold this identifier")
	}
	if charge != 1 {
		t.Fatalf("the reservation must keep its Receive Maximum charge, got %d in flight", charge)
	}
	if testInflightMints(t, e.sess, pi) {
		t.Fatalf("bumpPI handed out identifier %d while a possibly-attempted exchange still holds it", pi)
	}
	if mappingLeft {
		t.Fatal("the source mapping goes with the consumer being destroyed: nothing can re-offer that source")
	}
	if len(cancels) != 0 {
		t.Fatalf("a confirmed allocation is not a cancellation, got %v", cancels)
	}
	// And the DURABLE owner slot survives, because the purge collected no sequence for it.
	// That is what lets the exchange still complete; see the PUBREC test below.
	for _, r := range regs {
		if r.pi == pi {
			t.Fatalf("the purge must not collect a reserved entry's register: its ladder would destroy the owner slot the exchange needs")
		}
	}
	if rec, smsg := e.loadRegister(t, pi); rec == nil || smsg == nil {
		t.Fatal("the reserved exchange's register must survive the teardown")
	}
}

// The VOID STAGING gate, in both arms, because one arm alone proves nothing: a void verdict
// is a durable "never transmitted" claim that recovery acts on by freeing the identifier, so
// it may be recorded for a PROVEN-UNSENT register and must not be for a possibly-attempted
// one.
//
// Mutation check: remove the staging gate and the possibly-attempted arm's assertion fails
// while the proven-unsent arm still passes.
func TestMQTTTeardownStagingVoidsOnlyAProvenUnsentRegister(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		transmit bool
		wantVoid bool
	}{
		{"proven-unsent", false, true},
		{"possibly-attempted", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-stage-"+tc.name)
			dur := e.sess.idHash + "_" + nuid.Next()
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 2)
			nonce := e.nonceOf(pi)
			if tc.transmit {
				e.transmit(t, pi, dur, sseq, true)
			}

			e.sess.mu.Lock()
			_, abandoned := e.sess.voidRegistrationsLocked(dur)
			_, held := e.sess.cancelledRegNonces[nonce]
			e.sess.mu.Unlock()

			// Membership in the set, never its revision value: a verdict restored from the
			// record legitimately carries revision zero, so a value read would prove nothing.
			if held != tc.wantVoid {
				t.Fatalf("verdict recorded=%v, want %v for a %s register", held, tc.wantVoid, tc.name)
			}
			if returned := len(abandoned) == 1 && abandoned[0] == nonce; returned != tc.wantVoid {
				t.Fatalf("verdict returned=%v, want %v (got %v)", returned, tc.wantVoid, abandoned)
			}
		})
	}
}

// A PROVEN-UNSENT entry is still released by the teardown, with its whole ladder. This is
// the population the gates let through, and without it the item would look like it had
// simply stopped releasing anything.
//
// Mutation check: gate the purge on something that also refuses a proven-unsent entry and
// the release assertions fail.
func TestMQTTTeardownStillReleasesAProvenUnsentEntry(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-purge-release")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	nonce := e.nonceOf(pi)

	e.sess.mu.Lock()
	_, abandoned := e.sess.voidRegistrationsLocked(dur)
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	_, stillTracked := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()

	if stillTracked {
		t.Fatal("a proven-unsent entry is released by the teardown: the client has never seen this PUBLISH")
	}
	if !testInflightMints(t, e.sess, pi) {
		t.Fatalf("identifier %d must be allocatable again once its never-transmitted exchange is released", pi)
	}
	if len(abandoned) != 1 || abandoned[0] != nonce {
		t.Fatalf("a proven-unsent register's verdict is still recorded, got %v", abandoned)
	}
	var collected bool
	for _, r := range regs {
		if r.pi == pi && r.nonce == nonce {
			collected = true
		}
	}
	if !collected {
		t.Fatal("a released entry's register is still owed to the teardown's ladder")
	}
}

// A STAGED VERDICT FENCES ITS OWN REGISTER. Staging decides before a send, so without this
// the delivery callback that was already past the subscription barrier could open a section,
// delete the marker and transmit — leaving the membership-removal save carrying a
// known-false "never transmitted" verdict for an exchange the client now holds, which
// recovery reads as licence to free the identifier.
//
// Both directions are asserted, so neither can pass on a default: the section is refused
// while the verdict stands, and admitted once a rollback withdraws it.
//
// Mutation check: remove the verdict refusal from beginSendLocked and the first assertion
// fails.
func TestMQTTStagedVoidVerdictFencesItsRegisterAgainstASend(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-verdict-fence")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	nonce := e.nonceOf(pi)
	e.bind()

	// The teardown stages its verdict on a register that is, at this instant, genuinely
	// proven unsent — so the gate records it.
	e.sess.mu.Lock()
	_, abandoned := e.sess.voidRegistrationsLocked(dur)
	e.sess.mu.Unlock()
	if len(abandoned) != 1 {
		t.Fatalf("the staging must record a verdict for this proven-unsent register, got %v", abandoned)
	}

	// The callback that was already past the barrier now tries to transmit.
	e.sess.mu.Lock()
	_, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if ok {
		t.Fatal("a register under an abandonment verdict must not transmit: the verdict would become a known-false durable claim")
	}

	// A ROLLED-BACK packet keeps its subscription and still owes this delivery, so
	// withdrawing the verdict restores admission in the same step.
	e.sess.unvoidRegNonces([]string{nonce})
	e.sess.mu.Lock()
	tok, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("withdrawing the verdict must restore send admission, or a rollback strands its own deliveries")
	}
	e.sess.endPublishSend(e.c, pi, tok, mqttIntentNone, false)
}

// AN OPEN SEND SECTION ACROSS A TEARDOWN IS RESERVED, in every outcome the section can
// reach. The teardown cannot decide an outcome that has not happened yet and it schedules
// no later release, so all four arms hold the identifier.
//
// The last arm is the item's ACCEPTED COST, asserted rather than assumed: a first-send
// definite intent refusal leaves the exchange provably unsent and its identifier is held
// anyway. docs/CONFORMANCE.md states it.
//
// Mutation check: reinstate condemnSendLocked with mqttSendSettlePurgeTeardown inside the
// reserved arm (HEAD's behaviour) and the confirmed and ambiguous arms fail.
func TestMQTTTeardownReservesAnOpenSendSectionInEveryOutcome(t *testing.T) {
	// NOT parallel: the marker-delete hook is process-global.
	for _, tc := range []struct {
		name string
		// refuse makes the sender's marker delete fail DEFINITELY on every attempt, which
		// is the only outcome that leaves the entry provably unsent. A generic error is
		// AMBIGUOUS, never definite (mqttDeleteRefusedBeforeMutation).
		refuse bool
		// ambiguous makes it fail without an authoritative answer.
		ambiguous bool
		wrote     bool
		// markerSurvives is the DURABLE outcome the two faulted arms leave, and after
		// F-102 it is what decides the restart half below: a surviving marker is positive
		// proof this PUBLISH never went out, so a fresh recovery over a source nothing can
		// re-offer RELEASES the pair, while a deleted marker leaves it possibly-attempted
		// and reserved. The live half is unaffected either way.
		markerSurvives bool
	}{
		{name: "confirmed-and-written", wrote: true},
		{name: "confirmed-but-unwritten"},
		{name: "ambiguous", ambiguous: true, markerSurvives: true},
		{name: "definite-refusal", refuse: true, markerSurvives: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "client-opensec-"+tc.name)
			// The marker-delete fault is process-global and has to be handed back BEFORE
			// recovery runs, not at subtest exit, or recovery's own marker cleanup is
			// faulted too. Idempotent so the deferred call is harmless after an explicit
			// one.
			markerHookRestore := func() {}
			releaseMarkerHook := func() {
				markerHookRestore()
				markerHookRestore = func() {}
			}
			defer func() { releaseMarkerHook() }()
			dur := e.sess.idHash + "_" + nuid.Next()
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 2)
			nonce := e.nonceOf(pi)

			tok := e.openSection(t, pi, dur, sseq)

			// The teardown runs WITH the section open. Staging must record nothing: an
			// entry whose transmission is unresolved is not proven unsent.
			e.sess.mu.Lock()
			_, abandoned := e.sess.voidRegistrationsLocked(dur)
			regs, _ := e.sess.purgeConsumerPendingLocked(dur)
			_, trackedDuring := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			if len(abandoned) != 0 {
				t.Fatalf("an open section is not proven unsent, so no verdict may be staged, got %v", abandoned)
			}
			if !trackedDuring {
				t.Fatal("the entry must survive the purge while its section is open")
			}
			for _, r := range regs {
				if r.pi == pi {
					t.Fatalf("the purge must not collect the register of an entry whose section is open")
				}
			}

			// THE REAL DURABLE INTENT, not a hand-passed outcome. Passing an intent value
			// straight to endPublishSend would mark the entry transmitted in memory while
			// leaving the unsent marker intact on the stream — the inconsistent shape no
			// production path produces, and the one this plan prohibits. So the outcome is
			// produced by faulting the marker delete itself.
			if tc.refuse || tc.ambiguous {
				var err error = &ApiError{
					Code: 400, ErrCode: uint16(JSBadRequestErr),
					Description: "test: refused before any mutation",
				}
				if tc.ambiguous {
					err = errors.New("test: no authoritative answer from the marker delete")
				}
				markerHookRestore = setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
					return err
				})
			}
			intent := e.sess.recordSendIntent(e.c, pi, tok)
			switch {
			case tc.refuse && intent != mqttIntentDefiniteFailure:
				t.Fatalf("the fixture must produce a DEFINITE refusal, got %v", intent)
			case tc.ambiguous && intent != mqttIntentAmbiguous:
				t.Fatalf("the fixture must produce an AMBIGUOUS intent, got %v", intent)
			case !tc.refuse && !tc.ambiguous && intent != mqttIntentConfirmed:
				t.Fatalf("the fixture must produce a CONFIRMED intent, got %v", intent)
			}
			e.sess.endPublishSend(e.c, pi, tok, intent, tc.wrote)

			// The durable marker now agrees with the memory state, in both directions.
			_, markerMsg := e.loadMarker(t, pi)
			if tc.refuse || tc.ambiguous {
				if markerMsg == nil {
					t.Fatalf("a %s intent must leave the unsent marker in place", tc.name)
				}
			} else if markerMsg != nil {
				t.Fatal("a confirmed intent deletes the unsent marker, so the pair reads possibly-attempted")
			}

			present, transmitted, _, _ := e.entryState(pi)
			if !present {
				t.Fatalf("the entry must still be reserved after a %s outcome: the teardown parked no continuation", tc.name)
			}
			if wantTransmitted := !tc.refuse && !tc.ambiguous; transmitted != wantTransmitted {
				t.Fatalf("%s: transmitted=%v, want %v", tc.name, transmitted, wantTransmitted)
			}
			if testInflightMints(t, e.sess, pi) {
				t.Fatalf("bumpPI handed out identifier %d after a %s outcome", pi, tc.name)
			}
			e.sess.mu.Lock()
			charge := e.sess.qos12InflightForCap()
			_, verdicted := e.sess.cancelledRegNonces[nonce]
			e.sess.mu.Unlock()
			if charge != 1 {
				t.Fatalf("the reservation keeps its capacity charge, got %d in flight", charge)
			}
			if verdicted {
				t.Fatal("no teardown verdict may exist for an entry whose section the teardown could not decide")
			}
			if rec, smsg := e.loadRegister(t, pi); rec == nil || smsg == nil {
				t.Fatal("the register survives too: nothing collected it")
			}

			// AND ACROSS A RESTART, where F-102 splits the four arms in two.
			//
			// The consumer goes, so the source classifies Lost through the REAL
			// classifiers — no hook — and recovery then reads the marker. The live hold
			// above is F-101's rule and is unchanged in every arm: a teardown cannot
			// manufacture non-transmission proof while a send section is unresolved. What
			// changed is what a LATER pass may do once that uncertainty has settled.
			//
			// Marker DELETED (the two confirmed arms): possibly-attempted, the receiver may
			// hold this identifier, so it stays reserved with its charge — the residual
			// docs/CONFORMANCE.md still states.
			//
			// Marker SURVIVING (ambiguous, definite-refusal): proven-unsent. Neither arm
			// wrote any bytes — an unresolved or definitely-refused intent skips the write
			// — and the surviving marker is what proves it; ambiguity ALONE would not, and
			// the assertion at the top of this subtest is what establishes the marker is
			// really there. F-102 releases the pair. This arm is therefore also the
			// hook-free end-to-end fixture for the Lost classification.
			if _, err := e.sess.jsa.deleteConsumer(mqttStreamName, dur, true); err != nil {
				t.Fatalf("delete the source consumer: %v", err)
			}
			// HAND THE STREAM BACK before recovery runs. The fault above is a
			// process-global hook held to subtest exit, and recovery's own marker cleanup
			// goes through it — leaving it installed would fault the very delete the
			// release assertions below are about, and the arm would then pass for the
			// wrong reason.
			releaseMarkerHook()
			// The restart, through this file's own idiom: drop the in-memory maps and walk
			// the registers again on the SAME incarnation. Going through
			// createOrRestoreSession instead would mint a FRESH generation, because nothing
			// here persisted the session record — and the walk filter is per-incarnation, so
			// it would find no register at all and the assertion below would pass against a
			// recovery that reserved nothing. That is how this arm first failed.
			e.forgetInMemory()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			restored := e.sess
			restored.mu.Lock()
			_, restoredReserved := restored.pendingPublish[pi]
			restoredCharge := restored.qos12InflightForCap()
			restored.mu.Unlock()
			restoredReg, _ := e.loadRegister(t, pi)
			if tc.markerSurvives {
				// PROVEN-UNSENT over a Lost source: released, and the durable pair goes
				// with it. Asserted as the exact opposite of the reserved arms rather than
				// by dropping the checks — the identifier, the charge, the register and
				// re-allocation all have to move together, or the release is partial.
				if restoredReserved {
					t.Fatalf("a fresh recovery must RELEASE identifier %d: its marker proves "+
						"the PUBLISH never went out and its source can never be re-offered", pi)
				}
				if restoredCharge != 0 {
					t.Fatalf("the released exchange still charges Receive Maximum, got %d", restoredCharge)
				}
				if restoredReg != nil {
					t.Fatal("the durable register survived a release that freed its identifier")
				}
				if !testInflightMints(t, restored, pi) {
					t.Fatalf("identifier %d was released but bumpPI still refuses it", pi)
				}
			} else {
				if !restoredReserved {
					t.Fatalf("a fresh recovery must reserve identifier %d: its marker is gone, so "+
						"the client may hold it from an attempt whose acknowledgement was lost", pi)
				}
				if restoredCharge != 1 {
					t.Fatalf("the restored reservation carries its capacity charge, got %d", restoredCharge)
				}
				if restoredReg == nil {
					t.Fatal("the reserved exchange's register must survive with it")
				}
				if testInflightMints(t, restored, pi) {
					t.Fatalf("a fresh recovery handed out reserved identifier %d", pi)
				}
			}
		})
	}
}

// The reserved exchange still COMPLETES, and at HEAD it could not: with the entry deleted a
// legitimate PUBREC reached mqttNewCorrelatedPubRel with no exchange identity, which returns
// nil, so the handler rolled back and returned an error out of the parse loop — closing the
// connection of a conforming client.
//
// Mutation check: collect the reserved entry's register for the teardown's ladder (destroying
// the owner slot) and the PUBREL admission assertion fails while the reservation assertions
// still pass.
func TestMQTTReservedTeardownExchangeStillAnswersAPubRec(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-reserved-pubrec")
	dur := e.sess.idHash + "_" + nuid.Next()
	e.subscribePubRel(t)

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.transmit(t, pi, dur, sseq, true)

	// THE WHOLE TEARDOWN, staging and ladder included, not just the purge. Running the purge
	// alone would leave nothing that could ever delete the owner slot, so the slot-survival
	// assertion below would hold no matter what the purge collected — which is exactly what a
	// mutation that collects the reserved register proved when this test ran without the
	// ladder (feedback_fixture_must_reach_its_route).
	e.sess.mu.Lock()
	_, abandoned := e.sess.voidRegistrationsLocked(dur)
	regs, cancels := e.sess.purgeConsumerPendingLocked(dur)
	e.sess.mu.Unlock()
	if len(abandoned) != 0 {
		t.Fatalf("a possibly-attempted register must get no never-transmitted verdict, got %v", abandoned)
	}
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)
	if len(cancels) != 0 {
		e.sess.secureRegCancellations(e.c, recordTx{}, cancels)
	}
	if rec, smsg := e.loadRegister(t, pi); rec == nil || smsg == nil {
		t.Fatal("the teardown's ladder must not delete the reserved exchange's owner slot")
	}

	// The consumer goes, as a real teardown's tail does. It matters for the PUBCOMP below:
	// terminal cleanup may only remove the owner slot once the source is provably
	// non-reofferable, and with the consumer gone the source classifies Lost, which is
	// exactly the state that permits it. Leaving the consumer alive instead makes PUBCOMP
	// retain the reservation — correct behaviour, and not the edge under test here.
	if _, err := e.sess.jsa.deleteConsumer(mqttStreamName, dur, true); err != nil {
		t.Fatalf("delete the source consumer: %v", err)
	}

	e.bind()
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess, proto: mqttProtoLevel5}
	t.Cleanup(func() { e.c.mqtt = nil })

	if err := e.c.mqttProcessPublishReceived(pi, true, 0); err != nil {
		t.Fatalf("a legitimate PUBREC for a reserved exchange must be answered, not error the connection: %v", err)
	}
	e.sess.mu.Lock()
	rel, isRel := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if !isRel {
		t.Fatal("the PUBREC must hand the identifier on to the PUBREL phase")
	}
	if rel.slot.name.subject == _EMPTY_ {
		t.Fatal("the PUBREL entry must carry the surviving owner slot: it is the exchange's transmission authority")
	}
	if testInflightMints(t, e.sess, pi) {
		t.Fatalf("identifier %d is still in flight as a PUBREL exchange", pi)
	}

	// AND THE SURVIVING SLOT CARRIES WHAT ADMISSION REQUIRES. Stopping at "a PUBREL entry
	// exists with a slot reference" would be satisfied by a stored PUBREL that admission
	// refuses forever — precisely what a destroyed slot produces, since admitStoredPubRel
	// accepts a slot-less record only for a retained exchange and
	// retainedPubRelAdmittedLocked_ then refuses one whose regSeq is non-zero.
	//
	// So what is asserted is the DURABLE PRECONDITION admission tests: the PUBREC re-phased
	// the surviving slot to pubrel_ready, recorded the canonical PUBREL sequence on it, and
	// that record exists on the stream naming THIS exchange. Re-driving admitStoredBubRel
	// here with a synthetic delivery instead was FLAKY — it races the store's readback, so
	// it passed alone and failed intermittently in the package, and a flaky oracle is worth
	// less than none (the admission arrow itself is F-092's tested territory).
	slotRec, slotMsg := e.loadRegister(t, pi)
	if slotRec == nil || slotMsg == nil {
		t.Fatal("the surviving owner slot is the exchange's transmission authority and must still be there")
	}
	if slotRec.State != mqttInflightPubRelReady && slotRec.State != mqttInflightPubRelSending {
		t.Fatalf("the PUBREC must re-phase the surviving slot past PUBLISH, got state %v", slotRec.State)
	}
	if slotRec.PubRelStreamSeq == 0 || slotRec.Nonce != rel.regNonce {
		t.Fatalf("the slot must name this exchange's canonical PUBREL record, got seq=%d nonce=%q want nonce %q",
			slotRec.PubRelStreamSeq, slotRec.Nonce, rel.regNonce)
	}
	if canon, err := e.asm.jsa.loadMsg(mqttOutStreamName, slotRec.PubRelStreamSeq); err != nil || canon == nil {
		t.Fatalf("the canonical PUBREL record the slot names must exist, or nothing can be admitted: %v", err)
	}
	// The test stops here deliberately. Everything past this point — admission's arrow, the
	// PUBREL transmission, PUBCOMP and the terminal slot — is F-092's and F-089's lifecycle
	// and has its own tests; driving it from here meant racing the live PUBREL delivery and
	// made this oracle intermittent. What F-101 owns is that the teardown left an exchange
	// that CAN complete: the acknowledgement was accepted rather than erroring the
	// connection, and the durable authority it needs is still there.
}
