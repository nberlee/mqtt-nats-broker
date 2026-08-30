// Copyright 2024 The NATS Authors
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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// F-087: every retargeted CAS on $MQTT_inflight rebuilds from the record it just read.
//
// The tests below are organized by what a rebuild PRESERVES — the fact a concurrent
// writer proved between the caller's build and its re-target — because that is the
// property, and a per-site assertion would go stale the moment a sixth writer appears.

// testRegisterAt reads the register on pi and fails the test if it is not there.
func testRegisterAt(t *testing.T, e *testInflightEnv, pi uint16) (*mqttInflightRecord, *StoredMsg) {
	t.Helper()
	rec, smsg := e.loadRegister(t, pi)
	if rec == nil || smsg == nil {
		t.Fatalf("the register on packet identifier %d must exist", pi)
	}
	return rec, smsg
}

// testMarkRegister rewrites the register on pi with mutate applied, at its exact
// sequence — a concurrent writer, from outside the operation under test.
func testMarkRegister(t *testing.T, e *testInflightEnv, pi uint16, mutate func(*mqttInflightRecord)) uint64 {
	t.Helper()
	rec, smsg := testRegisterAt(t, e, pi)
	next := *rec
	mutate(&next)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	seq, err := e.asm.jsa.storeInflightCAS(subject, smsg.Sequence, &next)
	if err != nil {
		t.Fatalf("concurrent rewrite of the register on %d: %v", pi, err)
	}
	return seq
}

// The alpha-blocking instance: a claim contest that re-targets must not re-propose the
// body it built before the re-read. A Retired written in between is what it would erase,
// and erasing it re-sends a payload the client has already completed.
func TestMQTTInflightContestRetargetPreservesConcurrentRetirement(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-contest-rebuild")
	_, _, dur := mqttSharedNames("g", "contest.rebuild")
	sseq := e.publishSourceGroup(t, dur, "g", "contest.rebuild", "contest.rebuild")
	pi := e.track(t, dur, sseq, 2)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	asmB, sessB, cB := e.peer(t, "node2", "client-contest-rebuild")
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}

	// B's FIRST contest CAS fails without landing, and a concurrent writer marks the
	// record Retired in the same window. B's re-target then re-reads a record carrying a
	// fact its own snapshot never had.
	var retiredSeq uint64
	failed := 0
	asmB.mu.Lock()
	asmB.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if failed == 0 && strings.HasPrefix(r.Dur, mqttSharedConsumerDurablePrefix) {
			failed++
			retiredSeq = testMarkRegister(t, e, pi, func(rec *mqttInflightRecord) { rec.Retired = true })
			return 0, fmt.Errorf("test: the contest's first attempt did not land")
		}
		return asmB.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	asmB.mu.Unlock()

	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	asmB.mu.Lock()
	asmB.testInflightStore = nil
	asmB.mu.Unlock()
	if failed != 1 {
		t.Fatalf("the non-landing first attempt was not armed, failed=%d", failed)
	}

	subject := mqttInflightSubject(sessB.idHash, sessB.gen, pi)
	rec, smsg, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("the contested register must survive: rec=%+v err=%v", rec, err)
	}
	if smsg.Sequence == retiredSeq {
		t.Fatal("the contest must have re-targeted and landed, not adopted the retirement's sequence")
	}
	if !rec.Retired {
		t.Fatal("the re-targeted claim ERASED a concurrently written Retired: the payload would be re-sent after PUBCOMP")
	}
	if rec.Claim != sessB.regClaim {
		t.Fatalf("the contest owns the arbitration fields: claim=%q want %q", rec.Claim, sessB.regClaim)
	}

	// Routed on what the contest RETURNED, not on the bucket the walk sorted it into: a
	// newly retired record installs a suppression and reserves NO identifier.
	sessB.mu.Lock()
	entry := sessB.pendingPublish[pi]
	rs := sessB.srcAckOwed[mqttDeferredKey{dur, sseq}]
	sessB.mu.Unlock()
	if entry != nil {
		t.Fatalf("a newly retired register must not be installed as a deliverable entry: %+v", entry)
	}
	if rs == nil || rs.nonce != rec.Nonce {
		t.Fatalf("the retirement's suppression must be installed for this exchange: %+v", rs)
	}
}

// The retirement writer owns Retired and nothing else. Its re-target must carry a
// concurrent cancellation forward, and must not refresh the arbitration fields.
func TestMQTTInflightRetireRetargetPreservesConcurrentCancellation(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-retire-rebuild")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	rec, smsg := testRegisterAt(t, e, pi)

	// A foreign claim value, planted so a refresh of the arbitration fields is visible.
	foreignClaim := mqttNewInflightNonce()
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) { r.Claim = foreignClaim })

	// The first retirement CAS is aimed at the stale sequence, so it rejects; the
	// re-read then finds a record carrying BOTH the foreign claim and a cancellation.
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) { r.Cancelled = true })

	if _, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, smsg.Sequence,
		rec.Nonce, _EMPTY_, _EMPTY_); !ok {
		t.Fatal("the retirement must resolve through its re-target")
	}
	got, _ := testRegisterAt(t, e, pi)
	if !got.Retired {
		t.Fatal("the retirement did not land")
	}
	if !got.Cancelled {
		t.Fatal("the re-targeted retirement ERASED a concurrent cancellation")
	}
	if got.Claim != foreignClaim {
		t.Fatalf("the retirement refreshed the arbitration fields, which belong to the contest: claim=%q want %q",
			got.Claim, foreignClaim)
	}
}

// Both cancellation writers own Cancelled and nothing else. A re-target that
// re-proposed the sparse first-CAS body would erase a Retired — the catastrophic
// direction, because recovery then treats a completed exchange as deliverable.
func TestMQTTInflightCancelRetargetPreservesConcurrentRetirement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mark func(e *testInflightEnv, pi uint16, dur string, sseq, regSeq uint64, nonce string) error
	}{
		{"markInflightRegisterCancelled", func(e *testInflightEnv, pi uint16, dur string, sseq, regSeq uint64, nonce string) error {
			return e.sess.markInflightRegisterCancelled(e.c, pi, dur, sseq, regSeq, nonce)
		}},
		{"markInflightRegisterCancelledPending", func(e *testInflightEnv, pi uint16, dur string, sseq, regSeq uint64, nonce string) error {
			return e.sess.markInflightRegisterCancelledPending(e.c, mqttRegCancellation{
				pi: pi, nonce: nonce, durable: dur, sseq: sseq,
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-cancel-rebuild-"+tc.name)
			dur := e.sess.idHash + "_plain"
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 2)
			rec, smsg := testRegisterAt(t, e, pi)

			// The retirement lands between the caller's build and its re-target: the
			// first CAS (create-only, or at the stale sequence) rejects, and the re-read
			// finds a Retired record.
			testMarkRegister(t, e, pi, func(r *mqttInflightRecord) { r.Retired = true })

			if err := tc.mark(e, pi, dur, sseq, smsg.Sequence, rec.Nonce); err != nil {
				t.Fatalf("the cancellation must resolve through its re-target: %v", err)
			}
			got, _ := testRegisterAt(t, e, pi)
			if !got.Cancelled {
				t.Fatal("the cancellation did not land")
			}
			if !got.Retired {
				t.Fatal("the re-targeted cancellation ERASED a concurrent retirement: a completed exchange would be re-sent")
			}
		})
	}
}

// Retired dominates Cancelled at every reader. Recovery is the one that decides whether
// an identifier is freed, so it is the one worth stating twice: a record carrying both
// routes to the suppression, and its identifier is NOT handed to another message.
func TestMQTTInflightRetiredDominatesCancelledAtRecovery(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-precedence")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) { r.Retired, r.Cancelled = true, true })
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	_, sessB, cB := e.peer(t, "node2", "client-precedence")
	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	sessB.mu.Lock()
	entry := sessB.pendingPublish[pi]
	rs := sessB.srcAckOwed[mqttDeferredKey{dur, sseq}]
	sessB.mu.Unlock()
	if entry != nil {
		t.Fatalf("a Retired-and-Cancelled register must not be installed as deliverable: %+v", entry)
	}
	if rs == nil {
		t.Fatal("a Retired-and-Cancelled register must reinstate its suppression rather than free the identifier")
	}
}

// The contest's close arm consumes a PURE tombstone. A record carrying both bits is not
// a close: consuming it would free an identifier whose exchange the client completed.
func TestMQTTInflightContestCloseArmRefusesRetiredTombstone(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-close-precedence")
	_, _, dur := mqttSharedNames("g", "close.precedence")
	sseq := e.publishSourceGroup(t, dur, "g", "close.precedence", "close.precedence")
	pi := e.track(t, dur, sseq, 2)
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) { r.Retired, r.Cancelled = true, true })
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	asmB, sessB, cB := e.peer(t, "node2", "client-close-precedence")
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	// The close arm is only REACHED through a failed CAS and its re-read, so the first
	// attempt is made to fail without landing. Without this the contest's CAS simply
	// succeeds and the arm under test never runs.
	failed := 0
	asmB.mu.Lock()
	asmB.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if failed == 0 && strings.HasPrefix(r.Dur, mqttSharedConsumerDurablePrefix) {
			failed++
			return 0, fmt.Errorf("test: the contest's first attempt did not land")
		}
		return asmB.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	asmB.mu.Unlock()
	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	asmB.mu.Lock()
	asmB.testInflightStore = nil
	asmB.mu.Unlock()
	if failed != 1 {
		t.Fatalf("the re-read arm was never reached, failed=%d", failed)
	}
	subject := mqttInflightSubject(sessB.idHash, sessB.gen, pi)
	rec, smsg, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil {
		t.Fatalf("load the contested register: %v", err)
	}
	if rec == nil || smsg == nil {
		t.Fatal("the contest consumed a Retired record as a close and DELETED it")
	}
	sessB.mu.Lock()
	rs := sessB.srcAckOwed[mqttDeferredKey{dur, sseq}]
	sessB.mu.Unlock()
	if rs == nil {
		t.Fatal("the retired record must reinstate its suppression")
	}
}

// Every exact-sequence delete whose caller can name the nonce routes through the verdict
// property: a verdicted register is deleted with PROOF and its verdict released against
// that proof, and a delete that cannot be proven keeps BOTH.
func TestMQTTInflightVerdictReapForgetsOnlyOnProof(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-verdict-property")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	_, smsg := testRegisterAt(t, e, pi)
	nonce := e.nonceOf(pi)

	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	e.sess.mu.Unlock()

	// A delete that FAILS keeps the register and the verdict: the next scan retries the
	// whole sequence against a verdict that is already durable.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: the delete could not be proven") }
	e.asm.mu.Unlock()
	if e.sess.reapRegisterWithVerdict(smsg.Sequence, nonce, false) {
		t.Fatal("an unproven delete must not report the register gone")
	}
	e.sess.mu.Lock()
	_, stillHeld := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if !stillHeld {
		t.Fatal("the verdict was forgotten against a delete that was never proven")
	}

	e.asm.mu.Lock()
	e.asm.testInflightDelete = nil
	e.asm.mu.Unlock()
	if !e.sess.reapRegisterWithVerdict(smsg.Sequence, nonce, false) {
		t.Fatal("a proven delete must report the register gone")
	}
	e.sess.mu.Lock()
	_, held := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if held {
		t.Fatal("a verdict whose register was deleted WITH PROOF must be forgotten, or it accumulates for the session's life")
	}
	if rec, _ := e.loadRegister(t, pi); rec != nil {
		t.Fatal("the register must be gone")
	}
}

// The sequence a resume applies to a live entry is judged against the operation's whole
// LINEAGE, not against its base alone: an operation may legitimately pass through S2 on
// its way from S1 to S3.
func TestMQTTInflightLineageAwareSequenceApply(t *testing.T) {
	t.Parallel()
	rv := mqttRecoveredInflightReg{seq: 30, traversed: []uint64{10, 20, 30}}
	for _, seq := range []uint64{10, 20, 30} {
		if !rv.traversedContains(seq) {
			t.Fatalf("sequence %d is a link in this operation's chain and must be applied", seq)
		}
	}
	if rv.traversedContains(40) {
		t.Fatal("a sequence outside the chain belongs to a writer this operation never saw; its fact stands")
	}
	// The degenerate case, which is the ordinary one: a record nothing contested carries
	// a one-element chain, and the check is exactly "unchanged since the walk".
	plain := mqttRecoveredInflightReg{seq: 7, traversed: []uint64{7}}
	if !plain.traversedContains(7) || plain.traversedContains(6) {
		t.Fatal("an uncontested record must apply at its walk sequence and nowhere else")
	}
}

// The quarantine holds its identifier through the loss of its consumer, which is the
// case a pi-keyed record could not survive: the mapping goes with the consumer, and a
// re-subscribe re-offers the same source.
func TestMQTTInflightQuarantineSurvivesItsConsumer(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-quarantine-consumer")
	_, _, dur := mqttSharedNames("g", "quarantine.consumer")
	sseq := e.publishSourceGroup(t, dur, "g", "quarantine.consumer", "quarantine.consumer")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	_, sessB, cB := e.peer(t, "node2", "client-quarantine-consumer")
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}

	// A's stale allocation for the same source meets B's claimed register.
	n2 := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq, regPending: true, regNonce: n2}
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, sseq, n2, _EMPTY_, _EMPTY_) {
		t.Fatal("the stale allocation must lose the arbitration")
	}

	// The consumer goes: the mapping goes with it, the RESERVATION stays, and the
	// teardown collects no register sequence for it — the sequence is the rival's.
	e.sess.mu.Lock()
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	held := e.sess.pendingPublish[pi]
	contested := e.sess.sourceForeignContestedLocked(dur, sseq)
	e.sess.mu.Unlock()
	for _, r := range regs {
		if r.pi == pi {
			t.Fatalf("the teardown collected the rival's register sequence for a quarantined identifier: %+v", r)
		}
	}
	if held == nil || !held.regForeign {
		t.Fatalf("the quarantine must survive its consumer: %+v", held)
	}
	if !contested {
		t.Fatal("the source-keyed contest must survive the consumer, which is what the pre-allocation decline consults")
	}

	// The re-subscribe case: with no mapping left, only the source-keyed record can
	// refuse the offer — and it must, or one message takes a second identifier.
	e.sess.mu.Lock()
	rePI, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if rePI != 0 || isNew {
		t.Fatalf("a re-offer after the purge must allocate nothing, got pi=%d isNew=%v", rePI, isNew)
	}
}

// A consumer teardown that withdraws the allocation between the failed CAS and the
// quarantine install must still leave the SOURCE quarantined.
//
// Otherwise the window degrades to the behavior this item removes: the entry and its
// mapping are gone, a re-subscribe re-offers the source, trackPublish finds neither a
// mapping nor a contest, and one message goes out under a second identifier while the
// rival's register still owns the first.
func TestMQTTInflightQuarantineSurvivesATeardownRacingItsInstall(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-quarantine-teardown-race")
	_, _, dur := mqttSharedNames("g", "quarantine.race")
	sseq := e.publishSourceGroup(t, dur, "g", "quarantine.race", "quarantine.race")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	_, sessB, cB := e.peer(t, "node2", "client-quarantine-teardown-race")
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}

	// Our stale allocation for the same source, and a teardown that fires while the
	// store's CAS is in flight — the window between observing the foreign register and
	// installing the quarantine.
	n2 := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq, regPending: true, regNonce: n2}
	e.sess.cpending[dur] = map[uint64]uint16{sseq: pi}
	e.sess.mu.Unlock()

	purged := 0
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if purged == 0 && r.Nonce == n2 {
			purged++
			e.sess.mu.Lock()
			e.sess.purgeConsumerPendingLocked(dur)
			e.sess.mu.Unlock()
			return 0, fmt.Errorf("test: the store did not land")
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	e.sess.storeInflightRegister(e.c, pi, 1, dur, sseq, n2, _EMPTY_, _EMPTY_)
	e.asm.mu.Lock()
	e.asm.testInflightStore = nil
	e.asm.mu.Unlock()
	if purged != 1 {
		t.Fatalf("the teardown window was never entered, purged=%d", purged)
	}

	e.sess.mu.Lock()
	contested := e.sess.sourceForeignContestedLocked(dur, sseq)
	e.sess.mu.Unlock()
	if !contested {
		t.Fatal("a teardown racing the install must still leave the SOURCE quarantined, or a re-subscribe " +
			"allocates a second identifier for a delivery the rival's register still names")
	}

	// The re-subscribe: a REAL re-offer of the same source must allocate nothing.
	e.sess.mu.Lock()
	rePI, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if rePI != 0 || isNew {
		t.Fatalf("a re-offer after the racing teardown must allocate nothing, got pi=%d isNew=%v", rePI, isNew)
	}

	// And it must CONVERGE. This contest owns no reservation — the teardown's
	// cancellation bookkeeping owns the entry — so a release that demanded one would be
	// rejected at every resume, leaving the source declined until the generation retired.
	// The rival's register goes; the next successful resume must free the source.
	subject := mqttInflightSubject(sessB.idHash, sessB.gen, pi)
	_, smsg, lerr := e.asm.jsa.loadInflightRecord(subject)
	if lerr != nil || smsg == nil {
		t.Fatalf("the rival register must exist before it is consumed: %v", lerr)
	}
	if derr := e.asm.deleteInflightRegisterConfirmed(smsg.Sequence); derr != nil {
		t.Fatalf("the rival's terminal delete: %v", derr)
	}
	staged, rerr := e.sess.recoverInflightRegisters(e.c)
	if rerr != nil {
		t.Fatalf("resume recovery: %v", rerr)
	}
	if len(staged) != 1 {
		t.Fatalf("the settled source-only contest must be staged for release, got %+v", staged)
	}
	e.sess.commitStagedReleases(e.c, staged)
	e.sess.mu.Lock()
	stillContested := e.sess.sourceForeignContestedLocked(dur, sseq)
	e.sess.mu.Unlock()
	if stillContested {
		t.Fatal("a source-only quarantine must converge at a successful resume, not wait for generation retirement")
	}
	e.sess.mu.Lock()
	freshPI, _, _, freshNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 3))
	e.sess.mu.Unlock()
	if freshPI == 0 || !freshNew {
		t.Fatalf("the released source must allocate normally again, got pi=%d isNew=%v", freshPI, freshNew)
	}
}

// There is no live release: the quarantine outlives the rival's completion and is
// removed only by a SUCCESSFUL resume's reconciliation or by generation retirement.
func TestMQTTInflightQuarantineHoldsUntilResume(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-quarantine-hold")
	_, _, dur := mqttSharedNames("g", "quarantine.hold")
	sseq := e.publishSourceGroup(t, dur, "g", "quarantine.hold", "quarantine.hold")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", "client-quarantine-hold")
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	n2 := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq, regPending: true, regNonce: n2}
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, sseq, n2, _EMPTY_, _EMPTY_) {
		t.Fatal("the stale allocation must lose the arbitration")
	}

	// The rival COMPLETES: its terminal path deletes the register. Nothing in the live
	// session may act on that.
	subject := mqttInflightSubject(sessB.idHash, sessB.gen, pi)
	_, smsg, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || smsg == nil {
		t.Fatalf("the rival register must exist: %v", err)
	}
	if derr := asmB.deleteInflightRegisterConfirmed(smsg.Sequence); derr != nil {
		t.Fatalf("the rival's terminal delete: %v", derr)
	}
	for i := 0; i < 3; i++ {
		e.sess.mu.Lock()
		rePI, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, uint64ToInt(i)+2))
		e.sess.mu.Unlock()
		if rePI != 0 {
			t.Fatalf("a quarantine must not be released by live traffic, got pi=%d", rePI)
		}
	}
	e.sess.mu.Lock()
	stillHeld := e.sess.pendingPublish[pi] != nil && e.sess.pendingPublish[pi].regForeign
	stillContested := e.sess.sourceForeignContestedLocked(dur, sseq)
	e.sess.mu.Unlock()
	if !stillHeld || !stillContested {
		t.Fatal("the quarantine must hold: both live release shapes were refuted, so nothing here may free the identifier")
	}

	// The resume's reconciliation STAGES the release; the identifier is freed only when
	// the staged release is committed, which happens once the resume has succeeded.
	staged, err := e.sess.recoverInflightRegisters(e.c)
	if err != nil {
		t.Fatalf("resume recovery: %v", err)
	}
	if len(staged) != 1 || staged[0].pi != pi {
		t.Fatalf("the reconciliation must stage exactly this release, got %+v", staged)
	}
	e.sess.mu.Lock()
	beforeCommit := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if beforeCommit == nil || !beforeCommit.regForeign {
		t.Fatal("the walk must not APPLY the release: a resume refused after it still preserves the session's maps")
	}

	e.sess.commitStagedReleases(e.c, staged)
	e.sess.mu.Lock()
	afterCommit := e.sess.pendingPublish[pi]
	afterContested := e.sess.sourceForeignContestedLocked(dur, sseq)
	e.sess.mu.Unlock()
	if afterCommit != nil || afterContested {
		t.Fatalf("the committed release must free the identifier and the contest: entry=%+v contested=%v",
			afterCommit, afterContested)
	}
	// A released source is a fresh source.
	e.sess.mu.Lock()
	freshPI, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 4))
	e.sess.mu.Unlock()
	if freshPI == 0 || !isNew {
		t.Fatalf("a released source must allocate normally, got pi=%d isNew=%v", freshPI, isNew)
	}
}

// uint64ToInt keeps the loop above readable; the ack subject builder takes a count.
func uint64ToInt(i int) int { return i }

// A staged release is NOT applied to a quarantine the walk no longer describes. The
// fence has two halves and each is asserted ALONE, because a test that moves both at
// once passes with either one deleted.
func TestMQTTInflightStagedReleaseFenceKeepsAReplacedQuarantine(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		what    string
		disturb func(sess *mqttSession, key mqttDeferredKey)
	}{
		{"the CONTEST record was replaced: a fresh rival, so a new sequence", func(sess *mqttSession, key mqttDeferredKey) {
			sess.foreignContested[key].regSeq = 9
		}},
		{"the RESERVATION was replaced: the entry no longer describes the staged tuple", func(sess *mqttSession, key mqttDeferredKey) {
			sess.pendingPublish[7] = &mqttPending{jsDur: key.jsDur, sseq: key.sseq, regSeq: 9, regForeign: true}
		}},
	} {
		t.Run(tc.what, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-staged-fence-"+tc.what)
			dur := e.sess.idHash + "_plain"
			key := mqttDeferredKey{dur, 11}
			e.sess.mu.Lock()
			if e.sess.pendingPublish == nil {
				e.sess.pendingPublish = make(map[uint16]*mqttPending)
			}
			e.sess.pendingPublish[7] = &mqttPending{jsDur: dur, sseq: 11, regSeq: 5, regForeign: true}
			e.sess.foreignContested = map[mqttDeferredKey]*mqttForeignContest{
				key: {pi: 7, subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, 7), regSeq: 5, reserved: true},
			}
			e.sess.mu.Unlock()
			staged := []mqttStagedRelease{{key: key, pi: 7, regSeq: 5}}

			// The disturbance lands between the walk and the commit.
			e.sess.mu.Lock()
			tc.disturb(e.sess, key)
			e.sess.mu.Unlock()

			e.sess.commitStagedReleases(e.c, staged)
			e.sess.mu.Lock()
			kept := e.sess.pendingPublish[7]
			contested := e.sess.sourceForeignContestedLocked(dur, 11)
			e.sess.mu.Unlock()
			if kept == nil || !kept.regForeign || !contested {
				t.Fatalf("a staged release must KEEP the quarantine when %s; the next resume re-observes it", tc.what)
			}
		})
	}
}

// The sweep RETAINS a verdict-bearing register it cannot durably settle, and deletes
// one it can — with proof, releasing the verdict against that proof. A retained
// candidate must also report no mutation, or a stable prefix of them consumes the
// per-tick budget and starves every actionable candidate behind it.
func TestMQTTInflightSweepRetainsAVerdictItCannotForget(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-sweep-retention")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	rec, smsg := testRegisterAt(t, e, pi)
	nonce := e.nonceOf(pi)
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	e.sess.mu.Unlock()

	name := &mqttInflightName{
		idHash: e.sess.idHash, gen: e.sess.gen, pi: pi,
		subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, pi),
	}
	carrying := *rec
	carrying.Retired, carrying.Cancelled = true, true

	// NO resident, identity-fenced session: nothing here can forget the verdict, so the
	// register — the verdict's only carrier — must survive.
	e.asm.mu.Lock()
	delete(e.asm.sessByHash, e.sess.idHash)
	e.asm.mu.Unlock()
	if e.asm.disposeSettledAffineRegister(name, &carrying, smsg.Sequence) {
		t.Fatal("a retained candidate must report no mutation, or it charges the sweep's budget for doing nothing")
	}
	if got, _ := e.loadRegister(t, pi); got == nil {
		t.Fatal("deleting the register leaves the verdict with nothing to identify it by, for the life of the generation")
	}

	// With the session resident and its generation matching, the same disposal deletes
	// with proof and releases the verdict.
	e.asm.mu.Lock()
	e.asm.sessByHash[e.sess.idHash] = e.sess
	e.asm.mu.Unlock()
	if !e.asm.disposeSettledAffineRegister(name, &carrying, smsg.Sequence) {
		t.Fatal("a settleable register must be disposed of, and the mutation charged")
	}
	if got, _ := e.loadRegister(t, pi); got != nil {
		t.Fatal("the register must be gone")
	}
	e.sess.mu.Lock()
	_, held := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if held {
		t.Fatal("the verdict must be released against the proof of the deletion")
	}
}

// A source that has been re-offered under a SECOND identifier must not veto the release
// of the first one's quarantine.
//
// The rival consumed the quarantined register, the source was legitimately re-offered
// under another identifier, and recovery installs that newer entry — remapping the source
// — before the commit runs. Reading that difference as a fence mismatch would refuse the
// release at every resume forever, while nothing on the live connection ever retries it:
// the identifier stays reserved and counted, and at Receive Maximum 1 the session stops
// delivering entirely.
func TestMQTTInflightStagedReleaseIsNotVetoedByANewerIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-staged-cross-pi")
	dur := e.sess.idHash + "_plain"
	key := mqttDeferredKey{dur, 51}
	const oldPI, newPI = 3, 4

	e.sess.mu.Lock()
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	// The quarantine on the OLD identifier, and the recovered live entry on the NEW one.
	e.sess.pendingPublish[oldPI] = &mqttPending{jsDur: dur, sseq: 51, regSeq: 6, regForeign: true}
	e.sess.pendingPublish[newPI] = &mqttPending{jsDur: dur, sseq: 51, regSeq: 12, regNonce: mqttNewInflightNonce()}
	e.sess.cpending = map[string]map[uint64]uint16{dur: {51: newPI}}
	e.sess.foreignContested = map[mqttDeferredKey]*mqttForeignContest{
		key: {pi: oldPI, subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, oldPI), regSeq: 6, walk: 1, reserved: true},
	}
	e.sess.mu.Unlock()

	e.sess.commitStagedReleases(e.c, []mqttStagedRelease{{key: key, pi: oldPI, regSeq: 6}})

	e.sess.mu.Lock()
	oldEntry := e.sess.pendingPublish[oldPI]
	newEntry := e.sess.pendingPublish[newPI]
	mapped := e.sess.cpending[dur][51]
	contested := e.sess.sourceForeignContestedLocked(dur, 51)
	e.sess.mu.Unlock()

	if oldEntry != nil || contested {
		t.Fatalf("the old identifier's quarantine must be released: entry=%+v contested=%v", oldEntry, contested)
	}
	if newEntry == nil || newEntry.regSeq != 12 {
		t.Fatalf("the newer allocation must be untouched, got %+v", newEntry)
	}
	if mapped != newPI {
		t.Fatalf("the source mapping belongs to the newer identifier and must stand: cpending=%d want %d", mapped, newPI)
	}
}

// The send section holds the identifier for as long as the write does — including
// across a session discard, which resets the identifier counter to zero.
func TestMQTTInflightSendSectionHoldsTheIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-section")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok || token == _EMPTY_ {
		t.Fatal("a confirmed entry on this session's connection must be reservable")
	}

	// A second section for the same identifier is refused while the first is open.
	e.sess.mu.Lock()
	_, twice := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if twice {
		t.Fatal("two send sections for one identifier would put two PUBLISHes on the wire under it")
	}

	// And refused even when the ENTRY says nothing, which is the case the registry
	// exists for: a successful PUBREC replaces the PUBLISH entry while the write is
	// still out, so a re-installed entry on this identifier carries no section of its
	// own. Only the registry knows the identifier is still being written.
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq}
	_, afterReplace := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if afterReplace {
		t.Fatal("the REGISTRY, not the entry, is what holds an identifier whose write has not returned")
	}

	// The registry, not the entry, is what bumpPI consults — so the identifier survives
	// a discard that nils every map and resets the counter.
	e.sess.mu.Lock()
	e.sess.pendingPublish = nil
	e.sess.pendingPubRel = nil
	e.sess.cpending = nil
	e.sess.last_pi = 0
	e.sess.pendingPublish = make(map[uint16]*mqttPending)
	e.sess.pendingPubRel = make(map[uint16]*mqttPending)
	e.sess.cpending = make(map[string]map[uint64]uint16)
	minted := make(map[uint16]bool)
	for i := 0; i < int(pi)+2; i++ {
		next := e.sess.bumpPI()
		if next == 0 {
			break
		}
		minted[next] = true
		e.sess.pendingPublish[next] = &mqttPending{}
	}
	e.sess.mu.Unlock()
	if minted[pi] {
		t.Fatalf("packet identifier %d was re-minted while its write was still in flight", pi)
	}

	e.sess.endSend(e.c, pi, token)
	e.sess.mu.Lock()
	_, stillReserved := e.sess.sendsInFlight[pi]
	e.sess.mu.Unlock()
	if stillReserved {
		t.Fatal("endSend must release the registry entry, or the identifier is held for the session's life")
	}
}

// A terminal acknowledgement arriving mid-write PARKS its identifier-freeing half. The
// protocol answer is unaffected; the untrack and the register delete wait for the write.
func TestMQTTInflightTerminalAckParksWhileASendIsInFlight(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-park")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	parked := ok && e.sess.condemnSendLocked(pi, mqttSendSettleAckComplete)
	entry := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if !ok || !parked {
		t.Fatal("a removal path meeting an active send section must condemn the entry rather than delete it")
	}
	if entry == nil || entry.sendSettle.kind != mqttSendSettleAckComplete {
		t.Fatalf("the parked disposition must name the continuation it owes: %+v", entry)
	}

	// A SECOND remover does its own durable work but neither replaces the parked
	// disposition nor executes it early.
	e.sess.mu.Lock()
	e.sess.condemnSendLocked(pi, mqttSendSettlePurgeTeardown)
	kind := e.sess.pendingPublish[pi].sendSettle.kind
	resident := e.sess.pendingPublish[pi] != nil
	e.sess.mu.Unlock()
	if kind != mqttSendSettleAckComplete {
		t.Fatalf("the FIRST parked settle wins, got %v", kind)
	}
	if !resident {
		t.Fatal("the identifier must stay resident until the write settles, or bumpPI can re-mint it")
	}

	e.sess.endSend(e.c, pi, token)
	e.sess.mu.Lock()
	after := e.sess.pendingPublish[pi]
	_, stillReserved := e.sess.sendsInFlight[pi]
	e.sess.mu.Unlock()
	if after != nil || stillReserved {
		t.Fatalf("endSend must complete the parked continuation: entry=%+v reserved=%v", after, stillReserved)
	}
	if rec, _ := e.loadRegister(t, pi); rec != nil {
		t.Fatal("the acknowledgement's deferred register delete must run at endSend")
	}
}

// The real acknowledgement path parks, not just the primitive: a PUBACK arriving while
// this entry's transmission is still out leaves the identifier resident and its register
// standing until the write returns.
func TestMQTTInflightPubAckParksThroughTheAckPath(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-puback-park")
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the entry must be reservable")
	}

	if err := e.c.mqttProcessPubAck(pi); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	e.sess.mu.Lock()
	entry := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if entry == nil {
		t.Fatal("a PUBACK that freed the identifier mid-write lets bumpPI re-mint it under the blocked PUBLISH")
	}
	if entry.sendSettle.kind != mqttSendSettleAckComplete {
		t.Fatalf("the acknowledgement must park its freeing half: %+v", entry.sendSettle)
	}
	if rec, _ := e.loadRegister(t, pi); rec == nil {
		t.Fatal("the register delete belongs to the parked continuation, not to the acknowledgement")
	}

	e.sess.endSend(e.c, pi, token)
	e.sess.mu.Lock()
	after := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if after != nil {
		t.Fatal("endSend must complete the acknowledgement's freeing half")
	}
	if rec, _ := e.loadRegister(t, pi); rec != nil {
		t.Fatal("endSend must issue the deferred register delete")
	}
}

// Every JETSTREAM-BACKED QoS 1/2 PUBLISH transmission is inside a send section — asserted
// STRUCTURALLY, over the source, because the property is about the call sites rather than
// about one execution.
//
// Two identifier-bearing transmissions are outside this guard, both filed rather than
// glossed. The PUBREL transmission does not reserve its identifier (F-089). And a
// RETAINED QoS 1/2 message allocates an identifier through trackPublishRetained, builds
// its frame directly, and is flushed by a later queueOutbound with no section at all
// (F-091) — this guard cannot see it because it matches the delivery callbacks' enqueue
// helper, and the retained path does not use it. Named here so the scope reads as two
// known holes rather than as coverage.
//
// A behavioral test can only cover the sites it happens to drive, and this is exactly
// the class of defect that reappears at whichever site the last review did not name: a
// new delivery path that enqueues a PUBLISH under an allocated identifier without
// reserving it re-opens the stale-send race in full. The identifier-carrying enqueues
// are the closed set here; the QoS 0 ones (pi == 0) allocate nothing and are excluded by
// the same rule that keeps retained sends outside the register invariants (R6).
func TestMQTTInflightEveryQoS12SendIsReserved(t *testing.T) {
	t.Parallel()
	for _, file := range []string{"mqtt.go", "qos2_shared_affine.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		lines := strings.Split(string(src), "\n")
		found := 0
		for i, line := range lines {
			if !strings.Contains(line, "mqttEnqueuePublishMsgTo(cc, sub, pi,") {
				continue // a QoS 0 enqueue, or the definition itself
			}
			found++
			start := max(0, i-40)
			window := strings.Join(lines[start:i], "\n")
			if !strings.Contains(window, "beginSendLocked(cc, pi,") {
				t.Errorf("%s:%d enqueues a QoS 1/2 PUBLISH outside a send section: the identifier can be "+
					"freed and re-minted while the write is blocked", file, i+1)
			}
			if !strings.Contains(window, "defer sess.endSend(cc, pi,") {
				t.Errorf("%s:%d reserves the identifier without arming endSend with defer: an early return "+
					"or a recovered panic would hold it for the connection's life", file, i+1)
			}
		}
		if found == 0 {
			t.Errorf("%s: no identifier-carrying enqueue was found, so this guard checks nothing", file)
		}
	}
}

// A NAK is withheld from every entry a redelivery could not help, and each state is
// asserted on its own so deleting any one clause fails.
func TestMQTTInflightRedeliveryWithheldFromUnsendableEntries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		what  string
		entry *mqttPending
		want  bool
	}{
		{"a confirmed, idle entry", &mqttPending{regSeq: 4}, true},
		{"an allocation whose register is not confirmed (R1)", &mqttPending{regPending: true}, false},
		{"a quarantined identifier", &mqttPending{regSeq: 4, regForeign: true}, false},
		{"an entry whose transmission has not returned", &mqttPending{regSeq: 4, sendNonce: "tok"}, false},
	} {
		if got := mqttPendingRedeliverable(tc.entry); got != tc.want {
			t.Errorf("%s: redeliverable=%v, want %v", tc.what, got, tc.want)
		}
	}
}

// A subscribe denial must not take a quarantined identifier: its register is another
// writer's, so freeing the entry would leave the durable custody unrepresented and let
// the source's next offer mint a second identifier.
func TestMQTTInflightDenialLeavesAQuarantineAlone(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-denial-quarantine")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	e.sess.pendingPublish[5] = &mqttPending{jsDur: dur, sseq: sseq, regSeq: 3, regForeign: true}
	e.sess.cpending = map[string]map[uint64]uint16{dur: {sseq: 5}}
	e.sess.mu.Unlock()

	e.sess.releaseTransmittedIdentifier(dur, testInflightAckSubject(dur, sseq, 1))

	e.sess.mu.Lock()
	held := e.sess.pendingPublish[5]
	mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if held == nil || !held.regForeign || mapped != 5 {
		t.Fatalf("a terminal denial must leave a quarantine standing: entry=%+v cpending=%d", held, mapped)
	}
}

// A same-delivery occupant that is RETIRED is adopted, never replaced: it proves this
// identifier's exchange for this very source is past PUBREC, so replacing it would erase
// the client's own PUBREC and re-send a completed payload — while abandoning the
// allocation would let the source's next offer take a second identifier.
func TestMQTTInflightTakeoverAdoptsARetiredOccupant(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-takeover-adopt")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	// The occupant: same source, ANOTHER writer's nonce, and retired.
	occupantNonce := mqttNewInflightNonce()
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) {
		r.Retired, r.Nonce = true, occupantNonce
	})
	_, occupantMsg := testRegisterAt(t, e, pi)

	// Our own fresh allocation for the same source and identifier.
	n2 := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq, regPending: true, regNonce: n2}
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, n2, _EMPTY_, _EMPTY_) {
		t.Fatal("an allocation meeting a retired occupant must not be cleared to send")
	}

	got, gotMsg := testRegisterAt(t, e, pi)
	if gotMsg.Sequence != occupantMsg.Sequence || got.Nonce != occupantNonce || !got.Retired {
		t.Fatalf("the retired occupant must be adopted, not rewritten: seq=%d nonce=%q retired=%v",
			gotMsg.Sequence, got.Nonce, got.Retired)
	}
	e.sess.mu.Lock()
	rs := e.sess.srcAckOwed[mqttDeferredKey{dur, sseq}]
	e.sess.mu.Unlock()
	if rs == nil || rs.nonce != occupantNonce || !rs.durable {
		t.Fatalf("the adoption must reinstate the OCCUPANT's suppression: %+v", rs)
	}

	// The point of installing the suppression rather than abandoning: the source's next
	// offer meets sourceRetiredLocked — which the delivery callback consults BEFORE
	// trackPublish — so it is acknowledged and discarded instead of allocating a second
	// identifier for a message the client has already completed.
	e.sess.mu.Lock()
	suppressed := e.sess.sourceRetiredLocked(dur, sseq)
	e.sess.mu.Unlock()
	if !suppressed {
		t.Fatal("the adopted retirement must answer the source's next offer, or it allocates a second identifier")
	}
}

// The adoption is GENERATION-FENCED: a discard that ran while the reconcile was outside
// the lock has minted a new incarnation, and a late install would write the retiring
// generation's suppression into the reused object.
func TestMQTTInflightTakeoverAdoptionIsGenerationFenced(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-takeover-fence")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	occupantNonce := mqttNewInflightNonce()
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) {
		r.Retired, r.Nonce = true, occupantNonce
	})

	n2 := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq, regPending: true, regNonce: n2}
	e.sess.mu.Unlock()

	// The generation is re-minted while the store's CAS is in flight, which is exactly
	// the window a discard occupies.
	bumped := 0
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if bumped == 0 && r.Nonce == n2 {
			bumped++
			e.sess.mu.Lock()
			e.sess.gen = mqttNewInflightNonce()
			e.sess.mu.Unlock()
			return 0, fmt.Errorf("test: the store did not land")
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, n2, _EMPTY_, _EMPTY_)
	e.asm.mu.Lock()
	e.asm.testInflightStore = nil
	e.asm.mu.Unlock()
	if bumped != 1 {
		t.Fatalf("the re-mint window was never entered, bumped=%d", bumped)
	}

	e.sess.mu.Lock()
	rs := e.sess.srcAckOwed[mqttDeferredKey{dur, sseq}]
	e.sess.mu.Unlock()
	if rs != nil {
		t.Fatalf("the retiring generation's suppression was installed into the new incarnation: %+v", rs)
	}
}

// A session discard does not release an identifier whose write is still blocked. clear()
// nils every tracking map AND resets the counter to zero, so a reused session object
// would otherwise mint the same identifier for a new incarnation while the displaced
// connection's write is still in the socket.
func TestMQTTInflightSendSectionSurvivesTheDiscard(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-discard")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the entry must be reservable")
	}

	// A quarantine is standing when the discard runs, so the assertion below is about
	// this map being CLEARED rather than about it having been empty all along.
	e.sess.mu.Lock()
	e.sess.foreignContested = map[mqttDeferredKey]*mqttForeignContest{
		{dur, sseq + 1}: {pi: pi + 1, subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, pi+1), regSeq: 3, reserved: true},
	}
	e.sess.mu.Unlock()

	if err := e.sess.clear(false); err != nil {
		t.Fatalf("clear: %v", err)
	}

	e.sess.mu.Lock()
	e.sess.maxp = 100
	e.sess.pendingPublish = make(map[uint16]*mqttPending)
	minted := make(map[uint16]bool)
	for i := 0; i < int(pi)+2; i++ {
		next := e.sess.bumpPI()
		if next == 0 {
			break
		}
		minted[next] = true
		e.sess.pendingPublish[next] = &mqttPending{}
	}
	quarantines := len(e.sess.foreignContested)
	e.sess.mu.Unlock()
	if minted[pi] {
		t.Fatalf("the new incarnation minted packet identifier %d while the displaced connection's write was still out", pi)
	}
	if quarantines != 0 {
		t.Fatal("quarantines belong to the retiring incarnation's registers and must not cross the discard")
	}

	e.sess.endSend(e.c, pi, token)
	e.sess.mu.Lock()
	_, stillReserved := e.sess.sendsInFlight[pi]
	e.sess.mu.Unlock()
	if stillReserved {
		t.Fatal("on nilled maps the registry removal IS the whole continuation, and it must still happen")
	}
}

// A resume decides only the quarantines its own walk COVERED, and that eligibility is
// judged BEFORE any disposition is consulted.
//
// The second axis is the one that matters and the one a single-arm test misses: when the
// walk DID route this source — from the earlier register it read — that result describes
// a contest this quarantine is not part of. Applying it would dispose of the quarantine
// on evidence predating it, freeing the identifier while the rival register still
// stands, so a later offer for the same source transmits under a second identifier.
func TestMQTTInflightReconciliationDecidesOnlyWhatItsWalkCovered(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-walk-identity")
	dur := e.sess.idHash + "_plain"
	for _, tc := range []struct {
		what      string
		installed uint64
		routed    bool
		wantStage bool
	}{
		{"installed BEFORE this walk, subject enumerated empty", 4, false, true},
		{"installed DURING this walk, no result for its source", 5, false, false},
		{"installed DURING this walk, but the walk ROUTED that source from an EARLIER register",
			5, true, false},
		{"installed BEFORE this walk, and the walk routed the source closed", 4, true, true},
	} {
		key := mqttDeferredKey{dur, 21}
		final := map[mqttDeferredKey]mqttContestDisposition{}
		if tc.routed {
			// A Closed disposition for the SOURCE, reached from the register this walk
			// actually read — which is not the register the quarantine is contesting.
			final[key] = mqttContestDisposition{
				class: mqttContestClosed,
				reg: mqttRecoveredInflightReg{
					rec: &mqttInflightRecord{PI: 7, Dur: dur, Seq: 21}, seq: 3,
				},
			}
		}
		e.sess.mu.Lock()
		e.sess.inflightWalks = 5
		if e.sess.pendingPublish == nil {
			e.sess.pendingPublish = make(map[uint16]*mqttPending)
		}
		e.sess.pendingPublish[7] = &mqttPending{jsDur: dur, sseq: 21, regSeq: 3, regForeign: true}
		e.sess.foreignContested = map[mqttDeferredKey]*mqttForeignContest{
			key: {pi: 7, subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, 7), regSeq: 3, walk: tc.installed, reserved: true},
		}
		staged := e.sess.reconcileForeignContestsLocked(e.c, final, map[uint16]bool{}, 5, func(mqttStoredPubRel) {})
		held := e.sess.pendingPublish[7]
		contested := e.sess.sourceForeignContestedLocked(dur, 21)
		e.sess.mu.Unlock()
		if got := len(staged) == 1; got != tc.wantStage {
			t.Errorf("%s: staged=%d, want stage=%v", tc.what, len(staged), tc.wantStage)
		}
		if held == nil || !held.regForeign {
			t.Errorf("%s: the reconciliation must never APPLY a release, only stage one", tc.what)
		}
		if !tc.wantStage && !contested {
			t.Errorf("%s: an ineligible quarantine must be kept whole, contest included", tc.what)
		}
	}
}

// The disposition index is keyed by the SOURCE, and a source is not an identifier. A
// record the walk recovered on ANOTHER identifier for the same delivery must not be
// installed into this quarantine's entry — that would leave a redelivery transmitting
// under this identifier while the durable register and its reservation sit on the other.
func TestMQTTInflightReconciliationRefusesEvidenceForAnotherIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-cross-pi")
	dur := e.sess.idHash + "_plain"
	key := mqttDeferredKey{dur, 31}
	const quarantinedPI, otherPI = 7, 9

	for _, tc := range []struct {
		what string
		d    mqttContestDisposition
	}{
		{"an owned-live register recovered on another identifier", mqttContestDisposition{
			class: mqttContestOwnedLive,
			reg: mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: otherPI, Dur: dur, Seq: 31, Nonce: mqttNewInflightNonce()},
				seq: 12,
			},
		}},
		{"a retirement recovered on another identifier", mqttContestDisposition{
			class: mqttContestRetired,
			reg: mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: otherPI, Dur: dur, Seq: 31, Retired: true}, seq: 12,
			},
		}},
		{"a PUBREL record owning another identifier", mqttContestDisposition{
			class: mqttContestPubRelOwned,
			rel:   mqttStoredPubRel{pi: otherPI, srcDur: dur, srcSeq: 31},
		}},
	} {
		e.sess.mu.Lock()
		e.sess.inflightWalks = 9
		if e.sess.pendingPublish == nil {
			e.sess.pendingPublish = make(map[uint16]*mqttPending)
		}
		e.sess.pendingPubRel = make(map[uint16]*mqttPending)
		e.sess.pendingPublish[quarantinedPI] = &mqttPending{jsDur: dur, sseq: 31, regSeq: 4, regForeign: true}
		e.sess.foreignContested = map[mqttDeferredKey]*mqttForeignContest{
			key: {pi: quarantinedPI, subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, quarantinedPI), regSeq: 4, walk: 8, reserved: true},
		}
		installed := 0
		// The walk DID enumerate this identifier's subject, so the fallback cannot
		// release it either: the only question under test is the cross-identifier one.
		staged := e.sess.reconcileForeignContestsLocked(e.c,
			map[mqttDeferredKey]mqttContestDisposition{key: tc.d},
			map[uint16]bool{quarantinedPI: true}, 9,
			func(mqttStoredPubRel) { installed++ })
		held := e.sess.pendingPublish[quarantinedPI]
		contested := e.sess.sourceForeignContestedLocked(dur, 31)
		e.sess.mu.Unlock()

		if held == nil || !held.regForeign || held.regSeq != 4 {
			t.Errorf("%s: the quarantine must be untouched, got %+v", tc.what, held)
		}
		if !contested {
			t.Errorf("%s: the contest must survive evidence about another identifier", tc.what)
		}
		if len(staged) != 0 {
			t.Errorf("%s: nothing may be staged on evidence for another identifier, got %+v", tc.what, staged)
		}
		if installed != 0 {
			t.Errorf("%s: no PUBREL reservation may be installed for another identifier", tc.what)
		}
	}
}

// A retirement for THIS identifier frees it — and freeing is staged, never applied by
// the walk, because a resume refused after recovery preserves the session's maps.
func TestMQTTInflightRetiredQuarantineReleaseIsStaged(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-retired-stage")
	dur := e.sess.idHash + "_plain"
	key := mqttDeferredKey{dur, 41}
	e.sess.mu.Lock()
	e.sess.inflightWalks = 3
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	e.sess.pendingPublish[6] = &mqttPending{jsDur: dur, sseq: 41, regSeq: 8, regForeign: true}
	e.sess.foreignContested = map[mqttDeferredKey]*mqttForeignContest{
		key: {pi: 6, subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, 6), regSeq: 8, walk: 2, reserved: true},
	}
	staged := e.sess.reconcileForeignContestsLocked(e.c,
		map[mqttDeferredKey]mqttContestDisposition{key: {
			class: mqttContestRetired,
			reg: mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: 6, Dur: dur, Seq: 41, Retired: true}, seq: 8,
			},
		}},
		map[uint16]bool{6: true}, 3, func(mqttStoredPubRel) {})
	stillHeld := e.sess.pendingPublish[6]
	e.sess.mu.Unlock()

	if len(staged) != 1 || staged[0].pi != 6 {
		t.Fatalf("a retirement for this identifier must STAGE its release, got %+v", staged)
	}
	if stillHeld == nil || !stillHeld.regForeign {
		t.Fatal("the walk must not free the identifier itself: a resume refused after recovery keeps these maps")
	}
	e.sess.commitStagedReleases(e.c, staged)
	e.sess.mu.Lock()
	after := e.sess.pendingPublish[6]
	contested := e.sess.sourceForeignContestedLocked(dur, 41)
	e.sess.mu.Unlock()
	if after != nil || contested {
		t.Fatalf("the committed release must free the identifier and the contest: entry=%+v contested=%v", after, contested)
	}
}

// The verdict property is guarded STRUCTURALLY, over the source of every production file
// in this package: a register delete whose caller can name the exchange nonce AND can
// reach the verdict set must route through reapRegisterWithVerdict, so a NEW delete site
// fails the suite instead of silently rejoining the class that strands verdicts.
//
// The second clause is not decoration. The verdict set lives on a session, so a caller
// with none resident can consult nothing however well it knows the nonce — which is
// F-090, filed, and why one allow-list entry below reads as it does.
//
// Three separate reviews each found one more stranding delete site — the affine sweep,
// the store's stale-record reap, recovery's second retirement drop — which is what makes
// a list the wrong shape for this and an enforced boundary the right one. The allow-list
// below is that boundary made explicit: every entry is a site that CANNOT name a nonce
// (or IS the property's own implementation), and adding to it is a deliberate act a
// reviewer can see.
func TestMQTTInflightEveryNonceRecoverableDeleteRoutesThroughTheProperty(t *testing.T) {
	t.Parallel()
	// site -> why it is outside the property. Nonce-UNRECOVERABLE deletes only.
	//
	// THREE shapes are outside it. A site that CANNOT name a nonce. A site that
	// implements the same discipline with a STRONGER ladder of its own — confirmed
	// delete, else a tombstone on the register, else the nonce verdict — which is not a
	// bypass but the full version of what the helper does in one step. And a site that
	// can name the nonce but cannot REACH the verdict set, which lives on a session:
	// the orphan sweep with none resident, which is F-090.
	allowed := map[string]string{
		"reapRegisterWithVerdict":          "the property's own implementation",
		"deleteInflightRegister":           "the fire-and-forget transport primitive the property calls",
		"deleteInflightRegisterConfirmed":  "the confirmed transport primitive the property calls",
		"deleteInflightRegistersPipelined": "the pipelined transport primitive over sequences already collected",
		"voidRegistersOnStream":            "the teardown's own three-rung ladder: confirmed delete then tombstone then verdict, forgetting on each proven rung",
		"confirmInflightRegister":          "the cancelled-allocation settle, which runs that same three-rung ladder for one register",
		"reapIncarnationRegisters":         "the generation reap: the whole incarnation goes, verdicts with it",
		"reconcileOrphanInflight":          "the account-wide sweep's candidate router, which holds no session to consult",
		"disposeSettledAffineRegister": "routes through the property where an identity-fenced session is resident, " +
			"and RETAINS a register that CARRIES a cancellation where none is. A verdict living only in a " +
			"non-resident session's persisted CancelledRegs is invisible from there and is NOT covered: F-090",
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		checked++
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			ast.Inspect(fn.Body, func(inner ast.Node) bool {
				call, ok := inner.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "deleteInflightRegister", "deleteInflightRegisterConfirmed":
				case "deleteMsg":
					// Only the register stream's deletes are in this class.
					if len(call.Args) == 0 {
						return true
					}
					id, ok := call.Args[0].(*ast.Ident)
					if !ok || id.Name != "mqttInflightStreamName" {
						return true
					}
				default:
					return true
				}
				if _, ok := allowed[fn.Name.Name]; !ok {
					t.Errorf("%s: %s deletes a register directly. If it can name the exchange nonce it must "+
						"route through reapRegisterWithVerdict, or a cancellation verdict naming that register "+
						"is stranded for the life of the session; if it genuinely cannot, add it to the "+
						"allow-list with the reason", fset.Position(call.Pos()), fn.Name.Name)
				}
				return true
			})
			return false
		})
	}
	if checked < 5 {
		t.Fatalf("only %d production files were parsed; this guard would pass vacuously", checked)
	}
}

// beginSendLocked refuses every entry state that must not be transmitted.
func TestMQTTInflightBeginSendRefusesUnsendableEntries(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-refuse")
	dur := e.sess.idHash + "_plain"
	e.sess.mu.Lock()
	e.sess.c = e.c
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	defer e.sess.mu.Unlock()
	for _, tc := range []struct {
		what  string
		entry *mqttPending
	}{
		{"an allocation whose register is not confirmed (R1)", &mqttPending{jsDur: dur, sseq: 1, regPending: true}},
		{"a withdrawn allocation", &mqttPending{jsDur: dur, sseq: 1, regCancelled: true}},
		{"a quarantined identifier", &mqttPending{jsDur: dur, sseq: 1, regForeign: true}},
		{"an entry whose removal is already parked", &mqttPending{
			jsDur: dur, sseq: 1, sendSettle: mqttSendSettle{kind: mqttSendSettleAckComplete},
		}},
	} {
		e.sess.pendingPublish[9] = tc.entry
		if _, ok := e.sess.beginSendLocked(e.c, 9, dur, 1); ok {
			t.Errorf("a send section was granted over %s", tc.what)
		}
		delete(e.sess.sendsInFlight, 9)
	}
}
