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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// T36's LIFECYCLE rows (F-088 plan rev 20, consult C6 rows 1-17): every cancellation runs
// through the production securing path (secureRegCancellationsClassified) inside its packet's
// record transaction, with HEAD's verdict lifecycle unchanged — the early forgetVoidedReg after
// the fence lands included; assertions are on durable state read back from the store, the custody
// ledger, the identifier exclusion and route witnesses. No record-state, instance or
// retirement-proof assertion exists.
func t36LifecycleRows() []t36Row {
	return []t36Row{
		{"01-pre-write-cancellation", t36Row01PreWriteCancellation},
		{"02-fence-wins", t36Row02FenceWins},
		{"03-admission-wins", t36Row03AdmissionWins},
		{"04-ordinary-unresolved-handle", t36Row04UnresolvedHandle},
		{"05-affine-paused-write", t36Row05AffinePausedWrite},
		{"06-affine-ambiguous-return", t36Row06AffineAmbiguousReturn},
		{"07-failed-occupant-marking-then-crash", t36Row07FailedOccupantMarkingThenCrash},
		{"08-both-completion-orders", t36Row08BothCompletionOrders},
		{"09-disposal-continuation-identity", t36Row09DisposalContinuationIdentity},
		{"10-resident-reconnect", t36Row10ResidentReconnect},
		{"11-cold-takeover", t36Row11ColdTakeover},
		{"12-generation-change", t36Row12GenerationChange},
		{"13-successor-preservation", t36Row13SuccessorPreservation},
		{"14-uncovered-verdict-does-not-block-disposal", t36Row14UncoveredVerdict},
		{"15-recovery-disposes-without-coverage-obligation", t36Row15RecoveryWithoutCoverage},
		{"16-uncancelled-admission", t36Row16UncancelledAdmission},
		{"17-crash-after-disposal-no-verdict", t36Row17CrashAfterDisposalNoVerdict},
	}
}

// t36Fix is one ordinary QoS 1 allocation, tracked as a delivery callback tracks it, with its
// admission's prerequisites standing.
type t36Fix struct {
	e       *testInflightEnv
	dur     string
	sseq    uint64
	pi      uint16
	nonce   string
	gen     string
	subject string
	qos     byte
}

func newT36Fix(t *testing.T, clientID string) *t36Fix {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	f := &t36Fix{e: e, dur: e.sess.idHash + "_" + nuid.Next(), qos: 1}
	f.sseq = e.publishSource(t, f.dur)
	e.admissionPrerequisites(t, f.dur, f.qos)
	e.acceptUnderServingAuthority(t)
	f.track(t)
	return f
}

// track allocates (or re-allocates) the delivery's identifier.
func (f *t36Fix) track(t *testing.T) {
	t.Helper()
	f.e.sess.mu.Lock()
	pi, isNew := f.e.sess.trackPublish(f.dur, testInflightAckSubject(f.dur, f.sseq, 1))
	var nonce string
	if p := f.e.sess.pendingPublish[pi]; p != nil {
		nonce = p.regNonce
	}
	f.gen = f.e.sess.gen
	f.e.sess.mu.Unlock()
	if pi == 0 || !isNew || nonce == _EMPTY_ {
		t.Fatalf("trackPublish: pi=%d isNew=%v nonce=%q", pi, isNew, nonce)
	}
	f.pi, f.nonce = pi, nonce
	f.subject = mqttInflightSubject(f.e.sess.idHash, f.gen, pi)
}

// cancel is the consumer teardown's cancellation through the PRODUCTION securing path, inside a
// record transaction of its own packet. It returns what the teardown reported.
func (f *t36Fix) cancel(t *testing.T) []mqttRegCancellation {
	t.Helper()
	f.e.sess.mu.Lock()
	_, cancels := f.e.sess.purgeConsumerPendingLocked(f.dur, mqttTestLicenceLocked(f.e.sess, nil))
	f.e.sess.mu.Unlock()
	tok := f.e.sess.beginRecordTx()
	f.e.sess.secureRegCancellationsClassified(f.e.c, tok, cancels, mqttLicenceOf(f.e.c))
	if err := f.e.sess.endRecordTx(tok); err != nil {
		t.Errorf("closing the cancelling packet's record transaction: %v", err)
	}
	return cancels
}

func (f *t36Fix) admit() bool {
	return f.e.sess.admitInflightPublish(f.e.c, f.pi, f.qos, f.dur, f.sseq, f.nonce, _EMPTY_, _EMPTY_)
}

func (f *t36Fix) register(t *testing.T) (*mqttInflightRecord, *StoredMsg) {
	t.Helper()
	rec, smsg, err := f.e.asm.jsa.loadInflightRecord(f.subject)
	if err != nil {
		t.Fatalf("reading %q: %v", f.subject, err)
	}
	return rec, smsg
}

// exchange is the allocation's exchange (a create's lineage is its nonce), nil when absent.
func (f *t36Fix) exchange() *mqttOutboundExchange {
	x, _, _ := f.e.asm.jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.nonce)
	return x
}

func (f *t36Fix) fenceStands(t *testing.T) (bool, uint64) {
	t.Helper()
	rec, smsg := f.register(t)
	if smsg == nil || rec == nil || !mqttValidPendingFence(f.subject, rec) {
		return false, 0
	}
	return true, smsg.Sequence
}

func (f *t36Fix) verdicted() bool {
	f.e.sess.mu.Lock()
	defer f.e.sess.mu.Unlock()
	_, ok := f.e.sess.voidedRegNonces[f.nonce]
	return ok
}

// disposeFence disposes of the standing fence through the one door, by provenance alone.
func (f *t36Fix) disposeFence(t *testing.T) {
	t.Helper()
	rec, smsg := f.register(t)
	if smsg == nil || !mqttValidPendingFence(f.subject, rec) {
		t.Fatalf("fixture: no pending fence stands on %q (%+v)", f.subject, rec)
	}
	if out := f.e.sess.reapLoadedRegister(f.subject, smsg.Sequence, rec, mqttSlotDeletePendingFence,
		mqttLicenceOf(f.e.c), nil); out != mqttReapDeleted {
		t.Fatalf("the fence must be disposed of by its provenance alone, got %v", out)
	}
}

// independent lands ANOTHER delivery's admission on another source, whose custody and register
// every row requires preserved.
func (f *t36Fix) independent(t *testing.T) (uint16, string) {
	t.Helper()
	dur := f.e.sess.idHash + "_" + nuid.Next()
	sseq := f.e.publishSource(t, dur)
	pi := f.e.trackAdmitted(t, dur, sseq, 1)
	return pi, f.e.nonceOf(pi)
}

func (f *t36Fix) independentKept(t *testing.T, pi uint16, nonce string) {
	t.Helper()
	if !testCustodyHasExchange(f.e.sess, nonce) {
		t.Errorf("the independent delivery %d lost its custody charge", pi)
	}
	if rec, _ := f.e.loadRegister(t, pi); rec == nil || rec.Nonce != nonce {
		t.Errorf("the independent delivery %d lost its register: %+v", pi, rec)
	}
}

// recordDeletes wraps the register delete to record every sequence it is asked to remove.
func (f *t36Fix) recordDeletes() func() []uint64 {
	var mu sync.Mutex
	var seqs []uint64
	f.e.asm.mu.Lock()
	f.e.asm.testInflightDelete = func(seq uint64) error {
		mu.Lock()
		seqs = append(seqs, seq)
		mu.Unlock()
		return f.e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	f.e.asm.mu.Unlock()
	return func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]uint64(nil), seqs...)
	}
}

func t36Contains(seqs []uint64, s uint64) bool {
	for _, x := range seqs {
		if x == s {
			return true
		}
	}
	return false
}

// coldResumeUnsaved restores a session object from whatever the session record already holds —
// no save first: a process that stopped — and runs its recovery.
func (f *t36Fix) coldResumeUnsaved(t *testing.T) *mqttSession {
	t.Helper()
	restored, _, err := f.e.asm.createOrRestoreSession(f.e.sess.id, f.e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if restored.gen != f.gen {
		t.Fatalf("fixture: the stored record must name this generation, %q vs %q", restored.gen, f.gen)
	}
	if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	return restored
}

// Row 1: the cancellation lands after custody opened and BEFORE the ladder's entry check; the
// ladder's pre-write refusal runs and the deferred finishAdmissionCustody settles the admission.
func t36Row01PreWriteCancellation(t *testing.T) {
	f := newT36Fix(t, "f088-t36-01")
	ipi, inonce := f.independent(t)
	var reached atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testAdmitReserved = func(s *mqttSession, n string) {
		if n == f.nonce && reached.CompareAndSwap(false, true) {
			f.cancel(t)
		}
	}
	f.e.asm.mu.Unlock()
	if f.admit() {
		t.Fatal("a cancelled allocation must not be admitted")
	}
	if !reached.Load() {
		t.Fatal("fixture: the cancellation must land between the opening and the ladder")
	}
	if testCustodyHasAdmission(f.e.sess, f.nonce) {
		t.Fatal("the admission must be settled: no permanently unsettled placeholder")
	}
	if x := f.exchange(); x != nil {
		t.Fatalf("a pre-write refusal writes no exchange, got %+v", x)
	}
	f.independentKept(t, ipi, inonce)
}

// Row 2: the fence WINS — the admission paused before its authority read, and before its commit.
// The admission's create-only member is refused by the fence, the admission settles never-landed,
// and the fence is disposed of by its exact sequence.
func t36Row02FenceWins(t *testing.T) {
	for _, at := range []string{"before-authority-read", "before-commit"} {
		at := at
		t.Run(at, func(t *testing.T) {
			f := newT36Fix(t, "f088-t36-02-"+at)
			deletes := f.recordDeletes()
			var fseq uint64
			var once atomic.Bool
			pause := func() {
				if !once.CompareAndSwap(false, true) {
					return
				}
				f.cancel(t)
				ok, seq := f.fenceStands(t)
				if !ok {
					t.Error("the securing pass must land a pending fence on the empty subject")
				}
				fseq = seq
			}
			if at == "before-authority-read" {
				f.e.asm.mu.Lock()
				f.e.asm.testAdmitWrite = func(s *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
					pause()
					return write()
				}
				f.e.asm.mu.Unlock()
			} else {
				hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
					if tr.Attempt.Kind == mqttTransitionAdmitExchange {
						pause()
					}
					return send()
				}
				f.e.asm.jsa.testTransitionCommit.Store(&hook)
				t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
			}
			if f.admit() {
				t.Fatal("a cancelled allocation must not be admitted")
			}
			if fseq == 0 {
				t.Fatal("fixture: the pause was never reached")
			}
			if x := f.exchange(); x != nil {
				t.Fatalf("the fence must refuse the admission's create: no exchange may land, got %+v", x)
			}
			if testCustodyHasAdmission(f.e.sess, f.nonce) {
				t.Fatal("the refused admission must settle never-landed")
			}
			// F-119: the cancelling ladder's own disposal is refused while that ladder runs (its
			// custody admission's ladder has not returned), so the fence OUTLIVES the admission ...
			if ok, seq := f.fenceStands(t); !ok || seq != fseq {
				t.Fatalf("the fence must outlive the live ladder at %d, stands=%v seq=%d", fseq, ok, seq)
			}
			if t36Contains(deletes(), fseq) {
				t.Fatalf("nothing may delete the fence while its ladder runs, deletes %v", deletes())
			}
			// ... and once the ladder has returned the one door disposes of it at its exact sequence.
			f.disposeFence(t)
			f.e.waitRegisterCount(t, 0)
			if !t36Contains(deletes(), fseq) {
				t.Fatalf("the fence must be disposed of at its exact sequence %d, deletes %v", fseq, deletes())
			}
		})
	}
}

// Row 3: the ADMISSION WINS — its batch commits first; the cancellation's create then meets the
// landed register and marks it `occupant` (never `pending-create`); the register is withdrawn on
// its standing marker and deleted with confirmation.
func t36Row03AdmissionWins(t *testing.T) {
	f := newT36Fix(t, "f088-t36-03")
	var origin mqttCancelOrigin
	var once atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testAdmitWrite = func(s *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if err == nil && once.CompareAndSwap(false, true) {
			f.cancel(t)
			if rec, _ := f.register(t); rec != nil {
				origin = rec.CancelOrigin
			}
		}
		return out, err
	}
	f.e.asm.mu.Unlock()
	if f.admit() {
		t.Fatal("a cancelled allocation must not be released for sending")
	}
	if !once.Load() {
		t.Fatal("fixture: the admission never committed")
	}
	if origin != mqttCancelOriginOccupant {
		t.Fatalf("the cancellation over the landed register must be an occupant cancellation, got %q", origin)
	}
	if x := f.exchange(); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the landed register must be withdrawn on its standing marker, got %+v", x)
	}
	f.e.waitRegisterCount(t, 0)
}

// t36UnresolvedAdmission makes the allocation's admission UNRESOLVED: its attempt is submitted
// (so custody holds a handle for it) but its batch never leaves and the failure reads as a lost
// response, while the custody resolution is held unresolved. The cancellation lands in that
// window, so the securing pass writes a fence on the still-empty subject. It returns the release
// of the resolution fault.
func t36UnresolvedAdmission(t *testing.T, f *t36Fix) (release func()) {
	t.Helper()
	var fault atomic.Bool
	fault.Store(true)
	var once atomic.Bool
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange && once.CompareAndSwap(false, true) {
			f.cancel(t)
			return 0, errors.New("test: the admission's response was lost")
		}
		return send()
	}
	f.e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if fault.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	f.e.asm.mu.Unlock()
	if f.admit() {
		t.Fatal("a cancelled allocation must not be released for sending")
	}
	if !once.Load() {
		t.Fatal("fixture: the admission's commit was never reached")
	}
	return func() { fault.Store(false) }
}

// Row 4: a lost admission response keeps its custody charge until resolve-abort-resolve settles
// it; the fence's disposal meanwhile releases no charge.
func t36Row04UnresolvedHandle(t *testing.T) {
	f := newT36Fix(t, "f088-t36-04")
	release := t36UnresolvedAdmission(t, f)
	if !testCustodyHasAdmission(f.e.sess, f.nonce) {
		t.Fatal("an unresolved admission must keep its custody charge")
	}
	if ok, _ := f.fenceStands(t); ok {
		f.disposeFence(t)
	}
	testCustodyStaysFor(t, "the charge survives the fence's disposal", 200*time.Millisecond, func() bool {
		return testCustodyHasAdmission(f.e.sess, f.nonce)
	})
	release()
	testCustodyKick(f.e.sess)
	testCustodyEventually(t, "resolve-abort-resolve settles the admission", func() bool {
		return !testCustodyHasAdmission(f.e.sess, f.nonce)
	})
	if x := f.exchange(); x != nil {
		t.Fatalf("a batch that never left lands no exchange, got %+v", x)
	}
}

// t36Affine is an EXPERIMENTAL AFFINE (slot-only) allocation, admitted through its own store path.
type t36Affine struct {
	*t36Fix
	// The LATE PAIR's licence witness: its register sequence, and whether its unsent marker for
	// this allocation stood right after the write; deletes records every register delete.
	mu          sync.Mutex
	lateSeq     uint64
	markerStood bool
	deletes     func() []uint64
}

func newT36Affine(t *testing.T, clientID string) *t36Affine {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	f := &t36Fix{e: e, dur: mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_" + nuid.Next()[:6], qos: 2, sseq: 9}
	f.track(t)
	a := &t36Affine{t36Fix: f}
	a.deletes = f.recordDeletes()
	return a
}

// writeLate performs the late pair's REAL write and witnesses its licence state.
func (a *t36Affine) writeLate(t *testing.T, subject string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
	seq, err := a.e.asm.jsa.storeInflightPair(subject, regExpect, markerExpect, r)
	if err == nil {
		load := a.e.asm.loadInflightMarker(mqttMarkerCallerEvidence, subject)
		a.mu.Lock()
		a.lateSeq, a.markerStood = seq, mqttMarkerNamesAllocation(load, a.nonce)
		a.mu.Unlock()
	}
	return seq, err
}

// affineSettled asserts what every affine row requires: nothing fabricated (no exchange: the
// class has none), and the late pair either PRESERVED (register and marker kept) or cleaned up
// only as a whole pair (the marker-backed cleanup).
func (a *t36Affine) affineSettled(t *testing.T) {
	t.Helper()
	if x := a.exchange(); x != nil {
		t.Fatalf("an affine delivery has no exchange, so none may be fabricated: %+v", x)
	}
	rec, smsg := a.register(t)
	marker, _ := a.e.loadMarker(t, a.pi)
	switch {
	case smsg != nil && rec != nil && rec.Nonce == a.nonce:
		if !mqttDurIsSlotOnlyClass(rec.Dur) {
			t.Fatalf("the preserved pair must stay slot-only: %+v", rec)
		}
		if marker == nil {
			t.Logf("observed: the late affine register is preserved without its marker (transmitted or consumed)")
		} else {
			t.Logf("observed: the late affine pair is PRESERVED (register and marker kept)")
		}
	case smsg == nil && marker == nil:
		// THE CLEANUP'S LICENCE: the cancellation's evidence survived (the in-memory cancelled
		// entry), and the pair may go only on its STANDING marker — witnessed as standing right
		// after the late write, with the cleanup deleting exactly that register.
		a.mu.Lock()
		lateSeq, stood := a.lateSeq, a.markerStood
		a.mu.Unlock()
		if lateSeq == 0 || !stood {
			t.Fatalf("the late pair was removed without its marker standing (seq %d, marker stood %v)", lateSeq, stood)
		}
		if !t36Contains(a.deletes(), lateSeq) {
			t.Fatalf("the late register %d was not removed by an exact delete, deletes %v", lateSeq, a.deletes())
		}
		t.Logf("observed: the late affine pair was cleaned up as a whole on its standing marker")
	default:
		t.Fatalf("the affine pair was split: register %+v, marker %+v", rec, marker)
	}
}

// Row 5: the affine ladder pauses after its ownership check, before its write; the fence lands and
// is disposed of by provenance alone; the resumed pair lands and is preserved.
func t36Row05AffinePausedWrite(t *testing.T) {
	a := newT36Affine(t, "f088-t36-05")
	var once atomic.Bool
	var disposed atomic.Bool
	a.e.asm.mu.Lock()
	a.e.asm.testInflightStorePair = func(subject string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
		if subject == a.subject && r != nil && r.Nonce == a.nonce && once.CompareAndSwap(false, true) {
			a.cancel(t)
			if ok, _ := a.fenceStands(t); ok {
				a.disposeFence(t)
				disposed.Store(true)
			}
			return a.writeLate(t, subject, regExpect, markerExpect, r)
		}
		return a.e.asm.jsa.storeInflightPair(subject, regExpect, markerExpect, r)
	}
	a.e.asm.mu.Unlock()
	a.e.sess.storeInflightRegister(a.e.c, a.pi, a.qos, a.dur, a.sseq, a.nonce, _EMPTY_, _EMPTY_)
	if !once.Load() {
		t.Fatal("fixture: the affine write was never reached")
	}
	if !disposed.Load() {
		t.Fatal("fixture: the slot-only fence must land through storePendingFenceCAS and be disposed of")
	}
	a.affineSettled(t)
}

// Row 6: an AMBIGUOUS affine write (it lands, its response reports failure) after the fence's
// disposal; subtest ambiguous-then-success: the ladder's next attempt succeeds. The earlier
// landing is preserved, not counted settled by the later success, and nothing is fabricated.
func t36Row06AffineAmbiguousReturn(t *testing.T) {
	run := func(t *testing.T, name string, thenSucceed bool) {
		a := newT36Affine(t, name)
		var calls atomic.Int32
		a.e.asm.mu.Lock()
		a.e.asm.testInflightStorePair = func(subject string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
			if subject != a.subject || r == nil || r.Nonce != a.nonce {
				return a.e.asm.jsa.storeInflightPair(subject, regExpect, markerExpect, r)
			}
			n := calls.Add(1)
			if n == 1 {
				a.cancel(t)
				if ok, _ := a.fenceStands(t); ok {
					a.disposeFence(t)
				}
				if _, err := a.writeLate(t, subject, regExpect, markerExpect, r); err != nil {
					t.Errorf("fixture: the ambiguous write must land: %v", err)
				}
				return 0, errors.New("test: the affine write's response was lost")
			}
			if thenSucceed {
				return a.e.asm.jsa.storeInflightPair(subject, regExpect, markerExpect, r)
			}
			return 0, errors.New("test: the affine write was refused")
		}
		a.e.asm.mu.Unlock()
		a.e.sess.storeInflightRegister(a.e.c, a.pi, a.qos, a.dur, a.sseq, a.nonce, _EMPTY_, _EMPTY_)
		if calls.Load() == 0 {
			t.Fatal("fixture: the affine write was never reached")
		}
		t.Logf("observed: %d affine write attempt(s)", calls.Load())
		a.affineSettled(t)
	}
	t.Run("ambiguous", func(t *testing.T) { run(t, "f088-t36-06a", false) })
	t.Run("ambiguous-then-success", func(t *testing.T) { run(t, "f088-t36-06b", true) })
}

// Row 7: the admission lands, its cancellation is SAVED with the record, the register's own
// tombstoning never happens (the process stops first); the cold resume restores VoidedRegs and
// recovery's genuine-cancellation arm withdraws the UNMARKED register on its standing marker.
func t36Row07FailedOccupantMarkingThenCrash(t *testing.T) {
	f := newT36Fix(t, "f088-t36-07")
	if !f.admit() {
		t.Fatal("fixture: the admission must land")
	}
	if rec, _ := f.register(t); rec == nil || rec.Cancelled {
		t.Fatalf("fixture: an unmarked landed register is required, got %+v", rec)
	}
	f.e.sess.mu.Lock()
	staged := f.e.sess.stageVoidVerdictLocked(mqttTestLicenceLocked(f.e.sess, nil), f.nonce)
	f.e.sess.mu.Unlock()
	if !staged {
		t.Fatal("fixture: the void verdict must stage")
	}
	restored := testF088ColdResume(t, f.e) // the save carries VoidedRegs; nothing tombstoned the register
	if x := f.exchange(); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("recovery's genuine-cancellation arm must withdraw the unmarked register, got %+v", x)
	}
	f.e.waitRegisterCount(t, 0)
	restored.mu.Lock()
	_, acting := restored.pendingPublish[f.pi]
	restored.mu.Unlock()
	if acting {
		t.Fatal("a withdrawn obligation must not be restored acting")
	}
}

// Row 8: both completion orders of an unresolved admission and its fence: in both, the
// independent custody survives the disposal, the admission's charge is released exactly at its
// settlement, and the identifier comes back only on the fence's confirmed absence.
func t36Row08BothCompletionOrders(t *testing.T) {
	for _, order := range []string{"fence-disposed-then-settled", "settled-then-fence-disposed"} {
		order := order
		t.Run(order, func(t *testing.T) {
			f := newT36Fix(t, "f088-t36-08-"+order[:5])
			ipi, inonce := f.independent(t)
			release := t36UnresolvedAdmission(t, f)
			fence, _ := f.fenceStands(t)
			settle := func() {
				release()
				testCustodyKick(f.e.sess)
				testCustodyEventually(t, "the admission settles", func() bool {
					return !testCustodyHasAdmission(f.e.sess, f.nonce)
				})
			}
			if order == "fence-disposed-then-settled" {
				if fence {
					f.disposeFence(t)
				}
				if !testCustodyHasAdmission(f.e.sess, f.nonce) {
					t.Fatal("the fence's disposal released the admission's charge")
				}
				settle()
			} else {
				settle()
				if fence {
					if testCustodyMints(f.e.sess, f.pi) == f.pi {
						t.Fatal("the identifier was handed out while its fence still stands")
					}
					f.disposeFence(t)
				}
			}
			f.independentKept(t, ipi, inonce)
			if rec, smsg := f.register(t); smsg != nil {
				t.Fatalf("nothing of this allocation may remain, got %+v", rec)
			}
		})
	}
}

// Row 9: a fence step's confirmation, paused right after the exact-sequence delete, resumes after
// a successor landed, or after the exclusion was REPLACED by a new one (a new instance): the
// successor is preserved and the old continuation lifts nothing of the new exclusion.
func t36Row09DisposalContinuationIdentity(t *testing.T) {
	for _, change := range []string{"successor", "replaced-exclusion"} {
		change := change
		t.Run(change, func(t *testing.T) {
			r := newT36Reclaim(t, "f088-t36-09-"+change[:4])
			const X = uint16(10)
			f, n, seq := r.fence("f", 110)
			r.exclude(r.e.sess, X, f, n)
			successor := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 110, Dur: r.dur,
				Seq: 99, QoS: 1, Nonce: mqttNewInflightNonce(), SourceAckReply: testInflightAckSubject(r.dur, 99, 1)})
			var newInst uint64
			r.afterDelete = func(s uint64, err error) {
				if s != seq || err != nil {
					return
				}
				switch change {
				case "successor":
					if _, serr := r.e.asm.jsa.storeInflightCAS(f, 0, successor); serr != nil {
						t.Errorf("landing the successor: %v", serr)
					}
				default:
					r.e.sess.mu.Lock()
					r.e.sess.adoptIdentifierExclusionLocked(X, f, n)
					r.e.sess.excludeIdentifierLocked(X, f, mqttNewInflightNonce())
					if h := r.e.sess.sendsInFlight[X]; h != nil && h.slotOwed != nil {
						newInst = h.slotOwed.inst
					}
					r.e.sess.mu.Unlock()
				}
			}
			r.pass(r.e.sess)
			if !t36Excluded(r.e.sess, X) {
				t.Fatal("the old continuation lifted an exclusion it did not observe")
			}
			if change == "successor" {
				rec, _, _ := r.e.asm.jsa.loadInflightRecord(f)
				if rec == nil || rec.Nonce != successor.Nonce {
					t.Fatalf("the successor must be preserved, got %+v", rec)
				}
			} else if x := t36Exclusion(r.e.sess, X); x == nil || x.inst != newInst || newInst == 0 {
				t.Fatalf("the new exclusion must stand at its own instance %d, got %+v", newInst, x)
			}
		})
	}
}

// Row 10: a same-generation RESIDENT reconnect keeps the ledger; the pending admission settles
// through it.
func t36Row10ResidentReconnect(t *testing.T) {
	f := newT36Fix(t, "f088-t36-10")
	release := t36UnresolvedAdmission(t, f)
	f.e.sess.mu.Lock()
	ledger := f.e.sess.custody
	f.e.sess.mu.Unlock()
	// The resident reconnect's CONNECT recovery runs on the SAME object, in the same generation.
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("the resident reconnect's recovery: %v", err)
	}
	f.e.sess.mu.Lock()
	same := f.e.sess.custody == ledger
	f.e.sess.mu.Unlock()
	if !same {
		t.Fatal("a same-generation resident reconnect must keep the ledger")
	}
	if !testCustodyHasAdmission(f.e.sess, f.nonce) {
		t.Fatal("the unresolved admission's charge must survive the reconnect")
	}
	if ok, _ := f.fenceStands(t); ok {
		t.Fatal("the reconnect's recovery must dispose of the fence (rule 3)")
	}
	release()
	testCustodyKick(f.e.sess)
	testCustodyEventually(t, "the pending admission settles through the kept ledger", func() bool {
		return !testCustodyHasAdmission(f.e.sess, f.nonce)
	})
}

// Row 11: a cold PEER takeover: recovery disposes of the fence (rule 3) and restores a landed
// register by its ordinary rules; no acting entry for a fence.
func t36Row11ColdTakeover(t *testing.T) {
	f := newT36Fix(t, "f088-t36-11")
	f.cancel(t)
	ok, _ := f.fenceStands(t)
	if !ok {
		t.Fatal("fixture: the pending fence must stand")
	}
	// Another delivery, landed with its marker standing: ordinary recovery restores it.
	dur2 := f.e.sess.idHash + "_" + nuid.Next()
	sseq2 := f.e.publishSource(t, dur2)
	pi2 := f.e.trackAdmitted(t, dur2, sseq2, 1)
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	_, peer, pc := f.e.peer(t, "node2", f.e.sess.id)
	if _, err := peer.recoverInflightRegisters(pc); err != nil {
		t.Fatalf("the peer's recovery: %v", err)
	}
	if ok, _ := f.fenceStands(t); ok {
		t.Fatal("the peer's recovery must dispose of the fence")
	}
	peer.mu.Lock()
	_, fenceEntry := peer.pendingPublish[f.pi]
	landed := peer.pendingPublish[pi2]
	peer.mu.Unlock()
	if fenceEntry {
		t.Fatal("no entry may be installed for a pending fence")
	}
	if landed == nil || !landed.acts() {
		t.Fatalf("the landed register must be restored acting by its ordinary rules, got %+v", landed)
	}
}

// Row 12: a CLEAN START discards the old generation's fences and custody by the generation's own
// lifecycle; the old admission's later settlement is inert in the new generation.
func t36Row12GenerationChange(t *testing.T) {
	f := newT36Fix(t, "f088-t36-12")
	release := t36UnresolvedAdmission(t, f)
	oldGen := f.gen
	if err := f.e.sess.clear(false); err != nil {
		t.Fatalf("clear: %v", err)
	}
	f.e.sess.mu.Lock()
	newGen := f.e.sess.gen
	held := len(f.e.sess.custody.heldAdmissions)
	fresh := f.e.sess.custody.forGen(newGen)
	// An exclusion may outlive the generation, by design: its bind names the OLD generation's
	// subject and is confirmed against that subject alone, never against the new one.
	var oldBinds bool
	if h := f.e.sess.sendsInFlight[f.pi]; slotExclusionHeldLocked(h) {
		oldBinds = true
		for _, b := range h.slotOwed.binds {
			if b.subject != mqttInflightSubject(f.e.sess.idHash, oldGen, f.pi) {
				oldBinds = false
			}
		}
	} else {
		oldBinds = true
	}
	f.e.sess.mu.Unlock()
	if newGen == oldGen {
		t.Fatal("fixture: a clean start must change the generation")
	}
	if held != 0 || !fresh {
		t.Fatalf("the new generation must carry nothing of the old one's custody: admissions %d, fresh ledger %v", held, fresh)
	}
	if !oldBinds {
		t.Fatal("a retained exclusion must bind only the old generation's subject")
	}
	release()
	testCustodyKick(f.e.sess)
	testCustodyStaysFor(t, "the old continuation is inert in the new generation", 300*time.Millisecond, func() bool {
		f.e.sess.mu.Lock()
		defer f.e.sess.mu.Unlock()
		return len(f.e.sess.custody.heldAdmissions) == 0 && !f.e.sess.custody.hasExchange(newGen, f.nonce)
	})
}

// Row 13: the fence at S is replaced by a successor at S2 before its delete: the successor
// survives, the identifier stays excluded, and the continuation keeps retrying. (T39 witnesses
// the same property through recovery's drop list; this row is the continuation's.)
func t36Row13SuccessorPreservation(t *testing.T) {
	r := newT36Reclaim(t, "f088-t36-13")
	const X = uint16(10)
	f, n, seq := r.fence("f", 110)
	r.exclude(r.e.sess, X, f, n)
	successor := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 110, Dur: r.dur,
		Seq: 98, QoS: 1, Nonce: mqttNewInflightNonce(), SourceAckReply: testInflightAckSubject(r.dur, 98, 1)})
	r.fenceFound = func(uint16, string) {
		if _, err := r.e.asm.jsa.storeInflightCAS(f, seq, successor); err != nil {
			t.Errorf("replacing the fence with its successor: %v", err)
		}
	}
	steps := r.pass(r.e.sess)
	r.fenceFound = nil
	if len(steps) != 1 || steps[0] != "10:f:kept" {
		t.Fatalf("the step over a replaced fence must keep the exclusion, got %v", steps)
	}
	rec, _, _ := r.e.asm.jsa.loadInflightRecord(f)
	if rec == nil || rec.Nonce != successor.Nonce {
		t.Fatalf("the successor must survive, got %+v", rec)
	}
	if !t36Excluded(r.e.sess, X) {
		t.Fatal("the identifier must stay excluded under a successor")
	}
	if steps := r.pass(r.e.sess); len(steps) != 0 {
		t.Fatalf("the successor is no fence: no fence step, got %v", steps)
	}
	if !t36Excluded(r.e.sess, X) {
		t.Fatal("the retry must keep the identifier excluded while the successor stands")
	}
}

// Row 14: the session-record save FAILS while the fence stands: the fence is still disposed of by
// provenance alone, and the save debt stays owed to HEAD's backstop.
func t36Row14UncoveredVerdict(t *testing.T) {
	f := newT36Fix(t, "f088-t36-14")
	var failing atomic.Bool
	failing.Store(true)
	hook := func(s *mqttSession) error {
		if s == f.e.sess && failing.Load() {
			return errors.New("test: the session record save failed")
		}
		return nil
	}
	mqttTestSessSaveHookV.Store(&hook)
	t.Cleanup(func() { mqttTestSessSaveHookV.Store(nil) })
	f.cancel(t)
	ok, _ := f.fenceStands(t)
	if !ok {
		t.Fatal("fixture: the fence must land")
	}
	f.e.sess.mu.Lock()
	owed := f.e.sess.voidedRegsRev != f.e.sess.voidedRegsSavedRev
	f.e.sess.mu.Unlock()
	if !owed {
		t.Fatal("fixture: the failed save must leave the verdict revision unsaved")
	}
	f.disposeFence(t)
	if ok, _ := f.fenceStands(t); ok {
		t.Fatal("the fence must be disposed of whatever the save says")
	}
	f.e.sess.mu.Lock()
	owed = f.e.sess.voidedRegsRev != f.e.sess.voidedRegsSavedRev
	f.e.sess.mu.Unlock()
	if !owed {
		t.Fatal("the save debt must stay owed to the backstop, not be discharged by the disposal")
	}
	failing.Store(false)
}

// Row 15: the fence is written and the save FAILS; the process stops. Recovery disposes of the
// proven fence without manufacturing a coverage obligation, and a subsequent admissible late
// landing follows ordinary recovery.
func t36Row15RecoveryWithoutCoverage(t *testing.T) {
	f := newT36Fix(t, "f088-t36-15")
	// The session's record as it stood before the cancelling packet (its incarnation and claim);
	// only the packet's own save fails.
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	var failing atomic.Bool
	failing.Store(true)
	hook := func(s *mqttSession) error {
		if s == f.e.sess && failing.Load() {
			return errors.New("test: the session record save failed")
		}
		return nil
	}
	mqttTestSessSaveHookV.Store(&hook)
	t.Cleanup(func() { mqttTestSessSaveHookV.Store(nil) })
	f.cancel(t)
	if ok, _ := f.fenceStands(t); !ok {
		t.Fatal("fixture: the fence must land")
	}
	failing.Store(false)
	restored := f.coldResumeUnsaved(t)
	if ok, _ := f.fenceStands(t); ok {
		t.Fatal("recovery must dispose of the proven fence")
	}
	restored.mu.Lock()
	_, obligated := restored.voidedRegNonces[f.nonce]
	restored.mu.Unlock()
	if obligated {
		t.Fatal("recovery must not manufacture a coverage obligation for a disposed fence")
	}
	// A late landing of the same allocation, then ordinary recovery.
	f.e.landAdmission(t, f.pi, 1, f.dur, f.sseq, f.nonce)
	again := f.coldResumeUnsaved(t)
	if x := f.exchange(); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("ordinary recovery fabricates nothing for the late landing, got %+v", x)
	}
	again.mu.Lock()
	entry := again.pendingPublish[f.pi]
	again.mu.Unlock()
	t.Logf("observed: the late landing after recovery is %s", t36EntryState(entry))
}

func t36EntryState(p *mqttPending) string {
	switch {
	case p == nil:
		return "not installed"
	case p.acts():
		return "restored ACTING"
	}
	return "reserved (non-acting)"
}

// Row 16: an ordinary REFUSED admission with no cancellation writes no fence and leaves
// VoidedRegs unchanged.
func t36Row16UncancelledAdmission(t *testing.T) {
	f := newT36Fix(t, "f088-t36-16")
	f.e.sess.mu.Lock()
	before := len(f.e.sess.voidedRegNonces)
	f.e.sess.mu.Unlock()
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange {
			return 0, errors.New("test: the admission's batch was refused")
		}
		return send()
	}
	f.e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
	if f.admit() {
		t.Fatal("fixture: the admission must be refused")
	}
	if rec, smsg := f.register(t); smsg != nil {
		t.Fatalf("an uncancelled refusal writes no fence, got %+v", rec)
	}
	f.e.sess.mu.Lock()
	after := len(f.e.sess.voidedRegNonces)
	f.e.sess.mu.Unlock()
	if after != before {
		t.Fatalf("VoidedRegs changed from %d to %d without a cancellation", before, after)
	}
}

// Row 17 (F-119's schedule, through the production path): the cancellation's fence lands; the
// admission's next attempt reads the authority AFTER it and is held unsent and unsettled; the
// cancelling ladder's own disposal is refused while it runs; once it has returned the fence is
// disposed of through the one door, behind its authority advance; the held batch then reaches the
// store and is REFUSED; the process stops. A cold resume with no surviving verdict finds nothing of
// the cancelled allocation to restore. (Before F-119 this row injected the late landing with a FRESH
// authority read after the disposal and recorded it restored acting; that injection never witnessed
// a previously prepared batch.)
func t36Row17CrashAfterDisposalNoVerdict(t *testing.T) {
	f := newT36Fix(t, "f088-t36-17")
	var cancelled atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testAdmitWrite = func(s *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if cancelled.CompareAndSwap(false, true) {
			f.cancel(t)
		}
		return write()
	}
	f.e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
		return mqttTransitionUnresolved, true
	}
	f.e.asm.mu.Unlock()
	var held func() (uint64, error)
	var once atomic.Bool
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange {
			if once.CompareAndSwap(false, true) {
				held = send
				return 0, errors.New("test: the post-fence attempt's response is lost; its batch is held")
			}
			return 0, errors.New("test: later admission attempts are not sent")
		}
		return send()
	}
	f.e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
	if f.admit() {
		t.Fatal("a cancelled allocation must not be admitted")
	}
	if held == nil {
		t.Fatal("fixture: the post-fence attempt was never held")
	}
	if ok, _ := f.fenceStands(t); !ok {
		t.Fatal("the cancelling ladder's own disposal must be refused while it runs")
	}
	if f.verdicted() {
		t.Fatal("fixture: HEAD's early forget runs once the fence lands")
	}
	f.disposeFence(t)
	if _, err := held(); err == nil {
		t.Fatal("the held post-fence batch must be refused once the disposal's advance landed")
	}
	restored := testF088ColdResume(t, f.e)
	if rec, smsg := f.register(t); smsg != nil && rec != nil && rec.Nonce == f.nonce && !rec.Cancelled {
		t.Fatalf("no late landing may stand for the cancelled allocation: %+v", rec)
	}
	if x := f.exchange(); x != nil {
		t.Fatalf("no exchange may stand for the cancelled allocation: %+v", x)
	}
	restored.mu.Lock()
	entry := restored.pendingPublish[f.pi]
	restored.mu.Unlock()
	if entry != nil && entry.regNonce == f.nonce && entry.acts() {
		t.Fatalf("a cold resume must not restore the cancelled allocation acting: %s", t36EntryState(entry))
	}
}
