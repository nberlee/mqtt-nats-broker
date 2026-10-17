// Copyright 2025 The mqtt-nats-broker Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"go/ast"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nuid"
)

// F-088 / F-097 (consult C4, plan rev 20): the pending cancellation fence and its dedicated
// disposal rule, witnessed on the real store. T33-T35, T37-T39 and T41.

// testF088WriteFence runs the REAL producer for allocation nonce on pi and returns the fence it
// wrote, asserting the written provenance.
func testF088WriteFence(t *testing.T, e *testInflightEnv, pi uint16, dur string, sseq uint64, nonce string) (*mqttInflightRecord, uint64) {
	t.Helper()
	if err := e.sess.markInflightRegisterCancelledPending(e.c, mqttRegCancellation{
		pi: pi, nonce: nonce, durable: dur, sseq: sseq, reply: testInflightAckSubject(dur, sseq, 1),
	}, mqttLicenceOf(e.c)); err != nil {
		t.Fatalf("the producer must write the fence: %v", err)
	}
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil || !mqttValidPendingFence(subject, rec) {
		t.Fatalf("the producer must leave a valid pending fence on %q, got %+v (%v)", subject, rec, err)
	}
	return rec, smsg.Sequence
}

// testF088ColdResume persists the session record, restores a session object from it in the same
// generation (a cold resume), and runs recovery on it.
func testF088ColdResume(t *testing.T, e *testInflightEnv) *mqttSession {
	t.Helper()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	restored, _, err := e.asm.createOrRestoreSession(e.sess.id, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if restored.gen != e.sess.gen {
		t.Fatalf("fixture: a cold resume must keep the generation, %q vs %q", restored.gen, e.sess.gen)
	}
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	return restored
}

// T33. The REAL producer writes `pending-create`; a cold resume routes the fence to the drop list
// under its own disposition, deletes exactly that fence (confirmed), installs no acting entry, and
// does not withhold allocation. Subtest `affine`: the slot-only class persists its fence through
// the validated create-only entry, and the recovery partition diverts it BEFORE the affine claim
// pass ever receives it.
//
// Deliberately NOT parallel: the claim-pass witness is a package-level seam.
func TestF088PendingFenceFromTheProducerIsDroppedOnColdResume(t *testing.T) {
	for _, tc := range []struct {
		name   string
		affine bool
	}{
		{"ordinary", false},
		{"affine", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "f088-t33-"+tc.name)
			dur := e.sess.idHash + "_" + nuid.Next()
			if tc.affine {
				dur = mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_fence"
			} else {
				e.acceptUnderServingAuthority(t)
			}
			e.sess.mu.Lock()
			pi := e.sess.bumpPI()
			e.sess.mu.Unlock()
			fence, fseq := testF088WriteFence(t, e, pi, dur, 4, mqttNewInflightNonce())
			if mqttDurIsSlotOnlyClass(fence.Dur) != tc.affine {
				t.Fatalf("fixture: the fence's class is wrong: %q", fence.Dur)
			}
			subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

			var mu sync.Mutex
			var claimed bool
			mqttTestClaimRecoveredHook = func(in []mqttRecoveredInflightReg) {
				for _, r := range in {
					if r.rec != nil && r.rec.Nonce == fence.Nonce {
						mu.Lock()
						claimed = true
						mu.Unlock()
					}
				}
			}
			t.Cleanup(func() { mqttTestClaimRecoveredHook = nil })
			var deleted []uint64
			e.asm.mu.Lock()
			e.asm.testInflightDelete = func(seq uint64) error {
				mu.Lock()
				deleted = append(deleted, seq)
				mu.Unlock()
				return e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
			}
			e.asm.mu.Unlock()

			restored := testF088ColdResume(t, e)
			mu.Lock()
			gotDeleted, gotClaimed := append([]uint64(nil), deleted...), claimed
			mu.Unlock()
			if gotClaimed {
				t.Fatal("the affine claim pass received a pending fence: recovery must divert fences before it")
			}
			found := false
			for _, s := range gotDeleted {
				if s == fseq {
					found = true
				}
			}
			if !found {
				t.Fatalf("recovery must delete exactly the observed fence at %d, deleted %v", fseq, gotDeleted)
			}
			if rec, smsg, _ := e.asm.loadInflightRecord(subject); smsg != nil {
				t.Fatalf("the fence must be gone (confirmed), found %+v", rec)
			}
			restored.mu.Lock()
			_, acting := restored.pendingPublish[pi]
			incomplete := restored.piSpaceIncomplete
			withholds := restored.custody.withholdsAllocation()
			restored.mu.Unlock()
			if acting {
				t.Fatal("recovery installed an entry for a pending fence")
			}
			if incomplete || withholds {
				t.Fatalf("a dropped fence must not withhold allocation: incomplete=%v withholds=%v", incomplete, withholds)
			}
		})
	}
}

// T34. The provenance is enforced at the boundary, for every class.
func TestF088PendingFenceProvenanceIsEnforcedAtTheBoundary(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f088-t34")
	e.acceptUnderServingAuthority(t)
	ent, _ := e.c.outboundEntitlement()
	jsa := &e.asm.jsa
	tomb := func(subject string, expect uint64, body *mqttInflightRecord) error {
		_, err := jsa.opTombstoneRegister(&mqttTombstoneRegisterRequest{
			ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-tombstone-register",
			Ent: ent, Subject: subject, Expect: expect, Body: body,
		})
		return err
	}

	// An admitted ordinary register to copy, as the copy-and-CAS fallback does.
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	pi := e.sess.bumpPI()
	e.sess.mu.Unlock()
	admitted := e.landAdmission(t, pi, 1, dur, sseq, mqttNewInflightNonce())
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	_, amsg := e.loadRegister(t, pi)

	t.Run("(a)+(b) a pending-create member at a nonzero expectation (a copied occupant) is refused", func(t *testing.T) {
		copied := *admitted
		copied.Cancelled, copied.CancelOrigin = true, mqttCancelOriginPendingCreate
		err := tomb(subject, amsg.Sequence, &copied)
		if err == nil || !strings.Contains(err.Error(), "pending cancellation fence") {
			t.Fatalf("a CAS minting pending-create must be refused at the member check, got %v", err)
		}
		if rec, smsg := e.loadRegister(t, pi); rec == nil || smsg.Sequence != amsg.Sequence || rec.Cancelled {
			t.Fatalf("the occupant must be untouched, got %+v", rec)
		}
	})
	// (b), PRODUCTION ROUTE (M45a's target): the copy-and-CAS fallback of
	// markInflightRegisterCancelled - the one production writer that copies an occupant - writes
	// the OCCUPANT provenance, and its write lands. A fallback that copied the occupant with
	// `pending-create` would be refused at the member check above, so the cancellation would not
	// land at all: the forged provenance is caught by the boundary AND by this assertion.
	t.Run("(b) production: the copy-and-CAS fallback writes an occupant cancellation", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "f088-t34b-prod", 1)
		ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
		if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
			t.Fatalf("fixture: consuming the marker: %v", err)
		}
		// Witness (the fallback's input): the marker is authoritatively absent, so the withdrawal
		// arm has no evidence and the function falls through to the copy-and-CAS fallback.
		if load := f.e.asm.loadInflightMarker(mqttMarkerCallerStore, f.subject); load.kind != mqttInflightResponseAbsent {
			t.Fatalf("fixture: the marker must read absent, got kind %v", load.kind)
		}
		before, seq := f.register(t)
		if before == nil || before.Cancelled {
			t.Fatalf("fixture: an uncancelled admitted register is required, got %+v", before)
		}
		out, err := f.e.sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce,
			mqttLicenceOf(f.e.c))
		rec, rseq := f.register(t)
		if err != nil || out != mqttRegCancelStored || rec == nil || rseq == seq || !rec.Cancelled ||
			rec.CancelOrigin != mqttCancelOriginOccupant {
			t.Fatalf("the copy-and-CAS fallback must land an OCCUPANT cancellation (never a pending-create copy): "+
				"out %v, err %v, register %+v", out, err, rec)
		}
	})
	t.Run("(c) a pending-create body that is not the sparse form is refused", func(t *testing.T) {
		base := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 9, Dur: dur, Seq: 3, QoS: 1,
			Nonce: mqttNewInflightNonce(), Cancelled: true, CancelOrigin: mqttCancelOriginPendingCreate})
		if err := mqttValidateInflightShape(base); err != nil {
			t.Fatalf("fixture: the base must be a valid fence: %v", err)
		}
		for name, mut := range map[string]func(r *mqttInflightRecord){
			"QoS 2":         func(r *mqttInflightRecord) { r.QoS = 2 },
			"Acq 1":         func(r *mqttInflightRecord) { r.Acq = 1 },
			"Lin != Nonce":  func(r *mqttInflightRecord) { r.Lin = mqttNewInflightNonce() },
			"retired":       func(r *mqttInflightRecord) { r.Retired = true },
			"not cancelled": func(r *mqttInflightRecord) { r.Cancelled = false },
		} {
			bad := *base
			mut(&bad)
			if err := mqttValidateInflightShape(&bad); err == nil {
				t.Errorf("%s: a malformed pending fence must be refused by the shape matrix", name)
			}
		}
	})
	t.Run("(d) an ack re-phase, an acquisition and a replacement over a fence are refused", func(t *testing.T) {
		fe := testNewInflightEnv(t, "f088-t34d")
		fe.acceptUnderServingAuthority(t)
		fdur := fe.sess.idHash + "_" + nuid.Next()
		fe.sess.mu.Lock()
		fpi := fe.sess.bumpPI()
		fe.sess.mu.Unlock()
		fence, fseq := testF088WriteFence(t, fe, fpi, fdur, 6, mqttNewInflightNonce())
		cleared := *fence
		cleared.Cancelled, cleared.CancelOrigin = false, _EMPTY_
		cleared.State = mqttInflightPublishTerminal
		if mqttIsAckPhaseAdvance(fence, &cleared) {
			t.Error("an acknowledgement re-phase must not be recognised over a pending fence")
		}
		acq := *fence
		acq.Cancelled, acq.CancelOrigin = false, _EMPTY_
		acq.ActionToken, acq.ActionClaim, acq.ActionClaimSeq, acq.Acq = mqttNewInflightNonce(), fence.Claim, fence.ClaimSeq, 1
		if mqttIsActionTokenAcquisition(fence, &acq) {
			t.Error("a token acquisition must not be recognised over a pending fence")
		}
		repl := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: fpi, Dur: fence.Dur, Seq: fence.Seq,
			QoS: 1, Nonce: mqttNewInflightNonce(), Claim: fence.Claim, SourceAckReply: fence.SourceAckReply})
		body, _ := json.Marshal(repl)
		fsubject := mqttInflightSubject(fe.sess.idHash, fe.sess.gen, fpi)
		err := fe.asm.jsa.checkRegisterReplacements(&mqttTransitionAttempt{Txn: "t34d", Kind: mqttTransitionAdmitExchange},
			[]mqttTransitionMember{{subj: fsubject, expect: fseq, body: body}})
		if err == nil || !strings.Contains(err.Error(), "pending cancellation fence") {
			t.Fatalf("a same-delivery replacement over a pending fence must be refused, got %v", err)
		}
	})
	t.Run("(e)+(h) the generic sinks refuse to mint or carry pending-create", func(t *testing.T) {
		affDur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_t34"
		sparse := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 77, Dur: affDur, Seq: 5, QoS: 1,
			Nonce: mqttNewInflightNonce(), Cancelled: true, CancelOrigin: mqttCancelOriginPendingCreate})
		s77 := mqttInflightSubject(e.sess.idHash, e.sess.gen, 77)
		if _, err := e.asm.storeInflightCAS(s77, 0, sparse); err == nil {
			t.Error("storeInflightCAS must refuse a pending-create body")
		}
		if _, err := e.asm.storeInflightPair(s77, 0, 0, sparse); err == nil {
			t.Error("storeInflightPair must refuse a pending-create body")
		}
		ref := mqttSlotRefFromRecord(subject, amsg.Sequence, admitted)
		if _, ok := e.sess.casInflightSlot(e.c, *ref, _EMPTY_, mqttSlotMarkRetired, func(r *mqttInflightRecord) {
			r.Cancelled, r.CancelOrigin = true, mqttCancelOriginPendingCreate
		}); ok {
			t.Error("casInflightSlot must refuse a rewrite minting pending-create")
		}
		if rec, smsg := e.loadRegister(t, pi); rec == nil || smsg.Sequence != amsg.Sequence {
			t.Fatalf("the refused rewrite must leave the occupant untouched, got %+v", rec)
		}
	})
	t.Run("(f) storePendingFenceCAS lands a valid slot-only fence at expectation 0", func(t *testing.T) {
		affDur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_t34f"
		sparse := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 78, Dur: affDur, Seq: 5, QoS: 1,
			Nonce: mqttNewInflightNonce(), Cancelled: true, CancelOrigin: mqttCancelOriginPendingCreate})
		s78 := mqttInflightSubject(e.sess.idHash, e.sess.gen, 78)
		if _, err := e.asm.storePendingFenceCAS(s78, 0, sparse); err != nil {
			t.Fatalf("a valid slot-only fence must land: %v", err)
		}
		if rec, _, _ := e.asm.loadInflightRecord(s78); rec == nil || rec.CancelOrigin != mqttCancelOriginPendingCreate {
			t.Fatalf("the landed fence must carry its provenance, got %+v", rec)
		}
	})
	t.Run("(g) storePendingFenceCAS refuses a nonzero expectation, an ordinary body and a malformed body", func(t *testing.T) {
		affDur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_t34g"
		mk := func(d string) *mqttInflightRecord {
			return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 79, Dur: d, Seq: 5, QoS: 1,
				Nonce: mqttNewInflightNonce(), Cancelled: true, CancelOrigin: mqttCancelOriginPendingCreate})
		}
		s79 := mqttInflightSubject(e.sess.idHash, e.sess.gen, 79)
		if _, err := e.asm.storePendingFenceCAS(s79, 3, mk(affDur)); err == nil {
			t.Error("a nonzero expectation must be refused")
		}
		if _, err := e.asm.storePendingFenceCAS(s79, 0, mk(dur)); err == nil {
			t.Error("an ordinary-class body must be refused")
		}
		bad := mk(affDur)
		bad.QoS = 2
		if _, err := e.asm.storePendingFenceCAS(s79, 0, bad); err == nil {
			t.Error("a malformed fence body must be refused")
		}
		if _, smsg, _ := e.asm.loadInflightRecord(s79); smsg != nil {
			t.Fatal("no refused write may land")
		}
	})
	t.Run("(i) the affine claim rewriting cannot write or change the provenance", func(t *testing.T) {
		affDur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_t34i"
		sparse := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 80, Dur: affDur, Seq: 5, QoS: 1,
			Nonce: mqttNewInflightNonce(), Claim: "another-object", Cancelled: true, CancelOrigin: mqttCancelOriginPendingCreate})
		s80 := mqttInflightSubject(e.sess.idHash, e.sess.gen, 80)
		seq, err := e.asm.storePendingFenceCAS(s80, 0, sparse)
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		e.sess.claimRecoveredInflight(e.c, []mqttRecoveredInflightReg{{rec: sparse, seq: seq}}, mqttInflightClaimTransferable)
		rec, smsg, _ := e.asm.loadInflightRecord(s80)
		if rec != nil && smsg != nil && smsg.Sequence != seq && rec.CancelOrigin == mqttCancelOriginPendingCreate {
			t.Fatalf("the claim pass rewrote a pending fence and kept its provenance: %+v", rec)
		}
		if rec != nil && rec.CancelOrigin != mqttCancelOriginPendingCreate && rec.Cancelled {
			t.Fatalf("the claim pass changed a fence's provenance: %+v", rec)
		}
	})
}

// testF088CopiedFreshTombstone builds T35's record: a freshly ADMITTED ordinary QoS 1 register
// (exchange at Publish, Lin == Nonce, Acq 0), whose marker an intent consumed, cancelled by the
// copy-and-CAS fallback. Its body equals the sparse fence form field for field except its
// provenance, which says `occupant`.
func testF088CopiedFreshTombstone(t *testing.T, clientID string) (*testWithdrawFix, *mqttInflightRecord, uint64) {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, 1)
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the marker: %v", err)
	}
	_, seq := f.register(t)
	if _, err := f.e.sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce,
		mqttLicenceOf(f.e.c)); err != nil {
		t.Fatalf("the copy-and-CAS fallback must land: %v", err)
	}
	rec, rseq := f.register(t)
	if rec == nil || !rec.Cancelled || rec.CancelOrigin != mqttCancelOriginOccupant {
		t.Fatalf("the fallback must write an occupant cancellation, got %+v", rec)
	}
	asFence := *rec
	asFence.CancelOrigin = mqttCancelOriginPendingCreate
	if !mqttPendingFenceBodyOK(&asFence) {
		t.Fatalf("fixture: the copied tombstone must equal the sparse form but for its provenance: %+v", rec)
	}
	return f, rec, rseq
}

// T35. A copied fresh-admission tombstone is NOT a fence: the provenance, not the body, decides.
// Route subtests: the classifier, the door and recovery each refuse to treat it as a fence.
func TestF088CopiedFreshAdmissionTombstoneIsNotAFence(t *testing.T) {
	t.Parallel()
	t.Run("authority and a late PUBACK", func(t *testing.T) {
		t.Parallel()
		f, rec, rseq := testF088CopiedFreshTombstone(t, "f088-t35")
		rel := f.e.sess.mintSlotRelease(f.subject, rec, rseq)
		if rel.auth == mqttSlotPendingFenceAuthority {
			t.Fatal("an occupant cancellation was given the pending-fence authority")
		}
		if rel.auth != mqttSlotReservedAuthority {
			t.Fatalf("arm 4 must reserve a marker-less occupant cancellation, got %d", rel.auth)
		}
		// The late PUBACK completes the exchange (refuted-cancellation clearance on the marker's
		// authoritative absence).
		f.e.sess.mu.Lock()
		ack := f.e.sess.pendingPublish[f.pi]
		f.e.sess.mu.Unlock()
		if ack == nil {
			t.Fatal("fixture: the delivery's entry must still stand")
		}
		f.e.sess.completeAcknowledgedPublish(f.e.c, f.pi, ack)
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangeTerminal {
			t.Fatalf("a late PUBACK must complete the exchange, got %+v", x)
		}
	})
	t.Run("classifier", func(t *testing.T) {
		t.Parallel()
		f, rec, rseq := testF088CopiedFreshTombstone(t, "f088-t35-classifier")
		obs := mqttSlotObservation{resp: mqttSlotSubjectPresent, occupant: rec, seq: rseq,
			subjectNamesOccupant: true, cancelMarker: mqttSlotMarkerAbsent}
		v := mqttClassifyInflightSlot(obs, mqttAllocationIntent{pi: f.pi, dur: f.e.sess.idHash + "_other", sseq: 9,
			nonce: mqttNewInflightNonce()})
		if v.disposition == mqttSlotDispReapPendingFenceThenRetry {
			t.Fatal("the classifier reaped an occupant cancellation as a fence")
		}
		if after, aseq := f.register(t); after == nil || aseq != rseq {
			t.Fatalf("the register must survive, got %+v", after)
		}
	})
	t.Run("door", func(t *testing.T) {
		t.Parallel()
		f, rec, rseq := testF088CopiedFreshTombstone(t, "f088-t35-door")
		// The ordinary never-transmitted request FIRST: it is the one the door's dedicated fence
		// mapping (mqttDoorDisposition, M45c's site) could divert; the direct fence request is the
		// fence authority's alone. Each evaluation is reported on its own (non-fatal), so a deletion
		// is attributed to the request that caused it.
		for _, d := range []mqttInflightDisposition{mqttSlotDeleteNeverTransmitted, mqttSlotDeletePendingFence} {
			if out := f.e.sess.reapLoadedRegister(f.subject, rseq, rec, d, mqttLicenceOf(f.e.c), nil); out != mqttReapRefused {
				t.Errorf("the door must refuse disposition %d over an occupant cancellation, got %v", d, out)
			}
		}
		if after, aseq := f.register(t); after == nil || aseq != rseq {
			t.Fatalf("the register must survive, got %+v", after)
		}
	})
	t.Run("recovery", func(t *testing.T) {
		t.Parallel()
		f, _, rseq := testF088CopiedFreshTombstone(t, "f088-t35-recovery")
		// REACH (the recovery partition): the record leaves the fence partition by exactly one of
		// its two exits for it - diverted to the fence drop list (a delete attempted at its
		// sequence) or passed on to the ordinary classification (recovery's cancellation
		// classifier fires for p). Either proves the partition evaluated it.
		var classified, dropAttempted atomic.Bool
		f.e.asm.mu.Lock()
		f.e.asm.testRecoverCancellation = func(pi uint16, nonce string, recCancelled, verdict bool, markerSeq uint64) {
			if pi == f.pi && recCancelled {
				classified.Store(true)
			}
		}
		f.e.asm.testInflightDelete = func(seq uint64) error {
			if seq == rseq {
				dropAttempted.Store(true)
			}
			return f.e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
		}
		f.e.asm.mu.Unlock()
		restored := testF088ColdResume(t, f.e)
		if !classified.Load() && !dropAttempted.Load() {
			t.Fatal("witness: recovery's partition never evaluated the occupant cancellation")
		}
		if after, aseq := f.register(t); after == nil || aseq != rseq {
			t.Fatalf("recovery must not divert an occupant cancellation to the fence drop list, got %+v", after)
		}
		restored.mu.Lock()
		held := restored.pendingPublish[f.pi] != nil || restored.custody.holdsPI(f.pi) || restored.sendReservedLocked(f.pi)
		restored.mu.Unlock()
		if !held {
			t.Fatalf("identifier %d must stay held", f.pi)
		}
	})
}

// T37. A Cancelled register written WITHOUT its provenance (raw store write, test-only) is an
// unsupported shape: refused at decode, reserved by recovery as unreadable, refused by the door.
// Nothing is inferred from its sparsity.
func TestF088UnmarkedCancelledRegisterIsRefused(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f088-t37")
	e.sess.mu.Lock()
	pi := e.sess.bumpPI()
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	dur := e.sess.idHash + "_" + nuid.Next()
	unmarked := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: dur, Seq: 4, QoS: 1,
		Nonce: mqttNewInflightNonce(), Cancelled: true})
	body, _ := json.Marshal(unmarked)
	if _, err := mqttDecodeInflightRecord(body); err == nil {
		t.Fatal("a cancelled register without provenance must be refused at decode")
	}
	if _, err := e.asm.jsa.storeMsg(subject, 0, body); err != nil {
		t.Fatalf("raw store write: %v", err)
	}
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || smsg == nil || cur != nil {
		t.Fatalf("the reader must report the record present but unreadable, got rec=%+v err=%v", cur, err)
	}
	ref := &mqttInflightSlotRef{name: mqttInflightName{idHash: e.sess.idHash, gen: e.sess.gen, pi: pi, subject: subject},
		seq: smsg.Sequence, nonce: unmarked.Nonce, state: mqttInflightPublish, dur: dur, sseq: 4}
	if out := e.sess.reapRegisterGatedOutcome(smsg.Sequence, unmarked.Nonce, true, ref, mqttSlotDeleteNeverTransmitted,
		mqttLicenceOf(e.c)); out != mqttReapRefused {
		t.Fatalf("the door must refuse an unreadable record, got %v", out)
	}
	restored := testF088ColdResume(t, e)
	if _, after, _ := e.asm.loadInflightRecord(subject); after == nil || after.Sequence != smsg.Sequence {
		t.Fatal("recovery must keep an unreadable record, never interpret or migrate it")
	}
	restored.mu.Lock()
	held := restored.pendingPublish[pi] != nil || restored.sendReservedLocked(pi)
	restored.mu.Unlock()
	if !held {
		t.Fatalf("recovery must reserve identifier %d under an unreadable record", pi)
	}
}

// T38. An `occupant` tombstone whose exchange read reports ABSENT is not a fence: not disposed of
// by provenance, not withdrawn, register preserved (the P-d2 refusal). Real-store witness: the
// exchange message is deleted, so the read genuinely answers absent.
func TestF088FalseExchangeAbsenceDoesNotMakeAFence(t *testing.T) {
	t.Parallel()
	f, rec, rseq := testF088CopiedFreshTombstone(t, "f088-t38")
	xsubj := mqttExchangeSubject(f.e.sess.idHash, f.gen, f.lin)
	testDeleteSubject(t, f.e, xsubj)
	if x, _, err := f.e.asm.jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.lin); x != nil || !mqttExchangeReadAbsent(err) {
		t.Fatalf("fixture: the exchange read must report absent, got %+v (%v)", x, err)
	}
	for _, d := range []mqttInflightDisposition{mqttSlotDeletePendingFence, mqttSlotDeleteNeverTransmitted, mqttSlotDeleteComplete} {
		if out := f.e.sess.reapLoadedRegister(f.subject, rseq, rec, d, mqttLicenceOf(f.e.c), nil); out != mqttReapRefused {
			t.Fatalf("disposition %d over an exchange-less occupant cancellation must be refused, got %v", d, out)
		}
	}
	if after, aseq := f.register(t); after == nil || aseq != rseq || after.CancelOrigin != mqttCancelOriginOccupant {
		t.Fatalf("the register must be preserved untouched, got %+v at %d", after, aseq)
	}
	// AND ON RESUME (P-d2): recovery reserves the exchange-less occupant cancellation as a shape it
	// does not continue - never diverted as a fence, never restored acting on its bit.
	restored := testF088ColdResume(t, f.e)
	if after, aseq := f.register(t); after == nil || aseq != rseq {
		t.Fatalf("recovery must keep the register, got %+v at %d", after, aseq)
	}
	restored.mu.Lock()
	entry := restored.pendingPublish[f.pi]
	reserved := restored.sendReservedLocked(f.pi) || (entry != nil && !entry.acts())
	withheld := restored.custody.withholdsAllocation()
	restored.mu.Unlock()
	// The exchange record is gone while the generation's duty ledger still names its lineage, so
	// the inventory census is UNCERTAIN here, not complete: that census decides nothing about
	// this register, and its uncertainty withholds every new allocation instead (HEAD's
	// incomplete-census rule). Only a COMPLETE census reserves the register as P-d2 - and no
	// cancellation bit exempts it from that any more (F-088).
	if entry != nil && entry.acts() && !withheld {
		t.Fatal("recovery restored an exchange-less occupant cancellation ACTING with allocation open")
	}
	if !reserved && !withheld {
		t.Fatalf("recovery must keep identifier %d reserved or allocation withheld", f.pi)
	}
	t.Logf("observed: restored entry %s, allocation withheld %v", t36EntryState(entry), withheld)
}

// T39. The fence is observed at S; a successor replaces it at S2 before the delete. The exact-
// sequence delete removes nothing of the successor, the successor survives, and the identifier's
// exclusion is not lifted. Driven through recovery's drop of the fence.
func TestF088PendingFenceDisposalPreservesASuccessor(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f088-t39")
	e.acceptUnderServingAuthority(t)
	dur := e.sess.idHash + "_" + nuid.Next()
	e.sess.mu.Lock()
	pi := e.sess.bumpPI()
	e.sess.mu.Unlock()
	fence, fseq := testF088WriteFence(t, e, pi, dur, 4, mqttNewInflightNonce())
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	successor := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: dur, Seq: 5, QoS: 1,
		Nonce: mqttNewInflightNonce(), Claim: fence.Claim, SourceAckReply: testInflightAckSubject(dur, 5, 1)})
	var once sync.Once
	var raced bool
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(seq uint64) error {
		if seq == fseq {
			once.Do(func() {
				if _, err := e.asm.jsa.storeInflightCAS(subject, fseq, successor); err != nil {
					t.Errorf("installing the successor: %v", err)
					return
				}
				raced = true
			})
		}
		return e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	e.asm.mu.Unlock()

	restored := testF088ColdResume(t, e)
	if !raced {
		t.Fatal("fixture: the fence's delete was never reached, so no successor raced it")
	}
	rec, smsg, _ := e.asm.loadInflightRecord(subject)
	if rec == nil || smsg == nil || rec.Nonce != successor.Nonce {
		t.Fatalf("the successor must survive the fence's disposal, got %+v", rec)
	}
	restored.mu.Lock()
	excluded := restored.sendReservedLocked(pi)
	restored.mu.Unlock()
	if !excluded {
		t.Fatalf("identifier %d must stay excluded: its subject is occupied by a successor", pi)
	}
}

// T41. The settlement invariant (C6): a cancellation before admission opening, or after opening
// but before the ladder's entry check, leaves NO permanently unsettled admission; the ordinary
// caller's deferred finishAdmissionCustody runs.
func TestF088CancellationBeforeOpeningLeavesNoUnsettledPlaceholder(t *testing.T) {
	t.Parallel()
	// THE STRUCTURAL HALF: no early return was added to the opening or ahead of the deferred
	// settlement by this change. The return statements of the two opening functions are frozen
	// at their HEAD 7dd2c3e count (read from the HEAD source when this was written), and in the
	// admission body the deferred finishAdmissionCustody is the statement right after the
	// opening's own refusal, with no return before it but that refusal's.
	t.Run("structural", func(t *testing.T) {
		_, funcs, _ := f088ScanProvenance(t)
		returns := func(fn string) int {
			fd := funcs[fn]
			if fd == nil {
				t.Fatalf("%s is gone: re-derive this assertion", fn)
			}
			n := 0
			ast.Inspect(fd.Body, func(x ast.Node) bool {
				if _, ok := x.(*ast.ReturnStmt); ok {
					n++
				}
				return true
			})
			return n
		}
		for fn, frozen := range map[string]int{"openAdmissionCustodyLocked": 4, "openAdmission": 3} {
			if got := returns(fn); got != frozen {
				t.Errorf("%s has %d return statements, frozen at %d: an exit added to the opening can leave "+
					"a cancelled allocation's placeholder unsettled", fn, got, frozen)
			}
		}
		body := funcs["admitThroughAuthority"]
		if body == nil {
			t.Fatal("admitThroughAuthority is gone: re-derive this assertion")
		}
		deferAt, refusalAt, earlyReturns := -1, -1, 0
		for i, st := range body.Body.List {
			if d, ok := st.(*ast.DeferStmt); ok {
				if sel, ok := d.Call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "finishAdmissionCustody" {
					deferAt = i
					break
				}
			}
			if is, ok := st.(*ast.IfStmt); ok {
				if u, ok := is.Cond.(*ast.UnaryExpr); ok {
					if id, ok := u.X.(*ast.Ident); ok && id.Name == "opened" {
						refusalAt = i
						continue
					}
				}
			}
			ast.Inspect(st, func(x ast.Node) bool {
				if _, ok := x.(*ast.ReturnStmt); ok {
					earlyReturns++
				}
				return true
			})
		}
		if deferAt < 0 || refusalAt < 0 || deferAt != refusalAt+1 {
			t.Errorf("the deferred finishAdmissionCustody must follow the opening's refusal directly "+
				"(refusal at %d, defer at %d)", refusalAt, deferAt)
		}
		if earlyReturns != 0 {
			t.Errorf("%d return statement(s) precede the deferred settlement besides the opening's refusal", earlyReturns)
		}
	})
	for _, tc := range []struct {
		name         string
		afterOpening bool
	}{
		{"before opening", false},
		{"after opening, before the ladder", true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "f088-t41-"+map[bool]string{false: "pre", true: "post"}[tc.afterOpening])
			dur := e.sess.idHash + "_" + nuid.Next()
			sseq := e.publishSource(t, dur)
			e.admissionPrerequisites(t, dur, 1)
			e.acceptUnderServingAuthority(t)
			e.sess.mu.Lock()
			pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
			nonce := e.sess.pendingPublish[pi].regNonce
			gen := e.sess.gen
			e.sess.mu.Unlock()
			cancel := func() {
				e.sess.mu.Lock()
				e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
				e.sess.mu.Unlock()
			}
			var reached bool
			if tc.afterOpening {
				e.asm.mu.Lock()
				e.asm.testAdmitReserved = func(s *mqttSession, n string) {
					if n == nonce {
						reached = true
						cancel()
					}
				}
				e.asm.mu.Unlock()
			} else {
				cancel()
			}
			if e.sess.admitInflightPublish(e.c, pi, 1, dur, sseq, nonce, _EMPTY_, _EMPTY_) {
				t.Fatal("a cancelled allocation must not be admitted")
			}
			if tc.afterOpening && !reached {
				t.Fatal("fixture: the cancellation must land after the custody opening")
			}
			e.sess.mu.Lock()
			open := e.sess.custody.holdsAdmission(gen, nonce)
			e.sess.mu.Unlock()
			if open {
				t.Fatal("the admission must be settled: no permanently unsettled placeholder")
			}
		})
	}
}
