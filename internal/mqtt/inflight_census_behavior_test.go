package mqtt

import (
	"errors"
	"testing"

	"github.com/nats-io/nuid"
)

// testCensusSlot builds each supported slot shape from one exchange identity. Keeping
// this fixture here makes the census tests vary the phase without quietly varying the
// source, nonce, custody, or canonical PUBREL tuple at the same time.
func testCensusSlot(state mqttInflightState) (*mqttInflightRecord, string) {
	idHash, gen := getHash("census-client"), mqttNewSessGen()
	rec := freshRegisterLineage(&mqttInflightRecord{
		State: state, PI: 7, Dur: "SRC", Seq: 41, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	if mqttInflightPastPubRec(state) {
		rec.Claim, rec.ClaimSeq = mqttNewInflightNonce(), 3
		rec.PubRelDurable, rec.PubRelStreamSeq = "PUBREL", 71
	}
	if mqttInflightActionState(state) {
		rec.ActionToken = mqttNewInflightNonce()
		rec.ActionClaim, rec.ActionClaimSeq = rec.Claim, rec.ClaimSeq
	}
	return rec, mqttInflightSubject(idHash, gen, rec.PI)
}

func testCensusSessionForSlot(t *testing.T, rec *mqttInflightRecord, subject string, seq uint64) (*mqttSession, *[]uint64) {
	t.Helper()
	as := &mqttAccountSessionManager{}
	deleted := testPlantInflight(t, as, map[string]testInflightOccupant{
		subject: {rec: rec, seq: seq},
	}, nil)
	name, err := mqttParseInflightSubject(subject)
	if err != nil {
		t.Fatalf("parse fixture subject: %v", err)
	}
	sess := &mqttSession{
		asm: as, idHash: name.idHash, gen: name.gen,
		cancelledRegNonces: make(map[string]uint64),
	}
	return sess, deleted
}

// C20a/C20b: the generic reaper rejects a legal exchange in the wrong phase, while a
// permitted deletion targets the sequence returned by the subject read.
func TestMQTTGatedReapRefusesAnUnpermittedTransition(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPubRelReady)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	ref := mqttSlotRefFromRecord(subject, 91, rec)
	if sess.reapRegisterWithVerdictGated(91, rec.Nonce, false, ref, mqttSlotDeleteComplete) {
		t.Fatal("source completion must not reap a slot that is already past PUBREC")
	}
	if len(*deleted) != 0 {
		t.Fatalf("the refused reap issued deletes %v", *deleted)
	}
}

func TestF092GatedReapUsesSlotPhaseNotRetiredSourceFact(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPublish)
	rec.Retired = true
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	ref := mqttSlotRefFromRecord(subject, 91, rec)
	if !sess.reapRegisterWithVerdictGated(91, rec.Nonce, false, ref, mqttSlotDeleteComplete) {
		t.Fatal("a source-side Retired fact was mistaken for an explicit past-PUBREC slot phase")
	}
	if len(*deleted) != 1 || (*deleted)[0] != 91 {
		t.Fatalf("deletes = %v, want the PUBLISH slot at sequence 91", *deleted)
	}
}

func TestF092DrainReleaseProtectsExplicitPastPubRecPhase(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPubRelReady)
	rec.Retired = true
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	got := sess.asm.releaseInflightRegisterByIdentity(nil, sess, subject, rec.Dur, rec.Seq)
	if got != mqttInflightReleased {
		t.Fatalf("past-PUBREC drain release = %v, want handoff complete", got)
	}
	if len(*deleted) != 0 {
		t.Fatalf("drain handoff deleted the explicit past-PUBREC owner: %v", *deleted)
	}
}

func TestMQTTGatedReapDeletesOnlyThePermittedSequence(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPublish)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	ref := mqttSlotRefFromRecord(subject, 91, rec)
	// The legacy remembered sequence is deliberately different. The loaded subject
	// sequence is both the freshness observation and the deletion target.
	if !sess.reapRegisterWithVerdictGated(999, rec.Nonce, false, ref, mqttSlotDeleteComplete) {
		t.Fatal("a matching PUBLISH-phase completion must be reapable")
	}
	if len(*deleted) != 1 || (*deleted)[0] != 91 {
		t.Fatalf("deletes = %v, want only the subject's current sequence 91", *deleted)
	}
}

// C21a/C21b: the pending-entry helper has no fallback when its captured slot reference
// is absent, and delegates with that snapshot when it is present.
func TestMQTTDeleteInflightRegisterForFailsClosedWithNoCapturedSlot(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPublish)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	sess.deleteInflightRegisterFor(&mqttPending{regSeq: 91, regNonce: rec.Nonce}, mqttSlotDeleteComplete)
	if len(*deleted) != 0 {
		t.Fatalf("an entry with no captured slot issued deletes %v", *deleted)
	}
}

func TestMQTTDeleteInflightRegisterForCarriesItsEntrysReference(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPublish)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	ref := mqttSlotRefFromRecord(subject, 91, rec)
	sess.deleteInflightRegisterFor(&mqttPending{
		regSeq: 999, regNonce: rec.Nonce, slot: *ref,
	}, mqttSlotDeleteComplete)
	if len(*deleted) != 1 || (*deleted)[0] != 91 {
		t.Fatalf("deletes = %v, want the captured slot's current sequence 91", *deleted)
	}
}

// The PUBREC handoff owns the identifier for the whole exchange even if its canonical
// record readback fails and leaves the slot temporarily in PUBLISH for recovery route B.
// This is the release decision shared by every source-disposition arm in the real
// acknowledgement handler; changing the successful-PUBREC row back to true recreates
// the affine-completion stranding delete found during the final review.
func TestMQTTPublishAcknowledgementReleaseKeepsSuccessfulPubRecSlot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                             string
		isPubRec, failed, deferred, want bool
	}{
		{name: "PUBACK releases", want: true},
		{name: "rejected PUBREC releases", isPubRec: true, failed: true, want: true},
		{name: "successful PUBREC keeps route-B owner", isPubRec: true},
		{name: "deferred PUBACK leaves release to send settlement", deferred: true},
		{name: "deferred rejected PUBREC leaves release to send settlement", isPubRec: true, failed: true, deferred: true},
		{name: "deferred successful PUBREC keeps owner", isPubRec: true, deferred: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := mqttPublishAckMayReleaseSlot(tc.isPubRec, tc.failed, tc.deferred); got != tc.want {
				t.Fatalf("release = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestF092ConsumerPurgePreservesPastPubRecOwnerSlot(t *testing.T) {
	t.Parallel()
	const (
		dur    = "SRC"
		sseq   = uint64(41)
		regSeq = uint64(91)
		pi     = uint16(7)
	)
	key := mqttDeferredKey{jsDur: dur, sseq: sseq}
	sess := &mqttSession{
		srcAckOwed: map[mqttDeferredKey]*mqttRetiredSource{
			key: {pi: pi, regSeq: regSeq, nonce: mqttNewInflightNonce(), durable: true},
		},
	}

	regs, _ := sess.purgeConsumerPendingLocked(dur)
	if _, present := sess.srcAckOwed[key]; present {
		t.Fatal("consumer purge kept source suppression after its consumer was removed")
	}
	for _, reg := range regs {
		if reg.seq == regSeq {
			t.Fatalf("consumer purge queued the past-PUBREC owner slot for deletion: %+v", regs)
		}
	}
}

func TestF092ConsumerTeardownCannotDeletePastPubRecSlot(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPubRelReady)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	sess.cancelledRegNonces = make(map[string]uint64)

	unproven := sess.voidRegistersOnStream(&client{}, []mqttVoidedReg{{
		seq: 91, nonce: rec.Nonce, pi: rec.PI, dur: rec.Dur, sseq: rec.Seq,
	}})
	if !unproven {
		t.Fatal("a teardown refused by the past-PUBREC phase gate was reported fully removed")
	}
	if len(*deleted) != 0 {
		t.Fatalf("consumer teardown deleted a past-PUBREC owner slot: %v", *deleted)
	}
}

func TestF092ConsumerTeardownTombstonesSameExchangeRewrite(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("teardown-rewrite"), mqttNewSessGen()
	const pi = uint16(7)
	subject := mqttInflightSubject(idHash, gen, pi)
	current := mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: "SRC", Seq: 41, QoS: 1,
		Nonce: mqttNewInflightNonce(),
	}
	currentSeq := uint64(91)
	deletes, stores := 0, 0
	as := &mqttAccountSessionManager{}
	as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
		if got != subject {
			t.Fatalf("load subject = %q, want %q", got, subject)
		}
		copy := current
		return &copy, &StoredMsg{Subject: subject, Sequence: currentSeq}, nil
	}
	as.testInflightDelete = func(uint64) error {
		deletes++
		return nil
	}
	as.testInflightStore = func(got string, expect uint64, next *mqttInflightRecord) (uint64, error) {
		if got != subject || expect != currentSeq {
			t.Fatalf("cancellation CAS = %q/%d, want %q/%d", got, expect, subject, currentSeq)
		}
		stores++
		current = *next
		currentSeq++
		return currentSeq, nil
	}
	sess := &mqttSession{
		asm: as, idHash: idHash, gen: gen,
		cancelledRegNonces: map[string]uint64{current.Nonce: 1},
	}
	unproven := sess.voidRegistersOnStream(nil, []mqttVoidedReg{{
		seq: 71, nonce: current.Nonce, pi: pi, dur: current.Dur, sseq: current.Seq,
	}})
	if unproven || deletes != 0 || stores != 1 || !current.Cancelled {
		t.Fatalf("rewrite teardown: unproven=%v deletes=%d stores=%d current=%+v",
			unproven, deletes, stores, current)
	}
}

func TestF092PendingCancellationCannotRewritePastPubRecSlot(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPubRelReady)
	current := *rec
	stores := 0
	as := &mqttAccountSessionManager{}
	as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
		if got != subject {
			t.Fatalf("load subject = %q, want %q", got, subject)
		}
		copy := current
		return &copy, &StoredMsg{Subject: subject, Sequence: 91}, nil
	}
	as.testInflightStore = func(got string, expect uint64, next *mqttInflightRecord) (uint64, error) {
		stores++
		if got != subject || expect != 0 {
			t.Fatalf("initial create-only CAS = %q/%d, want %q/0", got, expect, subject)
		}
		return 0, errors.New("test: slot already exists")
	}
	name, err := mqttParseInflightSubject(subject)
	if err != nil {
		t.Fatalf("parse fixture subject: %v", err)
	}
	sess := &mqttSession{
		asm: as, idHash: name.idHash, gen: name.gen,
		regClaim: mqttNewInflightNonce(),
	}
	err = sess.markInflightRegisterCancelledPending(nil, mqttRegCancellation{
		pi: rec.PI, nonce: rec.Nonce, durable: rec.Dur, sseq: rec.Seq,
	})
	if err == nil {
		t.Fatal("pending cancellation rewrote a slot after it crossed PUBREC")
	}
	if stores != 1 || current.Cancelled {
		t.Fatalf("past-PUBREC cancellation stores=%d current=%+v", stores, current)
	}
}

func TestF092PubCompRefusesSlotBackedEntryWithoutCapturedSlot(t *testing.T) {
	t.Parallel()
	const pi = uint16(7)
	ack := &mqttPending{
		jsDur: "PUBREL", sseq: 41, jsAckSubject: "reply",
		regSeq: 91, regNonce: mqttNewInflightNonce(),
	}
	sess := &mqttSession{pendingPubRel: map[uint16]*mqttPending{pi: ack}}
	c := &client{mqtt: &mqtt{sess: sess}}
	sess.c = c

	c.mqttProcessPubComp(pi)
	sess.mu.Lock()
	got := sess.pendingPubRel[pi]
	sess.mu.Unlock()
	if got != ack {
		t.Fatalf("PUBCOMP removed a source-backed entry with no captured slot: got=%+v", got)
	}
}

func testResolvedRetirementDoesNotReapTerminal(t *testing.T, resolve bool) {
	t.Helper()
	rec, subject := testCensusSlot(mqttInflightPubRelTerminal)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	key := mqttDeferredKey{jsDur: rec.Dur, sseq: rec.Seq}
	sess.srcAckOwed = map[mqttDeferredKey]*mqttRetiredSource{
		key: {pi: rec.PI, regSeq: 91, nonce: rec.Nonce, durable: true},
	}
	if resolve {
		sess.asm.mu.Lock()
		sess.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
			return mqttInflightSourceComplete
		}
		sess.asm.mu.Unlock()
		if !sess.resolveRetiredSource(&client{}, key) {
			t.Fatal("a provably complete source retirement was not resolved")
		}
	} else {
		sess.clearRetiredSource(&client{}, key.jsDur, key.sseq)
	}
	if _, present := sess.srcAckOwed[key]; present {
		t.Fatal("the source-retirement bookkeeping was not removed")
	}
	if len(*deleted) != 0 {
		t.Fatalf("settling source bookkeeping bypassed terminal finalization: deletes %v", *deleted)
	}
}

func TestMQTTRetiredSourceBookkeepingRemovalIsSeparateFromTheReap(t *testing.T) {
	t.Parallel()
	testResolvedRetirementDoesNotReapTerminal(t, true)
}

func TestMQTTRetiredSourceReapRefusesAPubRelStateSlot(t *testing.T) {
	t.Parallel()
	testResolvedRetirementDoesNotReapTerminal(t, true)
}

func TestMQTTClearRetiredSourceDoesNotAuthorizeDeletion(t *testing.T) {
	t.Parallel()
	testResolvedRetirementDoesNotReapTerminal(t, false)
}

// C09: retirement is a source fact and therefore preserves every supported phase while
// setting exactly the Retired bit through the slot CAS.
func TestMQTTMarkRetiredIsLegalFromEveryKnownState(t *testing.T) {
	t.Parallel()
	for _, state := range []mqttInflightState{
		mqttInflightPublish,
		mqttInflightPubRelReady,
		mqttInflightPubRelSending,
		mqttInflightPubRelTerminalDraining,
		mqttInflightPubRelTerminal,
	} {
		state := state
		t.Run(string(state), func(t *testing.T) {
			rec, subject := testCensusSlot(state)
			name, err := mqttParseInflightSubject(subject)
			if err != nil {
				t.Fatalf("parse fixture subject: %v", err)
			}
			as := &mqttAccountSessionManager{}
			as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
				copy := *rec
				return &copy, &StoredMsg{Sequence: 91, Subject: subject}, nil
			}
			var stored *mqttInflightRecord
			as.testInflightStore = func(got string, expect uint64, next *mqttInflightRecord) (uint64, error) {
				if got != subject || expect != 91 {
					t.Fatalf("CAS target = %q/%d, want %q/91", got, expect, subject)
				}
				copy := *next
				stored = &copy
				return 92, nil
			}
			sess := &mqttSession{
				asm: as, idHash: name.idHash, gen: name.gen,
				regClaim: rec.Claim, seq: rec.ClaimSeq,
			}
			if seq, ok := sess.markInflightRegisterRetired(&client{}, rec.PI, rec.Dur, rec.Seq, 91,
				rec.Nonce, rec.Q2AffClaim, rec.Q2AffCopy); !ok || seq != 92 {
				t.Fatalf("mark from %q = (%d, %v), want (92, true)", state, seq, ok)
			}
			if stored == nil || !stored.Retired || stored.State != state {
				t.Fatalf("mark from %q stored %+v", state, stored)
			}
		})
	}
}

// C19: even the orphan-affine path must leave a resident terminal owner to its PUBCOMP
// finalizer; source completion is not permission to skip that finalizer's record scan.
func TestOrphanSweepCannotMutateAnActiveSessionSlot(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPubRelTerminal)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	name, err := mqttParseInflightSubject(subject)
	if err != nil {
		t.Fatalf("parse fixture subject: %v", err)
	}
	sess.asm.sessByHash = map[string]*mqttSession{name.idHash: sess}
	if sess.asm.disposeSettledAffineRegister(name, rec, 91) {
		t.Fatal("the orphan sweep reported a terminal session slot disposed")
	}
	if len(*deleted) != 0 {
		t.Fatalf("the orphan sweep issued deletes %v for a terminal session slot", *deleted)
	}
}

// C22: a failed PUBREL census is an incomplete identifier-space observation, not an
// empty one. This returns before any JetStream access, making the failure deterministic.
func TestMQTTInflightRecoveryScanFailureReservesTheIdentifierSpace(t *testing.T) {
	t.Parallel()
	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
		return nil, 0, false
	}
	sess := &mqttSession{
		asm: as, id: "census-scan", idHash: getHash("census-scan"), gen: mqttNewSessGen(),
		pubRelSubject: "PUBREL.scan",
	}
	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("a failed scan withholds allocation; it must not reject recovery: %v", err)
	}
	if !sess.piSpaceIncomplete {
		t.Fatal("a failed PUBREL scan was treated as an empty identifier space")
	}
}

// C22b: an enumerated register whose batched read fails is still a potentially held
// identifier. The read edge must run, and its indeterminate result must close the
// allocator rather than be interpreted as an absent slot.
func TestMQTTInflightRecoveryBatchReadFailureReservesTheIdentifierSpace(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("census-batch-read"), mqttNewSessGen()
	subject := mqttInflightSubject(idHash, gen, 7)
	loadCalled := false
	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
		return nil, 0, true
	}
	as.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return map[string]uint64{subject: 1}, nil
	}
	as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		loadCalled = true
		if len(subjects) != 1 || subjects[0] != subject {
			t.Fatalf("batch subjects = %v, want [%s]", subjects, subject)
		}
		return nil, errors.New("injected batch-read failure")
	}
	sess := &mqttSession{
		asm: as, id: "census-batch-read", idHash: idHash, gen: gen,
		pubRelSubject: "PUBREL.batch-read",
	}
	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("a failed batch read withholds allocation; it must not reject recovery: %v", err)
	}
	if !loadCalled {
		t.Fatal("recovery bypassed the protected batched-read edge")
	}
	if !sess.piSpaceIncomplete {
		t.Fatal("a failed batch read was treated as an empty identifier space")
	}
}

// C39a/C39b: identity release treats a past-PUBREC slot as handed to its owner and never
// sends either the first or a rewritten sequence through the PUBLISH-completion delete.
func TestMQTTIdentityReleaseInitialEdgeIsGated(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPubRelReady)
	sess, deleted := testCensusSessionForSlot(t, rec, subject, 91)
	if got := sess.asm.releaseInflightRegisterByIdentity(nil, sess, subject, rec.Dur, rec.Seq); got != mqttInflightReleased {
		t.Fatalf("past-PUBREC handoff outcome = %v, want Released", got)
	}
	if len(*deleted) != 0 {
		t.Fatalf("identity release deleted a past-PUBREC slot: %v", *deleted)
	}
}

func TestMQTTIdentityReleaseRewrittenEdgeIsGated(t *testing.T) {
	t.Parallel()
	rec, subject := testCensusSlot(mqttInflightPublish)
	ready := *rec
	ready.State = mqttInflightPubRelReady
	ready.Claim, ready.ClaimSeq = mqttNewInflightNonce(), 3
	ready.PubRelDurable, ready.PubRelStreamSeq = "PUBREL", 71
	as := &mqttAccountSessionManager{}
	reads := 0
	var deleted []uint64
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		reads++
		if reads == 1 {
			return rec, &StoredMsg{Sequence: 91, Subject: subject}, nil
		}
		return &ready, &StoredMsg{Sequence: 92, Subject: subject}, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		deleted = append(deleted, seq)
		return nil
	}
	sess := &mqttSession{asm: as, cancelledRegNonces: make(map[string]uint64)}
	if got := as.releaseInflightRegisterByIdentity(nil, sess, subject, rec.Dur, rec.Seq); got != mqttInflightReleased {
		t.Fatalf("rewritten past-PUBREC handoff outcome = %v, want Released", got)
	}
	if len(deleted) != 1 || deleted[0] != 91 {
		t.Fatalf("deletes = %v, want only the original PUBLISH sequence 91", deleted)
	}
}

func testRecoveryReapSeesLaterPhase(t *testing.T, cancelled, retiredRefresh bool) {
	t.Helper()
	e := testNewInflightEnv(t, "census-recovery-"+nuid.Next())
	dur := e.sess.idHash + "_" + nuid.Next()
	const sseq = uint64(41)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if retiredRefresh {
		if _, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, ack.regSeq, ack.regNonce, _EMPTY_, _EMPTY_); !ok {
			t.Fatal("mark the recovery fixture retired")
		}
	} else if cancelled {
		e.sess.mu.Lock()
		e.sess.voidRegNonceLocked(ack.regNonce)
		e.sess.mu.Unlock()
	}
	e.forgetInMemory()

	var classifications int
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		classifications++
		if retiredRefresh && classifications == 1 {
			return mqttInflightSourceLive
		}
		return mqttInflightSourceComplete
	}
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		rec, smsg, err := e.asm.jsa.loadInflightRecord(subject)
		if rec != nil {
			copy := *rec
			copy.State = mqttInflightPubRelReady
			copy.Claim, copy.ClaimSeq = mqttNewInflightNonce(), 3
			copy.PubRelDurable, copy.PubRelStreamSeq = "PUBREL", 71
			copy.Retired = retiredRefresh
			rec = &copy
		}
		return rec, smsg, err
	}
	e.asm.mu.Unlock()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.asm.mu.Lock()
	e.asm.testInflightLoad = nil
	e.asm.testInflightSourceState = nil
	e.asm.mu.Unlock()
	if rec, _ := e.loadRegister(t, pi); rec == nil {
		t.Fatal("a PUBLISH-side recovery drop deleted a slot that had crossed PUBREC")
	}
}

func TestMQTTRecoveryDropIsGated(t *testing.T) {
	t.Parallel()
	testRecoveryReapSeesLaterPhase(t, false, false)
}

func TestMQTTRecoveryDropByNonceIsGated(t *testing.T) {
	t.Parallel()
	testRecoveryReapSeesLaterPhase(t, true, false)
}

func TestMQTTRecoverySecondRetirementDropIsGated(t *testing.T) {
	t.Parallel()
	testRecoveryReapSeesLaterPhase(t, false, true)
}
