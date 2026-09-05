// The acceptance conditions for F-092 and F-093.
//
// A green suite is not evidence that either is fixed — it is evidence that nothing
// regressed. These tests are the positive half: each drives the production route and
// asserts the behaviour the findings name, and each carries a route oracle (`RR:`) so a
// handler that returned early cannot satisfy it by doing nothing.

package mqtt

import (
	"encoding/json"
	"fmt"
	"testing"
)

// testPubRelSlot reads the durable slot for one identifier out of the fixture's store.
func testPubRelSlot(t *testing.T, e *testInflightEnv, pi uint16) *mqttInflightRecord {
	t.Helper()
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	e.asm.mu.RLock()
	defer e.asm.mu.RUnlock()
	return e.pubRelSlots[mqttInflightSubject(idHash, gen, pi)]
}

func TestF092CanonicalPubRelProofMatchesTheWholeExchange(t *testing.T) {
	const nonce = "xxxxxxxxxxxxxxxxxxxxxx"
	slot := &mqttInflightRecord{
		State: mqttInflightPubRelReady, PI: 7, Dur: "SRC", Seq: 41, QoS: 2,
		Nonce: nonce, Claim: "claim", ClaimSeq: 1,
		PubRelDurable: "PUBREL", PubRelStreamSeq: 91,
	}
	exact := mqttStoredPubRelShape{
		pi: 7, owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: nonce},
		srcDur: "SRC", srcSeq: 41,
	}
	tests := []struct {
		name  string
		shape mqttStoredPubRelShape
		alter func(*mqttInflightRecord)
		want  bool
	}{
		{name: "exact", shape: exact, want: true},
		{name: "wrong source", shape: func() mqttStoredPubRelShape {
			s := exact
			s.srcSeq++
			return s
		}()},
		{name: "wrong nonce", shape: func() mqttStoredPubRelShape {
			s := exact
			s.owner.Nonce = "yyyyyyyyyyyyyyyyyyyyyy"
			return s
		}()},
		{name: "QoS 1 cannot own PUBREL", shape: exact,
			alter: func(r *mqttInflightRecord) { r.QoS = 1 }},
		{name: "retained shape cannot back a slot", shape: func() mqttStoredPubRelShape {
			s := exact
			s.srcDur, s.srcSeq = _EMPTY_, 0
			return s
		}()},
		{name: "matching affine reference", shape: func() mqttStoredPubRelShape {
			s := exact
			s.affRef = mqttPubRelAffineRef{State: mqttPubRelAffineRefValid, Claim: "A", Copy: "B"}
			return s
		}(), alter: func(r *mqttInflightRecord) { r.Q2AffClaim, r.Q2AffCopy = "A", "B" }, want: true},
		{name: "absent affine proof cannot back affine slot", shape: exact,
			alter: func(r *mqttInflightRecord) { r.Q2AffClaim, r.Q2AffCopy = "A", "B" }},
		{name: "malformed affine proof remains sendable but unacknowledgeable", shape: func() mqttStoredPubRelShape {
			s := exact
			s.affRef = mqttPubRelAffineRef{State: mqttPubRelAffineRefMalformed, Claim: "A"}
			return s
		}(), alter: func(r *mqttInflightRecord) { r.Q2AffClaim, r.Q2AffCopy = "A", "B" }, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur := *slot
			if tt.alter != nil {
				tt.alter(&cur)
			}
			if got := mqttStoredPubRelShapeMatchesSlot(tt.shape, &cur); got != tt.want {
				t.Fatalf("canonical match = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestF092UnreadableActionSlotNeverRetiresItsOnlyHolder(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("unreadable-action"), mqttNewSessGen()
	const pi = uint16(7)
	subject := mqttInflightSubject(idHash, gen, pi)
	name, err := mqttParseInflightSubject(subject)
	if err != nil {
		t.Fatalf("parse fixture subject: %v", err)
	}
	newFixture := func(state mqttSendActionState) (*mqttSession, *mqttSendAction) {
		as := &mqttAccountSessionManager{}
		as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
			if got != subject {
				t.Fatalf("load subject = %q, want %q", got, subject)
			}
			return nil, &StoredMsg{Subject: subject, Sequence: 91}, nil
		}
		act := &mqttSendAction{
			token: mqttNewInflightNonce(), state: state,
			slot: mqttInflightSlotRef{
				name: *name, seq: 71, nonce: mqttNewInflightNonce(),
				state: mqttInflightPubRelSending, dur: "SRC", sseq: 41,
			},
		}
		sess := &mqttSession{
			asm: as, gen: gen,
			sendsInFlight: map[uint16]*mqttSendHold{pi: {action: act}},
		}
		return sess, act
	}

	t.Run("ambiguous acquisition", func(t *testing.T) {
		sess, act := newFixture(mqttSendActionTentative)
		sess.resolveUnacquiredAction(nil, act)
		if act.drained || sess.sendsInFlight[pi] == nil || sess.sendsInFlight[pi].action != act {
			t.Fatal("an unreadable post-CAS slot retired the tentative action holder")
		}
	})
	t.Run("durable drain", func(t *testing.T) {
		sess, act := newFixture(mqttSendActionAcquired)
		sess.startActionDrain(nil, act)
		if act.drained || sess.sendsInFlight[pi] == nil || sess.sendsInFlight[pi].action != act {
			t.Fatal("an unreadable active slot retired the acquired action holder")
		}
	})
}

func TestF092PubCompDoesNotTreatUnreadableSlotAsAbsent(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("unreadable-terminal"), mqttNewSessGen()
	const pi = uint16(7)
	subject := mqttInflightSubject(idHash, gen, pi)
	name, err := mqttParseInflightSubject(subject)
	if err != nil {
		t.Fatalf("parse fixture subject: %v", err)
	}
	as := &mqttAccountSessionManager{}
	as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
		if got != subject {
			t.Fatalf("load subject = %q, want %q", got, subject)
		}
		return nil, &StoredMsg{Subject: subject, Sequence: 91}, nil
	}
	sess := &mqttSession{asm: as, gen: gen}
	ref := mqttInflightSlotRef{
		name: *name, seq: 71, nonce: mqttNewInflightNonce(),
		state: mqttInflightPubRelReady, dur: "SRC", sseq: 41,
	}
	if sess.mqttTerminalizeOnPubComp(nil, ref).completed() {
		t.Fatal("PUBCOMP reported an unreadable durable slot absent and releasable")
	}
}

func TestF092DuplicateDisposalRequiresTheDeliveredRecordsWholeExchange(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("duplicate-whole-exchange"), mqttNewSessGen()
	_, _, pubRelDurable := mqttPubRelNames(idHash, gen)
	claim, nonce := mqttNewInflightNonce(), mqttNewInflightNonce()
	cur := &mqttInflightRecord{
		State: mqttInflightPubRelReady, PI: 23, Dur: "SRC", Seq: 41, QoS: 2,
		Nonce: nonce, Claim: claim, ClaimSeq: 1,
		PubRelDurable: pubRelDurable, PubRelStreamSeq: 7,
	}
	subject := mqttInflightSubject(idHash, gen, cur.PI)
	as := &mqttAccountSessionManager{}
	loads := 0
	as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
		loads++
		if got != subject {
			t.Fatalf("slot load = %q, want %q", got, subject)
		}
		copy := *cur
		return &copy, &StoredMsg{Subject: subject, Sequence: 71}, nil
	}
	sess := &mqttSession{
		asm: as, id: "duplicate-whole-exchange", idHash: idHash, gen: gen,
		regClaim: claim, pubRelConsumer: &ConsumerConfig{Durable: pubRelDurable},
	}
	owner := mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: nonce}
	// Sequence 8 makes this look like a duplicate of canonical sequence 7. The source
	// mismatch must refuse it BEFORE duplicate disposal tries to load or acknowledge the
	// canonical record. This fixture intentionally has no mqttJSA, so entering that old
	// branch panics rather than papering over the route with a no-op assertion.
	act, admitted := sess.admitStoredPubRel(&client{}, cur.PI, owner,
		testInflightAckSubject(pubRelDurable, 8, 1),
		mqttPubRelCorrelation{SrcDur: "OTHER", SrcSeq: cur.Seq, Ok: true},
		mqttPubRelAffineRef{})
	if act != nil || admitted {
		t.Fatalf("mismatched duplicate admission = (%+v, %v), want refusal", act, admitted)
	}
	if loads != 1 {
		t.Fatalf("slot loads = %d, want only the admission load", loads)
	}
}

func TestF092RecoveryRouteBUsesThePresentExactPubRel(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("route-b-unit"), mqttNewSessGen()
	rec := &mqttInflightRecord{
		State: mqttInflightPublish, PI: 17, Dur: "SRC", Seq: 41, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 2,
	}
	subject := mqttInflightSubject(idHash, gen, rec.PI)
	current, currentSeq := *rec, uint64(71)
	as := &mqttAccountSessionManager{}
	as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
		if got != subject {
			t.Fatalf("load subject = %q, want %q", got, subject)
		}
		copy := current
		return &copy, &StoredMsg{Subject: subject, Sequence: currentSeq}, nil
	}
	as.testInflightStore = func(got string, expect uint64, next *mqttInflightRecord) (uint64, error) {
		if got != subject || expect != currentSeq {
			t.Fatalf("CAS target = %q/%d, want %q/%d", got, expect, subject, currentSeq)
		}
		current = *next
		currentSeq++
		return currentSeq, nil
	}
	_, _, pubRelDurable := mqttPubRelNames(idHash, gen)
	sess := &mqttSession{
		asm: as, idHash: idHash, gen: gen,
		pubRelConsumer: &ConsumerConfig{Durable: pubRelDurable},
	}
	rel := mqttStoredPubRel{
		seq: 113, pi: rec.PI, srcDur: rec.Dur, srcSeq: rec.Seq,
		owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: rec.Nonce},
	}
	got, ok := sess.phaseRecoveredPublishSlotToReady(&client{},
		mqttRecoveredInflightReg{rec: rec, seq: 71}, rel)
	if !ok {
		t.Fatal("an exact present PUBREL did not complete the recovery handoff")
	}
	if got.rec == nil || got.rec.State != mqttInflightPubRelReady ||
		got.rec.PubRelDurable != pubRelDurable || got.rec.PubRelStreamSeq != rel.seq {
		t.Fatalf("recovered slot = %+v, want ready canonical %s/%d", got.rec, pubRelDurable, rel.seq)
	}
	if got.rec.Claim != rec.Claim || got.rec.ClaimSeq != rec.ClaimSeq {
		t.Fatalf("route B rebuilt custody: got %q/%d, want %q/%d",
			got.rec.Claim, got.rec.ClaimSeq, rec.Claim, rec.ClaimSeq)
	}
	if current.State != mqttInflightPubRelReady || current.PubRelStreamSeq != rel.seq {
		t.Fatalf("stored route-B slot = %+v", current)
	}

	wrong := rel
	wrong.owner.Nonce = mqttNewInflightNonce()
	if _, ok := sess.phaseRecoveredPublishSlotToReady(&client{}, got, wrong); ok {
		t.Fatal("a non-matching PUBREL was accepted as route-B evidence")
	}
}

func TestF092RecoveryRoutesPublishPlusExactPubRelBeforeSourceCompletion(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("route-b-recovery"), mqttNewSessGen()
	_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
	rec := &mqttInflightRecord{
		State: mqttInflightPublish, PI: 18, Dur: "SRC", Seq: 42, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1,
	}
	subject := mqttInflightSubject(idHash, gen, rec.PI)
	current, currentSeq := *rec, uint64(81)
	rel := mqttStoredPubRel{
		seq: 121, pi: rec.PI, srcDur: rec.Dur, srcSeq: rec.Seq,
		owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: rec.Nonce},
	}
	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = func(got string) ([]mqttStoredPubRel, int, bool) {
		if got != pubRelSubject {
			t.Fatalf("PUBREL scan subject = %q, want %q", got, pubRelSubject)
		}
		return []mqttStoredPubRel{rel}, 0, true
	}
	as.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return map[string]uint64{subject: 1}, nil
	}
	as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		if len(subjects) != 1 || subjects[0] != subject {
			t.Fatalf("register load set = %v, want {%q}", subjects, subject)
		}
		data, err := json.Marshal(&current)
		if err != nil {
			t.Fatal(err)
		}
		return []*JSApiMsgGetResponse{{Message: &StoredMsg{
			Subject: subject, Sequence: currentSeq, Data: data,
		}}}, nil
	}
	as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
		if got != subject {
			t.Fatalf("load subject = %q, want %q", got, subject)
		}
		copy := current
		return &copy, &StoredMsg{Subject: subject, Sequence: currentSeq}, nil
	}
	as.testInflightStore = func(got string, expect uint64, next *mqttInflightRecord) (uint64, error) {
		if got != subject || expect != currentSeq {
			t.Fatalf("CAS target = %q/%d, want %q/%d", got, expect, subject, currentSeq)
		}
		current = *next
		currentSeq++
		return currentSeq, nil
	}
	as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	sess := &mqttSession{
		asm: as, id: "route-b-recovery", idHash: idHash, gen: gen,
		regClaim: mqttNewInflightNonce(), seq: 9,
		pubRelSubject: pubRelSubject, pubRelConsumer: &ConsumerConfig{Durable: pubRelDurable},
		cancelledRegNonces: make(map[string]uint64),
		pendingPublish: map[uint16]*mqttPending{
			rec.PI: {
				jsDur: rec.Dur, sseq: rec.Seq, regSeq: currentSeq, regNonce: rec.Nonce,
			},
		},
		cpending: map[string]map[uint64]uint16{rec.Dur: {rec.Seq: rec.PI}},
	}
	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if current.State != mqttInflightPubRelReady || current.PubRelDurable != pubRelDurable ||
		current.PubRelStreamSeq != rel.seq {
		t.Fatalf("durable route-B result = %+v", current)
	}
	if current.Claim != sess.regClaim || current.ClaimSeq != sess.seq {
		t.Fatalf("route-B custody = %q/%d, want %q/%d",
			current.Claim, current.ClaimSeq, sess.regClaim, sess.seq)
	}
	sess.mu.Lock()
	_, asPublish := sess.pendingPublish[rec.PI]
	pending := sess.pendingPubRel[rec.PI]
	incomplete := sess.piSpaceIncomplete
	sess.mu.Unlock()
	if asPublish || pending == nil {
		t.Fatalf("recovered maps: asPublish=%v pendingPubRel=%+v", asPublish, pending)
	}
	if incomplete {
		t.Fatal("an exact route-B handoff made the identifier space incomplete")
	}
}

func TestF092RecoveryDoesNotInferRouteBFromSourceOnly(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("route-b-source-only"), mqttNewSessGen()
	_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
	rec := &mqttInflightRecord{
		State: mqttInflightPublish, PI: 19, Dur: "SRC", Seq: 43, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1,
	}
	subject := mqttInflightSubject(idHash, gen, rec.PI)
	current, currentSeq := *rec, uint64(91)
	rel := mqttStoredPubRel{
		seq: 131, pi: rec.PI, srcDur: rec.Dur, srcSeq: rec.Seq,
		owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: mqttNewInflightNonce()},
	}
	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
		return []mqttStoredPubRel{rel}, 0, true
	}
	as.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return map[string]uint64{subject: 1}, nil
	}
	as.testInflightLoadMulti = func([]string) ([]*JSApiMsgGetResponse, error) {
		data, err := json.Marshal(&current)
		if err != nil {
			t.Fatal(err)
		}
		return []*JSApiMsgGetResponse{{Message: &StoredMsg{
			Subject: subject, Sequence: currentSeq, Data: data,
		}}}, nil
	}
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		copy := current
		return &copy, &StoredMsg{Subject: subject, Sequence: currentSeq}, nil
	}
	as.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		t.Fatal("source-only evidence must not rewrite the slot")
		return 0, nil
	}
	as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	sess := &mqttSession{
		asm: as, id: "route-b-source-only", idHash: idHash, gen: gen,
		regClaim: mqttNewInflightNonce(), seq: 9,
		pubRelSubject: pubRelSubject, pubRelConsumer: &ConsumerConfig{Durable: pubRelDurable},
		cancelledRegNonces: make(map[string]uint64),
		pendingPublish: map[uint16]*mqttPending{
			rec.PI: {
				jsDur: rec.Dur, sseq: rec.Seq, regSeq: currentSeq, regNonce: rec.Nonce,
			},
		},
		cpending: map[string]map[uint64]uint16{rec.Dur: {rec.Seq: rec.PI}},
	}
	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if current.State != mqttInflightPublish || current.PubRelStreamSeq != 0 {
		t.Fatalf("source-only evidence changed the owner slot: %+v", current)
	}
	if !sess.piSpaceIncomplete {
		t.Fatal("a source-only PUBREL mismatch did not quarantine the identifier space")
	}
	sess.mu.Lock()
	pending := sess.pendingPublish[rec.PI]
	_, mapped := sess.cpending[rec.Dur][rec.Seq]
	sess.mu.Unlock()
	if pending == nil || pending.jsDur != _EMPTY_ || pending.sseq != 0 || mapped {
		t.Fatalf("source-only conflict left a resident PUBLISH entry sendable: pending=%+v mapped=%v",
			pending, mapped)
	}
}

func TestF092FailedRouteBHandoffDemotesResidentPublishEntry(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("route-b-failed-cas"), mqttNewSessGen()
	_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
	rec := &mqttInflightRecord{
		State: mqttInflightPublish, PI: 21, Dur: "SRC", Seq: 45, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1,
	}
	subject := mqttInflightSubject(idHash, gen, rec.PI)
	currentSeq := uint64(111)
	rel := mqttStoredPubRel{
		seq: 141, pi: rec.PI, srcDur: rec.Dur, srcSeq: rec.Seq,
		owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: rec.Nonce},
	}
	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
		return []mqttStoredPubRel{rel}, 0, true
	}
	as.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return map[string]uint64{subject: 1}, nil
	}
	as.testInflightLoadMulti = func([]string) ([]*JSApiMsgGetResponse, error) {
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		return []*JSApiMsgGetResponse{{Message: &StoredMsg{
			Subject: subject, Sequence: currentSeq, Data: data,
		}}}, nil
	}
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		copy := *rec
		return &copy, &StoredMsg{Subject: subject, Sequence: currentSeq}, nil
	}
	as.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("injected route-B CAS failure")
	}
	sess := &mqttSession{
		asm: as, id: "route-b-failed-cas", idHash: idHash, gen: gen,
		regClaim: mqttNewInflightNonce(), seq: 9,
		pubRelSubject: pubRelSubject, pubRelConsumer: &ConsumerConfig{Durable: pubRelDurable},
		cancelledRegNonces: make(map[string]uint64),
		pendingPublish: map[uint16]*mqttPending{
			rec.PI: {
				jsDur: rec.Dur, sseq: rec.Seq, regSeq: currentSeq, regNonce: rec.Nonce,
			},
		},
		cpending: map[string]map[uint64]uint16{rec.Dur: {rec.Seq: rec.PI}},
	}
	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	sess.mu.Lock()
	pending := sess.pendingPublish[rec.PI]
	_, mapped := sess.cpending[rec.Dur][rec.Seq]
	incomplete := sess.piSpaceIncomplete
	sess.mu.Unlock()
	if pending == nil || pending.jsDur != _EMPTY_ || pending.sseq != 0 || mapped || !incomplete {
		t.Fatalf("failed route B left a resident PUBLISH entry sendable: pending=%+v mapped=%v incomplete=%v",
			pending, mapped, incomplete)
	}
}

func TestF092RecoveryReservesRetiredPublishSlotWithoutExactPubRel(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("route-b-retired-without-record"), mqttNewSessGen()
	_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
	rec := &mqttInflightRecord{
		State: mqttInflightPublish, PI: 20, Dur: "SRC", Seq: 44, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1, Retired: true,
	}
	subject := mqttInflightSubject(idHash, gen, rec.PI)
	currentSeq := uint64(101)
	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
		return nil, 0, true
	}
	as.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return map[string]uint64{subject: 1}, nil
	}
	as.testInflightLoadMulti = func([]string) ([]*JSApiMsgGetResponse, error) {
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		return []*JSApiMsgGetResponse{{Message: &StoredMsg{
			Subject: subject, Sequence: currentSeq, Data: data,
		}}}, nil
	}
	as.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		t.Fatal("a retired PUBLISH slot without an exact PUBREL must not be rewritten or deleted")
		return 0, nil
	}
	as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceLive
	}
	sess := &mqttSession{
		asm: as, id: "route-b-retired-without-record", idHash: idHash, gen: gen,
		regClaim: mqttNewInflightNonce(), seq: 9,
		pubRelSubject: pubRelSubject, pubRelConsumer: &ConsumerConfig{Durable: pubRelDurable},
		cancelledRegNonces: make(map[string]uint64),
		pendingPublish: map[uint16]*mqttPending{
			rec.PI: {
				jsDur: rec.Dur, sseq: rec.Seq, regSeq: currentSeq, regNonce: rec.Nonce,
			},
		},
		cpending: map[string]map[uint64]uint16{rec.Dur: {rec.Seq: rec.PI}},
	}
	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	sess.mu.Lock()
	pending := sess.pendingPubRel[rec.PI]
	_, deliverable := sess.pendingPublish[rec.PI]
	_, sourceMapped := sess.cpending[rec.Dur][rec.Seq]
	incomplete := sess.piSpaceIncomplete
	sess.mu.Unlock()
	if pending == nil || pending.jsAckSubject != _EMPTY_ || pending.regNonce != rec.Nonce ||
		pending.slot.state != mqttInflightPublish {
		t.Fatalf("retired PUBLISH slot did not retain a non-actionable reservation: %+v", pending)
	}
	if deliverable {
		t.Fatal("retired PUBLISH slot remained deliverable through pendingPublish")
	}
	if sourceMapped {
		t.Fatal("retired PUBLISH slot kept a source mapping that can retransmit its payload")
	}
	if incomplete {
		t.Fatal("a fully identified retired slot must not make unrelated packet identifiers unavailable")
	}
}

func TestF092CurrentConnectionDebtResetPreservesTheDurableAction(t *testing.T) {
	const pi = uint16(8)
	current := &client{cid: 22}
	action := &mqttSendAction{token: "aaaaaaaaaaaaaaaaaaaaaa", state: mqttSendActionAcquired}
	sess := &mqttSession{
		c: current,
		sendsInFlight: map[uint16]*mqttSendHold{
			pi: {owner: 11, pubCompOwed: 1, action: action},
		},
	}
	token, ok := sess.beginPubRelSendLocked(current, pi)
	if !ok || token == _EMPTY_ {
		t.Fatal("the current connection must be able to replace a predecessor's inert debt")
	}
	hold := sess.sendsInFlight[pi]
	if hold == nil || hold.action != action {
		t.Fatalf("debt replacement dropped the non-connection-owned action: %+v", hold)
	}
	if hold.owner != current.cid || hold.pubCompOwed != 1 {
		t.Fatalf("replacement debt = owner %d count %d, want %d/1",
			hold.owner, hold.pubCompOwed, current.cid)
	}
}

func TestF093HandoffRefusesAConflictingPublishExchange(t *testing.T) {
	const pi = uint16(9)
	action := &mqttSendAction{slot: mqttInflightSlotRef{
		name: mqttInflightName{pi: pi}, nonce: "aaaaaaaaaaaaaaaaaaaaaa",
		dur: "SRC", sseq: 41, state: mqttInflightPubRelSending,
	}}
	pub := &mqttPending{
		jsDur: "SRC", sseq: 41, regNonce: "bbbbbbbbbbbbbbbbbbbbbb",
	}
	rel := &mqttPending{regNonce: action.slot.nonce}
	sess := &mqttSession{
		pendingPublish: map[uint16]*mqttPending{pi: pub},
		pendingPubRel:  map[uint16]*mqttPending{pi: rel},
		cpending:       map[string]map[uint64]uint16{"SRC": {41: pi}},
	}
	if sess.handOffPublishEntryLocked(pi, action) {
		t.Fatal("a PUBLISH entry from another exchange was accepted into the PUBREL handoff")
	}
	if sess.pendingPublish[pi] != pub || sess.pendingPubRel[pi] != rel ||
		sess.cpending["SRC"][41] != pi {
		t.Fatal("a refused cross-exchange handoff mutated one of its participants")
	}
}

// TestF092StalePubRelUnderAnotherExchangesIdentifierIsRefused is F-092's acceptance.
//
// THE FINDING: a stale stored PUBREL whose acknowledgement was lost stays AckWait-eligible
// while nothing names its identifier. After the identifier is reused, its redelivery used
// to reach the send section and put PUBREL bytes on the wire under an identifier a
// DIFFERENT exchange owns — and the client's PUBCOMP then completed the wrong exchange.
//
// The fix is not an inference about which entry holds the identifier. Five plan revisions
// tried that and each was refuted. There is exactly one authoritative claimant and it is
// the durable slot, so the record is refused because the slot names a different exchange.
//
// `RR:` the refusal is asserted by the slot being UNCHANGED and no transmission having
// happened — a handler that returned before ever comparing would leave the same end state,
// so the test also asserts the matching-nonce case DOES transmit on the same fixture. One
// arm alone proves nothing; the pair discriminates.
func TestF092StalePubRelUnderAnotherExchangesIdentifierIsRefused(t *testing.T) {
	e := testNewInflightEnv(t, "f092-stale")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(4)
	testSeedPubRelReadySlot(t, e, pi)

	// The slot names exchange A. The record names exchange B — a stale record from an
	// exchange that used this identifier earlier and whose acknowledgement was lost.
	const foreign = "yyyyyyyyyyyyyyyyyyyyyy"
	before := testPubRelSlot(t, e, pi)
	if before == nil || before.Nonce == foreign {
		t.Fatalf("the fixture must seed a slot naming a DIFFERENT exchange than the record: %+v", before)
	}

	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1), foreign)
	})
	if writes != 0 {
		t.Errorf("a stale PUBREL naming exchange %q was transmitted under an identifier whose slot "+
			"names %q: %d write(s). That is F-092 itself", foreign, before.Nonce, writes)
	}

	// The slot is untouched: no action was acquired and no phase moved.
	after := testPubRelSlot(t, e, pi)
	if after == nil {
		t.Fatal("the refusal must not delete the slot: the exchange it names is still live")
	}
	if after.State != mqttInflightPubRelReady || after.ActionToken != _EMPTY_ {
		t.Errorf("the refusal mutated the slot: state=%q action=%q", after.State, after.ActionToken)
	}

	// THE NOT-ACKNOWLEDGED HALF is deliberately not asserted here. It is what makes
	// refusing safe — AckWait re-offers the record, so a refusal cannot lose a PUBREL the
	// client is owed — but this fixture has no seam that observes an acknowledgement, and
	// adding a production hook to count one would be test machinery in the broker. It is
	// covered where it IS observable: the e2e drain tests, whose repeated redeliveries are
	// visible precisely because nothing acknowledges the refused record.

	// THE DISCRIMINATING ARM: the same fixture, the same identifier, the record naming the
	// exchange the slot actually owns. This must transmit, or the arm above would pass
	// against a handler that refuses everything.
	writes = testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 2), before.Nonce)
	})
	if writes != 1 {
		t.Errorf("the exchange's OWN PUBREL was not transmitted (%d write(s)); the refusal above is "+
			"then not discriminating", writes)
	}
}

// TestF092AdmissionRequiresAnAtomicallyAcquiredAction pins the invariant that replaced
// "the slot must already say pubrel_ready".
//
// The corrected invariant is: NO SLOT-BACKED PUBREL BYTES ARE TRANSMITTED WITHOUT FIRST
// ATOMICALLY ACQUIRING `pubrel_sending` (a live retained exchange has no slot, R6, and is
// authorized by its live in-memory owner instead). The weaker version was wrong and cost
// the ported suite —
// the store makes a record deliverable and JetStream delivers it before the PUBREC
// handler's own phase CAS can run, so requiring `pubrel_ready` refused the very PUBREL the
// PUBREC had just created and every exchange waited out AckWait.
//
// So a `publish` slot with a MATCHING nonce must transmit, via the direct
// `publish -> pubrel_sending` arrow, and the slot must end up carrying the canonical
// record identity the delivery supplied.
func TestF092AdmissionRequiresAnAtomicallyAcquiredAction(t *testing.T) {
	e := testNewInflightEnv(t, "f092-direct")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(6)
	testSeedPubRelReadySlot(t, e, pi)

	// Put the slot back into `publish`, which is what the callback sees when it wins the
	// race against the PUBREC handler's phase CAS.
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	e.asm.mu.Lock()
	rec := e.pubRelSlots[subject]
	rec.State = mqttInflightPublish
	rec.PubRelDurable, rec.PubRelStreamSeq = _EMPTY_, 0
	nonce := rec.Nonce
	e.asm.mu.Unlock()

	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1), nonce)
	})
	if writes != 1 {
		t.Fatalf("a valid same-nonce PUBREL on a `publish` slot must transmit through the direct "+
			"arrow, got %d write(s): requiring `pubrel_ready` is the AckWait regression", writes)
	}
	// `RR:` the direct arrow is what ran, evidenced by the canonical identity the delivery
	// supplied now being on the slot. A handler that transmitted without acquiring would
	// leave the slot in `publish` with no canonical tuple.
	after := testPubRelSlot(t, e, pi)
	if after == nil {
		t.Fatal("the slot must survive the transmission")
	}
	if after.PubRelDurable == _EMPTY_ || after.PubRelStreamSeq == 0 {
		t.Errorf("the direct arrow must install the canonical record identity atomically with the "+
			"action; got durable=%q seq=%d", after.PubRelDurable, after.PubRelStreamSeq)
	}
}

// TestF092ActionAcquisitionDoesNotFollowACustodyRewrite closes the race between
// admission's current-owner check and the action CAS. A successor may transfer a ready
// slot after the check; the predecessor must not follow that rewrite and bind a new action
// to custody it does not own.
func TestF092ActionAcquisitionDoesNotFollowACustodyRewrite(t *testing.T) {
	e := testNewInflightEnv(t, "f092-action-custody-race")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(24)
	testSeedPubRelReadySlot(t, e, pi)

	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading ready slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	stale := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	if stale == nil {
		t.Fatal("the ready slot must yield a reference")
	}
	const successorClaim = "successor-custody"
	if _, ok := e.sess.casInflightSlot(e.c, *stale, mqttInflightPubRelReady,
		mqttSlotTransferCustody, func(next *mqttInflightRecord) {
			next.Claim, next.ClaimSeq = successorClaim, 99
		}); !ok {
		t.Fatal("transferring the fixture to successor custody")
	}

	if act := e.sess.mqttAcquirePubRelAction(e.c, *stale); act != nil {
		t.Fatal("a stale custodian followed the successor's rewrite and acquired an action")
	}
	after, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || after == nil {
		t.Fatalf("loading slot after refused acquisition: rec=%+v err=%v", after, err)
	}
	if after.Claim != successorClaim || after.State != mqttInflightPubRelReady ||
		after.ActionToken != _EMPTY_ {
		t.Fatalf("refused acquisition mutated successor custody: %+v", after)
	}
}

// TestF092ActionDrainRequiresTheExactToken proves the token check is part of the CAS
// predicate. Letting a foreign holder reach a no-op mutator still rewrites the slot and
// moves the freshness fence under the real holder.
func TestF092ActionDrainRequiresTheExactToken(t *testing.T) {
	e := testNewInflightEnv(t, "f092-action-drain-token")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(25)
	testSeedPubRelReadySlot(t, e, pi)

	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading ready slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	if ref == nil {
		t.Fatal("the ready slot must yield a reference")
	}
	act := e.sess.mqttAcquirePubRelAction(e.c, *ref)
	if act == nil {
		t.Fatal("acquiring the fixture action")
	}
	before, beforeMsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || before == nil || beforeMsg == nil {
		t.Fatalf("loading active slot: rec=%+v msg=%+v err=%v", before, beforeMsg, err)
	}
	foreign := act.slot
	foreign.actionToken = "yyyyyyyyyyyyyyyyyyyyyy"
	if _, ok := e.sess.casInflightSlot(e.c, foreign, mqttInflightPubRelSending,
		mqttSlotDrainAction, func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelReady
			next.ActionToken, next.ActionClaim, next.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
		}); ok {
		t.Fatal("a foreign action token was allowed to rewrite the active slot")
	}
	after, afterMsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || after == nil || afterMsg == nil {
		t.Fatalf("loading preserved active slot: rec=%+v msg=%+v err=%v", after, afterMsg, err)
	}
	if afterMsg.Sequence != beforeMsg.Sequence || after.ActionToken != act.token ||
		after.State != mqttInflightPubRelSending {
		t.Fatalf("foreign drain moved or changed the active slot: before=%+v/%d after=%+v/%d",
			before, beforeMsg.Sequence, after, afterMsg.Sequence)
	}
	e.sess.startActionDrain(e.c, act)
}

// TestF092DurableCanonicalOutranksStaleInMemorySequence ensures the pre-F-092 duplicate
// check cannot override the owner slot after admission has selected its canonical record.
// The stale entry is memory only; acknowledging the slot's canonical because of it would
// leave the exchange permanently naming a deleted record.
func TestF092DurableCanonicalOutranksStaleInMemorySequence(t *testing.T) {
	e := testNewInflightEnv(t, "f092-canonical-over-memory")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(27)
	testSeedPubRelReadySlot(t, e, pi)
	nonce := testPubRelSlot(t, e, pi).Nonce

	e.sess.mu.Lock()
	if e.sess.pendingPubRel == nil {
		e.sess.pendingPubRel = make(map[uint16]*mqttPending, 1)
	}
	e.sess.pendingPubRel[pi] = &mqttPending{
		jsDur: "PUBREL_F089", sseq: testCanonicalPubRelSeq + 1,
		jsAckSubject: testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq+1, 1),
		regNonce:     nonce,
	}
	e.sess.mu.Unlock()

	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi,
			testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 2), nonce)
	})
	if writes != 1 {
		t.Fatalf("the durable canonical was suppressed by a stale in-memory sequence: %d writes", writes)
	}
}

// TestF092RetainedPubRelIsRefusedWithoutItsLiveOwner is R6's acceptance, and it is the case
// slice 1 had to ship as still-broken.
//
// A retained QoS 2 exchange has NO in-flight register at all, so nothing durable can
// authorize its PUBREL. Its authority is the live in-memory owner identity of this
// incarnation — which is exactly why a retained record surviving a restart must be refused:
// that identity died with the process.
//
// Refused, and NOT acknowledged: acknowledging it away would destroy a record an operator
// may need to see, and the cost of keeping it is stream space rather than an identifier.
func TestF092RetainedPubRelIsRefusedWithoutItsLiveOwner(t *testing.T) {
	e := testNewInflightEnv(t, "f092-retained")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(9)
	// NO slot for this identifier: that is the retained shape.

	const owner = "zzzzzzzzzzzzzzzzzzzzzz"
	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1), owner)
	})
	if writes != 0 {
		t.Errorf("a retained PUBREL with no live owner was transmitted (%d write(s)); across a "+
			"restart nothing can authorize it", writes)
	}

	// THE DISCRIMINATING ARM: with the live owner present, the same record transmits. This
	// is what stops the arm above passing against a handler that refuses every slot-less
	// record unconditionally.
	e.sess.mu.Lock()
	if e.sess.pendingPubRel == nil {
		e.sess.pendingPubRel = make(map[uint16]*mqttPending, 1)
	}
	e.sess.pendingPubRel[pi] = &mqttPending{jsDur: "PUBREL_F089", regNonce: owner}
	e.sess.mu.Unlock()

	writes = testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 2), owner)
	})
	if writes != 1 {
		t.Errorf("a retained PUBREL WITH its live owner must transmit, got %d write(s); the refusal "+
			"above is then not discriminating", writes)
	}
}

func TestF092RetainedPubRecHandoffCarriesItsLiveOwner(t *testing.T) {
	const pi = uint16(9)
	const owner = "zzzzzzzzzzzzzzzzzzzzzz"
	sess := &mqttSession{
		id:             "f092-retained-handoff",
		pubRelConsumer: &ConsumerConfig{Durable: "PUBREL_F092_RETAINED"},
		pendingPublish: map[uint16]*mqttPending{
			pi: {regNonce: owner},
		},
	}

	sess.mu.Lock()
	sess.trackAsPubRel(pi, _EMPTY_)
	rel := sess.pendingPubRel[pi]
	sess.mu.Unlock()

	if rel == nil || rel.regNonce != owner {
		t.Fatalf("retained PUBREC handoff owner = %#v, want nonce %q", rel, owner)
	}
	if !sess.retainedPubRelAdmittedLocked_(nil, pi,
		mqttPubRelOwner{Nonce: owner, State: mqttPubRelOwnerValid}) {
		t.Fatal("the retained PUBREL produced by the live handoff was refused")
	}
}

func TestF092RetainedOwnerCannotOverrideUnreadableDurableSlot(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("retained-vs-unreadable"), mqttNewSessGen()
	const (
		pi    = uint16(9)
		owner = "zzzzzzzzzzzzzzzzzzzzzz"
	)
	subject := mqttInflightSubject(idHash, gen, pi)
	as := &mqttAccountSessionManager{}
	as.testInflightLoad = func(got string) (*mqttInflightRecord, *StoredMsg, error) {
		if got != subject {
			t.Fatalf("load subject = %q, want %q", got, subject)
		}
		return nil, &StoredMsg{Subject: subject, Sequence: 91}, nil
	}
	sess := &mqttSession{
		asm: as, idHash: idHash, gen: gen,
		pendingPubRel: map[uint16]*mqttPending{pi: {regNonce: owner}},
	}
	act, admitted := sess.admitStoredPubRel(nil, pi,
		mqttPubRelOwner{Nonce: owner, State: mqttPubRelOwnerValid}, _EMPTY_,
		mqttPubRelCorrelation{}, mqttPubRelAffineRef{})
	if act != nil || admitted {
		t.Fatalf("unreadable durable occupant admitted as retained: action=%+v admitted=%v", act, admitted)
	}
}

// TestF092TerminalReleaseFreesTheIdentifier is the other half of the pair the fifth attempt
// withdrew.
//
// Re-phasing the slot past PUBREC without a terminal release leaks an identifier per QoS 2
// message, which is strictly worse than the hazard it fixes — that is exactly why the fifth
// attempt withdrew the transition rather than shipping it half-done. So the acceptance is
// that a completed exchange leaves NOTHING: no slot, and the identifier allocatable again.
func TestF092TerminalReleaseFreesTheIdentifier(t *testing.T) {
	e := testNewInflightEnv(t, "f092-terminal")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(3)
	testSeedPubRelReadySlot(t, e, pi)
	nonce := testPubRelSlot(t, e, pi).Nonce

	testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1), nonce)
	if slot := testPubRelSlot(t, e, pi); slot == nil {
		t.Fatal("the slot must survive the transmission; the exchange is not over until PUBCOMP")
	}
	// With Receive Maximum exhausted, a source delivery is parked. The terminal shadow
	// must keep it parked while cleanup is unconfirmed, then its release must wake it just
	// like the ordinary PUBCOMP path did before the shadow existed.
	e.sess.mu.Lock()
	e.sess.rmax = 1
	deferredKey := mqttDeferredKey{"SRC-DEFERRED", 99}
	if e.sess.deferred == nil {
		e.sess.deferred = make(map[mqttDeferredKey]string)
	}
	e.sess.deferred[deferredKey] = _EMPTY_
	e.sess.mu.Unlock()

	e.c.mqttProcessPubComp(pi)

	if slot := testPubRelSlot(t, e, pi); slot != nil {
		t.Errorf("the completed exchange left its slot behind in state %q: a slot that outlives its "+
			"exchange reserves an identifier for the life of the session", slot.State)
	}
	e.sess.mu.Lock()
	_, terminalReservationLeft := e.sess.pendingPubRel[pi]
	_, deferredLeft := e.sess.deferred[deferredKey]
	e.sess.mu.Unlock()
	if terminalReservationLeft {
		t.Errorf("the confirmed terminal cleanup left packet identifier %d reserved in memory", pi)
	}
	if deferredLeft {
		t.Error("terminal reservation release did not wake the delivery parked at Receive Maximum")
	}
	if !testMintsPI(t, e.sess, pi) {
		t.Errorf("packet identifier %d was not released by the completed exchange", pi)
	}
}

// Live terminal cleanup uses the installed consumer's durable. Recovery has no installed
// config yet and falls back to the generation-derived name, but requiring that derived name
// even while a live consumer is present rejects a valid slot before its exact delete.
func TestF092TerminalFinalizerUsesInstalledConsumerDurable(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("terminal-installed-durable"), mqttNewSessGen()
	const pi = uint16(3)
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := &mqttInflightRecord{
		State: mqttInflightPubRelTerminal, PI: pi, Dur: "SRC", Seq: 41, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: mqttNewInflightNonce(), ClaimSeq: 1,
		PubRelDurable: "PUBREL_FIXTURE", PubRelStreamSeq: 7,
	}
	as := &mqttAccountSessionManager{}
	deleted := testPlantInflight(t, as, map[string]testInflightOccupant{
		subject: {rec: rec, seq: 91},
	}, nil)
	as.testInflightPubRelScan = func(got string) ([]mqttStoredPubRel, int, bool) {
		if got != "terminal.installed.durable" {
			t.Fatalf("PUBREL scan subject = %q", got)
		}
		return nil, 0, true
	}
	as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	sess := &mqttSession{
		asm: as, idHash: idHash, gen: gen,
		pubRelSubject:      "terminal.installed.durable",
		pubRelConsumer:     &ConsumerConfig{Durable: rec.PubRelDurable},
		cancelledRegNonces: make(map[string]uint64),
	}
	ref := mqttSlotRefFromRecord(subject, 91, rec)
	if ref == nil || !sess.finalizeTerminalSlot(nil, *ref).completed() {
		t.Fatal("terminal cleanup refused the installed PUBREL consumer durable")
	}
	if len(*deleted) != 1 || (*deleted)[0] != 91 {
		t.Fatalf("terminal deletes = %v, want [91]", *deleted)
	}
}

// TestF092TerminalRecoveryDoesNotRequireTheConsumerConfig covers the pre-CONNACK
// ordering: register recovery runs before the PUBREL consumer is restored. A terminal
// slot must therefore derive the generation-scoped durable name instead of requiring the
// not-yet-installed in-memory config.
func TestF092TerminalRecoveryDoesNotRequireTheConsumerConfig(t *testing.T) {
	e := testNewInflightEnv(t, "f092-terminal-pre-consumer")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(26)
	testSeedPubRelReadySlot(t, e, pi)

	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	_, _, durable := mqttPubRelNames(idHash, gen)
	e.sess.pubRelConsumer = nil
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	e.asm.mu.Lock()
	e.pubRelSlots[subject].PubRelDurable = durable
	e.asm.mu.Unlock()
	// A completed unsubscribe drain removes its source consumer before the client can
	// answer PUBCOMP. Lost is not Complete and may never reap a PUBLISH-phase slot, but at
	// terminal it positively means the source cannot be re-offered and must not make the
	// drained identifier immortal.
	if _, err := e.sess.jsa.deleteConsumer(mqttStreamName, "SRC", true); err != nil {
		t.Fatalf("deleting the completed drain's source consumer: %v", err)
	}

	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading ready slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	if ref == nil {
		t.Fatal("the ready slot must yield a reference")
	}
	e.sess.mqttTerminalizeOnPubComp(e.c, *ref)
	if slot := testPubRelSlot(t, e, pi); slot != nil {
		t.Fatalf("terminal recovery retained the slot until consumer restore: %+v", slot)
	}
}

// TestF092TerminalSlotRetainsAnUnfinishedSourceWithoutRetiredBit covers the crash
// between the durable phase transition and the later Retired rewrite. The phase itself
// proves that the client has answered the source; deleting the terminal slot while that
// source is still live would erase the only cross-restart suppression evidence.
func TestF092TerminalSlotRetainsAnUnfinishedSourceWithoutRetiredBit(t *testing.T) {
	e := testNewInflightEnv(t, "f092-terminal-unfinished-source")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(27)
	testSeedPubRelReadySlot(t, e, pi)

	stored, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+"f092.source", 0, []byte("source"))
	if err != nil {
		t.Fatalf("storing the unfinished source: %v", err)
	}
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	e.asm.mu.Lock()
	e.pubRelSlots[subject].Seq = stored.Sequence
	if e.pubRelSlots[subject].Retired {
		e.asm.mu.Unlock()
		t.Fatal("the fixture must exercise the pre-Retired crash window")
	}
	e.asm.mu.Unlock()

	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading the ready slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	if ref == nil {
		t.Fatal("the ready slot must yield a reference")
	}
	if e.sess.mqttTerminalizeOnPubComp(e.c, *ref).completed() {
		t.Fatal("terminal cleanup reported success while the source was still live")
	}
	if slot := testPubRelSlot(t, e, pi); slot == nil ||
		slot.State != mqttInflightPubRelTerminal || slot.Retired {
		t.Fatalf("the unfinished, not-yet-marked source must retain its terminal slot: %+v", slot)
	}
}

// TestF092IncompleteTerminalDuplicateScanRetainsTheSlot is the fail-closed half of
// terminal cleanup. An unreadable tail is not evidence that no duplicate remains, so the
// terminal slot must stay as the durable owner.
func TestF092IncompleteTerminalDuplicateScanRetainsTheSlot(t *testing.T) {
	e := testNewInflightEnv(t, "f092-terminal-scan")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(21)
	testSeedPubRelReadySlot(t, e, pi)

	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading seeded slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	restore := setMQTTTestLoadNextMsgHook(func(stream, scanSubject string, _ uint64) error {
		if stream == mqttOutStreamName && scanSubject == e.sess.pubRelSubject {
			return fmt.Errorf("test: incomplete terminal scan")
		}
		return nil
	})
	defer restore()

	if e.sess.mqttTerminalizeOnPubComp(e.c, *ref).completed() {
		t.Fatal("an incomplete terminal scan reported cleanup complete")
	}
	if slot := testPubRelSlot(t, e, pi); slot == nil || slot.State != mqttInflightPubRelTerminal {
		t.Fatalf("an incomplete scan must retain the terminal slot, got %+v", slot)
	}

	// PUBCOMP normally untracks the delivered record before terminal cleanup. A durable
	// slot alone does not participate in bumpPI, so the failed cleanup must install a
	// local shadow reservation until a later retry proves the slot gone. Pass no client
	// here to exercise the reservation without arming a background retry in the fixture.
	e.sess.retainAndReconcileTerminal(nil, *ref)
	e.sess.mu.Lock()
	reserved := e.sess.pendingPubRel[pi]
	e.sess.last_pi = pi - 1
	next := e.sess.bumpPI()
	e.sess.mu.Unlock()
	if reserved == nil || reserved.regNonce != ref.nonce || reserved.jsAckSubject != _EMPTY_ {
		t.Fatalf("failed terminal cleanup did not retain the exact exchange: %+v", reserved)
	}
	if next == pi {
		t.Fatalf("failed terminal cleanup re-minted packet identifier %d", pi)
	}
}

// TestF092TerminalFinalizationRereadsTheFreshnessFence forces a same-identity rewrite
// while the complete PUBREL scan is in progress. The final reread must see the moved
// sequence and preserve it rather than deleting through an early observation.
func TestF092TerminalFinalizationRereadsTheFreshnessFence(t *testing.T) {
	e := testNewInflightEnv(t, "f092-terminal-freshness")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(22)
	testSeedPubRelReadySlot(t, e, pi)

	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading seeded slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	readyRef := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	seq, ok := e.sess.casInflightSlot(e.c, *readyRef, mqttInflightPubRelReady,
		mqttSlotAdvancePhase, func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelTerminal
		})
	if !ok {
		t.Fatal("advancing the fixture to terminal")
	}
	terminal, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || terminal == nil {
		t.Fatalf("loading terminal slot: rec=%+v err=%v", terminal, err)
	}
	ref := mqttSlotRefFromRecord(subject, seq, terminal)
	if ref == nil {
		t.Fatal("the terminal slot must yield a reference")
	}

	originalLoad := e.asm.testInflightLoad
	loads := 0
	e.asm.testInflightLoad = func(gotSubject string) (*mqttInflightRecord, *StoredMsg, error) {
		loads++
		if loads == 2 {
			cur, msg, lerr := originalLoad(gotSubject)
			if lerr == nil && cur != nil && msg != nil {
				if _, werr := e.asm.testInflightStore(gotSubject, msg.Sequence, cur); werr != nil {
					return nil, nil, werr
				}
			}
		}
		return originalLoad(gotSubject)
	}
	defer func() { e.asm.testInflightLoad = originalLoad }()

	e.sess.finalizeTerminalSlot(e.c, *ref)
	after, afterMsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || after == nil || afterMsg == nil {
		t.Fatalf("the rewritten terminal slot must survive: rec=%+v msg=%+v err=%v", after, afterMsg, err)
	}
	if afterMsg.Sequence == ref.seq {
		t.Fatalf("the fixture did not move the slot sequence: still %d", ref.seq)
	}
}

// TestF092TerminalDuplicateDisposalPrecedesSlotDeletion plants two real records for the
// same exchange. Terminal cleanup must remove both before releasing the owner slot.
func TestF092TerminalDuplicateDisposalPrecedesSlotDeletion(t *testing.T) {
	e := testNewInflightEnv(t, "f092-terminal-duplicates")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(23)
	_, err := e.sess.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			DeliverSubject: "terminal.duplicates.delivery",
			Durable:        "PUBREL_F089",
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  e.sess.pubRelSubject,
			AckWait:        mqttDefaultAckWait,
			MaxAckPending:  mqttDefaultMaxAckPending,
		},
	})
	if err != nil {
		t.Fatalf("creating the PUBREL consumer: %v", err)
	}
	testSeedPubRelReadySlot(t, e, pi)
	slot := testPubRelSlot(t, e, pi)
	for i := 0; i < 2; i++ {
		msg, hdrLen := mqttNewCorrelatedPubRel(pi, slot.Nonce, slot.Dur, slot.Seq,
			_EMPTY_, _EMPTY_, _EMPTY_)
		if _, err := e.sess.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
			t.Fatalf("storing PUBREL record %d: %v", i+1, err)
		}
	}
	records, _, complete := e.sess.scanPubRelRecords(e.sess.pubRelSubject)
	if !complete || len(records) != 2 {
		t.Fatalf("fixture PUBREL scan = %d records complete=%v, want 2/true", len(records), complete)
	}

	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading seeded slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	e.sess.mqttTerminalizeOnPubComp(e.c, *ref)

	if slot := testPubRelSlot(t, e, pi); slot != nil {
		t.Fatalf("terminal cleanup retained the slot after complete duplicate disposal: %+v", slot)
	}
	if records, _, complete := e.sess.scanPubRelRecords(e.sess.pubRelSubject); !complete || len(records) != 0 {
		t.Fatalf("terminal cleanup left %d same-exchange record(s), complete=%v", len(records), complete)
	}
}

// TestF093CompletedExchangeLeavesNoPublishEntryOrMapping is F-093's acceptance.
//
// THE FINDING: `finalizeDrainDelivery` released the recovered in-flight register but never
// removed the recovered `pendingPublish` entry or its `cpending` mapping. `trackAsPubRel`
// then installed `pendingPubRel[pi]` BESIDE it, and PUBCOMP removed only the PUBREL entry —
// so the stale PUBLISH entry kept reserving the identifier and its Receive Maximum slot for
// the life of the session, and with a Receive Maximum it was charged TWICE while both
// entries lived.
//
// So the acceptance is what survives a completed exchange: no `pendingPublish` entry, no
// `cpending` mapping, the identifier allocatable, and the capacity charge back to zero.
func TestF093CompletedExchangeLeavesNoPublishEntryOrMapping(t *testing.T) {
	e := testNewInflightEnv(t, "f093-complete")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(7)
	testSeedPubRelReadySlot(t, e, pi)
	nonce := testPubRelSlot(t, e, pi).Nonce

	// A recovered PUBLISH entry and its source mapping, which is the shape the finding is
	// about. Admission must consume both as part of the PUBLISH-to-PUBREL handoff.
	e.sess.mu.Lock()
	e.sess.rmax = 1
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending, 1)
	}
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: "SRC", sseq: 41, regNonce: nonce}
	if e.sess.cpending == nil {
		e.sess.cpending = make(map[string]map[uint64]uint16, 1)
	}
	e.sess.cpending["SRC"] = map[uint64]uint16{41: pi}
	charged := e.sess.qos12InflightForCap()
	e.sess.mu.Unlock()
	if charged == 0 {
		t.Fatal("the fixture must start with the identifier charged, or the capacity assertion below proves nothing")
	}

	testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1), nonce)

	// The handoff itself must already have removed the PUBLISH half. Waiting for PUBCOMP
	// would leave the identifier double-owned for the whole PUBREL round trip.
	e.sess.mu.Lock()
	_, hadPub := e.sess.pendingPublish[pi]
	_, hadRel := e.sess.pendingPubRel[pi]
	both := e.sess.qos12InflightForCap()
	e.sess.mu.Unlock()
	if !hadRel {
		t.Fatal("the delivery must install the PUBREL entry, or nothing below is under test")
	}
	if hadPub {
		t.Fatal("the PUBREL handoff left the recovered PUBLISH entry beside it")
	}
	if mapped := e.sess.cpending["SRC"][41] == pi; mapped {
		t.Fatal("the PUBREL handoff left the recovered PUBLISH source mapping behind")
	}
	if both != 1 {
		t.Fatalf("the handed-off exchange consumes %d capacity slots, want exactly 1", both)
	}

	e.c.mqttProcessPubComp(pi)

	e.sess.mu.Lock()
	_, pubLeft := e.sess.pendingPublish[pi]
	_, relLeft := e.sess.pendingPubRel[pi]
	mapLeft := e.sess.cpending["SRC"][41] == pi
	_, retirementLeft := e.sess.srcAckOwed[mqttDeferredKey{"SRC", 41}]
	after := e.sess.qos12InflightForCap()
	e.sess.mu.Unlock()

	if pubLeft {
		t.Error("the completed exchange left its PUBLISH entry behind: that entry reserves the " +
			"identifier and a Receive Maximum slot for the life of the session — F-093 itself")
	}
	if relLeft {
		t.Error("the completed exchange left its PUBREL entry behind")
	}
	if mapLeft {
		t.Error("the completed exchange left its cpending source mapping behind, so a source " +
			"redelivery would still resolve to this identifier")
	}
	if retirementLeft {
		t.Error("the completed exchange left a source retirement behind")
	}
	if after != 0 {
		t.Errorf("the completed exchange still charges %d against Receive Maximum", after)
	}
	if !testMintsPI(t, e.sess, pi) {
		t.Errorf("packet identifier %d was not released by the completed exchange", pi)
	}
}

// TestF092OrdinaryCorrelatedStalePubRelIsRefused is the test whose ABSENCE let a critical
// defect through, and it is here for that reason as much as for the property.
//
// Every other acceptance fixture delivered a record with an EMPTY source correlation, so
// none of them reached the ordinary correlated route. That route was bypassing slot
// admission altogether: the drain check used `drainIntentOwnedBy`, which returns true when
// the durable is not draining at all — "nothing to contradict" — so every ordinary
// correlated PUBREL was admitted as drain-owned without the slot being loaded, its nonce
// compared, or an action acquired. F-092 was reproduced in full on the commonest path while
// four green acceptance tests said otherwise.
//
// So this drives a record that is correlated to a real source, on a session with NO drain
// for that durable, and requires the refusal to come from the slot.
func TestF092OrdinaryCorrelatedStalePubRelIsRefused(t *testing.T) {
	e := testNewInflightEnv(t, "f092-ordinary")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(12)
	testSeedPubRelReadySlot(t, e, pi)

	slot := testPubRelSlot(t, e, pi)
	if slot == nil {
		t.Fatal("the fixture must seed a slot")
	}
	// The seeded slot's own source, so the correlation is genuinely valid — the record is
	// stale in its EXCHANGE, not malformed in its correlation.
	srcDur, srcSeq := slot.Dur, slot.Seq

	e.sess.mu.Lock()
	noDrain := e.sess.drainByDur[srcDur] == nil
	e.sess.mu.Unlock()
	if !noDrain {
		t.Fatalf("this fixture must have NO drain for %q, or it exercises the drain route instead", srcDur)
	}

	const foreign = "yyyyyyyyyyyyyyyyyyyyyy"
	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelCorrelated(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1),
			foreign, srcDur, srcSeq)
	})
	if writes != 0 {
		t.Errorf("an ordinary CORRELATED stale PUBREL naming exchange %q was transmitted under an "+
			"identifier whose slot names %q: %d write(s). That is F-092 on the commonest path",
			foreign, slot.Nonce, writes)
	}
	// And nothing was handed off: a bypassing admission would have let the handoff delete
	// the live exchange's PUBLISH entry on the strength of a stale record.
	after := testPubRelSlot(t, e, pi)
	if after == nil {
		t.Fatal("the refusal must not delete the slot")
	}
	if after.State != mqttInflightPubRelReady || after.ActionToken != _EMPTY_ {
		t.Errorf("the refusal mutated the slot: state=%q action=%q", after.State, after.ActionToken)
	}

	// THE DISCRIMINATING ARM: the same correlated shape, the slot's own exchange, transmits.
	writes = testCountPubRelWrites(t, e, func() {
		testDeliverPubRelCorrelated(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 2),
			slot.Nonce, srcDur, srcSeq)
	})
	if writes != 1 {
		t.Errorf("the exchange's OWN correlated PUBREL was not transmitted (%d write(s))", writes)
	}
}

// TestF092AdmissionRefusesForeignIncarnationCustody discriminates the production custody
// check in pubRelAdmissionContextOK (cur.Claim == sess.regClaim). The sibling test
// TestF092ActionAcquisitionDoesNotFollowACustodyRewrite exercises only the CAS-race guard
// (a stale ref whose claim no longer matches the rewritten slot); it passes even with the
// admission custody check removed, because mqttAcquirePubRelAction never runs that check.
// This test loads the slot FRESH — so the CAS predicate would accept it — and proves that
// admission itself refuses a slot whose custody belongs to another incarnation. Removing
// the `cur.Claim != sess.regClaim` check makes the foreign-claim assertion below fail.
func TestF092AdmissionRefusesForeignIncarnationCustody(t *testing.T) {
	e := testNewInflightEnv(t, "f092-foreign-custody")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(28)

	e.sess.mu.Lock()
	dur := e.sess.pubRelDurable()
	regClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	if dur == _EMPTY_ {
		t.Fatal("the fixture must have a PUBREL durable")
	}

	// A fully valid pubrel_ready slot whose custody belongs to a DIFFERENT incarnation —
	// the shape a cross-broker resume leaves before the pre-CONNACK refresh re-claims it.
	// Every other admission gate passes, so only the custody check can refuse.
	cur := &mqttInflightRecord{
		PI: pi, Dur: "SRC", Seq: 41, QoS: 2,
		Nonce: "xxxxxxxxxxxxxxxxxxxxxx",
		State: mqttInflightPubRelReady,
		Claim: "predecessor-incarnation-claim", ClaimSeq: 1,
		PubRelDurable: dur, PubRelStreamSeq: testCanonicalPubRelSeq,
	}
	if cur.Claim == regClaim {
		t.Fatal("the fixture's foreign claim must differ from this session's regClaim")
	}
	corr := mqttPubRelCorrelation{SrcDur: "SRC", SrcSeq: 41, Ok: true}

	if e.sess.pubRelAdmissionContextOK(e.c, pi, cur, dur, testCanonicalPubRelSeq, corr, mqttPubRelAffineRef{}) {
		t.Fatal("admission accepted a slot whose custody belongs to another incarnation: a " +
			"superseded broker could acquire an action under a successor's claim (F-092)")
	}

	// Positive control: the SAME slot with THIS incarnation's claim is admitted, proving the
	// refusal above is the custody check and not an unrelated gate.
	cur.Claim = regClaim
	if !e.sess.pubRelAdmissionContextOK(e.c, pi, cur, dur, testCanonicalPubRelSeq, corr, mqttPubRelAffineRef{}) {
		t.Fatal("admission refused a slot whose custody IS this incarnation's; the test is not " +
			"isolating the custody check")
	}
}

// TestF092RecoveredPlaceholderNonceLetsTerminalCleanupReleaseIt discriminates the F-093 crash
// cut a holistic re-review found: recovery installs a surviving PUBREL as a nonce-bound empty
// placeholder (installPubRelLocked). Terminal cleanup of a finished slot must ADOPT that
// placeholder (installTerminalReservationLocked) and later RELEASE it (releaseTerminalReservation)
// — both are nonce-keyed. A nonce-LESS placeholder (the bug) could neither be adopted nor
// released, so the durable slot was deleted while the in-memory entry leaked the identifier and
// its Receive Maximum charge forever.
func TestF092RecoveredPlaceholderNonceLetsTerminalCleanupReleaseIt(t *testing.T) {
	const pi = uint16(30)
	nonce := mqttNewInflightNonce()
	gen := mqttNewSessGen()
	idHash := getHash("recovered-placeholder")
	subject := mqttInflightSubject(idHash, gen, pi)
	ref := mqttInflightSlotRef{
		name: mqttInflightName{idHash: idHash, gen: gen, pi: pi, subject: subject},
		seq:  5, nonce: nonce, state: mqttInflightPubRelTerminal, dur: "SRC", sseq: 41,
	}
	sess := &mqttSession{
		idHash: idHash, gen: gen,
		pendingPubRel: map[uint16]*mqttPending{
			// The recovered placeholder exactly as installPubRelLocked now writes it: an empty
			// ack subject, source coordinates, and — the fix — the exchange nonce.
			pi: {jsDur: "SRC", sseq: 41, regNonce: nonce},
		},
	}

	sess.mu.Lock()
	adopted := sess.installTerminalReservationLocked(ref)
	sess.mu.Unlock()
	if !adopted {
		t.Fatal("terminal cleanup could not adopt the recovered PUBREL placeholder; a nonce-less " +
			"placeholder leaks the identifier (F-093 crash cut)")
	}

	sess.releaseTerminalReservation(ref)

	sess.mu.Lock()
	_, leaked := sess.pendingPubRel[pi]
	sess.mu.Unlock()
	if leaked {
		t.Fatal("the recovered PUBREL placeholder was not released after terminal cleanup: its " +
			"identifier and Receive Maximum charge leak for the life of the session (F-093)")
	}
}

// TestF092RecoveredSourceBackedPlaceholderIsNotRetained discriminates the jsDur guard in
// retainedPubRelAdmittedLocked_. A drain/removal-versus-PUBREL-store crash cut leaves a
// recovered placeholder whose durable owner slot is absent; installPubRelLocked writes it with
// jsDur = the SOURCE consumer durable (or empty when the record has no source). Neither shape
// names THIS generation's PUBREL consumer, so neither is a live retained exchange, and admitting
// either through the slot-less retained path transmits a PUBREL with no durable action, recreating
// F-092 across brokers. jsDur — not sseq — is the discriminator: a genuine retained exchange
// carries jsDur = pubRelDurable() and a NON-ZERO sseq on every AckWait redelivery after the first,
// so an sseq test refused legitimate retained redeliveries. The sibling redelivery test covers
// the admitted side.
func TestF092RecoveredSourceBackedPlaceholderIsNotRetained(t *testing.T) {
	const owner = "zzzzzzzzzzzzzzzzzzzzzz"
	for _, arm := range []struct {
		name  string
		jsDur string
		sseq  uint64
	}{
		{"source-backed", "SRC", 41}, // installPubRelLocked's jsDur = srcDur, sseq = srcSeq
		{"source-less", _EMPTY_, 0},  // a recovered record with no source: jsDur empty
	} {
		t.Run(arm.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "f092-recovered-"+arm.name)
			testReadyForPubRelDelivery(t, e)
			const pi = uint16(31)
			// NO durable slot for this identifier (the crash cut), and a recovered placeholder
			// whose jsDur is not this generation's PUBREL consumer — the shape installPubRelLocked
			// writes on recovery.
			e.sess.mu.Lock()
			if e.sess.pendingPubRel == nil {
				e.sess.pendingPubRel = make(map[uint16]*mqttPending, 1)
			}
			e.sess.pendingPubRel[pi] = &mqttPending{jsDur: arm.jsDur, sseq: arm.sseq, regNonce: owner}
			e.sess.mu.Unlock()

			writes := testCountPubRelWrites(t, e, func() {
				testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1), owner)
			})
			if writes != 0 {
				t.Errorf("a recovered %s PUBREL whose slot is absent was transmitted via the retained "+
					"path (%d write(s)): no durable action fenced it, recreating F-092", arm.name, writes)
			}
		})
	}
}

// TestF092RetainedPubRelRedeliveryIsAdmitted is the finding's reproducer: after the first
// retained PUBREL delivery, trackAsPubRel stamps the entry with a NON-ZERO sseq (the PUBREL
// delivery's own sequence) and fills jsAckSubject. The old sseq != 0 guard then refused every
// AckWait redelivery, stalling the exchange and Receive Maximum 1 with it. jsDur = pubRelDurable()
// keeps the entry admissible across redeliveries. Reinstating the sseq guard fails this test.
func TestF092RetainedPubRelRedeliveryIsAdmitted(t *testing.T) {
	e := testNewInflightEnv(t, "f092-retained-redeliver")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(9)
	const owner = "zzzzzzzzzzzzzzzzzzzzzz"
	// The entry the first delivery's trackAsPubRel leaves: jsDur = this generation's PUBREL
	// consumer, a non-zero sseq from the PUBREL delivery, and a filled ack subject. No slot,
	// because a retained send has no register (R6).
	e.sess.mu.Lock()
	if e.sess.pendingPubRel == nil {
		e.sess.pendingPubRel = make(map[uint16]*mqttPending, 1)
	}
	e.sess.pendingPubRel[pi] = &mqttPending{
		jsDur:        e.sess.pubRelDurable(),
		sseq:         testCanonicalPubRelSeq,
		jsAckSubject: testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 1),
		regNonce:     owner,
	}
	e.sess.mu.Unlock()

	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject("PUBREL_F089", testCanonicalPubRelSeq, 2), owner)
	})
	if writes != 1 {
		t.Errorf("a retained PUBREL redelivery (non-zero sseq, live owner) was not transmitted "+
			"(%d write(s)); an sseq guard stalls the exchange and Receive Maximum 1 with it", writes)
	}
}
