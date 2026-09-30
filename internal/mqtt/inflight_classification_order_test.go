// F-099: recovery classification follows adopted slot state and ordered PUBREL evidence.
//
// Two arms, one property. Arm A: per-exchange evidence is read AFTER the register it
// judges, so an absence cannot predate the record it rules out. Arm B: a contest's OWNED
// result is routed by the body it ACTUALLY ADOPTED, not by the bucket the walk sorted the
// record into before the contest ran.
//
// Every fixture here manipulates ONE thing — when the staged PUBREL record becomes
// visible, or what phase the contest's reread returns — so that the HEAD ordering and the
// repaired ordering differ in nothing else.

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// f099ScanStaging models the peer that stores a PUBREL and re-phases its slot between this
// pass's register load and its PUBREL scan.
//
// The trigger is the LAST register batch, not the first call: loadInflightRecordsMulti runs
// once PER BATCH, so "the record appears once a batch has been loaded" would also be
// satisfied by a scan placed BETWEEN batches — which violates the after-every-batch
// invariant while passing every single-batch fixture.
type f099ScanStaging struct {
	wantBatches int
	batches     int
	record      mqttStoredPubRel
}

func (s *f099ScanStaging) noteBatch() { s.batches++ }

func (s *f099ScanStaging) scan(string) ([]mqttStoredPubRel, int, bool) {
	if s.batches < s.wantBatches {
		return nil, 0, true // the peer has not stored it yet, as far as this pass can see
	}
	return []mqttStoredPubRel{s.record}, 0, true
}

// f099Recorder captures every durable proposal so an oracle can assert on transitions
// rather than on a final snapshot, which cannot tell "never proposed" from "proposed and
// overwritten".
type f099Recorder struct {
	states  []mqttInflightState
	deletes []uint64
}

func (r *f099Recorder) sawState(st mqttInflightState) bool {
	for _, s := range r.states {
		if s == st {
			return true
		}
	}
	return false
}

// T1 (arm A, the `finished` consumer). A pubrel_ready slot whose PUBREL record a peer
// stored between this pass's register load and its scan is OWED, not finished.
//
// Under the HEAD ordering the scan ran first and answered "no record", `finished` became
// `!exact && sourceComplete`, the slot was advanced to terminal and cleanup deleted the
// client's PUBREL — a lost message with no PUBCOMP ever sent.
//
// Mutation checks: M1 (restore the HEAD scan position) and M2 (keep the move but compute
// `finished` from a second old-site scan) must both fail this. M5a/M5b are the control:
// with the record staged UNCONDITIONALLY the test passes under both orderings, which is
// what shows it is driven by the ORDER and not by anything else the fixture does.
func TestF099RecoveryOwesAPubRelStoredAfterTheRegisterRead(t *testing.T) {
	// A SLOT-ONLY (experimental affine; shared until 7a) source, deliberately: this fake store has no JetStream behind it, and
	// the recovery mechanics asserted here are shared by both classes up to the class split. An
	// ORDINARY slot's recovery also reads (and on route B writes) its exchange, which only a real
	// store can host; that class is covered on the real store by TestF105RouteBRecordsTheRecoveredPubRec,
	// TestF105RecoveryAbsenceDoesNotEndAnOrdinaryExchange and
	// TestF105RecoveryFinishesARevivedSlotBesideATerminalExchange (consult 26).
	t.Parallel()
	idHash, gen := getHash("f099-owed"), mqttNewSessGen()
	_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
	rec := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPubRelReady, PI: 21, Dur: mqttQ2AffineConsumerDurablePrefix + "SRC", Seq: 44, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1,
		PubRelDurable: pubRelDurable, PubRelStreamSeq: 141,
	})
	subject := mqttInflightSubject(idHash, gen, rec.PI)
	current, currentSeq := *rec, uint64(71)
	staging := &f099ScanStaging{
		wantBatches: 1,
		record: mqttStoredPubRel{
			seq: 141, pi: rec.PI, srcDur: rec.Dur, srcSeq: rec.Seq,
			owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: rec.Nonce},
		},
	}
	rc := &f099Recorder{}

	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = staging.scan
	as.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return map[string]uint64{subject: 1}, nil
	}
	as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		staging.noteBatch()
		data, err := json.Marshal(&current)
		if err != nil {
			t.Fatal(err)
		}
		return []*JSApiMsgGetResponse{{Message: &StoredMsg{
			Subject: subjects[0], Sequence: currentSeq, Data: data,
		}}}, nil
	}
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		cp := current
		return &cp, &StoredMsg{Subject: subject, Sequence: currentSeq}, nil
	}
	as.testInflightStore = func(_ string, expect uint64, next *mqttInflightRecord) (uint64, error) {
		if expect != currentSeq {
			return 0, errors.New("test: stale CAS base")
		}
		rc.states = append(rc.states, next.State)
		if next.State == mqttInflightPubRelTerminal {
			// REFUSED, so a broken ordering reports itself instead of running on into
			// terminal cleanup and nil-dereferencing the absent JetStream handle. The
			// refusal makes the advance unresolved, which recovery surfaces as an error the
			// oracle below reads. Under the correct ordering no terminal CAS is proposed at
			// all, so this arm is never taken and the fixture is unchanged by it.
			return 0, errors.New("test: terminal advance refused")
		}
		current = *next
		currentSeq++
		return currentSeq, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		rc.deletes = append(rc.deletes, seq)
		return nil
	}
	as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	sess := f099Session(as, "f099-owed", idHash, gen, pubRelSubject, pubRelDurable)

	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	// The first two clauses are not decoration: a pass that failed early also leaves the
	// storage untouched and issues no deletes, so without them this oracle passes
	// vacuously for the wrong reason.
	sess.mu.Lock()
	incomplete := sess.piSpaceIncomplete
	owed := sess.pendingPubRel[rec.PI]
	_, asPublish := sess.pendingPublish[rec.PI]
	sess.mu.Unlock()
	if incomplete {
		t.Fatal("an owed past-PUBREC exchange made the identifier space incomplete")
	}
	if owed == nil {
		t.Fatal("no pendingPubRel reservation for the owed exchange")
	}
	if asPublish {
		t.Fatal("a past-PUBREC exchange was installed as an ordinary PUBLISH delivery")
	}
	if rc.sawState(mqttInflightPubRelTerminal) {
		t.Fatalf("the owed slot was advanced to terminal from a stale absence: %v", rc.states)
	}
	if current.State != mqttInflightPubRelReady {
		t.Fatalf("durable slot state = %q, want %q", current.State, mqttInflightPubRelReady)
	}
	if len(rc.deletes) != 0 {
		t.Fatalf("the owed slot was deleted at %v", rc.deletes)
	}
}

// T4 (arm A, route B) — and the multiple-batch case that discriminates "after the LAST
// batch" from "after some batch".
//
// A `publish` slot plus a PRESENT exact record is route B: the crash cut in which the
// record landed but the owner slot never left `publish`. Reading the scan too early misses
// it, the slot is restored as an ordinary PUBLISH register, and beginSendLocked — which
// does not inspect slot state — can retransmit a payload the client already answered with
// PUBREC.
//
// Mutation checks: M1, M3 (an old-site scan consulted by route B alone) and M1b (the scan
// placed BETWEEN batches, which only the multi-batch fixture can see).
func TestF099RecoveryRoutesRouteBFromEvidenceReadAfterTheLastBatch(t *testing.T) {
	// A SLOT-ONLY (experimental affine; shared until 7a) source, deliberately: this fake store has no JetStream behind it, and
	// the recovery mechanics asserted here are shared by both classes up to the class split. An
	// ORDINARY slot's recovery also reads (and on route B writes) its exchange, which only a real
	// store can host; that class is covered on the real store by TestF105RouteBRecordsTheRecoveredPubRec,
	// TestF105RecoveryAbsenceDoesNotEndAnOrdinaryExchange and
	// TestF105RecoveryFinishesARevivedSlotBesideATerminalExchange (consult 26).
	t.Parallel()
	idHash, gen := getHash("f099-routeb"), mqttNewSessGen()
	_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)

	// More than one batch, and THE TARGET IS IN THE FINAL ONE. That placement is what makes
	// a between-batches scan a genuine violation rather than merely an earlier observation:
	// the staged store happens after the last batch, so a scan after batch 1 would be
	// reading BEFORE the target's own register was loaded. With the target in the first
	// batch the same mutation reads after that register and misses the record only because
	// it did not exist yet, which is no defect at all.
	//
	// Subjects sort as STRINGS, so packet identifier 9999 sorts after every filler in
	// 1..1024 and lands last.
	const filler = mqttInflightLoadBatch
	target := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 9999, Dur: mqttQ2AffineConsumerDurablePrefix + "SRC", Seq: 45, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1,
	})
	targetSubject := mqttInflightSubject(idHash, gen, target.PI)

	subjects := map[string]uint64{targetSubject: 1}
	bodies := map[string]*mqttInflightRecord{targetSubject: target}
	for i := 0; i < filler; i++ {
		pi := uint16(1 + i)
		r := freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: pi, Dur: "FILL", Seq: uint64(1000 + i), QoS: 1,
			Nonce: mqttNewInflightNonce(),
		})
		s := mqttInflightSubject(idHash, gen, pi)
		subjects[s] = 1
		bodies[s] = r
	}
	wantBatches := (len(subjects) + mqttInflightLoadBatch - 1) / mqttInflightLoadBatch
	if wantBatches < 2 {
		t.Fatalf("fixture must span multiple batches, got %d", wantBatches)
	}
	staging := &f099ScanStaging{
		wantBatches: wantBatches,
		record: mqttStoredPubRel{
			seq: 151, pi: target.PI, srcDur: target.Dur, srcSeq: target.Seq,
			owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: target.Nonce},
		},
	}

	seqs := map[string]uint64{}
	var next uint64 = 100
	for s := range subjects {
		next++
		seqs[s] = next
	}
	landed := map[string]mqttInflightRecord{}
	for s, b := range bodies {
		landed[s] = *b
	}

	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = staging.scan
	as.testInflightEnumerate = func(string) (map[string]uint64, error) { return subjects, nil }
	as.testInflightLoadMulti = func(batch []string) ([]*JSApiMsgGetResponse, error) {
		staging.noteBatch()
		out := make([]*JSApiMsgGetResponse, 0, len(batch))
		for _, s := range batch {
			body := landed[s]
			data, err := json.Marshal(&body)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, &JSApiMsgGetResponse{Message: &StoredMsg{
				Subject: s, Sequence: seqs[s], Data: data,
			}})
		}
		return out, nil
	}
	as.testInflightLoad = func(s string) (*mqttInflightRecord, *StoredMsg, error) {
		body := landed[s]
		return &body, &StoredMsg{Subject: s, Sequence: seqs[s]}, nil
	}
	as.testInflightStore = func(s string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		if expect != seqs[s] {
			return 0, errors.New("test: stale CAS base")
		}
		seqs[s]++
		landed[s] = *r
		return seqs[s], nil
	}
	as.testInflightDelete = func(uint64) error { return nil }
	as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	sess := f099Session(as, "f099-routeb", idHash, gen, pubRelSubject, pubRelDurable)

	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	got := landed[targetSubject]
	if got.State != mqttInflightPubRelReady {
		t.Fatalf("route B did not run: slot state = %q, want %q", got.State, mqttInflightPubRelReady)
	}
	if got.PubRelDurable != pubRelDurable || got.PubRelStreamSeq != staging.record.seq {
		t.Fatalf("route B canonical tuple = %q/%d, want %q/%d",
			got.PubRelDurable, got.PubRelStreamSeq, pubRelDurable, staging.record.seq)
	}
	sess.mu.Lock()
	_, asPublish := sess.pendingPublish[target.PI]
	sess.mu.Unlock()
	if asPublish {
		t.Fatal("a route-B exchange was installed as an ordinary PUBLISH delivery")
	}
}

// T8 (arm A, the retired-ready custody consumer). The third reader of the scan.
//
// A RETIRED pubrel_ready slot is normally excluded from custody transfer, but a PRESENT
// exact PUBREL makes it actionable: the client is still waiting for that protocol step. So
// a stale absence leaves the PREDECESSOR's custody standing, and admission then refuses the
// exchange forever after restart.
//
// The oracle is that custody TRANSFERRED, not that the identifier stayed reserved — the
// broken behavior also leaves it reserved, so reservation alone discriminates nothing.
//
// Mutation check: M12, which restores the stale scan at this consumer ALONE, so this test
// is shown to kill it rather than riding on M1.
func TestF099RetiredReadyTakesCustodyFromEvidenceReadAfterTheRegister(t *testing.T) {
	// A SLOT-ONLY (experimental affine; shared until 7a) source, deliberately: this fake store has no JetStream behind it, and
	// the recovery mechanics asserted here are shared by both classes up to the class split. An
	// ORDINARY slot's recovery also reads (and on route B writes) its exchange, which only a real
	// store can host; that class is covered on the real store by TestF105RouteBRecordsTheRecoveredPubRec,
	// TestF105RecoveryAbsenceDoesNotEndAnOrdinaryExchange and
	// TestF105RecoveryFinishesARevivedSlotBesideATerminalExchange (consult 26).
	t.Parallel()
	idHash, gen := getHash("f099-retired-ready"), mqttNewSessGen()
	_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
	rec := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPubRelReady, PI: 23, Dur: mqttQ2AffineConsumerDurablePrefix + "SRC", Seq: 46, QoS: 2,
		Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1, Retired: true,
		PubRelDurable: pubRelDurable, PubRelStreamSeq: 161,
	})
	subject := mqttInflightSubject(idHash, gen, rec.PI)
	current, currentSeq := *rec, uint64(81)
	staging := &f099ScanStaging{
		wantBatches: 1,
		record: mqttStoredPubRel{
			seq: 161, pi: rec.PI, srcDur: rec.Dur, srcSeq: rec.Seq,
			owner: mqttPubRelOwner{State: mqttPubRelOwnerValid, Nonce: rec.Nonce},
		},
	}

	as := &mqttAccountSessionManager{}
	as.testInflightPubRelScan = staging.scan
	as.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return map[string]uint64{subject: 1}, nil
	}
	as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		staging.noteBatch()
		data, err := json.Marshal(&current)
		if err != nil {
			t.Fatal(err)
		}
		return []*JSApiMsgGetResponse{{Message: &StoredMsg{
			Subject: subjects[0], Sequence: currentSeq, Data: data,
		}}}, nil
	}
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		cp := current
		return &cp, &StoredMsg{Subject: subject, Sequence: currentSeq}, nil
	}
	as.testInflightStore = func(_ string, expect uint64, next *mqttInflightRecord) (uint64, error) {
		if expect != currentSeq {
			return 0, errors.New("test: stale CAS base")
		}
		current = *next
		currentSeq++
		return currentSeq, nil
	}
	as.testInflightDelete = func(uint64) error { return nil }
	as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	sess := f099Session(as, "f099-retired-ready", idHash, gen, pubRelSubject, pubRelDurable)

	if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	if current.Claim != sess.regClaim || current.ClaimSeq != sess.seq {
		t.Fatalf("custody = %q/%d, want this object's %q/%d — a stale absence left the "+
			"predecessor's claim standing, which admission refuses forever",
			current.Claim, current.ClaimSeq, sess.regClaim, sess.seq)
	}
	if current.State != mqttInflightPubRelReady {
		t.Fatalf("durable slot state = %q, want %q", current.State, mqttInflightPubRelReady)
	}
}

func f099Session(as *mqttAccountSessionManager, id, idHash, gen, pubRelSubject, pubRelDurable string) *mqttSession {
	return &mqttSession{
		asm: as, id: id, idHash: idHash, gen: gen,
		regClaim: mqttNewInflightNonce(), seq: 9,
		pubRelSubject: pubRelSubject, pubRelConsumer: &ConsumerConfig{Durable: pubRelDurable},
		cancelledRegNonces: make(map[string]uint64),
		pendingPublish:     map[uint16]*mqttPending{},
		cpending:           map[string]map[uint64]uint16{},
	}
}

// The target must actually land in the LAST batch, or M1b is not the mutation this test
// claims to kill. Asserted rather than assumed: the ordering is a property of string
// sorting over packet identifiers, which a later edit to the fixture could silently break.
func TestF099RouteBFixtureTargetIsInTheFinalBatch(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("f099-routeb-shape"), mqttNewSessGen()
	subjects := make([]string, 0, mqttInflightLoadBatch+1)
	for i := 0; i < mqttInflightLoadBatch; i++ {
		subjects = append(subjects, mqttInflightSubject(idHash, gen, uint16(1+i)))
	}
	target := mqttInflightSubject(idHash, gen, 9999)
	subjects = append(subjects, target)
	sort.Strings(subjects)
	if subjects[len(subjects)-1] != target {
		t.Fatalf("target %q does not sort last; the multi-batch fixture would put it in an "+
			"earlier batch and M1b would stop being a real violation", target)
	}
	if len(subjects) <= mqttInflightLoadBatch {
		t.Fatalf("fixture spans one batch (%d subjects)", len(subjects))
	}
}

// f099AdoptedContest makes a custody contest come back with a body whose PHASE moved.
//
// The contest reaches its moved-and-ours arm only on a FAILED CAS followed by a reread
// showing THIS session's claim, the SAME nonce, and a MOVED sequence — same nonce alone
// does not select that arm. So the store is refused once and the reread answers with the
// adopted body.
type f099AdoptedContest struct {
	subject  string
	adopted  mqttInflightRecord
	seq      uint64
	refused  bool
	rereadAt uint64
}

func (a *f099AdoptedContest) load(string) (*mqttInflightRecord, *StoredMsg, error) {
	cp := a.adopted
	at := a.seq
	if a.refused {
		at = a.rereadAt // MOVED, which is what proves our claim landed and its response was lost
	}
	return &cp, &StoredMsg{Subject: a.subject, Sequence: at}, nil
}

func (a *f099AdoptedContest) store(_ string, _ uint64, _ *mqttInflightRecord) (uint64, error) {
	a.refused = true
	return 0, errors.New("test: custody CAS rejected, the peer rewrote the slot")
}

// T5 (arm B, plain QoS 2). A slot a peer re-phased `publish -> pubrel_ready` during the
// custody contest comes back OWNED carrying a past-PUBREC body. It must be routed as the
// past-PUBREC exchange it now is, not as the PUBLISH register the walk sorted it into.
//
// The scan is staged EMPTY and the source COMPLETE deliberately: that is the exact
// combination under which the older-scan `finished` verdict would fire, so the
// not-advanced-to-terminal clause is a real assertion rather than a vacuous one. The
// adopted body is newer than the scan, so no verdict from it is admissible.
//
// Mutation checks: M6 (route the owned set by the pre-contest bucket, the HEAD behavior),
// M8 (split into the PRE-contest pastPubRec input, which re-contests and re-exposes the
// stale verdict) and M9 (a state guard at the apply install instead of routing).
func TestF099AdoptedPastPubRecBodyIsNotInstalledAsAPublishDelivery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		source       mqttInflightSourceState
		wantSuppress bool
	}{
		// The LIVE arm is where the retransmission actually happens, and the LOST arm pins
		// the suppression predicate as mqttInflightSourceMayReoffer rather than
		// !sourceComplete: a lost source can never be re-offered, so suppressing it would
		// claim a fact about a delivery that cannot recur (M11).
		{"source complete", mqttInflightSourceComplete, false},
		{"source live", mqttInflightSourceLive, true},
		{"source lost", mqttInflightSourceLost, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idHash, gen := getHash("f099-adopted-"+tc.name), mqttNewSessGen()
			_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
			nonce := mqttNewInflightNonce()
			rec := freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: 31, Dur: "SRC", Seq: 51, QoS: 2,
				Nonce: nonce, Claim: "predecessor", ClaimSeq: 1,
			})
			subject := mqttInflightSubject(idHash, gen, rec.PI)
			sess := f099Session(nil, "f099-adopted", idHash, gen, pubRelSubject, pubRelDurable)

			adopted := *rec
			adopted.State = mqttInflightPubRelReady
			adopted.Claim = sess.regClaim
			adopted.ClaimSeq = sess.seq
			adopted.PubRelDurable = pubRelDurable
			adopted.PubRelStreamSeq = 171
			contest := &f099AdoptedContest{subject: subject, adopted: adopted, seq: 61, rereadAt: 62}

			as := &mqttAccountSessionManager{}
			sess.asm = as
			// EMPTY throughout: the record this pass could ask about does not exist as far
			// as the scan can see, which is the state that would license `finished`.
			as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
				return nil, 0, true
			}
			as.testInflightEnumerate = func(string) (map[string]uint64, error) {
				return map[string]uint64{subject: 1}, nil
			}
			as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
				data, err := json.Marshal(rec)
				if err != nil {
					t.Fatal(err)
				}
				return []*JSApiMsgGetResponse{{Message: &StoredMsg{
					Subject: subjects[0], Sequence: contest.seq, Data: data,
				}}}, nil
			}
			as.testInflightLoad = contest.load
			as.testInflightStore = contest.store
			as.testInflightDelete = func(uint64) error { return nil }
			as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return tc.source
			}

			if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}

			sess.mu.Lock()
			owed := sess.pendingPubRel[rec.PI]
			_, asPublish := sess.pendingPublish[rec.PI]
			srcMapped := sess.cpending[rec.Dur] != nil && sess.cpending[rec.Dur][rec.Seq] == rec.PI
			suppressed := sess.sourceRetiredLocked(rec.Dur, rec.Seq)
			sess.mu.Unlock()

			if asPublish {
				t.Fatal("an adopted past-PUBREC body was installed as an ordinary PUBLISH " +
					"delivery; its payload can be retransmitted under a fresh identifier")
			}
			if srcMapped {
				t.Fatal("an adopted past-PUBREC body restored its source mapping, so a " +
					"redelivery re-sends a payload the client already answered")
			}
			if owed == nil {
				t.Fatal("no pendingPubRel reservation for the adopted past-PUBREC exchange")
			}
			if owed.regSeq != contest.rereadAt {
				t.Fatalf("reservation carries sequence %d, want the ADOPTED %d",
					owed.regSeq, contest.rereadAt)
			}
			if suppressed != tc.wantSuppress {
				t.Fatalf("source suppression = %v, want %v for %s — the predicate is "+
					"mqttInflightSourceMayReoffer, not !sourceComplete",
					suppressed, tc.wantSuppress, tc.name)
			}
		})
	}
}

// T6a and T6b (arm B, the shared contests), retargeted by 7a. The boundary consult 2 set is a
// property, not a site list: every OWNED contest result is re-judged on the body it NOW carries.
//
// The LIVE shared contest (T6a) survives 7a at QoS 1: a supported shared register — QoS 1, beside its
// exchange — whose peer re-phased it `publish -> publish_terminal` (its PUBACK) during the custody
// contest comes back OWNED carrying a terminal body. It must not be installed as the PUBLISH
// delivery the walk sorted it into, nor restore its source mapping, and its identifier must not be
// re-allocatable. (The former QoS 2 `pubrel_ready` body no longer exists for a shared delivery.)
//
// The RETIRED shared contest (T6b) is DEAD for shared since 7a: a retired shared register is a shape
// this broker no longer writes, and recovery refuses it before either contest runs
// (sharedRecordUnsupported). Its subtests therefore assert that refusal — no custody write, nothing
// installed, no source retirement, the identifier held — and the second contest call site stays
// only as 7b's to remove with the regClaim traversal.
func TestF099AdoptedPastPubRecBodyOnTheSharedContests(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		retired bool
		source  mqttInflightSourceState
	}{
		{"live contest", false, mqttInflightSourceComplete},
		{"retired contest", true, mqttInflightSourceLive},
		{"retired and cancelled", true, mqttInflightSourceLive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idHash, gen := getHash("f099-shared-"+tc.name), mqttNewSessGen()
			_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
			acq := testValidSharedAcq()
			qos := byte(1)
			if tc.retired {
				qos = 2 // the refused pre-7a population: a retired shared QoS 2 register
			}
			rec := freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: 41, QoS: qos,
				Dur: mqttSharedDurableOf(acq), Seq: 61,
				Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1,
				Retired: tc.retired,
			})
			if !tc.retired {
				testRegisterSourceReply(rec)
			}
			if tc.name == "retired and cancelled" {
				rec.Cancelled = true
			}
			subject := mqttInflightSubject(idHash, gen, rec.PI)
			sess := f099Session(nil, "f099-shared", idHash, gen, pubRelSubject, pubRelDurable)

			adopted := *rec
			adopted.State = mqttInflightPublishTerminal
			adopted.Claim = sess.regClaim
			adopted.ClaimSeq = sess.seq
			contest := &f099AdoptedContest{subject: subject, adopted: adopted, seq: 71, rereadAt: 72}

			as := &mqttAccountSessionManager{}
			sess.asm = as
			as.testSharedExchangeLoad = testSharedExchangeFor(rec, acq)
			as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
				return nil, 0, true
			}
			as.testInflightEnumerate = func(string) (map[string]uint64, error) {
				return map[string]uint64{subject: 1}, nil
			}
			as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
				data, err := json.Marshal(rec)
				if err != nil {
					t.Fatal(err)
				}
				return []*JSApiMsgGetResponse{{Message: &StoredMsg{
					Subject: subjects[0], Sequence: contest.seq, Data: data,
				}}}, nil
			}
			as.testInflightLoad = contest.load
			as.testInflightStore = contest.store
			as.testInflightDelete = func(uint64) error { return nil }
			as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return tc.source
			}

			if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}

			sess.mu.Lock()
			ent := sess.pendingPublish[rec.PI]
			rel := sess.pendingPubRel[rec.PI]
			srcMapped := sess.cpending[rec.Dur] != nil && sess.cpending[rec.Dur][rec.Seq] == rec.PI
			_, retiredSrc := sess.srcAckOwed[mqttDeferredKey{rec.Dur, rec.Seq}]
			incomplete := sess.piSpaceIncomplete
			sess.mu.Unlock()

			if tc.retired {
				if contest.refused {
					t.Fatal("recovery issued a custody write over an unsupported retired shared register")
				}
				if ent == nil || !(ent.regForeign || ent.markerOpaque) || srcMapped || retiredSrc {
					t.Fatalf("the unsupported record was not refused: entry=%+v mapped=%v retired=%v", ent, srcMapped, retiredSrc)
				}
				return
			}
			if !contest.refused {
				t.Fatal("fixture error: the live contest's custody CAS was never issued")
			}
			if ent != nil && !(ent.regForeign || ent.markerOpaque) {
				t.Fatalf("an adopted terminal body was installed as an ordinary PUBLISH delivery: %+v", ent)
			}
			if srcMapped {
				t.Fatal("an adopted terminal body restored its source mapping")
			}
			if ent == nil && rel == nil && !incomplete {
				t.Fatal("the adopted terminal body's identifier is re-allocatable: nothing reserves it and the space is not withheld")
			}
		})
	}
}

// f099OwnedRoute is one reviewed DIRECT call site that takes a claim contest's OWNED
// result, bound positionally to discovery order.
type f099OwnedRoute struct {
	wrapper string // the contest wrapper this site calls
	test    string // the behavioral test carrying this route's semantics
}

// f099OwnedRouteManifest is the reviewed inventory, in the source order discovery walks.
var f099OwnedRouteManifest = []f099OwnedRoute{
	{"claimRecoveredSharedInflight", "TestF099AdoptedPastPubRecBodyOnTheSharedContests"},
	{"claimRecoveredSharedInflight", "TestF099AdoptedPastPubRecBodyOnTheSharedContests"},
	{"claimRecoveredTransferableInflight", "TestF099AdoptedPastPubRecBodyIsNotInstalledAsAPublishDelivery"},
	{"claimRecoveredRetiredPubRelInflight", "TestF099RetiredReadyTakesCustodyFromEvidenceReadAfterTheRegister"},
	{"claimRecoveredTransferableInflight", "TestF092RecoveryRoutesPublishPlusExactPubRelBeforeSourceCompletion"},
}

// f099CalleeName returns the called function's name for both call forms, because a wrapper
// need not be a method: a free function is called as a bare identifier and a selector-only
// scan walks straight past it.
func f099CalleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fn.Sel.Name
	case *ast.Ident:
		return fn.Name
	}
	return ""
}

// This check is a SOURCE-INVENTORY TRIPWIRE, and its predicate is frozen.
//
// It establishes exactly three things about the scanned production files: the recognized
// direct-call inventory matches its manifest; every manifest entry names a test that
// exists; and any reference bearing the contest's or a discovered wrapper's name OUTSIDE
// recognized direct-call syntax fails it closed. Those are facts about the source that the
// behavioral mutations cannot supply, because an ADDED, UNEXERCISED call site is invisible
// to a test nobody wrote for it.
//
// It does NOT prove routing correctness, and it does NOT discover arbitrary indirect
// routes. An earlier version tried to: it inspected each site's block for the routing
// structure the manifest named. That mechanism was faulted in five successive review rounds
// — aspirational, then self-contradictory, then a non-discriminating mutation, then
// validating declared strings instead of real routing, then pooling both shared sites'
// routing because they share an enclosing block — and each round closed one evasion and
// exposed the next. The reason is structural: "no owned-result route can be added unseen"
// is a DATAFLOW property, and Go indirection is not an enumerable class for a syntactic
// check. What ROUTING each site performs is proven instead by mutation, in the tests named
// in the manifest.
//
// So the routing inspection is deleted rather than patched again. A construction outside
// this frozen predicate is not grounds to widen it unless it demonstrates a production
// defect.
//
// Mutation checks: M10a (drop a manifest entry), M10b (add an unaccounted direct call
// site), M13 (clear a matched entry's test name) and M15 (take a method value of a wrapper,
// the alias form, which must fail closed).
func TestF099OwnedContestCallSiteInventoryIsReviewed(t *testing.T) {
	t.Parallel()
	const contest = "claimRecoveredInflight"
	wrappers := map[string]bool{}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	fset := token.NewFileSet()
	var names []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names) // deterministic discovery order, so the manifest can be positional
	parsed := map[string]*ast.File{}
	for _, name := range names {
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		parsed[name] = f
	}

	// Pass 1: DISCOVER the wrappers. A wrapper is any function whose body calls the contest,
	// in either call form.
	for _, name := range names {
		ast.Inspect(parsed[name], func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			ast.Inspect(fn.Body, func(m ast.Node) bool {
				if call, ok := m.(*ast.CallExpr); ok && f099CalleeName(call) == contest {
					wrappers[fn.Name.Name] = true
				}
				return true
			})
			return true
		})
	}
	if len(wrappers) == 0 {
		t.Fatalf("discovery found no %s wrappers; the tripwire would pass vacuously", contest)
	}
	named := func(n string) bool { return n == contest || wrappers[n] }

	// Pass 2: the direct-call inventory, and a FAIL-CLOSED rejection of every other
	// appearance of these names. A method value — `claim := sess.claimRecoveredTransferable
	// Inflight` — introduces a route the inventory cannot see, so it is refused rather than
	// resolved: refusing needs no dataflow analysis and cannot be evaded by a cleverer alias.
	type site struct {
		wrapper string
		pos     token.Pos
	}
	var sites []site
	for _, name := range names {
		f := parsed[name]
		callTargets := map[ast.Expr]bool{}
		// A function's own DECLARATION name is an identifier bearing the same text and is
		// not a reference to it. Excluded, or every wrapper reports itself.
		declNames := map[*ast.Ident]bool{}
		selFields := map[*ast.Ident]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				callTargets[x.Fun] = true
			case *ast.FuncDecl:
				declNames[x.Name] = true
			case *ast.SelectorExpr:
				// The selector's field is visited again as a bare identifier; the
				// SelectorExpr arm already judges it, so the identifier arm must not.
				selFields[x.Sel] = true
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				nm := f099CalleeName(x)
				if wrappers[nm] {
					// A wrapper calling the contest is the definition, not a route.
					sites = append(sites, site{wrapper: nm, pos: x.Pos()})
				}
			case *ast.SelectorExpr:
				if named(x.Sel.Name) && !callTargets[ast.Expr(x)] {
					t.Fatalf("%s: %q is referenced outside a direct call. An owned-result "+
						"route reached through a value cannot be inventoried, so it is "+
						"refused rather than resolved", fset.Position(x.Pos()), x.Sel.Name)
				}
			case *ast.Ident:
				if named(x.Name) && !declNames[x] && !selFields[x] && !callTargets[ast.Expr(x)] {
					t.Fatalf("%s: %q is referenced outside a direct call. An owned-result "+
						"route reached through a value cannot be inventoried, so it is "+
						"refused rather than resolved", fset.Position(x.Pos()), x.Name)
				}
			}
			return true
		})
	}
	// A wrapper's own body calls the contest; those are definitions, and pass 1 already
	// accounted for them. Drop sites that sit inside a wrapper.
	var routes []site
	for _, name := range names {
		ast.Inspect(parsed[name], func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil || wrappers[fn.Name.Name] {
				return true
			}
			for _, s := range sites {
				if s.pos >= fn.Body.Pos() && s.pos <= fn.Body.End() {
					routes = append(routes, s)
				}
			}
			return false
		})
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].pos < routes[j].pos })

	if len(routes) != len(f099OwnedRouteManifest) {
		var got []string
		for _, s := range routes {
			got = append(got, fmt.Sprintf("%s@%s", s.wrapper, fset.Position(s.pos)))
		}
		t.Fatalf("discovery found %d owned-result call site(s) but the manifest reviews %d: "+
			"a route has been added or removed without review.\nsites: %v",
			len(routes), len(f099OwnedRouteManifest), got)
	}

	tests := map[string]bool{}
	testFset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(testFset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		for _, d := range f.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok {
				tests[fn.Name.Name] = true
			}
		}
	}
	for i, want := range f099OwnedRouteManifest {
		got := routes[i]
		where := fset.Position(got.pos)
		if got.wrapper != want.wrapper {
			t.Fatalf("site %d at %s calls %s, but the manifest reviews %s there",
				i, where, got.wrapper, want.wrapper)
		}
		if want.test == "" {
			t.Fatalf("site %d at %s names no behavioral test", i, where)
		}
		if !tests[want.test] {
			t.Fatalf("site %d at %s names behavioral test %q, which does not exist",
				i, where, want.test)
		}
	}
}

// Arm B's OTHER half at the plain-QoS 2 contest: an adopted RETIREMENT.
//
// That site selects candidates on `publish` AND not-Retired AND not-Cancelled, so either
// fact can change under the contest. A body adopted with Retired set but still `publish` is
// a legal shape, and left in the PUBLISH population it gets a pendingPublish entry and its
// cpending mapping back — so a LIVE source re-sends a payload the client has already
// answered with PUBREC. That is the same retransmission as the phase gap, by the other
// route, and it needs its own kill: splitting only the phase leaves it open.
//
// Retirement is NOT a substitute phase, so the oracle requires the retirement disposition —
// a non-actionable reservation and source suppression — and NOT an invented pubrel_ready
// transition. Retired-AND-Cancelled is included because retirement dominates cancellation
// (F-087 WP3), so the contest's pure-cancellation branch does not consume it.
//
// Mutation check: M14, which drops the retirement split at this site alone.
func TestF099AdoptedRetirementAtThePlainContestTakesTheRetirementPath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		cancelled bool
	}{
		{"retired", false},
		{"retired and cancelled", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idHash, gen := getHash("f099-adopted-retired-"+tc.name), mqttNewSessGen()
			_, pubRelSubject, pubRelDurable := mqttPubRelNames(idHash, gen)
			rec := freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: 51, Dur: "SRC", Seq: 71, QoS: 2,
				Nonce: mqttNewInflightNonce(), Claim: "predecessor", ClaimSeq: 1,
			})
			subject := mqttInflightSubject(idHash, gen, rec.PI)
			sess := f099Session(nil, "f099-adopted-retired", idHash, gen, pubRelSubject, pubRelDurable)

			// The body the contest adopts: retirement acquired, phase UNCHANGED.
			adopted := *rec
			adopted.Retired = true
			adopted.Cancelled = tc.cancelled
			adopted.Claim = sess.regClaim
			adopted.ClaimSeq = sess.seq
			contest := &f099AdoptedContest{subject: subject, adopted: adopted, seq: 91, rereadAt: 92}

			as := &mqttAccountSessionManager{}
			sess.asm = as
			as.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
				return nil, 0, true
			}
			as.testInflightEnumerate = func(string) (map[string]uint64, error) {
				return map[string]uint64{subject: 1}, nil
			}
			as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
				data, err := json.Marshal(rec)
				if err != nil {
					t.Fatal(err)
				}
				return []*JSApiMsgGetResponse{{Message: &StoredMsg{
					Subject: subjects[0], Sequence: contest.seq, Data: data,
				}}}, nil
			}
			as.testInflightLoad = contest.load
			as.testInflightStore = contest.store
			as.testInflightDelete = func(uint64) error { return nil }
			// LIVE, because that is the arm where the retransmission actually happens.
			as.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return mqttInflightSourceLive
			}

			if _, err := sess.recoverInflightRegisters(&client{}); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}

			sess.mu.Lock()
			_, asPublish := sess.pendingPublish[rec.PI]
			srcMapped := sess.cpending[rec.Dur] != nil && sess.cpending[rec.Dur][rec.Seq] == rec.PI
			suppressed := sess.sourceRetiredLocked(rec.Dur, rec.Seq)
			owed := sess.pendingPubRel[rec.PI]
			sess.mu.Unlock()

			if asPublish {
				t.Fatal("an adopted RETIRED body was installed as an ordinary PUBLISH " +
					"delivery; a live source then re-sends an answered payload")
			}
			if srcMapped {
				t.Fatal("an adopted RETIRED body restored its source mapping")
			}
			if !suppressed {
				t.Fatal("an adopted RETIRED body did not suppress its source")
			}
			if owed == nil {
				t.Fatal("an adopted RETIRED body reserved nothing for its identifier")
			}
		})
	}
}
