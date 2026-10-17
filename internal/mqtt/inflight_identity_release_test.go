// The identity-conditioned, subject-addressed register release (F-034).
//
// These tests pin the contract that closed F-034's independently reachable defect: a
// register delete is addressed by SUBJECT and removes only the exact sequence that
// subject currently holds, and only when the occupant's identity matches.
//
// The defect the contract replaces, from F-066's own table: `RegSeq` is an absolute
// $MQTT_inflight sequence persisted in the session record, and the drain's release used
// to delete it directly "with no subject or record identity check at all". After that
// stream is recreated the sequence names an unrelated register — another session's — and
// the delete destroyed it.
//
// The mutations these pair with are the load-bearing part, and they are DIFFERENT
// invariants that an earlier draft conflated into one:
//
//	M7  removes the identity COMPARISON   -> the victim is destroyed
//	M7b restores RegSeq ADDRESSING        -> the victim is destroyed, even with the
//	                                        comparison still in place
//
// M7b exists because review found that M7 alone proves only occupant checking: a
// sequence-addressed implementation that KEPT the comparison would pass M7 while
// reproducing the phantom, because it would compare the wrong record.

package mqtt

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testInflightOccupant is one planted register, as the seam returns it.
type testInflightOccupant struct {
	rec *mqttInflightRecord
	seq uint64
}

// testPlantInflight installs a deterministic load seam over a subject -> occupant map,
// and records every delete the code under test issues. Nothing here talks to JetStream:
// the contract's six outcomes are decided entirely by what a load returns, so they are
// exactly testable at this level, and a fixture that spun up a cluster would prove less
// while hiding the discrimination behind timing.
func testPlantInflight(t *testing.T, as *mqttAccountSessionManager,
	occupants map[string]testInflightOccupant, loadErr map[string]error) *[]uint64 {

	t.Helper()
	deleted := &[]uint64{}
	as.mu.Lock()
	// THE MARKER SEAM MODELS THE STORE TOO, and it has to: C-IV's evidence loader probes
	// the marker before granting any release authority, and an UNRESOLVED marker withholds
	// (arm 1) — so a harness with no marker backend would make every release Undecided,
	// which is a fixture artefact rather than a property of the code.
	//
	// ABSENT is this harness's truthful model: it plants a register and never writes a
	// marker, so there is none. A test that wants a marker present or unreadable overrides
	// this seam, exactly as it overrides the load seam above.
	if as.testInflightMarkerLoad == nil {
		as.testInflightMarkerLoad = func(string) (*StoredMsg, error) { return nil, nil }
	}
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if err := loadErr[subject]; err != nil {
			return nil, nil, err
		}
		occ, ok := occupants[subject]
		if !ok {
			return nil, nil, nil // empty subject
		}
		// The seam MODELS THE STORE: once this occupant's sequence has been deleted the
		// subject reads as empty. Without that the release's re-read loop would see the
		// same record forever — which is a fixture artefact, not a property of the code,
		// and one that hid behind a static map until the loop was added.
		for _, d := range *deleted {
			if d == occ.seq {
				return nil, nil, nil
			}
		}
		smsg := &StoredMsg{Sequence: occ.seq, Subject: subject}
		if occ.rec == nil {
			return nil, smsg, nil // present but unreadable
		}
		return occ.rec, smsg, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		*deleted = append(*deleted, seq)
		return nil
	}
	as.mu.Unlock()
	return deleted
}

// testPlantUnsentMarkers models the MARKER half of the store for a planted fixture:
// `unsent` maps a register subject to the nonce its still-present unsent marker names,
// and every other subject reads ABSENT — which is what a store holds once a PUBLISH has
// left, and is deliberately NOT a proof of anything.
//
// A present marker naming the record's OWN nonce is the only proof of non-transmission a
// fixture may supply for an exchange that never transmitted (C-IV arm 4, F-098). It is
// declared per subject AND per nonce rather than as a blanket "grant authority" switch,
// because that is the shape of the real evidence: a predecessor's marker on the same
// subject names a different exchange and licenses nothing, and a fixture whose exchange
// actually TRANSMITS has no marker to declare. Handing one to such a fixture would be
// fabricating the proof rather than supplying it.
func testPlantUnsentMarkers(as *mqttAccountSessionManager, unsent map[string]string) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.testInflightMarkerLoad = func(markerSubject string) (*StoredMsg, error) {
		reg := strings.TrimSuffix(markerSubject, "."+mqttInflightMarkerSuffix)
		nonce, ok := unsent[reg]
		if !ok {
			return nil, nil // no marker beside this register: possibly-attempted
		}
		body, err := mqttBuildInflightMarker(nonce)
		if err != nil {
			return nil, err
		}
		return &StoredMsg{Subject: markerSubject, Sequence: 1, Data: body}, nil
	}
}

// testPlantAbsentMarkers is the truthful default for a harness that plants registers and
// never writes a marker beside them. It exists so a release fixture has an evidence
// BACKEND at all: with none, C-IV's loader reports the marker INDETERMINATE and arm 1
// withholds every release, which is a fixture artefact rather than a property of the code.
func testPlantAbsentMarkers(as *mqttAccountSessionManager) {
	testPlantUnsentMarkers(as, nil)
}

// TestInflightIdentityReleaseContract walks all six outcomes of the contract, because
// each one is a caller obligation rather than an implementation detail: two authorize
// the caller to record progress, one deletes, and three withhold.
func TestInflightIdentityReleaseContract(t *testing.T) {
	t.Parallel()
	const (
		subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
		ourDur  = "$MQTT_abc"
		ourSeq  = uint64(41)
	)
	ours := func() *mqttInflightRecord {
		return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: ourDur, Seq: ourSeq, QoS: 2, Nonce: mqttNewInflightNonce()})
	}

	for _, tc := range []struct {
		name      string
		occupants map[string]testInflightOccupant
		// unsent names the subjects whose planted occupant still has its OWN unsent
		// marker beside it. That is the one admissible proof of non-transmission, and a
		// release that DELETES requires it (C-IV arm 4); every other subject models a
		// store with no marker, which proves only that the exchange is possibly-attempted.
		unsent  []string
		loadErr map[string]error
		want    mqttInflightIdentityOutcome
		wantDel []uint64
	}{
		{
			name:      "absent subject is already gone",
			occupants: map[string]testInflightOccupant{},
			want:      mqttInflightAlreadyGone,
		},
		{
			name:      "an identity match PROVEN UNSENT is released at the sequence the load returned",
			occupants: map[string]testInflightOccupant{subject: {rec: ours(), seq: 900}},
			unsent:    []string{subject},
			want:      mqttInflightReleased,
			wantDel:   []uint64{900},
		},
		{
			// D3, and the half of the contract that separates RELEASING THE REQUEST from
			// FREEING THE IDENTIFIER. The occupant is ours by identity, so the caller's
			// request is discharged and its drain may proceed — but with no marker and no
			// cancellation nothing proves the PUBLISH never left, so the slot is PRESERVED
			// and the identifier stays reserved. The pre-F-098 contract deleted here, which
			// is the second opinion about a live generation's identifier that C-IV removes.
			name:      "an identity match with NO proof of non-transmission is released WITHOUT deletion",
			occupants: map[string]testInflightOccupant{subject: {rec: ours(), seq: 904}},
			want:      mqttInflightReleased,
			wantDel:   nil,
		},
		{
			name: "identity MISMATCH preserves the occupant",
			occupants: map[string]testInflightOccupant{subject: {
				rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_other", Seq: 12345, QoS: 2, Nonce: mqttNewInflightNonce()}),
				seq: 901,
			}},
			want: mqttInflightAlreadyGone,
			// The whole point: nothing is deleted. Under sequence addressing this is
			// the case that destroyed another session's register.
			wantDel: nil,
		},
		{
			name: "a LATER exchange on the same identifier is preserved too",
			occupants: map[string]testInflightOccupant{subject: {
				rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: ourDur, Seq: ourSeq + 1, QoS: 2, Nonce: mqttNewInflightNonce()}),
				seq: 902,
			}},
			want:    mqttInflightAlreadyGone,
			wantDel: nil,
		},
		{
			name:      "an unreadable occupant fails closed",
			occupants: map[string]testInflightOccupant{subject: {rec: nil, seq: 903}},
			want:      mqttInflightUndecided,
			wantDel:   nil,
		},
		{
			name:      "an indeterminate read fails closed and is never an absence",
			occupants: map[string]testInflightOccupant{},
			loadErr:   map[string]error{subject: fmt.Errorf("timeout")},
			want:      mqttInflightUndecided,
			wantDel:   nil,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			as := &mqttAccountSessionManager{}
			deleted := testPlantInflight(t, as, tc.occupants, tc.loadErr)
			markers := map[string]string{}
			for _, subj := range tc.unsent {
				markers[subj] = tc.occupants[subj].rec.Nonce
			}
			testPlantUnsentMarkers(as, markers)

			got := as.releaseInflightRegisterByIdentity(nil, &mqttSession{asm: as}, subject, ourDur, ourSeq)
			if got != tc.want {
				t.Errorf("outcome = %d, want %d", got, tc.want)
			}
			if len(*deleted) != len(tc.wantDel) {
				t.Fatalf("deleted %v, want %v", *deleted, tc.wantDel)
			}
			for i, seq := range tc.wantDel {
				if (*deleted)[i] != seq {
					t.Errorf("delete %d = %d, want %d", i, (*deleted)[i], seq)
				}
			}
		})
	}
}

// TestInflightIdentityReleaseSparesAReusedSequence is the fixture M7 and M7b both
// mutate, and it is written to fail for the RIGHT reason under each.
//
// The staging is the recreation: our register is GONE (the stream was recreated), and an
// unrelated session's register now occupies the absolute sequence our record still
// remembers. Two distinct facts are asserted:
//
//  1. addressing by our SUBJECT finds nothing, so we release nothing — which is what
//     M7b breaks by addressing the remembered sequence instead;
//  2. if the subject is instead occupied by a FOREIGN register, the identity comparison
//     spares it — which is what M7 breaks by deleting whatever it finds.
func TestInflightIdentityReleaseSparesAReusedSequence(t *testing.T) {
	t.Parallel()
	const (
		ourSubject    = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
		victimSubject = mqttInflightStreamSubjectPrefix + "zyxwvuts.hhhhhhhhhhhhhhhhhhhhhh.9"
		ourDur        = "$MQTT_abc"
		ourSeq        = uint64(41)
		rememberedSeq = uint64(500) // what the drain record still carries as RegSeq
	)

	t.Run("our subject is empty after the recreation, so nothing is released", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		// The victim occupies the sequence our record remembers, but on ITS OWN
		// subject. Addressing by subject cannot reach it.
		deleted := testPlantInflight(t, as, map[string]testInflightOccupant{
			victimSubject: {rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 9, Dur: "$MQTT_victim", Seq: 77,
				QoS: 2, Nonce: mqttNewInflightNonce()}), seq: rememberedSeq},
		}, nil)

		if got := as.releaseInflightRegisterByIdentity(nil, &mqttSession{asm: as}, ourSubject, ourDur, ourSeq); got != mqttInflightAlreadyGone {
			t.Fatalf("outcome = %d, want AlreadyGone", got)
		}
		if len(*deleted) != 0 {
			t.Fatalf("the victim at sequence %d was destroyed: deletes = %v", rememberedSeq, *deleted)
		}
	})

	t.Run("a foreign occupant on our subject is spared by the identity comparison", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		deleted := testPlantInflight(t, as, map[string]testInflightOccupant{
			ourSubject: {rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_victim", Seq: 77,
				QoS: 2, Nonce: mqttNewInflightNonce()}), seq: rememberedSeq},
		}, nil)

		if got := as.releaseInflightRegisterByIdentity(nil, &mqttSession{asm: as}, ourSubject, ourDur, ourSeq); got != mqttInflightAlreadyGone {
			t.Fatalf("outcome = %d, want AlreadyGone", got)
		}
		if len(*deleted) != 0 {
			t.Fatalf("a foreign register on our own subject was destroyed: deletes = %v", *deleted)
		}
	})
}

// testReapEnv drives reapIncarnationRegisters deterministically: a subject enumeration
// with a watermark, a batched load, and a record of every delete issued.
//
// The enumeration and the load are separate seams ON PURPOSE. The window this pass has to
// get right is the one BETWEEN them — a register written after the enumeration took its
// watermark — and a fixture that produced both from one snapshot could not stage it.
type testReapEnv struct {
	subjects  map[string]uint64 // what the enumeration returns
	watermark uint64
	loaded    map[string]testInflightOccupant // what the batched load returns
	// mu guards deleted: the reap issues its confirmed deletes CONCURRENTLY under
	// mqttInflightReapConcurrency, so a seam that records them is called from many
	// goroutines. An unguarded append here stayed quiet under -race only because no
	// fixture had enough sequences to run two at once.
	mu      sync.Mutex
	deleted []uint64
	enumErr error
	loadErr error
}

func (e *testReapEnv) record(seq uint64) {
	e.mu.Lock()
	e.deleted = append(e.deleted, seq)
	e.mu.Unlock()
}

func (e *testReapEnv) deletions() []uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]uint64, len(e.deleted))
	copy(out, e.deleted)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (e *testReapEnv) install(t *testing.T, as *mqttAccountSessionManager) {
	t.Helper()
	as.mu.Lock()
	as.testInflightEnumerateBounded = func(string) (map[string]uint64, uint64, error) {
		if e.enumErr != nil {
			return nil, 0, e.enumErr
		}
		return e.subjects, e.watermark, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		e.record(seq)
		return nil
	}
	as.mu.Unlock()
}

// TestReapIncarnationRegistersRespectsItsWatermark is the fixture M8 and M8b mutate, and
// like M7/M7b they target DIFFERENT invariants:
//
//	M8  restores blind, unbounded deletion  -> a register on a non-enumerated subject dies
//	M8b drops the watermark check           -> a register written AFTER the enumeration
//	                                           dies, which M8 cannot show because that
//	                                           register IS enumerated
//
// The second case is the one identity checking provably cannot see: the pass learns both
// the nonce and the sequence from the same load, so it holds no prior identity with which
// to call the newcomer a mismatch. Only the watermark separates them.
func TestReapIncarnationRegistersRespectsItsWatermark(t *testing.T) {
	t.Parallel()
	const (
		idHash = "abcdefgh"
		gen    = "gggggggggggggggggggggg"
	)
	ourSubject := mqttInflightSubject(idHash, gen, 7)
	lateSubject := mqttInflightSubject(idHash, gen, 8)

	t.Run("a register at or below the watermark is reaped", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		env := &testReapEnv{
			subjects:  map[string]uint64{ourSubject: 1},
			watermark: 100,
			loaded: map[string]testInflightOccupant{ourSubject: {
				rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_abc", Seq: 41, QoS: 2, Nonce: mqttNewInflightNonce()}),
				seq: 90,
			}},
		}
		env.install(t, as)
		testPlantInflightLoadMulti(t, as, env)
		if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err != nil {
			t.Fatalf("reap: %v", err)
		}
		if got := env.deletions(); len(got) != 1 || got[0] != 90 {
			t.Fatalf("deleted %v, want [90]", env.deletions())
		}
	})

	t.Run("a register ABOVE the watermark is spared", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		env := &testReapEnv{
			subjects:  map[string]uint64{ourSubject: 1, lateSubject: 1},
			watermark: 100,
			loaded: map[string]testInflightOccupant{
				ourSubject: {rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_abc", Seq: 41,
					QoS: 2, Nonce: mqttNewInflightNonce()}), seq: 90},
				// Written after the enumeration took its watermark: enumerated, but not
				// this pass's to destroy.
				lateSubject: {rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 8, Dur: "$MQTT_abc", Seq: 42,
					QoS: 2, Nonce: mqttNewInflightNonce()}), seq: 900},
			},
		}
		env.install(t, as)
		testPlantInflightLoadMulti(t, as, env)
		if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err != nil {
			t.Fatalf("reap: %v", err)
		}
		if got := env.deletions(); len(got) != 1 || got[0] != 90 {
			t.Fatalf("deleted %v, want [90] — the register above the watermark must survive", env.deletions())
		}
	})

	t.Run("a zero watermark is not a licence to delete anything", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		env := &testReapEnv{
			subjects:  map[string]uint64{ourSubject: 1},
			watermark: 0,
			loaded: map[string]testInflightOccupant{ourSubject: {
				rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_abc", Seq: 41, QoS: 2, Nonce: mqttNewInflightNonce()}),
				seq: 90,
			}},
		}
		env.install(t, as)
		testPlantInflightLoadMulti(t, as, env)
		if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err == nil {
			t.Fatal("a zero watermark must refuse the pass, not license an unbounded one")
		}
		if got := env.deletions(); len(got) != 0 {
			t.Fatalf("deleted %v with no proven bound", env.deletions())
		}
	})

	t.Run("an unreadable occupant aborts the teardown", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		env := &testReapEnv{
			subjects:  map[string]uint64{ourSubject: 1},
			watermark: 100,
			loaded:    map[string]testInflightOccupant{ourSubject: {rec: nil, seq: 90}},
		}
		env.install(t, as)
		testPlantInflightLoadMulti(t, as, env)
		if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err == nil {
			t.Fatal("an unreadable register must abort the teardown rather than be purged blind")
		}
		if got := env.deletions(); len(got) != 0 {
			t.Fatalf("deleted %v despite an unreadable occupant", env.deletions())
		}
	})
}

// testPlantInflightLoadMulti serves the reap's batched load from the env's `loaded` map,
// which is deliberately separate from the enumeration's `subjects`: the two are different
// observations, and the window the reap must get right lies between them.
func testPlantInflightLoadMulti(t *testing.T, as *mqttAccountSessionManager, e *testReapEnv) {
	t.Helper()
	as.mu.Lock()
	as.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		if e.loadErr != nil {
			return nil, e.loadErr
		}
		out := make([]*JSApiMsgGetResponse, 0, len(subjects))
		for _, subject := range subjects {
			occ, ok := e.loaded[subject]
			if !ok {
				out = append(out, &JSApiMsgGetResponse{ApiResponse: ApiResponse{
					Error: &ApiError{ErrCode: uint16(JSNoMessageFoundErr), Code: 404},
				}})
				continue
			}
			data := []byte("not json")
			if occ.rec != nil {
				b, err := json.Marshal(occ.rec)
				if err != nil {
					t.Fatalf("marshalling a planted register: %v", err)
				}
				data = b
			}
			out = append(out, &JSApiMsgGetResponse{Message: &StoredMsg{
				Subject: subject, Sequence: occ.seq, Data: data,
			}})
		}
		return out, nil
	}
	as.mu.Unlock()
}

// TestInflightIdentityReleaseFollowsARewrite covers the window holistic review round 2
// found: the release loaded a MATCHING register at S, deleted S, and returned Released —
// but if the same delivery was rewritten to S2 in between, S was already absent, the
// confirmed delete read that as success, and the caller went on to clear its progress,
// persist its claim removal and acknowledge the source while S2 survived. The same
// phantom subject addressing exists to prevent, reached one step later.
//
// The fixture stages exactly that: the first load sees S, and by the time the release
// re-reads, the subject holds S2 with the SAME (Dur, Seq). A correct release follows the
// rewrite; the pre-fix one reported Released after deleting S alone.
func TestInflightIdentityReleaseFollowsARewrite(t *testing.T) {
	t.Parallel()
	const (
		subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
		ourDur  = "$MQTT_abc"
		ourSeq  = uint64(41)
	)
	// THE REWRITE KEEPS THE WHOLE TRANSMISSION IDENTITY — the same allocation's nonce and the
	// same counter — because that is what "the same delivery, new sequence" means under C-I.
	// This fixture used to mint a FRESH nonce per occupant, which made S2 a DIFFERENT
	// allocation and turned the test into a requirement to delete a successor on evidence
	// about ours (holistic review round 1, finding 1; C14). The successor cases are now their
	// own twins below.
	nonce := mqttNewInflightNonce()
	rec := func() *mqttInflightRecord {
		return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: ourDur, Seq: ourSeq, QoS: 2, Nonce: nonce})
	}

	as := &mqttAccountSessionManager{}
	// A sequence of occupants: S, then the rewrite at S2, then clear.
	occupants := []testInflightOccupant{
		{rec: rec(), seq: 100},
		{rec: rec(), seq: 200}, // the rewrite: same identity, new sequence
	}
	var reads int
	var deleted []uint64
	as.mu.Lock()
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		defer func() { reads++ }()
		if reads >= len(occupants) {
			return nil, nil, nil // clear
		}
		occ := occupants[reads]
		return occ.rec, &StoredMsg{Sequence: occ.seq, Subject: subject}, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		deleted = append(deleted, seq)
		return nil
	}
	as.mu.Unlock()
	// THE SAME ALLOCATION'S OWN MARKER, and it stays present across the rewrite: a rewrite
	// moves a register to a new sequence, it does not put the PUBLISH on the wire. So the
	// proof of non-transmission that licensed deleting S licenses following it to S2, which
	// is exactly the window this fixture exists for — without it the release would stop at
	// the first slot and leave S2 behind for a different reason than the one under test.
	testPlantUnsentMarkers(as, map[string]string{subject: nonce})

	got := as.releaseInflightRegisterByIdentity(nil, &mqttSession{asm: as}, subject, ourDur, ourSeq)
	if got != mqttInflightReleased {
		t.Fatalf("outcome = %d, want Released once the subject is genuinely clear", got)
	}
	// BOTH sequences must be deleted. Deleting only 100 is the defect: the caller would
	// then persist its claim removal and ack the source while 200 survived with its
	// consumer about to be destroyed.
	if len(deleted) != 2 || deleted[0] != 100 || deleted[1] != 200 {
		t.Fatalf("deleted %v, want [100 200] — the release must follow the rewrite rather than "+
			"reading an already-absent sequence as a clear subject", deleted)
	}
}

// testReleaseSequence plants a sequence of occupants the release reads one after another,
// modelling the store: each read returns the next occupant until they run out, and every
// delete is recorded.
func testReleaseSequence(t *testing.T, as *mqttAccountSessionManager, subject string,
	occupants []testInflightOccupant) *[]uint64 {

	t.Helper()
	var reads int
	deleted := &[]uint64{}
	as.mu.Lock()
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		defer func() { reads++ }()
		if reads >= len(occupants) {
			return nil, nil, nil
		}
		occ := occupants[reads]
		return occ.rec, &StoredMsg{Sequence: occ.seq, Subject: subject}, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		*deleted = append(*deleted, seq)
		return nil
	}
	as.mu.Unlock()
	// The truthful default: this harness plants registers and writes no marker beside
	// them, so the marker reads ABSENT. A fixture that means to prove non-transmission
	// overrides it with testPlantUnsentMarkers.
	testPlantAbsentMarkers(as)
	return deleted
}

// THE SUCCESSOR TWINS (C14). After OUR sequence is deleted, the subject is found holding the
// same source under a DIFFERENT transmission identity — a new allocation (fresh nonce), or the
// same allocation transmitted again (Acq+1). Either is a successor: preserved, AlreadyGone,
// and only sequence 100 deleted. The pre-fix release followed the source and deleted it.
//
// Mutation M93: compare only (Dur, Seq) in the rewrite loop, as HEAD did.
func TestInflightIdentityReleasePreservesASuccessor(t *testing.T) {
	t.Parallel()
	const (
		subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
		ourDur  = "$MQTT_abc"
		ourSeq  = uint64(41)
	)
	ours := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: ourDur, Seq: ourSeq, QoS: 2, Nonce: mqttNewInflightNonce()})
	newAllocation := *ours
	newAllocation.Nonce = mqttNewInflightNonce()
	retransmitted := *ours
	retransmitted.Acq = ours.Acq + 1
	for _, tc := range []struct {
		name      string
		successor *mqttInflightRecord
	}{
		{"a new allocation for the same source", &newAllocation},
		{"the same allocation transmitted again", &retransmitted},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			as := &mqttAccountSessionManager{}
			deleted := testReleaseSequence(t, as, subject, []testInflightOccupant{
				{rec: ours, seq: 100},
				{rec: tc.successor, seq: 200},
			})
			// OUR OWN ALLOCATION'S MARKER, so the deletion of 100 is licensed by real
			// evidence and the successor's survival is attributable to IDENTITY FENCING
			// rather than to a fixture with no evidence backend — which would have
			// preserved 200 for the wrong reason and proved nothing about C14. It matters
			// most for the retransmitted twin: that record shares our nonce, so it is
			// PROVEN UNSENT too, and only its advanced Acq stands between it and deletion.
			testPlantUnsentMarkers(as, map[string]string{subject: ours.Nonce})
			got := as.releaseInflightRegisterByIdentity(nil, &mqttSession{asm: as}, subject, ourDur, ourSeq)
			if got != mqttInflightAlreadyGone {
				t.Fatalf("outcome = %d, want AlreadyGone: the successor is not ours to release", got)
			}
			if len(*deleted) != 1 || (*deleted)[0] != 100 {
				t.Fatalf("deleted %v, want [100] only — the successor at 200 must survive", *deleted)
			}
		})
	}
}

// THE PERSISTED REQUEST BINDS EVERY RETRY (C13 gate for finding 1). Rel(k) was recorded by an
// earlier attempt — possibly before a restart — and the delete did not land; the subject now
// holds the same allocation at k+1. The retry must NOT delete it, must retire exactly Rel(k),
// and must leave a NEWER request alone.
//
// Mutation M94: have the release rebuild its identity from the first load when a Rel exists.
func TestInflightIdentityReleaseIsBoundToThePersistedRequest(t *testing.T) {
	t.Parallel()
	const (
		subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
		ourDur  = "$MQTT_abc"
		ourSeq  = uint64(41)
	)
	nonce := mqttNewInflightNonce()
	atK := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: ourDur, Seq: ourSeq, QoS: 2, Nonce: nonce})
	atK1 := *atK
	atK1.Acq = atK.Acq + 1
	relK := &mqttPersistedDrainRelease{Subject: subject, Nonce: nonce, Acq: atK.Acq}

	drainSession := func(as *mqttAccountSessionManager, rel *mqttPersistedDrainRelease) (*mqttSession, *drainSeq) {
		st := &drainSeq{claimed: true, rel: rel}
		sess := &mqttSession{asm: as, drainByDur: map[string]*mqttDrain{
			ourDur: {cc: &ConsumerConfig{Durable: ourDur}, seqs: map[uint64]*drainSeq{ourSeq: st}},
		}}
		return sess, st
	}

	t.Run("a superseded request retires and deletes nothing", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		deleted := testReleaseSequence(t, as, subject, []testInflightOccupant{{rec: &atK1, seq: 300}})
		// The occupant shares our nonce, so its marker is OURS and it is proven unsent.
		// The stale Acq is then the only thing standing between the retry and a deletion,
		// which is what this subtest is for: `Rel` binds the REQUEST, and non-transmission
		// is a separate fact that never authorizes acting on a different acquisition.
		testPlantUnsentMarkers(as, map[string]string{subject: nonce})
		sess, st := drainSession(as, relK)
		if got := as.releaseInflightRegisterByIdentity(nil, sess, subject, ourDur, ourSeq); got != mqttInflightAlreadyGone {
			t.Fatalf("outcome = %d, want AlreadyGone", got)
		}
		if len(*deleted) != 0 {
			t.Fatalf("deleted %v: a request for Acq %d authorizes nothing about Acq %d", *deleted, atK.Acq, atK1.Acq)
		}
		// THE REQUEST STANDS until the claim removal that consumes it is durable (holistic
		// review round 4, finding 1). Retiring it here was the earlier rule, and it let a
		// retry whose claim removal had been refused bind to the successor this call just
		// preserved. What must not happen is a DELETION, asserted above.
		if st.rel != relK {
			t.Fatalf("the request must still bind this retry, got %+v", st.rel)
		}
	})
	t.Run("the matching request still deletes its own target", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		deleted := testReleaseSequence(t, as, subject, []testInflightOccupant{{rec: atK, seq: 300}})
		// The releasable target's OWN matching marker: the request identifies WHICH slot,
		// and this is the separate proof that its PUBLISH never left, which is what
		// licenses freeing the identifier rather than merely discharging the request.
		testPlantUnsentMarkers(as, map[string]string{subject: nonce})
		sess, _ := drainSession(as, relK)
		if got := as.releaseInflightRegisterByIdentity(nil, sess, subject, ourDur, ourSeq); got != mqttInflightReleased {
			t.Fatalf("outcome = %d, want Released", got)
		}
		if len(*deleted) != 1 || (*deleted)[0] != 300 {
			t.Fatalf("deleted %v, want [300]", *deleted)
		}
	})
	t.Run("an uninterpretable request never falls back to the occupant", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		deleted := testReleaseSequence(t, as, subject, []testInflightOccupant{{rec: atK, seq: 300}})
		sess, _ := drainSession(as, &mqttPersistedDrainRelease{Subject: subject, Nonce: "not-a-nonce"})
		if got := as.releaseInflightRegisterByIdentity(nil, sess, subject, ourDur, ourSeq); got != mqttInflightUndecided {
			t.Fatalf("outcome = %d, want Undecided", got)
		}
		if len(*deleted) != 0 {
			t.Fatalf("deleted %v on a request this broker cannot interpret", *deleted)
		}
	})
	// A VALID REQUEST FOR A DIFFERENT TARGET — the drained delivery was re-offered under a new
	// identifier after an earlier attempt recorded it. It is settled against ITS OWN target
	// first, then the release proceeds on the current one. The pre-fix release refused such a
	// request as uninterpretable on every retry, so the drain was never retired.
	// Mutation M110: refuse a Rel naming another subject.
	t.Run("a request for a different target is settled first", func(t *testing.T) {
		t.Parallel()
		const oldSubject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.6"
		as := &mqttAccountSessionManager{}
		deleted := testPlantInflight(t, as, map[string]testInflightOccupant{
			subject: {rec: atK, seq: 300}, // the current target; the old one is gone
		}, nil)
		// Only the CURRENT target has a marker. The old subject holds nothing at all, so
		// settling its request cannot be mistaken for evidence about this one.
		testPlantUnsentMarkers(as, map[string]string{subject: nonce})
		old := &mqttPersistedDrainRelease{Subject: oldSubject, Nonce: mqttNewInflightNonce(), Acq: 1}
		sess, st := drainSession(as, old)
		if got := as.releaseInflightRegisterByIdentity(nil, sess, subject, ourDur, ourSeq); got != mqttInflightReleased {
			t.Fatalf("outcome = %d, want Released once the old request is settled", got)
		}
		if len(*deleted) != 1 || (*deleted)[0] != 300 {
			t.Fatalf("deleted %v, want [300] — the current target, after the old request settled", *deleted)
		}
		if st.rel != nil && st.rel.Subject == oldSubject {
			t.Fatalf("the settled request for %q must be retired", oldSubject)
		}
	})
	t.Run("retirement never clears a newer request", func(t *testing.T) {
		t.Parallel()
		newer := &mqttPersistedDrainRelease{Subject: subject, Nonce: nonce, Acq: atK1.Acq}
		sess, st := drainSession(&mqttAccountSessionManager{}, newer)
		sess.retireDrainReleaseRequest(ourDur, ourSeq, relK)
		if st.rel != newer {
			t.Fatalf("retiring Rel(%d) cleared the newer Rel(%d)", relK.Acq, newer.Acq)
		}
	})
}

// TestReapIncarnationRegistersResamplesAZeroWatermark covers the rule holistic review
// round 2 required: a zero watermark is RESAMPLED, not refused on the first sample.
//
// STREAM.INFO builds State before it fills State.Subjects from a separate store read, so a
// walk that started against an empty stream can legitimately be handed a non-empty subject
// map with a zero last sequence. The inbound hold walk already applies this rule; an
// earlier revision of the reap refused immediately, which turned an ordinary sampling
// artefact into a wedged teardown.
//
// Nothing is written between samples, which is what makes retrying safe, and the watermark
// never rewinds, so a later sample resolves it.
func TestReapIncarnationRegistersResamplesAZeroWatermark(t *testing.T) {
	t.Parallel()
	const (
		idHash = "abcdefgh"
		gen    = "gggggggggggggggggggggg"
	)
	subject := mqttInflightSubject(idHash, gen, 7)

	t.Run("a zero first sample is retried and then succeeds", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		env := &testReapEnv{
			subjects: map[string]uint64{subject: 1},
			loaded: map[string]testInflightOccupant{subject: {
				rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_abc", Seq: 41, QoS: 2,
					Nonce: mqttNewInflightNonce()}), seq: 90,
			}},
		}
		var samples int
		as.mu.Lock()
		as.testInflightEnumerateBounded = func(string) (map[string]uint64, uint64, error) {
			samples++
			if samples == 1 {
				return env.subjects, 0, nil // the sampling artefact
			}
			return env.subjects, 100, nil
		}
		as.testInflightDelete = func(seq uint64) error {
			env.record(seq)
			return nil
		}
		as.mu.Unlock()
		testPlantInflightLoadMulti(t, as, env)

		if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err != nil {
			t.Fatalf("a zero FIRST sample must be resampled, not refused: %v", err)
		}
		if samples < 2 {
			t.Errorf("took %d samples, want at least 2 — the zero was not resampled", samples)
		}
		if got := env.deletions(); len(got) != 1 || got[0] != 90 {
			t.Errorf("deleted %v, want [90]", env.deletions())
		}
	})

	t.Run("a persistently zero watermark exhausts and refuses", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		env := &testReapEnv{subjects: map[string]uint64{subject: 1}}
		as.mu.Lock()
		as.testInflightEnumerateBounded = func(string) (map[string]uint64, uint64, error) {
			return env.subjects, 0, nil
		}
		as.testInflightDelete = func(seq uint64) error {
			env.record(seq)
			return nil
		}
		as.mu.Unlock()
		testPlantInflightLoadMulti(t, as, env)

		if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err == nil {
			t.Fatal("an unusable watermark must refuse the pass rather than license an unbounded one")
		}
		if got := env.deletions(); len(got) != 0 {
			t.Fatalf("deleted %v with no proven bound", env.deletions())
		}
	})
}

// TestFinalizeDrainDeliveryAddressesByPacketIdentifier pins the addressing decision ON THE
// PRODUCTION PATH, which is what holistic review round 2 required.
//
// The earlier M7b test called drainRegisterSubject directly, so a regression that pointed
// finalizeDrainDelivery back at deleteInflightRegisterConfirmed(regSeq) — leaving the
// helper intact but unused — would have passed it. That is
// feedback_fixture_must_reach_its_route exactly: a green test that never reaches the code
// it names.
//
// So this drives finalizeDrainDelivery itself and asserts which SUBJECT the release asked
// for. The register read is made indeterminate on purpose: the release then withholds and
// the finalize returns before opening a record transaction, which keeps the fixture to the
// one decision under test.
func TestFinalizeDrainDeliveryAddressesByPacketIdentifier(t *testing.T) {
	t.Parallel()
	const (
		idHash        = "abcdefgh"
		gen           = "gggggggggggggggggggggg"
		jsDur         = "$MQTT_drain_abc"
		sourceSeq     = uint64(41)
		pi            = uint16(7)
		rememberedSeq = uint64(500) // the stale RegSeq a recreation made meaningless
	)

	as := &mqttAccountSessionManager{}
	var asked []string
	as.mu.Lock()
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		asked = append(asked, subject)
		// Indeterminate: the release withholds, so the finalize returns before any
		// record transaction. The assertion is about the SUBJECT, not the outcome.
		return nil, nil, fmt.Errorf("test: indeterminate")
	}
	as.testInflightDelete = func(seq uint64) error {
		t.Errorf("no delete may be issued when the register read is indeterminate: got %d", seq)
		return nil
	}
	as.mu.Unlock()

	sess := &mqttSession{id: "drain-addressing", idHash: idHash, gen: gen, asm: as}
	d := &mqttDrain{
		cc: &ConsumerConfig{Durable: jsDur},
		seqs: map[uint64]*drainSeq{
			sourceSeq: {claimed: true, pi: pi, regSeq: rememberedSeq},
		},
	}
	sess.drainByDur = map[string]*mqttDrain{jsDur: d}

	if handled := sess.finalizeDrainDelivery(nil, jsDur, sourceSeq, _EMPTY_); !handled {
		t.Fatal("the finalize must handle the delivery, withholding on an unresolved register")
	}
	if len(asked) != 1 {
		t.Fatalf("the release made %d reads, want exactly 1: %v", len(asked), asked)
	}
	want := mqttInflightSubject(idHash, gen, pi)
	if asked[0] != want {
		t.Errorf("the release addressed %q, want %q — the production path must derive the "+
			"subject from the packet identifier, never from the remembered sequence %d",
			asked[0], want, rememberedSeq)
	}
	// And the remembered sequence must not appear in the address at all, which is the
	// discriminating half: a subject built from it would name a different identifier.
	if asked[0] == mqttInflightSubject(idHash, gen, uint16(rememberedSeq%65535+1)) {
		t.Errorf("the release addressed a subject derived from the remembered sequence: %q", asked[0])
	}
}

// TestReapIncarnationRegistersPipelinesUnderItsCap exercises the CONCURRENT half of the
// reap, which no other fixture reaches: with one or two sequences the delete pipeline runs
// a single goroutine, so the -race detector never sees it and the concurrency cap is
// untested.
//
// It asserts three things a serial implementation would also satisfy but a broken pipeline
// would not: every doomed sequence is deleted exactly once, in-flight deletes never exceed
// mqttInflightReapConcurrency, and the pass still reports success. Run under -race, the
// unguarded-seam bug this file previously had would surface here.
func TestReapIncarnationRegistersPipelinesUnderItsCap(t *testing.T) {
	t.Parallel()
	const (
		idHash = "abcdefgh"
		gen    = "gggggggggggggggggggggg"
		count  = 300 // comfortably above the cap, and above one load batch would be 1024
	)

	as := &mqttAccountSessionManager{}
	env := &testReapEnv{
		subjects:  make(map[string]uint64, count),
		watermark: 1_000_000,
		loaded:    make(map[string]testInflightOccupant, count),
	}
	for i := 1; i <= count; i++ {
		subject := mqttInflightSubject(idHash, gen, uint16(i))
		env.subjects[subject] = 1
		env.loaded[subject] = testInflightOccupant{
			rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: uint16(i), Dur: "$MQTT_abc", Seq: uint64(i), QoS: 2,
				Nonce: mqttNewInflightNonce()}),
			seq: uint64(1000 + i),
		}
	}

	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)
	as.mu.Lock()
	as.testInflightEnumerateBounded = func(string) (map[string]uint64, uint64, error) {
		return env.subjects, env.watermark, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		// Long enough that concurrent callers genuinely overlap, short enough not to
		// slow the suite.
		time.Sleep(time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		env.record(seq)
		return nil
	}
	as.mu.Unlock()
	testPlantInflightLoadMulti(t, as, env)

	if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err != nil {
		t.Fatalf("reap: %v", err)
	}
	got := env.deletions()
	if len(got) != count {
		t.Fatalf("deleted %d sequences, want %d", len(got), count)
	}
	seen := map[uint64]bool{}
	for _, seq := range got {
		if seen[seq] {
			t.Fatalf("sequence %d was deleted twice", seq)
		}
		seen[seq] = true
	}
	if peak > mqttInflightReapConcurrency {
		t.Errorf("peak concurrent deletes = %d, which exceeds the cap of %d",
			peak, mqttInflightReapConcurrency)
	}
	if peak < 2 {
		// Otherwise this fixture proves nothing about the pipeline: a serial
		// implementation would pass every assertion above.
		t.Errorf("peak concurrent deletes = %d, so the pipeline never actually ran "+
			"concurrently and this fixture does not discriminate", peak)
	}
}

// TestReapIncarnationRegistersAggregatesFailures pins the failure-aggregation half: a
// partial pass must REPORT that it was partial, because the teardown must not release the
// record while any register of unknown status remains.
func TestReapIncarnationRegistersAggregatesFailures(t *testing.T) {
	t.Parallel()
	const (
		idHash = "abcdefgh"
		gen    = "gggggggggggggggggggggg"
		count  = 40
	)
	as := &mqttAccountSessionManager{}
	env := &testReapEnv{
		subjects:  make(map[string]uint64, count),
		watermark: 1_000_000,
		loaded:    make(map[string]testInflightOccupant, count),
	}
	for i := 1; i <= count; i++ {
		subject := mqttInflightSubject(idHash, gen, uint16(i))
		env.subjects[subject] = 1
		env.loaded[subject] = testInflightOccupant{
			rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: uint16(i), Dur: "$MQTT_abc", Seq: uint64(i), QoS: 2,
				Nonce: mqttNewInflightNonce()}),
			seq: uint64(1000 + i),
		}
	}
	as.mu.Lock()
	as.testInflightEnumerateBounded = func(string) (map[string]uint64, uint64, error) {
		return env.subjects, env.watermark, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		if seq%7 == 0 {
			return fmt.Errorf("test: delete unavailable")
		}
		env.record(seq)
		return nil
	}
	as.mu.Unlock()
	testPlantInflightLoadMulti(t, as, env)

	err := as.reapIncarnationRegisters(nil, idHash, gen, false)
	if err == nil {
		t.Fatal("a pass with failed deletions must report failure, or the teardown would " +
			"release the record while registers of unknown status remain")
	}
	// Progress is still real: the deletions that landed stay landed. Whether anything
	// re-enumerates the remainder is the CALLER's property and is NOT what this fixture
	// shows — it drives reapIncarnationRegisters directly. A record-backed teardown keeps
	// its marked record and re-drives; clear()'s no-record branch has neither record nor
	// marker, so there the survivors are the orphan sweep's alone.
	if len(env.deletions()) == 0 {
		t.Error("no deletion landed at all, so the aggregation is hiding total failure")
	}
}

// TestFinalizeDrainDeliveryDoesNotTrustAZeroRegSeq covers the defect holistic review round
// 3 found: the release used to run only when the stale `regSeq` hint was NONZERO.
//
// That guard was itself the bug. Recovery zeroes `regSeq` for the AMBIGUOUS duplicate-register
// case, so a zero hint meant "do not release", and the finalize read it as "nothing to
// release" — skipping classification and continuing into durable claim removal and source
// acknowledgement while the registers survived behind a consumer about to be deleted. That
// is the indefinite identifier reservation the whole change exists to prevent, reached
// through the guard meant to prevent it.
//
// Two states, two required behaviours, and the point is that a zero hint decides NEITHER.
func TestFinalizeDrainDeliveryDoesNotTrustAZeroRegSeq(t *testing.T) {
	t.Parallel()
	const (
		idHash    = "abcdefgh"
		gen       = "gggggggggggggggggggggg"
		jsDur     = "$MQTT_drain_abc"
		sourceSeq = uint64(41)
		pi        = uint16(7)
	)
	subject := mqttInflightSubject(idHash, gen, pi)

	t.Run("regSeq zero with a MATCHING occupant still releases it", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		var deleted []uint64
		// ONE allocation across every read, so the marker planted below names THIS
		// occupant. A fresh nonce per read would make each load a different exchange and
		// no marker could ever match one.
		nonce := mqttNewInflightNonce()
		as.mu.Lock()
		as.testInflightLoad = func(s string) (*mqttInflightRecord, *StoredMsg, error) {
			if s != subject {
				t.Errorf("addressed %q, want the PI-derived %q", s, subject)
			}
			if len(deleted) > 0 {
				// The confirming re-read is made indeterminate on purpose: the release
				// then withholds and the finalize returns BEFORE opening a record
				// transaction, keeping this fixture to the one decision under test —
				// that a zero hint did not skip the release. The delete has already
				// happened by then, which is what the assertion checks.
				return nil, nil, fmt.Errorf("test: indeterminate confirming read")
			}
			return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: jsDur, Seq: sourceSeq, QoS: 2,
				Nonce: nonce}), &StoredMsg{Sequence: 90, Subject: subject}, nil
		}
		as.testInflightDelete = func(seq uint64) error {
			deleted = append(deleted, seq)
			return nil
		}
		as.mu.Unlock()
		// The occupant's own marker. Nothing in this scenario requires prior transmission —
		// the drain is releasing a delivery whose PUBLISH never left — so this is the
		// truthful store state, and it is what licenses the deletion of 90 that the zero
		// hint must not skip.
		testPlantUnsentMarkers(as, map[string]string{subject: nonce})

		sess := &mqttSession{id: "zero-hint", idHash: idHash, gen: gen, asm: as}
		d := &mqttDrain{cc: &ConsumerConfig{Durable: jsDur}, seqs: map[uint64]*drainSeq{
			// The hint is ZERO, and there is nonetheless a register of ours to release.
			sourceSeq: {claimed: true, pi: pi, regSeq: 0},
		}}
		sess.drainByDur = map[string]*mqttDrain{jsDur: d}

		_ = sess.finalizeDrainDelivery(nil, jsDur, sourceSeq, _EMPTY_)
		if len(deleted) != 1 || deleted[0] != 90 {
			t.Fatalf("deleted %v, want [90] — a zero hint must not skip the release", deleted)
		}
	})

	t.Run("an AMBIGUOUS delivery releases nothing and does not finalize", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		var deleted []uint64
		var loads int
		as.mu.Lock()
		as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
			loads++
			return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: jsDur, Seq: sourceSeq, QoS: 2,
				Nonce: mqttNewInflightNonce()}), &StoredMsg{Sequence: 90, Subject: subject}, nil
		}
		as.testInflightDelete = func(seq uint64) error {
			deleted = append(deleted, seq)
			return nil
		}
		as.mu.Unlock()

		sess := &mqttSession{id: "ambiguous", idHash: idHash, gen: gen, asm: as}
		st := &drainSeq{claimed: true, pi: pi, regSeq: 0, regAmbiguous: true}
		d := &mqttDrain{cc: &ConsumerConfig{Durable: jsDur}, seqs: map[uint64]*drainSeq{sourceSeq: st}}
		sess.drainByDur = map[string]*mqttDrain{jsDur: d}

		if handled := sess.finalizeDrainDelivery(nil, jsDur, sourceSeq, _EMPTY_); !handled {
			t.Fatal("the finalize must handle the delivery, withholding on ambiguity")
		}
		if len(deleted) != 0 {
			t.Errorf("deleted %v: an ambiguous delivery must release NOTHING", deleted)
		}
		if loads != 0 {
			t.Errorf("made %d register reads: ambiguity is decided before any read", loads)
		}
		// And the claim must still stand, or the source would be acknowledged.
		if !st.claimed {
			t.Error("the claim was cleared, so the finalize proceeded despite ambiguity")
		}
	})
}

// TestReapIncarnationRegistersTargetsTheLoadedSequence pins the EXACT-TARGET invariant of
// row 8i, which plan revision 6 assigned to M9 and which an earlier revision of these tests
// silently dropped — the identifier M9 was reused for the concurrency cap instead. Review
// caught the substitution.
//
// The window: a register is enumerated, loaded at S, and REPLACED at S2 before the delete
// is issued. Deleting the exact loaded sequence removes nothing (S is vacated), so the
// replacement survives for the next pass. An implementation that instead deleted a
// "current" target — re-reading the subject, or purging it — would destroy the replacement,
// which belongs to whoever wrote it.
//
// The fixture stages that by making the delete seam observe the sequence it is handed while
// the load has already reported S.
func TestReapIncarnationRegistersTargetsTheLoadedSequence(t *testing.T) {
	t.Parallel()
	const (
		idHash = "abcdefgh"
		gen    = "gggggggggggggggggggggg"
	)
	subject := mqttInflightSubject(idHash, gen, 7)

	as := &mqttAccountSessionManager{}
	env := &testReapEnv{
		subjects:  map[string]uint64{subject: 1},
		watermark: 1_000_000,
		loaded: map[string]testInflightOccupant{subject: {
			rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_abc", Seq: 41, QoS: 2,
				Nonce: mqttNewInflightNonce()}),
			seq: 100, // S, what the load reports
		}},
	}
	env.install(t, as)
	testPlantInflightLoadMulti(t, as, env)
	// A single-record re-read of the same subject reports the REPLACEMENT at S2. Only an
	// implementation that addresses a moving target consults it, which is what makes this
	// fixture discriminate rather than merely pass.
	as.mu.Lock()
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_abc", Seq: 41, QoS: 2,
			Nonce: mqttNewInflightNonce()}), &StoredMsg{Sequence: 200, Subject: subject}, nil
	}
	as.mu.Unlock()

	if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err != nil {
		t.Fatalf("reap: %v", err)
	}
	got := env.deletions()
	if len(got) != 1 {
		t.Fatalf("issued %d deletes, want exactly 1: %v", len(got), got)
	}
	// The ONLY acceptable target is the sequence the load returned. A "current target"
	// implementation would have re-read and deleted 200 instead, destroying a replacement
	// it never proved anything about.
	if got[0] != 100 {
		t.Errorf("deleted sequence %d, want the LOADED sequence 100 — a destructive pass may "+
			"only target what it read and classified, never a moving target", got[0])
	}
}

// TestInflightDestructivePathsBindPIToSubject pins the rule holistic review round 4 found
// missing from BOTH destructive paths: a record whose body names a different packet
// identifier than the subject it was addressed by is UNREADABLE, not deletable.
//
// Recovery already refuses such a record. A destructive path may not be more permissive
// than the reader — being so is how it removes evidence for an identifier the client is
// still holding, on a record the rest of the tree declines to interpret.
func TestInflightDestructivePathsBindPIToSubject(t *testing.T) {
	t.Parallel()
	const (
		idHash = "abcdefgh"
		gen    = "gggggggggggggggggggggg"
		ourDur = "$MQTT_abc"
		ourSeq = uint64(41)
	)
	subject := mqttInflightSubject(idHash, gen, 7)
	// Body says PI 9; the subject says 7. (Dur, Seq) match, so a check on those alone
	// would delete it.
	inconsistent := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 9, Dur: ourDur, Seq: ourSeq, QoS: 2,
		Nonce: mqttNewInflightNonce()})

	t.Run("the release refuses it", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		deleted := testPlantInflight(t, as, map[string]testInflightOccupant{
			subject: {rec: inconsistent, seq: 100},
		}, nil)
		if got := as.releaseInflightRegisterByIdentity(nil, &mqttSession{asm: as}, subject, ourDur, ourSeq); got != mqttInflightUndecided {
			t.Fatalf("outcome = %d, want Undecided: a body/subject mismatch is unreadable", got)
		}
		if len(*deleted) != 0 {
			t.Fatalf("deleted %v: an inconsistent record must not be destroyed", *deleted)
		}
	})

	t.Run("the teardown reap refuses it and aborts", func(t *testing.T) {
		t.Parallel()
		as := &mqttAccountSessionManager{}
		env := &testReapEnv{
			subjects:  map[string]uint64{subject: 1},
			watermark: 1_000_000,
			loaded:    map[string]testInflightOccupant{subject: {rec: inconsistent, seq: 100}},
		}
		env.install(t, as)
		testPlantInflightLoadMulti(t, as, env)
		if err := as.reapIncarnationRegisters(nil, idHash, gen, false); err == nil {
			t.Fatal("the reap must abort on a body/subject mismatch rather than delete it")
		}
		if got := env.deletions(); len(got) != 0 {
			t.Fatalf("deleted %v: an inconsistent record must not be destroyed", got)
		}
	})
}

// TestDrainAmbiguityClearsWhenTheDuplicatesVanish pins the convergence hole review round 4
// found in my own reasoning: I had checked that a group of exactly ONE clears the flag, but
// not the case where BOTH duplicates disappear — the grouping loop only visits pairs that
// still have registers, so the flag stayed set and the drain stayed unfinalizable forever
// with allocation withheld, while no ambiguous register remained.
//
// A flag that only ever sets is a latch, not an observation. The fix recomputes from
// scratch over every claimed entry, and this asserts the zero-member case specifically.
func TestDrainAmbiguityClearsWhenTheDuplicatesVanish(t *testing.T) {
	t.Parallel()
	const (
		jsDur     = "$MQTT_drain_abc"
		sourceSeq = uint64(41)
	)
	st := &drainSeq{claimed: true, pi: 7, regSeq: 0, regAmbiguous: true}
	d := &mqttDrain{cc: &ConsumerConfig{Durable: jsDur}, seqs: map[uint64]*drainSeq{sourceSeq: st}}
	sess := &mqttSession{id: "converge", drains: map[string]*mqttDrain{"filter": d}}

	// Drive the PRODUCTION recomputation with an EMPTY scan result — both duplicates gone.
	// The grouping loop then visits nothing for this key, which is exactly the case that
	// used to leave the flag standing. Calling the real function rather than re-enacting
	// it is the point: feedback_fixture_must_reach_its_route.
	sess.mu.Lock()
	owned, ambiguous := sess.recomputeDrainRegisterStateLocked(map[string][]mqttRecoveredInflightReg{})
	sess.mu.Unlock()
	if len(owned) != 0 || len(ambiguous) != 0 {
		t.Errorf("an empty scan reported owned=%v ambiguous=%v", owned, ambiguous)
	}

	if st.regAmbiguous {
		t.Fatal("the flag survived a scan that found no registers at all: it is a latch, not " +
			"an observation, and the drain can never finalize")
	}
}

// TestStoreInflightRegisterRefusesAMismatchedOccupant reaches the THIRD destructive path —
// the store's ambiguous-resolution switch — which holistic review round 5 found lacked the
// PI-to-subject binding that rounds 4 required of the other two.
//
// Every branch of that switch either replaces or deletes on the strength of the body it has
// just read: it reaps a `Cancelled` record, reaps one whose named source is provably
// complete, or takes the slot over from a same-source writer. Recovery classifies a record
// whose body PI disagrees with its subject as UNREADABLE and reserves the identifier; this
// path could erase it and then transmit under that identifier, which is how a corrupted
// record becomes an identifier reused while the client may still hold it.
//
// Staged with the real environment rather than seams, because the point is that the
// PRODUCTION path refuses: a mismatched occupant is planted on the subject the allocation
// will address, and the store must refuse the identifier and leave the record alone.
func TestStoreInflightRegisterRefusesAMismatchedOccupant(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-pi-mismatch")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Occupy identifier 42's subject with a record whose BODY names 43. It is otherwise
	// perfectly valid — and `Cancelled`, so without the binding the switch would happily
	// reap it as "never transmitted" and hand out 42.
	e.sess.mu.Lock()
	e.sess.last_pi = 41
	e.sess.mu.Unlock()
	mismatched := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 43, Dur: dur, Seq: 999, QoS: 2,
		// F-088: a cancellation carries its written provenance; this one is an occupant CAS.
		Nonce: mqttNewInflightNonce(), Cancelled: true, CancelOrigin: mqttCancelOriginOccupant})
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 42)
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, mismatched); err != nil {
		t.Fatalf("seed a mismatched register: %v", err)
	}

	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 500, 1))
	e.sess.mu.Unlock()
	if pi != 42 || !isNew {
		t.Fatalf("expected the allocation to land on 42, got pi=%d isNew=%v", pi, isNew)
	}

	if e.sess.storeInflightRegister(e.c, pi, 2, dur, 500, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatal("an identifier occupied by a record whose body names a DIFFERENT identifier " +
			"must be refused: the record is unreadable by recovery's own rule, and reaping it " +
			"here would free this identifier on evidence about another one")
	}
	// And the record must survive: it could be a live delivery's identity under the
	// identifier its body actually names.
	rec, _ := e.loadRegister(t, 42)
	if rec == nil || rec.Nonce != mismatched.Nonce {
		t.Fatalf("the mismatched register was destroyed (%+v)", rec)
	}
}

// ---------------------------------------------------------------------------
// The F-036 necessity result, as a shippable reproducer
// ---------------------------------------------------------------------------

// TestInflightLiveOwnerContentionNeedsAnOwnershipPredicate is the artifact plan stopping
// rule 5 requires before F-034 may route its residual contention to F-036: a **passing**
// reproducer that positively asserts the open precondition, rather than a failing test that
// cannot ship.
//
// # Why the first version of this test did not count
//
// It labelled a synthetic record as a live peer's and asserted that F-034's guards accepted
// it. Review rejected that, correctly: it built no peer, no ownership transition and no
// active exchange, so every assertion passed for an ordinary uncontended record too. It
// proved that matching fields satisfy the guards, which is not the precondition — and it
// completed in 0.00s, which was the tell.
//
// # What this builds instead
//
// TWO REAL BROKERS over one JetStream, and a genuine ownership move:
//
//	A provisions a session, starts a delivery, and persists its in-flight register.
//	B claims the teardown of that generation, so DURABLE OWNERSHIP HAS MOVED to B.
//	A is never told, and stays resident on that generation with the register live.
//
// Then it asserts, positively, that **both actors present identical naming evidence over the
// same register**: A's own authority matches it, and B — now the durable owner — enumerates
// it with an entitlement watermark that covers it. Neither side is distinguishable from a
// legitimate operation by anything F-034 added, because the two actors do not disagree about
// identity. They disagree about who owns the session, and no amount of naming answers that.
//
// That is the necessity result: what is needed is an ownership predicate, which is F-036's —
// a resident session object can run register recovery against a generation whose durable
// ownership has moved, because a resident object never reads the retirement watermark that
// the record-restoring CONNECT path does (createOrRestoreSessionWithin, sess_manager.go).
func TestInflightLiveOwnerContentionNeedsAnOwnershipPredicate(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const cid = "f036-contention"

	// ---- Actor A: a live session with a real started delivery -------------------
	sessA := testProvisionSession(t, a, cid)
	dur := sessA.idHash + "_" + nuid.Next()

	sessA.mu.Lock()
	sessA.last_pi = 6
	pi, isNew := sessA.trackPublish(dur, testInflightAckSubject(dur, 500, 1))
	var nonce string
	if ack := sessA.pendingPublish[pi]; ack != nil {
		nonce = ack.regNonce
	}
	sessA.mu.Unlock()
	if !isNew || pi == 0 || nonce == _EMPTY_ {
		t.Fatalf("the allocation must be new and carry a nonce: pi=%d isNew=%v nonce=%q", pi, isNew, nonce)
	}
	testOpenStoreCustody(t, sessA, pi)
	if !sessA.storeInflightRegister(a.asm.jsa.c, pi, 2, dur, 500, nonce, _EMPTY_, _EMPTY_) {
		t.Fatal("A's register must persist: without it there is no contended object")
	}
	subject := mqttInflightSubject(sessA.idHash, sessA.gen, pi)
	gen, idHash := sessA.gen, sessA.idHash

	rec, smsg, err := a.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("A's register must be readable: rec=%v smsg=%v err=%v", rec, smsg, err)
	}
	if rec.Nonce != nonce {
		t.Fatalf("the register on %q is not A's: nonce %q want %q", subject, rec.Nonce, nonce)
	}

	// ---- The ownership move: B claims the teardown of A's generation ------------
	sessB, _, err := b.asm.createOrRestoreSession(cid, b.s.getOpts())
	if err != nil {
		t.Fatalf("B restoring the session: %v", err)
	}
	cur, curSeq := testLoadSessRecord(t, b, idHash)
	if cur == nil {
		t.Fatal("the session record must exist for B to claim its teardown")
	}
	tok, ok := sessB.tryBeginRecordTx()
	if !ok {
		t.Fatal("B must be able to open a record transaction")
	}
	_, _, outcome, cerr := sessB.claimTeardown(tok, cur, curSeq, cur.WID, nil)
	if terr := sessB.endRecordTx(tok); terr != nil {
		t.Fatalf("B releasing its record transaction: %v", terr)
	}
	if outcome != mqttTeardownClaimWon {
		t.Fatalf("B must win the teardown claim to move ownership: outcome=%v err=%v", outcome, cerr)
	}

	// A IS NEVER TOLD. Still resident on that generation, register still live.
	if sessA.gen != gen {
		t.Fatalf("A's generation changed to %q: the contended state requires it to keep %q", sessA.gen, gen)
	}
	rec2, smsg2, err := a.asm.loadInflightRecord(subject)
	if err != nil || rec2 == nil || rec2.Nonce != nonce {
		t.Fatalf("A's register did not survive the ownership move, so nothing is contended: "+
			"rec=%v err=%v", rec2, err)
	}

	// ---- The precondition, asserted positively ----------------------------------
	//
	// 1. A's own authority still matches the register exactly, so every check F-034 added
	//    passes on A's side: nothing stops A from continuing to use it.
	if !mqttInflightRecordMatchesSubject(rec2, subject) {
		t.Error("the PI-to-subject binding rejects A's own register: not the contended shape")
	}
	if !mqttInflightRecordIdentityOK(rec2) {
		t.Error("A's own register is unreadable: not the contended shape")
	}
	if rec2.Dur != dur || rec2.Seq != 500 {
		t.Errorf("A's authority (%s/%d) does not match its register (%s/%d)",
			dur, 500, rec2.Dur, rec2.Seq)
	}

	// 2. And B, now the durable owner, enumerates that same register with an entitlement
	//    watermark that COVERS it — so B's cleanup would match it by identical evidence.
	subjects, watermark, werr := b.asm.enumerateInflightSubjectsBounded(mqttInflightWalkFilter(idHash, gen))
	if werr != nil {
		t.Fatalf("B enumerating A's registers: %v", werr)
	}
	if _, seen := subjects[subject]; !seen {
		t.Fatalf("B's enumeration did not reach %q, so B is not a competitor for it", subject)
	}
	if watermark < smsg2.Sequence {
		t.Fatalf("B's watermark %d does not cover A's register at %d, so B is not entitled and "+
			"this is not the contended shape", watermark, smsg2.Sequence)
	}

	// 3. THE RESULT: two reachable actors, identical naming evidence over one register, one
	//    still live, and nothing in identity or entitlement separating them. Every guard
	//    F-034 added is satisfied on BOTH sides, which is why naming cannot resolve this and
	//    an ownership predicate — F-036's — is what is required.
	//
	//    If a future change makes any assertion above fail, this residual has been closed by
	//    something other than F-036 and the routing must be re-derived.
	t.Logf("contended: register %q at sequence %d is durably owned by B and still live on A "+
		"(generation %q); B's entitlement watermark is %d", subject, smsg2.Sequence, gen, watermark)
}

// TestDrainAmbiguityStaysWhenTheSurvivorIsOnAnotherPI is the fixture holistic review round 6
// found MISSING: the dossier named it and the branch it covers went into the tree without it.
// The mutation ledger recorded neither. That is feedback_fixture_must_reach_its_route in its
// worst form — not a fixture that missed the route it named, a fixture that was never written
// at all, while the claim that it existed was carried forward into a review.
//
// # What the branch is for
//
// Review round 5's fix made the ambiguity flag an OBSERVATION rather than a latch, recomputed
// from scratch each scan. That created a second hole in the other direction: a group of
// exactly one cleared the flag on size alone. With duplicates on identifiers A and B, if A's
// register vanishes the next scan sees a singleton on B and reads it as settled — while this
// drain entry still ADDRESSES A. The release then finds A absent, reads AlreadyGone, and lets
// the claim and the consumer go, while B survives as a phantom register holding a Receive
// Maximum slot for the life of the session.
//
// So a singleton settles an entry only when it sits on the identifier that entry addresses.
// Both directions are asserted, because a guard that refused every singleton would also pass
// the ambiguous half while breaking every ordinary recovery.
func TestDrainAmbiguityStaysWhenTheSurvivorIsOnAnotherPI(t *testing.T) {
	t.Parallel()
	const (
		jsDur     = "$MQTT_drain_survivor"
		sourceSeq = uint64(41)
		entryPI   = uint16(7)
		regSeq    = uint64(500)
	)

	// One scan member, and the ONLY thing that varies between the two runs is the identifier
	// its body names. Everything else — the durable, the source sequence, the register
	// sequence — is held fixed, so a pass cannot come from the group being unmatched.
	scanOn := func(pi uint16) map[string][]mqttRecoveredInflightReg {
		return map[string][]mqttRecoveredInflightReg{
			"single": {{
				rec: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: jsDur, Seq: sourceSeq, QoS: 2,
					Nonce: mqttNewInflightNonce()}),
				seq: regSeq,
			}},
		}
	}
	newSess := func() (*mqttSession, *drainSeq) {
		st := &drainSeq{claimed: true, pi: entryPI}
		d := &mqttDrain{cc: &ConsumerConfig{Durable: jsDur}, seqs: map[uint64]*drainSeq{sourceSeq: st}}
		return &mqttSession{
			id:         "survivor",
			drains:     map[string]*mqttDrain{"filter": d},
			drainByDur: map[string]*mqttDrain{jsDur: d},
		}, st
	}

	t.Run("the survivor is on THIS entry's identifier", func(t *testing.T) {
		sess, st := newSess()
		sess.mu.Lock()
		owned, ambiguous := sess.recomputeDrainRegisterStateLocked(scanOn(entryPI))
		sess.mu.Unlock()

		if len(ambiguous) != 0 {
			t.Errorf("a singleton on the addressed identifier was reported ambiguous: %v", ambiguous)
		}
		if !owned["single"] {
			t.Errorf("a singleton on the addressed identifier was not reported owned: %v", owned)
		}
		if st.regAmbiguous {
			t.Error("the entry stayed ambiguous even though the one surviving register is the " +
				"identifier it addresses: no scan can ever finalize this delivery")
		}
		if st.regSeq != regSeq {
			t.Errorf("the register hint is %d, want the scanned sequence %d: without it the "+
				"release has nothing to address", st.regSeq, regSeq)
		}
	})

	t.Run("the survivor is on ANOTHER identifier", func(t *testing.T) {
		sess, st := newSess()
		sess.mu.Lock()
		owned, ambiguous := sess.recomputeDrainRegisterStateLocked(scanOn(entryPI + 2))
		sess.mu.Unlock()

		if len(owned) != 0 {
			t.Errorf("a singleton on a DIFFERENT identifier was reported owned: %v", owned)
		}
		if !ambiguous["single"] {
			t.Errorf("a singleton on a DIFFERENT identifier was not reported ambiguous: %v", ambiguous)
		}
		if !st.regAmbiguous {
			t.Fatal("one register survived, on an identifier this entry does not address, and the " +
				"entry was read as settled: the release will find its own identifier absent, " +
				"take that for completion, and leave the survivor as a phantom holding a " +
				"Receive Maximum slot")
		}
		if st.regSeq != 0 {
			t.Errorf("the register hint is %d: an ambiguous entry must address nothing", st.regSeq)
		}
	})
}

// ---------------------------------------------------------------------------
// The read-after-CAS reconciliation family
// ---------------------------------------------------------------------------

// Holistic review round 6 found the PI-to-subject binding missing from a fourth shape, and
// the invariant I had written down was wrong rather than merely under-applied. Rounds 4 and
// 5 audited DELETE sites: "no other delete site reads a body it classifies." The
// reconciliation paths delete nothing — they CAS-REPLACE, at the sequence they just read —
// and a replace is destructive by the same route. It turns a record recovery calls
// unreadable, whose identifier is therefore RESERVED, into a valid tombstone for the
// subject's own identifier, which a later resume reads as freeable.
//
// So the invariant is: every destructive path — delete OR overwrite — that reads a body
// binds it to its subject. These four fixtures are the family, one per site.
//
// Each has to make the FIRST CAS fail, because the reconciliation read only happens after a
// rejection. Two of them do it with a stale expected sequence and one with a create-only
// expectation against an occupied subject, which is the shape each caller really uses.

// testPlantMismatchedRegister occupies pi's subject with a record that is valid in every
// respect EXCEPT that its body names a different identifier, and returns the sequence it
// landed on. terminal marks it Retired or Cancelled, which is what selects between a
// reconciliation arm that overwrites and one that reads it as already resolved.
func testPlantMismatchedRegister(t *testing.T, e *testInflightEnv, pi uint16, dur string,
	sseq uint64, nonce string, retired, cancelled bool) (string, uint64) {

	t.Helper()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	plant := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: pi + 1, Dur: dur, Seq: sseq, QoS: 2, Nonce: nonce,
		Claim: e.sess.regClaim, Retired: retired, Cancelled: cancelled,
	})
	if cancelled {
		plant.CancelOrigin = mqttCancelOriginOccupant // F-088: a cancellation carries its provenance
	}
	seq, err := e.asm.jsa.storeInflightCAS(subject, 0, plant)
	if err != nil {
		t.Fatalf("plant a mismatched register on %q: %v", subject, err)
	}
	return subject, seq
}

// testRequireUntouchedPlant asserts the planted record is still exactly what was planted:
// same nonce, and still naming the identifier the subject does NOT. Both halves matter — a
// bare nonce check would pass against a tombstone rewritten in place, which is precisely the
// overwrite these tests forbid.
func testRequireUntouchedPlant(t *testing.T, e *testInflightEnv, subject, nonce string, wantPI uint16) {
	t.Helper()
	rec, smsg, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil {
		t.Fatalf("re-read %q: %v", subject, err)
	}
	if smsg == nil || rec == nil {
		t.Fatalf("the mismatched record on %q was destroyed", subject)
	}
	if rec.Nonce != nonce || rec.PI != wantPI {
		t.Fatalf("the record on %q was rewritten to %+v: recovery reserves this identifier and "+
			"keeps the record, so a reconciliation may refuse but never replace", subject, rec)
	}
}

// TestMarkInflightRegisterRetiredRefusesAMismatchedOccupant covers site 1 of the family.
//
// A retirement says "this exchange is past PUBREC" about ONE identifier. Written over a
// record whose body names another, it both destroys evidence the reader was preserving and
// asserts the fact under the wrong identifier. Fail closed: the caller keeps the PUBREL, so
// the exchange stays recoverable, which is the outcome a lost retirement is designed for.
func TestMarkInflightRegisterRetiredRefusesAMismatchedOccupant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		retired bool
	}{
		{"the arm that would OVERWRITE it", false},
		{"the arm that would read it as ALREADY retired", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "client-retire-"+strconv.FormatBool(tc.retired))
			dur := e.sess.idHash + "_" + nuid.Next()
			nonce := mqttNewInflightNonce()
			subject, seq := testPlantMismatchedRegister(t, e, 42, dur, 500, nonce, tc.retired, false)

			// A STALE expected sequence, so the first CAS is rejected and the
			// reconciliation read runs. This is the real shape: regSeq is the sequence
			// the register was stored at, and a peer's rewrite moves it.
			got, ok := e.sess.markInflightRegisterRetired(e.c, 42, dur, 500, seq+7, nonce, _EMPTY_, _EMPTY_)
			if ok {
				t.Errorf("the retirement was reported durable at %d against a record naming a "+
					"different identifier: the caller will now acknowledge its PUBREL away", got)
			}
			if got != 0 {
				t.Errorf("a refused retirement returned sequence %d", got)
			}
			testRequireUntouchedPlant(t, e, subject, nonce, 43)
		})
	}
}

// TestMarkInflightRegisterCancelledRefusesAMismatchedOccupant covers site 2.
//
// A cancellation tombstone is one of two facts that let a resume FREE an identifier rather
// than reserve it — the other is an unsent marker over a non-reofferable source (F-102),
// which this test does not exercise. What it pins is the tombstone route, and that
// hold it, so writing one over a mismatched body is the most direct form of this defect:
// it frees this subject's identifier on evidence about another one. An error rather than the
// site's own "our register is gone" nil, because both callers answer an error by recording
// the nonce verdict on the session record — the same fact, on a rung this write cannot
// corrupt.
func TestMarkInflightRegisterCancelledRefusesAMismatchedOccupant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		cancelled bool
	}{
		{"the arm that would OVERWRITE it", false},
		{"the arm that would read it as ALREADY cancelled", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "client-cancel-"+strconv.FormatBool(tc.cancelled))
			dur := e.sess.idHash + "_" + nuid.Next()
			nonce := mqttNewInflightNonce()
			subject, seq := testPlantMismatchedRegister(t, e, 42, dur, 500, nonce, false, tc.cancelled)

			if _, err := e.sess.markInflightRegisterCancelled(e.c, 42, dur, 500, seq+7, nonce, mqttTestLicence(e.sess, nil)); err == nil {
				t.Error("the cancellation was reported durable against a record naming a different " +
					"identifier: a later resume will read this identifier as never transmitted " +
					"and hand it to a new delivery")
			}
			testRequireUntouchedPlant(t, e, subject, nonce, 43)
		})
	}
}

// TestMarkInflightRegisterCancelledPendingRefusesAMismatchedOccupant covers site 3, the one
// round 6 named.
//
// This is the same tombstone for an allocation whose own store is still in flight, so it has
// no sequence to expect and writes CREATE-ONLY — which is why any occupant at all fails the
// first CAS and every call reaches the reconciliation read. It also has the sharpest
// consequence: tombstoneCancellationsOnStream answers success by FORGETTING the in-memory
// nonce verdict, so a mismatched body read as resolution discards the last remaining record
// that this delivery was never transmitted.
func TestMarkInflightRegisterCancelledPendingRefusesAMismatchedOccupant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		cancelled bool
	}{
		{"the arm that would OVERWRITE it", false},
		{"the arm that would read it as ALREADY cancelled", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "client-pending-"+strconv.FormatBool(tc.cancelled))
			dur := e.sess.idHash + "_" + nuid.Next()
			nonce := mqttNewInflightNonce()
			subject, _ := testPlantMismatchedRegister(t, e, 42, dur, 500, nonce, false, tc.cancelled)

			rc := mqttRegCancellation{pi: 42, nonce: nonce, durable: dur, sseq: 500, reply: testInflightAckSubject(dur, 500, 1)}
			if err := e.sess.markInflightRegisterCancelledPending(e.c, rc, mqttTestLicence(e.sess, nil)); err == nil {
				t.Error("the pending cancellation was reported marked against a record naming a " +
					"different identifier, so its caller will forget the nonce verdict and no " +
					"durable evidence of 'never transmitted' will remain anywhere")
			}
			testRequireUntouchedPlant(t, e, subject, nonce, 43)
		})
	}
}

// TestMarkInflightRegisterCancelledRefusesAnUnreadableOccupant is holistic round 7's
// finding, and it is the OTHER half of the same omission the mismatch guard closed.
//
// `loadInflightRecord` has four outcomes and the second is `(nil, smsg, nil)`: a record is
// PRESENT but unreadable — malformed JSON, or readable JSON failing
// `mqttInflightRecordIdentityOK`. That is a definite fact about the record, not an
// indeterminate read, and it is not an absence.
//
// The site's `default` arm concluded "our register is gone" and returned success for it.
// For a FOREIGN readable record that conclusion is sound: something else demonstrably
// occupies the subject. For an unreadable one it is a positive claim drawn from a read that
// failed — and reporting success makes `voidRegistersOnStream` forget the nonce verdict and
// go on to destroy the consumer, so nothing anywhere records that this delivery was never
// transmitted while the record still occupies the identifier.
//
// The release and the store already fail closed on exactly this outcome, which is what
// makes the omission a gap in a rule rather than a design choice: I split the MISMATCHED
// record off into its own arm at round 6 and left the unreadable one behind.
func TestMarkInflightRegisterCancelledRefusesAnUnreadableOccupant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body func() ([]byte, int)
	}{
		{
			// Valid JSON, no usable identity: PI 0 fails mqttInflightRecordIdentityOK.
			// The same shape TestMQTTInflightRecoveryReservesUnreadableRegister plants.
			name: "valid JSON with no usable identity",
			body: func() ([]byte, int) { return buildInflightRecord(0, &mqttInflightRecord{State: mqttInflightPublish}) },
		},
		{
			// Not JSON at all, so the unmarshal itself fails. Built by hand rather than
			// through buildInflightRecord, which can only emit a well-formed record.
			name: "a body that is not JSON",
			body: func() ([]byte, int) {
				hdr := hdrLine + JSExpectedLastSubjSeq + ":0" + CR_LF + CR_LF
				return []byte(hdr + "not a register at all"), len(hdr)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "client-unreadable-cancel")
			dur := e.sess.idHash + "_" + nuid.Next()
			nonce := mqttNewInflightNonce()
			subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 42)

			msg, hdrLen := tc.body()
			resp, err := e.asm.jsa.storeMsg(subject, hdrLen, msg)
			if err != nil {
				t.Fatalf("seed an unreadable register: %v", err)
			}

			// A stale expected sequence, so the first CAS is rejected and the
			// reconciliation read runs and finds the unreadable occupant.
			if _, merr := e.sess.markInflightRegisterCancelled(e.c, 42, dur, 500, resp.Sequence+7, nonce, mqttTestLicence(e.sess, nil)); merr == nil {
				t.Error("the cancellation was reported durable against an UNREADABLE record: its " +
					"caller will now forget the nonce verdict and destroy the consumer, leaving " +
					"nothing anywhere that says this delivery was never transmitted")
			}
			// And the record must survive: an unreadable record still reserves its
			// identifier, and overwriting it would free that identifier.
			cur, smsg, lerr := e.asm.jsa.loadInflightRecord(subject)
			if lerr != nil {
				t.Fatalf("re-read %q: %v", subject, lerr)
			}
			if smsg == nil {
				t.Fatal("the unreadable record was destroyed")
			}
			if cur != nil {
				t.Fatalf("the unreadable record was REWRITTEN into a readable one (%+v)", cur)
			}
		})
	}

	// The caller-level half, because the cost of the defect is the caller's: an error must
	// leave the nonce verdict standing, which is the rung that survives when neither
	// register-stream rung can be proven.
	//
	// Staging note, and it is the point of this subtest rather than a detail. The FIRST
	// version planted the unreadable record at the very sequence the caller expects, and
	// nothing was refused: the CAS at that exact sequence SUCCEEDS, rewriting the record in
	// place with a truthful tombstone for the identifier the caller already knows is its
	// own. That is not the defect and it is arguably the better outcome. The refusal is
	// reached only when the register has MOVED — a peer rewrote it — and what now occupies
	// the subject cannot be read. So the plant is at a later sequence than the caller's,
	// and the fixture asserting a refusal is what tells the two situations apart.
	t.Run("its caller keeps the nonce verdict", func(t *testing.T) {
		e := testNewInflightEnv(t, "client-unreadable-void")
		dur := e.sess.idHash + "_" + nuid.Next()
		nonce := mqttNewInflightNonce()
		subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 42)

		// The register as this caller knows it: its own confirmed store, at ownSeq.
		ownSeq, err := e.asm.jsa.storeInflightCAS(subject, 0, freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
			PI: 42, Dur: dur, Seq: 500, QoS: 1, Nonce: nonce,
		}))
		if err != nil {
			t.Fatalf("seed this caller's own register: %v", err)
		}
		// And what actually occupies the subject now: a MOVED record that cannot be read.
		msg, hdrLen := buildInflightRecord(ownSeq, &mqttInflightRecord{State: mqttInflightPublish})
		if _, err := e.asm.jsa.storeMsg(subject, hdrLen, msg); err != nil {
			t.Fatalf("seed an unreadable register over it: %v", err)
		}

		// The confirmed delete must FAIL, or voidRegistersOnStream never reaches the
		// tombstone rung at all. A refused delete is the real shape: one storage domain,
		// so the delete and the tombstone usually fail together.
		e.asm.mu.Lock()
		e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("delete refused") }
		e.asm.mu.Unlock()

		voided := []mqttVoidedReg{{seq: ownSeq, nonce: nonce, pi: 42, dur: dur, sseq: 500}}
		if !e.sess.voidRegistersOnStream(e.c, voided, mqttTestLicence(e.sess, nil)) {
			t.Error("voidRegistersOnStream reported every register proven, though neither its " +
				"delete nor its tombstone could land")
		}
		e.sess.mu.Lock()
		_, kept := e.sess.voidedRegNonces[nonce]
		e.sess.mu.Unlock()
		if !kept {
			t.Fatal("the nonce verdict was not recorded: with the delete refused and the tombstone " +
				"refused, this is the only remaining rung that can tell a later resume the " +
				"delivery was never transmitted")
		}
	})
}

// TestFinalizeDrainDeliveryRefusesAfterAClearRemintsTheGeneration is holistic review round
// 23's HIGH finding, made deterministic.
//
// finalizeDrainDelivery captures (drain, seq) at its head, unlocks, and does JetStream work.
// A concurrent clear() nils drainByDur, RE-MINTS the generation, and resets last_pi to 0 so
// a reused object reallocates the same low identifiers. Reading sess.gen for the release
// without re-proving the captured drain therefore pairs the OLD delivery's packet identifier
// with the NEW generation — and releaseInflightRegisterByIdentity matches on the subject's
// own identifier plus (Dur, Seq), where shared group durables are named deterministically.
// The fresh incarnation's register is then a FULL match and is deleted while its client may
// still hold that identifier, with the record fence acquired too late to stop it.
//
// The planted record is a genuine production match rather than a partial struct: the body's
// PI agrees with the subject that addresses it, Dur and Seq are the values the stale drain
// carries, QoS is real and the nonce is the full 22 characters. A fixture that returned a
// half-built record would fail the release's own readability checks instead of reaching the
// identity comparison, and would pass against the defect.
//
// Mutations: M20 removes the pre-release currency check; M21 restores the live
// sess.idHash/sess.gen read for the subject. Each must fail this test.
func TestFinalizeDrainDeliveryRefusesAfterAClearRemintsTheGeneration(t *testing.T) {
	t.Parallel()
	const (
		idHash    = "abcdefgh"
		oldGen    = "gggggggggggggggggggggg"
		freshGen  = "hhhhhhhhhhhhhhhhhhhhhh"
		jsDur     = "$MQTT_SHARED_deterministic"
		sourceSeq = uint64(41)
		pi        = uint16(1) // what a reset allocator hands out again
		freshSeq  = uint64(9001)
	)

	as := &mqttAccountSessionManager{}
	sess := &mqttSession{id: "drain-reminted", idHash: idHash, gen: oldGen, asm: as}
	d := &mqttDrain{
		cc: &ConsumerConfig{Durable: jsDur},
		seqs: map[uint64]*drainSeq{
			sourceSeq: {claimed: true, pi: pi},
		},
	}
	sess.drainByDur = map[string]*mqttDrain{jsDur: d}

	// The FRESH incarnation's register: same durable (group durables are deterministic),
	// same source sequence (the group re-offered the message), same identifier (the
	// allocator was reset) — everything the release compares.
	freshSubject := mqttInflightSubject(idHash, freshGen, pi)
	var reads, deletes []string
	as.mu.Lock()
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		reads = append(reads, subject)
		if subject != freshSubject {
			return nil, nil, nil // nothing on the retired incarnation's subject
		}
		return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
			PI:    pi,
			Dur:   jsDur,
			Seq:   sourceSeq,
			QoS:   2,
			Nonce: "nnnnnnnnnnnnnnnnnnnnnn",
		}), &StoredMsg{Subject: freshSubject, Sequence: freshSeq}, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		deletes = append(deletes, fmt.Sprintf("seq=%d", seq))
		return nil
	}
	// The clear() happens in the one window that matters: after the entry capture, before
	// the release proves the drain current. Only the parts that drive this hazard are
	// staged — the drain maps dropped, the generation re-minted, the allocator reset.
	as.testDrainBeforeRegisterRelease = func() {
		sess.mu.Lock()
		sess.drainByDur = nil
		sess.gen = freshGen
		sess.last_pi = 0
		sess.mu.Unlock()
	}
	as.mu.Unlock()

	sess.finalizeDrainDelivery(nil, jsDur, sourceSeq, _EMPTY_)

	if len(deletes) != 0 {
		t.Fatalf("the stale finalizer deleted a FRESH incarnation's register: %v — its client "+
			"may still hold packet identifier %d, and recovery will not reserve it", deletes, pi)
	}
	for _, r := range reads {
		if r == freshSubject {
			t.Fatalf("the stale finalizer addressed the fresh incarnation's subject %q: the "+
				"release must not run at all once the captured drain is retired", r)
		}
	}
}

// TestFinalizeDrainDeliveryWithholdsWhenClearedDuringTheRelease is the SECOND window, and it
// is not a repetition of the first: clear() can finish during the register round trips, after
// which tryBeginRecordTx opens legitimately against the NEW record epoch while every local
// still refers to the RETIRED incarnation's drain state. Mutating, saving or acking then
// carries a retired delivery's decisions into a fresh session's record.
//
// Staged from inside the load hook, which runs during the release itself. The release is
// allowed to complete — that work is irreversible by design — and what must NOT happen is
// the save or the ack.
//
// Mutation: M22 removes the post-release, post-fence revalidation. Without it the second
// guard would be asserted but never mutation-proved.
func TestFinalizeDrainDeliveryWithholdsWhenClearedDuringTheRelease(t *testing.T) {
	t.Parallel()
	const (
		idHash    = "abcdefgh"
		oldGen    = "gggggggggggggggggggggg"
		freshGen  = "hhhhhhhhhhhhhhhhhhhhhh"
		jsDur     = "$MQTT_SHARED_midrelease"
		sourceSeq = uint64(53)
		pi        = uint16(3)
	)

	as := &mqttAccountSessionManager{}
	sess := &mqttSession{id: "drain-midrelease", idHash: idHash, gen: oldGen, asm: as}
	st := &drainSeq{claimed: true, pi: pi}
	d := &mqttDrain{
		cc:   &ConsumerConfig{Durable: jsDur},
		seqs: map[uint64]*drainSeq{sourceSeq: st},
	}
	sess.drainByDur = map[string]*mqttDrain{jsDur: d}

	acked := 0
	as.mu.Lock()
	as.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		// The release is under way; the session is cleared and reused right here.
		sess.mu.Lock()
		sess.drainByDur = nil
		sess.gen = freshGen
		sess.last_pi = 0
		sess.mu.Unlock()
		return nil, nil, nil // our register is already gone: the release succeeds
	}
	as.testInflightSourceAck = func(string) error {
		acked++
		return nil
	}
	as.mu.Unlock()

	if handled := sess.finalizeDrainDelivery(nil, jsDur, sourceSeq, _EMPTY_); !handled {
		t.Fatal("the delivery must be reported handled: the register release already happened, " +
			"so it may not be re-driven as though nothing had been done")
	}
	if acked != 0 {
		t.Fatalf("the stale finalizer acknowledged the source %d time(s) after the session was "+
			"cleared mid-release", acked)
	}
	// The retired state must be left exactly as it was: a retired delivery's claim is not
	// this fresh incarnation's to clear, and unclaimUnconfirmed marks a removal in progress.
	sess.mu.Lock()
	claimed, unconfirmed := st.claimed, st.unclaimUnconfirmed
	sess.mu.Unlock()
	if !claimed || unconfirmed {
		t.Fatalf("the retired drain state was mutated after the clear: claimed=%v unconfirmed=%v",
			claimed, unconfirmed)
	}
}
