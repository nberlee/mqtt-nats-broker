package mqtt

import (
	"strings"
	"testing"
)

// A create descends from the record whose deletion emptied the subject (F-029b).
//
// Two brokers that both find the subject empty and both create at base 0 are admitted
// pre-proposal, both apply, and MaxMsgsPer:1 discards the earlier — after which the erased
// side could prove nothing about the survivor, because both records were parentless. The
// durable identity of an empty-subject interval is the identity of the record that was
// deleted to produce it, and the generation-retirement watermark already records it
// permanently, per client, before every deletion.
//
// The tests here are about the create SIDE: what a record-absent create inherits, from
// which of the three sources, and what it deliberately inherits from nothing. The
// watermark's own writer and the CONNECT rollback that now arbitrates before it fences are
// in sess_retirement_test.go.

// testCreateLineageEnv is one broker plus the client ID under test.
type testCreateLineageEnv struct {
	*testTeardownEnv
	clientID string
	idHash   string
}

func testNewCreateLineageEnv(t *testing.T, clientID string) (*testCreateLineageEnv, *testTeardownEnv) {
	t.Helper()
	testQuietExpirySweeps(t)
	a, b := testNewTeardownEnv(t)
	return &testCreateLineageEnv{testTeardownEnv: a, clientID: clientID, idHash: getHash(clientID)}, b
}

// testCreateAndSave runs the production create and lands its first record, returning the
// session and the record that create wrote.
func testCreateAndSave(t *testing.T, env *testTeardownEnv, clientID string) (*mqttSession, *mqttPersistedSession) {
	t.Helper()
	sess, existed, err := env.asm.createOrRestoreSession(clientID, env.s.getOpts())
	if err != nil {
		t.Fatalf("the create must succeed: %v", err)
	}
	if existed {
		t.Fatal("a create over an empty subject reports Session Present false")
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("the create's first save: %v", err)
	}
	ps, _ := testLoadSessRecord(t, env, sess.idHash)
	if ps == nil {
		t.Fatal("the create's first save left no record")
	}
	return sess, ps
}

// testStageCreateOver writes what sess's next save WOULD write, CASing at atSeq — the
// shape a cross-term create leaves when its proposal applies after another's. The lineage
// is stamped exactly as the store boundary stamps it, so the staged record is one the
// writer really produces.
func testStageCreateOver(t *testing.T, env *testTeardownEnv, sess *mqttSession, atSeq uint64) *mqttPersistedSession {
	t.Helper()
	sess.mu.Lock()
	ps := sess.buildPersistedSessionLocked()
	ps.WID, ps.PWID = mqttNewRecordWID(), sess.parentForNextWriteLocked()
	sess.mu.Unlock()
	if _, err := mqttStoreSessionRecordCAS(&env.asm.jsa, env.asm.domainTk, &ps, sess.idHash, atSeq); err != nil {
		t.Fatalf("staging the cross-term create: %v", err)
	}
	return &ps
}

// testStageSiblingOver writes a SIBLING of the pair a session is acknowledged for — the
// same parent, an identity of its own — over the subject at atSeq. Unlike
// testStageCreateOver it takes the parent explicitly, because the session it stages
// against already holds a record and its next write would be that record's CHILD.
func testStageSiblingOver(t *testing.T, env *testTeardownEnv, sess *mqttSession,
	ackPWID string, atSeq uint64) *mqttPersistedSession {

	t.Helper()
	sess.mu.Lock()
	ps := sess.buildPersistedSessionLocked()
	ps.Gen = sess.gen
	ps.WID, ps.PWID = mqttNewRecordWID(), ackPWID
	sess.mu.Unlock()
	if _, err := mqttStoreSessionRecordCAS(&env.asm.jsa, env.asm.domainTk, &ps, sess.idHash, atSeq); err != nil {
		t.Fatalf("staging the sibling: %v", err)
	}
	return &ps
}

// ---------------------------------------------------------------------------
// T3 / T4: the create reads the watermark, and every status is dispositioned
// ---------------------------------------------------------------------------

// TestCreateInheritsTheRetiredRecordIdentity is T3 (M1, M3).
//
// The subject is empty because a record was deleted from it, and the watermark that
// deletion wrote first names that record. The create descends from it, which is what makes
// a sibling of the create recognisable instead of conceding a takeover nothing showed.
func TestCreateInheritsTheRetiredRecordIdentity(t *testing.T) {
	e, _ := testNewCreateLineageEnv(t, "create-inherits")

	// A watermark standing over an EMPTY subject, which is the state a completed teardown
	// leaves. Its RWID is deliberately not the nonce: the two are separate fields, and
	// inheriting the nonce would miss the mixed pair T5 asserts.
	retired := mqttNewRecordWID()
	testPlantRetirement(t, e.testTeardownEnv, e.idHash, &mqttGenRetirement{
		ID: e.clientID, Gen: mqttNewSessGen(), MarkerSeq: 7, RWID: retired,
		Nonce: "a-nonce-that-is-not-the-record",
	}, 0)

	sess, ps := testCreateAndSave(t, e.testTeardownEnv, e.clientID)
	if ps.PWID != retired {
		t.Fatalf("the create must descend from the record the watermark names: PWID=%q want %q",
			ps.PWID, retired)
	}
	if ps.WID == _EMPTY_ || ps.WID == retired {
		t.Fatalf("the create must mint an identity of its own: WID=%q PWID=%q", ps.WID, ps.PWID)
	}
	sess.mu.Lock()
	seeded := sess.retiredBaseWID
	sess.mu.Unlock()
	if seeded != retired {
		t.Fatalf("the create's next-write parent = %q, want the retired record %q", seeded, retired)
	}
}

// TestCreateWatermarkDispositions is T4 (M7, M20): every status has a disposition, and the
// two that carry no evidence make no positive claim from its absence.
//
// The hook returns a NON-EMPTY RWID alongside every status, so the parentless assertions
// cannot pass merely because the expected value is also the zero value.
func TestCreateWatermarkDispositions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  mqttRetirementStatus
		refused bool
	}{
		{"absent creates parentless", mqttRetirementAbsent, false},
		{"indeterminate creates parentless", mqttRetirementIndeterminate, false},
		{"corrupt refuses the CONNECT", mqttRetirementCorrupt, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := testNewCreateLineageEnv(t, "create-disp-"+strings.ReplaceAll(tc.name, " ", "-"))
			offered := mqttNewRecordWID()
			gate := newTestHookGate(t)
			restore := setMQTTTestRetirementLoadHook(func(idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
				if !gate.enter() {
					return nil, 0, mqttRetirementAbsent
				}
				defer gate.leave()
				if idHash != e.idHash {
					return nil, 0, mqttRetirementAbsent
				}
				return &mqttGenRetirement{
					ID: e.clientID, Gen: mqttNewSessGen(), MarkerSeq: 3, RWID: offered,
				}, 3, tc.status
			})
			defer restore()

			sess, existed, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
			if tc.refused {
				if err == nil {
					t.Fatal("a watermark this broker cannot read must REFUSE the CONNECT: its own " +
						"later teardown could never record a retirement, so the record would stay marked")
				}
				for _, want := range []string{e.clientID, "$MQTT_", "recreate"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the refusal must name %q, got: %v", want, err)
					}
				}
				if ps, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash); ps != nil {
					t.Errorf("a refused create must write no record, got %+v", ps)
				}
				return
			}
			if err != nil {
				t.Fatalf("a transient or absent watermark must not fail a CONNECT: %v", err)
			}
			if existed {
				t.Fatal("Session Present must be false for a create")
			}
			if err := testSessSave(t, sess); err != nil {
				t.Fatalf("the create's first save: %v", err)
			}
			ps, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
			if ps == nil {
				t.Fatal("the create's first save left no record")
			}
			if ps.PWID != _EMPTY_ {
				t.Fatalf("no retirement is RECORDED, so nothing may be inherited: PWID=%q "+
					"(the hook offered %q)", ps.PWID, offered)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T5 / T5b: the mixed pair — the discriminating test
// ---------------------------------------------------------------------------

// TestCreateMixedPairIsRecognisedAsSiblings is T5 (M1, M3, M10).
//
// One broker tears the record down itself and creates through the token clear() kept; the
// other finds the subject already empty and creates through the watermark. The two sources
// must agree, or the pair concedes from both sides — which is what inheriting the
// watermark's NONCE would produce.
func TestCreateMixedPairIsRecognisedAsSiblings(t *testing.T) {
	e, b := testNewCreateLineageEnv(t, "create-mixed-pair")

	sessA := testProvisionSession(t, e.testTeardownEnv, e.clientID)
	base, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if base == nil || base.WID == _EMPTY_ {
		t.Fatal("the base record must carry an identity")
	}
	if err := sessA.clear(false); err != nil {
		t.Fatalf("the clean-start discard must succeed: %v", err)
	}

	// B finds the subject empty and inherits from the watermark A's teardown wrote.
	sessB, recB := testCreateAndSave(t, b, e.clientID)
	if recB.PWID != base.WID {
		t.Fatalf("the watermark side must descend from the retired record: PWID=%q want %q",
			recB.PWID, base.WID)
	}

	// A's own create lands second — the cross-term apply — and carries the same parent
	// through the token clear() kept.
	recA := testStageCreateOver(t, e.testTeardownEnv, sessA, testCurrentRecordSeqIn(t, e.testTeardownEnv, e.idHash))
	if recA.PWID != base.WID {
		t.Fatalf("the teardown side must descend from the same record: PWID=%q want %q",
			recA.PWID, base.WID)
	}
	if recA.WID == recB.WID {
		t.Fatal("the two creates must carry distinct identities, or they are one record")
	}

	// B's record is gone under MaxMsgsPer:1, and B can now PROVE the survivor is no
	// successor of it.
	if got := classifyRecordLineage(recA, recB.WID, recB.PWID); got != mqttLineageForked {
		t.Fatalf("the survivor of a cross-term create pair is a proven SIBLING: got %v", got)
	}
	sessB.mu.Lock()
	bWID := sessB.wid
	sessB.mu.Unlock()
	if bWID != recB.WID {
		t.Fatalf("fixture: B must be acknowledged for its own create: %q vs %q", bWID, recB.WID)
	}
}

// TestCreateAdoptedTeardownPairIsRecognisedAsSiblings is T5b (M2).
//
// The same pair with the local side arriving through an ADOPTED teardown: the marker's own
// PWID is the record that teardown claimed, and the watermark the finisher writes must name
// that record too. Stamping the MARKER's identity instead gives the two sides different
// tokens, and both concede.
func TestCreateAdoptedTeardownPairIsRecognisedAsSiblings(t *testing.T) {
	e, b := testNewCreateLineageEnv(t, "create-adopted-pair")

	marker, _ := testStageClaimedMarker(t, e.testTeardownEnv, e.clientID)
	if marker.PWID == _EMPTY_ || marker.WID == marker.PWID {
		t.Fatalf("the staged marker must name the record it claimed: %+v", marker)
	}

	// A adopts and finishes the teardown, then creates. Its own token is the marker's
	// parent.
	sessA, existed, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("adopting the teardown: %v", err)
	}
	if existed {
		t.Fatal("an adopted teardown starts the client fresh")
	}

	// And the watermark that finisher wrote names the same record, so B — which sees only
	// an empty subject — inherits it too.
	r, _, status := testLoadRetirement(t, e.testTeardownEnv, e.idHash)
	if status != mqttRetirementValid {
		t.Fatalf("the finisher must have recorded a retirement, got status %v", status)
	}
	if r.RWID != marker.PWID {
		t.Fatalf("the watermark must name the record the teardown CLAIMED, not the marker: "+
			"RWID=%q want %q (marker identity %q)", r.RWID, marker.PWID, marker.WID)
	}

	_, recB := testCreateAndSave(t, b, e.clientID)
	recA := testStageCreateOver(t, e.testTeardownEnv, sessA, testCurrentRecordSeqIn(t, e.testTeardownEnv, e.idHash))
	if recA.PWID != marker.PWID || recB.PWID != marker.PWID {
		t.Fatalf("both creates must descend from the claimed record: A=%q B=%q want %q",
			recA.PWID, recB.PWID, marker.PWID)
	}
	if got := classifyRecordLineage(recA, recB.WID, recB.PWID); got != mqttLineageForked {
		t.Fatalf("the adopted-teardown pair must be recognised as siblings: got %v", got)
	}
}

// testCurrentRecordSeqIn is the sequence a client's record currently sits at.
func testCurrentRecordSeqIn(t *testing.T, env *testTeardownEnv, idHash string) uint64 {
	t.Helper()
	_, seq := testLoadSessRecord(t, env, idHash)
	if seq == 0 {
		t.Fatal("fixture: the subject must hold a record")
	}
	return seq
}

// ---------------------------------------------------------------------------
// T6: a RESTORE never takes its parent from the watermark
// ---------------------------------------------------------------------------

// TestRestoreNeverInheritsFromTheWatermark is T6 (M6).
//
// The watermark answers one question — what the DELETED record was — and a restore has a
// record of its own. Two halves, because the obvious oracle is impossible and the obvious
// mutation is a no-op: a record-found restore already loads the watermark for the
// retirement fence, so "the hook was never called" would fail correct code; and seeding the
// field on the record-found path is invisible while wid is non-empty, because
// parentForNextWriteLocked prefers it.
func TestRestoreNeverInheritsFromTheWatermark(t *testing.T) {
	t.Run("a restored record keeps its own pair", func(t *testing.T) {
		e, _ := testNewCreateLineageEnv(t, "restore-own-pair")
		sess := testProvisionSession(t, e.testTeardownEnv, e.clientID)
		if err := testSessSave(t, sess); err != nil {
			t.Fatalf("give the record a parent: %v", err)
		}
		rec, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
		if rec == nil || rec.WID == _EMPTY_ || rec.PWID == _EMPTY_ {
			t.Fatalf("fixture: the record must carry a complete pair, got %+v", rec)
		}
		// A watermark whose RWID is NOTHING this record names, standing over a record that
		// exists. Its generation is a fresh one, so the CONNECT retirement fence — which
		// reads the same watermark — leaves this record alone.
		foreign := mqttNewRecordWID()
		testPlantRetirement(t, e.testTeardownEnv, e.idHash, &mqttGenRetirement{
			ID: e.clientID, Gen: mqttNewSessGen(), MarkerSeq: 5, RWID: foreign,
		}, 0)
		e.asm.removeSession(sess, true)

		restored, existed, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
		if err != nil {
			t.Fatalf("the restore must succeed: %v", err)
		}
		if !existed {
			t.Fatal("a record that exists is RESUMED, or this asserts nothing about a restore")
		}
		restored.mu.Lock()
		wid, pwid, seeded := restored.wid, restored.pwid, restored.retiredBaseWID
		restored.mu.Unlock()
		if wid != rec.WID || pwid != rec.PWID {
			t.Fatalf("the restore adopts the RECORD's pair: got (%q,%q) want (%q,%q)",
				wid, pwid, rec.WID, rec.PWID)
		}
		if seeded == foreign {
			t.Fatalf("a restore must not take a parent from the watermark: seeded %q", seeded)
		}
	})

	t.Run("a WID-less record restores parentless", func(t *testing.T) {
		e, _ := testNewCreateLineageEnv(t, "restore-widless")
		sess := testProvisionSession(t, e.testTeardownEnv, e.clientID)
		rec, seq := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
		if rec == nil {
			t.Fatal("fixture: the provisioned session must have a record")
		}
		// The DISCRIMINATING half: a record with no identity of its own, so a mutation that
		// seeded the field on the record-found path would show through
		// parentForNextWriteLocked's fallback. Such a record predates the lineage work; it
		// is not resumed under a parent it never had.
		rec.WID, rec.PWID = _EMPTY_, _EMPTY_
		if _, err := mqttStoreSessionRecordCAS(&e.asm.jsa, e.asm.domainTk, rec, e.idHash, seq); err != nil {
			t.Fatalf("staging the WID-less record: %v", err)
		}
		offered := mqttNewRecordWID()
		testPlantRetirement(t, e.testTeardownEnv, e.idHash, &mqttGenRetirement{
			ID: e.clientID, Gen: mqttNewSessGen(), MarkerSeq: 5, RWID: offered,
		}, 0)
		e.asm.removeSession(sess, true)

		restored, existed, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
		if err != nil {
			t.Fatalf("the restore must succeed: %v", err)
		}
		if !existed {
			t.Fatal("the record exists, so this must be a resume")
		}
		if err := testSessSave(t, restored); err != nil {
			t.Fatalf("the restored session's next save: %v", err)
		}
		next, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
		if next == nil {
			t.Fatal("the save left no record")
		}
		if next.PWID != _EMPTY_ {
			t.Fatalf("a restore holds a record, so it inherits from no watermark: PWID=%q "+
				"(the watermark named %q)", next.PWID, offered)
		}
	})
}

// ---------------------------------------------------------------------------
// T10: recognised, and repaired by nothing
// ---------------------------------------------------------------------------

// TestCreateForkIsRecognisedAndRepairedByNothing is T10 (M11).
//
// Both halves in one test. Asserting only "no repair" would pass against code that
// disabled recognition entirely; asserting only recognition would not pin that a create
// cannot be repaired. A create has a parent POINTER and no parent CONTENT, so the
// three-way merge has no base — and merging against an empty base is the union Slice C
// rejected, which resurrects filters the survivor UNSUBACKed.
//
// The acknowledged grant is deliberately NON-EMPTY: a create's first save usually has an
// empty delta anyway, so a broken implementation that read a missing parent as an EMPTY
// grant would compute an empty delta and mint nothing either.
func TestCreateForkIsRecognisedAndRepairedByNothing(t *testing.T) {
	e, _ := testNewCreateLineageEnv(t, "create-fork-unrepairable")

	sess := testProvisionSession(t, e.testTeardownEnv, e.clientID)
	base, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if err := sess.clear(false); err != nil {
		t.Fatalf("the clean-start discard: %v", err)
	}
	// A grant the repair would have something to re-assert, if a create were repairable.
	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	sess.subs["create/fork/granted"] = 1
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("the create's first save: %v", err)
	}
	sess.mu.Lock()
	ackWID, ackPWID := sess.wid, sess.pwid
	body, parent := sess.grantBody, sess.grantBodyParent
	sess.mu.Unlock()
	if ackPWID != base.WID {
		t.Fatalf("fixture: the create must descend from the retired record: %q want %q", ackPWID, base.WID)
	}
	if len(body) == 0 {
		t.Fatal("fixture: the acknowledged grant must be retained, or 'nothing to repair' is trivial")
	}
	if parent != nil {
		t.Fatal("fixture: a create has no parent CONTENT, which is the whole point")
	}

	// A sibling of the create erases it.
	sibling := testStageSiblingOver(t, e.testTeardownEnv, sess, ackPWID,
		testCurrentRecordSeqIn(t, e.testTeardownEnv, e.idHash))
	if got := classifyRecordLineage(sibling, ackWID, ackPWID); got != mqttLineageForked {
		t.Fatalf("a sibling of a CREATE must still be recognised as a fork: got %v", got)
	}

	// And it repairs nothing: no capsule is minted, so nothing can re-assert the grant.
	sess.mintForkedGrant(ackWID, ackPWID)
	e.asm.mu.Lock()
	capsule := e.asm.forkedGrants[e.clientID]
	e.asm.mu.Unlock()
	if capsule != nil {
		t.Fatalf("a create-parented fork is UNREPAIRABLE: merging against an empty base is the "+
			"union Slice C refuted, got a capsule %+v", capsule)
	}
}

// ---------------------------------------------------------------------------
// R4: an out-of-band deletion, both documented directions
// ---------------------------------------------------------------------------

// TestCreateOutOfBandDeletionKeepsBothResidualDirections pins the residual `$MQTT_sess`
// having DenyDelete == false leaves behind, in the shape a review round had to correct
// twice: the watermark names the last RETIRED record, which after any acknowledged save in
// the current interval is neither the record an operator deletes nor that record's parent.
//
// It asserts today's behavior, not a fix — the point is that a future change cannot read
// the watermark as authority over a record the broker never deleted without this failing.
func TestCreateOutOfBandDeletionKeepsBothResidualDirections(t *testing.T) {
	e, b := testNewCreateLineageEnv(t, "create-oob-delete")

	sess := testProvisionSession(t, e.testTeardownEnv, e.clientID)
	base, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if err := sess.clear(false); err != nil {
		t.Fatalf("the clean-start discard: %v", err)
	}
	retired := base.WID

	// The interval: a create that inherited `retired`, then an ordinary save on top of it.
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("the create's first save: %v", err)
	}
	created, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if created == nil || created.PWID != retired {
		t.Fatalf("fixture: the create must inherit the retired record: %+v want parent %q", created, retired)
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("the interval's second save: %v", err)
	}
	saved, savedSeq := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if saved == nil || saved.PWID != created.WID {
		t.Fatalf("fixture: the second save must parent to the create: %+v", saved)
	}

	// An operator deletes the LIVE record. The watermark is untouched and still names the
	// retired record, which is now two writes behind.
	if err := e.asm.jsa.deleteMsg(mqttSessStreamName, savedSeq, true); err != nil {
		t.Fatalf("the out-of-band deletion: %v", err)
	}
	if r, _, st := testLoadRetirement(t, e.testTeardownEnv, e.idHash); st != mqttRetirementValid || r.RWID != retired {
		t.Fatalf("an out-of-band deletion must leave the watermark alone: status %v", st)
	}

	_, next := testCreateAndSave(t, b, e.clientID)
	if next.PWID != retired {
		t.Fatalf("the create after the deletion inherits the last RETIRED record, not the "+
			"deleted one: PWID=%q want %q", next.PWID, retired)
	}

	// Direction one, the dangerous one: against a broker still acknowledged for the CREATE,
	// the new record is a false fork — same parent, different identity, different intervals.
	if got := classifyRecordLineage(next, created.WID, created.PWID); got != mqttLineageForked {
		t.Fatalf("R4's false-fork direction: got %v, want forked", got)
	}
	// Direction two: against a broker acknowledged for the SAVE, nothing matches, so it
	// concedes 0x8E and runs the takeover-path Will cancellation — exactly the behavior
	// F-029b removes everywhere a retirement history describes the interval.
	if got := classifyRecordLineage(next, saved.WID, saved.PWID); got == mqttLineageForked {
		t.Fatal("R4's concede direction must stay a CONCESSION: the watermark cannot speak " +
			"for a record this broker never deleted, and reading it as authority here would " +
			"be a positive claim built on a deletion nothing recorded")
	}
}
