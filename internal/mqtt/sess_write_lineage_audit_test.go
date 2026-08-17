// Tests for the session write-lineage audit: what a parentless create concedes, what
// the retained obligations still hold, and the refusal of the register stream that is
// no longer defined.
//
// The pairing across this file is deliberate and is the discrimination the whole change
// rests on. "Takeover" is the DEFAULT verdict, so a test that only asserts a create
// concedes would pass equally against a classifier that had been deleted or bypassed
// (feedback_mutation_checks_must_discriminate). Every parentlessness assertion here is
// therefore paired with TestMQTTAuthoredForkStillClassifies, which proves the classifier
// is still reached and still reports the shape it was built for.

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// ---------------------------------------------------------------------------
// Test 1: a create is parentless on every production path, from both orientations
// ---------------------------------------------------------------------------

// TestMQTTParentlessCreateConcedesFromBothOrientations walks EVERY production route that
// reaches mqttSessionCreate and asserts that none of them hands the new object a parent —
// then drives the ordinary foreign disposition over the resulting record and asserts the
// takeover vocabulary from both sides of the pair.
//
// The three routes are the three that used to differ:
//
//   - the record-absent create, which read the generation-retirement watermark's RWID;
//   - the adopted-marker create, which read the marker's own PWID;
//   - the reused object after clear(), which was stamped with the record it claimed.
//
// Each is staged so that a non-empty identity IS available at the moment of the create.
// That is what makes the assertion a measurement rather than an absence: an
// implementation that re-introduced any one inheritance would have something to
// inherit. See parentForNextWriteLocked.
// NOT parallel, at any level: the adopted-marker subtest quiets the expiry sweep, which
// is process-global, and TestParallelTestsTouchNoProcessGlobalState reads a t.Parallel()
// anywhere in this function as putting the whole of it beside whatever else is running.
func TestMQTTParentlessCreateConcedesFromBothOrientations(t *testing.T) {
	t.Run("the record-absent create over a valid watermark", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const cid = "parentless-absent"

		sess := testProvisionSession(t, a, cid)
		base, _ := testLoadSessRecord(t, a, sess.idHash)
		if base == nil || base.WID == _EMPTY_ {
			t.Fatal("the base record must carry an identity")
		}
		if err := sess.clear(false); err != nil {
			t.Fatalf("the discard must succeed: %v", err)
		}
		// The watermark is the identity this route used to inherit, and it must be
		// present and valid for the assertion below to mean anything.
		r, _, status := testLoadRetirement(t, a, sess.idHash)
		if status != mqttRetirementValid || r.RWID != base.WID {
			t.Fatalf("the teardown must leave a valid watermark naming the removed record: "+
				"status=%v r=%+v want RWID %q", status, r, base.WID)
		}

		fresh, existed, err := a.asm.createOrRestoreSession(cid, a.s.getOpts())
		if err != nil {
			t.Fatalf("creating into the emptied subject: %v", err)
		}
		if existed {
			t.Fatal("an emptied subject is a fresh session: Session Present must be false")
		}
		testAssertParentlessCreate(t, a, fresh, r.RWID)
	})

	t.Run("the adopted-marker create", func(t *testing.T) {
		testQuietExpirySweeps(t)
		a, _ := testNewTeardownEnv(t)
		const cid = "parentless-adopted"

		marker, _ := testStageClaimedMarker(t, a, cid)
		if marker.PWID == _EMPTY_ {
			t.Fatal("the staged marker must name the record its teardown claimed")
		}

		fresh, existed, err := a.asm.createOrRestoreSession(cid, a.s.getOpts())
		if err != nil {
			t.Fatalf("adopting the teardown: %v", err)
		}
		if existed {
			t.Fatal("an adopted teardown starts the client FRESH")
		}
		testAssertParentlessCreate(t, a, fresh, marker.PWID)
	})

	t.Run("the reused object after clear()", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const cid = "parentless-remint"

		sess := testProvisionSession(t, a, cid)
		base, _ := testLoadSessRecord(t, a, sess.idHash)
		if base == nil || base.WID == _EMPTY_ {
			t.Fatal("the base record must carry an identity")
		}
		if err := sess.clear(false); err != nil {
			t.Fatalf("the discard must succeed: %v", err)
		}
		// The SAME object, reused as a fresh session, which is what clear() is for on
		// the clean-start and overdue paths.
		testAssertParentlessCreate(t, a, sess, base.WID)
	})
}

// testAssertParentlessCreate is test 1's shared body: the object holds no parent, its
// first save stamps none, and the resulting record settles to the TAKEOVER vocabulary
// from both orientations against a sibling of the identity it declined to inherit.
//
// available is the non-empty identity that WAS reachable at the create — the watermark's
// RWID, the marker's parent, or the record clear() claimed. Passing it in is what keeps
// each subtest a measurement: without it every arm would assert an emptiness that could
// equally mean nothing was ever there.
func testAssertParentlessCreate(t *testing.T, env *testTeardownEnv, sess *mqttSession, available string) {
	t.Helper()
	if available == _EMPTY_ {
		t.Fatal("fixture: no identity was available to inherit, so parentlessness proves nothing")
	}

	sess.mu.Lock()
	held := sess.parentForNextWriteLocked()
	sess.mu.Unlock()
	if held != _EMPTY_ {
		t.Fatalf("the create holds no parent: got %q (%q was available)", held, available)
	}

	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("the create's first save: %v", err)
	}
	created, _ := testLoadSessRecord(t, env, sess.idHash)
	if created == nil {
		t.Fatal("the create's save left no record")
	}
	if created.PWID != _EMPTY_ {
		t.Fatalf("the create stamps no parent: got %q (%q was available)", created.PWID, available)
	}
	if created.WID == _EMPTY_ || created.WID == available {
		t.Fatalf("a parentless create still mints an identity of its OWN: %q", created.WID)
	}

	// The classifier's own answer, as a MUTATION CONTROL rather than as the behavioural
	// claim: a sibling of the identity we declined to inherit establishes nothing. The
	// behavioural claim — that this reaches the takeover vocabulary through a real
	// settlement, in both orientations — is
	// TestMQTTParentlessCreateSettlesToTakeoverBothOrientations, because
	// mqttLineageNotForked is this enum's DEFAULT, so a consult path that bypassed or
	// mis-mapped the classifier would satisfy the assertion below unchanged.
	sibling := testLineageRecord(created.Gen, mqttNewRecordWID(), available)
	if got := classifyRecordLineage(sibling, created.WID, created.PWID); got != mqttLineageNotForked {
		t.Fatalf("a parentless acknowledgement establishes no sibling-hood: got %v", got)
	}
}

// TestMQTTParentlessCreateSettlesToTakeoverBothOrientations drives the concession through
// the ORDINARY foreign disposition — a real rejected CAS, settled by
// settleRecordRejection — and asserts the vocabulary the verdict is MAPPED to rather than
// a constant the test chose.
//
// The two orientations are not the same assertion twice: they concede by DIFFERENT
// conjuncts of arm 2, and each is killed by a different mutation.
//
//   - Both records PARENTLESS — the cross-term create pair itself, `{c, ""}` against
//     `{x, ""}`. The parents MATCH, so `found.PWID == ackPWID` does not save it; the
//     only guard is `ackPWID != ""`, and dropping that conjunct makes two unrelated
//     first creates accuse each other.
//   - MIXED — acknowledged `{b, p}` after two saves, against a parentless create
//     `{x, ""}`. Here `ackPWID != ""` holds and `found.PWID == ackPWID` is the guard;
//     treating an empty survivor parent as a wildcard makes it accuse.
//
// A change that restored one direction and left the other would have two brokers
// disagreeing about one event, and only this pairing catches it.
//
// NOT parallel: the envs install process-global store hooks.
func TestMQTTParentlessCreateSettlesToTakeoverBothOrientations(t *testing.T) {
	t.Run("both records parentless: the cross-term create pair", func(t *testing.T) {
		// One save only, so the acknowledged pair is a create's: {c, ""}.
		e := testNewRejectionEnv(t, "parentless-settle-a")
		e.dropPersistObservations(t, e.idHash)
		e.sess.mu.Lock()
		ackWID, ackPWID := e.sess.wid, e.sess.pwid
		e.sess.mu.Unlock()
		if ackWID == _EMPTY_ || ackPWID != _EMPTY_ {
			t.Fatalf("fixture: the acknowledged pair must be a create's, got (%q,%q)", ackWID, ackPWID)
		}
		// The survivor is ALSO a parentless create: testLandSibling copies our own
		// parent, which is empty. This is the cross-term create pair itself — both
		// admitted at base 0, both applied, the earlier discarded — and the ONLY thing
		// that stops the two empty parents from reading as shared ancestry is arm 2's
		// `ackPWID != ""` conjunct.
		sib := testLandSibling(t, e.testInflightEnv, testMutateSibling)
		if sib.PWID != _EMPTY_ {
			t.Fatalf("fixture: the survivor must be parentless too, got %q", sib.PWID)
		}
		testAssertSettlesToTakeover(t, e)
	})

	t.Run("mixed: acknowledged for a PARENTED record, a parentless create stands", func(t *testing.T) {
		// Two saves, so the acknowledged pair is complete: {b, p}.
		e := testNewForkEnv(t, "parentless-settle-b")
		e.sess.mu.Lock()
		ackWID, ackPWID := e.sess.wid, e.sess.pwid
		e.sess.mu.Unlock()
		if ackWID == _EMPTY_ || ackPWID == _EMPTY_ {
			t.Fatalf("fixture: the acknowledged pair must be complete, got (%q,%q)", ackWID, ackPWID)
		}
		// The survivor is a parentless create — the shape every create now writes.
		testLandSibling(t, e.testInflightEnv, func(ps *mqttPersistedSession) {
			testMutateSibling(ps)
			ps.PWID = _EMPTY_
		})
		testAssertSettlesToTakeover(t, e)
	})
}

// testAssertSettlesToTakeover rejects a real save, settles it through the production
// path, and asserts the verdict AND the vocabulary it maps to.
//
// The vocabulary comes from mqttDisposalForRejection applied to the verdict the
// settlement actually returned, never from a constant chosen here: that is what makes
// this fail if a consult path stops reaching the classifier, or maps its result to the
// wrong claim.
func testAssertSettlesToTakeover(t *testing.T, e *testRejectionEnv) {
	t.Helper()
	e.rejectSave(t, e.sess)

	got := e.sess.settleRecordRejection(e.asm)
	if got != mqttRejectionForeign {
		t.Fatalf("a pair with no shared parent establishes no sibling-hood: "+
			"settleRecordRejection = %v, want foreign", got)
	}
	if v := mqttDisposalForRejection(got); v.reason != mqttReasonSessionTakenOver {
		t.Fatalf("the conceding verdict must map to 0x8E 'session taken over': got %#v", v)
	}
	// Nothing is repairable from a verdict that never proved a sibling.
	if g := e.capsuleOrNil(); g != nil {
		t.Fatalf("a conceding verdict mints nothing: %+v", g)
	}
	if e.resident(e.sess) {
		t.Fatal("a disposed session must not stay resident")
	}
}

// ---------------------------------------------------------------------------
// Test 2: the retained obligation, and the discriminator for every test above
// ---------------------------------------------------------------------------

// TestMQTTAuthoredForkStillClassifies is the positive half of this change, and the
// reason none of the concessions above can be satisfied by a deleted classifier.
//
// The authored-record fork — two CAS writes off a base this broker actually HELD — is
// the shape classifyRecordLineage exists for, and its retirement is unproved on the
// writer every accepted CONNECT uses: that write is an AllowAtomicPublish batch, and
// F-030's dependency inventory covers the single-message path only. So the classifier
// and sess_fork_repair.go stay, and this pins that they still work.
func TestMQTTAuthoredForkStillClassifies(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "authored-fork")

	// Two saves, so the session is acknowledged for a record that has a real parent —
	// which is exactly what a create no longer has, and exactly what an authored fork
	// still does.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("the create's save: %v", err)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("the second save: %v", err)
	}
	e.sess.mu.Lock()
	ackWID, ackPWID := e.sess.wid, e.sess.pwid
	e.sess.mu.Unlock()
	if ackWID == _EMPTY_ || ackPWID == _EMPTY_ {
		t.Fatalf("the acknowledged pair must be complete: (%q,%q)", ackWID, ackPWID)
	}

	// The sibling: another writer's record off the SAME base, which is what a fork is.
	sibling := testLineageRecord("gen-other", mqttNewRecordWID(), ackPWID)
	if got := classifyRecordLineage(sibling, ackWID, ackPWID); got != mqttLineageForked {
		t.Fatalf("two writes off one HELD base are siblings: got %v", got)
	}
	if got := mqttDisposalForForeignResolution(mqttUncertainForked); got.reason != mqttReasonServerBusy {
		t.Fatalf("a proven sibling licenses 0x89, not a takeover claim: %#v", got)
	}

	// And a legitimate successor of the acknowledged record still concedes, so the arm
	// above is not simply reporting "foreign".
	child := testLineageRecord("gen-other", mqttNewRecordWID(), ackWID)
	if got := classifyRecordLineage(child, ackWID, ackPWID); got != mqttLineageNotForked {
		t.Fatalf("a child of the acknowledged record is a successor, not a fork: got %v", got)
	}
}

// ---------------------------------------------------------------------------
// Test 3: the retirement watermark still names the record the teardown removed
// ---------------------------------------------------------------------------

// TestMQTTRetirementNamesTheRecordThatActuallyStood is the RWID obligation, which is
// RETAINED: no create reads it any more, but it remains the fact the watermark
// attributes a retirement to, pinned until F-036 and the vanished-record vocabulary
// decide whether it keeps a reader.
//
// Two things make this discriminating where the existing assertion in
// sess_retirement_test.go is not.
//
// First, the comparison is against the WID of the record that actually STOOD on the
// subject when the teardown claimed it, read back independently, rather than against a
// field of the same object the claim wrote. Comparing the watermark to the marker passes
// whenever both are wrong the same way, and claimTeardown writes both — one bug produces
// one consistent pair.
//
// Second, the record it is compared against is produced by the AT-BASE RETRY inside
// resolveContendedRecordSave, not by an ordinary save. That path proves a 10164-rejected
// write durable outside saveWithDetail's success arm, and it must adopt the lineage along
// with the sequence: a site that moved the sequence while leaving sess.wid stale would
// have clear() capture the OLD identity as retiringWID and record a watermark naming a
// record the deletion did not remove. The retry is what puts a stale-lineage bug and a
// correct one on different values here.
//
// NOT parallel: it installs process-global store hooks.
func TestMQTTRetirementNamesTheRecordThatActuallyStood(t *testing.T) {
	e := testNewRejectionEnv(t, "retirement-names-stood")
	e.dropPersistObservations(t, e.idHash)

	before := testLoadAuditRecord(t, e.asm, e.idHash)
	if before == nil || before.WID == _EMPTY_ {
		t.Fatal("the seeded record must carry an identity")
	}

	// A save the store reports as 10164-contended while the record has NOT moved, so
	// resolveContendedRecordSave takes its at-base arm and the exact-byte retry lands.
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"rwid/retry": 1}
	e.sess.mu.Unlock()
	stop := testForceContendedRejection(t, e.sess)
	_, detail, serr := testSaveClassified(t, e.sess)
	stop()
	if serr == nil {
		t.Fatal("the forced save must be rejected")
	}
	if got := e.sess.resolveContendedRecordSave(&detail); got != mqttRepairSaveLanded {
		t.Fatalf("the identical retry must LAND: got %v", got)
	}

	// The record that now STANDS, read from the subject rather than from the session.
	stood := testLoadAuditRecord(t, e.asm, e.idHash)
	if stood == nil || stood.WID == _EMPTY_ {
		t.Fatal("the retry must leave a record carrying an identity")
	}
	if stood.WID == before.WID {
		t.Fatal("fixture: the retry's record must be a NEW write, or a stale lineage would " +
			"be indistinguishable from a correct one")
	}

	// The teardown, through the production path. It captures retiringWID off the session
	// before its reset, so this is where a lineage that lagged the sequence surfaces.
	if err := e.sess.clear(false); err != nil {
		t.Fatalf("the discard must succeed: %v", err)
	}

	r, _, status := e.asm.jsa.loadGenRetirement(e.asm.domainTk, e.idHash)
	if status != mqttRetirementValid {
		t.Fatalf("the teardown must record a retirement, got status %v", status)
	}
	if r.RWID != stood.WID {
		t.Fatalf("the watermark must name the record that STOOD when the teardown claimed it: "+
			"RWID=%q want %q (the record before the retry was %q)", r.RWID, stood.WID, before.WID)
	}
}

// testLoadAuditRecord returns the client's current session record from an account
// manager, for the tests here that hold no *testTeardownEnv.
func testLoadAuditRecord(t *testing.T, asm *mqttAccountSessionManager, idHash string) *mqttPersistedSession {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("loadSessionMsg: %v", err)
		}
		return nil
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		t.Fatalf("unmarshal session record: %v", uerr)
	}
	return ps
}

// ---------------------------------------------------------------------------
// Test 4: the watermark validator's RWID row
// ---------------------------------------------------------------------------

// TestMQTTRetirementRefusesAnEmptyRWID is the RWID != "" row on its own, with every
// other field valid, so nothing else in the validator can account for the refusal.
//
// The row is what makes a record carrying no identity fail its own teardown closed
// rather than being retired under a name nothing can attribute. It survives the
// write-lineage audit unchanged: it is a property of the watermark, not of the create
// that used to read it.
func TestMQTTRetirementRefusesAnEmptyRWID(t *testing.T) {
	t.Parallel()

	valid := &mqttGenRetirement{
		ID:        "rwid-validator",
		Gen:       mqttNewSessGen(),
		MarkerSeq: 7,
		RWID:      mqttNewRecordWID(),
		Nonce:     "nonce",
		Origin:    "originbroker",
		StartedAt: 1,
	}
	// The positive control: with RWID present the same watermark is accepted, so the
	// refusal below is attributable to that field alone.
	if err := mqttValidateGenRetirement(valid, getHash(valid.ID)); err != nil {
		t.Fatalf("fixture: the watermark must otherwise be valid: %v", err)
	}

	empty := *valid
	empty.RWID = _EMPTY_
	err := mqttValidateGenRetirement(&empty, getHash(valid.ID))
	if err == nil {
		t.Fatal("a watermark that cannot name the record its deletion removed must be REFUSED")
	}
	if !strings.Contains(err.Error(), "retired") {
		t.Fatalf("the refusal must name what is missing, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test 5: the create-side corrupt-watermark refusal survives the deletion beside it
// ---------------------------------------------------------------------------

// TestMQTTCreateRefusesAnUnreadableRetirementWatermark is the CREATE arm's refusal,
// which is a separate obligation from the inheritance that used to sit beside it and
// which the audit deliberately kept.
//
// Admitting a session over a watermark this broker cannot read leaves that session's OWN
// later teardown unable to record a retirement — recordGenRetirement refuses to replace a
// corrupt standing watermark — so the record would stay MARKED for every future finisher.
// The existing TestRetirementUnreadableWatermarkRefusesTheConnect covers the RESUME-side
// reader, which is a different branch reached with a record present; this one runs with
// the subject EMPTY.
//
// All four statuses are exercised, because the refusal must be exactly one of them: an
// implementation that refused on absent or indeterminate would fail every first-ever
// CONNECT, and one that refused on none would admit the unusable store.
func TestMQTTCreateRefusesAnUnreadableRetirementWatermark(t *testing.T) {
	a, _ := testNewTeardownEnv(t)

	cases := []struct {
		name   string
		status mqttRetirementStatus
		refuse bool
	}{
		{"corrupt", mqttRetirementCorrupt, true},
		{"absent", mqttRetirementAbsent, false},
		{"indeterminate", mqttRetirementIndeterminate, false},
		{"valid", mqttRetirementValid, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cid := "create-watermark-" + tc.name
			idHash := getHash(cid)
			restore := setMQTTTestRetirementLoadHook(func(hash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
				if hash != idHash {
					return a.asm.jsa.loadGenRetirementRecord(a.asm.domainTk, hash)
				}
				if tc.status == mqttRetirementValid {
					return &mqttGenRetirement{
						ID: cid, Gen: mqttNewSessGen(), MarkerSeq: 3,
						RWID: mqttNewRecordWID(), Nonce: "n", Origin: "o", StartedAt: 1,
					}, 4, mqttRetirementValid
				}
				return nil, 0, tc.status
			})
			sess, _, err := a.asm.createOrRestoreSession(cid, a.s.getOpts())
			restore()

			if tc.refuse {
				if err == nil {
					t.Fatal("a create over an UNREADABLE watermark must be refused: its own " +
						"teardown could never record a retirement")
				}
				if !strings.Contains(err.Error(), "unreadable") {
					t.Fatalf("the refusal must say why, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("status %v must ADMIT the create, got %v", tc.status, err)
			}
			// And whatever it read, it inherited nothing from it — the valid arm above
			// supplies a real RWID precisely so this is not vacuous.
			sess.mu.Lock()
			held := sess.parentForNextWriteLocked()
			sess.mu.Unlock()
			if held != _EMPTY_ {
				t.Fatalf("an admitted create holds no parent, whatever the watermark said: %q", held)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test 6: the register stream is refused, not tolerated
// ---------------------------------------------------------------------------

// TestMQTTStartupRefusesALeftoverOriginStream covers all three preflight outcomes.
//
// $MQTT_origin is a stream this design no longer defines, which the compatibility
// boundary makes an unsupported persisted shape: refused, never decoded or migrated. The
// check cannot be a provisioning-table entry — ensureBrokerStream CREATES an absent
// stream before validating it — so it is a lookup-only preflight, and separating "absent"
// from "could not be read" is the part that has to be pinned.
func TestMQTTStartupRefusesALeftoverOriginStream(t *testing.T) {
	t.Parallel()

	info := func() (*StreamInfo, error) {
		return &StreamInfo{Config: StreamConfig{Name: mqttOriginStreamName}}, nil
	}
	notFound := ApiErrors[JSStreamNotFoundErr]
	other := errors.New("no responders")

	for _, tc := range []struct {
		name   string
		reply  func() (*StreamInfo, error)
		refuse bool
		why    string
	}{
		{"absent: no info, not-found", func() (*StreamInfo, error) { return nil, notFound },
			false, "the only outcome that may proceed"},
		{"present: info, no error", info,
			true, "the account still holds the retired register"},
		{"present: info AND a not-found error", func() (*StreamInfo, error) {
			return &StreamInfo{Config: StreamConfig{Name: mqttOriginStreamName}}, notFound
		}, true, "a self-contradictory reply must be decided by the half that says the " +
			"forbidden stream EXISTS, not by the half that says it does not"},
		{"undecided: no info, no error", func() (*StreamInfo, error) { return nil, nil },
			true, "a reply that carried neither decides nothing, and absence is not the default"},
		{"undecided: no info, some other error", func() (*StreamInfo, error) { return nil, other },
			true, "an unreadable account is not an account without the stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stub := &stubStreamAPI{lookupResults: []func() (*StreamInfo, error){tc.reply}}
			err := mqttRefuseRetiredOriginStream(stub)
			if tc.refuse && err == nil {
				t.Fatalf("startup must be refused: %s", tc.why)
			}
			if !tc.refuse && err != nil {
				t.Fatalf("startup must proceed (%s), got %v", tc.why, err)
			}
			if err != nil && !strings.Contains(err.Error(), mqttOriginStreamName) {
				t.Fatalf("every refusal must name the stream, got %v", err)
			}
			// LOOKUP-ONLY on every arm: this check runs before the provisioning loop
			// precisely because ensureBrokerStream creates an absent stream before it
			// validates one, so a create here would provision what it exists to refuse.
			if stub.creates != 0 {
				t.Fatalf("the preflight must create nothing, got %d create(s)", stub.creates)
			}
			if stub.lookups != 1 {
				t.Fatalf("the preflight must look up exactly once, got %d", stub.lookups)
			}
		})
	}
}

// TestMQTTStartupRefusalIsWiredIntoManagerInit is the preflight's INTEGRATION half, and
// it exists because the table test above cannot reach the question it answers.
//
// Those subtests drive mqttRefuseRetiredOriginStream directly through a stub, so every
// one of them passes against a broker that never CALLS it: deleting the call site from
// getOrCreateMQTTAccountSessionManager leaves the whole package suite green, tagged
// compilation included. That is the "a test that passes against deliberately broken code
// proves nothing" trap, and only a real bring-up closes it.
//
// So: a live JetStream, $MQTT_origin created by hand as an operator's leftover would be,
// and the assertion that account-manager initialization FAILS. The negative control is
// the same bring-up without the stream, which must succeed — without it, a manager that
// failed for any unrelated reason would satisfy this test.
func TestMQTTStartupRefusalIsWiredIntoManagerInit(t *testing.T) {
	t.Parallel()

	t.Run("a leftover register aborts initialization", func(t *testing.T) {
		t.Parallel()
		s, c, nc := newBringUpServer(t)
		js, ctx := newJS(t, nc)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     mqttOriginStreamName,
			Subjects: []string{mqttPrefix + "origin.>"},
			Storage:  jetstream.FileStorage,
		}); err != nil {
			t.Fatalf("planting the retired register: %v", err)
		}

		_, err := s.getOrCreateMQTTAccountSessionManager(c)
		if err == nil {
			t.Fatal("an account still holding the retired register must not initialize: the " +
				"preflight is not reached from getOrCreateMQTTAccountSessionManager")
		}
		if !strings.Contains(err.Error(), mqttOriginStreamName) {
			t.Fatalf("the failure must name the stream to delete, got %v", err)
		}
		// And it refused BEFORE provisioning: the stream this broker would otherwise
		// have created first must not exist.
		if _, serr := js.Stream(ctx, mqttSessStreamName); serr == nil {
			t.Fatalf("the refusal must precede the provisioning loop, but %s was created",
				mqttSessStreamName)
		}
	})

	t.Run("negative control: the same bring-up without it succeeds", func(t *testing.T) {
		t.Parallel()
		s, c, _ := newBringUpServer(t)
		if _, err := s.getOrCreateMQTTAccountSessionManager(c); err != nil {
			t.Fatalf("fixture: a clean account must initialize, or the arm above proves "+
				"nothing about the register: %v", err)
		}
	})
}

// TestMQTTProvisionedStreamsExcludeTheRetiredRegister states the count as a fact of its
// own, beside TestBrokerStreamSpecTable's exact-membership check.
//
// The two are not redundant: the membership list is a test fixture that a change could
// edit in the same commit, whereas this asserts the retired stream is absent by NAME from
// whatever the table happens to contain.
func TestMQTTProvisionedStreamsExcludeTheRetiredRegister(t *testing.T) {
	t.Parallel()
	specs := testBrokerStreamSpecs()
	if len(specs) != 9 {
		t.Fatalf("the broker provisions nine streams, got %d", len(specs))
	}
	for _, spec := range specs {
		if spec.name == mqttOriginStreamName {
			t.Fatalf("%s is refused at startup and must never be provisioned", mqttOriginStreamName)
		}
		if strings.Contains(fmt.Sprint(spec.cfg.Subjects), "origin.") {
			t.Fatalf("entry %q still owns the retired register's subject space: %v",
				spec.name, spec.cfg.Subjects)
		}
	}
}
