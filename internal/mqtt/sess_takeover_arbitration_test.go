package mqtt

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// F-029 (Slice B): arbitrating two frontends taking over one session from the same
// record base. See sess_takeover_redrive.go and docs/findings/ha-arbitration.md#f-029.
//
// The record-level half lives here. The binding transitions T1/T2 need a live CONNECT
// and are in sess_takeover_redrive_connect_test.go.

// testFreshSession builds the object a broker holds after finding NO record: sequence
// 0, a fresh incarnation, no lineage. It is what both sides of a create x create race
// start from.
func testFreshSession(env *testTeardownEnv, clientID string) *mqttSession {
	sess := mqttSessionCreate(&env.asm.jsa, clientID, getHash(clientID),
		mqttNewSessGen(), 0, _EMPTY_, _EMPTY_, env.s.getOpts())
	sess.asm = env.asm
	sess.domainTk = env.asm.domainTk
	return sess
}

// testSaveDetail saves inside a record transaction and returns the store-boundary
// detail, which is what carries the CAS rejection's TYPE — save() %v-formats it away.
func testSaveDetail(t testing.TB, sess *mqttSession) (mqttSessSaveDetail, error) {
	t.Helper()
	var d mqttSessSaveDetail
	err := testSessRecordTx(t, sess, func(tok recordTx) error {
		var serr error
		d, serr = sess.saveWithDetail(tok)
		return serr
	})
	return d, err
}

// testQuietExpirySweeps stops the account manager's PERIODIC expiry sweep from acting
// during a test, and must be called BEFORE the environment is created: the sweep's
// ticker reads this interval once, when the manager starts.
//
// Every fixture below stages an OVERDUE record or an abandoned teardown marker, which is
// precisely what that sweep exists to act on — it claims a teardown of its own and moves
// the subject. Without this the fixture races the sweep it baited, and the failure looks
// like a CAS rejection in the staging rather than what it is.
func testQuietExpirySweeps(t *testing.T) {
	t.Helper()
	restore := setMQTTSessionExpirySweepInterval(time.Hour)
	t.Cleanup(restore)
}

// ---------------------------------------------------------------------------
// Test 1 / M1: no session-record write is unconditional
// ---------------------------------------------------------------------------

// TestSessBaseZeroCreateIsConditional is B1 at the framing boundary and at the store.
//
// A first-ever create used to carry no CAS header, so it landed on top of whatever was
// there: two brokers that both found no record both created one, the later apply won
// under MaxMsgsPer:1, and the earlier broker's acknowledged state was erased with no
// rejection anywhere. The header at sequence 0 means "the subject must currently be
// EMPTY", which turns that erasure into a rejection.
//
// The positive control comes FIRST and is not decoration: without it, "the second
// create was rejected" would also be satisfied by a create that can never succeed.
func TestSessBaseZeroCreateIsConditional(t *testing.T) {
	t.Parallel()
	a, b := testNewTeardownEnv(t)
	const cid = "b1cond"

	// The framing itself. A body framed at 0 must carry a header; that is the single
	// fact M1 reverts.
	if _, hdr := mqttFrameRecordWrite(0, []byte(`{"id":"x"}`)); hdr == 0 {
		t.Fatal("a base-0 write must be framed WITH a CAS header: an unconditional create arbitrates nothing")
	}

	// Positive control: against an empty subject the same create succeeds.
	sa := testFreshSession(a, cid)
	if _, err := testSaveDetail(t, sa); err != nil {
		t.Fatalf("a base-0 create against an EMPTY subject must succeed: %v", err)
	}
	ps, seq := testLoadSessRecord(t, a, sa.idHash)
	if ps == nil {
		t.Fatal("the first create left no record")
	}

	// The same base, against a subject that is no longer empty.
	sb := testFreshSession(b, cid)
	d, err := testSaveDetail(t, sb)
	if err == nil {
		t.Fatal("a base-0 create against an OCCUPIED subject must be rejected: " +
			"this is the erasure F-029 closes")
	}
	if !mqttIsRecordCASRejection(d.rawErr) {
		t.Fatalf("the rejection must be a definite CAS rejection, got %v", d.rawErr)
	}
	// Nothing stored: the winner's record is untouched, at its own sequence.
	got, gotSeq := testLoadSessRecord(t, a, sa.idHash)
	if got == nil || gotSeq != seq {
		t.Fatalf("the rejected create must have stored nothing, record now %+v at %v (was %v)", got, gotSeq, seq)
	}
	if got.WID != ps.WID {
		t.Fatalf("the winner's record identity changed: %q -> %q", ps.WID, got.WID)
	}
}

// ---------------------------------------------------------------------------
// Test 2 / M1: the superseded-claim stomp, with the re-drive out of the picture
// ---------------------------------------------------------------------------

// TestSessSupersededClaimCannotStompTheWinner is the finding as observable behavior,
// staged so that B1 is the ONLY thing under test: clear() is called directly rather
// than through a CONNECT, so no re-drive runs and the accept save the old code would
// have performed is issued by hand.
//
// Before B1 that save landed unconditionally at base 0 and erased the winner's record
// inside a single leader term. It must now be rejected, and the winner's record must
// be byte-identical afterwards.
func TestSessSupersededClaimCannotStompTheWinner(t *testing.T) {
	t.Parallel()
	a, b := testNewTeardownEnv(t)
	const cid = "b2stomp"

	// A holds the session and knows its sequence.
	sessA := testProvisionSession(t, a, cid)

	// B takes it over: a real record write off the same base, so A's view is stale.
	sessB := testProvisionSession(t, b, cid)
	winner := testRecordBodyAt(t, b, sessB.idHash)
	_, winnerSeq := testLoadSessRecord(t, b, sessB.idHash)

	// A's clean-start discard now claims a base that is not current.
	if err := sessA.clear(false); !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("A's claim must be reported superseded, got %v", err)
	}
	// clear() reset A to sequence 0 without detaching it — which is exactly the state
	// that used to produce an unconditional create.
	sessA.mu.Lock()
	base := sessA.seq
	sessA.mu.Unlock()
	if base != 0 {
		t.Fatalf("clear() must have re-minted A at sequence 0, got %v", base)
	}

	// The accept save the old code went on to perform.
	d, err := testSaveDetail(t, sessA)
	if err == nil {
		t.Fatal("the superseded broker's create must be REJECTED: landing it erases the winner's record")
	}
	if !mqttIsRecordCASRejection(d.rawErr) {
		t.Fatalf("the create must be rejected by the CAS, got %v", d.rawErr)
	}
	if got := testRecordBodyAt(t, b, sessB.idHash); string(got) != string(winner) {
		t.Fatalf("the winner's record was rewritten:\n got %s\nwant %s", got, winner)
	}
	if _, seq := testLoadSessRecord(t, b, sessB.idHash); seq != winnerSeq {
		t.Fatalf("the winner's record moved to %v, want %v", seq, winnerSeq)
	}
}

// ---------------------------------------------------------------------------
// Test 10 / M5, M5b, M6, M16, M17: the lineage chain crosses the teardown re-mint
// ---------------------------------------------------------------------------

// TestSessCleanStartCreateIsParentlessAndConcedes is B3's shape after the write-lineage
// audit, through the PRODUCTION path rather than a synthetic record: a real clear()
// followed by the session's own first save, classified against a resume child of the
// same base.
//
// The verdict is the one this change GIVES UP, and it is asserted in both directions
// deliberately. A resume child of base a is {b, a}. A clean-start survivor written after
// tearing a down was briefly {c, a} and is now {c, ""} again, so the two have nothing in
// common, and a broker whose record that survivor erased CONCEDES — since F-084 with
// 0x89 ownership-unsettled, which is truthful; it used to answer 0x8E "session taken over",
// a claim nothing arbitrated. That is the residual stated in docs/CONFORMANCE.md,
// and it is the cost of removing an inherited identity that outlived the interval it
// named: every route by which the chain reached this verdict was reachable equally by a
// FALSE fork. See parentForNextWriteLocked.
func TestSessCleanStartCreateIsParentlessAndConcedes(t *testing.T) {
	t.Parallel()
	a, b := testNewTeardownEnv(t)
	const cid = "b3fork"

	sessA := testProvisionSession(t, a, cid)
	base, _ := testLoadSessRecord(t, a, sessA.idHash)
	if base == nil || base.WID == _EMPTY_ {
		t.Fatal("the base record must carry an identity")
	}

	// The clean-start survivor, through the production path: clear() claims and
	// releases the base, then the reused object's first save creates a fresh record.
	if err := sessA.clear(false); err != nil {
		t.Fatalf("the clean-start discard must succeed: %v", err)
	}
	if err := testSessSave(t, sessA); err != nil {
		t.Fatalf("the re-minted session's first save: %v", err)
	}
	survivor, survivorSeq := testLoadSessRecord(t, a, sessA.idHash)
	if survivor == nil || survivor.PWID != _EMPTY_ {
		t.Fatalf("the clean-start create descends from nothing: %+v", survivor)
	}
	if survivor.WID == _EMPTY_ || survivor.WID == base.WID {
		t.Fatalf("the clean-start create must mint an identity of its own: %+v", survivor)
	}

	// Direction 1, the concession. The OTHER sibling is a broker that RESUMED the same
	// base: its acknowledged pair is {b, a}, a child of a, and it is asked about the
	// survivor {c, ""}. Sibling-hood is defined against the acknowledged pair, and with
	// the survivor carrying no parent there is nothing to match — the answer is the
	// UNSETTLED vocabulary (0x8E before F-084).
	//
	// The resume child's identity is synthetic because the two records cannot both be
	// current — one erased the other. What has to come from the production path is the
	// SURVIVOR, and it does.
	resumeChildWID := mqttNewRecordWID()
	if got := classifyRecordLineage(survivor, resumeChildWID, base.WID); got != mqttLineageNotForked {
		t.Fatalf("a parentless clean-start survivor establishes no sibling-hood: got %v for %+v",
			got, survivor)
	}

	// Direction 2, the same question from the survivor's own side: a broker acknowledged
	// for the survivor, asked about the resume child, equally concedes. Both orientations
	// are pinned so a future change cannot restore one and leave the other, which would
	// make two brokers disagree about the same event.
	resumeChild := testLineageRecord(survivor.Gen, resumeChildWID, base.WID)
	if got := classifyRecordLineage(resumeChild, survivor.WID, survivor.PWID); got != mqttLineageNotForked {
		t.Fatalf("the reverse orientation must concede identically: got %v", got)
	}

	// Negative control: a genuine SEQUENTIAL handoff still concedes, and for its own
	// reason rather than through the parentlessness above.
	if got := classifyRecordLineage(survivor, base.WID, base.PWID); got != mqttLineageNotForked {
		t.Fatalf("a survivor of the acknowledged record is no sibling: got %v", got)
	}

	// The authored-record fork is NOT given up, and this is the discrimination: a
	// child-shaped record written through the production path by the other broker is not a
	// fork, while a body CLAIMING that same parent is (TestSessAuthoredForkStillClassifies).
	// Neither verdict says who holds the session — this is lineage only (F-084).
	sessB := testFreshSession(b, cid)
	sessB.mu.Lock()
	sessB.seq, sessB.wid, sessB.pwid = survivorSeq, survivor.WID, survivor.PWID
	sessB.mu.Unlock()
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("the successor save: %v", err)
	}
	successor, _ := testLoadSessRecord(t, b, sessB.idHash)
	if classifyRecordLineage(successor, survivor.WID, survivor.PWID) != mqttLineageNotForked {
		t.Fatalf("a child of the acknowledged record is child-shaped, not a fork: %+v", successor)
	}
}

// TestSessParentForNextWriteIsTheHeldRecordOnly is M9's paired accessor half: the
// parent-selection seam in isolation from the save path.
//
// wid is the record we are ACKNOWLEDGED for, at six reading sites, and it is now the
// ONLY thing a write descends from. The accessor used to fall back to the identity of
// the record whose removal emptied the subject; that fallback and its three sources are
// gone, so a session holding no record writes a parentless create.
func TestSessParentForNextWriteIsTheHeldRecordOnly(t *testing.T) {
	t.Parallel()
	a, _ := testNewTeardownEnv(t)
	sess := testFreshSession(a, "b3accessor")

	sess.mu.Lock()
	defer sess.mu.Unlock()

	sess.wid = _EMPTY_
	if got := sess.parentForNextWriteLocked(); got != _EMPTY_ {
		t.Fatalf("holding no record, the next write is parentless: got %q", got)
	}
	sess.wid = "held"
	if got := sess.parentForNextWriteLocked(); got != "held" {
		t.Fatalf("holding a record, the next write descends from it: got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Test 3(h) / M27, test 3(i) / M28, M29: the adopted-teardown superseded release
// ---------------------------------------------------------------------------

// TestSessAdoptedTeardownSupersededIsSurfaced is R1's one production violation.
//
// finishAdoptedTeardown used to convert a superseded release into success, after which
// createOrRestoreSession handed back a fresh base-0 object WITHOUT re-reading the
// record that superseded it — a write on a base it had authoritatively been told was
// stale. The outcome must reach the caller as a typed one (M27).
func TestSessAdoptedTeardownSupersededIsSurfaced(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const cid = "b2adopt"

	// A claims a teardown and stops before releasing it: the marker is durable.
	sessA := testProvisionSession(t, a, cid)
	var markerPS *mqttPersistedSession
	var markerSeq uint64
	restore := setMQTTTestTeardownClaimedHook(func(s *mqttSession, nonce string, seq uint64) {
		if s != sessA || markerPS != nil {
			return
		}
		markerPS, markerSeq = testLoadSessRecord(t, a, sessA.idHash)
		// B's record replaces the marker on the same subject, so A's exact-sequence
		// release removes nothing and its re-read finds a record that is not its own.
		sessB := testFreshSession(b, cid)
		sessB.mu.Lock()
		sessB.seq = markerSeq
		sessB.mu.Unlock()
		if err := testSessSave(t, sessB); err != nil {
			t.Errorf("B's replacing record: %v", err)
		}
	})
	defer restore()
	_ = sessA.clear(false)
	restore()

	if markerPS == nil || markerPS.Teardown == nil {
		t.Fatal("the hook did not observe a claimed teardown marker")
	}

	// An adopter now finds B's record where the marker was.
	err := b.asm.finishAdoptedTeardown(markerPS, sessA.idHash, markerSeq, b.s.getOpts())
	if !errors.Is(err, errMQTTTeardownAdoptionSuperseded) {
		t.Fatalf("a superseded release must be SURFACED, not swallowed: got %v", err)
	}
}

// testSupersedeInAdoptionWindow arms the adoption window so that the FIRST adoption to
// reach it has its record replaced by a FOREIGN one, which is what makes the release
// that follows classify superseded. It reports whether the window was reached.
//
// The write goes through the ADOPTER's own JetStream connection rather than a second
// broker's, and deliberately so: what makes the record foreign is its identity, not who
// wrote it, and routing it through a peer whose lifetime this fixture does not control
// made the hook fail against a torn-down server.
func testSupersedeInAdoptionWindow(t *testing.T, peer *testTeardownEnv, forClientID string) (rival func() string, restore func()) {
	t.Helper()
	var (
		mu       sync.Mutex
		rivalWID string
		done     bool
	)
	// Scoped to ONE client ID and disarmed at cleanup. The hook is global and a sweep
	// goroutine outliving another test can reach it, which would otherwise land a rival
	// record for a client this fixture knows nothing about — and, worse, fail THIS test
	// from a goroutine running after its server is gone.
	t.Cleanup(func() { mu.Lock(); done = true; mu.Unlock() })
	restore = setMQTTTestTeardownAdoptionWindowHook(func(clientID, idHash string, markerSeq uint64) {
		// The WHOLE plant runs under the lock, not just the gate, and that is what makes
		// this fixture single-shot. Two adoptions of the same marker run concurrently by
		// design — the test's own (a CONNECT, an expiry or a sweep) and a background sweep
		// still holding a previous environment's marker — and `rivalWID` is only set once a
		// CAS has landed, so gating on it while planting OUTSIDE the lock let both plant:
		// the second record superseded the first, `rivalWID` named whichever finished last,
		// and the restore was then compared against a rival that had not superseded the
		// adoption it was compared for.
		//
		// Holding it across the plant is also what keeps a slow firing from starving the
		// test's own: the second firing waits for the first to finish and then either sees
		// a rival already planted or plants one itself. A gate that reserved the slot
		// before the CAS instead let a firing whose JetStream round trip was still in
		// flight consume it, and the test asserted "no rival" while that plant was pending.
		mu.Lock()
		defer mu.Unlock()
		if done || rivalWID != _EMPTY_ || clientID != forClientID {
			return
		}
		// Written through the raw record CAS rather than through a session save: this is
		// simulating ANOTHER BROKER's write, and routing it through a local session
		// object would drag that object's own fence and sequence bookkeeping into a
		// fixture that only has to put foreign bytes on the subject.
		//
		// It CASes at whatever the subject holds right now, retrying once, because the
		// account's own sweep loop can be adopting the same marker concurrently: what
		// this has to produce is "a rival's record is there when the release
		// classifies", not one particular route to it.
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			// Read WITHOUT the t.Fatalf helper: this runs on whatever goroutine reached
			// the adoption — a sweep loop, not the test's — so a failure here must be
			// reported, never used to abort a goroutine that does not own the test.
			var cur uint64
			if smsg, lerr := peer.asm.jsa.loadSessionMsg(peer.asm.domainTk, idHash); lerr == nil && smsg != nil {
				cur = smsg.Sequence
			} else if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				lastErr = lerr
				continue
			}
			ps := &mqttPersistedSession{
				Origin: "rival-frontend", ID: clientID, Gen: mqttNewSessGen(),
				WID: mqttNewRecordWID(), PWID: mqttNewRecordWID(),
			}
			if _, err := mqttStoreSessionRecordCAS(&peer.asm.jsa, peer.asm.domainTk, ps, idHash, cur); err == nil {
				rivalWID = ps.WID
				return
			} else {
				lastErr = err
			}
		}
		// Deliberately NOT a test failure. The hook is global and a background sweep can
		// reach it for this client ID from an environment whose server is already gone —
		// a dead broker cannot stage anything, and the test's own call will. `rivalWID` is
		// set only where the rival actually landed, so a fixture that never staged still
		// fails, just not from the wrong goroutine.
		_ = lastErr
		_ = markerSeq
	})
	// The rival's IDENTITY, not just "it happened": every assertion about where the
	// restore came from compares against THIS record, never against a later re-read. A
	// re-read races anything else that writes to the subject afterwards, and what R1
	// promises is about the record that superseded the marker.
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return rivalWID
	}, restore
}

// testWarnRecorder captures WARN-level log records. That level is the ONLY thing the
// two terminal adoption branches change — both already returned without acting — so it
// is what discriminates the branch from the error arm it would otherwise fall into.
type testWarnRecorder struct {
	slog.Handler
	mu    sync.Mutex
	warns []string
}

func (h *testWarnRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *testWarnRecorder) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		h.mu.Lock()
		h.warns = append(h.warns, r.Message)
		h.mu.Unlock()
	}
	return nil
}

func (h *testWarnRecorder) sawWarning(t *testing.T, substr string) bool {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, w := range h.warns {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}

// testCaptureWarnings installs a WARN recorder on env's server for the rest of the test.
func testCaptureWarnings(t *testing.T, env *testTeardownEnv) *testWarnRecorder {
	t.Helper()
	h := &testWarnRecorder{Handler: slog.NewTextHandler(io.Discard, nil)}
	prev := env.s.log.Load()
	env.s.log.Store(slog.New(h))
	t.Cleanup(func() { env.s.log.Store(prev) })
	return h
}

// testStageClaimedMarker leaves a durable teardown marker on env's record for clientID
// — the state a broker that crashed between its claim and its release leaves behind —
// and returns the marker record and its sequence.
//
// Written directly through the same CAS claimTeardown uses, rather than by interrupting
// a real clear(): what the adoption paths take as input is the RECORD, and staging it
// this way keeps the fixture from depending on where a teardown happens to abort.
func testStageClaimedMarker(t *testing.T, env *testTeardownEnv, clientID string) (*mqttPersistedSession, uint64) {
	t.Helper()
	sess := testProvisionSession(t, env, clientID)
	ps, seq := testLoadSessRecord(t, env, sess.idHash)
	if ps == nil {
		t.Fatal("the provisioned session must have a record to mark")
	}
	ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
	ps.Teardown = &mqttSessTeardown{
		Origin:    "test-abandoned-owner",
		Nonce:     nuid.Next(),
		StartedAt: time.Now().Unix(),
	}
	// Overdue, so the record-driven expiry path admits it too. The teardown branch runs
	// ahead of any deletion, so this only decides whether that path looks at the record
	// at all.
	ps.ExpiryInterval, ps.DisconnectedAt = 1, time.Now().Unix()-3600
	markerSeq, err := mqttStoreSessionRecordCAS(&env.asm.jsa, env.asm.domainTk, ps, sess.idHash, seq)
	if err != nil {
		t.Fatalf("staging the teardown marker: %v", err)
	}
	return ps, markerSeq
}

// testQuarantineHas reports whether the sweep still holds an observation for key.
func testQuarantineHas(q *mqttSweepQuarantine, key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.entries[key]
	return ok
}

// TestSessSupersededAdoptionIsTerminalForTheSweep is test 3(i)'s sweep half (M29).
//
// The failure it pins is SILENT: falling into the sweep's error arm would skip
// q.forget, so every later pass would re-observe a marker that no longer exists and
// re-derive the same verdict forever. The discriminators are therefore the FORGOTTEN
// quarantine entry and the absence of the warning.
func TestSessSupersededAdoptionIsTerminalForTheSweep(t *testing.T) {
	testQuietExpirySweeps(t)
	a, _ := testNewTeardownEnv(t)
	const cid = "b2sweep"

	ps, markerSeq := testStageClaimedMarker(t, a, cid)
	idHash := getHash(cid)

	// The sweep adopts only what it has seen unchanged for a whole grace; zero makes
	// the first pass the qualifying one.
	zero := time.Duration(0)
	a.asm.mu.Lock()
	a.asm.testOrphanGrace = &zero
	a.asm.mu.Unlock()
	q := a.asm.quarantine()
	key := mqttQuarantineTeardownKey(idHash)
	fp := mqttQuarantineSeqFP(markerSeq) + "|" + ps.Teardown.Nonce
	// One sighting authorizes nothing, by design: eligibility needs a later,
	// independent check. Priming here makes the sweep's own call the qualifying one.
	q.observedOrphanFor(key, fp, 0)
	if !testQuarantineHas(q, key) {
		t.Fatal("the entry must be present before the adoption runs, or the assertion below proves nothing")
	}

	rival, restore := testSupersedeInAdoptionWindow(t, a, cid)
	defer restore()
	log := testCaptureWarnings(t, a)
	a.asm.adoptAbandonedTeardowns(a.s)
	restore()

	if rival() == _EMPTY_ {
		t.Fatal("the sweep never reached the adoption window: the staging did not exercise the branch")
	}
	if testQuarantineHas(q, key) {
		t.Fatal("a superseded adoption must FORGET the quarantine entry: left standing it " +
			"re-observes a marker that is gone, on every pass, forever")
	}
	if log.sawWarning(t, "unable to finish the abandoned teardown") {
		t.Fatal("a superseded adoption is ordinary arbitration, not a failure of the sweep")
	}
}

// TestSessSupersededAdoptionIsTerminalForRecordExpiry is test 3(i)'s expiry half (M28).
//
// expireSessionFromRecord returns either way, so the branch's ONLY observable effect is
// that it does not report a failure. A newer record means this expiry has nothing to
// expire, which is arbitration working, not an error worth an operator's time.
func TestSessSupersededAdoptionIsTerminalForRecordExpiry(t *testing.T) {
	testQuietExpirySweeps(t)
	a, _ := testNewTeardownEnv(t)
	const cid = "b2expiry"

	testStageClaimedMarker(t, a, cid)

	rival, restore := testSupersedeInAdoptionWindow(t, a, cid)
	defer restore()
	log := testCaptureWarnings(t, a)
	a.asm.expireSessionFromRecord(a.s, getHash(cid))
	restore()

	if rival() == _EMPTY_ {
		t.Fatal("the expiry never reached the adoption window: the staging did not exercise the branch")
	}
	if log.sawWarning(t, "finishing the claimed teardown") {
		t.Fatal("a superseded adoption must not be reported as a failed teardown: " +
			"it means the record is not ours, which is the outcome, not a fault")
	}
}

// ---------------------------------------------------------------------------
// Test 9 / M9, M21: the two bounds are independent, and both hold
// ---------------------------------------------------------------------------

// TestReDriveBudgetsAreIndependent is test 9(b) on the budget itself.
//
// A shared counter is indistinguishable from two separate ones when both caps are the
// same number, which is why they are deliberately unequal. The sequence here spends
// cycle budget first and then proves the create-retry budget is still FULL — the one
// shape that discriminates.
func TestReDriveBudgetsAreIndependent(t *testing.T) {
	t.Parallel()
	rd := mqttNewRecordReDrive()

	// Spend all but one cycle. The initial attempt is not a retry, so what is bounded
	// is repetition.
	for i := 0; i < mqttRecordReDriveCycles-1; i++ {
		if err := rd.admitCycle(); err != nil {
			t.Fatalf("cycle %d must be admitted: %v", i, err)
		}
	}
	// The create-retry budget is untouched by that.
	for i := 0; i < mqttRecordCreateRetries; i++ {
		if err := rd.admitCreateRetry(); err != nil {
			t.Fatalf("create retry %d must be admitted after %d cycles: %v (a shared counter)",
				i, mqttRecordReDriveCycles-1, err)
		}
	}
	if err := rd.admitCreateRetry(); err != errMQTTReDriveCreateRetriesExhausted {
		t.Fatalf("create retries must stop at %d, got %v", mqttRecordCreateRetries, err)
	}
	// And the last cycle is still there, because the retries did not consume it.
	if err := rd.admitCycle(); err != nil {
		t.Fatalf("the remaining cycle must survive the create retries: %v", err)
	}
	if err := rd.admitCycle(); err != errMQTTReDriveCyclesExhausted {
		t.Fatalf("cycles must stop at %d, got %v", mqttRecordReDriveCycles, err)
	}
}

// TestReDriveDeadlineAdmitsNoNewWork is test 9(c): the deadline is an ADMISSION bound,
// driven through an injected clock rather than by sleeping.
//
// It bounds exactly what it claims and nothing more — no new top-level cycle and no new
// accept-save retry — which is why both admissions are asserted and neither is asserted
// to interrupt work already in flight.
func TestReDriveDeadlineAdmitsNoNewWork(t *testing.T) {
	now := time.Now()
	var mu sync.Mutex
	restore := setMQTTTestReDriveClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	})
	defer restore()

	rd := mqttNewRecordReDrive()
	if err := rd.admitCycle(); err != nil {
		t.Fatalf("a cycle inside the deadline must be admitted: %v", err)
	}
	if err := rd.admitCreateRetry(); err != nil {
		t.Fatalf("a create retry inside the deadline must be admitted: %v", err)
	}

	mu.Lock()
	now = now.Add(mqttRecordReDriveDeadline)
	mu.Unlock()

	if err := rd.admitCycle(); err != errMQTTReDriveDeadlineReached {
		t.Fatalf("no new cycle may be admitted after the deadline, got %v", err)
	}
	if err := rd.admitCreateRetry(); err != errMQTTReDriveDeadlineReached {
		t.Fatalf("no new accept-save retry may be admitted after the deadline, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test 5 / M2, M3: every TERMINAL clear() caller treats superseded as done
// ---------------------------------------------------------------------------

// TestSupersededDiscardIsTerminalForTheExpiryCallers covers the two expiry callers of
// clear() (mqtt.go's expireSession and expireSessionFromRecord).
//
// The discriminator is the ABSENCE of warning-level logging, because that is the only
// thing the new branch changes: both callers already returned without acting. "The
// record was untouched" and "nothing was re-parked" are kept as positive controls and
// labelled as such — the recovery paths key on errMQTTClearRefusedUnresolved alone, so
// both would hold even without the branch.
func TestSupersededDiscardIsTerminalForTheExpiryCallers(t *testing.T) {
	testQuietExpirySweeps(t)
	a, b := testNewTeardownEnv(t)

	for _, tc := range []struct {
		name string
		cid  string
		warn string
		run  func(t *testing.T, sess *mqttSession)
	}{
		{
			name: "expireSessionFromRecord",
			cid:  "term-fromrec",
			warn: "session expiry cleanup",
			run: func(t *testing.T, sess *mqttSession) {
				a.asm.expireSessionFromRecord(a.s, sess.idHash)
			},
		},
		{
			name: "expireSession",
			cid:  "term-expire",
			warn: "session expiry cleanup",
			run: func(t *testing.T, sess *mqttSession) {
				a.asm.expireSession(a.s, sess.idHash)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := testProvisionSession(t, a, tc.cid)
			// The record is overdue, so the expiry paths admit it at all.
			sess.mu.Lock()
			sess.expiryInterval, sess.disconnectedAt = 1, time.Now().Unix()-3600
			sess.mu.Unlock()
			if err := testSessSave(t, sess); err != nil {
				t.Fatalf("stamping the overdue record: %v", err)
			}

			// A rival takes the session over, so the expiry's teardown claim is
			// superseded against a base that is no longer current. Staged BEFORE the
			// session is made resident: an overdue session arms an IMMEDIATE expiry
			// timer, and that timer's own run would move the subject under this CAS.
			rival := testFreshSession(b, tc.cid)
			// Re-read and retry: even with the periodic sweep quiet, the CAS must be
			// against the sequence the subject holds AT THE MOMENT OF THE WRITE, never
			// one sampled earlier. A staging step that assumes its own read is still
			// current is the same mistake this whole item is about.
			var saved bool
			for attempt := 0; attempt < 3 && !saved; attempt++ {
				_, seq := testLoadSessRecord(t, a, sess.idHash)
				rival.mu.Lock()
				rival.seq = seq
				rival.mu.Unlock()
				if err := testSessSave(t, rival); err == nil {
					saved = true
				} else if attempt == 2 {
					t.Fatalf("the rival's takeover: %v", err)
				}
			}
			winner, winnerSeq := testLoadSessRecord(t, a, sess.idHash)

			// Resident only now. The timer this arms may run the same expiry
			// concurrently, which is harmless for what is asserted: both runs take the
			// superseded branch, and the discriminator is the absence of a WARNING —
			// a level, not a count.
			a.asm.addSession(sess, true)

			log := testCaptureWarnings(t, a)
			tc.run(t, sess)

			if log.sawWarning(t, tc.warn) {
				t.Fatalf("losing an arbitration is a COMPLETE outcome for %s, not a failed teardown: "+
					"it was logged as one", tc.name)
			}
			// Positive controls, not discriminators.
			got, gotSeq := testLoadSessRecord(t, a, sess.idHash)
			if got == nil || gotSeq != winnerSeq || got.WID != winner.WID {
				t.Fatalf("a superseded teardown must delete nothing: record %+v at %v, want %v at %v",
					got, gotSeq, winner, winnerSeq)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test 12 / M4, M12: the rejection marker represents base 0, and clear() disarms it
// ---------------------------------------------------------------------------

// TestRejectionMarkerRepresentsBaseZero is B4's third consequence.
//
// The marker's validity used to require a non-zero sequence, because zero was a safe
// stand-in for "no marker": a base-0 create could not be rejected at all. It can now, so
// the armed flag carries that fact — and clear(), the one route that re-mints at
// sequence 0, is obliged to disarm it. Left armed there, the marker matches the fresh
// session's own sequence and gates its serving for the life of the session.
//
// The positive assertion comes first, so the negative one after clear() cannot pass
// against a marker that was never armed.
func TestRejectionMarkerRepresentsBaseZero(t *testing.T) {
	t.Parallel()
	a, b := testNewTeardownEnv(t)
	const cid = "b4marker"

	// A rival occupies the subject, so this broker's first-ever create is REJECTED at
	// base 0 — the shape that did not exist before F-029.
	rival := testFreshSession(b, cid)
	if err := testSessSave(t, rival); err != nil {
		t.Fatalf("the rival's create: %v", err)
	}
	sess := testFreshSession(a, cid)
	sess.asm = a.asm
	if _, err := testSaveDetail(t, sess); err == nil {
		t.Fatal("a base-0 create against an occupied subject must be rejected")
	}

	sess.mu.Lock()
	armed, gates := sess.recordRejectedArmed, sess.recordRejectionGatesServingLocked()
	base := sess.recordRejectedSeq
	sess.mu.Unlock()
	if !armed || base != 0 {
		t.Fatalf("a base-0 rejection must arm the marker at base 0: armed=%v base=%v", armed, base)
	}
	if !gates {
		t.Fatal("an outstanding rejection gates serving: the subject moved and this broker's " +
			"belief that it owns the session may be obsolete")
	}

	// clear() re-mints at sequence 0. Without an explicit disarm the marker matches the
	// fresh session's own sequence forever.
	if err := sess.clear(false); err != nil {
		t.Fatalf("discarding a session that holds no record must succeed: %v", err)
	}
	sess.mu.Lock()
	armed, gates = sess.recordRejectedArmed, sess.recordRejectionGatesServingLocked()
	sess.mu.Unlock()
	if armed || gates {
		t.Fatalf("clear() must DISARM the rejection marker: armed=%v gating=%v — "+
			"a re-minted session would otherwise never serve a packet", armed, gates)
	}
}

// ---------------------------------------------------------------------------
// Test 16 / M20a, M20b: guards against the two designs this item REJECTED
// ---------------------------------------------------------------------------

// TestWonClaimDeletesEvenWhenItsReleaseIsSuperseded guards the rejected B5 reordering
// (M20a), and states the window the roadmap tracks.
//
// Releasing before deleting was designed to close the claim-won deletion window and was
// refuted: releaseTeardown proves absence rather than authorship, the unsafe interval
// merely moves, shared group durables are not generation-scoped, and after the release
// the durable target list is gone while PUBREL durables have no orphan reaper.
//
// The discriminating shape is a teardown whose claim was WON and whose release was then
// SUPERSEDED. Under the shipped order the deletions have already happened, so the
// retiring generation's consumers are gone — which is exactly the open window. Under the
// rejected order they would still be there. Asserting on the ordering at claim time
// cannot tell the two apart, because both orders have deleted nothing yet.
func TestWonClaimDeletesEvenWhenItsReleaseIsSuperseded(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, "b5order")
	_, _, pubRelDur := mqttPubRelNames(sess.idHash, sess.gen)
	if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
		t.Fatal("the fixture needs the retiring generation's PUBREL consumer to exist")
	}

	// A rival replaces the marker once the claim is won, so the exact-sequence release
	// removes nothing and its classifying re-read reports superseded.
	var staged bool
	restore := setMQTTTestTeardownClaimedHook(func(s *mqttSession, nonce string, markerSeq uint64) {
		if s != sess || staged {
			return
		}
		staged = true
		ps := &mqttPersistedSession{
			Origin: "rival-frontend", ID: sess.id, Gen: mqttNewSessGen(),
			WID: mqttNewRecordWID(), PWID: mqttNewRecordWID(),
		}
		if _, err := mqttStoreSessionRecordCAS(&a.asm.jsa, a.asm.domainTk, ps, sess.idHash, markerSeq); err != nil {
			t.Errorf("landing the rival over the marker: %v", err)
		}
	})
	defer restore()
	err := sess.clear(false)
	restore()

	if !staged {
		t.Fatal("the claim was never won, so the release could not be superseded")
	}
	if !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("the release must report superseded, got %v", err)
	}
	if testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
		t.Fatal("a WON claim deletes its targets before it learns whether it still owns the " +
			"claim: finding the consumer intact means the release was moved ahead of the " +
			"deletions (the rejected B5 reordering). That ordering loses the target list to " +
			"a crash, and PUBREL durables have no orphan reaper")
	}
}

// TestParentlessForeignRecordStillConcedes guards the rejected B6 arm (M20b).
//
// A bound-gated arm calling a parentless foreign record a fork was designed and refuted:
// its premise — that nothing legitimately deletes a bound session's record — is false,
// and a FALSE fork suppresses delayed-Will cancellation, so the failure was never
// vocabulary-only. Cross-term create x create recognition stays a ROADMAP §0.2 blocker.
//
// Aimed at that ADDITIVE arm specifically. The broader mutation — dropping the
// ackPWID != "" conjunct outright — is already killed by TestSessEmptyParentsAreNotSiblings,
// so a guard aimed at it would be redundant.
func TestParentlessForeignRecordStillConcedes(t *testing.T) {
	t.Parallel()
	// Acknowledged for a real record with a real parent; the survivor carries neither.
	parentless := testLineageRecord("gen-other", mqttNewRecordWID(), _EMPTY_)
	if got := classifyRecordLineage(parentless, "ack-wid", "ack-pwid"); got != mqttLineageNotForked {
		t.Fatalf("a parentless foreign record establishes NOTHING about ancestry: got %v. "+
			"The bound-gated arm that called it a fork was refuted — a legitimate clean-start "+
			"release plus a crash produces exactly this record while an owner is still bound, "+
			"and a false fork leaves an obsolete delayed Will eligible to fire", got)
	}
}

// TestSessAdoptedTeardownCreateIsParentless is test 10's adopted-teardown variant after
// the write-lineage audit, and it is a DISCRIMINATING assertion rather than a default
// one: a non-empty identity is available at this exact site — the marker's own PWID,
// which names the record the adopted teardown claimed — and the create must not carry
// it. This was the fourth create-parent source, and the last one whose input a test can
// hold in its hand.
//
// The claimed record's identity is not discarded, only re-scoped: the release still hands
// it to recordGenRetirement, no longer as a stored field but as the PRECONDITION that the
// record being retired has an identity at all. Either way it is a fact about the teardown
// rather than about the next write's ancestry.
func TestSessAdoptedTeardownCreateIsParentless(t *testing.T) {
	testQuietExpirySweeps(t)
	a, _ := testNewTeardownEnv(t)
	const cid = "b3adopt"

	marker, _ := testStageClaimedMarker(t, a, cid)
	if marker.PWID == _EMPTY_ {
		t.Fatal("the staged marker must name the record its teardown claimed")
	}

	fresh, existed, err := a.asm.createOrRestoreSession(cid, a.s.getOpts())
	if err != nil {
		t.Fatalf("adopting the teardown: %v", err)
	}
	if existed {
		t.Fatal("an adopted teardown starts the client FRESH: Session Present must be false")
	}
	fresh.mu.Lock()
	held := fresh.parentForNextWriteLocked()
	fresh.mu.Unlock()
	if held != _EMPTY_ {
		t.Fatalf("an adopted-teardown create descends from nothing: got %q (the marker's "+
			"parent is %q, which must NOT be inherited)", held, marker.PWID)
	}

	// And that reaches the wire: the first save stamps no parent.
	if err := testSessSave(t, fresh); err != nil {
		t.Fatalf("the fresh session's first save: %v", err)
	}
	ps, _ := testLoadSessRecord(t, a, fresh.idHash)
	if ps == nil || ps.PWID != _EMPTY_ {
		t.Fatalf("the adopted create's stamped parent = %+v, want empty", ps)
	}
	if ps.WID == _EMPTY_ {
		t.Fatal("a parentless create still mints an identity of its OWN")
	}
}

// ---------------------------------------------------------------------------
// Test 3(h) / M27: an adopted marker whose own release is superseded re-enters R1
// ---------------------------------------------------------------------------

// TestSessSupersededAdoptionReEntersTheRestore is R1 at the one place the production
// code violated it.
//
// finishAdoptedTeardown converted a superseded release into success, and
// createOrRestoreSession then handed back a FRESH base-0 object built from the marker —
// without re-reading the record it had authoritatively been told exists. B1 would
// usually reject that object's create, but if the record vanished first the create could
// land carrying lineage derived from a marker that is nobody's base.
//
// The discriminator is what comes back: a session restored from the RIVAL's record, at
// the rival's sequence, rather than a base-0 object carrying the marker's parent.
func TestSessSupersededAdoptionReEntersTheRestore(t *testing.T) {
	testQuietExpirySweeps(t)
	a, _ := testNewTeardownEnv(t)
	const cid = "b2readopt"

	marker, _ := testStageClaimedMarker(t, a, cid)
	rival, restore := testSupersedeInAdoptionWindow(t, a, cid)
	defer restore()

	sess, existed, err := a.asm.createOrRestoreSession(cid, a.s.getOpts())
	restore()
	if err != nil {
		t.Fatalf("the CONNECT restore must converge, not fail: %v", err)
	}
	rivalWID := rival()
	if rivalWID == _EMPTY_ {
		t.Fatal("the rival never replaced the marker, so the superseded release was not exercised")
	}

	sess.mu.Lock()
	seq, wid := sess.seq, sess.wid
	sess.mu.Unlock()

	if seq == 0 {
		t.Fatalf("a superseded adoption must RE-READ before anything is built from it: "+
			"got a base-0 object (marker parent=%q)", marker.PWID)
	}
	// Compared against the rival's own IDENTITY rather than a re-read of the subject:
	// the subject can move again after the restore, and what R1 promises is that the
	// restore came from the record that superseded the marker.
	if wid != rivalWID {
		t.Fatalf("the restore must come from the record that superseded the marker: "+
			"acknowledged wid=%q at seq %v, want the rival's %q", wid, seq, rivalWID)
	}
	if !existed {
		t.Fatal("a record restored from the subject is an existing session, not a fresh one")
	}
}
