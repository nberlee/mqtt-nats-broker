// Permanent per-CONNECT acceptance facts: the record, and the atomic batch that
// commits it with the session record and the ownership receipt.
//
// The claimant-side half — what a delayed Will does with these facts — lives in
// will_accept_fence_test.go.

package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// testAcceptSave runs the ACCEPTING save production runs — saveAcceptWithOwnership,
// which commits the acceptance fact, the ownership receipt and the session record in
// one atomic batch — under its own record transaction. The receipt answers a creation
// proposal, so the client's ownership is reserved first, as the accepting CONNECT
// reserves it (opClaimClient) before its metadata commit.
func testAcceptSave(t testing.TB, sess *mqttSession) error {
	t.Helper()
	_, err := testAcceptSaveDetail(t, sess)
	return err
}

// testAcceptSaveDetail is testAcceptSave returning the save's detail, whose rawErr is the
// store's own answer to the batch.
func testAcceptSaveDetail(t testing.TB, sess *mqttSession) (mqttSessSaveDetail, error) {
	t.Helper()
	epoch := sess.asm.sessStreamEpoch
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	al := sess.jsa.loadOutboundAuthority(sess.id, epoch)
	if al.status == mqttOutboundAbsent {
		txn := mqttNewInflightNonce()
		if _, err := sess.jsa.opClaimClient(sess.id, epoch, txn, gen, "tenure-"+txn, "intent-"+txn); err != nil {
			t.Fatalf("reserving the client's ownership: %v", err)
		}
		al = sess.jsa.loadOutboundAuthority(sess.id, epoch)
	}
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Creation == nil {
		t.Fatalf("no standing creation proposal to accept under: status=%v", al.status)
	}
	proposal, perr := mqttProposalFromCreation(al.rec.Creation)
	if perr != nil {
		t.Fatalf("deriving the creation proposal: %v", perr)
	}
	var detail mqttSessSaveDetail
	err := testSessRecordTx(t, sess, func(tok recordTx) error {
		var serr error
		detail, serr = sess.saveAcceptWithOwnership(sess.jsa, tok, sess.id, epoch, proposal)
		return serr
	})
	return detail, err
}

// testPlantAcceptFact writes one well-formed acceptance fact directly, bypassing
// the accept batch. Used where the test needs a fact at a chosen TIME or a chosen
// stream position rather than whatever a real CONNECT would produce.
func testPlantAcceptFact(t testing.TB, asm *mqttAccountSessionManager, clientID string, at int64) (string, uint64) {
	t.Helper()
	f := &mqttAcceptFact{
		ClientID:  clientID,
		SessEpoch: asm.sessStreamEpoch,
		AcceptID:  mqttNewAcceptID(),
		At:        at,
	}
	subj := mqttAcceptSubject(asm.domainTk, getHash(clientID), f.SessEpoch, f.AcceptID)
	body, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal acceptance fact: %v", err)
	}
	return subj, testPlantRawAcceptFact(t, asm, subj, body)
}

// testPlantRawAcceptFact stores arbitrary bytes on an acceptance-fact subject, for
// the fixtures that must be refused rather than interpreted.
func testPlantRawAcceptFact(t testing.TB, asm *mqttAccountSessionManager, subject string, body []byte) uint64 {
	t.Helper()
	framed, hdr := mqttFrameRecordWrite(0, body)
	resp, err := asm.jsa.storeMsg(subject, hdr, framed)
	if err != nil {
		t.Fatalf("store acceptance fact on %q: %v", subject, err)
	}
	return resp.Sequence
}

// testFactsFor returns every acceptance fact stored for a client in this broker's
// incarnation, oldest first, with the sequence each sits at.
func testFactsFor(t testing.TB, asm *mqttAccountSessionManager, clientID string) ([]*mqttAcceptFact, []uint64) {
	t.Helper()
	filter := mqttAcceptClientEpochFilter(asm.domainTk, getHash(clientID), asm.sessStreamEpoch)
	var facts []*mqttAcceptFact
	var seqs []uint64
	for seq := uint64(1); ; {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				t.Fatalf("walk acceptance facts: %v", err)
			}
			return facts, seqs
		}
		seq = smsg.Sequence + 1
		f, derr := mqttDecodeAcceptFact(asm.domainTk, asm.sessStreamEpoch, smsg)
		if derr != nil {
			t.Fatalf("decode acceptance fact on %q: %v", smsg.Subject, derr)
		}
		facts, seqs = append(facts, f), append(seqs, smsg.Sequence)
	}
}

// testCountFactLoads installs a counting pass-through on the fact walk's point read
// and returns the counter. Counting is the discriminating assertion wherever a
// persisted decision is supposed to be REUSED: a claimant that reused one and a
// claimant that rescanned and happened to find nothing both publish, so only "no
// fact was read at all" tells them apart.
func testCountFactLoads(t testing.TB, asm *mqttAccountSessionManager) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	asm.mu.Lock()
	asm.testFactLoad = func(filter string, seq uint64) (*StoredMsg, error) {
		n.Add(1)
		return asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testFactLoad = nil
		asm.mu.Unlock()
	})
	return &n
}

// testSessStreamSubjects is the exact subject set $MQTT_sess must carry: the
// session records, and the permanent CONNECT acceptance facts an accepted CONNECT
// commits atomically with its record.
func testSessStreamSubjects() []string {
	return []string{
		mqttSessStreamSubjectPrefix + ">",
		mqttAcceptSubjectPrefix + ">",
		mqttHandoffSubjectPrefix + ">",
		mqttTeardownReceiptSubjectPrefix + ">",
	}
}

// ---------------------------------------------------------------------------
// T2: the batch's ordering, and its ack
// ---------------------------------------------------------------------------

// TestAcceptFactSortsBelowItsOwnRecord is T2.
//
// Both halves matter and they are different claims. The ack must name the SESSION
// RECORD (the sequence the session adopts), which is only true because the record
// is the message carrying the commit; and the fact must sort strictly BELOW that
// record, which is what makes a connection unable to suppress its own delayed Will
// — the suppression predicate requires a fact sequence strictly above the Will's
// origin, and the origin IS this record's sequence.
func TestAcceptFactSortsBelowItsOwnRecord(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "accept-order"
	sess := testProvisionSession(t, a, cid)

	if err := testAcceptSave(t, sess); err != nil {
		t.Fatalf("the accepting save: %v", err)
	}

	sess.mu.Lock()
	adopted := sess.seq
	sess.mu.Unlock()

	smsg, err := a.asm.jsa.loadSessionMsg(a.asm.domainTk, sess.idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if adopted != smsg.Sequence {
		t.Fatalf("the session adopted %d but its record is at %d: the batch ack must name the RECORD, "+
			"which it only does because the record carries the commit", adopted, smsg.Sequence)
	}

	facts, seqs := testFactsFor(t, a.asm, cid)
	if len(facts) != 1 {
		t.Fatalf("one accepting save must leave exactly one acceptance fact, got %d", len(facts))
	}
	if seqs[0] >= smsg.Sequence {
		t.Fatalf("the fact is at %d and its own record at %d: staged FIRST, the fact must sort below, "+
			"or a connection could suppress its own Will", seqs[0], smsg.Sequence)
	}
	if facts[0].ClientID != cid {
		t.Fatalf("the fact must carry the EXACT client id, got %q", facts[0].ClientID)
	}

	// And the connection's own fact is non-qualifying against a Will that connection
	// enqueued: the Will's origin is this very record.
	rec := testWillFor(a.asm, cid, "own-fact-w", time.Now().Add(time.Hour).UnixNano(), smsg.Sequence)
	if mqttAcceptFactQualifies(facts[0], seqs[0], a.asm.sessStreamEpoch, rec) {
		t.Fatal("a connection's own acceptance must never qualify against the Will that same connection enqueued")
	}
}

// ---------------------------------------------------------------------------
// T5: atomicity, in both directions
// ---------------------------------------------------------------------------

// TestAcceptBatchIsAtomicInBothDirections is T5.
//
// Both directions, because one of them alone is satisfied by a broken
// implementation. "Both landed" is what a pair of independent writes also
// produces; "neither landed when the record CAS is refused" is what tells a batch
// from two writes issued in a row.
func TestAcceptBatchIsAtomicInBothDirections(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "accept-atomic"
	sess := testProvisionSession(t, a, cid)

	// Direction 1: move the record out from under the session so its CAS base is
	// stale, then accept. The record CAS is refused — and the fact, whose own
	// create-only CAS would have succeeded on its unique subject, must NOT be stored.
	// Run FIRST, because a refused batch stores nothing, so the one creation proposal's
	// create-only receipt slot is still free for the committing direction below.
	sess.mu.Lock()
	stale := sess.seq
	sess.seq = stale - 1
	sess.mu.Unlock()

	before, _ := testFactsFor(t, a.asm, cid)
	detail, err := testAcceptSaveDetail(t, sess)
	if err == nil {
		t.Fatal("an accepting save at a stale record sequence must be refused")
	}
	// The refusal must be the BATCH's: the store's record CAS, not a proposal check that
	// stopped before anything was sent, which would prove nothing about atomicity.
	if !mqttIsRecordCASRejection(detail.rawErr) {
		t.Fatalf("the refusal did not come from the batch's record CAS: %v (store answered %v)", err, detail.rawErr)
	}
	after, _ := testFactsFor(t, a.asm, cid)
	if len(after) != len(before) {
		t.Fatalf("a REFUSED batch stored an acceptance fact anyway: %d facts before, %d after — "+
			"the fact's own CAS succeeds on its unique subject, so only atomicity keeps it out",
			len(before), len(after))
	}

	// Direction 2: a batch that commits stores BOTH.
	sess.mu.Lock()
	sess.seq = stale
	sess.mu.Unlock()
	if err := testAcceptSave(t, sess); err != nil {
		t.Fatalf("the accepting save: %v", err)
	}
	if facts, _ := testFactsFor(t, a.asm, cid); len(facts) != len(before)+1 {
		t.Fatalf("a committed batch must store the fact too, got %d facts (had %d)", len(facts), len(before))
	}
}

// ---------------------------------------------------------------------------
// T9: nothing removes a fact
// ---------------------------------------------------------------------------

// TestSessionTeardownLeavesAcceptFactsIntact is T9.
//
// The teardown release deletes the session record BY EXACT SEQUENCE, and the facts
// now share that stream. An untargeted removal — a purge, or a subject-filtered
// delete — would take the client's whole acceptance history with the record, and
// every delayed Will that history was entitled to suppress would then fire.
func TestSessionTeardownLeavesAcceptFactsIntact(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "accept-teardown"
	sess := testProvisionSession(t, a, cid)
	if err := testAcceptSave(t, sess); err != nil {
		t.Fatalf("the accepting save: %v", err)
	}
	before, seqsBefore := testFactsFor(t, a.asm, cid)
	if len(before) != 1 {
		t.Fatalf("seed one acceptance fact, got %d", len(before))
	}

	if err := sess.clear(false); err != nil {
		t.Fatalf("the teardown release: %v", err)
	}
	if ps, _ := testLoadSessRecord(t, a, sess.idHash); ps != nil {
		t.Fatal("the teardown must have released the record")
	}

	after, seqsAfter := testFactsFor(t, a.asm, cid)
	if len(after) != 1 || seqsAfter[0] != seqsBefore[0] {
		t.Fatalf("the teardown removed the client's acceptance history: %d facts at %v, want 1 at %v",
			len(after), seqsAfter, seqsBefore)
	}
}

// ---------------------------------------------------------------------------
// T10: the stream's shape is a prerequisite, not a preference
// ---------------------------------------------------------------------------

// TestValidateSessStreamRequiresBatchesAndBothSubjects is T10.
func TestValidateSessStreamRequiresBatchesAndBothSubjects(t *testing.T) {
	t.Parallel()
	const domainTk = _EMPTY_
	base := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:               mqttSessStreamName,
			Subjects:           testSessStreamSubjects(),
			Storage:            FileStorage,
			Retention:          LimitsPolicy,
			Discard:            DiscardOld,
			MaxMsgsPer:         1,
			AllowAtomicPublish: true,
			Metadata:           map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
		}}
	}
	if err := validateSessStream(base(), domainTk, 0); err != nil {
		t.Fatalf("the baseline configuration must validate: %v", err)
	}

	for _, tc := range []struct {
		name    string
		poison  func(*StreamConfig)
		errWant string
	}{
		{"no atomic publish", func(c *StreamConfig) { c.AllowAtomicPublish = false }, "allow_atomic"},
		{"only the record subject", func(c *StreamConfig) {
			c.Subjects = []string{mqttSessStreamSubjectPrefix + domainTk + ">"}
		}, "subjects"},
		{"only the accept subject", func(c *StreamConfig) {
			c.Subjects = []string{mqttAcceptSubjectPrefix + domainTk + ">"}
		}, "subjects"},
		{"only the handoff subject", func(c *StreamConfig) {
			c.Subjects = []string{mqttHandoffSubjectPrefix + domainTk + ">"}
		}, "subjects"},
		{"a fourth subject", func(c *StreamConfig) {
			c.Subjects = append(c.Subjects, "$MQTT.other.>")
		}, "subjects"},
		// The duplicate list is the RIGHT LENGTH, because the size check runs first: a
		// longer list would be refused for its size and this case would stop testing the
		// repeat it exists for. One subject space is therefore left unowned, which is the
		// hazard the repeat check is about.
		{"the record subject twice", func(c *StreamConfig) {
			c.Subjects = []string{
				mqttSessStreamSubjectPrefix + domainTk + ">",
				mqttSessStreamSubjectPrefix + domainTk + ">",
				mqttAcceptSubjectPrefix + domainTk + ">",
				mqttHandoffSubjectPrefix + domainTk + ">",
			}
		}, "repeat"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := base()
			tc.poison(&si.Config)
			err := validateSessStream(si, domainTk, 0)
			if err == nil {
				t.Fatal("the validator must refuse this configuration")
			}
			if !strings.Contains(err.Error(), tc.errWant) {
				t.Fatalf("the refusal must name %q, got: %v", tc.errWant, err)
			}
		})
	}

	// Order independence is the one relaxation, and it is deliberate: JetStream does
	// not promise to return the configured order.
	si := base()
	si.Config.Subjects = []string{
		mqttHandoffSubjectPrefix + domainTk + ">",
		mqttAcceptSubjectPrefix + domainTk + ">",
		mqttSessStreamSubjectPrefix + domainTk + ">",
		mqttTeardownReceiptSubjectPrefix + domainTk + ">",
	}
	if err := validateSessStream(si, domainTk, 0); err != nil {
		t.Fatalf("the subject set is order-independent: %v", err)
	}
}

// ---------------------------------------------------------------------------
// The fact record's own bindings
// ---------------------------------------------------------------------------

// TestAcceptFactBindingsAreAllChecked pins every binding a reader may not take on
// trust. A fact is evidence used to DESTROY a Will, so an unchecked binding is a
// way to make one client's acceptance — or a previous incarnation's — suppress
// another's.
func TestAcceptFactBindingsAreAllChecked(t *testing.T) {
	t.Parallel()
	const domainTk = _EMPTY_
	epoch := mqttGenSessEpoch()
	good := func() *mqttAcceptFact {
		return &mqttAcceptFact{ClientID: "binding-client", SessEpoch: epoch, AcceptID: mqttNewAcceptID(), At: 42}
	}
	subjectOf := func(f *mqttAcceptFact) string {
		return mqttAcceptSubject(domainTk, getHash(f.ClientID), f.SessEpoch, f.AcceptID)
	}
	base := good()
	if err := validateAcceptFactRecord(domainTk, epoch, subjectOf(base), base); err != nil {
		t.Fatalf("the baseline fact must validate: %v", err)
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*mqttAcceptFact) string // returns the subject to validate against
		errWant string
	}{
		{"no client id", func(f *mqttAcceptFact) string { s := subjectOf(f); f.ClientID = _EMPTY_; return s }, "no client id"},
		{"malformed epoch", func(f *mqttAcceptFact) string { f.SessEpoch = "nuid:short"; return subjectOf(f) }, "not well-formed"},
		{"dotted accept id", func(f *mqttAcceptFact) string { f.AcceptID = "a.b"; return subjectOf(f) }, "single subject token"},
		{"undated", func(f *mqttAcceptFact) string { f.At = 0; return subjectOf(f) }, "no acceptance time"},
		{"another incarnation", func(f *mqttAcceptFact) string { f.SessEpoch = mqttGenSessEpoch(); return subjectOf(f) }, "not this broker's incarnation"},
		{"stored on another client's subject", func(f *mqttAcceptFact) string {
			return mqttAcceptSubject(domainTk, getHash("someone-else"), f.SessEpoch, f.AcceptID)
		}, "but it is stored on"},
		{"body accept id differs from the subject's", func(f *mqttAcceptFact) string {
			s := subjectOf(f)
			f.AcceptID = mqttNewAcceptID()
			return s
		}, "but it is stored on"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := good()
			subj := tc.mutate(f)
			err := validateAcceptFactRecord(domainTk, epoch, subj, f)
			if err == nil {
				t.Fatal("the binding must be refused")
			}
			if !strings.Contains(err.Error(), tc.errWant) {
				t.Fatalf("the refusal must say %q, got: %v", tc.errWant, err)
			}
		})
	}
}

// TestAcceptFactHashCollisionIsSkippedNotRefused pins the one shape that is valid
// and non-qualifying rather than corrupt. Two distinct clients can share a hash;
// the fact of one is a perfectly good fact that simply says nothing about the
// other's Will. Refusing it would DEFER the second client's Wills forever.
func TestAcceptFactHashCollisionIsSkippedNotRefused(t *testing.T) {
	t.Parallel()
	epoch := mqttGenSessEpoch()
	other := &mqttAcceptFact{ClientID: "other-client", SessEpoch: epoch, AcceptID: mqttNewAcceptID(), At: 1}
	subj := mqttAcceptSubject(_EMPTY_, getHash(other.ClientID), epoch, other.AcceptID)
	if err := validateAcceptFactRecord(_EMPTY_, epoch, subj, other); err != nil {
		t.Fatalf("a valid fact about another client must not be refused: %v", err)
	}
	rec := &mqttWillOutbox{
		ClientID: "the-will-client", ClientIDHash: getHash("other-client"),
		FireAt: 1 << 62, OriginSessEpoch: epoch, OriginSessionSeq: 1,
	}
	if mqttAcceptFactQualifies(other, 99, epoch, rec) {
		t.Fatal("equal hashes are not equal clients: another client's acceptance must never qualify")
	}
}

// TestAcceptFactRefusalIsReportedAndDefersTheClient pins the operator-facing half of
// a fact this broker cannot interpret: nothing reclaims it, so every scan of that
// client meets it again — which is exactly why it is reported and why the scan must
// defer rather than proceed.
func TestAcceptFactRefusalIsReportedAndDefersTheClient(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "accept-refused"
	cidHash := getHash(cid)

	origin := testSessStreamFloor(t, a.asm)
	subj := mqttAcceptSubject(a.asm.domainTk, cidHash, a.asm.sessStreamEpoch, mqttNewAcceptID())
	testPlantRawAcceptFact(t, a.asm, subj, []byte(`{"cid":"`+cid+`","epoch":"nope","aid":"x","at":1}`))

	rec := testWillFor(a.asm, cid, "refused-w", time.Now().Add(time.Hour).UnixNano(), origin)
	sc := &mqttFactScan{upTo: 1 << 40, deadline: time.Now().Add(time.Minute), renewEvery: time.Minute}
	if v := a.asm.scanAcceptFacts(rec, sc); v != mqttFactDefer {
		t.Fatalf("a fact this broker cannot interpret must DEFER the Will, got %v: it may be the very "+
			"acceptance that suppresses it", v)
	}
	a.asm.mu.Lock()
	_, warned := a.asm.refusedAcceptFacts[subj]
	a.asm.mu.Unlock()
	if !warned {
		t.Fatal("a refused fact must be reported once per subject: nothing else ever clears it")
	}
}

// TestAcceptFactScanRespectsItsCutoff pins that a decision is reproducible: the scan
// reads nothing above the cutoff it was given, so a later claimant holding the same
// persisted cutoff reaches the same verdict.
func TestAcceptFactScanRespectsItsCutoff(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "accept-cutoff"
	fireAt := time.Now().Add(time.Hour).UnixNano()

	origin := testSessStreamFloor(t, a.asm)
	_, seq := testPlantAcceptFact(t, a.asm, cid, time.Now().UnixNano())
	rec := testWillFor(a.asm, cid, "cutoff-w", fireAt, origin)

	below := &mqttFactScan{upTo: seq - 1, deadline: time.Now().Add(time.Minute), renewEvery: time.Minute}
	if v := a.asm.scanAcceptFacts(rec, below); v != mqttFactNone {
		t.Fatalf("a fact above the cutoff must be invisible, got %v", v)
	}
	at := &mqttFactScan{upTo: seq, deadline: time.Now().Add(time.Minute), renewEvery: time.Minute}
	if v := a.asm.scanAcceptFacts(rec, at); v != mqttFactQualified {
		t.Fatalf("the same fact AT the cutoff must qualify, got %v: otherwise the row above is about the fixture", v)
	}
}

// TestAcceptFactCutoffZeroIsAValidCutoff pins why DecisionCutoff is a pointer. A
// fresh or recreated session stream has LastSeq 0, so a numeric-zero sentinel would
// collapse "no cutoff was ever taken" into a perfectly valid one.
func TestAcceptFactCutoffZeroIsAValidCutoff(t *testing.T) {
	t.Parallel()
	zero := uint64(0)
	if err := validateWillDecisionShape(mqttWillCancelled, mqttWillDecisionCancel, &zero); err != nil {
		t.Fatalf("a cancel licensed under cutoff 0 is legal: %v", err)
	}
	if err := validateWillDecisionShape(mqttWillCancelled, mqttWillDecisionCancel, nil); err == nil {
		t.Fatal("a cancel with NO cutoff must be refused, which a zero sentinel could not express")
	}
	a := &mqttWillOutbox{Decision: mqttWillDecisionPublish, DecisionCutoff: &zero}
	b := &mqttWillOutbox{Decision: mqttWillDecisionPublish, DecisionCutoff: nil}
	if a.sameDecision(b) {
		t.Fatal("a decision under cutoff 0 is not the same decision as one with no cutoff at all")
	}
}

// TestWillDecisionLegalMatrix pins the shapes a record may carry, and every shape it
// may not. publishing/cancel is the row that matters most: a cancel is licensed
// straight to cancelled, so a publishing record carrying one could only be a
// decision the entry branch would try to EXECUTE.
func TestWillDecisionLegalMatrix(t *testing.T) {
	t.Parallel()
	cut := uint64(5)
	for _, tc := range []struct {
		state mqttWillState
		dec   mqttWillDecision
		cutof *uint64
		legal bool
	}{
		{mqttWillPending, mqttWillDecisionUnset, nil, true},
		{mqttWillPending, mqttWillDecisionPublish, &cut, false},
		{mqttWillPending, mqttWillDecisionCancel, &cut, false},
		{mqttWillPublishing, mqttWillDecisionUnset, nil, true},
		{mqttWillPublishing, mqttWillDecisionPublish, &cut, true},
		{mqttWillPublishing, mqttWillDecisionCancel, &cut, false},
		{mqttWillCancelled, mqttWillDecisionCancel, &cut, true},
		{mqttWillCancelled, mqttWillDecisionPublish, &cut, false},
		{mqttWillCancelled, mqttWillDecisionUnset, nil, false},
		{mqttWillDone, mqttWillDecisionPublish, &cut, true},
		{mqttWillDone, mqttWillDecisionCancel, &cut, false},
		{mqttWillDone, mqttWillDecisionUnset, nil, false},
		{mqttWillPublishing, mqttWillDecisionPublish, nil, false},
		{mqttWillPublishing, mqttWillDecisionUnset, &cut, false},
		{mqttWillPending, "nonsense", &cut, false},
	} {
		t.Run(fmt.Sprintf("%s/%s/cutoff=%v", tc.state, tc.dec, tc.cutof != nil), func(t *testing.T) {
			err := validateWillDecisionShape(tc.state, tc.dec, tc.cutof)
			if tc.legal && err != nil {
				t.Fatalf("this shape is legal: %v", err)
			}
			if !tc.legal && err == nil {
				t.Fatal("this shape must be refused")
			}
		})
	}
}
