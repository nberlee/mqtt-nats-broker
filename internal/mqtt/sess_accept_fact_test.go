// Permanent per-CONNECT acceptance facts: the record, the atomic batch, and the
// paired ambiguity classification.
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

// testAcceptSave runs the ACCEPTING save — the one that commits the session record
// and the permanent acceptance fact in one atomic batch — under its own record
// transaction.
func testAcceptSave(t testing.TB, sess *mqttSession) error {
	t.Helper()
	epoch := sess.asm.sessStreamEpoch
	return testSessRecordTx(t, sess, func(tok recordTx) error {
		_, err := sess.saveAcceptWithDetail(tok, sess.id, epoch)
		return err
	})
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

	// Direction 1: a batch that commits stores BOTH.
	if err := testAcceptSave(t, sess); err != nil {
		t.Fatalf("the accepting save: %v", err)
	}
	if facts, _ := testFactsFor(t, a.asm, cid); len(facts) != 1 {
		t.Fatalf("a committed batch must store the fact too, got %d facts", len(facts))
	}

	// Direction 2: move the record out from under the session so its CAS base is
	// stale, then accept again. The record CAS is refused — and the fact, whose own
	// create-only CAS would have succeeded on its unique subject, must NOT be stored.
	sess.mu.Lock()
	stale := sess.seq
	sess.seq = stale - 1
	sess.mu.Unlock()

	before, _ := testFactsFor(t, a.asm, cid)
	err := testAcceptSave(t, sess)
	if err == nil {
		t.Fatal("an accepting save at a stale record sequence must be refused")
	}
	if !mqttIsRecordCASRejection(errCause(err)) {
		t.Logf("the refusal was not a CAS rejection (%v); the atomicity assertion below still holds", err)
	}
	after, _ := testFactsFor(t, a.asm, cid)
	if len(after) != len(before) {
		t.Fatalf("a REFUSED batch stored an acceptance fact anyway: %d facts before, %d after — "+
			"the fact's own CAS succeeds on its unique subject, so only atomicity keeps it out",
			len(before), len(after))
	}
}

// errCause unwraps the %v-formatted store error far enough for the CAS-kind test.
func errCause(err error) error { return err }

// ---------------------------------------------------------------------------
// T6: the paired readback
// ---------------------------------------------------------------------------

// TestAcceptPairedReadbackTable is T6.
//
// The classification is driven directly, because the point is the TABLE and not the
// plumbing that reaches it: each row is a pair of observations, and three of the
// rows are the ones a single-observation classifier gets wrong.
func TestAcceptPairedReadbackTable(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "accept-pairs"
	sess := testProvisionSession(t, a, cid)

	pw := &mqttPendingWrite{
		baseSeq:    7,
		factSubj:   mqttAcceptSubject(a.asm.domainTk, sess.idHash, a.asm.sessStreamEpoch, mqttNewAcceptID()),
		factBody:   []byte(`{"cid":"x"}`),
		factDigest: mqttRecordDigest([]byte(`{"cid":"x"}`)),
	}

	for _, tc := range []struct {
		name     string
		fact     mqttAcceptFactReadback
		verdict  mqttAttemptReadback
		wantDone bool
		want     mqttUncertainResolution
		wantErr  string
	}{
		{"landed + ours", mqttAcceptFactLanded, mqttAttemptOurs, true, mqttUncertainOurs, _EMPTY_},
		{"landed + foreign", mqttAcceptFactLanded, mqttAttemptForeign, true, mqttUncertainForeign, _EMPTY_},
		{"landed + gone", mqttAcceptFactLanded, mqttAttemptGone, true, mqttUncertainForeign, _EMPTY_},
		{"landed + at base is impossible", mqttAcceptFactLanded, mqttAttemptAtBase, true, mqttUncertainUnresolved, "still at the write's base sequence"},
		{"absent + ours is impossible", mqttAcceptFactAbsent, mqttAttemptOurs, true, mqttUncertainUnresolved, "missing from"},
		{"absent + at base falls through to the re-attempt", mqttAcceptFactAbsent, mqttAttemptAtBase, false, 0, _EMPTY_},
		{"absent + foreign falls through", mqttAcceptFactAbsent, mqttAttemptForeign, false, 0, _EMPTY_},
		{"unreadable settles nothing", mqttAcceptFactUnreadable, mqttAttemptForeign, true, mqttUncertainUnresolved, "unable to read back"},
		{"corrupt refuses", mqttAcceptFactCorrupt, mqttAttemptOurs, true, mqttUncertainUnresolved, "not the one this CONNECT wrote"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The landed+ours row settles by adopting a sequence, which needs a real
			// transaction; every other row returns before touching one.
			var got mqttUncertainResolution
			var gerr error
			var done bool
			run := func(tok recordTx) error {
				got, gerr, done = sess.resolveAcceptPair(tok, pw, tc.fact, tc.verdict, 9, _EMPTY_, _EMPTY_)
				return nil
			}
			if tc.fact == mqttAcceptFactLanded && tc.verdict == mqttAttemptOurs {
				sess.mu.Lock()
				sess.seqUncertain, sess.pendingWrite = true, pw
				sess.mu.Unlock()
			}
			if err := testSessRecordTx(t, sess, run); err != nil {
				t.Fatalf("drive the pair: %v", err)
			}
			if done != tc.wantDone {
				t.Fatalf("done = %v, want %v", done, tc.wantDone)
			}
			if !tc.wantDone {
				return
			}
			if got != tc.want {
				t.Fatalf("resolution = %v, want %v (err=%v)", got, tc.want, gerr)
			}
			if tc.wantErr != _EMPTY_ {
				if gerr == nil || !strings.Contains(gerr.Error(), tc.wantErr) {
					t.Fatalf("the refusal must say %q, got %v", tc.wantErr, gerr)
				}
			}
		})
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

// TestAcceptFactBatchFramingCarriesBothCASBases pins that the two messages of the
// batch each keep their OWN per-subject CAS. The binding between them is atomicity;
// a cross-subject CAS header inside a batch is refused by the server with a code
// this codebase reads as transient contention, so a design that reached for one
// would retry a permanent misconfiguration forever.
func TestAcceptFactBatchFramingCarriesBothCASBases(t *testing.T) {
	t.Parallel()
	fact, record, batchID := mqttComposeAcceptBatch("$MQTT.accept.h.e.a", []byte(`{"f":1}`), _EMPTY_, "h", 12, []byte(`{"r":1}`))
	fh, rh := string(fact.msg[:fact.hdr]), string(record.msg[:record.hdr])

	for _, want := range []string{JSExpectedLastSubjSeq + ":0", JSBatchId + ":", JSBatchSeq + ":1"} {
		if !strings.Contains(fh, want) {
			t.Fatalf("the staged fact's header must carry %q: %q", want, fh)
		}
	}
	if strings.Contains(fh, JSBatchCommit) {
		t.Fatalf("only the LAST message commits: %q", fh)
	}
	for _, want := range []string{JSExpectedLastSubjSeq + ":12", JSBatchSeq + ":2", JSBatchCommit + ":1"} {
		if !strings.Contains(rh, want) {
			t.Fatalf("the record's header must carry %q: %q", want, rh)
		}
	}
	const crossSubjectCAS = JSExpectedLastSubjSeq + "-Subject"
	if strings.Contains(fh, crossSubjectCAS) || strings.Contains(rh, crossSubjectCAS) {
		t.Fatalf("no cross-subject CAS header may appear in a batch — the server refuses it with a code "+
			"this codebase reads as transient contention: %q / %q", fh, rh)
	}

	// A fresh batch id per attempt: reuse while a batch is staged abandons it.
	_, _, batchID2 := mqttComposeAcceptBatch("$MQTT.accept.h.e.a", []byte(`{"f":1}`), _EMPTY_, "h", 12, []byte(`{"r":1}`))
	if batchID == batchID2 {
		t.Fatal("two compositions of the same semantic attempt must not share a batch id")
	}
	// The id is RETURNED, not merely embedded, because the acknowledgement has to be
	// bound to this batch: "some batch of two committed" is not evidence about the
	// batch whose sequence the caller is about to adopt.
	if idOf(t, fact.msg[:fact.hdr]) != batchID || idOf(t, record.msg[:record.hdr]) != batchID {
		t.Fatalf("both messages must carry the returned batch id %q", batchID)
	}
	// And a single write is framed exactly as it always was.
	single, shdr := mqttFrameRecordWrite(12, []byte(`{"r":1}`))
	if strings.Contains(string(single[:shdr]), "Nats-Batch") {
		t.Fatalf("a non-batch record write must emit no batch headers: %q", single[:shdr])
	}
}

func idOf(t *testing.T, hdr []byte) string {
	t.Helper()
	for _, line := range strings.Split(string(hdr), CR_LF) {
		if v, ok := strings.CutPrefix(line, JSBatchId+":"); ok {
			return v
		}
	}
	t.Fatalf("no batch id in %q", hdr)
	return _EMPTY_
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

// TestAcceptBatchAckMustNameThisBatch pins that a success is bound to the batch this
// broker actually sent.
//
// "Some batch of two committed" is not the claim the caller acts on: it adopts the
// acknowledged sequence as the SESSION RECORD's, which is only true of an
// acknowledgement naming this batch and both of its messages. A nil embedded PubAck
// is the same class, and would panic at the dereference rather than be refused.
func TestAcceptBatchAckMustNameThisBatch(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "batch-ack"
	sess := testProvisionSession(t, a, cid)

	compose := func() (mqttBatchMsg, mqttBatchMsg, string) {
		fact := &mqttAcceptFact{ClientID: cid, SessEpoch: a.asm.sessStreamEpoch,
			AcceptID: mqttNewAcceptID(), At: time.Now().UnixNano()}
		body, err := json.Marshal(fact)
		if err != nil {
			t.Fatalf("marshal the fact: %v", err)
		}
		subj := mqttAcceptSubject(a.asm.domainTk, sess.idHash, fact.SessEpoch, fact.AcceptID)
		// The record's CURRENT sequence, re-read rather than taken from memory: a
		// refused acknowledgement does not mean the write was refused, so the previous
		// send in this test really did advance the subject.
		smsg, lerr := a.asm.jsa.loadSessionMsg(a.asm.domainTk, sess.idHash)
		if lerr != nil {
			t.Fatalf("read the record's current sequence: %v", lerr)
		}
		return mqttComposeAcceptBatch(subj, body, a.asm.domainTk, sess.idHash, smsg.Sequence, []byte(`{"r":1}`))
	}

	// A batch id the send never used. The server answers with the id it was actually
	// given, so the boundary must refuse its own answer — and note what that refusal
	// does NOT mean: the batch COMMITTED. Refusing an unbindable acknowledgement is
	// therefore an UNCERTAIN outcome for the caller, not a definite one, which is
	// exactly how persistRecord classifies a non-CAS error.
	fm, rm, _ := compose()
	_, err := a.asm.jsa.storeAcceptBatch(a.asm.domainTk, sess.idHash, "a-batch-id-we-never-sent", fm, rm)
	if err == nil {
		t.Fatal("an acknowledgement that does not name this batch must be refused")
	}
	if !strings.Contains(err.Error(), "but this broker sent") {
		t.Fatalf("the refusal must name the batch this broker sent, got: %v", err)
	}

	// The control: the SAME send, validated against its own id, succeeds — so the row
	// above is about the binding and not about the fixture.
	fm, rm, id := compose()
	resp, err := a.asm.jsa.storeAcceptBatch(a.asm.domainTk, sess.idHash, id, fm, rm)
	if err != nil {
		t.Fatalf("a correctly bound acceptance batch must commit: %v", err)
	}
	if resp.BatchId != id || resp.BatchSize != mqttAcceptBatchSize {
		t.Fatalf("the acknowledgement must name this batch and both messages, got %q/%d",
			resp.BatchId, resp.BatchSize)
	}

	// And an acknowledgement carrying no PubAck at all is refused rather than
	// dereferenced: the embedded *PubAck is a POINTER.
	if batchIdOf(&JSPubAckResponse{}) != _EMPTY_ || batchSizeOf(&JSPubAckResponse{}) != 0 {
		t.Fatal("an acknowledgement with no publish acknowledgement must read as empty, never panic")
	}
	if batchIdOf(nil) != _EMPTY_ || batchSizeOf(nil) != 0 {
		t.Fatal("a nil acknowledgement must read as empty, never panic")
	}
}

// TestAcceptStoreObserverSeesBothMoments pins the accepted-CONNECT store observation
// seam's own contract, on a real store: it fires once before the batch is issued and
// once after the acknowledgement has been bound to that batch, both moments describe
// the SAME attempt, only the second carries a sequence, and an observer that refuses a
// moment fails the store rather than merely being told about it.
//
// The seam exists for TestMQTTAcceptBatchReplicatedIndivisible, which has to read both
// subjects back at the store boundary and compare them against what went out. The
// (sess, resp, err) result hook cannot carry that: it has neither subject nor digest,
// and it fires only after storeAcceptBatch has returned.
func TestAcceptStoreObserverSeesBothMoments(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "accept-obs"
	sess := testProvisionSession(t, a, cid)

	// The record's CURRENT sequence is re-read for every attempt: a refused attempt
	// does not prove the subject stayed where it was.
	compose := func() (mqttBatchMsg, mqttBatchMsg, string) {
		t.Helper()
		fact := &mqttAcceptFact{ClientID: cid, SessEpoch: a.asm.sessStreamEpoch,
			AcceptID: mqttNewAcceptID(), At: time.Now().UnixNano()}
		body, err := json.Marshal(fact)
		if err != nil {
			t.Fatalf("marshal the fact: %v", err)
		}
		subj := mqttAcceptSubject(a.asm.domainTk, sess.idHash, fact.SessEpoch, fact.AcceptID)
		smsg, lerr := a.asm.jsa.loadSessionMsg(a.asm.domainTk, sess.idHash)
		if lerr != nil {
			t.Fatalf("read the record's current sequence: %v", lerr)
		}
		return mqttComposeAcceptBatch(subj, body, a.asm.domainTk, sess.idHash,
			smsg.Sequence, []byte(`{"r":1}`))
	}

	var seen []mqttAcceptStoreObs
	restore := a.asm.jsa.setAcceptStoreObserver(func(o mqttAcceptStoreObs) error {
		seen = append(seen, o)
		return nil
	})
	fm, rm, id := compose()
	resp, err := a.asm.jsa.storeAcceptBatch(a.asm.domainTk, sess.idHash, id, fm, rm)
	restore()
	if err != nil {
		t.Fatalf("the acceptance batch must commit: %v", err)
	}

	if len(seen) != 2 {
		t.Fatalf("the seam must fire exactly twice for one batch, got %d", len(seen))
	}
	if seen[0].Phase != mqttAcceptStoreIssuing || seen[1].Phase != mqttAcceptStoreAcked {
		t.Fatalf("the moments must be issuing then acked, got %d then %d",
			seen[0].Phase, seen[1].Phase)
	}
	for i, o := range seen {
		if o.BatchID != id || o.FactSubj != fm.subj || o.RecSubj != rm.subj {
			t.Fatalf("moment %d does not describe this attempt: batch %q, fact %q, record %q",
				i, o.BatchID, o.FactSubj, o.RecSubj)
		}
		if o.FactDigest != mqttRecordDigest(fm.msg[fm.hdr:]) ||
			o.RecDigest != mqttRecordDigest(rm.msg[rm.hdr:]) {
			t.Fatalf("moment %d does not carry the digests of the bodies that went out", i)
		}
	}
	// Only the acknowledged moment has a sequence, and it is the SESSION RECORD's —
	// which is the whole reason the phase is part of the contract.
	if seen[0].Seq != 0 {
		t.Fatalf("the pre-issue moment cannot name a sequence, got %d", seen[0].Seq)
	}
	if seen[1].Seq != resp.Sequence {
		t.Fatalf("the acknowledged moment must carry the record's sequence %d, got %d",
			resp.Sequence, seen[1].Seq)
	}

	// A refusal at the pre-issue moment fails the store AND leaves the subject where it
	// was: an interception point, not a notification.
	before, lerr := a.asm.jsa.loadSessionMsg(a.asm.domainTk, sess.idHash)
	if lerr != nil {
		t.Fatalf("read the record before the refused attempt: %v", lerr)
	}
	restore = a.asm.jsa.setAcceptStoreObserver(func(o mqttAcceptStoreObs) error {
		if o.Phase == mqttAcceptStoreIssuing {
			return fmt.Errorf("refused before the batch was issued")
		}
		return nil
	})
	defer restore()
	fm, rm, id = compose()
	if _, err := a.asm.jsa.storeAcceptBatch(a.asm.domainTk, sess.idHash, id, fm, rm); err == nil ||
		!strings.Contains(err.Error(), "refused before the batch was issued") {
		t.Fatalf("a refusal at the pre-issue moment must fail the store, got: %v", err)
	}
	after, lerr := a.asm.jsa.loadSessionMsg(a.asm.domainTk, sess.idHash)
	if lerr != nil {
		t.Fatalf("read the record after the refused attempt: %v", lerr)
	}
	if after.Sequence != before.Sequence {
		t.Fatalf("the refused attempt must not have written anything: the record moved "+
			"from %d to %d", before.Sequence, after.Sequence)
	}
}
