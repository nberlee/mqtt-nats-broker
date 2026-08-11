package mqtt

import (
	"encoding/json"
	"errors"
	"testing"
)

// testTeardownEnv is one embedded NATS/JetStream with two account session
// managers in front of it, which is what a teardown race needs: distinct broker
// identities (jsa.id) sharing one $MQTT_sess stream. The in-process locks the old
// code relied on are per-manager, so this is exactly the arbitration they never
// provided.
type testTeardownEnv struct {
	s   *Server
	asm *mqttAccountSessionManager
}

func testNewTeardownEnv(t *testing.T) (a, b *testTeardownEnv) {
	t.Helper()
	_, nc := runNATS(t)

	newBroker := func(node string) *testTeardownEnv {
		s := &Server{
			nrc:      nc,
			opts:     &Options{},
			quitCh:   make(chan struct{}),
			nodeName: node,
		}
		s.running.Store(true)
		s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
		t.Cleanup(func() { close(s.quitCh) })
		c := s.createInternalAccountClient()
		asm, err := s.getOrCreateMQTTAccountSessionManager(c)
		if err != nil {
			t.Fatalf("getOrCreateMQTTAccountSessionManager(%s): %v", node, err)
		}
		return &testTeardownEnv{s: s, asm: asm}
	}
	a, b = newBroker("nodeA"), newBroker("nodeB")
	if a.asm.jsa.id == b.asm.jsa.id {
		t.Fatal("the two brokers must have distinct JSA identities")
	}
	return a, b
}

// testLoadSessRecord returns the client's current session record and its sequence,
// or nil when there is none.
func testLoadSessRecord(t *testing.T, env *testTeardownEnv, idHash string) (*mqttPersistedSession, uint64) {
	t.Helper()
	smsg, err := env.asm.jsa.loadSessionMsg(env.asm.domainTk, idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("loadSessionMsg: %v", err)
		}
		return nil, 0
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("unmarshal session record: %v", err)
	}
	return ps, smsg.Sequence
}

// testRecordBodyAt returns the RAW stored bytes of a client's session record — the
// digest identity, as opposed to the decoded view testLoadSessRecord returns.
func testRecordBodyAt(t *testing.T, env *testTeardownEnv, idHash string) []byte {
	t.Helper()
	smsg, err := env.asm.jsa.loadSessionMsg(env.asm.domainTk, idHash)
	if err != nil {
		t.Fatalf("loadSessionMsg: %v", err)
	}
	return smsg.Data
}

// testConsumerExists reports whether a durable is present on a stream.
func testConsumerExists(t *testing.T, env *testTeardownEnv, stream, durable string) bool {
	t.Helper()
	ci, err := env.asm.jsa.lookupConsumer(stream, durable)
	if err != nil {
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			t.Fatalf("lookupConsumer(%q, %q): %v", stream, durable, err)
		}
		return false
	}
	return ci != nil
}

// testProvisionSession creates a session on env with a QoS 2 subscription's worth
// of durable state — a plain consumer, a PUBREL consumer and an inbound QoS 2 hold —
// saves its record, and returns it. Everything it creates is named after the
// session's incarnation generation (or, for the plain durable, per-incarnation
// unique), which is what the teardown tests assert about.
func testProvisionSession(t *testing.T, env *testTeardownEnv, clientID string) *mqttSession {
	t.Helper()
	sess, _, err := env.asm.createOrRestoreSession(clientID, env.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if sess.asm == nil {
		t.Fatal("createOrRestoreSession must attach the account manager")
	}

	_, _, pubRelDur := mqttPubRelNames(sess.idHash, sess.gen)
	ccr := &CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			DeliverSubject: sess.pubRelDeliverySubject,
			Durable:        pubRelDur,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  sess.pubRelSubject,
		},
	}
	if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
		t.Fatalf("create PUBREL consumer: %v", err)
	}
	sess.pubRelConsumer = &ccr.Config

	if _, err := sess.jsa.storeMsg(mqttQoS2HoldSubject(sess.idHash, sess.gen, 1), 0, []byte("held")); err != nil {
		t.Fatalf("store QoS2 hold: %v", err)
	}

	sess.subs = map[string]byte{"foo": 2}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	return sess
}

// TestSessTeardownLostClaimDeletesNothing is test 0b: broker A resolved against
// record sequence S, broker B took the session over and saved a newer record, and
// only then does A attempt its teardown. A's claim CAS must lose, and A must delete
// NOTHING — not the record, and not one durable B just restored.
//
// Regression: clear() previously deleted the durables and then the record at S,
// which MaxMsgsPer:1 had already removed — reporting "sequence not found", which it
// read as success. B was left with a valid record naming deleted consumers, and on
// the interest-retained streams their messages went with them.
func TestSessTeardownLostClaimDeletesNothing(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "lostclaim"

	sessA := testProvisionSession(t, a, clientID)
	// Captured before the teardown, which re-mints sessA.gen in memory.
	retiredGen := sessA.gen
	_, _, pubRelDur := mqttPubRelNames(sessA.idHash, retiredGen)
	retiredHold := mqttQoS2HoldSubject(sessA.idHash, retiredGen, 1)
	staleSeq := sessA.seq

	// B takes over: it restores the SAME record (same generation, same durables) and
	// saves, so the record now sits beyond A's stale sequence.
	sessB, existed, err := b.asm.createOrRestoreSession(clientID, b.s.getOpts())
	if err != nil || !existed {
		t.Fatalf("B restore: existed=%v err=%v", existed, err)
	}
	if sessB.gen != retiredGen {
		t.Fatalf("a resume must preserve the generation: %q vs %q", retiredGen, sessB.gen)
	}
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("B save: %v", err)
	}
	_, bSeq := testLoadSessRecord(t, b, sessA.idHash)
	if bSeq <= staleSeq {
		t.Fatalf("B's record must sit beyond A's stale sequence, got %v vs %v", bSeq, staleSeq)
	}

	// A now tears down against its stale view. Losing the claim is a COMPLETE
	// outcome — there is nothing left for A to do — and since F-029 it is REPORTED as
	// such rather than as success: a caller that read it as success went on to write
	// at the sequence clear() had just reset to 0, erasing B's record. What still
	// matters just as much is that A touched nothing.
	if err := sessA.clear(false); !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("A's superseded teardown must report the superseded outcome, got %v", err)
	}

	ps, seq := testLoadSessRecord(t, a, sessA.idHash)
	if ps == nil {
		t.Fatal("A deleted B's session record after losing the teardown claim")
	}
	if seq != bSeq {
		t.Errorf("record sequence = %v, want B's %v: A rewrote a record it does not own", seq, bSeq)
	}
	if ps.Teardown != nil {
		t.Error("A marked a record it lost the claim on")
	}
	if !testConsumerExists(t, b, mqttOutStreamName, pubRelDur) {
		t.Error("A deleted the PUBREL consumer B is using after losing the teardown claim")
	}
	if _, err := b.asm.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, retiredHold); err != nil {
		t.Errorf("A purged the QoS2 hold B is using after losing the teardown claim: %v", err)
	}
}

// TestSessTeardownLateDeleteIsHarmless is test 0a: the window a nonce re-check
// cannot close. A is paused AFTER its claim is confirmed and before its first
// delete; B finishes the whole teardown, releases the record and creates a fresh
// session; only then does A run its deletions and its release.
//
// Every resource of the NEW incarnation must survive, and A must classify its own
// release as superseded rather than as success.
func TestSessTeardownLateDeleteIsHarmless(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "latedelete"

	sessA := testProvisionSession(t, a, clientID)
	retiredGen := sessA.gen
	_, retiredSeq := testLoadSessRecord(t, a, sessA.idHash)

	var (
		freshSess *mqttSession
		freshGen  string
	)
	// Park A in the post-claim window and let B do everything.
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, nonce string, markerSeq uint64) {
		if sess != sessA {
			return
		}
		// B adopts the marker, finishes the teardown and starts fresh. This is the
		// CONNECT path: a marked record is finished, never resumed.
		ps, seq := testLoadSessRecord(t, b, sessA.idHash)
		if ps == nil || ps.Teardown == nil {
			t.Errorf("B should see A's teardown marker, got ps=%v", ps)
			return
		}
		if ps.Teardown.Nonce != nonce || seq != markerSeq {
			t.Errorf("marker mismatch: nonce %q vs %q, seq %v vs %v", ps.Teardown.Nonce, nonce, seq, markerSeq)
		}
		var existed bool
		var err error
		freshSess, existed, err = b.asm.createOrRestoreSession(clientID, b.s.getOpts())
		if err != nil {
			t.Errorf("B CONNECT over a marked record: %v", err)
			return
		}
		if existed {
			t.Error("a marked record must never be resumed: Session Present must be false")
		}
		freshGen = freshSess.gen
		if freshGen == retiredGen {
			t.Error("the fresh session must not reuse the retiring generation")
		}
		// B provisions the new incarnation's resources, all named after freshGen.
		_, _, freshPubRelDur := mqttPubRelNames(freshSess.idHash, freshGen)
		ccr := &CreateConsumerRequest{
			Stream: mqttOutStreamName,
			Config: ConsumerConfig{
				DeliverSubject: freshSess.pubRelDeliverySubject,
				Durable:        freshPubRelDur,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  freshSess.pubRelSubject,
			},
		}
		if _, err := freshSess.jsa.createDurableConsumer(ccr); err != nil {
			t.Errorf("B create fresh PUBREL consumer: %v", err)
			return
		}
		freshSess.pubRelConsumer = &ccr.Config
		if _, err := freshSess.jsa.storeMsg(mqttQoS2HoldSubject(freshSess.idHash, freshGen, 1), 0, []byte("fresh")); err != nil {
			t.Errorf("B store fresh QoS2 hold: %v", err)
			return
		}
		freshSess.subs = map[string]byte{"foo": 2}
		if err := testSessSave(t, freshSess); err != nil {
			t.Errorf("B save fresh session: %v", err)
		}
	})
	defer restore()

	// A resumes: its deletions and its release now land after everything above. Its
	// release re-read finds B's record, so the outcome is superseded — a complete one,
	// reported as errMQTTClearSuperseded since F-029.
	if err := sessA.clear(false); !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("A's late teardown must report the superseded outcome, got %v", err)
	}
	restore()

	if freshSess == nil || freshGen == _EMPTY_ {
		t.Fatal("the hook did not complete B's handover")
	}

	// Every resource of the NEW incarnation must still be there.
	_, _, freshPubRelDur := mqttPubRelNames(freshSess.idHash, freshGen)
	if !testConsumerExists(t, b, mqttOutStreamName, freshPubRelDur) {
		t.Error("the new incarnation's PUBREL consumer was deleted by the retired incarnation's late teardown")
	}
	if _, err := b.asm.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName,
		mqttQoS2HoldSubject(freshSess.idHash, freshGen, 1)); err != nil {
		t.Errorf("the new incarnation's QoS2 hold was purged by the retired incarnation's late teardown: %v", err)
	}
	ps, seq := testLoadSessRecord(t, b, sessA.idHash)
	if ps == nil {
		t.Fatal("the new incarnation's session record was deleted by the retired incarnation's late release")
	}
	if ps.Teardown != nil {
		t.Error("the new incarnation's record carries a teardown marker")
	}
	if ps.Gen != freshGen {
		t.Errorf("record generation = %q, want the fresh %q", ps.Gen, freshGen)
	}
	if seq <= retiredSeq {
		t.Errorf("the surviving record must be the fresh one, at a sequence beyond %v, got %v", retiredSeq, seq)
	}
	// And the retired incarnation's own resources really are gone.
	_, _, retiredPubRelDur := mqttPubRelNames(sessA.idHash, retiredGen)
	if testConsumerExists(t, a, mqttOutStreamName, retiredPubRelDur) {
		t.Error("the retired incarnation's PUBREL consumer survived its teardown")
	}
}

// TestSessTeardownAdoptedByConnect is tests 0c and 0d: a CONNECT that finds a
// teardown marker finishes it — deleting the retiring incarnation's resources and
// releasing the record — and then starts a fresh session with Session Present
// false. That holds whether the original owner is merely paused (0c) or gone
// entirely (0d, a crashed teardown, which previously leaked until the orphan
// sweep).
func TestSessTeardownAdoptedByConnect(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "adopted"

	sessA := testProvisionSession(t, a, clientID)
	retiredGen := sessA.gen
	_, _, retiredPubRelDur := mqttPubRelNames(sessA.idHash, retiredGen)
	retiredHold := mqttQoS2HoldSubject(sessA.idHash, retiredGen, 1)

	// Leave a marker with nobody finishing it: A claimed and then "crashed".
	ps, seq := testLoadSessRecord(t, a, sessA.idHash)
	if ps == nil {
		t.Fatal("no session record to mark")
	}
	var (
		markerSeq uint64
		nonce     string
	)
	claimTok := sessA.beginRecordTx()
	markerSeq, nonce, claim, err := sessA.claimTeardown(claimTok, ps, seq, sessA.wid)
	if claim != mqttTeardownClaimWon {
		t.Fatalf("claimTeardown = %v, err=%v", claim, err)
	}
	if err := sessA.endRecordTx(claimTok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	marked, mseq := testLoadSessRecord(t, a, sessA.idHash)
	if marked == nil || marked.Teardown == nil || marked.Teardown.Nonce != nonce || mseq != markerSeq {
		t.Fatalf("marker not persisted as claimed: %v at %v (want nonce %q at %v)", marked, mseq, nonce, markerSeq)
	}
	// The marker must PRESERVE the record's content, or the orphan sweep would read
	// the durables it names as unreferenced and reap them mid-teardown.
	if marked.Gen != retiredGen || len(marked.Subs) != len(ps.Subs) || marked.PubRel == nil {
		t.Errorf("the teardown marker must keep the record's content, got %+v", marked)
	}

	// B CONNECTs over it.
	fresh, existed, err := b.asm.createOrRestoreSession(clientID, b.s.getOpts())
	if err != nil {
		t.Fatalf("B CONNECT over a marked record: %v", err)
	}
	if existed {
		t.Error("a marked record must not be resumed: Session Present must be false")
	}
	if fresh.gen == retiredGen {
		t.Error("the adopting CONNECT must start on a fresh generation")
	}
	if !mqttValidSessGen(fresh.gen) {
		t.Errorf("the adopting CONNECT minted an invalid generation %q", fresh.gen)
	}
	if fresh.seq != 0 {
		t.Errorf("the fresh session must not inherit the released record's sequence, got %v", fresh.seq)
	}

	// The retiring incarnation's resources are gone, and so is its record.
	if testConsumerExists(t, b, mqttOutStreamName, retiredPubRelDur) {
		t.Error("the adopting CONNECT left the retiring incarnation's PUBREL consumer behind")
	}
	if _, err := b.asm.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, retiredHold); err == nil {
		t.Error("the adopting CONNECT left the retiring incarnation's QoS2 hold behind")
	}
	if got, _ := testLoadSessRecord(t, b, sessA.idHash); got != nil {
		t.Errorf("the adopting CONNECT did not release the marked record, still %+v", got)
	}

	// 0c: the original owner wakes up. Its deletions name the retired generation
	// only, and its release must classify as superseded/released, never as license
	// to keep deleting. Superseded is now reported as errMQTTClearSuperseded rather
	// than as success (F-029); anything else is a real failure.
	if err := sessA.clear(false); err != nil && !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("the paused owner's teardown reported an error after the handover: %v", err)
	}
}

// TestSessTeardownReleaseIsCAS is test 0e: a release whose delete removes nothing
// must NOT be read as success. With the marker replaced by a foreign write, the
// re-read sees another owner and the teardown stops — in particular it must not go
// on to release shared group durables the new owner may hold.
func TestSessTeardownReleaseIsCAS(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "releasecas"

	sessA := testProvisionSession(t, a, clientID)
	ps, seq := testLoadSessRecord(t, a, sessA.idHash)
	target := mqttTeardownTargetFromRecord(ps, sessA.idHash)

	tok := sessA.beginRecordTx()
	markerSeq, nonce, claim, err := sessA.claimTeardown(tok, ps, seq, sessA.wid)
	if claim != mqttTeardownClaimWon {
		t.Fatalf("claimTeardown = %v, err=%v", claim, err)
	}

	// A foreign owner replaces the record. MaxMsgsPer:1 removes markerSeq as it
	// lands, so A's sequence-targeted delete will remove nothing at all.
	foreign := *ps
	foreign.Teardown = nil
	foreign.Origin = b.asm.jsa.id
	foreignSeq, err := mqttStoreSessionRecordCAS(&b.asm.jsa, b.asm.domainTk, &foreign, sessA.idHash, markerSeq)
	if err != nil {
		t.Fatalf("foreign takeover write: %v", err)
	}

	res, rerr := sessA.releaseTeardown(tok, target, nonce, markerSeq)
	if res != mqttTeardownSuperseded {
		t.Fatalf("release classification = %v (err=%v), want superseded: a no-op delete was read as our own success", res, rerr)
	}
	if got, gotSeq := testLoadSessRecord(t, a, sessA.idHash); got == nil || gotSeq != foreignSeq {
		t.Fatalf("the foreign record must survive at sequence %v, got %+v at %v", foreignSeq, got, gotSeq)
	}
	if err := sessA.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// Our own marker still current, by contrast, is the one retryable outcome and
	// must end as released once the delete takes.
	sessA.seq = foreignSeq
	ps2, seq2 := testLoadSessRecord(t, a, sessA.idHash)
	tok2 := sessA.beginRecordTx()
	markerSeq2, nonce2, claim2, err := sessA.claimTeardown(tok2, ps2, seq2, sessA.wid)
	if claim2 != mqttTeardownClaimWon {
		t.Fatalf("second claimTeardown = %v, err=%v", claim2, err)
	}
	if res, rerr := sessA.releaseTeardown(tok2, target, nonce2, markerSeq2); res != mqttTeardownReleased {
		t.Fatalf("release classification = %v (err=%v), want released", res, rerr)
	}
	if err := sessA.endRecordTx(tok2); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	if got, _ := testLoadSessRecord(t, a, sessA.idHash); got != nil {
		t.Fatalf("the record should be gone after a released teardown, got %+v", got)
	}
}

// TestSessTeardownNoWaitReleaseIsStillClassified pins that a no-wait teardown —
// the shape every clean disconnect uses — ends in a CLASSIFIED release and a
// complete cleanup.
//
// Note what this does and does not prove. It is not a regression test for an
// observed failure: with a fire-and-forget record delete the queued request
// usually still reaches the server before the classifying re-read, so this test
// passes either way. That timing is exactly why releaseTeardown confirms the
// record delete unconditionally instead of honoring the no-wait flag — when the
// re-read loses that race it sees our own marker, misreads an unlanded delete as
// "retry", and fails an ordinary disconnect. A property that holds only by
// send-queue timing is not a property.
func TestSessTeardownNoWaitReleaseIsStillClassified(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "nowait"

	sess := testProvisionSession(t, a, clientID)
	retiredGen := sess.gen
	_, _, retiredPubRelDur := mqttPubRelNames(sess.idHash, retiredGen)

	if err := sess.clear(true); err != nil {
		t.Fatalf("a no-wait teardown must still classify its release: %v", err)
	}
	if ps, _ := testLoadSessRecord(t, a, sess.idHash); ps != nil {
		t.Fatalf("the record survived a no-wait teardown: %+v", ps)
	}
	if testConsumerExists(t, a, mqttOutStreamName, retiredPubRelDur) {
		t.Error("the retiring incarnation's PUBREL consumer survived a no-wait teardown")
	}
}
