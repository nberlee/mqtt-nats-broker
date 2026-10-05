// Copyright 2026 The mqtt-nats-bridge Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// F-105 8c: a closure certificate that outlives its process, and the clearance it licenses.
//
// None of these tests is parallel: the persistence observer is process-wide, because the
// persister runs in its own goroutine with no account in reach.

// testObserveCertPersist installs the persistence observer for one binding. Certificates of THIS
// binding are delivered on the returned channel and written only when write is true; every other
// binding's certificate is written untouched, so a concurrent fixture is not disturbed.
func testObserveCertPersist(t *testing.T, b *mqttConnBinding, write bool) <-chan *mqttDurableCloseCert {
	t.Helper()
	ch := make(chan *mqttDurableCloseCert, 4)
	h := func(c *mqttDurableCloseCert) bool {
		if c.Run != b.run || c.Conn != b.conn {
			return true
		}
		ch <- c
		return write
	}
	prev := mqttTestCloseCertPersistV.Swap(&h)
	t.Cleanup(func() { mqttTestCloseCertPersistV.Store(prev) })
	return ch
}

func testAwaitCertPersist(t *testing.T, ch <-chan *mqttDurableCloseCert, what string) *mqttDurableCloseCert {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: no closure certificate was persisted", what)
	}
	return nil
}

// testAwaitDurableCert polls the store until the certificate of b under the section's generation
// stands. A persisted write is asynchronous by design, so the reader waits for the fact.
func testAwaitDurableCert(t *testing.T, e *testInflightEnv, b *mqttConnBinding) *mqttDurableCloseCert {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := e.asm.jsa.loadDurableCloseCert(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, b.run, b.conn)
		if err == nil && c != nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("the durable certificate of %s/%s never stood (last: %v, %v)", b.run, b.conn, c, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testPersistLaunched(b *mqttConnBinding) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.persistLaunched
}

// testCertifiedStandingToken builds the crash cut 8c exists for: an ordinary delivery acquired on
// a connection, its token standing on the register, the connection certified closed — and the
// holder's own drain never run. persist decides whether the certificate reaches the store.
func testCertifiedStandingToken(t *testing.T, clientID string, qos byte, persist bool) (*testRow2PubSection, *mqttInflightRecord, uint64) {
	t.Helper()
	return testCertifiedStandingTokenAfter(t, clientID, qos, persist, nil)
}

// testCertifiedStandingTokenAfter is testCertifiedStandingToken with a step between the
// acquisition and the certification, for a fixture whose token must survive a later arrow.
func testCertifiedStandingTokenAfter(t *testing.T, clientID string, qos byte, persist bool,
	after func(s *testRow2PubSection)) (*testRow2PubSection, *mqttInflightRecord, uint64) {
	t.Helper()
	s := testRow2OpenSection(t, clientID, qos)
	s.acquire(t)
	if after != nil {
		after(s)
	}
	pre, smsg, err := s.e.asm.loadInflightRecord(s.subject)
	if err != nil || smsg == nil || pre == nil || pre.ActionToken == _EMPTY_ {
		t.Fatalf("the fixture needs a standing token: %+v (%v)", pre, err)
	}
	b := s.e.c.outBind
	got := testObserveCertPersist(t, b, persist)
	mqttCertifyClosed(s.e.c, b, "crash cut: the holder never drains")
	testAwaitCertPersist(t, got, "certifying the acquiring connection")
	if persist {
		testAwaitDurableCert(t, s.e, b)
	}
	return s, pre, smsg.Sequence
}

// testResumeOnPeer resumes the fixture's session on a second broker and runs its register walk.
//
// walk, when set, runs at the boundary between the certified-clearance pre-pass and the walk.
func testResumeOnPeer(t *testing.T, s *testRow2PubSection, clientID string,
	walk func(asm *mqttAccountSessionManager)) (*mqttAccountSessionManager, *mqttSession, *client) {
	t.Helper()
	asm2, sess2, c2 := s.e.peer(t, "node2", clientID)
	if c2.outBind == nil {
		c2.outBind = mqttBindConnection(c2.cid + 1000)
	}
	sess2.mu.Lock()
	sess2.c = c2
	sess2.mu.Unlock()
	if walk != nil {
		asm2.mu.Lock()
		asm2.testResumeWalkEntry = func(string) { walk(asm2) }
		asm2.mu.Unlock()
	}
	if _, err := sess2.recoverInflightRegisters(c2); err != nil {
		t.Fatalf("the successor's resume failed: %v", err)
	}
	return asm2, sess2, c2
}

// --- Persistence ------------------------------------------------------------------------------

// P: the certificate of a connection that acquired is persisted, naming the connection and the
// generation it acquired under.
func TestF105CertificateOfAnAcquiringConnectionIsPersisted(t *testing.T) {
	s := testRow2OpenSection(t, "cert-persist", 1)
	s.acquire(t)
	b := s.e.c.outBind
	got := testObserveCertPersist(t, b, true)
	mqttCertifyClosed(s.e.c, b, "teardown")
	c := testAwaitCertPersist(t, got, "teardown")
	if c.ID != s.e.sess.id || c.Epoch != s.e.asm.sessStreamEpoch || c.Gen != s.e.sess.gen {
		t.Fatalf("the certificate names %+v, not the acquiring generation", c)
	}
	testAwaitDurableCert(t, s.e, b)
}

// P-none: a connection that never attempted an acquisition persists nothing, because it can have
// left no token for a certificate to clear.
func TestF105CertificateOfANeverAcquiringConnectionIsNotPersisted(t *testing.T) {
	c, _, b := testFrameClient(t, 0)
	mqttCertifyClosed(c, b, "refused")
	if b.closeCert() == nil {
		t.Fatal("the connection was not certified in process")
	}
	if testPersistLaunched(b) {
		t.Fatal("a connection that never attempted an acquisition launched a persistence")
	}
}

// P-late: a connection already certified when it registers an acquisition persists then.
func TestF105LateRegistrationOnACertifiedConnectionPersists(t *testing.T) {
	e := testNewInflightEnv(t, "cert-late")
	c, _, b := testFrameClient(t, 0)
	got := testObserveCertPersist(t, b, false)
	mqttCertifyClosed(c, b, "closed first")
	if testPersistLaunched(b) {
		t.Fatal("nothing was registered yet, so nothing may persist")
	}
	if err := b.registerAcquisition(e.asm.jsa.newCertPersistCtx(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen)); err != nil {
		t.Fatalf("registering: %v", err)
	}
	testAwaitCertPersist(t, got, "late registration")
}

// One generation per connection: a second context naming another generation is refused.
func TestF105AcquisitionRegistersOneGenerationPerConnection(t *testing.T) {
	e := testNewInflightEnv(t, "cert-onegen")
	_, _, b := testFrameClient(t, 0)
	if err := b.registerAcquisition(e.asm.jsa.newCertPersistCtx(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen)); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := b.registerAcquisition(e.asm.jsa.newCertPersistCtx(e.sess.id, e.asm.sessStreamEpoch, "othergen")); err == nil {
		t.Fatal("a connection registered acquisitions under two generations")
	}
}

// R1a: the TORN WRITER, with no later helper, persists its own certificate.
func TestF105TornWriterPersistsItsCertificate(t *testing.T) {
	e := testNewInflightEnv(t, "cert-torn")
	c, _, b := testFrameClient(t, 3)
	if err := b.registerAcquisition(e.asm.jsa.newCertPersistCtx(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen)); err != nil {
		t.Fatalf("registering: %v", err)
	}
	got := testObserveCertPersist(t, b, false)
	act := testFrameAction(b, 1)
	if res, _ := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
		Action: act, Binding: b, Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
	}); res != mqttOutputFailed {
		t.Fatalf("the fixture did not tear the frame: %v", res)
	}
	testAwaitCertPersist(t, got, "the torn writer")
}

// R1b: an already-SEALED, uncertified binding is certified and persisted by the external
// certifier that completes it.
func TestF105ExternalCertifierPersistsASealedBinding(t *testing.T) {
	e := testNewInflightEnv(t, "cert-sealed")
	c, _, b := testFrameClient(t, 0)
	if err := b.registerAcquisition(e.asm.jsa.newCertPersistCtx(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen)); err != nil {
		t.Fatalf("registering: %v", err)
	}
	got := testObserveCertPersist(t, b, false)
	sealOutput(b)
	if b.closeCert() != nil || testPersistLaunched(b) {
		t.Fatal("a bare seal recorded or persisted a certificate")
	}
	mqttCertifyClosed(c, b, "teardown after a seal")
	testAwaitCertPersist(t, got, "the external certifier")
}

// P-elig: eligibility is registered BEFORE the acquisition's batch is sent, so an acquisition
// whose answer never comes back still has its connection's certificate persisted. Certified from
// INSIDE the commit, before the batch is sent.
func TestF105AcquisitionRegistersItsCertificateBeforeItsBatch(t *testing.T) {
	s := testRow2OpenSection(t, "cert-elig", 1)
	b := s.e.c.outBind
	got := testObserveCertPersist(t, b, false)
	var once sync.Once
	launchedAtSend := false
	testSetTransitionCommit(t, s.e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind != mqttTransitionAcquireAction {
			return send()
		}
		once.Do(func() {
			completeCertification(b, "certified while the acquisition is in flight")
			launchedAtSend = testPersistLaunched(b)
		})
		return 0, errors.New("test: the acquisition's answer is lost")
	})
	_ = s.e.sess.acquirePublishAction(s.e.c, s.pi, &s.tok)
	if !launchedAtSend {
		t.Fatal("the connection had no certificate context while its acquisition was in flight")
	}
	testAwaitCertPersist(t, got, "the in-flight acquisition")
}

// --- The clearance ----------------------------------------------------------------------------

// Rc: a successor's resume clears the token a certified connection left standing, at the
// recovery boundary — before any acquisition could run — and writes nothing but the end of
// custody: not the phase, not the counter, not the unsent marker (Rc-unsent).
func TestF105ResumeClearsACertifiedConnectionsToken(t *testing.T) {
	s, pre, _ := testCertifiedStandingToken(t, "cert-rc", 1, true)
	var atWalk *mqttInflightRecord
	reached := false
	testResumeOnPeer(t, s, "cert-rc", func(asm *mqttAccountSessionManager) {
		reached = true
		atWalk, _, _ = asm.jsa.loadInflightRecord(s.subject)
	})
	if !reached {
		t.Fatal("the resume never reached its register walk")
	}
	if atWalk == nil {
		t.Fatal("the register was gone before the walk began")
	}
	if want := testClearedBody(pre); *atWalk != want {
		t.Fatalf("as the walk begins the register is\n%+v\nwant exactly the cleared\n%+v", *atWalk, want)
	}
	if st, ok := s.state(pre.Acq); ok {
		t.Fatalf("the clearance touched the frame guard of the acquisition (%v): it is not an abandonment", st)
	}
}

// Rc-absent: with no durable certificate, the token stands.
func TestF105ResumeWithoutACertificateLeavesTheToken(t *testing.T) {
	s, pre, _ := testCertifiedStandingToken(t, "cert-absent", 1, false)
	testResumeOnPeer(t, s, "cert-absent", nil)
	cur, _, err := s.e.asm.loadInflightRecord(s.subject)
	if err != nil || cur == nil || cur.ActionToken != pre.ActionToken {
		t.Fatalf("the token was cleared on no certificate: %+v (%v)", cur, err)
	}
}

// Rc-wrong: a certificate of ANOTHER connection of the same run and generation licenses nothing.
func TestF105ResumeIgnoresAnotherConnectionsCertificate(t *testing.T) {
	s, pre, _ := testCertifiedStandingToken(t, "cert-wrong", 1, false)
	b := s.e.c.outBind
	other := &mqttDurableCloseCert{ID: s.e.sess.id, Epoch: s.e.asm.sessStreamEpoch, Gen: s.e.sess.gen,
		Run: b.run, Conn: b.conn + "9"}
	if err := s.e.asm.jsa.publishDurableCloseCert(other); err != nil {
		t.Fatalf("planting another connection's certificate: %v", err)
	}
	testResumeOnPeer(t, s, "cert-wrong", nil)
	cur, _, err := s.e.asm.loadInflightRecord(s.subject)
	if err != nil || cur == nil || cur.ActionToken != pre.ActionToken {
		t.Fatalf("another connection's certificate cleared this token: %+v (%v)", cur, err)
	}
}

// Rc-absent (b): a body at the right subject naming another generation is unusable, not a licence.
func TestF105ResumeRefusesAForeignCertificateBody(t *testing.T) {
	s, pre, _ := testCertifiedStandingToken(t, "cert-foreign", 1, false)
	b := s.e.c.outBind
	if _, err := s.e.asm.jsa.storeMsg(mqttCloseCertSubject(s.e.sess.idHash, s.e.sess.gen, b.run, b.conn),
		0, []byte(`{"id":"`+s.e.sess.id+`","epoch":"`+s.e.asm.sessStreamEpoch+`","gen":"othergen","run":"`+
			b.run+`","conn":"`+b.conn+`"}`)); err != nil {
		t.Fatalf("planting a foreign body: %v", err)
	}
	testResumeOnPeer(t, s, "cert-foreign", nil)
	cur, _, err := s.e.asm.loadInflightRecord(s.subject)
	if err != nil || cur == nil || cur.ActionToken != pre.ActionToken {
		t.Fatalf("a foreign certificate body cleared this token: %+v (%v)", cur, err)
	}
}

// Rc-stale: a register that moves between the licence and the write is never rebased onto. The
// first submission carries the ORIGINAL sequence and is rejected; the clearance then re-reads and
// proves its licence again before the submission that lands.
func TestF105CertifiedClearanceNeverRebases(t *testing.T) {
	s, pre, preSeq := testCertifiedStandingToken(t, "cert-stale", 1, true)
	var seqs []uint64
	var mu sync.Mutex
	moved := false
	s.e.asm.mu.Lock()
	s.e.asm.testCertifiedClearSubmit = func(subject string, seq uint64, cur *mqttInflightRecord) {
		mu.Lock()
		defer mu.Unlock()
		seqs = append(seqs, seq)
		if !moved {
			moved = true
			// A same-token rewrite moves the sequence and keeps every field: exactly the move a
			// rebasing writer would silently adopt.
			if _, err := s.e.asm.jsa.storeInflightCAS(subject, seq, cur); err != nil {
				t.Errorf("moving the register: %v", err)
			}
		}
	}
	s.e.asm.mu.Unlock()
	ok, _, _, _ := s.e.sess.clearCertifiedRegister(nil, s.subject)
	if !ok {
		t.Fatal("the clearance did not land after re-proving its licence")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 2 || seqs[0] != preSeq || seqs[1] == preSeq {
		t.Fatalf("submissions expected sequences %v: want the original %d rejected, then a fresh one", seqs, preSeq)
	}
	cur, _, _ := s.e.asm.loadInflightRecord(s.subject)
	if cur == nil || *cur != testClearedBody(pre) {
		t.Fatalf("the landed clearance is %+v", cur)
	}
}

// Rc-revisit: an acquisition that meets a token a durably certified connection left standing
// clears it under the same licence and acquires. The certificate is persisted only AFTER the
// token is met once with none standing, which is the successor that came first.
//
// The fixture reconnects the same session on a new connection and leaves the first section's
// in-process state behind (its hold and its open section), which is what the holder's process
// no longer has after it is gone: only the durable token and the certificate decide.
func TestF105AcquisitionClearsACertifiedConnectionsToken(t *testing.T) {
	s, pre, _ := testCertifiedStandingToken(t, "cert-revisit", 1, false)
	e := s.e
	c3 := e.s.createInternalAccountClient()
	testConnIdentity(t, e, c3)
	c3.outBind = mqttBindConnection(c3.cid)
	tc := &testFrameConn{}
	c3.nc, c3.rawConn = tc, tc
	c3.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	c3.setOutboundEntitlement(mqttOutboundEntitlement{Gen: e.sess.gen, Tenure: s.tok.out.action.Tenure})
	open := func() mqttSendToken {
		t.Helper()
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		e.sess.c = c3
		if ack := e.sess.pendingPublish[s.pi]; ack != nil {
			ack.sendNonce = _EMPTY_
			ack.sendSettle = mqttSendSettle{}
		}
		delete(e.sess.sendsInFlight, s.pi)
		tok, ok := e.sess.beginSendLocked(c3, s.pi, testRealPubRelSrcDur, testRealPubRelSrcSeq)
		if !ok {
			t.Fatal("the reconnected session could not open a send section for the delivery")
		}
		return tok
	}
	tok := open()
	if e.sess.acquirePublishAction(c3, s.pi, &tok) {
		t.Fatal("an acquisition took a slot whose token stands with no certificate")
	}
	b := s.e.c.outBind
	if err := e.asm.jsa.publishDurableCloseCert(&mqttDurableCloseCert{ID: e.sess.id,
		Epoch: e.asm.sessStreamEpoch, Gen: e.sess.gen, Run: b.run, Conn: b.conn}); err != nil {
		t.Fatalf("persisting the certificate late: %v", err)
	}
	tok = open()
	if !e.sess.acquirePublishAction(c3, s.pi, &tok) {
		t.Fatal("the acquisition did not clear the certified token and acquire")
	}
	cur, _, _ := e.asm.loadInflightRecord(s.subject)
	if cur == nil || cur.ActionToken == _EMPTY_ || cur.ActionToken == pre.ActionToken || cur.Acq != pre.Acq+1 {
		t.Fatalf("the new acquisition is not the one standing: %+v", cur)
	}
}

// Rc-resident: the resident reference follows the clearance only from the exact pre-clearance
// sequence; an entry another writer already moved keeps its fact.
func TestF105ClearanceWriteBackIsConditionedOnTheExactSequence(t *testing.T) {
	s := testRow2OpenSection(t, "cert-resident", 1)
	pre := &mqttInflightRecord{PI: s.pi, Nonce: "nonce-1", ActionToken: "tok-1"}
	mk := func(seq uint64) *mqttPending {
		p := &mqttPending{regSeq: seq, regNonce: "nonce-1"}
		p.slot.nonce, p.slot.seq, p.slot.actionToken, p.slot.actionLive = "nonce-1", seq, "tok-1", true
		return p
	}
	s.e.sess.mu.Lock()
	defer s.e.sess.mu.Unlock()
	s.e.sess.pendingPublish[s.pi] = mk(10)
	s.e.sess.advanceResidentAfterClearanceLocked(pre, 10, 12)
	if p := s.e.sess.pendingPublish[s.pi]; p.regSeq != 12 || p.slot.seq != 12 || p.slot.actionLive || p.slot.actionToken != _EMPTY_ {
		t.Fatalf("an entry at the pre-clearance sequence did not follow the clearance: %+v", p)
	}
	s.e.sess.pendingPublish[s.pi] = mk(13)
	s.e.sess.advanceResidentAfterClearanceLocked(pre, 10, 12)
	if p := s.e.sess.pendingPublish[s.pi]; p.regSeq != 13 || p.slot.seq != 13 || !p.slot.actionLive {
		t.Fatalf("an entry another writer moved was re-aimed at the clearance: %+v", p)
	}
}

// Rc-resident (lost response): a clearance whose answer is lost is recognized by the identical
// body it left, and adopted at the sequence it landed at.
func TestF105CertifiedClearanceAdoptsALostResponse(t *testing.T) {
	s, pre, preSeq := testCertifiedStandingToken(t, "cert-lost", 1, true)
	s.e.asm.mu.Lock()
	s.e.asm.testInflightStore = func(subject string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		seq, err := s.e.asm.jsa.storeInflightCAS(subject, expect, r)
		if err == nil && subject == s.subject && r.ActionToken == _EMPTY_ {
			return 0, errors.New("test: the clearance landed and its answer was lost")
		}
		return seq, err
	}
	s.e.asm.mu.Unlock()
	ok, _, gotPre, landed := s.e.sess.clearCertifiedRegister(nil, s.subject)
	if !ok || gotPre != preSeq {
		t.Fatalf("a landed clearance whose answer was lost was not adopted: ok=%v pre=%d", ok, gotPre)
	}
	_, smsg, _ := s.e.asm.loadInflightRecord(s.subject)
	if smsg == nil || landed != smsg.Sequence {
		t.Fatalf("adopted at %d, the register stands at %v", landed, smsg)
	}
	_ = pre
}

// The kind table: a PUBLISH token survives a successful PUBREC into either past-PUBREC phase; a
// PUBREL token stands in pubrel_ready and pubrel_terminal; nothing else.
func TestF105CertifiedClearanceKindTable(t *testing.T) {
	for _, tc := range []struct {
		kind mqttActionKind
		st   mqttInflightState
		ok   bool
	}{
		{mqttActionPublish, mqttInflightPublish, true},
		{mqttActionPublish, mqttInflightPubRelIntended, true},
		{mqttActionPublish, mqttInflightPubRelReady, true},
		{mqttActionPublish, mqttInflightPubRelTerminal, false},
		{mqttActionPublish, mqttInflightPublishTerminal, false},
		{mqttActionPubRel, mqttInflightPubRelReady, true},
		{mqttActionPubRel, mqttInflightPubRelTerminal, true},
		{mqttActionPubRel, mqttInflightPublish, false},
		{mqttActionPubRel, mqttInflightPubRelIntended, false},
	} {
		if got := mqttActionKindSurvivesInto(tc.kind, tc.st); got != tc.ok {
			t.Errorf("%s token in %s: survives=%v, want %v", tc.kind, tc.st, got, tc.ok)
		}
	}
}

// testAtWalk resumes on a peer and returns what the given subjects hold as its walk begins.
func testAtWalk(t *testing.T, s *testRow2PubSection, clientID string, subjects ...string) []*mqttInflightRecord {
	t.Helper()
	var at []*mqttInflightRecord
	reached := false
	testResumeOnPeer(t, s, clientID, func(asm *mqttAccountSessionManager) {
		reached = true
		for _, subj := range subjects {
			r, _, _ := asm.jsa.loadInflightRecord(subj)
			at = append(at, r)
		}
	})
	if !reached {
		t.Fatal("the resume never reached its register walk")
	}
	return at
}

// Rc-qos2: a PUBLISH token survives a successful PUBREC into pubrel_intended, and the certificate
// of its connection still clears it there.
func TestF105ResumeClearsAPublishTokenThatSurvivedPubRec(t *testing.T) {
	s, pre, _ := testCertifiedStandingTokenAfter(t, "cert-qos2", 2, true, func(s *testRow2PubSection) {
		held, hsmsg, err := s.e.asm.loadInflightRecord(s.subject)
		if err != nil || held == nil || held.ActionToken == _EMPTY_ {
			t.Fatalf("the fixture needs the live token before the PUBREC: %+v (%v)", held, err)
		}
		if _, err := s.e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: s.e.sess.id, Epoch: s.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-test-pubrec",
			Gen: s.e.sess.gen, Nonce: held.Lin, To: mqttExchangePubRecObserved, Acq: held.Acq,
			Register: held, RegisterSeq: hsmsg.Sequence,
		}); err != nil {
			t.Fatalf("the PUBREC over the live token was refused: %v", err)
		}
	})
	if pre.State != mqttInflightPubRelIntended {
		t.Fatalf("the fixture's token must stand in pubrel_intended, not %s", pre.State)
	}
	at := testAtWalk(t, s, "cert-qos2", s.subject)
	if at[0] == nil || *at[0] != testClearedBody(pre) {
		t.Fatalf("as the walk begins the past-PUBREC register is %+v, want exactly the cleared %+v",
			at[0], testClearedBody(pre))
	}
}

// Rc-many: every certified register of the generation is cleared by one resume.
func TestF105ResumeClearsEveryCertifiedToken(t *testing.T) {
	s, pre, _ := testCertifiedStandingTokenAfter(t, "cert-many", 1, true, func(s *testRow2PubSection) {
		for i := uint64(1); i <= 3; i++ {
			pi := s.e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq+i, 1)
			s.e.sess.mu.Lock()
			tok, ok := s.e.sess.beginSendLocked(s.e.c, pi, testRealPubRelSrcDur, testRealPubRelSrcSeq+i)
			s.e.sess.mu.Unlock()
			if !ok || !s.e.sess.acquirePublishAction(s.e.c, pi, &tok) {
				t.Fatalf("acquiring the extra delivery %d failed", i)
			}
		}
	})
	subjects, err := s.e.asm.enumerateInflightSubjects(mqttInflightWalkFilter(s.e.sess.idHash, s.e.sess.gen))
	if err != nil || len(subjects) != 4 {
		t.Fatalf("the fixture needs four registers, has %d (%v)", len(subjects), err)
	}
	var subs []string
	for subj := range subjects {
		r, _, _ := s.e.asm.loadInflightRecord(subj)
		if r == nil || r.ActionToken == _EMPTY_ {
			t.Fatalf("register %s carries no standing token: %+v", subj, r)
		}
		subs = append(subs, subj)
	}
	for i, r := range testAtWalk(t, s, "cert-many", subs...) {
		if r == nil || r.ActionToken != _EMPTY_ {
			t.Fatalf("register %s still carries a token as the walk begins: %+v", subs[i], r)
		}
	}
	_ = pre
}

// Rc-pubrel: in pubrel_terminal the token is the only thing keeping its exchange pending-drain,
// so the clearance and the terminal advance land as one.
func TestF105ResumeClearsAPendingDrainPubRelAtomically(t *testing.T) {
	e := testNewInflightEnv(t, "cert-pubrel")
	testReadyForRealPubRelDelivery(t, e)
	testStampAcceptedConnection(t, e)
	seed := testSeedRealPubRelReady(t, e)
	_, _ = testRow2AcquiredPubRel(t, e, seed)
	held, hsmsg, err := e.asm.loadInflightRecord(seed.subject)
	if err != nil || held == nil || held.ActionToken == _EMPTY_ || held.State != mqttInflightPubRelReady {
		t.Fatalf("the fixture needs a live PUBREL token in pubrel_ready: %+v (%v)", held, err)
	}
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-test-pubcomp",
		Gen: e.sess.gen, Nonce: held.Lin, To: mqttExchangeTerminalPendingDrain, Acq: held.Acq,
		Register: held, RegisterSeq: hsmsg.Sequence,
	}); err != nil {
		t.Fatalf("the PUBCOMP under the live PUBREL token was refused: %v", err)
	}
	pre, _, _ := e.asm.loadInflightRecord(seed.subject)
	if pre == nil || pre.State != mqttInflightPubRelTerminal || pre.ActionToken == _EMPTY_ {
		t.Fatalf("the fixture needs pubrel_terminal under a live token: %+v", pre)
	}
	b := e.c.outBind
	got := testObserveCertPersist(t, b, true)
	mqttCertifyClosed(e.c, b, "crash cut: the PUBREL holder never drains")
	testAwaitCertPersist(t, got, "certifying the PUBREL holder's connection")
	testAwaitDurableCert(t, e, b)

	asm2, sess2, c2 := e.peer(t, "node2", "cert-pubrel")
	c2.outBind = mqttBindConnection(c2.cid + 1000)
	sess2.mu.Lock()
	sess2.c = c2
	sess2.mu.Unlock()
	var atWalk *mqttInflightRecord
	var phase mqttExchangePhase
	asm2.mu.Lock()
	asm2.testResumeWalkEntry = func(string) {
		atWalk, _, _ = asm2.jsa.loadInflightRecord(seed.subject)
		if x, _, xerr := asm2.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, pre.Lin); xerr == nil && x != nil {
			phase = x.Phase
		}
	}
	asm2.mu.Unlock()
	if _, err := sess2.recoverInflightRegisters(c2); err != nil {
		t.Fatalf("the successor's resume failed: %v", err)
	}
	if atWalk == nil || *atWalk != testClearedBody(pre) {
		t.Fatalf("as the walk begins the pubrel_terminal register is %+v, want exactly the cleared %+v",
			atWalk, testClearedBody(pre))
	}
	if phase != mqttExchangeTerminal {
		t.Fatalf("the exchange stands at phase %d beside the cleared token, want terminal", phase)
	}
}

// testClearedBody is the one body a certified clearance may leave, stated independently of the
// production helper so a mutation of that helper cannot move the expectation with it: the
// register as read with its live custody ended, and every other field — the phase, the counter,
// the unsent and cancellation facts, the custody stamp — exactly as it was.
func testClearedBody(pre *mqttInflightRecord) mqttInflightRecord {
	want := *pre
	want.ActionToken, want.ActionClaim, want.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	return want
}

// Rc-revisit (PUBREL): a PUBREL acquisition that meets a token a durably certified connection
// left standing clears it and acquires — the retained replay's PUBREL emission reaches the
// acquisition with no token check of its own.
func TestF105PubRelAcquisitionClearsACertifiedConnectionsToken(t *testing.T) {
	e := testNewInflightEnv(t, "cert-pubrel-revisit")
	testReadyForRealPubRelDelivery(t, e)
	testStampAcceptedConnection(t, e)
	seed := testSeedRealPubRelReady(t, e)
	_, _ = testRow2AcquiredPubRel(t, e, seed)
	pre, _, _ := e.asm.loadInflightRecord(seed.subject)
	if pre == nil || pre.ActionToken == _EMPTY_ {
		t.Fatalf("the fixture needs a standing PUBREL token: %+v", pre)
	}
	b := e.c.outBind
	got := testObserveCertPersist(t, b, false)
	mqttCertifyClosed(e.c, b, "crash cut: the PUBREL holder never drains")
	testAwaitCertPersist(t, got, "certifying the PUBREL holder's connection")

	c3 := e.s.createInternalAccountClient()
	testConnIdentity(t, e, c3)
	c3.outBind = mqttBindConnection(c3.cid)
	c3.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	ent, _ := e.c.outboundEntitlement()
	c3.setOutboundEntitlement(ent)
	e.sess.mu.Lock()
	e.sess.c = c3
	e.sess.mu.Unlock()
	// The holder's in-process state is gone with its process: only the durable token and the
	// certificate decide.
	forgetHolder := func() {
		e.sess.mu.Lock()
		delete(e.sess.sendsInFlight, seed.pi)
		e.sess.mu.Unlock()
	}
	forgetHolder()
	ref, cur, _ := testRow2Ref(t, e, seed.subject)
	if act, _ := e.sess.mqttAcquirePubRelAction(c3, ref, cur.Lin, 0); act != nil {
		t.Fatal("a PUBREL acquisition took a slot whose token stands with no certificate")
	}
	if err := e.asm.jsa.publishDurableCloseCert(&mqttDurableCloseCert{ID: e.sess.id,
		Epoch: e.asm.sessStreamEpoch, Gen: e.sess.gen, Run: b.run, Conn: b.conn}); err != nil {
		t.Fatalf("persisting the certificate late: %v", err)
	}
	forgetHolder()
	ref, cur, _ = testRow2Ref(t, e, seed.subject)
	act, _ := e.sess.mqttAcquirePubRelAction(c3, ref, cur.Lin, 0)
	if act == nil {
		t.Fatal("the PUBREL acquisition did not clear the certified token and acquire")
	}
	now, _, _ := e.asm.loadInflightRecord(seed.subject)
	if now == nil || now.ActionToken == _EMPTY_ || now.ActionToken == pre.ActionToken || now.Acq != pre.Acq+1 {
		t.Fatalf("the new PUBREL acquisition is not the one standing: %+v", now)
	}
	e.sess.startActionDrain(c3, act)
}

// H-1: a connection number is unique only within one server, so two servers in one process —
// or a replacement server — reuse it. A certificate persisted for an EARLIER connection that
// happened to carry the same number must not end the token of a live connection carrying it now.
func TestF105CertificateOfAnEqualNumberedConnectionLicensesNothing(t *testing.T) {
	s := testRow2OpenSection(t, "cert-samecid", 1)
	s.acquire(t)
	pre, _, _ := s.e.asm.loadInflightRecord(s.subject)
	if pre == nil || pre.ActionToken == _EMPTY_ {
		t.Fatalf("the fixture needs a standing token: %+v", pre)
	}
	earlier := mqttBindConnection(s.e.c.cid)
	if earlier.run != s.e.c.outBind.run {
		t.Fatalf("the fixture needs the same run: %s vs %s", earlier.run, s.e.c.outBind.run)
	}
	if err := s.e.asm.jsa.publishDurableCloseCert(&mqttDurableCloseCert{ID: s.e.sess.id,
		Epoch: s.e.asm.sessStreamEpoch, Gen: s.e.sess.gen, Run: earlier.run, Conn: earlier.conn}); err != nil {
		t.Fatalf("persisting the earlier connection's certificate: %v", err)
	}
	at := testAtWalk(t, s, "cert-samecid", s.subject)
	if at[0] == nil || at[0].ActionToken != pre.ActionToken {
		t.Fatalf("an equal-numbered connection's certificate cleared a live connection's token: %+v", at[0])
	}
}
