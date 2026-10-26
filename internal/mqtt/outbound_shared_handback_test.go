// Copyright 2026 The mqtt-nats-broker Authors
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

//go:build mqtt_fulltests

package mqtt

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// A shared delivery abandoned on an ACL deny is HANDED BACK to its group and never positively
// acknowledged by this member (7a; consults 82-84). Every test starts from a shared QoS 1 delivery
// admitted through the authority and TRANSMITTED (its unsent marker deleted), with the restored
// entry and source mapping standing — the population the deny guard meets on a resume.

// testSourceAckLog records what the positive source-ACK gate submitted for one durable.
type testSourceAckLog struct {
	mu       sync.Mutex
	dur      string
	positive []string
	nak      []string
}

func (l *testSourceAckLog) record(reply string, positive bool) {
	if !strings.Contains(reply, "."+l.dur+".") {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if positive {
		l.positive = append(l.positive, reply)
	} else {
		l.nak = append(l.nak, reply)
	}
}

func (l *testSourceAckLog) counts() (positive, nak int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.positive), len(l.nak)
}

// testObserveSourceAcks installs the gate observer for one durable. The observer is process-wide,
// so a test using it is not parallel with another that installs one; the durable filter keeps
// other tests' submissions out of the log.
func testObserveSourceAcks(t *testing.T, dur string) *testSourceAckLog {
	t.Helper()
	l := &testSourceAckLog{dur: dur}
	fn := l.record
	if !mqttTestSourceAckObserver.CompareAndSwap(nil, &fn) {
		t.Fatal("fixture error: another source-ACK observer is installed")
	}
	t.Cleanup(func() { mqttTestSourceAckObserver.Store(nil) })
	return l
}

type testSharedHandBackFix struct {
	*testSharedAdmitRig
	pi      uint16
	subject string
	reply   string
	lin     string
}

// exchange reads the fixture's exchange by its lineage, independent of whether its register
// still stands.
func (f *testSharedHandBackFix) exchange(t *testing.T) *mqttOutboundExchange {
	t.Helper()
	x, _, err := f.e.sess.jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.e.sess.gen, f.lin)
	if err != nil {
		t.Fatalf("loading the exchange: %v", err)
	}
	return x
}

func testSharedTransmitted(t *testing.T, clientID string) *testSharedHandBackFix {
	t.Helper()
	r := newTestSharedAdmitRig(t, clientID)
	pi, ok := r.admit(t, r.dep())
	if !ok {
		t.Fatal("fixture: the shared delivery was not admitted")
	}
	e := r.e
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	ml := e.asm.jsa.loadOccupantMarkerLoad(subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the marker (the transmission's intent delete): %v", err)
	}
	e.sess.mu.Lock()
	e.sess.c = e.c
	ent := e.sess.pendingPublish[pi]
	mapped := e.sess.cpending[r.dur][r.sseq] == pi
	e.sess.mu.Unlock()
	if ent == nil || !mapped {
		t.Fatalf("fixture: the restored entry and mapping must stand: %+v mapped=%v", ent, mapped)
	}
	testCustodyRM1(t, e.sess)
	// The record a resume restores from: the generation and the shared membership persist.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	reg, _ := r.exchangeOf(t, pi)
	return &testSharedHandBackFix{testSharedAdmitRig: r, pi: pi, subject: subject,
		reply: testInflightAckSubject(r.dur, r.sseq, 2), lin: reg.Lin}
}

func (f *testSharedHandBackFix) held(sess *mqttSession) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.sourceHandBackHeldLocked(f.dur, f.sseq)
}

func (f *testSharedHandBackFix) reoffer(t *testing.T, qos byte) {
	t.Helper()
	hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
	rmsg := append(append([]byte(nil), hdr...), "handed back"...)
	pc := &client{kind: JETSTREAM}
	pc.pa.hdr = len(hdr)
	sub := &subscription{client: f.e.c, queue: []byte("q"), mqtt: &mqttSub{qos: qos, jsDur: f.dur, shared: true}}
	mqttDeliverMsgCbQoS12(sub, pc, nil, mqttStreamSubjectPrefix+"foo.bar", f.reply, rmsg)
}

// T7. The deny abandons durably through the ordinary disposal — the durable fact, the charge
// released, the identifier kept — and hands the group's message BACK: one paced NAK, never a
// positive acknowledgement, no source retirement. A re-offer to this member while the HandBack
// stands is handed back too and admits nothing. A late PUBACK is still routed by the retained
// binding and ends the exchange Terminal; the cleanup that follows deletes the register against
// that ending alone (the typed HandBack proof) and releases the binding and the identifier — and
// still never acknowledges the group source. Control: an ordinary deny acknowledges.
//
// Mutations: M7b (no binding for shared -> the late PUBACK advances nothing), M15/M16 (the gate's
// predicate -> a positive ACK is observed), M23 (the deletion gate refuses the typed proof -> the
// register and binding never go), M9 (shared stays slot-only -> no durable abandonment).
func TestSharedDenyNeverAcksTheGroupSource(t *testing.T) {
	f := testSharedTransmitted(t, "shared-handback-t7")
	sess := f.e.sess
	log := testObserveSourceAcks(t, f.dur)

	if r := sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply); r != mqttAbandonDone {
		t.Fatalf("the shared deny established %v, want the durable abandonment", r)
	}
	_, x := f.exchangeOf(t, f.pi)
	if x == nil || x.Phase != mqttExchangeDeliveryAbandoned || x.Abandoned == nil || x.SharedAcq == nil {
		t.Fatalf("the exchange after the deny: %+v", x)
	}
	if !f.held(sess) {
		t.Fatal("no HandBack is held for the abandoned shared source")
	}
	// NO RETIREMENT WAS WRITTEN: retireSourceLocked is the suppression that acknowledges every
	// re-offer away. (sourceRetiredLocked also answers true for a held binding by design; what it
	// then does with the offer is the gate's, asserted below.)
	sess.mu.Lock()
	_, retired := sess.srcAckOwed[mqttDeferredKey{f.dur, f.sseq}]
	sess.mu.Unlock()
	if retired {
		t.Fatal("the shared source was RETIRED: retirement acknowledges every re-offer away")
	}
	if pos, nak := log.counts(); pos != 0 || nak != 1 {
		t.Fatalf("the deny submitted %d positive acknowledgement(s) and %d hand-back(s), want 0 and 1", pos, nak)
	}
	testCustodyEventually(t, "the abandoned delivery's charge released", func() bool {
		return testCustodyCapacity(sess) == 1
	})
	if got := testCustodyMints(sess, f.pi); got == f.pi {
		t.Fatalf("the abandoned delivery's identifier %d was handed out", f.pi)
	}

	// A RE-OFFER while the HandBack stands: handed back before any tracking.
	sess.mu.Lock()
	before, lastPI := len(sess.pendingPublish), sess.last_pi
	sess.mu.Unlock()
	f.reoffer(t, 1)
	sess.mu.Lock()
	after, afterPI := len(sess.pendingPublish), sess.last_pi
	sess.mu.Unlock()
	if after != before || afterPI != lastPI {
		t.Fatalf("the handed-back source was re-admitted: entries %d->%d, last_pi %d->%d", before, after, lastPI, afterPI)
	}
	if pos, nak := log.counts(); pos != 0 || nak != 2 {
		t.Fatalf("the re-offer: %d positive, %d hand-back(s), want 0 and 2", pos, nak)
	}

	// THE LATE PUBACK, routed by the retained binding.
	if err := f.e.c.mqttProcessPublishReceived(f.pi, false, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "the abandoned exchange ended Terminal with its fact", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal && x.Abandoned != nil
	})
	testCustodyEventually(t, "the register deleted and the HandBack released", func() bool {
		reg, _ := f.exchangeOf(t, f.pi)
		return reg == nil && !f.held(sess)
	})
	testCustodyEventually(t, "the identifier released after the cleanup", func() bool {
		return testCustodyMints(sess, f.pi) == f.pi
	})
	if pos, _ := log.counts(); pos != 0 {
		t.Fatalf("the group source was positively acknowledged %d time(s) after its hand-back", pos)
	}

	// CONTROL: an ordinary deny still acknowledges its own source.
	g := testAbandonAdmitted(t, "shared-handback-t7-control", 1)
	olog := &testSourceAckLog{dur: testRealPubRelSrcDur}
	mqttTestSourceAckObserver.Store(nil)
	ofn := olog.record
	mqttTestSourceAckObserver.Store(&ofn)
	g.e.asm.mu.Lock()
	g.e.asm.testInflightSourceAck = func(string) error { return nil }
	g.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return mqttInflightSourceLive }
	g.e.asm.mu.Unlock()
	g.e.sess.mu.Lock()
	g.e.sess.c = g.e.c
	g.e.sess.mu.Unlock()
	if r := g.deny(); r != mqttAbandonDone {
		t.Fatalf("control deny: %v", r)
	}
	testCustodyEventually(t, "control: the ordinary abandoned source acknowledged", func() bool {
		pos, _ := olog.counts()
		return pos > 0
	})
}

// T7 (restart leg): the HandBack survives a restart. A fresh session object reconstructs the
// binding from the durable abandonment fact, so a re-offer after recovery is still handed back.
func TestSharedHandBackSurvivesRestart(t *testing.T) {
	f := testSharedTransmitted(t, "shared-handback-restart")
	log := testObserveSourceAcks(t, f.dur)
	if r := f.e.sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	restored, _, err := f.e.asm.createOrRestoreSession(f.e.sess.id, f.e.s.getOpts())
	if err != nil || restored == f.e.sess {
		t.Fatalf("createOrRestoreSession must build a FRESH object: %v", err)
	}
	if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if !f.held(restored) {
		restored.mu.Lock()
		t.Logf("restored: gen=%s bindings=%d disposals=%d pending=%v", restored.gen, len(restored.abandonedAcks), len(restored.disposals), restored.pendingPublish)
		restored.mu.Unlock()
		reg, x := f.exchangeOf(t, f.pi)
		t.Logf("register=%+v exchange=%+v", reg, x)
		t.Fatal("the restored session holds no HandBack for the abandoned shared source")
	}
	restored.mu.Lock()
	ok := restored.submitSourceAckLocked(f.reply, nil)
	restored.mu.Unlock()
	if ok != mqttSourceAckHandedBack {
		t.Fatalf("a positive acknowledgement after the restart returned %v, want a hand-back", ok)
	}
	if pos, _ := log.counts(); pos != 0 {
		t.Fatalf("%d positive acknowledgement(s) of the handed-back source", pos)
	}
}

// T18. The binding lifetime: a Terminal exchange whose register still stands keeps the HandBack
// (custody observation of the ending alone must not remove it), and so does a crash in that
// window — the restored object reconstructs it from the Terminal exchange's fact.
//
// Mutations: M17a (removal on ending alone), M18 (no reconstruction for a Terminal exchange).
func TestAbandonedAckBindingLifetime(t *testing.T) {
	f := testSharedTransmitted(t, "shared-binding-lifetime")
	sess := f.e.sess
	log := testObserveSourceAcks(t, f.dur)
	if r := sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	// Every register delete fails: the Terminal exchange's register stands.
	f.e.asm.mu.Lock()
	f.e.asm.testInflightDelete = func(uint64) error { return errors.New("test: the delete is refused") }
	f.e.asm.mu.Unlock()
	if err := f.e.c.mqttProcessPublishReceived(f.pi, false, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "the abandoned exchange ended Terminal", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	// The custody pass observes the ending; the binding must survive it.
	testCustodyKick(sess)
	testCustodyEventually(t, "the ending observed (charge accounting settled)", func() bool {
		return testCustodyCapacity(sess) == 1
	})
	if reg, _ := f.exchangeOf(t, f.pi); reg == nil {
		t.Fatal("fixture error: the register was deleted though every delete fails")
	}
	if !f.held(sess) {
		t.Fatal("the HandBack was released on the ending alone, while the register still stands")
	}
	// The crash in that window.
	restored, _, err := f.e.asm.createOrRestoreSession(sess.id, f.e.s.getOpts())
	if err != nil || restored == sess {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	restored.asm.mu.Lock()
	restored.asm.testInflightDelete = func(uint64) error { return errors.New("test: the delete is refused") }
	restored.asm.mu.Unlock()
	if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if !f.held(restored) {
		t.Fatal("the restored session did not reconstruct the HandBack of a Terminal abandoned exchange")
	}
	if pos, _ := log.counts(); pos != 0 {
		t.Fatalf("%d positive acknowledgement(s) of the handed-back source", pos)
	}
}

// T18 (stale worker) + T17: a continuation whose ownership ended between its last check and its
// submission acknowledges nothing, and a confirmed acknowledgement waits with sess.mu released.
//
// Mutations: M21 (the gate skips the owner revalidation), M22 (the wait holds sess.mu).
func TestSourceAckGateRevalidatesTheOwnerAndWaitsUnlocked(t *testing.T) {
	r := newTestSharedAdmitRig(t, "shared-gate-owner")
	sess := r.e.sess
	log := testObserveSourceAcks(t, r.dur)
	reply := testInflightAckSubject(r.dur, r.sseq, 1)
	res, _ := sess.submitSourceAck(reply, false, func() bool { return false })
	if res != mqttSourceAckStale {
		t.Fatalf("a stale owner's submission returned %v, want stale", res)
	}
	if pos, nak := log.counts(); pos != 0 || nak != 0 {
		t.Fatalf("a stale owner's submission emitted %d positive and %d hand-back(s)", pos, nak)
	}
	var lockedDuringWait bool
	hook := func(s *mqttSession) {
		if s != sess {
			return
		}
		if sess.mu.TryLock() {
			sess.mu.Unlock()
		} else {
			lockedDuringWait = true
		}
	}
	mqttTestSourceAckWaitHook = hook
	defer func() { mqttTestSourceAckWaitHook = nil }()
	_, _ = sess.submitSourceAck(reply, true, nil)
	if lockedDuringWait {
		t.Fatal("the confirmed acknowledgement waited with sess.mu held")
	}
	if pos, _ := log.counts(); pos != 1 {
		t.Fatalf("the confirmed acknowledgement was submitted %d time(s), want 1", pos)
	}
}

// T19. A shared QoS 0 re-offer of a source this member holds a HandBack for emits no frame and no
// acknowledgement: it is handed back BEFORE output.
//
// Mutation M18e: the pre-output check removed -> the positive acknowledgement is observed.
func TestSharedQoS0EmitsNothingUnderHandBack(t *testing.T) {
	f := testSharedTransmitted(t, "shared-handback-qos0")
	log := testObserveSourceAcks(t, f.dur)
	if r := f.e.sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	_, nakBefore := log.counts()
	// The output window's seam fires immediately before the frame write: reaching it means a frame.
	var framed int
	hook := func(jsDur, _ string) {
		if jsDur == f.dur {
			framed++
		}
	}
	mqttTestSharedQoS0OutputHook.Store(&hook)
	defer mqttTestSharedQoS0OutputHook.Store(nil)
	f.reoffer(t, 0)
	if framed != 0 {
		t.Fatalf("a shared QoS 0 re-offer of a handed-back source reached its frame write %d time(s)", framed)
	}
	pos, nak := log.counts()
	if pos != 0 {
		t.Fatalf("a shared QoS 0 re-offer of a handed-back source was acknowledged %d time(s)", pos)
	}
	if nak != nakBefore+1 {
		t.Fatalf("the shared QoS 0 re-offer was not handed back (hand-backs %d -> %d)", nakBefore, nak)
	}
}

// T8. A GENUINE shared PUBACK — no abandonment, no HandBack — ends the exchange Terminal and the
// terminal continuation positively acknowledges the group source; the identifier is released only
// after that cleanup.
//
// Mutations: M15 (the gate's predicate is the shared prefix alone -> this acknowledgement is handed
// back), M9 (shared stays slot-only -> the exchange never reaches Terminal).
func TestSharedPubAckSettlesTheSourceThroughTheTerminalContinuation(t *testing.T) {
	f := testSharedTransmitted(t, "shared-puback-t8")
	sess := f.e.sess
	log := testObserveSourceAcks(t, f.dur)
	f.e.asm.mu.Lock()
	f.e.asm.testInflightSourceAck = func(string) error { return nil }
	f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return mqttInflightSourceLive }
	f.e.asm.mu.Unlock()
	if got := testCustodyMints(sess, f.pi); got == f.pi {
		t.Fatal("fixture error: the in-flight identifier is mintable before its PUBACK")
	}
	if err := f.e.c.mqttProcessPublishReceived(f.pi, false, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "the shared exchange ended Terminal", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal && x.Abandoned == nil
	})
	// The advance's successor carries the dependency unchanged (M10).
	if x := f.exchange(t); x.SharedAcq == nil || *x.SharedAcq != f.acq {
		t.Fatalf("the Terminal successor depends on %+v, want %+v", x.SharedAcq, f.acq)
	}
	testCustodyEventually(t, "the group source positively acknowledged", func() bool {
		pos, _ := log.counts()
		return pos > 0
	})
	if _, nak := log.counts(); nak != 0 {
		t.Fatalf("a genuine shared PUBACK's source was handed back %d time(s)", nak)
	}
	testCustodyEventually(t, "the register deleted and the identifier released", func() bool {
		reg, _ := f.exchangeOf(t, f.pi)
		return reg == nil && testCustodyMints(sess, f.pi) == f.pi
	})
}

// T10. The close's shared purge withdraws a PROVEN-UNSENT shared allocation through the ordinary
// door — a marker-evidenced tombstone and a durable exchange ending — and keeps a
// POSSIBLY-TRANSMITTED one's identifier and custody: a disconnect is not client completion.
//
// Mutation M12: the purge's proven-unsent arm takes the raw destroy (the exchange is left at Publish).
func TestSharedProvenUnsentPurgeWithdrawsTheExchange(t *testing.T) {
	r := newTestSharedAdmitRig(t, "shared-purge-t10")
	sess := r.e.sess
	unsentPI, ok := r.admit(t, r.dep())
	if !ok {
		t.Fatal("fixture: the unsent delivery was not admitted")
	}
	unsentReg, _ := r.exchangeOf(t, unsentPI)
	// A second delivery of the group, TRANSMITTED (its marker deleted).
	sseq2 := r.e.publishSourceGroup(t, r.dur, "g"+getHashSize("shared-purge-t10", 8), "foo.bar", "foo.bar")
	r.sseq = sseq2
	sentPI, ok := r.admit(t, r.dep())
	if !ok {
		t.Fatal("fixture: the transmitted delivery was not admitted")
	}
	sentSubject := mqttInflightSubject(sess.idHash, sess.gen, sentPI)
	ml := r.e.asm.jsa.loadOccupantMarkerLoad(sentSubject)
	if err := r.e.asm.deleteInflightMarker(mqttMarkerCallerSender, sentSubject, ml.seq, true); err != nil {
		t.Fatalf("deleting the transmitted delivery's marker: %v", err)
	}
	// And the entry says what a send's intent delete leaves it saying.
	sess.mu.Lock()
	if ent := sess.pendingPublish[sentPI]; ent != nil {
		ent.transmitted, ent.markerSeq = true, 0
	}
	sess.mu.Unlock()
	sentReg, _ := r.exchangeOf(t, sentPI)

	sess.purgeSharedPending(mqttTestLicence(sess, nil))

	x, _, err := sess.jsa.loadOutboundExchange(sess.id, r.e.asm.sessStreamEpoch, sess.gen, unsentReg.Lin)
	if err != nil || x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the proven-unsent shared exchange after the purge: %+v (%v), want WithdrawnUnsent", x, err)
	}
	y, _, yerr := sess.jsa.loadOutboundExchange(sess.id, r.e.asm.sessStreamEpoch, sess.gen, sentReg.Lin)
	if yerr != nil || y == nil || y.Phase != mqttExchangePublish {
		t.Fatalf("the possibly-transmitted shared exchange after the purge: %+v (%v), want Publish", y, yerr)
	}
	sess.mu.Lock()
	_, held := sess.pendingPublish[sentPI]
	sess.mu.Unlock()
	if !held {
		t.Fatal("the purge released a possibly-transmitted shared delivery's identifier")
	}
	if reg, _ := r.exchangeOf(t, sentPI); reg == nil {
		t.Fatal("the purge deleted a possibly-transmitted shared delivery's register")
	}
}

// T12c. The action-acquisition ENTRANCE refuses a contradictory pair before any transition is
// composed: an ordinary exchange (no shared acquisition) presented with a register on a shared
// durable acquires nothing, and no action record exists afterwards.
//
// Mutation M19: opAcquireAction skips the pair rule -> the action record is written.
func TestAcquireActionRefusesAnInconsistentSharedPair(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-acq-pair")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
	forged := *reg
	forged.Dur = mqttSharedDurableOf(testValidSharedAcq())
	_, act, err := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq", Op: "op-acq",
		Gen: auth.Gen, Tenure: auth.Tenure, Nonce: nonce, Acq: reg.Acq + 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242), Register: &forged, RegisterSeq: seq, Token: mqttNewInflightNonce(),
	})
	if err == nil || !errors.Is(err, errMQTTSharedAcqPair) || act != nil {
		t.Fatalf("a contradictory pair acquired output: act=%v err=%v", act, err)
	}
	subs, whole, serr := e.asm.jsa.streamSubjectsComplete(mqttInflightStreamName,
		mqttActionWalkFilter(getHash(e.id), auth.Gen, nonce))
	if serr != nil || !whole || len(subs) != 0 {
		t.Fatalf("an action record exists after the refusal: %v whole=%v err=%v", subs, whole, serr)
	}
}

// T19b (consult 84). The shared QoS 0 check does not reserve the source: a HandBack installed AFTER
// an allowed offer passed its pre-output check, through a legitimate path (the same source tracked,
// transmitted and then denied), can overlap that offer's write. What must still hold: the offer's
// ACK goes through the gate, which now holds the HandBack and hands the source back — ZERO positive
// acknowledgements, and the group source stays available for re-offer. A duplicate is the stated
// consequence (docs/CONFORMANCE.md), a positive acknowledgement is not.
func TestSharedQoS0OverlapWithALaterHandBackNeverAcks(t *testing.T) {
	f := testSharedTransmitted(t, "shared-qos0-overlap")
	log := testObserveSourceAcks(t, f.dur)
	var installed, windows int
	hook := func(jsDur, reply string) {
		if jsDur != f.dur {
			return
		}
		windows++
		// The later installation, through the real deny route over the tracked allocation.
		if r := f.e.sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply); r == mqttAbandonDone {
			installed++
		}
	}
	mqttTestSharedQoS0OutputHook.Store(&hook)
	defer mqttTestSharedQoS0OutputHook.Store(nil)
	if f.held(f.e.sess) {
		t.Fatal("fixture error: a HandBack is held before the QoS 0 offer's check")
	}
	f.reoffer(t, 0)
	if windows != 1 || installed != 1 {
		t.Fatalf("fixture error: the window was reached %d time(s) and the HandBack installed %d time(s)", windows, installed)
	}
	if !f.held(f.e.sess) {
		t.Fatal("the HandBack installed inside the window is not held")
	}
	if pos, nak := log.counts(); pos != 0 || nak < 1 {
		t.Fatalf("the overlapped offer: %d positive acknowledgement(s), %d hand-back(s); want 0 and at least 1", pos, nak)
	}
}

// H7a-1 f1. A PUBACK that ends the abandoned shared exchange Terminal BEFORE the disposal settles its
// source must not leave the source without its HandBack: the disposal's terminal handoff builds the
// binding from the Terminal exchange's fact and hands it to the terminal owner before leaving, so the
// group source is never positively acknowledged and the cleanup still completes.
//
// Mutation M25: the handoff does not build the binding -> the HandBack is gone after the handoff.
func TestSharedDisposalTerminalHandoffKeepsTheHandBack(t *testing.T) {
	f := testSharedTransmitted(t, "shared-handoff-race")
	sess := f.e.sess
	log := testObserveSourceAcks(t, f.dur)
	var raced int
	sess.mu.Lock()
	sess.testDisposalSubmitted = func(w *mqttDisposal) {
		if raced > 0 {
			return
		}
		raced++
		// The PUBACK wins the race to the exchange: the abandonment has landed, the disposal has
		// not yet installed its binding or settled its source.
		if err := f.e.c.mqttProcessPublishReceived(f.pi, false, mqttReasonSuccess); err != nil {
			t.Errorf("the racing PUBACK: %v", err)
		}
	}
	sess.mu.Unlock()
	defer func() {
		sess.mu.Lock()
		sess.testDisposalSubmitted = nil
		sess.mu.Unlock()
	}()
	_ = sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply)
	if raced != 1 {
		t.Fatalf("fixture error: the race window was reached %d time(s)", raced)
	}
	testCustodyEventually(t, "the abandoned exchange ended Terminal with its fact", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal && x.Abandoned != nil
	})
	// Re-offers while the register stands are handed back, never acknowledged.
	f.reoffer(t, 1)
	testCustodyEventually(t, "the register deleted and the HandBack released", func() bool {
		reg, _ := f.exchangeOf(t, f.pi)
		return reg == nil && !f.held(sess)
	})
	if pos, _ := log.counts(); pos != 0 {
		t.Fatalf("the handed-back group source was positively acknowledged %d time(s)", pos)
	}
}

// H7a-2. The sibling ordering: the PUBACK captures the tracked entry (its terminal owner carries no
// binding) and pauses before its advance; the abandonment lands, the disposal installs the binding
// and settles. The PUBACK's terminal cleanup must still release that HandBack once the register is
// deleted — the binding set, not the owner's captured copy, is what the gate reads.
//
// Mutation: finishTerminalSource resolves the binding from the carried refs only (the round-2
// defect) — the HandBack outlives the register.
func TestSharedTerminalOwnerCapturedBeforeTheBindingReleasesTheHandBack(t *testing.T) {
	f := testSharedTransmitted(t, "shared-capture-before-binding")
	sess := f.e.sess
	log := testObserveSourceAcks(t, f.dur)
	atAdvance, resume, acked := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var advanced, raced int
	f.e.asm.mu.Lock()
	f.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest,
		write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if advanced++; advanced == 1 {
			close(atAdvance)
			<-resume
		}
		return write()
	}
	f.e.asm.mu.Unlock()
	sess.mu.Lock()
	sess.testDisposalSubmitted = func(w *mqttDisposal) {
		if raced > 0 {
			return
		}
		raced++
		// The abandonment has landed; the PUBACK captures the still-tracked entry and pauses
		// before its advance, holding a terminal owner that carries no binding.
		go func() { acked <- f.e.c.mqttProcessPublishReceived(f.pi, false, mqttReasonSuccess) }()
		select {
		case <-atAdvance:
		case <-time.After(10 * time.Second):
			t.Errorf("fixture error: the PUBACK never reached its advance")
		}
	}
	sess.mu.Unlock()
	defer func() {
		sess.mu.Lock()
		sess.testDisposalSubmitted = nil
		sess.mu.Unlock()
		f.e.asm.mu.Lock()
		f.e.asm.testAckAdvance = nil
		f.e.asm.mu.Unlock()
	}()
	_ = sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply)
	if raced != 1 {
		t.Fatalf("fixture error: the race window was reached %d time(s)", raced)
	}
	// The disposal installs the binding and settles while the PUBACK is paused.
	testCustodyEventually(t, "the disposal settled with the HandBack installed", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		for k := range sess.disposals {
			if k.dur == f.dur && k.seq == f.sseq {
				return false
			}
		}
		return sess.sourceHandBackHeldLocked(f.dur, f.sseq)
	})
	close(resume)
	select {
	case err := <-acked:
		if err != nil {
			t.Fatalf("the PUBACK: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the PUBACK never returned")
	}
	testCustodyEventually(t, "the abandoned exchange ended Terminal with its fact", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal && x.Abandoned != nil
	})
	testCustodyEventually(t, "the register deleted and the HandBack released", func() bool {
		reg, _ := f.exchangeOf(t, f.pi)
		return reg == nil && !f.held(sess)
	})
	if pos, _ := log.counts(); pos != 0 {
		t.Fatalf("the handed-back group source was positively acknowledged %d time(s)", pos)
	}
}

// Consult 85. The SURVIVING-DISCONNECT purge (both disconnect forms) keeps the source mapping of every
// possibly-attempted shared entry it retains — the group durable survives and re-offers the source to
// this member on its resume, which must retransmit that exchange — and drops every other mapping:
// a proven-unsent entry's, and the whole bucket once empty. A plain durable is untouched. It is
// idempotent. A DELIVERY-TARGET TEARDOWN still drops every mapping.
//
// Mutations: the surviving mode restored to whole-bucket deletion (the retained mapping goes); all
// mappings preserved (the proven-unsent one survives); the teardown given the surviving mode (the
// control keeps a mapping); a disconnect form wired to the teardown mode.
func TestSharedSurvivingDisconnectPurgeKeepsRetainedMappings(t *testing.T) {
	for _, staged := range []bool{false, true} {
		staged := staged
		name := map[bool]string{false: "ordinary", true: "staged"}[staged]
		t.Run(name, func(t *testing.T) {
			r := newTestSharedAdmitRig(t, "shared-surviving-purge-"+name)
			sess := r.e.sess
			sentPI, ok := r.admit(t, r.dep())
			if !ok {
				t.Fatal("fixture: the transmitted delivery was not admitted")
			}
			sentSeq := r.sseq
			sentSubject := mqttInflightSubject(sess.idHash, sess.gen, sentPI)
			ml := r.e.asm.jsa.loadOccupantMarkerLoad(sentSubject)
			if err := r.e.asm.deleteInflightMarker(mqttMarkerCallerSender, sentSubject, ml.seq, true); err != nil {
				t.Fatal(err)
			}
			sess.mu.Lock()
			sess.pendingPublish[sentPI].transmitted, sess.pendingPublish[sentPI].markerSeq = true, 0
			sess.mu.Unlock()
			r.sseq = r.e.publishSourceGroup(t, r.dur, "g"+getHashSize("shared-surviving-purge-"+name, 8), "foo.bar", "foo.bar")
			unsentPI, ok := r.admit(t, r.dep())
			if !ok {
				t.Fatal("fixture: the proven-unsent delivery was not admitted")
			}
			unsentSeq := r.sseq
			plainDur := sess.idHash + "_plain"
			sess.mu.Lock()
			sess.cpending[plainDur] = map[uint64]uint16{7: 99}
			sess.mu.Unlock()

			purge := func() {
				if staged {
					_ = sess.purgeSharedPendingStaged(r.e.c, mqttTestLicence(sess, nil))
				} else {
					sess.purgeSharedPending(mqttTestLicence(sess, nil))
				}
			}
			purge()
			check := func(round string) {
				sess.mu.Lock()
				defer sess.mu.Unlock()
				if got := sess.cpending[r.dur][sentSeq]; got != sentPI {
					t.Fatalf("%s: the retained possibly-attempted entry lost its mapping: cpending[%s][%d]=%d, want %d",
						round, r.dur, sentSeq, got, sentPI)
				}
				if _, still := sess.cpending[r.dur][unsentSeq]; still {
					t.Fatalf("%s: the proven-unsent entry kept its mapping", round)
				}
				if sess.pendingPublish[sentPI] == nil {
					t.Fatalf("%s: the possibly-attempted entry was released", round)
				}
				if sess.cpending[plainDur][7] != 99 {
					t.Fatalf("%s: a plain durable's mapping was touched", round)
				}
			}
			check("first purge")
			purge()
			check("second purge")
			_ = unsentPI

			// CONTROL: a delivery-target teardown drops every mapping of the durable.
			sess.mu.Lock()
			sess.purgeConsumerPendingLocked(r.dur, mqttTestLicenceLocked(sess, nil))
			_, bucket := sess.cpending[r.dur]
			sess.mu.Unlock()
			if bucket {
				t.Fatal("a delivery-target teardown kept the durable's mappings")
			}
		})
	}
}
