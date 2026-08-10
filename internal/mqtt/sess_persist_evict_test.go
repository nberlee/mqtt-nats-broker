package mqtt

import (
	"encoding/json"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// The persist-observation eviction path, CLASSIFIED.
//
// processSessionPersist used to conclude "the session was taken over" from the
// PubAck alone — synchronously evicting with 0x8E and destroying the displaced
// incarnation's enqueued delayed Will on evidence that a leader-transition fork
// also produces. The eviction now runs on a per-client worker
// (evictDisplacedOnRemotePersist) that reads and classifies the surviving record
// first: only a record that is not a proven same-base sibling licenses the
// takeover claim and the scoped Will cancel.
//
// Every fixture here is a REAL record over a real JetStream; observations are
// either real peer saves (delivered on the real subscription) or synthetic
// deliveries into processSessionPersist, which decouple the observed sequence
// from the planted record — the state a lost/late core-NATS notification leaves.

// testNewEvictEnv is testNewForkEnv with the persist-observation drop DISARMED:
// these tests are about the observation route, so observations must reach
// processSessionPersist (testNewForkEnv arms the drop for the rejection tests'
// benefit; this env's own saves are self-id-filtered regardless).
func testNewEvictEnv(t *testing.T, clientID string) *testRejectionEnv {
	t.Helper()
	e := testNewForkEnv(t, clientID)
	e.asm.mu.Lock()
	e.asm.testSessPersistDrop = nil
	e.asm.mu.Unlock()
	return e
}

// deliverPersistObservation hands processSessionPersist a crafted remote persist
// ack, exactly as the JSA reply subscription would deliver one from another
// broker. Synthetic delivery decouples the observed sequence from any record
// actually present, which no real save can do.
func (e *testRejectionEnv) deliverPersistObservation(t *testing.T, seq uint64) {
	t.Helper()
	b, err := json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: mqttSessStreamName, Sequence: seq}})
	if err != nil {
		t.Fatalf("marshal the persist ack: %v", err)
	}
	subject := mqttJSARepliesPrefix + "remoteid." + mqttJSASessPersist + "." + e.idHash + ".reply"
	e.asm.processSessionPersist(nil, &client{}, nil, subject, _EMPTY_, append(b, CR_LF...))
}

// armEvictObserver arms the completion seam: the returned channel carries one
// value per eviction-worker completion for this client — whether that worker
// applied an eviction. Asserting "no eviction" AFTER a receive makes the
// negative the worker's decision rather than its absence.
func (e *testRejectionEnv) armEvictObserver() chan bool {
	ch := make(chan bool, 16)
	e.asm.mu.Lock()
	e.asm.testEvictComplete = func(h string, applied bool) {
		if h == e.idHash {
			ch <- applied
		}
	}
	e.asm.mu.Unlock()
	return ch
}

func testAwaitEvictDone(t *testing.T, ch chan bool) bool {
	t.Helper()
	select {
	case applied := <-ch:
		return applied
	case <-time.After(testRejectionWait):
		t.Fatal("the eviction worker never completed")
		return false
	}
}

// plantPendingWill stores a pending delayed Will outbox record shaped as the
// displaced incarnation's own enqueue would have left it: this session's Gen, an
// origin sequence at the session's acknowledged record, and a far-future FireAt
// that keeps the claim loop out of the picture — so any state change is
// attributable to the eviction path's scoped cancel.
func (e *testRejectionEnv) plantPendingWill(t *testing.T) string {
	t.Helper()
	e.sess.mu.Lock()
	gen, seq := e.sess.gen, e.sess.seq
	e.sess.mu.Unlock()
	willID := mqttNewRecordWID()
	subject := mqttWillQSubject(e.asm.domainTk, e.idHash, willID)
	rec := &mqttWillOutbox{
		WillID:           willID,
		ClientIDHash:     e.idHash,
		Gen:              gen,
		FireAt:           time.Now().Add(time.Hour).UnixNano(),
		State:            mqttWillPending,
		OriginSessionSeq: seq,
	}
	if _, err := e.asm.jsa.storeWillOutboxCAS(subject, 0, rec); err != nil {
		t.Fatalf("plant the pending Will outbox record: %v", err)
	}
	return subject
}

// willOutboxState asserts on the RECORD's state, not on stream counts — the
// interest-retention hazard: absence never proves anything about what ran.
func (e *testRejectionEnv) willOutboxState(t *testing.T, subject string) mqttWillState {
	t.Helper()
	rec, _, err := e.asm.jsa.loadWillOutbox(subject)
	if err != nil || rec == nil {
		t.Fatalf("load the Will outbox record: %v", err)
	}
	return rec.State
}

// landRecord lands a record at the session's subject UNCONDITIONALLY (no CAS),
// the durable end state a leader transition produces. The default shape is a
// SIBLING of the acknowledged record (same Gen, PWID = our acknowledged parent,
// fresh WID); shape overrides it per row.
func (e *testRejectionEnv) landRecord(t *testing.T, shape func(*mqttPersistedSession)) uint64 {
	t.Helper()
	e.sess.mu.Lock()
	ps := e.sess.buildPersistedSessionLocked()
	ps.Gen = e.sess.gen
	ps.PWID = e.sess.pwid
	ps.WID = mqttNewRecordWID()
	domainTk, idHash := e.sess.domainTk, e.sess.idHash
	e.sess.mu.Unlock()
	if shape != nil {
		shape(&ps)
	}
	testMutateSibling(&ps)
	body, _ := json.Marshal(&ps)
	resp, err := e.sess.jsa.storeSessionMsg(domainTk, idHash, 0, body)
	if err != nil {
		t.Fatalf("land the staged record: %v", err)
	}
	return resp.Sequence
}

// siblingTakeover restores the same client through a peer manager, points the
// peer object's acknowledged WID at OUR acknowledged PARENT, and saves: the
// peer's write then derives from the same base as our acknowledged record — a
// proven sibling — and its REAL persist observation is what our subscription
// delivers. Returns the sequence the record now sits at.
func (e *testRejectionEnv) siblingTakeover(t *testing.T, node, clientID string) uint64 {
	t.Helper()
	_, psess, _ := e.peer(t, node, clientID)
	e.sess.mu.Lock()
	ackPWID := e.sess.pwid
	e.sess.mu.Unlock()
	psess.mu.Lock()
	psess.wid = ackPWID
	psess.mu.Unlock()
	if err := testSessSave(t, psess); err != nil {
		t.Fatalf("%s: persist the sibling record: %v", node, err)
	}
	psess.mu.Lock()
	defer psess.mu.Unlock()
	return psess.seq
}

// disconnectReasonString returns the Reason String property of the first server
// DISCONNECT written, or "" when none carries one.
func (b *testBoundConn) disconnectReasonString(t *testing.T) string {
	t.Helper()
	for _, p := range testParseWire(t, b.out.bytes()) {
		if p.typ != mqttPacketDisconnect || len(p.body) < 2 {
			continue
		}
		i, pl, mult := 1, 0, 0
		for {
			if i >= len(p.body) {
				t.Fatalf("truncated DISCONNECT properties length: %x", p.body)
			}
			bb := p.body[i]
			i++
			pl += int(bb&0x7f) * (1 << mult)
			if bb&0x80 == 0 {
				break
			}
			mult += 7
		}
		for end := i + pl; i < end; {
			if p.body[i] != mqttPropReasonString {
				t.Fatalf("unexpected DISCONNECT property 0x%x in %x", p.body[i], p.body)
			}
			i++
			n := int(p.body[i])<<8 | int(p.body[i+1])
			i += 2
			return string(p.body[i : i+n])
		}
	}
	return _EMPTY_
}

func (e *testRejectionEnv) holdSessionGate() {
	e.asm.mu.Lock()
	e.asm.sessLocked[e.sess.id] = struct{}{}
	e.asm.mu.Unlock()
}

func (e *testRejectionEnv) releaseSessionGate() {
	e.asm.mu.Lock()
	delete(e.asm.sessLocked, e.sess.id)
	e.asm.mu.Unlock()
}

func (e *testRejectionEnv) armExpiryTimer() {
	e.asm.mu.Lock()
	e.asm.scheduleSessionExpiryLocked(e.s, e.idHash, time.Hour)
	e.asm.mu.Unlock()
}

func (e *testRejectionEnv) hasPendingExpiry() bool {
	e.asm.mu.Lock()
	defer e.asm.mu.Unlock()
	_, ok := e.asm.pendingExpiries[e.idHash]
	return ok
}

// parkEvictWorker arms the given pause seam to park the FIRST worker call and
// then disarm itself, the testMQTTStartParkedReconcile pattern: later iterations
// of the same worker (and later workers) run through. Returns parked (closed
// once the worker is parked) and release.
func (e *testRejectionEnv) parkEvictWorker(t *testing.T, afterLoad bool) (parked, release chan struct{}) {
	t.Helper()
	parked = make(chan struct{})
	release = make(chan struct{})
	var once atomic.Bool
	hook := func(string) {
		if !once.CompareAndSwap(false, true) {
			return
		}
		close(parked)
		<-release
	}
	e.asm.mu.Lock()
	if afterLoad {
		e.asm.testEvictPauseAfterLoad = hook
	} else {
		e.asm.testEvictPauseBeforeLoad = hook
	}
	e.asm.mu.Unlock()
	return parked, release
}

// awaitClosed bounds the asynchronous `go closeConnection` tail.
func testAwaitConnClosed(t *testing.T, b *testBoundConn) {
	t.Helper()
	deadline := time.Now().Add(testRejectionWait)
	for !b.closed() {
		if time.Now().After(deadline) {
			t.Fatal("the evicted connection was never closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// The verdicts (plan tests 1, 2, 11)
// ---------------------------------------------------------------------------

// TestMQTTPersistEvictForkedSiblingBound is the finding, end to end: a REAL peer
// save whose record is a proven same-base sibling displaces a bound client. The
// broker must still evict (the record provably moved with content that is not
// ours) but may claim only what the evidence supports: 0x89 "server busy" with
// the ownership-unsettled Reason String — inviting the reconnect 0x8E forbids —
// and the displaced incarnation's enqueued delayed Will record stays PENDING,
// because nothing arbitrated a session continuation (M1, M3, M7).
func TestMQTTPersistEvictForkedSiblingBound(t *testing.T) {
	e := testNewEvictEnv(t, "pevfork")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	will := e.plantPendingWill(t)
	e.armExpiryTimer()
	done := e.armEvictObserver()

	e.siblingTakeover(t, "node2", "pevfork")

	if !testAwaitEvictDone(t, done) {
		t.Fatal("a forked verdict still evicts — only the CLAIM differs")
	}
	if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonServerBusy {
		t.Fatalf("a proven sibling licenses 0x89 server busy, got %x", reasons)
	}
	if rs := b.disconnectReasonString(t); rs != mqttReasonStrOwnershipUnsettled {
		t.Fatalf("the 0x89 must carry the ownership-unsettled Reason String, got %q", rs)
	}
	testAwaitConnClosed(t, b)
	if e.resident(e.sess) {
		t.Fatal("the displaced session must be evicted")
	}
	if e.hasPendingExpiry() {
		t.Fatal("the expiry timer must be cancelled: the record is owned remotely now")
	}
	if st := e.willOutboxState(t, will); st != mqttWillPending {
		t.Fatalf("a forked verdict must NOT cancel the Will outbox record, got %v", st)
	}
}

// TestMQTTPersistEvictTakeoverCancelsWill is the paired successor case: a real
// peer RESUME save is a child of our acknowledged record — a demonstrated
// takeover — so the claim is 0x8E and the scoped Will cancel runs, sequenced
// before the completion observer (which is what makes this assertion causal
// rather than a poll against the scan).
func TestMQTTPersistEvictTakeoverCancelsWill(t *testing.T) {
	e := testNewEvictEnv(t, "pevtake")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	will := e.plantPendingWill(t)
	done := e.armEvictObserver()

	e.remoteTakeover(t, "node2", "pevtake")

	if !testAwaitEvictDone(t, done) {
		t.Fatal("a demonstrated takeover must evict")
	}
	if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
		t.Fatalf("a demonstrated successor licenses 0x8E, got %x", reasons)
	}
	if e.resident(e.sess) {
		t.Fatal("the displaced session must be evicted")
	}
	if st := e.willOutboxState(t, will); st != mqttWillCancelled {
		t.Fatalf("the takeover verdict must cancel the displaced incarnation's Will, got %v", st)
	}
}

// TestMQTTPersistEvictResidentUnbound is plan test 11: the resident-DISCONNECTED
// shape (expiry timer armed, delayed Will possibly enqueued, no connection to
// notify). Both verdicts evict silently and cancel the timer; only the Will
// record's fate differs.
func TestMQTTPersistEvictResidentUnbound(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape func(*testRejectionEnv, *mqttPersistedSession)
		want  mqttWillState
	}{
		{"forked preserves the outbox record", func(e *testRejectionEnv, ps *mqttPersistedSession) {
			// The default landRecord shape IS the sibling.
		}, mqttWillPending},
		{"takeover cancels it", func(e *testRejectionEnv, ps *mqttPersistedSession) {
			e.sess.mu.Lock()
			ps.PWID = e.sess.wid // a CHILD of the acknowledged record
			e.sess.mu.Unlock()
		}, mqttWillCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewEvictEnv(t, "pevunb-"+tc.name[:4])
			will := e.plantPendingWill(t)
			e.armExpiryTimer()
			done := e.armEvictObserver()

			seq := e.landRecord(t, func(ps *mqttPersistedSession) { tc.shape(e, ps) })
			e.deliverPersistObservation(t, seq)

			if !testAwaitEvictDone(t, done) {
				t.Fatal("the eviction must apply on both verdicts")
			}
			if e.resident(e.sess) {
				t.Fatal("the displaced session must be evicted")
			}
			if e.hasPendingExpiry() {
				t.Fatal("the expiry timer must be cancelled on both verdicts")
			}
			if st := e.willOutboxState(t, will); st != tc.want {
				t.Fatalf("Will outbox record state = %v, want %v", st, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The license rule and the strict staleness bound (plan test 3; M2, M6, M14)
// ---------------------------------------------------------------------------

// TestMQTTPersistEvictLicenseTable drives the concede/refuse table with planted
// records and synthetic observations. The license is the OBSERVATION: a record
// the worker cannot read — absent, undecodable, or older than the write that
// licensed the eviction — narrows nothing, so those rows concede to 0x8E and
// still cancel the Will. The one refusal is equality: no remote write can land
// AT our acknowledged sequence within a stream epoch, so the session's own
// record is never a takeover.
func TestMQTTPersistEvictLicenseTable(t *testing.T) {
	type row struct {
		name string
		// stage returns the sequence to deliver as the observation.
		stage    func(t *testing.T, e *testRejectionEnv) uint64
		evicted  bool
		reason   byte
		wantWill mqttWillState
	}
	for _, tc := range []row{
		{"absent record concedes", func(t *testing.T, e *testRejectionEnv) uint64 {
			smsg, err := e.asm.jsa.loadSessionMsg(e.asm.domainTk, e.idHash)
			if err != nil {
				t.Fatalf("load the record to delete it: %v", err)
			}
			if err := e.asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil {
				t.Fatalf("delete the session record: %v", err)
			}
			return smsg.Sequence + 1
		}, true, mqttReasonSessionTakenOver, mqttWillCancelled},
		{"undecodable record concedes", func(t *testing.T, e *testRejectionEnv) uint64 {
			resp, err := e.sess.jsa.storeSessionMsg(e.asm.domainTk, e.idHash, 0, []byte("{not-a-record"))
			if err != nil {
				t.Fatalf("land the undecodable record: %v", err)
			}
			return resp.Sequence
		}, true, mqttReasonSessionTakenOver, mqttWillCancelled},
		{"generation mismatch concedes", func(t *testing.T, e *testRejectionEnv) uint64 {
			return e.landRecord(t, func(ps *mqttPersistedSession) { ps.Gen = mqttNewSessGen() })
		}, true, mqttReasonSessionTakenOver, mqttWillCancelled},
		{"absent lineage concedes", func(t *testing.T, e *testRejectionEnv) uint64 {
			return e.landRecord(t, func(ps *mqttPersistedSession) { ps.WID, ps.PWID = _EMPTY_, _EMPTY_ })
		}, true, mqttReasonSessionTakenOver, mqttWillCancelled},
		{"below-target sibling concedes through the license", func(t *testing.T, e *testRejectionEnv) uint64 {
			// A FORKED record below the trigger: classifying it would answer
			// 0x89 for a write it cannot speak for (M14). The license path
			// concedes at the observation level instead.
			return e.landRecord(t, nil) + 1
		}, true, mqttReasonSessionTakenOver, mqttWillCancelled},
		{"equality is never a takeover", func(t *testing.T, e *testRejectionEnv) uint64 {
			e.sess.mu.Lock()
			defer e.sess.mu.Unlock()
			return e.sess.seq
		}, false, 0, mqttWillPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewEvictEnv(t, "pevlic-"+tc.name[:6])
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			will := e.plantPendingWill(t)
			done := e.armEvictObserver()

			obs := tc.stage(t, e)
			e.deliverPersistObservation(t, obs)

			applied := testAwaitEvictDone(t, done)
			if applied != tc.evicted {
				t.Fatalf("eviction applied = %v, want %v", applied, tc.evicted)
			}
			if e.resident(e.sess) == tc.evicted {
				t.Fatalf("residency must match the verdict: evicted=%v", tc.evicted)
			}
			reasons := b.disconnectAttempts(t)
			if !tc.evicted {
				if len(reasons) != 0 {
					t.Fatalf("a refused eviction must write NO DISCONNECT, got %x", reasons)
				}
			} else if len(reasons) != 1 || reasons[0] != tc.reason {
				t.Fatalf("DISCONNECT reasons = %x, want [%x]", reasons, tc.reason)
			}
			if st := e.willOutboxState(t, will); st != tc.wantWill {
				t.Fatalf("Will outbox record state = %v, want %v", st, tc.wantWill)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Races against the resident object (plan tests 4, 5; M4, M5)
// ---------------------------------------------------------------------------

// TestMQTTPersistEvictReplacementObject is plan test 4: the worker is per-CLIENT,
// not per-object. It loads the predecessor record, parks, and while parked the
// old object is disposed through a real disposal path and a REPLACEMENT is
// restored and saved ABOVE the loaded sequence. The released worker must resolve
// residency FRESH and refuse on staleness — a worker that decided against its
// spawn-time object (M4) would pass staleness on the removed object's lower
// sequence and delete the replacement by hash.
func TestMQTTPersistEvictReplacementObject(t *testing.T) {
	e := testNewEvictEnv(t, "pevrepl")
	done := e.armEvictObserver()
	parked, release := e.parkEvictWorker(t, true)

	// A foreign successor moves the record; its observation spawns the worker,
	// which loads it and parks post-load.
	seq := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})
	e.deliverPersistObservation(t, seq)
	<-parked

	// Dispose the old object through the real foreign disposition, then restore
	// and persist a replacement — the accept-save shape a reconnect leaves.
	e.sess.disposeForeignSession(nil, mqttDisposalTakenOver)
	if e.resident(e.sess) {
		t.Fatal("fixture: the old object must be disposed while the worker is parked")
	}
	repl, present, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil || !present {
		t.Fatalf("restore the replacement: present=%v err=%v", present, err)
	}
	if err := testSessSave(t, repl); err != nil {
		t.Fatalf("the replacement's accept save must land: %v", err)
	}
	e.asm.addSession(repl, true)
	repl.mu.Lock()
	replSeq := repl.seq
	repl.mu.Unlock()
	if replSeq <= seq {
		t.Fatalf("fixture: the replacement must sit ABOVE the loaded sequence: %d <= %d", replSeq, seq)
	}

	close(release)
	if testAwaitEvictDone(t, done) {
		t.Fatal("the worker must refuse on the replacement's staleness, not evict")
	}
	e.asm.mu.RLock()
	still := e.asm.sessByHash[e.idHash]
	e.asm.mu.RUnlock()
	if still != repl {
		t.Fatal("the replacement must remain resident: deciding against a removed object deletes it by hash")
	}
}

// TestMQTTPersistEvictStaleAfterLocalSave is plan test 5: a local save adopts a
// higher sequence between the worker's load and its decision, so the loaded
// knowledge is stale and the eviction is refused (M5 — dropping the staleness
// check evicts a session over its own newer record).
func TestMQTTPersistEvictStaleAfterLocalSave(t *testing.T) {
	e := testNewEvictEnv(t, "pevstale")
	done := e.armEvictObserver()
	parked, release := e.parkEvictWorker(t, true)

	e.sess.mu.Lock()
	seq := e.sess.seq
	e.sess.mu.Unlock()
	// Equality passes the callback's cheap filter, so the worker spawns and
	// loads the session's own record.
	e.deliverPersistObservation(t, seq)
	<-parked

	// A real local save moves the acknowledged sequence strictly past the
	// loaded one.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("the local save must land: %v", err)
	}

	close(release)
	if testAwaitEvictDone(t, done) {
		t.Fatal("knowledge older than the acknowledged record must not evict")
	}
	if !e.resident(e.sess) {
		t.Fatal("the session must survive a stale-knowledge decision")
	}
}

// ---------------------------------------------------------------------------
// The observed gate and the record fence (plan tests 6, 7; M10, M11)
// ---------------------------------------------------------------------------

// TestMQTTPersistEvictDefersToHeldGate is plan test 6a: a bare sessLocked entry —
// the production interval between a holder's lockSession and its record
// transaction, fence FREE so nothing else can mask the check — defers the worker
// (bounded retries, then a refused exit). Removing the entry and delivering a
// fresh observation applies the eviction, which is what pins the refusal on the
// gate (M10).
func TestMQTTPersistEvictDefersToHeldGate(t *testing.T) {
	e := testNewEvictEnv(t, "pevgate")
	done := e.armEvictObserver()

	seq := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})
	e.holdSessionGate()
	e.deliverPersistObservation(t, seq)

	if testAwaitEvictDone(t, done) {
		t.Fatal("an eviction may not barge a held ownership gate")
	}
	if !e.resident(e.sess) {
		t.Fatal("the deferred eviction must leave the session resident")
	}

	e.releaseSessionGate()
	e.deliverPersistObservation(t, seq)
	if !testAwaitEvictDone(t, done) {
		t.Fatal("with the gate free the same observation must evict")
	}
	if e.resident(e.sess) {
		t.Fatal("the session must be evicted once the gate is free")
	}
}

// testBoundSubscribe builds a v5 SUBSCRIBE packet (one filter). SUBSCRIBE is the
// packet that takes the ownership gate (mqttProcessSubs → lockSession), which is
// what makes it the discriminating probe below — a PUBLISH never touches the
// gate and would pass against a gate-claiming worker.
func testBoundSubscribe(pi uint16, topic string, opts byte) []byte {
	body := newMQTTWriter(0)
	body.WriteUint16(pi)
	body.WriteByte(0) // no properties
	body.WriteString(topic)
	body.WriteByte(opts)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | 0x2)
	w.WriteVarInt(body.Len())
	w.Write(body.Bytes())
	return w.Bytes()
}

// TestMQTTPersistEvictClaimsNothing is plan test 6b: the worker OBSERVES the
// gate, never claims it, so a healthy bound client's own SUBSCRIBE — which
// takes lockSession for its duration — is served while the worker is mid-run. A
// design that claimed the gate would refuse it as a packet error and close a
// live connection.
func TestMQTTPersistEvictClaimsNothing(t *testing.T) {
	e := testNewEvictEnv(t, "pevnoclaim")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	done := e.armEvictObserver()
	parked, release := e.parkEvictWorker(t, true)

	e.sess.mu.Lock()
	seq := e.sess.seq
	e.sess.mu.Unlock()
	e.deliverPersistObservation(t, seq)
	<-parked

	// A SUBSCRIBE from the still-bound client, delivered through the readLoop
	// contract, must be served while the worker is parked mid-run.
	if err := b.deliver(t, testBoundSubscribe(7, "pev/t", 1)); err != nil {
		t.Fatalf("the bound client's SUBSCRIBE must be served: %v", err)
	}
	if !b.sawPacket(t, mqttPacketSubAck) {
		t.Fatalf("expected a SUBACK while the worker is mid-run; wire %x", b.out.bytes())
	}

	close(release)
	if testAwaitEvictDone(t, done) {
		t.Fatal("the equality observation must not evict")
	}
	if !e.resident(e.sess) || b.closed() {
		t.Fatal("the healthy bound client must be untouched")
	}
}

// TestMQTTPersistEvictDefersToOpenRecordTx is plan test 7: a TX-ONLY writer —
// the record transaction open, NO ownership gate, between its store and its
// adoption — is exactly what the fence guards and nothing else does (M11). The
// worker must not decide until the writer completes; once it does, the adopted
// sequence refuses the eviction.
func TestMQTTPersistEvictDefersToOpenRecordTx(t *testing.T) {
	e := testNewEvictEnv(t, "pevtx")
	done := e.armEvictObserver()

	// Park a real save between its store and its adoption: tx open, gate free.
	var storedSeq atomic.Uint64
	hookEntered := make(chan struct{}, 1)
	releaseSave := make(chan struct{})
	restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != e.sess {
			return resp, err
		}
		if err == nil && resp != nil && resp.PubAck != nil {
			storedSeq.Store(resp.Sequence)
		}
		select {
		case hookEntered <- struct{}{}:
		default:
		}
		<-releaseSave
		return resp, err
	})
	defer restore()

	saveDone := make(chan error, 1)
	go func() { saveDone <- testSessSave(t, e.sess) }()
	<-hookEntered

	// The worker's fence attempts are observable at the try-caller's pre-lock
	// seam; one refusal in flight proves the fence is what defers it.
	tried := make(chan struct{}, 1)
	restoreTry := setMQTTTestTryBeginPreLockHook(func(sess *mqttSession) {
		if sess == e.sess {
			select {
			case tried <- struct{}{}:
			default:
			}
		}
	})
	defer restoreTry()

	e.deliverPersistObservation(t, storedSeq.Load())
	select {
	case <-tried:
	case <-time.After(testRejectionWait):
		// A worker that never tries the fence is the M11 mutant; fall through
		// and let the assertions below convict it.
	}
	close(releaseSave)
	if err := <-saveDone; err != nil {
		t.Fatalf("the parked save must land: %v", err)
	}

	if testAwaitEvictDone(t, done) {
		t.Fatal("the worker must not evict: the writer's adopted sequence refuses it")
	}
	if !e.resident(e.sess) {
		t.Fatal("the session must survive its own in-flight save")
	}
}

// ---------------------------------------------------------------------------
// Decision-time freshness (plan tests 8, 10; M8, M9)
// ---------------------------------------------------------------------------

// TestMQTTPersistEvictFreshTriggerReloads is plan test 8: a strictly newer
// observation delivered while the worker is parked post-load makes it RE-LOAD
// and apply the newer record's verdict — here flipping takeover to forked, which
// both the vocabulary and the Will record's final state assert (M9).
func TestMQTTPersistEvictFreshTriggerReloads(t *testing.T) {
	e := testNewEvictEnv(t, "pevfresh")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	will := e.plantPendingWill(t)
	done := e.armEvictObserver()
	parked, release := e.parkEvictWorker(t, true)

	// A successor at N: verdict takeover.
	succ := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})
	e.deliverPersistObservation(t, succ)
	<-parked

	// A sibling at N+1 flips the verdict; its observation advances the trigger
	// while the worker is parked (the running worker consumes it, no second
	// worker spawns).
	sib := e.landRecord(t, nil)
	e.deliverPersistObservation(t, sib)

	close(release)
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the eviction must apply from the freshest knowledge")
	}
	if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonServerBusy {
		t.Fatalf("the newer record's FORKED verdict must apply, got %x", reasons)
	}
	if st := e.willOutboxState(t, will); st != mqttWillPending {
		t.Fatalf("the forked verdict must preserve the Will record, got %v", st)
	}
}

// TestMQTTPersistEvictUnusableLoadFreshTrigger is the license path's freshness
// arm (holistic round 2): an UNREADABLE record concedes, but a strictly newer
// observation delivered before the decision is DELIVERED knowledge, not a lost
// one — the worker must re-load, and the record readable at the new level may
// prove the fork. A worker that skips the freshness arm on the unusable path
// concedes 0x8E at the old level and irreversibly cancels a Will that delivered
// knowledge would have preserved.
func TestMQTTPersistEvictUnusableLoadFreshTrigger(t *testing.T) {
	e := testNewEvictEnv(t, "pevunusable")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	will := e.plantPendingWill(t)
	done := e.armEvictObserver()
	parked, release := e.parkEvictWorker(t, true)

	// An undecodable record at T1: the first load cannot narrow the claim.
	resp, err := e.sess.jsa.storeSessionMsg(e.asm.domainTk, e.idHash, 0, []byte("{not-a-record"))
	if err != nil {
		t.Fatalf("land the undecodable record: %v", err)
	}
	e.deliverPersistObservation(t, resp.Sequence)
	<-parked

	// A classifiable sibling lands at T2 and its observation is DELIVERED while
	// the worker is parked (it advances the trigger; the running worker consumes
	// it).
	sib := e.landRecord(t, nil)
	e.deliverPersistObservation(t, sib)

	close(release)
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the eviction must apply from the freshest knowledge")
	}
	if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonServerBusy {
		t.Fatalf("the delivered newer sibling proves the fork; got %x", reasons)
	}
	if st := e.willOutboxState(t, will); st != mqttWillPending {
		t.Fatalf("the forked verdict must preserve the Will record, got %v", st)
	}
}

// TestMQTTPersistEvictClassifiesAtDecisionTime is plan test 10, the recognition
// work's round-2 ordering hazard: acknowledged {a,p}, a real local save of
// {c,a} parked between its store (S+1) and its adoption, a sibling {b,a} landing
// at S+2 whose observation the worker consumes. The decision runs under the
// fence, which the parked save holds until it has adopted — so the pair the
// classifier sees is {c,a}, which proves {b,a} forked. A pair sampled at or
// before the load sees {a,p} and concedes 0x8E (M8).
func TestMQTTPersistEvictClassifiesAtDecisionTime(t *testing.T) {
	e := testNewEvictEnv(t, "pevpair")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	will := e.plantPendingWill(t)
	done := e.armEvictObserver()
	// Park the worker post-load so the load — and anything a mutant samples at
	// load time — provably happens while the save is still parked pre-adoption.
	parked, release := e.parkEvictWorker(t, true)

	e.sess.mu.Lock()
	ackWID := e.sess.wid
	e.sess.mu.Unlock()

	// Park the local save of {c,a} after its store lands at S+1.
	hookEntered := make(chan struct{}, 1)
	releaseSave := make(chan struct{})
	restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != e.sess {
			return resp, err
		}
		select {
		case hookEntered <- struct{}{}:
		default:
		}
		<-releaseSave
		return resp, err
	})
	defer restore()
	saveDone := make(chan error, 1)
	go func() { saveDone <- testSessSave(t, e.sess) }()
	<-hookEntered

	// The sibling of the ATTEMPT: {b, a} at S+2. Against the stale pair {a,p}
	// it reads unrelated (concede); against the adopted pair {c,a} it is a
	// proven sibling.
	sib := e.landRecord(t, func(ps *mqttPersistedSession) { ps.PWID = ackWID })
	e.deliverPersistObservation(t, sib)
	<-parked

	// Release the save: it adopts {c,a}@S+1 atomically inside its transaction
	// and only then releases the fence the worker is waiting out. Only then is
	// the worker released into its decision.
	close(releaseSave)
	if err := <-saveDone; err != nil {
		t.Fatalf("the parked save must land: %v", err)
	}
	close(release)

	if !testAwaitEvictDone(t, done) {
		t.Fatal("the sibling at S+2 must evict (it moved past the adopted S+1)")
	}
	if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonServerBusy {
		t.Fatalf("the decision-time pair proves the sibling FORKED; got %x", reasons)
	}
	if st := e.willOutboxState(t, will); st != mqttWillPending {
		t.Fatalf("the forked verdict must preserve the Will record, got %v", st)
	}
}

// TestMQTTPersistEvictUnresolvedAttemptPair pins what a stale acknowledged pair
// can and cannot do (holistic round 1). An AMBIGUOUS local save — the store
// landed, the response was lost — arms seqUncertain, closes its transaction and
// leaves the acknowledged pair {a,p} stale behind a FREE fence, so the fence
// cannot make the pair current here. The two rows are the two directions of that
// staleness, and their asymmetry is the point:
//
//   - a remote sibling of the UNADOPTED attempt ({b,a}) reads unrelated against
//     {a,p} and CONCEDES to 0x8E plus the scoped Will cancel — the stated
//     one-hop recognition miss, byte-identical to the pre-change behavior;
//   - a remote sibling of the ACKNOWLEDGED record ({b,p}) still classifies
//     FORKED — 0x89, Will preserved — a true positive the staleness cannot
//     destroy. A stale pair only ever narrows the verdict; no record shape
//     manufactures a false forked claim from it.
func TestMQTTPersistEvictUnresolvedAttemptPair(t *testing.T) {
	for _, tc := range []struct {
		name      string
		siblingOf string // "attempt" or "acknowledged"
		reason    byte
		wantWill  mqttWillState
	}{
		{"sibling of the unadopted attempt concedes", "attempt", mqttReasonSessionTakenOver, mqttWillCancelled},
		{"sibling of the acknowledged record still proves the fork", "acknowledged", mqttReasonServerBusy, mqttWillPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewEvictEnv(t, "pevunres-"+tc.siblingOf)
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			will := e.plantPendingWill(t)
			done := e.armEvictObserver()

			e.sess.mu.Lock()
			ackWID := e.sess.wid
			e.sess.mu.Unlock()

			// The ambiguous attempt {c,a}: a REAL store whose response is
			// swapped for a generic (non-CAS) error, so the body is durable, the
			// session is unresolved, and the transaction has CLOSED without
			// adopting.
			restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
				if sess != e.sess {
					return resp, err
				}
				return nil, errMQTTInjectedLostStoreResponse
			})
			if err := testSessSave(t, e.sess); err == nil {
				restore()
				t.Fatal("the ambiguous save must report an error")
			}
			restore()
			if !e.sess.isSeqUncertain() {
				t.Fatal("fixture: the ambiguous save must leave the session unresolved")
			}

			shape := func(ps *mqttPersistedSession) {
				if tc.siblingOf == "attempt" {
					ps.PWID = ackWID // {b,a}: same parent as the unadopted {c,a}
				}
				// default landRecord shape is {b,p}: sibling of the
				// acknowledged record (sess.pwid is still p — nothing adopted).
			}
			seq := e.landRecord(t, shape)
			e.deliverPersistObservation(t, seq)

			if !testAwaitEvictDone(t, done) {
				t.Fatal("both rows evict; only the claim differs")
			}
			if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != tc.reason {
				t.Fatalf("DISCONNECT reasons = %x, want [%x]", reasons, tc.reason)
			}
			if st := e.willOutboxState(t, will); st != tc.wantWill {
				t.Fatalf("Will outbox record state = %v, want %v", st, tc.wantWill)
			}
			if e.resident(e.sess) {
				t.Fatal("the displaced session must be evicted on both rows")
			}
		})
	}
}

// errMQTTInjectedLostStoreResponse models a landed store whose response was lost:
// a non-CAS failure, so saveWithDetail records the attempt as UNRESOLVED rather
// than definitely rejected.
var errMQTTInjectedLostStoreResponse = errors.New("mqtt test: injected lost store response")

// ---------------------------------------------------------------------------
// Coalescing and the cleanup handoff (plan test 9; M12, M13)
// ---------------------------------------------------------------------------

// TestMQTTPersistEvictCoalesces is plan test 9's first half: observations
// arriving while a worker runs are consumed by THAT worker — exactly one
// completion fires (M12), and the eviction it applies is computed from
// knowledge at least as new as every delivered observation.
func TestMQTTPersistEvictCoalesces(t *testing.T) {
	e := testNewEvictEnv(t, "pevcoal")
	done := e.armEvictObserver()
	parked, release := e.parkEvictWorker(t, false)

	succ := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})
	e.deliverPersistObservation(t, succ)
	<-parked
	// A second, strictly newer observation while the worker is parked: it must
	// coalesce onto the running worker.
	e.deliverPersistObservation(t, succ+1)
	close(release)

	if !testAwaitEvictDone(t, done) {
		t.Fatal("the coalesced trigger must still evict")
	}
	select {
	case <-done:
		t.Fatal("exactly one worker may run: a second completion means a second worker")
	case <-time.After(500 * time.Millisecond):
	}
	if e.resident(e.sess) {
		t.Fatal("the session must be evicted")
	}
}

// TestMQTTPersistEvictCleanupHandoff is plan test 9's second half, made causal
// by the cleanup-entry seam: a worker exits busy-refused (gate held through its
// retries); AT its terminal-cleanup entry the gate is released and a strictly
// newer observation delivered — which declines to spawn, the flag still being
// held. The cleanup's atomic re-check must hand the trigger to the SAME worker,
// which loops and applies the eviction; clearing the flag without the re-check
// strands the trigger forever (M13).
func TestMQTTPersistEvictCleanupHandoff(t *testing.T) {
	e := testNewEvictEnv(t, "pevhand")
	done := e.armEvictObserver()

	succ := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})

	var handed atomic.Bool
	e.asm.mu.Lock()
	e.asm.testEvictCleanupEntry = func(h string) {
		if h != e.idHash || !handed.CompareAndSwap(false, true) {
			return
		}
		e.releaseSessionGate()
		e.deliverPersistObservation(t, succ+1)
	}
	e.asm.mu.Unlock()

	e.holdSessionGate()
	e.deliverPersistObservation(t, succ)

	if !testAwaitEvictDone(t, done) {
		t.Fatal("the trigger delivered at cleanup entry must be consumed by the same worker")
	}
	select {
	case <-done:
		t.Fatal("the handoff must loop the SAME worker, not spawn a second one")
	case <-time.After(500 * time.Millisecond):
	}
	if e.resident(e.sess) {
		t.Fatal("the handed-off trigger must evict")
	}
	if !handed.Load() {
		t.Fatal("fixture: the cleanup-entry seam never fired")
	}
}

// TestMQTTPersistEvictBusyExitConsumesOnlyItsTrigger pins the busy-exit
// accounting (holistic round 4): a busy-refused iteration decided NOTHING, so it
// consumes only the trigger it answered — never the newer record its load
// happened to return. Staged so the two levels differ: trigger N, loaded record
// N+1 (landed while the worker is parked pre-load), gate held through every
// retry; at cleanup entry the gate is released and record N+1's own DELAYED
// observation delivered — equal to the loaded sequence, so an implementation
// that credited the refused iteration with the loaded level reads it as already
// consumed and strands a genuine successor's trigger, leaving the stale client
// serving indefinitely. The same worker must loop and apply the eviction.
func TestMQTTPersistEvictBusyExitConsumesOnlyItsTrigger(t *testing.T) {
	e := testNewEvictEnv(t, "pevbusycons")
	done := e.armEvictObserver()
	parked, release := e.parkEvictWorker(t, false)

	// Trigger N: a successor at N, observed while the gate is held.
	succ := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})
	e.holdSessionGate()
	e.deliverPersistObservation(t, succ)
	<-parked

	// Record N+1 lands before the load runs; its observation is delayed.
	succ2 := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})

	var handed atomic.Bool
	e.asm.mu.Lock()
	e.asm.testEvictCleanupEntry = func(h string) {
		if h != e.idHash || !handed.CompareAndSwap(false, true) {
			return
		}
		e.releaseSessionGate()
		// The delayed observation of the record the refused iteration loaded:
		// EQUAL to the loaded sequence, strictly above the trigger.
		e.deliverPersistObservation(t, succ2)
	}
	e.asm.mu.Unlock()
	close(release)

	if !testAwaitEvictDone(t, done) {
		t.Fatal("the successor's delayed observation must be consumed by the same worker, not stranded")
	}
	select {
	case <-done:
		t.Fatal("the handoff must loop the SAME worker, not spawn a second one")
	case <-time.After(500 * time.Millisecond):
	}
	if e.resident(e.sess) {
		t.Fatal("the handed-off trigger must evict the displaced session")
	}
	if succ2 <= succ {
		t.Fatal("fixture: the loaded record must sit strictly above the trigger")
	}
}

// TestMQTTPersistEvictNoResidentTriggerNotReplayed pins the trigger boundary
// (holistic round 3): an observation delivered while NO session is resident
// belongs to the no-local reconcile branch exclusively — in the remove→re-add
// window that branch deliberately does nothing — and must never be replayed by a
// lingering eviction worker against a re-added object. Staged causally at the
// cleanup-entry seam: the worker exhausts its gate deferral; at cleanup entry the
// session is removed, a strictly newer observation is delivered with none
// resident (it takes the no-local branch), and the object is re-added; the
// cleanup re-check must NOT hand that observation to the worker as a trigger.
func TestMQTTPersistEvictNoResidentTriggerNotReplayed(t *testing.T) {
	e := testNewEvictEnv(t, "pevnoresid")
	done := e.armEvictObserver()

	succ := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})

	var staged atomic.Bool
	e.asm.mu.Lock()
	e.asm.testEvictCleanupEntry = func(h string) {
		if h != e.idHash || !staged.CompareAndSwap(false, true) {
			return
		}
		// The remove→observe→re-add window, driven inside the worker's own
		// cleanup entry so the re-check provably runs after all three.
		e.asm.removeSession(e.sess, true)
		e.deliverPersistObservation(t, succ+1)
		e.asm.addSession(e.sess, true)
		e.releaseSessionGate()
	}
	e.asm.mu.Unlock()

	e.holdSessionGate()
	e.deliverPersistObservation(t, succ)

	if testAwaitEvictDone(t, done) {
		t.Fatal("an observation delivered with no resident session must not evict the re-added object")
	}
	if !e.resident(e.sess) {
		t.Fatal("the re-added session must survive the lingering worker")
	}
	if !staged.Load() {
		t.Fatal("fixture: the cleanup-entry seam never fired")
	}
}

// ---------------------------------------------------------------------------
// Abnormal-exit cleanup (plan test 14; M15)
// ---------------------------------------------------------------------------

// TestMQTTPersistEvictAbnormalExitPreFence is plan test 14a: a Goexit from the
// pre-fence park seam runs the deferred cleanup — flag cleared, counter
// decremented, entry pruned, completion observer still fired — and a subsequent
// observation spawns a FRESH worker whose eviction applies.
func TestMQTTPersistEvictAbnormalExitPreFence(t *testing.T) {
	e := testNewEvictEnv(t, "pevgoexit")
	done := e.armEvictObserver()

	succ := e.landRecord(t, func(ps *mqttPersistedSession) {
		e.sess.mu.Lock()
		ps.PWID = e.sess.wid
		e.sess.mu.Unlock()
	})

	var died atomic.Bool
	e.asm.mu.Lock()
	e.asm.testEvictPauseBeforeLoad = func(h string) {
		if h == e.idHash && died.CompareAndSwap(false, true) {
			runtime.Goexit()
		}
	}
	e.asm.mu.Unlock()

	e.deliverPersistObservation(t, succ)
	if testAwaitEvictDone(t, done) {
		t.Fatal("the killed worker cannot have applied an eviction")
	}
	e.asm.mu.RLock()
	_, entryPresent := e.asm.reconcileWM[e.idHash]
	e.asm.mu.RUnlock()
	if entryPresent {
		t.Fatal("the abnormal exit must prune the watermark entry")
	}

	e.deliverPersistObservation(t, succ)
	if !testAwaitEvictDone(t, done) {
		t.Fatal("a fresh observation must spawn a fresh worker that evicts")
	}
	if e.resident(e.sess) {
		t.Fatal("the fresh worker's eviction must apply")
	}
}

// TestMQTTPersistEvictAbnormalExitPostFence is plan test 14b, sequenced so each
// assertion proves what it claims. The session is UNBOUND with an ARMED
// void-revision gap — the state in which the dangerous synchronous handoff is
// actually due — and the worker dies at the post-fence seam while holding as.mu
// and the open fence.
//
//  1. The bounded completion-observer wait: a guard that ran the FULL
//     endRecordTx under as.mu clears the fence and then self-deadlocks in the
//     void-verdict handoff (which reacquires as.mu), so the observer misses this
//     deadline — a later fence probe alone would pass that mutant (M15b).
//  2. The NON-PERSISTING fence probe, released handoff-free: a leaked token
//     refuses it (M15a). It must not save — a save would marshal and credit the
//     armed gap, destroying step 3's assertion.
//  3. The periodic backstop owns the handoff the abandon release skipped:
//     reconcileResidentRecordState reaches flushVoidVerdictGap once the fence
//     reads closed, and both the credited revision and the durable nonce prove
//     it PERSISTED the gap rather than merely returning.
func TestMQTTPersistEvictAbnormalExitPostFence(t *testing.T) {
	e := testNewEvictEnv(t, "pevpostfence")
	done := e.armEvictObserver()

	// Arm the void-revision gap on the unbound resident session.
	const nonce = "pev-void-nonce"
	e.sess.mu.Lock()
	if e.sess.c != nil {
		t.Fatal("fixture: this staging is unbound-only")
	}
	e.sess.voidedRegsRev++
	staged := e.sess.voidedRegsRev
	if e.sess.cancelledRegNonces == nil {
		e.sess.cancelledRegNonces = make(map[string]uint64)
	}
	e.sess.cancelledRegNonces[nonce] = staged
	seq := e.sess.seq
	e.sess.mu.Unlock()

	var died atomic.Bool
	e.asm.mu.Lock()
	e.asm.testEvictPostFence = func(h string) {
		if h == e.idHash && died.CompareAndSwap(false, true) {
			runtime.Goexit()
		}
	}
	e.asm.mu.Unlock()

	// Equality passes the callback's cheap filter; the decision reaches the
	// post-fence seam before its staleness refusal.
	e.deliverPersistObservation(t, seq)

	// 1. Bounded observer wait (never the suite timeout).
	if testAwaitEvictDone(t, done) {
		t.Fatal("the killed worker cannot have applied an eviction")
	}

	// 2. The fence must be free, proven by a non-persisting probe, released
	// handoff-free so the armed gap survives for step 3.
	tok, ok := e.sess.tryBeginRecordTx()
	if !ok {
		t.Fatal("the abnormal exit leaked the record fence")
	}
	if err := e.sess.abandonRecordTx(tok); err != nil {
		t.Fatalf("release the probe handoff-free: %v", err)
	}
	e.sess.mu.Lock()
	if e.sess.voidedRegsSavedRev >= staged {
		e.sess.mu.Unlock()
		t.Fatal("fixture: nothing may have credited the gap before the backstop runs")
	}
	e.sess.mu.Unlock()

	// 3. The periodic backstop persists the skipped handoff.
	e.asm.reconcileResidentRecordState(e.s)
	e.sess.mu.Lock()
	saved := e.sess.voidedRegsSavedRev
	e.sess.mu.Unlock()
	if saved < staged {
		t.Fatalf("the periodic backstop must persist the gap: saved=%d staged=%d", saved, staged)
	}
	ps := testLoadRecordLineage(t, e.testInflightEnv)
	found := false
	for _, n := range ps.CancelledRegs {
		if n == nonce {
			found = true
		}
	}
	if !found {
		t.Fatal("the staged nonce must be durable in the persisted record")
	}
}
