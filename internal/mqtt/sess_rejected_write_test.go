package mqtt

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testRejectionWait bounds the ONE intentionally asynchronous observation in this file
// — the core-NATS persist notification, which is delivered on a subscription goroutine.
// Everything else here is driven synchronously.
const testRejectionWait = 10 * time.Second

// Settling a DEFINITELY REJECTED session-record CAS.
//
// The reproduction the roadmap item demanded is a CAUSAL PAIR, because the defect is
// the absence of a reaper rather than a wrong decision: the persist notification is an
// at-most-once core-NATS observation, and only by showing that the same takeover is
// reaped WITH it and not reaped WITHOUT it is the notification pinned as the sole
// evictor. TestMQTTRejectedRecordNotificationDropped and
// TestMQTTRejectedRecordNotificationDelivered are that pair; the second is what stops
// the first from being vacuous.
//
// Two account session managers over one embedded JetStream are the cross-broker
// boundary here: distinct session objects and distinct JSA ids over one real
// $MQTT_sess, so a peer's save moves the subject exactly as another broker's would and
// a real CAS rejection — not a seam — arms the marker.

type testRejectionEnv struct {
	*testInflightEnv
	clientID string
	idHash   string
}

// testNewRejectionEnv builds a RESIDENT, UNBOUND, NEVER-EXPIRING session with its
// record persisted. Never-expiring is not a corner: every MQTT 3.1.1 persistent
// session is, no expiry timer exists for one, and sweepSessionExpiries skips it — so
// the periodic pass is the only thing that can ever look at it again.
func testNewRejectionEnv(t *testing.T, clientID string) *testRejectionEnv {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	e.sess.mu.Lock()
	e.sess.expiryInterval = mqttSessionNeverExpire
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	// Resident, as a session that has been through a CONNECT is.
	e.asm.addSession(e.sess, true)
	return &testRejectionEnv{testInflightEnv: e, clientID: clientID, idHash: e.sess.idHash}
}

// dropPersistObservations arms the instance-scoped, hash-keyed seam: this broker never
// learns of a remote persist for THIS client ID, which is the state a lost core-NATS
// notification leaves. It returns the count of dropped observations.
func (e *testRejectionEnv) dropPersistObservations(t *testing.T, idHash string) *atomic.Int64 {
	t.Helper()
	var dropped atomic.Int64
	e.asm.mu.Lock()
	e.asm.testSessPersistDrop = func(h string) bool {
		if h != idHash {
			return false
		}
		dropped.Add(1)
		return true
	}
	e.asm.mu.Unlock()
	return &dropped
}

// remoteTakeover restores the same client ID through a second account session manager
// and persists it, which is what moves the $MQTT_sess subject out from under the local
// session. Returns the sequence the record now sits at.
func (e *testRejectionEnv) remoteTakeover(t *testing.T, node, clientID string) uint64 {
	t.Helper()
	_, psess, _ := e.peer(t, node, clientID)
	if err := testSessSave(t, psess); err != nil {
		t.Fatalf("%s: persist the taken-over session record: %v", node, err)
	}
	psess.mu.Lock()
	defer psess.mu.Unlock()
	return psess.seq
}

// rejectSave performs a real session save that JetStream must REJECT, and returns the
// sequence the rejected CAS was based on. This is the arming path in production: the
// marker is armed centrally in saveWithDetail, by the store boundary itself.
func (e *testRejectionEnv) rejectSave(t *testing.T, sess *mqttSession) uint64 {
	t.Helper()
	sess.mu.Lock()
	base := sess.seq
	sess.mu.Unlock()
	err := testSessSave(t, sess)
	if err == nil {
		t.Fatal("the save must be REJECTED: the record moved under it")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.recordRejectionPendingLocked() {
		t.Fatalf("a definite CAS rejection must arm the marker (rejectedSeq=%d seq=%d uncertain=%v): %v",
			sess.recordRejectedSeq, sess.seq, sess.seqUncertain, err)
	}
	if sess.recordRejectedSeq != base {
		t.Fatalf("the marker must carry the REJECTED CAS's base sequence: got %d, want %d",
			sess.recordRejectedSeq, base)
	}
	return base
}

// resident reports whether both residency maps still point at this object.
func (e *testRejectionEnv) resident(sess *mqttSession) bool {
	e.asm.mu.RLock()
	defer e.asm.mu.RUnlock()
	return e.asm.sessions[sess.id] == sess || e.asm.sessByHash[sess.idHash] == sess
}

func (e *testRejectionEnv) markerPending(sess *mqttSession) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.recordRejectionPendingLocked()
}

// countProbes counts session-record SAVE ATTEMPTS at the save-entry hook, which fires
// before the uncertainty refusal and before the store. Several claims can only be made
// on this counter: a probe that is refused at entry stores nothing, disposes nothing
// and would be invisible to any state-shaped assertion.
func testCountSessSaves(t *testing.T) (*atomic.Int64, func()) {
	t.Helper()
	var n atomic.Int64
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		n.Add(1)
		return nil
	})
	return &n, restore
}

// forceContendedRejection makes THIS session's next saves fail as production-shaped
// 10164s with the record genuinely UNTOUCHED, which is the sustained-contention state:
// JetStream returns 10164 while a write for the subject is in process upstream, so a
// readback finds the base sequence and can conclude nothing.
//
// Composed of two seams, because neither alone can do it: the result hook fires AFTER a
// real store, so rewriting its error would leave the record moved; the suppress seam
// stops the store but produces a generic error rather than a CAS rejection. The shim
// exports the constants and ApiErrors, not the constructor.
func testForceContendedRejection(t *testing.T, target *mqttSession) func() {
	t.Helper()
	mqttTestSessStoreSuppressV.Store(true)
	restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != target {
			return resp, err
		}
		return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
	})
	return func() {
		mqttTestSessStoreSuppressV.Store(false)
		restore()
	}
}

// TestMQTTRejectedRecordContendedKeepsMarker is MU9's paired test: a readback that
// finds the record still at the probe's base sequence must NOT be read as ownership.
// Clearing the marker there is the design this plan's first review round killed with
// upstream source — the contender's proposal can commit a moment later — and it would
// delete the only reason anything ever looks again.
func TestMQTTRejectedRecordContendedKeepsMarker(t *testing.T) {
	e := testNewRejectionEnv(t, "rejcontend")
	e.dropPersistObservations(t, e.idHash)

	stop := testForceContendedRejection(t, e.sess)
	base := e.rejectSave(t, e.sess)

	e.asm.reconcileResidentRecordState(e.s)

	if !e.resident(e.sess) {
		t.Fatal("contention is not a foreign owner: the session must not be disposed")
	}
	if !e.markerPending(e.sess) {
		t.Fatal("a contended probe must KEEP the marker, or nothing ever settles this session")
	}
	e.sess.mu.Lock()
	if e.sess.recordRejectedSeq != base || e.sess.seq != base {
		t.Fatalf("neither the marker nor the sequence may move on contention: rejected=%d seq=%d base=%d",
			e.sess.recordRejectedSeq, e.sess.seq, base)
	}
	if e.sess.seqUncertain {
		t.Fatal("a rejected probe is DEFINITE: it must not arm uncertainty")
	}
	e.sess.mu.Unlock()
	stop()

	// The convergence claim the arm above cannot make on its own: with contention over, the
	// same marker settles on the next pass. One probe per pass IS the cadence — there is no
	// licence beyond the sweep interval itself.
	e.remoteTakeover(t, "node2", "rejcontend")
	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		t.Fatal("once the record has moved, the next pass must dispose the session")
	}
}

// TestMQTTRejectedRecordGoneIsForeign covers the released-record arm: our write cannot
// have landed and then vanished on its own, so an absent record means somebody released
// this session and its state is not ours to rebuild.
func TestMQTTRejectedRecordGoneIsForeign(t *testing.T) {
	e := testNewRejectionEnv(t, "rejgone")
	e.dropPersistObservations(t, e.idHash)

	// Move the record (so the probe is rejected) and then delete it, which is what a
	// remote teardown leaves behind.
	e.remoteTakeover(t, "node2", "rejgone")
	e.rejectSave(t, e.sess)
	smsg, err := e.asm.jsa.loadSessionMsg(e.asm.domainTk, e.idHash)
	if err != nil {
		t.Fatalf("load the record: %v", err)
	}
	if err := e.asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("delete the record: %v", err)
	}

	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		t.Fatal("an absent record is a released session: the local object must be disposed")
	}
}

// TestMQTTRejectedRecordProbeLands is the OURS arm: the rejection was transient and the
// probe's own CAS wins. Nothing is disposed, the sequence advances — which invalidates
// the marker without any path having to clear it — and the void-register revision those
// bytes marshalled is credited, which is why the landed arm needs no separate backstop.
func TestMQTTRejectedRecordProbeLands(t *testing.T) {
	e := testNewRejectionEnv(t, "rejlands")
	e.dropPersistObservations(t, e.idHash)

	stop := testForceContendedRejection(t, e.sess)
	base := e.rejectSave(t, e.sess)
	// Stage a void-register verdict, so the landed probe's credit is observable.
	e.sess.mu.Lock()
	e.sess.voidedRegsRev++
	staged := e.sess.voidedRegsRev
	e.sess.mu.Unlock()
	stop() // contention over: the probe's CAS at base must now succeed

	e.asm.reconcileResidentRecordState(e.s)

	if !e.resident(e.sess) {
		t.Fatal("a landed probe proves the session is OURS: it must not be disposed")
	}
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if e.sess.seq <= base {
		t.Fatalf("a landed probe must advance the sequence: %d <= %d", e.sess.seq, base)
	}
	if e.sess.recordRejectionPendingLocked() {
		t.Fatal("the advanced sequence must invalidate the marker")
	}
	if e.sess.voidedRegsSavedRev < staged {
		t.Fatalf("a landed probe credits what it marshalled: saved=%d staged=%d",
			e.sess.voidedRegsSavedRev, staged)
	}
}

// TestMQTTRejectedRecordStaleMarkerIsNotProbed is MU3's paired test: a marker whose
// rejection a later landed save already settled owes nothing. Asserted on the probe
// COUNT, because a stale marker's probe would simply succeed and leave a state
// indistinguishable from not having probed at all.
func TestMQTTRejectedRecordStaleMarkerIsNotProbed(t *testing.T) {
	e := testNewRejectionEnv(t, "rejstale")
	e.dropPersistObservations(t, e.idHash)

	stop := testForceContendedRejection(t, e.sess)
	e.rejectSave(t, e.sess)
	stop()
	// A save that LANDS moves the sequence past the rejection's base: the question the
	// marker asked is settled.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("the follow-up save must land: %v", err)
	}
	if e.markerPending(e.sess) {
		t.Fatal("a landed save must invalidate the marker")
	}

	saves, restore := testCountSessSaves(t)
	defer restore()
	e.asm.reconcileResidentRecordState(e.s)
	if n := saves.Load(); n != 0 {
		t.Fatalf("a stale marker must not be probed: %d save attempts", n)
	}
	if !e.resident(e.sess) {
		t.Fatal("nothing was outstanding: the session must survive")
	}
}

// TestMQTTRejectedRecordInFenceRecheck is MU8's paired test: the pre-fence sample is a
// check-then-act, and only a re-read INSIDE the fence sees a save that landed since.
// The window seam is what makes that reachable — the pass skips a session whose fence is
// open, so nothing outside can otherwise open this window — and T*StaleMarker above
// cannot discriminate it, because its staleness is visible to the outer sample too.
func TestMQTTRejectedRecordInFenceRecheck(t *testing.T) {
	e := testNewRejectionEnv(t, "rejrecheck")
	e.dropPersistObservations(t, e.idHash)

	stop := testForceContendedRejection(t, e.sess)
	e.rejectSave(t, e.sess)
	stop()

	saves, restoreCount := testCountSessSaves(t)
	defer restoreCount()
	var fired int
	restoreWindow := setMQTTTestRejectionWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttRejectionWindowSettle || fired > 0 {
			return
		}
		fired++
		// A landed save whose transaction CLOSES inside the window: the marker is
		// stale from here on, and only the in-fence re-read can know it.
		if err := testSessSave(t, sess); err != nil {
			t.Errorf("the window save must land: %v", err)
		}
	})
	defer restoreWindow()

	e.asm.reconcileResidentRecordState(e.s)

	if fired == 0 {
		t.Fatal("the window seam never fired, so the fixture proved nothing")
	}
	// One save, the window's own; the probe must not run.
	if n := saves.Load(); n != 1 {
		t.Fatalf("the in-fence re-read must skip a marker staled inside the window: %d save attempts", n)
	}
	if !e.resident(e.sess) {
		t.Fatal("nothing was outstanding by the time the fence was held: the session must survive")
	}
}

// TestMQTTRejectedRecordUncertainInFence is MU4's paired test. An unresolved session's
// save is refused AT ENTRY, before the store, so the detail comes back zero-valued and
// the probe's readback would compare a real record against base sequence 0 and call it
// foreign — destroying the one object that holds the pendingWrite and its
// continuations. The assertion is on the probe COUNT: a predicate without !seqUncertain
// still ends in a refused save and disposes nothing, so no state-shaped assertion can
// tell the two apart.
func TestMQTTRejectedRecordUncertainInFence(t *testing.T) {
	e := testNewRejectionEnv(t, "rejuncertain")
	e.dropPersistObservations(t, e.idHash)

	stop := testForceContendedRejection(t, e.sess)
	base := e.rejectSave(t, e.sess)
	stop()

	saves, restoreCount := testCountSessSaves(t)
	defer restoreCount()
	var fired int
	restoreWindow := setMQTTTestRejectionWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttRejectionWindowSettle || fired > 0 {
			return
		}
		fired++
		// The state an off-readLoop writer's ambiguity leaves, armed in the window the
		// pre-fence sample cannot cover.
		sess.mu.Lock()
		sess.markWriteUncertainLocked(base, []byte("body"), 0)
		sess.mu.Unlock()
	})
	defer restoreWindow()

	e.asm.reconcileResidentRecordState(e.s)

	if fired == 0 {
		t.Fatal("the window seam never fired, so the fixture proved nothing")
	}
	if n := saves.Load(); n != 0 {
		t.Fatalf("an unresolved session must not be probed: %d save attempts", n)
	}
	if !e.resident(e.sess) {
		t.Fatal("the object holding the pendingWrite must survive")
	}
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if !e.sess.seqUncertain || e.sess.pendingWrite == nil {
		t.Fatal("the uncertainty and its attempted-write identity must be left untouched")
	}
}

// TestMQTTRejectedRecordFenceBusy pins the non-blocking fence acquisition: a transaction
// opened in the window makes tryBeginRecordTx refuse, which means "not now", not "settled".
// Nothing is probed, the marker is kept, and a later pass settles it.
func TestMQTTRejectedRecordFenceBusy(t *testing.T) {
	e := testNewRejectionEnv(t, "rejfence")
	e.dropPersistObservations(t, e.idHash)

	stop := testForceContendedRejection(t, e.sess)
	e.rejectSave(t, e.sess)
	stop()

	saves, restoreCount := testCountSessSaves(t)
	defer restoreCount()
	var tok recordTx
	var held bool
	restoreWindow := setMQTTTestRejectionWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttRejectionWindowSettle || held {
			return
		}
		held = true
		tok = sess.beginRecordTx()
	})
	e.asm.reconcileResidentRecordState(e.s)
	restoreWindow()
	if !held {
		t.Fatal("the window seam never fired, so the fixture proved nothing")
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	if n := saves.Load(); n != 0 {
		t.Fatalf("a refused fence must probe nothing: %d save attempts", n)
	}
	if !e.markerPending(e.sess) {
		t.Fatal("a refused fence settles nothing: the marker must be kept")
	}

	// A later pass settles it, which is what makes the refusal a deferral.
	e.remoteTakeover(t, "node2", "rejfence")
	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		t.Fatal("the next pass must settle the deferred marker")
	}
}

// TestMQTTRejectedRecordGateHeld is MU6's paired test: with the ownership gate held —
// which is what a CONNECT in progress looks like — the pass must not load, probe or
// dispose. Asserted on the counters and the surviving marker rather than on "nothing
// was disposed", which is also true of a probe that simply found the record at base.
func TestMQTTRejectedRecordGateHeld(t *testing.T) {
	e := testNewRejectionEnv(t, "rejgate")
	e.dropPersistObservations(t, e.idHash)

	e.remoteTakeover(t, "node2", "rejgate")
	e.rejectSave(t, e.sess)

	saves, restoreCount := testCountSessSaves(t)
	defer restoreCount()
	var loads atomic.Int64
	restoreLoad := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if stream == mqttSessStreamName && strings.HasSuffix(subject, e.idHash) {
			loads.Add(1)
		}
		return nil
	})
	defer restoreLoad()

	if err := e.asm.lockSession(e.sess, nil); err != nil {
		t.Fatalf("take the ownership gate: %v", err)
	}
	e.asm.reconcileResidentRecordState(e.s)
	e.asm.unlockSession(e.sess)

	if n := saves.Load(); n != 0 {
		t.Fatalf("the gate serializes this pass against CONNECT: %d save attempts", n)
	}
	if n := loads.Load(); n != 0 {
		t.Fatalf("no readback may happen without the gate: %d loads", n)
	}
	if !e.markerPending(e.sess) {
		t.Fatal("a refused gate settles nothing: the marker must be kept")
	}
	if !e.resident(e.sess) {
		t.Fatal("nothing may be disposed without the gate")
	}
}

// TestMQTTRejectedRecordReplacementResident is MU7's paired test: with a same-broker
// REPLACEMENT object installed under the same client ID, the stale object's probe must
// never run. Plain lockSession would grant it — it checks only sessLocked and sess.c —
// and the stale object would then classify the replacement's own locally authored record
// as another owner's and dispose of the live session. Residency by identity, claimed in
// the same critical section, is what makes "a newer record is not ours" true.
func TestMQTTRejectedRecordReplacementResident(t *testing.T) {
	e := testNewRejectionEnv(t, "rejreplace")
	e.dropPersistObservations(t, e.idHash)

	stop := testForceContendedRejection(t, e.sess)
	e.rejectSave(t, e.sess)
	stop()

	// A replacement object for the same client ID, resident in place of the stale one —
	// the state a local CONNECT that restored the session leaves.
	replacement, _, err := e.asm.createOrRestoreSession("rejreplace", e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	e.asm.addSession(replacement, true)
	if err := testSessSave(t, replacement); err != nil {
		t.Fatalf("the replacement's own save must land: %v", err)
	}

	saves, restoreCount := testCountSessSaves(t)
	defer restoreCount()
	// The per-session step is driven DIRECTLY, because in production the stale pointer
	// reaches it from the pass's own snapshot: the replacement is installed between the
	// snapshot and the gate claim, so a candidate walk taken now would simply not contain
	// the evicted object and the gate would never be exercised at all.
	e.asm.settleRejectionFromSweep(e.s, e.sess)
	// And the ordinary pass agrees, for the same reason.
	e.asm.reconcileResidentRecordState(e.s)

	if n := saves.Load(); n != 0 {
		t.Fatalf("an evicted object must not probe: %d save attempts", n)
	}
	if !e.resident(replacement) {
		t.Fatal("the replacement must be untouched")
	}
	replacement.mu.Lock()
	defer replacement.mu.Unlock()
	if replacement.c != nil {
		t.Fatal("the replacement must not have been detached")
	}
}

// TestMQTTRejectedRecordUnreadableReadback is the INDETERMINATE arm: a readback that
// cannot be performed says nothing about ownership, so the marker is kept and nothing
// is disposed. The second pass, with the record readable, is the convergence half.
func TestMQTTRejectedRecordUnreadableReadback(t *testing.T) {
	e := testNewRejectionEnv(t, "rejunread")
	e.dropPersistObservations(t, e.idHash)

	e.remoteTakeover(t, "node2", "rejunread")
	e.rejectSave(t, e.sess)

	restoreLoad := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if stream == mqttSessStreamName && strings.HasSuffix(subject, e.idHash) {
			return errors.New("test: the session record is unreadable")
		}
		return nil
	})
	e.asm.reconcileResidentRecordState(e.s)
	restoreLoad()

	if !e.resident(e.sess) {
		t.Fatal("an unreadable readback is not evidence of a foreign owner")
	}
	if !e.markerPending(e.sess) {
		t.Fatal("an indeterminate probe must keep the marker")
	}

	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		t.Fatal("with the record readable again, the next pass must settle it")
	}
}

// bind attaches the env's client to the session, which is what a CONNECT leaves — and what
// makes the periodic pass leave the session alone, since its gate claim is unbound-only.
func (e *testRejectionEnv) bind(t *testing.T) {
	t.Helper()
	e.c.mqtt = &mqtt{sess: e.sess, proto: mqttProtoLevel5, asm: e.asm, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
}

// TestMQTTRejectedRecordPassLeavesBoundSessionsAlone is the unboundness check's paired test,
// and the reason the shipped settlement is unbound-only: the pass's gate claim inserts into
// the shared client-ID gate, so taking it for a live connection makes a concurrent
// SUBSCRIBE's own claim fail — and that packet error closes a connection this broker should
// keep. The assertion is ZERO probes, which only a mutation of the gate's own check can
// break; a duplicate check in the caller would mask it.
//
// A bound session's rejection is therefore settled when it unbinds, which is the residual
// docs/CONFORMANCE.md states and the subject of its own roadmap item.
func TestMQTTRejectedRecordPassLeavesBoundSessionsAlone(t *testing.T) {
	e := testNewRejectionEnv(t, "rejboundalone")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)

	e.remoteTakeover(t, "node2", "rejboundalone")
	e.rejectSave(t, e.sess)

	saves, restore := testCountSessSaves(t)
	defer restore()
	e.asm.reconcileResidentRecordState(e.s)
	if n := saves.Load(); n != 0 {
		t.Fatalf("the periodic pass must not probe a BOUND session: %d save attempts", n)
	}
	if !e.resident(e.sess) || !e.markerPending(e.sess) {
		t.Fatal("the pass must leave a bound session and its marker untouched")
	}

	// And once it unbinds, the very same pass settles it — otherwise "settled when it
	// unbinds" would be a claim with no evidence behind it.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()
	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		t.Fatal("an unbound session whose record moved must be disposed by the pass")
	}
}

// TestMQTTRejectedRecordDeferredDischargeSite covers the DEFERRED CLOSE's classified save
// (sess_uncertain.go), the first of the four rejection sites the plan enumerated, through the
// production path rather than a stand-in: a close parked by an ambiguous write, resolution
// classifying that write not-landed, and the discharge's own save then CAS-rejected.
//
// The rejection is genuinely reachable there because a REMOTE broker needs none of our
// locks: resolution adopts a sequence under the record fence, and a peer can move the record
// before the discharge's save reaches the stream. The fixture moves it at the save's own
// entry hook, which is that window exactly.
//
// This is also the site with NO reaper of its own for a never-expiring session, which is what
// the roadmap bullet was about.
func TestMQTTRejectedRecordDeferredDischargeSite(t *testing.T) {
	e := testNewRejectionEnv(t, "rejdefersite")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)

	// An AMBIGUOUS write: the store never reaches the stream and the caller cannot classify
	// the error, so the session is unresolved and its close must park.
	mqttTestSessStoreSuppressV.Store(true)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the suppressed save must fail")
	}
	mqttTestSessStoreSuppressV.Store(false)
	if !e.sess.isSeqUncertain() {
		t.Fatal("the suppressed save must leave the session unresolved")
	}
	e.s.mqttHandleClosedClient(e.c)
	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect != nil
	base := e.sess.seq
	e.sess.mu.Unlock()
	if !parked {
		t.Fatal("a close on an unresolved session must park its obligations")
	}

	// The discharge's save is rejected: a peer moves the record at the save's entry, which is
	// the one window our fence cannot cover.
	var moved int
	restore := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess != e.sess || moved > 0 {
			return nil
		}
		moved++
		e.remoteTakeover(t, "node2", "rejdefersite")
		return nil
	})
	e.asm.reconcileResidentRecordState(e.s)
	restore()

	if moved == 0 {
		t.Fatal("the discharge never saved, so the fixture proved nothing")
	}
	e.sess.mu.Lock()
	rejected := e.sess.recordRejectedSeq
	uncertain := e.sess.seqUncertain
	e.sess.mu.Unlock()
	if uncertain {
		t.Fatal("resolution must have classified the ambiguous write")
	}
	if rejected != base {
		t.Fatalf("the discharge's rejected save must arm the marker centrally at its own "+
			"base: rejected=%d want=%d", rejected, base)
	}
	if !e.resident(e.sess) {
		t.Fatal("a rejected discharge leaves the session resident: that IS the leak")
	}

	// And the next pass settles it, which is the whole point of the marker.
	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		t.Fatal("the pass must settle the discharge's rejection")
	}
}

// TestMQTTRejectedRecordPurgeSaveSite covers the ORDINARY CLOSE's purge fallback save
// (inflight_register.go, reached from purgeSharedPending) — the site the plan's own
// enumeration missed until a review round found it, which is the argument for arming the
// marker centrally at the store boundary rather than per site.
//
// A never-expiring session with real shared pending state closes while the register stream is
// unavailable, so the purge's third rung falls back to the session record; the record has
// already moved, so that save is CAS-rejected and nothing else on this session would ever
// look again.
func TestMQTTRejectedRecordPurgeSaveSite(t *testing.T) {
	d := testNewDeferredEnv(t, "rejpurgesite", "rej/purgesite")
	e := &testRejectionEnv{testInflightEnv: d.testInflightEnv, clientID: "rejpurgesite", idHash: d.sess.idHash}
	e.sess.mu.Lock()
	e.sess.expiryInterval = mqttSessionNeverExpire
	e.sess.mu.Unlock()
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	d.failRegisterStream(t)

	e.sess.mu.Lock()
	base := e.sess.seq
	e.sess.mu.Unlock()
	e.remoteTakeover(t, "node2", "rejpurgesite")

	e.s.mqttHandleClosedClient(e.c)

	e.sess.mu.Lock()
	rejected := e.sess.recordRejectedSeq
	e.sess.mu.Unlock()
	if rejected != base {
		t.Fatalf("the purge's rejected fallback save must arm the marker centrally at its "+
			"own base: rejected=%d want=%d", rejected, base)
	}
	if !e.resident(e.sess) {
		t.Fatal("nothing evicts a never-expiring session here: that IS the leak")
	}
	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		t.Fatal("the pass must settle the purge save's rejection")
	}
}

// TestMQTTRejectedRecordFiniteCloseSite covers the ordinary close's DISCONNECT save
// (mqtt.go), whose only reaper today is expireSession's superseded branch — bounded by
// the Session Expiry Interval, which v5 allows to be ~136 years. The pass settles it
// long before that, and the expiry entry must not be left armed against an object that
// has since been evicted.
func TestMQTTRejectedRecordFiniteCloseSite(t *testing.T) {
	e := testNewRejectionEnv(t, "rejfinite")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	e.sess.mu.Lock()
	e.sess.expiryInterval = 3600
	e.sess.mu.Unlock()

	e.remoteTakeover(t, "node2", "rejfinite")
	// The close's disconnect save is rejected by the moved record.
	e.s.mqttHandleClosedClient(e.c)

	if !e.markerPending(e.sess) {
		t.Fatal("the close's rejected disconnect save must arm the marker")
	}
	e.asm.mu.RLock()
	_, armed := e.asm.pendingExpiries[e.idHash]
	e.asm.mu.RUnlock()
	if !armed {
		t.Fatal("a finite close arms an expiry timer; without one this test proves nothing")
	}

	e.asm.reconcileResidentRecordState(e.s)

	if e.resident(e.sess) {
		t.Fatal("the pass must settle the close's rejection without waiting out the interval")
	}
	e.asm.mu.RLock()
	_, stillArmed := e.asm.pendingExpiries[e.idHash]
	e.asm.mu.RUnlock()
	if stillArmed {
		t.Fatal("the disposal must cancel the expiry timer it evicted the object for")
	}
}

// TestMQTTRejectedRecordCloseStampsDisconnect is MU13's and MU14's paired test, and the
// reason the stamp is part of this change at all.
//
// The delayed-Will record fence reads a record NEWER than the Will's origin sequence
// with DisconnectedAt == 0 as a live newer connection, and cancels the Will. A
// never-expiring session makes no disconnect save, so without the stamp every later
// write on it — a purge, a void-verdict handoff, a rejection probe — publishes exactly
// that shape and destroys a Will the client is owed.
//
// MU14 (the stamp moved after the purge) is pinned at the SAVE-ENTRY hook, on the value
// the marshal is about to carry: neither the Will fence (the close captures
// originSessionSeq after the purge, so the purge's own sequence never exceeds the Will's
// origin) nor the store-result hook (whose signature carries no body) can see it.
func TestMQTTRejectedRecordCloseStampsDisconnect(t *testing.T) {
	// A session with real shared pending state and a register stream that FAILS, so the
	// purge's third rung falls back to the session record: that is the write whose bytes
	// must already carry the disconnect time, and the only one the purge itself makes.
	// The gate-release handoff's save at the very end of the close would be satisfied by
	// a stamp placed anywhere, which is why it cannot be the fixture.
	d := testNewDeferredEnv(t, "rejstamp", "rej/stamp")
	e := &testRejectionEnv{testInflightEnv: d.testInflightEnv, clientID: "rejstamp", idHash: d.sess.idHash}
	e.sess.mu.Lock()
	e.sess.expiryInterval = mqttSessionNeverExpire
	e.sess.mu.Unlock()
	e.bind(t)
	d.failRegisterStream(t)

	var stampAtSave atomic.Int64
	var saves atomic.Int64
	restore := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess != e.sess || saves.Add(1) != 1 {
			return nil
		}
		// The FIRST record write of the close, which is the purge's own.
		sess.mu.Lock()
		stampAtSave.Store(sess.disconnectedAt)
		sess.mu.Unlock()
		return nil
	})
	e.s.mqttHandleClosedClient(e.c)
	restore()

	if saves.Load() == 0 {
		t.Fatal("the close's purge must have made a record write, or this fixture proves nothing")
	}
	if stampAtSave.Load() == 0 {
		t.Fatal("the never-expire close must stamp the disconnect time BEFORE the purge's save")
	}

	// And the record the fence reads says disconnected, so a Will owed by the
	// incarnation that just closed still fires.
	e.sess.mu.Lock()
	seq := e.sess.seq
	e.sess.mu.Unlock()
	rec := &mqttWillOutbox{
		ClientIDHash:     e.idHash,
		OriginSessionSeq: seq - 1, // the record is NEWER than the Will's origin
		OriginSessEpoch:  e.asm.sessStreamEpoch,
		State:            mqttWillPending,
	}
	if d := e.asm.sessionRecordFence(rec); d != willFenceProceed {
		t.Fatalf("a disconnected session's newer record must not cancel its owed Will: %v", d)
	}
}

// TestMQTTRejectedRecordStampNeedsTheFence pins two facts about the never-expire stamp
// that `sess.mu` alone cannot give it.
//
// It is taken INSIDE a record transaction when one can be had, which is what orders it
// against an off-readLoop writer's already-marshalled `DisconnectedAt = 0`: acquiring the
// fence proves every earlier marshal completed and advanced the sequence the Will's origin
// capture reads, and every later marshal needs that same fence and therefore carries the
// stamp. And the acquisition is NON-BLOCKING: waiting is what bought two defects for one
// fix — a wait spans the very writer being waited on going ambiguous, leaving the entry
// uncertainty check stale, and a park taken after a wait timestamps the close up to a
// JetStream timeout late, re-opening an already-elapsed Will Delay for a reconnect to
// cancel.
//
// The transaction is observed through the fence's own minting counter, and the claim is
// scoped to what that counter proves: the close TAKES a record transaction for the stamp.
// On this fixture the stamp is the close's only one (no pending state to purge, no finite
// disconnect save, no staged verdict for the gate release). Whether the assignment sits
// inside or a few instructions outside the transaction is NOT observable here — the
// busy-fence arm stamps either way — and is not claimed; see the mutation report.
func TestMQTTRejectedRecordStampNeedsTheFence(t *testing.T) {
	e := testNewRejectionEnv(t, "rejstampfence")
	e.bind(t)
	e.sess.mu.Lock()
	seqnoBefore := e.sess.recordTxSeqno
	e.sess.mu.Unlock()

	e.s.mqttHandleClosedClient(e.c)

	e.sess.mu.Lock()
	stamp := e.sess.disconnectedAt
	seqnoAfter := e.sess.recordTxSeqno
	e.sess.mu.Unlock()
	if stamp == 0 {
		t.Fatal("the never-expire close must stamp the disconnect time")
	}
	if seqnoAfter == seqnoBefore {
		t.Fatal("the stamp must TAKE a record transaction: without one it orders against " +
			"nothing, and an already-marshalled DisconnectedAt = 0 can land after the " +
			"Will's origin sequence is captured")
	}

	// And with the fence HELD by another writer the close does not wait for it. (What it
	// does instead — stamp anyway, unordered — is TestMQTTRejectedRecordBusyFenceStampBinds
	// TheProbe's subject.)
	e2 := testNewRejectionEnv(t, "rejstampbusy")
	e2.bind(t)
	tok := e2.sess.beginRecordTx()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e2.s.mqttHandleClosedClient(e2.c)
	}()
	select {
	case <-done:
	case <-time.After(testRejectionWait):
		t.Fatal("the close must not WAIT for the fence")
	}
	if err := e2.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
}

// TestMQTTRejectedRecordBusyFenceStampBindsTheProbe closes the hole the non-blocking form
// would otherwise leave, and it is specifically about THIS change rather than the
// pre-existing posture: the rejection probe is a writer this work ADDS.
//
// A close that finds the fence busy cannot order its stamp against the body already
// marshalled inside that transaction. But that transaction can be rejected at the
// UNCHANGED base — arming the marker without moving the record — and once contention
// clears the probe persists CURRENT MEMORY at a newer sequence. If the close had skipped
// its stamp, those bytes would carry DisconnectedAt = 0 and the Will fence would read the
// probe's own record as a reconnect and cancel a Will the client is owed. So the busy-fence
// arm stamps anyway, unordered, and every marshal from that point on — the probe included
// — carries the disconnect time.
func TestMQTTRejectedRecordBusyFenceStampBindsTheProbe(t *testing.T) {
	e := testNewRejectionEnv(t, "rejbusyfence")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)

	rejected := make(chan struct{})
	release := make(chan struct{})
	released := make(chan struct{})
	go func() {
		defer close(released)
		// A holder whose own save is REJECTED at the unchanged base: the record does not
		// move, and the marker is armed centrally.
		stop := testForceContendedRejection(t, e.sess)
		tok := e.sess.beginRecordTx()
		if err := e.sess.save(tok); err == nil {
			t.Error("the holder's save must be rejected")
		}
		stop()
		close(rejected)
		<-release
		if err := e.sess.endRecordTx(tok); err != nil {
			t.Errorf("endRecordTx: %v", err)
		}
	}()
	<-rejected

	// The close runs with the fence still held by that holder.
	e.s.mqttHandleClosedClient(e.c)
	e.sess.mu.Lock()
	stampedAtClose := e.sess.disconnectedAt
	originSeq := e.sess.seq // what the close captured: nothing has landed
	e.sess.mu.Unlock()
	// The holder releases the fence, and the probe below must not race it: without this
	// the pass can find the fence still held and simply skip, which would make the
	// assertions below vacuous rather than false.
	close(release)
	<-released

	if stampedAtClose == 0 {
		t.Fatal("a close that cannot order its stamp must still stamp: the probe below is a " +
			"NEW writer of the exact record shape that cancels an owed Will")
	}
	if !e.markerPending(e.sess) {
		t.Fatal("the holder's rejection must have armed the marker")
	}

	// Contention is over: the probe lands, writing current memory at a newer sequence.
	e.asm.reconcileResidentRecordState(e.s)
	e.sess.mu.Lock()
	newSeq := e.sess.seq
	e.sess.mu.Unlock()
	if newSeq <= originSeq {
		t.Fatalf("the probe must have landed: seq %d <= origin %d", newSeq, originSeq)
	}

	// The probe's own record is newer than the Will's origin, and must NOT read as a
	// reconnect.
	rec := &mqttWillOutbox{
		ClientIDHash:     e.idHash,
		OriginSessionSeq: originSeq,
		OriginSessEpoch:  e.asm.sessStreamEpoch,
		State:            mqttWillPending,
	}
	if d := e.asm.sessionRecordFence(rec); d != willFenceProceed {
		t.Fatalf("the probe's record must not cancel the owed Will: %v", d)
	}
	ps, _, err := e.asm.loadFenceSession(e.idHash)
	if err != nil || ps == nil {
		t.Fatalf("load the record: ps=%v err=%v", ps, err)
	}
	if ps.DisconnectedAt == 0 {
		t.Fatal("the probe's bytes must carry the disconnect time")
	}
}

// TestMQTTRejectedRecordStampDefersOnLateAmbiguity pins the in-fence re-check. Acquiring
// the fence proves every earlier marshal completed; it does NOT prove that none of them
// armed uncertainty between the close's entry check and this acquisition. Stamping and
// detaching on a sequence nobody has classified is exactly what that entry check exists to
// withhold, so the close must park instead — and only the window seam can produce the
// state, because the two checks are otherwise indistinguishable: uncertainty armed any
// earlier is caught by the entry check itself.
func TestMQTTRejectedRecordStampDefersOnLateAmbiguity(t *testing.T) {
	e := testNewRejectionEnv(t, "rejstampambig")
	e.bind(t)

	var fired int
	restoreWindow := setMQTTTestRejectionWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttRejectionWindowStamp || fired > 0 {
			return
		}
		fired++
		// The state an off-readLoop writer's ambiguous save leaves, armed in the window
		// the entry check cannot cover.
		sess.mu.Lock()
		sess.markWriteUncertainLocked(sess.seq, []byte("body"), 0)
		sess.mu.Unlock()
	})
	e.s.mqttHandleClosedClient(e.c)
	restoreWindow()

	if fired == 0 {
		t.Fatal("the stamp window never fired, so the fixture proved nothing")
	}
	e.sess.mu.Lock()
	stamp := e.sess.disconnectedAt
	e.sess.mu.Unlock()
	if stamp != 0 {
		t.Fatal("an unresolved session must not be stamped: the teardown decisions that " +
			"follow are the ones the uncertainty check exists to withhold")
	}
	action := e.sess.takeDeferredDisconnect()
	if action == nil {
		t.Fatal("a close that finds the record state unresolved with the fence held must PARK")
	}
	if action.disconnectedAt == 0 {
		t.Fatal("the parked action must carry a disconnect time")
	}
}

// TestMQTTRejectedRecordNoBackstopUnlessOurs is MU10's paired test: the void-verdict
// backstop may run only after ownership has been ESTABLISHED. Before the settlement its
// save would be the same doomed CAS, and after a foreign disposition the session is not
// ours to write for at all — which is why the FOREIGN arm's count is exact (one write,
// the probe's) and is what MU10 breaks by writing before probing.
//
// The contended arm makes the weaker claim deliberately. Releasing the ownership gate
// runs the PRE-EXISTING release handoff (unlockSession), which is a different mechanism
// from the periodic backstop and fires for every gate holder on a session with a staged
// verdict; what this change owes is that nothing is CREDITED without a landed save.
func TestMQTTRejectedRecordNoBackstopUnlessOurs(t *testing.T) {
	t.Run("contended", func(t *testing.T) {
		e := testNewRejectionEnv(t, "rejnobackstop")
		e.dropPersistObservations(t, e.idHash)
		stop := testForceContendedRejection(t, e.sess)
		defer stop()
		e.rejectSave(t, e.sess)
		// A staged verdict is exactly what the backstop exists to flush.
		e.sess.mu.Lock()
		e.sess.voidedRegsRev++
		staged := e.sess.voidedRegsRev
		e.sess.mu.Unlock()

		e.asm.reconcileResidentRecordState(e.s)
		if !e.resident(e.sess) || !e.markerPending(e.sess) {
			t.Fatal("a contended arm establishes nothing: session and marker must stand")
		}
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		if e.sess.voidedRegsSavedRev >= staged {
			t.Fatalf("no write landed, so no verdict may be credited: saved=%d staged=%d",
				e.sess.voidedRegsSavedRev, staged)
		}
	})
	t.Run("foreign", func(t *testing.T) {
		e := testNewRejectionEnv(t, "rejnobackstop2")
		e.dropPersistObservations(t, e.idHash)
		e.remoteTakeover(t, "node2", "rejnobackstop2")
		e.rejectSave(t, e.sess)
		e.sess.mu.Lock()
		e.sess.voidedRegsRev++
		e.sess.mu.Unlock()

		saves, restore := testCountSessSaves(t)
		defer restore()
		e.asm.reconcileResidentRecordState(e.s)
		if e.resident(e.sess) {
			t.Fatal("the foreign session must be disposed")
		}
		if n := saves.Load(); n != 1 {
			t.Fatalf("nothing may write for a disposed session: %d save attempts", n)
		}
	})
}

// TestMQTTRejectedProbeClassifierRefusal is MU12's paired test, and it is a DEFENSIVE
// branch stated as such: under the marker's predicate and the probe's fence the state it
// guards is not production-reachable — nothing can arm uncertainty between the in-fence
// re-check and the save's own entry check — so it is constructed directly.
//
// What it pins is that "refused" and "rejected" are different facts. A definite failure
// that never reached the store comes back with a ZERO-VALUED detail, and classifying its
// readback would compare a real record against base sequence 0 and call every session
// foreign.
func TestMQTTRejectedProbeClassifierRefusal(t *testing.T) {
	record := &StoredMsg{Sequence: 42}
	refused := mqttSessSaveDetail{} // the shape an entry refusal returns
	if v := classifyRejectedProbe(saveDefinitelyFailed, refused, record, nil); v != mqttRejectionKeep {
		t.Fatalf("a REFUSED save is not a rejected CAS: got verdict %v, want keep", v)
	}
	// A non-CAS store error is the same fact by another route.
	other := mqttSessSaveDetail{baseSeq: 41, rawErr: errors.New("test: store unavailable")}
	if v := classifyRejectedProbe(saveDefinitelyFailed, other, record, nil); v != mqttRejectionKeep {
		t.Fatalf("a non-CAS failure carries no readback verdict: got %v, want keep", v)
	}
	// And a real CAS rejection does consult the readback, which is what makes the guard
	// a guard rather than a blanket refusal.
	rejected := mqttSessSaveDetail{baseSeq: 41, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
	if v := classifyRejectedProbe(saveDefinitelyFailed, rejected, record, nil); v != mqttRejectionForeign {
		t.Fatalf("a CAS-rejected probe with a moved record is foreign: got %v", v)
	}
	atBase := mqttSessSaveDetail{baseSeq: 42, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
	if v := classifyRejectedProbe(saveDefinitelyFailed, atBase, record, nil); v != mqttRejectionKeep {
		t.Fatalf("contention is never ownership: got %v, want keep", v)
	}
	unreadable := mqttSessSaveDetail{baseSeq: 41, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
	if v := classifyRejectedProbe(saveDefinitelyFailed, unreadable, nil,
		errors.New("test: unreadable")); v != mqttRejectionKeep {
		t.Fatalf("an unreadable readback arbitrates nothing: got %v, want keep", v)
	}
}

// TestMQTTRejectedRecordNotificationDropped is the reproduction: with the persist
// observation lost, a remote takeover leaves the local session resident and its
// deferred obligation parked, and the periodic pass is what settles it. The probe's own
// CAS is rejected, the readback finds a newer, different record, and the session is
// disposed.
//
// It is also MU1's (no central arming) and MU2's (dispose without probing) paired test
// from opposite sides: without arming nothing is ever disposed; without the probe a
// rejection alone would dispose.
func TestMQTTRejectedRecordNotificationDropped(t *testing.T) {
	e := testNewRejectionEnv(t, "rejdrop")
	dropped := e.dropPersistObservations(t, e.idHash)

	// A parked discard-only obligation, set directly: what it pins is that the
	// disposal takes it, so its ORIGIN does not matter and building one through the
	// uncertainty machinery would test that machinery instead.
	e.sess.mu.Lock()
	e.sess.deferredDisconnect = &mqttDeferredDisconnect{disconnectedAt: time.Now().Unix()}
	e.sess.mu.Unlock()

	newSeq := e.remoteTakeover(t, "node2", "rejdrop")
	base := e.rejectSave(t, e.sess)
	if newSeq <= base {
		t.Fatalf("the takeover must advance the record: %d <= %d", newSeq, base)
	}

	// The observation was dropped, so nothing has evicted the session: this is the
	// defect's state, and it survives any number of passes without the settlement.
	if !e.resident(e.sess) {
		t.Fatal("with the persist observation dropped, nothing else may have evicted the session")
	}

	e.asm.reconcileResidentRecordState(e.s)

	if e.resident(e.sess) {
		t.Fatal("the pass must dispose a session whose probe found a foreign record")
	}
	if a := e.sess.takeDeferredDisconnect(); a != nil {
		t.Fatal("the disposal must take the parked obligation: it belongs to the new owner now")
	}
	if dropped.Load() == 0 {
		t.Fatal("the seam never dropped an observation, so the fixture proved nothing")
	}
}

// TestMQTTRejectedRecordNotificationDelivered is the paired POSITIVE: the identical
// takeover with the seam disarmed is evicted by processSessionPersist alone, with no
// pass run at all. Without it the test above could not tell "settled by the new
// mechanism" from "the notification would have done it anyway".
func TestMQTTRejectedRecordNotificationDelivered(t *testing.T) {
	e := testNewRejectionEnv(t, "rejdeliver")
	e.remoteTakeover(t, "node2", "rejdeliver")

	// Asynchronous by design: the observation is a core-NATS delivery on the JSA
	// reply subject, so this is the one bounded poll in the file.
	deadline := time.Now().Add(testRejectionWait)
	for e.resident(e.sess) {
		if time.Now().After(deadline) {
			t.Fatal("the persist observation must evict the displaced local session")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMQTTRejectedRecordPersistDropIsScoped is MU11's paired test: the drop seam is
// keyed by client-ID hash, so an unrelated session on the same manager is still
// evicted by its own observation. An unkeyed seam would strand it too, and every claim
// the dropped-notification tests make about "nothing else evicts this session" would
// silently become a claim about the whole manager.
func TestMQTTRejectedRecordPersistDropIsScoped(t *testing.T) {
	e := testNewRejectionEnv(t, "rejscoped")
	dropped := e.dropPersistObservations(t, e.idHash)

	// A second resident session on the SAME manager, whose hash the seam does not name.
	other, _, err := e.asm.createOrRestoreSession("rejscoped-other", e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if err := testSessSave(t, other); err != nil {
		t.Fatalf("persist the second session record: %v", err)
	}
	e.asm.addSession(other, true)

	e.remoteTakeover(t, "node2", "rejscoped")
	e.remoteTakeover(t, "node3", "rejscoped-other")

	deadline := time.Now().Add(testRejectionWait)
	for e.resident(other) {
		if time.Now().After(deadline) {
			t.Fatal("the unrelated session's own observation must still evict it")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.resident(e.sess) {
		t.Fatal("the named session's observation must still be dropped")
	}
	if dropped.Load() == 0 {
		t.Fatal("the seam never dropped an observation, so the fixture proved nothing")
	}
}
