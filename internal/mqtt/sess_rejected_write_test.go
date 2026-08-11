package mqtt

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
	t.Parallel()
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
		testMarkUncertainLocked(sess, base, []byte("body"))
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
// production path rather than a stand-in: a close parked by an ambiguous write, the resolver
// settling that write by re-attempting its bytes, and the discharge's own save then
// CAS-rejected.
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
	// The base the discharge's save actually CAS'd against: the sequence the resolution
	// adopted for the re-attempt it landed, which a rejection leaves untouched. Read
	// AFTER the pass for that reason — the pre-resolution sequence is a different write's.
	base := e.sess.seq
	e.sess.mu.Unlock()
	if uncertain {
		t.Fatal("the resolver must have settled the ambiguous write")
	}
	if rejected == 0 || rejected != base {
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
	t.Parallel()
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
	t.Parallel()
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
		WillID:           mqttNewRecordWID(),
		ClientIDHash:     e.idHash,
		Gen:              mqttNewSessGen(),
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
	t.Parallel()
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
	// The suppression seam is installed and restored on THIS goroutine, not inside
	// the holder below. It is a process-global, and a write that happens on a
	// goroutine can outlive the test that launched it — landing while the parallel
	// tests that read these globals are running. Installing before the holder
	// starts and restoring after <-rejected keeps the same ordering the holder
	// needs, with the write bounded by the test body.
	stop := testForceContendedRejection(t, e.sess)
	go func() {
		defer close(released)
		// A holder whose own save is REJECTED at the unchanged base: the record does not
		// move, and the marker is armed centrally.
		tok := e.sess.beginRecordTx()
		if err := e.sess.save(tok); err == nil {
			t.Error("the holder's save must be rejected")
		}
		close(rejected)
		<-release
		if err := e.sess.endRecordTx(tok); err != nil {
			t.Errorf("endRecordTx: %v", err)
		}
	}()
	<-rejected
	// Restored before the close path runs, exactly as when this lived inside the
	// holder: the close's own save must not be suppressed.
	stop()

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
		WillID:           mqttNewRecordWID(),
		ClientIDHash:     e.idHash,
		Gen:              mqttNewSessGen(),
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
		testMarkUncertainLocked(sess, sess.seq, []byte("body"))
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
//
// The acknowledged lineage pair is empty throughout: these fixtures are about the CAS
// guard, and an empty pair concedes at every arm, so the verdicts here are the
// pre-lineage ones by construction. The lineage rule has its own tests.
func TestMQTTRejectedProbeClassifierRefusal(t *testing.T) {
	t.Parallel()
	record := &StoredMsg{Sequence: 42}
	refused := mqttSessSaveDetail{} // the shape an entry refusal returns
	if v := classifyRejectedProbe(saveDefinitelyFailed, refused, record, nil, _EMPTY_, _EMPTY_); v != mqttRejectionKeep {
		t.Fatalf("a REFUSED save is not a rejected CAS: got verdict %v, want keep", v)
	}
	// A non-CAS store error is the same fact by another route.
	other := mqttSessSaveDetail{baseSeq: 41, rawErr: errors.New("test: store unavailable")}
	if v := classifyRejectedProbe(saveDefinitelyFailed, other, record, nil, _EMPTY_, _EMPTY_); v != mqttRejectionKeep {
		t.Fatalf("a non-CAS failure carries no readback verdict: got %v, want keep", v)
	}
	// And a real CAS rejection does consult the readback, which is what makes the guard
	// a guard rather than a blanket refusal.
	rejected := mqttSessSaveDetail{baseSeq: 41, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
	if v := classifyRejectedProbe(saveDefinitelyFailed, rejected, record, nil, _EMPTY_, _EMPTY_); v != mqttRejectionForeign {
		t.Fatalf("a CAS-rejected probe with a moved record is foreign: got %v", v)
	}
	atBase := mqttSessSaveDetail{baseSeq: 42, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
	if v := classifyRejectedProbe(saveDefinitelyFailed, atBase, record, nil, _EMPTY_, _EMPTY_); v != mqttRejectionKeep {
		t.Fatalf("contention is never ownership: got %v, want keep", v)
	}
	unreadable := mqttSessSaveDetail{baseSeq: 41, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
	if v := classifyRejectedProbe(saveDefinitelyFailed, unreadable, nil,
		errors.New("test: unreadable"), _EMPTY_, _EMPTY_); v != mqttRejectionKeep {
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

// ---------------------------------------------------------------------------
// A BOUND session's settlement, before the packet is dispatched.
// ---------------------------------------------------------------------------
//
// Every assertion here runs through a REAL SOCKET and through the readLoop's own
// contract, and neither is decoration. A bare mqttParse call cannot see the two things
// this feature is about: whether the connection was actually torn down (mqttParse only
// enqueues the error DISCONNECT and returns — the shape of the defect that returned nil
// mid-loop and closed nothing), and what the client was told before it was. A fixture
// asserting on in-memory state alone passes against a broker that stops nothing.

// testWireCapture is the client end of a bound connection: it records everything the
// broker writes and blocks reads until the connection is closed.
//
// It replaces a net.Pipe plus a draining goroutine deliberately. net.Pipe is unbuffered,
// so every PINGRESP, PUBACK and DISCONNECT the parse goroutine writes needs a concurrent
// reader — and a reader that appends to a capture buffer records the bytes SOME TIME AFTER
// the broker's Write returns, which makes every "the client was told X" assertion a race
// the test loses at random. Capturing inside Write makes the record complete the moment
// mqttParse returns. TestMQTTBoundRejectionNoReentryAfterForeign uses a real net.Pipe,
// because what it asserts is socket behaviour rather than packet content.
//
// recordAfterClose makes it an ATTEMPT LOG rather than a delivery record, which is what
// the DISCONNECT-claim tests need. A displacement closes the socket from another
// goroutine, so a capture that silently drops post-close writes would make "only one
// DISCONNECT was written" true even with the claim deleted — the close, not the claim,
// would be doing the work. Recording the attempt and still returning io.ErrClosedPipe
// keeps production's close-on-write-error path intact while the assertions inspect what
// the broker TRIED to send; nothing here claims those bytes reached a wire.
type testWireCapture struct {
	mu               sync.Mutex
	data             []byte
	closed           chan struct{}
	once             sync.Once
	recordAfterClose bool
}

func (w *testWireCapture) Read(p []byte) (int, error) {
	<-w.closed
	return 0, io.EOF
}

func (w *testWireCapture) Write(p []byte) (int, error) {
	select {
	case <-w.closed:
		if !w.recordAfterClose {
			return 0, io.ErrClosedPipe
		}
		w.mu.Lock()
		w.data = append(w.data, p...)
		w.mu.Unlock()
		return 0, io.ErrClosedPipe
	default:
	}
	w.mu.Lock()
	w.data = append(w.data, p...)
	w.mu.Unlock()
	return len(p), nil
}

func (w *testWireCapture) Close() error {
	w.once.Do(func() { close(w.closed) })
	return nil
}

func (w *testWireCapture) LocalAddr() net.Addr                { return testWireAddr{} }
func (w *testWireCapture) RemoteAddr() net.Addr               { return testWireAddr{} }
func (w *testWireCapture) SetDeadline(t time.Time) error      { return nil }
func (w *testWireCapture) SetReadDeadline(t time.Time) error  { return nil }
func (w *testWireCapture) SetWriteDeadline(t time.Time) error { return nil }

type testWireAddr struct{}

func (testWireAddr) Network() string { return "pipe" }
func (testWireAddr) String() string  { return "test" }

func (w *testWireCapture) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.data...)
}

type testBoundConn struct {
	*testRejectionEnv
	bc  *client
	out *testWireCapture
}

// bindSocket binds the env's session to a NEW client over a connection, as a completed
// CONNECT leaves it: connectReceived set (mqttParse derives "connected" from that flag
// alone, and without it every packet is refused before the gate is reached) and the
// success CONNACK recorded, which is what licenses a server DISCONNECT at all.
func (e *testRejectionEnv) bindSocket(t *testing.T, proto byte) *testBoundConn {
	t.Helper()
	return e.bindSocketOver(t, proto, &testWireCapture{closed: make(chan struct{})})
}

// bindSocketAttemptLog binds over a capture that keeps recording write ATTEMPTS after the
// socket closes. Use it wherever the assertion is about what the broker tried to send —
// the server-DISCONNECT claim — rather than about what a client received.
func (e *testRejectionEnv) bindSocketAttemptLog(t *testing.T, proto byte) *testBoundConn {
	t.Helper()
	return e.bindSocketOver(t, proto, &testWireCapture{closed: make(chan struct{}), recordAfterClose: true})
}

func (e *testRejectionEnv) bindSocketOver(t *testing.T, proto byte, out *testWireCapture) *testBoundConn {
	t.Helper()

	// mpay is the account max payload the publish path sizes messages against; leaving it
	// at zero makes every PUBLISH fail with Quota Exceeded, which would let a "the packet
	// was served" assertion pass on an ERROR ack.
	bc := &client{srv: e.s, acc: e.s.globalAccount(), nrc: e.s.nrc, nc: out, rawConn: out,
		mpay: 1024 * 1024}
	bc.mqtt = &mqtt{
		sess:  e.sess,
		asm:   e.asm,
		proto: proto,
		pp:    &mqttPublish{},
		cp:    &mqttConnectProto{},
	}
	bc.initClient()
	bc.mu.Lock()
	bc.flags.set(connectReceived)
	bc.mu.Unlock()
	bc.connAckWritten.Store(true)

	e.sess.mu.Lock()
	e.sess.c = bc
	e.sess.mu.Unlock()

	t.Cleanup(func() { _ = out.Close() })
	return &testBoundConn{testRejectionEnv: e, bc: bc, out: out}
}

// armWill puts a Will on the connection, so a disposition assertion has something to
// observe.
func (b *testBoundConn) armWill() {
	b.bc.mu.Lock()
	b.bc.mqtt.cp.will = &mqttWill{topic: []byte("will/topic"), subject: []byte("will.topic"), message: []byte("bye")}
	b.bc.mqtt.willDisposition = mqttWillArmed
	b.bc.mu.Unlock()
}

// deliver hands a buffer to the parser THROUGH THE READLOOP'S CONTRACT
// (shim_engine.go's readLoop): a parse error runs the close bookkeeping and closes the
// connection. Reproducing it here rather than calling mqttParse directly is what makes
// "the connection is closed" and "the teardown ran" real assertions.
func (b *testBoundConn) deliver(t *testing.T, buf []byte) error {
	t.Helper()
	err := b.bc.mqttParse(buf)
	if err != nil {
		b.s.mqttHandleClosedClient(b.bc)
		b.bc.closeConnection(ClientClosed)
		return err
	}
	if b.closed() {
		// The parse itself closed the connection (the foreign and displaced arms do),
		// so the readLoop's very next Read fails and runs the same two calls. Without
		// this the fixture would stop one step short of the teardown production
		// performs.
		b.s.mqttHandleClosedClient(b.bc)
		b.bc.closeConnection(ClientClosed)
	}
	return nil
}

func (b *testBoundConn) closed() bool { return b.bc.closed.Load() }

// testWirePacket is one control packet read back off the wire.
type testWirePacket struct {
	typ  byte
	body []byte
}

// testParseWire splits the captured bytes into control packets. Scanning for a byte
// pattern instead would match a reason code inside another packet's payload.
func testParseWire(t *testing.T, data []byte) []testWirePacket {
	t.Helper()
	var pkts []testWirePacket
	for i := 0; i < len(data); {
		typ := data[i] & mqttPacketMask
		i++
		var pl, mult int
		for {
			if i >= len(data) {
				t.Fatalf("truncated remaining length in captured bytes %x", data)
			}
			b := data[i]
			i++
			pl += int(b&0x7f) * (1 << mult)
			if b&0x80 == 0 {
				break
			}
			mult += 7
		}
		if i+pl > len(data) {
			t.Fatalf("truncated packet body in captured bytes %x", data)
		}
		pkts = append(pkts, testWirePacket{typ: typ, body: data[i : i+pl]})
		i += pl
	}
	return pkts
}

// sawPacket reports whether a packet of this type was written to the client.
func (b *testBoundConn) sawPacket(t *testing.T, typ byte) bool {
	t.Helper()
	for _, p := range testParseWire(t, b.out.bytes()) {
		if p.typ == typ {
			return true
		}
	}
	return false
}

// sawPubAckSuccess reports whether a SUCCESS PUBACK was written for this identifier. The
// reason code matters: a Quota Exceeded or Not Authorized PUBACK is also a PUBACK, and a
// bare packet-type check would read one as "the packet was served".
func (b *testBoundConn) sawPubAckSuccess(t *testing.T, pi uint16) bool {
	t.Helper()
	for _, p := range testParseWire(t, b.out.bytes()) {
		if p.typ != mqttPacketPubAck || len(p.body) < 2 {
			continue
		}
		if uint16(p.body[0])<<8|uint16(p.body[1]) != pi {
			continue
		}
		// The short form (no reason byte) is success; the long form carries it.
		if len(p.body) == 2 || p.body[2] == mqttReasonSuccess {
			return true
		}
	}
	return false
}

// sawDisconnect reports whether a server DISCONNECT carrying this reason code was written.
func (b *testBoundConn) sawDisconnect(t *testing.T, reason byte) bool {
	t.Helper()
	for _, p := range testParseWire(t, b.out.bytes()) {
		if p.typ == mqttPacketDisconnect && len(p.body) > 0 && p.body[0] == reason {
			return true
		}
	}
	return false
}

// disconnectAttempts returns the reason code of every server DISCONNECT the broker tried
// to write, in order. On an attempt-log capture (bindSocketAttemptLog) that includes
// writes made after the socket closed, which is the point: a second notification must be
// suppressed by the claim BEFORE the write, not by a socket that happened to be gone.
func (b *testBoundConn) disconnectAttempts(t *testing.T) []byte {
	t.Helper()
	var reasons []byte
	for _, p := range testParseWire(t, b.out.bytes()) {
		if p.typ != mqttPacketDisconnect {
			continue
		}
		if len(p.body) == 0 {
			// The zero-length form means Normal Disconnection; count it, so a
			// stray one can never hide from an "exactly one" assertion.
			reasons = append(reasons, mqttReasonSuccess)
			continue
		}
		reasons = append(reasons, p.body[0])
	}
	return reasons
}

// forgetConnAck puts a bound connection back in the state a refused CONNECT leaves it in:
// still bound to the session, but never told its CONNECT succeeded. mqttAbortAcceptedConnect
// produces exactly this — the connection stays bound until its readLoop detaches it — and
// it is the state in which a displacement must NOT write a DISCONNECT, since CONNACK has to
// be the first packet the client sees [MQTT-3.2.0-1].
func (b *testBoundConn) forgetConnAck() { b.bc.connAckWritten.Store(false) }

// testBoundPublish builds a PUBLISH packet.
func testBoundPublish(v5 bool, qos byte, pi uint16, topic, payload string) []byte {
	body := newMQTTWriter(0)
	body.WriteString(topic)
	if qos > 0 {
		body.WriteUint16(pi)
	}
	if v5 {
		body.WriteByte(0) // no properties
	}
	body.Write([]byte(payload)) // the payload is raw, not length-prefixed
	pkt := newMQTTWriter(0)
	pkt.WriteByte(mqttPacketPub | (qos << 1))
	pkt.WriteVarInt(body.Len())
	pkt.Write(body.Bytes())
	return pkt.Bytes()
}

// testBoundUnsub builds an UNSUBSCRIBE packet for one filter.
func testBoundUnsub(v5 bool, pi uint16, filter string) []byte {
	body := newMQTTWriter(0)
	body.WriteUint16(pi)
	if v5 {
		body.WriteByte(0) // no properties
	}
	body.WriteString(filter)
	pkt := newMQTTWriter(0)
	pkt.WriteByte(mqttPacketUnsub | 0x02)
	pkt.WriteVarInt(body.Len())
	pkt.Write(body.Bytes())
	return pkt.Bytes()
}

// watchDelivery subscribes to the NATS subject an MQTT topic fans out on, so "the PUBLISH
// was actually routed" is observed rather than inferred from its ack. Counting messages on
// $MQTT_msgs would prove nothing here: that stream is interest-retained, so a store with
// no consumer succeeds and keeps nothing.
func (e *testRejectionEnv) watchDelivery(t *testing.T, topic string) *nats.Subscription {
	t.Helper()
	sub, err := e.s.nrc.SubscribeSync(strings.ReplaceAll(topic, "/", "."))
	if err != nil {
		t.Fatalf("subscribe to the delivery subject: %v", err)
	}
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush the subscription: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return sub
}

// deliveryBarrier publishes a sentinel on the same subject and connection the broker's own
// fan-out uses, then returns the FIRST message the watcher sees. One connection preserves
// order, so a sentinel arriving first is proof the gated PUBLISH was never routed — an
// absence check with no barrier would pass against a delivery that is merely slower than
// the test.
func (e *testRejectionEnv) deliveryBarrier(t *testing.T, sub *nats.Subscription, topic string) *nats.Msg {
	t.Helper()
	if err := e.s.nrc.Publish(strings.ReplaceAll(topic, "/", "."), []byte("barrier")); err != nil {
		t.Fatalf("publish the barrier: %v", err)
	}
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush the barrier: %v", err)
	}
	msg, err := sub.NextMsg(testRejectionWait)
	if err != nil {
		t.Fatalf("the barrier itself never arrived, so the fixture proved nothing: %v", err)
	}
	return msg
}

// TestMQTTBoundRejectionForeignStopsPublish is the reproduction the roadmap bullet
// demanded, and the one nine review rounds of earlier attempts never had: the packet is a
// QoS 1 PUBLISH, not a PINGREQ. Being served first is harmless for a PINGREQ, which is
// exactly why every earlier test read past a settlement that ran AFTER the dispatch
// switch; a PUBLISH routed and acked from a connection whose session another broker owns
// cannot be taken back.
//
// The marker is armed the production way — a real remote takeover through a second
// account session manager moves the subject, a real save is CAS-rejected by the stream,
// and the persist notification that would otherwise evict the object is dropped.
func TestMQTTBoundRejectionForeignStopsPublish(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		proto byte
	}{
		{"v5", mqttProtoLevel5},
		{"3.1.1", mqttProtoLevel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, "boundforeign")
			e.dropPersistObservations(t, e.idHash)
			b := e.bindSocket(t, tc.proto)
			b.armWill()

			e.remoteTakeover(t, "node2", "boundforeign")
			e.rejectSave(t, e.sess)

			watch := e.watchDelivery(t, "bound/foreign")
			willWatch := e.watchDelivery(t, "will/topic")
			if err := b.deliver(t, testBoundPublish(tc.proto == mqttProtoLevel5, 1, 1, "bound/foreign", "nope")); err != nil {
				t.Fatalf("the foreign arm must return nil, as the DISCONNECT branch does: %v", err)
			}

			if b.sawPacket(t, mqttPacketPubAck) {
				t.Fatal("a PUBLISH from a displaced connection must not be acked")
			}
			if msg := e.deliveryBarrier(t, watch, "bound/foreign"); string(msg.Data) != "barrier" {
				t.Fatalf("the PUBLISH must not be routed: the watcher saw %q first", msg.Data)
			}
			if !b.closed() {
				t.Fatal("a foreign verdict must close the connection [MQTT-3.1.4-3]")
			}
			if e.resident(e.sess) {
				t.Fatal("a foreign verdict must evict the session object")
			}
			// The Will goes the way a local takeover's does: this connection is being
			// displaced, and the displacement is not a Will publication trigger. Asserted
			// as the SEMANTIC outcome — no publication — because the disposition field is
			// mqttWillDischarged either way once the close bookkeeping's mqttHandleWill
			// has consumed an owed Will, which is another layer's guarantee and would let
			// a skipped takeover discard pass.
			if msg := e.deliveryBarrier(t, willWatch, "will/topic"); string(msg.Data) != "barrier" {
				t.Fatalf("the Will must not be published: the watcher saw %q first", msg.Data)
			}
			if tc.proto == mqttProtoLevel5 {
				if !b.sawDisconnect(t, mqttReasonSessionTakenOver) {
					t.Fatalf("a v5 client must be told 0x8E before the close: wire %x", b.out.bytes())
				}
			} else if len(b.out.bytes()) != 0 {
				t.Fatalf("a 3.1.1 client has no server DISCONNECT: wire %x", b.out.bytes())
			}
		})
	}
}

// TestMQTTBoundRejectionProbeLandsServesPacket is the causal pair's other half. Without
// it, a broker that refused every packet after any rejection would pass the test above:
// the claim is that a LANDED CAS — and only that — resumes service.
func TestMQTTBoundRejectionProbeLandsServesPacket(t *testing.T) {
	e := testNewRejectionEnv(t, "boundlands")
	b := e.bindSocket(t, mqttProtoLevel5)

	// A contended rejection with the record left genuinely untouched: the probe that
	// follows can therefore land.
	restore := testForceContendedRejection(t, e.sess)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the forced save must be rejected")
	}
	restore()
	if !e.markerPending(e.sess) {
		t.Fatal("the forced rejection must arm the marker")
	}
	seqBefore := func() uint64 {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		return e.sess.seq
	}()

	watch := e.watchDelivery(t, "bound/lands")
	if err := b.deliver(t, testBoundPublish(true, 1, 3, "bound/lands", "served")); err != nil {
		t.Fatalf("a landed probe must let the packet through: %v", err)
	}
	if !b.sawPubAckSuccess(t, 3) {
		t.Fatalf("the packet must be served after the probe lands: wire %x", b.out.bytes())
	}
	if msg := e.deliveryBarrier(t, watch, "bound/lands"); string(msg.Data) != "served" {
		t.Fatalf("the PUBLISH must be routed: the watcher saw %q first", msg.Data)
	}
	if b.closed() {
		t.Fatal("a landed probe must leave the connection open")
	}
	e.sess.mu.Lock()
	seqAfter, marker := e.sess.seq, e.sess.recordRejectedSeq
	gates := e.sess.recordRejectionGatesServingLocked()
	e.sess.mu.Unlock()
	if seqAfter <= seqBefore {
		t.Fatalf("the probe must have landed a save: seq %d -> %d", seqBefore, seqAfter)
	}
	if gates {
		t.Fatalf("a landed CAS retires the marker: rejectedSeq=%d seq=%d", marker, seqAfter)
	}
}

// testCountRejectionProbes counts SETTLEMENT ATTEMPTS at the pre-fence window hook, which
// the bound settlement fires exactly once per probe. It is the right counter for "did the
// settlement run at all?": counting record saves cannot answer that on any path where the
// close bookkeeping saves too, and counting evictions cannot distinguish a settlement that
// never ran from one that ran and kept the session.
func testCountRejectionProbes(t *testing.T) (*atomic.Int64, func()) {
	t.Helper()
	var n atomic.Int64
	restore := setMQTTTestRejectionWindowHook(func(_ *mqttSession, window string) {
		if window == mqttRejectionWindowSettle {
			n.Add(1)
		}
	})
	return &n, restore
}

// TestMQTTBoundRejectionContendedFailsClosed pins the no-verdict rule: a probe that is
// CONTENDED (10164 with the record demonstrably still at our base) establishes nothing,
// and nothing is not permission. The connection closes with 0x89 — "try again later" —
// rather than 0x8E, which would announce a takeover no evidence supports.
func TestMQTTBoundRejectionContendedFailsClosed(t *testing.T) {
	e := testNewRejectionEnv(t, "boundcontend")
	b := e.bindSocket(t, mqttProtoLevel5)

	stop := testForceContendedRejection(t, e.sess)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the forced save must be rejected")
	}
	if !e.markerPending(e.sess) {
		t.Fatal("the forced rejection must arm the marker")
	}

	watch := e.watchDelivery(t, "bound/contend")
	err := b.deliver(t, testBoundPublish(true, 1, 5, "bound/contend", "nope"))
	if !errors.Is(err, errMQTTSessionOwnershipUnsettled) {
		t.Fatalf("a contended probe must fail closed on the sentinel: %v", err)
	}
	if b.sawPubAckSuccess(t, 5) {
		t.Fatal("no verdict is not permission: the packet must not be served")
	}
	if msg := e.deliveryBarrier(t, watch, "bound/contend"); string(msg.Data) != "barrier" {
		t.Fatalf("the PUBLISH must not be routed: the watcher saw %q first", msg.Data)
	}
	if !b.closed() {
		t.Fatal("a no-verdict outcome must close the connection")
	}
	if !b.sawDisconnect(t, mqttReasonServerBusy) {
		t.Fatalf("a v5 client must be told 0x89, not 0x8E: wire %x", b.out.bytes())
	}
	if b.sawDisconnect(t, mqttReasonSessionTakenOver) {
		t.Fatal("0x8E claims a takeover nothing proved")
	}
	if !e.markerPending(e.sess) {
		t.Fatal("an unarbitrated question stays outstanding: the marker must still be armed")
	}

	// The retry path is the existing machinery, not this connection: with contention over
	// and the session unbound, the periodic pass settles what the connection could not.
	// While the forced 10164 is still installed every probe is necessarily Keep, so it
	// must come off before this half means anything.
	stop()
	e.asm.reconcileResidentRecordState(e.s)
	if e.markerPending(e.sess) {
		t.Fatal("the periodic pass must settle the marker once the session unbinds")
	}
	if !e.resident(e.sess) {
		t.Fatal("the probe lands on an untouched record, so the session stays ours")
	}
}

// TestMQTTBoundRejectionUncertaintySuspendsProbe covers the conjunct the serve gate
// deliberately does NOT share with the probe's licence. While a write of ours is
// unresolved no probe is even possible — an unresolved session's save is refused at entry
// — so a predicate that read "cannot probe" as "may serve" would serve every packet for as
// long as the ambiguity lasted.
func TestMQTTBoundRejectionUncertaintySuspendsProbe(t *testing.T) {
	e := testNewRejectionEnv(t, "boundsuspend")
	b := e.bindSocket(t, mqttProtoLevel5)

	// The positive control: this fixture answers a PINGREQ when nothing is outstanding.
	// Without it, a broker that refused every packet would pass the assertions below.
	if err := b.deliver(t, []byte{mqttPacketPing, 0}); err != nil {
		t.Fatalf("the control PINGREQ must be served: %v", err)
	}
	if !b.sawPacket(t, mqttPacketPingResp) {
		t.Fatalf("the control PINGREQ must be answered: wire %x", b.out.bytes())
	}
	pingResps := func() int {
		n := 0
		for _, p := range testParseWire(t, b.out.bytes()) {
			if p.typ == mqttPacketPingResp {
				n++
			}
		}
		return n
	}
	before := pingResps()

	stop := testForceContendedRejection(t, e.sess)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the forced save must be rejected")
	}
	stop()
	if !e.markerPending(e.sess) {
		t.Fatal("the forced rejection must arm the marker")
	}
	// Another writer's ambiguity on top of the rejection: the store never reaches the
	// stream, so the session is unresolved with its sequence still at the marker's base.
	mqttTestSessStoreSuppressV.Store(true)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the suppressed save must fail")
	}
	mqttTestSessStoreSuppressV.Store(false)
	if !e.sess.isSeqUncertain() {
		t.Fatal("the suppressed save must leave the session unresolved")
	}

	saves, restoreSaves := testCountSessSaves(t)
	defer restoreSaves()
	probes, restoreProbes := testCountRejectionProbes(t)
	defer restoreProbes()

	err := b.deliver(t, []byte{mqttPacketPing, 0})
	if !errors.Is(err, errMQTTSessionOwnershipUnsettled) {
		t.Fatalf("a suspended settlement must fail closed on the sentinel: %v", err)
	}
	if n := probes.Load(); n != 0 {
		t.Fatalf("no probe is possible while the record is unresolved: %d attempts", n)
	}
	if n := saves.Load(); n != 0 {
		t.Fatalf("a suspended settlement must attempt no record write at all: %d saves", n)
	}
	if pingResps() != before {
		t.Fatalf("the PINGREQ must not be served: wire %x", b.out.bytes())
	}
	if !b.closed() || !b.sawDisconnect(t, mqttReasonServerBusy) {
		t.Fatalf("the connection must close with 0x89: closed=%v wire %x", b.closed(), b.out.bytes())
	}
	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect != nil
	e.sess.mu.Unlock()
	if !parked {
		t.Fatal("a close on an unresolved session must park its obligations for the resolver")
	}
}

// TestMQTTBoundRejectionGatesPerPacket is why the check lives inside the packet loop and
// not at mqttParse's entry. TCP does not preserve write boundaries, so two packets
// routinely arrive in one read — and the FIRST one's own record save is one of the ways
// the marker gets armed in production. An entry check samples the marker before that
// happened and serves the second packet unarbitrated.
func TestMQTTBoundRejectionGatesPerPacket(t *testing.T) {
	e := testNewRejectionEnv(t, "boundcoalesced")
	e.dropPersistObservations(t, e.idHash)
	b := e.bindSocket(t, mqttProtoLevel5)
	// The UNSUBSCRIBE must actually change something, or the session never saves and the
	// marker is never armed: sess.update persists only a real membership change.
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"bound.gone": 0} // membership is keyed by NATS subject
	e.sess.mu.Unlock()
	e.remoteTakeover(t, "node2", "boundcoalesced")

	probes, restoreProbes := testCountRejectionProbes(t)
	defer restoreProbes()
	watch := e.watchDelivery(t, "bound/coalesced")

	// One buffer, two packets: an UNSUBSCRIBE whose record save the moved subject rejects
	// (v5 reports that per filter and still answers, so the packet itself completes and
	// the marker is armed centrally), then a QoS 1 PUBLISH that must be gated.
	buf := append(testBoundUnsub(true, 9, "bound/gone"),
		testBoundPublish(true, 1, 11, "bound/coalesced", "nope")...)
	if err := b.deliver(t, buf); err != nil {
		t.Fatalf("the foreign arm must return nil: %v", err)
	}

	if !b.sawPacket(t, mqttPacketUnsubAck) {
		t.Fatalf("the first packet must have completed, or the fixture proves nothing: wire %x", b.out.bytes())
	}
	if n := probes.Load(); n != 1 {
		t.Fatalf("the second packet must settle the marker the first one armed: %d probes", n)
	}
	if b.sawPubAckSuccess(t, 11) {
		t.Fatal("the second packet in the same buffer must be gated")
	}
	if msg := e.deliveryBarrier(t, watch, "bound/coalesced"); string(msg.Data) != "barrier" {
		t.Fatalf("the second packet must not be routed: the watcher saw %q first", msg.Data)
	}
	if !b.closed() || e.resident(e.sess) {
		t.Fatalf("the foreign verdict must close and evict: closed=%v resident=%v", b.closed(), e.resident(e.sess))
	}
}

// TestMQTTBoundRejectionGatesPingReq pins the packet type every earlier attempt used as its
// only test — and exempting it would be tempting, since a PINGREQ routes nothing. It is
// gated because it is what BOUNDS convergence: a conformant client sends one within its
// keepalive, so an otherwise silent displaced connection still closes.
func TestMQTTBoundRejectionGatesPingReq(t *testing.T) {
	t.Parallel()
	e := testNewRejectionEnv(t, "boundping")
	e.dropPersistObservations(t, e.idHash)
	b := e.bindSocket(t, mqttProtoLevel5)
	e.remoteTakeover(t, "node2", "boundping")
	e.rejectSave(t, e.sess)

	if err := b.deliver(t, []byte{mqttPacketPing, 0}); err != nil {
		t.Fatalf("the foreign arm must return nil: %v", err)
	}
	if b.sawPacket(t, mqttPacketPingResp) {
		t.Fatalf("a PINGREQ must be gated too: wire %x", b.out.bytes())
	}
	if !b.sawDisconnect(t, mqttReasonSessionTakenOver) || !b.closed() || e.resident(e.sess) {
		t.Fatalf("the settlement must run on a PINGREQ: closed=%v resident=%v wire %x",
			b.closed(), e.resident(e.sess), b.out.bytes())
	}
}

// TestMQTTBoundRejectionDisconnectExempt pins the one packet type the gate must never
// refuse. A DISCONNECT routes nothing and acks nothing; what it carries is the client's own
// instruction about its Will and the teardown that ENDS the split-brain. Fail-closed on it
// would manufacture a Will publication the client explicitly forbade — from a session that
// may well still be ours — and would refuse the packet that converges the very condition
// the gate exists for.
func TestMQTTBoundRejectionDisconnectExempt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		packet    []byte
		published bool
	}{
		{"normal DISCONNECT discards the Will", []byte{mqttPacketDisconnect, 0}, false},
		{"reason 0x04 publishes it", []byte{mqttPacketDisconnect, 1, mqttReasonDisconnectWithWill}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, "bounddisc")
			e.dropPersistObservations(t, e.idHash)
			b := e.bindSocket(t, mqttProtoLevel5)
			b.armWill()
			e.remoteTakeover(t, "node2", "bounddisc")
			e.rejectSave(t, e.sess)

			probes, restore := testCountRejectionProbes(t)
			defer restore()
			watch := e.watchDelivery(t, "will/topic")

			if err := b.deliver(t, tc.packet); err != nil {
				t.Fatalf("a DISCONNECT must be dispatched, not refused: %v", err)
			}
			if n := probes.Load(); n != 0 {
				t.Fatalf("a DISCONNECT must not be gated: %d probes", n)
			}
			if !b.closed() {
				t.Fatal("a DISCONNECT tears the connection down")
			}
			if b.sawDisconnect(t, mqttReasonServerBusy) || b.sawDisconnect(t, mqttReasonSessionTakenOver) {
				t.Fatalf("a client that said goodbye is told nothing: wire %x", b.out.bytes())
			}
			// The SEMANTIC outcome, not the disposition field: this same dispatch runs
			// mqttHandleClosedClient, whose mqttHandleWill consumes an owed Will on the
			// spot, so both variants end at mqttWillDischarged.
			msg := e.deliveryBarrier(t, watch, "will/topic")
			if published := string(msg.Data) == "bye"; published != tc.published {
				t.Fatalf("Will published=%v, want %v (watcher saw %q first)", published, tc.published, msg.Data)
			}
		})
	}
}

// TestMQTTBoundRejectionGateRefused covers the claim being REFUSED, which is two different
// facts wearing one error and must not be collapsed into one answer.
//
// A CONNECT registers the shared client-ID gate for its whole duration and rebinds the
// session only part-way through, and a resolver can hold the gate for a session that stays
// OURS. So the settlement retries while this connection is still the owner and classifies
// the outcome:
//
//   - still ours when the bounded spin gives up: nothing observable is displacing us, so
//     this is an ordinary no-verdict outcome and says 0x89;
//   - REBOUND: a takeover owns this connection's fate. It is told nothing (the takeover
//     sends its own 0x8E, and a courtesy 0x89 would contradict it on the same socket), and
//     its Will is settled by the takeover's own rule rather than by the close bookkeeping —
//     which marks a Will OWED before it contends for the gate, so an early close would
//     otherwise publish a Will the takeover was about to discard.
func TestMQTTBoundRejectionGateRefused(t *testing.T) {
	t.Run("held but still ours: 0x89", func(t *testing.T) {
		e := testNewRejectionEnv(t, "boundgateheld")
		e.dropPersistObservations(t, e.idHash)
		b := e.bindSocket(t, mqttProtoLevel5)
		e.remoteTakeover(t, "node2", "boundgateheld")
		e.rejectSave(t, e.sess)

		// The gate is held by an operation that is NOT displacing this connection — a
		// resolver, or a CONNECT that has not rebound yet.
		e.asm.mu.Lock()
		e.asm.sessLocked[e.sess.id] = struct{}{}
		// The close bookkeeping retries the teardown claim for as long as this connection
		// is still the session's owner, so a fixture that never released would hang by
		// construction. Release from the seam that fires before that loop.
		var released atomic.Bool
		e.asm.testBeforeTeardownLock = func(string) {
			if released.CompareAndSwap(false, true) {
				e.asm.mu.Lock()
				delete(e.asm.sessLocked, e.sess.id)
				e.asm.mu.Unlock()
			}
		}
		e.asm.mu.Unlock()
		defer func() {
			e.asm.mu.Lock()
			delete(e.asm.sessLocked, e.sess.id)
			e.asm.mu.Unlock()
		}()

		probes, restore := testCountRejectionProbes(t)
		defer restore()

		err := b.deliver(t, []byte{mqttPacketPing, 0})
		if !errors.Is(err, errMQTTSessionOwnershipUnsettled) {
			t.Fatalf("a gate held by something that is not displacing us is a no-verdict "+
				"outcome, not a displacement: %v", err)
		}
		if n := probes.Load(); n != 0 {
			t.Fatalf("a refused claim probes nothing: %d probes", n)
		}
		if !released.Load() {
			t.Fatal("the close bookkeeping never ran, so the fixture proved nothing")
		}
		if b.sawPacket(t, mqttPacketPingResp) {
			t.Fatalf("the packet must not be served: wire %x", b.out.bytes())
		}
		if !b.closed() || !b.sawDisconnect(t, mqttReasonServerBusy) {
			t.Fatalf("the connection must close with 0x89: closed=%v wire %x", b.closed(), b.out.bytes())
		}
	})

	t.Run("held then released: the settlement waits and probes", func(t *testing.T) {
		e := testNewRejectionEnv(t, "boundgatewait")
		b := e.bindSocket(t, mqttProtoLevel5)

		stop := testForceContendedRejection(t, e.sess)
		if err := testSessSave(t, e.sess); err == nil {
			t.Fatal("the forced save must be rejected")
		}
		stop()
		if !e.markerPending(e.sess) {
			t.Fatal("the forced rejection must arm the marker")
		}

		// A competing operation holds the gate briefly and leaves the session ours. The
		// settlement must WAIT it out rather than read "could not ask" as a verdict:
		// giving up immediately would close a healthy connection over contention that was
		// about to clear.
		e.asm.mu.Lock()
		e.asm.sessLocked[e.sess.id] = struct{}{}
		e.asm.mu.Unlock()
		go func() {
			time.Sleep(2 * mqttTeardownLockItvl)
			e.asm.mu.Lock()
			delete(e.asm.sessLocked, e.sess.id)
			e.asm.mu.Unlock()
		}()

		if err := b.deliver(t, testBoundPublish(true, 1, 51, "bound/gatewait", "served")); err != nil {
			t.Fatalf("the settlement must wait for the gate and probe: %v", err)
		}
		if !b.sawPubAckSuccess(t, 51) {
			t.Fatalf("the probe lands once the gate clears, so the packet is served: wire %x", b.out.bytes())
		}
		if b.closed() {
			t.Fatal("transient contention must not close a connection whose probe then lands")
		}
	})

	t.Run("rebound by a takeover: silent, and the Will is the takeover's", func(t *testing.T) {
		e := testNewRejectionEnv(t, "boundrebound")
		e.dropPersistObservations(t, e.idHash)
		b := e.bindSocket(t, mqttProtoLevel5)
		b.armWill()
		e.remoteTakeover(t, "node2", "boundrebound")
		e.rejectSave(t, e.sess)

		// A local CONNECT mid-takeover: it holds the gate for its whole duration and has
		// already rebound the session, but has NOT yet reached its own Will discard.
		newConn := &client{srv: e.s, acc: e.s.globalAccount(), nrc: e.s.nrc}
		newConn.mqtt = &mqtt{sess: e.sess, asm: e.asm, proto: mqttProtoLevel5, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
		e.asm.mu.Lock()
		e.asm.sessLocked[e.sess.id] = struct{}{}
		e.asm.mu.Unlock()
		e.sess.mu.Lock()
		e.sess.c = newConn
		e.sess.mu.Unlock()
		defer func() {
			e.asm.mu.Lock()
			delete(e.asm.sessLocked, e.sess.id)
			e.asm.mu.Unlock()
		}()

		probes, restore := testCountRejectionProbes(t)
		defer restore()
		willWatch := e.watchDelivery(t, "will/topic")

		start := time.Now()
		if err := b.deliver(t, []byte{mqttPacketPing, 0}); err != nil {
			t.Fatalf("a displaced connection is closed, not errored: %v", err)
		}
		// The rebind is observed on the first attempt, so this must not sit out the
		// bounded spin.
		if waited := time.Since(start); waited > mqttTeardownLockAttempts*mqttTeardownLockItvl/2 {
			t.Fatalf("a rebind must be observed immediately, not waited out: took %v", waited)
		}
		if n := probes.Load(); n != 0 {
			t.Fatalf("a refused claim probes nothing: %d probes", n)
		}
		if b.sawPacket(t, mqttPacketPingResp) {
			t.Fatalf("the packet must not be served: wire %x", b.out.bytes())
		}
		if !b.closed() {
			t.Fatal("the connection must be closed")
		}
		if len(b.out.bytes()) != 0 {
			t.Fatalf("a displaced connection must be told nothing at all: wire %x", b.out.bytes())
		}
		// The Will is the takeover's to decide, and its rule is to discard an armed one.
		// Without this the close bookkeeping would have marked it OWED before contending
		// for the gate, and the takeover's later discard preserves an owed Will by design
		// — so the Will of a connection a takeover displaced would be published.
		if msg := e.deliveryBarrier(t, willWatch, "will/topic"); string(msg.Data) != "barrier" {
			t.Fatalf("a displaced connection's armed Will must not be published: the "+
				"watcher saw %q first", msg.Data)
		}
	})
}

// TestMQTTBoundRejectionDisplacedSaysNothing is the same rule against the persist
// observation route, which never CLAIMS the ownership gate (its eviction worker observes a
// held gate and defers to it instead of overlapping its holder): the worker evicts the
// session, clears sess.c, enqueues its own classified notification and closes
// ASYNCHRONOUSLY.
//
// Two orderings, and the second is the one with teeth. Waiting for the eviction to become
// visible usually waits past its close too — the close marks the client closed before it
// touches the socket, and testWireCapture then rejects every later write — so in that
// state a gate arm that closed nothing, or that wrote an extra DISCONNECT, would look
// exactly like a correct one. The second subtest therefore stages the state the eviction
// leaves BEFORE its close lands, using the production halves of that tail, and owns the
// window.
func TestMQTTBoundRejectionDisplacedSaysNothing(t *testing.T) {
	t.Parallel()
	t.Run("after the eviction completed", func(t *testing.T) {
		e := testNewRejectionEnv(t, "bounddisplaced")
		b := e.bindSocket(t, mqttProtoLevel5)

		// The persist observation is NOT dropped here: this is the delivered-notification
		// ordering, which is the common one — the same remote save both moves the record
		// (arming the marker through the rejected save below) and notifies this broker.
		e.remoteTakeover(t, "node2", "bounddisplaced")
		e.rejectSave(t, e.sess)

		// Asynchronous by design: the observation is a core-NATS delivery, so this is a
		// bounded poll, the same shape TestMQTTRejectedRecordNotificationDelivered uses.
		deadline := time.Now().Add(testRejectionWait)
		for {
			e.sess.mu.Lock()
			bound := e.sess.c
			e.sess.mu.Unlock()
			if bound == nil && !e.resident(e.sess) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the persist observation must evict the displaced local session")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !b.sawDisconnect(t, mqttReasonSessionTakenOver) {
			t.Fatalf("the eviction must have told the client 0x8E: wire %x", b.out.bytes())
		}

		// What this ordering can still show: the gate refuses to SERVE, and answers with
		// no error — a sentinel here would have appended 0x89 to that 0x8E.
		if err := b.deliver(t, testBoundPublish(true, 1, 41, "bound/displaced", "nope")); err != nil {
			t.Fatalf("a displaced connection is closed, not errored: %v", err)
		}
		if b.sawPubAckSuccess(t, 41) {
			t.Fatal("a displaced connection must not be served")
		}
		if b.sawDisconnect(t, mqttReasonServerBusy) {
			t.Fatalf("0x89 must not follow the takeover's 0x8E: wire %x", b.out.bytes())
		}
	})

	t.Run("inside the eviction's enqueue-to-close window", func(t *testing.T) {
		e := testNewRejectionEnv(t, "boundwindowdisp")
		e.dropPersistObservations(t, e.idHash)
		b := e.bindSocket(t, mqttProtoLevel5)
		e.remoteTakeover(t, "node2", "boundwindowdisp")
		e.rejectSave(t, e.sess)

		// The two production halves of the eviction's tail, without its `go
		// closeConnection`: the session evicted and detached, the takeover DISCONNECT
		// already on the wire, the socket still open. That is the instant the displaced
		// readLoop can reach this gate in.
		stale, applied := e.sess.disposeForeignStateLocked(e.asm, nil)
		if !applied || stale != b.bc {
			t.Fatalf("the fixture must stage the eviction's own state: applied=%v stale=%p", applied, stale)
		}
		stale.mqttNotifyForeignDisposal(e.sess.id, mqttDisposalTakenOver)
		if !b.sawDisconnect(t, mqttReasonSessionTakenOver) {
			t.Fatalf("the staged eviction must have written 0x8E: wire %x", b.out.bytes())
		}
		if b.closed() {
			t.Fatal("the window this test exists for is the one BEFORE the close: it is already shut")
		}
		before := len(testParseWire(t, b.out.bytes()))

		if err := b.deliver(t, testBoundPublish(true, 1, 43, "bound/windowdisp", "nope")); err != nil {
			t.Fatalf("a displaced connection is closed, not errored: %v", err)
		}
		if b.sawPubAckSuccess(t, 43) {
			t.Fatal("a displaced connection must not be served")
		}
		if !b.closed() {
			t.Fatal("the gate must close a displaced connection itself: the eviction's own " +
				"close is asynchronous, and until it lands this readLoop keeps parsing")
		}
		if after := len(testParseWire(t, b.out.bytes())); after != before {
			t.Fatalf("nothing may follow the takeover DISCONNECT: %d packets became %d (wire %x)",
				before, after, b.out.bytes())
		}
	})
}

// TestMQTTBoundRejectionKeepRetiredInWindow and its sibling below pin the two ways a
// post-fence Keep can end. Between the caller's pre-gate sample of the marker and the fence
// that makes a sample sound, another writer can land a save — which settles the ownership
// question without any probe at all. The re-read after the fence is what notices, and it
// re-reads the SERVE GATE rather than the probe's licence.
func TestMQTTBoundRejectionKeepRetiredInWindow(t *testing.T) {
	e := testNewRejectionEnv(t, "boundwindowlands")
	b := e.bindSocket(t, mqttProtoLevel5)

	stop := testForceContendedRejection(t, e.sess)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the forced save must be rejected")
	}
	stop()
	if !e.markerPending(e.sess) {
		t.Fatal("the forced rejection must arm the marker")
	}

	saves, restoreSaves := testCountSessSaves(t)
	defer restoreSaves()
	var once atomic.Bool
	restoreWindow := setMQTTTestRejectionWindowHook(func(sess *mqttSession, window string) {
		if window != mqttRejectionWindowSettle || sess != e.sess || !once.CompareAndSwap(false, true) {
			return
		}
		// A save LANDS in the window, moving the sequence past the marker's base.
		if err := testSessSave(t, sess); err != nil {
			t.Errorf("the in-window save must land: %v", err)
		}
	})
	defer restoreWindow()

	if err := b.deliver(t, testBoundPublish(true, 1, 21, "bound/window", "served")); err != nil {
		t.Fatalf("a retired marker must let the packet through: %v", err)
	}
	if !once.Load() {
		t.Fatal("the window never fired, so the fixture proved nothing")
	}
	if !b.sawPubAckSuccess(t, 21) {
		t.Fatalf("the packet must be served: wire %x", b.out.bytes())
	}
	if b.closed() {
		t.Fatal("nothing was outstanding by the time the fence was held: the connection stays")
	}
	// Exactly the in-window save: the in-fence re-read found nothing pending, so the probe
	// itself never ran. Two saves would mean the settlement probed anyway.
	if n := saves.Load(); n != 1 {
		t.Fatalf("the probe must be skipped once the marker is retired: %d record saves", n)
	}
}

// TestMQTTBoundRejectionKeepSuspendedInWindow is the other post-fence Keep: what landed in
// the window was an AMBIGUITY, not a save. The probe is impossible and the question is
// still outstanding, so the same Keep verdict must fail closed here — which is exactly why
// the re-read cannot be the probe's licence, whose third conjunct this case clears.
func TestMQTTBoundRejectionKeepSuspendedInWindow(t *testing.T) {
	e := testNewRejectionEnv(t, "boundwindowuncertain")
	b := e.bindSocket(t, mqttProtoLevel5)

	stop := testForceContendedRejection(t, e.sess)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the forced save must be rejected")
	}
	stop()
	if !e.markerPending(e.sess) {
		t.Fatal("the forced rejection must arm the marker")
	}

	var once atomic.Bool
	restoreWindow := setMQTTTestRejectionWindowHook(func(sess *mqttSession, window string) {
		if window != mqttRejectionWindowSettle || sess != e.sess || !once.CompareAndSwap(false, true) {
			return
		}
		mqttTestSessStoreSuppressV.Store(true)
		if err := testSessSave(t, sess); err == nil {
			t.Error("the suppressed in-window save must fail")
		}
		mqttTestSessStoreSuppressV.Store(false)
	})
	defer restoreWindow()

	err := b.deliver(t, testBoundPublish(true, 1, 23, "bound/windowu", "nope"))
	if !errors.Is(err, errMQTTSessionOwnershipUnsettled) {
		t.Fatalf("an ambiguity in the window must fail closed on the sentinel: %v", err)
	}
	if !once.Load() {
		t.Fatal("the window never fired, so the fixture proved nothing")
	}
	if b.sawPubAckSuccess(t, 23) {
		t.Fatal("the packet must not be served")
	}
	if !b.closed() || !b.sawDisconnect(t, mqttReasonServerBusy) {
		t.Fatalf("the connection must close with 0x89: closed=%v wire %x", b.closed(), b.out.bytes())
	}
	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect != nil
	e.sess.mu.Unlock()
	if !parked {
		t.Fatal("a close on an unresolved session must park its obligations")
	}
}

// TestMQTTBoundRejectionNoReentryAfterForeign runs the REAL readLoop, because what it
// asserts is the shape of the foreign arm's tail rather than the content of a packet: the
// 0x8E is written and the connection CLOSED before mqttParse returns, so the loop's next
// Read fails instead of parsing whatever the client sent next.
//
// Closing asynchronously — the shape every off-readLoop disposal caller uses, and which
// this arm deliberately does not — would let the loop re-enter and turn a queued packet
// into a second, contradictory DISCONNECT (0x89 "try again later" after 0x8E "taken
// over"), or close the socket before the 0x8E reached the client at all.
//
// The second packet is staged ACROSS THE SOCKET rather than appended to the first buffer:
// the foreign arm returns nil from mid-parse, so a same-buffer remainder is discarded
// whichever way the tail closes, and would discriminate nothing.
func TestMQTTBoundRejectionNoReentryAfterForeign(t *testing.T) {
	t.Parallel()
	e := testNewRejectionEnv(t, "boundnoreentry")
	e.dropPersistObservations(t, e.idHash)

	brokerSide, clientSide := net.Pipe()
	defer clientSide.Close()
	// rawConn is what closeConnection closes; a fixture that left it unwired would show a
	// connection that never dies no matter which tail ran.
	bc := &client{srv: e.s, acc: e.s.globalAccount(), nrc: e.s.nrc, nc: brokerSide, rawConn: brokerSide,
		mpay: 1024 * 1024}
	bc.mqtt = &mqtt{sess: e.sess, asm: e.asm, proto: mqttProtoLevel5, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
	bc.initClient()
	bc.mu.Lock()
	bc.flags.set(connectReceived)
	bc.mu.Unlock()
	bc.connAckWritten.Store(true)
	e.sess.mu.Lock()
	e.sess.c = bc
	e.sess.mu.Unlock()

	e.remoteTakeover(t, "node2", "boundnoreentry")
	e.rejectSave(t, e.sess)

	e.s.clientWG.Add(1) // readLoop's defer releases the lifecycle token
	go bc.readLoop(nil)

	_ = clientSide.SetWriteDeadline(time.Now().Add(testRejectionWait))
	if _, err := clientSide.Write(testBoundPublish(true, 1, 31, "bound/noreentry", "nope")); err != nil {
		t.Fatalf("write the triggering PUBLISH: %v", err)
	}

	// Read until the takeover DISCONNECT arrives.
	var seen []byte
	deadline := time.Now().Add(testRejectionWait)
	buf := make([]byte, 256)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("no 0x8E arrived before the connection went away: read %x", seen)
		}
		_ = clientSide.SetReadDeadline(time.Now().Add(time.Second))
		n, err := clientSide.Read(buf)
		seen = append(seen, buf[:n]...)
		if mqttTestSawDisconnect(t, seen, mqttReasonSessionTakenOver) {
			break
		}
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the connection was closed before the 0x8E: read %x (%v)", seen, err)
		}
	}

	// The connection is closed by the time mqttParse returns, so a client that keeps
	// talking is severed rather than served. With an asynchronous close the readLoop can
	// be back in Read, and this write is accepted and parsed.
	_ = clientSide.SetWriteDeadline(time.Now().Add(testRejectionWait))
	if _, err := clientSide.Write(testBoundPublish(true, 1, 33, "bound/noreentry", "again")); err == nil {
		t.Fatal("a packet sent after the 0x8E must not be accepted: the connection is closed")
	}

	_ = clientSide.SetReadDeadline(time.Now().Add(time.Second))
	for {
		n, err := clientSide.Read(buf)
		seen = append(seen, buf[:n]...)
		if err != nil {
			break
		}
	}
	if mqttTestSawDisconnect(t, seen, mqttReasonServerBusy) {
		t.Fatalf("a second DISCONNECT contradicting the first: read %x", seen)
	}
}

// mqttTestSawDisconnect reports whether the bytes read so far contain a complete server
// DISCONNECT with this reason code. Tolerates a trailing partial packet.
func mqttTestSawDisconnect(t *testing.T, data []byte, reason byte) bool {
	t.Helper()
	for i := 0; i < len(data); {
		typ := data[i] & mqttPacketMask
		i++
		var pl, mult int
		for {
			if i >= len(data) {
				return false
			}
			b := data[i]
			i++
			pl += int(b&0x7f) * (1 << mult)
			if b&0x80 == 0 {
				break
			}
			mult += 7
		}
		if i+pl > len(data) {
			return false
		}
		if typ == mqttPacketDisconnect && pl > 0 && data[i] == reason {
			return true
		}
		i += pl
	}
	return false
}
