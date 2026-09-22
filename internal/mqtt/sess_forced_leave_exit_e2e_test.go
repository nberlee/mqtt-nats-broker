//go:build mqttport_fulltests

// End-to-end tests for the 3.1.1 forced leave's EXIT DISPOSITION: which save outcome refuses
// the CONNECT and which one lets it live.
//
// The defect: the leave's record write had two failure arms and BOTH returned an error from
// mqttProcessConnect, which closes. So a client that had just been acknowledged (Session
// Present = 1) got a dead socket and its Will was published — because the broker could not
// persist its own hygiene write. The leave is broker-side hygiene the client neither asked for
// nor can observe, so its failure owes the client nothing.
//
// What makes the fix small rather than sprawling: for THIS write, saveDefinitelyFailed cannot
// contain a transient store failure at all. persistRecord arms uncertainty for every
// non-CAS-rejection store error, so an ordinary "JetStream was briefly unavailable" classifies
// as AMBIGUOUS. What is left definite is 10071, 10164, and refusals that never reached
// JetStream — and each of those is a case where refusing is the right answer.

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// setMQTTTestRestoreOpenHook installs (or, with nil, clears) the gate-open hook and
// returns a function that restores the previous value.
//
// It lives HERE, beside its only caller, rather than next to
// mqttTestRestoreOpenHookV in sess_restore_gate.go. The variable belongs there because
// openRestoreGate reads it; the setter is test-only and reachable from no default
// build, so leaving it in a production file made `unused` right to flag it.
func setMQTTTestRestoreOpenHook(h func()) func() {
	prev := mqttTestRestoreOpenHookV.Load()
	if h == nil {
		mqttTestRestoreOpenHookV.Store(nil)
	} else {
		mqttTestRestoreOpenHookV.Store(&h)
	}
	return func() { mqttTestRestoreOpenHookV.Store(prev) }
}

// testForcedLeaveSession drives a v5 session that owns a shared QoS 1 subscription, then
// disconnects it cleanly so the next CONNECT is a resume rather than a takeover race.
//
// The returned options are the server's, for the 3.1.1 resume the caller then performs.
func testForcedLeaveSession(t *testing.T, o *Options, cid, filter string) {
	t.Helper()
	ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(300)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	// QoS 1 so a group durable AND an in-memory cons entry exist.
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}})
	testMQTTDisconnect(t, c, nil)
}

// TestForcedLeaveAmbiguousSaveKeepsTheConnection is the arm that carries the defect's weight.
//
// An ambiguous forced-leave save is the ORDINARY failure — any non-CAS store error arms
// uncertainty — and it used to close an acknowledged connection. It is now resolved IN LINE
// under the CONNECT's own accept transaction, and the connection survives.
//
// The resolution is what the old code could not do: resolveUncertainNow takes the ownership
// gate this CONNECT already holds (asm.sessLocked) and needs a record fence this frame already
// owns, so it returned "unresolved" without attempting anything at all.
func TestForcedLeaveAmbiguousSaveKeepsTheConnection(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared, plain = "fl-ambig", "$share/g/fl/ambig", "fl/plain"
	testForcedLeaveSession(t, o, cid, shared)

	// Give the resumed session a plain subscription too, so "the connection still works" is
	// observable rather than merely "the socket is open".
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rp)
	testMQTTSubV5(t, cp, rp, 1, []mqttV5SubFilter{{topic: plain, opts: 1}})
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// Make the forced-leave save AMBIGUOUS exactly once: the request goes out and its outcome
	// is unknown, which is what markWriteUncertainLocked records.
	fired, restore := testMQTTFailSessStoreOnce(t, func(sess *mqttSession) bool {
		if sess.id != cid {
			return false
		}
		sess.mu.Lock()
		leaving := len(sess.sharedSubs) == 0
		sess.mu.Unlock()
		return leaving
	})
	defer restore()

	// Record the ORDER of the two events, not just that both happened. The commit must precede
	// the gate open: the gate is permission to transmit, and it may not be granted while the
	// group state this resume is required to release is still in place.
	var order []string
	var orderMu sync.Mutex
	note := func(what string) {
		orderMu.Lock()
		order = append(order, what)
		orderMu.Unlock()
	}
	committed := make(chan struct{})
	var commitOnce sync.Once
	restoreCommit := setMQTTTestForcedLeaveCommitHook(func() {
		note("commit")
		commitOnce.Do(func() { close(committed) })
	})
	defer restoreCommit()
	opened := make(chan struct{})
	var openOnce sync.Once
	restoreOpen := setMQTTTestRestoreOpenHook(func() {
		note("open")
		openOnce.Do(func() { close(opened) })
	})
	defer restoreOpen()

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	testForcedLeaveWaitFired(t, fired)
	// THE CONNECTION IS STILL ALIVE. Before this change the CONNACK was followed by a close.
	testMQTTFlush(t, c3, nil, r3)
	testMQTTExpectNothing(t, r3)

	// AND THE LEAVE ACTUALLY COMPLETED, observed through the commit itself.
	//
	// Every indirect observable here is a trap, and two were tried and rejected. The in-memory
	// membership reads empty whether or not the commit ran, because the STAGING half nils it
	// before the save — an assertion on it cannot see the commit at all. The group durable's
	// release is also performed later by the orphaned-consumer sweep, so watching it passes on
	// somebody else's work. Only the commit hook observes the commit.
	//
	// It must have run by the time the CONNECT returns, not eventually: the settlement is
	// in line under the accept transaction, and the restore gate opens after it. A commit that
	// happens later means the gate opened over unreleased group state.
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("the forced leave's commit never ran during the in-line settlement: the " +
			"ambiguity was resolved but nothing released the group state, so the memberships " +
			"are gone from the record while their durables, configs and pending deliveries " +
			"remain — and the restore gate opened over exactly that")
	}
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("the restore verdict never opened, so the session's withheld deliveries are " +
			"still parked")
	}
	// THE ORDER. "Both happened" is satisfied by a commit that ran LATER, drained by some
	// unrelated ambiguity under a transaction this CONNECT never opened — which is exactly what
	// registering the landed continuation after the resolve produces. The gate would then have
	// granted permission to transmit while the group state was still in place.
	orderMu.Lock()
	got := append([]string(nil), order...)
	orderMu.Unlock()
	if len(got) < 2 || got[0] != "commit" {
		t.Fatalf("event order was %v, want the commit strictly before the gate open: the gate is "+
			"permission to transmit, and it must not be granted while the group state this "+
			"resume is required to release is still in place", got)
	}
}

// TestForcedLeavePreStoreRefusalStillRefuses is the discriminating pair for the test above, and
// it exists because the two arms must not be able to pass on each other's mechanism.
//
// A refusal that never reached JetStream — the save hook is exactly that shape, since
// persistRecord consults it before the store and therefore never sets detail.rawErr nor arms
// uncertainty — is DEFINITE and is refused, with the removal rolled back inside the still-open
// transaction. That rollback is saveDefinitelyFailed's stated contract, and dropping it would
// leave a permanent session resident with the membership removed in memory only while the
// record still names it.
func TestForcedLeavePreStoreRefusalStillRefuses(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-prestore", "$share/g/fl/prestore"
	testForcedLeaveSession(t, o, cid, shared)

	var fired bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid {
			return nil
		}
		sess.mu.Lock()
		leaving := len(sess.sharedSubs) == 0
		sess.mu.Unlock()
		if !leaving || fired {
			return nil
		}
		fired = true
		return errMQTTForcedLeaveTestRefusal
	})
	defer restoreSave()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTExpectDisconnect(t, c2)
	if !fired {
		t.Fatal("the pre-store refusal never fired; the test proved nothing")
	}
	restoreSave()

	// THE ROLLBACK: the record still names the membership, so a v5 resume gets it back. If the
	// removal had been left applied in memory only, a later save would have persisted a session
	// whose shared subscription had silently vanished.
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	got := len(sess.sharedSubs)
	sess.mu.Unlock()
	if got != 1 {
		t.Fatalf("after a definite refusal the session holds %d shared membership(s), want 1: "+
			"saveDefinitelyFailed requires the caller to undo its mutation", got)
	}
}

// TestForcedLeaveDoesNotPublishTheWillWhileTheConnectionLives is the harm the roadmap bullet
// names, asserted directly: the Will announces to other clients that this client died, and it
// fired because the BROKER could not persist a bookkeeping write.
//
// Suppressing the Will is not the fix and never was — [MQTT-3.1.2-8] requires it on a close not
// preceded by a DISCONNECT — so the fix is to stop taking that exit, which is what the
// ambiguous arm now does.
func TestForcedLeaveDoesNotPublishTheWillWhileTheConnectionLives(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-will", "$share/g/fl/will"
	const willTopic = "fl/will/topic"
	testForcedLeaveSession(t, o, cid, shared)

	// A witness for the Will.
	cw, rw := testMQTTConnect(t, &mqttConnInfo{clientID: "fl-will-witness", cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer cw.Close()
	testMQTTCheckConnAck(t, rw, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cw, rw, []*mqttFilter{{filter: willTopic, qos: 0}}, []byte{0})
	testMQTTFlush(t, cw, nil, rw)

	fired, restore := testMQTTFailSessStoreOnce(t, func(sess *mqttSession) bool {
		if sess.id != cid {
			return false
		}
		sess.mu.Lock()
		leaving := len(sess.sharedSubs) == 0
		sess.mu.Unlock()
		return leaving
	})
	defer restore()

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false,
		will: &mqttWill{topic: []byte(willTopic), message: []byte("gone"), qos: 0}},
		o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	testForcedLeaveWaitFired(t, fired)
	// No Will, because the connection was not closed.
	testMQTTExpectNothingFor(t, rw, 500*time.Millisecond)
}

// TestForcedLeaveExpiryIsNeverZeroOnThisPath pins A3, a claim in the roadmap bullet that turned
// out to be VACUOUS on this path — recorded as a test rather than deleted, so a later change
// that makes it reachable fails here instead of passing quietly.
//
// The bullet inherited "and can delete a zero-expiry session" from F-064's general review of
// post-CONNACK failure. It cannot happen here: a non-clean 3.1.1 CONNECT maps to
// mqttSessionNeverExpire, and a clean start has no persisted membership to leave — so
// len(sharedSubs) > 0 and expiry == 0 cannot co-occur.
func TestForcedLeaveExpiryIsNeverZeroOnThisPath(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-expiry", "$share/g/fl/expiry"
	testForcedLeaveSession(t, o, cid, shared)

	var seen bool
	var interval uint32
	var clean bool
	restore := setMQTTTestRestorePause(func(at string) {
		if at != "before-connack" {
			return
		}
		sess := testMQTTResidentSessionOrNil(testMQTTGetGlobalASM(t, s), cid)
		if sess == nil {
			return
		}
		sess.mu.Lock()
		if len(sess.sharedSubs) > 0 {
			seen, interval, clean = true, sess.expiryInterval, sess.clean
		}
		sess.mu.Unlock()
	})
	defer restore()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	if !seen {
		t.Fatal("the pause never observed a resume carrying a shared membership; the premise " +
			"this test pins was never exercised")
	}
	if interval == 0 || clean {
		t.Fatalf("a non-clean 3.1.1 resume carrying a shared membership had expiryInterval=%d "+
			"clean=%v, want a non-zero interval and clean=false: the roadmap's "+
			"'deletes a zero-expiry session' harm is unreachable on this path, and if that "+
			"stops being true the forced leave's refusal arms become destructive", interval, clean)
	}
}

// errMQTTForcedLeaveTestRefusal is a PRE-STORE failure, and the distinction is the whole point
// of the fixture that uses it: persistRecord consults the save hook before the store, so a hook
// error never sets detail.rawErr and never arms uncertainty. It is therefore a definite refusal
// that reached no stream at all — not a CAS rejection, and not an ambiguity.
var errMQTTForcedLeaveTestRefusal = errors.New("simulated pre-store refusal of the forced leave")

// errMQTTForcedLeaveTestAmbiguous stands in for a lost store response. It is deliberately NOT a
// CAS rejection: mqttIsRecordCASRejection tests for 10071 and 10164 specifically, so any other
// store error takes persistRecord's else branch, arms uncertainty, and classifies as
// saveMaybeLanded.
var errMQTTForcedLeaveTestAmbiguous = errors.New("simulated lost response for the forced leave")

// testMQTTFailSessStoreOnce makes the NEXT session-record store whose session satisfies `when`
// look AMBIGUOUS: the store result is replaced with a timeout-shaped error, which is not a CAS
// rejection, so persistRecord arms uncertainty and classifySave returns saveMaybeLanded.
//
// It hooks the store RESULT rather than the save entry point deliberately. A hook at the entry
// point cannot produce this outcome at all — it returns before the store, so it can only ever
// simulate a pre-store refusal — and a test that used one while claiming to exercise the
// ambiguous arm would pass against the wrong code path.
func testMQTTFailSessStoreOnce(t *testing.T, when func(*mqttSession) bool) (<-chan struct{}, func()) {
	t.Helper()
	var once sync.Once
	fired := make(chan struct{})
	h := func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if when(sess) {
			once.Do(func() { close(fired) })
			return nil, errMQTTForcedLeaveTestAmbiguous
		}
		return resp, err
	}
	mqttTestSessStoreResultHookV.Store(&h)
	restore := func() { mqttTestSessStoreResultHookV.Store(nil) }
	t.Cleanup(restore)
	return fired, restore
}

// testForcedLeaveWaitFired waits for the injected forced-leave failure to actually happen.
//
// The wait is load-bearing rather than defensive: the forced leave runs AFTER the success
// CONNACK, so a test that reads its flag as soon as it has read the CONNACK is racing the very
// code it is trying to observe. Two of these fixtures were written that way first and passed
// for the wrong reason — they reported "never fired" because they looked too early.
func testForcedLeaveWaitFired(t *testing.T, fired <-chan struct{}) {
	t.Helper()
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("the injected forced-leave save failure never fired; the test proved nothing")
	}
}

// TestForcedLeaveMalformedMembershipKeyIsRefusedBeforeTheConnack pins the preflight.
//
// mqttSharedDurablesForKey returns nil for a key it cannot parse, so a leave that met one used
// to remove the membership from the record while its delete loop iterated nothing — the durable
// lost its last reference with no diagnostic. Retaining just that key does not help either: a
// key that names no durable cannot make the last-holder scan report anything held, so the same
// commit would still delete a target a resident cons entry supplied.
//
// So the keys are parsed BEFORE the CONNACK and corrupt state refuses the resume there. This is
// the side of the acknowledgement where refusing costs nothing: the abort discards the Will and
// leaves the lifetime ungranted.
//
// A broker-minted key always parses, so this is reachable only from corrupt, out-of-band or —
// as here — doctored state.
func TestForcedLeaveMalformedMembershipKeyIsRefusedBeforeTheConnack(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-badkey", "$share/g/fl/badkey"
	const willTopic = "fl/badkey/will"
	testForcedLeaveSession(t, o, cid, shared)

	// Doctor the record: replace the shared membership with a key mqttParseSharedKey cannot
	// read back. Corrupt persisted state, which the compatibility boundary says to REFUSE.
	asm := testMQTTGetGlobalASM(t, s)
	ps, _ := testReconcileRecord(t, asm, cid)
	// The EMPTY STRING, deliberately. It is an unparseable key like any other, and JSON permits
	// it — and it is the one that a `badKey != ""` sentinel would silently let through, straight
	// past the refusal and on into destructive processing. Found by the holistic review.
	ps.SharedSubs = map[string]mqttSharedMembership{"": testSharedMembership("", 1)}
	b, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshal the doctored record: %v", err)
	}
	if _, err := asm.jsa.storeSessionMsg(asm.domainTk, string(getHash(cid)), 0, b); err != nil {
		t.Fatalf("store the doctored record: %v", err)
	}
	s = testReconcileRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	// The witness connects AFTER the restart, or the restart closes it and the Will assertion
	// below reads EOF instead of silence.
	cw, rw := testMQTTConnect(t, &mqttConnInfo{clientID: "fl-badkey-witness", cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer cw.Close()
	testMQTTCheckConnAck(t, rw, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cw, rw, []*mqttFilter{{filter: willTopic, qos: 0}}, []byte{0})
	testMQTTFlush(t, cw, nil, rw)

	// The resume is refused BEFORE the CONNACK, so a 3.1.1 client sees a bare close and never a
	// success CONNACK. Asserting the close alone would not distinguish the two sides of the
	// acknowledgement, so the Will is the discriminator: a post-CONNACK refusal would publish it.
	// The error-returning form, because a 3.1.1 pre-CONNACK refusal is a BARE CLOSE: there is no
	// CONNACK to read, so the ordinary helper would fatal on the EOF that is the expected result.
	c2, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: false,
		will: &mqttWill{topic: []byte(willTopic), message: []byte("gone"), qos: 0}},
		o.MQTT.Host, o.MQTT.Port, 0)
	if c2 != nil {
		defer c2.Close()
	}
	if err == nil {
		t.Fatal("the 3.1.1 resume of a record carrying a malformed shared key was ACCEPTED; the " +
			"preflight must refuse it before the CONNACK")
	}
	// The discriminator: no Will. A refusal on the other side of the acknowledgement would
	// publish it, because [MQTT-3.1.2-8] gives no licence to discard it once the CONNACK is out.
	testMQTTExpectNothingFor(t, rw, 500*time.Millisecond)
}

// TestPreConnackRefusalSignalAtTheLifetimeGrant pins the GENERALISED refusal signal at a site
// that was silent before this change.
//
// The lifetime grant is the last thing that can refuse a CONNECT before the CONNACK, and it
// closed without a word. For an automatic v5 client a silent close is indistinguishable from a
// network fault, and the recovery — a clean start — is a choice it can only make if it knows
// there is something to recover from.
//
// The grant save is targeted by REQUESTING A DIFFERENT INTERVAL from the one the session holds:
// applyRequestedLifetime applies the new values and then persists them, so the save whose
// session already carries the requested interval is the grant's own and no other.
//
// Deliberately NOT aimed at the forced leave's malformed-key preflight, which cannot exercise
// this: that preflight is 3.1.1-only, and a 3.1.1 CONNACK has no reason code above 5. An earlier
// version of this test did aim there and passed with the signal disabled, because it was
// measuring the pre-CONNACK restore refusal that already signalled before this change.
func TestPreConnackRefusalSignalAtTheLifetimeGrant(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "grant-signal"
	// Establish the session with one interval...
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// ...then fail the save that applies a DIFFERENT one.
	var fired bool
	restore := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || fired {
			return nil
		}
		sess.mu.Lock()
		granting := sess.expiryInterval == 600
		sess.mu.Unlock()
		if !granting {
			return nil
		}
		fired = true
		return errMQTTForcedLeaveTestRefusal
	})
	defer restore()

	// Count the CONNACKs the broker ATTEMPTS, not what reaches the wire: "signalled" and
	// "enqueued into an already-closing socket" are different facts, and only the attempt
	// counter separates them.
	var attempts atomic.Int32
	var attemptReason atomic.Int32
	restoreAck := setMQTTTestConnAckAttemptHook(func(rc byte) {
		attempts.Add(1)
		attemptReason.Store(int32(rc))
	})
	defer restoreAck()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	sp, reason, _ := testMQTTReadConnAckV5(t, r2)
	if !fired {
		t.Fatal("the lifetime-grant save failure never fired; the test proved nothing")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("the refused grant attempted %d CONNACK(s), want exactly 1: a silent close is "+
			"the defect, and two would mean a success CONNACK went out as well", got)
	}
	if reason != mqttReasonImplementationSpecificError {
		t.Fatalf("v5 CONNACK reason = %#x, want %#x: a refused lifetime grant must tell the "+
			"client why instead of closing silently", reason, mqttReasonImplementationSpecificError)
	}
	if sp {
		t.Fatal("a failure CONNACK must carry Session Present = 0 [MQTT-3.2.2-4]")
	}
}

// TestForcedLeaveRefusalTerminatesTheRestoreVerdict pins the obligation that moving the restore
// gate created — a defect introduced by the fix for the serve-while-unproven window, and caught
// in review rather than shipped.
//
// The verdict has exactly two terminal transitions and its channel is closed exactly once, by
// openRestoreGate OR abandonRestoreGate. While the gate opened BEFORE the forced leave, every
// post-CONNACK refusal already had its terminal behind it. Now that it opens AFTER the
// settlement, a refusing arm that says nothing leaves the verdict un-terminated — and that is
// not untidy, it is a lost first transmission: a withheld delivery parks until its bound
// expires, finds the session detached, and can no longer record itself as never transmitted, so
// its redelivery reaches the client marked DUP.
//
// The assertion is the WAKE, not merely "the gate was not opened". A test that only checked the
// latter would pass against the un-terminated bug, which is the whole hazard.
func TestForcedLeaveRefusalTerminatesTheRestoreVerdict(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-gate", "$share/g/fl/gate"
	testForcedLeaveSession(t, o, cid, shared)

	// A pre-store refusal: definite, so the CONNECT is refused and the verdict must be
	// abandoned rather than left hanging.
	var fired bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid {
			return nil
		}
		sess.mu.Lock()
		leaving := len(sess.sharedSubs) == 0
		sess.mu.Unlock()
		if !leaving || fired {
			return nil
		}
		fired = true
		return errMQTTForcedLeaveTestRefusal
	})
	defer restoreSave()

	// Observe the session's verdict from a waiter, exactly as a delivery callback would.
	var (
		mu      sync.Mutex
		verdict mqttTransmissionVerdict
		woke    bool
	)
	done := make(chan struct{})
	restorePause := setMQTTTestRestorePause(func(at string) {
		if at != "before-open" {
			return
		}
		sess := testMQTTResidentSessionOrNil(testMQTTGetGlobalASM(t, s), cid)
		if sess == nil {
			return
		}
		go func() {
			v := sess.awaitTransmissionGates(nil)
			mu.Lock()
			verdict, woke = v, true
			mu.Unlock()
			close(done)
		}()
	})
	defer restorePause()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTExpectDisconnect(t, c2)
	if !fired {
		t.Fatal("the pre-store refusal never fired; the test proved nothing")
	}

	// The wake must be PROMPT. Left un-terminated the waiter blocks until its own bound, which
	// is far longer than this, so a timeout here IS the defect.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the withheld waiter was never woken: a post-CONNACK refusal that neither opens " +
			"nor abandons the restore verdict leaves every withheld delivery parked until its " +
			"bound expires, after which it can no longer record itself as never transmitted and " +
			"its redelivery reaches the client marked DUP")
	}
	mu.Lock()
	gotWoke, gotVerdict := woke, verdict
	mu.Unlock()
	if !gotWoke {
		t.Fatal("the waiter reported no verdict")
	}
	if gotVerdict == mqttTransmissionOpen {
		t.Fatalf("the waiter saw verdict %v, want anything but Open: a REFUSED CONNECT must not "+
			"release the deliveries it withheld", gotVerdict)
	}
}

// NOTE on the site this file's own change deliberately left silent, and what closed it.
//
// It used to say the reconciliation verdicts stay silent, because reconcileResumedConsumers
// DISPOSED the session internally before returning — so a CONNACK added at the caller had
// already lost the race with its own socket close — and because its foreign verdict merged
// takeover with forked, which need different projections.
//
// Both are gone. The phase now RETURNS an outcome and a vocabulary, mqttProcessConnect's
// refuseDisposing runs abort → signal → dispose, and the merged verdict is split into
// DisposingNonForked and DisposingForked. The same treatment closed the lifetime grant, which
// this note never mentioned because the roadmap bullet said the reconciliation verdicts were
// the only site left and it was wrong.
//
// The claim that reaching the route needed "a reconciliation-scoped injection seam that does
// not exist yet" was ALSO wrong: TestMQTTReconcileRefusalDisciplineWithWill already had one —
// a save hook narrowed by testReconcileIsRepairSave — and that is where the production
// coverage now lives, alongside
// TestPreConnackRefusalSignalAtTheLifetimeGrantDisposingArms here.

// testMQTTExpectRefusalThenDisconnect reads the failure CONNACK a pre-CONNACK refusal now sends
// and then asserts the close, for fixtures that previously expected the close alone.
//
// Those fixtures were not wrong: until the pre-CONNACK refusal signal was generalised, a refusal
// inside the CONNECT wrote no packet at all, and several of them said so in a comment naming the
// roadmap item that would change it. This helper is that change applied to them — it asserts
// STRICTLY MORE than they did before (a refusal reason AND Session Present 0 AND the close),
// never less.
func testMQTTExpectRefusalThenDisconnect(t testing.TB, c net.Conn, r *mqttReader) byte {
	t.Helper()
	pt, _ := testMQTTReadPacket(t, r)
	if pt&mqttPacketMask != mqttPacketConnectAck {
		t.Fatalf("a refused CONNECT answered with packet type 0x%x, want a CONNACK", pt)
	}
	sp, err := r.readByte("connack flags")
	if err != nil {
		t.Fatalf("reading the connack flags: %v", err)
	}
	reason, err := r.readByte("connack reason")
	if err != nil {
		t.Fatalf("reading the connack reason: %v", err)
	}
	if reason == mqttConnAckRCConnectionAccepted {
		t.Fatal("a refused CONNECT was ACCEPTED")
	}
	if sp&0x1 != 0 {
		t.Fatalf("a failure CONNACK (reason %#x) carried Session Present = 1, which "+
			"[MQTT-3.2.2-4] forbids", reason)
	}
	testMQTTExpectDisconnect(t, c)
	return reason
}

// TestForcedLeaveContentionLandsAndKeepsTheConnection pins the 10164 arm, which is the one an
// earlier revision of this design would have closed destructively.
//
// 10164 is CONTENTION, not a verdict: mqttIsRecordCASRejection's own comment says it "signals
// contention, not a lost race". A first draft treated the whole CAS-rejection class as "the
// subject moved" and refused, which would have evicted an acknowledged client — publishing its
// Will — every time two writers merely collided in a clustered inflight branch.
//
// resolveContendedRecordSave re-reads and retries the exact bytes. When that lands, the leave is
// durable and the connection continues, and the commit is observed DIRECTLY rather than through
// the membership (nil'd by staging either way) or the group durable (also deleted later by the
// orphan sweep).
func TestForcedLeaveContentionLandsAndKeepsTheConnection(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-10164", "$share/g/fl/contend"
	testForcedLeaveSession(t, o, cid, shared)

	// Reject the forced-leave save ONCE with 10164. The retry inside resolveContendedRecordSave
	// then re-sends the same bytes and succeeds.
	var rejected atomic.Bool
	fired := make(chan struct{})
	var rejOnce sync.Once
	h := func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		sess.mu.Lock()
		leaving := len(sess.sharedSubs) == 0
		sess.mu.Unlock()
		if leaving && rejected.CompareAndSwap(false, true) {
			rejOnce.Do(func() { close(fired) })
			return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		}
		return resp, err
	}
	mqttTestSessStoreResultHookV.Store(&h)
	defer mqttTestSessStoreResultHookV.Store(nil)

	committed := make(chan struct{})
	var once sync.Once
	restoreCommit := setMQTTTestForcedLeaveCommitHook(func() { once.Do(func() { close(committed) }) })
	defer restoreCommit()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// WAIT for the injection: the forced leave runs AFTER the CONNACK, so reading the flag as
	// soon as the CONNACK has been read is racing the code under test.
	testForcedLeaveWaitFired(t, fired)
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("contention on the forced-leave save did not resolve to a landed write: either " +
			"the retry never ran, or 10164 was treated as a verdict rather than as contention")
	}
	// The connection SURVIVES. Treating 10164 as "the subject moved" would have refused here,
	// closing an acknowledged connection and publishing its Will over a mere collision.
	testMQTTFlush(t, c2, nil, r2)
	testMQTTExpectNothing(t, r2)
}

// testForcedLeaveConsumerExists reports whether a group durable is still on $MQTT_msgs, without
// needing a live client to look it up through.
func testForcedLeaveConsumerExists(t *testing.T, s *Server, durable string) bool {
	t.Helper()
	asm := testMQTTGetGlobalASM(t, s)
	cons, err := asm.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("listing consumers: %v", err)
	}
	for _, ci := range cons {
		if ci != nil && ci.Name == durable {
			return true
		}
	}
	return false
}

// NOTE on how the two gates in front of phase 3 came to be tested, because it took three
// fixtures and two of them were deleted rather than shipped.
//
// The commit has TWO independent gates before its irreversible phase — the ownership settlement
// and the void-verdict durability check — and each is pinned by its own fixture, because a
// fixture that enters the bad state after the first gate cannot kill removing the first gate.
// Hence the pre- and post-settlement hooks.
//
// The two failed attempts, recorded so the next person does not repeat them:
//
//  1. Injecting into a transmitted entry's cleanup reached only the REGISTER stream, so the
//     session-record hook never fired and the fixture never entered the window it named.
//  2. Arming the rejection marker BEFORE the leave does not survive it: a landed save advances
//     sess.seq, which RETIRES the marker by design — so at phase 3 there was correctly no
//     question outstanding, and the fixture was asserting against correct behaviour.
//
// Both gates are now entered through a hook at the exact instant that makes the state meaningful,
// rather than by reproducing an ambiguous cleanup write end to end. That is a deliberate trade:
// the hook makes the assertion deterministic and mutation-sensitive, and reproducing the state
// naturally defeated two attempts. What remains unverified is the end-to-end path by which a real
// cleanup write raises the question, as opposed to the gate's response once it is raised.

// TestForcedLeaveWithholdsTheReleaseWhenAVerdictIsOwed pins the DURABILITY gate.
//
// A post-settlement hook stages a NEW uncredited void verdict: ownership already settled, a
// verdict still owed — which is precisely the state this gate exists for. Removing the gate
// enters phase 3 and fails this test.
func TestForcedLeaveWithholdsTheReleaseWhenAVerdictIsOwed(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-owedverdict", "$share/g/fl/owedverdict"
	testForcedLeaveSession(t, o, cid, shared)
	_, _, durable := mqttSharedNames("g", "fl.owedverdict", 1)
	if !testForcedLeaveConsumerExists(t, s, durable) {
		t.Fatalf("the fixture's group durable %q does not exist, so a surviving durable below "+
			"would prove nothing", durable)
	}

	var staged atomic.Bool
	restoreSettle := setMQTTTestForcedLeavePostSettleHook(func(sess *mqttSession) {
		if sess.id != cid || !staged.CompareAndSwap(false, true) {
			return
		}
		sess.mu.Lock()
		sess.voidRegNonceLocked("test-owed-verdict-nonce")
		sess.mu.Unlock()
	})
	defer restoreSettle()

	var released atomic.Bool
	restoreRelease := setMQTTTestForcedLeaveReleaseHook(func() { released.Store(true) })
	defer restoreRelease()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !staged.Load() {
			return errors.New("the post-settlement hook has not fired yet")
		}
		return nil
	})
	// WAIT FOR THE COMMIT TO FINISH before asserting an absence. The staged flag is set INSIDE
	// the hook, so it is true while the connection goroutine is still working — and a negative
	// assertion made there races the very thing it is denying. An owed verdict makes the commit
	// withhold and the CONNECT refuse, so the connection's close is the completion boundary.
	testMQTTExpectDisconnect(t, c2)

	if released.Load() {
		t.Fatal("the forced leave entered its release phase with a staged void verdict still " +
			"uncredited: deleting a group durable there strands a live register with no verdict, " +
			"no membership and no consumer, reserving its packet identifier and Receive Maximum " +
			"slot permanently — and on an interest-retained stream it destroys the backlog of " +
			"every member that still holds the group")
	}
	if !testForcedLeaveConsumerExists(t, s, durable) {
		t.Fatalf("the group durable %q was deleted despite an owed verdict", durable)
	}
}

// TestForcedLeaveWithholdsTheReleaseWhenOwnershipIsUnresolved pins the OTHER gate: the ownership
// settlement. The two are independent, and a fixture entering the bad state after the settlement
// cannot kill removing the settlement — which is the gap round 6 named.
//
// An unresolved record write is armed BEFORE the settlement runs, and the store keeps failing so
// neither the resolution nor the probe can settle it. The verdict is then unresolved, and phase 3
// must not be entered.
func TestForcedLeaveWithholdsTheReleaseWhenOwnershipIsUnresolved(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, shared = "fl-unresolved", "$share/g/fl/unresolved"
	testForcedLeaveSession(t, o, cid, shared)
	_, _, durable := mqttSharedNames("g", "fl.unresolved", 1)
	if !testForcedLeaveConsumerExists(t, s, durable) {
		t.Fatalf("the fixture's group durable %q does not exist, so a surviving durable below "+
			"would prove nothing", durable)
	}

	// Not a CAS rejection, so the resolution's re-attempt cannot land either and the settlement
	// can only report unresolved.
	var failing atomic.Bool
	h := func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id == cid && failing.Load() {
			return nil, errMQTTForcedLeaveTestAmbiguous
		}
		return resp, err
	}
	mqttTestSessStoreResultHookV.Store(&h)
	defer mqttTestSessStoreResultHookV.Store(nil)

	var armed atomic.Bool
	restorePre := setMQTTTestForcedLeavePreSettleHook(func(sess *mqttSession) {
		if sess.id != cid || !armed.CompareAndSwap(false, true) {
			return
		}
		failing.Store(true)
		sess.mu.Lock()
		sess.markWriteUncertainLocked(&mqttSessSaveDetail{baseSeq: sess.seq, body: []byte("{}")})
		sess.mu.Unlock()
	})
	defer restorePre()

	var released atomic.Bool
	restoreRelease := setMQTTTestForcedLeaveReleaseHook(func() { released.Store(true) })
	defer restoreRelease()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !armed.Load() {
			return errors.New("the pre-settlement hook has not fired yet")
		}
		return nil
	})
	// WAIT FOR COMPLETION, and here it matters even more than in the sibling fixture: the
	// settlement performs JetStream requests, so the window between the hook returning and the
	// verdict being acted on is a round trip wide. An unresolved verdict makes the CONNECT
	// refuse, so the close is the boundary.
	testMQTTExpectDisconnect(t, c2)

	if released.Load() {
		t.Fatal("the forced leave entered its release phase with record ownership UNRESOLVED: a " +
			"CONSUMER.DELETE there can destroy a group durable another owner holds, and the " +
			"last-holder scan cannot prevent it because it skips records whose ID matches this " +
			"session's own")
	}
	if !testForcedLeaveConsumerExists(t, s, durable) {
		t.Fatalf("the group durable %q was deleted with ownership unresolved", durable)
	}
}

// TestPreConnackRefusalSignalAtTheLifetimeGrantDisposingArms is T6, and it covers the
// population the roadmap bullet wrongly reported as done.
//
// TestPreConnackRefusalSignalAtTheLifetimeGrant above injects a PRE-STORE failure, which
// reaches the grant's local arm — no vocabulary, no disposal. Its DISPOSING arms had no
// production coverage at all, and they were the ones with the defect: the grant used to
// dispose the session itself and return a bare error, so its caller attempted 0x83 after
// the close was already scheduled on another goroutine, and a record CLAIMING OUR PARENT
// — where 0x89 "try again" is both legal and exactly right — had its vocabulary computed,
// spent on a DISCONNECT that connAckWritten == false suppresses, and thrown away.
//
// Two rows, and the difference between them is the whole point of splitting the verdict:
//
//   - a SIBLING body (same PWID, a fresh WID of its own) is positive evidence that
//     nothing established a takeover, so the client is invited to retry with 0x89;
//   - anything else — here a body that keeps our own WID and only differs in content —
//     establishes nothing, so the client is told only that ownership could not be
//     confirmed, never that it was taken over.
//
// The grant save is targeted the way the local-arm test targets it: by REQUESTING A
// DIFFERENT INTERVAL, so the save whose session already carries the requested value is
// the grant's own and no other. The record is bumped from inside that seam, so the CAS
// that follows is a real 10071 rather than a fabricated error.
func TestPreConnackRefusalSignalAtTheLifetimeGrantDisposingArms(t *testing.T) {
	for _, tc := range []struct {
		name string
		// sibling decides whether the interleaved record claims our parent.
		sibling       bool
		wantReason    byte
		wantReasonStr string
	}{
		{"a body claiming our parent", true, mqttReasonServerBusy, mqttReasonStrOwnershipUnsettled},
		{"a body establishing nothing", false, mqttReasonImplementationSpecificError, mqttReasonStrOwnershipUnconfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			cid := "grant-dispose-" + getHash(tc.name)
			hash := getHash(cid)

			// Establish the session with one interval, so the reconnect's accept save has a
			// parent to claim and the grant below actually changes something.
			c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
				props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
			if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || sp {
				t.Fatalf("v5 CONNACK: sp=%v reason=%x", sp, reason)
			}
			testMQTTDisconnect(t, c, nil)
			c.Close()

			asm := testMQTTGetGlobalASM(t, s)

			// Interleave a record write immediately before the grant save's CAS, from
			// inside the save seam, so the rejection is a real 10071.
			var mu sync.Mutex
			var fired bool
			restore := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
				if sess.id != cid {
					return nil
				}
				sess.mu.Lock()
				granting := sess.expiryInterval == 600
				sess.mu.Unlock()
				if !granting {
					return nil
				}
				mu.Lock()
				defer mu.Unlock()
				if fired {
					return nil
				}
				fired = true
				smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
				if err != nil {
					t.Errorf("load record for the bump: %v", err)
					return nil
				}
				ps := &mqttPersistedSession{}
				if err := json.Unmarshal(smsg.Data, ps); err != nil {
					t.Errorf("unmarshal record for the bump: %v", err)
					return nil
				}
				// The discriminator, and it is exactly classifyRecordLineage's predicate:
				// keeping the PWID and minting a distinct WID makes this a sibling, while
				// keeping our WID makes it a body that establishes nothing.
				if tc.sibling {
					ps.WID = mqttNewRecordWID()
				}
				ps.Origin = "interleaved"
				b, _ := json.Marshal(ps)
				if _, err := asm.jsa.storeMsg(mqttSessStreamSubjectPrefix+asm.domainTk+hash, -1, b); err != nil {
					t.Errorf("store the bump: %v", err)
				}
				return nil
			})
			defer restore()

			// Count the CONNACKs the broker ATTEMPTS and check the session was still bound
			// and resident AT THAT MOMENT. That is the ordering assertion, and it is
			// deterministic rather than timing-dependent: disposeForeignStateLocked clears
			// sess.c and removes residency SYNCHRONOUSLY before it schedules the close, so
			// a signal emitted after the disposal sees neither. M1 (signal after dispose)
			// and M6 (dispose twice, once inside the helper) both flip these two booleans.
			var attempts atomic.Int32
			var boundAtAttempt, residentAtAttempt atomic.Bool
			sessObj := testMQTTResidentSession(t, s, cid)
			restoreAck := setMQTTTestConnAckAttemptHook(func(rc byte) {
				if rc == mqttConnAckRCConnectionAccepted {
					return
				}
				attempts.Add(1)
				sessObj.mu.Lock()
				boundAtAttempt.Store(sessObj.c != nil)
				sessObj.mu.Unlock()
				asm.mu.RLock()
				_, resident := asm.sessions[cid]
				asm.mu.RUnlock()
				residentAtAttempt.Store(resident)
			})
			defer restoreAck()

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
				props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()

			sp, reason, props := testMQTTReadConnAckV5(t, r2)
			mu.Lock()
			didFire := fired
			mu.Unlock()
			if !didFire {
				t.Fatal("the grant save's interleaved bump never fired; the test proved nothing")
			}
			if got := attempts.Load(); got != 1 {
				t.Fatalf("the refused grant attempted %d failure CONNACK(s), want exactly 1", got)
			}
			if reason != tc.wantReason {
				t.Fatalf("v5 CONNACK reason = %#x, want %#x", reason, tc.wantReason)
			}
			if sp {
				t.Fatal("a failure CONNACK must carry Session Present = 0 [MQTT-3.2.2-4]")
			}
			if props == nil || props.reasonString != tc.wantReasonStr {
				t.Fatalf("Reason String %q, want exactly %q", func() string {
					if props == nil {
						return "<absent>"
					}
					return props.reasonString
				}(), tc.wantReasonStr)
			}
			// THE ORDER: signalled while the session was still bound and resident, i.e.
			// before the disposal ran.
			if !boundAtAttempt.Load() {
				t.Fatal("the failure CONNACK was attempted AFTER the disposal cleared sess.c: " +
					"it is racing the socket close the disposal schedules")
			}
			if !residentAtAttempt.Load() {
				t.Fatal("the failure CONNACK was attempted AFTER the disposal evicted the session")
			}
			// And the disposal did happen: the session is gone afterwards.
			checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
				asm.mu.RLock()
				_, resident := asm.sessions[cid]
				asm.mu.RUnlock()
				if resident {
					return fmt.Errorf("a disposing verdict must evict the session")
				}
				return nil
			})
			testMQTTExpectDisconnect(t, c2)
			// Zero on THESE branches, and for a reason narrower than an earlier draft of this
			// comment claimed. A fork capsule's candidate delta is built from acknowledged
			// grant TERMS, each sid's DRAIN presence, in-flight register nonces and a PUBREL
			// consumer the parent lacked (mqttGrantEntriesDiffer, buildForkedGrantLocked).
			// This session owns one plain QoS 1 filter, so the restore provisions no PUBREL
			// consumer and no register, and a CONNECT acknowledges no SUBSCRIBE and no
			// UNSUBSCRIBE — so nothing supplies a delta and the fork is classified without
			// minting. A repair's new consumer identity is invisible to the predicate on
			// purpose.
			//
			// It would be FALSE to generalise that to "no pre-CONNACK write can produce a
			// delta": processSubs runs BEFORE the CONNACK (F-050 moved it there), so a resume
			// restoring a QoS 2 or shared filter DOES provision grant-tracked state that a
			// later pre-CONNACK save confirms. That route, and the caller-side double-mint
			// mutation it makes detectable, is covered by
			// TestPreConnackForkMintIsExactlyOncePerVerdict.
			if n := asm.forkedGrantStores.Load(); n != 0 {
				t.Fatalf("the grant path stored %d fork capsules; it minted none when this "+
					"was measured, and a change to that reopens whether a caller-side "+
					"double mint is detectable here", n)
			}
		})
	}
}

// TestPreConnackForkMintIsExactlyOncePerVerdict is the production-caller half of the fork
// mint's exactly-once property, and it exists because holistic review round 3 refuted the
// premise I had used to argue it was unreachable.
//
// I claimed a pre-CONNACK write could never produce a non-empty candidate delta, because
// registers and the PUBREL consumer are provisioned "after the CONNACK". **That is false, and
// this project's own design is why:** F-050 moved subscription restoration BEFORE the CONNACK,
// so `processSubs` provisions the PUBREL consumer a QoS 2 filter needs while the CONNACK is
// still unwritten. A later pre-CONNACK save then confirms a body carrying `PubRel` against an
// accept body that lacked it — and `buildForkedGrantLocked` reads exactly that as a delta.
//
// So the route is live, and this is it, end to end through a real CONNECT:
//
//  1. a v5 session takes a plain QoS 2 filter, so its resume must provision a PUBREL
//     consumer, and disconnects without a clean start;
//  2. the reconnect's accept save lands a body with no `PubRel`;
//  3. restoration, still pre-CONNACK, creates the PUBREL consumer, and the next successful
//     record save adopts a body that has it — the retained pair now differs;
//  4. the lifetime grant's save is rejected by a SIBLING record, so the classifier reaches a
//     forked verdict and mints a capsule whose delta is non-empty.
//
// The assertion is EXACTLY ONE store. The capsule itself cannot show that — every store
// replaces the map entry with an equivalent one — so the per-manager counter is the only
// observable, and it is what kills a second mint added anywhere on the caller's side of the
// classification, which is where the approved plan's M9 placed it.
func TestPreConnackForkMintIsExactlyOncePerVerdict(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic = "fork-mint-once", "fork/mint/q2"
	hash := getHash(cid)

	// A QoS 2 filter is what makes the resume provision a PUBREL consumer, which is the
	// grant-tracked state the delta comes from.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || sp {
		t.Fatalf("v5 CONNACK: sp=%v reason=%x", sp, reason)
	}
	// The SHARED filter is not decoration: it is what makes a record save happen AFTER
	// restoration and before the grant, namely proveSharedMembershipOwnership's own write.
	// Without it nothing adopts the post-restore body and the retained pair never differs.
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{
		{topic: topic, opts: 2},
		{topic: "$share/gmint/fork/mint/shared", opts: 1},
	})
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	asm := testMQTTGetGlobalASM(t, s)

	// Strip PubRel from the stored record, out of band. The first connection persisted it,
	// so without this the reconnect's accept body ALREADY carries it and the restore
	// provisions nothing new — which is exactly what an earlier version of this fixture
	// measured as an empty delta. Removing it makes the resume recreate the PUBREL consumer
	// pre-CONNACK, which is the grant-tracked state the delta is made of.
	func() {
		smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
		if err != nil {
			t.Fatalf("load the record to strip PubRel: %v", err)
		}
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			t.Fatalf("unmarshal the record to strip PubRel: %v", err)
		}
		if ps.PubRel == nil {
			t.Fatal("the QoS 2 subscription did not persist a PubRel consumer, so this " +
				"fixture cannot create the delta it needs")
		}
		ps.PubRel = nil
		b, _ := json.Marshal(ps)
		if _, err := asm.jsa.storeMsg(mqttSessStreamSubjectPrefix+asm.domainTk+hash, -1, b); err != nil {
			t.Fatalf("store the PubRel-stripped record: %v", err)
		}
	}()

	// The sibling lands at the GRANT save, identified the way the other grant fixtures
	// identify it: by the session already carrying the requested interval. Keeping the PWID
	// and minting a distinct WID is classifyRecordLineage's predicate for a body claiming our
	// parent, which is the verdict that mints.
	var mu sync.Mutex
	var fired bool
	restore := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid {
			return nil
		}
		sess.mu.Lock()
		granting := sess.expiryInterval == 600
		sess.mu.Unlock()
		if !granting {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if fired {
			return nil
		}
		fired = true
		smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
		if err != nil {
			t.Errorf("load record for the sibling bump: %v", err)
			return nil
		}
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			t.Errorf("unmarshal record for the sibling bump: %v", err)
			return nil
		}
		ps.WID = mqttNewRecordWID()
		ps.Origin = "sibling"
		b, _ := json.Marshal(ps)
		if _, err := asm.jsa.storeMsg(mqttSessStreamSubjectPrefix+asm.domainTk+hash, -1, b); err != nil {
			t.Errorf("store the sibling bump: %v", err)
		}
		return nil
	})
	defer restore()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()

	sp, reason, props := testMQTTReadConnAckV5(t, r2)
	mu.Lock()
	didFire := fired
	mu.Unlock()
	if !didFire {
		t.Fatal("the grant save's sibling bump never fired; the test proved nothing")
	}
	if reason != mqttReasonServerBusy {
		t.Fatalf("a body claiming our parent must be refused 0x89, got %#x", reason)
	}
	if sp {
		t.Fatal("a failure CONNACK must carry Session Present = 0 [MQTT-3.2.2-4]")
	}
	if props == nil || props.reasonString != mqttReasonStrOwnershipUnsettled {
		t.Fatalf("Reason String %v, want exactly %q", props, mqttReasonStrOwnershipUnsettled)
	}

	// The delta was real — otherwise the count below is zero either way and this fixture
	// proves nothing about a second mint. Checked FIRST, so a change that empties the delta
	// reports that rather than silently turning the next assertion vacuous.
	n := asm.forkedGrantStores.Load()
	if n == 0 {
		t.Fatal("the forked verdict minted nothing, so its candidate delta was empty: this " +
			"fixture can no longer discriminate a second mint, and the pre-CONNACK PUBREL " +
			"provisioning it relies on must have changed")
	}
	if n != 1 {
		t.Fatalf("the forked verdict stored %d capsules, want exactly 1: one verdict samples "+
			"the record and the acknowledged pair once, under one fence", n)
	}
	testMQTTExpectDisconnect(t, c2)
}
