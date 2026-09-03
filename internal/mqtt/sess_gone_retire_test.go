package mqtt

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// The licensed retirement of a BOUND session whose record was deleted
// (sess_gone_retire.go), F-036 site 1.
//
// The shape: a client negotiated Keep Alive 0 and went silent, and another broker's
// teardown deleted its record. reconcileResidentOwnership recognises that and arms the
// ownership marker, but before this route nothing consumed it — the bound half of the
// settlement is pre-dispatch on the connection's own readLoop, and the unbound half
// claims the client-ID gate as nobody. So the client held a session another broker had
// torn down for the whole remaining life of the connection.
//
// None of these tests is parallel: several install process-global hooks.

// testPlantGoneLicence writes a valid retirement watermark for this client whose marker
// sequence exceeds the session's acknowledged record sequence — the licence itself.
//
// markerAbove says whether the marker should REACH past our sequence. False builds the
// no-licence case, where a watermark exists but proves nothing about a record this
// session is already acknowledged beyond.
func (e *testRejectionEnv) plantGoneLicence(t *testing.T, gen string, markerAbove bool) {
	t.Helper()
	e.sess.mu.Lock()
	idHash, id, ourSeq := e.sess.idHash, e.sess.id, e.sess.seq
	e.sess.mu.Unlock()
	if ourSeq == 0 {
		t.Fatal("the session has no acknowledged record sequence, so no licence case can be built")
	}
	markerSeq := ourSeq + 1
	if !markerAbove {
		markerSeq = ourSeq
	}
	subject := mqttTeardownSubject(e.asm.domainTk, idHash)
	_, seq, status := e.asm.jsa.loadGenRetirement(e.asm.domainTk, idHash)
	var expect uint64
	if status != mqttRetirementAbsent {
		expect = seq
	}
	r := &mqttGenRetirement{ID: id, Gen: gen, MarkerSeq: markerSeq, Nonce: nuid.Next(), Origin: "node2"}
	if _, err := e.asm.jsa.storeGenRetirementCAS(subject, expect, r); err != nil {
		t.Fatalf("planting the retirement watermark: %v", err)
	}
}

// deleteRecordRemotely removes this client's record the way a peer's teardown release
// does — an exact-sequence delete — leaving the subject authoritatively empty. It is the
// state no sequence comparison can see, which is why the backstop arms the marker for it
// instead of feeding the eviction trigger.
func (e *testRejectionEnv) deleteRecordRemotely(t *testing.T) []byte {
	t.Helper()
	e.sess.mu.Lock()
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	smsg, err := e.asm.jsa.loadSessionMsg(e.asm.domainTk, idHash)
	if err != nil || smsg == nil {
		t.Fatalf("the record must exist before it can be deleted: smsg=%v err=%v", smsg, err)
	}
	// The bytes are returned so a fixture can put the record BACK, which is how the
	// between-read seam builds "a peer restored this client ID inside the fence".
	body := append([]byte(nil), smsg.Data...)
	if derr := e.asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); derr != nil {
		t.Fatalf("deleting the session record: %v", derr)
	}
	return body
}

// restoreRecordRemotely writes bytes back onto this client's record subject — the state a
// peer's CONNECT leaves when it restores the same client ID.
func (e *testRejectionEnv) restoreRecordRemotely(t *testing.T, body []byte) {
	t.Helper()
	e.sess.mu.Lock()
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if _, err := e.asm.jsa.storeSessionMsg(e.asm.domainTk, idHash, 0, body); err != nil {
		t.Fatalf("restoring the session record: %v", err)
	}
}

// TestF036SilentBoundGoneWithHigherRetirementIsRetired is the headline: a bound, silent
// client whose record is gone and whose retirement is provable is disposed of by the
// periodic pass, with no traffic from that client at all.
func TestF036SilentBoundGoneWithHigherRetirementIsRetired(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonebound")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()

	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	// The FIRST pass arms the marker and consumes it in the same pass; a second is run
	// because a refusal before the detach leaves the marker armed for a retry, and the
	// test must not depend on which pass succeeds.
	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		e.asm.reconcileResidentRecordState(e.s)
	}

	if e.resident(e.sess) {
		t.Fatal("a bound, silent session whose record was deleted under a provable retirement was never disposed of")
	}
	// Disposal means DETACHED, not merely deresidented: removeSession alone would leave
	// the old client bound to a wiped, evicted object, and its readLoop gate checks the
	// binding rather than residency.
	e.sess.mu.Lock()
	stillBound := e.sess.c != nil
	e.sess.mu.Unlock()
	if stillBound {
		t.Fatal("the displaced connection is still bound to the retired session object")
	}
}

// TestF036SilentBoundGoneAfterLaterGenerationRetirement is the case that makes the
// licence a SEQUENCE test rather than a generation test.
//
// The watermark's single slot has been replaced by a LATER generation's retirement, so it
// cannot name ours. Its marker sequence still proves a teardown claimed this client's
// record subject after the record we are acknowledged for, which is the whole licence.
//
// Mutation: requiring watermark generation EQUALITY here must fail this test.
func TestF036SilentBoundGoneAfterLaterGenerationRetirement(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonelater")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)

	e.deleteRecordRemotely(t)
	// A DIFFERENT generation occupies the slot.
	e.plantGoneLicence(t, mqttNewSessGen(), true)

	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		e.asm.reconcileResidentRecordState(e.s)
	}
	if e.resident(e.sess) {
		t.Fatal("a marker sequence beyond our own record is the licence: a different generation in the slot must not defeat it")
	}
}

// TestF036SilentBoundGoneWithoutLicenceRemainsBound is the negative, and it is the
// stated residual: absence alone is not the licence.
//
// Three sub-cases, each a way the licence can be missing. In every one the incumbent must
// be left bound with its session intact — refusal, never disposal under a live owner.
func TestF036SilentBoundGoneWithoutLicenceRemainsBound(t *testing.T) {
	t.Run("no watermark at all", func(t *testing.T) {
		e := testNewRejectionEnv(t, "f036gonenowm")
		e.dropPersistObservations(t, e.idHash)
		e.bind(t)
		e.deleteRecordRemotely(t)

		e.asm.reconcileResidentRecordState(e.s)
		e.asm.reconcileResidentRecordState(e.s)
		e.assertStillBound(t, "an absent watermark is not a licence: absence of a record proves nothing on its own")
	})

	t.Run("marker does not reach past our record", func(t *testing.T) {
		e := testNewRejectionEnv(t, "f036gonelow")
		e.dropPersistObservations(t, e.idHash)
		e.bind(t)
		e.sess.mu.Lock()
		gen := e.sess.gen
		e.sess.mu.Unlock()
		e.deleteRecordRemotely(t)
		// Valid, and naming our own generation — but its marker landed at or below the
		// record we are acknowledged for, so it describes a retirement this session
		// already outlived.
		e.plantGoneLicence(t, gen, false)

		e.asm.reconcileResidentRecordState(e.s)
		e.asm.reconcileResidentRecordState(e.s)
		e.assertStillBound(t, "a marker at or below our own sequence is not a licence")
	})

	t.Run("unreadable watermark", func(t *testing.T) {
		e := testNewRejectionEnv(t, "f036gonecorrupt")
		e.dropPersistObservations(t, e.idHash)
		e.bind(t)
		e.deleteRecordRemotely(t)
		// Scoped to THIS client and delegating every other hash to the real read: the
		// hook is process-global and paused parallel tests resume between sequential
		// ones, so an unscoped hook makes concurrently running tests read a corrupt
		// watermark for their own sessions.
		ourHash := e.sess.idHash
		asm := e.asm
		restore := setMQTTTestRetirementLoadHook(
			func(idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
				if idHash != ourHash {
					return asm.jsa.loadGenRetirementRecord(asm.domainTk, idHash)
				}
				return nil, 5, mqttRetirementCorrupt
			})
		defer restore()

		e.asm.reconcileResidentRecordState(e.s)
		e.asm.reconcileResidentRecordState(e.s)
		e.assertStillBound(t, "a watermark this broker cannot interpret is never a licence")
	})
}

// assertStillBound is the refusal assertion, and it checks the disposition rather than
// just survival: the incumbent must still be resident AND still bound AND still hold its
// armed marker, because "left alone" is what the refusal promises.
func (e *testRejectionEnv) assertStillBound(t *testing.T, why string) {
	t.Helper()
	if !e.resident(e.sess) {
		t.Fatalf("%s: the session was disposed of anyway", why)
	}
	e.sess.mu.Lock()
	bound := e.sess.c != nil
	e.sess.mu.Unlock()
	if !bound {
		t.Fatalf("%s: the incumbent connection was detached", why)
	}
	if !e.markerPending(e.sess) {
		t.Fatalf("%s: the ownership marker was cleared, so nothing will ever ask again", why)
	}
}

// TestF036GoneRetirementRevalidatesInsideRecordFence proves the in-fence re-check is
// load-bearing rather than decoration.
//
// The seam alters the binding after the off-gate prefilter has captured its licence and
// after the record fence is open — the exact window a CONNECT or a close occupies — and
// the proof must then refuse. Without the re-check the pass would retire a session whose
// binding it no longer describes.
//
// Mutation: removing any of the in-fence identity checks must fail this test.
func TestF036GoneRetirementRevalidatesInsideRecordFence(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonefence")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	fired := 0
	restore := setMQTTTestGoneRetireIdentityHook(func(sess *mqttSession) {
		if sess != e.sess {
			return // another test's session: this hook is process-global
		}
		fired++
		// A replacement binds in the window: the object the prefilter licensed is no
		// longer the one bound to that client.
		sess.mu.Lock()
		sess.c = &client{}
		sess.mu.Unlock()
	})
	defer restore()

	e.asm.reconcileResidentRecordState(e.s)

	if fired == 0 {
		t.Fatal("the in-fence identity seam never ran, so this test proves nothing about revalidation")
	}
	if !e.resident(e.sess) {
		t.Fatal("the retirement proceeded against a binding that changed inside the fence")
	}
	if !e.markerPending(e.sess) {
		t.Fatal("a refused proof must leave the ownership marker armed for the next pass")
	}
}

// TestF036GoneRetirementRefusesOnARecordThatCameBack is the BETWEEN-READ case, and it is
// what makes the in-fence durable re-reads provable rather than decorative.
//
// The off-gate prefilter read the record as absent and the watermark as licensing. Both
// observations were taken with no gate and no fence held, so either can be stale by the
// time the proof runs: a peer's CONNECT can restore this client's record in that window.
// The proof re-reads both INSIDE the fence, and the record read goes LAST so it is the
// final fallible observation.
//
// Mutations: removing the in-fence record-absence re-read, or the in-fence watermark
// re-validation, must fail this test. Neither is killable without this seam, because the
// prefilter's own reads already satisfy every other fixture.
func TestF036GoneRetirementRefusesOnARecordThatCameBack(t *testing.T) {
	t.Run("the record is restored inside the fence", func(t *testing.T) {
		e := testNewRejectionEnv(t, "f036goneback")
		e.dropPersistObservations(t, e.idHash)
		e.bind(t)
		e.sess.mu.Lock()
		gen := e.sess.gen
		e.sess.mu.Unlock()
		body := e.deleteRecordRemotely(t)
		e.plantGoneLicence(t, gen, true)

		fired := 0
		restore := setMQTTTestGoneRetireProofEntryHook(func(sess *mqttSession) {
			if sess != e.sess || fired > 0 {
				return // another test's session: this hook is process-global
			}
			fired++
			// A peer restores this client ID: the subject is occupied again, so the
			// absence the licence rested on is gone.
			e.restoreRecordRemotely(t, body)
		})
		defer restore()

		e.asm.reconcileResidentRecordState(e.s)
		restore()

		if fired == 0 {
			t.Fatal("the proof-entry seam never ran, so this test proves nothing about the in-fence re-reads")
		}
		e.sess.mu.Lock()
		bound := e.sess.c != nil
		e.sess.mu.Unlock()
		if !bound {
			t.Fatal("the retirement proceeded even though a session record was present again inside the fence")
		}
	})

	t.Run("the watermark is replaced inside the fence", func(t *testing.T) {
		e := testNewRejectionEnv(t, "f036gonewmback")
		e.dropPersistObservations(t, e.idHash)
		e.bind(t)
		e.sess.mu.Lock()
		gen := e.sess.gen
		e.sess.mu.Unlock()
		e.deleteRecordRemotely(t)
		e.plantGoneLicence(t, gen, true)

		fired := 0
		restore := setMQTTTestGoneRetireProofEntryHook(func(sess *mqttSession) {
			if sess != e.sess || fired > 0 {
				return // another test's session: this hook is process-global
			}
			fired++
			// The slot is replaced by a retirement whose marker no longer reaches past
			// our record, so the licence the prefilter read no longer exists.
			e.plantGoneLicence(t, mqttNewSessGen(), false)
		})
		defer restore()

		e.asm.reconcileResidentRecordState(e.s)
		restore()

		if fired == 0 {
			t.Fatal("the proof-entry seam never ran")
		}
		e.sess.mu.Lock()
		bound := e.sess.c != nil
		e.sess.mu.Unlock()
		if !bound {
			t.Fatal("the retirement proceeded on a licence that was withdrawn inside the fence")
		}
	})
}

// TestF036GoneRetirementAbnormalExitUnwindsTheProof is the abnormal-exit case, and the
// reason the unwind guard lives INSIDE retireBoundIncarnationChecked rather than in this
// route.
//
// At the point the proof runs, the helper has not returned the prepared clear, so nothing
// outside that frame holds a reference to the open record transaction or to the closed
// tracker. A panic or runtime.Goexit escaping the callback would therefore leave a
// session that refuses every write AND every packet for the rest of the process's life —
// strictly worse than the stranding this whole route exists to fix.
//
// runtime.Goexit is what t.Fatal in a hook does, which is why it is the shape used here:
// it runs deferred functions while skipping the rest of the frame.
//
// Mutations: removing the guard's p.abort(), or its tracker reopen, must fail this test.
func TestF036GoneRetirementAbnormalExitUnwindsTheProof(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonegoexit")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	var died atomic.Bool
	restore := setMQTTTestGoneRetireProofEntryHook(func(sess *mqttSession) {
		if sess == e.sess && died.CompareAndSwap(false, true) {
			runtime.Goexit()
		}
	})
	defer restore()

	// The pass runs on its own goroutine, because Goexit terminates the goroutine it
	// fires on and would otherwise end the test itself.
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.asm.reconcileResidentRecordState(e.s)
	}()
	<-done
	restore()

	if !died.Load() {
		t.Fatal("the proof-entry seam never fired, so no abnormal exit was exercised")
	}
	// The RECORD FENCE must be free: an unreleased transaction makes every later save on
	// this session refuse, silently and forever.
	tok, ok := e.sess.tryBeginRecordTx()
	if !ok {
		t.Fatal("the abnormal exit left the record transaction OPEN: this session can never persist again")
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("release the probe transaction: %v", err)
	}
	// And ADMISSION must be open: a closed tracker refuses every packet the incumbent
	// sends for the rest of the connection.
	e.sess.mu.Lock()
	tracker := e.sess.leases
	e.sess.mu.Unlock()
	lease, lok := tracker.acquire()
	if !lok {
		t.Fatal("the abnormal exit left the incarnation tracker CLOSED: the incumbent can no longer serve anything")
	}
	lease.release()
	// The incumbent is untouched, which is what a refusal — however it arrived — owes.
	e.assertStillBound(t, "an abnormal exit must leave the incumbent exactly as it was")
}

// TestF036GoneRetirementAbnormalExitAfterTheDetach is the POST-detach abnormal-exit case,
// and holistic review round 1 found it missing: the tail was straight-line code, so a
// panic or runtime.Goexit escaping the Will phase let `destroy`'s own defers close the
// record transaction and retire the tracker, and the outer defer release the gate, while
// the notification and the socket close were skipped. The client stayed connected
// indefinitely against a RETIRED tracker — a stranding strictly worse than the one this
// route exists to fix.
//
// The exit is driven from inside the Will phase, which is the reachable post-detach seam
// on this route. An exit from inside `destroy` is NOT reachable here and the reason is
// worth recording: `destroy`'s own seam fires only after a teardown claim is won, and the
// claim cannot land against a record that is already absent — which is the very fact that
// licensed this retirement.
//
// What must hold: the ownership gate is released, and the connection is CLOSED. The
// connTail is therefore armed BEFORE the Will runs rather than after it, so the close is
// owed from the moment the disposition is known.
//
// Mutations: removing the deferred `willPhase` or `connTail` wrapper, arming connTail
// after applyWill instead of before it, or dropping the take-and-clear before either
// invocation, must fail this test.
func TestF036GoneRetirementAbnormalExitAfterTheDetach(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonetail")
	e.dropPersistObservations(t, e.idHash)
	b := e.bindSocket(t, mqttProtoLevel5)
	b.armWill()
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	var died atomic.Bool
	restore := setMQTTTestWillPhaseProbe(func(sess *mqttSession, _ *mqttAccountSessionManager) {
		if sess == e.sess && died.CompareAndSwap(false, true) {
			runtime.Goexit()
		}
	})
	defer restore()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.asm.reconcileResidentRecordState(e.s)
	}()
	<-done
	restore()

	if !died.Load() {
		t.Fatal("the Will-phase seam never fired, so no post-detach abnormal exit was exercised")
	}
	// THE GATE. A leaked client-ID gate entry blocks every future CONNECT for this
	// client ID, permanently.
	if err := e.asm.lockSession(e.sess, nil); err != nil {
		t.Fatalf("the abnormal exit leaked the ownership gate: %v", err)
	}
	e.asm.unlockSession(e.sess)
	// THE CONNECTION. It must be closed rather than left attached to a session object
	// that has been reset and re-minted underneath it.
	// Polled, because the close itself runs on its own goroutine: the tail deliberately
	// does not block the sweep on a socket write that takes c.mu with no deadline.
	deadline := time.Now().Add(2 * time.Second)
	for !b.closed() {
		if time.Now().After(deadline) {
			t.Fatal("the abnormal exit left the displaced connection OPEN against a retired incarnation")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestF036GoneRetirementAbnormalExitPreservesADelayedWill is holistic review round 2's
// high finding, and it is the half round 1's repair did NOT cover.
//
// Arming the connection tail before the Will guarantees the socket closes. That alone is
// not enough. If an abnormal exit escapes `applyWill` before the Will is consumed, the
// tail closes an already-detached connection, and that connection's read-loop teardown
// takes the rebound path through `mqttPublishOwedWill` — which publishes ONLY an
// immediate Will. A Will carrying a nonzero Will Delay Interval is `mqttWillOwedDelayed`
// there, so it is neither published nor enqueued: SILENT LOSS, under the exact condition
// the repair claims to handle.
//
// The fixture is therefore a v5 connection with a nonzero Will Delay and a connection
// expiry that outlives it, so `mqttWillIsDelayed` answers yes and the correct outcome is a
// DURABLE outbox record rather than a publication. Round 1's test used an immediate Will
// and could not see this at all.
//
// Mutation: removing the deferred Will BACKSTOP must fail this test.
func TestF036GoneRetirementAbnormalExitPreservesADelayedWill(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonedelayed")
	e.dropPersistObservations(t, e.idHash)
	b := e.bindSocket(t, mqttProtoLevel5)

	b.bc.mu.Lock()
	b.bc.mqtt.cp.will = &mqttWill{
		topic:   []byte("will/topic"),
		subject: []byte("will.topic"),
		message: []byte("delayed-bye"),
		props:   &mqttProperties{willDelay: 3600},
	}
	b.bc.mqtt.willDisposition = mqttWillArmed
	// The connection's own expiry, which is what mqttHandleWill caps the delay against.
	// It must outlive the connection for the Will to be DELAYED rather than immediate.
	b.bc.mqtt.sessionExpiry = 600
	b.bc.mu.Unlock()

	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	// The abnormal exit lands INSIDE the Will phase, before the Will is consumed.
	var died atomic.Bool
	restore := setMQTTTestWillPhaseProbe(func(sess *mqttSession, _ *mqttAccountSessionManager) {
		if sess == e.sess && died.CompareAndSwap(false, true) {
			runtime.Goexit()
		}
	})
	defer restore()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.asm.reconcileResidentRecordState(e.s)
	}()
	<-done
	restore()

	if !died.Load() {
		t.Fatal("the Will-phase seam never fired, so no abnormal exit was exercised")
	}

	// The Will must be DURABLY ENQUEUED, exactly once. Neither published (it is delayed)
	// nor dropped (the close path cannot enqueue a delayed one).
	filter := mqttWillQSubjectPrefix + e.asm.domainTk + e.idHash + ".*"
	deadline := time.Now().Add(5 * time.Second)
	for {
		subjects, _, err := e.asm.jsa.streamSubjects(mqttWillQStreamName, filter)
		if err == nil && len(subjects) == 1 {
			for subject := range subjects {
				rec, _, lerr := e.asm.jsa.loadWillOutbox(e.asm.domainTk, subject)
				if lerr != nil || rec == nil {
					t.Fatalf("load the enqueued Will: %v", lerr)
				}
				if rec.State != mqttWillPending {
					t.Fatalf("the enqueued Will must be pending, got %v", rec.State)
				}
			}
			return
		}
		if len(subjects) > 1 {
			t.Fatalf("the Will was enqueued more than once: %d outbox records", len(subjects))
		}
		if time.Now().After(deadline) {
			t.Fatal("the abnormal exit LOST the delayed Will: the close path publishes only an immediate one, so nothing else enqueues it")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestF036GoneRetirementRefusesABoundSessionWithAParkedAction is the discriminator for
// escalation consult 4's Course A, and the state it covers is REACHABLE rather than
// hypothetical: `dischargeDeferredIfUnbound` tests for exactly a bound session with a
// parked action, so that conjunct would be dead code if the state could not occur.
//
// Why the route must REFUSE rather than handle it: a capsule carrying both a parked
// action and a bound client cannot be disposed correctly by `disposeCaptured`, which
// prioritises the action — the live client's Will is never consumed, and the tail then
// closes a connection whose own teardown publishes only an IMMEDIATE Will, losing a
// delayed one silently. Making the state unreachable on this route is the deletion-shaped
// answer; building an idempotent two-phase Will handoff for it was rejected as a fourth
// variant of a mechanism that had already generated two review rounds.
//
// Every assertion here is about the incumbent being left EXACTLY as it was, because that
// is what a pre-detach refusal promises.
//
// Mutation: removing the nil-action conjunct must fail this test.
func TestF036GoneRetirementRefusesABoundSessionWithAParkedAction(t *testing.T) {
	e := testNewRejectionEnv(t, "f036goneparked")
	e.dropPersistObservations(t, e.idHash)
	b := e.bindSocket(t, mqttProtoLevel5)

	// A DELAYED live Will on the current connection — the thing that would be lost.
	b.bc.mu.Lock()
	b.bc.mqtt.cp.will = &mqttWill{
		topic:   []byte("will/topic"),
		subject: []byte("will.topic"),
		message: []byte("parked-bye"),
		props:   &mqttProperties{willDelay: 3600},
	}
	b.bc.mqtt.willDisposition = mqttWillArmed
	b.bc.mqtt.sessionExpiry = 600
	b.bc.mu.Unlock()

	// A PRE-EXISTING parked action, as an earlier connection's deferred close leaves —
	// and a client bound over it, which is the combination the route must refuse.
	parked := &mqttDeferredDisconnect{disconnectedAt: time.Now().Unix(), expiryInterval: 0}
	e.sess.mu.Lock()
	e.sess.deferredDisconnect = parked
	gen := e.sess.gen
	e.sess.mu.Unlock()

	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	willWatch := e.watchDelivery(t, "will/topic")
	e.asm.reconcileResidentRecordState(e.s)
	e.asm.reconcileResidentRecordState(e.s)

	// The incumbent is untouched, and every part of "untouched" is checked separately:
	// a refusal that disposed of any one of these would be the defect.
	e.assertStillBound(t, "a bound session with a parked action must be refused, not retired")
	e.sess.mu.Lock()
	stillParked := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if stillParked != parked {
		t.Fatalf("the refusal consumed or replaced the parked action: %+v", stillParked)
	}
	b.bc.mu.Lock()
	willStillThere := b.bc.mqtt.cp.will != nil
	b.bc.mu.Unlock()
	if !willStillThere {
		t.Fatal("the refusal consumed the live connection's Will")
	}
	if b.closed() {
		t.Fatal("the refusal closed the incumbent's connection")
	}
	// ADMISSION reopened: a refusal that left the tracker closed would leave the
	// incumbent unable to serve anything.
	e.sess.mu.Lock()
	tracker := e.sess.leases
	e.sess.mu.Unlock()
	lease, ok := tracker.acquire()
	if !ok {
		t.Fatal("the refusal left the incarnation tracker CLOSED")
	}
	lease.release()
	// And nothing was published or enqueued for a Will that is still the connection's.
	if msg := e.deliveryBarrier(t, willWatch, "will/topic"); string(msg.Data) != "barrier" {
		t.Fatalf("the refusal published the Will of a session it did not retire: %q", msg.Data)
	}
	filter := mqttWillQSubjectPrefix + e.asm.domainTk + e.idHash + ".*"
	if subjects, _, err := e.asm.jsa.streamSubjects(mqttWillQStreamName, filter); err == nil && len(subjects) != 0 {
		t.Fatalf("the refusal enqueued %d delayed Will record(s) for a session it did not retire", len(subjects))
	}
}

// TestF036GoneRetirementRefusesOnASplitIndex covers the two manager-index checks, which
// are DEFENSIVE DEPTH rather than a reachable production race — and the fixture says so
// rather than dressing them up.
//
// Every normal manager operation updates as.sessions and as.sessByHash together, so
// dropping either check is masked by the other and neither mutation can be killed by an
// ordinary fixture. The state is therefore constructed deliberately through the in-fence
// seam, one index at a time. The identity set matched here is the one
// finishPendingDiscard already requires, and the reason both are checked is that the
// record watch's eviction worker resolves through sessByHash while a CONNECT looks up
// through sessions: with only one checked, this route and that worker could each believe
// it owns the object.
//
// Mutations: dropping the as.sessions check must fail the first subtest, and dropping the
// as.sessByHash check the second. No production race is claimed by either.
func TestF036GoneRetirementRefusesOnASplitIndex(t *testing.T) {
	for _, tc := range []struct {
		name string
		drop func(as *mqttAccountSessionManager, sess *mqttSession)
	}{
		{"the client-ID index no longer names this object", func(as *mqttAccountSessionManager, sess *mqttSession) {
			as.mu.Lock()
			delete(as.sessions, sess.id)
			as.mu.Unlock()
		}},
		{"the hash index no longer names this object", func(as *mqttAccountSessionManager, sess *mqttSession) {
			as.mu.Lock()
			delete(as.sessByHash, sess.idHash)
			as.mu.Unlock()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, "f036gonesplit")
			e.dropPersistObservations(t, e.idHash)
			e.bind(t)
			e.sess.mu.Lock()
			gen := e.sess.gen
			e.sess.mu.Unlock()
			e.deleteRecordRemotely(t)
			e.plantGoneLicence(t, gen, true)

			fired := 0
			restore := setMQTTTestGoneRetireIdentityHook(func(sess *mqttSession) {
				if sess != e.sess {
					return // another test's session: this hook is process-global
				}
				fired++
				tc.drop(e.asm, sess)
			})
			defer restore()

			e.asm.reconcileResidentRecordState(e.s)
			restore()

			if fired == 0 {
				t.Fatal("the in-fence seam never ran, so this test proves nothing")
			}
			// The object must still be BOUND: the proof had to refuse. Residency is not
			// the assertion here — the fixture itself removed one index entry — so what
			// is asserted is that no disposal happened, which is the detach.
			e.sess.mu.Lock()
			bound := e.sess.c != nil
			e.sess.mu.Unlock()
			if !bound {
				t.Fatal("the retirement proceeded against an object one manager index no longer names")
			}
			if !e.markerPending(e.sess) {
				t.Fatal("a refused proof must leave the ownership marker armed")
			}
		})
	}
}

// TestF036GoneRetirementReopensAdmissionOnRefusal is the disposition half of a refusal,
// and it is what every earlier attempt at this finding could not supply.
//
// A refusal closes and drains the incarnation tracker on its way in. If it did not REOPEN
// it, the incumbent would be left resident and bound but unable to serve a single further
// packet — a session that refuses everything for the rest of the connection's life, which
// is worse than the stranding this route exists to fix.
func TestF036GoneRetirementReopensAdmissionOnRefusal(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonereopen")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	restore := setMQTTTestGoneRetireIdentityHook(func(sess *mqttSession) {
		if sess != e.sess {
			return // another test's session: this hook is process-global
		}
		sess.mu.Lock()
		sess.c = &client{}
		sess.mu.Unlock()
	})
	defer restore()
	e.asm.reconcileResidentRecordState(e.s)
	restore()

	// Admission must be open again: a lease acquired against the tracker the connection
	// was bound to has to succeed, or nothing this session serves can proceed.
	e.sess.mu.Lock()
	tracker := e.sess.leases
	e.sess.mu.Unlock()
	lease, ok := tracker.acquire()
	if !ok {
		t.Fatal("a refused retirement left the incarnation tracker CLOSED: the incumbent can no longer serve anything")
	}
	lease.release()
}

// TestF036GoneRetirementDischargesWillExactlyOnce pins the Will disposition, which is
// where F-084 is easiest to break.
//
// `licensed=false` is the rule: a retirement watermark proves a TEARDOWN, not a successor
// connection, so this route may not discard the Will — a disposal that suppressed it
// would be silent loss for a client whose session ended and whose subscribers were never
// told. The Will is therefore PUBLISHED, and the test observes the publication rather
// than a disposition field, because discard and consume both leave the same terminal
// disposition behind and only the publication distinguishes them.
//
// It also pins the parked-action invariant: a successful bound retirement captures
// `action == nil`, because parking consumes the connection's Will AND nils sess.c, so
// the pre-detach owner check refuses a parked session outright. Without that invariant
// `disposeCaptured` would prefer a capsule action and leave the live connection's Will
// for the close path to publish a second time.
//
// Mutations: passing licensed=true (the Will is discarded, so the barrier arrives first),
// and omitting applyWill (nothing is published at all), must both fail this test.
func TestF036GoneRetirementDischargesWillExactlyOnce(t *testing.T) {
	e := testNewRejectionEnv(t, "f036gonewill")
	e.dropPersistObservations(t, e.idHash)
	b := e.bindSocket(t, mqttProtoLevel5)
	b.armWill()

	e.sess.mu.Lock()
	gen := e.sess.gen
	// A parked action and a live Will cannot coexist on a successful retirement, and the
	// check below is what makes that an invariant rather than a comment.
	parked := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if parked != nil {
		t.Fatal("this fixture requires a session with no parked disconnect action")
	}

	e.deleteRecordRemotely(t)
	e.plantGoneLicence(t, gen, true)

	willWatch := e.watchDelivery(t, "will/topic")
	e.asm.reconcileResidentRecordState(e.s)
	if e.resident(e.sess) {
		e.asm.reconcileResidentRecordState(e.s)
	}
	if e.resident(e.sess) {
		t.Fatal("the retirement did not run, so nothing can be asserted about its Will")
	}

	// The Will must have been PUBLISHED. The barrier is what makes this an assertion
	// rather than a race: one connection preserves order, so the Will arriving before
	// the sentinel is proof it was published, and the sentinel arriving first is proof
	// it never was.
	if msg := e.deliveryBarrier(t, willWatch, "will/topic"); string(msg.Data) == "barrier" {
		t.Fatal("the Will was never published: a retirement watermark proves a teardown, not a successor, so suppressing it is silent loss (F-084)")
	}

	// And exactly once: nothing may be left parked for a later path to publish again.
	e.sess.mu.Lock()
	leftover := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if leftover != nil {
		t.Fatalf("the retirement left a parked disconnect action behind, which a later path would discharge as a second Will: %+v", leftover)
	}
}
