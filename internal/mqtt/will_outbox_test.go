//go:build mqttport_fulltests

// Cross-replica tests for the delayed-Will durable outbox (Layer 2). Two broker
// instances share one embedded NATS/JetStream (same StoreDir), modelling two
// replicas in front of one JetStream cluster: a delayed Will enqueued on one
// broker must still fire from the other after the first exits, a reconnect on
// either broker must cancel it, and two claimants must not double-fire it.

package mqtt

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// runSharedReplica starts a second broker instance bound to the same embedded
// NATS (JetStream store) as the first, so both see the same $MQTT_willq stream.
// The caller owns cleanup ordering: the replica that owns the shared NATS is
// torn down with testMQTTShutdownServer (deferred first, so LIFO runs it last),
// and every other replica is stopped with a plain Shutdown deferred after it.
func runSharedReplica(t *testing.T, first *Options) (*Server, *Options) {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.StoreDir = first.StoreDir // share the embedded NATS + JetStream
	s := testMQTTRunServer(t, o)
	return s, o
}

// TestMQTTWillOutboxCrossReplicaHandoff: a delayed Will enqueued on broker A
// fires from broker B after A is (gracefully) shut down.
func TestMQTTWillOutboxCrossReplicaHandoff(t *testing.T) {
	t.Parallel()
	oA := testMQTTDefaultOptionsV5()
	sA := testMQTTRunServer(t, oA)
	sB, oB := runSharedReplica(t, oA)
	// Clean up the shared embedded NATS via B at the end. A is stopped mid-test
	// with sA.Shutdown() (MQTT only) so the shared JetStream survives for B.
	defer testMQTTShutdownServer(sB)

	// Subscriber on the surviving broker B.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "hoffsub", cleanStart: true}, oB.MQTT.Host, oB.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/hoff", opts: 0}})

	// Will client on broker A: delayed Will, surviving session.
	will := &mqttWill{topic: []byte("will/hoff"), message: []byte("from-A"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "hoffpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, oA.MQTT.Host, oA.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)

	// Drop the client (Will now pending on the outbox), then shut down broker A
	// gracefully. Only A's MQTT server is stopped; the shared NATS stays up for B.
	cw.Close()
	sA.Shutdown()

	// B's claim loop fires the handed-off Will once the delay elapses.
	testMQTTReadPublishV5(t, rs, "will/hoff", []byte("from-A"))
}

// TestMQTTWillOutboxReconnectCancelsCrossReplica: reconnecting the client on the
// OTHER broker before the delay elapses cancels the pending Will everywhere.
func TestMQTTWillOutboxReconnectCancelsCrossReplica(t *testing.T) {
	t.Parallel()
	oA := testMQTTDefaultOptionsV5()
	sA := testMQTTRunServer(t, oA)
	defer testMQTTShutdownServer(sA) // shared-NATS owner: LIFO runs this last
	sB, oB := runSharedReplica(t, oA)
	defer sB.Shutdown() // stop replica B's MQTT before the shared NATS goes down

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rcxsub", cleanStart: true}, oA.MQTT.Host, oA.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/rcx", opts: 0}})

	// Will client on A with a 2s delay.
	will := &mqttWill{topic: []byte("will/rcx"), message: []byte("should-not-fire"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rcxpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(2), props: mqttV5ConnPropsSessionExpiry(60)}, oA.MQTT.Host, oA.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// Wait for the pending Will to land on the outbox.
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTNumPendingWills(t, sA, "rcxsub"); n != 1 {
			return fmt.Errorf("expected 1 pending will, got %d", n)
		}
		return nil
	})

	// Reconnect the same client ID on broker B (the other replica) before the
	// delay: mqttProcessConnect -> cancelWillOutbox must suppress it.
	cw2, rw2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rcxpub", cleanStart: true}, oB.MQTT.Host, oB.MQTT.Port)
	defer cw2.Close()
	testMQTTReadConnAckV5(t, rw2)

	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTNumPendingWills(t, sA, "rcxsub"); n != 0 {
			return fmt.Errorf("expected pending will cancelled, got %d", n)
		}
		return nil
	})

	// The Will must never be delivered, even past its original delay.
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxNoDoubleFire: with two replicas both scanning, a delayed
// Will is delivered exactly once (the pending->publishing CAS dedupes claimants).
func TestMQTTWillOutboxNoDoubleFire(t *testing.T) {
	t.Parallel()
	oA := testMQTTDefaultOptionsV5()
	sA := testMQTTRunServer(t, oA)
	defer testMQTTShutdownServer(sA) // shared-NATS owner: LIFO runs this last
	sB, _ := runSharedReplica(t, oA)
	defer sB.Shutdown() // stop replica B's MQTT before the shared NATS goes down

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ndfsub", cleanStart: true}, oA.MQTT.Host, oA.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/ndf", opts: 0}})

	will := &mqttWill{topic: []byte("will/ndf"), message: []byte("once"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ndfpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, oA.MQTT.Host, oA.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// First delivery.
	testMQTTReadPublishV5(t, rs, "will/ndf", []byte("once"))
	// No second delivery from the other replica.
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxShutdownUnblocksBarrier: a delayed-Will publisher worker
// blocked in the subscribe barrier when shutdown begins must be unblocked by the
// Phase-2 dispatch-stop signal (willClaimStopCh), so the shutdown lifecycle
// actually completes (shutdownDone closes) instead of hanging on willWorkerWG.
func TestMQTTWillOutboxShutdownUnblocksBarrier(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s) // NATS cleanup; s.Shutdown is idempotent

	// A live client to reach the account manager (and its subs barrier).
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bbsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	asm := testMQTTGetAccountSessionManager(t, s, "bbsub")

	// Hold the subscribe barrier open so the QoS 1 Will publisher worker blocks in
	// mqttSubsBarrier.wait() rather than completing. Released at test end.
	asm.jsa.subsBarrier.enter()
	defer asm.jsa.subsBarrier.leave()

	// Enqueue a due delayed Will (QoS 1 so publication reaches the barrier).
	will := &mqttWill{topic: []byte("will/bb"), message: []byte("bye"), qos: 1}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bbpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// Wait until the claim loop has claimed and dispatched a worker: the record is
	// transitioned to publishing before the worker calls wait(), so this confirms
	// a worker is now (about to be) blocked in the held barrier.
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if !testMQTTHasWillState(t, asm, mqttWillPublishing) {
			return fmt.Errorf("no will record in publishing state yet")
		}
		return nil
	})

	// Begin shutdown: Phase 2 closes willClaimStopCh, which must abort the worker's
	// barrier wait so it completes and Phase 3 drains. If the abort did not work,
	// shutdownDone would never close and this times out.
	done := s.BeginShutdown()
	select {
	case <-done:
	case <-time.After(shutdownDrainTimeout + 3*time.Second):
		t.Fatal("shutdown did not complete: publisher worker stuck in the subscribe barrier")
	}
}

// TestMQTTWillOutboxPublishFailureRecovery: when a claimant fails during
// publication (before any message is delivered), the record stays publishing and
// a surviving replica reclaims it on lease expiry and publishes the Will — proving
// the durable outbox's central recovery promise (a Will is not lost on claimant
// failure). Uses an instance-scoped publish seam so the injected failure is
// isolated to a specific broker, not a package global.
func TestMQTTWillOutboxPublishFailureRecovery(t *testing.T) {
	t.Parallel()
	oA := testMQTTDefaultOptionsV5()
	sA := testMQTTRunServer(t, oA)
	sB, oB := runSharedReplica(t, oA)
	defer testMQTTShutdownServer(sB) // shared-NATS owner: LIFO runs this last
	defer sA.Shutdown()              // idempotent; A is also stopped mid-test

	asmA := testMQTTGetGlobalASM(t, sA)
	asmB := testMQTTGetGlobalASM(t, sB)

	// Inject a publish failure BEFORE any delivery on both replicas, so whichever
	// wins pending->publishing fails and leaves the record publishing.
	failPublish := func(rec *mqttWillOutbox) error { return fmt.Errorf("injected publish failure") }
	setWillPublishHook(asmA, failPublish)
	setWillPublishHook(asmB, failPublish)

	// Subscriber on the survivor (B), which must receive the recovered Will.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pfrsub", cleanStart: true}, oB.MQTT.Host, oB.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/pfr", opts: 0}})

	// Enqueue a due delayed Will via a client disconnect on A.
	will := &mqttWill{topic: []byte("will/pfr"), message: []byte("survives"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pfrpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, oA.MQTT.Host, oA.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// A claimant took the record and its publish failed: the record must remain in
	// publishing (not deleted, not done), and nothing was delivered.
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if !testMQTTHasWillState(t, asmB, mqttWillPublishing) {
			return fmt.Errorf("record not in publishing state yet")
		}
		return nil
	})
	if testMQTTHasWillState(t, asmB, mqttWillDone) {
		t.Fatal("record was marked done despite the injected publish failure")
	}
	testMQTTExpectNothing(t, rs)

	// Recovery: clear B's failure and stop A, so B is the sole replica that will
	// reclaim the expired-lease publishing record and publish the Will.
	setWillPublishHook(asmB, nil)
	sA.Shutdown()

	// The survivor publishes the Will exactly once.
	testMQTTReadPublishV5(t, rs, "will/pfr", []byte("survives"))
	testMQTTExpectNothing(t, rs)

	// The successful native record is eventually removed.
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asmB); n != 0 {
			return fmt.Errorf("expected outbox record removed after publish, got %d", n)
		}
		return nil
	})
}

// TestMQTTWillOutboxOverlappingGenerations exercises the central reason the
// outbox keys a subject per WillID rather than per client: a generation that has
// won pending->publishing is irrevocably due and must survive a reconnect, while
// the newer generation that same reconnect enqueues is an independent record a
// later reconnect can still cancel. Neither can evict the other, and their CAS
// state machines advance independently.
//
// It pins generation N in the publishing state with the instance-scoped publish
// seam (blocking, not failing), creates generation N+1 alongside it via a second
// disconnect, cancels N+1 with a third reconnect, then releases N. The publish
// path is stubbed by the seam, so verification is by outbox state and the seam's
// per-WillID publish counter, not end-to-end delivery (covered by the handoff /
// no-double-fire tests). N is asserted to publish at least once — a parked worker
// whose 2s test lease lapses is re-fired and re-published, the documented
// at-least-once behavior — while N+1, once cancelled, must publish exactly zero
// times.
func TestMQTTWillOutboxOverlappingGenerations(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Park every Will publication on a per-WillID gate. releaseAll on return so a
	// failed assertion below cannot leave a worker blocked in the seam and wedge
	// the deferred shutdown (LIFO: this runs before testMQTTShutdownServer).
	gate := newWillPublishGate()
	defer gate.releaseAll()
	setWillPublishHook(asm, gate.hook)

	// Generation N: connect with a short-delay Will, then drop the connection.
	willN := &mqttWill{topic: []byte("will/ov"), message: []byte("gen-N"), qos: 0}
	cN, rN := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ovpub", cleanStart: true, will: willN,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rN)
	cN.Close()

	// N fires: the claim loop CAS-transitions it pending->publishing and its worker
	// parks in the gate. Capture N's WillID and subject from the publishing record.
	var subjN, widN string
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		for _, w := range testMQTTListWillRecords(t, asm) {
			if w.rec.State == mqttWillPublishing {
				subjN, widN = w.subject, w.rec.WillID
				return nil
			}
		}
		return fmt.Errorf("generation N not publishing yet")
	})
	// Parked, not published: the worker is blocked in the seam, so N has not fired.
	if n := gate.publishedCount(widN); n != 0 {
		t.Fatalf("generation N published %d time(s) while its worker should be parked", n)
	}

	// Reconnect the same client WITH another delayed Will, then drop it. The
	// reconnect's cancel scan must leave N alone (it is publishing, irrevocably
	// due), and the drop enqueues generation N+1 as an independent pending record.
	willNp1 := &mqttWill{topic: []byte("will/ov"), message: []byte("gen-Np1"), qos: 0}
	cN1, rN1 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ovpub", cleanStart: true, will: willNp1,
		willProps: mqttV5WillDelayProps(30), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rN1)
	cN1.Close()

	// Both generations now coexist on DISTINCT subjects: N publishing, N+1 pending.
	// This is the per-WillID isolation the single-subject-per-client design lacked.
	var subjNp1, widNp1 string
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		recs := testMQTTListWillRecords(t, asm)
		if len(recs) != 2 {
			return fmt.Errorf("expected 2 coexisting generations, got %d", len(recs))
		}
		var pubOK, penOK bool
		for _, w := range recs {
			switch w.rec.State {
			case mqttWillPublishing:
				if w.rec.WillID != widN {
					return fmt.Errorf("publishing generation changed WillID: %s != %s", w.rec.WillID, widN)
				}
				pubOK = true
			case mqttWillPending:
				subjNp1, widNp1 = w.subject, w.rec.WillID
				penOK = true
			default:
				return fmt.Errorf("unexpected state %q for %s", w.rec.State, w.rec.WillID)
			}
		}
		if !pubOK || !penOK {
			return fmt.Errorf("want one publishing + one pending, got %+v", recs)
		}
		return nil
	})
	if subjNp1 == subjN {
		t.Fatalf("generations N and N+1 share subject %q — per-WillID subject isolation is broken", subjN)
	}
	if widNp1 == widN {
		t.Fatalf("generations N and N+1 share WillID %q", widN)
	}

	// Cancel N+1 with a third reconnect (no Will, kept open so it enqueues nothing).
	// Only the pending N+1 is CAS'd pending->cancelled; publishing N is untouched.
	cN2, rN2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ovpub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cN2.Close()
	testMQTTReadConnAckV5(t, rN2)

	// N+1 leaves the pending set (cancelled, then reaped), while N stays publishing
	// throughout — a newer generation's cancellation never disturbs the in-flight one.
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		var nPublishing bool
		for _, w := range testMQTTListWillRecords(t, asm) {
			switch {
			case w.rec.WillID == widN && w.rec.State == mqttWillPublishing:
				nPublishing = true
			case w.rec.WillID == widNp1 && w.rec.State == mqttWillPending:
				return fmt.Errorf("generation N+1 still pending after reconnect")
			}
		}
		if !nPublishing {
			return fmt.Errorf("generation N lost its publishing claim")
		}
		return nil
	})
	// N+1 must never have reached the publish path.
	if n := gate.publishedCount(widNp1); n != 0 {
		t.Fatalf("cancelled generation N+1 was published %d time(s); it must never fire", n)
	}

	// Release N: its parked worker(s) return and finalize the record (delete).
	gate.release(widN)
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if n := gate.publishedCount(widN); n < 1 {
			return fmt.Errorf("generation N not published after release")
		}
		return nil
	})

	// Terminal cleanup: N's published record is deleted and N+1's cancelled record
	// is reaped, draining the outbox. N+1 stayed at zero publications throughout.
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asm); n != 0 {
			return fmt.Errorf("expected outbox drained, got %d record(s)", n)
		}
		return nil
	})
	if n := gate.publishedCount(widNp1); n != 0 {
		t.Fatalf("cancelled generation N+1 was published %d time(s); it must never fire", n)
	}
}

// TestMQTTWillOutboxEnqueueExhaustionImmediatePublish: when the durable enqueue
// of a delayed Will cannot be confirmed after the bounded retries, the Will must
// not be lost — enqueueDelayedWill publishes it IMMEDIATELY as a fail-safe,
// sacrificing the delay. A store seam forces every enqueue CAS to fail, so a Will
// with a long delay is nonetheless delivered promptly and leaves no outbox record
// behind (nothing was ever durably written to fire again).
func TestMQTTWillOutboxEnqueueExhaustionImmediatePublish(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Force every enqueue store to fail so the retry budget is exhausted and the
	// immediate-publish fail-safe runs. The real load then finds nothing (nothing
	// was written), so the "did our write land" recheck cannot mask the failure.
	setWillEnqueueStoreHook(asm, func(string, uint64, *mqttWillOutbox) (uint64, error) {
		return 0, fmt.Errorf("injected enqueue store failure")
	})
	defer setWillEnqueueStoreHook(asm, nil)

	// Subscriber for the Will topic.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "exhsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/exh", opts: 0}})

	// Will client with a LONG delay (30s): a genuinely delayed publication would not
	// arrive within the read timeout, so a prompt delivery proves the fail-safe fired.
	will := &mqttWill{topic: []byte("will/exh"), message: []byte("failsafe"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "exhpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(30), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// Delivered immediately despite the 30s delay (the durable enqueue never confirmed).
	testMQTTReadPublishV5(t, rs, "will/exh", []byte("failsafe"))

	// Nothing durable was left behind: every enqueue store failed, so there is no
	// outbox record for a claim loop to fire again.
	if n := testMQTTCountWillRecords(t, asm); n != 0 {
		t.Fatalf("expected no outbox record after fail-safe publish, got %d", n)
	}
	// And the Will is delivered exactly once.
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxAmbiguousPublishDuplicateRecovery: a claimant whose publish
// actually reaches subscribers but whose OUTCOME is reported as a failure — an
// ambiguous publication — leaves the record publishing, so a later reclaim
// republishes it. That duplicate is exactly what the at-least-once guarantee
// permits. The test asserts the Will is never lost (delivered on the ambiguous
// publish), that recovery redelivers it (a second, duplicate copy), and that
// once the ambiguity clears the record reaches a clean terminal state (deleted).
//
// A single broker claims strictly sequentially (the scan re-fires only a
// publishing record whose lease has lapsed, and a fresh claim resets that lease),
// so the duplicates come from successive lease-expiry cycles, never from two live
// claimants racing the same record.
func TestMQTTWillOutboxAmbiguousPublishDuplicateRecovery(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Ambiguous publish: deliver for real, count it, then report failure so the
	// worker leaves the record publishing for lease-expiry recovery. Cleared later
	// so a clean reclaim finally finalizes (deletes) the record.
	var mu sync.Mutex
	deliveries := 0
	setWillPublishHook(asm, func(rec *mqttWillOutbox) error {
		_ = asm.deliverWillRecord(s, rec) // the message really reaches subscribers
		mu.Lock()
		deliveries++
		mu.Unlock()
		return fmt.Errorf("ambiguous publish outcome")
	})

	// Subscriber.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/amb", opts: 0}})

	// Enqueue a due delayed Will.
	will := &mqttWill{topic: []byte("will/amb"), message: []byte("dup"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// No loss: the ambiguous publish still delivered the Will.
	testMQTTReadPublishV5(t, rs, "will/amb", []byte("dup"))
	// Recovery: the record stayed publishing, so once the lease lapses a reclaim
	// republishes it — a second, duplicate delivery (allowed under at-least-once).
	testMQTTReadPublishV5(t, rs, "will/amb", []byte("dup"))

	// The record never left the publishing state (never lost, never prematurely done).
	if !testMQTTHasWillState(t, asm, mqttWillPublishing) {
		t.Fatal("record not publishing during the ambiguous-recovery cycle")
	}

	// Clear the ambiguity: the next reclaim publishes cleanly and deletes the record.
	setWillPublishHook(asm, nil)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asm); n != 0 {
			return fmt.Errorf("outbox not drained after ambiguity cleared, got %d", n)
		}
		return nil
	})

	mu.Lock()
	got := deliveries
	mu.Unlock()
	if got < 2 {
		t.Fatalf("expected at least 2 real deliveries (ambiguous publish + recovery), got %d", got)
	}
}

// TestMQTTWillOutboxTerminalCASRetry: the terminal publishing->done CAS failing
// transiently WITHOUT writing must be retried against the record's re-read
// sequence, so publication becomes durably terminal, the native record is then
// deleted, and the Will is delivered exactly once across a full lease window.
func TestMQTTWillOutboxTerminalCASRetry(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Fail only the first terminal CAS, without writing; later calls pass through.
	var calls int32
	setWillFinalizeStoreHook(asm, func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return 0, fmt.Errorf("injected terminal CAS failure (no write)")
		}
		return asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
	})
	defer setWillFinalizeStoreHook(asm, nil)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "tcrsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/tcr", opts: 0}})

	will := &mqttWill{topic: []byte("will/tcr"), message: []byte("once"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "tcrpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// Delivered exactly once: the publish happens once; the retry only re-commits state.
	testMQTTReadPublishV5(t, rs, "will/tcr", []byte("once"))
	testMQTTExpectNothing(t, rs)

	// The record became durably done and was then deleted (drained), and the
	// terminal CAS was in fact retried (more than one call).
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asm); n != 0 {
			return fmt.Errorf("expected record drained after durable done, got %d", n)
		}
		return nil
	})
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("expected the terminal CAS to be retried, got %d call(s)", got)
	}
}

// TestMQTTWillOutboxAmbiguousTerminalCAS: a terminal CAS that actually lands but
// returns an error (ambiguous ack) must be reconciled by re-reading the now-done
// record and treated as success, never republished.
func TestMQTTWillOutboxAmbiguousTerminalCAS(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Perform the real done CAS, then report an error: the finalizer must re-read,
	// see done, and stop (no republish). Only the genuine publishing->done write is
	// ambiguated.
	var ambiguated int32
	setWillFinalizeStoreHook(asm, func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		seq, err := asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
		if err == nil && rec.State == mqttWillDone {
			atomic.AddInt32(&ambiguated, 1)
			return 0, fmt.Errorf("injected ambiguous terminal ack")
		}
		return seq, err
	})
	defer setWillFinalizeStoreHook(asm, nil)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "atcsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/atc", opts: 0}})

	will := &mqttWill{topic: []byte("will/atc"), message: []byte("once"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "atcpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	testMQTTReadPublishV5(t, rs, "will/atc", []byte("once"))
	testMQTTExpectNothing(t, rs)

	// Reconciled to done and drained; the ambiguous ack fired exactly once.
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asm); n != 0 {
			return fmt.Errorf("expected record drained after ambiguous-but-committed done, got %d", n)
		}
		return nil
	})
	if got := atomic.LoadInt32(&ambiguated); got != 1 {
		t.Fatalf("expected exactly one ambiguous terminal ack, got %d", got)
	}
}

// TestMQTTWillOutboxTerminalCASStaleSequenceReconcile: when a slow publisher's
// terminal CAS loses because the record's sequence advanced under it (a peer
// reclaimed the expired lease and re-CAS'd it), the publisher must re-read the
// record and commit done at the CURRENT sequence, so it becomes durably terminal
// and never re-enters a lease-recovery cycle. Driven single-broker by parking the
// worker in a publish gate and advancing the record's sequence by hand (a
// deterministic stand-in for a peer reclaim). A native record is deleted on done,
// so the reconcile is observed as: the record drains to gone AND the Will is
// published exactly once (a broken reconcile would leave it publishing and re-fire
// on lease expiry, publishing twice).
func TestMQTTWillOutboxTerminalCASStaleSequenceReconcile(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	gate := newWillPublishGate()
	defer gate.releaseAll()
	setWillPublishHook(asm, gate.hook)

	// A due pending record with a valid origin (nonzero seq + current epoch) so the
	// fence proceeds; no watermark exists for this hash, so it falls back to the
	// (absent) session record and fires.
	cidHash := getHash("scr-cid")
	wid := "scr-w"
	subject := mqttWillQSubject(asm.domainTk, cidHash, wid)
	seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: wid, ClientIDHash: cidHash, Gen: mqttNewSessGen(), Topic: "will/scr", Msg: []byte("x"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// The claim loop claims pending->publishing (seq1); its worker parks in the gate.
	var seq1 uint64
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		rec, seq, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
		if err != nil || rec == nil || rec.State != mqttWillPublishing {
			return fmt.Errorf("record not publishing yet")
		}
		seq1 = seq
		return nil
	})

	// Simulate a peer reclaiming: advance the still-valid publishing record to a
	// higher sequence (seq2) while the first worker remains parked holding seq1.
	rec1, s1, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
	if err != nil || rec1 == nil {
		t.Fatalf("load publishing record: %v", err)
	}
	rec1.ClaimOwner = "peer-node"
	rec1.ClaimUntil = time.Now().Add(mqttWillClaimLease).UnixNano()
	seq2, err := asm.jsa.storeWillOutboxCAS(subject, s1, rec1)
	if err != nil {
		t.Fatalf("advance record to a higher sequence: %v", err)
	}
	if seq2 <= seq1 {
		t.Fatalf("expected advanced sequence > %d, got %d", seq1, seq2)
	}

	// Release the parked worker: publish succeeds, its terminal CAS at the stale
	// seq1 loses, and it must reconcile to seq2, commit done there, and delete.
	gate.release(wid)

	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if rec, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject); err != nil || rec != nil {
			return fmt.Errorf("record not drained yet (err=%v)", err)
		}
		return nil
	})

	// No further lease-recovery cycle: the record stays gone and the Will was
	// published exactly once across a full lease window (a broken reconcile would
	// re-fire and publish twice).
	time.Sleep(mqttWillClaimLease + mqttWillClaimInterval + 200*time.Millisecond)
	if rec, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject); err != nil || rec != nil {
		t.Fatalf("record reappeared after lease window (err=%v rec=%v)", err, rec)
	}
	if n := gate.publishedCount(wid); n != 1 {
		t.Fatalf("want exactly one publication after reconcile, got %d", n)
	}
}

// TestMQTTWillOutboxNewerDisconnectedRecordFires: the takeover fence must FIRE a
// Will when the current session record is newer (sequence > OriginSessionSeq) but
// DISCONNECTED (DisconnectedAt > 0) — e.g. the expiry sweep stamped it, or a
// reconnect that has since taken a finite-expiry disconnect. A sequence-only fence
// would wrongly suppress a legitimate Will (no-loss regression); the DisconnectedAt
// gate prevents that.
func TestMQTTWillOutboxNewerDisconnectedRecordFires(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Newer session (seq 100) but disconnected: fence must proceed (fire).
	setWillSessionLoadHook(asm, func(cidHash string) (*mqttPersistedSession, uint64, error) {
		return &mqttPersistedSession{DisconnectedAt: time.Now().Unix()}, 100, nil
	})
	defer setWillSessionLoadHook(asm, nil)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ndrfsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/ndrf", opts: 0}})

	seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "ndrf-w", ClientIDHash: getHash("ndrf-pub"), Gen: mqttNewSessGen(), Topic: "will/ndrf", Msg: []byte("fire"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch, // older than the newer-but-disconnected session (100)
	})

	testMQTTReadPublishV5(t, rs, "will/ndrf", []byte("fire"))
}

// TestMQTTWillOutboxLateEnqueueSurvivesOriginLoss: the fence must cancel a Will
// purely from persisted state when the current session record is newer AND shows a
// currently-connected generation (DisconnectedAt == 0) — a reconnect/takeover
// superseded it. No live origin broker is needed: the durable session record alone
// drives the suppression.
func TestMQTTWillOutboxLateEnqueueSurvivesOriginLoss(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Newer session (seq 100) AND connected: fence must cancel.
	setWillSessionLoadHook(asm, func(cidHash string) (*mqttPersistedSession, uint64, error) {
		return &mqttPersistedSession{DisconnectedAt: 0}, 100, nil
	})
	defer setWillSessionLoadHook(asm, nil)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "lesolsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/lesol", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "lesol-w", ClientIDHash: getHash("lesol-pub"), Gen: mqttNewSessGen(), Topic: "will/lesol", Msg: []byte("suppressed"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// The claimant fence cancels the superseded Will; it is never delivered.
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		rec, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
		if err != nil {
			return err
		}
		if rec == nil {
			return nil // cancelled then reaped: an acceptable terminal outcome
		}
		if rec.State != mqttWillCancelled {
			return fmt.Errorf("want cancelled (or reaped), got %q", rec.State)
		}
		return nil
	})
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxFenceIndeterminateLoadDefers: when the session record cannot be
// read (load/decode error), the fence must DEFER — neither fire nor cancel — so a
// later scan resolves it once the read succeeds. This proves an indeterminate read
// never risks publishing a suppressed Will nor loses a legitimate one.
func TestMQTTWillOutboxFenceIndeterminateLoadDefers(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Fail every session load: the fence defers on each scan.
	var loads int32
	setWillSessionLoadHook(asm, func(cidHash string) (*mqttPersistedSession, uint64, error) {
		atomic.AddInt32(&loads, 1)
		return nil, 0, fmt.Errorf("injected session load error")
	})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fildsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/fild", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "fild-w", ClientIDHash: getHash("fild-pub"), Gen: mqttNewSessGen(), Topic: "will/fild", Msg: []byte("later"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// While loads fail, the record stays pending (deferred) and nothing is delivered.
	time.Sleep(500 * time.Millisecond)
	if rec, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject); err != nil || rec == nil || rec.State != mqttWillPending {
		got := "nil"
		if rec != nil {
			got = string(rec.State)
		}
		t.Fatalf("want record still pending while load errors, got state=%s err=%v", got, err)
	}
	testMQTTExpectNothing(t, rs)
	if atomic.LoadInt32(&loads) == 0 {
		t.Fatal("expected the fence to attempt the session load")
	}

	// Clear the seam: the real load finds no session record (absent) => proceed, so
	// the deferred Will now fires (never lost).
	setWillSessionLoadHook(asm, nil)
	testMQTTReadPublishV5(t, rs, "will/fild", []byte("later"))
}

// TestMQTTWillOutboxEnqueueAckDelayedNoCancelWhilePublishing: the enqueue-side
// early cancel must never write cancelled over a record that has already advanced
// to publishing (an irrevocable claim). Simulates a create whose ack is lost only
// after a claimant advanced the record to publishing; even though the fence would
// otherwise want to cancel (a connected newer session), reconciliation must leave
// the publishing record alone so the in-flight publish completes.
func TestMQTTWillOutboxEnqueueAckDelayedNoCancelWhilePublishing(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm := testMQTTGetGlobalASM(t, s)

	// Make the fence WANT to cancel, so the test proves the state guard (not the
	// fence) is what protects the publishing record.
	setWillSessionLoadHook(asm, func(cidHash string) (*mqttPersistedSession, uint64, error) {
		return &mqttPersistedSession{DisconnectedAt: 0}, 1 << 30, nil
	})
	defer setWillSessionLoadHook(asm, nil)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "eadsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/ead", opts: 0}})

	// Enqueue seam: perform the real create, then advance the record straight to
	// publishing (as a claimant would), then return an error simulating a lost ack.
	// enqueueDelayedWill must re-read, see publishing (not pending), and return
	// WITHOUT writing cancelled over the in-flight claim.
	var advanced int32
	setWillEnqueueStoreHook(asm, func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		seq, err := asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
		if err != nil {
			return seq, err
		}
		if atomic.AddInt32(&advanced, 1) == 1 {
			pub := *rec
			pub.State = mqttWillPublishing
			pub.ClaimOwner = s.NodeName()
			pub.ClaimUntil = time.Now().Add(mqttWillClaimLease).UnixNano()
			if _, aerr := asm.jsa.storeWillOutboxCAS(subject, seq, &pub); aerr != nil {
				return 0, fmt.Errorf("advance-to-publishing failed: %v", aerr)
			}
			return 0, fmt.Errorf("injected lost ack after create")
		}
		return seq, nil
	})
	defer setWillEnqueueStoreHook(asm, nil)

	// Disconnect a client with a delayed Will to drive the enqueue path.
	will := &mqttWill{topic: []byte("will/ead"), message: []byte("inflight"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "eadpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// The record was left publishing (not cancelled): once its lease lapses the
	// claim loop reclaims it (recovery bypasses the fence) and a real worker delivers
	// the Will. If the enqueue-side had cancelled it, nothing would ever be delivered.
	testMQTTReadPublishV5(t, rs, "will/ead", []byte("inflight"))
}

// TestMQTTWillOutboxTakeoverLateEnqueue is the end-to-end late-enqueue regression:
// a displaced connection's delayed Will whose durable enqueue lands AFTER the
// reconnecting broker's cancel scan must still be suppressed, here by the
// enqueue-side fence (earlyCancelIfSuperseded). It exercises the full real
// plumbing — OriginSessionSeq captured in mqttHandleClosedClient, threaded through
// enqueueDelayedWill, and re-checked against the live session record — rather than
// a pre-seeded record.
//
// Two brokers share one JetStream. Broker A's enqueue is blocked with the
// enqueue-store seam. The same client takes over on broker B, whose synchronous
// cancel (completed before B's CONNACK) runs on the still EMPTY outbox. A's own
// takeover-triggered cancel cannot run here at all: its classifying eviction
// worker defers to the ownership gate A's blocked close holds across the parked
// enqueue, and a deferred-out eviction is refused, not retried — so A's persist
// observation is DROPPED outright (the lost-notification state, which the drop
// seam exists to produce deterministically) rather than awaited. Only then is A's
// real create released, so the sole thing that can suppress the record is the new
// fence. A far-future FireAt (60s) keeps the claim loop out of the picture
// entirely, so any cancellation is attributable to the ENQUEUE-side fence, not
// the claimant. Broken origin-sequence plumbing (OriginSessionSeq left 0)
// bypasses the fence, so the record simply stays pending (its 60s FireAt is never
// reached during the test) and the 3s cancellation assertion below times out —
// which is how this test fails on a broken fence.
func TestMQTTWillOutboxTakeoverLateEnqueue(t *testing.T) {
	t.Parallel()
	oA := testMQTTDefaultOptionsV5()
	sA := testMQTTRunServer(t, oA)
	sB, oB := runSharedReplica(t, oA)
	defer testMQTTShutdownServer(sB) // shared-NATS owner: LIFO runs this last
	defer sA.Shutdown()              // idempotent

	asmA := testMQTTGetGlobalASM(t, sA)

	// Subscriber on B: must receive NOTHING.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "tlesub", cleanStart: true}, oB.MQTT.Host, oB.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/tle", opts: 0}})

	// Block A's durable enqueue create until released, then perform the real store.
	// `entered` fires once the seam is reached — i.e. after A's disconnect save
	// (which captures OriginSessionSeq) has completed — so the B takeover below does
	// not race A's session-record write. `stored` fires only after the real create
	// lands, so "record absent" downstream can only mean cancelled-then-reaped, never
	// "not created yet".
	entered := make(chan struct{}, 1)
	stored := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }
	defer doRelease() // never leave A's readLoop blocked
	setWillEnqueueStoreHook(asmA, func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		seq, err := asmA.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
		if err == nil {
			select {
			case stored <- struct{}{}:
			default:
			}
		}
		return seq, err
	})

	// Will client on A: a long Will delay (60s) on a finite-expiry surviving session,
	// so the disconnect save runs and OriginSessionSeq is captured. The far-future
	// FireAt keeps the claim loop out of the picture entirely, so the ONLY thing that
	// can suppress the released record is the enqueue-side fence — cancellation is
	// therefore attributable to it, not to the claimant-side fence.
	will := &mqttWill{topic: []byte("will/tle"), message: []byte("suppressed"), qos: 0}
	cwA, rwA := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "tlepub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(60), props: mqttV5ConnPropsSessionExpiry(60)}, oA.MQTT.Host, oA.MQTT.Port)
	testMQTTReadConnAckV5(t, rwA)

	// Drop A's persist observations for this client. A's close is about to park
	// inside the enqueue seam while HOLDING the ownership gate, and the classifying
	// eviction worker defers to a held gate and then refuses — so A's
	// takeover-triggered cancel can no longer run in this staging, only lose a
	// bounded race about WHEN it gives up. The drop pins the refused/lost outcome
	// deterministically; either way nothing of A's touches the outbox before the
	// release, which is what the attribution below needs.
	asmA.mu.Lock()
	willHash := getHash("tlepub")
	asmA.testSessPersistDrop = func(h string) bool { return h == willHash }
	asmA.mu.Unlock()

	// Disconnect: A's readLoop enters enqueueDelayedWill and blocks in the seam.
	cwA.Close()
	// Wait until A's disconnect save has completed and the enqueue is blocked, so the
	// B takeover does not race A's session-record write.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("A's enqueue was never reached after disconnect")
	}

	// Take over the same client on B. B's synchronous cancelWillOutbox completes
	// before this CONNACK; both cancels run on the still-empty outbox.
	cwB, rwB := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "tlepub", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(60)}, oB.MQTT.Host, oB.MQTT.Port)
	defer cwB.Close() // keep B connected (DisconnectedAt == 0) through the assertions
	testMQTTReadConnAckV5(t, rwB)

	// A's observation is dropped (above), so no cancel of A's can race the released
	// enqueue: B's synchronous cancel has already completed before its CONNACK, and
	// the outbox is still empty.

	// Release A's real create: it lands pending carrying A's (older) OriginSessionSeq,
	// and enqueueDelayedWill's earlyCancelIfSuperseded re-reads B's live session
	// (newer sequence, DisconnectedAt == 0) and cancels it.
	doRelease()
	// The real create landed: from here "record absent" means cancelled-then-reaped.
	select {
	case <-stored:
	case <-time.After(5 * time.Second):
		t.Fatal("A's real enqueue create never completed after release")
	}

	// The record is cancelled (then possibly reaped) by the enqueue-side fence. With
	// broken origin plumbing (OriginSessionSeq left 0) the fence is bypassed and the
	// record stays pending forever (FireAt is 60s out), so this check would time out.
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		recs := testMQTTListWillRecords(t, asmA)
		if len(recs) == 0 {
			return nil // cancelled then reaped
		}
		if len(recs) != 1 {
			return fmt.Errorf("want at most 1 record, got %d", len(recs))
		}
		switch recs[0].rec.State {
		case mqttWillCancelled:
			return nil
		case mqttWillPending:
			return fmt.Errorf("record still pending: enqueue-side fence has not cancelled it")
		default:
			return fmt.Errorf("unexpected state %q", recs[0].rec.State)
		}
	})

	// Never delivered: the record is durably cancelled/reaped, so it can never fire
	// (and the claim loop would not reach its 60s FireAt during the test anyway).
	testMQTTExpectNothing(t, rs)
}

// setWillPublishHook installs (or clears) the instance-scoped publish seam on asm.
func setWillPublishHook(asm *mqttAccountSessionManager, fn func(rec *mqttWillOutbox) error) {
	asm.mu.Lock()
	asm.testPublishWill = fn
	asm.mu.Unlock()
}

// setWillEnqueueStoreHook installs (or clears) the instance-scoped enqueue-store
// seam on asm.
func setWillEnqueueStoreHook(asm *mqttAccountSessionManager, fn func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error)) {
	asm.mu.Lock()
	asm.testEnqueueStore = fn
	asm.mu.Unlock()
}

// setWillFinalizeStoreHook installs (or clears) the instance-scoped terminal
// (publishing->done) CAS seam on asm.
func setWillFinalizeStoreHook(asm *mqttAccountSessionManager, fn func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error)) {
	asm.mu.Lock()
	asm.testFinalizeStore = fn
	asm.mu.Unlock()
}

// setWillSessionLoadHook installs (or clears) the instance-scoped session-load
// seam used by the takeover fence on asm.
func setWillSessionLoadHook(asm *mqttAccountSessionManager, fn func(cidHash string) (*mqttPersistedSession, uint64, error)) {
	asm.mu.Lock()
	asm.testSessionLoad = fn
	asm.mu.Unlock()
}

// setWillCancelDoneHook installs (or clears) the instance-scoped observer invoked
// after each cancelWillOutbox pass on asm.
func setWillCancelDoneHook(asm *mqttAccountSessionManager, fn func()) {
	asm.mu.Lock()
	asm.testCancelWillDone = fn
	asm.mu.Unlock()
}

// seedWillRecord stores a create-only outbox record directly on asm's stream and
// returns its subject and stream sequence, for tests that need to inject a record
// with specific fields (OriginSessionSeq, OriginSessEpoch, FireAt) without a live client.
func seedWillRecord(t testing.TB, asm *mqttAccountSessionManager, rec *mqttWillOutbox) (string, uint64) {
	t.Helper()
	subject := mqttWillQSubject(asm.domainTk, rec.ClientIDHash, rec.WillID)
	seq, err := asm.jsa.storeWillOutboxCAS(subject, 0, rec)
	if err != nil {
		t.Fatalf("seed will record: %v", err)
	}
	return subject, seq
}

// testMQTTGetGlobalASM returns the eager-initialized global account session
// manager for s (available after Start without any client).
func testMQTTGetGlobalASM(t testing.TB, s *Server) *mqttAccountSessionManager {
	t.Helper()
	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	asm := sm.sessions[s.globalAccount().GetName()]
	require_NotNil(t, asm)
	return asm
}

// testMQTTCountWillRecords returns the number of outbox records for the account.
func testMQTTCountWillRecords(t testing.TB, asm *mqttAccountSessionManager) int {
	t.Helper()
	n := 0
	for seq := uint64(1); ; {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, mqttWillQAccountFilter(asm.domainTk), seq)
		if err != nil {
			break
		}
		seq = smsg.Sequence + 1
		n++
	}
	return n
}

// testWillRec pairs an outbox record with the subject and stream sequence it
// lives on.
type testWillRec struct {
	subject string
	seq     uint64
	rec     *mqttWillOutbox
}

// testMQTTListWillRecords returns every outbox record for the account paired with
// its subject and stream sequence, in stream order.
func testMQTTListWillRecords(t testing.TB, asm *mqttAccountSessionManager) []testWillRec {
	t.Helper()
	var out []testWillRec
	for seq := uint64(1); ; {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, mqttWillQAccountFilter(asm.domainTk), seq)
		if err != nil {
			break
		}
		seq = smsg.Sequence + 1
		rec := &mqttWillOutbox{}
		if json.Unmarshal(smsg.Data, rec) != nil {
			continue
		}
		out = append(out, testWillRec{subject: smsg.Subject, seq: smsg.Sequence, rec: rec})
	}
	return out
}

// willPublishGate is an instance-scoped publish seam that parks each Will
// publication on a per-WillID channel until the test releases it, so a test can
// pin one generation in the publishing state while it manipulates others. Keying
// on WillID lets overlapping generations of a single client be controlled
// independently. It records a per-WillID publish count (a released worker that
// returns success), so a test can assert at-least-once for a released generation
// and exactly-zero for a suppressed one.
type willPublishGate struct {
	mu        sync.Mutex
	gates     map[string]chan struct{} // WillID -> closed to release its publication
	published map[string]int           // WillID -> successful (released) publications
	openAll   bool                     // after releaseAll, never block a new caller again
}

func newWillPublishGate() *willPublishGate {
	return &willPublishGate{gates: map[string]chan struct{}{}, published: map[string]int{}}
}

// hook is the testPublishWill seam: it blocks the calling worker on the WillID's
// gate until released (or the gate has been globally opened), then records the
// publication and returns success without performing a real publish.
func (g *willPublishGate) hook(rec *mqttWillOutbox) error {
	g.mu.Lock()
	if g.openAll {
		g.published[rec.WillID]++
		g.mu.Unlock()
		return nil
	}
	ch, ok := g.gates[rec.WillID]
	if !ok {
		ch = make(chan struct{})
		g.gates[rec.WillID] = ch
	}
	g.mu.Unlock()
	<-ch
	g.mu.Lock()
	g.published[rec.WillID]++
	g.mu.Unlock()
	return nil
}

// release opens the gate for one WillID so its parked worker(s) proceed. Any
// later worker for the same WillID (e.g. a lease-expiry re-fire) passes straight
// through. Idempotent.
func (g *willPublishGate) release(willID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ch, ok := g.gates[willID]
	if !ok {
		ch = make(chan struct{})
		g.gates[willID] = ch
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// releaseAll opens every existing gate and stops blocking future callers, so a
// failed assertion cannot leave a worker parked in the seam and wedge shutdown.
func (g *willPublishGate) releaseAll() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.openAll = true
	for _, ch := range g.gates {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

// publishedCount returns how many times the given WillID's publication was
// released through the gate.
func (g *willPublishGate) publishedCount(willID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.published[willID]
}

// testMQTTHasWillState reports whether any outbox record for the account is in
// the given state.
func testMQTTHasWillState(t testing.TB, asm *mqttAccountSessionManager, want mqttWillState) bool {
	t.Helper()
	for seq := uint64(1); ; {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, mqttWillQAccountFilter(asm.domainTk), seq)
		if err != nil {
			return false
		}
		seq = smsg.Sequence + 1
		rec := &mqttWillOutbox{}
		if json.Unmarshal(smsg.Data, rec) == nil && rec.State == want {
			return true
		}
	}
}

// withLongWillScan pins the claim-scan interval far above any delay a test
// uses, so a Will that arrives on time proves the LOCAL FIRE TIMER delivered it
// rather than a lucky scan tick. It must be called before the server starts:
// the claim ticker begins with account-manager creation, so changing the
// interval afterwards leaves tick phase in the assertion.
//
// mqttWillClaimInterval is a package global (mqtt_v5_test.go's init() drops it
// to 50ms for the whole package), so a test using this must not run in
// parallel. Nothing in this package calls t.Parallel.
func withLongWillScan(t *testing.T) {
	t.Helper()
	prevInterval, prevLease := mqttWillClaimInterval, mqttWillClaimLease
	mqttWillClaimInterval = 30 * time.Second
	// Keep the lease above the interval, as production does.
	mqttWillClaimLease = 60 * time.Second
	t.Cleanup(func() {
		mqttWillClaimInterval, mqttWillClaimLease = prevInterval, prevLease
	})
}

// testWillTimerCount reports how many delayed-Will fire timers are armed.
func testWillTimerCount(s *Server) int {
	s.willDispatchMu.Lock()
	defer s.willDispatchMu.Unlock()
	return len(s.willTimers)
}

// TestMQTTWillOutboxTimerLatency is the regression guard for the conformance
// failure: a delayed Will must fire at its Will Delay Interval, not at the next
// outbox scan. With the scan pinned to 30s and a 1s delay, only the local fire
// timer can deliver in time -- before the fix the Will waited out the scan
// interval, which is exactly what made Paho's test_will_delay fail.
func TestMQTTWillOutboxTimerLatency(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "tlsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/latency", opts: 0}})

	will := &mqttWill{topic: []byte("will/latency"), message: []byte("prompt"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "tlpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)

	start := time.Now()
	cw.Close()
	testMQTTReadPublishV5(t, rs, "will/latency", []byte("prompt"))
	elapsed := time.Since(start)

	if elapsed < time.Second {
		t.Errorf("delayed Will fired after %s, before its 1s Will Delay Interval", elapsed)
	}
	// Generous ceiling: the point is that it did not wait out the 30s scan.
	if elapsed > 10*time.Second {
		t.Errorf("delayed Will took %s for a 1s delay; it waited for the outbox scan instead of its fire timer", elapsed)
	}
}

// TestMQTTWillOutboxAmbiguousEnqueueAckTimer covers the second durability path.
// enqueueDelayedWill also treats an enqueue as confirmed when the create CAS
// errors but a re-read finds the record already written (a lost ack). That
// branch returns without a clean CAS result, so arming only on the clean path
// would leave these Wills with the full scan-interval latency.
func TestMQTTWillOutboxAmbiguousEnqueueAckTimer(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "aesub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/ambig", opts: 0}})

	asm := testMQTTGetAccountSessionManager(t, s, "aesub")
	// Perform the real store, then report a lost ack. The record IS durable and
	// pending, so enqueueDelayedWill must take the re-read confirmation branch.
	var injected atomic.Bool
	setWillEnqueueStoreHook(asm, func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		if _, err := asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec); err != nil {
			return 0, err
		}
		injected.Store(true)
		return 0, fmt.Errorf("injected lost ack")
	})
	defer setWillEnqueueStoreHook(asm, nil)

	will := &mqttWill{topic: []byte("will/ambig"), message: []byte("ambiguous"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "aepub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)

	start := time.Now()
	cw.Close()
	testMQTTReadPublishV5(t, rs, "will/ambig", []byte("ambiguous"))
	elapsed := time.Since(start)

	if !injected.Load() {
		t.Fatal("the enqueue seam never ran; the ambiguous-ack branch was not exercised")
	}
	if elapsed > 10*time.Second {
		t.Errorf("Will confirmed via the lost-ack re-read took %s for a 1s delay; no fire timer was armed on that path", elapsed)
	}
}

// TestMQTTWillOutboxTimerShutdownDrains checks the drain contract the new timer
// participates in: shutdown Phase 2 must stop every armed fire timer and release
// its willTimerWG token, so Phase 3 is not held by a Will that is not due for
// minutes. The durable record stays pending for a peer or a restart.
func TestMQTTWillOutboxTimerShutdownDrains(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	// Idempotent, and needed on the failure path: if the lifecycle wait below
	// t.Fatals, the explicit teardown never runs and this server plus its embedded
	// NATS would leak into every later test in the package.
	defer testMQTTShutdownServer(s)

	will := &mqttWill{topic: []byte("will/drain"), message: []byte("later"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "drainpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(300), props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// Wait for the timer to be armed.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if testWillTimerCount(s) == 0 {
			return fmt.Errorf("no fire timer armed yet")
		}
		return nil
	})

	// A 5-minute Will must not hold shutdown open. Wait on BeginShutdown's channel
	// rather than Shutdown()/testMQTTShutdownServer: those return after the
	// shutdownDrainTimeout whether or not the lifecycle finished, so a leaked
	// willTimerWG token would look like a pass.
	lifecycleDone := s.BeginShutdown()
	select {
	case <-lifecycleDone:
	case <-time.After(shutdownDrainTimeout + 5*time.Second):
		t.Fatal("shutdown lifecycle never completed; an armed fire timer was not stopped and its willTimerWG token not released")
	}
	// Only now tear down the embedded NATS.
	testMQTTShutdownServer(s)

	if n := testWillTimerCount(s); n != 0 {
		t.Errorf("%d fire timer(s) still tracked after shutdown", n)
	}
}

// TestMQTTWillOutboxTimerReconnectCancels checks that reconnect suppression
// still holds now that a fire timer races the reconnect: with the scan pinned
// long, a timer is armed, the client reconnects inside the delay, and nothing is
// published past FireAt.
//
// It does NOT cover the callback: reconnect cancellation stops and untracks the
// timer, so the callback never runs. TestMQTTWillOutboxTimerOnCancelledRecord
// covers that path, and TestMQTTWillOutboxTimerReleasedOnCancel covers the
// release itself.
func TestMQTTWillOutboxTimerReconnectCancels(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "trcsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/trc", opts: 0}})

	will := &mqttWill{topic: []byte("will/trc"), message: []byte("suppressed"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "trcpub", cleanStart: false, will: will,
		willProps: mqttV5WillDelayProps(2), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// A timer must actually be armed before the reconnect, or the rest of this
	// test would pass with no timer at all and only re-test durable cancellation.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if testWillTimerCount(s) == 0 {
			return fmt.Errorf("no fire timer armed for the delayed Will")
		}
		return nil
	})

	// Reconnect well inside the delay: this cancels the pending record.
	cw2, rw2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "trcpub", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	defer cw2.Close()
	testMQTTReadConnAckV5(t, rw2)

	// Wait past the original FireAt and confirm nothing is published. The timer
	// was already stopped by the cancellation, and the scan is pinned to 30s, so
	// neither dispatch source can deliver it.
	time.Sleep(3 * time.Second)
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxTimerReleasedOnCancel is the retention guard. A cancelled
// Will must release its fire timer immediately, not keep it armed until FireAt:
// MQTT permits a Will Delay of 0xFFFFFFFF seconds (~136 years), so a retained
// timer -- and the account manager its callback captures -- would accumulate for
// the life of the process under ordinary reconnect churn, even though the
// cancelled records themselves are reaped within seconds.
//
// The delay here must be long enough that the timer cannot possibly self-release
// by firing; otherwise the assertion cannot tell "released on cancel" from
// "released when it fired", which is exactly the trap the short-delay
// cancellation test above falls into.
func TestMQTTWillOutboxTimerReleasedOnCancel(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	will := &mqttWill{topic: []byte("will/release"), message: []byte("never"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "relpub", cleanStart: false, will: will,
		willProps: mqttV5WillDelayProps(300), props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if testWillTimerCount(s) == 0 {
			return fmt.Errorf("no fire timer armed for the delayed Will")
		}
		return nil
	})

	// Reconnect cancels the record. With a 300s delay the timer cannot fire, so
	// the only way the count reaches zero is the cancel path releasing it.
	cw2, rw2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "relpub", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
	defer cw2.Close()
	testMQTTReadConnAckV5(t, rw2)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if n := testWillTimerCount(s); n != 0 {
			return fmt.Errorf("%d fire timer(s) still armed after the reconnect cancelled the Will; it would be retained until FireAt (300s here, up to ~136 years in the protocol)", n)
		}
		return nil
	})
}

// TestMQTTWillOutboxTimerOnCancelledRecord covers the fire path the two
// cancellation tests above never reach. A normal reconnect stops and untracks
// the timer, so its callback does not run; here the record is CAS-cancelled
// directly, bypassing cancelWillOutbox, so the timer stays armed and the
// callback really does execute after FireAt. It must publish nothing and release
// its own token.
//
// Note what this does NOT prove. The callback reloads the record rather than
// trusting its enqueue-time snapshot, but that is defence in depth, not the
// safety property: a stale snapshot carries a stale sequence, so the
// pending->publishing CAS in tryFireWill fails and nothing is published either
// way. Substituting a snapshot for the reload keeps this test passing. The CAS
// is the guard; the reload just avoids a pointless fence and round trip.
func TestMQTTWillOutboxTimerOnCancelledRecord(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rldsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/reload", opts: 0}})

	will := &mqttWill{topic: []byte("will/reload"), message: []byte("stale"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rldpub", cleanStart: false, will: will,
		willProps: mqttV5WillDelayProps(2), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	asm := testMQTTGetAccountSessionManager(t, s, "rldsub")
	filter := mqttWillQClientFilter(asm.domainTk, getHash("rldpub"))

	// Wait for the pending record and its armed timer.
	var subject string
	var rec *mqttWillOutbox
	var seq uint64
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, filter, 1)
		if err != nil {
			return fmt.Errorf("no outbox record yet: %v", err)
		}
		r := &mqttWillOutbox{}
		if err := json.Unmarshal(smsg.Data, r); err != nil {
			return err
		}
		if r.State != mqttWillPending {
			return fmt.Errorf("record is %q, want pending", r.State)
		}
		if testWillTimerCount(s) == 0 {
			return fmt.Errorf("no fire timer armed yet")
		}
		subject, rec, seq = smsg.Subject, r, smsg.Sequence
		return nil
	})

	// Cancel the record durably WITHOUT going through cancelWillOutbox, so
	// cancelWillTimer is never called and the timer stays armed.
	rec.State = mqttWillCancelled
	if _, err := asm.jsa.storeWillOutboxCAS(subject, seq, rec); err != nil {
		t.Fatalf("direct cancel CAS: %v", err)
	}
	if n := testWillTimerCount(s); n != 1 {
		t.Fatalf("timer count = %d, want the timer still armed: this test needs the callback to run", n)
	}

	// Past FireAt the callback runs. Reloading, it sees cancelled and publishes
	// nothing; then it untracks itself, which is how we know it actually ran.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if n := testWillTimerCount(s); n != 0 {
			return fmt.Errorf("%d fire timer(s) still armed; the callback has not run yet", n)
		}
		return nil
	})
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxTimerReleasedWhenRecordReaped covers the interleaving the
// per-record cleanup cannot see:
//
//	A arms a long fire timer.
//	A peer cancels the durable record and a claim loop reaps it.
//	A's takeover cancellation then scans and finds nothing.
//
// The scan completes successfully having never returned the subject, so the
// per-record cancelWillTimer never fires for it. Timers are keyed by exact
// subject, so A can no longer name its own timer: without the captured sweep on
// a conclusively completed scan it stays armed until FireAt, which the protocol
// allows to be ~136 years out.
//
// The reap is simulated by deleting the record outright, which is what the claim
// loop does to a terminal record.
func TestMQTTWillOutboxTimerReleasedWhenRecordReaped(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "reapsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)

	// Long delay: the timer cannot self-release by firing.
	will := &mqttWill{topic: []byte("will/reap"), message: []byte("never"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "reappub", cleanStart: false, will: will,
		willProps: mqttV5WillDelayProps(300), props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	asm := testMQTTGetAccountSessionManager(t, s, "reapsub")
	filter := mqttWillQClientFilter(asm.domainTk, getHash("reappub"))

	var recSeq uint64
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, filter, 1)
		if err != nil {
			return fmt.Errorf("no outbox record yet: %v", err)
		}
		if testWillTimerCount(s) == 0 {
			return fmt.Errorf("no fire timer armed yet")
		}
		recSeq = smsg.Sequence
		return nil
	})

	// Reap the record before any cancellation scan can observe it.
	if err := asm.jsa.deleteMsg(mqttWillQStreamName, recSeq, true); err != nil {
		t.Fatalf("reap record: %v", err)
	}
	if n := testWillTimerCount(s); n != 1 {
		t.Fatalf("timer count = %d before the cancel scan, want 1", n)
	}

	// The reconnect's cancellation scan now finds nothing at all.
	cw2, rw2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "reappub", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(600)}, o.MQTT.Host, o.MQTT.Port)
	defer cw2.Close()
	testMQTTReadConnAckV5(t, rw2)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if n := testWillTimerCount(s); n != 0 {
			return fmt.Errorf("%d fire timer(s) retained after the record was reaped; the completed scan never saw the subject, so only the captured sweep can release it", n)
		}
		return nil
	})
}

// TestMQTTWillTimerSweepExcludesLaterArmings pins the ordering rule that makes
// the post-scan sweep safe: it releases only timers captured BEFORE the
// cancellation scan, never one armed while the scan ran.
//
// That window is reachable in production. On the remote-takeover path the old
// connection's delayed-Will enqueue can be blocked behind the asynchronous
// cancellation; the scan observes no records and returns, the enqueue then lands
// and arms a timer for a record that IS still pending. Sweeping everything
// matching the client prefix at that point would stop the new timer and drop the
// Will back to the 5s scan latency this change exists to remove.
//
// Driven at the Server primitives rather than through the broker, because the
// interleaving is exactly "between scan completion and the sweep" and cannot be
// staged reliably from the protocol surface.
func TestMQTTWillTimerSweepExcludesLaterArmings(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const prefix = "$MQTT.willq.sweepcid."
	base := testWillTimerCount(s)

	// Armed before the capture: the sweep owns this one.
	older := prefix + "older"
	if !s.scheduleWillTimer(older, time.Hour, func() {}) {
		t.Fatal("could not arm the pre-capture timer")
	}

	captured := s.snapshotWillTimersWithPrefix(prefix)
	if len(captured) != 1 {
		t.Fatalf("captured %d timers, want 1", len(captured))
	}

	// Armed after the capture, standing in for an enqueue that landed once the
	// scan had already completed.
	newer := prefix + "newer"
	if !s.scheduleWillTimer(newer, time.Hour, func() {}) {
		t.Fatal("could not arm the post-capture timer")
	}

	s.stopWillTimers(captured)

	s.willDispatchMu.Lock()
	_, olderStillArmed := s.willTimers[older]
	_, newerStillArmed := s.willTimers[newer]
	s.willDispatchMu.Unlock()

	if olderStillArmed {
		t.Error("the captured timer survived the sweep")
	}
	if !newerStillArmed {
		t.Error("the sweep stopped a timer armed after the capture; a Will still pending would fall back to the 5s scan")
	}
	if n := testWillTimerCount(s); n != base+1 {
		t.Errorf("timer count = %d, want %d (only the post-capture timer left)", n, base+1)
	}

	// Leave nothing armed for the shutdown drain to trip over.
	s.cancelWillTimer(newer)
}

// TestMQTTWillOutboxTimerReleasedOnAmbiguousEarlyCancel drives the
// reconciliation branch in earlyCancelIfSuperseded: its cancel CAS fails, and
// the re-read shows the record is no longer pending, so the local fire timer
// must be released rather than left armed until FireAt.
//
// The lost-ack enqueue test does not reach here -- its re-read finds the record
// still pending and takes the ordinary path. This drives the failure branch
// directly instead, with a deliberately stale sequence.
func TestMQTTWillOutboxTimerReleasedOnAmbiguousEarlyCancel(t *testing.T) {
	withLongWillScan(t)

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "aecsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, "aecsub")

	cidHash := getHash("aec-pub")
	subject := mqttWillQSubject(asm.domainTk, cidHash, "aec-wid")
	// A valid epoch from a DIFFERENT incarnation than the account's, so the
	// watermark path below decides cancel.
	originEpoch := mqttSessEpochNUIDPrefix + nuid.Next()

	// Seed a pending record far in the future and keep its sequence.
	pending := &mqttWillOutbox{
		WillID: "aec-wid", ClientIDHash: cidHash, Gen: mqttNewSessGen(), Topic: "will/aec", Msg: []byte("x"), QoS: 0,
		FireAt: time.Now().Add(time.Hour).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: originEpoch,
	}
	staleSeq, err := asm.jsa.storeWillOutboxCAS(subject, 0, pending)
	if err != nil {
		t.Fatalf("seed pending record: %v", err)
	}

	// Arm the fire timer for that subject, as a real enqueue would.
	asm.armDelayedWillTimer(s, subject, pending.FireAt)
	if n := testWillTimerCount(s); n != 1 {
		t.Fatalf("timer count = %d, want 1 armed", n)
	}

	// Advance the record to publishing, making staleSeq stale.
	publishing := *pending
	publishing.State = mqttWillPublishing
	publishing.ClaimOwner = "someone-else"
	publishing.ClaimUntil = time.Now().Add(mqttWillClaimLease).UnixNano()
	if _, err := asm.jsa.storeWillOutboxCAS(subject, staleSeq, &publishing); err != nil {
		t.Fatalf("advance record to publishing: %v", err)
	}

	// Force the takeover fence to decide cancel: a valid watermark whose epoch
	// differs from the record's origin epoch is a different incarnation.
	setWillCancelWatermarkLoadHook(asm, func(string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus) {
		return &mqttWillCancelWatermark{ClientIDHash: cidHash, SessionSeq: 1, SessEpoch: asm.sessStreamEpoch}, 1, mqttWillCancelValid
	})
	defer setWillCancelWatermarkLoadHook(asm, nil)

	// Early-cancel with the stale sequence: the CAS loses, and the re-read sees
	// publishing. The record is no longer this broker's to fire, so the timer
	// must go.
	stale := *pending
	asm.earlyCancelIfSuperseded(s, subject, &stale, staleSeq)

	if n := testWillTimerCount(s); n != 0 {
		t.Errorf("%d fire timer(s) still armed after an ambiguous early-cancel whose re-read found the record non-pending; it would be retained until FireAt", n)
	}

	// The losing CAS must not have rewritten the record.
	cur, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
	if err != nil || cur == nil {
		t.Fatalf("reload record: %v", err)
	}
	if cur.State != mqttWillPublishing {
		t.Errorf("record state = %q, want publishing: the stale CAS must not have overwritten a claimed record", cur.State)
	}
}

// TestMQTTWillOutboxDisplacedCancelIsGenerationScoped: the displaced-broker
// cancel must only cancel the Wills its own evicted incarnation enqueued.
//
// Regression (a cross-incarnation data loss in the same class as the late-delete
// race that generations exist to close): the displaced broker's cancel is
// dispatched asynchronously from mqttHandleRemoteSessionPersist, so it can land
// after the client has reconnected on another broker and disconnected again with a
// NEW delayed Will. An unscoped cancel then destroys that Will, and nothing
// recovers it — the reconnect watermark sits BELOW its origin sequence, so the
// claimant fence lets it fire, but there is no longer a pending record to fire.
//
// The reconnect cancel on the CONNECT path stays deliberately unscoped, which the
// second half asserts: a reconnect supersedes every earlier incarnation.
func TestMQTTWillOutboxDisplacedCancelIsGenerationScoped(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, _ := testMQTTConnect(t, &mqttConnInfo{clientID: "genscope", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer nc.Close()
	asm := testMQTTGetGlobalASM(t, s)

	cidHash := getHash("genscope")
	retiredGen, freshGen := mqttNewSessGen(), mqttNewSessGen()

	// Two pending records for the same client, from two incarnations. FireAt is far
	// out so no claim loop resolves them underneath the test.
	fireAt := time.Now().Add(time.Hour).UnixNano()
	enqueue := func(gen string, originSeq uint64) string {
		t.Helper()
		willID := nuid.Next()
		subject := mqttWillQSubject(asm.domainTk, cidHash, willID)
		rec := &mqttWillOutbox{
			WillID: willID, ClientIDHash: cidHash, Gen: gen,
			Topic: "will/genscope", Msg: []byte(gen), FireAt: fireAt,
			State: mqttWillPending, OriginSessionSeq: originSeq, OriginSessEpoch: asm.sessStreamEpoch,
		}
		if _, err := asm.jsa.storeWillOutboxCAS(subject, 0, rec); err != nil {
			t.Fatalf("enqueue pending record for generation %q: %v", gen, err)
		}
		return subject
	}
	const displacedSeq = uint64(10)
	retiredSubject, freshSubject := enqueue(retiredGen, displacedSeq), enqueue(freshGen, displacedSeq)
	// The case a generation-only scope cannot see: the SAME session resumed on
	// another broker, disconnected again, and enqueued a new Will. A persistent
	// session keeps its incarnation across reconnects, so this record carries the
	// retired generation too — only its origin sequence, advanced past the
	// displaced broker's view by the resuming broker's own takeover save,
	// distinguishes it.
	resumedSubject := enqueue(retiredGen, displacedSeq+1)

	stateOf := func(subject string) mqttWillState {
		t.Helper()
		rec, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
		if err != nil || rec == nil {
			t.Fatalf("reload %q: rec=%v err=%v", subject, rec, err)
		}
		return rec.State
	}

	// The displaced broker's late cancel, scoped to the connection it evicted.
	if err := asm.cancelWillOutboxForIncarnation(s, cidHash, retiredGen, displacedSeq); err != nil {
		t.Fatalf("cancelWillOutboxForIncarnation: %v", err)
	}
	if got := stateOf(retiredSubject); got != mqttWillCancelled {
		t.Errorf("retired incarnation's Will state = %q, want cancelled", got)
	}
	if got := stateOf(freshSubject); got != mqttWillPending {
		t.Errorf("later incarnation's Will state = %q, want pending: a displaced broker's cancel destroyed a Will it does not own", got)
	}
	if got := stateOf(resumedSubject); got != mqttWillPending {
		t.Errorf("resumed connection's Will state = %q, want pending: the same session keeps its generation across a reconnect, "+
			"so a generation-only scope destroys a Will enqueued after the displacement", got)
	}

	// A reconnect, by contrast, supersedes every incarnation.
	if err := asm.cancelWillOutbox(s, cidHash); err != nil {
		t.Fatalf("cancelWillOutbox: %v", err)
	}
	if got := stateOf(freshSubject); got != mqttWillCancelled {
		t.Errorf("later incarnation's Will state = %q after an unscoped reconnect cancel, want cancelled", got)
	}
}
