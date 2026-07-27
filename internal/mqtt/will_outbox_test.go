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
		WillID: wid, ClientIDHash: cidHash, Topic: "will/scr", Msg: []byte("x"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// The claim loop claims pending->publishing (seq1); its worker parks in the gate.
	var seq1 uint64
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		rec, seq, err := asm.jsa.loadWillOutbox(subject)
		if err != nil || rec == nil || rec.State != mqttWillPublishing {
			return fmt.Errorf("record not publishing yet")
		}
		seq1 = seq
		return nil
	})

	// Simulate a peer reclaiming: advance the still-valid publishing record to a
	// higher sequence (seq2) while the first worker remains parked holding seq1.
	rec1, s1, err := asm.jsa.loadWillOutbox(subject)
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
		if rec, _, err := asm.jsa.loadWillOutbox(subject); err != nil || rec != nil {
			return fmt.Errorf("record not drained yet (err=%v)", err)
		}
		return nil
	})

	// No further lease-recovery cycle: the record stays gone and the Will was
	// published exactly once across a full lease window (a broken reconcile would
	// re-fire and publish twice).
	time.Sleep(mqttWillClaimLease + mqttWillClaimInterval + 200*time.Millisecond)
	if rec, _, err := asm.jsa.loadWillOutbox(subject); err != nil || rec != nil {
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
		WillID: "ndrf-w", ClientIDHash: getHash("ndrf-pub"), Topic: "will/ndrf", Msg: []byte("fire"), QoS: 0,
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
		WillID: "lesol-w", ClientIDHash: getHash("lesol-pub"), Topic: "will/lesol", Msg: []byte("suppressed"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// The claimant fence cancels the superseded Will; it is never delivered.
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		rec, _, err := asm.jsa.loadWillOutbox(subject)
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
		WillID: "fild-w", ClientIDHash: getHash("fild-pub"), Topic: "will/fild", Msg: []byte("later"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// While loads fail, the record stays pending (deferred) and nothing is delivered.
	time.Sleep(500 * time.Millisecond)
	if rec, _, err := asm.jsa.loadWillOutbox(subject); err != nil || rec == nil || rec.State != mqttWillPending {
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
// cancel (completed before B's CONNACK) and A's asynchronous cancel (via
// processSessionPersist, awaited through the cancel observer) both run on the still
// EMPTY outbox. Only then is A's real create released, so the sole thing that can
// suppress the record is the new fence. A far-future FireAt (60s) keeps the claim
// loop out of the picture entirely, so any cancellation is attributable to the
// ENQUEUE-side fence, not the claimant. Broken origin-sequence plumbing
// (OriginSessionSeq left 0) bypasses the fence, so the record simply stays pending
// (its 60s FireAt is never reached during the test) and the 3s cancellation
// assertion below times out — which is how this test fails on a broken fence.
func TestMQTTWillOutboxTakeoverLateEnqueue(t *testing.T) {
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

	// Observe A's asynchronous cancel AFTER A's own CONNECT — that CONNECT's own
	// cancelWillOutbox would otherwise pre-fill the channel with a stale token. A's
	// own session saves are ignored by its processSessionPersist (self jsa.id), so
	// from here the only observer fire is A's takeover-triggered async cancel below.
	aCancelled := make(chan struct{}, 4)
	setWillCancelDoneHook(asmA, func() {
		select {
		case aCancelled <- struct{}{}:
		default:
		}
	})

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

	// A's takeover-triggered async cancel MUST complete on the still-empty outbox
	// before release, so no stray old cancel can later mask a broken fence.
	select {
	case <-aCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("A's takeover-triggered async cancel never completed on the empty outbox")
	}
	// Stop observing/cancelling further so nothing races the released enqueue.
	setWillCancelDoneHook(asmA, nil)

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
