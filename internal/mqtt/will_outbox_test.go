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

// TestMQTTWillOutboxLegacyMigration: a delayed Will persisted in the legacy
// session-record format (as a pre-Layer-2 build wrote it) is migrated to the
// outbox by a starting replica and fires exactly once, and the migration is
// idempotent (a terminal tombstone stops re-creation).
func TestMQTTWillOutboxLegacyMigration(t *testing.T) {
	oA := testMQTTDefaultOptionsV5()
	sA := testMQTTRunServer(t, oA)
	defer testMQTTShutdownServer(sA)

	// Subscriber for the migrated Will topic.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "migsub", cleanStart: true}, oA.MQTT.Host, oA.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/mig", opts: 0}})

	// Seed a legacy session record carrying a delayed Will, due shortly. sA's
	// account manager already ran migration at start (on an empty stream), so it
	// will not re-migrate; the second replica's start does.
	nc, js := jsClientConnect(t, sA)
	defer nc.Close()
	cidHash := getHash("migpub")
	ps := mqttPersistedSession{
		Origin: "old-node",
		ID:     "migpub",
		Will: &mqttPersistedWill{
			Topic:  "will/mig",
			Msg:    []byte("legacy-bye"),
			QoS:    0,
			FireAt: time.Now().Add(200 * time.Millisecond).UnixNano(),
		},
		ExpiryInterval: 300,
	}
	b, _ := json.Marshal(&ps)
	if _, err := js.Publish(mqttSessStreamSubjectPrefix+cidHash, b); err != nil {
		t.Fatalf("seed legacy session record: %v", err)
	}

	// Second replica: its eager init migrates the legacy Will to the outbox,
	// which then fires.
	sB, _ := runSharedReplica(t, oA)
	defer sB.Shutdown() // stop replica B's MQTT before the shared NATS goes down

	// Fires exactly once (the deterministic-WillID create-only CAS dedupes the
	// two replicas' migrations, and the pending->publishing CAS dedupes firing).
	testMQTTReadPublishV5(t, rs, "will/mig", []byte("legacy-bye"))
	testMQTTExpectNothing(t, rs)

	// The legacy Will field must be cleared from the session record.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		m, err := js.GetLastMsg(mqttSessStreamName, mqttSessStreamSubjectPrefix+cidHash)
		if err != nil {
			return nil // record gone entirely: fine
		}
		if json.Valid(m.Data) && containsJSONKey(m.Data, "will") {
			return fmt.Errorf("legacy will not cleared from session record: %s", m.Data)
		}
		return nil
	})
}

// containsJSONKey reports whether the JSON object bytes contain a non-null value
// for key (a cheap check sufficient for these tests).
func containsJSONKey(data []byte, key string) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return false
	}
	v, ok := m[key]
	return ok && string(v) != "null"
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

// setWillPublishHook installs (or clears) the instance-scoped publish seam on asm.
func setWillPublishHook(asm *mqttAccountSessionManager, fn func(rec *mqttWillOutbox) error) {
	asm.mu.Lock()
	asm.testPublishWill = fn
	asm.mu.Unlock()
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

// testWillRec pairs an outbox record with the subject it lives on.
type testWillRec struct {
	subject string
	rec     *mqttWillOutbox
}

// testMQTTListWillRecords returns every outbox record for the account paired with
// its subject, in stream order.
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
		out = append(out, testWillRec{subject: smsg.Subject, rec: rec})
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
