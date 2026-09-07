// Copyright 2025 The mqtt-nats-bridge Authors
// SPDX-License-Identifier: Apache-2.0

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMQTTQoS0DowngradeSweepReplicated is the cross-broker half of the consumer-release
// item, and it exists because the orphan sweep's protection is LOCAL.
//
// The sweep spares every durable a RESIDENT session names in sess.cons, but that pass walks
// only its own broker's as.sessions (mqtt.go:7852); the record walk and the deletion follow
// independently. No CAS fences a peer's consumer deletion either — the record's
// expected-subject-sequence CAS arbitrates record WRITES. So between the confirmed save and
// the teardown, a PEER broker can delete the durable this packet is about to delete anyway.
//
// Two arms, and they are the two halves of the same rule:
//
//   - The DESTRUCTIVE arm: the peer deletes, and that is the accepted disposition. Every
//     route into that window is destructive by construction — the pending teardown is the
//     one that would delete the same durable — so the peer performs the packet's own
//     decision early. Asserted rather than claimed away, and the paired mutation is the
//     tempting "spare anything that might belong to a resident session" guard.
//   - The ADOPTED arm: the same peer sweep must NOT delete a durable an adoption has moved
//     into sess.cons while the membership is still at the pre-packet QoS 0. That durable
//     still owes the client a started exchange, and what protects it is the record naming
//     it — which is why the record builder keys on membership PRESENCE and why the
//     positive-QoS predicate this item first proposed was refused.
//
// Selected by Makefile's cluster-failover-tests target through the *Replicated suffix.
func TestMQTTQoS0DowngradeSweepReplicated(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b0 := c.startBroker(t, 0)
	defer b0.cleanup()
	b1 := c.startBroker(t, 1)
	defer b1.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	// The PEER's account manager, taken from broker 1 directly: it has no session for this
	// client, which is exactly the condition that makes its sparing pass blind.
	peerASM := b1.s.mqtt.sessmgr.sessions[globalAccountName]
	if peerASM == nil {
		t.Fatal("the peer broker has no account session manager yet")
	}

	t.Run("the peer sweep deletes a durable whose teardown is owed", func(t *testing.T) {
		const (
			cid   = "qos0sweepdel"
			topic = "qos0sweepdel/topic"
		)
		cl, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid}, b0.opts.MQTT.Host, b0.opts.MQTT.Port)
		defer cl.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, cl, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
		testMQTTFlush(t, cl, nil, r)

		sess := testMQTTResidentSession(t, b0.s, cid)
		dur, sid := testDowngradeDurable(t, sess, topic)

		// Hold broker A inside its own teardown, which is the window this test is about.
		// The hook fires BEFORE the teardown mutates any state, so the durable is still
		// there and the record has already landed without naming it.
		paused := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		restore := setMQTTTestDeferredDeleteHook(func(gotSid, _ string) {
			if gotSid != sid {
				return
			}
			once.Do(func() { close(paused) })
			<-release
		})
		defer restore()
		// Released on every exit path: a leaked hook blocks a broker goroutine for the rest
		// of the binary.
		var releasedOnce sync.Once
		releaseA := func() { releasedOnce.Do(func() { close(release) }) }
		defer releaseA()

		// Written directly: the packet blocks in the hook, so the SUBACK cannot be waited
		// for yet.
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
		w.WriteVarInt(2 + 2 + len(topic) + 1)
		w.WriteUint16(2)
		w.WriteBytes([]byte(topic))
		w.WriteByte(0)
		if _, err := testMQTTWrite(cl, w.Bytes()); err != nil {
			t.Fatalf("write SUBSCRIBE: %v", err)
		}
		select {
		case <-paused:
		case <-time.After(10 * time.Second):
			t.Fatal("the downgrade never reached its teardown, so the window this test needs " +
				"was never opened")
		}

		// The landed record already omits the consumer, which is what leaves the peer
		// nothing to spare it with.
		ps, _ := testReconcileRecord(t, peerASM, cid)
		if qos, ok := ps.Subs[sid]; !ok || qos != 0 {
			t.Fatalf("the landed record must carry the membership at QoS 0, got %v (present %v)", qos, ok)
		}
		if cc := ps.Cons[sid]; cc != nil {
			t.Fatalf("the landed record must not name the released consumer, got %+v", cc)
		}

		// THE PEER SWEEPS. Its cutoff is the durable's creation time, so an already-created
		// consumer qualifies immediately: this is not a grace period starting at the
		// downgrade.
		peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
		checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
			if testDurableExists(t, b0.s, dur) {
				return fmt.Errorf("the peer's sweep must delete a durable no record and no "+
					"session of ITS OWN names: %q", dur)
			}
			return nil
		})

		// And broker A's teardown tolerates the already-absent durable: the packet still
		// completes and the client is still served.
		releaseA()
		// The packet's own SUBACK arrives only now, so it is read before anything else is
		// asked of the connection.
		testMQTTReadSubAckAnyCode(t, r, 1)
		testMQTTFlush(t, cl, nil, r)
		testMQTTSub(t, 3, cl, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
		testMQTTFlush(t, cl, nil, r)
	})

	t.Run("the peer sweep spares an adopted durable at a QoS 0 membership", func(t *testing.T) {
		const (
			cid   = "qos0sweepkeep"
			topic = "qos0sweepkeep/topic"
		)
		cl, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
			props: mqttV5ConnPropsSessionExpiry(300)}, b0.opts.MQTT.Host, b0.opts.MQTT.Port)
		defer cl.Close()
		testMQTTReadConnAckV5(t, r)
		if codes := testMQTTSubV5(t, cl, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
			t.Fatalf("SUBACK: %v", codes)
		}
		testMQTTFlush(t, cl, nil, r)

		sess := testMQTTResidentSession(t, b0.s, cid)
		dur, sid := testDowngradeDurable(t, sess, topic)

		// A started, unacknowledged QoS 1 exchange: this is what the drain preserves and
		// what an early sweep would destroy.
		pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid + "pub", cleanStart: true},
			b0.opts.MQTT.Host, b0.opts.MQTT.Port)
		defer pc.Close()
		testMQTTReadConnAckV5(t, pr)
		testMQTTPublishV5(t, pc, 1, 1, topic, []byte("owed"))
		if reason, _ := testMQTTReadPubAckV5(t, pr, 1); reason != mqttReasonSuccess {
			t.Fatalf("PUBACK to the publisher: 0x%x", reason)
		}
		_, _, _, pi, ok := testMQTTTryReadPublishV5(r, 5*time.Second)
		if !ok {
			t.Fatal("fixture: the subscriber never received the QoS 1 delivery, so no started " +
				"exchange exists to protect")
		}

		// The downgrade converts it into a drain, which is d554f13's guarantee.
		if codes := testMQTTSubV5(t, cl, r, 2, []mqttV5SubFilter{{topic: topic, opts: 0}}); len(codes) != 1 || codes[0] != 0 {
			t.Fatalf("SUBACK for the downgrade: %v", codes)
		}
		testMQTTFlush(t, cl, nil, r)

		// Hold the packet that ADOPTS it, at the shared filter's early save, so the peer
		// sweeps while that record is still the current one. Sampling later cannot serve:
		// after the final update the membership is QoS 1 and the consumer is named by the
		// ordinary rule, so a sweep then proves nothing about this window.
		paused := make(chan struct{})
		release := make(chan struct{})
		var once, releasedOnce sync.Once
		var saves atomic.Int32
		restore := setMQTTTestSessStoreResultHook(
			func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
				if hs != sess || err != nil {
					return resp, err
				}
				if saves.Add(1) == 1 {
					once.Do(func() { close(paused) })
					<-release
				}
				return resp, err
			})
		defer restore()
		releaseA := func() { releasedOnce.Do(func() { close(release) }) }
		defer releaseA()

		sw := newMQTTWriter(0)
		const shared = "$share/qos0sweepkeepg/other"
		sw.WriteByte(mqttPacketSub | mqttSubscribeFlags)
		sw.WriteVarInt(2 + 1 + (2 + len(topic) + 1) + (2 + len(shared) + 1))
		sw.WriteUint16(3)
		sw.WriteByte(0) // v5: empty property length
		sw.WriteBytes([]byte(topic))
		sw.WriteByte(1)
		sw.WriteBytes([]byte(shared))
		sw.WriteByte(1)
		if _, err := testMQTTWrite(cl, sw.Bytes()); err != nil {
			t.Fatalf("write SUBSCRIBE: %v", err)
		}
		select {
		case <-paused:
		case <-time.After(10 * time.Second):
			t.Fatal("the adopting packet never reached a record write, so the mid-packet " +
				"window this arm needs was never opened")
		}

		// The current record must name the adopted durable — that is what protects it — and
		// the peer's sweep must therefore spare it.
		ps, _ := testReconcileRecord(t, peerASM, cid)
		if qos, present := ps.Subs[sid]; !present || qos != 0 {
			t.Fatalf("this arm needs the PRE-PACKET QoS 0 membership to still be current, got "+
				"%v (present %v)", qos, present)
		}
		if cc := ps.Cons[sid]; cc == nil || cc.Durable != dur {
			t.Fatalf("the current record must name the adopted durable %q in Cons, got %+v: "+
				"without it the peer below deletes a live exchange", dur, cc)
		}
		peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
		if !testDurableExists(t, b0.s, dur) {
			t.Fatalf("the peer's sweep deleted an ADOPTED durable %q that still owes a started "+
				"exchange: the record named it, and that naming is the only thing protecting "+
				"it across brokers", dur)
		}

		releaseA()
		// The adopting packet's SUBACK arrives only now. Read as a v5 packet: its variable
		// header carries a property block between the identifier and the reason codes.
		b, pl := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketSubAck {
			t.Fatalf("expected the adopting packet's SUBACK, got packet type %x", pt)
		}
		start := r.pos
		if _, err := r.readUint16("suback pi"); err != nil {
			t.Fatalf("reading the SUBACK identifier: %v", err)
		}
		if _, err := r.readProperties(mqttPacketSubAck); err != nil {
			t.Fatalf("reading the SUBACK properties: %v", err)
		}
		for r.pos-start < pl {
			if _, err := r.readByte("suback reason code"); err != nil {
				t.Fatalf("reading a SUBACK reason code: %v", err)
			}
		}
		// The exchange the drain preserved still completes under its original identifier.
		testMQTTSendPIPacket(mqttPacketPubAck, t, cl, pi)
		testMQTTFlush(t, cl, nil, r)
	})
}

// TestMQTTQoS0DowngradeSweepAfterBrokerShutdownReplicated is the convergence half of the
// same window: broker A GOES AWAY while the durable is alive and named nowhere, and the
// peer is the only thing left that can reap it. It is a separate top-level test because it
// stops a broker, which inside the previous test's cluster would break its other arm.
//
// A is stopped GRACEFULLY, and the shutdown lifecycle is AWAITED to completion. This is
// deliberately NOT an abrupt-loss fixture: an abrupt death is not reachable in an
// in-process harness, and Shutdown() is not one either — it calls BeginShutdown() and
// waits for the phased drain (shim_public.go), so a fixture claiming otherwise would be
// describing a mechanism it does not use. Nothing here is claimed about abrupt orderings,
// and the residual in docs/CONFORMANCE.md says "a broker that is gone" for that reason.
//
// What ordering 1 asserts about A is therefore bounded, and stated as such: the durable
// survives A's COMPLETED shutdown lifecycle. That is a regression barrier, not evidence
// about the release — A's drain walks live clients (drainClients) and the periodic passes
// that walk as.sessions stop with quitCh, so this session, already unbound when its
// unresolved packet closed the connection, is reached by nothing in that lifecycle. No
// mutation in the tree kills the assertion, which is recorded rather than dressed up.
//
// Both orderings the design owes are here, and both use the AMBIGUOUS-LANDED route rather
// than a paused teardown. That choice is deliberate and it is what makes stopping A safe:
// on that route processSubs returns before its deferred-delete loop and a destructive
// descriptor registers no continuation, so the record lands carrying the QoS 0 membership
// with the durable alive and named nowhere — and no broker goroutine is parked when the
// shutdown runs. Parking one and then draining the broker that owns it is how such a
// fixture hangs instead of proving anything.
func TestMQTTQoS0DowngradeSweepAfterBrokerShutdownReplicated(t *testing.T) {
	// Each arm gets its own cluster: one of them shuts its broker A down.
	run := func(t *testing.T, sweepBeforeShutdown bool) {
		t.Helper()
		c := testNewJetStreamCluster(t, 3)
		b0 := c.startBroker(t, 0)
		b1 := c.startBroker(t, 1)
		defer b1.cleanup()
		c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

		peerASM := b1.s.mqtt.sessmgr.sessions[globalAccountName]
		if peerASM == nil {
			t.Fatal("the peer broker has no account session manager yet")
		}

		cid := "qos0gone" + map[bool]string{true: "after", false: "before"}[sweepBeforeShutdown]
		topic := cid + "/topic"
		cl, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid}, b0.opts.MQTT.Host, b0.opts.MQTT.Port)
		defer cl.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, cl, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
		testMQTTFlush(t, cl, nil, r)

		sess := testMQTTResidentSession(t, b0.s, cid)
		dur, sid := testDowngradeDurable(t, sess, topic)
		// Taken while the connection is still UP: the helper finds the manager through the
		// live client, and the unresolved SUBSCRIBE below closes that connection.
		localASM := testMQTTGetAccountSessionManager(t, b0.s, cid)

		// The teardown must not run on this route, and that is observed rather than assumed.
		var tornDown atomic.Bool
		restoreDel := setMQTTTestDeferredDeleteHook(func(gotSid, _ string) {
			if gotSid == sid {
				tornDown.Store(true)
			}
		})
		defer restoreDel()

		// Lose the response to the packet's final save exactly once: the write LANDS, its
		// acknowledgement does not.
		var lost atomic.Int32
		observed := make(chan struct{})
		var once sync.Once
		restore := setMQTTTestSessStoreResultHook(
			func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
				if hs != sess || err != nil {
					return resp, err
				}
				if lost.Add(1) == 1 {
					once.Do(func() { close(observed) })
					return nil, errors.New("test: session record response lost")
				}
				return resp, err
			})
		defer restore()

		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
		w.WriteVarInt(2 + 2 + len(topic) + 1)
		w.WriteUint16(2)
		w.WriteBytes([]byte(topic))
		w.WriteByte(0)
		if _, err := testMQTTWrite(cl, w.Bytes()); err != nil {
			t.Fatalf("write SUBSCRIBE: %v", err)
		}
		select {
		case <-observed:
		case <-time.After(10 * time.Second):
			t.Fatal("the packet's final save never reached the store-result hook, so the " +
				"ambiguity this test needs was never created")
		}
		// The landed record — read through the PEER, which is the broker that will sweep —
		// carries the membership at QoS 0 and names the durable nowhere. That is what leaves
		// the peer nothing to spare it with once A is gone, and its sequence is the positive
		// marker the resolution barrier below waits for.
		var landedSeq uint64
		checkFor(t, 15*time.Second, 25*time.Millisecond, func() error {
			ps, seq := testReconcileRecord(t, peerASM, cid)
			if qos, ok := ps.Subs[sid]; !ok || qos != 0 {
				return fmt.Errorf("waiting for the landed record to carry the membership at QoS 0, got %v (present %v)", qos, ok)
			}
			if cc := ps.Cons[sid]; cc != nil {
				return fmt.Errorf("the landed record still names the released consumer %q", cc.Durable)
			}
			landedSeq = seq
			return nil
		})

		// SETTLE THE AMBIGUITY, positively. The store hook signals BEFORE it returns the
		// injected error, so at that point the packet has not yet reached its saveMaybeLanded
		// arm and the session is not uncertain YET: a poll for !isSeqUncertain() would pass
		// on the value the flag has before arming. Wait for the ambiguity to be PUBLISHED (or
		// the landed sequence already adopted, which only settleLandedAttempt does), then run
		// the resolution here on reconcileResidentRecordState's route for an unbound
		// uncertain session, and take the record fence afterwards — the landed continuations
		// run inside it, so an idle fence is what proves the settling pass has finished.
		checkFor(t, 15*time.Second, 25*time.Millisecond, func() error {
			sess.mu.Lock()
			armed, adopted := sess.seqUncertain, sess.seq
			sess.mu.Unlock()
			if !armed && adopted < landedSeq {
				return fmt.Errorf("waiting for the packet to publish the ambiguity of its final save")
			}
			return nil
		})
		checkFor(t, 15*time.Second, 25*time.Millisecond, func() error {
			if err := localASM.lockSession(sess, nil); err != nil {
				return fmt.Errorf("waiting for the session's ownership gate: %w", err)
			}
			res := b0.s.resolveUncertainSession(localASM, sess)
			localASM.unlockSession(sess)
			if res != mqttUncertainOurs {
				return fmt.Errorf("the resolver must classify a write that LANDED as ours, got %v", res)
			}
			return nil
		})
		checkFor(t, 15*time.Second, 25*time.Millisecond, func() error {
			tok, ok := sess.tryBeginRecordTx()
			if !ok {
				return fmt.Errorf("waiting for the resolver to release the record fence")
			}
			if err := sess.endRecordTx(tok); err != nil {
				return fmt.Errorf("releasing the probe transaction: %w", err)
			}
			return nil
		})
		if sess.isSeqUncertain() {
			t.Fatal("the session must be RESOLVED before broker A is stopped: an unresolved " +
				"session refuses every save, so its shutdown would prove nothing")
		}
		if tornDown.Load() {
			t.Fatal("the ambiguous-landed route must leave the teardown unrun: that is what " +
				"makes the durable an orphan at the moment broker A goes away")
		}
		if !testDurableExists(t, b0.s, dur) {
			t.Fatalf("the durable %q must be alive before the broker is stopped", dur)
		}

		// Stop A and WAIT FOR THE LIFECYCLE TO COMPLETE. Shutdown()'s own wait is bounded
		// and returns either way, so it cannot stand in for completion: a phase still
		// running would leave every assertion below racing a broker that is still working.
		stopA := func() {
			done := b0.s.BeginShutdown()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("broker A's shutdown lifecycle did not complete: nothing below can " +
					"distinguish a broker that is GONE from one still draining")
			}
		}

		if sweepBeforeShutdown {
			// ORDERING 2 — the peer deletes, THEN A goes away. Nothing is owed afterwards:
			// there is no durable left and no teardown to run.
			peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
			checkFor(t, 15*time.Second, 50*time.Millisecond, func() error {
				if testDurableExists(t, b1.s, dur) {
					return fmt.Errorf("the peer's sweep must reap the orphan %q", dur)
				}
				return nil
			})
			stopA()
			if tornDown.Load() {
				t.Fatal("nothing may have run the teardown: the route never scheduled one")
			}
			// The deletion STAYS after A's completed shutdown — a drain that re-created or
			// resurrected anything would show up here — and a later sweep is a no-op rather
			// than an error, because nothing is owed.
			if testDurableExists(t, b1.s, dur) {
				t.Fatalf("the durable %q must still be absent once A's shutdown lifecycle "+
					"has completed", dur)
			}
			peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
			if testDurableExists(t, b1.s, dur) {
				t.Fatalf("a second sweep must leave the durable %q absent", dur)
			}
			return
		}

		// ORDERING 1 — A goes away BEFORE either deletion. The durable survives A's own
		// completed shutdown lifecycle, no record and no surviving session names it, and a
		// LATER peer sweep reaps it. The sweep half is the convergence claim the residual
		// rests on; the survival half is a bounded regression barrier (see the header).
		stopA()
		if !testDurableExists(t, b1.s, dur) {
			t.Fatalf("the durable %q must survive broker A's COMPLETED shutdown lifecycle: "+
				"nothing in that lifecycle reaches an unbound session's consumers", dur)
		}
		if tornDown.Load() {
			t.Fatal("and A's drain must not have run the deferred teardown either: the " +
				"ambiguous-landed route never scheduled one")
		}
		peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
		checkFor(t, 15*time.Second, 50*time.Millisecond, func() error {
			if testDurableExists(t, b1.s, dur) {
				return fmt.Errorf("a later peer sweep must reap the orphan %q left by a broker "+
					"that is gone: it is the only convergence point this route has", dur)
			}
			return nil
		})
	}

	// NOT parallel: both arms install process-global hooks.
	t.Run("broker A is shut down before either deletion", func(t *testing.T) { run(t, false) })
	t.Run("broker A is shut down after the peer deleted", func(t *testing.T) { run(t, true) })
}
