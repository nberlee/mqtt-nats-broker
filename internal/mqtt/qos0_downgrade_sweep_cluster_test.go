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

	"github.com/nats-io/nuid"
)

// TestMQTTQoS0DowngradeSweepReplicated is the cross-broker half of the consumer-release
// item, and it exists because the orphan sweep's sparing pass is LOCAL.
//
// The sweep spares every durable a RESIDENT session names in sess.cons, but that pass walks
// only its own broker's as.sessions; the record walk follows independently. No CAS fences a
// peer's consumer deletion either — the record's expected-subject-sequence CAS arbitrates
// record WRITES. So between the confirmed save and the teardown, a PEER broker sees a durable
// named by neither its own residents nor the record.
//
// Since F-105 (§5.1 point 5) what licenses the sweep's delete is the durable name's RETIRED
// ownership, never the absence of a reference, and the two arms are two halves of that rule:
//
//   - The RELEASED arm: inside the window the name is still LIVE — the packet has decided
//     nothing yet — so the peer's sweep WITHHOLDS it. The packet's own teardown records the
//     disposal decision and the retirement owner retires the name and deletes the durable
//     (the executor route). A delayed CREATE that resurrects it afterwards is an orphan
//     under a RETIRED name, which the peer reaps in the same pass that withholds an
//     MQTT-shaped durable no ownership record claims. Before F-105 the peer deleted the
//     durable early, on absence alone; that premise is gone.
//   - The ADOPTED arm: the same peer sweep must NOT delete a durable an adoption has moved
//     into sess.cons while the membership is still at the pre-packet QoS 0. That durable
//     still owes the client a started exchange, and the record naming it protects it —
//     which is why the record builder keys on membership PRESENCE and why the positive-QoS
//     predicate this item first proposed was refused.
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

	t.Run("the peer sweep withholds a LIVE released durable and reaps it once RETIRED", func(t *testing.T) {
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
		// downgrade. The name is still LIVE — nothing has decided the consumer's end yet —
		// so the peer must withhold it, however unreferenced it looks from there.
		testRequireNameOwnership(t, peerASM, dur, mqttNameOwnLive)
		peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
		if !testDurableExists(t, b0.s, dur) {
			t.Fatalf("the peer's sweep deleted %q while its name stands LIVE: a reference's absence "+
				"never licenses a delete, only a RETIRED name does", dur)
		}

		// The packet completes and the client is still served.
		releaseA()
		// The packet's own SUBACK arrives only now, so it is read before anything else is
		// asked of the connection.
		testMQTTReadSubAckAnyCode(t, r, 1)
		testMQTTFlush(t, cl, nil, r)

		// THE EXECUTOR ROUTE: the teardown handed its decision to the retirement owner, which
		// retires the name and deletes the durable.
		testWaitSourceRetired(t, peerASM, b1.s, dur, mqttDisposalDowngraded)

		// A delayed CREATE resurrects the durable under its RETIRED name, beside an
		// MQTT-shaped durable no ownership record claims; one peer pass reaps the first and
		// withholds the second.
		testRecreatePlainDurable(t, peerASM, dur, topic)
		unowned := testCreateUnownedPlainDurable(t, peerASM, topic)
		peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
		if testDurableExists(t, b1.s, dur) {
			t.Fatalf("the peer's sweep left %q, resurrected under a RETIRED name", dur)
		}
		if !testDurableExists(t, b1.s, unowned) {
			t.Fatalf("the peer's sweep deleted %q, whose name no ownership record claims: a missing "+
				"record is withheld, never read as permission", unowned)
		}

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
// same window: broker A GOES AWAY while the durable is alive and named nowhere. It is a
// separate top-level test because it stops a broker, which inside the previous test's
// cluster would break its other arm.
//
// Since F-105 the peer's sweep is NOT the convergence point: on this route nothing decided
// the consumer's end, so its name stands LIVE and every peer pass WITHHOLDS it (a reference's
// absence never licenses a delete; only a RETIRED name does). What converges it is the
// session's next ACCEPTED tenure (outbound_tenure_settle.go): the client reconnects — here to
// the peer — and the settlement finds a source consumer provisioned under a tenure that no
// longer serves and named by nothing the serving tenure holds, decides it, and hands it to the
// retirement owner, which retires the name and deletes the durable. A delayed CREATE after that
// is a RETIRED-name orphan the peer reaps, in the same pass that withholds an MQTT-shaped
// durable no ownership record claims.
//
// A is stopped GRACEFULLY, and the shutdown lifecycle is AWAITED to completion. This is
// deliberately NOT an abrupt-loss fixture: an abrupt death is not reachable in an
// in-process harness, and Shutdown() is not one either — it calls BeginShutdown() and
// waits for the phased drain (shim_public.go), so a fixture claiming otherwise would be
// describing a mechanism it does not use. Nothing here is claimed about abrupt orderings.
//
// What ordering 1 asserts about A is therefore bounded, and stated as such: the durable
// survives A's COMPLETED shutdown lifecycle. That is a regression barrier, not evidence
// about the release — A's drain walks live clients (drainClients) and the periodic passes
// that walk as.sessions stop with quitCh, so this session, already unbound when its
// unresolved packet closed the connection, is reached by nothing in that lifecycle. No
// mutation in the tree kills the assertion, which is recorded rather than dressed up.
//
// Both orderings use the AMBIGUOUS-LANDED route rather than a paused teardown. That choice
// is deliberate and it is what makes stopping A safe: on that route processSubs returns
// before its deferred-delete loop and a destructive descriptor registers no continuation, so
// the record lands carrying the QoS 0 membership with the durable alive and named nowhere —
// and no broker goroutine is parked when the shutdown runs. Parking one and then draining
// the broker that owns it is how such a fixture hangs instead of proving anything.
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

		// The peer's sweep, while A is still up: the name stands LIVE and undecided, so the
		// durable is withheld however unreferenced it looks from the peer.
		testRequireNameOwnership(t, peerASM, dur, mqttNameOwnLive)
		peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
		if !testDurableExists(t, b1.s, dur) {
			t.Fatalf("the peer's sweep deleted %q while its name stands LIVE: a reference's absence "+
				"never licenses a delete, only a RETIRED name does", dur)
		}

		// The convergence point: the session's next accepted tenure, on the peer, settles the
		// consumer nothing names and the retirement owner retires it.
		resume := func() {
			t.Helper()
			rc, rr := testMQTTConnect(t, &mqttConnInfo{clientID: cid}, b1.opts.MQTT.Host, b1.opts.MQTT.Port)
			t.Cleanup(func() { rc.Close() })
			testMQTTCheckConnAck(t, rr, mqttConnAckRCConnectionAccepted, true)
			testWaitSourceRetired(t, peerASM, b1.s, dur, mqttDisposalTenureSuperseded)
		}
		// A delayed CREATE under the RETIRED name, beside an MQTT-shaped durable no ownership
		// record claims: one peer pass reaps the first and withholds the second.
		reapResurrected := func() {
			t.Helper()
			testRecreatePlainDurable(t, peerASM, dur, topic)
			unowned := testCreateUnownedPlainDurable(t, peerASM, topic)
			peerASM.sweepOrphanedConsumersBefore(b1.s, time.Now().Add(time.Minute))
			if testDurableExists(t, b1.s, dur) {
				t.Fatalf("the peer's sweep left %q, resurrected under a RETIRED name", dur)
			}
			if !testDurableExists(t, b1.s, unowned) {
				t.Fatalf("the peer's sweep deleted %q, whose name no ownership record claims: a "+
					"missing record is withheld, never read as permission", unowned)
			}
		}

		if sweepBeforeShutdown {
			// ORDERING 2 — the durable is retired, THEN A goes away. Nothing is owed
			// afterwards: there is no durable left and no teardown to run.
			resume()
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
			reapResurrected()
			return
		}

		// ORDERING 1 — A goes away BEFORE the retirement. The durable survives A's own
		// completed shutdown lifecycle and the peer's sweep still withholds it; the resumed
		// tenure on the peer is what converges it. The survival half is a bounded regression
		// barrier (see the header).
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
		if !testDurableExists(t, b1.s, dur) {
			t.Fatalf("the peer's sweep deleted %q once A was gone, while its name stands LIVE: a "+
				"broker's absence is not a retirement", dur)
		}
		resume()
		reapResurrected()
	}

	// NOT parallel: both arms install process-global hooks.
	t.Run("broker A is shut down before the retirement", func(t *testing.T) { run(t, false) })
	t.Run("broker A is shut down after the retirement", func(t *testing.T) { run(t, true) })
}

// testRequireNameOwnership asserts the ownership state of a plain durable's name on
// $MQTT_msgs, read through the given broker.
func testRequireNameOwnership(t *testing.T, asm *mqttAccountSessionManager, durable string, want mqttNameOwnState) {
	t.Helper()
	own, _, err := asm.jsa.loadNameOwnershipPhysical(asm.sessStreamEpoch, mqttOwnedStreamMsgs, durable)
	if err != nil || own == nil || own.State != want {
		t.Fatalf("the name %q must stand in ownership state %d, got %+v (%v)", durable, want, own, err)
	}
}

// testWaitSourceRetired waits for a source consumer's retirement to complete — its name RETIRED,
// its resource CleanupComplete, the durable gone — and asserts the route that decided it, by the
// disposal reason the decision recorded.
func testWaitSourceRetired(t *testing.T, asm *mqttAccountSessionManager, s *Server, durable, reason string) {
	t.Helper()
	var res *mqttOutboundResource
	checkFor(t, 15*time.Second, 50*time.Millisecond, func() error {
		own, _, err := asm.jsa.loadNameOwnershipPhysical(asm.sessStreamEpoch, mqttOwnedStreamMsgs, durable)
		if err != nil || own == nil || own.State != mqttNameOwnRetired {
			return fmt.Errorf("the name %q is not RETIRED yet: %+v (%v)", durable, own, err)
		}
		r, _, rerr := asm.jsa.loadOutboundResource(own.ID, asm.sessStreamEpoch, own.OwnerGen, own.OwnerInc)
		if rerr != nil || r == nil || r.State != mqttResourceCleanupComplete {
			return fmt.Errorf("the cleanup of %q is not complete: %+v (%v)", durable, r, rerr)
		}
		if testDurableExists(t, s, durable) {
			return fmt.Errorf("the retired durable %q still exists", durable)
		}
		res = r
		return nil
	})
	if res.Disposal == nil || res.Disposal.Reason != reason {
		t.Fatalf("the durable %q was retired by an unexpected route: disposal %+v, want reason %q",
			durable, res.Disposal, reason)
	}
}

// testRecreatePlainDurable is a delayed CREATE landing under a name after its retirement.
func testRecreatePlainDurable(t *testing.T, asm *mqttAccountSessionManager, durable, topic string) {
	t.Helper()
	conv, _ := mqttToNATSSubjectConversion([]byte(topic), false)
	if _, err := asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: durable, DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew,
			FilterSubject: mqttStreamSubjectPrefix + string(conv)}}); err != nil {
		t.Fatalf("the delayed CREATE of %q: %v", durable, err)
	}
}

// testCreateUnownedPlainDurable creates an MQTT-shaped durable (delivering to "$MQTT.sub.") that
// no record, no session and no ownership record names, and returns its name.
func testCreateUnownedPlainDurable(t *testing.T, asm *mqttAccountSessionManager, topic string) string {
	t.Helper()
	durable := "unowned_" + nuid.Next()
	testRecreatePlainDurable(t, asm, durable, topic)
	return durable
}
