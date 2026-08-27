//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

// The v5 UNSUBSCRIBE is the ONE operation this broker acknowledges over a record
// write whose outcome it does not know: it overwrites every per-filter reason with
// 0x83 and still sends the UNSUBACK. A write that turns out to be durable therefore
// leaves the client told the unsubscribe failed while the record says it happened.
//
// The resolver settles such a write forward, which would make that contradiction
// deterministic — so the acknowledgment waits for the resolution the readLoop
// performs anyway and tells the truth whenever proof has arrived by then. The signal
// is the attempt's OWN landed continuation, never a session-state predicate.

// testUnsubAckSession returns the resident session of a connected client.
func testUnsubAckSession(t *testing.T, s *Server, cid string) *mqttSession {
	t.Helper()
	return testMQTTResidentSession(t, s, cid)
}

// TestMQTTUnsubAckIsTruthfulOnceTheWriteIsSettled is the acknowledged caller's half
// of the settlement contract, end to end.
//
// The packet's record write never reaches the stream, so the resolution the readLoop
// runs settles it by re-attempting the packet's own bytes. By the time the UNSUBACK
// is built the unsubscribe IS durable, so the client must be told what actually
// happened — including the Reason String, which the encoder carries from the first
// failed filter and which would otherwise report "unable to persist unsubscribe" for
// a filter that simply had no subscription.
//
// Mutation M8: skip the restore and the client is acknowledged 0x83 for an
// unsubscribe the record carries.
// Mutation M10: restore the reason byte but not the reason string, and the Reason
// String still reads "unable to persist unsubscribe".
func TestMQTTUnsubAckIsTruthfulOnceTheWriteIsSettled(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsubtruth", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "truth/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testUnsubAckSession(t, s, "unsubtruth")

	// The UNSUBSCRIBE's write is suppressed exactly once: it never reaches the
	// stream, so only the resolver's re-attempt can make the unsubscribe durable.
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sp != sess || err != nil {
			return resp, err
		}
		sp.mu.Lock()
		_, still := sp.subs["truth.topic"]
		sp.mu.Unlock()
		if !still && once.fire() {
			// The packet's own write: the membership is already out of memory.
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	defer restore()

	// One filter that exists and one that never did: their true reasons differ, so a
	// blanket restore of a single code could not pass this.
	codes, props := testMQTTUnsubV5Ex(t, c, r, 2, []string{"truth/topic", "truth/never"})
	restore()
	if len(codes) != 2 {
		t.Fatalf("UNSUBACK codes = %v, want two", codes)
	}
	if codes[0] != 0 || codes[1] != mqttReasonNoSubscriptionExisted {
		t.Fatalf("UNSUBACK codes = %v, want [0x00 0x%x]: the unsubscribe is durable, so 0x83 is a lie",
			codes, mqttReasonNoSubscriptionExisted)
	}
	if props == nil || props.reasonString != "no matching subscription existed" {
		t.Fatalf("UNSUBACK Reason String = %+v, want the true one for the 0x11 filter", props)
	}

	// And it really is durable: the resolution settled the write, and the record no
	// longer carries the subscription.
	if sess.isSeqUncertain() {
		t.Fatal("the packet's write must have been settled before the acknowledgment")
	}
	ps := testLoadDrainRecord(t, sess)
	if _, still := ps.Subs["truth.topic"]; still {
		t.Fatalf("the record still carries the unsubscribed filter: %v", ps.Subs)
	}
}

// TestMQTTUnsubAckIsAttemptScopedNotSessionScoped pins WHAT the restore is keyed on.
//
// seqUncertain is not the answer: it is cleared by a foreign disposition as well as
// by a settlement, and it can be re-armed by a completely different attempt. Here the
// packet's own write is settled — so its client is owed the truth — and a SECOND
// ambiguity is armed by another writer in the same breath. A coarse "is the session
// resolved?" check answers NO and keeps the 0x83; the attempt's own landed
// continuation answers YES, because it fired for THIS write.
//
// Mutation M9: key the restore on !sess.isSeqUncertain() and the client is
// acknowledged 0x83 for an unsubscribe that is durably applied.
func TestMQTTUnsubAckIsAttemptScopedNotSessionScoped(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsubscope", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "scope/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testUnsubAckSession(t, s, "unsubscope")

	// The packet's write never reaches the stream, so only the resolver's re-attempt
	// can settle it — which is what puts the seam below inside the resolution.
	// Suppression is armed at the save seam (before the write) and disarmed at the
	// result seam, so exactly one write is affected.
	restoreStore := testSuppressNextUnsubWrite(t, sess, "scope.topic")
	defer restoreStore()

	// Attempt B, armed from INSIDE the resolution: the seam fires after the packet's
	// continuation is registered, so the continuation registered here runs after it —
	// A's flag is already set, and B's ambiguity is armed before the packet tail
	// looks at anything. Continuations run in registration order on the resolving
	// goroutine, so there is no timing dependence.
	var armed atomicOnce
	restoreSeam := setMQTTTestResolveReattemptHook(func(sp *mqttSession) {
		if sp != sess || !armed.fire() {
			return
		}
		sp.retainOnLanded(func(tok recordTx) {
			// A save that goes ambiguous: the session is uncertain AGAIN, on an
			// attempt that has nothing to do with this packet.
			mqttTestSessStoreSuppressV.Store(true)
			_ = sp.save(tok)
			mqttTestSessStoreSuppressV.Store(false)
		})
	})
	defer restoreSeam()

	codes, _ := testMQTTUnsubV5Ex(t, c, r, 2, []string{"scope/topic"})
	restoreSeam()
	restoreStore()
	mqttTestSessStoreSuppressV.Store(false)

	if !armed.fired() {
		t.Fatal("the second ambiguity was never armed; the test proved nothing")
	}
	if !sess.isSeqUncertain() {
		t.Fatal("the second ambiguity must still be outstanding, or this test cannot discriminate")
	}
	if len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("UNSUBACK codes = %v, want [0x00]: the packet's OWN write was proven durable, "+
			"whatever another writer did to the session afterwards", codes)
	}
}

// TestMQTTUnsubAckSuppressedWhenDisplaced pins the other half of the tail: a
// connection the resolution DISPLACED is not acknowledged at all.
//
// A foreign verdict detaches the client and notifies it through the shared DISCONNECT
// claim, so what the client sees is the displacement's 0x89 ownership-unsettled — never an
// UNSUBACK claiming an unsubscribe on a session whose record is no longer ours.
//
// The gate that produces that outcome here is the CLAIM, read inside the enqueue's own
// c.mu critical section: every displacement reaching this tail has a written CONNACK
// and therefore consumes it. Dropping the tail's ownership sample (mutation M12) does
// NOT change this test — recorded as such rather than dressed up, and the sample is
// documented as the defensive half in mqttAckUnsubscribe. What DOES change it is
// mutation M14, on the claim check itself; TestMQTTUnsubAckSuppressedByADisplacementInTheWindow
// drives the interleaving that only the claim can catch.
func TestMQTTUnsubAckSuppressedWhenDisplaced(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsubdisp", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "disp/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testUnsubAckSession(t, s, "unsubdisp")

	// The packet's write never reaches the stream: the resolver's re-attempt is what
	// meets the competing write below, and a rejected re-attempt is what makes the verdict
	// foreign.
	restoreStore := testSuppressNextUnsubWrite(t, sess, "disp.topic")
	defer restoreStore()

	// A competing write lands on the record in the resolver's own window, so the re-attempt
	// is rejected and the classifying re-read finds content that is not this attempt's:
	// FOREIGN. Foreign means the record is NOT OURS; it does not identify a holder, and
	// this connection is displaced on that basis alone (F-084).
	var took atomicOnce
	restoreSeam := setMQTTTestResolveReattemptHook(func(sp *mqttSession) {
		if sp != sess || !took.fire() {
			return
		}
		sp.mu.Lock()
		base := sp.pendingWrite.baseSeq
		domainTk, idHash := sp.domainTk, sp.idHash
		sp.mu.Unlock()
		if _, err := mqttStoreSessionRecordCASBody(sp.jsa, domainTk,
			[]byte(`{"id":"unsubdisp","origin":"someone-else"}`), idHash, base); err != nil {
			t.Errorf("simulate the takeover in the resolver's window: %v", err)
		}
	})
	defer restoreSeam()

	testUnsubAckSendUnsubscribeV5(t, c, 2, "disp/topic")

	// Everything this connection is sent before it closes: the displacement DISCONNECT,
	// and never an UNSUBACK for a session whose record is no longer ours. Read to the CLOSE
	// rather than stopping at the first packet — an acknowledgment enqueued behind that
	// DISCONNECT is exactly the shape being excluded.
	//
	// The packet TYPE is the assertion and the reason code deliberately is not: this route
	// proves no successor, so what it carries is 0x89, and pinning a code here would
	// duplicate the vocabulary fixtures that own that question.
	sawDisconnect := false
	for {
		b, pl, ok := testMQTTReadPacketReady(r, 10*time.Second)
		if !ok {
			break // the connection is gone
		}
		r.pos += pl // skip the payload; only the packet type matters here
		switch b & mqttPacketMask {
		case mqttPacketUnsubAck:
			t.Fatal("a displaced connection was acknowledged: the UNSUBACK claims an unsubscribe " +
				"on a session whose record is no longer ours, and it arrives behind the " +
				"displacement DISCONNECT")
		case mqttPacketDisconnect:
			sawDisconnect = true
		}
	}
	if !took.fired() {
		t.Fatal("the competing write never ran; the test proved nothing")
	}
	if !sawDisconnect {
		t.Fatal("the displaced connection was never notified")
	}
}

// TestMQTTUnsubAckSuppressedByADisplacementInTheWindow is the CHECK/ENQUEUE race:
// the displacement lands AFTER the acknowledgment's ownership sample and BEFORE its
// enqueue.
//
// The ownership sample releases sess.mu, and the enqueue takes c.mu afterwards.
// `processSessionPersist` holds neither, so in that interval it can notify and detach — and
// an UNSUBACK appended after that DISCONNECT violates [MQTT-3.14.4-1]. Which reason code
// that notification carries depends on what the observed record proved and is not this
// fixture's question (F-084); the ORDERING is.
// What closes the window is the second gate: the DISCONNECT claim is read inside the
// enqueue's own c.mu critical section, so the refusal is atomic with the write.
//
// The seam models exactly that leg: the notification through the shared claim, and the
// detach, both from another goroutine while the tail is mid-flight.
//
// Mutation M14: drop the `unlessDisconnected` refusal from mqttEnqueueProtoLocked and
// the UNSUBACK arrives behind the 0x8E.
func TestMQTTUnsubAckSuppressedByADisplacementInTheWindow(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsubrace", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "race/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testUnsubAckSession(t, s, "unsubrace")

	// No ambiguity is needed: the window is a property of the tail itself, on the
	// ordinary path.
	var displaced atomicOnce
	restore := setMQTTTestUnsubAckWindowHook(func(bc *client) {
		if !displaced.fire() {
			return
		}
		// The persist-observation leg, in the one interval it can occupy: notify
		// through the shared claim, then detach. Done on another goroutine so it
		// cannot borrow the readLoop's own locks.
		//
		// The vocabulary is the one that leg actually passes. A persist observation
		// shows the record is not ours and nothing more, so it is unsettled-ownership
		// and not taken-over (F-084); calling mqttNotifyTakenOver here would model a
		// route this seam does not stand for.
		done := make(chan struct{})
		go func() {
			defer close(done)
			bc.mqttNotifyDisplaced(mqttDisposalOwnershipUnsettled)
			sess.mu.Lock()
			sess.c = nil
			sess.mu.Unlock()
		}()
		<-done
	})
	defer restore()

	testUnsubAckSendUnsubscribeV5(t, c, 2, "race/topic")

	b, pl, ok := testMQTTReadPacketReady(r, 10*time.Second)
	if !ok {
		t.Fatal("the displaced connection was never notified")
	}
	r.pos += pl
	if b&mqttPacketMask != mqttPacketDisconnect {
		t.Fatalf("first packet 0x%x, want the displacement DISCONNECT", b&mqttPacketMask)
	}
	if !displaced.fired() {
		t.Fatal("the seam never fired; the test proved nothing")
	}
	// And NOTHING follows it. Under the mutation the UNSUBACK is appended here.
	if b2, _, more := testMQTTReadPacketReady(r, 500*time.Millisecond); more {
		t.Fatalf("packet 0x%x followed the DISCONNECT: an acknowledgment appended after the "+
			"displacement notification breaks [MQTT-3.14.4-1], and claims an unsubscribe on a "+
			"session this connection no longer owns", b2&mqttPacketMask)
	}
}

// TestMQTTUnsubCommitReleasesUnderTheResolverToken is the per-wrapper effect test for
// commitUnsub: the work it defers for want of proof must actually LAND when the
// resolver runs it, which means every write it reaches must use the RESOLVER's record
// transaction and not the packet's.
//
// The packet's transaction is closed long before a deferred settlement runs, and a
// write under a closed token is refused at entry — silently, since none of these
// cleanup calls is checked for an outcome. Threading the token into commitUnsub is not
// enough on its own: it reaches the shared release through removeJSCons, a closure
// defined OUTSIDE it that captured the packet's `tok` and kept using it after the
// signature changed. That is the shape this test exists to catch, and only a test that
// asserts a DURABLE EFFECT can: the group durable of a shared subscription is deleted
// by that nested path once the record no longer names the membership.
//
// What this test does NOT do is guard the token: on this fixture the released consumer
// has no pending registers, so `releaseSharedConsumer` never reaches its token-gated
// record fallback and the durable is deleted either way. It is an end-to-end check that
// commitUnsub runs from the RESOLUTION at all and releases what it owns.
//
// The token wiring is guarded structurally instead — `removeUnsubConsumer` is a method,
// so the packet's transaction is not in scope to be captured (see its comment) — and
// passing the wrong token EXPLICITLY remains uncovered by any test. That gap is stated
// in docs/ROADMAP.md rather than implied away; closing it needs a fixture that forces
// the cancellation tombstone to fall back to a record save AND can observe the
// consumer delete ordered against the verdict's durability, which needs a seam the
// harness does not have.
func TestMQTTUnsubCommitReleasesUnderTheResolverToken(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsubtoken", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	// A SHARED QoS 1 filter, so the packet owns a group durable whose release is the
	// observable effect.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/tokgrp/token/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testUnsubAckSession(t, s, "unsubtoken")

	durs := testDrainStreamDurables(t, sess)
	if len(durs) == 0 {
		t.Fatal("the shared subscription must have created a group durable")
	}

	// The packet's record write never reaches the stream, so the release below can only
	// run from the resolution — under the resolver's token.
	restore := testSuppressNextUnsubWrite(t, sess, "$share/tokgrp/token.topic")
	defer restore()

	codes, _ := testMQTTUnsubV5Ex(t, c, r, 2, []string{"$share/tokgrp/token/topic"})
	restore()
	if len(codes) != 1 {
		t.Fatalf("UNSUBACK = %v", codes)
	}
	if sess.isSeqUncertain() {
		t.Fatal("the packet's write must have been settled by the resolver")
	}

	// The record no longer names the membership, and the group durable it referenced is
	// GONE — released by the nested cleanup, under a token that was still open.
	ps := testLoadDrainRecord(t, sess)
	if len(ps.SharedSubs) != 0 {
		t.Fatalf("the settled record still carries the shared membership: %v", ps.SharedSubs)
	}
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		remaining := testDrainStreamDurables(t, sess)
		for name := range durs {
			if remaining[name] {
				return fmt.Errorf("the group durable %q was not released: commitUnsub's cleanup wrote "+
					"under a CLOSED packet token, so every release it gates was refused at entry", name)
			}
		}
		return nil
	})
}

// testUnsubAckSendUnsubscribeV5 writes an UNSUBSCRIBE without waiting for its
// acknowledgment, for a test whose point is what does (not) come back.
func testUnsubAckSendUnsubscribeV5(t *testing.T, c net.Conn, pi uint16, filter string) {
	t.Helper()
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	vh.WriteVarInt(0) // no properties
	vh.WriteBytes([]byte(filter))
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("sending UNSUBSCRIBE: %v", err)
	}
}

// testSuppressNextUnsubWrite makes the NEXT record write of this session — the one an
// UNSUBSCRIBE issues once sid is out of the membership map — never reach the stream,
// leaving it genuinely ambiguous with the record still at its CAS base.
//
// Armed at the save seam (which runs BEFORE the write) and disarmed at the result
// seam, so exactly one write is affected and the resolver's own re-attempt is not.
func testSuppressNextUnsubWrite(t *testing.T, sess *mqttSession, sid string) func() {
	t.Helper()
	var armed, disarmed atomicOnce
	restoreSave := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
		if sp != sess {
			return nil
		}
		sp.mu.Lock()
		_, still := sp.subs[sid]
		sp.mu.Unlock()
		if !still && armed.fire() {
			mqttTestSessStoreSuppressV.Store(true)
		}
		return nil
	})
	restoreResult := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sp == sess && armed.fired() && disarmed.fire() {
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	return func() {
		mqttTestSessStoreSuppressV.Store(false)
		restoreResult()
		restoreSave()
	}
}
