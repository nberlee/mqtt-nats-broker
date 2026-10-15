//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// A PUBACK that lands while its PUBLISH's send section is still draining PARKS, and the
// section's end owes the acknowledgement's terminal release. That release must not run on
// the section's goroutine, which is the subscription's delivery callback: JetStream's
// re-offers of the same source queue behind it there, the release settles the source and
// lifts its suppression first, and the queued re-offer is then admitted as a NEW delivery
// of a message the client already acknowledged — a fresh packet identifier, DUP=0. Handed
// off (handOffAcknowledgedPublish), the re-offer is processed while the release is still
// running and meets the suppression the PUBACK installed.
//
// Made deterministic rather than timed: the section is held open until the PUBACK has
// parked on it, and the release's confirmed source acknowledgement is held until a re-offer
// of that source has REACHED the suppression. Inline, no re-offer can reach it while the
// release occupies the delivery goroutine, so the rendezvous never happens.
//
// Serial: it installs a process-global hook (setMQTTTestPublishDrainedPause).
//
// Mutations: endSendCore's acknowledgement arm calling completeAcknowledgedPublish (inline)
// fails the rendezvous; calling attemptTerminalRegisterRelease directly fails it too.
func TestMQTTInflightParkedAckReleaseLeavesReOffersSuppressed(t *testing.T) {
	const clientID = "inflight-parked-release"
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, clientID, 1)
	defer sub.Close()
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	sess := testInflightSession(t, s, clientID)

	// Hold the section open, after its drain, until the PUBACK has parked on its entry.
	var parkedOnce atomic.Bool
	var parked atomic.Bool
	restorePause := setMQTTTestPublishDrainedPause(func(pi uint16) {
		if !parkedOnce.CompareAndSwap(false, true) {
			return
		}
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			sess.mu.Lock()
			ack := sess.pendingPublish[pi]
			condemned := ack != nil && ack.sendSettle.kind == mqttSendSettleAckComplete
			sess.mu.Unlock()
			if condemned {
				parked.Store(true)
				return
			}
		}
	})
	defer restorePause()

	// Hold the release's confirmed source acknowledgement until a re-offer of this source has
	// reached the suppression (the owner records the newest re-offer's reply), then land it.
	var reached, hookCalls atomic.Int32
	var landErr atomic.Pointer[error]
	asm.mu.Lock()
	asm.testInflightSourceAck = func(ackSubject string) error {
		hookCalls.Add(1)
		dur, seq := testInflightAckSubjectSource(ackSubject)
		for deadline := time.Now().Add(6 * o.MQTT.AckWait); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if testInflightReOfferSuppressed(sess, dur, seq) {
				reached.Add(1)
				break
			}
		}
		r, err := sess.submitSourceAck(ackSubject, true, nil)
		if err == nil && !r.positive() {
			err = fmt.Errorf("the source acknowledgement was not submitted (result %d)", r)
		}
		if err != nil {
			landErr.Store(&err)
		}
		return err
	}
	asm.mu.Unlock()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-parked-release", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("once"))
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("once"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	acked := time.Now()

	// Nothing may follow the PUBACK: every later PUBLISH of this source is a duplicate of an
	// acknowledged message.
	for deadline := acked.Add(8 * o.MQTT.AckWait); time.Now().Before(deadline); {
		b, pl, ok := testMQTTReadPacketReady(subr, time.Until(deadline))
		if !ok {
			break
		}
		if b&mqttPacketMask == mqttPacketPub {
			flags, dupPI, _, payload := testMQTTGetPubMsgExEx(t, nil, subr, b, pl, "foo", nil)
			t.Fatalf("an acknowledged message was delivered again %v after its PUBACK: flags=%x pi=%d payload=%q",
				time.Since(acked), flags, dupPI, payload)
		}
		subr.pos += pl
	}

	if !parked.Load() {
		t.Fatalf("fixture: the PUBACK never parked on the open section, so the parked release was not exercised")
	}
	if hookCalls.Load() == 0 {
		t.Fatalf("fixture: the release never reached the source acknowledgement")
	}
	if p := landErr.Load(); p != nil {
		t.Fatalf("fixture: the held source acknowledgement did not land: %v", *p)
	}
	if reached.Load() == 0 {
		t.Fatalf("no re-offer reached the source's suppression while its release was held: " +
			"the release is occupying the delivery goroutine the re-offers queue on")
	}

	// And the release still completes: register gone, identifier free, no suppression owner.
	idHash, gen := testInflightIdentity(sess)
	testInflightWaitNoRegisters(t, asm, idHash, gen)
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.sendReservedLocked(pi) {
			return errors.New("the identifier is still reserved after its terminal cleanup")
		}
		if n := len(sess.terminalSources); n != 0 {
			return fmt.Errorf("%d source suppression owner(s) outlived the completed release", n)
		}
		return nil
	})
}

// The QoS 2 control: a source acknowledgement confirmed only after AckWait (the PUBREC's)
// leaves no PUBLISH of the completed message after its PUBCOMP. The retirement suppression
// carries this exchange, so this holds with or without the handoff; it is here so the QoS 1
// test above is read against a protocol that already behaves.
func TestMQTTInflightQoS2SlowSourceAckLeavesNoDuplicate(t *testing.T) {
	t.Parallel()
	const clientID = "inflight-qos2-slow-ack"
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, clientID, 2)
	defer sub.Close()
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	sess := testInflightSession(t, s, clientID)
	var hookCalls atomic.Int32
	asm.mu.Lock()
	asm.testInflightSourceAck = func(ackSubject string) error {
		hookCalls.Add(1)
		time.Sleep(3 * o.MQTT.AckWait)
		r, err := sess.submitSourceAck(ackSubject, true, nil)
		if err == nil && !r.positive() {
			err = fmt.Errorf("the source acknowledgement was not submitted (result %d)", r)
		}
		return err
	}
	asm.mu.Unlock()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-qos2-slow-ack", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("once"))
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("once"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	done := time.Now()

	// A PUBREL retransmitted before the PUBCOMP was processed is legitimate; a PUBLISH is not.
	for deadline := done.Add(8 * o.MQTT.AckWait); time.Now().Before(deadline); {
		b, pl, ok := testMQTTReadPacketReady(subr, time.Until(deadline))
		if !ok {
			break
		}
		if b&mqttPacketMask == mqttPacketPub {
			flags, dupPI, _, payload := testMQTTGetPubMsgExEx(t, nil, subr, b, pl, "foo", nil)
			t.Fatalf("a completed QoS 2 message was delivered again %v after its PUBCOMP: flags=%x pi=%d payload=%q",
				time.Since(done), flags, dupPI, payload)
		}
		subr.pos += pl
	}
	if hookCalls.Load() == 0 {
		t.Fatalf("fixture: the exchange never reached the source acknowledgement")
	}
}

// testInflightAckSubjectSource names the source a JetStream ack subject acknowledges.
func testInflightAckSubjectSource(ackSubject string) (string, uint64) {
	dur, seq, _, _ := mqttSourceAckScope(ackSubject)
	return dur, seq
}

// testInflightReOfferSuppressed reports whether a RE-OFFER (delivery count above one) of
// the source has reached its terminal suppression: the owner records the newest offer's
// reply only when an offer meets it.
func testInflightReOfferSuppressed(sess *mqttSession, dur string, seq uint64) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for key, w := range sess.terminalSources {
		if key.dur != dur || key.sseq != seq || w.reply == _EMPTY_ {
			continue
		}
		if _, _, dc, _, _ := ackReplyInfo(w.reply); dc > 1 {
			return true
		}
	}
	return false
}

func testInflightIdentity(sess *mqttSession) (string, string) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.idHash, sess.gen
}
