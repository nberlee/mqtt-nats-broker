//go:build mqttport_fulltests

package mqtt

import (
	"sync"
	"testing"
	"time"
)

// TestMQTTInflightStrandedReservationOverADeletedConsumerIsReleased is F-102 end to end,
// through a real broker with NO source-state hook anywhere: the classifier has to answer
// Lost by itself, from a consumer this test really deletes.
//
// It follows the finding's own route rather than a shortcut to the final state, because
// the ORDERING is the finding. Recovery runs before the reap (mqttProcessConnect calls
// recoverInflightRegisters, and only later resendPendingRedelivery), so a fixture that
// removes the source message before resuming has recovery see a COMPLETE source — the
// consumer still exists — and HEAD already consumes that pair. The strand needs the
// source to be LIVE at recovery and gone by the time the reap looks, which is what the
// before-connack pause is for.
//
//  1. A confirmed pair whose PUBLISH never went out: the send section is aborted at
//     mqttMarkerSendReserved, so the marker survives and the client sees nothing.
//  2. A SAME-OBJECT resume — no restart. That matters: recovery installs no ack subject
//     (mqttRecoveredPending), and the reap only scans entries that have one, so only the
//     original pending entry surviving through the in-memory-wins branch reaches it.
//  3. At before-connack, delete the source message. The reap then finds it absent,
//     proves the entry unsent and releases it — F-100's arm, working correctly — and
//     leaves the durable pair behind.
//  4. Delete the source consumer. Now the classifiers answer Lost rather than Complete.
//  5. Resume again. Before F-102 the pair was restored with its marker, its mapping and
//     no ack subject, and nothing could ever release it. Now it is consumed.
func TestMQTTInflightStrandedReservationOverADeletedConsumerIsReleased(t *testing.T) {
	// NOT parallel: the send-window and restore-pause hooks are process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "f102-e2e"
	sub, subr := testInflightSubscriber(t, o, clientID, 1)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "f102-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)

	// Two completed exchanges first, so the identifier under test is not the one a fresh
	// allocator would hand out and the release assertion cannot pass by accident.
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte(payload))
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
	}

	// (1) The pair confirms and the send aborts before any byte leaves. Every section for
	// this delivery is aborted, not just the first: an AckWait re-offer would otherwise
	// delete the marker and turn the exchange possibly-attempted.
	confirmed := make(chan uint16, 1)
	var once sync.Once
	restoreSend := setMQTTTestMarkerSendWindowHook(func(sess *mqttSession, pi uint16, window string) bool {
		// THIS SESSION ONLY. The hook is process-global, and a delivery of any other session in
		// the binary reaching its send window first would otherwise be the one stopped — its
		// identifier captured here, while this test's own PUBLISH went out and made the entry
		// possibly-attempted, which the reap then rightly refuses to release.
		if window != mqttMarkerSendReserved || sess.id != clientID {
			return true
		}
		once.Do(func() { confirmed <- pi })
		return false
	})
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("stranded"))
	var pi uint16
	select {
	case pi = <-confirmed:
	case <-time.After(5 * time.Second):
		restoreSend()
		t.Fatal("the send section never opened after the pair was confirmed")
	}
	if pi <= 1 {
		restoreSend()
		t.Fatalf("packet identifier %d is what a fresh allocator would produce", pi)
	}
	testMQTTExpectNothing(t, subr)

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		restoreSend()
		t.Fatal("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	ack := sess.pendingPublish[pi]
	var srcDur string
	var srcSeq uint64
	var hadAckSubject bool
	if ack != nil {
		srcDur, srcSeq, hadAckSubject = ack.jsDur, ack.sseq, ack.jsAckSubject != _EMPTY_
	}
	sess.mu.Unlock()
	if ack == nil || !hadAckSubject {
		restoreSend()
		t.Fatalf("fixture: the live entry for %d must carry an ack subject, or the reap "+
			"in step 3 will never scan it", pi)
	}
	regSubj := mqttInflightSubject(idHash, gen, pi)
	markerSubj := regSubj + "." + mqttInflightMarkerSuffix
	if smsg, err := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markerSubj); err != nil || smsg == nil {
		restoreSend()
		t.Fatalf("fixture: the unsent marker must survive an aborted send: msg=%v err=%v", smsg, err)
	}

	// Close before restoring, so no AckWait re-offer can slip a PUBLISH out in between.
	sub.Close()
	restoreSend()

	// (2)+(3) The same-object resume, with the source message removed after recovery has
	// classified it LIVE and before the reap looks.
	reaped := make(chan struct{}, 1)
	restorePause := setMQTTTestRestorePause(func(at string) {
		switch at {
		case "before-connack":
			// Recovery has already run and restored nothing new; the resident entry with
			// its ack subject is what the reap will scan.
			_ = asm.jsa.deleteMsg(mqttStreamName, srcSeq, true)
		case "resume-naks-done":
			select {
			case reaped <- struct{}{}:
			default:
			}
		}
	})
	sub, _ = testInflightReconnect(t, o, clientID)
	select {
	case <-reaped:
	case <-time.After(5 * time.Second):
		restorePause()
		sub.Close()
		t.Fatal("the resume redelivery pass never completed, so the reap proved nothing")
	}
	restorePause()

	// (4) The strand, asserted before it is converted into the Lost case. All three, or a
	// later green assertion could mean the fixture never reached this state.
	sess.mu.Lock()
	_, stillTracked := sess.pendingPublish[pi]
	_, stillMapped := sess.cpending[srcDur][srcSeq]
	sess.mu.Unlock()
	if stillTracked || stillMapped {
		sub.Close()
		t.Fatalf("fixture: F-100's reap must have released the entry (tracked=%v mapped=%v)",
			stillTracked, stillMapped)
	}
	if smsg, err := asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj); err != nil || smsg == nil {
		sub.Close()
		t.Fatalf("fixture: the durable register must survive that release: msg=%v err=%v", smsg, err)
	}
	if smsg, err := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markerSubj); err != nil || smsg == nil {
		sub.Close()
		t.Fatalf("fixture: the marker must survive too, or the pair is not proven-unsent: msg=%v err=%v", smsg, err)
	}
	if ci, err := asm.jsa.lookupConsumer(mqttStreamName, srcDur); err != nil || ci == nil {
		sub.Close()
		t.Fatalf("fixture: the source consumer must still exist here, or the source was "+
			"never Live at recovery: ci=%v err=%v", ci, err)
	}

	// (5) Now the consumer goes, and only now does the source classify Lost.
	if _, err := asm.jsa.deleteConsumer(mqttStreamName, srcDur, true); err != nil {
		sub.Close()
		t.Fatalf("delete the source consumer: %v", err)
	}
	sub.Close()

	sub, subr = testInflightReconnect(t, o, clientID)
	defer sub.Close()

	sess.mu.Lock()
	_, restored := sess.pendingPublish[pi]
	charge := sess.receiveMaxOccupancyLocked()
	sess.mu.Unlock()

	// HEAD's shape at exactly this point: restored, with its marker and mapping and an
	// EMPTY ack subject, so the reap can never scan it again and no consumer remains to
	// offer the source.
	if restored {
		t.Fatalf("packet identifier %d was restored over a Lost source: with no ack subject "+
			"and no consumer, nothing can ever release it", pi)
	}
	if charge != 0 {
		t.Fatalf("the released exchange still charges Receive Maximum, got %d", charge)
	}
	if smsg, _ := asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj); smsg != nil {
		t.Fatal("the durable register survived the release")
	}
	if smsg, _ := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markerSubj); smsg != nil {
		t.Fatal("the unsent marker survived the release")
	}

	// And the session still works: a fresh publication is delivered over the repaired
	// subscription. Checked AFTER the assertions above, because "nothing arrived" would
	// also be the symptom of a subscription repair that failed for unrelated reasons.
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("after"))
	got := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("after"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, got)
	testMQTTFlush(t, sub, nil, subr)
}
