//go:build mqtt_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// What TestMQTTRedeliveryAckWait's two historic failures were made of, each forced:
//
//   - A QoS 1 source re-offer that JetStream dispatched BEFORE it processed the source
//     acknowledgement, and that reaches this broker's delivery callback only AFTER the client's
//     PUBACK ended the exchange and its terminal cleanup confirmed that acknowledgement, names a
//     source this session already completed: it is not admitted as a new exchange.
//   - A PUBLISH-action drain that exhausts its attempts leaves no tracked exchange once its
//     section closes, and is released by the reconcile with no further client packet.
//   - A retransmission that reaches its write entry after the PUBACK was consumed writes nothing;
//     one that entered before it may legally finish after it.
//
// NOT parallel: the delivery and writer seams are process-global.

// testOutboundMapsDump reads the two maps TestMQTTRedeliveryAckWait asserts on, and names every
// entry: which identifier, which source, and what kind of entry holds it.
func testOutboundMapsDump(sess *mqttSession) (lpi, lsseq int, dump string) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	var lines []string
	for pi, ack := range sess.pendingPublish {
		if ack == nil {
			lines = append(lines, fmt.Sprintf("pendingPublish[%d]=nil", pi))
			continue
		}
		lines = append(lines, fmt.Sprintf("pendingPublish[%d]: dur=%s sseq=%d kind=%v regPending=%v regNonce=%s regSeq=%d "+
			"transmitted=%v sendNonce=%q originConn=%v originSub=%v ackSubject=%s",
			pi, ack.jsDur, ack.sseq, ack.kind, ack.regPending, ack.regNonce, ack.regSeq,
			ack.transmitted, ack.sendNonce, ack.origin.cc != nil, ack.origin.sub != nil, ack.jsAckSubject))
	}
	for dur, seqs := range sess.cpending {
		for sseq, pi := range seqs {
			lines = append(lines, fmt.Sprintf("cpending[%s][%d]=%d", dur, sseq, pi))
		}
		lsseq += len(seqs)
	}
	sort.Strings(lines)
	return len(sess.pendingPublish), lsseq, strings.Join(lines, "\n  ")
}

// testTerminalCleanupDone reports that pi's acknowledged exchange is fully over: no entry, no
// identifier hold, no source suppression and no terminal continuation left.
func testTerminalCleanupDone(sess *mqttSession, pi uint16) bool {
	return sess.pendingPublish[pi] == nil && sess.sendsInFlight[pi] == nil &&
		len(sess.terminalSources) == 0 && len(sess.terminalRetries) == 0
}

// testRecordAdmissions records every fresh QoS 1/2 admission (trackPublish's isNew arm, seen
// at its locally confirmed admission) on the named session.
func testRecordAdmissions(t *testing.T, clientID string) func() []uint16 {
	var mu sync.Mutex
	var admitted []uint16
	h := func(sess *mqttSession, pi uint16) {
		if sess.id != clientID {
			return
		}
		mu.Lock()
		admitted = append(admitted, pi)
		mu.Unlock()
	}
	mqttTestDeliveryAdmittedPauseV.Store(&h)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	return func() []uint16 {
		mu.Lock()
		defer mu.Unlock()
		return append([]uint16(nil), admitted...)
	}
}

// testDescribeNextPacket reads whatever the broker wrote next, if anything, for the report.
func testDescribeNextPacket(t *testing.T, r *mqttReader, within time.Duration) string {
	t.Helper()
	b, pl, ok := testMQTTReadPacketReady(r, within)
	if !ok {
		return "nothing"
	}
	if b&mqttPacketMask != mqttPacketPub {
		r.pos += pl
		return fmt.Sprintf("packet type %x", b&mqttPacketMask)
	}
	flags, pi, topic, payload := testMQTTGetPubMsgExEx(t, nil, r, b, pl, _EMPTY_, nil)
	return fmt.Sprintf("PUBLISH topic=%q pi=%d qos=%d dup=%v payload=%q",
		topic, pi, mqttGetQoS(flags), flags&mqttPubFlagDup != 0, payload)
}

// TestMQTTRedeliveryAckWait's own schedule, on the RESUMED session: after the reconnect's
// redelivery of foo2, the next AckWait re-offer of foo2 is held at the callback across the final
// PUBACK and released only once the PUBACK's terminal cleanup has confirmed the source. The live
// connection's form of this decline is TestQueuedReofferOfAcknowledgedSourceIsDeclined; this one
// pins it through a resume and with the test's exact map assertion.
func TestAckWaitReofferCrossingTheFinalPubAckLeavesNoExchange(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const id = "ackwait-cross-sub"
	cisub := &mqttConnInfo{clientID: id, cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("foo1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 2, []byte("foo2"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("foo1"))
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("foo2"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, 1)
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("foo2"))

	c.Close()
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	if pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("foo2")); pi != 2 {
		t.Fatalf("Unexpected pi to be 2, got %v", pi)
	}
	mc := testMQTTGetClient(t, s, id)
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()

	// Hold the NEXT offer of this session at the callback: with AckWait at 250 ms it is an AckWait
	// re-offer of foo2, dispatched while the exchange is still open.
	parked, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var taken atomic.Pointer[subscription]
	pre := func(sub *subscription) {
		if sub.client == nil || sub.client.mqtt == nil || sub.client.mqtt.sess != sess {
			return
		}
		if !taken.CompareAndSwap(nil, sub) {
			return
		}
		close(parked)
		<-release
	}
	fin := func(sub *subscription) {
		if taken.Load() == sub && sub != nil {
			select {
			case <-done:
			default:
				close(done)
			}
		}
	}
	mqttTestDeliveryPreLockHookV.Store(&pre)
	mqttTestDeliveryDoneHookV.Store(&fin)
	t.Cleanup(func() {
		mqttTestDeliveryPreLockHookV.Store(nil)
		mqttTestDeliveryDoneHookV.Store(nil)
		select {
		case <-release:
		default:
			close(release)
		}
	})
	reofferAwait(t, parked, "an AckWait re-offer of foo2 reaching the delivery callback")

	testMQTTSendPIPacket(mqttPacketPubAck, t, c, 2)
	testMQTTFlush(t, c, nil, r)
	testMQTTWaitSendSectionClosed(t, sess, 2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		sess.mu.Lock()
		over := testTerminalCleanupDone(sess, 2)
		sess.mu.Unlock()
		if over {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture: the PUBACK's terminal cleanup never finished")
		}
		time.Sleep(5 * time.Millisecond)
	}

	admissions := testRecordAdmissions(t, id)
	close(release)
	reofferAwait(t, done, "the held re-offer callback returning")

	lpi, lsseq, dump := testOutboundMapsDump(sess)
	if lpi != 0 || lsseq != 0 {
		wire := testDescribeNextPacket(t, r, 500*time.Millisecond)
		t.Fatalf("Maps should be empty, got %v, %v; fresh admissions %v; wire: %s\n  %s", lpi, lsseq, admissions(), wire, dump)
	}
}

// An owed drain ALONE leaves no tracked exchange: the PUBACK parked on the retransmission's
// section is discharged by the section's end even though the drain exhausts its attempts, and
// once storage is back the owed drain lands and the identifier is released with no further
// client packet.
func TestUnresolvedPublishDrainReleasesItsIdentifierWithoutAnotherPacket(t *testing.T) {
	const id = "owed-drain-release"
	rig, _ := reofferStart(t, id, 1, subscriber311(t, id, 1))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, reofferTopic, mqttPubQos1, []byte("payload"))

	parked, release := rig.holdNextSection(pi)
	rig.nakSource()
	reofferAwait(t, parked, "the retransmission section reaching its write")

	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.c, pi)
	rig.waitFor("the PUBACK parking its settlement", func(sess *mqttSession) bool {
		ack := sess.pendingPublish[pi]
		return ack != nil && ack.sendSettle.kind == mqttSendSettleAckComplete
	})

	// Storage fails every register read from here until restored: the section's drain exhausts
	// its bounded attempts and enrols the reconcile.
	as := rig.sess.asm
	var failing atomic.Bool
	var failedLoads atomic.Int64
	failing.Store(true)
	as.mu.Lock()
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if failing.Load() {
			failedLoads.Add(1)
			return nil, nil, errors.New("injected: register read unavailable")
		}
		return as.jsa.loadInflightRecord(subject)
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testInflightLoad = nil
		as.mu.Unlock()
	})

	close(release)
	testMQTTWaitSendSectionClosed(t, rig.sess, pi)

	owed := func() int {
		rig.s.owedDrainsMu.Lock()
		defer rig.s.owedDrainsMu.Unlock()
		return len(rig.s.owedDrains)
	}
	if n := owed(); n != 1 {
		t.Fatalf("fixture: the drain did not exhaust its attempts (owed drains %d, failed loads %d)", n, failedLoads.Load())
	}
	lpi, lsseq, dump := testOutboundMapsDump(rig.sess)
	rig.sess.mu.Lock()
	h := rig.sess.sendsInFlight[pi]
	var terminalOwed string
	if h != nil {
		terminalOwed = h.terminalOwed
	}
	armedRetries := len(rig.sess.terminalRetries)
	rig.sess.mu.Unlock()
	t.Logf("while the drain is owed: terminal continuations armed %d", armedRetries)
	if lpi != 0 || lsseq != 0 {
		t.Fatalf("an owed drain left a tracked exchange after its section closed: maps %d, %d\n  %s", lpi, lsseq, dump)
	}
	if terminalOwed == _EMPTY_ {
		t.Fatalf("the acknowledged identifier %d is not held for its terminal cleanup while the drain is owed", pi)
	}
	t.Logf("owed drain: maps 0, 0; identifier %d held for terminal cleanup (nonce %s); failed loads %d",
		pi, terminalOwed, failedLoads.Load())

	failing.Store(false)
	restored := time.Now()
	// Both the reconcile and the terminal continuation are paced by mqttActionReconcileDelay, so
	// the release is bounded in periods of it rather than by the rig's generic wait.
	var landed time.Duration
	for deadline := restored.Add(10 * mqttActionReconcileDelay); ; {
		if landed == 0 && owed() == 0 {
			landed = time.Since(restored)
		}
		rig.sess.mu.Lock()
		over := testTerminalCleanupDone(rig.sess, pi)
		rig.sess.mu.Unlock()
		if over {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the identifier %d was not released within %v of storage returning (owed drains %d)",
				pi, 10*mqttActionReconcileDelay, owed())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := owed(); n != 0 {
		t.Fatalf("the identifier was released while the drain is still owed (owed drains %d)", n)
	}
	t.Logf("storage restored: the owed drain landed after %v, identifier %d released after %v",
		landed.Round(time.Millisecond), pi, time.Since(restored).Round(time.Millisecond))
	testMQTTExpectNothing(t, rig.r)
}

// A QoS 1 retransmission that reaches its write entry only AFTER the client's PUBACK was
// consumed writes nothing: the PUBACK marks the open section's output answered, and the holder's
// write-entry decision refuses an answered exchange. An execution that entered before the PUBACK
// may legally finish after it; one that enters after it may not exist.
func TestRetransmissionEnteringItsWriteAfterThePubAckWritesNothing(t *testing.T) {
	const id = "answered-write-entry"
	rig, _ := reofferStart(t, id, 1, subscriber311(t, id, 1))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, reofferTopic, mqttPubQos1, []byte("payload"))
	testRetransmissionHeldAcrossThePubAck(t, rig, pi)
	testMQTTExpectNothing(t, rig.r)
}

// The same schedule observed at the write-entry seam rather than on the wire: no PUBLISH
// execution is admitted to its write after its exchange read as answered. The seam reports,
// for every admitted execution, what the holder's answered flag was before the entry was
// decided, so an entry decision that ignored the acknowledgement is named at the execution
// that slipped through, not inferred from a frame.
func TestNoRetransmissionIsAdmittedAfterItsExchangeWasAnswered(t *testing.T) {
	const id = "answered-entry-seam"
	rig, _ := reofferStart(t, id, 1, subscriber311(t, id, 1))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, reofferTopic, mqttPubQos1, []byte("payload"))

	var mu sync.Mutex
	var enteredAfterAnswer []uint16
	entered := func(p uint16, answeredBeforeEntry bool) {
		if answeredBeforeEntry {
			mu.Lock()
			enteredAfterAnswer = append(enteredAfterAnswer, p)
			mu.Unlock()
		}
	}
	rig.sess.testPublishWriteEntered.Store(&entered)
	defer rig.sess.testPublishWriteEntered.Store(nil)

	testRetransmissionHeldAcrossThePubAck(t, rig, pi)
	mu.Lock()
	violations := append([]uint16(nil), enteredAfterAnswer...)
	mu.Unlock()
	if len(violations) != 0 {
		t.Fatalf("PUBLISH executions of identifiers %v were admitted to their write after their exchange was answered", violations)
	}
	testMQTTExpectNothing(t, rig.r)
}

// testRetransmissionHeldAcrossThePubAck forces a retransmission of pi to reach its write entry
// only after the client's PUBACK was consumed, and returns once that section has ended.
//
// The retransmission section is held at its last point before the writer (the intent window).
// It cannot sit closer to write entry: the writer holds the connection's lock from before its
// pre-lock seam, and every read-loop parse takes that lock before parsing, so a PUBACK read
// after the hold would not be processed until the write. The PUBACK's answered transition is
// then positively observed on the section's own output, and only then is the section released
// into its write.
func testRetransmissionHeldAcrossThePubAck(t *testing.T, rig *reofferRig, pi uint16) {
	t.Helper()
	parked, release := rig.holdNextSection(pi)
	rig.nakSource()
	reofferAwait(t, parked, "the retransmission section reaching its write")
	rig.sess.mu.Lock()
	var out *mqttPublishOutput
	if h := rig.sess.sendsInFlight[pi]; h != nil {
		out = h.pubOut
	}
	rig.sess.mu.Unlock()
	if out == nil {
		t.Fatalf("fixture: no open retransmission section for identifier %d", pi)
	}

	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.c, pi)
	deadline := time.Now().Add(10 * time.Second)
	for !out.answered.Load() {
		if time.Now().After(deadline) {
			t.Fatal("fixture: the PUBACK never marked the open section answered")
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(release)
	rig.waitFor("the retransmission section ending", func(sess *mqttSession) bool {
		h := sess.sendsInFlight[pi]
		return sess.pendingPublish[pi] == nil && (h == nil || h.token == _EMPTY_)
	})
}
