//go:build mqtt_fulltests

package mqtt

import (
	"net"
	"sync"
	"testing"
	"time"
)

// F-115: a QoS 2 PUBLISH was re-sent after the client's PUBREC, in two shapes with one root,
// a source re-offer racing the acknowledgement of its exchange.
//
//   - Shape A: the re-offer opened a send section on the SAME identifier before the PUBREC was
//     processed, and the section's write executed after it: PUBREC, PUBLISH DUP, PUBREL.
//   - Shape B: the re-offer arrived after the PUBREC handoff removed the source's link, and was
//     admitted under a FRESH identifier: the payload delivered twice, after its PUBREL.
//
// Every test here forces its re-offer with a NAK on the source's own reply and holds the window
// open with a seam, so none of them races AckWait (which is set far beyond the test). Each
// asserts it reached its window before asserting the outcome. NOT parallel: the seams are
// process-global.

// f115Rig is one subscriber session with one delivery outstanding, and the seams that hold its
// windows open.
type f115Rig struct {
	t        *testing.T
	s        *Server
	sess     *mqttSession
	c        net.Conn
	r        *mqttReader
	clientID string

	mu    sync.Mutex
	reply map[uint64]string // newest offered reply per source sequence, this session only
}

const f115Topic = "f115"

func f115Options() *Options {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Minute // no spontaneous re-offer: every one in these tests is forced
	return o
}

// f115Start runs a broker, attaches the subscriber the caller connected with connect, installs the
// offer recorder, publishes one message at qos and returns the rig.
func f115Start(t *testing.T, clientID string, qos byte, connect func(o *Options) (net.Conn, *mqttReader)) (*f115Rig, *Options) {
	t.Helper()
	o := f115Options()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &f115Rig{t: t, s: s, clientID: clientID, reply: map[uint64]string{}}
	offered := func(sub *subscription, reply string) {
		if !rig.mine(sub) {
			return
		}
		sseq, _, _, _, _ := ackReplyInfo(reply)
		rig.mu.Lock()
		rig.reply[sseq] = reply
		rig.mu.Unlock()
	}
	mqttTestDeliveryOfferedHookV.Store(&offered)
	t.Cleanup(func() { mqttTestDeliveryOfferedHookV.Store(nil) })

	rig.c, rig.r = connect(o)
	t.Cleanup(func() { rig.c.Close() })
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	rig.sess = asm.sessions[clientID]
	asm.mu.RUnlock()
	if rig.sess == nil {
		t.Fatal("no resident session for the subscriber")
	}

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pub.Close() })
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, qos, false, false, f115Topic, 1, []byte("payload"))
	return rig, o
}

func (rig *f115Rig) mine(sub *subscription) bool {
	return sub != nil && sub.client != nil && sub.client.mqtt != nil &&
		sub.client.mqtt.sess != nil && sub.client.mqtt.sess.id == rig.clientID
}

// nakSource forces JetStream to re-offer the one outstanding source delivery at once.
func (rig *f115Rig) nakSource() {
	rig.t.Helper()
	rig.mu.Lock()
	var reply string
	for _, r := range rig.reply {
		reply = r
	}
	rig.mu.Unlock()
	if reply == _EMPTY_ {
		rig.t.Fatal("fixture: no source offer was recorded for the subscriber")
	}
	rig.sess.jsa.sendMsg(reply, AckNak)
}

// waitFor polls cond under sess.mu until it holds.
func (rig *f115Rig) waitFor(what string, cond func(sess *mqttSession) bool) {
	rig.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rig.sess.mu.Lock()
		ok := cond(rig.sess)
		rig.sess.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	rig.t.Fatalf("fixture: %s never happened", what)
}

func f115Handoff(pi uint16) func(sess *mqttSession) bool {
	return func(sess *mqttSession) bool {
		return sess.pendingPublish[pi] == nil && sess.pendingPubRel[pi] != nil
	}
}

// parkReoffer holds the NEXT delivery callback of this session before its lock, and reports
// when that callback has returned. release lets it go.
type f115Park struct {
	parked   chan struct{}
	done     chan struct{}
	release  chan struct{}
	once     sync.Once
	doneOnce sync.Once
}

func (rig *f115Rig) parkNextDelivery() *f115Park {
	p := &f115Park{parked: make(chan struct{}), done: make(chan struct{}), release: make(chan struct{})}
	var armed sync.Mutex
	taken := false
	var held *subscription
	pre := func(sub *subscription) {
		if !rig.mine(sub) {
			return
		}
		armed.Lock()
		if taken {
			armed.Unlock()
			return
		}
		taken, held = true, sub
		armed.Unlock()
		p.once.Do(func() { close(p.parked) })
		<-p.release
	}
	done := func(sub *subscription) {
		armed.Lock()
		ours := taken && sub == held
		armed.Unlock()
		if ours {
			p.doneOnce.Do(func() { close(p.done) })
		}
	}
	mqttTestDeliveryPreLockHookV.Store(&pre)
	mqttTestDeliveryDoneHookV.Store(&done)
	rig.t.Cleanup(func() {
		mqttTestDeliveryPreLockHookV.Store(nil)
		mqttTestDeliveryDoneHookV.Store(nil)
		select {
		case <-p.release:
		default:
			close(p.release)
		}
	})
	return p
}

func f115Await(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("fixture: %s never happened", what)
	}
}

// holdHandoff holds the next successful PUBREC of this session just after its handoff unlock.
func (rig *f115Rig) holdHandoff(pi uint16) (reached, release chan struct{}) {
	reached, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	h := func(hpi uint16) {
		if hpi != pi {
			return
		}
		once.Do(func() {
			close(reached)
			<-release
		})
	}
	mqttTestPubRecHandoffV.Store(&h)
	rig.t.Cleanup(func() {
		mqttTestPubRecHandoffV.Store(nil)
		select {
		case <-release:
		default:
			close(release)
		}
	})
	return reached, release
}

// holdNextSection parks the next send section of this session for pi at the intent window, the
// last point before its write.
func (rig *f115Rig) holdNextSection(pi uint16) (parked, release chan struct{}) {
	parked, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	restore := setMQTTTestMarkerSendWindowHook(func(sess *mqttSession, hpi uint16, window string) bool {
		if sess.id != rig.clientID || hpi != pi || window != mqttMarkerSendIntent {
			return true
		}
		once.Do(func() {
			close(parked)
			<-release
		})
		return true
	})
	rig.t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		restore()
	})
	return parked, release
}

func f115Subscriber311(t *testing.T, clientID string, qos byte) func(o *Options) (net.Conn, *mqttReader) {
	return func(o *Options) (net.Conn, *mqttReader) {
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: f115Topic, qos: qos}}, []byte{qos})
		testMQTTFlush(t, c, nil, r)
		return c, r
	}
}

// T1, shape B inside the handler: the re-offer is released while the PUBREC handler is held
// after its handoff, before its durable intent and long before the source acknowledgement.
func TestF115ReofferInsideThePubRecHandlerIsNotAdmittedAgain(t *testing.T) {
	const id = "f115-t1"
	rig, _ := f115Start(t, id, 2, f115Subscriber311(t, id, 2))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, f115Topic, mqttPubQoS2, []byte("payload"))

	park := rig.parkNextDelivery()
	rig.nakSource()
	f115Await(t, park.parked, "the forced re-offer reaching the delivery callback")

	reached, releaseHandler := rig.holdHandoff(pi)
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, pi)
	f115Await(t, reached, "the PUBREC handler reaching its handoff")
	rig.waitFor("the PUBREC handoff", f115Handoff(pi))

	close(park.release)
	f115Await(t, park.done, "the re-offer callback returning")
	close(releaseHandler)

	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, pi)
	testMQTTExpectNothing(t, rig.r)
}

// T2, shape B after the handler: the re-offer was dispatched before the source acknowledgement
// and reaches the callback only after the handler confirmed it and the PUBREL went out.
func TestF115StaleReofferAfterTheSourceAckIsNotAdmittedAgain(t *testing.T) {
	const id = "f115-t2"
	rig, _ := f115Start(t, id, 2, f115Subscriber311(t, id, 2))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, f115Topic, mqttPubQoS2, []byte("payload"))

	park := rig.parkNextDelivery()
	rig.nakSource()
	f115Await(t, park.parked, "the forced re-offer reaching the delivery callback")

	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, pi)
	// The read loop handles packets in order, so a PINGRESP proves the PUBREC handler, and with
	// it the confirmed source acknowledgement, has returned.
	testMQTTFlush(t, rig.c, nil, rig.r)

	close(park.release)
	f115Await(t, park.done, "the stale re-offer callback returning")

	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, pi)
	testMQTTExpectNothing(t, rig.r)
}

// T3, shape A: a retransmission section opened before the PUBREC and held at its last point
// before the write does not write once the PUBREC has been processed.
func TestF115SectionOpenAtPubRecDoesNotWrite(t *testing.T) {
	const id = "f115-t3"
	rig, _ := f115Start(t, id, 2, f115Subscriber311(t, id, 2))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, f115Topic, mqttPubQoS2, []byte("payload"))

	parked, release := rig.holdNextSection(pi)
	rig.nakSource()
	f115Await(t, parked, "the retransmission section reaching its write")

	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, pi)
	rig.waitFor("the PUBREC handoff", f115Handoff(pi))
	close(release)

	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, pi)
	testMQTTExpectNothing(t, rig.r)
}

// T3b, the answer's TIMING: the open section is released and runs to its end while the PUBREC
// handler is still held right after its handoff, so the answer must already be on the section's
// output in the handoff's own critical section.
func TestF115SectionIsAnsweredInTheHandoffsCriticalSection(t *testing.T) {
	const id = "f115-t3b"
	rig, _ := f115Start(t, id, 2, f115Subscriber311(t, id, 2))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, f115Topic, mqttPubQoS2, []byte("payload"))

	parked, release := rig.holdNextSection(pi)
	rig.nakSource()
	f115Await(t, parked, "the retransmission section reaching its write")

	reached, releaseHandler := rig.holdHandoff(pi)
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, pi)
	f115Await(t, reached, "the PUBREC handler reaching its handoff")

	close(release)
	rig.waitFor("the retransmission section ending", func(sess *mqttSession) bool {
		h := sess.sendsInFlight[pi]
		return h == nil || h.pubIdent == _EMPTY_
	})
	close(releaseHandler)

	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, pi)
	testMQTTExpectNothing(t, rig.r)
}

// T4, shape A on QoS 1: a PUBACK parks its settlement on the open section; the section must not
// then write the acknowledged PUBLISH.
func TestF115SectionOpenAtPubAckDoesNotWrite(t *testing.T) {
	const id = "f115-t4"
	rig, _ := f115Start(t, id, 1, f115Subscriber311(t, id, 1))
	pi := testMQTTCheckPubMsgNoAck(t, rig.c, rig.r, f115Topic, mqttPubQos1, []byte("payload"))

	parked, release := rig.holdNextSection(pi)
	rig.nakSource()
	f115Await(t, parked, "the retransmission section reaching its write")

	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.c, pi)
	rig.waitFor("the PUBACK parking its settlement", func(sess *mqttSession) bool {
		ack := sess.pendingPublish[pi]
		return ack != nil && ack.sendSettle.kind == mqttSendSettleAckComplete
	})
	close(release)
	rig.waitFor("the section ending", func(sess *mqttSession) bool { return sess.pendingPublish[pi] == nil })

	testMQTTExpectNothing(t, rig.r)
}

// T4b, shape A on a REFUSED PUBREC (MQTT 5, reason 0x80): the exchange ends without a PUBREL,
// and the open section must not write the refused PUBLISH either.
func TestF115SectionOpenAtFailedPubRecDoesNotWrite(t *testing.T) {
	const id = "f115-t4b"
	connect := func(o *Options) (net.Conn, *mqttReader) {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: f115Topic, opts: 2}})
		testMQTTFlush(t, c, nil, r)
		return c, r
	}
	rig, _ := f115Start(t, id, 2, connect)
	qos, pi := testMQTTReadPublishV5(t, rig.r, f115Topic, []byte("payload"))
	if qos != 2 {
		t.Fatalf("fixture: expected a QoS 2 delivery, got %d", qos)
	}

	parked, release := rig.holdNextSection(pi)
	rig.nakSource()
	f115Await(t, parked, "the retransmission section reaching its write")

	if _, err := testMQTTWrite(rig.c, []byte{mqttPacketPubRec, 3, byte(pi >> 8), byte(pi), 0x80}); err != nil {
		t.Fatalf("writing the refused PUBREC: %v", err)
	}
	rig.waitFor("the refused PUBREC parking its settlement", func(sess *mqttSession) bool {
		ack := sess.pendingPublish[pi]
		return ack != nil && ack.sendSettle.kind == mqttSendSettlePubRecFailed
	})
	close(release)
	rig.waitFor("the section ending", func(sess *mqttSession) bool { return sess.pendingPublish[pi] == nil })

	testMQTTExpectNothing(t, rig.r)
}
