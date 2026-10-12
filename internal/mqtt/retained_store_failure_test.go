//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
)

// F-060: a publication whose retained update the store did not confirm is never given
// final success. QoS 0 closes the connection, QoS 1 withholds PUBACK, QoS 2 withholds
// PUBCOMP, and a delayed Will stays `publishing` for its lease to re-fire. The
// $MQTT_msgs store is still attempted for QoS 1/2, and its success never erases the
// retained error.
//
// None of these tests is parallel: the store hooks and the F-060 seams are process
// globals. Each hook filters on THIS test's subject before doing anything, so tests
// running beside them (other files' parallel tests) are untouched.

// f060RetainedSubject is the $MQTT_rmsgs subject a topic's retained record lands on.
func f060RetainedSubject(topic string) string {
	return mqttRetainedMsgsStreamSubject + strings.ReplaceAll(topic, "/", ".")
}

// f060OnwardSubject is the $MQTT_msgs subject a topic's QoS 1/2 copy lands on.
func f060OnwardSubject(topic string) string {
	return mqttStreamSubjectPrefix + strings.ReplaceAll(topic, "/", ".")
}

var errF060RetainedInjected = errors.New("f060: retained store refused by test")

// f060FailRetained makes every retained store for topic fail before it is issued.
func f060FailRetained(t *testing.T, topic string) func() {
	t.Helper()
	want := f060RetainedSubject(topic)
	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if subject == want {
			return errF060RetainedInjected
		}
		return nil
	})
	t.Cleanup(restore)
	return restore
}

// f060ObserveResults records what mqttInitiateMsgDeliveryJSA returned for topic.
func f060ObserveResults(t *testing.T, topic string) func() []error {
	t.Helper()
	var mu sync.Mutex
	var got []error
	h := func(pp *mqttPublish, err error) {
		if string(pp.topic) != topic { // copied synchronously: pp is reused
			return
		}
		mu.Lock()
		got = append(got, err)
		mu.Unlock()
	}
	mqttTestDeliveryResultV.Store(&h)
	t.Cleanup(func() { mqttTestDeliveryResultV.Store(nil) })
	return func() []error {
		mu.Lock()
		defer mu.Unlock()
		return append([]error(nil), got...)
	}
}

// f060WaitResult waits for the first observed result.
func f060WaitResult(t *testing.T, results func() []error) error {
	t.Helper()
	var first error
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		r := results()
		if len(r) == 0 {
			return fmt.Errorf("no delivery result observed yet")
		}
		first = r[0]
		return nil
	})
	return first
}

// f060SendPubV5 writes a v5 PUBLISH with explicit DUP and RETAIN flags.
func f060SendPubV5(t *testing.T, c net.Conn, qos byte, pi uint16, dup, retain bool, topic string, payload []byte) {
	t.Helper()
	flags := qos << 1
	if dup {
		flags |= mqttPubFlagDup
	}
	if retain {
		flags |= mqttPubFlagRetain
	}
	vh := newMQTTWriter(0)
	vh.WriteBytes([]byte(topic))
	if qos > 0 {
		vh.WriteUint16(pi)
	}
	vh.WriteVarInt(0)
	vh.Write(payload)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub | flags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing PUBLISH: %v", err)
	}
}

// f060ExpectRefusedV5 asserts the publisher was NOT given final success: no PUBACK or
// PUBCOMP arrives, and the broker announces the close with a DISCONNECT, whose reason
// code is returned. The connection is then gone.
func f060ExpectRefusedV5(t *testing.T, c net.Conn, r *mqttReader) byte {
	t.Helper()
	for {
		b, pl, ok := testMQTTReadPacketReady(r, testMQTTTimeout)
		if !ok {
			t.Fatal("the connection ended without a DISCONNECT announcing why")
		}
		switch pt := b & mqttPacketMask; pt {
		case mqttPacketPubAck, mqttPacketPubComp:
			t.Fatalf("final success (packet %x) was reported for a retained update the store did not confirm", pt)
		case mqttPacketDisconnect:
			start := r.pos
			reason, err := r.readByte("disconnect reason")
			if err != nil {
				t.Fatalf("reading the DISCONNECT reason: %v", err)
			}
			r.pos = start + pl
			testMQTTExpectDisconnect(t, c)
			return reason
		default:
			r.pos += pl
		}
	}
}

// f060Watch connects a 3.1.1 witness subscribed to topic at qos.
func f060Watch(t *testing.T, o *Options, id, topic string, qos byte) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: qos}}, []byte{qos})
	testMQTTFlush(t, c, nil, r)
	return c, r
}

// f060ExpectRetained asserts a fresh subscriber is served payload as topic's retained
// message. A literal SUBSCRIBE loads the stream, so a record that landed without the
// local seed (an ambiguous store) is still found.
func f060ExpectRetained(t *testing.T, o *Options, id, topic string, payload []byte) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTCheckPubMsg(t, c, r, topic, mqttPubFlagRetain, payload)
}

// f060ExpectNoRetained asserts a fresh subscriber is served nothing for topic.
func f060ExpectNoRetained(t *testing.T, o *Options, id, topic string) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTExpectNothing(t, r)
}

// f060ConnectPersistentV5 connects a v5 publisher whose session survives a close, so a
// refused exchange is still there to re-send.
func f060ConnectPersistentV5(t *testing.T, o *Options, id string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(300)},
		o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	testMQTTReadConnAckV5(t, r)
	return c, r
}

// T1. QoS 0 has no acknowledgement to withhold, so the connection is the signal.
func TestRetainedStoreFailureQoS0ClosesV5(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// CONTROL, on its own topic: a QoS 0 RETAIN with nothing failing keeps the
	// connection open and is retained.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f060q0-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTSendPubV5Raw(t, cp, 0, 0, true, "f060q0/ok", nil, []byte("kept"))
	testMQTTExpectNothing(t, rp)
	f060ExpectRetained(t, o, "f060q0-ok-check", "f060q0/ok", []byte("kept"))

	const topic = "f060q0/fail"
	c0, r0 := f060Watch(t, o, "f060q0-live", topic, 0)
	results := f060ObserveResults(t, topic)
	restore := f060FailRetained(t, topic)

	testMQTTSendPubV5Raw(t, cp, 0, 0, true, topic, nil, []byte("lost"))
	if reason := f060ExpectRefusedV5(t, cp, rp); reason != mqttReasonUnspecifiedError {
		t.Fatalf("DISCONNECT reason = 0x%x, want 0x%x", reason, mqttReasonUnspecifiedError)
	}
	if err := f060WaitResult(t, results); !errors.Is(err, errMQTTRetainedStoreUnconfirmed) || !errors.Is(err, errF060RetainedInjected) {
		t.Fatalf("delivery result = %v, want the unconfirmed retained update", err)
	}
	// The live fan-out preceded the store and cannot be retracted: the residual,
	// asserted so it stays honest.
	testMQTTCheckPubMsgNoAck(t, c0, r0, topic, 0, []byte("lost"))

	restore()
	f060ExpectNoRetained(t, o, "f060q0-fail-check", topic)
}

// T1b. A 3.1.1 client has no DISCONNECT from the server; the close is the signal.
func TestRetainedStoreFailureQoS0Closes311(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "f060q0v4/fail"
	f060FailRetained(t, topic)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "f060q0v4-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, c, r, 0, false, true, topic, 0, []byte("lost"))
	testMQTTExpectDisconnect(t, c)
}

// T2 (+ T2b). QoS 1: no PUBACK, the stream-backed copy is still stored, and a re-send
// on the surviving session completes it. The tombstone arm is the same contract.
func TestRetainedStoreFailureQoS1WithholdsPubAck(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	t.Run("replacement", func(t *testing.T) {
		const topic = "f060q1/fail"
		c1, r1 := f060Watch(t, o, "f060q1-durable", topic, 1)
		restore := f060FailRetained(t, topic)

		cp, rp := f060ConnectPersistentV5(t, o, "f060q1-pub")
		f060SendPubV5(t, cp, 1, 7, false, true, topic, []byte("v1"))
		if reason := f060ExpectRefusedV5(t, cp, rp); reason != mqttReasonUnspecifiedError {
			t.Fatalf("DISCONNECT reason = 0x%x, want 0x%x", reason, mqttReasonUnspecifiedError)
		}
		// The $MQTT_msgs store was still attempted: the QoS 1 subscriber's copy comes
		// only from that store.
		testMQTTCheckPubMsg(t, c1, r1, topic, mqttPubQos1, []byte("v1"))

		restore()
		f060ExpectNoRetained(t, o, "f060q1-check-1", topic)

		// The re-send on the surviving session is the re-attempt.
		cp2, rp2 := f060ConnectPersistentV5(t, o, "f060q1-pub")
		f060SendPubV5(t, cp2, 1, 7, true, true, topic, []byte("v1"))
		if reason, _ := testMQTTReadPubAckV5(t, rp2, 7); reason != mqttReasonSuccess {
			t.Fatalf("re-send PUBACK reason = 0x%x, want success", reason)
		}
		f060ExpectRetained(t, o, "f060q1-check-2", topic, []byte("v1"))
	})

	t.Run("tombstone", func(t *testing.T) {
		const topic = "f060q1/tomb"
		cs, rs := f060ConnectPersistentV5(t, o, "f060q1t-seed")
		f060SendPubV5(t, cs, 1, 1, false, true, topic, []byte("old"))
		testMQTTReadPubAckV5(t, rs, 1)
		f060ExpectRetained(t, o, "f060q1t-check-1", topic, []byte("old"))

		f060FailRetained(t, topic)
		cp, rp := f060ConnectPersistentV5(t, o, "f060q1t-pub")
		f060SendPubV5(t, cp, 1, 2, false, true, topic, nil)
		f060ExpectRefusedV5(t, cp, rp)
		// The deletion did not happen, and the publisher was not told it did.
		f060ExpectRetained(t, o, "f060q1t-check-2", topic, []byte("old"))
	})
}

// T2c. Both stores fail: the returned error carries both, so neither erases the other.
func TestRetainedStoreFailureJoinsOnwardFailure(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "f060both/fail"
	errOnward := errors.New("f060: onward store refused by test")
	var reachedRetained, reachedOnward atomic.Bool
	retainedSubj, onwardSubj := f060RetainedSubject(topic), f060OnwardSubject(topic)
	t.Cleanup(setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		switch subject {
		case retainedSubj:
			reachedRetained.Store(true)
			return errF060RetainedInjected
		case onwardSubj:
			reachedOnward.Store(true)
			return errOnward
		}
		return nil
	}))
	results := f060ObserveResults(t, topic)

	f060Watch(t, o, "f060both-durable", topic, 1) // so the onward store is attempted with interest
	cp, rp := f060ConnectPersistentV5(t, o, "f060both-pub")
	f060SendPubV5(t, cp, 1, 3, false, true, topic, []byte("x"))
	f060ExpectRefusedV5(t, cp, rp)

	err := f060WaitResult(t, results)
	if !reachedRetained.Load() || !reachedOnward.Load() {
		t.Fatalf("both stores must be attempted: retained=%v onward=%v", reachedRetained.Load(), reachedOnward.Load())
	}
	for _, want := range []error{errMQTTRetainedStoreUnconfirmed, errF060RetainedInjected, errOnward} {
		if !errors.Is(err, want) {
			t.Fatalf("delivery result %v does not carry %v", err, want)
		}
	}
}

// T2e. A retained store that LANDED but whose reply was lost is still unconfirmed to the
// broker, so it is not acknowledged. The re-send re-stores it at a newer sequence, and
// overwrites anything another publisher stored in between: the stated residual.
func TestRetainedStoreAmbiguousIsNotAcknowledged(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	errLost := errors.New("f060: retained reply lost by test")
	for _, tc := range []struct {
		name     string
		keepResp bool
	}{
		{"reply-lost", false},
		{"reply-with-sequence", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := "f060amb/" + tc.name
			want := f060RetainedSubject(topic)
			var seenSeq atomic.Uint64
			restore := setMQTTTestStoreMsgResultHook(func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
				if subject != want {
					return resp, err
				}
				if err != nil || resp == nil || resp.Sequence == 0 {
					t.Errorf("fixture: the retained store must land before its reply is lost: resp=%v err=%v", resp, err)
					return resp, err
				}
				seenSeq.Store(resp.Sequence)
				if tc.keepResp {
					return resp, errLost
				}
				return nil, errLost
			})
			t.Cleanup(restore)

			id := "f060amb-" + tc.name
			cp, rp := f060ConnectPersistentV5(t, o, id)
			f060SendPubV5(t, cp, 1, 9, false, true, topic, []byte("X"))
			f060ExpectRefusedV5(t, cp, rp)
			if seenSeq.Load() == 0 {
				t.Fatal("the result hook never saw the landed retained store")
			}
			restore()
			// It landed, so it is served.
			f060ExpectRetained(t, o, id+"-check-1", topic, []byte("X"))

			// Another publisher stores Y; the first re-sends X; X wins (last arrival).
			co, ro := f060ConnectPersistentV5(t, o, id+"-other")
			f060SendPubV5(t, co, 1, 1, false, true, topic, []byte("Y"))
			testMQTTReadPubAckV5(t, ro, 1)
			f060ExpectRetained(t, o, id+"-check-2", topic, []byte("Y"))

			cp2, rp2 := f060ConnectPersistentV5(t, o, id)
			f060SendPubV5(t, cp2, 1, 9, true, true, topic, []byte("X"))
			if reason, _ := testMQTTReadPubAckV5(t, rp2, 9); reason != mqttReasonSuccess {
				t.Fatalf("re-send PUBACK reason = 0x%x, want success", reason)
			}
			f060ExpectRetained(t, o, id+"-check-3", topic, []byte("X"))
		})
	}

	t.Run("tombstone", func(t *testing.T) {
		const topic = "f060amb/tomb"
		cs, rs := f060ConnectPersistentV5(t, o, "f060ambt-seed")
		f060SendPubV5(t, cs, 1, 1, false, true, topic, []byte("old"))
		testMQTTReadPubAckV5(t, rs, 1)

		want := f060RetainedSubject(topic)
		t.Cleanup(setMQTTTestStoreMsgResultHook(func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if subject == want && err == nil {
				return nil, errLost
			}
			return resp, err
		}))
		cp, rp := f060ConnectPersistentV5(t, o, "f060ambt-pub")
		f060SendPubV5(t, cp, 1, 2, false, true, topic, nil)
		f060ExpectRefusedV5(t, cp, rp)
		// The deletion landed, though the publisher was not told it did.
		f060ExpectNoRetained(t, o, "f060ambt-check", topic)
	})
}

// T3. The retained update is attempted even when the live send fails, and a QoS 1/2
// publication's PUBACK then covers it. QoS 0 with a failed send still fails outright.
func TestRetainedStoreAttemptedWhenLiveFanOutFails(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic, topic0 = "f060fan/q1", "f060fan/q0"
	failing := map[string]bool{"f060fan.q1": true, "f060fan.q0": true}
	h := func(subject string) bool { return failing[subject] }
	mqttTestFanOutFailV.Store(&h)
	t.Cleanup(func() { mqttTestFanOutFailV.Store(nil) })

	cp, rp := f060ConnectPersistentV5(t, o, "f060fan-pub")
	f060SendPubV5(t, cp, 1, 4, false, true, topic, []byte("kept"))
	if reason, _ := testMQTTReadPubAckV5(t, rp, 4); reason != mqttReasonSuccess {
		t.Fatalf("PUBACK reason = 0x%x, want success", reason)
	}
	f060ExpectRetained(t, o, "f060fan-check", topic, []byte("kept"))

	// Control: QoS 0 with the live send failing reports the failed fan-out.
	results := f060ObserveResults(t, topic0)
	f060SendPubV5(t, cp, 0, 0, false, true, topic0, []byte("gone"))
	f060ExpectRefusedV5(t, cp, rp)
	if err := f060WaitResult(t, results); !errors.Is(err, errMQTTWillFanOutFailed) {
		t.Fatalf("QoS 0 result = %v, want %v", err, errMQTTWillFanOutFailed)
	}
}

// T4. QoS 2: the release withholds PUBCOMP and keeps the hold; the re-sent PUBREL
// completes it, and Nats-Msg-Id keeps the stream-backed copy single.
func TestRetainedStoreFailureQoS2WithholdsPubComp(t *testing.T) {
	b := testQoS2Setup(t, "f060q2")
	pi := uint16(41)
	subject := b.holdSubject(t, pi)

	f060SendPubV5(t, b.cp, 2, pi, false, true, b.topic, []byte("held"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	restore := f060FailRetained(t, b.topic)
	testQoS2SendPubRel(t, b.cp, pi)
	b.expectNoPubComp(t)
	testQoS2ExpectClientGone(t, b.s, b.pubID)

	// The onward store was still attempted.
	b.consumeOne(t, "held")
	restore()
	if rec, _, err := b.jsa.loadQoS2Record(subject); err != nil || rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("the hold must survive a release whose retained update failed: rec=%v err=%v", rec, err)
	}
	f060ExpectNoRetained(t, b.o, "f060q2-check-1", b.topic)

	cp2, rp2 := b.reconnectPublisher(t)
	testQoS2SendPubRel(t, cp2, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != mqttReasonSuccess {
		t.Fatalf("re-sent PUBREL: PUBCOMP reason 0x%x, want success", reason)
	}
	f060ExpectRetained(t, b.o, "f060q2-check-2", b.topic, []byte("held"))
	testMQTTExpectNothing(t, b.rs)
}

// T5. A delayed retained Will whose retained update fails is not finalized: the record
// stays `publishing` and lease expiry re-fires it, at every Will QoS.
func TestRetainedStoreFailureDelayedWillStaysPublishing(t *testing.T) {
	for _, qos := range []byte{0, 1} {
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			asm := testMQTTGetGlobalASM(t, s)
			testShortenWillLease(t, asm, time.Second)

			topic := fmt.Sprintf("f060will/q%d", qos)
			results := f060ObserveResults(t, topic)
			restore := f060FailRetained(t, topic)

			will := &mqttWill{topic: []byte(topic), message: []byte("bye"), qos: qos, retain: true}
			id := fmt.Sprintf("f060will-%d", qos)
			cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, will: will,
				willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, rw)
			cw.Close()

			err := f060WaitResult(t, results)
			if !errors.Is(err, errMQTTRetainedStoreUnconfirmed) {
				t.Fatalf("Will delivery result = %v, want the unconfirmed retained update", err)
			}
			if errors.Is(err, errMQTTWillDestinationUnavailable) {
				t.Fatalf("a post-fan-out retained failure must not be classified as a pre-effect refusal: %v", err)
			}
			if !testMQTTHasWillState(t, asm, mqttWillPublishing) {
				t.Fatal("the record must stay publishing after a failed retained update")
			}
			if testMQTTHasWillState(t, asm, mqttWillDone) {
				t.Fatal("the record was finalized although its retained update failed")
			}

			// Lease expiry re-fires it, and with the store back it completes.
			restore()
			checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
				if n := testMQTTCountWillRecords(t, asm); n != 0 {
					return fmt.Errorf("%d outbox records remain", n)
				}
				return nil
			})
			f060ExpectRetained(t, o, id+"-check", topic, []byte("bye"))
		})
	}
}

// f060AuthzRevokeOnce returns a pause that closes the publisher's per-client NATS
// connection once, then waits for the snapshot to be invalidated, as the late-fence
// fixtures in authz_test.go do.
func f060AuthzRevokeOnce(t *testing.T, dialed func(string) *nats.Conn, id string) func() {
	t.Helper()
	nc := dialed(id)
	if nc == nil {
		t.Fatal("fixture: the per-client NATS connection was not captured")
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			nc.Close()
			time.Sleep(300 * time.Millisecond)
		})
	}
}

// T6. The retained store's own fence: no write, and the abandonment is REPORTED.
func TestRetainedFenceAbandonIsReported(t *testing.T) {
	broker, dialed, _, _ := authzBrokerTrackingDials(t, authzConfOneAccount)

	pub, err := authzClient(t, broker, "f060fence-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	const topic = "f060fence/x"
	results := f060ObserveResults(t, topic)
	pause := f060AuthzRevokeOnce(t, dialed, "f060fence-pub")
	mqttTestRetainedFencePauseV.Store(&pause)
	t.Cleanup(func() { mqttTestRetainedFencePauseV.Store(nil) })

	pub.Publish(topic, 0, true, []byte("no")).WaitTimeout(5 * time.Second)
	// Wait for the outcome, but judge the two halves separately and the write first,
	// so a fence that is gone (the store proceeds) and a fence whose abandonment is not
	// reported each fail on their own assertion.
	result := f060WaitResult(t, results)
	mqttTestRetainedFencePauseV.Store(nil)

	// The fence: no retained copy was written.
	sub, err := authzClient(t, broker, "f060fence-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	got := make(chan pmqtt.Message, 1)
	if tok := sub.Subscribe(topic, 0, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	select {
	case m := <-got:
		t.Fatalf("a retained update whose identity was revoked before its store must not be written, got %q", m.Payload())
	case <-time.After(2 * time.Second):
	}
	// The report: the abandonment reached the caller.
	if !errors.Is(result, errMQTTServiceEpochMoved) {
		t.Fatalf("delivery result = %v, want %v", result, errMQTTServiceEpochMoved)
	}
}

// T6b. The derived Sparkplug fence reports its abandonment too.
func TestDerivedSparkplugFenceAbandonIsReported(t *testing.T) {
	broker, dialed, _, _ := authzBrokerTrackingDials(t, authzConfOneAccount)

	pub, err := authzClient(t, broker, "f060spb-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	const topic = "spBv1.0/other/NBIRTH/f060"
	results := f060ObserveResults(t, topic)
	pause := f060AuthzRevokeOnce(t, dialed, "f060spb-pub")
	mqttTestDerivedSendPauseV.Store(&pause)
	t.Cleanup(func() { mqttTestDerivedSendPauseV.Store(nil) })

	pub.Publish(topic, 0, false, []byte("no")).WaitTimeout(5 * time.Second)
	if err := f060WaitResult(t, results); !errors.Is(err, errMQTTServiceEpochMoved) {
		t.Fatalf("delivery result = %v, want %v", err, errMQTTServiceEpochMoved)
	}
}

// T2d. A retained failure followed by the onward store's fence: both are reported.
func TestRetainedFailureSurvivesOnwardFence(t *testing.T) {
	broker, dialed, _, _ := authzBrokerTrackingDials(t, authzConfOneAccount)

	pub, err := authzClient(t, broker, "f060of-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	const topic = "f060of/x"
	results := f060ObserveResults(t, topic)
	f060FailRetained(t, topic)
	pause := f060AuthzRevokeOnce(t, dialed, "f060of-pub")
	mqttTestDurableWritePauseV.Store(&pause)
	t.Cleanup(func() { mqttTestDurableWritePauseV.Store(nil) })

	pub.Publish(topic, 1, true, []byte("no")).WaitTimeout(5 * time.Second)
	err = f060WaitResult(t, results)
	for _, want := range []error{errMQTTRetainedStoreUnconfirmed, errMQTTServiceEpochMoved} {
		if !errors.Is(err, want) {
			t.Fatalf("delivery result %v does not carry %v", err, want)
		}
	}
}
