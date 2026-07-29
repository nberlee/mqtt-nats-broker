//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// Coverage for the JetStream error classification in the QoS 2 path: the
// isJSErrCausedBy classifier itself, the inbound dedup store that depends on it,
// and the PUBREL lookup/delete failures that must not be reported to the client
// as 0x92 "packet identifier not found".
//
// Not covered here, deliberately:
//   - The ambiguous delete (JetStream committed the removal but the response was
//     lost, or the process crashed after the commit). The hooks below fail before
//     the request is issued, which is the only outcome a test can arrange; the
//     ambiguous one is a tracked release blocker in docs/ROADMAP.md, not
//     something these tests can assert.
//   - The two PUBCOMP exits mqttProcessPubRel did not change: an expiry-dropped
//     hold is already covered by TestMQTTv5QoS2ExpiryAgesDuringPubRel, and a
//     malformed hold record is a broken-invariant path. Both keep answering the
//     client because the single gated defer in mqttProcessPubRel clears
//     sendPubComp only in the two classified-failure branches.

// A JetStream API error whose real cause is only present as interpolated text
// must be classified by error code plus a confined cause comparison — never by
// the rendered ApiError.Error() form, and never by a bare substring of it.
func TestQoS2JSErrClassifier(t *testing.T) {
	storeFailed := func(cause error) error {
		return natsserver.NewJSStreamStoreFailedError(cause)
	}
	// A cause wrapped BEFORE the ApiError is built: the wrapper text lands in
	// Description and is absent from the error chain, so errors.Is cannot see it.
	wrappedCause := storeFailed(fmt.Errorf("stream %q: %w", "X", ErrMaxMsgsPerSubject))

	for _, tc := range []struct {
		name  string
		err   error
		id    ErrorIdentifier
		cause error
		want  bool
	}{
		{
			name: "exact cause", err: storeFailed(ErrMaxMsgsPerSubject),
			id: JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: true,
		},
		{
			// errors.As must see through an outer %w wrapper; IsNatsErr's bare
			// type assertion would not.
			name: "outer %w wrapper",
			err:  fmt.Errorf("storing QoS2 hold: %w", storeFailed(ErrMaxMsgsPerSubject)),
			id:   JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: true,
		},
		{
			name: "cause wrapped before construction", err: wrappedCause,
			id: JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: true,
		},
		{
			name: "same code, different cause", err: storeFailed(errors.New("insufficient resources")),
			id: JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: false,
		},
		{
			// Same description text under a different code is a different error.
			name: "different code, same text",
			err:  natsserver.NewJSStreamMsgDeleteFailedError(ErrMaxMsgsPerSubject),
			id:   JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: false,
		},
		{
			// The reason a bare strings.Contains is not acceptable.
			name: "description merely contains the sentinel",
			err: storeFailed(errors.New("some unrelated " +
				ErrMaxMsgsPerSubject.Error() + " condition")),
			id: JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: false,
		},
		{
			name: "delete failed caused by msg not found",
			err:  natsserver.NewJSStreamMsgDeleteFailedError(ErrStoreMsgNotFound),
			id:   JSStreamMsgDeleteFailedF, cause: ErrStoreMsgNotFound, want: true,
		},
		{name: "nil error", err: nil, id: JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: false},
		{
			name: "nil cause", err: storeFailed(ErrMaxMsgsPerSubject),
			id: JSStreamStoreFailedF, cause: nil, want: false,
		},
		{
			name: "not an API error", err: errors.New(ErrMaxMsgsPerSubject.Error()),
			id: JSStreamStoreFailedF, cause: ErrMaxMsgsPerSubject, want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isJSErrCausedBy(tc.err, tc.id, tc.cause); got != tc.want {
				t.Fatalf("isJSErrCausedBy(%v, %v, %v) = %v, want %v",
					tc.err, tc.id, tc.cause, got, tc.want)
			}
		})
	}

	// The rendered form must not be what identity depends on: a classified error
	// and an unrelated one can render identically once formatting changes, and
	// the previous implementation compared exactly this string.
	if ae := jsAPIError(storeFailed(ErrMaxMsgsPerSubject)); ae == nil {
		t.Fatal("jsAPIError returned nil for a real ApiError")
	} else if ae.ErrCode != ApiErrors[JSStreamStoreFailedF].ErrCode {
		t.Fatalf("Expected ErrCode %v, got %v", ApiErrors[JSStreamStoreFailedF].ErrCode, ae.ErrCode)
	}
	if jsAPIError(errors.New("boom")) != nil {
		t.Fatal("jsAPIError returned non-nil for a plain error")
	}
}

// A DUP re-PUBLISH of a held QoS 2 packet identifier is rejected by the hold
// stream's MaxMsgsPer=1 as a generic "store failed" carrying
// ErrMaxMsgsPerSubject. That is the duplicate signal, so it must still produce a
// PUBREC, and the message must be delivered onward exactly once on PUBREL.
func TestQoS2DupPublishStoresOnce(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnect(t, &mqttConnInfo{clientID: "dupsub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cs, rs, []*mqttFilter{{filter: "dup/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, cs, nil, rs)

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "duppub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	pi := uint16(77)
	testMQTTSendPublishPacket(t, cp, 2, false, false, "dup/topic", pi, []byte("once"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, rp, pi)

	// The retry the client would send if it missed the PUBREC: same PI, DUP set.
	// A duplicate must be answered idempotently, not treated as an error.
	testMQTTSendPublishPacket(t, cp, 2, true, false, "dup/topic", pi, []byte("once"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, rp, pi)

	testQoS2SendPubRel(t, cp, pi)
	testMQTTReadPIPacket(mqttPacketPubComp, t, rp, pi)

	subPI := testMQTTCheckPubMsgNoAck(t, cs, rs, "dup/topic", mqttPubQoS2, []byte("once"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rs, subPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, subPI)

	// Only one onward delivery for the two PUBLISH packets.
	testMQTTExpectNothing(t, rs)
}

// testQoS2SendPubRel writes a PUBREL. Unlike PUBREC/PUBCOMP, PUBREL carries
// mandatory fixed-header flags 0x2 [MQTT-3.6.1-1], which testMQTTSendPIPacket
// does not set.
func testQoS2SendPubRel(t testing.TB, c net.Conn, pi uint16) {
	t.Helper()
	pkt := [4]byte{mqttPacketPubRel | 0x2, 0x2, byte(pi >> 8), byte(pi)}
	if _, err := testMQTTWrite(c, pkt[:]); err != nil {
		t.Fatalf("Error writing PUBREL: %v", err)
	}
}

// testQoS2ExpectClientGone waits until the broker no longer has a client for
// clientID, i.e. the connection was actually closed rather than merely left
// silent.
func testQoS2ExpectClientGone(t testing.TB, s *Server, clientID string) {
	t.Helper()
	checkFor(t, testMQTTTimeout, 20*time.Millisecond, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.clients {
			c.mu.Lock()
			match := c.isMqtt() && c.mqtt.cid == clientID
			c.mu.Unlock()
			if match {
				return fmt.Errorf("client %q is still connected", clientID)
			}
		}
		return nil
	})
}

// testQoS2HoldEntry reports whether the QoS 2 hold entry on subject exists. It
// takes the JSA rather than looking the client up, because the interesting checks
// happen after the failing PUBREL has closed the connection. Any installed fault
// hook must be cleared first (the callers do that).
func testQoS2HoldEntry(t testing.TB, jsa *mqttJSA, subject string) bool {
	t.Helper()
	stored, err := jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject)
	if err != nil && isErrorOtherThan(err, JSNoMessageFoundErr) {
		t.Fatalf("Unable to look up QoS2 hold entry on %q: %v", subject, err)
	}
	return stored != nil
}

// A broker-side JetStream failure while releasing an inbound QoS 2 message must
// not be reported as 0x92: that tells the client an exactly-once exchange
// finished for a message the broker still holds. The PUBCOMP is suppressed and
// the connection closed instead, so the client retries PUBREL against intact
// session state.
//
// Persistent session (cleanSess false): a clean-session disconnect discards the
// session and purges the hold entries (TestSessionClearPurgesQoS2Hold), leaving
// nothing to retry against.
func TestQoS2PubRelLookupFailureSuppressesPubComp(t *testing.T) {
	testQoS2PubRelFailureSuppressesPubComp(t, "lookup",
		func(t *testing.T, stream, subject string, pi uint16) func() {
			return setMQTTTestLoadLastMsgHook(func(gotStream, gotSubject string) error {
				if gotStream == stream && gotSubject == subject {
					return natsserver.NewJSStreamNotFoundError()
				}
				return nil
			})
		})
}

// Same for a failure to remove the hold entry. The onward delivery must not
// happen either: releasing the message while its hold entry survives would let a
// later PUBLISH reusing the packet identifier be rejected as a duplicate, or
// replay this payload on the next PUBREL.
func TestQoS2PubRelDeleteFailureSuppressesPubComp(t *testing.T) {
	testQoS2PubRelFailureSuppressesPubComp(t, "delete",
		func(t *testing.T, stream, subject string, pi uint16) func() {
			return setMQTTTestDeleteMsgHook(func(gotStream string, _ uint64) error {
				if gotStream == stream {
					return natsserver.NewJSStreamNotFoundError()
				}
				return nil
			})
		})
}

func testQoS2PubRelFailureSuppressesPubComp(t *testing.T, name string,
	install func(t *testing.T, stream, subject string, pi uint16) func()) {

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	subID := name + "-sub"
	pubID := name + "-pub"

	cs, rs := testMQTTConnect(t, &mqttConnInfo{clientID: subID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cs, rs, []*mqttFilter{{filter: name + "/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, cs, nil, rs)

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: pubID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	pi := uint16(31)
	testMQTTSendPublishPacket(t, cp, 2, false, false, name+"/topic", pi, []byte("held"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, rp, pi)

	// Captured while the publisher is still connected: the checks below run after
	// the failing PUBREL has closed its connection.
	c := testMQTTGetClient(t, s, pubID)
	jsa := c.mqtt.sess.jsa
	subject := c.mqttQoS2InternalSubject(pi)
	if !testQoS2HoldEntry(t, jsa, subject) {
		t.Fatal("Expected the QoS2 hold entry to exist after PUBREC")
	}

	restore := install(t, mqttQoS2IncomingMsgsStreamName, subject, pi)
	// Registered immediately: every check below can Fatal, and a fault hook left
	// installed is a global that would contaminate later tests.
	restoreOnce := sync.OnceFunc(restore)
	t.Cleanup(restoreOnce)

	testQoS2SendPubRel(t, cp, pi)

	// No PUBCOMP: the exchange is unfinished, so the client's send-quota slot
	// stays occupied (both are gated on the same sendPubComp flag).
	if b, _, ok := testMQTTReadPacketReady(rp, testMQTTTimeout); ok {
		t.Fatalf("Expected no PUBCOMP after a %s failure, got packet type %v", name, b&mqttPacketMask)
	}
	// ...and the connection really is gone. testMQTTReadPacketReady reports
	// !ok for a read timeout too, so on its own it would also pass for a
	// regression that suppressed the PUBCOMP but kept the connection open —
	// which the reconnect below could then mask as a takeover.
	testQoS2ExpectClientGone(t, s, pubID)
	// The subscriber must not have been given the message either.
	testMQTTExpectNothing(t, rs)

	restoreOnce()

	// The hold entry survived the failure, so the retry can complete. (Only the
	// pre-dispatch failure is asserted here; see the file comment.)
	if !testQoS2HoldEntry(t, jsa, subject) {
		t.Fatalf("Expected the QoS2 hold entry to survive the %s failure", name)
	}

	cp.Close()
	cp2, rp2 := testMQTTConnect(t, &mqttConnInfo{clientID: pubID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer cp2.Close()
	testMQTTCheckConnAck(t, rp2, mqttConnAckRCConnectionAccepted, true)

	testQoS2SendPubRel(t, cp2, pi)
	testMQTTReadPIPacket(mqttPacketPubComp, t, rp2, pi)

	// Retried successfully: the message is delivered onward exactly once, and the
	// hold entry is gone.
	subPI := testMQTTCheckPubMsgNoAck(t, cs, rs, name+"/topic", mqttPubQoS2, []byte("held"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rs, subPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, subPI)
	testMQTTExpectNothing(t, rs)

	if testQoS2HoldEntry(t, jsa, subject) {
		t.Fatal("Expected the QoS2 hold entry to be released after the successful retry")
	}
}

// A hold record removed between the lookup and the delete (a concurrent PUBREL,
// or a session discard purging it) must be answered 0x92 with the loaded copy NOT
// delivered — not a suppressed PUBCOMP and a dropped connection, which would turn
// a benign clustered race into a disconnect. See the tradeoff described at the
// branch in mqttProcessPubRel: delivering could duplicate a concurrent PUBREL's
// delivery or resurrect a discarded session's message.
func TestQoS2PubRelHoldAlreadyDeletedReports92(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "raced-sub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "raced/topic", opts: 2}})

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "raced-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)

	pi := uint16(19)
	testMQTTPublishV5(t, cp, 2, pi, "raced/topic", []byte("raced"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, rp, pi)

	cl := testMQTTGetClient(t, s, "raced-pub")
	jsa := cl.mqtt.sess.jsa
	subject := cl.mqttQoS2InternalSubject(pi)
	stored, err := jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject)
	if err != nil || stored == nil {
		t.Fatalf("Expected a QoS2 hold entry after PUBREC: stored=%v err=%v", stored, err)
	}

	// Model the race exactly: the PUBREL's lookup must SUCCEED (so the branch
	// under test is the delete's, not the missing-PI one), and the entry must then
	// be gone by the time the delete runs. The hook removes it for real — clearing
	// itself first so the inner call goes through — and returns the same
	// already-deleted error JetStream produces (a generic 10057 whose only record
	// of the reason is ErrStoreMsgNotFound in its Description).
	var once sync.Once
	var restore func()
	restore = setMQTTTestDeleteMsgHook(func(gotStream string, seq uint64) error {
		if gotStream != mqttQoS2IncomingMsgsStreamName || seq != stored.Sequence {
			return nil
		}
		var ret error
		once.Do(func() {
			restore()
			if err := jsa.deleteMsg(gotStream, seq, true); err != nil {
				ret = fmt.Errorf("test hook could not remove the hold entry: %w", err)
				return
			}
			ret = natsserver.NewJSStreamMsgDeleteFailedError(ErrStoreMsgNotFound)
		})
		return ret
	})
	t.Cleanup(func() { setMQTTTestDeleteMsgHook(nil) })

	testQoS2SendPubRel(t, cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected reason 0x%x for an already-removed hold entry, got 0x%x",
			mqttReasonPacketIDNotFound, reason)
	}
	// The loaded copy must not have been delivered onward.
	testMQTTExpectNothing(t, rs)
	// And the connection stays up: a second PUBREL is answered the same way (the
	// hook has fired and cleared itself, so this one takes the missing-PI path).
	testQoS2SendPubRel(t, cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected reason 0x%x on retry, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}
	if testQoS2HoldEntry(t, jsa, subject) {
		t.Fatal("Expected the hold entry to be gone")
	}
}

// A PUBREL for a packet identifier the broker has no state for is a genuine
// "not found" and keeps reporting 0x92 — the classification must not turn an
// absent hold entry into a connection drop.
func TestQoS2PubRelUnknownPIStillAnswered(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unknownpi", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	testQoS2SendPubRel(t, c, 4242)
	if reason, _ := testMQTTReadPubCompV5(t, r, 4242); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}

	// Still a live connection: a second PUBREL is answered the same way.
	testQoS2SendPubRel(t, c, 4242)
	if reason, _ := testMQTTReadPubCompV5(t, r, 4242); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected reason 0x%x on retry, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}
}
