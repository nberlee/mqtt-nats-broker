package mqtt

import (
	"strconv"
	"testing"

	"github.com/nats-io/nuid"
)

// TestSessionClearPurgesQoS2Hold verifies that discarding a session (sess.clear,
// invoked whenever a session ends: clean disconnect, clean-start CONNECT, session
// expiry, or a server-forced disconnect such as MQTT 5.0 Receive Maximum
// exceeded, reason 147) purges that client's inbound QoS2 records in
// $MQTT_qos2in — BOTH record states, held and the terminal tombstone a committed
// move leaves behind. The wildcard purge covers both; this test pins that, since
// a tombstone left behind would make a later session's PUBLISH on the same packet
// identifier take the reap-and-retry path instead of a clean create.
//
// Regression: without the purge, a discarded session's QoS2 records linger
// forever (MaxMsgsPer=1). A later session for the same client ID then collides by
// Packet Identifier and PUBREL could replay the stale payload from the discarded
// session instead of the new one. This is what made the Eclipse Paho interop
// suite's test_payload_format/test_publication_expiry see a stray message after
// test_flow_control2 force-disconnects "myclientid" with reason 147 (see
// docs/CONFORMANCE.md).
func TestSessionClearPurgesQoS2Hold(t *testing.T) {
	_, nc := runNATS(t)

	s := &Server{
		nrc:      nc,
		opts:     &Options{},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	defer close(s.quitCh)

	c := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}

	const clientID = "leaky"
	const pi = uint16(1)

	sess, _, err := asm.createOrRestoreSession(clientID, s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}

	subject := mqttQoS2IncomingMsgsStreamSubjectPrefix + sess.idHash + "." + strconv.FormatUint(uint64(pi), 10)
	if _, err := sess.jsa.storeMsg(subject, 0, []byte("stale")); err != nil {
		t.Fatalf("store QoS2 hold entry: %v", err)
	}
	if _, err := sess.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject); err != nil {
		t.Fatalf("expected QoS2 hold entry to be stored before clear: %v", err)
	}

	// A terminal tombstone on a DIFFERENT packet identifier of the same client, so
	// the purge is shown to clear both record states and not just the one it was
	// originally written for. Written through the real encoder at the sequence the
	// tombstone would replace, which is 0 here (nothing on that subject yet).
	const donePI = uint16(2)
	doneSubject := mqttQoS2IncomingMsgsStreamSubjectPrefix + sess.idHash + "." + strconv.FormatUint(uint64(donePI), 10)
	if _, err := sess.jsa.storeQoS2DoneCAS(doneSubject, nuid.Next(), 0); err != nil {
		t.Fatalf("store QoS2 terminal record: %v", err)
	}
	if rec, _, err := sess.jsa.loadQoS2Record(doneSubject); err != nil || rec == nil || rec.state != mqttQoS2Done {
		t.Fatalf("expected a terminal QoS2 record before clear, got rec=%v err=%v", rec, err)
	}

	// Discard the session, as mqttHandleClosedClient does on a force-disconnected
	// (or any clean) session. noWait=false so the purge is confirmed before we
	// continue, keeping the test deterministic.
	if err := sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}

	if _, err := sess.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject); err == nil {
		t.Fatal("expected QoS2 hold entry to be purged by sess.clear, but it is still present")
	}
	if rec, _, err := sess.jsa.loadQoS2Record(doneSubject); err != nil || rec != nil {
		t.Fatalf("expected the terminal QoS2 record to be purged by sess.clear, got rec=%v err=%v", rec, err)
	}

	// A later session for the same client ID hashes to the same hold subject; it
	// must be able to store fresh QoS2 state there without colliding with the
	// discarded session's (now-purged) entry.
	sess2, _, err := asm.createOrRestoreSession(clientID, s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession (second session): %v", err)
	}
	subject2 := mqttQoS2IncomingMsgsStreamSubjectPrefix + sess2.idHash + "." + strconv.FormatUint(uint64(pi), 10)
	if subject2 != subject {
		t.Fatalf("expected the same hold subject for the same client ID, got %q vs %q", subject2, subject)
	}
	if _, err := sess2.jsa.storeMsg(subject2, 0, []byte("fresh")); err != nil {
		t.Fatalf("store QoS2 hold entry for the new session: %v", err)
	}
	stored, err := sess2.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject2)
	if err != nil {
		t.Fatalf("load fresh QoS2 hold entry: %v", err)
	}
	if string(stored.Data) != "fresh" {
		t.Fatalf("payload = %q, want %q (stale entry from the discarded session leaked through)", stored.Data, "fresh")
	}
}
