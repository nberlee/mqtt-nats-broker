package mqtt

import (
	"testing"

	"github.com/nats-io/nuid"
)

// TestSessionClearPurgesQoS2Hold verifies that discarding a session (sess.clear,
// invoked whenever a session ends: clean disconnect, clean-start CONNECT, session
// expiry, or a server-forced disconnect such as MQTT 5.0 Receive Maximum
// exceeded, reason 147) purges that incarnation's inbound QoS2 records in
// $MQTT_qos2in — BOTH record states, held and the terminal tombstone a committed
// move leaves behind. The per-subject arbitrated purge covers both (there is no
// wildcard purge: the discard enumerates this incarnation's generation-scoped hold
// subjects and purges each where it wins the release claim); this test pins that,
// since a tombstone left behind would make a later PUBLISH on the same packet
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
	t.Parallel()
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

	if !mqttValidSessGen(sess.gen) {
		t.Fatalf("a freshly created session must carry a minted incarnation generation, got %q", sess.gen)
	}
	retiringGen := sess.gen // clear() re-mints, so capture it before the discard
	subject := mqttQoS2HoldSubject(sess.idHash, retiringGen, pi)
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
	doneSubject := mqttQoS2HoldSubject(sess.idHash, retiringGen, donePI)
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
	// clear() re-minted the incarnation, so a later session for the same client ID
	// hashes to the same idHash but holds on a DIFFERENT subject. That is what makes a
	// late purge from the discarded incarnation harmless TO THE NEXT INCARNATION: it can
	// no longer name this record. (F-062 for what it does not protect.)
	sess2, _, err := asm.createOrRestoreSession(clientID, s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession (second session): %v", err)
	}
	if sess2.idHash != sess.idHash {
		t.Fatalf("expected the same client hash, got %q vs %q", sess2.idHash, sess.idHash)
	}
	if sess2.gen == sess.gen {
		t.Fatal("expected the discarded session to have been re-minted with a fresh incarnation generation")
	}
	subject2 := mqttQoS2HoldSubject(sess2.idHash, sess2.gen, pi)
	if subject2 == subject {
		t.Fatalf("expected a distinct hold subject for the new incarnation, both are %q", subject2)
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

	// Re-running the RETIRED incarnation's destruction — the late-delete window a
	// nonce re-check cannot close — must not touch the new incarnation's record.
	// This is the arbitration walk now, and it is scoped by the retired generation, so it
	// cannot name the NEW incarnation's subjects. (A peer still serving the retiring
	// generation is a different matter: F-062.)
	if err := asm.arbitrateQoS2ReleaseClaims(s, sess.idHash, retiringGen); err != nil {
		t.Fatalf("replaying the retired incarnation's arbitration: %v", err)
	}
	if stored, err = sess2.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject2); err != nil {
		t.Fatalf("the new incarnation's hold entry was deleted by the retired incarnation's purge: %v", err)
	}
	if string(stored.Data) != "fresh" {
		t.Fatalf("payload = %q, want %q after replaying the retired purge", stored.Data, "fresh")
	}
}
