// Copyright 2026 The mqtt-nats-bridge Authors
//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"net"
	"testing"
	"time"
)

// End-to-end cover for a BOUND session's record-CAS settlement: real sockets, a real
// CONNECT, a real remote takeover of the session record, and a real client left publishing
// into a session another broker owns.
//
// Two prerequisites make it the production shape rather than a seam. The record must be
// moved by ANOTHER account session manager over the same JetStream — the cross-broker
// boundary — so the local save is rejected by the stream rather than by a hook; and the
// persist observation must be DROPPED, because with the notification delivered the old
// eviction path removes the client and this arm is never reached at all.

// testBoundE2EMoveRecord restores the same client ID through a second account session
// manager sharing this server's NATS connection, and persists it. That is what moves the
// $MQTT_sess subject out from under the local session, exactly as another broker taking the
// session over would.
func testBoundE2EMoveRecord(t *testing.T, s *Server, node, clientID string) {
	t.Helper()
	peer := &Server{nrc: s.nrc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: node}
	peer.running.Store(true)
	peer.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	t.Cleanup(func() { close(peer.quitCh) })

	ic := peer.createInternalAccountClient()
	asm, err := peer.getOrCreateMQTTAccountSessionManager(ic)
	if err != nil {
		t.Fatalf("%s: account session manager: %v", node, err)
	}
	sess, present, err := asm.createOrRestoreSession(clientID, peer.getOpts())
	if err != nil {
		t.Fatalf("%s: restore %q: %v", node, clientID, err)
	}
	if !present {
		t.Fatalf("%s must restore the EXISTING session, or it is not a takeover", node)
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("%s: persist the taken-over record: %v", node, err)
	}
}

// TestMQTTBoundRejectionE2EForeignClosesConnection is the roadmap item's own scenario over
// real connections: a live client whose session record has moved to another broker, with
// the persist notification lost, must be told and closed BEFORE its next packet takes
// effect — [MQTT-3.1.4-3] — and its Will must go the way a takeover's does.
func TestMQTTBoundRejectionE2EForeignClosesConnection(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A watcher for the Will topic, so "the Will was not published" is observed rather
	// than inferred from the disposition field.
	wc, wr := testMQTTConnect(t, &mqttConnInfo{clientID: "boundwatch", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer wc.Close()
	testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, wc, wr, []*mqttFilter{{filter: "bounde2e/will", qos: 0}}, []byte{0})
	testMQTTFlush(t, wc, nil, wr)

	ci := &mqttV5ConnInfo{
		clientID: "bounde2e",
		will:     &mqttWill{topic: []byte("bounde2e/will"), message: []byte("bye")},
	}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNACK reason %x", reason)
	}
	// A subscription to remove later: its removal is the record write that gets rejected,
	// which is how the marker is armed in production. sess.update persists only a real
	// membership change, so without this the UNSUBSCRIBE would save nothing.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "bounde2e/gone"}}); codes[0] != 0 {
		t.Fatalf("SUBACK reason %x", codes[0])
	}

	asm := testMQTTGetGlobalASM(t, s)
	sess := testMQTTGetClient(t, s, "bounde2e").mqtt.sess
	if sess == nil {
		t.Fatal("the connection must be bound to a session")
	}
	idHash := sess.idHash
	asm.mu.Lock()
	asm.testSessPersistDrop = func(h string) bool { return h == idHash }
	asm.mu.Unlock()

	testBoundE2EMoveRecord(t, s, "node2", "bounde2e")

	// The UNSUBSCRIBE's own record save is now CAS-rejected. v5 reports that per filter
	// (0x83) and still answers, so the packet completes and the connection stays up — with
	// the marker armed by the store boundary itself.
	if codes := testMQTTUnsubV5(t, c, r, 2, []string{"bounde2e/gone"}); codes[0] != mqttReasonImplementationSpecificError {
		t.Fatalf("the UNSUBSCRIBE's save must be rejected by the moved record: reason %x", codes[0])
	}
	sess.mu.Lock()
	armed := sess.recordRejectionGatesServingLocked()
	sess.mu.Unlock()
	if !armed {
		t.Fatal("the rejected save must have armed the marker, or the fixture proves nothing")
	}

	// The next packet must not be served.
	testMQTTSendPublishPacket(t, c, 1, false, false, "bounde2e/topic", 3, []byte("nope"))
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketDisconnect {
		t.Fatalf("expected a server DISCONNECT, got %x (pl=%d)", pt, pl)
	}
	reason, err := r.readByte("disconnect reason")
	if err != nil {
		t.Fatalf("read the DISCONNECT reason: %v", err)
	}
	if reason != mqttReasonSessionTakenOver {
		t.Fatalf("expected 0x8E session taken over, got %x", reason)
	}

	// Nothing follows the 0x8E — a served PUBLISH would have been acked BEFORE it, which
	// the packet-type assertion above already caught — and the socket really closes.
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [64]byte
	n, rerr := c.Read(buf[:])
	if n > 0 {
		t.Fatalf("nothing may follow the takeover DISCONNECT: got %x", buf[:n])
	}
	var nerr net.Error
	if rerr == nil || (errors.As(rerr, &nerr) && nerr.Timeout()) {
		t.Fatalf("the connection must be CLOSED after the 0x8E, not merely idle: %v", rerr)
	}

	// The Will goes the way a local takeover's does: this connection was displaced, and a
	// displacement is not a Will publication trigger. The window covers the teardown that
	// follows the close; the sentinel afterwards proves the watcher would have seen one.
	testMQTTExpectNothingFor(t, wr, 500*time.Millisecond)
	testMQTTPublish(t, wc, wr, 0, false, false, "bounde2e/will", 0, []byte("sentinel"))
	testMQTTCheckPubMsg(t, wc, wr, "bounde2e/will", 0, []byte("sentinel"))
}
