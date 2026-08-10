//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"
)

// End-to-end cover for the fork verdict: real sockets, a real CONNECT, a real record that
// FORKED under the connection, and a real client left publishing into it.
//
// The companion is TestMQTTBoundRejectionE2EForeignClosesConnection, which stages a
// legitimate SUCCESSOR through the same machinery and must still get 0x8E. Read them
// together: this file's assertion is only meaningful because that one exists — a change
// that downgraded every rejection to 0x89 would pass here and fail there.

// testE2ELandSibling stores a record derived from the SAME parent as the bound session's
// acknowledged one, unconditionally. That is the durable end state a leader transition
// produces, staged without one — see sess_fork_lineage_test.go.
func testE2ELandSibling(t *testing.T, sess *mqttSession) {
	t.Helper()
	sess.mu.Lock()
	sibling := sess.buildPersistedSessionLocked()
	sibling.Gen = sess.gen
	sibling.PWID = sess.pwid
	sibling.WID = mqttNewRecordWID()
	sibling.Subs = map[string]byte{"forked/e2e/marker": 1}
	domainTk, idHash, pwid := sess.domainTk, sess.idHash, sess.pwid
	sess.mu.Unlock()
	if pwid == _EMPTY_ {
		t.Fatal("fixture: the acknowledged record must have a parent, or no sibling can be proven")
	}
	body, err := json.Marshal(&sibling)
	if err != nil {
		t.Fatalf("marshal the sibling: %v", err)
	}
	if _, err := sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); err != nil {
		t.Fatalf("land the sibling record: %v", err)
	}
}

// TestMQTTForkedRecordE2EAnswers0x89AndPublishesTheWill is the finding's own scenario over
// real connections.
//
// A live client whose session record FORKED must not be told its session was taken over:
// nothing established another owner, and 0x8E instructs the client to stop reconnecting on
// the strength of a guess. It gets 0x89 "server busy, try again later" and the ordinary
// close bookkeeping runs — so its Will is PUBLISHED, because a displacement nobody
// demonstrated is not a Will-suppression trigger.
//
// That Will assertion is the sharpest difference from the takeover path, where the same
// fixture suppresses it, and it is deliberately the opposite of what the foreign arm does.
func TestMQTTForkedRecordE2EAnswers0x89AndPublishesTheWill(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	wc, wr := testMQTTConnect(t, &mqttConnInfo{clientID: "forkwatch", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer wc.Close()
	testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, wc, wr, []*mqttFilter{{filter: "forke2e/will", qos: 0}}, []byte{0})
	testMQTTFlush(t, wc, nil, wr)

	ci := &mqttV5ConnInfo{
		clientID: "forke2e",
		will:     &mqttWill{topic: []byte("forke2e/will"), message: []byte("bye")},
	}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNACK reason %x", reason)
	}
	// A subscription to remove later: its removal is the record write that gets rejected,
	// which is how the marker is armed in production.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "forke2e/gone"}}); codes[0] != 0 {
		t.Fatalf("SUBACK reason %x", codes[0])
	}

	asm := testMQTTGetGlobalASM(t, s)
	sess := testMQTTGetClient(t, s, "forke2e").mqtt.sess
	if sess == nil {
		t.Fatal("the connection must be bound to a session")
	}
	idHash := sess.idHash
	// Without this the persist observation's own classifying worker evicts the
	// connection first, and the rejection-settlement path under test is never reached.
	asm.mu.Lock()
	asm.testSessPersistDrop = func(h string) bool { return h == idHash }
	asm.mu.Unlock()

	testE2ELandSibling(t, sess)

	// The UNSUBSCRIBE's own record save is now CAS-rejected; v5 reports that per filter
	// and still answers, so the connection stays up with the marker armed.
	if codes := testMQTTUnsubV5(t, c, r, 2, []string{"forke2e/gone"}); codes[0] != mqttReasonImplementationSpecificError {
		t.Fatalf("the UNSUBSCRIBE's save must be rejected by the forked record: reason %x", codes[0])
	}
	sess.mu.Lock()
	armed := sess.recordRejectionGatesServingLocked()
	sess.mu.Unlock()
	if !armed {
		t.Fatal("the rejected save must have armed the marker, or the fixture proves nothing")
	}

	// The next packet must not be served — and the reason must be 0x89, not 0x8E.
	testMQTTSendPublishPacket(t, c, 1, false, false, "forke2e/topic", 3, []byte("nope"))
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketDisconnect {
		t.Fatalf("expected a server DISCONNECT, got %x (pl=%d)", pt, pl)
	}
	reason, err := r.readByte("disconnect reason")
	if err != nil {
		t.Fatalf("read the DISCONNECT reason: %v", err)
	}
	if reason != mqttReasonServerBusy {
		t.Fatalf("a forked record answers 0x89 'server busy, try again later', never 0x8E "+
			"'session taken over' — nothing established another owner. Got %x", reason)
	}

	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [256]byte
	n, rerr := c.Read(buf[:])
	if n > 0 {
		t.Fatalf("nothing may follow the DISCONNECT: got %x", buf[:n])
	}
	var nerr net.Error
	if rerr == nil || (errors.As(rerr, &nerr) && nerr.Timeout()) {
		t.Fatalf("the connection must be CLOSED after the 0x89, not merely idle: %v", rerr)
	}

	// The Will IS published. This is the whole behavioural difference from the takeover
	// path, which suppresses it: the 0x89 route runs the ordinary close bookkeeping, and a
	// displacement nobody demonstrated cannot be a reason to swallow the client's Will.
	testMQTTCheckPubMsg(t, wc, wr, "forke2e/will", 0, []byte("bye"))
}

// TestMQTTForkedRecordE2EConnectRebuildsFromRecord is M10's paired test.
//
// mqttProcessConnect resolves a resident session's unresolved write BEFORE computing
// Session Present and before its accept save, and its switch DEFAULTS to "resolved as
// ours". A forked resolution left out of that switch therefore does not merely log
// oddly: the CONNECT binds the stale object, whose CAS base is no longer the record's,
// so the accept save is rejected and the connection is refused.
//
// The assertion is that the reconnect SUCCEEDS — rebuilt from the surviving record, which
// is what the session's durable state now is whoever wrote it.
func TestMQTTForkedRecordE2EConnectRebuildsFromRecord(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "forkconnect", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "forkconnect/x", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	asm := testMQTTGetGlobalASM(t, s)
	sess := testMQTTGetClient(t, s, "forkconnect").mqtt.sess
	idHash := sess.idHash
	asm.mu.Lock()
	asm.testSessPersistDrop = func(h string) bool { return h == idHash }
	asm.mu.Unlock()

	// Leave the session with an unresolved write whose bytes never reached the stream,
	// then land a sibling of its acknowledged record. The resolver's readback now finds a
	// record that is provably no successor of ours.
	mqttTestSessStoreSuppressV.Store(true)
	_ = testSessSave(t, sess)
	mqttTestSessStoreSuppressV.Store(false)
	if !sess.isSeqUncertain() {
		t.Fatal("the suppressed save must leave the session unresolved")
	}
	testE2ELandSibling(t, sess)

	testMQTTDisconnect(t, c, nil)
	c.Close()

	// The reconnect resolves the ambiguity as FORKED, evicts the stale object and rebuilds
	// from the surviving record. Session Present is that record's, not the stale object's.
	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	sess2 := testMQTTGetClient(t, s, "forkconnect").mqtt.sess
	if sess2 == nil {
		t.Fatal("the reconnect must be bound to a session")
	}
	if sess2 == sess {
		t.Fatal("the forked resolution must EVICT the stale object; reusing it binds a " +
			"session whose CAS base is no longer the record's")
	}
	if sess2.isSeqUncertain() {
		t.Fatal("the rebuilt session must not inherit the stale object's uncertainty")
	}
}

// testE2ELiveCompetingOwner stands up a SECOND account session manager that restores the
// record and stays resident, then lands its own write derived from the record IT was
// acknowledged for. That write is a sibling of the local session's, and its author is
// alive — which is the shape the fork verdict cannot tell apart from an orphaned write,
// and therefore the shape that produces this change's sharpest residual.
//
// The peer's own persist observation is dropped for the same reason the local one is:
// otherwise the local save below evicts the peer through its own persist-observation
// worker, and there is no competing owner left to be competing.
func testE2ELiveCompetingOwner(t *testing.T, s *Server, node, clientID string, idHash string) *mqttSession {
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
	asm.mu.Lock()
	asm.testSessPersistDrop = func(h string) bool { return h == idHash }
	asm.mu.Unlock()

	sess, present, err := asm.createOrRestoreSession(clientID, peer.getOpts())
	if err != nil {
		t.Fatalf("%s: restore %q: %v", node, clientID, err)
	}
	if !present {
		t.Fatalf("%s must restore the EXISTING record, or it is no competitor", node)
	}
	asm.addSession(sess, true)

	// BIND a connection to it. "Resident" alone is an unrelated object sitting in a map;
	// what makes this a takeover whose Will would ordinarily be suppressed is that the
	// competitor has a live client of its own. The socket is a capture rather than a real
	// listener because the peer Server has no MQTT listener — it exists only to own the
	// second session — and nothing in this test reads what it writes.
	out := &testWireCapture{closed: make(chan struct{}), recordAfterClose: true}
	pc := &client{srv: peer, acc: peer.globalAccount(), nrc: peer.nrc, nc: out, rawConn: out,
		mpay: 1024 * 1024}
	pc.mqtt = &mqtt{sess: sess, asm: asm, proto: mqttProtoLevel5, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
	pc.initClient()
	pc.mu.Lock()
	pc.flags.set(connectReceived)
	pc.mu.Unlock()
	pc.connAckWritten.Store(true)
	sess.mu.Lock()
	sess.c = pc
	sess.mu.Unlock()
	t.Cleanup(func() { _ = out.Close() })
	return sess
}

// testE2ELandCompetingWrite lands the peer's own write, derived from the record IT was
// acknowledged for at restore time.
//
// It is called AFTER the local session has written again, which is what makes the two
// writes siblings rather than parent and child: both are derived from the record the peer
// restored, and only one of them could have committed under a working CAS. Getting that
// ordering wrong yields a legitimate successor and a 0x8E — which is the correct answer to
// a different question.
func testE2ELandCompetingWrite(t *testing.T, sess *mqttSession, idHash string) {
	t.Helper()
	sess.mu.Lock()
	rec := sess.buildPersistedSessionLocked()
	rec.Gen = sess.gen
	rec.WID, rec.PWID = mqttNewRecordWID(), sess.wid
	rec.Subs = map[string]byte{"forklive/e2e/peer": 1}
	domainTk := sess.domainTk
	sess.mu.Unlock()
	body, err := json.Marshal(&rec)
	if err != nil {
		t.Fatalf("marshal the peer record: %v", err)
	}
	resp, err := sess.jsa.storeSessionMsg(domainTk, idHash, 0, body)
	if err != nil {
		t.Fatalf("land the competing record: %v", err)
	}
	// The competitor ADOPTS what it just wrote, exactly as a confirmed save would: the
	// sequence and the lineage together. Without this the peer is resident but not
	// acknowledged for the record that survived, so it could not perform its own next
	// save — which would make it a bystander rather than the current owner this test
	// claims it is.
	sess.mu.Lock()
	sess.seq = resp.Sequence
	sess.wid, sess.pwid = rec.WID, rec.PWID
	sess.mu.Unlock()
}

// TestMQTTForkedRecordE2EWithALiveCompetingOwnerPublishesTheWill is the RESIDUAL, asserted
// on the shape that actually produces it and at the only layer where a Will publication is
// observable.
//
// A proven sibling does not prove the survivor is an orphan. Here the competing broker is
// real, resident, and holds the surviving record — a genuine takeover, as far as anything
// durable can show — and the displaced client's Will is nonetheless PUBLISHED, because the
// fork verdict withholds the takeover claim and the connection ends through ordinary close
// bookkeeping instead of mqttDiscardWillOnTakeover.
//
// This asserts the residual, so it FAILS if the behavior is ever changed. That is
// deliberate: docs/CONFORMANCE.md states it and the plan approved it, and pinning it is
// what stops it from being an untested claim. A change that suppresses the Will here must
// update this test and that document together.
func TestMQTTForkedRecordE2EWithALiveCompetingOwnerPublishesTheWill(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	wc, wr := testMQTTConnect(t, &mqttConnInfo{clientID: "forklivewatch", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer wc.Close()
	testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, wc, wr, []*mqttFilter{{filter: "forklive/will", qos: 0}}, []byte{0})
	testMQTTFlush(t, wc, nil, wr)

	ci := &mqttV5ConnInfo{
		clientID: "forklive",
		will:     &mqttWill{topic: []byte("forklive/will"), message: []byte("bye")},
	}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNACK reason %x", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "forklive/gone"}}); codes[0] != 0 {
		t.Fatalf("SUBACK reason %x", codes[0])
	}

	asm := testMQTTGetGlobalASM(t, s)
	sess := testMQTTGetClient(t, s, "forklive").mqtt.sess
	if sess == nil {
		t.Fatal("the connection must be bound to a session")
	}
	idHash := sess.idHash
	asm.mu.Lock()
	asm.testSessPersistDrop = func(h string) bool { return h == idHash }
	asm.mu.Unlock()

	// 1. The competitor restores the record as it stands, so both brokers now hold the
	//    SAME acknowledged identity.
	peerSess := testE2ELiveCompetingOwner(t, s, "node2", "forklive", idHash)

	// 2. The local session writes again. Its new record is derived from the one the
	//    competitor holds — so the competitor's next write will be its SIBLING, which is
	//    the shape a leader transition produces and a working CAS cannot.
	if codes := testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{{topic: "forklive/second"}}); codes[0] != 0 {
		t.Fatalf("the second SUBACK must succeed: reason %x", codes[0])
	}

	// 3. The competitor's own write lands, discarding ours under MaxMsgsPer:1, and the
	//    competitor adopts it.
	testE2ELandCompetingWrite(t, peerSess, idHash)

	// The competitor is now what a genuine takeover looks like from the durable side:
	// resident, bound to its own client, and acknowledged for the record that survived.
	// Asserted BEFORE the local gate runs, so the Will assertion below is about that
	// state and not about whatever the teardown leaves.
	testAssertCurrentOwner(t, peerSess, idHash)

	if codes := testMQTTUnsubV5(t, c, r, 3, []string{"forklive/gone"}); codes[0] != mqttReasonImplementationSpecificError {
		t.Fatalf("the UNSUBSCRIBE's save must be rejected by the competing record: reason %x", codes[0])
	}

	testMQTTSendPublishPacket(t, c, 1, false, false, "forklive/topic", 3, []byte("nope"))
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketDisconnect {
		t.Fatalf("expected a server DISCONNECT, got %x (pl=%d)", pt, pl)
	}
	reason, err := r.readByte("disconnect reason")
	if err != nil {
		t.Fatalf("read the DISCONNECT reason: %v", err)
	}
	if reason != mqttReasonServerBusy {
		t.Fatalf("a sibling with a LIVE competing owner is still only unsettled, so 0x89: got %x", reason)
	}

	// The residual itself: the Will goes out. A demonstrated takeover would have
	// suppressed it, and this competitor is as live as anything durable can show.
	testMQTTCheckPubMsg(t, wc, wr, "forklive/will", 0, []byte("bye"))
}

// testAssertCurrentOwner pins that the competing broker really is the session's current
// owner: bound to a live client, resident in its manager, and acknowledged for the exact
// record that is now durable.
func testAssertCurrentOwner(t *testing.T, sess *mqttSession, idHash string) {
	t.Helper()
	sess.mu.Lock()
	bound, seq, wid := sess.c, sess.seq, sess.wid
	asm := sess.asm
	sess.mu.Unlock()
	if bound == nil {
		t.Fatal("the competing owner must be BOUND, or it is not a takeover")
	}
	if asm != nil {
		asm.mu.RLock()
		_, resident := asm.sessions[sess.id]
		asm.mu.RUnlock()
		if !resident {
			t.Fatal("the competing owner must still be resident on its own broker")
		}
	}
	smsg, err := sess.jsa.loadSessionMsg(sess.domainTk, idHash)
	if err != nil {
		t.Fatalf("load the surviving record: %v", err)
	}
	if smsg.Sequence != seq {
		t.Fatalf("the competitor must be acknowledged for the SURVIVING record: sess.seq=%d, record at %d",
			seq, smsg.Sequence)
	}
	ps := mqttDecodeSessionRecord(smsg)
	if ps == nil || ps.WID != wid {
		t.Fatalf("the competitor's acknowledged identity must be the surviving record's: sess.wid=%q record=%+v",
			wid, ps)
	}
}
