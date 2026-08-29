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
// child-SHAPED record through the same machinery. Both now get 0x89 — F-084 withdrew the
// takeover claim from every record-derived route, a child-shaped body proving no successor
// — so the DISCRIMINATOR moved: the two are separated by the Reason String and by whether a
// fork grant is minted, not by the reason code. A change that collapsed the two classes
// would still fail one of them.

// testE2ELandSibling stores a record CLAIMING the SAME parent as the bound session's
// acknowledged one, unconditionally — no per-subject expectation, so nothing excludes it.
// This is the retention route itself, not a stand-in for a leader transition: that
// alternative is closed for two bridge-authored conditional writes (the same-base
// boundary, docs/CONFORMANCE.md). See sess_fork_lineage_test.go.
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
	t.Parallel()
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
	// record that is not demonstrably a successor of ours.
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

	// Test 21: the acknowledged SUBSCRIBE absent from the survivor is REPAIRED, and the survivor's
	// own membership is kept beside it. The sibling carried "forked/e2e/marker" and no
	// "forkconnect/x", while the parent record had neither — so the survivor made no grant
	// decision about the client's filter and the change applies, while the peer's filter is
	// not in the delta at all and is left exactly as it is. See sess_fork_repair.go.
	sess2.mu.Lock()
	ps := sess2.buildPersistedSessionLocked()
	sess2.mu.Unlock()
	// Membership is keyed by the NATS subject the filter maps to, not by the MQTT filter.
	if _, ok := ps.Subs["forkconnect.x"]; !ok {
		t.Fatalf("the record the reconnect wrote must carry the SUBACKed filter again: %v", ps.Subs)
	}
	if _, ok := ps.Subs["forked/e2e/marker"]; !ok {
		t.Fatalf("the surviving record's own membership must be preserved: %v", ps.Subs)
	}
	// And it DELIVERS, which is the only assertion that proves the repair reached the
	// consumer rather than only the record: the pre-CONNACK reconciliation is what recreates
	// a durable for a re-asserted membership.
	testMQTTPublish(t, c2, r2, 1, false, false, "forkconnect/x", 1, []byte("repaired"))
	testMQTTCheckPubMsg(t, c2, r2, "forkconnect/x", mqttPubQos1, []byte("repaired"))
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
// ordering wrong yields a child-shaped body, which is unsettled-and-no-mint — the correct
// answer to a different question.
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
	// The record BYTES go with the pair, for the same reason: this stands in for a confirmed
	// save, and a fixture that moved the sequence and the lineage while leaving the retained
	// bytes unset would silently mint nothing on a later fork.
	sess.adoptGrantBodyLocked(body)
	sess.mu.Unlock()
}

// TestMQTTForkedRecordE2EWithALiveCompetingOwnerPublishesTheWill is the RESIDUAL, asserted
// on the shape that actually produces it and at the only layer where a Will publication is
// observable.
//
// A body claiming our parent does not prove the survivor is an orphan. Here the competing
// broker is real, resident, and holds the surviving record — the FIXTURE knows that, and
// the displaced client's Will is nonetheless PUBLISHED, because the fork verdict withholds
// the takeover claim and the connection ends through ordinary close bookkeeping instead of
// mqttDiscardWillOnTakeover.
//
// That the competitor is genuine here is precisely what makes this the residual rather
// than a defect: nothing durable distinguishes this from a replay or a stranger, so the
// broker must answer the same way in all three, and the price is a Will published where a
// provable takeover would have suppressed it (F-084).
//
// This asserts the residual, so it FAILS if the behavior is ever changed. That is
// deliberate: docs/CONFORMANCE.md states it and the plan approved it, and pinning it is
// what stops it from being an untested claim. A change that suppresses the Will here must
// update this test and that document together.
func TestMQTTForkedRecordE2EWithALiveCompetingOwnerPublishesTheWill(t *testing.T) {
	t.Parallel()
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
	//    competitor holds — so the competitor's next write will CLAIM the same parent,
	//    which is the shape an unconditional write produces and a CAS cannot.
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
		t.Fatalf("a sibling is only ever unsettled, live competitor or not, so 0x89: got %x", reason)
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

// ---------------------------------------------------------------------------
// T9: the finding itself, over real connections
// ---------------------------------------------------------------------------

// TestMQTTCreateForkE2EAnswersUnsettledToTheDisplacedClient is T9 after the write-lineage
// audit, and it pins the RESIDUAL on the wire rather than a capability.
//
// The bound session's record here is a PRODUCTION create whose ONLY save is the accept
// save: the client's previous clean-start close deleted its record, and the reconnect's
// create used to inherit an identity from the retirement watermark. That inheritance is
// gone — and so is the field it read, removed with the vanished-record item — so
// the create is parentless and a sibling of it can no longer be proven.
//
// The RESIDUAL is that unprovability. What the displaced client is TOLD is no longer part
// of it: F-084 replaced the old 0x8E "session taken over" — a claim that somebody
// legitimately owns the session, when the two creates arbitrated nothing — with 0x89
// "ownership unsettled", which is what the evidence actually supports.
//
// Asserting it end to end is deliberate. The residual is stated in docs/CONFORMANCE.md,
// and a residual nothing exercises is a sentence rather than a fact; this is also the
// test that would fail first if a future change restored an inheritance without saying so.
//
// No Will assertion here, unlike the rejection-settlement e2e above, and the reason is no
// longer a residual: this verdict is reached by the eviction route, whose Will behaviour
// F-084 fixed and whose fixtures live with it
// (TestF084RemotePersistEvictionDischargesWill for a live Will,
// TestF084RemotePersistEvictionEnqueuesDelayedWill for a delayed one, and
// TestF084RemotePersistEvictionDischargesParkedAction for a parked action). Asserting
// it again here would duplicate them without discriminating anything this test is for,
// which is the INHERITANCE the displaced client is answered about.
//
// The verdict is reached through the persist observation rather than the rejection
// settlement, because the latter needs a record write to reject and any such write would
// itself give the record a parent.
func TestMQTTCreateForkE2EAnswersUnsettledToTheDisplacedClient(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A clean-start session whose close DISCARDS its record: that teardown writes the
	// retirement watermark before deleting anything.
	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: "createe2e", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c0, r0, []*mqttFilter{{filter: "createe2e/gone", qos: 1}}, []byte{1})
	testMQTTFlush(t, c0, nil, r0)
	testMQTTDisconnect(t, c0, nil)
	c0.Close()

	asm := testMQTTGetGlobalASM(t, s)
	idHash := getHash("createe2e")
	testMQTTWaitForRecordAbsent(t, asm, idHash)
	if _, _, st := asm.jsa.loadGenRetirement(asm.domainTk, idHash); st != mqttRetirementValid {
		t.Fatalf("the clean-start close must record a valid retirement watermark: status %v", st)
	}

	// The reconnect's record is a CREATE, and stays the only save. The watermark is
	// asserted VALID above and nothing more: it remains a generation-retirement fact, but
	// it no longer carries any record identity for a create to inherit — the reader was
	// removed by the write-lineage audit and the field itself was removed later. So this
	// fixture proves the create is the only save, not that an available identity was
	// deliberately ignored; that property is measured where an identity still exists, in
	// sess_write_lineage_audit_test.go's marker and clear() arms.
	ci := &mqttV5ConnInfo{
		clientID: "createe2e",
		will:     &mqttWill{topic: []byte("createe2e/will"), message: []byte("bye")},
	}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNACK reason %x", reason)
	}
	sess := testMQTTGetClient(t, s, "createe2e").mqtt.sess
	if sess == nil {
		t.Fatal("the connection must be bound to a session")
	}
	sess.mu.Lock()
	ackWID, ackPWID := sess.wid, sess.pwid
	sess.mu.Unlock()
	if ackWID == _EMPTY_ {
		t.Fatal("the accept save must be acknowledged")
	}
	if ackPWID != _EMPTY_ {
		t.Fatalf("the create must inherit NOTHING: with no second save, only an inheritance "+
			"could give this record a parent, and there is none left to draw on. Got %q", ackPWID)
	}

	// A sibling of the create lands, written as ANOTHER broker's record: the session-record
	// watch filters this broker's own writes out of the eviction route on Origin, so a
	// locally-authored sibling would never spawn the worker under test.
	sess.mu.Lock()
	sibling := sess.buildPersistedSessionLocked()
	sibling.Gen, sibling.Origin = sess.gen, "othernode"
	// The sibling carries the same (empty) parent, which is the real shape: two creates
	// into one emptied subject, each descended from nothing. That is exactly what arm 2's
	// ackPWID conjunct refuses to call sibling-hood, since two unrelated first creates
	// look identical to it.
	sibling.WID, sibling.PWID = mqttNewRecordWID(), ackPWID
	sibling.Subs = map[string]byte{"createe2e/sibling": 1}
	domainTk := sess.domainTk
	sess.mu.Unlock()
	body, merr := json.Marshal(&sibling)
	if merr != nil {
		t.Fatalf("marshal the sibling: %v", merr)
	}
	if _, serr := sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); serr != nil {
		t.Fatalf("land the sibling record: %v", serr)
	}

	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketDisconnect {
		t.Fatalf("the displaced connection must get a server DISCONNECT, got %x (pl=%d)", pt, pl)
	}
	reason, err := r.readByte("disconnect reason")
	if err != nil {
		t.Fatalf("read the DISCONNECT reason: %v", err)
	}
	// 0x89 ownership-unsettled. The write-lineage audit made a parentless create concede
	// here, because two unrelated first creates are indistinguishable and calling them
	// siblings would manufacture a FALSE fork — that part is unchanged and is still the
	// residual. What changed is what conceding SAYS: F-084 stopped it claiming a takeover,
	// since failing to recognise sibling-hood leaves this broker knowing LESS about who
	// holds the session, and less knowledge cannot license a stronger claim.
	//
	// This is now strictly closer to the goal than the 0x8E it replaced: the client is
	// invited to reconnect rather than told to stop, which is the truthful instruction on
	// evidence that established no successor.
	if reason != mqttReasonServerBusy {
		t.Fatalf("a create whose sibling erased it answers 0x89 'ownership unsettled': "+
			"nothing establishes the two as siblings, and nothing establishes a successor "+
			"either. Got %x", reason)
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
}

// testMQTTWaitForRecordAbsent waits for a client's session record to be gone, which is what
// a clean-start close leaves behind once its teardown completes.
func testMQTTWaitForRecordAbsent(t *testing.T, asm *mqttAccountSessionManager, idHash string) {
	t.Helper()
	deadline := time.Now().Add(testMQTTTimeout)
	for time.Now().Before(deadline) {
		if _, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash); err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				t.Fatalf("loadSessionMsg: %v", err)
			}
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("the clean-start close never deleted the session record")
}
