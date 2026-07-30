//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestMQTTPubRelStoreFailureUnwedgesIdentifier is test 7 end to end: a PUBREL
// store that genuinely fails must leave the exchange restartable, not wedged.
//
// Regression: the PUBREC path advances the in-memory state before the store, so a
// failed store left the identifier in pendingPubRel forever (nothing will ever
// fill its ack subject) and the delivery's PUBLISH tracking gone — the message
// could then be neither redelivered nor acknowledged, and the identifier leaked
// for the life of the session.
func TestMQTTPubRelStoreFailureUnwedgesIdentifier(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// PERSISTENT: the session (and its in-flight tracking) must survive the
	// connection drop the failed PUBREL store causes.
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "wedge"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "wedge/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "wedgepub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	// Publish at QoS 2, so the onward delivery is QoS 2 (min of publish and
	// subscribe) and the subscriber's PUBREC drives the path under test.
	testMQTTSendPublishPacket(t, pc, 2, false, false, "wedge/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "wedge/topic", mqttPubQoS2, []byte("payload"))

	// Fail the PUBREL store, WITHOUT letting it happen: nothing is stored and
	// nothing is delivered, which is the case the rollback exists for.
	var failed atomic.Bool
	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			failed.Store(true)
			return errors.New("simulated PUBREL store failure")
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)

	// The connection drops, as it does on any PUBREL store failure.
	testMQTTExpectDisconnect(t, c)
	restore()
	if !failed.Load() {
		t.Fatal("the PUBREL store hook never fired; the test proved nothing")
	}

	sess := testMQTTResidentSession(t, s, "wedge")
	sess.mu.Lock()
	_, wedged := sess.pendingPubRel[pi]
	pub, tracked := sess.pendingPublish[pi]
	sess.mu.Unlock()

	if wedged {
		t.Error("the packet identifier is wedged in pendingPubRel with no ack subject; nothing will ever free it")
	}
	if !tracked {
		t.Fatal("the delivery's PUBLISH tracking was dropped, so it can neither be redelivered nor acked")
	}
	if pub.jsAckSubject == _EMPTY_ {
		t.Error("the reinstated PUBLISH entry lost its JetStream ack subject")
	}

	// And the exchange really does restart: a reconnect redelivers under the SAME
	// identifier and completes.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "wedge"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	pi2 := testMQTTCheckPubMsgNoAck(t, c2, r2, "wedge/topic", mqttPubQoS2|mqttPubFlagDup, []byte("payload"))
	if pi2 != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", pi2, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, c2, pi2)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi2)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi2)
	testMQTTFlush(t, c2, nil, r2)
}

// TestMQTTPubRelAmbiguousStoreKeepsDeliveredExchange is test 8: a PUBREL store
// that LANDED and was already DELIVERED, but whose response the caller never saw,
// must NOT be rolled back.
//
// This needs the post-store seam: the pre-store hook models a store that never
// happened, which is the opposite scenario. Rolling back here would delete the
// callback's real pendingPubRel entry — leaking the identifier and leaving a
// delivered PUBREL that no PUBCOMP can acknowledge — and reinstate stale PUBLISH
// tracking for an exchange already past PUBREC.
func TestMQTTPubRelAmbiguousStoreKeepsDeliveredExchange(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// PERSISTENT: the session (and its in-flight tracking) must survive the
	// connection drop the failed PUBREL store causes.
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "ambig"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "ambig/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "ambigpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	// Publish at QoS 2, so the onward delivery is QoS 2 (min of publish and
	// subscribe) and the subscriber's PUBREC drives the path under test.
	testMQTTSendPublishPacket(t, pc, 2, false, false, "ambig/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "ambig/topic", mqttPubQoS2, []byte("payload"))

	// Let the store happen — so the PUBREL is real and gets delivered — and only
	// then replace the caller's result with an error.
	var suppressed atomic.Bool
	restore := setMQTTTestStoreMsgResultHook(func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil && strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			suppressed.Store(true)
			// Give the delivery callback a moment to run, so this really is the
			// "landed AND delivered, response lost" case rather than a race.
			time.Sleep(100 * time.Millisecond)
			return nil, errors.New("simulated lost PUBREL store response")
		}
		return resp, err
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)

	// The PUBREL really is delivered — that is what makes this the ambiguous case
	// rather than a plain failure, and it is what fills the callback's
	// pendingPubRel entry with a real ack subject. Deliberately NOT answered with
	// PUBCOMP, so the record stays pending and can be shown to replay below.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	// Only then does the suppressed error reach the readLoop and close the
	// connection, as any PUBREL store failure does.
	testMQTTExpectDisconnect(t, c)
	restore()
	if !suppressed.Load() {
		t.Fatal("the post-store hook never fired; the test proved nothing")
	}

	sess := testMQTTResidentSession(t, s, "ambig")
	sess.mu.Lock()
	rel, hasRel := sess.pendingPubRel[pi]
	_, backAsPublish := sess.pendingPublish[pi]
	sess.mu.Unlock()

	if !hasRel {
		t.Fatal("the delivered PUBREL's tracking was rolled back; its identifier leaks and PUBCOMP can never ack it")
	}
	if rel.jsAckSubject == _EMPTY_ {
		t.Error("the delivered PUBREL lost its ack subject, so PUBCOMP cannot ack the record")
	}
	if backAsPublish {
		t.Error("stale PUBLISH tracking was reinstated for an exchange already past PUBREC")
	}

	// The exchange completes on reconnect: the PUBREL replays, not the payload.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "ambig"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)
}

// testMQTTResidentSession returns the broker's in-memory session for a client ID,
// which is where the PUBLISH/PUBREL tracking these tests assert on lives.
func testMQTTResidentSession(t testing.TB, s *Server, clientID string) *mqttSession {
	t.Helper()
	asm := testMQTTGetGlobalASM(t, s)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for client %q; it should have survived", clientID)
	}
	return sess
}

// TestMQTTPubRelConsumerIsVerifiedOnResume is test 9: a resumed session must
// PROVE its PUBREL durable exists rather than trust the config restored from its
// record.
//
// Regression: ensurePubRelConsumerSubscription returned as soon as pubRelConsumer
// was non-nil. The durable may have been deleted out of band, or reaped by the
// very InactiveThreshold it sets — and because $MQTT_out is interest-retained, the
// session then stored PUBRELs that were silently dropped and never delivered a
// PUBREL again, for the life of that session.
func TestMQTTPubRelConsumerIsVerifiedOnResume(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "verify"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "verify/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, "verify")
	sess.mu.Lock()
	pubRelDur := _EMPTY_
	if sess.pubRelConsumer != nil {
		pubRelDur = sess.pubRelConsumer.Durable
	}
	sess.mu.Unlock()
	if pubRelDur == _EMPTY_ {
		t.Fatal("a QoS 2 subscribe should have created the PUBREL consumer")
	}

	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Delete the durable out of band, exactly as an operator or an inactivity
	// threshold would. The session record still names it.
	if _, err := sess.jsa.deleteConsumer(mqttOutStreamName, pubRelDur, true); err != nil {
		t.Fatalf("delete the PUBREL consumer out of band: %v", err)
	}
	// The delete is confirmed, but a lookup issued immediately after it can still
	// be served the pre-delete view, so poll rather than assert once.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if ci, err := sess.jsa.lookupConsumer(mqttOutStreamName, pubRelDur); err == nil && ci != nil {
			return errors.New("the PUBREL consumer is still present")
		}
		return nil
	})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "verify"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "verify/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c2, nil, r2)

	if ci, err := sess.jsa.lookupConsumer(mqttOutStreamName, pubRelDur); err != nil || ci == nil {
		t.Fatalf("the PUBREL consumer was not recreated on resume (err=%v); every PUBREL this session stores would be silently dropped", err)
	}

	// And a PUBREL stored after the resume is actually DELIVERED — which is the
	// property that matters, and the one interest retention would otherwise break.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "verifypub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, pc, 2, false, false, "verify/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	pi := testMQTTCheckPubMsgNoAck(t, c2, r2, "verify/topic", mqttPubQoS2, []byte("payload"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c2, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)
}

// TestMQTTStoredPubRelIsCorrelated is the non-drain half of test 16(c): an
// ordinary QoS 2 exchange still works exactly as before, and the PUBREL it stores
// now NAMES the source delivery it completes.
//
// The correlation is what a drain will later use to remove a source claim and to
// avoid resending the payload of an exchange already past PUBREC. Asserting it on
// the ordinary path first means the header plumbing is proven independently of
// the drain that consumes it.
func TestMQTTStoredPubRelIsCorrelated(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "corr"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "corr/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	// Capture what is actually written to the PUBREL subject.
	var gotHdr []byte
	var gotSubject string
	restore := setMQTTTestStoreMsgHook(func(subject string, hdrLen int, msg []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) && gotHdr == nil {
			gotSubject = subject
			gotHdr = append([]byte(nil), msg[:hdrLen]...)
		}
		return nil
	})
	defer restore()

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "corrpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, pc, 2, false, false, "corr/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	// The whole ordinary exchange still completes.
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "corr/topic", mqttPubQoS2, []byte("payload"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	testMQTTFlush(t, c, nil, r)

	if gotHdr == nil {
		t.Fatal("no PUBREL was stored; the test proved nothing")
	}
	sess := testMQTTResidentSession(t, s, "corr")
	sess.mu.Lock()
	wantSubject := sess.pubRelSubject
	sess.mu.Unlock()
	if gotSubject != wantSubject {
		t.Errorf("PUBREL stored on %q, want this incarnation's subject %q", gotSubject, wantSubject)
	}
	if got := mqttParsePubRelNATSHeader(gotHdr); got != pi {
		t.Errorf("stored PUBREL identifier = %v, want %v", got, pi)
	}
	corr := mqttParsePubRelCorrelation(gotHdr)
	if !corr.Ok {
		t.Fatalf("the stored PUBREL does not name its source delivery: %q", gotHdr)
	}
	if corr.SrcDur == _EMPTY_ || corr.SrcSeq == 0 {
		t.Errorf("correlation = (%q, %v), want a real durable and sequence", corr.SrcDur, corr.SrcSeq)
	}
	// No deduplication identity on this path: the only legitimate re-store of the
	// same (durable, sequence) is the post-restart one after a redelivery, and a
	// surviving dedupe entry would silently swallow it.
	if id := getHeader(JSMsgId, gotHdr); len(id) != 0 {
		t.Errorf("the ordinary PUBREL path must not carry a deduplication identity, got %q", id)
	}
}

// TestMQTTStoredPubRelIsDeliveredWithoutSubscriptionOrDrain pins that a resumed
// session re-establishes PUBREL delivery on the strength of the stored PUBREL
// alone — with no subscription and no drain to speak for it.
//
// The broker is RESTARTED, which is what makes this a real test: pendingPubRel is
// memory-only, so a same-process reconnect reuses the session object and its
// leftover entry drives ensurePubRelConsumerSubscription through
// resendPendingRedelivery. Across a restart there is no such entry, re-SUBSCRIBE
// is the only other thing that would establish the subscription, and this session
// does not re-subscribe. $MQTT_out is interest-retained, so without it the stored
// PUBREL is never delivered and the client holds a packet identifier it can never
// complete.
//
// Regression: the resume path asked "does this session hold a drain?" before
// making the PUBREL path ready. A session carrying only PUBRELs answered no.
func TestMQTTStoredPubRelIsDeliveredWithoutSubscriptionOrDrain(t *testing.T) {
	o := testMQTTDefaultOptions()
	// The stored PUBREL is redelivered by AckWait after the resume, so keep the
	// window short enough to test in.
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "relonly"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "relonly/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "relonlypub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, pc, 2, false, false, "relonly/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	// PUBREC but never PUBCOMP: a PUBREL is now stored and unacked.
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "relonly/topic", mqttPubQoS2, []byte("payload"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)

	// UNSUBSCRIBE leaves nothing started — the source was acked when the PUBREL was
	// stored — so no drain is created and the session resumes with neither.
	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: "relonly/topic"}})
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, "relonly")
	sess.mu.Lock()
	drains, hasPubRelConsumer := len(sess.drains), sess.pubRelConsumer != nil
	sess.mu.Unlock()
	if drains != 0 {
		t.Fatalf("this test needs a session with no drain at all, got %d", drains)
	}
	if !hasPubRelConsumer {
		t.Fatal("this test needs a session whose record still names a PUBREL consumer")
	}

	// Restart, so nothing in memory can stand in for the record. A same-process
	// reconnect would reuse the session object — pendingPubRel included — and
	// establish the subscription through resendPendingRedelivery instead.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()
	pc.Close()
	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "relonly"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// No re-SUBSCRIBE: the resume path alone must have made PUBREL delivery ready.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)
}
