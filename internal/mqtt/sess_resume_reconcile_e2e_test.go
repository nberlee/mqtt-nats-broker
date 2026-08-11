//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// End-to-end tests for the resume reconciliation phase
// (sess_resume_reconcile.go): plain QoS 1/2 durables are verified — or
// recreated under a fresh identity — BEFORE the CONNACK, instead of being
// trusted verbatim from the session record.
//
// Every delivery assertion here is QoS 1/2 WITH a packet identifier: a bare
// "publish arrives" can be served by the raw QoS 0 delivery path and would not
// discriminate a dead JetStream consumer at all.

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// testReconcileSubscribe connects a persistent 3.1.1 session and subscribes one
// filter at the given QoS.
func testReconcileSubscribe(t testing.TB, o *Options, cid, filter string, qos byte) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: qos}}, []byte{qos})
	testMQTTFlush(t, c, nil, r)
	return c, r
}

// testReconcileConsDur returns the durable the resident session currently names
// for sid.
//
// Callers asserting on a just-completed CONNECT must fence first
// (testMQTTFlush): subscription restoration runs AFTER the CONNACK, so reading
// the session the instant the CONNACK lands samples a resume still in progress.
func testReconcileConsDur(t testing.TB, s *Server, cid, sid string) string {
	t.Helper()
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	cc := sess.cons[sid]
	if cc == nil {
		t.Fatalf("session %q has no consumer entry for sid %q", cid, sid)
	}
	return cc.Durable
}

// testReconcileConsumerPresent reports whether the durable exists on the
// messages stream, failing the test on an indeterminate lookup.
func testReconcileConsumerPresent(t testing.TB, asm *mqttAccountSessionManager, dur string) bool {
	t.Helper()
	ci, err := asm.jsa.lookupConsumer(mqttStreamName, dur)
	if err != nil {
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			t.Fatalf("lookupConsumer %q: %v", dur, err)
		}
		return false
	}
	return ci != nil
}

// testReconcileDeleteConsumer deletes a durable out of band — the observable
// end state of an inactivity reap.
func testReconcileDeleteConsumer(t testing.TB, asm *mqttAccountSessionManager, dur string) {
	t.Helper()
	if _, err := asm.jsa.deleteConsumer(mqttStreamName, dur, true); err != nil {
		t.Fatalf("deleteConsumer %q: %v", dur, err)
	}
}

// testReconcileRecord reads the persisted session record back.
func testReconcileRecord(t testing.TB, asm *mqttAccountSessionManager, cid string) (*mqttPersistedSession, uint64) {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid))
	if err != nil {
		t.Fatalf("loadSessionMsg for %q: %v", cid, err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("unmarshal session record: %v", err)
	}
	return ps, smsg.Sequence
}

// testReconcileQoS1Publish publishes one QoS 1 message from a throwaway
// publisher connection.
func testReconcileQoS1Publish(t testing.TB, o *Options, topic string, payload []byte) {
	t.Helper()
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "reconcile-pub-" + topic, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, topic, 1, payload)
	testMQTTDisconnectEx(t, pc, nil, false)
}

// testReconcileExpectQoS1 asserts exactly one QoS 1 delivery with a packet
// identifier, acknowledges it, and asserts nothing further arrives.
func testReconcileExpectQoS1(t testing.TB, c net.Conn, r *mqttReader, topic string, payload []byte) uint16 {
	t.Helper()
	pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQos1, payload)
	if pi == 0 {
		t.Fatalf("QoS 1 delivery of %q carried no packet identifier", topic)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTFlush(t, c, nil, r)
	testMQTTExpectNothing(t, r)
	return pi
}

// testReconcileRefusedConnect sends a persistent-session CONNECT for cid and
// asserts the broker REFUSES it with no packet at all: the connection closes
// without a CONNACK and without a DISCONNECT (the pre-CONNACK abort
// discipline). testMQTTConnect cannot be used here — it fatals when no CONNACK
// arrives, which is exactly the outcome under test.
func testReconcileRefusedConnect(t testing.TB, o *Options, cid string) {
	t.Helper()
	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, err := testMQTTWrite(c, mqttCreateConnectProto(&mqttConnInfo{clientID: cid, cleanSess: false})); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	testMQTTExpectDisconnect(t, c)
}

// testReconcileRestart restarts the broker on the same store, so the next
// CONNECT resumes through the restore-from-record path.
func testReconcileRestart(t *testing.T, s *Server, o *Options) *Server {
	t.Helper()
	s.Shutdown()
	s = testMQTTRestartServerSameStore(t, s, o)
	if err := s.Start(); err != nil {
		t.Fatalf("restart the broker: %v", err)
	}
	return s
}

// ---------------------------------------------------------------------------
// T1 — same-broker resume after a reap
// ---------------------------------------------------------------------------

// The core fix, resident-session variant: a durable deleted while the client
// was away (the observable end state of an inactivity reap) is repaired at the
// next CONNECT under a FRESH identity, and QoS 1/2 delivery works again.
//
// Mutation checks: M1 (skip the phase — the delivery assertion fails: the
// restored subscription points at a dead deliver subject) and M2 (recreate
// under the recorded name — the fresh-name assertions fail).
func TestMQTTReconcileResumeAfterReap(t *testing.T) {
	t.Parallel()
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			cid := fmt.Sprintf("reap-q%d", qos)
			topic := fmt.Sprintf("reap/q%d", qos)
			// The session maps are keyed by the NATS-form subject.
			sid := fmt.Sprintf("reap.q%d", qos)
			c, _ := testReconcileSubscribe(t, o, cid, topic, qos)
			defer c.Close()
			oldDur := testReconcileConsDur(t, s, cid, sid)
			c.Close()

			asm := testMQTTGetGlobalASM(t, s)
			testReconcileDeleteConsumer(t, asm, oldDur)

			c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
			// Fence: subscription restoration runs after the CONNACK.
			testMQTTFlush(t, c2, nil, r2)

			// The repair minted a fresh identity: new name in memory AND in the
			// record, the new durable exists, the old name stays absent forever.
			newDur := testReconcileConsDur(t, s, cid, sid)
			if newDur == oldDur {
				t.Fatalf("the replacement reused the reaped durable name %q", oldDur)
			}
			if !testReconcileConsumerPresent(t, asm, newDur) {
				t.Fatalf("replacement durable %q does not exist", newDur)
			}
			if testReconcileConsumerPresent(t, asm, oldDur) {
				t.Fatalf("the reaped durable %q was recreated", oldDur)
			}
			ps, _ := testReconcileRecord(t, asm, cid)
			if cc := ps.Cons[sid]; cc == nil || cc.Durable != newDur {
				t.Fatalf("the record names %+v for %q, want durable %q", cc, sid, newDur)
			}

			// And the subscription is ALIVE again: a QoS message arrives exactly
			// once, with a packet identifier.
			pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "reap-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer pc.Close()
			testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, pc, pr, qos, false, false, topic, 1, []byte("after-repair"))

			expFlags := mqttPubQos1
			if qos == 2 {
				expFlags = mqttPubQoS2
			}
			pi := testMQTTCheckPubMsgNoAck(t, c2, r2, topic, expFlags, []byte("after-repair"))
			if pi == 0 {
				t.Fatal("delivery carried no packet identifier")
			}
			if qos == 1 {
				testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
			} else {
				testMQTTSendPIPacket(mqttPacketPubRec, t, c2, pi)
				testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
				testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
			}
			testMQTTFlush(t, c2, nil, r2)
			testMQTTExpectNothing(t, r2)
		})
	}
}

// ---------------------------------------------------------------------------
// T2 — record-restore resume after a reap
// ---------------------------------------------------------------------------

// The same repair through the restore-from-record path (a broker restart), and
// the record READBACK names the new durable — which is what pins the repair
// save (M3: skip the repair save and the readback still names the reaped one).
func TestMQTTReconcileRecordRestoreAfterReap(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "reap-restore", "reap/restore", "reap.restore"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()

	testReconcileDeleteConsumer(t, testMQTTGetGlobalASM(t, s), oldDur)

	s = testReconcileRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	asm := testMQTTGetGlobalASM(t, s)
	newDur := testReconcileConsDur(t, s, cid, sid)
	if newDur == oldDur {
		t.Fatalf("the replacement reused the reaped durable name %q", oldDur)
	}
	ps, _ := testReconcileRecord(t, asm, cid)
	if cc := ps.Cons[sid]; cc == nil || cc.Durable != newDur {
		t.Fatalf("the record names %+v for %q, want durable %q — the repair save never landed", cc, sid, newDur)
	}

	testReconcileQoS1Publish(t, o, topic, []byte("restored"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("restored"))
}

// ---------------------------------------------------------------------------
// T3 — the level-up companion of a multi-level filter
// ---------------------------------------------------------------------------

// A "foo/#" filter carries an independent level-up companion consumer for the
// exact "foo" subject. Reap ONLY the companion: the phase must collect both
// sids (M8: collect only the main sid and the exact-subject delivery stays
// dead).
func TestMQTTReconcileMultiLevelCompanion(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "reap-fwc"
	c, _ := testReconcileSubscribe(t, o, cid, "foo/#", 1)
	// The NATS-form sid of the filter is "foo.>", its companion "foo fwc".
	mainSid, fwcSid := "foo.>", "foo"+mqttMultiLevelSidSuffix
	mainDur := testReconcileConsDur(t, s, cid, mainSid)
	fwcDur := testReconcileConsDur(t, s, cid, fwcSid)
	c.Close()

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, fwcDur)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	// Only the companion was repaired; the main consumer was verified in place.
	if got := testReconcileConsDur(t, s, cid, mainSid); got != mainDur {
		t.Fatalf("the intact main consumer was replaced: %q -> %q", mainDur, got)
	}
	newFwcDur := testReconcileConsDur(t, s, cid, fwcSid)
	if newFwcDur == fwcDur {
		t.Fatalf("the companion replacement reused the reaped durable name %q", fwcDur)
	}
	ps, _ := testReconcileRecord(t, asm, cid)
	if cc := ps.Cons[fwcSid]; cc == nil || cc.Durable != newFwcDur {
		t.Fatalf("the record names %+v for the companion, want durable %q", cc, newFwcDur)
	}

	// A publish to the EXACT subject "foo" is the companion's delivery.
	testReconcileQoS1Publish(t, o, "foo", []byte("exact"))
	testReconcileExpectQoS1(t, c2, r2, "foo", []byte("exact"))
}

// ---------------------------------------------------------------------------
// T4 — charge accounting across the repair
// ---------------------------------------------------------------------------

// Exact consCharged keys and tmaxack transitions across verify-absent ->
// refund-old -> charge-new -> UNSUBSCRIBE refund, in both the resident-session
// variant (the stale charge exists and must be retired: M5) and the
// record-restored variant (no charge existed and none may be invented), with
// the replacement charged exactly once (M4). Non-default MaxAckPending, so an
// assertion cannot be satisfied by a default.
func TestMQTTReconcileChargeAccounting(t *testing.T) {
	t.Parallel()
	const cap = 111

	assertCharges := func(t *testing.T, s *Server, cid string, wantDur string, wantAmt int) {
		t.Helper()
		sess := testMQTTResidentSession(t, s, cid)
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if len(sess.consCharged) != 1 {
			t.Fatalf("consCharged = %v, want exactly {%q: %d}", sess.consCharged, wantDur, wantAmt)
		}
		if got := sess.consCharged[wantDur]; got != wantAmt {
			t.Fatalf("consCharged[%q] = %d, want %d (have %v)", wantDur, got, wantAmt, sess.consCharged)
		}
		if sess.tmaxack != wantAmt {
			t.Fatalf("tmaxack = %d, want %d", sess.tmaxack, wantAmt)
		}
	}

	t.Run("resident session retires the stale charge", func(t *testing.T) {
		o := testMQTTDefaultOptions()
		o.MQTT.MaxAckPending = cap
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		const cid, topic, sid = "charge-resident", "charge/resident", "charge.resident"
		c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
		oldDur := testReconcileConsDur(t, s, cid, sid)
		assertCharges(t, s, cid, oldDur, cap)
		c.Close()

		testReconcileDeleteConsumer(t, testMQTTGetGlobalASM(t, s), oldDur)

		c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
		testMQTTFlush(t, c2, nil, r2)

		// The stale charge is gone WITH its key; the replacement holds exactly
		// one charge. A skipped refund (M5) doubles tmaxack; a skipped charge
		// (M4) leaves the map empty and tmaxack zero.
		newDur := testReconcileConsDur(t, s, cid, sid)
		assertCharges(t, s, cid, newDur, cap)

		// UNSUBSCRIBE refunds the replacement exactly once.
		testMQTTUnsub(t, 2, c2, r2, []*mqttFilter{{filter: topic}})
		sess := testMQTTResidentSession(t, s, cid)
		sess.mu.Lock()
		charged, tmaxack := len(sess.consCharged), sess.tmaxack
		sess.mu.Unlock()
		if charged != 0 || tmaxack != 0 {
			t.Fatalf("after UNSUBSCRIBE: consCharged has %d entries, tmaxack = %d; want 0/0", charged, tmaxack)
		}
	})

	t.Run("record-restored session was never charged", func(t *testing.T) {
		o := testMQTTDefaultOptions()
		o.MQTT.MaxAckPending = cap
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		const cid, topic, sid = "charge-restored", "charge/restored", "charge.restored"
		c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
		oldDur := testReconcileConsDur(t, s, cid, sid)
		c.Close()
		testReconcileDeleteConsumer(t, testMQTTGetGlobalASM(t, s), oldDur)

		s = testReconcileRestart(t, s, o)
		defer testMQTTShutdownServer(s)

		c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
		testMQTTFlush(t, c2, nil, r2)

		// No stale charge existed (restored consumers are never charged); the
		// repair charged exactly the replacement.
		newDur := testReconcileConsDur(t, s, cid, sid)
		assertCharges(t, s, cid, newDur, cap)
	})

	t.Run("a failed repair refunds the replacement exactly once", func(t *testing.T) {
		o := testMQTTDefaultOptions()
		o.MQTT.MaxAckPending = cap
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		const cid, topic, sid = "charge-undo", "charge/undo", "charge.undo"
		c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
		oldDur := testReconcileConsDur(t, s, cid, sid)
		c.Close()

		asm := testMQTTGetGlobalASM(t, s)
		testReconcileDeleteConsumer(t, asm, oldDur)

		// Reject the replacement create through the repair-operations seam: the
		// same failure surface as a JetStream rejection, without a JetStream
		// outage bleeding into the rest of the test.
		asm.mu.Lock()
		asm.testResumeReconcilePreCreate = func(sess *mqttSession, deliver string) error {
			return errors.New("simulated consumer create rejection")
		}
		asm.mu.Unlock()
		defer func() {
			asm.mu.Lock()
			asm.testResumeReconcilePreCreate = nil
			asm.mu.Unlock()
		}()

		// The CONNECT is REFUSED: closed with no packet at all.
		testReconcileRefusedConnect(t, o, cid)

		// The undo restored the old (absent) entry UNCHARGED and permanently
		// retired the stale charge: no consumer key is charged and tmaxack is 0 —
		// a double refund would have gone negative and been clamped, so assert
		// the exact map too.
		sess := testMQTTResidentSession(t, s, cid)
		sess.mu.Lock()
		cc := sess.cons[sid]
		charged := len(sess.consCharged)
		tmaxack := sess.tmaxack
		sess.mu.Unlock()
		if cc == nil || cc.Durable != oldDur {
			t.Fatalf("cons[%q] = %+v, want the original (absent) durable %q restored", sid, cc, oldDur)
		}
		if charged != 0 || tmaxack != 0 {
			t.Fatalf("after the undo: consCharged has %d entries, tmaxack = %d; want 0/0", charged, tmaxack)
		}

		// And nothing the phase created survived it.
		ps, _ := testReconcileRecord(t, asm, cid)
		if got := ps.Cons[sid]; got == nil || got.Durable != oldDur {
			t.Fatalf("the record names %+v, want the pre-repair durable %q (nothing may have persisted)", got, oldDur)
		}

		// The next (uninjected) CONNECT repairs and delivers.
		asm.mu.Lock()
		asm.testResumeReconcilePreCreate = nil
		asm.mu.Unlock()
		c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		defer c3.Close()
		testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
		testReconcileQoS1Publish(t, o, topic, []byte("repaired"))
		testReconcileExpectQoS1(t, c3, r3, topic, []byte("repaired"))
	})
}

// ---------------------------------------------------------------------------
// T14 — absent identity: a QoS 1/2 filter with no Cons entry at all
// ---------------------------------------------------------------------------

// A record-restored session whose Subs carries a QoS 1 filter with NO matching
// Cons entry: the phase mints the consumer pre-CONNACK and the repair save
// persists it. Without the minting (M15), the identity created after the
// CONNACK is never persisted — the restore path runs no final subscription
// save — and the next resume starts from nothing again.
func TestMQTTReconcileAbsentIdentityMinted(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "absent-id", "absent/id", "absent.id"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()

	// Doctor the record: keep the subscription, drop its consumer identity (and
	// the consumer itself, so nothing dangles).
	asm := testMQTTGetGlobalASM(t, s)
	ps, _ := testReconcileRecord(t, asm, cid)
	delete(ps.Cons, sid)
	b, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshal doctored record: %v", err)
	}
	if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(cid), 0, b); err != nil {
		t.Fatalf("store doctored record: %v", err)
	}
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Restart so the resume is a pure record restore of the doctored state.
	s = testReconcileRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	// Minted pre-CONNACK and persisted: the readback names it.
	asm = testMQTTGetGlobalASM(t, s)
	mintedDur := testReconcileConsDur(t, s, cid, sid)
	ps2, _ := testReconcileRecord(t, asm, cid)
	if cc := ps2.Cons[sid]; cc == nil || cc.Durable != mintedDur {
		t.Fatalf("the record names %+v for %q, want the minted durable %q", cc, sid, mintedDur)
	}
	if !testReconcileConsumerPresent(t, asm, mintedDur) {
		t.Fatalf("minted durable %q does not exist", mintedDur)
	}

	testReconcileQoS1Publish(t, o, topic, []byte("minted"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("minted"))
}

// ---------------------------------------------------------------------------
// T5 — register identity is invariant across the repair
// ---------------------------------------------------------------------------

// A transmitted-unacked QoS 1 delivery holds a register naming (X, N). After X
// is reaped and repaired to Y, deliveries acked through Y push Y's ack floor
// past N — which is exactly the false-Complete precondition a recreation under
// the OLD name would trip (M2): the next resume would read (X, N) against a
// present consumer with AckFloor.Stream >= N, classify Complete, and free an
// identifier the client may still hold. Under the fresh-name rule X stays
// absent forever, so the register classifies Lost and the identifier stays
// reserved.
func TestMQTTReconcileRegisterIdentityInvariant(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "reg-id", "reg/id", "reg.id"
	c, r := testReconcileSubscribe(t, o, cid, topic, 1)
	defer c.Close()
	oldDur := testReconcileConsDur(t, s, cid, sid)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "reg-id-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// Burn two identifiers on completed deliveries so the one under test is not
	// what a fresh allocator would mint anyway.
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pc, pr, 1, false, false, topic, 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQos1, []byte(payload))
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		testMQTTFlush(t, c, nil, r)
	}
	// The delivery under test: transmitted, never acked.
	testMQTTPublish(t, pc, pr, 1, false, false, topic, 1, []byte("unacked"))
	heldPI := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQos1, []byte("unacked"))
	if heldPI <= 1 {
		t.Fatalf("identifier %d is what a fresh allocator would produce; the test would prove nothing", heldPI)
	}
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	held := sess.pendingPublish[heldPI]
	sess.mu.Unlock()
	if held == nil || held.jsDur != oldDur {
		t.Fatalf("pendingPublish[%d] = %+v, want a delivery on %q", heldPI, held, oldDur)
	}
	// N: the stream sequence the register names.
	regSeq := held.sseq
	c.Close()

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// First resume: the register classifies LOST (X absent) and the identifier
	// stays reserved; the filter is repaired to Y.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)
	newDur := testReconcileConsDur(t, s, cid, sid)
	if newDur == oldDur {
		t.Fatalf("the replacement reused the reaped durable name %q", oldDur)
	}
	// The lost delivery is NOT redelivered (Y is DeliverNew; the payload went
	// with X) — resendPendingRedelivery's NAKs of old-X ack subjects must change
	// nothing on Y.
	testMQTTExpectNothing(t, r2)

	// Drive and ack deliveries through Y until AckFloor.Stream provably passes
	// N, so the false-Complete precondition HOLDS for a same-name mutant.
	for i, payload := range []string{"three", "four", "five"} {
		testMQTTPublish(t, pc, pr, 1, false, false, topic, 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, c2, r2, topic, mqttPubQos1, []byte(payload))
		if pi == heldPI {
			t.Fatalf("delivery %d reused the reserved identifier %d", i, heldPI)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
		testMQTTFlush(t, c2, nil, r2)
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ci, err := asm.jsa.lookupConsumer(mqttStreamName, newDur)
		if err != nil || ci == nil {
			return fmt.Errorf("lookupConsumer %q: %v", newDur, err)
		}
		if ci.AckFloor.Stream < regSeq {
			return fmt.Errorf("AckFloor.Stream=%d has not passed the register's sequence %d", ci.AckFloor.Stream, regSeq)
		}
		return nil
	})
	c2.Close()

	// Second resume: the register still names X, X is still absent, so the
	// verdict stays LOST and the identifier stays reserved. A same-name mutant
	// flips it to a false Complete here and frees it.
	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	sess = testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	_, reserved := sess.pendingPublish[heldPI]
	sess.mu.Unlock()
	if !reserved {
		t.Fatalf("identifier %d was freed; its delivery was lost, not completed", heldPI)
	}
	testMQTTExpectNothing(t, r3)
}

// ---------------------------------------------------------------------------
// T6 — a real inactivity reap, end to end
// ---------------------------------------------------------------------------

// The broker's own consumer_inactive_threshold reaps the durable while the
// client is away; the next CONNECT repairs it and QoS 1 delivery works.
func TestMQTTReconcileRealReapBrokerThreshold(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.ConsumerInactiveThreshold = 300 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "real-reap", "real/reap", "real.reap"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()

	asm := testMQTTGetGlobalASM(t, s)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if testReconcileConsumerPresent(t, asm, oldDur) {
			return fmt.Errorf("durable %q still exists", oldDur)
		}
		return nil
	})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testReconcileQoS1Publish(t, o, topic, []byte("reaped-and-repaired"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("reaped-and-repaired"))
}

// The stream-forced ConsumerLimits.InactiveThreshold variant: the limit reaches
// consumers requesting 0, so the broker's own opt-out does not protect them.
func TestMQTTReconcileRealReapStreamForcedThreshold(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Bootstrap the account manager, then impose the limit out of band BEFORE
	// the subscription exists, as a deployment would.
	boot, bootr := testMQTTConnect(t, &mqttConnInfo{clientID: "sf-boot", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, bootr, mqttConnAckRCConnectionAccepted, false)
	boot.Close()
	asm := testMQTTGetGlobalASM(t, s)
	si, err := asm.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookupStream: %v", err)
	}
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = 300 * time.Millisecond
	if _, err := asm.jsa.updateStream(&cfg); err != nil {
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, err)
	}

	const cid, topic = "sf-reap", "sf/reap"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, "sf.reap")
	c.Close()

	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if testReconcileConsumerPresent(t, asm, oldDur) {
			return fmt.Errorf("durable %q still exists", oldDur)
		}
		return nil
	})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testReconcileQoS1Publish(t, o, topic, []byte("stream-forced"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("stream-forced"))
}

// Subscribe-before-create (M7) and in-place adoption (M11), pinned as ORDERING
// assertions through the repair-operations seam rather than by racing a timer:
// the pre-create hook observes that the flushed delivery subscription already
// exists when createDurableConsumer is about to run, and captures the
// subscription OBJECT so the post-restoration state can be proven to be the
// same object with its gate opened in place (a processSub re-issue installs a
// new object and fails the pointer assertion).
func TestMQTTReconcileSubscribeBeforeCreateAndInPlaceAdoption(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "order-pin", "order/pin", "order.pin"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	type observation struct {
		client     *client
		sub        *subscription
		subscribed bool
		closed     bool
		phaseOwned bool
		deliver    string
	}
	var obs atomic.Pointer[observation]
	asm.mu.Lock()
	asm.testResumeReconcilePreCreate = func(sess *mqttSession, deliver string) error {
		sess.mu.Lock()
		cc := sess.c
		sess.mu.Unlock()
		o := &observation{client: cc, deliver: deliver}
		if cc != nil {
			cc.mu.Lock()
			o.sub = cc.subs[deliver]
			cc.mu.Unlock()
			o.subscribed = o.sub != nil
			if o.sub != nil {
				sess.mu.Lock()
				sess.subsMu.Lock()
				if o.sub.mqtt != nil {
					o.closed = o.sub.mqtt.closed
					o.phaseOwned = o.sub.mqtt.phaseOwned
				}
				sess.subsMu.Unlock()
				sess.mu.Unlock()
			}
		}
		obs.Store(o)
		return nil
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testResumeReconcilePreCreate = nil
		asm.mu.Unlock()
	}()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	ob := obs.Load()
	if ob == nil {
		t.Fatal("the repair never reached the pre-create seam")
	}
	// M7: at create time, the flushed delivery subscription already exists,
	// closed and phase-owned.
	if !ob.subscribed {
		t.Fatal("createDurableConsumer was about to run with NO delivery subscription installed; the replacement could be reaped in the gap")
	}
	if !ob.closed || !ob.phaseOwned {
		t.Fatalf("the pre-create subscription must be closed (got %v) and phase-owned (got %v)", ob.closed, ob.phaseOwned)
	}

	// Restoration adopted it: delivery works...
	testReconcileQoS1Publish(t, o, topic, []byte("adopted"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("adopted"))

	// ...and it is the SAME subscription object, gate opened in place (M11).
	ob.client.mu.Lock()
	live := ob.client.subs[ob.deliver]
	ob.client.mu.Unlock()
	if live != ob.sub {
		t.Fatal("restoration re-issued the delivery subscription instead of adopting it in place; interest lapsed in between")
	}
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	sess.subsMu.Lock()
	closed, owned := live.mqtt.closed, live.mqtt.phaseOwned
	sess.subsMu.Unlock()
	sess.mu.Unlock()
	if closed || owned {
		t.Fatalf("the adopted subscription must be open (closed=%v) and no longer phase-owned (phaseOwned=%v)", closed, owned)
	}
}

// Interest-before-lookup for PRESENT consumers (M14): the reconciliation lookup
// hook sleeps past a stream-forced inactivity threshold before answering
// "present" — an answer computed before the reaper fired. With step 2's order
// (subscribe closed, then look up) the phase's subscription holds interest
// through the sleep, the consumer survives, and the stale "present" is
// harmlessly true. With the lookup first (M14), no interest exists during the
// sleep, the REAL reaper fires, the phase trusts the stale answer, restores a
// dead deliver subject, and the delivery assertion fails — deterministically,
// because the sleep is inside the hook, not a race.
func TestMQTTReconcileInterestHeldBeforeLookup(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	boot, bootr := testMQTTConnect(t, &mqttConnInfo{clientID: "hold-boot", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, bootr, mqttConnAckRCConnectionAccepted, false)
	boot.Close()
	asm := testMQTTGetGlobalASM(t, s)
	si, err := asm.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookupStream: %v", err)
	}
	const threshold = 1500 * time.Millisecond
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = threshold
	if _, err := asm.jsa.updateStream(&cfg); err != nil {
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, err)
	}

	const cid, topic, sid = "hold-lookup", "hold/lookup", "hold.lookup"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	dur := testReconcileConsDur(t, s, cid, sid)
	c.Close()

	// The hook: sleep well past the threshold, then answer "present" — the
	// answer a lookup that raced the reaper would have produced.
	asm.mu.Lock()
	asm.testResumeReconcileLookup = func(durables []string) ([]*ConsumerInfo, []bool, error) {
		time.Sleep(threshold + threshold)
		infos := make([]*ConsumerInfo, len(durables))
		oks := make([]bool, len(durables))
		for i, d := range durables {
			infos[i] = &ConsumerInfo{Name: d, Stream: mqttStreamName}
			oks[i] = true
		}
		return infos, oks, nil
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testResumeReconcileLookup = nil
		asm.mu.Unlock()
	}()

	// Reconnect immediately, well inside the threshold, so the consumer is
	// still alive when step 2 installs its interest.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	// The phase's interest carried the consumer through the sleep: same durable,
	// still present, still delivering.
	if got := testReconcileConsDur(t, s, cid, sid); got != dur {
		t.Fatalf("the present consumer %q was replaced by %q", dur, got)
	}
	if !testReconcileConsumerPresent(t, asm, dur) {
		t.Fatalf("durable %q was reaped while the phase held its interest", dur)
	}
	testReconcileQoS1Publish(t, o, topic, []byte("held"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("held"))
}

// ---------------------------------------------------------------------------
// T9 — a re-SUBSCRIBE-adopted drain is repaired at the next CONNECT
// ---------------------------------------------------------------------------

// An UNSUBSCRIBE stages a drain; its durable is deleted out of band; a
// re-SUBSCRIBE adopts the drain and inherits a dead entry (today-behavior, a
// stated residual). The next CONNECT's phase verifies the adopted entry, finds
// it absent, and repairs it — while recoverDrains keeps owning drain-side loss
// reporting for a drain the phase never touched (its sid is not in subs, so it
// is not collected).
func TestMQTTReconcileAdoptedDrainRepairedAtNextConnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "drain-t9"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "t9/a", qos: 1}, {filter: "t9/b", qos: 1}}, []byte{1, 1})
	testMQTTFlush(t, c, nil, r)
	durA := testReconcileConsDur(t, s, cid, "t9.a")
	durB := testReconcileConsDur(t, s, cid, "t9.b")

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "drain-t9-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// A started (transmitted, unacked) delivery on each filter, so both
	// UNSUBSCRIBEs stage drains.
	testMQTTPublish(t, pc, pr, 1, false, false, "t9/a", 1, []byte("started-a"))
	testMQTTCheckPubMsgNoAck(t, c, r, "t9/a", mqttPubQos1, []byte("started-a"))
	testMQTTPublish(t, pc, pr, 1, false, false, "t9/b", 1, []byte("started-b"))
	testMQTTCheckPubMsgNoAck(t, c, r, "t9/b", mqttPubQos1, []byte("started-b"))
	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: "t9/a"}, {filter: "t9/b"}})

	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	_, drainA := sess.drains["t9.a"]
	_, drainB := sess.drains["t9.b"]
	sess.mu.Unlock()
	if !drainA || !drainB {
		t.Fatalf("expected staged drains for both filters (a=%v b=%v)", drainA, drainB)
	}

	// Both drained durables are deleted out of band (the reap).
	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, durA)
	testReconcileDeleteConsumer(t, asm, durB)

	// The re-SUBSCRIBE adopts filter a's drain: the entry is back in cons,
	// naming the dead durable — and delivery is dead, as documented.
	testMQTTSub(t, 3, c, r, []*mqttFilter{{filter: "t9/a", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	if got := testReconcileConsDur(t, s, cid, "t9.a"); got != durA {
		t.Fatalf("the adopted entry names %q, want the drained durable %q", got, durA)
	}
	testMQTTPublish(t, pc, pr, 1, false, false, "t9/a", 2, []byte("dead-entry"))
	testMQTTExpectNothing(t, r)
	c.Close()

	// The next CONNECT repairs the adopted entry...
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)
	newDurA := testReconcileConsDur(t, s, cid, "t9.a")
	if newDurA == durA {
		t.Fatalf("the adopted entry was not repaired under a fresh name (still %q)", durA)
	}
	testReconcileQoS1Publish(t, o, "t9/a", []byte("repaired"))
	testReconcileExpectQoS1(t, c2, r2, "t9/a", []byte("repaired"))

	// ...and filter b's untouched drain was resolved by recoverDrains, which
	// reported its claims LOST (consumer gone, no PUBREL owed) and dropped it —
	// the phase never collected it (its filter is not in subs).
	sess = testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	_, drainBStill := sess.drains["t9.b"]
	_, consB := sess.cons["t9.b"]
	sess.mu.Unlock()
	if drainBStill {
		t.Fatal("recoverDrains did not resolve the lost drain; the phase must not have interfered")
	}
	if consB {
		t.Fatalf("the un-resubscribed drained filter t9.b must not have a consumer entry (durB=%q)", durB)
	}
}

// ---------------------------------------------------------------------------
// T10 — the reap gap is not replayed
// ---------------------------------------------------------------------------

// Messages retained by an OVERLAPPING consumer during the reap gap are not
// replayed to the repaired subscription: the replacement is DeliverNew, and no
// cursor proves what the reaped consumer was owed. This documents the
// delivery-start policy (a stated residual), it does not fix it.
func TestMQTTReconcileReapGapNotReplayed(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "gap-a", "gap/t", "gap.t"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()

	// The overlap: another persistent subscriber holds interest in the same
	// subject, so the gap message is retained by ITS consumer.
	cb, rb := testReconcileSubscribe(t, o, "gap-b", topic, 1)
	defer cb.Close()

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Published INSIDE the gap: only the overlapping consumer holds it.
	testReconcileQoS1Publish(t, o, topic, []byte("gap-message"))
	testReconcileExpectQoS1(t, cb, rb, topic, []byte("gap-message"))

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The gap message is NOT replayed to the repaired subscription...
	testMQTTExpectNothing(t, r2)

	// ...but everything from the repair on flows normally, to both.
	testReconcileQoS1Publish(t, o, topic, []byte("after-gap"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("after-gap"))
	testReconcileExpectQoS1(t, cb, rb, topic, []byte("after-gap"))
}

// ---------------------------------------------------------------------------
// Failure-injection scaffolding
// ---------------------------------------------------------------------------

// testReconcileIsRepairSave reports whether the save ABOUT to run for sess is
// the reconciliation phase's repair save: the accept save runs before the phase
// replaces anything, so the in-memory cons entry for sid still names oldDur
// there — and no longer does at the repair save.
func testReconcileIsRepairSave(sess *mqttSession, sid, oldDur string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	cc := sess.cons[sid]
	return cc != nil && cc.Durable != oldDur
}

// testReconcileRawConnect dials and writes a persistent-session 3.1.1 CONNECT
// without reading anything back, so the test can make assertions about what is
// (not) on the wire before the broker answers.
func testReconcileRawConnect(t testing.TB, o *Options, cid string) net.Conn {
	t.Helper()
	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := testMQTTWrite(c, mqttCreateConnectProto(&mqttConnInfo{clientID: cid, cleanSess: false})); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	return c
}

// testReconcileReadConnAck reads the (delayed) CONNACK off a raw connection.
func testReconcileReadConnAck(t testing.TB, c net.Conn, sessp bool) *mqttReader {
	t.Helper()
	buf, err := testMQTTRead(c)
	if err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}
	r := &mqttReader{reader: c}
	r.reset(buf)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, sessp)
	return r
}

// testReconcileClientGone waits until the server-side client for cid has been
// torn down — the readLoop runs mqttHandleClosedClient BEFORE closeConnection
// deregisters the client, so "gone from s.clients" fences the close
// bookkeeping the assertions are about.
func testReconcileClientGone(t testing.TB, s *Server, cid string) {
	t.Helper()
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.clients {
			c.mu.Lock()
			match := c.isMqtt() && c.mqtt.cid == cid
			c.mu.Unlock()
			if match {
				return fmt.Errorf("client %q is still registered", cid)
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// T7 — nothing on the wire before the repair save is confirmed
// ---------------------------------------------------------------------------

// Park the REPAIR save; while it is parked, a message is stored for the
// repaired filter. NOTHING may be on the wire — not the delivery (the
// replacement's subscription is closed: M6) and not the CONNACK itself (the
// save precedes it). Released, the CONNACK arrives and AckWait redelivers the
// gated message with its packet identifier.
func TestMQTTReconcileNothingBeforeConnAck(t *testing.T) {
	o := testMQTTDefaultOptions()
	// Longer than the parked window below: a delivery waiting on the phase gate
	// still holds a JetStream delivery attempt, so an AckWait shorter than the
	// park would have the message re-pushed and delivered a second time with DUP
	// — legal, but it would blur what this test is asserting.
	o.MQTT.AckWait = 10 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "park7", "park/7", "park.7"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileDeleteConsumer(t, testMQTTGetGlobalASM(t, s), oldDur)

	parked := make(chan struct{})
	release := make(chan struct{})
	var parkOnce, releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark) // a Fatal between park and release must not wedge the readLoop
	restoreHook := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		parkOnce.Do(func() {
			close(parked)
			<-release
		})
		return nil
	})
	defer restoreHook()

	conn := testReconcileRawConnect(t, o, cid)
	defer conn.Close()
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the repair save was never reached")
	}

	// Stored while the save is parked: the replacement consumer exists and its
	// closed subscription holds the interest, so the message is retained and
	// pushed — and gated.
	testReconcileQoS1Publish(t, o, topic, []byte("while-parked"))

	// NO wire output at all while parked: no CONNACK, no delivery.
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var buf [64]byte
	if n, rerr := conn.Read(buf[:]); rerr == nil {
		t.Fatalf("read %d bytes before the repair save was confirmed: %v", n, buf[:n])
	} else if ne, ok := rerr.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("the connection failed while parked: %v", rerr)
	}
	conn.SetReadDeadline(time.Time{})

	unpark()
	r := testReconcileReadConnAck(t, conn, true)
	// The delivery pushed while the save was parked WAITED on the phase gate, so
	// it arrives as the first transmission it is — not a DUP after AckWait.
	pi := testMQTTCheckPubMsgNoAck(t, conn, r, topic, mqttPubQos1, []byte("while-parked"))
	if pi == 0 {
		t.Fatal("delivery carried no packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn, pi)
	testMQTTFlush(t, conn, nil, r)
}

// ---------------------------------------------------------------------------
// T8 — takeover between the accept save and the repair save (10071)
// ---------------------------------------------------------------------------

// A foreign write lands on the session record between the accept save and the
// repair save; the repair save CAS-rejects with 10071 (the single-writer
// check), the session is foreign-disposed, and the CONNECT is refused with no
// packet at all.
//
// M9 is pinned by IMMEDIATE-state assertions on a finite-expiry session, not by
// the reconnect (a bare-removeSession mutant's next reconnect also succeeds by
// reloading the record): after the refusal, ordinary close bookkeeping must
// have been suppressed for the stolen session — no expiry timer, no disconnect
// save on top of the foreign record, and the client detached. The mutant runs
// that bookkeeping and produces exactly those.
func TestMQTTReconcileTakeoverDuringPhase(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "t8-steal", "t8/steal", "t8.steal"
	hash := getHash(cid)

	// A v5 session with a FINITE expiry, so the mutant's close bookkeeping has
	// observable products (a timer, a disconnect save).
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || sp {
		t.Fatalf("v5 CONNACK: sp=%v reason=%x", sp, reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, c, nil, r)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	sessObj := testMQTTResidentSession(t, s, cid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Interleave the foreign write from INSIDE the save seam, immediately
	// before the repair save issues its CAS — the exact window under test. The
	// write goes through the plain message store, not storeSessionMsg, so no
	// session-persist notification can evict the session before the CAS itself
	// decides. t.Errorf (never Fatalf) off the test goroutine.
	var bumpSeq atomic.Uint64
	var bumpOnce sync.Once
	restoreHook := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		bumpOnce.Do(func() {
			smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
			if err != nil {
				t.Errorf("load record for the foreign bump: %v", err)
				return
			}
			ps := &mqttPersistedSession{}
			if err := json.Unmarshal(smsg.Data, ps); err != nil {
				t.Errorf("unmarshal record for the foreign bump: %v", err)
				return
			}
			ps.Origin = "elsewhere"
			b, _ := json.Marshal(ps)
			resp, err := asm.jsa.storeMsg(mqttSessStreamSubjectPrefix+asm.domainTk+hash, -1, b)
			if err != nil {
				t.Errorf("store the foreign bump: %v", err)
				return
			}
			bumpSeq.Store(resp.Sequence)
		})
		return nil
	})
	defer restoreHook()

	// The reconnect is REFUSED: no CONNACK, no DISCONNECT, connection closed.
	c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTExpectDisconnect(t, c2)
	if bumpSeq.Load() == 0 {
		t.Fatal("the foreign bump never landed; the test proved nothing")
	}
	testReconcileClientGone(t, s, cid)

	// Immediate state (M9): the stolen session is evicted and detached, no
	// expiry timer was armed against it, and the foreign record is untouched —
	// close bookkeeping for it was suppressed, not merely late.
	asm.mu.RLock()
	_, resident := asm.sessions[cid]
	_, timer := asm.pendingExpiries[hash]
	asm.mu.RUnlock()
	if resident {
		t.Fatal("the stolen session is still mapped")
	}
	if timer {
		t.Fatal("an expiry timer was armed against the stolen session")
	}
	sessObj.mu.Lock()
	bound := sessObj.c != nil
	replacementDur := ""
	if cc := sessObj.cons[sid]; cc != nil {
		replacementDur = cc.Durable
	}
	sessObj.mu.Unlock()
	if bound {
		t.Fatal("the stolen session is still bound to the refused client")
	}
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
	if err != nil {
		t.Fatalf("re-read the record: %v", err)
	}
	if smsg.Sequence != bumpSeq.Load() {
		t.Fatalf("the record moved after the foreign bump (seq %d -> %d): local close bookkeeping wrote on a stolen session", bumpSeq.Load(), smsg.Sequence)
	}

	// The phase's replacement consumer is referenced by no record and no live
	// session: the orphan sweep reclaims it.
	if replacementDur == "" || replacementDur == oldDur {
		t.Fatalf("expected the phase to have minted a replacement (got %q)", replacementDur)
	}
	origGrace := mqttOrphanSweepGrace
	mqttOrphanSweepGrace = -time.Second
	defer func() { mqttOrphanSweepGrace = origGrace }()
	asm.sweepOrphanedConsumers(s)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if testReconcileConsumerPresent(t, asm, replacementDur) {
			return fmt.Errorf("orphaned replacement %q still exists", replacementDur)
		}
		return nil
	})

	// The next reconnect loads the (foreign) record fresh and succeeds.
	restoreHook()
	c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r3); reason != mqttReasonSuccess || !sp {
		t.Fatalf("reconnect after the takeover: sp=%v reason=%x", sp, reason)
	}
	testReconcileQoS1Publish(t, o, topic, []byte("fresh"))
	qos, pi := testMQTTReadPublishV5(t, r3, topic, []byte("fresh"))
	if qos != 1 || pi == 0 {
		t.Fatalf("delivery qos=%d pi=%d, want a QoS 1 delivery with a packet identifier", qos, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c3, pi)
	testMQTTFlush(t, c3, nil, r3)
}

// The 10164 variant: clustered contention is a rejection, never a verdict. The
// injection sits BELOW save()'s formatting boundary (a suppressed store plus a
// one-shot result rewrite producing the production-shaped *ApiError), because
// an error injected above it would bypass exactly the classification under
// test. The oracle REQUIRES the retry: the SAME CONNECT must re-read, re-issue
// the identical write, and send its CONNACK — with exactly one new record
// message (same base), and the landed sequence adopted.
//
// M13 (10164 -> foreign without the re-read) foreign-disposes the session and
// the CONNACK never comes; M17 (10164 -> local-definite without the retry)
// refuses the CONNECT — both fail the same-CONNECT-succeeds assertion.
func TestMQTTReconcileContendedRepairSaveRetries(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "t8-10164", "t8/contend", "t8.contend"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Arm at the save seam, rewrite at the store-result seam. The suppressed
	// attempt never reaches the stream (the record is genuinely unchanged) and
	// surfaces as a production-shaped 10164; the retry then goes through the
	// same seams, observed and untouched.
	var mu sync.Mutex
	var armed, rewritten bool
	var storeSeqs []uint64
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		mu.Lock()
		if !armed {
			armed = true
			mqttTestSessStoreSuppressV.Store(true)
		}
		mu.Unlock()
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		mu.Lock()
		defer mu.Unlock()
		if armed && !rewritten {
			rewritten = true
			mqttTestSessStoreSuppressV.Store(false)
			return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		}
		if resp != nil {
			storeSeqs = append(storeSeqs, resp.Sequence)
		}
		return resp, err
	})
	defer restoreResult()
	defer mqttTestSessStoreSuppressV.Store(false)

	// The SAME CONNECT succeeds.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	mu.Lock()
	sawRewrite := rewritten
	seqs := append([]uint64(nil), storeSeqs...)
	mu.Unlock()
	if !sawRewrite {
		t.Fatal("the 10164 injection never fired; the test proved nothing")
	}
	// Exactly two record writes landed for this session across the CONNECT: the
	// accept save and the ONE retried repair save — the suppressed attempt
	// stored nothing, so consecutive sequences prove the retry reused the same
	// base.
	if len(seqs) != 2 {
		t.Fatalf("observed %d landed record writes (%v), want the accept save and exactly one repair save", len(seqs), seqs)
	}
	if seqs[1] != seqs[0]+1 {
		t.Fatalf("the repair save landed at %d after the accept save at %d; something else wrote in between", seqs[1], seqs[0])
	}
	// The retried write's sequence was adopted, and the record it stored names
	// the replacement.
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	adopted := sess.seq
	newDur := sess.cons[sid].Durable
	sess.mu.Unlock()
	if adopted != seqs[1] {
		t.Fatalf("sess.seq = %d, want the landed repair-save sequence %d", adopted, seqs[1])
	}
	ps, recSeq := testReconcileRecord(t, asm, cid)
	if recSeq != seqs[1] {
		t.Fatalf("record head at %d, want %d", recSeq, seqs[1])
	}
	if cc := ps.Cons[sid]; cc == nil || cc.Durable != newDur || newDur == oldDur {
		t.Fatalf("the retried repair save stored %+v, want the fresh durable %q", cc, newDur)
	}

	testReconcileQoS1Publish(t, o, topic, []byte("contended"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("contended"))
}

// ---------------------------------------------------------------------------
// T11 — an indeterminate verification refuses the CONNECT
// ---------------------------------------------------------------------------

// A batch verification outage fails CLOSED: the CONNECT is refused with no
// packet, NO repair state lands anywhere (record, consumers, charges), and the
// next uninjected reconnect resumes and repairs normally. M10 (treat
// unresolved durables as present and continue) sends a CONNACK here instead.
func TestMQTTReconcileIndeterminateRefusesConnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "t11-outage", "t11/outage", "t11.outage"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	wantCharged := make(map[string]int, len(sess.consCharged))
	for k, v := range sess.consCharged {
		wantCharged[k] = v
	}
	wantTmaxack := sess.tmaxack
	sess.mu.Unlock()
	c.Close()

	asm := testMQTTGetGlobalASM(t, s)
	// The durable is gone AND unverifiable: exactly the state in which a repair
	// would be wrong (it may exist) and a resume would be wrong (it may not).
	testReconcileDeleteConsumer(t, asm, oldDur)
	asm.mu.Lock()
	asm.testResumeReconcileLookup = func(durables []string) ([]*ConsumerInfo, []bool, error) {
		return nil, nil, errors.New("simulated verification outage")
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testResumeReconcileLookup = nil
		asm.mu.Unlock()
	}()

	_, recSeqBefore := testReconcileRecord(t, asm, cid)
	testReconcileRefusedConnect(t, o, cid)
	testReconcileClientGone(t, s, cid)

	// No repair state landed: the record advanced only by the legitimate accept
	// save and still names the old durable; no consumer exists; no charge moved.
	ps, recSeqAfter := testReconcileRecord(t, asm, cid)
	if cc := ps.Cons[sid]; cc == nil || cc.Durable != oldDur {
		t.Fatalf("the record names %+v, want the unverifiable durable %q untouched", cc, oldDur)
	}
	// Exactly one write: the accept save. The CONNECT's requested lifetime is
	// applied only at the grant point, which a refused resume never reaches, so
	// there is nothing else to write and nothing to put back.
	if recSeqAfter != recSeqBefore+1 {
		t.Fatalf("record advanced %d -> %d; want the accept save only", recSeqBefore, recSeqAfter)
	}
	if ps.ExpiryInterval != mqttSessionNeverExpire || ps.Clean {
		t.Fatalf("the restored record carries ExpiryInterval=%d Clean=%v, want the session's own persistent lifetime",
			ps.ExpiryInterval, ps.Clean)
	}
	consumers, err := asm.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	if len(consumers) != 0 {
		t.Fatalf("%d consumers exist after the refusal; the phase must not have created any", len(consumers))
	}
	sess.mu.Lock()
	gotTmaxack := sess.tmaxack
	sameCharges := len(sess.consCharged) == len(wantCharged)
	for k, v := range wantCharged {
		if sess.consCharged[k] != v {
			sameCharges = false
		}
	}
	sess.mu.Unlock()
	if !sameCharges || gotTmaxack != wantTmaxack {
		t.Fatalf("charges moved on a refused CONNECT (tmaxack %d, want %d)", gotTmaxack, wantTmaxack)
	}

	// The next (uninjected) reconnect repairs and delivers.
	asm.mu.Lock()
	asm.testResumeReconcileLookup = nil
	asm.mu.Unlock()
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testReconcileQoS1Publish(t, o, topic, []byte("recovered"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("recovered"))
}

// ---------------------------------------------------------------------------
// T12 — repair-save ambiguity, both outcomes
// ---------------------------------------------------------------------------

// LANDED, response lost: the store went through and the result was rewritten to
// an unclassifiable error. The CONNECT is refused; the kept state and the
// pending-write identity let the next reconnect's readback classify the write
// as OURS, keep the replacement the record names, verify it present, and
// deliver.
func TestMQTTReconcileAmbiguousRepairSaveLanded(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "t12-landed", "t12/landed", "t12.landed"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	var mu sync.Mutex
	var lostOnce bool
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid || err != nil || resp == nil || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return resp, err
		}
		mu.Lock()
		defer mu.Unlock()
		if lostOnce {
			return resp, err
		}
		lostOnce = true
		return nil, errors.New("simulated lost store response")
	})
	defer restoreResult()

	testReconcileRefusedConnect(t, o, cid)
	testReconcileClientGone(t, s, cid)
	mu.Lock()
	fired := lostOnce
	mu.Unlock()
	if !fired {
		t.Fatal("the lost-response injection never fired; the test proved nothing")
	}

	// The state was KEPT (the write may have landed — here it did): the session
	// still names the replacement, unresolved until a readback.
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	keptDur := sess.cons[sid].Durable
	unresolved := sess.seqUncertain
	sess.mu.Unlock()
	if keptDur == oldDur {
		t.Fatal("the replacement was rolled back although its write may have landed")
	}
	if !unresolved {
		t.Fatal("the session must be unresolved until a readback classifies the write")
	}

	// The next reconnect resolves LANDED, keeps the replacement, verifies it
	// present, and delivers.
	restoreResult()
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)
	if got := testReconcileConsDur(t, s, cid, sid); got != keptDur {
		t.Fatalf("the landed replacement %q was replaced again by %q", keptDur, got)
	}
	ps, _ := testReconcileRecord(t, asm, cid)
	if cc := ps.Cons[sid]; cc == nil || cc.Durable != keptDur {
		t.Fatalf("the record names %+v, want the landed replacement %q", cc, keptDur)
	}
	testReconcileQoS1Publish(t, o, topic, []byte("landed"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("landed"))
}

// The store never reaches the stream: it is suppressed before it is issued. The
// CONNECT is refused with the repaired identity KEPT, and the next reconnect's
// resolution settles the ambiguity by re-attempting that repair record's exact
// bytes — so the SAME replacement becomes durable and is then verified present,
// rather than being rolled back and repaired all over again under a third name.
//
// Mutation M1: revert the at-base arm to clearing the uncertainty and the
// replacement is undone, so the reconnect repairs afresh under a new durable.
func TestMQTTReconcileAmbiguousRepairSaveIsSettledForward(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "t12-lost", "t12/lost", "t12.lost"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Suppress exactly the repair save: armed at the save seam, disarmed at the
	// result seam (which sees the suppressed error pass through unclassified).
	var mu sync.Mutex
	var armed, suppressed bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		mu.Lock()
		if !armed {
			armed = true
			mqttTestSessStoreSuppressV.Store(true)
		}
		mu.Unlock()
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		mu.Lock()
		defer mu.Unlock()
		if armed && !suppressed {
			suppressed = true
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	defer restoreResult()
	defer mqttTestSessStoreSuppressV.Store(false)

	testReconcileRefusedConnect(t, o, cid)
	testReconcileClientGone(t, s, cid)
	mu.Lock()
	fired := suppressed
	mu.Unlock()
	if !fired {
		t.Fatal("the suppression never fired; the test proved nothing")
	}

	// Kept while unresolved — the replacement is still named in memory.
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	firstReplacement := sess.cons[sid].Durable
	sess.mu.Unlock()
	if firstReplacement == oldDur {
		t.Fatal("the replacement was rolled back before the write was settled either way")
	}

	// The next reconnect: the resolution re-attempts the repair record's own bytes,
	// so the record ends up naming THIS replacement and the resume verifies it
	// present — no second repair.
	restoreSave()
	restoreResult()
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	settled := testReconcileConsDur(t, s, cid, sid)
	if settled != firstReplacement {
		t.Fatalf("the settled resume must keep the repaired identity %q, got %q (old %q)",
			firstReplacement, settled, oldDur)
	}
	ps, _ := testReconcileRecord(t, asm, cid)
	if cc := ps.Cons[sid]; cc == nil || cc.Durable != firstReplacement {
		t.Fatalf("the record names %+v, want the settled repair %q", cc, firstReplacement)
	}
	// And it is the durable that actually exists: the ambiguity was settled by making
	// the record match the resources, never by deleting them.
	if !testReconcileConsumerPresent(t, asm, firstReplacement) {
		t.Fatalf("the settled replacement %q must still exist", firstReplacement)
	}
	testReconcileQoS1Publish(t, o, topic, []byte("re-repaired"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("re-repaired"))
}

// ---------------------------------------------------------------------------
// T13 — refusal discipline with a Will
// ---------------------------------------------------------------------------

// A v5 CONNECT carrying a Will, refused in each branch: the client sees no
// packet at all (no CONNACK, no pre-CONNACK DISCONNECT — connection close
// only), and the NEW connection's Will is not published for a session that was
// never granted. M12 (drop the abort helper's Will/connectReceived clearing)
// fails both halves: the readLoop's v5 error path sends a DISCONNECT as the
// first packet, and close handling publishes the Will.
func TestMQTTReconcileRefusalDisciplineWithWill(t *testing.T) {
	const willTopic = "will/t13"

	type branch struct {
		name string
		// arm installs the refusal injection; it may use the save seam and must
		// clean up via the returned func.
		arm func(t *testing.T, asm *mqttAccountSessionManager, cid, sid, oldDur string) func()
	}
	branches := []branch{
		{"indeterminate lookup", func(t *testing.T, asm *mqttAccountSessionManager, _, _, _ string) func() {
			asm.mu.Lock()
			asm.testResumeReconcileLookup = func([]string) ([]*ConsumerInfo, []bool, error) {
				return nil, nil, errors.New("simulated verification outage")
			}
			asm.mu.Unlock()
			return func() {
				asm.mu.Lock()
				asm.testResumeReconcileLookup = nil
				asm.mu.Unlock()
			}
		}},
		{"failed create", func(t *testing.T, asm *mqttAccountSessionManager, _, _, _ string) func() {
			asm.mu.Lock()
			asm.testResumeReconcilePreCreate = func(*mqttSession, string) error {
				return errors.New("simulated consumer create rejection")
			}
			asm.mu.Unlock()
			return func() {
				asm.mu.Lock()
				asm.testResumeReconcilePreCreate = nil
				asm.mu.Unlock()
			}
		}},
		{"definite CAS", func(t *testing.T, asm *mqttAccountSessionManager, cid, sid, oldDur string) func() {
			// A foreign write interleaved immediately before the repair save's
			// CAS, from inside the save seam (the T8 machinery).
			hash := getHash(cid)
			var once sync.Once
			return setMQTTTestSessSaveHook(func(sess *mqttSession) error {
				if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
					return nil
				}
				once.Do(func() {
					smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
					if err != nil {
						t.Errorf("load record for the foreign bump: %v", err)
						return
					}
					ps := &mqttPersistedSession{}
					if err := json.Unmarshal(smsg.Data, ps); err != nil {
						t.Errorf("unmarshal record for the foreign bump: %v", err)
						return
					}
					ps.Origin = "elsewhere"
					b, _ := json.Marshal(ps)
					if _, err := asm.jsa.storeMsg(mqttSessStreamSubjectPrefix+asm.domainTk+hash, -1, b); err != nil {
						t.Errorf("store the foreign bump: %v", err)
					}
				})
				return nil
			})
		}},
		{"ambiguous save", func(t *testing.T, asm *mqttAccountSessionManager, cid, sid, oldDur string) func() {
			var mu sync.Mutex
			var armed, suppressed bool
			restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
				if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
					return nil
				}
				mu.Lock()
				if !armed {
					armed = true
					mqttTestSessStoreSuppressV.Store(true)
				}
				mu.Unlock()
				return nil
			})
			restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
				if sess.id != cid {
					return resp, err
				}
				mu.Lock()
				defer mu.Unlock()
				if armed && !suppressed {
					suppressed = true
					mqttTestSessStoreSuppressV.Store(false)
				}
				return resp, err
			})
			return func() {
				restoreSave()
				restoreResult()
				mqttTestSessStoreSuppressV.Store(false)
			}
		}},
	}

	for _, br := range branches {
		t.Run(br.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			cid := "t13-" + getHash(br.name)
			const topic, sid = "t13/t", "t13.t"
			hash := getHash(cid)

			// A v5 session with a finite expiry and a QoS 1 subscription.
			c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
				props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || sp {
				t.Fatalf("v5 CONNACK: sp=%v reason=%x", sp, reason)
			}
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
			testMQTTFlush(t, c, nil, r)
			oldDur := testReconcileConsDur(t, s, cid, sid)
			sessObj := testMQTTResidentSession(t, s, cid)
			c.Close()
			testReconcileClientGone(t, s, cid)

			// A watcher for the Will topic: it must never fire.
			wc, wr := testMQTTConnect(t, &mqttConnInfo{clientID: "t13-watch", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer wc.Close()
			testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, wc, wr, []*mqttFilter{{filter: willTopic, qos: 1}}, []byte{1})
			testMQTTFlush(t, wc, nil, wr)

			asm := testMQTTGetGlobalASM(t, s)
			// Every branch but the lookup outage needs an absent durable so the
			// refusal fires from the branch under test, not before it.
			if br.name != "indeterminate lookup" {
				testReconcileDeleteConsumer(t, asm, oldDur)
			}
			cleanup := br.arm(t, asm, cid, sid, oldDur)
			defer cleanup()

			// The refused CONNECT, carrying a Will: NO packet at all, and no Will.
			c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
				props: mqttV5ConnPropsSessionExpiry(300),
				will:  &mqttWill{topic: []byte(willTopic), message: []byte("boom"), qos: 1}},
				o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTExpectDisconnect(t, c2)
			testReconcileClientGone(t, s, cid)
			testMQTTExpectNothing(t, wr)

			// The "failed create" branch is the finite-expiry LOCAL refusal: the
			// resident session must have gone through ORDINARY close bookkeeping —
			// detached, its disconnect recorded, its expiry timer armed. The abort
			// helper suppresses the Will and the parser's DISCONNECT, never this.
			if br.name == "failed create" {
				checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
					sessObj.mu.Lock()
					bound := sessObj.c != nil
					disconnectedAt := sessObj.disconnectedAt
					sessObj.mu.Unlock()
					if bound {
						return fmt.Errorf("the resident session is still bound to the refused client")
					}
					if disconnectedAt == 0 {
						return fmt.Errorf("the local refusal did not record a disconnect time")
					}
					asm.mu.RLock()
					_, timer := asm.pendingExpiries[hash]
					_, resident := asm.sessions[cid]
					asm.mu.RUnlock()
					if !resident {
						return fmt.Errorf("the resident session was evicted on a LOCAL refusal")
					}
					if !timer {
						return fmt.Errorf("the finite expiry timer was not armed")
					}
					return nil
				})
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T15 — an unadopted phase subscription is reclaimed
// ---------------------------------------------------------------------------

// Restoration skips a collected QoS 2 filter (its PUBREL setup fails on the
// exhausted MaxAckPending budget after the PUBREL durable was reaped): the
// post-processSubs sweep must IMMEDIATELY remove the phase's closed
// subscription — nothing else ever would, and it holds consumer interest while
// delivering nothing (M16). The filter stays unavailable until the next
// CONNECT: a live re-SUBSCRIBE reaches the exists-branch and never recreates
// missing interest (a stated residual).
func TestMQTTReconcileUnadoptedPhaseSubReclaimed(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	// Comfortably longer than the whole test: nothing here waits on an AckWait
	// redelivery, and a short one would only add DUP copies of the two backlog
	// messages while they sit unacked.
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "t15-skip", "t15/a", "t15.a"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 2)
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	dur := sess.cons[sid].Durable
	deliver := sess.cons[sid].DeliverSubject
	pubRelDur := sess.pubRelConsumer.Durable
	sess.mu.Unlock()
	c.Close()
	testReconcileClientGone(t, s, cid)

	// The PUBREL durable is reaped; its recreation at restore time will hit an
	// exhausted MaxAckPending budget and fail the QoS 2 filter — the natural
	// "restoration skipped a collected filter" lever.
	asm := testMQTTGetGlobalASM(t, s)
	if _, err := asm.jsa.deleteConsumer(mqttOutStreamName, pubRelDur, true); err != nil {
		t.Fatalf("delete the PUBREL durable: %v", err)
	}
	sess.mu.Lock()
	sess.tmaxack = mqttMaxAckTotalLimit
	sess.mu.Unlock()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	// The PINGRESP fences CONNECT processing: the sweep has run.
	testMQTTFlush(t, c2, nil, r2)

	// IMMEDIATE reclamation: the phase's subscription is gone (no interest),
	// while the verified consumer itself is untouched.
	client := testMQTTGetClient(t, s, cid)
	client.mu.Lock()
	leftover := client.subs[deliver]
	client.mu.Unlock()
	if leftover != nil {
		t.Fatal("the unadopted phase subscription is still installed; it would hold consumer interest forever while delivering nothing")
	}
	if !testReconcileConsumerPresent(t, asm, dur) {
		t.Fatalf("the verified consumer %q was removed; the sweep owns only the interest", dur)
	}

	// The filter is dead on this connection...
	testReconcileQoS1Publish(t, o, topic, []byte("m1-skipped"))
	testMQTTExpectNothing(t, r2)

	// ...and a live re-SUBSCRIBE cannot recover it: the exists-branch refreshes
	// metadata and never recreates missing interest. (The budget is sane again,
	// so the PUBREL consumer recreates and the SUBACK grants QoS 2 — and still
	// no delivery.)
	sess.mu.Lock()
	sess.tmaxack = 0
	sess.mu.Unlock()
	testMQTTSub(t, 2, c2, r2, []*mqttFilter{{filter: topic, qos: 2}}, []byte{2})
	testMQTTFlush(t, c2, nil, r2)
	testReconcileQoS1Publish(t, o, topic, []byte("m2-resub"))
	testMQTTExpectNothing(t, r2)
	c2.Close()
	testReconcileClientGone(t, s, cid)

	// The next CONNECT's phase restores it: the pending messages are delivered in
	// order as the FIRST transmissions they are — they were never put on the wire
	// while the filter was unavailable, and the phase gate holds the backlog
	// JetStream pushes before the CONNACK rather than burning an attempt on it.
	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	// Both are already on the wire back to back, so they are read first and
	// acknowledged after: a flush between them would meet the second PUBLISH.
	for _, payload := range []string{"m1-skipped", "m2-resub"} {
		pi := testMQTTCheckPubMsgNoAck(t, c3, r3, topic, mqttPubQos1, []byte(payload))
		if pi == 0 {
			t.Fatalf("delivery of %q carried no packet identifier", payload)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c3, pi)
	}
	testMQTTFlush(t, c3, nil, r3)
	testReconcileQoS1Publish(t, o, topic, []byte("m3-restored"))
	testReconcileExpectQoS1(t, c3, r3, topic, []byte("m3-restored"))
}

// A post-CONNACK failure exit that never reaches processSubs — the 3.1.1
// forced-leave save failure — leaves the deferred fallback to reclaim the
// phase's subscriptions, and the session resumable.
func TestMQTTReconcileForcedLeaveFailureFallback(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, plainTopic, plainSid = "t15-leave", "t15/plain", "t15.plain"

	// A v5 session carrying BOTH a shared and a plain QoS 1 subscription.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || sp {
		t.Fatalf("v5 CONNACK: sp=%v reason=%x", sp, reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: plainTopic, opts: 1}, {topic: "$share/g/t15/shared", opts: 1}})
	testMQTTFlush(t, c, nil, r)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// Fail the forced-leave save of the 3.1.1 resume: by then the shared
	// memberships have been removed in memory (the accept save still carried
	// them, and the repair save — if any — never runs here: nothing was reaped).
	var mu sync.Mutex
	var failed bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid {
			return nil
		}
		sess.mu.Lock()
		leave := len(sess.sharedSubs) == 0
		sess.mu.Unlock()
		if !leave {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if failed {
			return nil
		}
		failed = true
		return errors.New("simulated forced-leave save failure")
	})
	defer restoreSave()

	// The 3.1.1 resume gets its CONNACK (the failure is post-CONNACK), then the
	// connection closes.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTExpectDisconnect(t, c2)
	testReconcileClientGone(t, s, cid)
	mu.Lock()
	fired := failed
	mu.Unlock()
	if !fired {
		t.Fatal("the forced-leave save failure never fired; the test proved nothing")
	}

	// The undo restored the memberships and the deferred fallback reclaimed the
	// phase's subscriptions; the session resumes cleanly as v5 and the plain
	// filter delivers.
	restoreSave()
	c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r3); reason != mqttReasonSuccess || !sp {
		t.Fatalf("v5 resume after the forced-leave failure: sp=%v reason=%x", sp, reason)
	}
	testReconcileQoS1Publish(t, o, plainTopic, []byte("survived"))
	qos, pi := testMQTTReadPublishV5(t, r3, plainTopic, []byte("survived"))
	if qos != 1 || pi == 0 {
		t.Fatalf("delivery qos=%d pi=%d, want a QoS 1 delivery with a packet identifier", qos, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c3, pi)
	testMQTTFlush(t, c3, nil, r3)
}

// Residual (h), pinned by a regression test: a live SUBSCRIBE that matches a
// STALE unmatched Cons entry (no corresponding subscription — step 1
// deliberately does not verify those) installs no delivery interest and stays
// unavailable until a subsequent CONNECT reconciles it.
func TestMQTTReconcileStaleUnmatchedConsEntryResidual(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "t15-stale"
	const staleTopic, staleSid = "t15/h", "t15.h"
	c, _ := testReconcileSubscribe(t, o, cid, "t15/x", 1)
	c.Close()

	// Doctor the record: a Cons entry with NO matching subscription, naming a
	// durable that does not exist.
	asm := testMQTTGetGlobalASM(t, s)
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	idHash := sess.idHash
	sess.mu.Unlock()
	ps, _ := testReconcileRecord(t, asm, cid)
	ps.Cons[staleSid] = &ConsumerConfig{
		Durable:        idHash + "_stalenonexistent",
		DeliverSubject: mqttSubPrefix + "stalenonexistent",
		FilterSubject:  mqttStreamSubjectPrefix + staleSid,
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverNew,
	}
	b, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshal doctored record: %v", err)
	}
	if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(cid), 0, b); err != nil {
		t.Fatalf("store doctored record: %v", err)
	}

	s = testReconcileRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	// The resume verifies only ACTIVE delivery entries; the stale unmatched one
	// is left alone (and refuses nothing).
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The live SUBSCRIBE matches the stale entry, is GRANTED, and delivers
	// nothing: the exists-branch installs no interest. The residual.
	testMQTTSub(t, 2, c2, r2, []*mqttFilter{{filter: staleTopic, qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)
	testReconcileQoS1Publish(t, o, staleTopic, []byte("dead"))
	testMQTTExpectNothing(t, r2)
	c2.Close()

	// The next CONNECT reconciles it (the filter is in Subs now): absent ->
	// repaired -> delivering.
	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c3, nil, r3)
	newDur := testReconcileConsDur(t, s, cid, staleSid)
	if newDur == idHash+"_stalenonexistent" {
		t.Fatal("the stale entry was not repaired")
	}
	testReconcileQoS1Publish(t, o, staleTopic, []byte("reconciled"))
	testReconcileExpectQoS1(t, c3, r3, staleTopic, []byte("reconciled"))
}

// ---------------------------------------------------------------------------
// A refused resume must not apply its own Session Expiry Interval
// ---------------------------------------------------------------------------

// Review finding: the accept save persists the CONNECT's requested Session
// Expiry Interval BEFORE the phase runs, and the abort helper deliberately
// preserves c.mqtt.sess so ordinary close bookkeeping still runs. A reconnect
// asking for interval 0 whose resume is then REFUSED would therefore have that
// bookkeeping discard a persistent session outright (doClean) — the client,
// never told the resume happened, retries and finds Session Present 0 with its
// subscriptions and durables gone.
//
// The abort helper restores the lifetime the session had before this CONNECT,
// so a refused resume leaves it exactly as it found it.
//
// Mutation check: drop the restore in abortAcceptedResume and this test fails —
// the retry reports Session Present 0 and delivers nothing.
func TestMQTTReconcileRefusedResumeKeepsSessionLifetime(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "keep-life", "keep/life", "keep.life"

	// A persistent v5 session: subscribed, long expiry.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || sp {
		t.Fatalf("v5 CONNACK: sp=%v reason=%x", sp, reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, c, nil, r)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// Make the verification indeterminate so the resume is refused.
	asm := testMQTTGetGlobalASM(t, s)
	asm.mu.Lock()
	asm.testResumeReconcileLookup = func([]string) ([]*ConsumerInfo, []bool, error) {
		return nil, nil, errors.New("simulated verification outage")
	}
	asm.mu.Unlock()

	// The refused reconnect asks for Session Expiry Interval 0 — "discard my
	// session when this connection ends". It is never granted.
	c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTExpectDisconnect(t, c2)
	testReconcileClientGone(t, s, cid)

	// The session survived with the lifetime it had: still resident, still
	// carrying its long interval, its record still naming its consumer.
	asm.mu.RLock()
	sess := asm.sessions[cid]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the refused resume discarded the session; a CONNECT that was never acknowledged must not apply its Session Expiry Interval")
	}
	sess.mu.Lock()
	gotExpiry, gotClean := sess.expiryInterval, sess.clean
	sess.mu.Unlock()
	if gotExpiry != 3600 || gotClean {
		t.Fatalf("session lifetime after a refused resume: expiry=%d clean=%v, want 3600/false", gotExpiry, gotClean)
	}
	ps, _ := testReconcileRecord(t, asm, cid)
	if cc := ps.Cons[sid]; cc == nil || cc.Durable != oldDur {
		t.Fatalf("the record names %+v, want the pre-CONNECT durable %q", cc, oldDur)
	}

	// And the client's retry resumes normally and delivers.
	asm.mu.Lock()
	asm.testResumeReconcileLookup = nil
	asm.mu.Unlock()
	c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	sp, reason, _ := testMQTTReadConnAckV5(t, r3)
	if reason != mqttReasonSuccess {
		t.Fatalf("retry CONNACK reason=%x", reason)
	}
	if !sp {
		t.Fatal("the retry reports Session Present 0: the refused resume destroyed the session state")
	}
	testReconcileQoS1Publish(t, o, topic, []byte("survived"))
	qos, pi := testMQTTReadPublishV5(t, r3, topic, []byte("survived"))
	if qos != 1 || pi == 0 {
		t.Fatalf("delivery qos=%d pi=%d, want a QoS 1 delivery with a packet identifier", qos, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c3, pi)
	testMQTTFlush(t, c3, nil, r3)
}

// ---------------------------------------------------------------------------
// A gated delivery that is never released is still a FIRST transmission
// ---------------------------------------------------------------------------

// Review finding: a delivery that runs out its phase-gate wait, or whose filter
// the sweep reclaims unadopted, was skipped with no record that it had never
// been transmitted — so JetStream's next attempt (delivery count > 1) reached
// the client with DUP set on what was, for the client, the first send.
//
// The skip now records the source the way an in-flight-cap withholding is
// recorded, which is exactly what makes trackPublish clear DUP on the attempt
// that finally goes out.
//
// Mutation check: drop the sess.deferred record in mqttDeliverMsgCbQoS12 and
// the redelivery arrives with DUP set, failing this test.
func TestMQTTReconcileUnreleasedGatedDeliveryIsFirstTransmission(t *testing.T) {
	o := testMQTTDefaultOptions()
	// Short, so the redelivery of the skipped attempt lands inside the test.
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "gate-first", "gate/first", "gate.first"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	dur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// A message waiting in the backlog for the resume.
	testReconcileQoS1Publish(t, o, topic, []byte("backlog"))

	// Hold the phase open past the gate wait: the lookup hook sleeps while the
	// consumer's delivery is parked on the gate, so the wait expires and the
	// attempt is skipped — the exact case the finding names.
	restoreWait := mqttPhaseGateWaitMax
	mqttPhaseGateWaitMax = 300 * time.Millisecond
	defer func() { mqttPhaseGateWaitMax = restoreWait }()

	asm := testMQTTGetGlobalASM(t, s)
	asm.mu.Lock()
	asm.testResumeReconcileLookup = func(durables []string) ([]*ConsumerInfo, []bool, error) {
		time.Sleep(900 * time.Millisecond) // longer than the gate wait above
		infos := make([]*ConsumerInfo, len(durables))
		oks := make([]bool, len(durables))
		for i, d := range durables {
			if d == dur {
				infos[i] = &ConsumerInfo{Name: d, Stream: mqttStreamName}
			}
			oks[i] = true
		}
		return infos, oks, nil
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testResumeReconcileLookup = nil
		asm.mu.Unlock()
	}()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The backlog message arrives on a later JetStream attempt, but it is the
	// client's FIRST sight of it: DUP must be 0 [MQTT-3.3.1-1].
	pi := testMQTTCheckPubMsgNoAck(t, c2, r2, topic, mqttPubQos1, []byte("backlog"))
	if pi == 0 {
		t.Fatal("delivery carried no packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)
}

// A refused resume must leave the RECORD describing the session's own lifetime,
// not the lifetime the refused CONNECT asked for.
//
// Review finding: restoring only memory does not converge. The close path saves
// exclusively for a finite interval, so a session put back to "never expire"
// kept the refused CONNECT's Clean=true / ExpiryInterval=0 in its record — and
// the next restore-from-record discarded it as a clean session, losing
// subscriptions and durables the client still believed it had.
//
// Mutation check: skip the save in restoreRefusedResumeLifetime and the
// restarted broker reports Session Present 0 here.
func TestMQTTReconcileRefusedResumePersistsRestoredLifetime(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A 3.1.1 persistent session: never-expire, which is exactly the case the
	// close path does NOT save for.
	const cid, topic, sid = "life-record", "life/record", "life.record"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	asm := testMQTTGetGlobalASM(t, s)
	asm.mu.Lock()
	asm.testResumeReconcileLookup = func([]string) ([]*ConsumerInfo, []bool, error) {
		return nil, nil, errors.New("simulated verification outage")
	}
	asm.mu.Unlock()

	// A v5 reconnect asking for Session Expiry Interval 0 — refused.
	c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTExpectDisconnect(t, c2)
	testReconcileClientGone(t, s, cid)

	// The RECORD describes the session as it was, so a broker that restarts and
	// restores from it does not read a clean, zero-expiry session.
	ps, _ := testReconcileRecord(t, asm, cid)
	if ps.ExpiryInterval != mqttSessionNeverExpire || ps.Clean {
		t.Fatalf("record after a refused resume: ExpiryInterval=%d Clean=%v, want the persistent lifetime restored",
			ps.ExpiryInterval, ps.Clean)
	}
	if cc := ps.Cons[sid]; cc == nil || cc.Durable != oldDur {
		t.Fatalf("the record names %+v, want the pre-CONNECT durable %q", cc, oldDur)
	}

	// Prove it end to end: restart (the only way to force a pure restore from
	// that record) and resume.
	asm.mu.Lock()
	asm.testResumeReconcileLookup = nil
	asm.mu.Unlock()
	s = testReconcileRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c3, nil, r3)
	testReconcileQoS1Publish(t, o, topic, []byte("still here"))
	testReconcileExpectQoS1(t, c3, r3, topic, []byte("still here"))
}

// A repair that FAILS must leave no never-transmitted marker behind for the
// consumer it deleted.
//
// Review finding: a gated backlog delivery records itself in sess.deferred so
// its eventual redelivery is not marked DUP. If the repair then fails, undo
// deletes that fresh consumer — and nothing will ever deliver on it again, so
// the marker can never be consumed. Every retried repair mints another
// identity to strand, growing the map for the life of the session.
//
// Mutation checks: drop undo's purge (M24) and the marker survives; drop the
// disown-before-release ordering and a delivery waking into the removal
// records a fresh one after the purge.
func TestMQTTReconcileFailedRepairStrandsNoDeferredMarker(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "strand", "strand/t", "strand.t"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Fail the repair SAVE definitively, after the replacement consumer exists
	// and has had a chance to push a delivery into the closed subscription.
	//
	// The message is published from INSIDE the save hook, which is the only
	// window in which the replacement can receive one: it is created DeliverNew,
	// so anything published before the repair is deliberately not replayed to
	// it (the documented delivery-start policy).
	var mu sync.Mutex
	var failed bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if failed {
			return nil
		}
		failed = true
		// Publish now: the replacement consumer exists and its (closed) delivery
		// subscription holds the interest, so this message is pushed straight
		// into the phase gate.
		go testReconcileQoS1Publish(t, o, topic, []byte("stranded"))
		// Then hold the repair open LONGER than the phase-gate wait (the AckWait
		// set above), so that delivery times out and records its
		// never-transmitted marker against the fresh consumer — the only way a
		// marker exists for a durable the undo below then deletes. A shorter
		// pause would leave it parked, and the undo would disown it before it
		// could record anything.
		time.Sleep(3 * time.Second)
		return errors.New("simulated definite repair-save failure")
	})
	defer restoreSave()

	testReconcileRefusedConnect(t, o, cid)
	testReconcileClientGone(t, s, cid)
	mu.Lock()
	fired := failed
	mu.Unlock()
	if !fired {
		t.Fatal("the repair-save failure never fired; the test proved nothing")
	}
	restoreSave()

	// No marker may survive for a consumer that no longer exists. Polled: a
	// delivery woken by the undo's gate release is racing this assertion, and
	// the point of the disown-before-release ordering is that it cannot add one.
	sess := testMQTTResidentSession(t, s, cid)
	deadline := time.Now().Add(2 * time.Second)
	for {
		sess.mu.Lock()
		var stranded []string
		for key := range sess.deferred {
			if key.jsDur != oldDur {
				stranded = append(stranded, key.jsDur)
			}
		}
		n := len(sess.deferred)
		sess.mu.Unlock()
		if len(stranded) > 0 {
			t.Fatalf("a never-transmitted marker survives for deleted consumer(s) %v; nothing can ever consume it", stranded)
		}
		if time.Now().After(deadline) {
			if n != 0 {
				t.Fatalf("sess.deferred still holds %d entries after a failed repair", n)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// And the session still works: the next resume repairs and delivers the
	// backlog, still as a first transmission.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)
	testReconcileQoS1Publish(t, o, topic, []byte("after-strand"))
	testReconcileExpectQoS1(t, c2, r2, topic, []byte("after-strand"))
}

// A refused resume whose lifetime write cannot be issued yet still converges
// the record once the earlier write resolves.
//
// Review finding: restoreRefusedResumeLifetime skipped its save while the
// session was already unresolved (save refuses at entry then), so the restored
// lifetime lived only in memory while the record still carried the refused
// CONNECT's — and the readback's continuations only reconciled the consumer
// replacements, never the lifetime. A restart in between would read the wrong
// one and discard the session.
//
// Mutation check: drop the lifetime restore's save and the record still carries the
// refused CONNECT's zero interval after resolution.
func TestMQTTReconcileUncertainRefusalStillRestoresLifetime(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "uncertain-life", "uncertain/life", "uncertain.life"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Make the REPAIR save ambiguous: the session is left unresolved, which is
	// exactly the state in which the lifetime write cannot be issued.
	var mu sync.Mutex
	var armed, suppressed bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		mu.Lock()
		if !armed {
			armed = true
			mqttTestSessStoreSuppressV.Store(true)
		}
		mu.Unlock()
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		mu.Lock()
		defer mu.Unlock()
		if armed && !suppressed {
			suppressed = true
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	defer restoreResult()
	defer mqttTestSessStoreSuppressV.Store(false)

	// The refused reconnect asks for expiry 0 — never granted.
	c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTExpectDisconnect(t, c2)
	testReconcileClientGone(t, s, cid)
	mu.Lock()
	fired := suppressed
	mu.Unlock()
	if !fired {
		t.Fatal("the ambiguous repair save never fired; the test proved nothing")
	}
	restoreSave()
	restoreResult()

	sess := testMQTTResidentSession(t, s, cid)
	if !sess.isSeqUncertain() {
		t.Fatal("the session should be unresolved after an ambiguous repair save")
	}

	// Resolve it the way the periodic pass does: the settlement carries the restored
	// lifetime into the record along with everything else those bytes marshalled.
	if err := asm.lockSession(sess, nil); err != nil {
		t.Fatalf("lockSession: %v", err)
	}
	res := s.resolveUncertainSession(asm, sess)
	asm.unlockSession(sess)
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}

	ps, _ := testReconcileRecord(t, asm, cid)
	if ps.ExpiryInterval != mqttSessionNeverExpire || ps.Clean {
		t.Fatalf("record after resolution: ExpiryInterval=%d Clean=%v, want the session's own persistent lifetime restored",
			ps.ExpiryInterval, ps.Clean)
	}
	// The resolution settled the repair forward — it re-attempted the repair
	// record's own bytes — so the record names the REPLACEMENT, and memory names
	// the same one. Record and memory agreeing is the load-bearing half: whichever
	// way the ambiguity settles, this filter must have exactly one durable identity.
	sess.mu.Lock()
	memDur := ""
	if cc := sess.cons[sid]; cc != nil {
		memDur = cc.Durable
	}
	sess.mu.Unlock()
	cc := ps.Cons[sid]
	if cc == nil || cc.Durable == oldDur {
		t.Fatalf("record names %+v after a settled repair, want the replacement (old %q)", cc, oldDur)
	}
	if memDur != cc.Durable {
		t.Fatalf("record names %q but memory names %q: the settlement left them disagreeing", cc.Durable, memDur)
	}
}

// Retry exhaustion is classified by a final re-read, not assumed local.
//
// Review finding: every CAS rejection inside the retry loop continued, and the
// loop then returned local-definite without ever classifying the last one — so a
// 10071 on the final attempt (another broker took the session over) undid our
// work and left a session we no longer own mapped and bound.
//
// Mutation check: return mqttRepairSaveLocalDefinite at the end of the loop
// instead of re-reading, and the session is not foreign-disposed here.
func TestMQTTReconcileContendedRetryExhaustionClassifiesForeign(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "contend-exhaust", "contend/exhaust", "contend.exhaust"
	hash := getHash(cid)
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)

	// Every repair-save attempt is rejected as clustered contention (10164), and
	// on the LAST one another broker's write lands — so the final re-read finds a
	// record that is not ours.
	var mu sync.Mutex
	var attempts int
	var armed bool
	// Armed only for the REPAIR save (and the retries that follow it, which do
	// not pass through this hook): rewriting the accept save's result too would
	// fail the CONNECT before the phase ever runs.
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		mu.Lock()
		armed = true
		mu.Unlock()
		mqttTestSessStoreSuppressV.Store(true)
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		mu.Lock()
		if !armed {
			mu.Unlock()
			return resp, err
		}
		attempts++
		n := attempts
		mu.Unlock()
		mqttTestSessStoreSuppressV.Store(false)
		// One store for the original save plus one per retry: the takeover must
		// land on the LAST retry, so the loop exits with its rejection
		// unclassified and only the post-loop re-read can decide.
		if n >= mqttRepairSaveAttempts+1 {
			smsg, lerr := asm.jsa.loadSessionMsg(asm.domainTk, hash)
			if lerr == nil {
				ps := &mqttPersistedSession{}
				if json.Unmarshal(smsg.Data, ps) == nil {
					ps.Origin = "elsewhere"
					b, _ := json.Marshal(ps)
					_, _ = asm.jsa.storeMsg(mqttSessStreamSubjectPrefix+asm.domainTk+hash, -1, b)
				}
			}
		}
		mqttTestSessStoreSuppressV.Store(true)
		return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
	})
	defer restoreResult()
	defer mqttTestSessStoreSuppressV.Store(false)

	testReconcileRefusedConnect(t, o, cid)
	testReconcileClientGone(t, s, cid)
	restoreSave()
	restoreResult()
	mqttTestSessStoreSuppressV.Store(false)
	mu.Lock()
	got := attempts
	mu.Unlock()
	if got < mqttRepairSaveAttempts+1 {
		t.Fatalf("only %d repair-save attempts were made, want the retries exhausted (%d)", got, mqttRepairSaveAttempts+1)
	}

	// Foreign, not local: the session another broker now owns is evicted rather
	// than left mapped as ours.
	asm.mu.RLock()
	_, resident := asm.sessions[cid]
	asm.mu.RUnlock()
	if resident {
		t.Fatal("a session taken over during the retries stayed mapped: the exhausted retry was classified local, not foreign")
	}
}

// A delivery gated for a SURVIVING consumer is still recorded as never
// transmitted when restoration skips its filter and the sweep reclaims it.
//
// Review finding: disowning every phase subscription before opening its gate is
// right only for a durable that is being deleted. For a verified consumer that
// survives — the sweep leaves the consumer alone, it just withdraws the
// interest — the message is still owed, so the woken delivery must record
// itself or its eventual redelivery reaches the client marked DUP on first
// sight, contradicting what CONFORMANCE promises for swept filters.
//
// Mutation check: disown unconditionally (M27) and the redelivery below arrives
// with DUP set.
func TestMQTTReconcileSweptSurvivorStillRecordsDelivery(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	// Comfortably longer than the wait inside the verification below, so the
	// delivery parked on the gate is released by the SWEEP rather than by its own
	// bound — the timeout path records a marker of its own and would mask what
	// this test is about. It also means the redelivery below can only be prompt
	// if the marker exists: resendPendingRedelivery NAKs recorded withholdings,
	// while an unrecorded one waits out this AckWait.
	o.MQTT.AckWait = 10 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "swept-marker", "swept/marker", "swept.marker"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 2)
	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	pubRelDur := sess.pubRelConsumer.Durable
	sess.mu.Unlock()
	c.Close()
	testReconcileClientGone(t, s, cid)

	// The same lever T15 uses to make restoration SKIP a collected filter: its
	// PUBREL consumer is gone and the budget cannot recreate it.
	asm := testMQTTGetGlobalASM(t, s)
	if _, err := asm.jsa.deleteConsumer(mqttOutStreamName, pubRelDur, true); err != nil {
		t.Fatalf("delete the PUBREL durable: %v", err)
	}
	sess.mu.Lock()
	sess.tmaxack = mqttMaxAckTotalLimit
	sess.mu.Unlock()

	// Publish from inside the verification, so the message is pushed into the
	// phase's still-closed subscription — the gated window this test is about.
	asm.mu.Lock()
	asm.testResumeReconcileLookup = func(durables []string) ([]*ConsumerInfo, []bool, error) {
		testReconcileQoS1Publish(t, o, topic, []byte("gated"))
		// Generous: the delivery must provably reach the gate before the
		// verification returns and the sweep runs. Under parallel load a short
		// wait lets the sweep win, and the message is then pushed at a dead
		// subject instead of being recorded — which is a test artifact, not the
		// behavior under test.
		time.Sleep(2 * time.Second)
		infos := make([]*ConsumerInfo, len(durables))
		oks := make([]bool, len(durables))
		for i, d := range durables {
			infos[i], oks[i] = &ConsumerInfo{Name: d, Stream: mqttStreamName}, true
		}
		return infos, oks, nil
	}
	asm.mu.Unlock()

	c2, _ := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	// The CONNACK is read off the wire by the dial helper. The sweep is fenced by
	// its own observable effect — the phase subscription is gone — rather than by
	// a PING round trip, which would race the very delivery this test is about.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		cc := sess.cons[sid]
		sess.mu.Unlock()
		if cc == nil {
			return fmt.Errorf("no consumer entry for %q", sid)
		}
		client := testMQTTGetClient(t, s, cid)
		if client == nil {
			return fmt.Errorf("no client yet")
		}
		client.mu.Lock()
		sub := client.subs[cc.DeliverSubject]
		client.mu.Unlock()
		if sub != nil {
			return fmt.Errorf("the unadopted phase subscription is still installed")
		}
		return nil
	})
	c2.Close()
	testReconcileClientGone(t, s, cid)

	// Budget and lookup back to normal; the next CONNECT restores the filter and
	// the message is redelivered — as the client's FIRST sight of it.
	asm.mu.Lock()
	asm.testResumeReconcileLookup = nil
	asm.mu.Unlock()
	sess.mu.Lock()
	sess.tmaxack = 0
	sess.mu.Unlock()

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	// The property under test is the flags on THIS delivery: recorded as
	// withheld, so it is the client's first sight of the message. Nothing is
	// asserted about what follows it — an unacknowledged copy may still be in
	// flight behind it — so the test ends on the acknowledgement rather than on
	// a PING that a second copy could answer first.
	pi := testMQTTCheckPubMsgNoAck(t, c3, r3, topic, mqttPubQos1, []byte("gated"))
	if pi == 0 {
		t.Fatal("delivery carried no packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c3, pi)
}

// A lifetime restore rejected by clustered contention (10164) is retried, not
// abandoned.
//
// Review finding: the restore save treated both CAS kinds as terminal, so
// transient contention left the record carrying the refused CONNECT's
// Clean=true / zero interval while memory held the restored one — and a restart
// before any later write read the record as disposable and discarded the
// session.
//
// A takeover discovered while a refused resume is being unwound hands the
// session over instead of restoring it locally.
//
// Review finding: the lifetime restore's own save can be the thing that proves
// another broker took the session — and the code merely logged, then detached
// and armed a local expiry timer for a session it no longer owned, which is the
// stale-write posture disposeForeignSessionLocked exists to prevent.
//
// A successful repair leaves no never-transmitted marker naming the durable it
// retired.
//
// Review finding: a delivery that ran out its gate wait while the OLD durable
// was still alive records a marker naming it; the lookup then reports that
// durable absent and the repair retires it. Nothing can ever consume that
// marker — the consumer will never deliver again — and resendPendingRedelivery
// now keeps markers when it NAKs them, so it would NAK a dead ack subject on
// every resume.
//
// Mutation check: drop the purge beside the charge refund and the marker
// survives.
func TestMQTTReconcileRepairPurgesRetiredDurableMarkers(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second // also the phase-gate bound
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "retire-marker", "retire/marker", "retire.marker"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// The old durable is left ALIVE so it can actually push into the gate; the
	// lookup then reports it absent, which is what drives the repair. That is
	// the real race — a durable reaped between the subscribe and the answer.
	asm := testMQTTGetGlobalASM(t, s)
	asm.mu.Lock()
	asm.testResumeReconcileLookup = func(durables []string) ([]*ConsumerInfo, []bool, error) {
		testReconcileQoS1Publish(t, o, topic, []byte("in-flight"))
		time.Sleep(2 * time.Second) // past the gate bound: the delivery records itself
		infos := make([]*ConsumerInfo, len(durables))
		oks := make([]bool, len(durables))
		for i := range durables {
			oks[i] = true // absent
		}
		return infos, oks, nil
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testResumeReconcileLookup = nil
		asm.mu.Unlock()
	}()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c2, nil, r2)

	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	var stranded []uint64
	for key := range sess.deferred {
		if key.jsDur == oldDur {
			stranded = append(stranded, key.sseq)
		}
	}
	newDur := ""
	if cc := sess.cons[sid]; cc != nil {
		newDur = cc.Durable
	}
	sess.mu.Unlock()
	if newDur == oldDur {
		t.Fatalf("the repair did not run; the test proved nothing (durable still %q)", oldDur)
	}
	if len(stranded) > 0 {
		t.Fatalf("markers %v survive for retired durable %q; nothing can ever consume them", stranded, oldDur)
	}
}

// A GRANTED resume persists the lifetime it was granted, so memory and record
// never disagree about the terms the session is running under.
//
// The requested Session Expiry Interval is deliberately not applied at rebind —
// that is what makes a refused resume harmless — so the grant point owns both
// applying it and writing it. Without that write the record would keep the
// PREVIOUS interval while the session ran under the new one, and a restart (or
// another broker) would resume, or expire, the session on the wrong terms.
//
// Mutation check: skip the grant-point save and the record below still carries
// the old interval.
func TestMQTTReconcileGrantedResumePersistsRequestedLifetime(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic = "grant-life", "grant/life"

	// A v5 session that never expires.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(mqttSessionNeverExpire)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || sp {
		t.Fatalf("v5 CONNACK: sp=%v reason=%x", sp, reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, c, nil, r)
	c.Close()
	testReconcileClientGone(t, s, cid)

	asm := testMQTTGetGlobalASM(t, s)
	if ps, _ := testReconcileRecord(t, asm, cid); ps.ExpiryInterval != mqttSessionNeverExpire {
		t.Fatalf("record before the reconnect has ExpiryInterval=%d, want never-expire", ps.ExpiryInterval)
	}

	// Resume asking for a FINITE interval: a real change of terms.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(1800)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r2); reason != mqttReasonSuccess || !sp {
		t.Fatalf("resume CONNACK: sp=%v reason=%x", sp, reason)
	}
	testMQTTFlush(t, c2, nil, r2)

	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	memExpiry := sess.expiryInterval
	sess.mu.Unlock()
	if memExpiry != 1800 {
		t.Fatalf("session runs under ExpiryInterval=%d, want the granted 1800", memExpiry)
	}
	ps, _ := testReconcileRecord(t, asm, cid)
	if ps.ExpiryInterval != 1800 {
		t.Fatalf("record carries ExpiryInterval=%d while the session runs under %d; a restart would resume it on the wrong terms",
			ps.ExpiryInterval, memExpiry)
	}
}

// A CLEARED session takes the requested lifetime immediately.
//
// Review finding: deferring the lifetime to the grant point was guarded on
// sessp, but a session cleared by Clean Start (or a legacy clean session, or an
// overdue record) is not resumed — and clear() does not reset expiryInterval or
// clean, so it kept the PREVIOUS session's terms. A client that clean-starts
// with Session Expiry Interval 0 would have had its new session outlive the
// connection it asked to end.
//
// Mutation check: drop the !sessp assignment at rebind and the session below
// keeps the old never-expire terms.
func TestMQTTReconcileClearedSessionTakesRequestedLifetime(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic = "cleared-life", "cleared/life"

	// A never-expiring session first.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(mqttSessionNeverExpire)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("v5 CONNACK reason=%x", reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, c, nil, r)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// Clean Start discards it and builds a fresh session that must end when this
	// connection does.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r2); reason != mqttReasonSuccess || sp {
		t.Fatalf("clean-start CONNACK: sp=%v reason=%x", sp, reason)
	}
	testMQTTFlush(t, c2, nil, r2)

	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	gotExpiry, gotClean := sess.expiryInterval, sess.clean
	sess.mu.Unlock()
	if gotExpiry != 0 || !gotClean {
		t.Fatalf("cleared session runs with expiry=%d clean=%v, want the requested 0/true", gotExpiry, gotClean)
	}
	asm := testMQTTGetGlobalASM(t, s)
	if ps, _ := testReconcileRecord(t, asm, cid); ps.ExpiryInterval != 0 {
		t.Fatalf("record carries ExpiryInterval=%d for a clean-start session, want 0", ps.ExpiryInterval)
	}

	// And it really does end at close: the next connect finds no session.
	c2.Close()
	testReconcileClientGone(t, s, cid)
	c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r3); reason != mqttReasonSuccess || sp {
		t.Fatalf("after a zero-expiry session closed: sp=%v reason=%x, want Session Present 0", sp, reason)
	}
}

// An ambiguous lifetime grant reaches the teardown decision ONLY on proof, and
// then it reaches it completely.
//
// Review finding: keeping the requested terms in memory while the write was
// unresolved meant the close path snapshotted THEM into the deferred disconnect
// (mqttDeferDisconnect), and applyDeferredDisconnect acts on that snapshot — so
// a requested interval of 0 whose outcome was unknown discarded a persistent
// session on nothing but a hopeful write. The parked action therefore carries the
// CONFIRMED lifetime, and only a settlement that proves the grant durable
// retargets it (retargetDeferredLifetime).
//
// Once that proof exists the granted terms DO apply, refused CONNACK or not: the
// lifetime is granted at the grant point, and the resolver settles an ambiguity by
// making exactly those bytes durable rather than by guessing them dead. The record
// then says "ends at disconnect", and the discharge honours it — which is the same
// outcome a landed original with a lost response always had.
//
// Mutation check: drop retargetDeferredLifetime from the grant's landed
// continuation and the parked action keeps a lifetime the record no longer states.
func TestMQTTReconcileAmbiguousGrantAppliesOnlyOnProof(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic = "grant-ambig", "grant/ambig"

	// A never-expiring v5 session with a subscription.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(mqttSessionNeverExpire)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("v5 CONNACK reason=%x", reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, c, nil, r)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// The reconnect asks for interval 0 — "end my session at this close" — and
	// its GRANT save is made ambiguous: issued, outcome unknown. The grant save
	// is the one that runs with the requested interval already applied, which is
	// what tells it apart from the accept save.
	var mu sync.Mutex
	var armed, suppressed bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid {
			return nil
		}
		sess.mu.Lock()
		granting := sess.expiryInterval == 0
		sess.mu.Unlock()
		if !granting {
			return nil
		}
		mu.Lock()
		if !armed {
			armed = true
			mqttTestSessStoreSuppressV.Store(true)
		}
		mu.Unlock()
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		mu.Lock()
		defer mu.Unlock()
		if armed && !suppressed {
			suppressed = true
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	defer restoreResult()
	defer mqttTestSessStoreSuppressV.Store(false)

	c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTExpectDisconnect(t, c2)
	testReconcileClientGone(t, s, cid)
	restoreSave()
	restoreResult()
	mu.Lock()
	fired := suppressed
	mu.Unlock()
	if !fired {
		t.Fatal("the ambiguous grant save never fired; the test proved nothing")
	}

	sess := testMQTTResidentSession(t, s, cid)
	if !sess.isSeqUncertain() {
		t.Fatal("the session should be unresolved after an ambiguous grant save")
	}
	// BEFORE any proof: the teardown the close path parked was captured against the
	// CONFIRMED lifetime, not the interval this refused CONNECT asked for. This is
	// the half that must never depend on a hopeful write.
	sess.mu.Lock()
	parked := sess.deferredDisconnect
	memExpiry := sess.expiryInterval
	sess.mu.Unlock()
	if parked == nil {
		t.Fatal("the close of an unresolved session must park its teardown")
	}
	if parked.expiryInterval != mqttSessionNeverExpire {
		t.Fatalf("the parked teardown carries ExpiryInterval=%d, want the CONFIRMED %d: an unproven grant reached the teardown decision",
			parked.expiryInterval, mqttSessionNeverExpire)
	}
	if memExpiry != mqttSessionNeverExpire {
		t.Fatalf("memory carries ExpiryInterval=%d while the grant is unresolved, want the confirmed %d",
			memExpiry, mqttSessionNeverExpire)
	}

	asm := testMQTTGetGlobalASM(t, s)
	if err := asm.lockSession(sess, nil); err != nil {
		t.Fatalf("lockSession: %v", err)
	}
	res := s.resolveUncertainSession(asm, sess)
	asm.unlockSession(sess)
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}

	// AFTER the proof: the grant's own bytes are durable, so the record states
	// "ends at disconnect" and the discharge honours it. The session is gone, and a
	// retry gets a clean one — the grant-at-durability outcome, reached because the
	// write is now settled rather than guessed.
	asm.mu.RLock()
	_, resident := asm.sessions[cid]
	asm.mu.RUnlock()
	if resident {
		t.Fatal("a grant PROVEN durable ends the session at its close: the discharge must have discarded it")
	}
	if _, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid)); err == nil {
		t.Fatal("the discarded session must leave no record")
	} else if isErrorOtherThan(err, JSNoMessageFoundErr) {
		t.Fatalf("reading the discarded session's record: %v", err)
	}

	c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(mqttSessionNeverExpire)}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r3); reason != mqttReasonSuccess || sp {
		t.Fatalf("retry after a granted zero interval: sp=%v reason=%x, want Session Present 0", sp, reason)
	}
}

// The LANDED half of an ambiguous lifetime grant: a grant that really did
// reach the stream must take effect on the teardown the close path parked
// while its outcome was unknown.
//
// Review finding: the not-landed case was fixed by keeping the CONFIRMED terms,
// but that left the landed case inconsistent — mqttDeferDisconnect had already
// snapshotted the old interval, and applyDeferredDisconnect decides discard and
// timer behavior from that snapshot rather than from the session, so the
// outcome depended on whether the close or the readback ran first.
//
// Mutation check: drop the retargeting in the landed continuation and the
// session below survives a granted interval of 0.
func TestMQTTReconcileLandedAmbiguousGrantAppliesToTeardown(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic = "grant-landed", "grant/landed"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(mqttSessionNeverExpire)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("v5 CONNACK reason=%x", reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, c, nil, r)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// The grant save LANDS, but its response is lost: the store goes through and
	// only its result is rewritten. That is the half the previous test could not
	// produce.
	var mu sync.Mutex
	var lost bool
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid || err != nil || resp == nil {
			return resp, err
		}
		sess.mu.Lock()
		granting := sess.expiryInterval == 0
		sess.mu.Unlock()
		mu.Lock()
		defer mu.Unlock()
		if !granting || lost {
			return resp, err
		}
		lost = true
		return nil, errors.New("simulated lost grant-save response")
	})
	defer restoreResult()

	// Requested: "end my session when this connection closes".
	c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTExpectDisconnect(t, c2)
	testReconcileClientGone(t, s, cid)
	restoreResult()
	mu.Lock()
	fired := lost
	mu.Unlock()
	if !fired {
		t.Fatal("the lost-response injection never fired; the test proved nothing")
	}

	sess := testMQTTResidentSession(t, s, cid)
	asm := testMQTTGetGlobalASM(t, s)
	if err := asm.lockSession(sess, nil); err != nil {
		t.Fatalf("lockSession: %v", err)
	}
	res := s.resolveUncertainSession(asm, sess)
	asm.unlockSession(sess)
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}

	// The grant landed, so the session really was granted "ends at close" — and
	// the teardown parked before that was known must honor it.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		asm.mu.RLock()
		_, resident := asm.sessions[cid]
		asm.mu.RUnlock()
		if resident {
			return fmt.Errorf("the session is still resident despite a granted interval of 0")
		}
		if _, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid)); err == nil {
			return fmt.Errorf("the session record still exists despite a granted interval of 0")
		}
		return nil
	})
}

// A teardown parked while a write was unresolved is discharged once that write
// resolves, even when the resolution comes from an off-readLoop resolver.
//
// Review finding: resolveUncertainNow acted only on a FOREIGN verdict. After
// "ours" it cleared the uncertainty and left any parked deferred disconnect in
// place — and from that moment nothing would ever discharge it, because the
// periodic pass skips a session that is no longer uncertain and the next
// CONNECT skips resolution for the same reason. The session would never arm its
// expiry and never publish its Will.
//
// Mutation check: drop the dischargeDeferredIfUnbound call and the session below
// still holds its parked action, with no expiry timer, after resolution.
func TestMQTTReconcileResolvedSessionDischargesParkedTeardown(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "park-discharge", "park/discharge", "park.discharge"
	hash := getHash(cid)

	// A finite-expiry v5 session, so the discharge has an observable product:
	// a persisted disconnect time and an armed expiry timer.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("v5 CONNACK reason=%x", reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, c, nil, r)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// Reconnect and make the REPAIR save ambiguous, so the session is unresolved
	// when the refused connection closes and its teardown is parked.
	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)
	var mu sync.Mutex
	var armed, suppressed bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		mu.Lock()
		if !armed {
			armed = true
			mqttTestSessStoreSuppressV.Store(true)
		}
		mu.Unlock()
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		mu.Lock()
		defer mu.Unlock()
		if armed && !suppressed {
			suppressed = true
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	defer restoreResult()
	defer mqttTestSessStoreSuppressV.Store(false)

	c2, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTExpectDisconnect(t, c2)
	testReconcileClientGone(t, s, cid)
	restoreSave()
	restoreResult()
	mu.Lock()
	fired := suppressed
	mu.Unlock()
	if !fired {
		t.Fatal("the ambiguous repair save never fired; the test proved nothing")
	}

	sess := testMQTTResidentSession(t, s, cid)
	if !sess.isSeqUncertain() {
		t.Fatal("the session should be unresolved after an ambiguous repair save")
	}

	// Resolve the way an off-readLoop writer does — the path that used to leave
	// the parked teardown behind forever.
	sess.resolveUncertainNow(asm.jsa.c)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		parked := sess.deferredDisconnect != nil
		disconnectedAt := sess.disconnectedAt
		sess.mu.Unlock()
		if parked {
			return fmt.Errorf("the parked teardown was never discharged; nothing else will ever do it")
		}
		if disconnectedAt == 0 {
			return fmt.Errorf("the discharge did not record the disconnect time")
		}
		asm.mu.RLock()
		_, timer := asm.pendingExpiries[hash]
		asm.mu.RUnlock()
		if !timer {
			return fmt.Errorf("the discharge did not arm the finite expiry")
		}
		return nil
	})
}

// Resolution and discharge are ONE step: an off-readLoop resolver that cannot
// take the ownership gate must leave the session uncertain rather than resolve
// it and hand the parked teardown to nobody.
//
// Review finding: clearing the uncertainty first leaves a window in which a
// reconnect binds — and that reconnect skips resolution precisely because the
// session is no longer uncertain, so the action is stranded permanently. The
// gate is what makes the pair atomic, and losing it means a CONNECT is already
// in flight and will resolve (and discharge) the session itself.
//
// Mutation check: resolve without taking the gate and this test sees the
// session resolved with its teardown still parked.
func TestMQTTReconcileResolveDefersToAnInFlightConnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic, sid = "resolve-gate", "resolve/gate", "resolve.gate"
	c, _ := testReconcileSubscribe(t, o, cid, topic, 1)
	oldDur := testReconcileConsDur(t, s, cid, sid)
	c.Close()
	testReconcileClientGone(t, s, cid)

	// Park a teardown behind an ambiguous repair save, exactly as the previous
	// test does.
	asm := testMQTTGetGlobalASM(t, s)
	testReconcileDeleteConsumer(t, asm, oldDur)
	var mu sync.Mutex
	var armed, suppressed bool
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid || !testReconcileIsRepairSave(sess, sid, oldDur) {
			return nil
		}
		mu.Lock()
		if !armed {
			armed = true
			mqttTestSessStoreSuppressV.Store(true)
		}
		mu.Unlock()
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess.id != cid {
			return resp, err
		}
		mu.Lock()
		defer mu.Unlock()
		if armed && !suppressed {
			suppressed = true
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	defer restoreResult()
	defer mqttTestSessStoreSuppressV.Store(false)

	testReconcileRefusedConnect(t, o, cid)
	testReconcileClientGone(t, s, cid)
	restoreSave()
	restoreResult()
	mu.Lock()
	fired := suppressed
	mu.Unlock()
	if !fired {
		t.Fatal("the ambiguous repair save never fired; the test proved nothing")
	}

	sess := testMQTTResidentSession(t, s, cid)
	sess.mu.Lock()
	parked := sess.deferredDisconnect != nil
	sess.mu.Unlock()
	if !parked {
		t.Fatal("the close should have parked a teardown while the session was unresolved")
	}

	// Hold the ownership gate, standing in for a CONNECT in flight.
	if err := asm.lockSession(sess, nil); err != nil {
		t.Fatalf("lockSession: %v", err)
	}
	res := sess.resolveUncertainNow(asm.jsa.c)
	asm.unlockSession(sess)

	if res != mqttUncertainUnresolved {
		t.Fatalf("resolution = %v while the gate was held, want unresolved: resolving here would strand the parked teardown", res)
	}
	if !sess.isSeqUncertain() {
		t.Fatal("the session was resolved without the gate; a reconnect binding now would never discharge its teardown")
	}
	sess.mu.Lock()
	stillParked := sess.deferredDisconnect != nil
	sess.mu.Unlock()
	if !stillParked {
		t.Fatal("the parked teardown was consumed by a resolver that did not hold the gate")
	}

	// With the gate free, the same call resolves and discharges.
	sess.resolveUncertainNow(asm.jsa.c)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.deferredDisconnect != nil {
			return fmt.Errorf("the teardown is still parked after an ungated resolution")
		}
		return nil
	})
}

// The ownership gate is taken UNCONDITIONALLY, not only when a teardown is
// already parked.
//
// Review finding: sampling "is anything parked?" and gating on the answer is a
// check-then-act. A close holding that same gate can park an action and unbind
// the session immediately afterwards, and the ungated resolver then clears the
// uncertainty and discharges (or strands) it with no serialization against a
// reconnect. Whether a discharge will be needed is simply not knowable before
// the resolution.
//
// This pins the case the unbound test cannot: a session that is BOUND when the
// resolver samples it. With the gate held, resolution must defer.
//
// Mutation check: gate only when something is already parked (the sampled
// version) and this test resolves through a held gate.
func TestMQTTReconcileResolveAlwaysTakesTheGate(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, topic = "gate-always", "gate/always"
	c, r := testReconcileSubscribe(t, o, cid, topic, 1)
	defer c.Close()
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, cid)
	asm := testMQTTGetGlobalASM(t, s)

	// A BOUND session carrying an unresolved write, and nothing parked: the
	// state in which the sampled version skips the gate entirely.
	sess.mu.Lock()
	bound := sess.c
	testMarkUncertainLocked(sess, sess.seq, []byte("an attempt that did not land"))
	parked := sess.deferredDisconnect
	sess.mu.Unlock()
	if bound == nil || parked != nil {
		t.Fatalf("precondition: want a bound session with nothing parked (bound=%v parked=%v)", bound != nil, parked != nil)
	}

	// Somebody else holds the gate — a CONNECT, a teardown, an expiry pass.
	if err := asm.lockSession(sess, bound); err != nil {
		t.Fatalf("lockSession: %v", err)
	}
	res := sess.resolveUncertainNow(asm.jsa.c)
	asm.unlockSession(sess)

	if res != mqttUncertainUnresolved {
		t.Fatalf("resolution = %v while the gate was held, want unresolved: resolving here is not serialized against a close parking a teardown", res)
	}
	if !sess.isSeqUncertain() {
		t.Fatal("the write was resolved without the ownership gate")
	}

	// With the gate free it resolves normally.
	if res := sess.resolveUncertainNow(asm.jsa.c); res != mqttUncertainOurs {
		t.Fatalf("resolution with a free gate = %v, want ours", res)
	}
	if sess.isSeqUncertain() {
		t.Fatal("the session is still uncertain after an ungated resolution")
	}
}

// A close that loses the ownership gate to an uncertain-write resolver parks
// its teardown instead of abandoning it.
//
// Review finding: the resolver now holds the gate across a JetStream readback
// (up to the request timeout), while the close path retried for only a second
// and read exhaustion as "another client rebound" — silently dropping the whole
// teardown of a session that was still its own: no detach, no persisted
// disconnect time, no expiry timer, no Will. Contention and rebind are
// different verdicts, and only the second forfeits the teardown.
//
// Mutation check: treat exhaustion as rebind again (return after
// mqttPublishOwedWill without the owner check) and the session below is never
// detached and never gets its expiry armed.
func TestMQTTReconcileCloseParksTeardownWhenResolverHoldsGate(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "gate-close"
	hash := getHash(cid)

	// A BOUND finite-expiry session, so the discharged teardown has observable
	// products: a disconnect time and an armed expiry timer.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("v5 CONNACK reason=%x", reason)
	}
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, cid)
	asm := testMQTTGetGlobalASM(t, s)

	// An unresolved write whose readback will classify OURS (the base matches
	// the record), so resolution ends in a discharge rather than a disposition.
	sess.mu.Lock()
	testMarkUncertainLocked(sess, sess.seq, []byte("attempt that did not land"))
	sess.mu.Unlock()

	// Stall the resolver's readback well past the close path's bounded gate
	// retry, and signal once it is provably inside the gate.
	inReadback := make(chan struct{}, 1)
	restoreLoad := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if stream != mqttSessStreamName || !strings.Contains(subject, hash) {
			return nil
		}
		select {
		case inReadback <- struct{}{}:
		default:
		}
		time.Sleep(3 * time.Second)
		return nil
	})
	defer restoreLoad()

	done := make(chan mqttUncertainResolution, 1)
	go func() { done <- sess.resolveUncertainNow(asm.jsa.c) }()
	select {
	case <-inReadback:
	case <-time.After(10 * time.Second):
		t.Fatal("the resolver never reached its readback")
	}

	// The connection closes while the resolver holds the gate. The teardown must
	// classify the contention and PARK, not forfeit.
	c.Close()

	select {
	case res := <-done:
		if res != mqttUncertainOurs {
			t.Fatalf("resolution = %v, want ours", res)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the resolver never finished")
	}

	// The parked teardown was discharged by the resolver before it released the
	// gate: detached, disconnect time persisted, finite expiry armed.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		bound := sess.c != nil
		parked := sess.deferredDisconnect != nil
		disconnectedAt := sess.disconnectedAt
		sess.mu.Unlock()
		if bound {
			return fmt.Errorf("the closed client is still bound: the teardown was read as a rebind and dropped")
		}
		if parked {
			return fmt.Errorf("the parked teardown was never discharged")
		}
		if disconnectedAt == 0 {
			return fmt.Errorf("no disconnect time was persisted")
		}
		asm.mu.RLock()
		_, timer := asm.pendingExpiries[hash]
		asm.mu.RUnlock()
		if !timer {
			return fmt.Errorf("the finite expiry was never armed")
		}
		return nil
	})
}

// A park refused because the session is not uncertain restores the Will it
// consumed, so the ordinary teardown that follows still publishes it.
//
// The refusal path: the close loses the gate to some bounded holder, the
// session is NOT unresolved, so the atomic check declines to park and the close
// goes back to retaking the gate. Building the action consumed the Will
// (mqttTakeWillLocked); without putting it back, the ordinary teardown finds
// the disposition already discharged and the Will silently vanishes.
//
// Mutation checks: drop the restoration (the Will is never published) or park
// unconditionally (the action strands on a resolved session, taking the Will
// with it) — both leave the watcher below empty.
func TestMQTTReconcileRefusedParkRestoresTheWill(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid, willTopic = "park-will", "park/will"

	// A watcher for the Will topic.
	wc, wr := testMQTTConnect(t, &mqttConnInfo{clientID: "park-will-watch", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer wc.Close()
	testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, wc, wr, []*mqttFilter{{filter: willTopic, qos: 1}}, []byte{1})
	testMQTTFlush(t, wc, nil, wr)

	// A bound session carrying a Will, with NOTHING unresolved about it.
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true,
		will: &mqttWill{topic: []byte(willTopic), message: []byte("boom"), qos: 1}},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, cid)
	asm := testMQTTGetGlobalASM(t, s)
	sess.mu.Lock()
	bound := sess.c
	sess.mu.Unlock()
	if bound == nil {
		t.Fatal("precondition: the session must be bound")
	}

	// Hold the gate as the bound owner — a stand-in for any bounded holder —
	// long enough that the close exhausts at least one retry round, attempts the
	// park, is refused (not uncertain), restores the Will, and loops.
	if err := asm.lockSession(sess, bound); err != nil {
		t.Fatalf("lockSession: %v", err)
	}
	release := time.AfterFunc(2500*time.Millisecond, func() { asm.unlockSession(sess) })
	defer release.Stop()

	// An abrupt close: the Will is owed immediately.
	c.Close()

	// Once the gate frees, the ordinary teardown runs and the Will — restored by
	// every refused park along the way — is published.
	pi := testMQTTCheckPubMsgNoAck(t, wc, wr, willTopic, mqttPubQos1, []byte("boom"))
	if pi == 0 {
		t.Fatal("the Will arrived without a packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, wc, pi)
	testMQTTFlush(t, wc, nil, wr)

	// And nothing was left parked on the resolved session.
	sess.mu.Lock()
	parked := sess.deferredDisconnect != nil
	sess.mu.Unlock()
	if parked {
		t.Fatal("an action was parked on a session that was never unresolved")
	}
}
