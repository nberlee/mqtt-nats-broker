//go:build mqttport_fulltests

// Copyright 2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestLiveSubPlainQoS0IsNotGated is the boundary that must NOT move.
//
// A plain QoS 0 subscription acknowledges nothing on any group's behalf, so gating it would
// lose messages while protecting nobody. It must keep flowing while a shared SUBSCRIBE on
// the same connection is mid-arbitration, and it is asserted through the REAL delivery
// callback rather than through the gate helper.
//
// This kills the mutation that drops `shared &&` from the callback's gate check (M18).
//
// SCOPE, stated because a reader would otherwise assume more: the paired half — that a
// SHARED QoS 0 delivery IS dropped in the same window — is NOT asserted here. Shared
// subscriptions are an MQTT 5.0 feature and this fixture's 3.1.1 connection never receives
// one, so a negative assertion would pass for the wrong reason. That half is owed as a v5
// fixture; see the lane's RUN.md.
func TestLiveSubPlainQoS0IsNotGated(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0gate", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "plain/topic", qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)

	cc := testMQTTGetClient(t, s, "q0gate")
	sess := cc.mqtt.sess

	// A PENDING gate: precisely the window a live shared SUBSCRIBE opens between
	// installing interest and proving ownership.
	gate := sess.armSubGate(cc)
	defer gate.resolve(mqttSubGateOpen)
	if !sess.subGateBlocksQoS0(cc) {
		t.Fatal("a pending gate must report as blocking for this connection, or the " +
			"control below proves nothing")
	}

	pubc, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "q0pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pubc.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pubc, pubr, 0, false, false, "plain/topic", 0, []byte("control"))

	testMQTTCheckPubMsg(t, c, r, "plain/topic", 0, []byte("control"))
}

// TestLiveSubGateWithholdsQoS12AndRecordsIt pins the QoS 1/2 disposition through the real
// callback: the delivery is withheld, and it is RECORDED as never transmitted so the attempt
// that finally goes out is still a first send [MQTT-3.3.1-1].
//
// Waiting rather than dropping is what keeps the ordinary case out of F-072's
// DUP-on-a-first-transmission population; the marker is what makes a withheld attempt
// distinguishable from a delivered one.
func TestLiveSubGateWithholdsQoS12AndRecordsIt(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q12gate", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "gated/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	cc := testMQTTGetClient(t, s, "q12gate")
	sess := cc.mqtt.sess

	// A WITHHELD verdict, which is what every non-owning outcome leaves behind.
	sess.armSubGate(cc).resolve(mqttSubGateWithheld)

	if got := sess.awaitTransmissionGates(cc); got != mqttTransmissionWithheld {
		t.Fatalf("the QoS 1/2 wait returned %v, want withheld: a live connection's restore "+
			"gate is already open, so consulting it alone would transmit under interest "+
			"whose ownership was never proved", got)
	}
}

// TestLiveSubUnprovableOwnershipIsNotAcknowledged is the production-path regression for the
// deferred-result bug, and its oracle is a STORE COUNT rather than the wire output.
//
// THE BUG. mqttProcessSubs returned its arbitration result with UNNAMED results, so the value
// was copied at the return statement — before the defers ran — and the whole arbitration tail
// runs in a defer, because it must observe the packet transaction closed and the ownership
// gate still held. Every tail decision was therefore invisible to the caller:
// `settledOwnAmbiguity` never suppressed mqttResolveAfterPacket, so the generic resolver —
// whose forked arm DISPOSES — could reach an ambiguity the live tail deliberately retained;
// and an eventual Foreign disposal never reported `disposed`, so the parser emitted its 0x89
// footer after 0x8E had already been sent.
//
// WHY THE WIRE OUTPUT CANNOT BE THE ORACLE, which two earlier drafts of this test got wrong.
// On the unclassified arm mqttProcessSubs returns a non-nil ownership-unsettled error either
// way, so the broken build produces the SAME "no SUBACK, one 0x89" result. Asserting that
// passes against the defect. The observable difference is the number of session-record store
// ATTEMPTS:
//
//  1. the ownership proof itself, suppressed before it reaches the stream;
//  2. the arbitration tail's byte-identical reattempt, suppressed again, so nothing is
//     classified and the tail reports settledOwnAmbiguity;
//  3. ONLY IF THE FLAG IS LOST — mqttParse runs mqttResolveAfterPacket and the generic
//     resolver attempts a third.
//
// So exactly two attempts is the property, and three is the bug.
//
// WHY AN UNCHANGED RE-SUBSCRIBE. It makes the ownership proof the packet's ONLY ordinary
// record store, which is what lets the count mean anything: the early per-filter membership
// save writes nothing because the membership and its options are unchanged, the final update
// likewise, and the group-consumer create/verify are not session-record stores at all. It
// also drives the REFRESH arm, one of the three routes that raise the arbitration trigger.
//
// WHY SUPPRESSION RATHER THAN A REWRITTEN RESULT. Rewriting a successful store's response
// leaves the write actually LANDED, so the tail reads back its own exact bytes and classifies
// Ours immediately — which exercises the tail but leaves the generic resolver nothing to
// mishandle, and is therefore blind to this regression.
//
// Not parallel: both seams are package-global.
func TestLiveSubUnprovableOwnershipIsNotAcknowledged(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "tailoracle"
	const filter = "$share/tailgrp/tail/topic"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: true,
	}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	// Subscribe once, successfully, so the membership and the group durable exist.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("first SUBACK %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)

	// The hook COUNTS ONLY. Rewriting the outcome here would leave the write landed; the
	// suppression below is what makes the ambiguity genuine.
	var stores atomic.Int32
	h := func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != nil && sess.id == clientID {
			stores.Add(1)
		}
		return resp, err
	}
	mqttTestSessStoreResultHookV.Store(&h)
	defer mqttTestSessStoreResultHookV.Store(nil)

	var sawCAS atomic.Bool
	ph := func(b string) {
		if b == "after-ownership-cas" {
			sawCAS.Store(true)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&ph)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	mqttTestSessStoreSuppressV.Store(true)
	defer mqttTestSessStoreSuppressV.Store(false)

	// The identical re-SUBSCRIBE, sent WITHOUT waiting for a SUBACK: a refused packet sends
	// none, so a helper insisting on one would fail for the right reason and report the
	// wrong thing.
	vh := newMQTTWriter(0)
	vh.WriteUint16(2)
	vh.WriteVarInt(0)
	vh.WriteBytes([]byte(filter))
	vh.WriteByte(1)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, werr := testMQTTWrite(c, w.Bytes()); werr != nil {
		t.Fatalf("Error writing SUBSCRIBE: %v", werr)
	}

	pt, pl, ready := testMQTTReadPacketReady(r, 5*time.Second)

	// The fixture must have reached the route it claims to test.
	if !sawCAS.Load() {
		t.Fatal("the re-SUBSCRIBE never reached the ownership proof, so this fixture proves " +
			"nothing about the arbitration tail")
	}
	if !ready {
		t.Fatal("the broker neither acknowledged nor refused an unprovable ownership write")
	}

	switch pt & mqttPacketMask {
	case mqttPacketSubAck:
		t.Fatal("the SUBSCRIBE was ACKNOWLEDGED although its ownership proof could not be " +
			"classified: the client would believe its shared subscription is live, and this " +
			"broker would serve and ACKNOWLEDGE group deliveries under a membership it never " +
			"proved it still holds")
	case mqttPacketDisconnect:
		// The reason code IS the claim, and it must be the modest one: nothing here
		// demonstrated another owner, so 0x8E would assert a takeover on no evidence.
		start := r.pos
		if pl < 1 {
			t.Fatal("the DISCONNECT omitted its reason code")
		}
		if reason := r.buf[start]; reason != mqttReasonServerBusy {
			t.Fatalf("DISCONNECT reason = 0x%02x, want 0x89 server busy", reason)
		}
		r.pos = start + pl
	default:
		t.Fatalf("unexpected packet 0x%02x after an unprovable ownership write", pt&mqttPacketMask)
	}

	if pt2, _, more := testMQTTReadPacketReady(r, 250*time.Millisecond); more {
		t.Fatalf("a second packet 0x%02x followed the refusal: the connection's one "+
			"DISCONNECT claim must make this impossible", pt2&mqttPacketMask)
	}

	// THE REGRESSION ORACLE. Two attempts is the property; a third means mqttParse ignored
	// settledOwnAmbiguity and ran the generic resolver — whose forked arm DISPOSES — over an
	// ambiguity the live tail had deliberately retained.
	if got := stores.Load(); got != 2 {
		t.Fatalf("ownership resolution made %d session-record store attempts, want exactly 2 "+
			"(the proof, then the tail's byte-identical reattempt): 3 means the tail's "+
			"decision never reached the caller, so mqttResolveAfterPacket ran a second "+
			"resolution — the unnamed-result defect", got)
	}
}

// TestLiveSubQuietPacketOpensDespiteAnAmbiguousAncillaryWrite pins an ordering that reads
// like a defect, and it exists because a review round read it as one.
//
// THE SHAPE. The quiet branch — no trigger committed AND no record write CAS-rejected —
// opens the gate BEFORE mqttProcessSubs checks isSeqUncertain. So a packet whose ancillary
// save went AMBIGUOUS opens the gate while the session carries an unresolved write, and an
// unresolved write is an outcome nobody READ: it includes "CAS-rejected, response lost".
//
// WHY IT STANDS. A quiet packet installed NOTHING, so the open releases only traffic backed
// by interest that PREDATES the packet. Withholding it would give one packet type a stronger
// delivery guarantee than this broker provides for any other ambiguous record write, and it
// would convert an ambiguous affine provisioning failure into a connection-level refusal —
// the contract the affine items own, and the one the wider trigger predicate already broke
// once, for 1352 tests. The exposure is stated as a residual in docs/CONFORMANCE.md.
//
// THE ROUTE, which is a production one. A shared QoS 2 filter provisions the session-affine
// delivery path BEFORE the group consumer is created, so a suppressed provisioning save is
// ambiguous, fails the FILTER and continues: no group consumer, therefore no trigger, and no
// CAS rejection either — on a session that is now uncertain.
//
// WHAT IS ASSERTED, all four, because each one is a way the ordering could be broken without
// the others noticing: the packet is ACKNOWLEDGED with exactly 0x80 for the failed filter;
// the ownership CAS never runs, since nothing raised the question; the connection stays LIVE,
// which is the affine contract; and the generic resolver's attempt happens BEFORE the SUBACK
// is enqueued, proved by the store count, while the session stays uncertain because the
// suppression is permanent — so the residual's honest form is "may continue serving until
// settlement converges", not "the window is brief".
//
// Kills M34: hoisting isSeqUncertain above the quiet branch.
//
// Not parallel: both seams are package-global.
func TestLiveSubQuietPacketOpensDespiteAnAmbiguousAncillaryWrite(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "quietunc"
	const filter = "$share/qugrp/qu/topic"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: true,
	}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	// Counts only. Two attempts are expected: the affine provisioning save, and the generic
	// resolver's re-attempt from mqttResolveAfterPacket — which the readLoop runs BEFORE it
	// enqueues the SUBACK, so observing the SUBACK with a count of 2 is what orders them.
	var stores atomic.Int32
	h := func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != nil && sess.id == clientID {
			stores.Add(1)
		}
		return resp, err
	}
	mqttTestSessStoreResultHookV.Store(&h)
	defer mqttTestSessStoreResultHookV.Store(nil)

	// The proof must NOT run: this packet raised no ownership question.
	var sawCAS atomic.Bool
	ph := func(b string) {
		if b == "after-ownership-cas" {
			sawCAS.Store(true)
		}
	}
	mqttTestSharedArbitratePauseV.Store(&ph)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	mqttTestSessStoreSuppressV.Store(true)
	defer mqttTestSessStoreSuppressV.Store(false)

	vh := newMQTTWriter(0)
	vh.WriteUint16(2)
	vh.WriteVarInt(0)
	vh.WriteBytes([]byte(filter))
	vh.WriteByte(2)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, werr := testMQTTWrite(c, w.Bytes()); werr != nil {
		t.Fatalf("Error writing SUBSCRIBE: %v", werr)
	}

	pt, pl, ready := testMQTTReadPacketReady(r, 5*time.Second)
	if !ready {
		t.Fatal("the broker answered nothing: an ambiguous affine provisioning save is a " +
			"PER-FILTER failure, so this packet is owed a SUBACK carrying 0x80")
	}
	if pt&mqttPacketMask != mqttPacketSubAck {
		t.Fatalf("packet 0x%02x, want SUBACK: refusing here would convert an ambiguous "+
			"affine provisioning failure into a connection-level refusal, which is the "+
			"contract the affine items own", pt&mqttPacketMask)
	}

	// SUBACK body: packet identifier, properties, then one reason code.
	start := r.pos
	if pl < 4 {
		t.Fatalf("SUBACK payload is %d bytes, too short to carry a reason code", pl)
	}
	reason := r.buf[start+pl-1]
	r.pos = start + pl
	if reason != mqttSubAckFailure {
		t.Fatalf("SUBACK reason = 0x%02x, want 0x80: the filter DID fail, and a success code "+
			"here would tell the client it holds a shared subscription this packet never "+
			"installed", reason)
	}

	if sawCAS.Load() {
		t.Fatal("the packet reached the ownership CAS although it committed no trigger and " +
			"had no CAS rejection: the proof is not free, and running it here would write " +
			"the record on behalf of a packet that installed nothing")
	}
	if got := stores.Load(); got < 2 {
		t.Fatalf("session-record store attempts = %d, want at least 2: the provisioning "+
			"save, and the generic resolver's re-attempt, which the readLoop must make "+
			"BEFORE enqueueing this SUBACK", got)
	}

	cc := testMQTTGetClient(t, s, clientID)
	if cc.mqtt == nil || cc.mqtt.sess == nil {
		t.Fatal("the connection lost its session although the packet was acknowledged")
	}
	if !cc.mqtt.sess.isSeqUncertain() {
		t.Fatal("the session is no longer uncertain, so this fixture is not measuring the " +
			"case it names: with the store permanently suppressed the resolver cannot " +
			"classify the write, which is exactly why the residual cannot claim the " +
			"window is brief")
	}
}
