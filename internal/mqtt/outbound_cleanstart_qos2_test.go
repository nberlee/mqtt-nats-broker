// Copyright 2026 The mqtt-nats-broker Authors
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

//go:build mqtt_fulltests

package mqtt

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// issue #115: a clean start over a session that delivers ordinary QoS 2 — one that owns a PUBREL
// consumer and no affine QoS 2 consumer — is ACCEPTED, and the predecessor generation is
// discarded and retired.
//
// The refusal was a producer naming the PUBREL consumer as an affine target on its own; the PUBREL
// durable is the affine target's proof source, and a descriptor naming it alone is half a
// descriptor, which the strict validator refuses by design. The validator stays strict; the
// producers name the affine target whole or not at all (mqttAffineRetireDescriptor).

// testPredecessor is one ordinary QoS 2 obligation past PUBREC on a persistent session whose
// connection has gone: the state a clean start then discards.
type testPredecessor struct {
	asm    *mqttAccountSessionManager
	idHash string
	gen    string
	nonce  string
	pi     uint16
	src    string
	pubRel string
}

func testCapture(t *testing.T, sess *mqttSession, pi uint16) *testPredecessor {
	t.Helper()
	p := &testPredecessor{asm: sess.asm, idHash: sess.idHash, pi: pi}
	// What the discard leaves is inspected afterwards, which the generation's retirement would
	// purge once its root retires.
	sess.asm.testHoldGenerationRetirement.Store(true)
	sess.mu.Lock()
	p.gen = sess.gen
	for _, cc := range sess.cons {
		p.src = cc.Durable
	}
	if sess.pubRelConsumer != nil {
		p.pubRel = sess.pubRelConsumer.Durable
	}
	sess.mu.Unlock()
	if p.src == _EMPTY_ || p.pubRel == _EMPTY_ {
		t.Fatalf("fixture: the session holds no source (%q) or PUBREL (%q) consumer", p.src, p.pubRel)
	}
	if sess.q2AffConsumer != nil {
		t.Fatalf("fixture: an ordinary QoS 2 session holds an affine consumer %q", sess.q2AffConsumer.Durable)
	}
	reg, _, err := sess.jsa.loadInflightRecord(mqttInflightSubject(p.idHash, p.gen, pi))
	if err != nil || reg == nil || !mqttInflightPastPubRec(reg.State) {
		t.Fatalf("fixture: the register is not past PUBREC: %+v (%v)", reg, err)
	}
	p.nonce = reg.Lin
	testExchangePhaseIs(t, sess, p.nonce, mqttExchangePubRecObserved)
	return p
}

// discardedAndRetired waits for the predecessor generation's discard to complete: the obligation
// Discarded, both consumers retired, the generation certified.
func (p *testPredecessor) discardedAndRetired(t *testing.T, s *Server, cid string) {
	t.Helper()
	f := &testDiscardFix{s: s, asm: p.asm, id: cid, gen: p.gen, nonce: p.nonce, src: p.src, pubRel: p.pubRel}
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		for _, e := range []error{
			f.obligationDiscarded(t),
			f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, p.src),
			f.retired(t, mqttOwnedStreamOut, mqttOutStreamName, p.pubRel),
			f.certified(t),
		} {
			if e != nil {
				return e
			}
		}
		return nil
	})
}

// T1: the bullet's reproduction (v5, Session Expiry 60, one ordinary QoS 2 delivery past PUBREC,
// then a Clean Start) on one broker. Mutation M1 (the request names the PUBREL durable alone).
func TestCleanStartOverAnOrdinaryQoS2SessionIsAccepted(t *testing.T) {
	const cid = "f113-t1"
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || present {
		t.Fatalf("fixture CONNACK: rc %d present %v", rc, present)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f113/t1", opts: 2}})

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 2, false, false, "f113/t1", 1, []byte("held"))

	_, _, _, pi, _ := testMQTTReadPubV5Packet(t, r)
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	_, sess := testResidentSession(t, s, cid)
	p := testCapture(t, sess, pi)
	c.Close()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	present, rc, _ := testMQTTReadConnAckV5(t, r2)
	if rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the clean start over an ordinary QoS 2 session was refused: reason code 0x%x", rc)
	}
	if present {
		t.Fatal("a clean start reported Session Present")
	}
	_, sess2 := testResidentSession(t, s, cid)
	sess2.mu.Lock()
	gen2 := sess2.gen
	sess2.mu.Unlock()
	if gen2 == p.gen {
		t.Fatalf("the clean start kept generation %q", p.gen)
	}
	if cl, _, st := p.asm.jsa.loadGenClosure(cid, p.asm.sessStreamEpoch, p.gen); st != mqttOutboundValid || cl == nil {
		t.Fatalf("the predecessor generation was not closed with Discard: %+v (%v)", cl, st)
	}
	p.discardedAndRetired(t, s, cid)
	if pt, _, ok := testMQTTReadPacketReady(r2, 2*time.Second); ok {
		t.Fatalf("the clean-started session received packet 0x%x from the discarded generation", pt&mqttPacketMask)
	}
}

// T2: the same refusal on a PEER broker, the route issue #107 WP-F W2's discarded-generation witness
// found it on: a persistent 3.1.1 QoS 2 session past PUBREC on broker A, then a clean-session
// CONNECT on broker B sharing A's JetStream. B's finalize must close A's generation with Discard.
// Mutation M1.
func TestCleanStartOnAPeerOverAnOrdinaryQoS2SessionIsAccepted(t *testing.T) {
	const cid = "f113-t2"
	oA := testMQTTDefaultOptions()
	oA.MQTT.AckWait = time.Second
	sA := testMQTTRunServer(t, oA)
	defer testMQTTShutdownServer(sA) // owns the shared nats-server: shut down last
	oB := testMQTTDefaultOptions()
	oB.MQTT.AckWait = time.Second
	testSetStoreDir(oB, testStoreDir(oA))
	sB := testMQTTRunServer(t, oB)
	defer sB.Shutdown()

	// B's session manager, reached through a throwaway client before the witness's client arrives.
	probe, probeR := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-probe", cleanSess: true}, oB.MQTT.Host, oB.MQTT.Port)
	testMQTTCheckConnAck(t, probeR, mqttConnAckRCConnectionAccepted, false)
	asmB := testMQTTGetAccountSessionManager(t, sB, cid+"-probe")
	probe.Close()

	sub, subr, sessA, pi, _ := testQoS2Delivered(t, oA, sA, cid)
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	p := testCapture(t, sessA, pi)
	epoch := p.asm.sessStreamEpoch

	var mu sync.Mutex
	var closerTxn string
	var closerSeq uint64
	testArmTransitionSeam(t, &asmB.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && tr.Closure != nil && tr.Closure.Gen == p.gen {
			mu.Lock()
			closerTxn, closerSeq = tr.Closure.Txn, seq
			mu.Unlock()
		}
		return seq, err
	})
	sub.Close()

	cB, rB := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, oB.MQTT.Host, oB.MQTT.Port)
	defer cB.Close()
	testMQTTCheckConnAck(t, rB, mqttConnAckRCConnectionAccepted, false)

	cl, clSeq, st := asmB.jsa.loadGenClosure(cid, epoch, p.gen)
	if st != mqttOutboundValid || cl == nil {
		t.Fatalf("the peer's clean start did not close A's generation with Discard: %+v (%v)", cl, st)
	}
	mu.Lock()
	txn, seq := closerTxn, closerSeq
	mu.Unlock()
	// The seam returns the sequence of the batch's LAST member; the closure is one of its members,
	// so it stands at or below that sequence.
	if txn == _EMPTY_ || txn != cl.Txn || clSeq > seq {
		t.Fatalf("the discarding closure (txn %q, seq %d) was not written by B's finalize (txn %q, batch ending at seq %d)", cl.Txn, clSeq, txn, seq)
	}
	if pt, _, ok := testMQTTReadPacketReady(rB, 3*time.Second); ok {
		t.Fatalf("the clean-started session on B received packet 0x%x from the discarded generation", pt&mqttPacketMask)
	}
}

// T4: the retirement a clean start carries over an ordinary QoS 2 session names no affine target
// and no PUBREL consumer. Mutation M1.
func TestTheCleanStartRetirementNamesNoAffineTarget(t *testing.T) {
	const cid = "f113-t4"
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr, sess, pi, _ := testQoS2Delivered(t, o, s, cid)
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	p := testCapture(t, sess, pi)

	var retire atomic.Pointer[mqttRetirementRequest]
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Retire != nil && tr.Retire.Gen == p.gen {
			r := *tr.Retire
			retire.Store(&r)
		}
	}
	p.asm.jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { p.asm.jsa.testTransitionPresend.Store(nil) })
	sub.Close()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	r := retire.Load()
	if r == nil {
		t.Fatal("the clean start's handoff carried no retirement of the predecessor")
	}
	if got := fmt.Sprintf("%q/%q/%q", r.AffineDurable, r.AffineGen, r.PubRelDurable); got != `""/""/""` {
		t.Fatalf("the retirement of an ordinary QoS 2 session named an affine target %s", got)
	}
}

// T3: the producers' projection names an affine target whole or not at all, and the validator it
// feeds stays strict: a PUBREL durable named alone — the shape every ordinary QoS 2 session
// produced before issue #115 — is still REFUSED when it reaches the validator unprojected. Mutations M2,
// M3 (the projection drops a whole or partial affine target) and M4 (the validator accepts an empty
// affine pair whatever the PUBREL name says).
func TestAnAffineDescriptorIsNamedWholeOrNotAtAll(t *testing.T) {
	const id = "f113-t3"
	idHash := getHash(id)
	_, _, pubRelDur := mqttPubRelNames(idHash, testGen)
	_, _, affDur := mqttQ2AffineNames(idHash, testGen2)

	for _, tc := range []struct {
		name                string
		affDur, affGen, rel string
		want                [3]string
	}{
		{"nothing named", "", "", "", [3]string{}},
		{"an ordinary QoS 2 session's PUBREL consumer alone", "", "", pubRelDur, [3]string{}},
		{"a whole affine target", affDur, testGen2, pubRelDur, [3]string{affDur, testGen2, pubRelDur}},
		{"an affine durable without its generation", affDur, "", pubRelDur, [3]string{affDur, "", pubRelDur}},
	} {
		d, g, r := mqttAffineRetireDescriptor(tc.affDur, tc.affGen, tc.rel)
		if got := [3]string{d, g, r}; got != tc.want {
			t.Fatalf("%s: projected to %q, want %q", tc.name, got, tc.want)
		}
	}
	if err := mqttValidateAffineDescriptor(idHash, testGen, testGen2, affDur, pubRelDur); err != nil {
		t.Fatalf("a whole affine target was refused: %v", err)
	}
	if err := mqttValidateAffineDescriptor(idHash, testGen, "", affDur, pubRelDur); err == nil ||
		!strings.Contains(err.Error(), "malformed affine generation") {
		t.Fatalf("an affine durable without its generation was not refused as malformed: %v", err)
	}

	// THE STRICT VALIDATOR, fed the unprojected PUBREL-only shape directly and through the intent
	// gate that persists it.
	if err := mqttValidateAffineDescriptor(idHash, testGen, "", "", pubRelDur); err == nil {
		t.Fatal("a PUBREL consumer named alone was accepted as an affine descriptor")
	}
	if err := mqttValidateRetireTargets(idHash, testGen, mqttClosingTargets{PubRelDurable: pubRelDur}); err == nil {
		t.Fatal("a Clean Start whose retirement names a PUBREL consumer alone was accepted")
	}
	if err := mqttValidateRetireTargets(idHash, testGen, mqttClosingTargets{}); err != nil {
		t.Fatalf("a Clean Start whose retirement names no affine target was refused: %v", err)
	}
}
