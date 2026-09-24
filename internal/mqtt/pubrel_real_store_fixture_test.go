// Copyright 2026 The mqtt-nats-bridge Authors
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

import "testing"

// The REAL-STORE PUBREL fixture (F-105 PUBCOMP arm, consult 26).
//
// testReadyForPubRelDelivery's world keeps the register in an in-memory map, which was enough
// while PUBCOMP moved only the slot. An ORDINARY exchange's completion is now one authority batch
// over the slot AND its exchange on $MQTT_inflight, and a map slot can never pair with a real
// exchange — so a test that asserts ordinary PUBCOMP, cleanup or admission behaviour seeds here
// instead, through production: an authority-admitted delivery and the real PUBREC handler, which
// records the intent on the slot and the exchange together, stores the canonical PUBREL, and
// rebinds the slot to it from a readback proof.
//
// The PUBREL consumer is REAL (the canonical record needs it: $MQTT_out is interest-retained),
// but its delivery subscription is deliberately not installed, so no delivery races the ones a
// test drives by hand; admission is reached only through testDeliverPubRelAs.

// testRealPubRelSource is the source correlation the seeded exchange carries: the same
// (durable, sequence) the map-world fixtures used, whose source message does not exist, so the
// source side is provably complete for terminal cleanup unless a test stores one.
const (
	testRealPubRelSrcDur = "SRC"
	testRealPubRelSrcSeq = uint64(41)
)

// testReadyForRealPubRelDelivery wires the session the way testReadyForPubRelDelivery does —
// a bound connection, the accepted session record, the SRC source consumer — WITHOUT the map
// store, and with the real PUBREL consumer and its name ownership but no delivery subscription.
func testReadyForRealPubRelDelivery(t *testing.T, e *testInflightEnv) {
	t.Helper()
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	// No delivery subscription: ensurePubRelConsumerSubscription then provisions only the
	// consumer and its name ownership.
	e.sess.pubRelSubscribed = true
	e.sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			DeliverSubject: "f105.real.source.delivery",
			Durable:        testRealPubRelSrcDur,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  mqttStreamSubjectPrefix + "f092.source",
		},
	}); err != nil {
		t.Fatalf("creating the source consumer: %v", err)
	}
	e.subscribePubRel(t)
}

// testRealPubRelSeed is one seeded exchange, as production left it.
type testRealPubRelSeed struct {
	pi      uint16
	nonce   string // the slot's exchange nonce, the one a delivered PUBREL names
	lin     string // the obligation identity, the exchange's key
	canon   uint64 // the canonical PUBREL's stream sequence, the one the slot names
	sseq    uint64 // the source sequence the exchange is correlated to
	pubRel  string // the PUBREL consumer durable
	subject string // the slot's subject
}

// reply is the JetStream reply subject of the canonical PUBREL's n-th delivery.
func (s testRealPubRelSeed) reply(n int) string {
	return testInflightAckSubject(s.pubRel, s.canon, n)
}

// testSeedRealPubRelReady admits an ordinary QoS 2 delivery through the authority and answers
// it with the real PUBREC handler, leaving the slot at pubrel_ready naming its stored canonical
// PUBREL, the exchange at PUBREC-observed, and the pending PUBREL entry the handler installed.
func testSeedRealPubRelReady(t *testing.T, e *testInflightEnv) testRealPubRelSeed {
	t.Helper()
	return testSeedRealPubRelReadyAt(t, e, testRealPubRelSrcSeq)
}

// testSeedRealPubRelReadyAt seeds the exchange for source sequence sseq, for a test that needs a
// source message that EXISTS (an unfinished source).
func testSeedRealPubRelReadyAt(t *testing.T, e *testInflightEnv, sseq uint64) testRealPubRelSeed {
	t.Helper()
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, sseq, 2)
	if err := e.c.mqttProcessPublishReceived(pi, true, mqttReasonSuccess); err != nil {
		t.Fatalf("the PUBREC for packet identifier %d was not recorded: %v", pi, err)
	}
	e.sess.mu.Lock()
	gen := e.sess.gen
	subject := mqttInflightSubject(e.sess.idHash, gen, pi)
	_, _, pubRelDur := mqttPubRelNames(e.sess.idHash, gen)
	e.sess.mu.Unlock()
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("the seeded slot does not load: %+v (%v)", rec, err)
	}
	if rec.State != mqttInflightPubRelReady || rec.PubRelStreamSeq == 0 || rec.ActionToken != _EMPTY_ {
		t.Fatalf("the PUBREC left the slot at %+v, want pubrel_ready naming its canonical PUBREL with no action", rec)
	}
	x, _, xerr := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, gen, rec.Lin)
	if xerr != nil || x == nil || x.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the PUBREC left the exchange at %+v (%v), want PUBREC-observed", x, xerr)
	}
	return testRealPubRelSeed{pi: pi, nonce: rec.Nonce, lin: rec.Lin, sseq: sseq, canon: rec.PubRelStreamSeq, pubRel: pubRelDur, subject: subject}
}

// testRealSlot reads the seeded slot from the store (nil when absent).
func testRealSlot(t *testing.T, e *testInflightEnv, s testRealPubRelSeed) *mqttInflightRecord {
	t.Helper()
	rec, smsg, err := e.asm.loadInflightRecord(s.subject)
	if err != nil {
		t.Fatalf("reading the slot: %v", err)
	}
	if smsg == nil {
		return nil
	}
	return rec
}
