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

//go:build mqttport_fulltests

package mqtt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// WD-4, witness 2 (consult 131): retaining a discarded generation's PUBREL state across a clean
// start cannot authorize the session serving the successor generation to transmit the discarded
// generation's exchange.
//
// Generation G holds one ordinary QoS 2 obligation past PUBREC; a clean start replaces it with G2
// (the plan's G') while every recovery driver is parked, so G's exchange still stands at
// PubRecObserved and its canonical PUBREL is still stored. That stored PUBREL is then handed to
// G2's real PUBREL delivery callback — JetStream delivers it onto G2's delivery subject, so the
// subscription the session installed receives it. G2 holds NO owner slot for the identifier (the ABSENT
// current-generation slot), so nothing can authorize the write: ZERO PUBREL writes, and the record
// is not acknowledged away.
//
// POSITIVE CONTROL in the same world: a G2 QoS 2 delivery's own PUBREL IS transmitted through the
// same callback and counted by the same counter, so a zero above is the callback's answer, not a
// counter that cannot count.
func TestF105APubRelOfADiscardedGenerationIsNeverTransmittedByItsSuccessor(t *testing.T) {
	// Not parallel: parking the recovery drivers and the PUBREL seams are process-wide.
	testDiscardParkAll(t)
	const cid = "f105-wd4-pubrel-gen"
	const topic = "f105/wd4"
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Generation G: one ordinary QoS 2 obligation past PUBREC.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || present {
		t.Fatalf("fixture CONNACK: rc %d present %v", rc, present)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 2}})
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 2, false, false, topic, 1, []byte("held"))
	_, _, _, pi, _ := testMQTTReadPubV5Packet(t, r)
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	_, sess := testResidentSession(t, s, cid)
	p := testF113Capture(t, sess, pi)
	epoch := p.asm.sessStreamEpoch

	// G's STORED canonical PUBREL, read back from G's own PUBREL subject.
	relSubjG, deliverG, _ := mqttPubRelNames(p.idHash, p.gen)
	var storedSeq uint64
	recs, _, ok := f099RealPubRelScan(p.asm, relSubjG)
	for _, rec := range recs {
		if ok && rec.shapeErr == nil && rec.pi == pi && rec.owner.Nonce == p.nonce {
			storedSeq = rec.seq
		}
	}
	if storedSeq == 0 {
		t.Fatalf("fixture: no canonical PUBREL for identifier %d of exchange %q on %q (%d records, ok=%v)",
			pi, p.nonce, relSubjG, len(recs), ok)
	}
	c.Close()

	// The clean start replaces G with G2.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted || present {
		t.Fatalf("the clean start: rc 0x%x present %v", rc, present)
	}
	_, sess2 := testResidentSession(t, s, cid)
	sess2.mu.Lock()
	gen2, deliver2 := sess2.gen, sess2.pubRelDeliverySubject
	sess2.mu.Unlock()
	if gen2 == p.gen {
		t.Fatalf("the clean start kept generation %q", p.gen)
	}
	if _, want, _ := mqttPubRelNames(p.idHash, gen2); deliver2 != want || deliver2 == deliverG {
		t.Fatalf("the clean-started session installed PUBREL delivery subject %q, want G2 %q's %q (G's is %q)",
			deliver2, gen2, want, deliverG)
	}
	// A QoS 2 subscription installs G2's PUBREL route; nothing is published to it yet, so no G2
	// exchange holds any identifier.
	testMQTTSubV5(t, c2, r2, 1, []mqttV5SubFilter{{topic: topic, opts: 2}})
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		sess2.mu.Lock()
		subscribed := sess2.pubRelSubscribed
		sess2.mu.Unlock()
		if !subscribed {
			return fmt.Errorf("the clean-started session never subscribed its PUBREL delivery subject")
		}
		return nil
	})

	// With every recovery driver parked, G's exchange still stands — the state in which its
	// retained PUBREL could be mistaken for one the successor owes.
	if x, _, err := p.asm.jsa.loadOutboundExchange(cid, epoch, p.gen, p.nonce); err != nil || x == nil ||
		x.Phase != mqttExchangePubRecObserved {
		t.Fatalf("G's exchange does not stand at PubRecObserved after the clean start: %+v (%v)", x, err)
	}
	// THE ABSENT CURRENT-GENERATION SLOT: G2 holds no owner slot for the identifier at all, let
	// alone one naming G's exchange.
	if reg, sm, err := p.asm.jsa.loadInflightRecord(mqttInflightSubject(p.idHash, gen2, pi)); err != nil || reg != nil || sm != nil {
		t.Fatalf("G2 holds an owner slot for identifier %d before any G2 delivery: %+v (%v)", pi, reg, err)
	}

	// Every PUBREL that reaches the write site, and every return of the PUBREL callback.
	var writes atomic.Int64
	restoreWrites := setMQTTTestPubRelWriteBlock(func() { writes.Add(1) })
	defer restoreWrites()
	var mu sync.Mutex
	var returned []string // the replies of PUBREL deliveries whose callback has returned
	restoreDone := setMQTTTestPubRelDeliveryDoneHook(func(_ *subscription, reply string) {
		mu.Lock()
		returned = append(returned, reply)
		mu.Unlock()
	})
	defer restoreDone()
	returnedFor := func(match func(string) bool) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, rep := range returned {
			if match(rep) {
				return true
			}
		}
		return false
	}

	// G's stored PUBREL, delivered by JETSTREAM to G2's delivery subject: a witness push consumer
	// filtered on G's PUBREL subject at exactly that record, so the subscription G2's session
	// installed receives the stored message with real delivery metadata, and the callback's
	// acknowledgement, if it made one, is observable on the witness consumer's ack subjects.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	const witnessDur = "f105-wd4-witness"
	acks := natsSubSync(t, nc, "$JS.ACK."+mqttOutStreamName+"."+witnessDur+".>")
	if err := nc.Flush(); err != nil {
		t.Fatalf("flushing the ack observer: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if _, err := js.AddConsumer(mqttOutStreamName, &nats.ConsumerConfig{
		Durable: witnessDur, DeliverSubject: deliver2, FilterSubject: relSubjG,
		DeliverPolicy: nats.DeliverByStartSequencePolicy, OptStartSeq: storedSeq,
		AckPolicy: nats.AckExplicitPolicy, AckWait: time.Minute, MaxDeliver: 1,
	}); err != nil {
		t.Fatalf("fixture: creating the witness consumer: %v", err)
	}
	defer func() { _ = js.DeleteConsumer(mqttOutStreamName, witnessDur) }()
	isWitnessDelivery := func(rep string) bool {
		st, dur, seq, ok := mqttParseSourceAckReply(rep)
		return ok && st == mqttOutStreamName && dur == witnessDur && seq == storedSeq
	}
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !returnedFor(isWitnessDelivery) {
			return fmt.Errorf("G2's PUBREL callback has not returned for G's stored PUBREL")
		}
		return nil
	})
	if n := writes.Load(); n != 0 {
		t.Fatalf("G2's PUBREL callback wrote %d PUBREL(s) for G's exchange %q (identifier %d) with no owner slot", n, p.nonce, pi)
	}
	if pt, _, ok := testMQTTReadPacketReady(r2, 2*time.Second); ok {
		t.Fatalf("the clean-started session received packet 0x%x after G's PUBREL reached its callback", pt&mqttPacketMask)
	}
	if m, err := acks.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("G2's PUBREL callback acknowledged G's stored PUBREL (%q on %q)", m.Data, m.Subject)
	}
	if recs, _, ok := f099RealPubRelScan(p.asm, relSubjG); !ok || len(recs) == 0 {
		t.Fatalf("G's stored PUBREL is gone after the refused delivery (%d records, ok=%v)", len(recs), ok)
	}

	// POSITIVE CONTROL: a G2 QoS 2 delivery's own PUBREL is transmitted through the same callback.
	_, _, pubRelDur2 := mqttPubRelNames(p.idHash, gen2)
	testMQTTPublish(t, pc, pr, 2, false, false, topic, 2, []byte("fresh"))
	_, _, payload, pi2, _ := testMQTTReadPubV5Packet(t, r2)
	if string(payload) != "fresh" {
		t.Fatalf("the positive control delivered %q, want the G2 message", payload)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, c2, pi2)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi2)
	if n := writes.Load(); n < 1 {
		t.Fatalf("the positive control's PUBREL was received but the write counter saw %d writes", n)
	}
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !returnedFor(func(rep string) bool {
			st, dur, _, ok := mqttParseSourceAckReply(rep)
			return ok && st == mqttOutStreamName && dur == pubRelDur2
		}) {
			return fmt.Errorf("the positive control's PUBREL did not come through G2's PUBREL delivery callback (%q)", pubRelDur2)
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi2)
}
