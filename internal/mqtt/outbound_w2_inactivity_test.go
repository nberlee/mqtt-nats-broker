//go:build mqttport_fulltests

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

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// The inactivity prerequisite of a depended-on consumer (F-105 WP-F W2, rows D4 and D4c).

// TestF105ReadinessRefusesAStreamInactivityLimit pins that readiness for use refuses a source
// whose STREAM imposes a consumer inactivity limit, and does so itself: the refusal is the typed
// inactivity refusal, from the durable/configuration half of readiness, which runs before any
// physical consumer is looked up. The server substitutes the stream's limit for a consumer that
// requests none, so the consumer-threshold check after the lookup would also refuse; a readiness
// that passed here would be leaning on that later snapshot of one consumer for a stream-wide fact.
func TestF105ReadinessRefusesAStreamInactivityLimit(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "w2-readiness-stream-limit")
	jsa := &e.asm.jsa
	cl := testClaimedSourceConsumer(t, e, "w2.readiness.limit")

	// The positive control: the same claimed source, on an unlimited stream, is ready.
	if err := jsa.mqttSourcePubRelReadyForUse(e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc); err != nil {
		t.Fatalf("fixture: a LIVE claimed source on an unlimited stream is not ready for use: %v", err)
	}

	si, serr := jsa.lookupStream(mqttStreamName)
	if serr != nil || si == nil {
		t.Fatalf("lookupStream(%q): %v", mqttStreamName, serr)
	}
	limited := si.Config
	limited.ConsumerLimits.InactiveThreshold = 300 * time.Millisecond
	if _, err := jsa.updateStream(&limited); err != nil {
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, err)
	}

	err := jsa.mqttSourcePubRelReadyForUse(e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc)
	if err == nil {
		t.Fatal("a source on a stream imposing a consumer inactivity limit was reported ready for use")
	}
	var unsafe mqttInactivityUnsafe
	if !errors.As(err, &unsafe) {
		t.Fatalf("readiness refused for another reason (%T: %v), want the stream inactivity refusal", err, err)
	}
}

// TestF105AdmittedQoS2DependencySurvivesOffline is the D4c witness (it re-expresses
// TestMQTTReconcileEnrolledSourceSurvivesBrokerThreshold, whose broker option no longer exists).
// A QoS 2 delivery past PUBREC is an admitted obligation depending on two consumers: its source
// (the redelivery) and the generation's PUBREL consumer (the interest that retains the canonical
// PUBREL, its completion evidence). While the client is away nothing may reap either: their
// lifetime is their resource's, so they are created with no inactivity threshold, and the
// effective one the server reports is zero.
//
// Survival is asserted BEFORE any reconnect, because the reconnect is not neutral: the
// re-materialization continuation repairs an absent PUBREL consumer and republishes the PUBREL,
// so a completed exchange afterwards proves nothing about what survived the absence. The
// consumers must be the ORIGINAL ones (same creation instant) and the canonical PUBREL must still
// stand at the stream sequence the slot recorded.
func TestF105AdmittedQoS2DependencySurvivesOffline(t *testing.T) {
	t.Parallel()
	w := testF105RestartWorldAtPubRelReady(t, "w2-d4c-offline")
	t.Cleanup(func() { testMQTTShutdownServer(w.s) })
	jsa := &w.asm.jsa

	w.sess.mu.Lock()
	gen, idHash := w.sess.gen, w.sess.idHash
	var srcDur string
	for _, cc := range w.sess.cons {
		if cc != nil {
			srcDur = cc.Durable
		}
	}
	nCons := len(w.sess.cons)
	w.sess.mu.Unlock()
	if nCons != 1 || srcDur == _EMPTY_ {
		t.Fatalf("fixture: want exactly one source durable, got %d (%q)", nCons, srcDur)
	}
	pubRelSubject, _, pubRelDur := mqttPubRelNames(idHash, gen)
	slot := w.slot(t)
	if slot == nil || slot.PubRelStreamSeq == 0 {
		t.Fatalf("fixture: the slot names no canonical PUBREL: %+v", slot)
	}
	canon := slot.PubRelStreamSeq
	canonMsg, cerr := jsa.loadMsg(mqttOutStreamName, canon)
	if cerr != nil || canonMsg == nil || canonMsg.Subject != pubRelSubject {
		t.Fatalf("fixture: the canonical PUBREL does not stand at %d on %q: %+v (%v)", canon, pubRelSubject, canonMsg, cerr)
	}

	// The client goes away with the release owed.
	w.sub.Close()

	created := map[string]time.Time{}
	for _, d := range []struct{ stream, dur string }{{mqttStreamName, srcDur}, {mqttOutStreamName, pubRelDur}} {
		ci, err := jsa.lookupConsumer(d.stream, d.dur)
		if err != nil || ci == nil || ci.Config == nil {
			t.Fatalf("the depended-on consumer %q does not read back: %+v (%v)", d.dur, ci, err)
		}
		if ci.Config.InactiveThreshold != 0 {
			t.Fatalf("the depended-on consumer %q carries an effective inactivity threshold of %v: it "+
				"is reapable under the obligation that depends on it", d.dur, ci.Config.InactiveThreshold)
		}
		created[d.dur] = ci.Created
	}

	// Offline. Checked throughout, so a reap at any point in the interval is caught.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, d := range []struct{ stream, dur string }{{mqttStreamName, srcDur}, {mqttOutStreamName, pubRelDur}} {
			ci, err := jsa.lookupConsumer(d.stream, d.dur)
			if err != nil && isErrorOtherThan(err, JSConsumerNotFoundErr) {
				t.Fatalf("looking up %q: %v", d.dur, err)
			}
			if ci == nil {
				t.Fatalf("the depended-on consumer %q was reaped while its client was offline", d.dur)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// BEFORE ANY RECONNECT OR REPAIR: the ORIGINAL consumers, and the canonical PUBREL where the
	// slot recorded it.
	for _, d := range []struct{ stream, dur string }{{mqttStreamName, srcDur}, {mqttOutStreamName, pubRelDur}} {
		ci, err := jsa.lookupConsumer(d.stream, d.dur)
		if err != nil || ci == nil {
			t.Fatalf("the depended-on consumer %q does not stand after the offline interval: %v", d.dur, err)
		}
		if !ci.Created.Equal(created[d.dur]) {
			t.Fatalf("the depended-on consumer %q was re-created while offline (%v -> %v): the original "+
				"did not survive", d.dur, created[d.dur], ci.Created)
		}
	}
	if m, err := jsa.loadMsg(mqttOutStreamName, canon); err != nil || m == nil || m.Subject != pubRelSubject ||
		string(m.Header) != string(canonMsg.Header) {
		t.Fatalf("the canonical PUBREL no longer stands at its recorded sequence %d: %+v (%v)", canon, m, err)
	}
	if cur := w.slot(t); cur == nil || cur.State != mqttInflightPubRelReady || cur.PubRelStreamSeq != canon {
		t.Fatalf("the slot no longer names the canonical PUBREL at %d: %+v", canon, cur)
	}

	// Then the client returns: the release is owed under the ORIGINAL identifier, and its PUBCOMP
	// ends the exchange.
	w.sub, w.subr = testMQTTConnectV5(t, w.ci, w.o.MQTT.Host, w.o.MQTT.Port)
	t.Cleanup(func() { w.sub.Close() })
	if present, _, _ := testMQTTReadConnAckV5(t, w.subr); !present {
		t.Fatal("the session must be resumed")
	}
	w.bind(t)
	testMQTTReadPIPacket(mqttPacketPubRel, t, w.subr, w.pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, w.sub, w.pi)
	testMQTTFlush(t, w.sub, nil, w.subr)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if got := w.phase(t); got != mqttExchangeTerminal {
			return fmt.Errorf("the exchange stands at phase %d, want Terminal", got)
		}
		return nil
	})
}
