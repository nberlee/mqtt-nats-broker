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

// A persistent session's pending-binding buckets exist only while they hold an entry. Every
// consumer create mints a fresh durable name, so an emptied bucket that stays behind is never
// reused: a client that subscribes and unsubscribes with deliveries in flight used to leave one
// per cycle for the life of the session.

import (
	"fmt"
	"testing"
	"time"
)

func pendingBucketCount(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.cpending)
}

// The drain route: UNSUBSCRIBE while a delivery is unacknowledged stages a drain (which by
// contract does not purge the mapping), the PUBACK then removes the entry and retires the
// drain. The bucket must go with its last entry.
func TestPendingBucketsDoNotAccumulate(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cycles = 30
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "buckets"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "buckets-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	sess := testMQTTResidentSession(t, s, "buckets")

	for i := 0; i < cycles; i++ {
		topic := fmt.Sprintf("buckets/%d", i)
		testMQTTSub(t, uint16(1+2*i), c, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
		testMQTTFlush(t, c, nil, r)
		testMQTTPublish(t, pc, pr, 1, false, false, topic, uint16(1+i), []byte("payload"))
		pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQos1, []byte("payload"))
		if pendingBucketCount(sess) != 1 {
			t.Fatalf("cycle %d: the in-flight delivery has no bucket, so the cycle proves nothing", i)
		}
		testMQTTUnsub(t, uint16(2+2*i), c, r, []*mqttFilter{{filter: topic}})
		testMQTTFlush(t, c, nil, r)
		if n := testDrainCount(sess); n != 1 {
			t.Fatalf("cycle %d: the UNSUBSCRIBE staged %d drains, want 1: the cycle did not take the drain route", i, n)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
			if n := testDrainCount(sess); n != 0 {
				return fmt.Errorf("drain still present (%d)", n)
			}
			return nil
		})
		if n := pendingBucketCount(sess); n != 0 {
			t.Fatalf("cycle %d: %d pending bucket(s) left after the drain retired", i, n)
		}
	}
}

// The compare-delete route: a transmitted delivery whose identifier is abandoned leaves its
// source mapping through settleAbandonedDeliveryLocked's deleteIf, and the durable's bucket,
// whose only entry that was, goes with it.
func TestAbandonedDeliveryLeavesNoPendingBucket(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-bucket", 1)
	sess := f.e.sess
	sess.mu.Lock()
	entries := sess.cpending.entries(testRealPubRelSrcDur)
	var reached int
	sess.testAbandonedAckInstalled = func() { reached++ }
	sess.mu.Unlock()
	if len(entries) != 1 {
		t.Fatalf("fixture: the durable's bucket must hold exactly the delivery, holds %d", len(entries))
	}
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("the deny established %v, want the abandonment", r)
	}
	f.requireAbandoned(t)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if reached == 0 {
		t.Fatal("the abandonment never reached settleAbandonedDeliveryLocked's removal, so this proves nothing")
	}
	if _, ok := sess.pendingPublish[f.pi]; ok {
		t.Fatal("the abandoned entry is still resident")
	}
	if _, ok := sess.cpending[testRealPubRelSrcDur]; ok {
		t.Fatal("the abandonment emptied the durable's bucket but left it in place")
	}
}
