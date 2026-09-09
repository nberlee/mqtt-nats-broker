// Copyright 2024 The NATS Authors
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
	"testing"
	"time"
)

// TestMQTTUnsubDefiniteFailureKeepsAnAdoptedRetention drives a REAL UNSUBSCRIBE, because the
// property it pins is about where settlement sits in that packet and nothing below the wire
// can see that.
//
// A retention may be released only by a CONFIRMED outcome. Settling before the packet's save
// looks harmless: the teardown has already run, so the consumer really is gone. But if the
// save then definitely fails, the rollback restores the consumer maps while nothing restores
// the retention — so the next record omits the durable, and a sibling broker's orphan sweep
// deletes it with its outstanding deliveries. That is message loss, not a leak, which is why
// settlement lives inside the confirmed commit closure rather than beside it.
//
// NOT parallel: it installs a process-global store-result seam.
func TestMQTTUnsubDefiniteFailureKeepsAnAdoptedRetention(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// cleanSess FALSE deliberately: a refused record write closes the connection, and a
	// clean session is DESTROYED on that close — which empties every map and would make
	// this test pass or fail for a reason that has nothing to do with settlement.
	ci := &mqttConnInfo{clientID: "unsub-retain", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sess := testSubRevokeSession(t, s, "unsub-retain")

	// A retained adoption on an UNRELATED sid, with no membership key: exactly the state a
	// filter that failed after adopting leaves, and the one whose durable only the
	// retention keeps named.
	const adoptedSid, adoptedDur = "bar", "dur-unsub-retain-adopted"
	testAdoptedDrain(t, sess, adoptedSid, adoptedDur, "bar")

	// A pending delivery on the adopted durable, so this UNSUBSCRIBE STAGES it as a drain
	// rather than tearing it down. That is the schedule the placement matters in:
	// settlement then sees an active drain under the sid and releases the retention as
	// "responsibility moved" — and if the packet's save definitely fails,
	// rollbackStagedDrains removes that drain and restores the consumer while nothing
	// restores the retention.
	sess.mu.Lock()
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	sess.cpending[adoptedDur] = map[uint64]uint16{55: 9}
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	sess.pendingPublish[9] = &mqttPending{
		jsDur: adoptedDur, sseq: 55, transmitted: true, jsAckSubject: "ack.adopted.55",
	}
	heldBefore := sess.adoptedDrains[adoptedSid] != nil
	sess.mu.Unlock()
	if !heldBefore {
		t.Fatal("fixture: the adoption must be retained before the UNSUBSCRIBE")
	}

	// The UNSUBSCRIBE's own record write is REFUSED, definitely: the server answers with a
	// rejected CAS, so nothing is stored and the packet rolls back.
	restore := setMQTTTestSessStoreResultHook(func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if hs != sess {
			return resp, err
		}
		return resp, ApiErrors[JSStreamWrongLastSequenceErrF]
	})
	// Written raw, and the UNSUBACK is deliberately NOT awaited: a refused record write
	// closes the connection, so a helper that expects the ack fails on EOF before the
	// property can be inspected. What is under test is the session's retention, not the
	// wire.
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	// TWO filters in one packet, and both are needed.
	//
	// The ADOPTED one, because unsubscribing anything else leaves its consumer entry in
	// place — settlement is then a no-op for that sid and the placement this test exists
	// for makes no difference.
	//
	// And a filter that really holds MEMBERSHIP, because an UNSUBSCRIBE of an unmembered
	// sid alone has nothing to remove: the update returns without saving, so there is no
	// write to refuse and no definite failure to observe.
	w.WriteVarInt(2 + (2 + len(adoptedSid)) + (2 + len("foo")))
	w.WriteUint16(1)
	w.WriteBytes([]byte(adoptedSid))
	w.WriteBytes([]byte("foo"))
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing UNSUBSCRIBE: %v", err)
	}
	// Wait on EVIDENCE THE CODE RAN, never a sleep and never the end state: the refused
	// write closes the connection, so the client detaches from the session. Waiting for the
	// membership to disappear would be waiting for the wrong thing — a definite failure
	// ROLLS THE REMOVAL BACK, so the subscription is restored.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sess.mu.Lock()
		attached := sess.c != nil
		sess.mu.Unlock()
		if attached {
			return fmt.Errorf("the UNSUBSCRIBE has not been processed yet")
		}
		return nil
	})
	restore()

	sess.mu.Lock()
	heldAfter := sess.adoptedDrains[adoptedSid] != nil
	consBack := sess.cons[adoptedSid] != nil
	sess.mu.Unlock()

	if !consBack {
		t.Fatal("fixture: the adopted consumer entry must still be there, or the session was " +
			"torn down and this asserts nothing about settlement")
	}

	if !heldAfter {
		t.Fatal("a DEFINITELY FAILED UNSUBSCRIBE must not release an adopted retention: the " +
			"rollback restores the consumer maps but nothing restores the retention, so the " +
			"next record would omit the durable and a sibling sweep would delete its " +
			"outstanding deliveries")
	}
}
