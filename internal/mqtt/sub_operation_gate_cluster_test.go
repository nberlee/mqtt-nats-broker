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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The name matches `make cluster-failover-tests`' regex (TestMQTTSharedGroup.*Replicated),
// which F-077 extended for exactly this reason: a replicated test the target does not run is
// a test nobody runs.

// TestMQTTSharedGroupLiveSubscribeArbitratedReplicated is F-078's case, and the counterpart
// to F-077's resume fixture.
//
// It drives the REAL production order — a live SUBSCRIBE through mqttProcessSubs, the
// ownership CAS, and its disposition tail — rather than installing a gate state by hand. A
// fixture that sets the gate directly can only prove the gate's own arithmetic; it cannot
// detect late arming, a premature open, or a wrong SUBACK.
//
// The ordering is load-bearing, and the same two dead ends apply as on the resume side:
//
//   - a takeover BEFORE the SUBSCRIBE's own writes would be arbitrated by the packet's
//     ordinary record save, so the fixture would re-prove F-029 rather than this item;
//   - a takeover that DELETES the group durable would be caught by an earlier verdict, in a
//     build with the ownership write removed too. So a SECOND, different-client member holds
//     the group throughout: the durable stays present and canonical, and A is refused by the
//     ownership write or not at all.
//
// It kills two mutations. M1 — discard the trigger and skip the proof, which is what `main`
// does. And M33 — collapse the Foreign arm into a non-disposing refusal, which the earlier
// "first packet is not a SUBACK" oracle PASSED, because every other refusal in the tail is
// also not a SUBACK. The oracle below therefore asserts the DISPOSAL positively: F-084 gave
// Foreign the same 0x89 and Reason String as Forked, so no byte on the wire separates them
// any more, and the disposal is the fact the old 0x8E assertion stood proxy for.
//
// Which boundary the takeover is staged at is load-bearing; the reason is at the handler.
func TestMQTTSharedGroupLiveSubscribeArbitratedReplicated(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b0 := c.startBroker(t, 0)
	defer b0.cleanup()
	b1 := c.startBroker(t, 1)
	defer b1.cleanup()

	const (
		clientA = "livesub-A"
		clientB = "livesub-B"
		group   = "livesubgrp"
		topic   = "livesub/topic"
		filter  = "$share/" + group + "/" + topic
	)

	// The other member, on b1, holding the group for the whole test. A DIFFERENT client ID,
	// so nothing it does touches A's record — and its membership is what keeps the group
	// durable present and canonical while A's own membership is removed.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientB,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b1.opts.MQTT.Host, b1.opts.MQTT.Port)
	defer cb.Close()
	testMQTTReadConnAckV5(t, rb)
	if codes := testMQTTSubV5(t, cb, rb, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("member B SUBACK %v", codes)
	}
	testMQTTFlush(t, cb, nil, rb)

	// A connects to b0 and is FULLY ESTABLISHED before it subscribes. That is what makes
	// this the live path: A's restore gate is already open, so nothing but the operation
	// gate can withhold what the SUBSCRIBE installs.
	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientA,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b0.opts.MQTT.Host, b0.opts.MQTT.Port)
	defer ca.Close()
	testMQTTReadConnAckV5(t, ra)

	// THE TAKEOVER, staged at the ownership CAS boundary: A has installed its shared
	// interest and is about to prove it still owns the membership. B's broker accepts a
	// newer CONNECT for A's client ID, which commits a newer record for that subject and
	// moves the sequence A is about to CAS against.
	var tookOver, arbitrated atomic.Bool
	h := func(boundary string) {
		if boundary == "after-ownership-cas" {
			// Only observed, never staged at: by here A's CAS has already
			// linearized, so a takeover landing now is legitimately AFTER A's write
			// and A is entitled to its SUBACK. Staging here proves nothing — an
			// earlier draft of this fixture did exactly that and failed a correct
			// implementation.
			arbitrated.Store(true)
			return
		}
		if !strings.HasPrefix(boundary, "after-group-create:") {
			return
		}
		// The LAST group-durable write of the packet, and the last boundary before
		// the ownership CAS. A takeover staged here moves the sequence A is about to
		// CAS against, which is the only interleaving this item is about.
		if tookOver.CompareAndSwap(false, true) {
			ct, rt := testMQTTConnectV5(t, &mqttV5ConnInfo{
				clientID:   clientA,
				cleanStart: true,
				props:      mqttV5ConnPropsSessionExpiry(300),
			}, b1.opts.MQTT.Host, b1.opts.MQTT.Port)
			testMQTTReadConnAckV5(t, rt)
			t.Cleanup(func() { ct.Close() })
		}
	}
	mqttTestSharedArbitratePauseV.Store(&h)
	defer mqttTestSharedArbitratePauseV.Store(nil)

	// The live SUBSCRIBE. Its ownership write CASes against a sequence B has moved, so it
	// must be refused — and A must not be told its subscription succeeded.
	// Sent WITHOUT waiting for a SUBACK: the whole point is that a refused packet sends
	// none, so a helper that insists on reading one would fail for the right reason but
	// report the wrong thing.
	{
		vh := newMQTTWriter(0)
		vh.WriteUint16(2)
		vh.WriteVarInt(0) // empty properties
		vh.WriteBytes([]byte(filter))
		vh.WriteByte(1)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if _, werr := testMQTTWrite(ca, w.Bytes()); werr != nil {
			t.Fatalf("Error writing SUBSCRIBE: %v", werr)
		}
	}

	pt, pl, answered := testMQTTReadPacketReady(ra, 5*time.Second)

	if !arbitrated.Load() {
		t.Fatal("the live SUBSCRIBE never reached the ownership write: without it there is " +
			"nothing for B's takeover to be observed by, and this fixture proves nothing " +
			"about F-078")
	}

	// A's FIRST packet must be a DISCONNECT, and the DISPOSAL must have happened. Both,
	// because F-084 took away the observable this fixture used to lean on.
	//
	// It used to require 0x8E, on the reasoning that the code was reachable only from the
	// Foreign arm after disposeForeignStateLocked, while Forked, Local and an unresolvable
	// ambiguity all said 0x89 and disposed nothing. The Foreign arm now says 0x89 too —
	// its verdict never established a successor, only that the record is not ours — and it
	// shares BOTH the code and the Reason String with the forked vocabulary, so no byte on
	// the wire separates them any more.
	//
	// So the fixture asserts the fact the 0x8E stood proxy for, directly: A's session was
	// DISPOSED — detached and evicted — which is exactly what Forked and Local do not do.
	// Reading the FIRST packet still catches a SUBACK emitted before the notification.
	if !answered {
		t.Fatal("broker A was told nothing after its ownership write was refused: the " +
			"displacing connection is established, so A is owed a DISCONNECT rather than " +
			"silence")
	}
	var reason byte
	if start := ra.pos; pl > 0 {
		reason = ra.buf[start]
		ra.pos = start + pl
	}
	if pt&mqttPacketMask == mqttPacketSubAck {
		t.Fatal("broker A was SUBACKed after another broker took the session over: it would " +
			"serve group deliveries under a membership the record no longer carries, and " +
			"ACKNOWLEDGE them, destroying them for the member that legitimately holds the " +
			"group")
	}
	if pt&mqttPacketMask != mqttPacketDisconnect || reason != mqttReasonServerBusy {
		t.Fatalf("broker A got packet %#x reason %#x after a record CAS its own takeover "+
			"caused; want DISCONNECT %#x with 0x89 ownership-unsettled. A takeover is NOT "+
			"claimed here: A observed a moved record, which establishes that the session is "+
			"no longer A's and nothing about who holds it (F-084)",
			pt&mqttPacketMask, reason, mqttPacketDisconnect)
	}

	// THE VERDICT, asserted on state rather than on a reason code. This is what separates
	// the Foreign arm from Forked and Local, which refuse the packet and leave the session
	// resident and bound. Without it the wire assertion above is satisfied by every
	// refusing arm, since they all send the same 0x89 with the same Reason String.
	// A NON-FATAL probe: testMQTTGetClient fatals when the client is absent, and absence is
	// precisely what is being asserted here.
	stillBound := func() bool {
		b0.s.mu.Lock()
		defer b0.s.mu.Unlock()
		for _, cc := range b0.s.clients {
			cc.mu.Lock()
			match := cc.isMqtt() && cc.mqtt != nil && cc.mqtt.cid == clientA
			cc.mu.Unlock()
			if match {
				return true
			}
		}
		return false
	}
	disposedBy := time.Now().Add(5 * time.Second)
	for stillBound() {
		if time.Now().After(disposedBy) {
			t.Fatal("broker A's packet was refused but its session was NOT disposed: the " +
				"Foreign arm must detach and evict it, or A keeps serving a group the " +
				"record no longer carries for it. Forked and Local refuse WITHOUT " +
				"disposing, and since F-084 they are indistinguishable from Foreign on " +
				"the wire, so this is the assertion that separates them")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The group durable survives. B holds the membership, and deleting it on A's foreign
	// verdict would break the legitimate member — every deletion here is refuted.
	//
	// Looked up through B's session, NOT A's: a correct foreign disposition detaches and
	// evicts A, so requiring A's client to still exist would fail the very behaviour this
	// fixture wants. An earlier draft did exactly that.
	sessB := testMQTTGetClient(t, b1.s, clientB)
	if sessB == nil || sessB.mqtt == nil || sessB.mqtt.sess == nil {
		t.Fatal("the legitimate member's session is gone, so nothing here can be checked")
	}
	durable := mqttSharedNamesDurable(group, strings.ReplaceAll(topic, "/", "."), 1)
	if ci, lerr := sessB.mqtt.sess.jsa.lookupConsumer(mqttStreamName, durable); lerr != nil || ci == nil {
		t.Fatalf("the group durable is gone after A's foreign verdict, but B still "+
			"holds the membership: %v", lerr)
	}

	// THE POSITIVE ORACLE. Refusing A is only worth doing if the messages then reach the
	// member that actually owns the membership.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   "livesub-pub",
		cleanStart: true,
	}, b1.opts.MQTT.Host, b1.opts.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, topic, []byte("for-B"))

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(rb, 2*time.Second)
		if !ready {
			continue
		}
		start := rb.pos
		body := string(rb.buf[start : start+pl])
		rb.pos = start + pl
		if pb&mqttPacketMask != mqttPacketPub {
			continue
		}
		if strings.Contains(body, topic) && strings.Contains(body, "for-B") {
			return
		}
	}
	t.Fatal("the legitimate member never received the group delivery: refusing A is only " +
		"correct if the message reaches the member that holds the membership")
}
