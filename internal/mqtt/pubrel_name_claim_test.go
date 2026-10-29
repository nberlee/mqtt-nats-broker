//go:build mqtt_fulltests

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

package mqtt

import (
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// The PUBREL consumer's name is claimed once per generation, by the first QoS 2 SUBSCRIBE. Every
// transition of the client CASes its one authority record, so a claim can lose to the client's own
// concurrent work — most visibly the previous connection's closing when a client identifier is
// reused at once. The claim is re-attempted, under the connection's own entitlement, adopting an
// attempt that landed behind a lost response; a QoS 2 SUBSCRIBE is not refused for contention.

// testPubRelClaimRig is a broker and one connected persistent client that has not yet subscribed.
type testPubRelClaimRig struct {
	s    *Server
	o    *Options
	c    net.Conn
	r    *mqttReader
	cli  *client
	sess *mqttSession
}

func newTestPubRelClaimRig(t *testing.T, id string) *testPubRelClaimRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &testPubRelClaimRig{s: s, o: o}
	rig.c, rig.r = testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.c.Close() })
	testMQTTCheckConnAck(t, rig.r, mqttConnAckRCConnectionAccepted, false)
	rig.cli = testMQTTGetClient(t, s, id)
	rig.sess = rig.cli.mqtt.sess
	return rig
}

// subscribeQoS2 sends a QoS 2 SUBSCRIBE and returns its SUBACK return code.
func testPubRelSubscribeQoS2(t *testing.T, c net.Conn, r *mqttReader, filter string) byte {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2 + len(filter) + 1)
	w.WriteUint16(1)
	w.WriteString(filter)
	w.WriteByte(2)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing the SUBSCRIBE: %v", err)
	}
	b, _ := testMQTTReadPacket(t, r)
	if b&mqttPacketMask != mqttPacketSubAck {
		t.Fatalf("expected a SUBACK, got %x", b)
	}
	if _, err := r.readUint16("packet identifier"); err != nil {
		t.Fatal(err)
	}
	code, err := r.readByte("return code")
	if err != nil {
		t.Fatal(err)
	}
	return code
}

// bump commits one authority write at the sequence it reads; tenure, when set, also moves it.
func (rig *testPubRelClaimRig) bump(tenure string) error {
	jsa := rig.sess.jsa
	al := jsa.loadOutboundAuthority(rig.sess.id, rig.sess.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		return fmt.Errorf("the authority reads %v", al.status)
	}
	next := *al.rec
	next.Rev++
	next.Txn = mqttNewInflightNonce()
	if tenure != _EMPTY_ {
		next.Tenure = tenure
	}
	b, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	_, err = jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(rig.sess.id)), hdr, msg)
	return err
}

// landClaim provisions the PUBREL name under the connection's entitlement, as an earlier attempt
// whose response was lost would have.
func (rig *testPubRelClaimRig) landClaim() error {
	ent, _ := rig.cli.outboundEntitlement()
	rig.sess.mu.Lock()
	_, _, name := mqttPubRelNames(rig.sess.idHash, rig.sess.gen)
	rig.sess.mu.Unlock()
	_, err := rig.sess.jsa.opProvisionResource(rig.sess.id, rig.sess.asm.sessStreamEpoch, mqttNewInflightNonce(),
		"op-test-landed", ent.Gen, ent.Tenure, mqttNewInflightNonce(), name, mqttResourcePubRelConsumer)
	return err
}

// presend runs window on each PUBREL-claim attempt, numbered from 1.
func (rig *testPubRelClaimRig) presend(t *testing.T, window func(k int32)) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	testPresendFor(t, rig.sess, "op-provision-pubrel", _EMPTY_, func(*mqttAuthorityTransition) {
		window(n.Add(1))
	})
	return &n
}

// TestPubRelClaimSurvivesAuthorityContention: one authority write landing inside the claim's first
// attempt no longer refuses the QoS 2 SUBSCRIBE.
func TestPubRelClaimSurvivesAuthorityContention(t *testing.T) {
	rig := newTestPubRelClaimRig(t, "pubrel-contended")
	testCountContentionWaits(t, rig.sess)
	attempts := rig.presend(t, func(k int32) {
		if k == 1 {
			if err := rig.bump(_EMPTY_); err != nil {
				t.Errorf("contending: %v", err)
			}
		}
	})
	if code := testPubRelSubscribeQoS2(t, rig.c, rig.r, "pr/t"); code != 2 {
		t.Fatalf("a QoS 2 SUBSCRIBE whose PUBREL claim met one contending write was answered %#x", code)
	}
	if n := attempts.Load(); n < 2 {
		t.Fatalf("fixture: the claim made %d attempt(s), so the contention never cost one", n)
	}
}

// TestPubRelClaimAdoptsLandedAttemptBeforeExhaustion: an earlier attempt that landed behind a lost
// response is adopted by the next attempt's ownership read, not discovered by the settlement after
// the budget is spent.
func TestPubRelClaimAdoptsLandedAttemptBeforeExhaustion(t *testing.T) {
	rig := newTestPubRelClaimRig(t, "pubrel-adopt")
	testCountContentionWaits(t, rig.sess)
	var adopted, settled atomic.Int32
	mqttTestPubRelClaimAdoptedHook.Set(func() { adopted.Add(1) })
	mqttTestPubRelClaimSettledHook.Set(func(int, bool, int, int) { settled.Add(1) })
	t.Cleanup(func() {
		mqttTestPubRelClaimAdoptedHook.Set(nil)
		mqttTestPubRelClaimSettledHook.Set(nil)
	})
	attempts := rig.presend(t, func(k int32) {
		if k == 1 {
			if err := rig.landClaim(); err != nil {
				t.Errorf("landing the claim beside attempt 1: %v", err)
			}
		}
	})
	if code := testPubRelSubscribeQoS2(t, rig.c, rig.r, "pr/t"); code != 2 {
		t.Fatalf("a QoS 2 SUBSCRIBE whose claim had landed was answered %#x", code)
	}
	if adopted.Load() != 1 || settled.Load() != 0 || attempts.Load() != 1 {
		t.Fatalf("adopted %d, settled %d, %d provisioning attempt(s): the landed claim must be adopted by "+
			"the second attempt's ownership read", adopted.Load(), settled.Load(), attempts.Load())
	}
}

// TestPubRelClaimExhaustionSettlesItsAttempts: a claim contended on every attempt refuses the
// SUBSCRIBE, and only after every attempt it submitted is settled.
func TestPubRelClaimExhaustionSettlesItsAttempts(t *testing.T) {
	rig := newTestPubRelClaimRig(t, "pubrel-exhausted")
	testCountContentionWaits(t, rig.sess)
	type settlement struct {
		submitted, aborted, open int
		landed                   bool
	}
	got := make(chan settlement, 1)
	mqttTestPubRelClaimSettledHook.Set(func(submitted int, landed bool, aborted, open int) {
		got <- settlement{submitted, aborted, open, landed}
	})
	t.Cleanup(func() { mqttTestPubRelClaimSettledHook.Set(nil) })
	rig.presend(t, func(int32) {
		if err := rig.bump(_EMPTY_); err != nil {
			t.Errorf("contending: %v", err)
		}
	})
	if code := testPubRelSubscribeQoS2(t, rig.c, rig.r, "pr/t"); code != mqttSubAckFailure {
		t.Fatalf("a claim contended on every attempt was answered %#x", code)
	}
	select {
	case st := <-got:
		if st.landed || st.open != 0 || st.submitted == 0 || st.aborted != st.submitted {
			t.Fatalf("the exhausted claim settled with landed=%v, %d of %d submitted attempt(s) aborted "+
				"and %d open: every unresolved attempt must be aborted so it can never land",
				st.landed, st.aborted, st.submitted, st.open)
		}
		t.Logf("settled %d submitted attempt(s), %d aborted", st.submitted, st.aborted)
	case <-time.After(5 * time.Second):
		t.Fatalf("the exhausted claim never settled its attempts")
	}
}

// TestPubRelClaimRefusesMovedTenure: a claim that landed under this connection's tenure is not
// adopted once a takeover has moved the tenure, and no consumer is created for it.
func TestPubRelClaimRefusesMovedTenure(t *testing.T) {
	rig := newTestPubRelClaimRig(t, "pubrel-moved")
	testCountContentionWaits(t, rig.sess)
	rig.presend(t, func(k int32) {
		if k != 1 {
			return
		}
		if err := rig.landClaim(); err != nil {
			t.Errorf("landing the claim: %v", err)
		}
		if err := rig.bump(mqttNewInflightNonce()); err != nil {
			t.Errorf("moving the tenure: %v", err)
		}
	})
	if code := testPubRelSubscribeQoS2(t, rig.c, rig.r, "pr/t"); code != mqttSubAckFailure {
		t.Fatalf("a displaced connection's QoS 2 SUBSCRIBE was answered %#x", code)
	}
	rig.sess.mu.Lock()
	_, _, name := mqttPubRelNames(rig.sess.idHash, rig.sess.gen)
	rig.sess.mu.Unlock()
	if ci, err := rig.sess.jsa.lookupConsumer(mqttOutStreamName, name); ci != nil {
		t.Fatalf("a PUBREL consumer was created for a displaced tenure (lookup err %v)", err)
	}
}

// TestPubRelClaimSurvivesQuickClientIdentifierReuse: a clean-session client reconnecting under the
// same identifier at once is granted its QoS 2 SUBSCRIBE, every time.
func TestPubRelClaimSurvivesQuickClientIdentifierReuse(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	for round := 0; round < 12; round++ {
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "pubrel-reuse", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		code := testPubRelSubscribeQoS2(t, c, r, "pr/reuse")
		c.Close()
		if code != 2 {
			t.Fatalf("round %d: the QoS 2 SUBSCRIBE of a reused clean-session identifier was answered %#x", round, code)
		}
		// THE SERVER HAS SEEN THE CLOSE before the identifier is reused: a CONNECT that arrives
		// first REPLACES a connection the server still counts as live, which the duplicate client
		// ID policy records as flapping and jails the next reuse for mqttSessJailDur. That policy
		// is not what this test is about; the closing work the claim races still runs after the
		// connection is dropped from the registry.
		checkFor(t, 5*time.Second, 5*time.Millisecond, func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, cl := range s.clients {
				cl.mu.Lock()
				live := cl.isMqtt() && cl.mqtt.cid == "pubrel-reuse"
				cl.mu.Unlock()
				if live {
					return fmt.Errorf("round %d: the closed connection is still registered", round)
				}
			}
			return nil
		})
	}
}
