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

// A denied delivery's disposal held on a session object that a takeover through another
// broker then displaces. The disposal is the one removed-object worker that waits on its
// successor's progress (outbound_disposal.go): this pins that, once the successor has ended
// the exchange, the displaced object lets go — and, in the delayed subtest, what happens when
// the displaced step first reads the pair only after the successor has ended and replaced it
// (the witness docs/findings/resource-lifetime.md records).

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Two users in one account: alice may subscribe to everything, carol is denied the literal
// deny.x. The session subscribes the wildcard deny/+, which NATS lets carol restore; it is the
// broker's delivery guard that refuses her the deny/x message, which is the route that
// installs a disposal.
const displacedDisposalConf = `
port: -1
host: "127.0.0.1"
jetstream: enabled
accounts {
  APP: {
    jetstream: enabled
    users: [
      { user: svc, password: svcpw, permissions: { publish: ">", subscribe: ">" } }
      { user: alice, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$MQTT.>"] }
          subscribe: { allow: [">"], deny: ["$MQTT.>"] }
      } }
      { user: carol, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$MQTT.>"] }
          subscribe: { allow: [">"], deny: ["$MQTT.>", "deny.x"] }
      } }
    ]
  }
}
`

type passthroughBroker struct {
	s *Server
	o *Options
}

func startPassthroughBroker(t *testing.T, url, node string) *passthroughBroker {
	t.Helper()
	nc, err := nats.Connect(url, nats.UserInfo("svc", "svcpw"))
	if err != nil {
		t.Fatal(err)
	}
	o := &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1, AckWait: time.Second}}
	s := NewServer(nc, o, node)
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		return nats.Connect(url, nats.UserInfo(user, pass))
	})
	if err := s.Start(); err != nil {
		nc.Close()
		t.Fatalf("start %s: %v", node, err)
	}
	t.Cleanup(func() {
		<-s.BeginShutdown()
		nc.Close()
	})
	o.MQTT.Port = s.Addr().(*net.TCPAddr).Port
	return &passthroughBroker{s: s, o: o}
}

func TestResourceDisplacedDisposalCollected(t *testing.T) {
	requireResourceSoak(t)
	const (
		afterSettlement  = iota // release A once B abandoned and settled the source
		afterReclamation        // ... and the client ended the identifier and the floor reclaimed the exchange
		afterRetirement         // ... and the source consumer was unsubscribed and positively retired
	)
	for _, tc := range []struct {
		name string
		// atPairRead holds the displaced disposal BEFORE its first pair read, so the successor
		// ends and replaces the pair before the displaced step ever sees it.
		atPairRead bool
		after      int
	}{
		{"HeldAcrossTakeover", false, afterSettlement},
		{"DelayedDisplacedDisposal", true, afterReclamation},
		{"SourceRetiredBeforeRelease", true, afterRetirement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := serviceReleaseNATS(t, displacedDisposalConf)
			a := startPassthroughBroker(t, url, "disp-a")
			b := startPassthroughBroker(t, url, "disp-b")
			id := "disp-" + tc.name
			removed := captureRemovedSessions(t, id)

			// 1. alice receives an allowed delivery on A and leaves it unacknowledged.
			cA, rA := testMQTTConnect(t, &mqttConnInfo{clientID: id, user: "alice", pass: "pw"}, a.o.MQTT.Host, a.o.MQTT.Port)
			testMQTTCheckConnAck(t, rA, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, cA, rA, []*mqttFilter{{filter: "deny/+", qos: 1}}, []byte{1})
			testMQTTFlush(t, cA, nil, rA)
			pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true, user: "alice", pass: "pw"},
				a.o.MQTT.Host, a.o.MQTT.Port)
			testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, pc, pr, 1, false, false, "deny/x", 1, []byte("held"))
			pc.Close()
			heldPI := testMQTTCheckPubMsgNoAck(t, cA, rA, "deny/x", mqttPubQos1, []byte("held"))
			cA.Close()

			// 2. The disposal the resume under carol installs on A's object is held there.
			objA := residentSessionOn(t, a.s, id)
			reached := make(chan *mqttDisposal, 1)
			release := make(chan struct{})
			released := false
			releaseOnce := func() {
				if !released {
					released = true
					close(release)
				}
			}
			t.Cleanup(releaseOnce)
			hold := func(w *mqttDisposal) {
				select {
				case reached <- w:
				default:
				}
				<-release
			}
			objA.mu.Lock()
			if tc.atPairRead {
				objA.testDisposalBeforePairRead = hold
			} else {
				objA.testDisposalBeforeSettle = hold
			}
			objA.mu.Unlock()

			cA2, rA2 := testMQTTConnect(t, &mqttConnInfo{clientID: id, user: "carol", pass: "pw"}, a.o.MQTT.Host, a.o.MQTT.Port)
			testMQTTCheckConnAck(t, rA2, mqttConnAckRCConnectionAccepted, true)
			var w *mqttDisposal
			select {
			case w = <-reached:
			case <-time.After(30 * time.Second):
				t.Fatal("no disposal reached the hold: the denying resume did not take the disposal route")
			}
			objA.mu.Lock()
			holds := objA.disposals[w.key] == w
			objA.mu.Unlock()
			if !holds {
				t.Fatal("the held disposal is not indexed on A's object: there is no retaining root")
			}
			cA2.Close()

			// 3. The takeover through B displaces A's object.
			cB, rB := testMQTTConnect(t, &mqttConnInfo{clientID: id, user: "carol", pass: "pw"}, b.o.MQTT.Host, b.o.MQTT.Port)
			testMQTTCheckConnAck(t, rB, mqttConnAckRCConnectionAccepted, true)
			defer cB.Close()
			checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
				removed.mu.Lock()
				defer removed.mu.Unlock()
				if len(removed.ptrs) == 0 {
					return fmt.Errorf("A's object was not displaced")
				}
				return nil
			})
			objA = nil // the oracle must not be defeated by this test's own reference

			// 4. B ends the exchange: the delivery stays denied for carol and is abandoned by
			// the broker that now serves the session. Independent evidence, read by identity.
			asB := testMQTTGetGlobalASM(t, b.s)
			checkFor(t, 60*time.Second, 250*time.Millisecond, func() error {
				x, _, err := asB.jsa.loadOutboundExchange(id, w.key.epoch, w.key.gen, w.key.lin)
				if err != nil {
					return err
				}
				if x == nil || x.Abandoned == nil {
					return fmt.Errorf("exchange not abandoned yet: %+v", x)
				}
				ci, err := asB.jsa.lookupConsumer(mqttInflightSourceStream(w.key.dur), w.key.dur)
				if err != nil {
					return err
				}
				if ci != nil && ci.AckFloor.Stream < w.key.seq {
					return fmt.Errorf("source %d not acknowledged (ack floor %d)", w.key.seq, ci.AckFloor.Stream)
				}
				return nil
			})

			switch tc.after {
			case afterReclamation:
				// The client ends the identifier (a late PUBACK, bound to the abandoned exchange),
				// and enough completed traffic on B drives the floor past its slack, until the
				// exchange record is gone: a classified "no message", not an unreadable store.
				testMQTTSendPIPacket(mqttPacketPubAck, t, cB, heldPI)
				testMQTTSub(t, 2, cB, rB, []*mqttFilter{{filter: "ok/+", qos: 1}}, []byte{1})
				testMQTTFlush(t, cB, nil, rB)
				pc2, pr2 := testMQTTConnect(t, &mqttConnInfo{clientID: id + "-floor", cleanSess: true, user: "alice", pass: "pw"},
					b.o.MQTT.Host, b.o.MQTT.Port)
				testMQTTCheckConnAck(t, pr2, mqttConnAckRCConnectionAccepted, false)
				for i := 0; i < 320; i++ {
					testMQTTPublish(t, pc2, pr2, 1, false, false, "ok/x", uint16(1+i), []byte("floor"))
					pi := testMQTTCheckPubMsgNoAck(t, cB, rB, "ok/x", mqttPubQos1, []byte("floor"))
					testMQTTSendPIPacket(mqttPacketPubAck, t, cB, pi)
				}
				pc2.Close()
				checkFor(t, 3*mqttFloorTick, time.Second, func() error {
					_, _, err := asB.jsa.loadOutboundExchange(id, w.key.epoch, w.key.gen, w.key.lin)
					if err == nil {
						return fmt.Errorf("the abandoned exchange is still stored")
					}
					if ae := jsAPIError(err); ae == nil || !IsNatsErr(ae, JSNoMessageFoundErr) {
						return fmt.Errorf("the exchange read is not a classified absence: %v", err)
					}
					return nil
				})
			case afterRetirement:
				// The client ends the identifier (otherwise the UNSUBSCRIBE stages a drain that keeps
				// the consumer for it) and unsubscribes on B, so the source consumer is retired:
				// absent, and its resource positively cleaned up or decided.
				testMQTTSendPIPacket(mqttPacketPubAck, t, cB, heldPI)
				testMQTTFlush(t, cB, nil, rB)
				testMQTTUnsub(t, 3, cB, rB, []*mqttFilter{{filter: "deny/+"}})
				testMQTTFlush(t, cB, nil, rB)
				checkFor(t, 120*time.Second, 250*time.Millisecond, func() error {
					infos, ok, err := asB.jsa.lookupConsumersMulti(mqttStreamName, []string{w.key.dur})
					if err != nil || len(ok) != 1 || !ok[0] || infos[0] != nil {
						return fmt.Errorf("the source consumer is not authoritatively absent yet")
					}
					o, _, err := asB.jsa.loadNameOwnership(id, w.key.epoch, mqttOwnedStreamMsgs, w.key.dur)
					if err != nil || o == nil {
						return fmt.Errorf("no ownership for the source: %v", err)
					}
					r, _, err := asB.jsa.loadOutboundResource(id, w.key.epoch, o.OwnerGen, o.OwnerInc)
					if err != nil || r == nil || (r.State != mqttResourceCleanupComplete && r.Disposal == nil) {
						return fmt.Errorf("the source resource is not retired: %+v %v", r, err)
					}
					return nil
				})
			}

			// 5. Only now does the displaced step run.
			releaseOnce()
			removed.awaitCollected(t, 60*time.Second)
			rc := countResources(a.s, testMQTTGetGlobalASM(t, a.s))
			if rc.owedDisposals != 0 {
				t.Fatalf("A still registers %d disposal(s) for an exchange its successor ended", rc.owedDisposals)
			}
		})
	}
}
