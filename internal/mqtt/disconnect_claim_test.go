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
	"errors"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// One owner for "displace this connection".
// ---------------------------------------------------------------------------
//
// Four paths displace a bound connection — a local CONNECT takeover, a remote persist
// observation, a resolver's foreign disposal and the bound rejection settlement — and each
// used to notify the client itself. Two of them racing put two server DISCONNECTs on one
// socket, which [MQTT-3.14.4-1] forbids and which tells the client the opposite of what is
// true, in an order neither path controls. mqttClaimDisconnectLocked makes the
// notification a claim consumed once per connection, and mqttNotifyTakenOver is the one
// tail all four take.
//
// Two layers of test, and the distinction is load-bearing. ROUTE tests drive the real
// production branch, because a test that calls the shared helper directly passes just as
// happily when production bypasses the helper. CLAIM tests call the helper twice and pin
// the at-most-one-ATTEMPT property itself, with no goroutine in the way.
//
// Every claim assertion needs TWO contenders. "One DISCONNECT was sent" with a single
// sender asserts the default and passes against a deleted claim.

// TestMQTTDisplacementClaimsTheOneDisconnect is the residual the roadmap item names, run
// through both production routes: a remote persist observation evicts this connection with
// 0x8E, and the parse footer then tries to append the settlement's 0x89 sentinel on the
// same socket. Exactly one DISCONNECT may be attempted, and it must be the takeover's.
//
// The capture keeps recording after the close on purpose. The persist path closes
// asynchronously, so a capture that dropped post-close writes would report one frame
// whether or not the claim exists.
func TestMQTTDisplacementClaimsTheOneDisconnect(t *testing.T) {
	e := testNewRejectionEnv(t, "dispclaim")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

	// Arm the second contender: a rejected save gates serving, and an unresolved write on
	// top of it suspends the probe, so the next packet fails closed on the sentinel and
	// the footer answers 0x89. This is the exact overlap CONFORMANCE records.
	stop := testForceContendedRejection(t, e.sess)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the forced save must be rejected")
	}
	stop()
	if !e.markerPending(e.sess) {
		t.Fatal("the forced rejection must arm the marker")
	}
	mqttTestSessStoreSuppressV.Store(true)
	if err := testSessSave(t, e.sess); err == nil {
		t.Fatal("the suppressed save must fail")
	}
	mqttTestSessStoreSuppressV.Store(false)
	if !e.sess.isSeqUncertain() {
		t.Fatal("the suppressed save must leave the session unresolved")
	}

	// The first contender: a real remote persist, observed by the real subscription, which
	// runs the takeover branch of processSessionPersist. Its notification is synchronous
	// and precedes its `go closeConnection`, so a closed connection proves it has run.
	e.remoteTakeover(t, "node2", "dispclaim")
	deadline := time.Now().Add(testRejectionWait)
	for !b.closed() {
		if time.Now().After(deadline) {
			t.Fatalf("the persist observation must evict the displaced connection: wire %x", b.out.bytes())
		}
		time.Sleep(10 * time.Millisecond)
	}

	err := b.deliver(t, []byte{mqttPacketPing, 0})
	if !errors.Is(err, errMQTTSessionOwnershipUnsettled) {
		// Without this the second contender may never have run, and the assertion below
		// would be about a single sender.
		t.Fatalf("the second contender must reach the footer on the sentinel: %v", err)
	}

	reasons := b.disconnectAttempts(t)
	if len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
		t.Fatalf("exactly one DISCONNECT may be ATTEMPTED on one connection [MQTT-3.14.4-1]; "+
			"got reasons %x (wire %x)", reasons, b.out.bytes())
	}
}

// TestMQTTDisconnectClaimConsumedOnce pins the at-most-one-attempt property itself, with
// two direct notifications and no goroutine: whichever displacement path is second, it is
// refused before it writes. Which production paths reach the helper is the route tests'
// question, deliberately not this one's.
func TestMQTTDisconnectClaimConsumedOnce(t *testing.T) {
	e := testNewRejectionEnv(t, "claimonce")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

	b.bc.mqttNotifyTakenOver()
	if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
		t.Fatalf("the first claimant must be told 0x8E: got %x", reasons)
	}
	b.bc.mqttNotifyTakenOver()

	if reasons := b.disconnectAttempts(t); len(reasons) != 1 {
		t.Fatalf("the connection's one DISCONNECT is consumed by the first claimant; got %x", reasons)
	}
	b.bc.mu.Lock()
	claimed := b.bc.mqtt.disconnectSent
	b.bc.mu.Unlock()
	if !claimed {
		t.Fatal("the notification must record that it was attempted, or nothing later is refused")
	}
}

// TestMQTTPersistTakeoverHonorsTheConnAckGate is the [MQTT-3.2.0-1] half, on the persist
// route. mqttAbortAcceptedConnect leaves a refused connection bound until its readLoop
// detaches it, so a displacement can find a bound connection that never saw a CONNACK; the
// takeover 0x8E used to go out anyway, ahead of the CONNACK the client is still waiting
// for. The CONNACKed case is the positive control: it asserts the default, and it exists
// so a gate that failed closed into total silence could not pass the case below it.
func TestMQTTPersistTakeoverHonorsTheConnAckGate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		connAckd bool
	}{
		{"a CONNACKed connection is told 0x8E", true},
		{"a pre-CONNACK connection is closed silently", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, "persistgate")
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			if !tc.connAckd {
				b.forgetConnAck()
			}

			e.remoteTakeover(t, "node2", "persistgate")
			deadline := time.Now().Add(testRejectionWait)
			for !b.closed() {
				if time.Now().After(deadline) {
					t.Fatalf("the persist observation must evict the displaced connection: wire %x", b.out.bytes())
				}
				time.Sleep(10 * time.Millisecond)
			}

			reasons := b.disconnectAttempts(t)
			if tc.connAckd {
				if len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
					t.Fatalf("a CONNACKed displaced client must be told 0x8E: got %x (wire %x)", reasons, b.out.bytes())
				}
				return
			}
			if len(reasons) != 0 {
				t.Fatalf("nothing may precede a client's CONNACK [MQTT-3.2.0-1]; got DISCONNECT reasons %x (wire %x)",
					reasons, b.out.bytes())
			}
		})
	}
}

// The CONNECT-takeover route's gate test needs real sockets and the real
// mqttProcessConnect, whose harness lives behind the mqttport_fulltests tag:
// TestMQTTv5TakeoverBeforeConnAckIsSilent, in mqtt_v5_test.go, beside the CONNACKed twin
// it must suppress.

// TestMQTTShutdownDisconnectYieldsToADisplacement covers the one server-DISCONNECT writer
// that bypasses the enqueue path: shutdownDisconnect writes directly, under a deadline, so
// it claims the notification itself. A client already told 0x8E must not then be told 0x8B.
func TestMQTTShutdownDisconnectYieldsToADisplacement(t *testing.T) {
	t.Run("the drain alone sends 0x8B", func(t *testing.T) {
		// A default-value check, not a claim test: it only keeps the case below from
		// passing against a drain that sends nothing at all.
		e := testNewRejectionEnv(t, "shutdownalone")
		b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
		b.bc.shutdownDisconnect(time.Now().Add(testRejectionWait))
		if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonServerShuttingDown {
			t.Fatalf("the drain must tell a CONNACKed v5 client 0x8B: got %x", reasons)
		}
	})

	t.Run("a displaced connection is not told twice", func(t *testing.T) {
		e := testNewRejectionEnv(t, "shutdownafter")
		b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
		b.bc.mqttNotifyTakenOver()
		b.bc.shutdownDisconnect(time.Now().Add(testRejectionWait))

		reasons := b.disconnectAttempts(t)
		if len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
			t.Fatalf("the drain must not append a second DISCONNECT [MQTT-3.14.4-1]: got %x (wire %x)",
				reasons, b.out.bytes())
		}
	})
}

// TestMQTTPeriodicForeignDisposalHonorsTheConnAckGate drives the PERIODIC resolver to a
// foreign verdict on a bound connection — the third displacement route,
// disposeForeignSessionLocked into mqttNotifyForeignDisposal. The gate that used to live
// inline in that function now lives in the shared tail; this pins that moving it did not
// lose it, through production rather than through the helper.
func TestMQTTPeriodicForeignDisposalHonorsTheConnAckGate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		connAckd bool
	}{
		{"a CONNACKed connection is told 0x8E", true},
		{"a pre-CONNACK connection is closed silently", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, "foreigngate")
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			if !tc.connAckd {
				b.forgetConnAck()
			}

			// An unresolved write of ours, and a record another owner has since moved:
			// the periodic pass resolves that to a foreign verdict.
			e.sess.mu.Lock()
			e.sess.markWriteUncertainLocked(e.sess.seq, []byte(`{"attempted":"ours"}`), 0)
			e.sess.mu.Unlock()
			subject := mqttSessStreamSubjectPrefix + e.sess.domainTk + e.sess.idHash
			if _, err := e.sess.jsa.storeMsg(subject, 0, []byte(`{"owner":"someone-else"}`)); err != nil {
				t.Fatalf("the foreign write must land: %v", err)
			}

			e.asm.reconcileResidentRecordState(e.s)

			e.sess.mu.Lock()
			bound := e.sess.c
			e.sess.mu.Unlock()
			if bound != nil {
				t.Fatal("a foreign verdict must detach the bound client, or the disposal never ran")
			}
			reasons := b.disconnectAttempts(t)
			if tc.connAckd {
				if len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
					t.Fatalf("a CONNACKed disposed client must be told 0x8E: got %x (wire %x)", reasons, b.out.bytes())
				}
				return
			}
			if len(reasons) != 0 {
				t.Fatalf("nothing may precede a client's CONNACK [MQTT-3.2.0-1]; got DISCONNECT reasons %x (wire %x)",
					reasons, b.out.bytes())
			}
		})
	}
}

// TestMQTTBoundSettlementForeignArmHonorsTheConnAckGate is the fourth route: the bound
// rejection settlement's foreign arm, which notifies on this connection's own readLoop
// after releasing the ownership gate. TestMQTTBoundRejectionGatesPingReq is the CONNACKed
// twin in the settlement's own file; this pins that the pre-CONNACK case says nothing.
func TestMQTTBoundSettlementForeignArmHonorsTheConnAckGate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		connAckd bool
	}{
		{"a CONNACKed connection is told 0x8E", true},
		{"a pre-CONNACK connection is closed silently", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, "settlegate")
			e.dropPersistObservations(t, e.idHash)
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			if !tc.connAckd {
				b.forgetConnAck()
			}
			e.remoteTakeover(t, "node2", "settlegate")
			e.rejectSave(t, e.sess)

			if err := b.deliver(t, []byte{mqttPacketPing, 0}); err != nil {
				t.Fatalf("the foreign arm must return nil: %v", err)
			}
			if !b.closed() || e.resident(e.sess) {
				t.Fatalf("the foreign verdict must close and evict, or the arm never ran: closed=%v resident=%v",
					b.closed(), e.resident(e.sess))
			}

			reasons := b.disconnectAttempts(t)
			if tc.connAckd {
				if len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
					t.Fatalf("a CONNACKed disposed client must be told 0x8E: got %x (wire %x)", reasons, b.out.bytes())
				}
				return
			}
			if len(reasons) != 0 {
				t.Fatalf("nothing may precede a client's CONNACK [MQTT-3.2.0-1]; got DISCONNECT reasons %x (wire %x)",
					reasons, b.out.bytes())
			}
		})
	}
}
