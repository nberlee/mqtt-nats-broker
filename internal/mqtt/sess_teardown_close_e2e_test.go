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
	"testing"
)

// The F-105 STANDALONE TEARDOWN authority close (row 8, Gap 1).
//
// A clean start displaces a predecessor through a handoff; a session that ENDS with no
// displacing successor — a zero session-expiry disconnect, a clean-session close, an elapsed
// expiry — has no handoff to close its authority. Before Gap 1 the discard deleted the record
// and left the authority SERVING, so a reconnect met a serving authority with no record and
// could neither resume it nor reserve over it. Now the discard commits a durable teardown
// receipt in the same atomic batch as the marker, then closes the authority DISCARDING on that
// receipt's authority before deleting the record. A reconnect then finds CLOSED and reserves a
// fresh generation over it.

// testF105StandaloneTeardownEnv connects a PERSISTENT v5 session, returns the account, its
// recovery epoch and the serving generation, and the running server's options.
func testF105StandaloneTeardownEnv(t *testing.T, clientID string) (*Server, *mqttAccountSessionManager, string, string, func()) {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ste/t", opts: 1}})
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	epoch := asm.sessStreamEpoch
	asm.mu.RUnlock()
	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		c.Close()
		testMQTTShutdownServer(s)
		t.Fatalf("the persistent connect did not leave a SERVING authority: %+v", al)
	}
	gen := al.rec.Gen

	// End the session with no successor: a DISCONNECT that overrides the session expiry to 0
	// discards the record. This is the standalone teardown — the destroy path with a serving
	// authority to close and no handoff.
	if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
		c.Close()
		testMQTTShutdownServer(s)
		t.Fatalf("writing the zero-expiry DISCONNECT: %v", err)
	}
	c.Close()
	testMQTTExpectSessionClientGone(t, s, clientID)
	return s, asm, epoch, gen, func() { testMQTTShutdownServer(s) }
}

// TestF105StandaloneTeardownClosesAuthorityDiscarding asserts the durable outcome of a
// standalone teardown: the retiring generation carries a DISCARDING closure, and the durable
// teardown receipt that authorized it stands, naming that exact generation with a session-end
// reason. This is the fact a close-admission could never write, made permanent by an authority
// the receipt supplies.
func TestF105StandaloneTeardownClosesAuthorityDiscarding(t *testing.T) {
	const clientID = "f105-standalone-close"
	_, asm, epoch, gen, done := testF105StandaloneTeardownEnv(t, clientID)
	defer done()

	cl, _, st := asm.jsa.loadGenClosure(clientID, epoch, gen)
	if st != mqttOutboundValid || cl == nil {
		t.Fatalf("the retiring generation %q must carry a closure, got status %v", gen, st)
	}
	if !cl.Discard {
		t.Fatalf("the standalone teardown must record a DISCARDING closure, got %+v", cl)
	}

	// The authorization stands and names this exact generation with a session-end reason: a
	// discarding close is authorized, never asserted.
	rcpt, _, rst := asm.jsa.loadTeardownReceipt(asm.domainTk, getHash(clientID), gen)
	if rst != mqttOutboundValid || rcpt == nil {
		t.Fatalf("the durable teardown receipt authorizing generation %q's discard must stand, got status %v", gen, rst)
	}
	if !rcpt.Retire.Discard || rcpt.Retire.Gen != gen {
		t.Fatalf("the receipt must discard generation %q, got %+v", gen, rcpt.Retire)
	}
	if rcpt.Retire.Reason != mqttRetireSessionEnd {
		t.Fatalf("a standalone teardown's receipt reason must be %q, got %q", mqttRetireSessionEnd, rcpt.Retire.Reason)
	}
}

// TestF105StandaloneTeardownPermitsReserveOverClosed is the production regression the close
// exists for: after a standalone teardown a reconnect must be ACCEPTED, reserving a fresh
// generation over the CLOSED authority — never met by a serving authority with a deleted
// record it can neither resume nor displace.
func TestF105StandaloneTeardownPermitsReserveOverClosed(t *testing.T) {
	const clientID = "f105-standalone-reserve"
	s, asm, epoch, oldGen, done := testF105StandaloneTeardownEnv(t, clientID)
	defer done()
	o := s.getOpts()

	c2, r2 := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true},
		o.MQTT.Host, o.MQTT.Port, 3)
	defer c2.Close()
	// A reconnect resumes nothing (the session was discarded) and is ACCEPTED.
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); sp {
		t.Fatalf("the reconnect must not resume a discarded session")
	}

	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the reconnect must install a SERVING authority, got %+v", al)
	}
	if al.rec.Gen == oldGen {
		t.Fatalf("the reconnect must serve a FRESH generation, still %q — it reserved over nothing", oldGen)
	}
}

// TestF105StandaloneTeardownCrashWindowsAreExecutableSpecs documents the cross-stream crash
// windows Codex enumerated (consult 7, section 4). They are executable specifications rather
// than run assertions because reaching them needs a broker parked between two separate
// transactions — the authority close and the record delete — which a single-broker harness
// cannot hold open the way a real crash does. Each window's REQUIRED recovery is already
// implemented and covered indirectly by the adopter and idempotency paths:
//
//   - Marker+receipt committed, authority still SERVING: the marker blocks resume; an adopter
//     (finishAdoptedTeardown) loads the SAME receipt and completes the SAME close, then cleans
//     up and releases. Covered by the adopter's loadTeardownReceipt -> closeTeardownGeneration
//     wiring.
//   - Authority CLOSED, marker still present: never resume that generation; finish the adopted
//     teardown, then reserve a fresh generation over CLOSED. Covered by closeTeardownGeneration's
//     idempotency — a standing DISCARDING closure of the generation is success — and by
//     TestF105StandaloneTeardownPermitsReserveOverClosed for the reserve.
//   - Closure acknowledgement lost: the bounded episode resolves the transition rather than
//     re-composing blind or deleting on a timeout. Covered by closeTeardownGeneration reusing
//     closeGenerationEnrolled's resolve/abort loop.
//   - Record absent while authority SERVING: a FORBIDDEN outcome. The close commits before the
//     record delete, and the seq==0 arm never discards an authorized generation on an absent
//     record — that state is proposal recovery's (Gap 2), not this unit's.
func TestF105StandaloneTeardownCrashWindowsAreExecutableSpecs(t *testing.T) {
	t.Skip("executable specification: the crash-window recoveries are implemented (adopter close, " +
		"idempotent re-close, bounded resolve) and asserted indirectly; a parked-broker harness for " +
		"the cross-stream window is owed with the Gap-2 CONNECT-side recovery work")
}
