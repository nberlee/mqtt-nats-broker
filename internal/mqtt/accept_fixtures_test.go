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
	"testing"
)

// Shared CONNECT and authority fixtures, kept from the files that tested the two-phase
// ownership protocol when that protocol was deleted (issue #133).

func testAccountEpoch(asm *mqttAccountSessionManager) string {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	return asm.sessStreamEpoch
}

// testRefusedConnectV5 connects and expects the CONNECT to be refused.
func testRefusedConnectV5(t *testing.T, o *Options, ci *mqttV5ConnInfo) {
	t.Helper()
	cc, _ := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTExpectDisconnect(t, cc)
	cc.Close()
}

// testServingAt asserts the authority serves and returns it.
func testServingAt(t *testing.T, asm *mqttAccountSessionManager, id, epoch string) *mqttOutboundAuthority {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the authority must serve, got status %v rec %+v", al.status, al.rec)
	}
	return al.rec
}

// testResidentObject is the session object the account holds for cid, bound or not.
func testResidentObject(asm *mqttAccountSessionManager, cid string) *mqttSession {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	return asm.sessions[cid]
}

// testHoldResident keeps a resident object resident while a fixture plants a foreign write
// under it: this broker would otherwise evict it asynchronously when it observes the remote
// persist (or its ownership backstop does), and the CONNECT under test would sometimes meet no
// resident object at all. That is the state a delayed notification leaves — the stale resident
// object the CONNECT's settlement must dispose of itself.
func testHoldResident(t *testing.T, asm *mqttAccountSessionManager, cid string) {
	t.Helper()
	h := getHash(cid)
	asm.mu.Lock()
	asm.testSessPersistDrop = func(x string) bool { return x == h }
	asm.testSessRecordBackstopOff = true
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testSessPersistDrop = nil
		asm.testSessRecordBackstopOff = false
		asm.mu.Unlock()
	})
}

// testClosureDiscards reports whether gen carries a DISCARDING closure.
func testClosureDiscards(asm *mqttAccountSessionManager, cid, epoch, gen string) bool {
	cl, _, st := asm.jsa.loadGenClosure(cid, epoch, gen)
	return st == mqttOutboundValid && cl != nil
}

// testRecordGone reports whether the client's session record is gone.
func testRecordGone(asm *mqttAccountSessionManager, cid string) bool {
	smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
	return err != nil || smsg == nil || mqttDecodeSessionRecord(smsg) == nil
}

// testArmBoundarySettle installs the discard-boundary observation seam for one test.
func testArmBoundarySettle(t *testing.T, asm *mqttAccountSessionManager, f func(id string, steps int)) {
	t.Helper()
	asm.mu.Lock()
	asm.testBoundarySettle = f
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testBoundarySettle = nil
		asm.mu.Unlock()
	})
}

// testResumeHandoff resumes a client's SERVING generation under a fresh tenure the way an
// accepted CONNECT does: one acceptance batch carrying the session record, then its bind. It
// returns the (unchanged) generation and the new tenure.
func testResumeHandoff(t *testing.T, jsa *mqttJSA, domainTk, id, epoch string,
	record *mqttPersistedSession, recordSeq uint64) (gen, tenure string) {
	t.Helper()
	serving, _, err := jsa.servingAuthority(id, epoch)
	if err != nil {
		t.Fatalf("a resume displaces a SERVING authority, and none stands: %v", err)
	}
	_ = record
	_ = recordSeq
	bt, _ := testAcceptAndBind(t, jsa, domainTk, id, epoch, "txn-resume-"+mqttNewInflightNonce(),
		serving.Gen, mqttNewInflightNonce(), false, mqttClosingTargets{})
	return bt.Auth.Gen, bt.Auth.Tenure
}
