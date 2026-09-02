//go:build mqttport_fulltests && mqttport_experimental_shared_qos2

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
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// TestQ2AffOrphanedSharedQoS2RecordRefusesRestore is the CONNECT half, and it is
// the enforcement point that makes the rule real: createOrRestoreSessionWithin
// validates the generation and nothing else, so without this the record resumes
// and is REWRITTEN before the sweep ever sees it.
//
// The refused rows assert three facts a migration, a discard or a skip-only
// regression would each break: no CONNACK, the record byte-identical at the same
// stream sequence afterwards, and the full actionable diagnostic. The accepted
// rows assert an ordinary CONNACK, which is what discriminates against a rule that
// over-refuses.
//
// Mutation M-A: remove the mqttValidateRecordAffinePair call from
// createOrRestoreSessionWithin and the refused rows resume normally.
// Mutations M-D / M-E: over-refuse, and the QoS 0/1 and un-minted rows fail.
func TestQ2AffOrphanedSharedQoS2RecordRefusesRestore(t *testing.T) {
	for _, tc := range testG7Cases(t) {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			// Only the STARTUP sweep may run: a periodic pass could arm a timer on the
			// fixture between the write and the CONNECT.
			defer setMQTTSessionExpirySweepInterval(time.Hour)()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			asm := testReservedProbeASM(t, s, o)
			// This test is about the CONNECT restore path alone. The record watch would
			// otherwise observe the planted record and reconcile it through a mechanism
			// this test is not about.
			testMQTTSuppressRecordWatch(t, asm)

			cid := "g7-connect-" + nuid.Next()
			ps := tc.build()
			// Never-expire keeps every expiry mechanism away, so a moved sequence can
			// only have been the restore.
			ps.ExpiryInterval = mqttSessionNeverExpire
			testWriteLegacyPersistedSession(t, asm, cid, ps)
			seq, data := testLoadRawSessionMsg(t, asm, cid)

			if tc.wantErr == _EMPTY_ {
				c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
					clientID:   cid,
					cleanStart: false,
					props:      mqttV5ConnPropsSessionExpiry(300),
				}, o.MQTT.Host, o.MQTT.Port)
				defer c.Close()
				if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
					t.Fatalf("a legal record was refused at CONNECT: reason 0x%02x", reason)
				}
				return
			}

			requireIntact := func(what string) {
				t.Helper()
				gotSeq, gotData := testLoadRawSessionMsg(t, asm, cid)
				if gotSeq != seq {
					t.Errorf("%s: session record moved from sequence %v to %v; the refusal wrote to it", what, seq, gotSeq)
				}
				if !bytes.Equal(gotData, data) {
					t.Errorf("%s: session record content changed\nbefore: %s\n after: %s", what, data, gotData)
				}
			}

			for _, cleanStart := range []bool{false, true} {
				c, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{
					clientID:   cid,
					cleanStart: cleanStart,
					props:      mqttV5ConnPropsSessionExpiry(300),
				}, o.MQTT.Host, o.MQTT.Port)
				testMQTTExpectDisconnect(t, c)
				c.Close()
				// A CLEAN START attempt is refused identically, and deliberately:
				// createOrRestoreSession runs before processConnect's clean-session
				// check, and the discard it would take derives shared durable names
				// from the record's own keys.
				requireIntact(fmt.Sprintf("after the refused connect (cleanStart=%v)", cleanStart))
			}

			// Compared in full rather than fragment by fragment: a negated message
			// contains every fragment while instructing the opposite.
			_, _, err := asm.createOrRestoreSession(cid, s.getOpts())
			// Compared in full rather than fragment by fragment: a negated message
			// ("do not stop ..., do not recreate ...") contains every fragment while
			// instructing the opposite.
			want := fmt.Sprintf("restoring session %q: %s; this record was written by a "+
				"pre-release development broker and its format is not supported: stop this "+
				"account's brokers and recreate the bridge-owned JetStream state (the $MQTT_* "+
				"streams)", cid, tc.wantErr)
			if err == nil || err.Error() != want {
				t.Fatalf("createOrRestoreSession error:\n got: %v\nwant: %s", err, want)
			}
		})
	}
}

// TestQ2AffOrphanedSharedQoS2RecordSurvivesTheExpirySweep pins the STARTUP sweep
// half. The sweep stamps DisconnectedAt on a record whose client it believes it
// owned, which is a full record WRITE, and it arms a timer that would go on to
// claim and delete the same record — F-058's rule is that a refused record is
// never claimed, published, cancelled or deleted, so this one is skipped whole.
//
// Mutation M-C1: remove the validation before the startup-sweep rewrite and the
// record is restamped at a new sequence.
func TestQ2AffOrphanedSharedQoS2RecordSurvivesTheExpirySweep(t *testing.T) {
	o := testMQTTDefaultOptions()
	defer setMQTTSessionExpirySweepInterval(time.Hour)()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testReservedProbeASM(t, s, o)
	testMQTTSuppressRecordWatch(t, asm)

	cid := "g7-sweep-" + nuid.Next()
	ps := &mqttPersistedSession{
		SharedSubs: map[string]byte{testG7SharedKey(t, "g", "g7/sweep"): 2},
		// DisconnectedAt zero and a finite interval is exactly the shape the sweep
		// rewrites: it reads "a connected client whose owner went down".
		ExpiryInterval: 300,
	}
	testWriteLegacyPersistedSession(t, asm, cid, ps)
	seq, data := testLoadRawSessionMsg(t, asm, cid)

	asm.sweepSessionExpiries(s)

	gotSeq, gotData := testLoadRawSessionMsg(t, asm, cid)
	if gotSeq != seq || !bytes.Equal(gotData, data) {
		t.Fatalf("the sweep rewrote a record the broker cannot interpret: sequence %v -> %v\nbefore: %s\n after: %s",
			seq, gotSeq, data, gotData)
	}
	// ...and it armed nothing, so nothing is queued to delete it either.
	asm.mu.Lock()
	_, armed := asm.pendingExpiries[string(getHash(cid))]
	asm.mu.Unlock()
	if armed {
		t.Fatal("the sweep armed an expiry timer on a refused record; the timer's own path would claim and delete it")
	}
}

// TestQ2AffOrphanedSharedQoS2RecordSurvivesTimerExpiry pins the TIMER-FIRED half,
// which is a separately removable decode path and the sharper of the two: it
// CLAIMS the record and deletes it, destroying the state an assignment owed to a
// started delivery is arbitrated from.
//
// Mutation M-C2: remove the validation from expireSessionFromRecord and the record
// is claimed and deleted.
func TestQ2AffOrphanedSharedQoS2RecordSurvivesTimerExpiry(t *testing.T) {
	o := testMQTTDefaultOptions()
	defer setMQTTSessionExpirySweepInterval(time.Hour)()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testReservedProbeASM(t, s, o)
	testMQTTSuppressRecordWatch(t, asm)

	cid := "g7-expire-" + nuid.Next()
	testWriteLegacyPersistedSession(t, asm, cid, &mqttPersistedSession{
		SharedSubs: map[string]byte{testG7SharedKey(t, "g", "g7/expire"): 2},
		// Overdue, so loadExpiringSessionRecord reports it due and the path runs all
		// the way to the claim rather than re-arming.
		ExpiryInterval: 1,
		DisconnectedAt: time.Now().Add(-time.Hour).Unix(),
	})
	seq, data := testLoadRawSessionMsg(t, asm, cid)

	asm.expireSessionFromRecord(s, string(getHash(cid)))

	gotSeq, gotData := testLoadRawSessionMsg(t, asm, cid)
	if gotSeq != seq || !bytes.Equal(gotData, data) {
		t.Fatalf("expiry acted on a record the broker cannot interpret: sequence %v -> %v\nbefore: %s\n after: %s",
			seq, gotSeq, data, gotData)
	}
}
