// Copyright 2025 The mqtt-nats-bridge Authors
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
	"encoding/json"
	"testing"
)

// NO ORDINARY DECISION DEPENDS ON THE REGISTER'S CLAIM STAMP (F-105 7b, plan-7b §3.5, T12).
// Ownership of an ordinary obligation is the connection's accepted entitlement, compared with the
// serving authority inside the batch that commits the decision. So a register carrying ANOTHER
// incarnation's valid stamp is served normally by the current connection, and a register
// carrying this incarnation's own stamp is refused to a connection whose entitlement no longer
// serves.

// testStampForeign rewrites the admitted register's custody stamp to a valid stamp of another
// incarnation, the shape a takeover history leaves.
func testStampForeign(t *testing.T, f *testWithdrawFix) {
	t.Helper()
	cur, seq := f.register(t)
	if cur == nil {
		t.Fatal("fixture: the admitted register must exist")
	}
	next := *cur
	next.Claim, next.ClaimSeq = mqttNewInflightNonce(), 1
	if _, err := f.e.asm.jsa.storeInflightCAS(f.subject, seq, &next); err != nil {
		t.Fatalf("stamping the register foreign: %v", err)
	}
	f.e.sess.mu.Lock()
	own := f.e.sess.regClaim
	f.e.sess.mu.Unlock()
	if got, _ := f.register(t); got == nil || got.Claim == own {
		t.Fatalf("fixture: the stamp must be foreign, got %+v", got)
	}
}

// testSupersedeTenure installs a successor tenure on the serving authority, as a takeover by
// another connection does: this env's connection keeps the entitlement it was accepted under,
// which no longer serves.
func testSupersedeTenure(t *testing.T, e *testInflightEnv) {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("no authority to supersede: %+v", al)
	}
	next := *al.rec
	next.Tenure = mqttNewInflightNonce()
	next.Rev++
	next.Txn = "txn-takeover-" + mqttNewInflightNonce()
	next.OwnerTxn = next.Txn
	b, _ := json.Marshal(&next)
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.sess.id)), hdr, msg); err != nil {
		t.Fatalf("installing the successor tenure: %v", err)
	}
	if ent, _ := e.c.outboundEntitlement(); ent.Tenure == next.Tenure {
		t.Fatal("fixture: the connection must still hold the superseded tenure")
	}
}

// T12, the foreign-stamp half: every ordinary route proceeds for the current connection.
//
// Mutation M16: claim equality re-added to an ordinary route -> that route refuses.
func TestF105ForeignStampDecidesNothingOnAnOrdinaryRoute(t *testing.T) {
	t.Parallel()
	t.Run("PUBACK completes the exchange", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stamp-puback", 1)
		testStampForeign(t, f)
		if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
			t.Fatalf("PUBACK: %v", err)
		}
		if x := f.exchange(t); x == nil || !mqttExchangePhaseEnded(x.Phase) {
			t.Fatalf("the PUBACK must end the exchange under a foreign stamp: %+v", x)
		}
	})
	t.Run("withdrawal ends the unsent obligation", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stamp-withdraw", 1)
		testStampForeign(t, f)
		if r := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttLicenceOf(f.e.c), nil); r != mqttWithdrawLanded {
			t.Fatalf("withdrawal under a foreign stamp: %v", r)
		}
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
			t.Fatalf("the exchange must read WithdrawnUnsent: %+v", x)
		}
	})
	t.Run("the cancellation tombstone lands", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stamp-tombstone", 1)
		testStampForeign(t, f)
		cur, seq := f.register(t)
		next := *cur
		next.Cancelled, next.CancelOrigin = true, mqttCancelOriginOccupant // F-088: an occupant CAS
		if err := f.e.sess.storeCancellationTombstone(f.subject, seq, &next, mqttLicenceOf(f.e.c)); err != nil {
			t.Fatalf("tombstone under a foreign stamp: %v", err)
		}
		if got, _ := f.register(t); got == nil || !got.Cancelled {
			t.Fatalf("the register must read cancelled: %+v", got)
		}
	})
	t.Run("the resume restores the delivery as work", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stamp-resume", 1)
		testStampForeign(t, f)
		testRestart(f.e)
		if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		f.e.sess.mu.Lock()
		ack := f.e.sess.pendingPublish[f.pi]
		var sendOK bool
		if ack != nil {
			_, sendOK = f.e.sess.beginSendLocked(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq)
		}
		f.e.sess.mu.Unlock()
		if ack == nil || ack.regNonce != f.nonce || !sendOK {
			t.Fatalf("a foreign stamp must not keep the resume from restoring the delivery: entry=%+v sendable=%v", ack, sendOK)
		}
	})
}

// T12, the stale-entitlement half: this incarnation's own stamp does not authorize a connection
// whose entitlement no longer serves. Each owner-dependent route refuses TERMINALLY, with no
// destructive fallback.
//
// Mutation M10: the void's entitlement comparison removed -> the stale connection's write lands.
func TestF105OwnStampWithAStaleEntitlementIsRefused(t *testing.T) {
	t.Parallel()
	t.Run("withdrawal", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stale-withdraw", 1)
		testSupersedeTenure(t, f.e)
		if r := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttLicenceOf(f.e.c), nil); r != mqttWithdrawUnlicensed {
			t.Fatalf("a stale entitlement's withdrawal must be refused as unlicensed, got %v", r)
		}
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
			t.Fatalf("the refused withdrawal must leave the exchange at Publish: %+v", x)
		}
	})
	t.Run("cancellation tombstone", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stale-tombstone", 1)
		testSupersedeTenure(t, f.e)
		cur, seq := f.register(t)
		next := *cur
		next.Cancelled, next.CancelOrigin = true, mqttCancelOriginOccupant // F-088: an occupant CAS
		err := f.e.sess.storeCancellationTombstone(f.subject, seq, &next, mqttLicenceOf(f.e.c))
		if !mqttTombstoneRefusedTerminally(err) {
			t.Fatalf("a stale entitlement's tombstone must be refused terminally, got %v", err)
		}
		if got, _ := f.register(t); got == nil || got.Cancelled {
			t.Fatalf("the refused tombstone must leave the register live: %+v", got)
		}
	})
}
