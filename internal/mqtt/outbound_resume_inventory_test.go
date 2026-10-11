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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// RESUME RESTORES FROM THE INVENTORY (F-105 7b, plan-7b §3.1-§3.2). The generation's duty
// inventory — the ledger's Hi and every ordinal below it, terminal entries included — is the
// root of what a resume restores, and an ordinal it cannot read is uncertainty, never a hole.
// The register walk stays the occupant superset, and an ordinary register the inventory did not
// reach is refused.

// testRestart forgets every in-memory projection and the custody ledger, as a broker restart
// does, so the next resume rebuilds both from the store alone.
//
// The source is reported LIVE: the fixtures' admitted deliveries have a standing source consumer
// but no stored source message, and a proven-unsent obligation over a lost source is legitimately
// consumed (F-102), which is not the population these tests restore.
func testRestart(e *testInflightEnv) {
	lostSourceHook(e, mqttInflightSourceLive)
	e.forgetInMemory()
	e.sess.mu.Lock()
	e.sess.custody = newMQTTCustodyLedger(e.sess.gen)
	e.sess.mu.Unlock()
}

// testDeleteSubject removes the last message on subject from the in-flight stream.
func testDeleteSubject(t *testing.T, e *testInflightEnv, subject string) {
	t.Helper()
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil || smsg == nil {
		t.Fatalf("loading %q to delete it: %v", subject, err)
	}
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("deleting %q: %v", subject, err)
	}
}

// testDutyOrdinalOf finds the ordinal of the exchange duty naming lin, scanning 1..Hi.
func testDutyOrdinalOf(t *testing.T, e *testInflightEnv, gen, lin string) uint64 {
	t.Helper()
	led, _, err := e.asm.jsa.loadGenLedger(e.sess.id, e.asm.sessStreamEpoch, gen)
	if err != nil || led == nil {
		t.Fatalf("loading the ledger of %q: %v", gen, err)
	}
	for ord := uint64(1); ord <= led.Hi; ord++ {
		d, _, derr := e.asm.jsa.loadDutyEntryAt(e.sess.id, e.asm.sessStreamEpoch, gen, ord)
		if derr == nil && d != nil && d.Kind == mqttDutyExchange && d.Ref == lin {
			return ord
		}
	}
	t.Fatalf("no exchange duty names %q below Hi %d", lin, led.Hi)
	return 0
}

// T1. An exchange whose register is gone still holds its charge and its identifier: the
// inventory, not the register walk, is what reaches it.
//
// Mutation M1: the inventory's exchange reservation omitted -> the identifier is handed out.
func TestF105ResumeInventoryReservesAnExchangeWithoutARegister(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "inv-t1", 1)
	e := f.e
	testDeleteSubject(t, e, f.subject)
	testDeleteSubject(t, e, mqttInflightMarkerSubject(e.sess.idHash, f.gen, f.pi))
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	held, charged := e.sess.custody.holdsPI(f.pi), e.sess.custody.chargesPI(f.pi)
	e.sess.mu.Unlock()
	if !held || !charged {
		t.Fatalf("an admitted exchange without a register must hold its identifier and charge: held=%v charged=%v", held, charged)
	}
	if got := testCustodyMints(e.sess, f.pi); got == f.pi {
		t.Fatalf("identifier %d was handed out while its exchange is outstanding", f.pi)
	}
}

// T3. A duty below Hi that is missing, unreadable or undecodable is uncertainty keyed by the
// DUTY subject: a charge held and allocation withheld, never a hole. Its individual re-read then
// decides: still absent stays uncertain; an exchange duty converts to the exchange; a RESOURCE
// duty discharges the custody uncertainty and leaves the resource duty exactly as it was.
//
// Mutations M3 (a hole read as complete) and M3b (a resource-duty re-read left uncertain forever,
// or taken as resource work done).
func TestF105ResumeInventoryHoleIsUncertainty(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		break_ func(t *testing.T, e *testInflightEnv, dutySubject string)
	}{
		{"missing", func(t *testing.T, e *testInflightEnv, dutySubject string) {
			testDeleteSubject(t, e, dutySubject)
		}},
		{"undecodable", func(t *testing.T, e *testInflightEnv, dutySubject string) {
			if _, err := e.asm.jsa.storeMsg(dutySubject, -1, []byte("not a duty")); err != nil {
				t.Fatalf("overwrite the duty: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := testWithdrawAdmitted(t, "inv-t3-"+tc.name, 1)
			e := f.e
			ord := testDutyOrdinalOf(t, e, f.gen, f.lin)
			dutySubject := mqttDutySubject(e.sess.idHash, f.gen, ord)
			original, _ := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, dutySubject)
			if original == nil {
				t.Fatal("fixture: the exchange duty must stand before it is broken")
			}
			tc.break_(t, e, dutySubject)
			testRestart(e)
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			e.sess.mu.Lock()
			_, uncertain := e.sess.custody.uncertainSubjects[dutySubject]
			withholds := e.sess.custody.withholdsAllocation()
			e.sess.mu.Unlock()
			if !uncertain || !withholds {
				t.Fatalf("a %s duty below Hi must be uncertainty that withholds allocation: uncertain=%v withholds=%v",
					tc.name, uncertain, withholds)
			}
			if got := testCustodyMints(e.sess, 9); got != 0 {
				t.Fatalf("allocation proceeded past a hole in the inventory: %d", got)
			}

			// Still broken: the re-read keeps it uncertain.
			testCustodyKick(e.sess)
			testCustodyStaysFor(t, "the broken duty stays uncertain", 300*time.Millisecond, func() bool {
				e.sess.mu.Lock()
				defer e.sess.mu.Unlock()
				_, still := e.sess.custody.uncertainSubjects[dutySubject]
				return still
			})

			// Restored to the exchange duty it was: the re-read converts it to the exchange.
			if _, err := e.asm.jsa.storeMsg(dutySubject, -1, original.Data); err != nil {
				t.Fatalf("restore the duty: %v", err)
			}
			testCustodyKick(e.sess)
			testCustodyEventually(t, "the duty converted to its exchange", func() bool {
				return testCustodyHasExchange(e.sess, f.lin)
			})
			if got := testCustodyMints(e.sess, f.pi); got == f.pi {
				t.Fatal("the recovered exchange's identifier was handed out")
			}
		})
	}
}

// T3, the resource arm: a hole whose re-read decodes to a RESOURCE duty discharges the custody
// uncertainty — a resource carries no identifier and no Receive Maximum charge — and the duty
// itself is left exactly as the inventory records it.
func TestF105ResumeInventoryResourceDutyDischargesCustodyOnly(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "inv-t3-resource", 1)
	e := f.e
	led, _, err := e.asm.jsa.loadGenLedger(e.sess.id, e.asm.sessStreamEpoch, f.gen)
	if err != nil || led == nil {
		t.Fatalf("loading the ledger: %v", err)
	}
	// A resource duty at a fresh ordinal, and the ledger advanced over it: the population a
	// provision leaves.
	ord := led.Hi + 1
	resource := &mqttDutyEntry{ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Gen: f.gen, Ord: ord,
		Kind: mqttDutyProvision, Ref: mqttNewInflightNonce(), State: mqttDutyPending}
	body, _ := json.Marshal(resource)
	dutySubject := mqttDutySubject(e.sess.idHash, f.gen, ord)
	next := *led
	next.Hi = ord
	lbody, _ := json.Marshal(&next)
	if _, err := e.asm.jsa.storeMsg(mqttGenLedgerSubject(e.sess.idHash, f.gen), -1, lbody); err != nil {
		t.Fatalf("advance the ledger: %v", err)
	}
	// Unreadable at the census, readable at the re-read.
	var hide atomic.Bool
	hide.Store(true)
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		out, err := e.asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
		if hide.Load() {
			for i, s := range subjects {
				if s == dutySubject && i < len(out) {
					out[i] = indeterminateResp()
				}
			}
		}
		return out, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testInflightLoadMulti = nil; e.asm.mu.Unlock() })
	if _, err := e.asm.jsa.storeMsg(dutySubject, -1, body); err != nil {
		t.Fatalf("store the resource duty: %v", err)
	}
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, uncertain := e.sess.custody.uncertainSubjects[dutySubject]
	e.sess.mu.Unlock()
	if !uncertain {
		t.Fatal("an unreadable duty below Hi must be uncertainty")
	}
	hide.Store(false)
	testCustodyKick(e.sess)
	testCustodyEventually(t, "the resource duty discharged the custody uncertainty", func() bool {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		_, still := e.sess.custody.uncertainSubjects[dutySubject]
		return !still && !e.sess.custody.withholdsAllocation()
	})
	d, _, derr := e.asm.jsa.loadDutyEntryAt(e.sess.id, e.asm.sessStreamEpoch, f.gen, ord)
	if derr != nil || d == nil || d.State != mqttDutyPending || d.Kind != mqttDutyProvision || d.Outcome != _EMPTY_ {
		t.Fatalf("the discharge must declare nothing about the resource's work: %+v (%v)", d, derr)
	}
}

// T7. An ordinary non-tombstone register whose exchange the inventory did not reach is REFUSED:
// it reserves its identifier and nothing acts on it. Restoration never manufactures admission
// from a register.
//
// Mutation M6: the refusal removed -> the register is restored as a sendable delivery.
func TestF105ResumeRefusesAnOrdinaryRegisterTheInventoryDidNotReach(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "inv-t7", 1)
	e := f.e
	// A register written ALONE, naming an obligation no admission created: a shape this broker
	// never writes (the register, its exchange and its duty land in one batch), beside a
	// complete inventory.
	const pi = uint16(9)
	nonce := mqttNewInflightNonce()
	subject := mqttInflightSubject(e.sess.idHash, f.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: testRealPubRelSrcDur, Seq: f.sseq + 1, QoS: 1, Nonce: nonce,
	})); err != nil {
		t.Fatalf("plant the unadmitted register: %v", err)
	}
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	var sendOK bool
	if ack != nil {
		_, sendOK = e.sess.beginSendLocked(e.c, pi, testRealPubRelSrcDur, f.sseq+1)
	}
	_, mapped := e.sess.cpending[testRealPubRelSrcDur][f.sseq+1]
	withholds := e.sess.custody.withholdsAllocation()
	admitted := e.sess.pendingPublish[f.pi]
	e.sess.mu.Unlock()
	if withholds {
		t.Fatal("fixture: the inventory must be complete, or the refusal decides nothing")
	}
	if ack == nil {
		t.Fatal("the unreached register must still reserve its identifier")
	}
	if sendOK || mapped || ack.regNonce == nonce {
		t.Fatalf("an unreached register must not be restored as work: sendable=%v mapped=%v actionable=%v",
			sendOK, mapped, ack.regNonce == nonce)
	}
	if admitted == nil || admitted.regNonce != f.nonce {
		t.Fatalf("the admitted delivery beside it must be restored: %+v", admitted)
	}
	if cur, _, _ := e.asm.loadInflightRecord(subject); cur == nil {
		t.Fatal("the refused register is preserved, never destroyed on the refusal")
	}
}

// T7b. A pending cancellation tombstone that won BEFORE any admission — no exchange, no ledger —
// takes the drop route across a crash: its identifier is released as before, and allocation is
// NOT withheld (no exchange duty names it; the generation's ledger may hold only the session's
// resource duties).
//
// Mutation M6b: the tombstone row removed, so the tombstone is refused as unreached (opaque).
func TestF105ResumeDropsAPendingTombstoneWithoutAnExchange(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "inv-t7b")
	testReadyForRealPubRelDelivery(t, e)
	e.acceptUnderServingAuthority(t)
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("allocate: pi=%d isNew=%v", pi, isNew)
	}
	// The cancellation lands its create-only tombstone before any admission was written.
	//
	// RE-EXPRESSED by F-097/F-088 (plan rev 20, seven-red-tests table): the fixture used to hand
	// a hand-built body to the tombstone sink, a shape that no longer carries enough to be told
	// apart from a copied admitted register. It now runs the REAL producer, which writes the
	// fence with its written provenance (`pending-create`); the drop and PI-reclamation contract
	// asserted below is unchanged, and no verdict-retention prerequisite is involved.
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if err := e.sess.markInflightRegisterCancelledPending(e.c, mqttRegCancellation{
		pi: pi, nonce: nonce, durable: testRealPubRelSrcDur, sseq: testRealPubRelSrcSeq,
		reply: testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1),
	}, mqttLicenceOf(e.c)); err != nil {
		t.Fatalf("land the pending tombstone: %v", err)
	}
	if cur, _, _ := e.asm.loadInflightRecord(subject); cur == nil || !mqttValidPendingFence(subject, cur) {
		t.Fatalf("fixture: the producer must write a pending fence, got %+v", cur)
	}
	if x, _, _ := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, nonce); x != nil {
		t.Fatalf("fixture: no admission ran, so the tombstone has no exchange, got %+v", x)
	}
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPublish[pi]
	withholds := e.sess.custody.withholdsAllocation()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if reserved || withholds || incomplete {
		t.Fatalf("a pre-admission tombstone takes the drop route: reserved=%v withholds=%v incomplete=%v",
			reserved, withholds, incomplete)
	}
	e.waitRegisterCount(t, 0)
}

// T8. A long history is read WHOLE: every ordinal up to Hi, terminal entries included, and no
// subject census is consulted. A hole near the end is uncertainty. The read count is recorded for
// the docs as a cost measurement, not presented as a scaling bound.
//
// Mutation M7: the traversal capped at the current in-flight count, or at its first page.
func TestF105ResumeInventoryReadsEveryOrdinal(t *testing.T) {
	t.Parallel()
	const hi = 5000
	const hole = hi - 1
	f := testWithdrawAdmitted(t, "inv-t8", 1)
	e := f.e
	led, _, err := e.asm.jsa.loadGenLedger(e.sess.id, e.asm.sessStreamEpoch, f.gen)
	if err != nil || led == nil || led.Hi >= hi {
		t.Fatalf("fixture ledger: %+v (%v)", led, err)
	}
	for ord := led.Hi + 1; ord <= hi; ord++ {
		if ord == hole {
			continue
		}
		d := &mqttDutyEntry{ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Gen: f.gen, Ord: ord,
			Kind: mqttDutyCleanup, Ref: mqttNewInflightNonce(), State: mqttDutyTerminal, Outcome: "test-terminal"}
		body, _ := json.Marshal(d)
		if _, err := e.asm.jsa.storeMsg(mqttDutySubject(e.sess.idHash, f.gen, ord), -1, body); err != nil {
			t.Fatalf("store duty %d: %v", ord, err)
		}
	}
	next := *led
	next.Hi = hi
	lbody, _ := json.Marshal(&next)
	if _, err := e.asm.jsa.storeMsg(mqttGenLedgerSubject(e.sess.idHash, f.gen), -1, lbody); err != nil {
		t.Fatalf("advance the ledger: %v", err)
	}
	var dutyReads atomic.Int64
	dutyPrefix := mqttF105SubjectPrefix() + mqttDutyClassTk + "."
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		for _, s := range subjects {
			if strings.HasPrefix(s, dutyPrefix) {
				dutyReads.Add(1)
			}
		}
		return e.asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testInflightLoadMulti = nil; e.asm.mu.Unlock() })
	var census atomic.Bool
	obs := func(filter string, _ int) (mqttSubjectPage, error) {
		if strings.Contains(filter, mqttExchangeClassTk) || strings.Contains(filter, mqttDutyClassTk) {
			census.Store(true)
		}
		return mqttSubjectPage{}, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&obs)
	t.Cleanup(func() { e.asm.jsa.exchangeSubjectsObs.Store(nil) })

	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if got := dutyReads.Load(); got < hi {
		t.Fatalf("the traversal read %d duty ordinals, want every one of %d", got, hi)
	}
	if census.Load() {
		t.Fatal("the traversal consulted a subject census; the inventory is the root")
	}
	holeSubject := mqttDutySubject(e.sess.idHash, f.gen, hole)
	e.sess.mu.Lock()
	_, uncertain := e.sess.custody.uncertainSubjects[holeSubject]
	held := e.sess.custody.holdsPI(f.pi)
	e.sess.mu.Unlock()
	if !uncertain {
		t.Fatalf("a hole at ordinal %d must be uncertainty", uint64(hole))
	}
	if !held {
		t.Fatal("the live exchange at the head of the history must still be held")
	}
	t.Logf("T8 cost: %d duty reads in batches of %d for Hi %d", dutyReads.Load(), mqttInflightLoadBatch, hi)
}
