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
	"strings"
	"testing"
)

// persistRecord's ownership metadata-commit path (F-105 cutover, Codex design steps 1-2). The
// metadata half of the ownership protocol is written THROUGH persistRecord, so it inherits the
// four invariants a session record write carries: lineage, void-register accounting, the
// ambiguous-write refusal and the record-tx fence. These fault tests exercise that path
// standalone (no accept loop yet — the wiring is a later WP), which is the point of the
// unwired-first order.

// testF105OwnAcceptSession builds a transient session bound to the env's account, seeded from
// the standing session record, plus a RESERVING authority carrying a creation proposal — the
// reserve-before-commit ordering the ownership protocol rests on.
func testF105OwnAcceptSession(t *testing.T, e *testF105Env) (*mqttSession, uint64, mqttOwnershipProposal) {
	t.Helper()
	ps, seq := testF105StandingSession(t, e)
	// The reservation's generation is the SESSION record's generation: the metadata commit
	// validates that the record it installs agrees with the proposal's generation, so the
	// outbound authority generation and the session generation are one, not two namespaces.
	if _, err := e.asm.jsa.opClaimClient(e.id, e.epoch, "txn-own", ps.Gen, "tenure-own", "intent-txn-own"); err != nil {
		t.Fatalf("reserving the client's ownership: %v", err)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Creation == nil {
		t.Fatalf("no standing reservation after opClaimClient: status=%v", al.status)
	}
	proposal, err := mqttProposalFromCreation(al.rec.Creation)
	if err != nil {
		t.Fatalf("deriving the creation proposal: %v", err)
	}
	o := testMQTTDefaultOptions()
	sess := mqttSessionCreate(&e.asm.jsa, e.id, getHash(e.id), ps.Gen, seq, ps.WID, ps.PWID, o)
	sess.asm = e.asm
	sess.domainTk = e.asm.domainTk
	return sess, seq, proposal
}

// TestF105OwnershipMetadataCommitPreservesTheRecordInvariants is the happy path: the
// three-member batch (fact + COMMITTED receipt + record) commits through persistRecord, the
// receipt slot reads committed, and sess.seq / lineage / void are adopted from THIS batch's own
// acknowledged record — never a newer record's sequence.
func TestF105OwnershipMetadataCommitPreservesTheRecordInvariants(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-accept-own")
	defer done()
	sess, baseSeq, proposal := testF105OwnAcceptSession(t, e)

	tok := sess.beginRecordTx()
	detail, err := sess.saveAcceptWithOwnership(sess.jsa, tok, e.id, e.epoch, proposal)
	if err != nil {
		t.Fatalf("the ownership metadata commit was refused: %v", err)
	}

	// The receipt slot the finalize step reads must now say COMMITTED.
	if v := e.asm.jsa.resolveMetadataAttempt(e.asm.domainTk, e.id, e.epoch, proposal); v != mqttMetadataCommitted {
		t.Fatalf("the receipt slot reads %v after the metadata commit, want committed", v)
	}
	// sess.seq adopted the batch's OWN landed record sequence (advanced past the base).
	sess.mu.Lock()
	landed, wid := sess.seq, sess.wid
	sess.mu.Unlock()
	if landed <= baseSeq {
		t.Fatalf("sess.seq is %d, not advanced past the base %d", landed, baseSeq)
	}
	// Lineage: the record on the stream carries the WID persistRecord stamped, and the session
	// adopted it — fork-repair depends on this parent pointer.
	if wid != detail.wid || wid == _EMPTY_ {
		t.Fatalf("session WID %q does not match the stamped write identity %q", wid, detail.wid)
	}
	smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(e.asm.domainTk, getHash(e.id)))
	if lerr != nil {
		t.Fatalf("reading back the committed record: %v", lerr)
	}
	ps := &mqttPersistedSession{}
	if derr := json.Unmarshal(smsg.Data, ps); derr != nil {
		t.Fatalf("decoding the committed record: %v", derr)
	}
	if ps.WID != wid {
		t.Fatalf("the committed record carries WID %q, but the session adopted %q", ps.WID, wid)
	}
	if smsg.Sequence != landed {
		t.Fatalf("the committed record stands at %d, but the session adopted %d", smsg.Sequence, landed)
	}
	// The acceptance fact committed atomically with it.
	if detail.factSubj == _EMPTY_ || len(detail.factBody) == 0 {
		t.Fatalf("the ownership commit carried no acceptance fact")
	}
	if _, fs := e.asm.jsa.readAcceptFactSeq(detail.factSubj, mqttRecordDigest(detail.factBody)); fs != mqttAcceptFactLanded {
		t.Fatalf("the acceptance fact is %v after the commit, want landed", fs)
	}
}

// TestF105OwnershipMetadataCommitRefusesAStaleHandoffPredecessor: a handoff proposal whose
// PrevSessSeq is not the fenced base is refused BEFORE the send, so a delayed handoff can never
// commit against a session record that has moved.
func TestF105OwnershipMetadataCommitRefusesAStaleHandoffPredecessor(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-accept-own-stale")
	defer done()
	sess, baseSeq, _ := testF105OwnAcceptSession(t, e)

	// A handoff proposal naming a DIFFERENT predecessor sequence than the fenced base.
	handoff := mqttOwnershipProposal{
		Kind: mqttProposalHandoff, ID: "intent-stale", Gen: testF105Gen, Tenure: "tenure-stale",
		PrevGen: testF105Gen, PrevTenure: "tenure-own", PrevOwnerTxn: "txn-own",
		PrevSessWID: "wid-x", PrevSessSeq: baseSeq + 999,
	}
	tok := sess.beginRecordTx()
	_, err := sess.saveAcceptWithOwnership(sess.jsa, tok, e.id, e.epoch, handoff)
	if err == nil {
		t.Fatal("a handoff naming a stale predecessor sequence was accepted; it must be refused")
	}
	if !strings.Contains(err.Error(), "expectation") && !strings.Contains(err.Error(), "predecessor") &&
		!strings.Contains(err.Error(), "record") {
		t.Fatalf("the refusal does not name the predecessor/sequence binding: %v", err)
	}
}

// TestF105OwnershipMetadataCommitRefusesAnUnresolvedWrite: the ambiguous-write invariant holds
// on the ownership path exactly as on the ordinary record write — a session with an unresolved
// earlier write refuses to compose a new one.
func TestF105OwnershipMetadataCommitRefusesAnUnresolvedWrite(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-accept-own-uncertain")
	defer done()
	sess, _, proposal := testF105OwnAcceptSession(t, e)

	sess.mu.Lock()
	sess.seqUncertain = true
	sess.mu.Unlock()

	tok := sess.beginRecordTx()
	_, err := sess.saveAcceptWithOwnership(sess.jsa, tok, e.id, e.epoch, proposal)
	if err == nil {
		t.Fatal("the ownership commit ran with an unresolved earlier write; it must refuse")
	}
	if !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("the refusal is not the unresolved-write one: %v", err)
	}
}

// TestF105OwnershipMetadataCommitCertainRejectionLeavesSeqIntact: a record CAS rejection stored
// nothing (fact, receipt and record are one atomic batch), so sess.seq is unchanged and the
// session stays usable — the same certainty the two-member accept path relies on.
func TestF105OwnershipMetadataCommitCertainRejectionLeavesSeqIntact(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-accept-own-reject")
	defer done()
	sess, baseSeq, proposal := testF105OwnAcceptSession(t, e)

	// Force the record CAS to reject by fencing on a base the subject is not at.
	wrong := baseSeq + 4242
	sess.mu.Lock()
	sess.seq = wrong
	sess.mu.Unlock()

	tok := sess.beginRecordTx()
	detail, err := sess.saveAcceptWithOwnership(sess.jsa, tok, e.id, e.epoch, proposal)
	if err == nil {
		t.Fatal("a metadata commit on a stale base was accepted; the record CAS must reject it")
	}
	if !mqttIsRecordCASRejection(detail.rawErr) {
		t.Fatalf("the rejection is not a certain record CAS rejection: %v", detail.rawErr)
	}
	sess.mu.Lock()
	after := sess.seq
	sess.mu.Unlock()
	if after != wrong {
		t.Fatalf("a certain rejection moved sess.seq from %d to %d; nothing was stored, so it must be intact", wrong, after)
	}
}
