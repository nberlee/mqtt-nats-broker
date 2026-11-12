// Copyright 2020-2026 The NATS Authors
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

// Building and writing the persisted session record: the record body, framed and batched
// writes, accept and ownership batches, and adopting committed metadata.

package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// buildPersistedSessionLocked builds the on-stream persisted form of the session.
// sess.mu must be held. Shared by save() and clear()'s pre-teardown snapshot, so the
// teardown marker preserves the same full resumable state a save would write rather than a
// partial record.
//
// Persist only consumer configs that still correspond to a live subscription:
//   - Shared (DeliverGroup) consumers are group-owned and reconstructed from
//     SharedSubs on restore; never persist them in Cons, where they would be
//     redundant with the sole authority that already names them.
//   - A plain consumer is persisted only while its subscription exists in Subs,
//     so a consumer removed from Subs but not yet torn down (e.g. UNSUBSCRIBE
//     persisting before teardown, or a rolled-back failed SUBSCRIBE) is not
//     written with no matching Subs entry. The level-up sid "<X> fwc" maps back
//     to the main membership key "<X>.>". Spec5 [4.8.2].
func (sess *mqttSession) buildPersistedSessionLocked() mqttPersistedSession {
	sel := sess.recordSelectionLocked()
	cons := sel.cons
	var drains map[string]*mqttPersistedDrain
	for sid, d := range sel.drains {
		if drains == nil {
			drains = make(map[string]*mqttPersistedDrain, len(sel.drains))
		}
		drains[sid] = d.persisted()
	}
	return mqttPersistedSession{
		Origin:            sess.jsa.id,
		ID:                sess.id,
		Clean:             sess.clean,
		Subs:              sess.subs,
		NoLocal:           mqttFilterSetToList(sess.noLocalSubs),
		RetainAsPublished: mqttFilterSetToList(sess.rapSubs),
		SubIDs:            sess.subIDs,
		SharedSubs:        sess.sharedSubs,
		Cons:              cons,
		PubRel:            sess.pubRelConsumer,
		Q2Aff:             sess.q2AffConsumer,
		Q2AffGen:          sess.q2AffGen,
		Gen:               sess.gen,
		VoidedRegs:        mqttSortedNonces(sess.voidedRegNonces),
		AcqReleases:       sess.acqReleasesSortedLocked(),
		Drains:            drains,
		Will:              sess.will,
		ExpiryInterval:    sess.expiryInterval,
		DisconnectedAt:    sess.disconnectedAt,
	}
}

// Persists a session, under an OPEN record transaction that tok must identify.
//
// The token is required, not optional: a transaction that starts after the
// mutation it is meant to protect cannot make that mutation invisible to another
// writer, so every writer opens its transaction before its first mutation and
// threads the token down. A stale or copied token is refused even within the same
// generation — see sess_recordtx.go.
//
// Lock not held on entry.
func (sess *mqttSession) save(tok recordTx) error {
	_, err := sess.saveWithDetail(tok)
	return err
}

// mqttSessSaveDetail is the raw identity of one attempted record write: the CAS
// base sequence, the exact bytes (body for the digest a readback compares,
// framed for a byte-identical retry), and the store-boundary error with its
// TYPE intact. It exists because save() re-formats the store error with %v,
// destroying the CAS kind — and 10071 (the subject moved) versus 10164 (clustered
// contention) demand opposite dispositions, so a caller that must turn the
// rejection into a DECISION captures it here, at the boundary, rather than
// parsing it back out of a string. See sess_resume_reconcile.go.
type mqttSessSaveDetail struct {
	baseSeq  uint64
	body     []byte // the record bytes a readback returns (digest identity)
	framed   []byte // CAS header + body, exactly as sent (for an identical retry)
	hdr      int
	domainTk string
	cidHash  string
	rawErr   error // the store-boundary error, type preserved; nil on success
	// wid / pwid are the LINEAGE these bytes carry: this write's own minted identity
	// and the record it was derived from. Carried on the detail because everything
	// that later proves or classifies this attempt holds only the detail — the three
	// markWriteUncertainLocked calls and resolveContendedRecordSave's landed arms —
	// so without it there is nothing for them to adopt, and the next save would parent
	// itself to whatever memory holds instead of to the record that actually landed.
	// See sess_fork_lineage.go.
	wid, pwid string
	// voidedRegsRev is the void-register revision these bytes MARSHALLED, so a
	// caller that proves them durable outside saveWithDetail's own success arm —
	// resolveContendedRecordSave classifies a 10164-rejected write landed by an
	// exact-byte retry or a digest match — credits exactly what they carried. See
	// mqttPendingWrite.
	voidedRegsRev uint64
	// acceptShape is the acceptance transition this write committed with, record member
	// included, exactly as sent: its ambiguity is resolved from the authority it installs,
	// and a re-send re-sends this transition whole, never the record alone.
	acceptShape *mqttAuthorityTransition
}

// mqttFrameRecordWrite frames a record body for the store: the CAS header requiring
// the subject's last sequence to be seq, followed by the body. It returns the framed
// bytes and the header length — the two arguments storeSessionMsg takes, and the two
// an ambiguous write keeps so it can be re-attempted verbatim.
//
// NO record write is unconditional, a first-ever create (seq 0) included: the header
// is emitted at every sequence, and at 0 it means "the subject must currently be
// EMPTY". That is the whole arbitration this broker has for two brokers creating one
// session from nothing — a headerless create lands whatever else is there, which
// erases a record another broker was acknowledged for, within a single leader term
// and with no rejection anywhere.
//
// The dependency honors it at both boundaries (nats-server v2.15.2): the store folds
// a not-found subject to fseq 0 (server/stream.go), and the clustered pre-proposal
// check does the same AND enrolls the write in the in-process contention checks
// (server/jetstream_batching.go), which is what makes a concurrent create fail 10164
// rather than silently interleave.
//
// So a create can now be REJECTED, which is a new outcome for its callers: 10071 for
// a subject that is no longer empty — and, deliberately, for a load error the server
// folds into the same code, so 10071 never proves a record exists — and 10164 for
// contention. Both are mqttIsRecordCASRejection, so the write is CERTAIN not to have
// stored anything; what turns that certainty into progress is the CONNECT's re-drive.
// See sess_uncertain.go and the re-drive in mqttProcessConnect.
func mqttFrameRecordWrite(seq uint64, body []byte) ([]byte, int) {
	return mqttFrameBatchWrite(seq, body, _EMPTY_, 0, false)
}

// mqttFrameBatchWrite is mqttFrameRecordWrite's sibling for one message of an
// atomic batch: the same per-subject CAS header, plus the headers that bind this
// message to the rest of the batch. batchID empty emits no batch headers at all,
// which is what makes the single-write form above literally the same framing it
// always was rather than a special case of a new one.
//
// The binding between the two messages of an accept batch is ATOMICITY, never a
// cross-subject CAS: each message keeps its own Nats-Expected-Last-Subject-Sequence
// and the server evaluates both before committing either. The cross-subject variant
// is refused inside a batch with 10164, which this codebase reads as transient
// contention — so a design that reached for it would retry a permanent
// misconfiguration forever.
//
// commit marks the LAST message. Until the server sees it, an incomplete batch
// stores nothing.
func mqttFrameBatchWrite(seq uint64, body []byte, batchID string, batchSeq int, commit bool) ([]byte, int) {
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatInt(int64(seq), 10))
	bb.WriteString(CR_LF)
	if batchID != _EMPTY_ {
		bb.WriteString(JSBatchId)
		bb.WriteString(":")
		bb.WriteString(batchID)
		bb.WriteString(CR_LF)
		bb.WriteString(JSBatchSeq)
		bb.WriteString(":")
		bb.WriteString(strconv.Itoa(batchSeq))
		bb.WriteString(CR_LF)
		if commit {
			bb.WriteString(JSBatchCommit)
			bb.WriteString(":1")
			bb.WriteString(CR_LF)
		}
	}
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	bb.Write(body)
	return bb.Bytes(), hdr
}

// mqttAcceptAttempt marks a save as an ACCEPTED CONNECT's, and carries the two
// identities the permanent acceptance fact is built from. It is a struct rather
// than a boolean deliberately: a boolean at this boundary would be one flag away
// from any of the seven other saves minting a fact, and only one save in a
// connection's life may — the CONNECT-internal save after the grant point must not.
type mqttAcceptAttempt struct {
	// clientID is the EXACT client identifier, of which the record subject carries
	// only the hash. Suppression is decided on it.
	clientID string
	// sessEpoch is the incarnation this acceptance belongs to, so a fact is never
	// comparable against sequences of another one.
	sessEpoch string
	// ownJSA is the ownership protocol's deadline view, used for the metadata commit when
	// ownProposal is set, so that commit honours the CONNECT's one deadline like every other
	// request of the protocol (issue #107 8a). nil means the session's own assistant.
	ownJSA *mqttJSA
	// shape, when set, makes this accept one batch of an acceptance shape (accept_shapes.go):
	// the authority transition the CONNECT composed, completed here with the fenced record
	// member and the acceptance fact, and committed as one act. It replaces ownProposal's
	// reserve-then-metadata-then-finalize sequence.
	shape *mqttAuthorityTransition
}

// saveWithDetail is save()'s body; it additionally returns the attempted
// write's raw identity so a caller can classify a definite CAS rejection by
// kind and retry (or read back) the exact bytes it sent. The returned error is
// unchanged from save()'s.
//
// Lock not held on entry.
func (sess *mqttSession) saveWithDetail(tok recordTx) (mqttSessSaveDetail, error) {
	return sess.persistRecord(tok, nil)
}

// saveAcceptShape is the accepting CONNECT's save under the acceptance shapes: the composed
// transition, the acceptance fact and the session record land as ONE atomic batch through the
// fenced boundary every session record write uses (lineage, void accounting, the
// ambiguous-write refusal, the record-tx fence). Lock not held on entry.
func (sess *mqttSession) saveAcceptShape(jsa *mqttJSA, tok recordTx, clientID, sessEpoch string,
	shape *mqttAuthorityTransition) (mqttSessSaveDetail, error) {
	return sess.persistRecord(tok, &mqttAcceptAttempt{
		clientID: clientID, sessEpoch: sessEpoch, ownJSA: jsa, shape: shape,
	})
}

// commitAcceptShape completes the composed acceptance with the fenced record and fact members
// and commits it. On success it adopts exactly as a committed metadata batch does.
func (sess *mqttSession) commitAcceptShape(accept *mqttAcceptAttempt, detail mqttSessSaveDetail,
	recSubj string) (mqttSessSaveDetail, error) {
	t := accept.shape
	if t == nil {
		return detail, fmt.Errorf("refusing to persist session %q: an acceptance with no acceptance shape", sess.id)
	}
	t.Record = &mqttTransitionMember{subj: recSubj, expect: detail.baseSeq, body: detail.body}
	detail.acceptShape = t
	ojsa := sess.jsa
	if accept.ownJSA != nil {
		ojsa = accept.ownJSA
	}
	authSeq, err := ojsa.commitAuthorityTransition(t)
	if err != nil {
		detail.rawErr = err
		return detail, fmt.Errorf("unable to persist session %q acceptance (seq=%v): %w", sess.id, detail.baseSeq, err)
	}
	sess.adoptCommittedMetadata(detail, authSeq+1)
	return detail, nil
}

// persistRecord is the single store boundary for a session record. accept is nil
// for every write but the accepting CONNECT's.
func (sess *mqttSession) persistRecord(tok recordTx, accept *mqttAcceptAttempt) (mqttSessSaveDetail, error) {
	var detail mqttSessSaveDetail
	if h := mqttTestSessSaveHookV.Load(); h != nil {
		if err := (*h)(sess); err != nil {
			return detail, err
		}
	}
	sess.mu.Lock()
	if err := sess.validateRecordTxLocked(tok); err != nil {
		sess.mu.Unlock()
		return detail, fmt.Errorf("refusing to persist session %q: %w", sess.id, err)
	}
	// An unresolved earlier write means sess.seq may be stale, so this CAS would be
	// meaningless: it could fail against a record that is in fact ours, or — worse,
	// once resolution adopts a sequence — overwrite one that is not. Refuse until a
	// readback has classified it. See sess_uncertain.go.
	if sess.seqUncertain {
		sess.mu.Unlock()
		return detail, fmt.Errorf("refusing to persist session %q: an earlier record write is unresolved", sess.id)
	}
	ps := sess.buildPersistedSessionLocked()
	// Stamp the lineage HERE, at the store boundary, never in the builder: the builder
	// is also clear()'s pre-teardown snapshot, so minting there would write a parent
	// pointer naming a record that was never stored. This write's parent is the record we
	// were last acknowledged for and nothing else — a session holding none, including one
	// a teardown re-minted, writes a PARENTLESS create — which sess.mu makes stable across
	// the two statements. See parentForNextWriteLocked.
	ps.WID, ps.PWID = mqttNewRecordWID(), sess.parentForNextWriteLocked()
	// body is what a readback returns (the CAS header below is a message header,
	// not part of the stored data), so it is what an ambiguous write is identified
	// by.
	body, _ := json.Marshal(&ps)
	b := body

	domainTk, cidHash := sess.domainTk, sess.idHash
	seq := sess.seq
	wid, pwid := ps.WID, ps.PWID
	// The void-register revision this write MARSHALLS, sampled in the same critical
	// section as the marshal, which is what makes it exact: the record above carries
	// every nonce recorded at or before it and none recorded after, so whoever later
	// proves these bytes durable can credit precisely this much. EVERY landed save
	// credits it — the accounting is a property of the durability boundary, not of the
	// path that happened to be persisting. See mqttPendingWrite.
	voidedRev := sess.voidedRegsRev
	sess.mu.Unlock()

	if h := mqttTestSessRecordBodyHookV.Load(); h != nil {
		(*h)(sess, body)
	}

	var resp *JSPubAckResponse
	var err error
	if accept == nil {
		b, hdr := mqttFrameRecordWrite(seq, b)
		detail = mqttSessSaveDetail{
			baseSeq: seq, body: body, framed: b, hdr: hdr,
			domainTk: domainTk, cidHash: cidHash, voidedRegsRev: voidedRev,
			wid: wid, pwid: pwid,
		}
		resp, err = sess.jsa.storeSessionMsg(domainTk, cidHash, hdr, b)
	} else {
		// A NEWLY COMPOSED acceptance attempt: a fresh acceptID, and the acceptance
		// time sampled here rather than at function entry. Each call composes one
		// attempt, so a re-drive that repeats the write composes a new one — which is
		// right, because the previous one is CERTAIN not to have stored anything. Only
		// an AMBIGUOUS re-send preserves the attempt, and it does so by re-sending
		// these exact bodies. See mqttFrozenMetadataAttempt.
		return sess.commitAcceptShape(accept, mqttSessSaveDetail{
			baseSeq: seq, body: body,
			domainTk: domainTk, cidHash: cidHash, voidedRegsRev: voidedRev,
			wid: wid, pwid: pwid,
		}, mqttSessSubject(domainTk, cidHash))
	}
	if h := mqttTestSessStoreResultHookV.Load(); h != nil {
		resp, err = (*h)(sess, resp, err)
	}
	if err != nil {
		detail.rawErr = err
		// A rejected CAS is CERTAIN: nothing was stored, so sess.seq is still right
		// and the session stays usable. Anything else (a timeout, a lost response)
		// leaves an outcome nobody can name, and a stale sess.seq would otherwise
		// reject the client's own next reconnect. Record the attempted write's
		// identity so a readback can classify it.
		//
		// For an acceptance batch the rejection is classified BEFORE the marker is
		// armed. The marker gates serving on the claim "the subject moved", and a
		// batch rejection does not carry that claim on its own: it names the first
		// failed precondition and nothing else, so the proof has to come from the
		// acceptance fact — absent means the batch never committed and the record CAS
		// is what was refused. See sess_rejected_write.go.
		armMarker := mqttIsRecordCASRejection(err)
		sess.mu.Lock()
		if mqttIsRecordCASRejection(err) {
			if armMarker {
				// Certain about the WRITE, and silent about the other fact the rejection
				// carries: the subject moved. Armed HERE, at the one boundary every record
				// write passes through, so a rejection site nobody enumerated is covered
				// anyway — the enumeration in this feature's plan was itself incomplete
				// twice over. See sess_rejected_write.go.
				sess.markWriteRejectedLocked(seq)
			}
		} else {
			// The framed bytes travel with the identity, because settling this
			// ambiguity means RE-SENDING them: a re-marshal of whatever memory holds
			// later could not carry the digest a readback compares against. See
			// mqttPendingWrite.
			sess.markWriteUncertainLocked(&detail)
		}
		sess.mu.Unlock()
		return detail, fmt.Errorf("unable to persist session %q (seq=%v): %v", ps.ID, seq, err)
	}
	sess.mu.Lock()
	sess.seq = resp.Sequence
	// The lineage is adopted with the sequence, in the same critical section, because a
	// pair that lagged the sequence would parent the NEXT write to a record that is no
	// longer the base — and every sibling of the real base would then be unprovable.
	sess.wid, sess.pwid = wid, pwid
	// And the record BYTES with them: the record just confirmed becomes the base a later
	// fork's delta is measured from, and the one it replaced becomes that base's parent. Two
	// slice assignments — the grant itself is materialised only if a fork ever happens, which
	// is what keeps this off the save path. See sess_fork_repair.go.
	sess.adoptGrantBodyLocked(body)
	// A CONFIRMED write is the durability boundary for everything it marshalled, so the
	// void-register verdicts it carried are credited HERE rather than by whichever caller
	// happened to want them recorded. Crediting only at the paths that set out to persist
	// a verdict left the other half open: a save that CARRIES a nonce without crediting it
	// leaves a settled reservation — a packet identifier and a Receive Maximum slot —
	// parked in memory although the fact is durable, and on an unbound session nothing
	// need ever look again. A reconnect's accept save is the case that proves the rule:
	// it inherits the verdicts and is not a verdict path at all.
	sess.creditVoidedRegsLocked(voidedRev)
	sess.mu.Unlock()
	return detail, nil
}

// adoptCommittedMetadata applies the durability-boundary accounting for a committed ownership
// metadata batch, from the detail alone. It is persistRecord's ownership success arm, factored
// out so a retry that re-sends the SAME frozen attempt through commitMetadataAttempt — the
// ownership executor's bounded 10164 resend (Codex consult 10) — credits identically without
// re-marshalling: the lineage (detail.wid/pwid), the record bytes (detail.body) and the void
// revision (detail.voidedRegsRev) all travelled on the detail, so nothing here reads a fenced
// local persistRecord no longer holds. landedSeq is the record's PROVEN sequence — the ack of
// the batch that just landed, or the record readback that a resolved-COMMITTED verdict located
// by write identity — never an unrelated newer record's.
func (sess *mqttSession) adoptCommittedMetadata(detail mqttSessSaveDetail, landedSeq uint64) {
	sess.mu.Lock()
	sess.seq = landedSeq
	sess.wid, sess.pwid = detail.wid, detail.pwid
	sess.adoptGrantBodyLocked(detail.body)
	sess.creditVoidedRegsLocked(detail.voidedRegsRev)
	sess.mu.Unlock()
}
