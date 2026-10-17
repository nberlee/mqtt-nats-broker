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
	"time"
)

// A closure certificate that outlives its process (F-105 8c).
//
// A connection's certificate (transport_binding.go) is the positive statement that the
// connection can emit nothing more, and only the process holding the socket can make it. That
// process may die before the holder of an action acquired on the connection clears its register
// token — a drain still owed, or a crash between the certificate and the drain — and the token
// then stands forever: no successor may acquire on the identifier, and a timeout or a vanished
// peer is not closure, so nothing else may clear it.
//
// So the certificate is PERSISTED, and a persisted certificate is the one licence besides the
// holder's own that ends live output custody. It does not say the frame was unsent and it does
// not say the exchange finished: it ends the token, and every protocol duty the exchange still
// owes keeps going exactly as it would have after the holder's own drain.
//
// ABSENCE IS NEVER A LICENCE. A certificate that was never written, whose write failed, that
// vanished, or that this broker cannot read leaves the token standing, which is the pre-8c leak
// and the documented residual — never a clearance on no evidence.

// mqttCloseCertClassTk is the durable closure certificate's record class.
//
// Its own class, neither an arbitration record nor an obligation: it is a fact about a socket,
// written by the process that owned it, and it arbitrates nothing — two writers can only write
// the same fact. It is never a member of an authority transition.
const mqttCloseCertClassTk = "cert"

// mqttCloseCertSubject names one connection's certificate under the client generation the
// connection served. One connection acquires under exactly one generation (its entitlement is
// set once and refused on replacement), so the generation scopes it without ambiguity.
func mqttCloseCertSubject(idHash, gen, run, conn string) string {
	return mqttF105SubjectPrefix() + mqttCloseCertClassTk + "." + idHash + "." + gen + "." + run + "." + conn
}

// mqttCloseCertWalkFilter enumerates one generation's certificates, and only those.
func mqttCloseCertWalkFilter(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttCloseCertClassTk + "." + idHash + "." + gen + ".*.*"
}

// mqttDurableCloseCert is the persisted certificate: which connection, of which broker run,
// serving which client generation, can emit nothing more.
//
// No reason member. The in-process certificate's reason is diagnosis only, and a durable record
// carrying it would invite a reader to treat it as part of the fact.
type mqttDurableCloseCert struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	Run   string `json:"run"`
	Conn  string `json:"conn"`
}

var mqttDurableCloseCertMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "run": {}, "conn": {},
}

// mqttValidateDurableCloseCert refuses a certificate that does not name exactly the connection
// and generation its reader asked about.
func mqttValidateDurableCloseCert(c *mqttDurableCloseCert, wantID, wantEpoch, wantGen, wantRun, wantConn string) error {
	if c == nil {
		return fmt.Errorf("closure certificate is absent")
	}
	if c.ID != wantID || c.Epoch != wantEpoch || c.Gen != wantGen || c.Run != wantRun || c.Conn != wantConn {
		return fmt.Errorf("closure certificate names %q/%q/%q connection %q/%q, not %q/%q/%q connection %q/%q",
			c.ID, c.Epoch, c.Gen, c.Run, c.Conn, wantID, wantEpoch, wantGen, wantRun, wantConn)
	}
	if !mqttF105IdentTk(c.Gen) || !mqttF105IdentTk(c.Run) || !mqttF105IdentTk(c.Conn) {
		return fmt.Errorf("closure certificate carries a token that cannot name a subject")
	}
	return nil
}

// loadDurableCloseCert reads one connection's certificate: nil with no error ONLY when the store
// positively answered that none stands. An unreadable or foreign body is an ERROR, never an
// absence — and neither is a licence, which is the caller's whole obligation.
func (jsa *mqttJSA) loadDurableCloseCert(id, epoch, gen, run, conn string) (*mqttDurableCloseCert, error) {
	if !mqttF105IdentTk(gen) || !mqttF105IdentTk(run) || !mqttF105IdentTk(conn) {
		return nil, fmt.Errorf("a closure certificate is named by generation, run and connection tokens, "+
			"and %q/%q/%q cannot all name one", gen, run, conn)
	}
	subj := mqttCloseCertSubject(getHash(id), gen, run, conn)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, fmt.Errorf("reading the closure certificate on %q: %w", subj, err)
		}
		return nil, nil
	}
	if smsg == nil {
		return nil, nil
	}
	if err := mqttJSONMembersAreExactAndUnique(smsg.Data, mqttDurableCloseCertMembers,
		"a closure certificate"); err != nil {
		return nil, fmt.Errorf("the closure certificate on %q is unsupported: %w", subj, err)
	}
	if err := mqttJSONMembersArePresent(smsg.Data, mqttDurableCloseCertMembers,
		"a closure certificate"); err != nil {
		return nil, fmt.Errorf("the closure certificate on %q is unsupported: %w", subj, err)
	}
	c := &mqttDurableCloseCert{}
	if err := json.Unmarshal(smsg.Data, c); err != nil {
		return nil, fmt.Errorf("the closure certificate on %q is unsupported: %w", subj, err)
	}
	if err := mqttValidateDurableCloseCert(c, id, epoch, gen, run, conn); err != nil {
		return nil, fmt.Errorf("the closure certificate on %q is unsupported: %w", subj, err)
	}
	return c, nil
}

// publishDurableCloseCert writes one certificate, create-only, ONCE.
//
// Not a loop. A failed or ambiguous write leaves no durable certificate this broker can rely
// on, and that is the residual, not a stall: the in-process certificate stays valid, and a token
// that needed the durable one stands until its generation is discarded. An answer that the
// subject is taken is settled by reading what stands: the identical certificate is success —
// two certifiers of one connection write one fact — and anything else is an error.
func (jsa *mqttJSA) publishDurableCloseCert(c *mqttDurableCloseCert) error {
	if verr := mqttValidateDurableCloseCert(c, c.ID, c.Epoch, c.Gen, c.Run, c.Conn); verr != nil {
		return fmt.Errorf("refusing to publish an unsupported closure certificate: %w", verr)
	}
	body, err := json.Marshal(c)
	if err != nil {
		return err
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, serr := jsa.storeMsg(mqttCloseCertSubject(getHash(c.ID), c.Gen, c.Run, c.Conn), hdr, msg); serr != nil {
		stood, lerr := jsa.loadDurableCloseCert(c.ID, c.Epoch, c.Gen, c.Run, c.Conn)
		if lerr != nil {
			return fmt.Errorf("persisting the closure certificate of connection %s/%s was unresolved (%v) "+
				"and what stands could not be read: %w", c.Run, c.Conn, serr, lerr)
		}
		if stood == nil {
			return fmt.Errorf("persisting the closure certificate of connection %s/%s is unresolved: %w",
				c.Run, c.Conn, serr)
		}
	}
	return nil
}

// mqttCertPersistCtx is where a binding's certificate is persisted: the client generation the
// connection acquired under, captured when it FIRST attempted an acquisition and never derived
// later from a session object a reconnect may have rebound.
type mqttCertPersistCtx struct {
	jsa   *mqttJSA
	id    string
	epoch string
	gen   string
}

// mqttPersistCloseCert persists one binding's certificate in its own goroutine.
//
// Off the caller's path by construction: certification runs at teardown, on a refused CONNECT
// and on a torn writer's delivery goroutine — the last under the session's delivery lock — and
// none of them may wait a store round trip, or take a lock, to state a fact the process already
// established. The goroutine holds no lock and uses only the captured context.
func mqttPersistCloseCert(ctx *mqttCertPersistCtx, run, conn string) {
	if ctx == nil || ctx.jsa == nil {
		return
	}
	cert := &mqttDurableCloseCert{ID: ctx.id, Epoch: ctx.epoch, Gen: ctx.gen, Run: run, Conn: conn}
	go func() {
		if h := mqttTestCloseCertPersistV.Load(); h != nil && !(*h)(cert) {
			return
		}
		if err := ctx.jsa.publishDurableCloseCert(cert); err != nil {
			mqttWarnf(ctx.jsa.c, "MQTT: the closure certificate of connection %s/%s could not be persisted, so a "+
				"token its actions leave standing is held until the generation is discarded: %v", run, conn, err)
		}
	}()
}

// newCertPersistCtx captures where this acquisition's binding persists its certificate. The
// assistant is taken WITHOUT a deadline: the certificate is written after whatever bounded
// operation registered it has ended.
func (jsa *mqttJSA) newCertPersistCtx(id, epoch, gen string) *mqttCertPersistCtx {
	return &mqttCertPersistCtx{jsa: jsa.withDeadline(time.Time{}), id: id, epoch: epoch, gen: gen}
}

// ---------------------------------------------------------------------------
// The clearance: a certified connection's token ends without its holder
// ---------------------------------------------------------------------------

// mqttActionKindSurvivesInto reports whether a live token of an action of this kind can stand on
// a register in this state.
//
// Derived from the register steps that preserve a live token: a PUBLISH token is installed in
// `publish` (mqttActionKindRegisterState) and survives a successful PUBREC, which re-phases the
// slot to `pubrel_intended` (ordinary) or straight to `pubrel_ready` (retained) beside the token;
// an acknowledgement can terminalise neither while it stands. A PUBREL token is installed into
// `pubrel_ready` and survives the PUBCOMP into `pubrel_terminal`, which is pending-drain exactly
// while it stands. Any other pairing is a register this broker does not write.
func mqttActionKindSurvivesInto(kind mqttActionKind, st mqttInflightState) bool {
	switch kind {
	case mqttActionPublish:
		return st == mqttInflightPublish || st == mqttInflightPubRelIntended || st == mqttInflightPubRelReady
	case mqttActionPubRel:
		return st == mqttInflightPubRelReady || st == mqttInflightPubRelTerminal
	}
	return false
}

// mqttCertifiedClearanceOf is the one body a certified clearance writes: the register as read,
// with its live custody ended and NOTHING else changed — not the phase, not the transmission
// counter, not the unsent marker, not the custody stamp.
func mqttCertifiedClearanceOf(pre *mqttInflightRecord) mqttInflightRecord {
	next := *pre
	next.ActionToken = _EMPTY_
	next.ActionClaim = _EMPTY_
	next.ActionClaimSeq = 0
	return next
}

// mqttCertifiedClearOutcome is what one clearance attempt established.
type mqttCertifiedClearOutcome uint8

const (
	// mqttCertifiedClearWithheld: no licence — no usable certificate, an action that does not
	// match, a read that could not answer. The token stands and the register routes as before.
	mqttCertifiedClearWithheld mqttCertifiedClearOutcome = iota
	// mqttCertifiedClearLanded: the token is cleared, at the returned sequence.
	mqttCertifiedClearLanded
	// mqttCertifiedClearMoved: the register moved under the attempt; re-read and re-validate.
	mqttCertifiedClearMoved
)

// clearCertifiedActionToken ends one register's live token on the strength of a PERSISTED
// certificate for the exact connection that acquired it.
//
// THE LICENCE, and every part of it is checked against durable state, never inferred:
//
//  1. the register is ordinary and carries a token (a slot-only register has no action record);
//  2. the action record at the register's own (lineage, transmission counter) decodes as THIS
//     client, epoch and generation, carries the register's identifier, and is of a kind whose
//     token can stand in the register's state;
//  3. a durable certificate names exactly that action's connection (run and connection), under
//     this generation;
//  4. the write lands at EXACTLY the sequence the register was read at. Never a rebase: a
//     register that moved may carry a successor's acquisition, so the caller re-reads and the
//     whole licence is proven again.
//
// In `pubrel_terminal` the token is cleared in the same batch that advances its exchange from
// terminal-pending-drain to terminal — the token is the only thing keeping it pending, so the two
// are one fact. Elsewhere the register alone is rewritten.
//
// Returns the outcome and, when landed, the sequence the cleared register stands at.
func (sess *mqttSession) clearCertifiedActionToken(c *client, subject string, seq uint64,
	pre *mqttInflightRecord) (mqttCertifiedClearOutcome, uint64) {

	as := sess.asm
	if as == nil || sess.jsa == nil || pre == nil || pre.ActionToken == _EMPTY_ || mqttDurIsSlotOnlyClass(pre.Dur) ||
		pre.CancelOrigin == mqttCancelOriginPendingCreate {
		// (A pending fence carries no token, so the shape matrix already keeps one out of here;
		// the provenance is named anyway so this sink states the generic refusal itself, F-088.)
		return mqttCertifiedClearWithheld, 0
	}
	sess.mu.Lock()
	id, idHash, gen := sess.id, sess.idHash, sess.gen
	sess.mu.Unlock()
	epoch := as.sessStreamEpoch
	if !mqttF105IdentTk(pre.Lin) {
		return mqttCertifiedClearWithheld, 0
	}
	amsg, aerr := sess.jsa.loadLastMsgFor(mqttInflightStreamName, mqttActionSubject(idHash, gen, pre.Lin, pre.Acq))
	if aerr != nil || amsg == nil {
		return mqttCertifiedClearWithheld, 0
	}
	act, derr := mqttDecodeOutboundAction(amsg.Data, id, epoch, gen, pre.Lin, pre.Acq)
	if derr != nil || act.PI != pre.PI || !mqttActionKindSurvivesInto(act.Kind, pre.State) {
		return mqttCertifiedClearWithheld, 0
	}
	cert, cerr := sess.jsa.loadDurableCloseCert(id, epoch, gen, act.Run, act.Conn)
	if cerr != nil || cert == nil {
		return mqttCertifiedClearWithheld, 0
	}
	as.mu.RLock()
	hook := as.testCertifiedClearSubmit
	as.mu.RUnlock()
	if hook != nil {
		hook(subject, seq, pre)
	}
	next := mqttCertifiedClearanceOf(pre)
	var werr error
	var landed uint64
	if pre.State == mqttInflightPubRelTerminal {
		_, werr = sess.completeExchangeDrain(&mqttCompleteExchangeDrainRequest{
			ID: id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-certified-drain",
			Gen: gen, Nonce: pre.Lin, Token: pre.ActionToken, Register: pre, RegisterSeq: seq,
		})
	} else {
		if verr := mqttValidateInflightShape(&next); verr != nil {
			return mqttCertifiedClearWithheld, 0
		}
		landed, werr = as.storeInflightCAS(subject, seq, &next)
	}
	// RE-READ ON EVERY OUTCOME. The terminal batch reports an authority sequence, not the
	// register's; and a write whose answer was lost may have landed. The register reading as
	// EXACTLY the clearance's body is that landing, whoever wrote it — the holder's own drain
	// writes the identical body — so adopting its sequence names the state either would reach.
	cur, smsg, lerr := as.loadInflightRecord(subject)
	if lerr != nil || smsg == nil || cur == nil {
		if werr == nil && landed != 0 {
			return mqttCertifiedClearLanded, landed
		}
		return mqttCertifiedClearWithheld, 0
	}
	if *cur == next {
		return mqttCertifiedClearLanded, smsg.Sequence
	}
	if werr != nil || smsg.Sequence != seq {
		mqttWarnf(c, "MQTT: the certified clearance of %q at sequence %d did not land (%v); re-reading", subject, seq, werr)
		return mqttCertifiedClearMoved, 0
	}
	return mqttCertifiedClearWithheld, 0
}

// clearCertifiedRegister runs the clearance over one register until it lands, is withheld, or
// the bounded attempts run out — re-reading and re-proving the WHOLE licence after the register
// moved, never retrying a stale write. Reports whether the token is now cleared, with the
// sequence the register was read at and the one it landed at.
func (sess *mqttSession) clearCertifiedRegister(c *client, subject string) (bool, *mqttInflightRecord, uint64, uint64) {
	as := sess.asm
	if as == nil {
		return false, nil, 0, 0
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil || smsg == nil || cur == nil || !mqttInflightRecordMatchesSubject(cur, subject) ||
			mqttValidateInflightShape(cur) != nil || cur.ActionToken == _EMPTY_ || mqttDurIsSlotOnlyClass(cur.Dur) {
			return false, nil, 0, 0
		}
		out, landed := sess.clearCertifiedActionToken(c, subject, smsg.Sequence, cur)
		switch out {
		case mqttCertifiedClearLanded:
			return true, cur, smsg.Sequence, landed
		case mqttCertifiedClearWithheld:
			return false, nil, 0, 0
		}
	}
	return false, nil, 0, 0
}

// advanceResidentAfterClearance moves a resident entry that still references the cleared
// register's pre-clearance sequence onto the sequence the clearance landed at, exactly as the
// holder's own drain write-back does.
//
// CONDITIONED ON THE EXACT PRE-CLEARANCE SEQUENCE and on the allocation's nonce. An entry at any
// other sequence was moved by a writer this clearance never saw, and its fact stands; re-aiming
// it would point its exact-sequence cleanup at a state it did not reach.
//
// Session lock held on entry.
func (sess *mqttSession) advanceResidentAfterClearanceLocked(pre *mqttInflightRecord, preSeq, landed uint64) {
	for _, m := range []map[uint16]*mqttPending{sess.pendingPublish, sess.pendingPubRel} {
		p := m[pre.PI]
		if p == nil {
			continue
		}
		if p.regNonce == pre.Nonce && p.regSeq == preSeq {
			p.regSeq = landed
		}
		if p.slot.nonce == pre.Nonce && p.slot.seq == preSeq {
			p.slot.seq = landed
			if p.slot.actionToken == pre.ActionToken {
				p.slot.actionLive = false
				p.slot.actionToken = _EMPTY_
			}
		}
	}
}

// clearCertifiedTokensForResume is the resume's clearance PRE-PASS: before the register walk
// classifies anything, every ordinary register whose live token a persisted certificate
// licenses ending is cleared, so the walk then runs, unchanged, over authoritative state.
//
// Before the walk rather than inside it, because a clearance moves a register's sequence: a walk
// that reloaded mid-pass would have to re-establish, for one register, every reading the pass
// built around it — retained read-failure semantics among them — and would spend the pass's
// bounded reclassification on progress rather than contention.
//
// GATED on the generation having a certificate at all, from a COMPLETE listing. No certificate,
// or a listing that could not be completed, licenses nothing and the pass does nothing: the
// tokens stand and route exactly as they did before 8c.
func (sess *mqttSession) clearCertifiedTokensForResume(c *client) {
	as := sess.asm
	if as == nil || sess.jsa == nil {
		return
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	if idHash == _EMPTY_ || !mqttF105IdentTk(gen) {
		return
	}
	certs, whole, err := sess.jsa.streamSubjectsComplete(mqttInflightStreamName, mqttCloseCertWalkFilter(idHash, gen))
	if err != nil || !whole || len(certs) == 0 {
		return
	}
	subjects, eerr := as.enumerateInflightSubjects(mqttInflightWalkFilter(idHash, gen))
	if eerr != nil {
		return
	}
	for subject := range subjects {
		ok, pre, preSeq, landed := sess.clearCertifiedRegister(c, subject)
		if !ok {
			continue
		}
		mqttWarnf(c, "MQTT: the live output token on %q was cleared on the persisted closure certificate "+
			"of the connection that acquired it", subject)
		sess.mu.Lock()
		sess.advanceResidentAfterClearanceLocked(pre, preSeq, landed)
		sess.mu.Unlock()
	}
}

// clearCertifiedBeforeAcquire is an acquisition's chance to end a token its holder's certified
// connection left standing: one clearance, and the resident write-back. Reports whether the token
// was cleared; the caller re-reads the register and re-proves everything it acts on.
//
// Session lock NOT held on entry.
func (sess *mqttSession) clearCertifiedBeforeAcquire(c *client, subject string) bool {
	ok, pre, preSeq, landed := sess.clearCertifiedRegister(c, subject)
	if !ok {
		return false
	}
	sess.mu.Lock()
	sess.advanceResidentAfterClearanceLocked(pre, preSeq, landed)
	sess.mu.Unlock()
	return true
}
