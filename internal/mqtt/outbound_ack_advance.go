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
	"encoding/json"
	"fmt"
	"strings"
)

// The acknowledgement-driven advance (F-105 row 3): one CLIENT ACKNOWLEDGEMENT moves the
// exchange's phase AND re-phases the durable in-flight slot, in ONE atomic authority batch.
//
// # Two records, one fact, one batch
//
// The exchange record and the register slot both govern whether this obligation may still
// emit or release its identifier, and an acknowledgement is one fact about both: the client
// answered. Splitting the two writes leaves a durable disagreement — a slot past PUBREC beside
// an exchange still at PUBLISH, or the reverse — that a peer or a restart must interpret, which
// is exactly the read-then-act shape F-105 exists to remove. JetStream batches are per stream,
// and both records live on `$MQTT_inflight`, so the two members commit together or not at all.
//
// # It records evidence; it does not admit work or drain an action
//
// The slot member is an ACK-DRIVEN PHASE ARROW, never a replacement that re-mints an
// allocation and never an action drain. It preserves the occupant's identity, delivery,
// lineage and QoS and moves ONLY its state, so it is recognised apart from a same-delivery
// replacement (mqttIsAckPhaseAdvance) exactly as the row-2 output-custody token is
// (mqttIsActionTokenAcquisition). checkAckPhaseAdvanceMembers binds it to the exchange member
// it rides beside; checkRegisterReplacements skips it so the displacement rules never run.
//
// # Standing authority, not serving
//
// An acknowledgement observes output that was already authorised, so this records no new work
// and needs no current entitlement — and a CLOSED generation's obligation must still reach its
// end under whoever serves now (TestF105ClosedGenerationsObligationStillEnds). That is why the
// op contends on the STANDING authority and why the register-member generation may lag the
// transition's own, exactly as the exchange member's may.

// mqttAdvanceExchangeAckRequest names one acknowledgement's advance of an obligation.
type mqttAdvanceExchangeAckRequest struct {
	ID    string
	Epoch string
	Txn   string
	Op    string

	// Gen is the exchange's OWN generation, which may lag the standing authority's: a G1
	// obligation is acknowledged on G1's connection while G2 serves, and only G2's authority
	// can commit the advance.
	Gen string

	// Nonce is the exchange (its per-OBLIGATION identity, equal to the slot's Lin), and To the
	// phase this acknowledgement establishes — DERIVED from the packet by mqttPhaseForAck,
	// never chosen by the caller.
	Nonce string
	To    mqttExchangePhase

	// Acq is the transmission this acknowledgement answers. A PUBACK or a refused PUBREC names
	// ONE transmission, so the terminal arrows are bound to it: terminalising a slot whose Acq
	// has moved on records a client answer over a retransmission the client never made
	// (holistic review round 3, finding 1). A successful PUBREC is deliberately NOT bound to it.
	Acq uint64

	// Register is the exact in-flight slot occupant this advance re-phases, read at RegisterSeq.
	// The slot member CASes it at RegisterSeq, so every predicate validated here against the
	// read record is still true at commit. It is REQUIRED: a pending-drain ending, whose output
	// is still outstanding, carries the exchange alone through opAdvanceExchange, and a retained
	// send (R6) has no durable slot at all.
	Register    *mqttInflightRecord
	RegisterSeq uint64

	// RelID is the identity a RETAINED slot's canonical PUBREL is created with on its successful
	// PUBREC (F-105 7c), minted by the caller once per decision so a retry of the same advance
	// names the same record. Manifest and ManifestSeq are that slot's copy as read for the
	// Publish-origin arrow's fence; the op loads them when the caller did not.
	RelID       string
	Manifest    *mqttRetainedManifest
	ManifestSeq uint64
}

// mqttAckSlotArrow is the slot state that records the acknowledgement `to` for a slot standing
// at `from`, and whether that pair is an acknowledgement arrow at all.
//
// KEYED ON THE ORIGIN AS WELL AS THE DESTINATION. Terminal ends a PUBLISH (a PUBACK) and a
// release (a PUBCOMP), and the two endings are different slot phases: the PUBLISH branch of
// terminal names no canonical PUBREL, the PUBREL branch does. A destination-only map would send
// a PUBCOMP's slot to publish_terminal and lose the canonical record it still has to dispose.
//
//	publish      + Terminal | Rejected                 -> publish_terminal (PUBACK, refused PUBREC)
//	publish      + PubRecObserved                      -> pubrel_intended  (successful PUBREC)
//	pubrel_ready + Terminal | TerminalPendingDrain     -> pubrel_terminal  (PUBCOMP)
//
// A PUBLISH-branch pending-drain ending does NOT re-phase the slot: a live action token is the
// outstanding output that makes it pending-drain, so it carries the exchange alone. The PUBREL
// branch is different on purpose: pubrel_terminal already IS the gated shape for a live token
// (finalizeTerminalSlot refuses disposal while one stands), so PUBCOMP records completion on the
// slot and the token keeps the cleanup waiting for its holder's drain.
//
// RETAINED SLOTS SKIP pubrel_intended (F-105 7c): a retained exchange's canonical PUBREL is created
// on $MQTT_inflight in the SAME batch as the PUBREC advance, so there is no cross-stream store for
// an intent phase to bridge and the arrow lands on pubrel_ready directly.
func mqttAckSlotArrow(retained bool, from mqttInflightState, to mqttExchangePhase) (mqttInflightState, bool) {
	switch from {
	case mqttInflightPublish, mqttInflightDeliveryAbandoned:
		// An abandoned delivery answers to the same acknowledgements as a live one (unit C2):
		// the abandonment ended the broker's delivery of it, not the client's answer to a copy
		// an earlier connection may have sent.
		switch to {
		case mqttExchangeTerminal, mqttExchangeRejected:
			return mqttInflightPublishTerminal, true
		case mqttExchangePubRecObserved:
			if retained {
				return mqttInflightPubRelReady, true
			}
			return mqttInflightPubRelIntended, true
		}
	case mqttInflightPubRelReady:
		switch to {
		case mqttExchangeTerminal, mqttExchangeTerminalPendingDrain:
			return mqttInflightPubRelTerminal, true
		}
	}
	return "", false
}

// mqttAckExchangeOrigin is the exchange phase a slot arrow from `from` requires the exchange to
// stand at. The generic successor table admits more than one route into Terminal — the drain
// completion is another — so the slot's origin, not the destination, is what binds the pair.
func mqttAckExchangeOrigin(from mqttInflightState) (mqttExchangePhase, bool) {
	switch from {
	case mqttInflightPublish:
		return mqttExchangePublish, true
	case mqttInflightDeliveryAbandoned:
		return mqttExchangeDeliveryAbandoned, true
	case mqttInflightPubRelReady:
		return mqttExchangePubRecObserved, true
	}
	return 0, false
}

// mqttAdvanceKindSlotState names the register states an advance-exchange transition may carry a
// slot member at. The coarse kind gate and the token-installation gate both enumerate them, so
// they have one home; the full shape and pairing validation is checkAckPhaseAdvanceMembers'.
//
// pubrel_ready is an advance's slot state for a RETAINED slot only: its PUBREC arrow lands there.
func mqttAdvanceKindSlotState(rec *mqttInflightRecord) bool {
	switch rec.State {
	case mqttInflightPublishTerminal, mqttInflightPubRelIntended, mqttInflightPubRelTerminal:
		return true
	case mqttInflightPubRelReady:
		return rec.retainedSource()
	}
	return false
}

// mqttAckRephaseState is the state-only form: the register states that are only ever reached by
// an acknowledgement re-phase, whatever the source arm.
func mqttAckRephaseState(st mqttInflightState) bool {
	switch st {
	case mqttInflightPublishTerminal, mqttInflightPubRelIntended, mqttInflightPubRelTerminal:
		return true
	}
	return false
}

// mqttIsAckPhaseAdvance reports whether rec is cur with ONLY its state moved along an
// acknowledgement arrow (mqttAckSlotArrow). Every other field is the occupant's, unchanged —
// including a live action token on a PUBCOMP's pubrel_terminal, which is PRESERVED, not
// installed.
//
// This is the recogniser that keeps an acknowledgement re-phase out of the replacement machine,
// mirroring mqttIsActionTokenAcquisition for the row-2 token. It does not enforce the arrows'
// token and transmission rules — that is checkAckPhaseAdvanceMembers' and the builder's job —
// because a recogniser that also judged legality would be a second definition of it.
func mqttIsAckPhaseAdvance(cur, rec *mqttInflightRecord) bool {
	if cur == nil || rec == nil {
		return false
	}
	recognised := false
	for _, to := range []mqttExchangePhase{mqttExchangeTerminal, mqttExchangeRejected,
		mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain} {
		if st, ok := mqttAckSlotArrow(cur.retainedSource(), cur.State, to); ok && st == rec.State {
			recognised = true
			break
		}
	}
	if !recognised {
		return false
	}
	want := *cur
	want.State = rec.State
	// A KNOWN-FALSE CANCELLATION IS CLEARED by the client's answer (F-088): a `publish` occupant
	// carrying Cancelled whose client acknowledges it was transmitted, so the bit's "never sent"
	// claim is refuted, and keeping it would build the publish_terminal/pubrel_intended shape the
	// validator refuses. The recogniser admits the cleared body; the write boundary separately
	// requires the marker of this allocation to read authoritatively absent before it lets the
	// clearing member land (mqttRefutedCancellation).
	if mqttClearsCancellation(cur, rec) {
		want.Cancelled, want.CancelOrigin = false, _EMPTY_
	}
	// A RETAINED PUBREC arrow installs its in-flight canonical with the phase: that is the whole of
	// what it adds, and the pair must be whole (the shape validator and the rel member check hold
	// the rest).
	if cur.retainedSource() && rec.State == mqttInflightPubRelReady && cur.State != mqttInflightPubRelReady {
		if rec.RelSubj == _EMPTY_ || rec.RelID == _EMPTY_ {
			return false
		}
		want.RelSubj, want.RelID = rec.RelSubj, rec.RelID
	}
	return want == *rec
}

// mqttClearsCancellation reports whether a register member clears a known-false cancellation: the
// occupant is an unretired `publish` record carrying Cancelled and the member does not. It is the
// SHAPE half of the rule, shared by the two recognisers that may carry it (the acknowledgement arrow
// and the output-token acquisition); the evidence half is mqttRefutedCancellation's, at the write
// boundary.
//
// ONLY AN OCCUPANT CANCELLATION IS EVER CLEARED, and both members go together (F-088, consult C4):
// a pending fence is not an exchange's identity carrier, so nothing advances or acquires it, and
// a cleared body carries neither Cancelled nor its provenance.
func mqttClearsCancellation(cur, rec *mqttInflightRecord) bool {
	return cur != nil && rec != nil && cur.State == mqttInflightPublish && cur.Cancelled && !cur.Retired &&
		cur.CancelOrigin == mqttCancelOriginOccupant && !rec.Cancelled && rec.CancelOrigin == _EMPTY_
}

// mqttRefutedCancellation is the ONE rule for clearing a cancellation bit on a register (F-088): a
// member may clear it only where the cancellation is refuted, i.e. the shape is
// mqttClearsCancellation's AND the unsent marker of this allocation reads AUTHORITATIVELY absent,
// read by the write boundary in the same attempt. A standing marker means the cancellation is
// genuine (the PUBLISH was never sent); an unreadable or indeterminate read proves nothing. Either
// refuses.
func mqttRefutedCancellation(cur, rec *mqttInflightRecord, marker mqttMarkerLoad) bool {
	return mqttClearsCancellation(cur, rec) && marker.kind == mqttInflightResponseAbsent
}

// mqttIsDrainTokenClearance reports whether rec is cur — a PUBCOMP'd pubrel_terminal slot with a
// live token — with ONLY its action token and the custody it was bound to cleared. That is the
// holder's own drain, and nothing else: the phase and Acq never move (a drain ends a
// transmission's authority, it does not un-count it).
func mqttIsDrainTokenClearance(cur, rec *mqttInflightRecord) bool {
	if cur == nil || rec == nil || cur.State != mqttInflightPubRelTerminal || cur.ActionToken == _EMPTY_ {
		return false
	}
	want := *cur
	want.ActionToken = _EMPTY_
	want.ActionClaim = _EMPTY_
	want.ActionClaimSeq = 0
	return want == *rec
}

// mqttAdvanceExchangeAckTransition assembles the atomic exchange-advance-and-slot-re-phase.
func mqttAdvanceExchangeAckTransition(prev *mqttOutboundAuthority, prevSeq uint64,
	r *mqttAdvanceExchangeAckRequest, cur *mqttOutboundExchange, curSeq uint64) (*mqttAuthorityTransition, error) {

	if r == nil {
		return nil, fmt.Errorf("an acknowledgement advance requires its request")
	}
	if cur == nil {
		return nil, fmt.Errorf("an acknowledgement advance of exchange %q requires the exchange as it stands", r.Nonce)
	}
	if r.Register == nil {
		return nil, fmt.Errorf("an acknowledgement advance of exchange %q requires the in-flight slot it re-phases: "+
			"a pending-drain ending carries the exchange alone", r.Nonce)
	}
	if r.RegisterSeq == 0 {
		return nil, fmt.Errorf("re-phasing the slot of exchange %q requires the sequence it was read at", r.Nonce)
	}
	reg := r.Register
	slotTo, ok := mqttAckSlotArrow(reg.retainedSource(), reg.State, r.To)
	if !ok {
		return nil, fmt.Errorf("an acknowledgement establishing phase %d does not re-phase a slot standing at %q: "+
			"only a PUBLISH ending, a successful PUBREC and a PUBCOMP do", r.To, string(reg.State))
	}
	// THE EXCHANGE STANDS WHERE THE SLOT'S ORIGIN SAYS. A PUBCOMP answers a release, so its
	// exchange must be past PUBREC; the successor table alone would also admit a pending-drain
	// exchange into Terminal, which is the drain's arrow, not this one.
	if want, _ := mqttAckExchangeOrigin(reg.State); cur.Phase != want {
		return nil, fmt.Errorf("acknowledgement advance of exchange %q re-phases a %q slot while the exchange "+
			"stands at phase %d, not %d", cur.Nonce, string(reg.State), cur.Phase, want)
	}
	// THE SLOT IS THE SAME OBLIGATION. Lin is the exchange's per-obligation identity and equals
	// its nonce; PI is the identifier both name.
	if reg.Lin != cur.Nonce || reg.PI != cur.PI {
		return nil, fmt.Errorf("acknowledgement advance of exchange %q/identifier %d names slot obligation %q/identifier %d",
			cur.Nonce, cur.PI, reg.Lin, reg.PI)
	}
	switch slotTo {
	case mqttInflightPublishTerminal:
		// THE PUBLISH TERMINAL ARROW IS TRANSMISSION-BOUND AND REFUSES A LIVE TOKEN. These are
		// the guards casInflightSlot(mqttSlotAdvanceAcknowledgedTerminal) applies at the store,
		// reconstructed here because the authority batch bypasses that path: the exact-sequence
		// CAS then keeps them true at commit. A PUBREC is deliberately neither (see
		// intendPubRelSlot).
		if reg.Acq != r.Acq {
			return nil, fmt.Errorf("acknowledgement advance of exchange %q answers transmission %d while the slot "+
				"stands at transmission %d: an acknowledgement answers ONE transmission", cur.Nonce, r.Acq, reg.Acq)
		}
		if reg.ActionToken != _EMPTY_ {
			return nil, fmt.Errorf("acknowledgement advance of exchange %q terminalises a slot with a live action "+
				"token: its holder may still be writing the transmission it answers", cur.Nonce)
		}
	case mqttInflightPubRelTerminal:
		// A PUBCOMP ENDS PENDING-DRAIN EXACTLY WHEN A PUBREL ACTION IS LIVE. The token is the
		// only output carrier a PUBREL has at this checkpoint, so it is what decides whether the
		// release boundary may treat the exchange as ended. Not transmission-bound: a PUBCOMP
		// answers the release, and any PUBREL transmission of it is the one it answers.
		if live := reg.ActionToken != _EMPTY_; live != (r.To == mqttExchangeTerminalPendingDrain) {
			return nil, fmt.Errorf("a PUBCOMP of exchange %q records phase %d over a slot whose PUBREL action "+
				"is live=%v: terminal-pending-drain exactly when a token stands", cur.Nonce, r.To, live)
		}
	}

	// The exchange advance validates the exchange and the phase move; the slot member re-phases
	// the occupant beside it, both on $MQTT_inflight, in one batch.
	tr, err := mqttAdvanceExchangeTransition(prev, prevSeq, r.Txn, r.Op, cur, curSeq, r.To)
	if err != nil {
		return nil, err
	}
	next := *reg
	next.State = slotTo
	// THE CLIENT'S ANSWER REFUTES A CANCELLATION (F-088): an acknowledged `publish` register was
	// transmitted, so a Cancelled bit on it is a known-false write and the acknowledged shape never
	// carries it. The write boundary admits the cleared member only on the marker's authoritative
	// absence (mqttRefutedCancellation).
	if reg.CancelOrigin == mqttCancelOriginPendingCreate {
		// A pending fence is never advanced: it is not an exchange's identity carrier (F-088).
		return nil, fmt.Errorf("advancing exchange %q: the register it names is a pending cancellation "+
			"fence, which no acknowledgement advances", cur.Nonce)
	}
	if reg.State == mqttInflightPublish && reg.Cancelled && !reg.Retired {
		next.Cancelled, next.CancelOrigin = false, _EMPTY_
	}
	// THE RETAINED PUBREC ARROW CREATES ITS CANONICAL PUBREL in this batch, and the Publish-origin
	// arrow fences on its copy's manifest (checkRetainedRelMembers, checkRetainedManifestArbitration).
	var retainedExtra []mqttTransitionMember
	if reg.retainedSource() && slotTo == mqttInflightPubRelReady && reg.State != mqttInflightPubRelReady {
		if !mqttF105IdentTk(r.RelID) {
			return nil, fmt.Errorf("a retained PUBREC advance of exchange %q requires the identity its canonical "+
				"PUBREL is created with", cur.Nonce)
		}
		idHash := getHash(prev.ID)
		next.RelSubj = mqttRetainedRelSubject(idHash, cur.Gen, cur.Nonce)
		next.RelID = r.RelID
		relBody, rerr := json.Marshal(&mqttRetainedRel{ID: prev.ID, Epoch: prev.Epoch, Gen: cur.Gen,
			Nonce: cur.Nonce, PI: reg.PI, Identity: r.RelID})
		if rerr != nil {
			return nil, rerr
		}
		retainedExtra = append(retainedExtra, mqttTransitionMember{subj: next.RelSubj, expect: 0, body: relBody})
		if reg.State == mqttInflightPublish {
			if r.Manifest == nil || r.ManifestSeq == 0 {
				return nil, fmt.Errorf("a retained PUBREC advance of exchange %q requires its copy's manifest", cur.Nonce)
			}
			mb, merr := json.Marshal(r.Manifest)
			if merr != nil {
				return nil, merr
			}
			retainedExtra = append(retainedExtra, mqttTransitionMember{
				subj: mqttManifestSubject(idHash, r.Manifest.Gen, r.Manifest.Payload), expect: r.ManifestSeq, body: mb})
		}
	}
	if verr := mqttValidateInflightShape(&next); verr != nil {
		return nil, fmt.Errorf("acknowledgement advance of exchange %q builds an unsupported slot shape: %w", cur.Nonce, verr)
	}
	body, merr := json.Marshal(&next)
	if merr != nil {
		return nil, merr
	}
	tr.Extra = append(tr.Extra, mqttTransitionMember{
		subj:   mqttInflightSubject(getHash(prev.ID), cur.Gen, reg.PI),
		expect: r.RegisterSeq,
		body:   body,
	})
	tr.Extra = append(tr.Extra, retainedExtra...)
	return tr, nil
}

// opAdvanceExchangeAck records one acknowledgement, moving the exchange and re-phasing its slot
// in one atomic batch.
func (jsa *mqttJSA) opAdvanceExchangeAck(r *mqttAdvanceExchangeAckRequest) (mqttOpOutcome, error) {
	if r == nil {
		return mqttOpOutcome{}, fmt.Errorf("an acknowledgement advance requires its request")
	}
	// STANDING AUTHORITY, not serving: an acknowledgement observes work already authorised, and
	// a closed generation's obligation must still reach its end under whoever serves now. Unlike
	// an acquisition, this authorises no new output, so it deliberately does NOT match the
	// current Gen/Tenure.
	auth, authSeq, err := jsa.requireAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no acknowledgement of its "+
			"may be recorded", r.ID)
	}
	cur, curSeq, lerr := jsa.loadOutboundExchange(r.ID, r.Epoch, r.Gen, r.Nonce)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	if reg := r.Register; reg != nil && reg.retainedSource() && reg.State == mqttInflightPublish &&
		r.To == mqttExchangePubRecObserved && r.Manifest == nil {
		m, mSeq, merr := jsa.loadRetainedManifest(r.ID, r.Epoch, r.Gen, reg.Payload)
		if merr != nil {
			return mqttOpOutcome{}, merr
		}
		rr := *r
		rr.Manifest, rr.ManifestSeq = m, mSeq
		r = &rr
	}
	tr, terr := mqttAdvanceExchangeAckTransition(auth, authSeq, r, cur, curSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	if herr := jsa.appendHoldReleases(tr, cur, r.To); herr != nil {
		return mqttOpOutcome{}, herr
	}
	return jsa.commitOp(tr)
}

// mqttAckExchangeClass is what an acknowledgement of one in-flight slot must advance.
type mqttAckExchangeClass uint8

const (
	// mqttAckSlotOnly: an experimental affine slot. No exchange exists for that class (its
	// conversion is chapter P's), so the slot's own arrow is the whole record of the answer.
	mqttAckSlotOnly mqttAckExchangeClass = iota + 1
	// mqttAckExchange: an ordinary obligation standing beside its exchange. The answer is one
	// fact about both records, so both move in ONE atomic batch.
	mqttAckExchange
	// mqttAckRefused: an ordinary slot whose exchange is absent, unreadable or names another
	// obligation. No route is licensed; the caller writes nothing and keeps its continuation.
	mqttAckRefused
)

// ackExchangeFor is THE acknowledgement class gate, the one definition every acknowledgement
// arrow (PUBACK, refused PUBREC, successful PUBREC) consults.
//
// The class is the source durable's prefix (consult 17), decided BEFORE any read: an affine slot
// has no exchange. Every other slot — a shared-group QoS 1 slot included since 7a — is ORDINARY. An
// ORDINARY slot is admitted beside its exchange in one
// opAdmitPublish batch, so an ordinary slot without a readable matching exchange is either a
// transient read failure or a shape this broker does not write — and neither licenses the
// slot-only arrow, which would leave the exchange at PUBLISH for recovery to call delivery owed
// (consult 20, superseding consult 18's "explicit absence means a legacy register").
//
// Session lock NOT held on entry.
func (sess *mqttSession) ackExchangeFor(cur *mqttInflightRecord, gen string) (mqttAckExchangeClass, *mqttOutboundExchange) {
	if cur == nil || sess.asm == nil {
		return mqttAckRefused, nil
	}
	if mqttDurIsSlotOnlyClass(cur.Dur) {
		return mqttAckSlotOnly, nil
	}
	x, _, err := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, gen, cur.Lin)
	if err != nil || x == nil || x.Nonce != cur.Lin || x.PI != cur.PI {
		return mqttAckRefused, nil
	}
	// The pair must state one shared dependency (or none): a contradiction is a shape this broker
	// does not write, and no acknowledgement route is licensed over it.
	if mqttCheckSharedAcqPair(cur, x) != nil {
		return mqttAckRefused, nil
	}
	return mqttAckExchange, x
}

// mqttDurIsSlotOnlyClass is the class half of ackExchangeFor, stated once: the EXPERIMENTAL
// affine source durable, which has no exchange (its conversion and storeInflightRegister's final
// deletion are chapter P's). A shared-group durable is not in it: since 7a a shared QoS 1 delivery
// is admitted through the authority and is ordinary everywhere (outbound_shared_acq.go). Callers
// holding only a slot REFERENCE (the terminal-cleanup dispositions) ask it directly rather than
// restating the prefix; an ordinary slot answers false whatever its exchange read would say, so an
// unreadable exchange never selects a slot-only route.
func mqttDurIsSlotOnlyClass(dur string) bool {
	return strings.HasPrefix(dur, mqttQ2AffineConsumerDurablePrefix)
}

// advanceExchangeAck is the one route both acknowledgement callers take to the atomic advance,
// honoring an instance-scoped test seam (testAckAdvance) exactly as storeInflightCAS honors the
// register store seam. Nil hook: the request goes to opAdvanceExchangeAck unchanged.
//
// Session lock NOT held on entry.
func (sess *mqttSession) advanceExchangeAck(r *mqttAdvanceExchangeAckRequest) (mqttOpOutcome, error) {
	// Whatever the advance's outcome — landed, lost, refused — the exchange MAY have ended, so
	// its custody is observed. The hint never releases; the exchange's own phase does.
	defer sess.hintCustody(r.Gen, r.Nonce)
	// An ending releases its holds in its own batch, which is what a disposed consumer's
	// retirement waits for: a hint, each step re-reads (F-105 7c-lc).
	defer sess.wakeSourceRetirements()
	write := func() (mqttOpOutcome, error) { return sess.jsa.opAdvanceExchangeAck(r) }
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testAckAdvance
		as.mu.RUnlock()
		if hook != nil {
			return hook(r, write)
		}
	}
	return write()
}

// mqttCompleteExchangeDrainRequest names one PUBREL holder's drain of a pending-drain
// completion.
type mqttCompleteExchangeDrainRequest struct {
	ID    string
	Epoch string
	Txn   string
	Op    string
	// Gen is the exchange's OWN generation; like an acknowledgement, the drain completes an
	// admitted obligation under whatever authority stands.
	Gen   string
	Nonce string
	// Token is the holder's own action token: only the exact holder's drain ends the output
	// that made the completion pending.
	Token string
	// Register is the pubrel_terminal slot as read at RegisterSeq, still carrying Token.
	Register    *mqttInflightRecord
	RegisterSeq uint64
}

// mqttCompleteExchangeDrainTransition assembles the atomic token clearance and the
// terminal-pending-drain -> terminal advance.
//
// ONE BATCH, for the reason the acknowledgement advance is one: a clearance that landed alone
// would hit the drain's "token already gone" early return on a lost response and leave the
// exchange reading "drain owed" forever; an advance that landed alone would record Terminal while
// the PUBREL write it waited for may still be on the wire.
func mqttCompleteExchangeDrainTransition(prev *mqttOutboundAuthority, prevSeq uint64,
	r *mqttCompleteExchangeDrainRequest, cur *mqttOutboundExchange, curSeq uint64) (*mqttAuthorityTransition, error) {

	if r == nil || cur == nil || r.Register == nil {
		return nil, fmt.Errorf("a drain completion requires its request, the exchange and the slot it clears")
	}
	if r.RegisterSeq == 0 {
		return nil, fmt.Errorf("clearing the PUBREL token of exchange %q requires the sequence the slot was read at", r.Nonce)
	}
	if cur.Phase != mqttExchangeTerminalPendingDrain {
		return nil, fmt.Errorf("exchange %q stands at phase %d: a drain completion ends only a terminal-pending-drain "+
			"completion", cur.Nonce, cur.Phase)
	}
	reg := r.Register
	if reg.State != mqttInflightPubRelTerminal || r.Token == _EMPTY_ || reg.ActionToken != r.Token {
		return nil, fmt.Errorf("exchange %q's slot is %q holding token %q: a drain completion clears the holder's "+
			"own token %q from a PUBCOMP'd slot", cur.Nonce, string(reg.State), reg.ActionToken, r.Token)
	}
	if reg.Lin != cur.Nonce || reg.PI != cur.PI {
		return nil, fmt.Errorf("drain completion of exchange %q/identifier %d names slot obligation %q/identifier %d",
			cur.Nonce, cur.PI, reg.Lin, reg.PI)
	}
	tr, err := mqttAdvanceExchangeTransition(prev, prevSeq, r.Txn, r.Op, cur, curSeq, mqttExchangeTerminal)
	if err != nil {
		return nil, err
	}
	next := *reg
	// The phase is untouched, and Acq is NEVER reset: a drain ends a transmission's authority,
	// it does not un-count it (C-I).
	next.ActionToken = _EMPTY_
	next.ActionClaim = _EMPTY_
	next.ActionClaimSeq = 0
	if verr := mqttValidateInflightShape(&next); verr != nil {
		return nil, fmt.Errorf("drain completion of exchange %q builds an unsupported slot shape: %w", cur.Nonce, verr)
	}
	body, merr := json.Marshal(&next)
	if merr != nil {
		return nil, merr
	}
	tr.Extra = append(tr.Extra, mqttTransitionMember{
		subj:   mqttInflightSubject(getHash(prev.ID), cur.Gen, reg.PI),
		expect: r.RegisterSeq,
		body:   body,
	})
	return tr, nil
}

// opCompleteExchangeDrain records that the exact action holding a pending-drain exchange's
// output has drained: its token is cleared and terminal-pending-drain -> terminal, atomically.
//
// STANDING authority, like the acknowledgement advance: finishing an admitted obligation needs
// no serving entitlement (outbound_completion.go).
func (jsa *mqttJSA) opCompleteExchangeDrain(r *mqttCompleteExchangeDrainRequest) (mqttOpOutcome, error) {
	if r == nil {
		return mqttOpOutcome{}, fmt.Errorf("a drain completion requires its request")
	}
	auth, authSeq, err := jsa.requireAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no drain of its may be recorded", r.ID)
	}
	cur, curSeq, lerr := jsa.loadOutboundExchange(r.ID, r.Epoch, r.Gen, r.Nonce)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	tr, terr := mqttCompleteExchangeDrainTransition(auth, authSeq, r, cur, curSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	if herr := jsa.appendHoldReleases(tr, cur, mqttExchangeTerminal); herr != nil {
		return mqttOpOutcome{}, herr
	}
	return jsa.commitOp(tr)
}

// completeExchangeDrain is the one route the PUBREL drain takes to the atomic completion,
// honoring an instance-scoped test seam (testDrainComplete) exactly as advanceExchangeAck honors
// testAckAdvance.
//
// Session lock NOT held on entry.
func (sess *mqttSession) completeExchangeDrain(r *mqttCompleteExchangeDrainRequest) (mqttOpOutcome, error) {
	// The drain may have moved the exchange to Terminal: observe it, whatever the outcome.
	defer sess.hintCustody(r.Gen, r.Nonce)
	defer sess.wakeSourceRetirements()
	write := func() (mqttOpOutcome, error) { return sess.jsa.opCompleteExchangeDrain(r) }
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testDrainComplete
		as.mu.RUnlock()
		if hook != nil {
			return hook(r, write)
		}
	}
	return write()
}
