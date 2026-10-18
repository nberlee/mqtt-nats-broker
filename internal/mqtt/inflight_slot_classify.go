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

// Classifying what a fresh allocation meets on its identifier's subject (F-105, row 1).
//
// storeInflightRegister's retry loop interleaves three things: reading the subject, DECIDING
// what the occupant is, and CARRYING OUT the side effects that decision implies — a source
// suppression, a confirmed reap, an abandonment. The conversion needs the middle one
// on its own, because the admitting batch has to know which disposition it is implementing
// before it composes a single member.
//
// So this file is the DECISION and nothing else. It reads no store, takes no locks, mutates
// nothing, and every fact it uses arrives as an argument. What it does not do is equally
// deliberate:
//
//   - it does not perform the side effects. Those belong to the orchestrator, AFTER
//     classification and IN THE ORDER THE LADDER REQUIRES — suppression before abandonment,
//     every cleanup at an exact observed
//     sequence, and a fresh observation after any retry. They are not interchangeable
//     "pre-steps": mqttSlotAdoptRetirement installs the source suppression FIRST precisely so
//     that a re-offer meets the suppression rather than allocating a second identifier, and an
//     orchestrator that ran them in a different order would be a different protocol.
//   - it does not decide the marker. A replacement's MODE is mqttDecideMarker's answer, asked
//     only once the disposition gate has passed, which is why the replace disposition below is
//     an instruction to ask rather than an answer.
//
// The arms are in the ladder's own order, and the order is load-bearing: a pure cancellation
// tombstone is classified BEFORE the same-delivery arms because our allocation is the fresh
// first transmission its freeing licensed, and a terminal publish is classified before the
// retirement arms because a completed exchange is not a retirement to adopt.

// mqttSlotResponse is what an authoritative read of the register subject returned. The three
// kinds are kept apart for the reason every other read in this package keeps them apart: an
// indeterminate read is never an absence.
type mqttSlotResponse uint8

const (
	// mqttSlotSubjectAbsent: a load POSITIVELY returned no message.
	mqttSlotSubjectAbsent mqttSlotResponse = iota + 1
	// mqttSlotSubjectPresent: a message stands, and occupant carries its decoded body.
	mqttSlotSubjectPresent
	// mqttSlotSubjectIndeterminate: the read did not resolve. Never folded into absent.
	mqttSlotSubjectIndeterminate
)

// mqttSlotObservation is everything the classifier is told about the subject.
type mqttSlotObservation struct {
	resp mqttSlotResponse
	// occupant and seq describe a PRESENT subject. occupant is nil when the body is one this
	// broker cannot decode, which is UNREADABLE and is not an absence.
	occupant *mqttInflightRecord
	seq      uint64
	// subjectNamesOccupant is mqttInflightRecordMatchesSubject: whether the body's identifier
	// is the one its subject names. Passed in rather than recomputed, because the classifier
	// is not given the subject string.
	subjectNamesOccupant bool
	// sourceComplete is set only where the caller has established that an occupant naming
	// ANOTHER delivery has a provably complete source. Absent means not established, which
	// is not the same as established-false.
	sourceComplete bool
	// provenUnsent says the same caller established proven-unsent release authority for that
	// occupant. Both are required before a stale foreign register may be reaped.
	provenUnsent bool
	// cancelMarker is the unsent marker of a CANCELLED occupant's own allocation, probed by the
	// caller BEFORE classification (F-088, WP3 edit 7): a cancellation is a decision, not a
	// proof, so only the marker says whether the cancelled occupant was never sent. The zero
	// value is Indeterminate, so a caller that did not probe fails closed.
	cancelMarker mqttSlotMarkerProof
}

// mqttAllocationIntent is the allocation asking for the subject.
type mqttAllocationIntent struct {
	pi    uint16
	dur   string
	sseq  uint64
	nonce string
}

// mqttSlotDisposition is the closed set of answers. A kind with no rule fails closed at the
// default, exactly as the transition kinds do.
type mqttSlotDisposition uint8

const (
	// mqttSlotDispRefuse: withdraw this allocation and touch nothing. The catch-all for
	// every shape whose meaning is not established — an unreadable occupant, a body naming
	// another identifier, a past-PUBREC retirement, an occupant this allocation may not act
	// on. Fails closed, and is the zero value's neighbour deliberately.
	mqttSlotDispRefuse mqttSlotDisposition = iota + 1

	// mqttSlotDispCreate: the subject is free. Create the pair — but a marker with no
	// register may still stand, and it must be reaped CAPTURE-FIRST before the create is
	// retried or its create-only CAS rejects the batch identically forever.
	mqttSlotDispCreate

	// mqttSlotDispReplace: a same-delivery occupant this allocation may rewrite. THE MODE IS
	// NOT DECIDED HERE — the orchestrator asks mqttDecideMarker under
	// mqttMarkerOpReplaceOccupant and gets pair, register-only, or withhold.
	mqttSlotDispReplace

	// mqttSlotDispAdoptLandedWrite: the occupant carries OUR nonce. Our own write landed and
	// its response was lost, which is the whole reason the nonce exists — the sequence cannot
	// say this, because the subject is reused. Settle against what actually stands, never
	// against what we meant to write.
	mqttSlotDispAdoptLandedWrite

	// mqttSlotDispReapCancelledThenRetry: a PURE cancellation tombstone, which records a
	// delivery that was never transmitted, so the identifier it names is free by
	// construction. Reap it confirmed at its exact sequence, then retry the create.
	// Deliberately above the same-delivery arms: when the tombstoned delivery names OUR
	// source, our allocation IS the first transmission its freeing licensed.
	mqttSlotDispReapCancelledThenRetry

	// mqttSlotDispTerminalCleanupWithdraw: a TERMINAL publish for this very source still
	// occupies the subject — its acknowledgement recorded the phase and its delete has not
	// landed. Attempt the identity-bound confirmed cleanup, SCHEDULE a continuation when that
	// cleanup cannot be proven, and withdraw this allocation either way. Replacement is
	// withheld until the delete is proven, because writing over an unresolved terminal record
	// destroys the only durable evidence the exchange completed.
	mqttSlotDispTerminalCleanupWithdraw

	// mqttSlotDispAdoptRetirement: the occupant proves this identifier's exchange for this
	// very source is past PUBREC — the client answered under this packet identifier.
	// Replacing it would erase that proof; abandoning alone would let the next offer allocate
	// a SECOND identifier. So install the source suppression FIRST, then abandon, then clean
	// the matching marker under evidence. The order is the disposition.
	mqttSlotDispAdoptRetirement

	// mqttSlotDispReapCompleteThenRetry: an occupant naming ANOTHER delivery whose source is
	// provably complete AND which can be proven never transmitted. A stale register left by a
	// lost delete. Both proofs are required: completion alone does not authorize the reap.
	mqttSlotDispReapCompleteThenRetry

	// mqttSlotDispAbandon: an occupant naming another delivery that is live, or whose
	// completion is not established. FAIL CLOSED — this identifier is not ours to use, and
	// the record is deliberately NOT reaped: on another broker it may be a LIVE delivery's
	// identity, and destroying it would strand the identifier the client is holding.
	mqttSlotDispAbandon

	// mqttSlotDispAbandonedHeld: the occupant is an ABANDONED delivery's register — the durable
	// disposal record of a delivery the broker gave up under its authorization policy. Withdraw
	// this allocation and touch nothing. Placed FIRST among the occupant arms: no nonce match, no
	// same-delivery replacement and no cancellation cleanup may act on it, because the register is
	// kept until its generation is discarded and the identifier it names is held until then.
	// Unreachable by allocation (the custody ledger holds the identifier), so it is a guard.
	mqttSlotDispAbandonedHeld

	// mqttSlotDispReapPendingFenceThenRetry: the occupant is a PENDING CANCELLATION FENCE of
	// another allocation, positively identified by its written provenance (F-088, consult C4).
	// Dispose of exactly that fence through mqttSlotDeletePendingFence — confirmed, at the
	// observed sequence, preserving any successor — then retry the create. The fence ends no
	// exchange and frees no independently held reservation.
	mqttSlotDispReapPendingFenceThenRetry
)

// mqttSlotVerdict is the classification, with the evidence the orchestrator needs to carry it
// out at an exact sequence.
type mqttSlotVerdict struct {
	disposition mqttSlotDisposition
	// seq is the occupant's exact observed sequence, and is the ONLY sequence any cleanup or
	// replacement this verdict licenses may address.
	seq uint64
	// occupant is the record the verdict was reached against, so the orchestrator acts on the
	// body that was classified rather than re-reading and acting on a different one.
	occupant *mqttInflightRecord
	// why is a short reason for the log line, in the vocabulary of the ladder it replaces.
	why string
}

// mqttClassifyInflightSlot decides what a fresh allocation meets on its subject.
//
// Pure: no store, no locks, no mutation. Every arm is the ladder's arm in the ladder's order,
// and the ordering comments on the constants above say why each precedes the next.
func mqttClassifyInflightSlot(obs mqttSlotObservation, want mqttAllocationIntent) mqttSlotVerdict {
	switch obs.resp {
	case mqttSlotSubjectAbsent:
		return mqttSlotVerdict{disposition: mqttSlotDispCreate, why: "the subject is free"}
	case mqttSlotSubjectIndeterminate:
		// NOT an absence and not an occupant. Nothing is established, so nothing is done —
		// the caller retries the read rather than acting on a non-answer.
		return mqttSlotVerdict{disposition: mqttSlotDispRefuse, why: "the subject did not resolve"}
	}

	cur, seq := obs.occupant, obs.seq
	v := func(d mqttSlotDisposition, why string) mqttSlotVerdict {
		return mqttSlotVerdict{disposition: d, seq: seq, occupant: cur, why: why}
	}
	if cur == nil {
		// UNREADABLE. Recovery classifies this shape as unreadable and RESERVES the
		// identifier; this path must not be more permissive, because every arm below either
		// replaces or deletes on the strength of the body it just read.
		return v(mqttSlotDispRefuse, "the occupant is a record this broker cannot read")
	}
	if !obs.subjectNamesOccupant {
		// The body names a DIFFERENT packet identifier than its subject. Erasing it and then
		// transmitting under this identifier is how a corrupted record becomes a reused
		// identifier the client may still hold.
		return v(mqttSlotDispRefuse, "the occupant's body names another packet identifier")
	}

	if cur.State == mqttInflightDeliveryAbandoned {
		return v(mqttSlotDispAbandonedHeld, "the occupant is an abandoned delivery's disposal record")
	}

	// OUR OWN NONCE, before anything else looks at the body's contents: it is the only fact
	// that identifies this ALLOCATION, and the subject is reused so the sequence cannot.
	// NO CLAIM COMPARISON (F-105 7b): the register's claim stamp decides nothing about an
	// ordinary allocation. Whether this connection may still act for the session is its accepted
	// entitlement's question, compared inside every owner-dependent authority batch that follows.
	if cur.Nonce == want.nonce {
		return v(mqttSlotDispAdoptLandedWrite, "our own write, whose response was lost")
	}

	// A PURE cancellation tombstone, ABOVE the same-delivery arms, decided by its MARKER (F-088).
	// Retired is excluded by the precedence rule: a record carrying both was transmitted.
	//   - marker STANDING for its allocation: a genuine never-transmitted cancellation; reaped
	//     through the gated door, which withdraws its obligation on that same marker;
	//   - marker AUTHORITATIVELY ABSENT: the cancellation is refuted — the occupant may be on the
	//     wire — so it is classified like any possibly-attempted occupant by the arms below (same
	//     delivery: the lineage-preserving register-only replacement; another delivery: reserve);
	//   - INDETERMINATE (or not probed): nothing is established; abandon with the identifier
	//     excluded, and the source re-offers later.
	// A PENDING CANCELLATION FENCE, identified by its written provenance (F-088, consult C4 item
	// 3): a create-only tombstone another allocation wrote on an empty subject before any
	// admission landed. It is no exchange's identity carrier and has no marker, so it is decided
	// by provenance alone, never by the cancelled-occupant probe below: reap exactly it through
	// the dedicated disposal rule, then retry the create.
	if mqttPendingFenceBodyOK(cur) {
		return v(mqttSlotDispReapPendingFenceThenRetry, "a pending cancellation fence")
	}
	if cur.Cancelled && !cur.Retired {
		switch obs.cancelMarker {
		case mqttSlotMarkerPresent:
			return v(mqttSlotDispReapCancelledThenRetry, "a pure cancellation tombstone whose marker stands")
		case mqttSlotMarkerAbsent:
			// Refuted: fall through to the delivery arms.
		default:
			return v(mqttSlotDispAbandon, "a cancelled occupant whose marker could not be resolved")
		}
	}

	sameDelivery := cur.Dur == want.dur && cur.Seq == want.sseq
	if sameDelivery {
		switch {
		case cur.State == mqttInflightPublishTerminal:
			// Above the retirement arms and tested INDEPENDENTLY of Retired: a terminal
			// record is not a PUBLISH-side retirement to adopt, and not a live occupant to
			// replace.
			return v(mqttSlotDispTerminalCleanupWithdraw, "a terminal publish for this source")
		case cur.Retired && mqttInflightPastPubRec(cur.State):
			// The slot is already the authoritative owner of a PUBREL exchange, so neither
			// rewriting it nor installing a PUBLISH-side retirement is legal.
			return v(mqttSlotDispRefuse, "a past-PUBREC slot carrying a retirement")
		case cur.Retired:
			return v(mqttSlotDispAdoptRetirement, "a retirement recorded for this source")
		default:
			// The replacement is opReplacePublish, which compares this connection's accepted
			// entitlement: a displaced connection's replacement cannot land (F-105 7b).
			return v(mqttSlotDispReplace, "a same-delivery occupant this session may rewrite")
		}
	}

	// ANOTHER DELIVERY. Two proofs, and completion alone is not enough: the reap needs
	// proven-unsent release authority for that exact occupant as well.
	if obs.sourceComplete && obs.provenUnsent {
		return v(mqttSlotDispReapCompleteThenRetry, "a stale register whose source is complete")
	}
	return v(mqttSlotDispAbandon, "an occupant naming a live or unknown delivery")
}

// ---------------------------------------------------------------------------
// The equivalence harness, and why it is gone
// ---------------------------------------------------------------------------
//
// While storeInflightRegister still carried its own predicates, an audit hook reported from
// every arm which arm the ladder TOOK beside which disposition this classifier PREDICTED, and a
// test drove the real store through planted occupants and failed on any disagreement. That is
// what earned the replacement: a unit table proves this file self-consistent, never faithful.
//
// The predicates are now gone — the ladder dispatches on mqttClassifyInflightSlot directly — so
// the audit would compare the verdict against itself. A test that cannot fail is worse than no
// test, because it reads like coverage. It is deleted with the hook, and what replaces it is
// the corpus: twelve test files and fifty-eight call sites drive this dispatch, and
// TestF105SlotClassifierDrivesTheLiveLadder asserts the observable outcome of each shape.
