// The durable QoS 2 owner slot: the design in which one PI-scoped `$MQTT_inflight` record
// owns one outbound QoS 2 exchange for its WHOLE life, not only its PUBLISH phase.
//
// This file is the state machine's shared substrate: the state set, the one legal-shape
// matrix, the slot reference, the dispositions and the phase gate. PUBREL admission and
// terminal release live in pubrel_action.go; every production write and read is checked
// against the matrix here.
//
// Before this, a register's life ended at PUBREC: the handoff deleted it and the
// correlated PUBREL record on `$MQTT_out` became the identifier's authority (R2/R5).
// That left the identifier's owner described by TWO independently changing durable
// sets — the registers and the PUBREL records — read by two non-transactional scans.
// Nothing could arbitrate between them, and F-092's whole population is a stale record
// meeting an identifier a later exchange now owns. Five plan revisions tried to
// arbitrate it and each was refuted; the answer is that there is only ever ONE
// authoritative claimant, and it is this record.
//
// # Why a re-read is not a fence
//
// Reading the slot immediately before acting is not enough, and this is the finding the
// whole mechanism exists to answer. Broker A reads the slot, pauses before transmitting;
// broker B performs the peer terminal PUBCOMP release; a later exchange installs a new
// slot under that identifier; broker A resumes and transmits the stale PUBREL.
// `sendsInFlight` is process-local and fences nothing across brokers, so moving the read
// closer to the write only narrows the window.
//
// A successful CAS is different in kind: it establishes an invariant that REMAINS TRUE
// THROUGH the action instead of reporting what was true before it. In the design that is
// what the two active states are for — while an action is live, custody transfer, slot
// replacement, ordinary deletion, identifier release and terminal deletion all refuse, and
// only the action's own holder can drain it.
//
// The CAS and action lifecycle that enforce this live in pubrel_action.go. Keeping the
// phase vocabulary and gate here gives every delete, rewrite and release one answer to
// the same state question.
//
// # Why there is no TTL, no expiry and no steal
//
// A lease would not make this correct. A broker can pause AFTER its own expiry check,
// let the lease lapse, and resume into the same stale write; and an MQTT client cannot
// validate a fencing token, so the external effect is not fenceable downstream. An
// abandoned action therefore holds its identifier until its own holder drains it, the
// generation is discarded, or an operator intervenes — a real availability cost, stated
// as such in docs/CONFORMANCE.md rather than disguised as bounded failover.

package mqtt

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
)

// mqttInflightState is the phase of one outbound QoS 2 exchange, and for QoS 1 the only
// phase there is.
//
// It is MANDATORY and it is never defaulted. An unreadable or empty state is an
// unsupported persisted shape and is refused, because reading it as PUBLISH would
// license a PUBLISH under an identifier that is already past PUBREC — exactly the
// duplicate delivery the compatibility boundary refuses to migrate its way around.
//
// The closed schema belongs to mqttDecodeInflightRecord, which refuses every unrecognised
// field as well as every state/field combination this broker does not write.
type mqttInflightState string

const (
	// mqttInflightPublish: the exchange is awaiting PUBACK or PUBREC. The PUBLISH paths
	// own it, unchanged from before the slot existed.
	mqttInflightPublish mqttInflightState = "publish"
	// mqttInflightPublishTerminal: the PUBLISH phase ENDED without a PUBREL exchange —
	// PUBACK for QoS 1, or a FAILED PUBREC for QoS 2. It is a durable terminal phase this
	// broker wrote, and it exists so that terminal authority is a FACT ON THE RECORD
	// rather than an inference from a missing PUBREL.
	//
	// It carries no canonical tuple: a failed PUBREC never creates one. Its deletion is
	// therefore its own disposition rather than the PUBREL branch's, whose duplicate scan
	// and affine disposal presuppose a canonical record.
	mqttInflightPublishTerminal mqttInflightState = "publish_terminal"
	// mqttInflightPubRelReady: past PUBREC. The exchange's PUBREL record exists and the
	// slot names it canonically. This is the state a PUBREL action is acquired FROM and
	// drained BACK TO.
	mqttInflightPubRelReady mqttInflightState = "pubrel_ready"
	// mqttInflightPubRelIntended: past PUBREC, and the slot says so BEFORE the canonical
	// record exists. It is a MONOTONE PHASE, never a holder-exclusive action: a crash
	// between this CAS and the store would permanently pin a holder-exclusive intent with
	// no other broker able to advance it, which is the correction consult C1 made to the
	// original candidate.
	//
	// A slot here is OWED a canonical record, and materialization — which needs custody
	// but no action — is what supplies one. See the owed-materialization work kind in
	// pubrel_action.go.
	mqttInflightPubRelIntended mqttInflightState = "pubrel_intended"
	// mqttInflightPubRelTerminal: PUBCOMP was observed. The exchange is over; what remains
	// is idempotent obligation completion, the complete same-nonce duplicate scan, the
	// canonical record's acknowledgement, and the exact deletion. Nothing transmits from
	// here, which is what makes terminal finalization custody-agnostic.
	mqttInflightPubRelTerminal mqttInflightState = "pubrel_terminal"
)

// mqttInflightTerminalPhase reports whether st is a durable terminal phase THIS BROKER
// WROTE — the only positive evidence that a client-facing obligation has ended.
//
// Both members are here deliberately. A rule naming only pubrel_terminal sends an
// acknowledged QoS 1 exchange, whose PUBACK advanced it to publish_terminal and whose
// holder then drained its token, to a permanent reservation: under a Receive Maximum of 1
// that stops the session receiving anything at all. The two branches owe DIFFERENT
// follow-on obligations, which is why they stay distinct phases with distinct
// dispositions rather than becoming one.
func mqttInflightTerminalPhase(st mqttInflightState) bool {
	return st == mqttInflightPublishTerminal || st == mqttInflightPubRelTerminal
}

// mqttInflightPastPubRec reports whether st is one of the states in which the client is
// holding the identifier for a PUBREL exchange.
//
// The distinction every gate below turns on: from PUBLISH an identifier may be reaped or
// cancelled because the client may never have seen it, and past PUBREC it may not.
//
// Written as an EXPLICIT SET rather than `st != mqttInflightPublish`, which would read an
// unknown state as past PUBREC. publish_terminal is deliberately NOT a member: its
// exchange ended without a PUBREL, so there is no PUBREL obligation to protect.
func mqttInflightPastPubRec(st mqttInflightState) bool {
	switch st {
	case mqttInflightPubRelIntended, mqttInflightPubRelReady, mqttInflightPubRelTerminal:
		return true
	}
	return false
}

// mqttInflightStateKnown reports whether s is a state this broker writes. Anything else
// is refused rather than normalised.
func mqttInflightStateKnown(s mqttInflightState) bool {
	switch s {
	case mqttInflightPublish, mqttInflightPublishTerminal, mqttInflightPubRelIntended,
		mqttInflightPubRelReady, mqttInflightPubRelTerminal:
		return true
	}
	return false
}

// mqttInflightShapeError names why a persisted slot is not a shape this broker writes. A
// distinct type so the refusal can be logged with its reason and so one validator can
// generate the shape tests.
type mqttInflightShapeError struct{ why string }

func (e *mqttInflightShapeError) Error() string { return e.why }

func mqttShapeErr(why string) error { return &mqttInflightShapeError{why: why} }

// mqttValidateInflightShape is THE legal-shape matrix, and it is deliberately one
// function rather than checks spread across the readers.
//
// Every earlier attempt at this subsystem failed by classifying owner state at the point
// of use, where the set of legal shapes was never closed and entries changed class in
// place underneath the classification. So legality is decided in exactly one place: a
// record either satisfies this function or it is refused, and no reader forms its own
// opinion.
//
// STRUCTURAL only. It proves the field combination is one this broker emits; it does not
// prove the named source, copy, PUBREL record, consumer durable or session acceptance
// exist. Those are CONTEXTUAL and are proved by a separate layer — and BOTH must pass
// before acting. Same division mqttParsePubRelAffineRef already draws against
// q2AffClaimUsableForDecision.
//
// Returns nil when the shape is legal.
// mqttDecodeInflightRecord is the ONE way a persisted register becomes a record, and it
// exists because validating at the recovery scan alone left every other reader trusting the
// bytes.
//
// The write side has had a single boundary for a while: storeInflightCAS validates and
// refuses rather than writing. This is its mirror. A holistic round found the asymmetry
// doing real damage — mqttSlotAdmitsDisposition checks identity and state, loadInflightRecord
// checked identity, and between them an illegal but identity-valid record (a `publish` slot
// carrying half an action, or half a PUBREL identity) counted as readable AND deletable, so a
// destructive release could act on a body the schema declares uninterpretable.
//
// The fix is a property rather than another site: shape validation belongs to every read that
// can authorize a mutation, and the way to guarantee that is to leave exactly one decoder.
// TestMQTTInflightRecordsAreDecodedThroughOneAPI holds the boundary closed.
//
// An error means UNREADABLE, which every caller already has a conservative path for: retain
// the record, reserve the identifier, and prove nothing from the body.
func mqttDecodeInflightRecord(data []byte) (*mqttInflightRecord, error) {
	// STRICT: an unrecognised field makes the record UNREADABLE, and this is the clause
	// that replaced five typed fields whose only job was to refuse themselves.
	//
	// A permissive json.Unmarshal silently drops any field this struct does not declare, so
	// an unsupported record carrying, say, an `action` or a `pubrel_dur` — or any control
	// field a future revision adds — would be read as an ordinary legal `publish` register
	// and acted on. Naming the fields to reject cannot close that: it closes exactly the set
	// someone guessed in advance.
	//
	// DisallowUnknownFields ALONE does not close it either, and a review round proved that
	// with counterexamples this decode now refuses. encoding/json matches member names
	// CASE-INSENSITIVELY and permits DUPLICATES with the last value winning, so
	//
	//	{"state":"pubrel_ready","state":"publish"}
	//	{"STATE":"pubrel_ready","state":"publish"}
	//
	// both decoded to an ordinary `publish` record while carrying an unsupported state —
	// the exact reading that licenses a PUBLISH under an identifier already past PUBREC.
	// So the key walk below is EXACT-CASE and duplicate-rejecting, and it runs before any
	// typed decode rather than relying on one.
	if err := mqttJSONMembersAreExactAndUnique(data, mqttInflightRecordJSONKeys(), "register"); err != nil {
		return nil, err
	}
	// PRESENCE, not value. `acq` is mandatory and zero is one of its legal values, so a
	// typed check after the decode cannot tell a fresh allocation from a record that
	// never carried the field. The key walk can, and it is the only place that can.
	if err := mqttJSONMembersArePresent(data, mqttInflightRecordRequiredJSONKeys, "register"); err != nil {
		return nil, err
	}
	r := &mqttInflightRecord{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	if err := mqttValidateInflightShape(r); err != nil {
		return nil, err
	}
	return r, nil
}

// mqttJSONMembersAreExactAndUnique walks a record's top level and requires every member
// name to be one of `allowed` — a field this broker defines — spelled EXACTLY, and present
// at most once. `noun` names the record class in the refusal ("register", "marker").
//
// Shared by the register decoder and the unsent-marker decoder (inflight_marker.go): the
// two persisted classes on $MQTT_inflight are different records with different schemas,
// and the closed-schema property must hold for both by the SAME walk rather than by a
// second one that can drift.
//
// json.Unmarshal is used for the typed decode afterwards rather than a Decoder, and that
// choice is what refuses TRAILING CONTENT: Unmarshal rejects it, where Decoder.More does
// not — More answers "is there another element in this array or object", so it returns
// false when the next byte is `]` or `}` and a record followed by a stray closing delimiter
// was accepted. TestInflightRecordDecodeIsAClosedSchema pins that, in all three forms,
// wherever the refusal comes from.
//
// The permitted set is derived from the struct's own JSON tags by reflection, so adding a
// field cannot desync it from a hand-maintained list — the failure mode that put three
// phantom names in an AST guard's allow-list elsewhere in this change.
func mqttJSONMembersAreExactAndUnique(data []byte, allowed map[string]struct{}, noun string) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return mqttShapeErr("a " + noun + " is a JSON object")
	}
	seen := make(map[string]struct{}, len(allowed))
	for dec.More() {
		kt, kerr := dec.Token()
		if kerr != nil {
			return kerr
		}
		key, ok := kt.(string)
		if !ok {
			return mqttShapeErr("a " + noun + " member name is not a string")
		}
		if _, ok := allowed[key]; !ok {
			return mqttShapeErr(noun + " member " + key + " is not a field this broker writes")
		}
		if _, dup := seen[key]; dup {
			return mqttShapeErr(noun + " member " + key + " appears more than once")
		}
		seen[key] = struct{}{}
		// The value is skipped rather than interpreted: this walk decides KEYS, and the
		// typed decode decides values. Decode into an interface consumes exactly one
		// value however deeply nested it is.
		var skip any
		if verr := dec.Decode(&skip); verr != nil {
			return verr
		}
	}
	// The closing brace. The END of input is deliberately NOT checked here, and the reason
	// is worth stating because the first version of this boundary checked it twice and
	// neither check was the one doing the work.
	//
	// json.Unmarshal — the typed decode in the caller — refuses trailing content itself, so
	// a second refusal here can never be the cause of one. A mutation reverting this to
	// Decoder.More proved it: the test still passed, because Unmarshal caught every case.
	// Two mechanisms stating one fact is how a reader and a writer come to disagree, so
	// there is one, and it is the caller's. Decoder.More is the wrong instrument in any
	// case: it answers "is there another element in this array or object" and returns false
	// when the next byte is `]` or `}`, which is exactly how a record followed by a stray
	// closing delimiter got accepted.
	if _, cerr := dec.Token(); cerr != nil {
		return cerr
	}
	return nil
}

// mqttInflightRecordRequiredJSONKeys are the members a register MUST carry. A record
// missing one is an unsupported persisted shape and is REFUSED, never defaulted.
//
// Only fields whose ZERO VALUE IS LEGAL need to be here: for the others, an absent member
// and a zero member are the same refusal downstream, and the validator already makes it.
// `acq` is the case that forced this walk — a fresh allocation carries Acq == 0, so
// "missing" and "new" are indistinguishable after the typed decode, and reading one as the
// other would hand a record from an unsupported shape a brand-new exchange's transmission
// identity.
var mqttInflightRecordRequiredJSONKeys = map[string]struct{}{"acq": {}}

// mqttJSONMembersArePresent requires every member in `required` to appear at the record's
// top level. It runs AFTER mqttJSONMembersAreExactAndUnique, which has already proved the
// document is an object with exact, unique, permitted names, so this walk only counts.
func mqttJSONMembersArePresent(data []byte, required map[string]struct{}, noun string) error {
	if len(required) == 0 {
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for key := range required {
		v, ok := raw[key]
		if !ok {
			return mqttShapeErr(noun + " member " + key + " is mandatory and absent")
		}
		// NULL IS ABSENCE SPELLED DIFFERENTLY (holistic review round 6, finding 5). The typed
		// decode turns it into the zero value, which for `acq` is a REAL transmission counter
		// — a fresh allocation carries zero — so an unsupported record would be handed a new
		// exchange's identity and the next save would write it back as an explicit zero. An
		// explicit numeric zero stays supported; only the unwritable spelling is refused.
		if string(bytes.TrimSpace(v)) == "null" {
			return mqttShapeErr(noun + " member " + key + " is mandatory and null")
		}
	}
	return nil
}

// mqttInflightRecordJSONKeys is the record's own JSON member names, from its tags.
//
// Computed once. A tag of "-" is excluded and an untagged exported field contributes its
// Go name, which is what encoding/json itself would accept.
var mqttInflightRecordJSONKeys = sync.OnceValue(func() map[string]struct{} {
	return mqttJSONKeysOfType(reflect.TypeOf(mqttInflightRecord{}))
})

// mqttJSONKeysOfType is the member-name set encoding/json would accept for a struct type,
// derived from its tags; shared with the marker's key set (inflight_marker.go).
func mqttJSONKeysOfType(t reflect.Type) map[string]struct{} {
	keys := make(map[string]struct{}, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != _EMPTY_ {
			continue // unexported: encoding/json ignores it
		}
		name := f.Name
		if tag := f.Tag.Get("json"); tag != _EMPTY_ {
			if tag == "-" {
				continue
			}
			if comma := strings.IndexByte(tag, ','); comma >= 0 {
				tag = tag[:comma]
			}
			if tag != _EMPTY_ {
				name = tag
			}
		}
		keys[name] = struct{}{}
	}
	return keys
}

func mqttValidateInflightShape(r *mqttInflightRecord) error {
	if r == nil {
		return mqttShapeErr("no record")
	}
	if !mqttInflightStateKnown(r.State) {
		// Never defaulted to PUBLISH: see mqttInflightState.
		return mqttShapeErr("unknown or absent state")
	}
	// The IDENTITY clause is delegated rather than restated. mqttInflightRecordIdentityOK
	// already decides it, it carries its own tests and its own mutation, and it holds one
	// clause this rule would otherwise lose — an affine claim subject must name a valid
	// INCARNATION, because _EMPTY_ is the deliberate opt-out at advanceQ2AffClaim's fence
	// and a record that opted out silently would let a stale exchange record AffineDone
	// against a replacement claim (F-065). Two copies of the identity rule is precisely the
	// redundancy that lets a reader and a writer disagree.
	if !mqttInflightRecordIdentityOK(r) {
		return mqttShapeErr("no usable record identity")
	}

	// The LINEAGE is mandatory, and absent fails exactly as an absent State does rather than
	// being defaulted to the nonce. Defaulting is the whole hazard: a record that predates
	// the lineage would silently acquire one equal to its allocation's nonce, which is the
	// value a FRESH exchange gets — so an inherited obligation would be read as a new one,
	// and completion evidence recorded against the real lineage would name nothing. The
	// bridge is pre-release and its `$MQTT_*` schemas carry no compatibility contract
	// (docs/ARCHITECTURE.md, "Compatibility boundary"): such a record means the development
	// store predates this design and must be recreated.
	if !mqttValidInflightNonce(r.Lin) {
		return mqttShapeErr("no well-formed exchange lineage")
	}

	// A sequence with NO claimant names nobody, and is refused at every QoS.
	//
	// A claimant with NO SEQUENCE is legal for QoS 1 ONLY, and the narrowness matters. QoS 1
	// records genuinely carry that shape today — the cancellation tombstones and the shared
	// reservations both do — and the arbitration compares `cur.ClaimSeq < claimSeq`, so a
	// zero sequence loses every ordering contest rather than being incoherent.
	//
	// That holds for QoS 2 too, and requiring a non-zero sequence there refused a broad
	// swathe of records the broker writes TODAY: `sess.seq` is zero until the session
	// record's first save, and outbound QoS 2 registers are written independently of it, so
	// a dozen suites went red.
	//
	// The claim's SHAPE is deliberately not validated. It is an opaque session-object
	// identity that this subsystem only ever compares for equality — it is never parsed,
	// and it is minted elsewhere — so imposing the register nonce's alphabet on it was my
	// invention rather than a property of the field, and it refused legitimate claims. Only
	// its PAIRING with the ordering sequence is a shape question.
	claimed := r.Claim != _EMPTY_
	if !claimed && r.ClaimSeq != 0 {
		return mqttShapeErr("claim sequence without a claimant")
	}

	// The ACTION triple is whole or absent, and its claimant must equal the custody it
	// was taken under. A partial triple names an action nobody can drain.
	hasAction := r.ActionToken != _EMPTY_
	if hasAction {
		if !mqttValidInflightNonce(r.ActionToken) {
			return mqttShapeErr("action token is not a valid nonce")
		}
		if r.ActionClaim == _EMPTY_ {
			return mqttShapeErr("action token without a claimant")
		}
		if r.ActionClaim != r.Claim || r.ActionClaimSeq != r.ClaimSeq {
			// The action belongs to the custody that acquired it. A mismatch means one
			// of the two was rewritten underneath the other, which is exactly the
			// stale-custodian case the exclusion exists to refuse.
			return mqttShapeErr("action claimant does not match the slot's custody")
		}
	} else if r.ActionClaim != _EMPTY_ || r.ActionClaimSeq != 0 {
		return mqttShapeErr("action claimant without an action token")
	}

	// The canonical PUBREL record identity is whole or absent, for the same reason: half
	// of it names no message.
	hasPubRel := r.PubRelDurable != _EMPTY_
	if hasPubRel != (r.PubRelStreamSeq != 0) {
		return mqttShapeErr("canonical PUBREL record identity is partially written")
	}

	// The affine pair is one reference, never half of one (F-065).
	hasAff := r.Q2AffClaim != _EMPTY_
	if hasAff != (r.Q2AffCopy != _EMPTY_) {
		return mqttShapeErr("affine reference is partially written")
	}

	if r.QoS == 1 {
		// QoS 1 has one acknowledgement, so no affine pair and no PUBREL belong to it.
		// Custody is MANDATORY now, at every QoS: C-III's first clause is that every slot
		// has custody at creation whatever its QoS and whether or not it is source-backed,
		// and permitting custody-free ordinary QoS 1 here is exactly the omission that
		// kept that contract from closing.
		//
		// Its two phases are publish and publish_terminal. The explicit check is
		// load-bearing: without it a QoS 1 record could borrow a PUBREL phase and pass
		// the generic pairing checks above.
		if r.State != mqttInflightPublish && r.State != mqttInflightPublishTerminal {
			return mqttShapeErr("QoS 1 outside the PUBLISH phases")
		}
		if !claimed {
			return mqttShapeErr("a QoS 1 slot carrying no custody")
		}
		if hasPubRel {
			return mqttShapeErr("QoS 1 carrying PUBREL state")
		}
		if hasAff {
			return mqttShapeErr("QoS 1 carrying an affine reference")
		}
		if r.Retired {
			return mqttShapeErr("QoS 1 marked retired")
		}
		return nil
	}

	// CUSTODY IS MANDATORY AT EVERY QOS AND IN EVERY PHASE (C-III, clause 1). A slot
	// without it has nothing for a takeover to transfer and nothing for an action to bind
	// to, and the earlier permission for custody-free PUBLISH-phase records is exactly
	// the population that kept the contract open.
	if !claimed {
		return mqttShapeErr("a slot carrying no custody")
	}

	// The QoS 2 per-state matrix.
	if r.State == mqttInflightPublish {
		// Retired IS legal here, and that surprised me until the tree said otherwise.
		// `Retired` is a fact about the SOURCE — "past PUBREC with an unproven
		// acknowledgement" — not about the slot's phase, and the pre-existing design
		// records it while the register is still in its PUBLISH shape: recovery reads
		// register-plus-PUBREL-record as past PUBREC precisely because the register
		// itself does not say so. Several e2e paths set it before anything could
		// re-phase the slot. Cancelled alone is legal too: a never-transmitted
		// allocation.
		//
		// What is NOT legal in `publish` is anything naming a PUBREL exchange, because
		// no PUBREL record exists until the PUBREC handoff writes one. An ACTION is now
		// legal here, and that is C-I's representation change: the PUBLISH transmission
		// holds a durable action exactly as a PUBREL transmission does, where before it
		// held only process memory.
		if hasPubRel {
			return mqttShapeErr("a PUBLISH-phase slot naming a canonical PUBREL record")
		}
		return nil
	}

	if r.State == mqttInflightPublishTerminal {
		// The PUBLISH phase ended without a PUBREL exchange, so there is no canonical
		// record to name and never was one. A token may survive here: the
		// acknowledgement and the holder's drain are independent events and either may
		// land first, which is precisely why terminal authority had to become a durable
		// phase rather than an inference.
		if hasPubRel {
			return mqttShapeErr("a publish_terminal slot naming a canonical PUBREL record")
		}
		if r.Cancelled && !r.Retired {
			// The exchange reached the client far enough to be acknowledged or refused,
			// so "never transmitted" is false here by construction.
			return mqttShapeErr("a publish_terminal slot marked cancelled but not retired")
		}
		return nil
	}

	// PAST PUBREC. The client is holding this identifier for a PUBREL exchange, so every
	// rule below exists to stop something freeing or re-minting it.

	// The canonical record is what the exchange IS from pubrel_ready on: admission
	// correlates against it, duplicate disposal is scoped by it, and terminal cleanup
	// acknowledges it.
	//
	// pubrel_intended is the ONE past-PUBREC phase that legally names none, and that is
	// the whole point of it: the phase is written BEFORE the store, so that a crash
	// between them leaves a slot that says "past PUBREC, canonical record OWED" instead
	// of a `publish` slot a peer may reap. A tuple-less intent is what materialization
	// binds.
	if !hasPubRel && r.State != mqttInflightPubRelIntended {
		return mqttShapeErr("a past-PUBREC slot naming no canonical PUBREL record")
	}

	// Cancelled says "never transmitted", and past PUBREC that is false by construction:
	// the client answered under this identifier. It is legal ONLY alongside Retired,
	// where Retired dominates and the exchange is correctly read as transmitted.
	if r.Cancelled && !r.Retired {
		return mqttShapeErr("a past-PUBREC slot marked cancelled but not retired")
	}

	// An action is legal in EVERY phase (C-I): the token is the authority to transmit,
	// the phase is what the exchange has reached, and encoding one in the other is what
	// made a PUBLISH transmission unfenceable. There is deliberately no rule here
	// pairing the two — the pairing rules that remain are the whole-or-absent triple and
	// the custody match, both checked above.
	return nil
}

// ---------------------------------------------------------------------------
// C-IV: the ONE release authority
// ---------------------------------------------------------------------------

// mqttSlotMarkerProof is what has been PROVEN about an allocation's unsent marker, and it
// is deliberately tri-state.
//
// A bool folds "absent" and "not observed" together, and that fold is how an
// indeterminate read becomes a licence to free an identifier. Every proof this subsystem
// admits is tri-state for the same reason the acceptance proof is.
type mqttSlotMarkerProof int

const (
	// mqttSlotMarkerIndeterminate: the marker could not be resolved. Nothing follows.
	mqttSlotMarkerIndeterminate mqttSlotMarkerProof = iota
	// mqttSlotMarkerPresent: the marker for THIS allocation is authoritatively present,
	// so no byte of this delivery has left. It is the strongest non-transmission proof.
	mqttSlotMarkerPresent
	// mqttSlotMarkerAbsent: the marker is authoritatively gone. This proves the exchange
	// is POSSIBLY-ATTEMPTED. It is not a proof of transmission and it is never a proof of
	// completion.
	mqttSlotMarkerAbsent
)

// mqttSlotAuthority is the answer to the only question a destructive operation may ask:
// "may I dispose of, or free capacity at, this slot, under the identity I am acting for?"
//
// It is a VALUE rather than a predicate so that it can be carried to the actual deletion
// boundary. A site that has not called mqttSlotReleaseAuthority cannot construct one, so
// an unenumerated durable release route is a compile error rather than a silent second
// opinion — which is the difference between a property and the census that three review
// rounds dismantled.
type mqttSlotAuthority int

const (
	// mqttSlotWithhold: no release authority, and no conclusion may be drawn. The
	// evidence is unusable or an action is live.
	mqttSlotWithhold mqttSlotAuthority = iota
	// mqttSlotTerminalAuthority: a durable terminal phase this broker wrote, bound to the
	// caller's identity. The phase decides WHICH terminal disposition applies; this value
	// says only that one of them is authorized.
	mqttSlotTerminalAuthority
	// mqttSlotProvenUnsentAuthority: positive evidence that this allocation never went on
	// the wire. The client cannot be holding the identifier, so it may be freed.
	mqttSlotProvenUnsentAuthority
	// mqttSlotReservedAuthority: the DEFAULT. The identifier and its Receive Maximum
	// capacity are held; nothing is deleted and no capacity is freed.
	//
	// It means "no release authority", NOT "no protocol progress". Owed materialization,
	// legitimate PUBREL transmission, custody transfer and phase advance are separate
	// permissions and are not gated on this function — conflating them would turn a
	// safety default into a liveness bug.
	mqttSlotReservedAuthority
)

// mqttSlotEvidence is what a caller has PROVEN about a slot, plus the identity it is
// acting for. Every member is a fact the caller established; none is re-read here.
type mqttSlotEvidence struct {
	// readable is true only when the record loaded, decoded and passed the shape matrix.
	readable bool
	// rec is that record. nil whenever readable is false.
	rec *mqttInflightRecord
	// bound is the transmission identity the caller's own exchange is bound to, captured
	// at that exchange and never refreshed from the slot afterwards (C-I's binding rule).
	bound mqttSlotIdentity
	// marker is what was proven about THIS allocation's unsent marker.
	marker mqttSlotMarkerProof
	// verdict is the session record's persisted CancelledRegs verdict naming THIS
	// allocation's nonce. It is a second carrier of the same cancellation fact as
	// rec.Cancelled and is normalized here rather than beside the function, so a route
	// that consults one and not the other cannot exist.
	verdict bool
}

// mqttSlotIdentity is the (subject, Nonce, Acq) triple that names one transmission's
// authority.
type mqttSlotIdentity struct {
	subject string
	nonce   string
	acq     uint64
}

// mqttSlotAuthorityPermitsRelease is C-IV's consolidation: the ONE table saying which
// authority licenses which RELEASE. It is total over both enums with a refusing default,
// so a disposition added later without a row refuses rather than inherits a licence.
//
// THE RELEASE CLASS IS NARROWER THAN mqttSlotDestructiveDisposition, and the difference is
// the whole point. `mqttSlotReplacePublish` is destructive — it is bound to the full
// transmission identity — but it INSTALLS A SUCCESSOR rather than freeing an identifier,
// so it is not a release and keeps its own identity, phase, action and
// obligation-continuity checks. The invariant is "Reserved permits no RELEASE", never
// "Reserved permits no destructive disposition".
//
// `mqttSlotDeleteComplete` and `mqttSlotDeleteNeverTransmitted` require the same
// authority and are deliberately NOT merged: they differ in the moved-sequence window,
// where reapRegisterWithVerdictGated maps a rewrite to refusal for the latter and to
// progress for the former. Two mechanisms differing in the window where each is the sole
// evidence are not one fact stated twice.
func mqttSlotAuthorityPermitsRelease(auth mqttSlotAuthority, d mqttInflightDisposition) bool {
	switch d {
	case mqttSlotDeleteTerminal, mqttSlotDeletePublishTerminal:
		// A durable terminal phase this broker wrote. Which disposition applies is the
		// phase's business; the authority says only that one of them is licensed.
		return auth == mqttSlotTerminalAuthority
	case mqttSlotDeleteComplete, mqttSlotDeleteNeverTransmitted:
		// Positive proof of non-transmission, and nothing else. Source completion
		// authorizes NOTHING here (C-II): the identifier belongs to the CLIENT exchange.
		return auth == mqttSlotProvenUnsentAuthority
	}
	// Not a release: marks, custody writes, phase advances, action acquisition and
	// replacement are separate permissions and are not gated here. Conflating them would
	// turn a safety default into a liveness bug.
	return false
}

// mqttSlotReleaseDisposition reports whether d is in the RELEASE class — the dispositions
// that discharge a client obligation by freeing the slot, and therefore the only ones that
// must spend an authority.
//
// It is narrower than mqttSlotDestructiveDisposition on purpose: replacement is bound to
// the transmission identity, so it is destructive, but it installs a successor rather than
// freeing an identifier and is not a release.
func mqttSlotReleaseDisposition(d mqttInflightDisposition) bool {
	switch d {
	case mqttSlotDeleteComplete, mqttSlotDeleteNeverTransmitted,
		mqttSlotDeleteTerminal, mqttSlotDeletePublishTerminal:
		return true
	}
	return false
}

// mqttSlotRelease is a release authority BOUND TO THE OBSERVATION THAT PRODUCED IT.
//
// A bare mqttSlotAuthority carries neither identity nor sequence, so it cannot prove it
// was computed against the record the delete will act on. This can: the destructive
// boundary re-checks `bound` and `seq` against what it holds before consulting the table
// above, so an authority computed for one observation cannot be spent on another.
//
// WHAT IT DOES NOT ESTABLISH, stated because overstating it cost several review rounds:
// it rejects a MISMATCHED observation, not a retained observation that has since gone
// stale. A caller whose record moved under it still validates here — which is why a
// release additionally requires the post-delete subject confirmation below, and why an
// exact-sequence delete reporting success is not by itself a release.
//
// The zero value carries mqttSlotWithhold, so a forgotten field refuses.
type mqttSlotRelease struct {
	auth  mqttSlotAuthority
	bound mqttSlotIdentity
	seq   uint64
}

// permits reports whether this release token licenses d against the record the caller
// holds, at the sequence it observed it.
func (rel mqttSlotRelease) permits(subject string, cur *mqttInflightRecord, seq uint64,
	d mqttInflightDisposition) bool {

	if cur == nil || rel.seq == 0 || rel.seq != seq {
		return false
	}
	if rel.bound != mqttSlotIdentityOf(subject, cur) {
		return false
	}
	return mqttSlotAuthorityPermitsRelease(rel.auth, d)
}

// mqttReleaseConfirmed is what a POST-DELETE subject read established, and it exists
// because an exact-sequence delete reads an ALREADY-ABSENT sequence as success — so a
// same-identity rewrite between the load and the delete looks exactly like a successful
// deletion. recoverPublishTerminalSlot has always performed this confirmation inline;
// extracting it is what lets every release-settling site share one definition.
//
// The ABSENT and OTHER answers are deliberately distinct. Both discharge the CALLER's own
// delete obligation, which is why recoverPublishTerminalSlot treats them alike, but only
// ABSENT says the identifier is free: another exchange occupying the subject is a
// successor whose identifier is not ours to release.
type mqttReleaseConfirmed int

const (
	// mqttReleaseUnresolved: indeterminate or unreadable. Not an absence, and not a
	// licence to conclude anything.
	mqttReleaseUnresolved mqttReleaseConfirmed = iota
	// mqttReleaseEstablishedAbsent: the subject holds nothing.
	mqttReleaseEstablishedAbsent
	// mqttReleaseEstablishedOther: another exchange occupies the subject. Ours is gone;
	// the identifier is NOT free.
	mqttReleaseEstablishedOther
	// mqttReleaseRefuted: our own record survives, rewritten. Nothing was released.
	mqttReleaseRefuted
)

// discharged reports whether the CALLER's own delete obligation is over, which is true for
// both established forms. It is NOT "the identifier is free" — only
// mqttReleaseEstablishedAbsent says that.
func (r mqttReleaseConfirmed) discharged() bool {
	return r == mqttReleaseEstablishedAbsent || r == mqttReleaseEstablishedOther
}

// mqttSlotReleaseAuthority is C-IV: ONE total function deciding release authority, with a
// fixed precedence and a conservative default.
//
// It is a TOTAL FUNCTION and that is the structural point. Three review rounds dismantled
// this contract while it was stated as an enumeration of slot shapes, because each round
// found a shape that matched no row — the last was `publish` + Cancelled + no marker + no
// token, which a lifecycle decision in the same revision created. A total function with a
// RESERVING DEFAULT cannot have that failure: a shape nobody enumerated falls to arm 4 and
// holds the identifier, which is the safe direction.
//
// The contract is about ADMISSIBLE PROOFS OF NON-TRANSMISSION with an explicit precedence
// among conflicting evidence and live actions. It is not about marker presence; stated as
// marker presence it stays an enumeration and the next round finds the next member.
//
// What this function deliberately does NOT answer, because they are different questions:
// source classification, marker validation, affine cleanup obligations, and
// generation-discard authority. They keep their own decisions and their own types, and
// none of them may independently conclude that a live incarnation's client obligation has
// ended.
func mqttSlotReleaseAuthority(ev mqttSlotEvidence) mqttSlotAuthority {
	// ARM 1 — unusable evidence. An unreadable or unsupported record, an unresolved
	// marker, or a caller acting for an identity this slot no longer carries. The last is
	// C-I doing its work: B holds evidence naming Acq=k while the occupant is at k+1, and
	// a release on B's authority would discharge an obligation that is not B's.
	if !ev.readable || ev.rec == nil || !mqttInflightStateKnown(ev.rec.State) {
		return mqttSlotWithhold
	}
	if ev.marker == mqttSlotMarkerIndeterminate {
		return mqttSlotWithhold
	}
	if ev.bound.subject == _EMPTY_ || ev.bound.nonce != ev.rec.Nonce || ev.bound.acq != ev.rec.Acq {
		return mqttSlotWithhold
	}

	// ARM 2 — ANY live action, INCLUDING THE CALLER'S OWN.
	//
	// The own-token half is not symmetry for its own sake, it is a real schedule: this
	// broker owns a PUBLISH action whose socket write is still running; another goroutine
	// observes terminal evidence, or the still-present unsent marker, disregards the token
	// because it is "ours", and frees the identifier while the writer can still transmit.
	//
	// A holder ending its OWN action uses mqttSlotDrainAction, which is a different
	// operation from a release and carries its own exact-token match. Releasing is never
	// how an action ends.
	if ev.rec.ActionToken != _EMPTY_ {
		return mqttSlotWithhold
	}

	// ARM 3 — a durable terminal phase THIS BROKER WROTE. Positive authority, not an
	// inference from something missing. Both branches qualify; which disposition applies
	// is the phase's business, and each owes its own follow-on obligations.
	if mqttInflightTerminalPhase(ev.rec.State) {
		return mqttSlotTerminalAuthority
	}

	// ARM 4 — positive proof of non-transmission, from the closed list of facts this
	// broker writes, and only where it is not contradicted.
	//
	// ELIGIBLE PHASE. Only `publish` is a pre-transmission phase. Past PUBREC the client
	// answered under this identifier, so non-transmission is false by construction
	// whatever a stale flag says.
	//
	// CONTRADICTION. Retired DOMINATES, and this is the correction that keeps arm 4 from
	// freeing a live exchange: a PUBLISH can reach the client and be successfully PUBREC'd
	// while the slot still says `publish`, with the retirement recorded and a cancellation
	// surviving over it. Retirement is backed by the client's own PUBREC, so the
	// transmission provably happened; a cancellation over a transmitted delivery is a
	// known-false write. Freeing an identifier the client holds is the catastrophic
	// direction, so a record carrying both reserves.
	//
	// THE EVIDENCE MUST NAME THIS ALLOCATION. Both carriers do — rec.Cancelled is on the
	// record itself, and the verdict was looked up by this record's own nonce — and the
	// marker proof was resolved for this allocation's marker. A cached marker or a raw
	// cancellation bit lifted from elsewhere is not a transferable capability.
	if ev.rec.State == mqttInflightPublish && !ev.rec.Retired {
		if ev.marker == mqttSlotMarkerPresent || ev.rec.Cancelled || ev.verdict {
			return mqttSlotProvenUnsentAuthority
		}
	}

	// ARM 5 — the conservative default. POSSIBLY-ATTEMPTED: the identifier and its
	// Receive Maximum capacity are held until the client's own acknowledgement, or the
	// generation's confirmed discard, settles the exchange.
	return mqttSlotReservedAuthority
}

// mqttSlotIdentityOf is the transmission identity a loaded record currently carries. It is
// for a caller BINDING a fresh exchange — never for re-stamping evidence that was already
// bound, which is the schedule C-I's binding rule forbids.
func mqttSlotIdentityOf(subject string, rec *mqttInflightRecord) mqttSlotIdentity {
	if rec == nil {
		return mqttSlotIdentity{}
	}
	return mqttSlotIdentity{subject: subject, nonce: rec.Nonce, acq: rec.Acq}
}

// mqttInflightSlotRef is the identity an irreversible slot operation must carry.
//
// It exists because the two generic helpers could not fence themselves: a remembered
// stream sequence names a MESSAGE, not an exchange, and `$MQTT_inflight` subjects are
// reused by later exchanges on the same identifier. Carrying a sequence and a nonce
// proved "this record is ours" but not "this record is still in a state my operation may
// touch" — so every caller carries the whole reference plus the permitted transition, and
// the helper re-proves both against the subject.
//
// Deliberately a value, not a pointer into session state: it is captured at one point and
// re-validated at another, and a pointer would silently follow the state it is meant to
// be compared against.
type mqttInflightSlotRef struct {
	// name is the subject's own decomposition: id hash, generation, packet identifier.
	// The SUBJECT is the identity (F-034); seq below is only where we last saw the record.
	name mqttInflightName
	// seq is the sequence the reference was observed at — a CAS base hint, never
	// authority for a delete.
	seq uint64
	// nonce is the exchange identity; state is what the exchange was doing when observed.
	nonce string
	state mqttInflightState
	// acq is the TRANSMISSION identity this reference is bound to (C-I). With the subject
	// and the nonce it is the triple every release, deletion and capacity free CASes on.
	//
	// It is compared by casInflightSlot for the DESTRUCTIVE dispositions only, not by
	// matchesRecord: identity-as-an-exchange and identity-as-a-transmission are two
	// different questions, and a mark or a custody write legitimately acts on an exchange
	// that has since transmitted again. A reference captured before an acquisition and
	// used to RELEASE after one therefore loses, which is the whole point — evidence
	// created before an obligation may not be applied after it. A retry may refresh this
	// reference's SEQUENCE; it may never refresh acq.
	acq uint64
	// dur/sseq are the source delivery, so a mismatched occupant is recognised as another
	// exchange rather than deleted.
	dur  string
	sseq uint64
	// claim/claimSeq are the custody observed with it.
	claim    string
	claimSeq uint64
	// actionLive records that a token was PRESENT on the record this reference was taken
	// from, whoever owns it. It replaces the question the two deleted composite phases
	// used to answer: a token is legal in every phase now, so "is an action live" can no
	// longer be read off `state`, and a phase-shaped test would miss a live transmission
	// on an ordinary `pubrel_ready` or `publish` slot entirely.
	//
	// Distinct from actionToken below, which is non-empty only for the holder.
	actionLive bool
	// actionToken is the licence to drain an active slot: empty for every other holder of
	// a reference, and the EXACT token for the holder that acquired it. A drain matches on
	// it and on nothing else, which is what stops one broker retiring another's action
	// after observing that one exists.
	actionToken string
}

// slotOK reports whether this reference names an exchange at all.
func (s mqttInflightSlotRef) slotOK() bool {
	return s.name.pi != 0 && mqttValidInflightNonce(s.nonce) && mqttInflightStateKnown(s.state)
}

// matchesRecord reports whether cur is the exchange this reference names, by identity
// rather than by sequence: packet identifier, exchange nonce, source delivery. State is
// deliberately NOT compared — a reference is often taken in one state and acted on in
// another, and which transitions are permitted is asked through
// mqttInflightTransitionAllowed.
func (s mqttInflightSlotRef) matchesRecord(cur *mqttInflightRecord) bool {
	if cur == nil {
		return false
	}
	return cur.PI == s.name.pi && cur.Nonce == s.nonce &&
		cur.Dur == s.dur && cur.Seq == s.sseq
}

// mqttSlotDestructiveDisposition reports whether d ends an exchange's occupancy of its
// identifier — a deletion, or a replacement that destroys the previous occupant.
//
// These are exactly the dispositions C-I binds to the FULL transmission identity: "every
// release, deletion or capacity free CASes on the whole triple". A mark, a custody
// transfer, a phase advance and an action acquisition are deliberately NOT here. They do
// not discharge a client obligation, so binding them to `Acq` would make an ordinary
// retirement mark fail against a slot that had merely been transmitted — a liveness bug
// bought for no safety.
func mqttSlotDestructiveDisposition(d mqttInflightDisposition) bool {
	switch d {
	case mqttSlotDeleteComplete, mqttSlotDeleteNeverTransmitted, mqttSlotDeleteTerminal,
		mqttSlotDeletePublishTerminal, mqttSlotReplacePublish:
		return true
	}
	return false
}

// mqttInflightDisposition is what an irreversible operation intends to do to a slot. It
// is passed explicitly rather than inferred, so a helper can refuse an operation whose
// intent does not match the state it finds — the check "audit every caller" could never
// make reliable.
type mqttInflightDisposition int

const (
	// mqttSlotDeleteComplete: a PUBLISH-phase exchange is proven complete and its slot may
	// go. Kept separate from terminal deletion so source-retirement settlement cannot
	// bypass terminal's duplicate scan and affine-proof disposal.
	mqttSlotDeleteComplete mqttInflightDisposition = iota
	// mqttSlotDeleteTerminal: PUBCOMP was observed, every record-side obligation was
	// discharged, and terminal cleanup may remove the owner slot. Legal only from terminal;
	// finalizeTerminalSlot is its sole production caller.
	mqttSlotDeleteTerminal
	// mqttSlotDeleteNeverTransmitted: a delivery PROVABLY never went out. Legal only from
	// PUBLISH.
	//
	// Two evidence routes reach it, and neither is "the source is finished". A
	// CANCELLATION carries a tombstone or a nonce verdict. RECOVERY's marker-proven drop
	// carries a present unsent marker, whose confirmed delete strictly precedes any write
	// — so the register can be removed over a source that is merely gone rather than
	// completed (F-102). It was cancellation-only until then, and reading it that way now
	// makes the recovery caller look unlicensed.
	mqttSlotDeleteNeverTransmitted
	// mqttSlotMarkRetired: recording that this exchange's SOURCE is past PUBREC with an
	// unproven acknowledgement. Legal from any supported state, because the mark is about
	// the SOURCE rather than the slot's phase and the tree records it on a PUBLISH slot.
	mqttSlotMarkRetired
	// mqttSlotMarkCancelled: recording that this allocation was NEVER TRANSMITTED. Legal
	// only from PUBLISH: past PUBREC the client is holding the identifier, so the claim
	// would be false and would license freeing an identifier still in use.
	mqttSlotMarkCancelled
	// mqttSlotDrainAction: the action's own holder finishing it — sending -> ready after a
	// returned write with no PUBCOMP, or terminal_draining -> terminal once the write
	// returns. Legal ONLY from the two active states, and the caller must additionally
	// match the exact token; the phase gate answers "may this class of thing happen here",
	// not "is this token yours".
	mqttSlotDrainAction
	// mqttSlotAcquireAction: a current custodian moving a quiescent PUBREL exchange into
	// an active send. Legal from publish or pubrel_ready, and the CAS additionally requires
	// the exact custody tuple captured by admission. That closes the race between the
	// admission check and a successor's custody transfer.
	mqttSlotAcquireAction
	// mqttSlotTransferCustody: a newly accepted session object taking custody. Legal only
	// from a QUIESCENT state — publish or pubrel_ready — because custody transfer during a
	// live action would hand the exchange to an object that cannot drain the action the
	// old custodian still holds. In an active state the transfer is WITHHELD: the
	// identifier stays reserved, that one exchange is non-actionable on the newcomer, and
	// the rest of the session resumes.
	mqttSlotTransferCustody
	// mqttSlotReplacePublish: the store-conflict loop replacing a foreign write for the
	// exact same source delivery with this allocation's nonce. Legal only while the slot
	// is still in PUBLISH; replacing a past-PUBREC slot would erase its canonical identity
	// and durable phase, reopening the payload under a fresh exchange.
	mqttSlotReplacePublish
	// mqttSlotAdvancePhase: the named phase arrows of the state machine. Every arrow is a
	// CAS against the exact subject sequence, nonce, current state and custody/action
	// fields; this disposition is what distinguishes an intended transition from a
	// rewrite that happens to produce a legal shape.
	mqttSlotAdvancePhase
	// mqttSlotDeletePublishTerminal: the PUBLISH phase ended — PUBACK, or a FAILED
	// PUBREC — and its slot may go. Deliberately NOT mqttSlotDeleteTerminal: that arm's
	// duplicate scan, canonical acknowledgement and affine disposal all presuppose a
	// canonical PUBREL record, and a publish_terminal slot has none.
	mqttSlotDeletePublishTerminal
	// mqttSlotRebindCanonical: owed materialization installing the canonical PUBREL
	// tuple. Legal only from pubrel_intended (an INITIAL binding, with no named record to
	// prove absent) and from pubrel_ready whose named record is PROVABLY absent — a failed
	// lookup, an incomplete scan, an unreadable record and a mismatching record are all
	// insufficient. It needs custody and NO action.
	mqttSlotRebindCanonical
	// mqttSlotAdvanceAcknowledgedTerminal: the `publish -> publish_terminal` arrow driven
	// by a CLIENT ACKNOWLEDGEMENT, and the one phase arrow that is bound to the full
	// transmission identity.
	//
	// Ordinary arrows deliberately are not (see mqttSlotDestructiveDisposition): binding a
	// retirement mark to Acq would refuse it against a slot that had merely been
	// transmitted, a liveness cost bought for no safety. This arrow is different because
	// the EVIDENCE is: a PUBACK names one transmission, and terminal authority recorded
	// over a successor would license disposing of an exchange the client never answered
	// (holistic review round 3, finding 1). A live token refuses it for the same reason —
	// its holder may still be writing the transmission being terminalized.
	mqttSlotAdvanceAcknowledgedTerminal
)

// mqttInflightTransitionAllowed reports whether disposition d may be applied to a slot
// currently in state st.
//
// This is the phase gate, in one place. Every site that deletes, rewrites or frees an
// identifier asks it, so the answer cannot drift between sites — and the sites written
// when a register's life ended at PUBREC are exactly the ones that would otherwise vacate
// a live slot once the slot outlives PUBREC.
//
// An UNSUPPORTED shape authorises nothing — the clause that stops a record from elsewhere
// licensing a delete — and past that, each arm is a real per-state rule. The distinction
// the gate turns on is PUBLISH versus past PUBREC. Retired is deliberately not folded into
// this decision: it is a fact about the source, not a second encoding of the slot phase.
func mqttInflightTransitionAllowed(d mqttInflightDisposition, st mqttInflightState) bool {
	if !mqttInflightStateKnown(st) {
		return false // an unsupported shape authorises nothing
	}
	switch d {
	case mqttSlotDeleteNeverTransmitted, mqttSlotMarkCancelled, mqttSlotReplacePublish:
		// "Never transmitted" is false past PUBREC by construction — the client answered
		// under this identifier — so these are legal only from PUBLISH. This is the arm
		// that stops every path written when a register's life ended at PUBREC from
		// freeing an identifier the client still owns.
		return st == mqttInflightPublish
	case mqttSlotDeleteComplete:
		// Source completion settles only the PUBLISH phase. In particular it may not delete
		// a terminal slot: that would bypass the complete PUBREL scan and affine proof.
		return st == mqttInflightPublish
	case mqttSlotDeleteTerminal:
		// The terminal arm is a permission, not the whole rule — finalizeTerminalSlot must
		// ALSO prove the source non-reofferable, run the complete same-nonce duplicate scan,
		// discharge affine proof and hold a freshness fence at the irreversible edge.
		return st == mqttInflightPubRelTerminal
	case mqttSlotDeletePublishTerminal:
		// The PUBLISH branch of terminal. It owes none of the PUBREL branch's
		// obligations, because a publish_terminal slot names no canonical record — which
		// is exactly why it may not borrow that branch's disposition.
		return st == mqttInflightPublishTerminal
	case mqttSlotRebindCanonical:
		// Materialization installs the canonical tuple from the two owed phases and
		// nowhere else. Which of the two it is decides whether an absence must be PROVED
		// first, and that is the caller's obligation, not this gate's.
		return st == mqttInflightPubRelIntended || st == mqttInflightPubRelReady
	case mqttSlotMarkRetired:
		// Any known state: the mark is about the SOURCE, and it is legitimately recorded
		// while the slot is still in its PUBLISH shape (see the validator's note).
		return true
	case mqttSlotDrainAction:
		// An action is legal in EVERY phase now that the token is no longer encoded in
		// the phase, so draining is a thing that may happen in any of them. The caller
		// must match the EXACT token as well, and that is what actually fences it; this
		// gate only refuses an unsupported shape, which the clause above already did.
		return true
	case mqttSlotAcquireAction:
		// The exact custody comparison is enforced by casInflightSlot; this is its phase
		// half. An action is acquired to TRANSMIT, so the terminal phases are excluded:
		// nothing transmits from them.
		//
		// `pubrel_intended` IS permitted, and the reasoning that excluded it was wrong.
		// It is true that materialization needs custody and no action — but the LIVE path
		// materializes by admitting a delivered record that PROVES the canonical
		// identity, and that admission installs the tuple and the action in ONE CAS.
		// Refusing the origin refused that arrow, which stranded every exchange whose
		// PUBREL had to be recreated: admission declined, nothing transmitted, and the
		// client waited for a PUBREL that never came.
		return st == mqttInflightPublish || st == mqttInflightPubRelIntended ||
			st == mqttInflightPubRelReady
	case mqttSlotTransferCustody:
		// Quiescent PHASES only, and a live token is withheld rather than refused
		// outright by the caller — the difference matters, because withholding keeps the
		// identifier reserved and lets the rest of the session resume, where a refusal
		// would either strand the exchange or hand it to an object that cannot drain the
		// action the old custodian still holds.
		//
		// pubrel_intended is a member now, and it has to be: C-III quantifies custody
		// over EVERY supported quiescent slot, and an owed slot that cannot be taken into
		// custody can never be materialized by the broker that finds it.
		return st == mqttInflightPublish || st == mqttInflightPubRelIntended ||
			st == mqttInflightPubRelReady
	case mqttSlotAdvancePhase:
		// Every state is a legal ORIGIN for some arrow; which arrow is legal from which
		// state is the CAS's business, since it alone knows the target. What this refuses
		// is an advance from an unsupported shape, which mqttInflightStateKnown above
		// already caught.
		return true
	case mqttSlotAdvanceAcknowledgedTerminal:
		// ONE arrow, from ONE origin: the acknowledgement ends a PUBLISH phase and nothing
		// else. The identity and token predicates are the CAS's, checked on every internal
		// read.
		return st == mqttInflightPublish
	}
	return false
}

// mqttSlotRefFromRecord builds a reference to a slot that has just been LOADED, so a
// destructive helper can carry the identity it already proved instead of re-deriving it.
//
// Returns nil when the record cannot be attributed to the subject, which is the same
// fail-closed reading every reader gives that shape: an unattributable record is not
// something to delete on.
func mqttSlotRefFromRecord(subject string, seq uint64, cur *mqttInflightRecord) *mqttInflightSlotRef {
	if cur == nil {
		return nil
	}
	name, err := mqttParseInflightSubject(subject)
	if err != nil || name == nil || name.pi != cur.PI {
		return nil
	}
	return &mqttInflightSlotRef{
		name: *name, seq: seq, nonce: cur.Nonce, acq: cur.Acq, state: cur.State,
		actionLive: cur.ActionToken != _EMPTY_,
		dur:        cur.Dur, sseq: cur.Seq, claim: cur.Claim, claimSeq: cur.ClaimSeq,
	}
}
