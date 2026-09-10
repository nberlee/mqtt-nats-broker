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
	// mqttInflightPubRelReady: past PUBREC. The exchange's PUBREL record exists and the
	// slot names it canonically; no transmission is in progress. This is the state a
	// PUBREL action is acquired FROM and drained BACK TO.
	mqttInflightPubRelReady mqttInflightState = "pubrel_ready"
	// mqttInflightPubRelSending: an action is live and a PUBREL write may be on the
	// wire. While this state holds, custody transfer, slot replacement, ordinary
	// deletion, identifier release and terminal deletion all refuse, and only the
	// action's own holder may drain it.
	mqttInflightPubRelSending mqttInflightState = "pubrel_sending"
	// mqttInflightPubRelTerminalDraining: PUBCOMP was observed while the write had not
	// returned. It keeps the SAME action token: PUBCOMP does not revoke an action, it
	// records that the exchange finished. Observing it is proof an ambiguous acquisition
	// landed, which is why the ambiguity reread accepts it (see the holder lifecycle).
	mqttInflightPubRelTerminalDraining mqttInflightState = "pubrel_terminal_draining"
	// mqttInflightPubRelTerminal: PUBCOMP was observed and no write is outstanding. The
	// exchange is over; what remains is idempotent obligation completion, the complete
	// same-nonce duplicate scan, the canonical record's acknowledgement, and the exact
	// deletion. Nothing transmits from here, which is what makes terminal finalization
	// custody-agnostic.
	mqttInflightPubRelTerminal mqttInflightState = "pubrel_terminal"
)

// mqttInflightPastPubRec reports whether st is one of the four states in which the client
// is holding the identifier for a PUBREL exchange.
//
// The distinction every gate below turns on: from PUBLISH an identifier may be reaped or
// cancelled because the client may never have seen it, and past PUBREC it may not.
func mqttInflightPastPubRec(st mqttInflightState) bool {
	switch st {
	case mqttInflightPubRelReady, mqttInflightPubRelSending,
		mqttInflightPubRelTerminalDraining, mqttInflightPubRelTerminal:
		return true
	}
	return false
}

// mqttInflightActionState reports whether st is one of the two states in which a durable
// PUBREL action is live, and therefore in which the slot is non-actionable by anyone but
// the action's own holder.
func mqttInflightActionState(st mqttInflightState) bool {
	return st == mqttInflightPubRelSending || st == mqttInflightPubRelTerminalDraining
}

// mqttInflightStateKnown reports whether s is a state this broker writes. Anything else
// is refused rather than normalised.
func mqttInflightStateKnown(s mqttInflightState) bool {
	switch s {
	case mqttInflightPublish, mqttInflightPubRelReady, mqttInflightPubRelSending,
		mqttInflightPubRelTerminalDraining, mqttInflightPubRelTerminal:
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
		// QoS 1 has one phase and one acknowledgement, so no affine pair belongs to it.
		// Custody is OPTIONAL here: a plain QoS 1 register carries none and a shared one
		// carries a valid claim, which is the pre-existing arbitration and is left alone.
		// Cancelled is free either way. The explicit state check is load-bearing now that
		// the type also names QoS 2 PUBREL phases: without it a QoS 1 record could borrow
		// a canonical tuple and action and pass the generic pairing checks above.
		if r.State != mqttInflightPublish {
			return mqttShapeErr("QoS 1 outside the PUBLISH phase")
		}
		if hasAction || hasPubRel {
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

	// A publish-state QoS 2 slot may be read without custody for compatibility with the
	// pre-transition shape, but production creates every new slot with custody and the
	// pre-CONNACK classifier refreshes every eligible recovered one. Admission requires
	// current custody before it can advance either from publish or ready. Past PUBREC the
	// shape itself makes custody mandatory, because an action has to bind to it.

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
		// no PUBREL record exists until the PUBREC handoff writes one.
		if hasAction {
			return mqttShapeErr("a PUBLISH-phase slot carrying a PUBREL action")
		}
		if hasPubRel {
			return mqttShapeErr("a PUBLISH-phase slot naming a canonical PUBREL record")
		}
		return nil
	}

	// PAST PUBREC. The client is holding this identifier for a PUBREL exchange, so every
	// rule below exists to stop something freeing or re-minting it.

	// The canonical record is what the exchange IS from here on: admission correlates
	// against it, duplicate disposal is scoped by it, and terminal cleanup acknowledges
	// it. A past-PUBREC slot that does not name one describes an exchange nothing can
	// complete.
	if !hasPubRel {
		return mqttShapeErr("a past-PUBREC slot naming no canonical PUBREL record")
	}

	// Custody is MANDATORY past PUBREC: it is what a takeover transfers and what an
	// action binds to, so a slot without it has nothing for either to check. ClaimSeq
	// stays free to be zero — sess.seq is zero until the session record's first save,
	// and a zero sequence LOSES every ordering contest rather than being incoherent.
	if !claimed {
		return mqttShapeErr("a past-PUBREC slot carrying no custody")
	}

	// Cancelled says "never transmitted", and past PUBREC that is false by construction:
	// the client answered under this identifier. It is legal ONLY alongside Retired,
	// where Retired dominates and the exchange is correctly read as transmitted.
	if r.Cancelled && !r.Retired {
		return mqttShapeErr("a past-PUBREC slot marked cancelled but not retired")
	}

	// An action is live in exactly the two active states, and nowhere else. In
	// `pubrel_ready` there is no transmission to fence; in `pubrel_terminal` the write
	// has returned and PUBCOMP was observed, so a surviving token would name a licence
	// nothing can exercise.
	if hasAction != mqttInflightActionState(r.State) {
		if hasAction {
			return mqttShapeErr("an action token outside pubrel_sending / pubrel_terminal_draining")
		}
		return mqttShapeErr("an active state carrying no action token")
	}
	return nil
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
	// dur/sseq are the source delivery, so a mismatched occupant is recognised as another
	// exchange rather than deleted.
	dur  string
	sseq uint64
	// claim/claimSeq are the custody observed with it.
	claim    string
	claimSeq uint64
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
	// mqttSlotDeleteNeverTransmitted: a cancellation whose delivery provably never went
	// out. Legal only from PUBLISH.
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
	case mqttSlotMarkRetired:
		// Any known state: the mark is about the SOURCE, and it is legitimately recorded
		// while the slot is still in its PUBLISH shape (see the validator's note).
		return true
	case mqttSlotDrainAction:
		// Only the two active states have an action to drain. The caller must match the
		// EXACT token as well; this gate says only that draining is a thing that may
		// happen here.
		return mqttInflightActionState(st)
	case mqttSlotAcquireAction:
		// The exact custody comparison is enforced by casInflightSlot; this is its phase
		// half. Ready is ordinary admission and publish is the direct race arrow.
		return st == mqttInflightPublish || st == mqttInflightPubRelReady
	case mqttSlotTransferCustody:
		// Quiescent states only. In an active state the transfer is withheld rather than
		// refused outright — the difference matters, because withholding keeps the
		// identifier reserved and lets the rest of the session resume, where a refusal
		// would either strand the exchange or hand it to an object that cannot drain the
		// action the old custodian still holds.
		return st == mqttInflightPublish || st == mqttInflightPubRelReady
	case mqttSlotAdvancePhase:
		// Every state is a legal ORIGIN for some arrow; which arrow is legal from which
		// state is the CAS's business, since it alone knows the target. What this refuses
		// is an advance from an unsupported shape, which mqttInflightStateKnown above
		// already caught.
		return true
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
		name: *name, seq: seq, nonce: cur.Nonce, state: cur.State,
		dur: cur.Dur, sseq: cur.Seq, claim: cur.Claim, claimSeq: cur.ClaimSeq,
	}
}
