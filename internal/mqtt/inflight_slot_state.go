// The durable QoS 2 owner slot: the design in which one PI-scoped `$MQTT_inflight` record
// owns one outbound QoS 2 exchange for its WHOLE life, not only its PUBLISH phase.
//
// WHAT THIS FILE IS, stated first because every rule below reads like a live invariant and
// on this tree most of them are not yet reachable. This is the SUBSTRATE: the state set,
// the one legal-shape matrix, the slot reference, the dispositions and the phase gate. The
// mechanism that would drive a slot past `publish` — PUBREL admission and the terminal
// release — is F-092's unbuilt half, so production writes only `publish` and the handoff
// still deletes the record at PUBREC. Read the rules here as the contract the admission
// half must satisfy, and the enforcement here as what is already in force for the PUBLISH
// phase: the shape matrix and the phase gate refuse anything else outright, which is what
// makes landing the rest a change to one file rather than a re-derivation.
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
// NONE OF THAT IS IN THIS FILE. The states, the durable action token and the dispositions
// that referred to them were built here and deleted, because nothing mints an action: the
// only thing that would is the unbuilt PUBREL write path. What survives is the reasoning —
// which is why the CAS has to be the mechanism, and why no lease can substitute — and it
// survives here because the next attempt should not have to re-derive it. See
// docs/ROADMAP.md's F-092 bullet.
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
// ONE state is defined, and the four that used to sit beside it — `pubrel_ready`,
// `pubrel_sending`, `pubrel_terminal_draining`, `pubrel_terminal` — were built and then
// DELETED before this landed. Nothing produced them: the PUBREC handoff still deletes the
// register, so `publish` is the only state ever written, and a validator that recognised
// four unproduced names was implementing an INCOMPLETE closed schema — those four
// rejected, any fifth plausible field silently accepted by encoding/json. The closed
// schema now belongs to the decoder (mqttDecodeInflightRecord), which refuses ANY
// unrecognised field, and the states come back with the admission half that produces them.
// docs/ROADMAP.md's F-092 bullet carries the design they belong to.
type mqttInflightState string

const (
	// mqttInflightPublish: the exchange is awaiting PUBACK or PUBREC. The PUBLISH paths
	// own it, unchanged from before the slot existed.
	mqttInflightPublish mqttInflightState = "publish"
)

// mqttInflightStateKnown reports whether s is a state this broker writes. Anything else
// is refused rather than normalised.
func mqttInflightStateKnown(s mqttInflightState) bool {
	return s == mqttInflightPublish
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
	if err := mqttInflightKeysAreExactAndUnique(data); err != nil {
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

// mqttInflightKeysAreExactAndUnique walks the record's top level and requires every member
// name to be a field this broker defines, spelled EXACTLY, and present at most once — then
// requires the input to end.
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
func mqttInflightKeysAreExactAndUnique(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return mqttShapeErr("a register is a JSON object")
	}
	allowed := mqttInflightRecordJSONKeys()
	seen := make(map[string]struct{}, len(allowed))
	for dec.More() {
		kt, kerr := dec.Token()
		if kerr != nil {
			return kerr
		}
		key, ok := kt.(string)
		if !ok {
			return mqttShapeErr("a register member name is not a string")
		}
		if _, ok := allowed[key]; !ok {
			return mqttShapeErr("register member " + key + " is not a field this broker writes")
		}
		if _, dup := seen[key]; dup {
			return mqttShapeErr("register member " + key + " appears more than once")
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
	t := reflect.TypeOf(mqttInflightRecord{})
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
})

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

	// The action fields and the canonical PUBREL record identity used to be validated here
	// as all-or-nothing pairs. Those five fields are GONE, and the closed-schema job they
	// were doing now belongs to mqttDecodeInflightRecord, which refuses any unrecognised
	// field rather than the handful this matrix could name in advance.
	//
	// The affine pair is one reference, never half of one (F-065).
	hasAff := r.Q2AffClaim != _EMPTY_
	if hasAff != (r.Q2AffCopy != _EMPTY_) {
		return mqttShapeErr("affine reference is partially written")
	}

	if r.QoS == 1 {
		// QoS 1 has one phase and one acknowledgement, so no affine pair belongs to it.
		// Custody is OPTIONAL here: a plain QoS 1 register carries none and a shared one
		// carries a valid claim, which is the pre-existing arbitration and is left alone.
		// Cancelled is free either way. A state check is redundant now that `publish` is
		// the only state the type has, and mqttInflightStateKnown above already required it.
		if hasAff {
			return mqttShapeErr("QoS 1 carrying an affine reference")
		}
		if r.Retired {
			return mqttShapeErr("QoS 1 marked retired")
		}
		return nil
	}

	// Universal QoS 2 custody is part of the UNBUILT mechanism, not of this shape rule, and
	// the distinction cost two red suites to find.
	//
	// The plan requires custody on every QoS 2 slot, because custody is what a takeover
	// transfers and what a PUBREL action binds to. Neither of those exists yet, and the
	// tree does not populate it: a plain, non-shared QoS 2 register carries no claim today,
	// because sess.regClaim is a shared-delivery ownership identity. Requiring it here
	// refused ordinary outbound QoS 2 registers — the common path — which is a far larger
	// break than the rule was worth.
	//
	// So custody stays OPTIONAL, exactly as today, and becomes mandatory in the same change
	// that makes it load-bearing. That change is also where the zero-sequence question is
	// settled: a ClaimSeq of zero names no persisted acceptance, so custody has to be
	// refreshed to the confirmed session-record sequence before an action may bind to it.
	// Recording it here so the next attempt does not rediscover it as a surprise.

	// `publish` is the only state, so this is the whole QoS 2 rule.
	//
	// Retired IS legal here, and that surprised me until the tree said otherwise.
	// `Retired` is a fact about the SOURCE — "past PUBREC with an unproven
	// acknowledgement" — not about the slot's phase, and the pre-existing design records it
	// while the register is still in its PUBLISH shape: recovery reads
	// register-plus-PUBREL-record as past PUBREC precisely because the register itself does
	// not say so. Several e2e paths set it before anything could re-phase the slot.
	// Cancelled alone is legal too: a never-transmitted allocation.
	//
	// The past-PUBREC half of this matrix — a canonical PUBREL record identity being
	// mandatory, cancelled-without-retired being refused, an action requiring custody with
	// a persisted acceptance sequence, and an action's claimant having to equal the
	// custodian — was written and DELETED with the states and fields it spoke about. Those
	// rules are the contract the admission half must satisfy and they are recorded in
	// docs/ROADMAP.md's F-092 bullet; validating them here against fields nothing writes
	// only made this function look like it was enforcing more than it was.
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
	// An actionToken belongs here — the licence to drain an active slot, empty for every
	// other holder of a reference — and went with the action itself. See mqttInflightState.
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
	// mqttSlotDeleteComplete: the exchange is finished and its slot may go. Legal from
	// PUBLISH, and from the terminal state once one exists.
	mqttSlotDeleteComplete mqttInflightDisposition = iota
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
)

// Three more dispositions belong to this set and went with the states they applied to:
// mqttSlotDrainAction (the action's holder finishing it), mqttSlotTransferCustody (a newly
// accepted session object taking custody, legal only from a quiescent state) and
// mqttSlotAdvancePhase (the named phase arrows). Every one of them was legal only from a
// state nothing produces, so each was a permission that could never be exercised. They
// return with admission; docs/ROADMAP.md's F-092 bullet holds the rules they carried.

// mqttInflightTransitionAllowed reports whether disposition d may be applied to a slot
// currently in state st.
//
// This is the phase gate, in one place. Every site that deletes, rewrites or frees an
// identifier asks it, so the answer cannot drift between sites — and the sites written
// when a register's life ended at PUBREC are exactly the ones that would otherwise vacate
// a live slot once the slot outlives PUBREC.
//
// With one state, every arm below reduces to "the shape is supported". That is not a
// no-op: an UNSUPPORTED shape authorises nothing, which is the clause that stops a record
// from elsewhere licensing a delete, and it is the clause the mutations target. The
// per-state arms come back with the states.
func mqttInflightTransitionAllowed(d mqttInflightDisposition, st mqttInflightState) bool {
	if !mqttInflightStateKnown(st) {
		return false // an unsupported shape authorises nothing
	}
	switch d {
	case mqttSlotDeleteNeverTransmitted, mqttSlotDeleteComplete, mqttSlotMarkCancelled:
		// Each of these is legal only from PUBLISH. Past PUBREC the client is holding the
		// identifier, so reaping or cancelling would free one it still owns — which is
		// what these arms will refuse once a slot can be past PUBREC.
		return st == mqttInflightPublish
	case mqttSlotMarkRetired:
		// Any known state: the mark is about the SOURCE, and it is legitimately recorded
		// while the slot is still in its PUBLISH shape (see the validator's note).
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
