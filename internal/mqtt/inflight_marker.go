package mqtt

// The UNSENT MARKER: the durable fact that distinguishes "registered" from "transmitted".
//
// An in-flight register (inflight_register.go) is created BEFORE the PUBLISH it covers is
// written to the socket, so that a crash between the two leaves the identifier reserved
// rather than reused. That order is right, and it leaves a gap: on resume the register alone
// cannot say whether the client ever saw the PUBLISH, and the outgoing DUP flag must be
// derived from exactly that ([MQTT-3.3.1-1]: DUP is set on a RE-delivery and MUST be 0 on a
// first delivery). Deriving it from the source consumer's delivery count is wrong — the
// count rises on every offer the broker declined before tracking — and an in-memory bit
// does not survive the restart that makes the question hard (F-072).
//
// So the register's PUBLISH phase is a PAIR. The same atomic batch that creates the register
// also creates a sibling record on
//
//	$MQTT.inflight.<idHash>.<gen>.<PI>.unsent      body {"nonce":"<the register's nonce>"}
//
// and a transmission is recorded by DELETING the marker at its exact stream sequence. The
// marker is present ⇔ nothing has been transmitted under this identifier within this pair
// identity; a resumed PUBLISH-phase register with no marker is a retransmission and carries
// DUP=1; one with a matching marker is a first transmission and carries DUP=0. Nothing
// rewrites either record to record the transmission; the one operation that mints a new
// pair identity is the same-delivery replacement, under the phase gate the register already
// has (mqttSlotReplacePublish).
//
// # Why the classification is one function
//
// Every earlier attempt at the register subsystem failed by classifying owner state at the
// point of use, where the set of legal readings was never closed and entries changed class
// underneath the classification (see mqttValidateInflightShape). The marker inherits the
// remedy: which observation of the pair licenses which action is decided in exactly ONE
// place, mqttDecideMarker, keyed by the operation being attempted. Every consumer — the
// store path, the replacement arm, the sender, the protocol-evidence consumers, terminal
// cleanup, the sweep and the generation teardown — hands it what it observed and executes
// the disposition it returns. No consumer defines a readability, attribution or eligibility
// rule of its own; TestMQTTMarkerDecisionsGoThroughOneBoundary holds that closed.
//
// # Provenance is not outcome
//
// A marker observation carries two independent facts. Its PROVENANCE says how the sequence
// was acquired: CAPTURED before the sibling register was read, so a delete at that sequence
// can tear nothing that a later writer created; RELOADED after (or without) the sibling
// read, so a delete at it could; or NONE HELD, which is a property of the in-memory entry
// (transmitted, no marker sequence) and not an observation of the stream at all. Its
// OUTCOME says what the read resolved to: a body naming this register (match), a body that
// does not (unattributed), a positive absence, or a load that did not resolve
// (indeterminate). The decision table needs both axes because "no marker" arrives in three
// ways that license three different things: an entry that HOLDS none may still have one on
// the stream; a positive absence completes a cleanup; an unresolved load completes nothing.
// A type that folded them would put the sweep's authority to delete behind the sender's
// authority to say nothing, which is the class of defect this file exists to refuse.

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// mqttInflightMarkerSuffix is the fixed final token of a marker subject. A register subject
// has exactly three tokens past the prefix and its parser refuses a fourth, so the two
// classes never parse as each other and mqttInflightWalkFilter (`...<gen>.*`) never
// enumerates a marker.
const mqttInflightMarkerSuffix = "unsent"

// mqttInflightMarker is the marker's body. It names the register it was created with, and
// nothing else: the subject already carries idHash, generation and packet identifier, and
// duplicating them would give a hand-edited body a second place to disagree with the name.
type mqttInflightMarker struct {
	Nonce string `json:"nonce"`
}

// mqttInflightMarkerSubject is the marker's subject for one register.
func mqttInflightMarkerSubject(idHash, gen string, pi uint16) string {
	return mqttInflightSubject(idHash, gen, pi) + "." + mqttInflightMarkerSuffix
}

// mqttInflightMarkerSubjectFor derives the marker subject from a register subject that has
// already been parsed, so a caller holding a register's name never rebuilds it from parts.
func mqttInflightMarkerSubjectFor(reg *mqttInflightName) string {
	return reg.subject + "." + mqttInflightMarkerSuffix
}

// mqttInflightMarkerWalkFilter enumerates one incarnation's markers and only those: exactly
// one token past the generation, then the fixed suffix. The register walk
// (mqttInflightWalkFilter) stays register-only; recovery runs the register enumeration
// FIRST and this one second, and never the other way round (the two-walk case is what
// makes the order matter — see mqttDecideMarker's resume rows).
func mqttInflightMarkerWalkFilter(idHash, gen string) string {
	return mqttInflightWalkFilter(idHash, gen) + "." + mqttInflightMarkerSuffix
}

// mqttParseInflightMarkerSubject parses a marker subject as strictly as
// mqttParseInflightSubject parses a register's, and returns the REGISTER's name: the sibling
// the marker belongs to. Strict for the same reason: a marker that cannot be attributed to a
// specific (idHash, generation, identifier) is retained rather than swept.
func mqttParseInflightMarkerSubject(subject string) (*mqttInflightName, error) {
	dot := strings.LastIndexByte(subject, '.')
	if dot < 0 || subject[dot+1:] != mqttInflightMarkerSuffix {
		return nil, fmt.Errorf("subject does not end in the unsent-marker token")
	}
	reg, err := mqttParseInflightSubject(subject[:dot])
	if err != nil {
		return nil, fmt.Errorf("unsent marker names no register: %v", err)
	}
	return reg, nil
}

// mqttBuildInflightMarker is the ONE way a marker body is written, and it refuses a nonce
// that mqttValidInflightNonce would refuse on the register, so the pair can never be created
// with a marker that names nothing.
func mqttBuildInflightMarker(nonce string) ([]byte, error) {
	if !mqttValidInflightNonce(nonce) {
		return nil, mqttShapeErr("an unsent marker must name a well-formed register nonce")
	}
	return json.Marshal(&mqttInflightMarker{Nonce: nonce})
}

// mqttDecodeInflightMarker is the ONE way marker bytes become a marker, mirroring
// mqttDecodeInflightRecord: the member names are walked exact-case and duplicate-rejecting
// before the typed decode, json.Unmarshal refuses trailing content, and the nonce must be
// well-formed. An error means UNATTRIBUTED: the marker exists but licenses nothing that
// depends on which register it names. TestMQTTMarkerDecodeIsAClosedSchema pins each refusal.
func mqttDecodeInflightMarker(data []byte) (*mqttInflightMarker, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttInflightMarkerJSONKeys(), "marker"); err != nil {
		return nil, err
	}
	m := &mqttInflightMarker{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	if !mqttValidInflightNonce(m.Nonce) {
		return nil, mqttShapeErr("unsent marker names no well-formed register nonce")
	}
	return m, nil
}

// mqttInflightMarkerJSONKeys is the marker's own member-name set, from its tags.
var mqttInflightMarkerJSONKeys = sync.OnceValue(func() map[string]struct{} {
	return mqttJSONKeysOfType(reflect.TypeOf(mqttInflightMarker{}))
})

// ---------------------------------------------------------------------------
// Observations
// ---------------------------------------------------------------------------

// mqttMarkerProvenance is HOW a marker observation's sequence was acquired. It decides
// whether a delete at that sequence is safe, independently of what the read found.
type mqttMarkerProvenance uint8

const (
	// mqttMarkerProvenanceUnset is the zero value and never a legal observation.
	mqttMarkerProvenanceUnset mqttMarkerProvenance = iota //nolint:unused // the zero value, rejected by wellFormed
	// mqttMarkerCaptured: the sequence was taken by this operation BEFORE the sibling
	// register observation it is paired with (the store path's post-commit load, recovery's
	// install, the sweep's read-before-revalidate), or is the sequence the entry has carried
	// since one of those. A delete at a captured sequence can tear only the record that was
	// observed, never one a rival created afterwards.
	mqttMarkerCaptured
	// mqttMarkerReloaded: the sequence was obtained by loading the marker subject AFTER the
	// sibling observation, or with no sibling observation at all. A record found this way may
	// belong to a pair that replaced the one the caller is reasoning about.
	mqttMarkerReloaded
	// mqttMarkerNoneHeld: the ENTRY carries no marker sequence and is transmitted (the shape
	// the resume's row 5 and the replacement's row 16 install). A property of memory, not an
	// observation of the stream, and never an absence claim: the stream may still hold a
	// marker this entry knows nothing about, which is why cleanup of such an entry reloads.
	mqttMarkerNoneHeld
)

// mqttMarkerOutcome is WHAT a read of the marker subject resolved to.
type mqttMarkerOutcome uint8

const (
	// mqttMarkerUnread: no read was performed. Legal only with mqttMarkerNoneHeld.
	mqttMarkerUnread mqttMarkerOutcome = iota
	// mqttMarkerMatch: present, the body decodes (mqttDecodeInflightMarker), and its nonce
	// names the register in question.
	mqttMarkerMatch
	// mqttMarkerUnattributed: present, but the body does not name that register — it does
	// not decode, or its nonce names another exchange.
	mqttMarkerUnattributed
	// mqttMarkerAbsent: a load of the subject POSITIVELY returned no message. Never inferred
	// from a delete's success, from a cached snapshot, or from mqttMarkerNoneHeld.
	mqttMarkerAbsent
	// mqttMarkerIndeterminate: the load did not resolve — timeout, transport error, an
	// ambiguous response. Distinct from absent; no row folds them.
	mqttMarkerIndeterminate
)

// mqttMarkerObservation is one observation of the marker, on both axes.
type mqttMarkerObservation struct {
	prov    mqttMarkerProvenance
	outcome mqttMarkerOutcome
	// seq is the marker's exact stream sequence when the outcome found one present. It is
	// the ONLY sequence a DeleteMarker disposition may address.
	seq uint64
}

// mqttRegisterKind is what the register observation paired with the marker resolved to.
// Which kinds an operation accepts is decided by the table, not by the caller.
type mqttRegisterKind uint8

const (
	// mqttRegisterUnobserved: the register is not a condition of this operation ("record a
	// transmission", "clean after protocol evidence").
	mqttRegisterUnobserved mqttRegisterKind = iota
	// mqttRegisterOurs: the register our own batch acknowledgement names (the store path),
	// or the same-delivery occupant the replacement arm read at its observed sequence.
	mqttRegisterOurs
	// mqttRegisterSnapshot: the recovery walk's readable live PUBLISH-phase body, NOT yet
	// reread. Sufficient while the marker is present; an absent marker is never interpreted
	// against it (the reread replaces it).
	mqttRegisterSnapshot
	// mqttRegisterSameExchange: a body that is readable — decodes, names its own subject
	// (mqttInflightRecordMatchesSubject), and only then matchesRecord — for the exchange in
	// question.
	mqttRegisterSameExchange
	// mqttRegisterDifferentExchange: readable, and matchesRecord false.
	mqttRegisterDifferentExchange
	// mqttRegisterUnreadable: present, and not readable. A body that decodes but names
	// another packet identifier is this, not a different exchange.
	mqttRegisterUnreadable
	// mqttRegisterAbsent: a load POSITIVELY returned no message.
	mqttRegisterAbsent
	// mqttRegisterIndeterminate: the load did not resolve.
	mqttRegisterIndeterminate
	// mqttRegisterIneligible: an opaque, poisoned or regForeign entry, or a duplicate-source
	// member. Derives no send state from any marker.
	mqttRegisterIneligible
)

// mqttRegisterObservation is the register half of the pair as this operation observed it.
type mqttRegisterObservation struct {
	kind mqttRegisterKind
	// seq is the sequence a present body was read at.
	seq uint64
	// snapshotSeq, on a resume reread only, is the sequence the walk observed the register
	// at. A same-exchange body at the same sequence is the register the walk saw, transmitted;
	// at a new sequence it is a rewrite the walk never saw, and is classified afresh.
	snapshotSeq uint64
}

// ---------------------------------------------------------------------------
// Operations and dispositions
// ---------------------------------------------------------------------------

// mqttMarkerOperation names what the caller intends to do with the pair. The same
// observation licenses different things under different operations — a present matching
// marker installs an unsent entry on resume, is replaced as a pair by the store path, and is
// deleted by a sender that wrote — so the operation is an input, not a caller convention.
type mqttMarkerOperation uint8

const (
	mqttMarkerOpUnset mqttMarkerOperation = iota //nolint:unused // the zero value, an unknown operation
	// mqttMarkerOpDeriveSendState: install an entry with its DUP-determining state — the
	// store path after the batch commits, or recovery's resume.
	mqttMarkerOpDeriveSendState
	// mqttMarkerOpReplaceOccupant: the store path found a same-delivery occupant and the
	// phase gate has been consulted.
	mqttMarkerOpReplaceOccupant
	// mqttMarkerOpRecordTransmission: the sender, at endSend, with whether it wrote.
	mqttMarkerOpRecordTransmission
	// mqttMarkerOpCleanAfterEvidence: a consumer holding client protocol evidence that the
	// PUBLISH was received (PUBACK, PUBREC, a rollback, an adopted Retired occupant, a
	// post-PUBREC drain owner, a register superseded by a PUBREL record).
	mqttMarkerOpCleanAfterEvidence
	// mqttMarkerOpReap: remove a marker that no live exchange needs — terminal cleanup, the
	// ladder's Cancelled and Complete arms, the store retry, the sweep, clear().
	mqttMarkerOpReap
)

// The callers, for the disposition and delete hooks: a test attributes a delete to the
// operation that issued it, because "no delete anywhere" is the wrong oracle when a
// legitimate cleanup runs beside the operation under test.
const (
	mqttMarkerCallerStore    = "store"
	mqttMarkerCallerResume   = "resume"
	mqttMarkerCallerReplace  = "replace"
	mqttMarkerCallerSender   = "sender"
	mqttMarkerCallerEvidence = "evidence"
	mqttMarkerCallerCleanup  = "cleanup"
	mqttMarkerCallerSweep    = "sweep"
	mqttMarkerCallerClear    = "clear"
)

// mqttMarkerContext is the operation and the facts about it the table keys on.
type mqttMarkerContext struct {
	op mqttMarkerOperation
	// caller tags the disposition for the test hook; it is never a condition.
	caller string
	// resume: for DeriveSendState, recovery's resume rather than the store path.
	resume bool
	// gatePassed: for ReplaceOccupant, whether mqttSlotReplacePublish admitted the occupant.
	gatePassed bool
	// wrote: for RecordTransmission, whether the PUBLISH was possibly written (true) or
	// refused before any write (false).
	wrote bool
}

// mqttMarkerDisposition is what the boundary returns; never a boolean.
type mqttMarkerDisposition uint8

const (
	mqttMarkerDispositionUnset mqttMarkerDisposition = iota
	// mqttMarkerInstallUnsent: install with transmitted=false and the marker's sequence; a
	// first transmission may follow.
	mqttMarkerInstallUnsent
	// mqttMarkerInstallTransmitted: install with transmitted=true and no marker sequence
	// (none held); any send is a retransmission, DUP=1.
	mqttMarkerInstallTransmitted
	// mqttMarkerReclassify: do not install this classification; re-enter classification
	// from the authoritative observation the row supplies — a body, under the ownership and
	// phase checks the claim path applies, or a proven absence, which releases the obsolete
	// reservation and installs nothing.
	mqttMarkerReclassify
	// mqttMarkerWithhold: the identifier is reserved with no send authority and no source
	// mapping, and piSpaceIncomplete is set — the marker-opaque reservation.
	mqttMarkerWithhold
	// mqttMarkerReplacePair: one atomic batch — register CAS at its observed sequence and
	// marker CAS at its observed sequence, both carrying OUR nonce.
	mqttMarkerReplacePair
	// mqttMarkerReplaceRegisterOnly: a single register CAS at its observed sequence carrying
	// our nonce; no marker written.
	mqttMarkerReplaceRegisterOnly
	// mqttMarkerDeleteMarker: delete the marker at the sequence the observation carries —
	// captured, or reload-acquired where the row admits it.
	mqttMarkerDeleteMarker
	// mqttMarkerPreserveMarker: this operation owes no delete, and the operation is
	// COMPLETE — a success, not a withholding. Returned for a present marker that must be
	// kept, for a marker proven absent, and for a sender that wrote nothing or holds none.
	mqttMarkerPreserveMarker
	// mqttMarkerRefuse: the operation may not proceed on this observation and writes
	// nothing; the caller's lifecycle decides what follows.
	mqttMarkerRefuse
)

func (d mqttMarkerDisposition) String() string {
	switch d {
	case mqttMarkerInstallUnsent:
		return "InstallUnsent"
	case mqttMarkerInstallTransmitted:
		return "InstallTransmitted"
	case mqttMarkerReclassify:
		return "Reclassify"
	case mqttMarkerWithhold:
		return "Withhold"
	case mqttMarkerReplacePair:
		return "ReplacePair"
	case mqttMarkerReplaceRegisterOnly:
		return "ReplaceRegisterOnly"
	case mqttMarkerDeleteMarker:
		return "DeleteMarker"
	case mqttMarkerPreserveMarker:
		return "PreserveMarker"
	case mqttMarkerRefuse:
		return "Refuse"
	}
	return "unset"
}

// mqttMarkerVerdict is a disposition and the table row that produced it. The row is for
// diagnostics and tests; consumers act on the disposition alone.
type mqttMarkerVerdict struct {
	disposition mqttMarkerDisposition
	row         string
}

// errMarkerObservationIll is the class of every rejection below: the observation is not an
// input the operation has a row for. A programming error at the caller, never a stream
// state — which is why it is an error and not a Refuse, and why a caller never turns it into
// one.
var errMarkerObservationIll = errors.New("marker observation is not an input of this operation")

func mqttMarkerIll(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errMarkerObservationIll, fmt.Sprintf(format, args...))
}

// mqttDecideMarker is THE marker-observation boundary: the decision table, keyed by
// operation. Its counterpart on the register side is mqttValidateInflightShape plus
// mqttSlotAdmitsDisposition. Row numbers follow the F-072 contract (docs/ARCHITECTURE.md,
// "Outbound in-flight registers", the unsent marker).
//
// Pure: no I/O, no lock, no state. The test hook fires on the way out with the verdict and
// the caller tag, so a behavioural test can assert which operation was told what.
func mqttDecideMarker(ctx mqttMarkerContext, reg mqttRegisterObservation, mk mqttMarkerObservation) (mqttMarkerVerdict, error) {
	v, err := mqttDecideMarkerRows(ctx, reg, mk)
	if err == nil {
		if h := mqttTestMarkerDispositionHookV.Load(); h != nil {
			(*h)(ctx.caller, v)
		}
	}
	return v, err
}

func mqttDecideMarkerRows(ctx mqttMarkerContext, reg mqttRegisterObservation, mk mqttMarkerObservation) (mqttMarkerVerdict, error) {
	verdict := func(d mqttMarkerDisposition, row string) (mqttMarkerVerdict, error) {
		return mqttMarkerVerdict{disposition: d, row: row}, nil
	}
	none := mqttMarkerVerdict{}

	// The two rows that read NO marker come first, so a caller that has none to offer for
	// an ineligible register or a refused gate is not made to fabricate one.
	switch ctx.op {
	case mqttMarkerOpDeriveSendState:
		if ctx.resume && reg.kind == mqttRegisterIneligible {
			return verdict(mqttMarkerWithhold, "14")
		}
	case mqttMarkerOpReplaceOccupant:
		if !ctx.gatePassed {
			// Row 18 precedes row 17: a refused gate refuses whatever the marker says.
			return verdict(mqttMarkerRefuse, "18")
		}
	case mqttMarkerOpRecordTransmission, mqttMarkerOpCleanAfterEvidence, mqttMarkerOpReap:
	default:
		return none, mqttMarkerIll("unknown operation %d", ctx.op)
	}

	if err := mk.wellFormed(); err != nil {
		return none, err
	}

	switch ctx.op {
	case mqttMarkerOpDeriveSendState:
		if mk.prov == mqttMarkerNoneHeld {
			return none, mqttMarkerIll("derive send state needs an observation of the stream, not an entry property")
		}
		if !ctx.resume {
			// The store path: the register is ours by the acknowledgement that named it.
			if reg.kind != mqttRegisterOurs {
				return none, mqttMarkerIll("the store path's register is the one its acknowledgement named")
			}
			switch mk.outcome {
			case mqttMarkerMatch:
				return verdict(mqttMarkerInstallUnsent, "1")
			case mqttMarkerAbsent:
				return verdict(mqttMarkerWithhold, "2")
			default: // unattributed, indeterminate
				return verdict(mqttMarkerWithhold, "3")
			}
		}
		// Resume. A present marker is classified against the walk's snapshot; an absent one
		// is NEVER interpreted against it — the caller rereads the register's own subject and
		// hands the reread here as the register observation (rows 5-9).
		switch reg.kind {
		case mqttRegisterSnapshot:
			switch mk.outcome {
			case mqttMarkerMatch:
				return verdict(mqttMarkerInstallUnsent, "4")
			case mqttMarkerUnattributed, mqttMarkerIndeterminate:
				return verdict(mqttMarkerWithhold, "4a")
			default:
				return none, mqttMarkerIll("an absent marker is not interpreted against the walk snapshot; reread the register")
			}
		case mqttRegisterSameExchange, mqttRegisterDifferentExchange, mqttRegisterUnreadable,
			mqttRegisterAbsent, mqttRegisterIndeterminate:
			if mk.outcome != mqttMarkerAbsent {
				return none, mqttMarkerIll("a register reread is entered only on an absent marker")
			}
			switch reg.kind {
			case mqttRegisterSameExchange:
				if reg.seq == 0 || reg.snapshotSeq == 0 {
					return none, mqttMarkerIll("a reread same-exchange body carries its sequence and the walk's")
				}
				if reg.seq == reg.snapshotSeq {
					return verdict(mqttMarkerInstallTransmitted, "5")
				}
				return verdict(mqttMarkerReclassify, "6")
			case mqttRegisterAbsent:
				return verdict(mqttMarkerReclassify, "7")
			case mqttRegisterDifferentExchange:
				return verdict(mqttMarkerReclassify, "8")
			default: // unreadable, indeterminate
				return verdict(mqttMarkerWithhold, "9")
			}
		default:
			return none, mqttMarkerIll("resume classifies the walk snapshot or its reread, got register kind %d", reg.kind)
		}

	case mqttMarkerOpReplaceOccupant:
		if reg.kind != mqttRegisterOurs {
			return none, mqttMarkerIll("replacement observes the same-delivery occupant at its sequence")
		}
		if mk.prov == mqttMarkerNoneHeld {
			return none, mqttMarkerIll("replacement reads the occupant's marker; an entry property is no observation")
		}
		switch mk.outcome {
		case mqttMarkerMatch:
			return verdict(mqttMarkerReplacePair, "15")
		case mqttMarkerAbsent:
			return verdict(mqttMarkerReplaceRegisterOnly, "16")
		default: // unattributed, indeterminate
			return verdict(mqttMarkerWithhold, "17")
		}

	case mqttMarkerOpRecordTransmission:
		// The sender's input space is exactly {captured, none held} × wrote. It performs no
		// marker read, so an absence or an indeterminate load is not an input here — it is a
		// caller that read when it should not have.
		if reg.kind != mqttRegisterUnobserved {
			return none, mqttMarkerIll("the register is not an input of record a transmission")
		}
		switch mk.prov {
		case mqttMarkerCaptured:
			if ctx.wrote {
				return verdict(mqttMarkerDeleteMarker, "19")
			}
			return verdict(mqttMarkerPreserveMarker, "20")
		case mqttMarkerNoneHeld:
			if ctx.wrote {
				return verdict(mqttMarkerPreserveMarker, "19a")
			}
			return verdict(mqttMarkerPreserveMarker, "20a")
		default:
			return none, mqttMarkerIll("record a transmission takes a captured or none-held marker, not a %s", mk.prov)
		}

	case mqttMarkerOpCleanAfterEvidence:
		if reg.kind != mqttRegisterUnobserved {
			return none, mqttMarkerIll("the register is not a condition of clean after protocol evidence")
		}
		switch mk.prov {
		case mqttMarkerCaptured:
			return verdict(mqttMarkerDeleteMarker, "21")
		case mqttMarkerReloaded:
			switch mk.outcome {
			case mqttMarkerMatch:
				// Reload-acquired, and its nonce names the exchange the evidence names.
				return verdict(mqttMarkerDeleteMarker, "22")
			case mqttMarkerAbsent:
				return verdict(mqttMarkerPreserveMarker, "22a")
			default: // unattributed, indeterminate
				return verdict(mqttMarkerRefuse, "23")
			}
		default:
			// A none-held entry's cleanup reloads and enters "reap"; it has no row here.
			return none, mqttMarkerIll("clean after protocol evidence has no row for a none-held entry; reload and reap")
		}

	case mqttMarkerOpReap:
		if mk.prov == mqttMarkerNoneHeld {
			// None held alone never completes a cleanup; the reload is mandatory.
			return none, mqttMarkerIll("reap needs a reload; none held is not an absence")
		}
		if mk.outcome == mqttMarkerAbsent {
			// Nothing to delete, whatever the acquisition and whatever the sibling says. Exempt
			// from row 29 because no delete follows, so acquisition order can tear nothing.
			return verdict(mqttMarkerPreserveMarker, "24a")
		}
		if mk.prov == mqttMarkerReloaded {
			// Present or indeterminate at a sequence acquired after the sibling was read: a
			// delete here could tear a pair a rival committed in between.
			return verdict(mqttMarkerRefuse, "29")
		}
		// Captured before the sibling read, present (match or unattributed).
		switch reg.kind {
		case mqttRegisterAbsent:
			return verdict(mqttMarkerDeleteMarker, "24")
		case mqttRegisterDifferentExchange:
			if mk.outcome == mqttMarkerMatch {
				return verdict(mqttMarkerDeleteMarker, "25")
			}
			return verdict(mqttMarkerPreserveMarker, "26a")
		case mqttRegisterSameExchange:
			if mk.outcome == mqttMarkerMatch {
				return verdict(mqttMarkerPreserveMarker, "26")
			}
			return verdict(mqttMarkerPreserveMarker, "26a")
		case mqttRegisterUnreadable:
			return verdict(mqttMarkerPreserveMarker, "27")
		case mqttRegisterIndeterminate:
			return verdict(mqttMarkerPreserveMarker, "28")
		default:
			return none, mqttMarkerIll("reap revalidates the sibling by an authoritative read, got register kind %d", reg.kind)
		}
	}
	return none, mqttMarkerIll("unknown operation %d", ctx.op)
}

// wellFormed rejects an observation whose two axes contradict each other. A captured
// sequence is of a marker that was PRESENT; none held performs no read; a present outcome
// names a sequence.
func (mk mqttMarkerObservation) wellFormed() error {
	switch mk.prov {
	case mqttMarkerNoneHeld:
		if mk.outcome != mqttMarkerUnread || mk.seq != 0 {
			return mqttMarkerIll("none held is an entry property and carries no read outcome")
		}
		return nil
	case mqttMarkerCaptured:
		if mk.outcome != mqttMarkerMatch && mk.outcome != mqttMarkerUnattributed {
			return mqttMarkerIll("a captured marker was present at capture; outcome %s is not a capture", mk.outcome)
		}
	case mqttMarkerReloaded:
		if mk.outcome == mqttMarkerUnread {
			return mqttMarkerIll("a reload has an outcome")
		}
	default:
		return mqttMarkerIll("marker observation has no provenance")
	}
	present := mk.outcome == mqttMarkerMatch || mk.outcome == mqttMarkerUnattributed
	if present && mk.seq == 0 {
		return mqttMarkerIll("a present marker has a stream sequence")
	}
	if !present && mk.seq != 0 {
		return mqttMarkerIll("an absent or indeterminate marker carries no sequence")
	}
	return nil
}

func (p mqttMarkerProvenance) String() string {
	switch p {
	case mqttMarkerCaptured:
		return "captured"
	case mqttMarkerReloaded:
		return "reloaded"
	case mqttMarkerNoneHeld:
		return "none held"
	}
	return "unset"
}

func (o mqttMarkerOutcome) String() string {
	switch o {
	case mqttMarkerMatch:
		return "match"
	case mqttMarkerUnattributed:
		return "unattributed"
	case mqttMarkerAbsent:
		return "absent"
	case mqttMarkerIndeterminate:
		return "indeterminate"
	}
	return "unread"
}

// mqttTestMarkerDispositionHookV records what the boundary returned to which caller.
var mqttTestMarkerDispositionHookV atomic.Pointer[func(caller string, v mqttMarkerVerdict)]

func setMQTTTestMarkerDispositionHook(h func(caller string, v mqttMarkerVerdict)) func() {
	prev := mqttTestMarkerDispositionHookV.Load()
	if h == nil {
		mqttTestMarkerDispositionHookV.Store(nil)
	} else {
		mqttTestMarkerDispositionHookV.Store(&h)
	}
	return func() { mqttTestMarkerDispositionHookV.Store(prev) }
}

// ---------------------------------------------------------------------------
// The pair on the stream: store, load, delete
// ---------------------------------------------------------------------------

// mqttInflightPairBatchSize is how many messages the PUBLISH-phase pair commits: the
// marker, staged first, and the register, whose commit the acknowledgement names.
const mqttInflightPairBatchSize = 2

// storeInflightPair writes the register and its unsent marker as ONE atomic batch and
// returns the REGISTER's sequence. regExpect and markerExpect are each subject's own
// Nats-Expected-Last-Subject-Sequence (0 = create-only); a failing CAS on either rejects
// the whole batch and nothing is stored. The binding between the two is atomicity, never a
// cross-subject CAS (the accept batch's reasoning, mqttFrameBatchWrite).
//
// The marker is member 1 and the register member 2 so the PubAck names the register and
// every existing use of that acknowledgement is untouched. The marker's sequence is NOT
// derived from it: the caller loads the marker authoritatively afterwards.
//
// The acknowledgement is validated as storeAcceptBatch validates its own — non-nil PubAck,
// this batch's id, both members, this stream — and an unusable one is an error, which the
// caller's ladder treats as ambiguous and resolves by rereading both subjects.
func (jsa *mqttJSA) storeInflightPair(subject string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
	// THE WRITE BOUNDARY VALIDATES, exactly as storeInflightCAS does and for the same
	// reason: every writer funnels through here, so this is the only place the claim
	// "no unsupported shape is ever written" can be true.
	if verr := mqttValidateInflightShape(r); verr != nil {
		return 0, fmt.Errorf("refusing to store an unsupported in-flight shape on %q: %w", subject, verr)
	}
	markerBody, merr := mqttBuildInflightMarker(r.Nonce)
	if merr != nil {
		return 0, fmt.Errorf("refusing to store an in-flight pair on %q: %w", subject, merr)
	}
	regBody, _ := json.Marshal(r)
	jsa.mu.Lock()
	batchID := jsa.nuid.Next()
	jsa.mu.Unlock()
	markerSubject := subject + "." + mqttInflightMarkerSuffix
	mmsg, mhdr := mqttFrameBatchWrite(markerExpect, markerBody, batchID, 1, false)
	rmsg, rhdr := mqttFrameBatchWrite(regExpect, regBody, batchID, 2, true)
	bi, err := jsa.newAtomicBatchRequest(mqttJSAMsgStore, _EMPTY_, []mqttBatchMsg{
		{subj: markerSubject, hdr: mhdr, msg: mmsg},
		{subj: subject, hdr: rhdr, msg: rmsg},
	})
	if err != nil {
		return 0, err
	}
	resp := bi.(*JSPubAckResponse)
	if aerr := resp.ToError(); aerr != nil {
		return 0, aerr
	}
	if resp.PubAck == nil {
		return 0, fmt.Errorf("the in-flight pair on %q was acknowledged with no publish acknowledgement", subject)
	}
	if resp.BatchId != batchID || resp.BatchSize != mqttInflightPairBatchSize {
		return 0, fmt.Errorf("the in-flight pair on %q was acknowledged as %d message(s) under batch %q, but this broker sent %d under %q",
			subject, resp.BatchSize, resp.BatchId, mqttInflightPairBatchSize, batchID)
	}
	if resp.Stream != mqttInflightStreamName {
		return 0, fmt.Errorf("the in-flight pair on %q was acknowledged on stream %q (want %q)", subject, resp.Stream, mqttInflightStreamName)
	}
	return resp.Sequence, nil
}

// storeInflightPair honours the instance-scoped test seams. testInflightStorePair replaces
// the pair store outright. testInflightStore — the seam ~90 fixtures already drive — models
// "the register's write" and keeps that meaning here: it decides the pair's outcome, and a
// success it reports leaves a GHOST marker behind (testMarkerGhosts) that loadInflightMarker
// reads back as present and ours, so a fixture that never touched the stream still
// observes the pair its hooked write claims to have landed. Cleared by the marker's delete,
// which is what makes a hooked exchange read as transmitted afterwards.
func (as *mqttAccountSessionManager) storeInflightPair(subject string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
	as.mu.RLock()
	pair, single := as.testInflightStorePair, as.testInflightStore
	as.mu.RUnlock()
	switch {
	case pair != nil:
		return pair(subject, regExpect, markerExpect, r)
	case single != nil:
		// Recorded on every hooked ATTEMPT with the hook's verdict: a fixture that models
		// "landed, response lost" writes the register for real and returns an error, and
		// the marker shares the register's fate in an atomic batch — so loadInflightMarker
		// honours the ghost when the hook reported success, or when the real register on
		// the subject carries the ghost's nonce (the hook's write landed); an attempt that
		// neither succeeded nor landed left no marker.
		seq, err := single(subject, regExpect, r)
		as.mu.Lock()
		if as.testMarkerGhosts == nil {
			as.testMarkerGhosts = make(map[string]mqttTestMarkerGhost)
		}
		as.testMarkerGhosts[subject] = mqttTestMarkerGhost{nonce: r.Nonce, reported: err == nil}
		as.mu.Unlock()
		return seq, err
	}
	return as.jsa.storeInflightPair(subject, regExpect, markerExpect, r)
}

// mqttTestGhostMarkerSeq is the sequence a ghost marker reports. Far above any real stream
// sequence a test reaches, so a fire-and-forget delete at it removes nothing real.
const mqttTestGhostMarkerSeq = uint64(1) << 62

// mqttTestMarkerGhost is the marker a hooked register store claims beside one subject.
type mqttTestMarkerGhost struct {
	nonce    string
	reported bool // the hook reported success
}

// mqttMarkerLoad is one authoritative read of a marker subject, before it is interpreted
// against any register. The three response kinds are kept apart here for the reason
// mqttClassifyMsgGetResponse keeps them apart: an indeterminate read is never an absence.
type mqttMarkerLoad struct {
	kind mqttInflightResponseKind
	seq  uint64
	// marker is the decoded body, nil when the read found none or the body is not one this
	// broker writes (decodeErr says why).
	marker    *mqttInflightMarker
	decodeErr error
}

// loadInflightMarker reads the marker on the register subject's sibling, honouring the
// instance-scoped test seam and reporting the read to the caller-tagged reload hook. The
// hook fires BEFORE the seam so a fixture can both observe that a reload happened and
// decide what it returns.
func (as *mqttAccountSessionManager) loadInflightMarker(caller, regSubject string) mqttMarkerLoad {
	subject := regSubject + "." + mqttInflightMarkerSuffix
	if h := mqttTestMarkerReloadHookV.Load(); h != nil {
		(*h)(caller, subject)
	}
	as.mu.RLock()
	hook := as.testInflightMarkerLoad
	ghost, ghosted := as.testMarkerGhosts[regSubject]
	// The ghost is part of the store hook's model and lives only while one is installed;
	// once the fixture hands the stream back, the stream is authoritative.
	ghosted = ghosted && (as.testInflightStore != nil || as.testInflightStorePair != nil)
	as.mu.RUnlock()
	if ghosted && !ghost.reported {
		// The hook reported failure: the ghost stands only if its write nonetheless
		// landed, which the real register on the subject tells.
		cur, _, lerr := as.loadInflightRecord(regSubject)
		ghosted = lerr == nil && cur != nil && cur.Nonce == ghost.nonce
	}
	var smsg *StoredMsg
	var err error
	switch {
	case hook != nil:
		smsg, err = hook(subject)
	case ghosted:
		body, _ := mqttBuildInflightMarker(ghost.nonce)
		smsg = &StoredMsg{Subject: subject, Sequence: mqttTestGhostMarkerSeq, Data: body}
	default:
		smsg, err = as.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	}
	switch {
	case err != nil && isErrorOtherThan(err, JSNoMessageFoundErr):
		return mqttMarkerLoad{kind: mqttInflightResponseIndeterminate}
	case err != nil || smsg == nil:
		return mqttMarkerLoad{kind: mqttInflightResponseAbsent}
	}
	m, derr := mqttDecodeInflightMarker(smsg.Data)
	return mqttMarkerLoad{kind: mqttInflightResponsePresent, seq: smsg.Sequence, marker: m, decodeErr: derr}
}

// mqttMarkerLoadFromResponse converts one batched read into a load, for the walks.
func mqttMarkerLoadFromResponse(resp *JSApiMsgGetResponse) mqttMarkerLoad {
	switch mqttClassifyMsgGetResponse(resp) {
	case mqttInflightResponseAbsent:
		return mqttMarkerLoad{kind: mqttInflightResponseAbsent}
	case mqttInflightResponsePresent:
		m, derr := mqttDecodeInflightMarker(resp.Message.Data)
		return mqttMarkerLoad{kind: mqttInflightResponsePresent, seq: resp.Message.Sequence, marker: m, decodeErr: derr}
	}
	return mqttMarkerLoad{kind: mqttInflightResponseIndeterminate}
}

// observe types a load as an observation for the register whose nonce is `nonce`, under
// the provenance the caller's acquisition order earned. An empty nonce asks only whether
// the body decodes, which is what a reap wants: it holds no exchange of its own and
// judges the sibling against the marker's body instead (nonceOf).
func (l mqttMarkerLoad) observe(prov mqttMarkerProvenance, nonce string) mqttMarkerObservation {
	switch l.kind {
	case mqttInflightResponseAbsent:
		return mqttMarkerObservation{prov: prov, outcome: mqttMarkerAbsent}
	case mqttInflightResponseIndeterminate:
		return mqttMarkerObservation{prov: prov, outcome: mqttMarkerIndeterminate}
	}
	if l.marker == nil || (nonce != _EMPTY_ && l.marker.Nonce != nonce) {
		return mqttMarkerObservation{prov: prov, outcome: mqttMarkerUnattributed, seq: l.seq}
	}
	return mqttMarkerObservation{prov: prov, outcome: mqttMarkerMatch, seq: l.seq}
}

// nonceOf is the exchange the marker's body names, empty when it names none.
func (l mqttMarkerLoad) nonceOf() string {
	if l.marker == nil {
		return _EMPTY_
	}
	return l.marker.Nonce
}

// deleteInflightMarker removes a marker at its EXACT sequence. Fire-and-forget by default,
// like the ordinary register delete: nothing waits on a marker delete (N-8.4), a failed one
// is retried by the next transmission or terminal pass and reaped by the sweep. confirm
// waits and retries, for the passes whose progress depends on the marker being gone (the
// store retry, the generation teardown).
//
// The delete hook records the CALLER with the call, so a test can attribute a delete to
// the operation that issued it; a non-nil error from the hook suppresses the delete, the
// same way testInflightDelete does for registers.
func (as *mqttAccountSessionManager) deleteInflightMarker(caller, regSubject string, seq uint64, confirm bool) error {
	if seq == 0 {
		return fmt.Errorf("a marker delete needs the marker's exact sequence")
	}
	subject := regSubject + "." + mqttInflightMarkerSuffix
	if h := mqttTestMarkerDeleteHookV.Load(); h != nil {
		if err := (*h)(caller, subject, seq); err != nil {
			return err
		}
	}
	if seq == mqttTestGhostMarkerSeq {
		as.mu.Lock()
		delete(as.testMarkerGhosts, regSubject)
		as.mu.Unlock()
		return nil
	}
	if !confirm {
		return as.jsa.deleteMsg(mqttInflightStreamName, seq, false)
	}
	var err error
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		err = as.jsa.deleteMsg(mqttInflightStreamName, seq, true)
		if err == nil {
			return nil
		}
		// Already gone is a success, in both shapes the server reports it
		// (deleteInflightRegisterConfirmed).
		if !isErrorOtherThan(err, JSSequenceNotFoundErrF) ||
			isJSErrCausedBy(err, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
			return nil
		}
	}
	return err
}

// ---------------------------------------------------------------------------
// The three delete-owning operations, as the consumers call them
// ---------------------------------------------------------------------------

// siblingObservation types an authoritative read of a register as the sibling of a marker
// whose body names `markerNonce`. "Same exchange" for a reap is the marker's own exchange:
// a readable sibling carrying the marker's nonce is the pair the marker belongs to.
func (as *mqttAccountSessionManager) siblingObservation(regSubject, markerNonce string) mqttRegisterObservation {
	cur, smsg, lerr := as.loadInflightRecord(regSubject)
	switch {
	case lerr != nil:
		return mqttRegisterObservation{kind: mqttRegisterIndeterminate}
	case smsg == nil:
		return mqttRegisterObservation{kind: mqttRegisterAbsent}
	case cur == nil || !mqttInflightRecordMatchesSubject(cur, regSubject):
		return mqttRegisterObservation{kind: mqttRegisterUnreadable, seq: smsg.Sequence}
	case markerNonce != _EMPTY_ && cur.Nonce == markerNonce:
		return mqttRegisterObservation{kind: mqttRegisterSameExchange, seq: smsg.Sequence}
	}
	return mqttRegisterObservation{kind: mqttRegisterDifferentExchange, seq: smsg.Sequence}
}

// reapMarkerCaptureFirst is operation "reap" for a caller holding NO marker sequence of its
// own: the store retry, the ladder's Cancelled and Complete arms, the sweep's marker arm and
// the generation teardown. It CAPTURES the marker — sequence and body — and only then reads
// the sibling, so the delete it may issue addresses exactly the record it observed and can
// tear nothing a rival committed after the sibling read (rows 24-28; row 24a when nothing
// is there). A capture that does not resolve completes nothing: the marker is left to the
// next pass.
func (as *mqttAccountSessionManager) reapMarkerCaptureFirst(c *client, caller, regSubject string, confirm bool) mqttMarkerDisposition {
	load := as.loadInflightMarker(caller, regSubject)
	if load.kind == mqttInflightResponseIndeterminate {
		mqttWarnf(c, "MQTT: the unsent marker beside %q could not be read, so nothing is reaped", regSubject)
		return mqttMarkerRefuse
	}
	var sibling mqttRegisterObservation
	mk := load.observe(mqttMarkerCaptured, _EMPTY_)
	if load.kind == mqttInflightResponseAbsent {
		// Row 24a: the sibling is not a condition when there is nothing to delete.
		mk = load.observe(mqttMarkerReloaded, _EMPTY_)
		sibling = mqttRegisterObservation{kind: mqttRegisterUnobserved}
	} else {
		sibling = as.siblingObservation(regSubject, load.nonceOf())
	}
	return as.executeReap(c, caller, regSubject, sibling, mk, confirm)
}

// reapMarkerCaptured is operation "reap" for terminal cleanup of an entry that CARRIES its
// marker sequence, captured at the store path's step 3 or at recovery's install. The
// register half has already been removed by the caller; the sibling is read afresh so a
// rewrite that moved the exchange (the freshness fence preserving it, or a peer's CAS) is
// seen as a present same-exchange sibling and the marker is kept (row 26), never inferred
// from the register delete's success result (N-5.7).
func (as *mqttAccountSessionManager) reapMarkerCaptured(c *client, caller, regSubject, nonce string, markerSeq uint64) mqttMarkerDisposition {
	mk := mqttMarkerObservation{prov: mqttMarkerCaptured, outcome: mqttMarkerMatch, seq: markerSeq}
	sibling := as.siblingObservation(regSubject, nonce)
	return as.executeReap(c, caller, regSubject, sibling, mk, false)
}

// reapMarkerNoneHeld is operation "reap" for terminal cleanup of an entry that holds NO
// marker sequence and is transmitted (N-5.9). The entry's state is not an observation of
// the stream, so the marker subject is RELOADED and the boundary decides: authoritatively
// absent completes the cleanup (row 24a); present or indeterminate refuses (row 29) — the
// sequence was acquired after the sibling was removed, so a delete at it could tear a pair
// a rival committed in between — and the marker is left to the sweep. None held alone
// never completes a cleanup.
func (as *mqttAccountSessionManager) reapMarkerNoneHeld(c *client, caller, regSubject string) mqttMarkerDisposition {
	load := as.loadInflightMarker(caller, regSubject)
	mk := load.observe(mqttMarkerReloaded, _EMPTY_)
	d := as.executeReap(c, caller, regSubject, mqttRegisterObservation{kind: mqttRegisterUnobserved}, mk, false)
	if d == mqttMarkerRefuse {
		mqttWarnf(c, "MQTT: an unsent marker is present or unreadable beside %q after its exchange completed; left to the sweep", regSubject)
	}
	return d
}

// executeReap hands a reap's observations to the boundary and performs the disposition.
func (as *mqttAccountSessionManager) executeReap(c *client, caller, regSubject string,
	sibling mqttRegisterObservation, mk mqttMarkerObservation, confirm bool) mqttMarkerDisposition {
	v, err := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpReap, caller: caller}, sibling, mk)
	if err != nil {
		mqttErrorf(c, "MQTT: reap of the unsent marker beside %q: %v", regSubject, err)
		return mqttMarkerRefuse
	}
	if v.disposition == mqttMarkerDeleteMarker {
		if derr := as.deleteInflightMarker(caller, regSubject, mk.seq, confirm); derr != nil && confirm {
			mqttWarnf(c, "MQTT: deleting the unsent marker beside %q at sequence %d: %v", regSubject, mk.seq, derr)
			return mqttMarkerRefuse
		}
	}
	return v.disposition
}

// cleanMarkerAfterEvidence is operation "clean after protocol evidence" for a consumer that
// holds the exchange's CAPTURED marker sequence (row 21): PUBACK, PUBREC, the PUBREC
// rollback. The register is not a condition, and a zero sequence is not an input — a
// none-held entry's marker is met by terminal cleanup's reload (reapMarkerNoneHeld), never
// here.
func (as *mqttAccountSessionManager) cleanMarkerAfterEvidence(c *client, regSubject string, markerSeq uint64) {
	if markerSeq == 0 || regSubject == _EMPTY_ {
		return
	}
	mk := mqttMarkerObservation{prov: mqttMarkerCaptured, outcome: mqttMarkerMatch, seq: markerSeq}
	v, err := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpCleanAfterEvidence, caller: mqttMarkerCallerEvidence},
		mqttRegisterObservation{kind: mqttRegisterUnobserved}, mk)
	if err != nil {
		mqttErrorf(c, "MQTT: cleaning the unsent marker beside %q: %v", regSubject, err)
		return
	}
	if v.disposition == mqttMarkerDeleteMarker {
		_ = as.deleteInflightMarker(mqttMarkerCallerEvidence, regSubject, markerSeq, false)
	}
}

// cleanMarkerAfterEvidenceReloaded is the same operation for a consumer holding evidence but
// no captured sequence — recovery's post-PUBREC drain owners, its Retired and
// PUBREL-superseded registers, the ladder's adopted Retired occupant. The marker is
// reload-acquired, so its body's nonce is validated against the exchange the evidence names
// before the delete (row 22); a body naming another exchange, or a read that did not
// resolve, refuses (row 23) and the marker is left to the sweep.
func (as *mqttAccountSessionManager) cleanMarkerAfterEvidenceReloaded(c *client, regSubject, nonce string, load mqttMarkerLoad) {
	mk := load.observe(mqttMarkerReloaded, nonce)
	v, err := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpCleanAfterEvidence, caller: mqttMarkerCallerEvidence},
		mqttRegisterObservation{kind: mqttRegisterUnobserved}, mk)
	if err != nil {
		mqttErrorf(c, "MQTT: cleaning the unsent marker beside %q: %v", regSubject, err)
		return
	}
	if v.disposition == mqttMarkerDeleteMarker {
		_ = as.deleteInflightMarker(mqttMarkerCallerEvidence, regSubject, mk.seq, false)
	}
}

// ---------------------------------------------------------------------------
// Test hooks, all in the mqttTestVoidHandoffWindowHookV idiom
// ---------------------------------------------------------------------------

// mqttTestMarkerDeleteHookV records every marker delete REQUEST with the caller that issued
// it. Returning an error suppresses the delete.
var mqttTestMarkerDeleteHookV atomic.Pointer[func(caller, subject string, seq uint64) error]

func setMQTTTestMarkerDeleteHook(h func(caller, subject string, seq uint64) error) func() {
	prev := mqttTestMarkerDeleteHookV.Load()
	if h == nil {
		mqttTestMarkerDeleteHookV.Store(nil)
	} else {
		mqttTestMarkerDeleteHookV.Store(&h)
	}
	return func() { mqttTestMarkerDeleteHookV.Store(prev) }
}

// mqttTestMarkerReloadHookV records every marker RELOAD with the caller that performed it.
var mqttTestMarkerReloadHookV atomic.Pointer[func(caller, subject string)]

func setMQTTTestMarkerReloadHook(h func(caller, subject string)) func() {
	prev := mqttTestMarkerReloadHookV.Load()
	if h == nil {
		mqttTestMarkerReloadHookV.Store(nil)
	} else {
		mqttTestMarkerReloadHookV.Store(&h)
	}
	return func() { mqttTestMarkerReloadHookV.Store(prev) }
}

// mqttTestMarkerSendWindowHookV fires in a sender's section at RESERVED (the section is
// open, nothing written) and WROTE (the enqueue returned), so a fixture can park a
// transmission between the two.
// mqttTestMarkerSendWindowHookV fires in a sender's section at RESERVED (open, nothing
// written) and WROTE (the enqueue returned). At RESERVED, returning false ABORTS the send
// before any byte — the section ends with wrote == false, which is what a crash between
// the pair's confirmation and the socket write leaves: the pair intact, no transmission
// recorded (T2). It always returns true at WROTE.
var mqttTestMarkerSendWindowHookV atomic.Pointer[func(sess *mqttSession, pi uint16, window string) bool]

const (
	mqttMarkerSendReserved = "reserved"
	mqttMarkerSendWrote    = "wrote"
)

func setMQTTTestMarkerSendWindowHook(h func(sess *mqttSession, pi uint16, window string) bool) func() {
	prev := mqttTestMarkerSendWindowHookV.Load()
	if h == nil {
		mqttTestMarkerSendWindowHookV.Store(nil)
	} else {
		mqttTestMarkerSendWindowHookV.Store(&h)
	}
	return func() { mqttTestMarkerSendWindowHookV.Store(prev) }
}

// markerSendWindowHook reports whether the send may proceed. Only the RESERVED window can
// decline; every other window and the unarmed case proceed.
func (sess *mqttSession) markerSendWindowHook(pi uint16, window string) bool {
	if h := mqttTestMarkerSendWindowHookV.Load(); h != nil {
		return (*h)(sess, pi, window)
	}
	return true
}

// mqttTestInflightPreSendHookV is consulted by both QoS 1/2 senders BEFORE trackPublish.
// Returning false declines the offer without acknowledging it — the shape of every
// pre-tracking refusal on those paths — so JetStream re-offers the sequence with a higher
// delivery count. It exists to prove that count is not where DUP comes from.
var mqttTestInflightPreSendHookV atomic.Pointer[func(jsDur, reply string) bool]

func setMQTTTestInflightPreSendHook(h func(jsDur, reply string) bool) func() {
	prev := mqttTestInflightPreSendHookV.Load()
	if h == nil {
		mqttTestInflightPreSendHookV.Store(nil)
	} else {
		mqttTestInflightPreSendHookV.Store(&h)
	}
	return func() { mqttTestInflightPreSendHookV.Store(prev) }
}

func mqttInflightPreSendAdmits(jsDur, reply string) bool {
	if h := mqttTestInflightPreSendHookV.Load(); h != nil {
		return (*h)(jsDur, reply)
	}
	return true
}
