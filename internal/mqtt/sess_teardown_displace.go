package mqtt

import "fmt"

// Displacing a marked record whose retirement is already durably owned.
//
// A CONNECT that reads a record carrying a teardown marker used to FINISH that teardown
// inline (finishAdoptedTeardown) before it could create: settlement, the closure, the shared
// closing, the inbound QoS 2 hold walk, the deletions and the record release, all on the
// CONNECT's path. When the teardown that wrote the marker has already made its retirement
// durably owned, none of that is the successor's to do. The clean close releases the
// client-ID gate at exactly that point (destroyReleasing), and its successor lands here.
//
// DURABLY OWNED is five facts, each read, none inferred:
//
//  1. the record names only this session's resources (mqttValidateTeardownTargets), so
//     nothing a later owner deletes on its strength is another session's;
//  2. the teardown receipt committed with the marker stands, so the discard was AUTHORIZED
//     against the exact ownership being retired;
//  3. the retirement watermark names this generation at or past the marker's sequence, so
//     the inbound QoS 2 release classifier refuses its releases and the orphan sweep is
//     licensed (qos2_retire_classify.go, orphan_retire_proof.go);
//  4. the generation's permanent closure stands with Discard, so its recovery executor owns
//     its consumers, its exchanges and its shared closing, the orphan sweep is licensed even
//     after the watermark slot moves on, and no authority can be minted at it again
//     (checkProposedGenerationIsFresh). Read from the closure subject rather than inferred
//     from the per-client authority, because fact 5 is VACUOUS for a generation with neither
//     ledger nor closure and so cannot stand in for it;
//  5. the release gate both teardown routes apply before releasing a record authorizes it:
//     the closure is sealed and its surviving work enrolled or certified.
//
// The closure and its Discard member are create-only and immutable, the receipt is
// create-only, and the release gate's facts are about a closed generation, so none of the
// five can regress before the successor's write lands. What ARBITRATES the displacement is
// that write: the accept batch's record CAS at the marker's exact sequence, the authority's
// reservation CAS and the permanent metadata slot. A continuation that released the record
// first makes the CAS fail and the CONNECT re-drives into a record-absent create; a
// continuation that releases after it reads a different generation and leaves it alone
// (condemnRetiredSurvivor).
//
// Anything short of all five, or any read that cannot be decided, keeps the inline adoption.

// mqttMarkedRecordArm is which way a CONNECT went with a marked record.
type mqttMarkedRecordArm int

const (
	mqttMarkedRecordAdopted mqttMarkedRecordArm = iota
	mqttMarkedRecordDisplaced
)

// mqttMarkedRecordFacts is what a CONNECT read about a marked record before deciding whether
// to displace it.
type mqttMarkedRecordFacts struct {
	gen       string // the marked record's generation
	markerSeq uint64 // the sequence the marked record was read at

	targetsErr error // mqttValidateTeardownTargets

	receipt mqttOutboundLoadStatus

	watermark       *mqttGenRetirement
	watermarkStatus mqttRetirementStatus

	releaseFacts    mqttAdoptedReleaseFacts
	releaseFactsErr error // gatherAdoptedReleaseFacts
	releaseErr      error // mqttAdoptedReleaseAuthorized
}

// mqttMarkedRecordDisplaceable decides, from facts alone, whether a marked record's
// retirement is durably owned. The reason names the first fact that failed.
func mqttMarkedRecordDisplaceable(f mqttMarkedRecordFacts) (bool, string) {
	switch {
	case f.targetsErr != nil:
		return false, fmt.Sprintf("the record names resources that are not this session's: %v", f.targetsErr)
	case f.receipt != mqttOutboundValid:
		return false, fmt.Sprintf("its teardown receipt reads %v", f.receipt)
	case f.watermarkStatus != mqttRetirementValid || f.watermark == nil:
		return false, fmt.Sprintf("its retirement watermark reads %v", f.watermarkStatus)
	case f.watermark.Gen != f.gen || f.watermark.MarkerSeq < f.markerSeq:
		return false, fmt.Sprintf("its retirement watermark names generation %q at %d, not %q at %d or later",
			f.watermark.Gen, f.watermark.MarkerSeq, f.gen, f.markerSeq)
	case f.releaseFactsErr != nil:
		return false, fmt.Sprintf("its release facts could not be read: %v", f.releaseFactsErr)
	case f.releaseFacts.Closed == nil || !f.releaseFacts.Closed.Discard:
		return false, "no discarding closure of its generation stands"
	case f.releaseErr != nil:
		return false, fmt.Sprintf("the release gate refuses: %v", f.releaseErr)
	}
	return true, ""
}

// markedRecordFacts reads the facts mqttMarkedRecordDisplaceable decides on, for the marked
// record ps read at seq on the subject of hash. Each read stops the gathering once a fact has
// already failed, since a later one cannot change the answer.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) markedRecordFacts(ps *mqttPersistedSession, hash string, seq uint64) mqttMarkedRecordFacts {
	f := mqttMarkedRecordFacts{gen: ps.Gen, markerSeq: seq}
	if f.targetsErr = mqttValidateTeardownTargets(ps, hash); f.targetsErr != nil {
		return f
	}
	if _, _, f.receipt = as.jsa.loadTeardownReceipt(as.domainTk, hash, ps.Gen); f.receipt != mqttOutboundValid {
		return f
	}
	if f.watermark, _, f.watermarkStatus = as.jsa.loadGenRetirement(as.domainTk, hash); f.watermarkStatus != mqttRetirementValid {
		return f
	}
	if f.releaseFacts, f.releaseFactsErr = as.gatherAdoptedReleaseFacts(ps.ID, as.sessStreamEpoch, ps.Gen); f.releaseFactsErr != nil {
		return f
	}
	f.releaseErr = mqttAdoptedReleaseAuthorized(ps.ID, ps.Gen, f.releaseFacts)
	return f
}
