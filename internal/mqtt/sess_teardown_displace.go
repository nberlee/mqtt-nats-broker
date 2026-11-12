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
// DURABLY OWNED is four facts, each read, none inferred:
//
//  1. the record names only this session's resources (mqttValidateTeardownTargets), so
//     nothing a later owner deletes on its strength is another session's;
//  2. the authority carries the marker's generation as outstanding with this marker's release
//     still pending (issue #133, plan 5.1 shape 4): the claim batch committed that entry and its
//     discarding closure with the marker, so the discard was authorized
//     against the exact ownership retired, and the displacement discharges exactly that release;
//  3. the generation's closure stands (every closure discards), so its recovery executor owns its
//     consumers, its exchanges and its shared closing;
//  4. the release gate both teardown routes apply before releasing a record authorizes it:
//     the closure is sealed and its surviving work enrolled or certified.
//
// What ARBITRATES the displacement is the acceptance's own batch: the authority CAS that
// discharges the pending release, and the record CAS at the marker's exact sequence. A
// continuation that released the record first makes the record CAS fail and the CONNECT
// re-drives; one that releases after it reads the release discharged as displaced.
//
// Anything short of all four, or any read that cannot be decided, keeps the inline adoption.

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

	// auth is the client's authority; pendingEntry reports whether it carries this marker's
	// generation as outstanding with the marker's release still pending.
	auth         mqttOutboundLoadStatus
	pendingEntry bool

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
	case f.auth != mqttOutboundValid:
		return false, fmt.Sprintf("its authority reads %v", f.auth)
	case !f.pendingEntry:
		return false, "its authority carries no pending release of this marker"
	case f.releaseFactsErr != nil:
		return false, fmt.Sprintf("its release facts could not be read: %v", f.releaseFactsErr)
	case f.releaseFacts.Closed == nil:
		return false, "no closure of its generation stands"
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
	al := as.jsa.loadOutboundAuthority(ps.ID, as.sessStreamEpoch)
	if f.auth = al.status; f.auth != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
		return f
	}
	if _, err := mqttDisplaceRelease(al.rec.Life.Outstanding, ps.Teardown.Nonce); err != nil ||
		al.rec.State != mqttAuthorityClosed || al.rec.Life.Phase != mqttLifecycleServing ||
		!mqttOutstandingNamesMarker(al.rec.Life, ps.Gen, ps.Teardown.Nonce) {
		return f
	}
	f.pendingEntry = true
	if f.releaseFacts, f.releaseFactsErr = as.gatherAdoptedReleaseFacts(ps.ID, as.sessStreamEpoch, ps.Gen); f.releaseFactsErr != nil {
		return f
	}
	f.releaseErr = mqttAdoptedReleaseAuthorized(ps.ID, ps.Gen, f.releaseFacts)
	return f
}
