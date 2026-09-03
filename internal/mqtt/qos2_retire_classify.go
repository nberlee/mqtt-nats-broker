// Classifying an inbound QoS 2 release against the generation-retirement watermark.
//
// The release claim decides the occupancy a discard ENUMERATED and competed for, and
// F-062's cutoff decides how much of a subject a purge was entitled to destroy. Neither
// reaches the two populations this file exists for: a hold the discard's one-shot
// enumeration never named, and one the cutoff SPARED above its own watermark. For both of
// those the session was durably discarded and this broker was never told — so a PUBREL
// arriving afterwards fans an acknowledged payload out onward and answers the client
// 0x00, for a session that no longer exists. That is F-036's site 2.
//
// What makes a decision possible here is that the retirement watermark is written BEFORE
// any teardown deletion (recordGenRetirement), and a write that cannot be confirmed
// aborts the teardown with nothing deleted. So a teardown whose enumeration or cutoff
// predates this hold necessarily recorded its watermark before this classification runs.
// The watermark is durable evidence of an earlier session-record arbitration; it is not a
// second arbitration of its own, and it does not replace either fence.
//
// THE CLASSIFICATION IS TWO-DIMENSIONAL, and one dimension alone is wrong in a way that
// cost F-036 an attempt. Refusing on a bare generation MISMATCH is wrong because the
// watermark is a single slot replaced by increasing MarkerSeq alone: retire G1, then
// create and retire G2, and a broker still serving G1 reads "a different generation" and
// fans out. Refusing only on a generation MATCH is wrong for the same slot reason from the
// other side. So the release is refused iff the watermark is valid AND either it names
// THIS incarnation, or its MarkerSeq proves the record subject advanced past the record
// this broker is acknowledged for.
//
// MarkerSeq == sess.seq is unreachable for a bound session and has no arm: every sess.seq
// adoption takes either a store response for this broker's own bytes or a digest-matched
// readback, and a marked record is never restored into a bound session.
//
// THE IDENTITY TERM IS THE PACKET LEASE, and this is what F-036's second attempt found
// unproved. mqttParse acquires the packet's completion lease against c.mqtt.tracker — the
// incarnation the CONNECTION was bound to — before the rejection gate and before any
// mutable session observation, and releases it only at the end of that packet's iteration.
// So the lease spans this whole handler, and no re-mint can complete while it is held:
// setIncarnationLocked requires a permit that only a closed, DRAINED tracker over
// sess.c == nil can mint, and this lease is an undrained owner. The incarnation, and
// therefore sess.gen and the hold subject derived from it, are stable here.
//
// The lease does NOT stabilize sess.seq — it pins the incarnation, not the sequence, and
// other admitted owners or local record writers can still advance it. That is why the
// sample below takes the RECORD TRANSACTION: the fence is what makes sess.seq and
// seqUncertain one observation rather than two reads that happen to be adjacent.
//
// Taking that fence here cannot deadlock against a local destruction, and the argument is
// structural rather than incidental. The apparent cycle is that a PUBREL holds the release
// op and waits for the record transaction while clear() holds the record transaction and
// waits for the release-op drain. It cannot become live because every local destruction
// CLOSES AND DRAINS the incarnation tracker before prepareClear acquires the transaction:
// this PUBREL's packet lease keeps the destroyer out of the transaction while the handler
// runs, and a destroyer that closed the tracker first makes packet admission park or
// refuse before the handler is entered at all.
//
// Where the watermark cannot be read or the session's own sequence is unresolved, the
// release FAILS CLOSED — no PUBCOMP, the hold and the claim both retained. An unresolved
// proof is not permission, and the surrounding PUBREL contract already suppresses PUBCOMP
// for exactly that reason: MQTT 5 gives PUBCOMP no reason code for "the broker could not
// decide".
//
// See docs/ARCHITECTURE.md, "The generation-retirement watermark", and
// docs/findings/ha-arbitration.md F-036.

package mqtt

import (
	"fmt"
	"sync/atomic"
)

// mqttQoS2RetireVerdict is what the watermark says about this release.
type mqttQoS2RetireVerdict int

const (
	// mqttQoS2RetireProceed: no valid watermark reaches this incarnation, so the
	// release may deliver onward. Absent, and a valid watermark for another
	// generation whose MarkerSeq does not exceed our own sequence, both land here.
	mqttQoS2RetireProceed mqttQoS2RetireVerdict = iota

	// mqttQoS2RetireRetired: positive evidence that this incarnation's session was
	// durably discarded. Nothing is delivered; the hold is released NONTERMINALLY and
	// the client is answered 0x92 — the same answer the durable claim already produces
	// when the discard was on another broker.
	mqttQoS2RetireRetired

	// mqttQoS2RetireUnresolved: the question could not be decided — an unreadable or
	// malformed watermark, an unavailable record fence, or a session whose own record
	// write is unresolved. The exchange is unfinished: no PUBCOMP, and both records
	// stay.
	mqttQoS2RetireUnresolved
)

func (v mqttQoS2RetireVerdict) String() string {
	switch v {
	case mqttQoS2RetireProceed:
		return "proceed"
	case mqttQoS2RetireRetired:
		return "retired"
	case mqttQoS2RetireUnresolved:
		return "unresolved"
	}
	return "invalid"
}

// mqttTestQoS2RetireSampledHookV fires immediately after the classifier has released the
// record fence and before its verdict is applied, carrying the verdict. It is the seam an
// ordering test uses to prove the classification happened BETWEEN the hold proof and the
// onward store, rather than merely that its outcome was reached. Nil in production.
var mqttTestQoS2RetireSampledHookV atomic.Pointer[func(verdict mqttQoS2RetireVerdict)]

// classifyQoS2ReleaseAgainstRetirement decides whether this incarnation's release may
// still deliver onward.
//
// It is called with the release claim held and the hold record re-proved at its own
// sequence, and BEFORE the onward store — the last point at which anything can still be
// taken back.
//
// Returns the verdict and, for an unresolved one, the reason to log. No lock held on
// entry; performs one JetStream read.
func (sess *mqttSession) classifyQoS2ReleaseAgainstRetirement(asm *mqttAccountSessionManager) (mqttQoS2RetireVerdict, error) {
	// The positive memo, read first because it costs nothing. A valid watermark naming a
	// generation is MONOTONE: once retired, a generation never becomes rightful again,
	// and single-slot replacement can erase the fact's discoverability but not the fact.
	// So this result may be remembered — and only this one. The three NEGATIVE outcomes
	// are never cached: each can be invalidated by a retirement landing afterwards, and a
	// finite window never arbitrates ownership.
	sess.mu.Lock()
	gen := sess.gen
	memoed := sess.retiredGenProven != _EMPTY_ && sess.retiredGenProven == gen
	sess.mu.Unlock()
	if memoed {
		return mqttQoS2RetireRetired, nil
	}

	// The fence. A refusal is not a verdict: a local writer holds it, or it is reserved
	// for an announced blocking writer, and in neither case has anything been arbitrated.
	// Never the blocking form — this runs on the client's readLoop with a release op
	// outstanding, and a bounded wait here would hold that op across another writer's
	// JetStream round trip.
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		return mqttQoS2RetireUnresolved, fmt.Errorf("the session record fence is held, so the retirement question cannot be sampled")
	}
	verdict, rerr := sess.classifyQoS2ReleaseInTx(asm, gen)
	if eerr := sess.endRecordTx(tok); eerr != nil {
		// The fence was not ours to release, which is a programming error rather than a
		// protocol state. Fail the release closed rather than acting on a sample taken
		// under a transaction we cannot account for.
		return mqttQoS2RetireUnresolved, fmt.Errorf("unable to release the record fence after sampling the retirement: %w", eerr)
	}
	if h := mqttTestQoS2RetireSampledHookV.Load(); h != nil {
		(*h)(verdict)
	}
	return verdict, rerr
}

// classifyQoS2ReleaseInTx is the classification proper, with the record fence held.
//
// The ORDER of the two observations matters and is not arbitrary: the watermark is read
// FIRST and the session's own sequence sampled after it, so the sequence the MarkerSeq
// comparison uses is at least as new as the watermark it is compared against. Reading the
// sequence first would allow a comparison against a sequence that had already advanced,
// which turns a positive proof into a false negative.
//
// The caller holds the record transaction; sess.mu is NOT held by it, because an open
// transaction does not leave that mutex held.
func (sess *mqttSession) classifyQoS2ReleaseInTx(asm *mqttAccountSessionManager, gen string) (mqttQoS2RetireVerdict, error) {
	sess.mu.Lock()
	idHash := sess.idHash
	sess.mu.Unlock()

	r, _, status := asm.jsa.loadGenRetirement(asm.domainTk, idHash)
	switch status {
	case mqttRetirementAbsent:
		// No retirement was ever recorded for this client. F-066 makes a lone
		// recreation of $MQTT_teardown a refused state — an account holding eight of
		// the nine streams fails the account-wide preflight before anything is
		// provisioned — so absence here is absence rather than a lost fence.
		return mqttQoS2RetireProceed, nil
	case mqttRetirementValid:
	default: // mqttRetirementCorrupt, mqttRetirementIndeterminate
		// Never repaired in place, and never read leniently: a watermark this broker
		// cannot interpret may name a retirement newer than anything it can see. Same
		// fail-closed disposition every other reader of this record applies.
		return mqttQoS2RetireUnresolved, fmt.Errorf("the retirement watermark is %s", mqttRetirementStatusLabel(status))
	}

	// The session's own sequence, sampled with its uncertainty in ONE critical section.
	// Sampling the sequence without the uncertainty flag would read a pre-write base as a
	// settled sequence: while a write of ours is unresolved sess.seq still holds the
	// base, so a MarkerSeq comparison against it would answer about a record state
	// nobody has classified.
	sess.mu.Lock()
	ourSeq, uncertain := sess.seq, sess.seqUncertain
	sess.mu.Unlock()
	if uncertain {
		return mqttQoS2RetireUnresolved, fmt.Errorf("this session's own record write is unresolved, so its sequence cannot bound a retirement")
	}

	// Dimension 1: the watermark names THIS incarnation. Positive and monotone, so this
	// is the one result the memo may keep.
	if r.Gen == gen {
		sess.mu.Lock()
		if sess.gen == gen {
			sess.retiredGenProven = gen
		}
		sess.mu.Unlock()
		return mqttQoS2RetireRetired, nil
	}

	// Dimension 2: the watermark names another generation, but its marker landed at a
	// $MQTT_sess sequence BEYOND the record this broker is acknowledged for — so a
	// teardown claimed this client's record subject after our own record, and this
	// incarnation is not the one serving it any more. Not memoized: this is a statement
	// about our sequence rather than about our generation, and our sequence moves.
	if r.MarkerSeq > ourSeq {
		return mqttQoS2RetireRetired, nil
	}

	// A valid watermark for a generation that is not ours, whose marker does not reach
	// past our own record. It is evidence about a retirement this incarnation outlived,
	// which is not evidence about this incarnation.
	return mqttQoS2RetireProceed, nil
}

// mqttRetirementStatusLabel names a non-actionable watermark status for a log line.
func mqttRetirementStatusLabel(status mqttRetirementStatus) string {
	switch status {
	case mqttRetirementAbsent:
		return "absent"
	case mqttRetirementValid:
		return "valid"
	case mqttRetirementCorrupt:
		return "unreadable or malformed"
	case mqttRetirementIndeterminate:
		return "indeterminate"
	}
	return "of an unknown status"
}
