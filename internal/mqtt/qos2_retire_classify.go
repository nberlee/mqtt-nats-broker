// Classifying an inbound QoS 2 release against where its generation stands.
//
// The release claim decides the occupancy a discard ENUMERATED and competed for, and
// issue #66's cutoff decides how much of a subject a purge was entitled to destroy. Neither
// reaches the two populations this file exists for: a hold the discard's one-shot
// enumeration never named, and one the cutoff SPARED above its own watermark. For both of
// those the session was durably discarded and this broker was never told — so a PUBREL
// arriving afterwards fans an acknowledged payload out onward and answers the client
// 0x00, for a session that no longer exists. That is issue #40's site 2.
//
// What makes a decision possible here is that a teardown makes its generation noncurrent in
// the same batch that marks the record, before any deletion (teardown_claim.go), and a Clean
// Start does so in its acceptance batch. So a teardown whose enumeration or cutoff predates
// this hold necessarily made the generation noncurrent before this classification runs. The
// authority is durable evidence of that arbitration; it is not a second arbitration of its
// own, and it does not replace either fence.
//
// The release proceeds only while the authority SERVES this generation: State serving and Gen
// equal, never Gen alone, because a closed authority keeps its last generation's name. HEAD
// needed a second dimension (a later marker's sequence past our record) because its watermark
// was one slot that lost earlier generations; the authority does not (issue #133, consult 10).
//
// THE IDENTITY TERM IS THE PACKET LEASE, and this is what issue #40's second attempt found
// unproved. mqttParse acquires the packet's completion lease against c.mqtt.tracker — the
// incarnation the CONNECTION was bound to — before the rejection gate and before any
// mutable session observation, and releases it only at the end of that packet's iteration.
// So the lease spans this whole handler, and no re-mint can complete while it is held:
// setIncarnationLocked requires a permit that only a closed, DRAINED tracker over
// sess.c == nil can mint, and this lease is an undrained owner. The incarnation, and
// therefore sess.gen and the hold subject derived from it, are stable here.
//
// The lease does NOT stabilize the record — it pins the incarnation, not the sequence, and
// other admitted owners or local record writers can still advance it. That is why the
// sample below takes the RECORD TRANSACTION, under which an unresolved write of ours refuses.
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
// Where the authority cannot be read or the session's own write is unresolved, the
// release FAILS CLOSED — no PUBCOMP, the hold and the claim both retained. An unresolved
// proof is not permission, and the surrounding PUBREL contract already suppresses PUBCOMP
// for exactly that reason: MQTT 5 gives PUBCOMP no reason code for "the broker could not
// decide".
//
// See https://github.com/nberlee/mqtt-nats-broker/issues/131 issue #40.

package mqtt

import (
	"fmt"
	"sync/atomic"
)

// mqttQoS2RetireVerdict is what the generation's standing says about this release.
type mqttQoS2RetireVerdict int

const (
	// mqttQoS2RetireProceed: the authority serves this incarnation's generation, so the
	// release may deliver onward.
	mqttQoS2RetireProceed mqttQoS2RetireVerdict = iota

	// mqttQoS2RetireRetired: positive evidence that this incarnation's session was
	// durably discarded. Nothing is delivered; the hold is released NONTERMINALLY and
	// the client is answered 0x92 — the same answer the durable claim already produces
	// when the discard was on another broker.
	mqttQoS2RetireRetired

	// mqttQoS2RetireUnresolved: the question could not be decided — an unreadable or
	// undecided authority, an unavailable record fence, or a session whose own record
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

// mqttTestQoS2RetireInTxV runs at the start of the classification proper, with the record
// transaction held: a test parks one release there to hold the transaction against another.
// Nil in production.
var mqttTestQoS2RetireInTxV atomic.Pointer[func(sess *mqttSession)]

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
	// The positive memo, read first because it costs nothing. A noncurrent generation is
	// MONOTONE: no transition makes a discarded generation current again. So this result
	// may be remembered — and only this one. The three NEGATIVE outcomes
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
// It reads where THIS generation stands, from the client's authority (gen_standing.go): only a
// generation the authority still SERVES proceeds. A noncurrent one, outstanding or retired, was
// discarded, so its releases fan out nothing; and an unreadable or undecided answer never
// proceeds. HEAD compared the watermark's marker sequence with this session's acknowledged
// record, because its single slot lost earlier generations' identities; the authority keeps
// them, and a teardown makes its generation noncurrent in the same batch that marks the record,
// so "a teardown landed after my record" is exactly "my generation is not current" (consult 10).
//
// The caller holds the record transaction; sess.mu is NOT held by it, because an open
// transaction does not leave that mutex held.
func (sess *mqttSession) classifyQoS2ReleaseInTx(asm *mqttAccountSessionManager, gen string) (mqttQoS2RetireVerdict, error) {
	if h := mqttTestQoS2RetireInTxV.Load(); h != nil {
		(*h)(sess)
	}
	sess.mu.Lock()
	id, uncertain := sess.id, sess.seqUncertain
	sess.mu.Unlock()
	if uncertain {
		// While a write of ours is unresolved this session cannot say which record it is
		// acknowledged for, so it does not answer for its generation either.
		return mqttQoS2RetireUnresolved, fmt.Errorf("this session's own record write is unresolved, so its standing cannot be sampled")
	}
	switch standing, _ := asm.genStanding(id, gen); standing {
	case mqttGenCurrent:
		return mqttQoS2RetireProceed, nil
	case mqttGenOutstanding, mqttGenRetired:
		// Positive and monotone: no transition makes a noncurrent generation current again, so
		// this is the one result the memo may keep.
		sess.mu.Lock()
		if sess.gen == gen {
			sess.retiredGenProven = gen
		}
		sess.mu.Unlock()
		return mqttQoS2RetireRetired, nil
	default:
		return mqttQoS2RetireUnresolved, fmt.Errorf("where generation %q stands cannot be read", gen)
	}
}
