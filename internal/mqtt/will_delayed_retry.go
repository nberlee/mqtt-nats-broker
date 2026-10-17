// Copyright 2025 The mqtt-nats-broker Authors

package mqtt

import "sync"

// A DELAYED Will whose enqueue was refused before it had any effect, held until its
// authorized account returns.
//
// Why this exists beside the immediate queue rather than reusing it. Both hold a
// Will across a service-connection blip, but their terminal actions are opposites:
// the immediate queue PUBLISHES on recovery (dischargeOwedImmediateWills ->
// publishWillRecord), which for a delayed Will is exactly the bypass the discharge
// path exists to close. A due delayed Will published on recovery skips the
// acceptance-fact arbitration, so a reconnect that genuinely occurred during the
// Will Delay is ignored; and a still-FUTURE one would go out before its FireAt.
// So this queue RE-ATTEMPTS THE ENQUEUE and never publishes.
//
// What it preserves, and why each is load-bearing. FireAt is the arbitration input
// -- `At < FireAt` is what dates a reconnect's entitlement to suppress -- so a
// vehicle that recomputed it would reintroduce the very defect anchoring the
// deadline was for. The authorized account is the identity the record is written
// under and the fence deliverWillRecord applies at fire time. The origin sequence
// and epoch are what mqttAcceptFactQualifies compares against. And the WillID and
// client-ID hash are carried IMMUTABLY so every re-attempt is the same create-only
// CAS on the same subject; see enqueueDelayedWill, whose ambiguity resolution
// depends on that identity being stable.
//
// Immutable identity is NOT durable deduplication. It makes a re-attempt idempotent
// only while the record exists: after a terminal deletion the same subject can be
// created again. Safety therefore rests equally on the terminal-outcome rules below
// -- every outcome except Unavailable removes the entry.
//
// Why in memory, and what that costs. The only durable park is $MQTT_willq, which
// is precisely what is unreachable while the service connection is down. So this
// restores ordinary-blip behaviour and claims nothing more: a restart, a shutdown,
// or the capacity bound still lose the Will. Each is a stated residual rather than
// an implied guarantee.
const (
	// mqttDelayedWillMaxBytes bounds what one broker holds across a blip, SEPARATELY
	// from the immediate queue's identical bound. Separate rather than shared so a
	// delayed backlog cannot consume the immediate queue's admission capacity, which
	// would let a Will Delay outage starve the Wills that are due now. The two
	// budgets together therefore permit twice this much charged message data; it is
	// a charged-bytes bound, not an exact heap ceiling.
	mqttDelayedWillMaxBytes = 4 << 20
)

// mqttDelayedWillIdentity is the part of a delayed-Will enqueue that must not change
// across re-attempts. Carried by value into enqueueDelayedWill.
type mqttDelayedWillIdentity struct {
	WillID  string
	CIDHash string
}

// mqttWillEnqueueOutcome classifies which EXIT of enqueueDelayedWill was taken.
//
// It is an outcome per exit rather than an error taxonomy, and that distinction is
// the point: the same error value can arrive from exits owing opposite dispositions.
// In particular a create that lands, whose acknowledgement and re-reads then fail,
// followed by a fail-safe that refuses the destination, returns
// errMQTTWillDestinationUnavailable from an exit that is TERMINAL -- retaining it on
// the strength of that sentinel would re-attempt an obligation that may already be
// durable, and mint a second record for one Will.
type mqttWillEnqueueOutcome int

const (
	// mqttWillEnqueueConfirmed: durability is proven, by the create CAS or by a
	// re-read that found our own record. The outbox arbitrates from here.
	mqttWillEnqueueConfirmed mqttWillEnqueueOutcome = iota
	// mqttWillEnqueueUnavailable: refused BEFORE any enqueue effect, because the
	// service account is not established or is not the one this Will was authorized
	// against. The ONLY retainable outcome, and it is retainable precisely because
	// nothing happened.
	mqttWillEnqueueUnavailable
	// mqttWillEnqueuePublishedInstead: the record was refused by its own writer
	// validation and the fail-safe published it. The Will Delay is sacrificed rather
	// than the Will. Terminal: no retry, no second publication.
	mqttWillEnqueuePublishedInstead
	// mqttWillEnqueueTerminalFailed: writer validation refused and the fail-safe
	// errored. Nothing was persisted, so nothing can fire later. Terminal loss.
	mqttWillEnqueueTerminalFailed
	// mqttWillEnqueueUnconfirmedPublished: the attempts were exhausted without
	// confirming, and the fail-safe published. A record may exist AND the Will may
	// have gone out. Terminal: the caller must neither retry nor publish again.
	mqttWillEnqueueUnconfirmedPublished
	// mqttWillEnqueueUnconfirmedAbandoned: the attempts were exhausted and the
	// fail-safe also failed. Terminal, with possible loss -- possible rather than
	// certain, because a create may have landed before the re-reads failed, in which
	// case the durable record still fires.
	mqttWillEnqueueUnconfirmedAbandoned
)

// retainable reports whether this outcome may be re-attempted later. Exactly one
// outcome may, and it is the one with no enqueue effect.
func (o mqttWillEnqueueOutcome) retainable() bool { return o == mqttWillEnqueueUnavailable }

// mqttOwedDelayedWill is one delayed-Will enqueue awaiting the return of the account
// it was authorized against. Every field is captured, immutable, and re-used verbatim
// on each attempt -- nothing here is re-derived, and no mutable session is held.
type mqttOwedDelayedWill struct {
	as       *mqttAccountSessionManager
	identity mqttDelayedWillIdentity
	clientID string
	sessGen  string
	will     *mqttWill
	props    []byte
	user     string
	fireAt   int64
	// account is what the enqueue must write the record under, and what a recovery
	// pass compares the current service account against. A pass under a DIFFERENT
	// account discharges nothing: the original may yet return, so each entry is
	// asked individually rather than the pass giving up on the first mismatch.
	account         string
	originSessSeq   uint64
	originSessEpoch string
	size            int
}

type mqttDelayedWillQueue struct {
	// dischargeMu admits ONE pass at a time and is held for the whole pass. Same
	// discipline, and same reason, as the immediate queue's: it makes the charge
	// exact by construction and makes a lost wake-up impossible, because a second
	// caller BLOCKS here and then runs a full pass that by definition sees whatever
	// the first one left.
	dischargeMu sync.Mutex

	mu   sync.Mutex
	owed []*mqttOwedDelayedWill
	// bytes charges exactly what the queue holds. An entry is uncharged only when it
	// leaves, and it leaves only when its outcome is terminal.
	bytes int
	// kickPending coalesces waiters to one, cleared as soon as the waiting pass takes
	// dischargeMu so a kick arriving DURING a pass schedules a fresh waiter.
	kickPending bool
	// dropped counts enqueues refused for capacity since start, for the operator log
	// and so a test can assert the bound is real rather than nominal.
	dropped uint64
}

// retainDelayedWillEnqueue holds a delayed-Will enqueue refused before it had any
// effect, to be re-attempted when its authorized account returns.
//
// Returns false when it could NOT be retained -- shutdown has begun, or the capacity
// bound is reached -- and a false return is a loss the caller must report. Silence is
// the one thing this must not do.
//
// The recheck-and-kick at the end is load-bearing, exactly as it is for the immediate
// queue: the reconnect that would discharge this obligation can complete BETWEEN the
// refusal that produced it and the insertion below, and a pass that ran in that window
// saw an empty queue. Asking again after inserting closes it.
func (s *Server) retainDelayedWillEnqueue(o *mqttOwedDelayedWill) bool {
	if o == nil || o.as == nil {
		return false
	}
	// The same admission gate every Will worker uses. A closed gate means shutdown
	// Phase 2 has begun and nothing new may be taken on.
	if !s.admitWillWorker() {
		return false
	}
	o.size = len(o.will.message) + len(o.props) + len(o.will.topic)
	q := &s.owedDelayedWills
	q.mu.Lock()
	if q.bytes+o.size > mqttDelayedWillMaxBytes {
		q.dropped++
		dropped, held := q.dropped, q.bytes
		q.mu.Unlock()
		s.willWorkerWG.Done()
		s.Errorf("MQTT: dropping the delayed Will on %q: %d bytes of delayed Wills are already held awaiting the return of their authorized account (%d dropped so far)",
			o.will.topic, held, dropped)
		return false
	}
	q.owed = append(q.owed, o)
	q.bytes += o.size
	q.mu.Unlock()
	// Released BEFORE the kick, which takes its own token: holding one across the
	// kick would make shutdown wait on this call for the whole pass, and the kick
	// could then find the gate closed with our token still outstanding.
	s.willWorkerWG.Done()

	// The kick asks first. Scanning the backlog on every insertion would be quadratic
	// in the number of Wills an outage produces for no gain -- while the destination
	// is away every entry refuses exactly as this one just did. The only reason to
	// look is that it is available NOW, which is the window this closes.
	if _, ok := s.serviceAccountForWill(); ok {
		s.kickOwedDelayedWills()
	}
	return true
}

// kickOwedDelayedWills asks for a pass WITHOUT blocking the caller, so a reconnect
// callback is never held behind a JetStream round trip.
func (s *Server) kickOwedDelayedWills() {
	q := &s.owedDelayedWills
	q.mu.Lock()
	if q.kickPending {
		q.mu.Unlock()
		return
	}
	q.kickPending = true
	q.mu.Unlock()

	// Admitted like every other Will worker. A reconnect callback can complete long
	// after shutdown closed the gate; refusing here leaves the entries for
	// discardOwedDelayedWills rather than writing through infrastructure being torn
	// down.
	if !s.admitWillWorker() {
		q.mu.Lock()
		q.kickPending = false
		q.mu.Unlock()
		return
	}
	go func() {
		defer s.willWorkerWG.Done()
		q.dischargeMu.Lock()
		defer q.dischargeMu.Unlock()
		// Cleared once this pass OWNS the lock and before it runs, so a kick arriving
		// while it runs schedules a fresh waiter instead of being folded into a pass
		// that has already looked.
		q.mu.Lock()
		q.kickPending = false
		q.mu.Unlock()
		s.dischargeOwedDelayedWills()
	}()
}

// dischargeOwedDelayedWills re-attempts every retained enqueue whose authorized
// account is the current one. Serialized by dischargeMu, which its only production
// caller takes.
//
// It RE-ATTEMPTS THE ENQUEUE and never publishes: that is the whole difference from
// the immediate queue, and the reason a delayed Will may not simply be parked there.
// The enqueue's own fence remains the authority -- this does not decide that a Will
// may be written, it only asks again -- so an entry whose account is still not the
// current one stays retained, which is why each is asked individually rather than the
// pass giving up on the first refusal.
func (s *Server) dischargeOwedDelayedWills() {
	q := &s.owedDelayedWills
	for i := 0; ; i++ {
		q.mu.Lock()
		if i >= len(q.owed) {
			q.mu.Unlock()
			return
		}
		o := q.owed[i]
		q.mu.Unlock()

		// Re-attempted with the queue UNLOCKED but the entry still in it and still
		// charged, so a concurrent retention cannot be admitted against space this
		// entry occupies -- and so one obligation can never be submitted twice.
		// Nothing else removes entries; dischargeMu admits one pass.
		outcome, err := o.as.enqueueDelayedWill(s, o.clientID, o.sessGen, o.will, o.props, o.user,
			o.fireAt, o.originSessSeq, o.originSessEpoch, o.account, &o.identity)
		if outcome.retainable() {
			continue // still away; the eventual return finds it
		}
		switch outcome {
		case mqttWillEnqueueConfirmed:
			s.Noticef("MQTT: the retained delayed Will on %q for account %q is durably enqueued", o.will.topic, o.account)
		case mqttWillEnqueuePublishedInstead, mqttWillEnqueueUnconfirmedPublished:
			s.Warnf("MQTT: the retained delayed Will on %q was published without its delay: %v", o.will.topic, err)
		default:
			s.Errorf("MQTT: the retained delayed Will on %q is abandoned: %v", o.will.topic, err)
		}
		// Terminal, so it leaves and is uncharged. Removed by identity rather than by
		// index: the slice is unlocked during the attempt above.
		q.mu.Lock()
		for j, cur := range q.owed {
			if cur == o {
				q.owed = append(q.owed[:j], q.owed[j+1:]...)
				q.bytes -= o.size
				break
			}
		}
		q.mu.Unlock()
		i-- // the slice shrank under this index
	}
}

// discardOwedDelayedWills reports and releases everything still held. Shutdown's
// last word on this queue, run after the Will workers have drained so nothing can be
// added behind it.
func (s *Server) discardOwedDelayedWills() {
	q := &s.owedDelayedWills
	q.mu.Lock()
	pending := q.owed
	q.owed, q.bytes = nil, 0
	q.mu.Unlock()
	for _, o := range pending {
		s.Errorf("MQTT: shutting down while still holding the delayed Will on %q for account %q; it is not published",
			o.will.topic, o.account)
	}
}
