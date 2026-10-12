package mqtt

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server/stree"
)

// The retained index (as.retmsgs plus as.rmsCache) is the volatile view every broker
// serves retained replay from. It is fed by three writers that observe the same durable
// authority — $MQTT_rmsgs, one record per topic — at different times: the retained
// observer (this broker's ephemeral AckNone push consumer), the publishing broker's own
// synchronous local seed, and the authoritative loads a SUBSCRIBE issues. F-059.
//
// Two mechanisms, and neither subsumes the other.
//
// ORDERING. Every writer goes through applyRetainedLocked, which orders what it applies by
// $MQTT_rmsgs stream sequence against what the index already holds and against W, the
// last observer delivery committed in an unbroken run of consumer delivery sequences. A
// deletion seen ahead of the observer leaves a PLACEHOLDER carrying its sequence, so a
// delayed older add cannot walk back over it.
//
// LOSS. A watermark cannot repair an application that never ran, and an AckNone
// delivery that nats.go dropped, or that a reconnect lost, or that a consumer reaped by
// InactiveThreshold never sent, runs no writer at all. The observer therefore tracks the
// consumer delivery sequence the way nats.go's ordered consumer does: a gap, an idle
// heartbeat naming a later delivery than the last one committed, or a run of missed
// heartbeats marks it RESETTING, and retainedObserverLoop re-creates the consumer from
// W+1. MaxMsgsPer:1 means the stream holds only each topic's latest record, so every
// subject whose state changed after W is redelivered.
//
// What stays bounded rather than closed is a SUPERSEDED value in the cache — see
// docs/CONFORMANCE.md, "The retained index is arbitrated by stream sequence". The index
// decides which subjects are candidates; values are always served from the cache or an
// authoritative load.

// mqttRetainedObserverHeartbeat is the observer's IdleHeartbeat. A lost tail delivery is
// noticed within one interval, a consumer that no longer exists within three, and a reset
// is attempted at most once per interval.
const mqttRetainedObserverHeartbeat = 5 * time.Second

// mqttRetainedLivenessBeats is how many heartbeat intervals may pass with no delivery and
// no heartbeat before the consumer is presumed gone.
const mqttRetainedLivenessBeats = 3

// errMQTTTestRetainedResetDeleteAmbiguous is what testRetainedResetDeleteAmbiguous makes
// the reset's definite delete report: the whole helper, both attempts, came back without a
// definite answer.
var errMQTTTestRetainedResetDeleteAmbiguous = errors.New("mqtt test: retained observer delete was ambiguous")

// mqttRetainedOrigin names which writer an application comes from. Only the observer
// applies in stream order, so only the observer advances W, and only a deletion the
// observer commits can be dropped without a placeholder.
type mqttRetainedOrigin uint8

const (
	mqttRetainedFromObserver mqttRetainedOrigin = iota
	mqttRetainedFromSeed
	mqttRetainedFromLoad
)

// mqttRetainedObserverState is the observer's half of the index, guarded by as.mu like
// the index itself, so that the index entry, W and the delivery bookkeeping change in one
// critical section.
type mqttRetainedObserverState struct {
	// sub is the CURRENT generation's subscription. A push delivery reaches the callback
	// with its original stream subject and an ACK reply naming a consumer whose name never
	// changes, so the subscription the shim invokes the callback with is the only identity
	// a generation has. Anything delivered on another one is stale.
	sub *subscription
	// resetting is set by every detector; while it is set nothing is committed.
	resetting bool
	// lastDseq is the last consumer delivery sequence committed in this generation.
	lastDseq uint64
	// applied is W.
	applied uint64
	// lastArrival is when the current generation last delivered anything, heartbeat
	// included.
	lastArrival time.Time
	// placeholders maps subject -> sequence of every placeholder in the index, so the
	// observer can prune those it has passed.
	placeholders map[string]uint64
	// lastAttempt is when the loop last attempted a reset; at most one per heartbeat.
	lastAttempt time.Time
	// wake asks the loop to look now rather than at its next tick. Buffered, size one.
	wake chan struct{}

	// Bookkeeping read by tests only: how many reset attempts ran, why the last reset was
	// requested, and the start sequence the last reset asked for.
	resets      uint64
	resetReason string
	resetStart  uint64
}

// markResettingLocked records that the observer's view can no longer be trusted to be
// complete. as.mu held.
func (o *mqttRetainedObserverState) markResettingLocked(reason string) {
	if o.resetting {
		return
	}
	o.resetting = true
	o.resetReason = reason
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// retainedSeams returns the server-scoped test seams, nil when the manager has no server
// (unit fixtures). Nil seams are the production configuration.
func (as *mqttAccountSessionManager) retainedSeams() *mqttSessionManager {
	if as.jsa.mqttJSACore == nil || as.jsa.c == nil || as.jsa.c.srv == nil {
		return nil
	}
	return &as.jsa.c.srv.mqtt.sessmgr
}

func (as *mqttAccountSessionManager) retainedObserverHeartbeat() time.Duration {
	if sm := as.retainedSeams(); sm != nil && sm.testRetainedObserverHeartbeat > 0 {
		return sm.testRetainedObserverHeartbeat
	}
	return mqttRetainedObserverHeartbeat
}

// applyRetainedLocked is the ONLY writer of an index entry other than the no-message arm
// (retainedNoMessageLocked). rm == nil is a deletion: an empty-body tombstone, a JetStream
// marker, or a record this broker cannot decode, which is a deletion AT its sequence. It
// reports whether the index changed. as.mu held.
func (as *mqttAccountSessionManager) applyRetainedLocked(subject string, seq uint64, rm *mqttRetainedMsg, origin mqttRetainedOrigin) bool {
	if seq == 0 {
		// No sequence, no order. Only an unparsable reply produces it, and the observer
		// treats that as a gap before it gets here.
		return false
	}
	if as.retmsgs == nil {
		as.retmsgs = stree.NewSubjectTree[mqttRetainedMsgRef]()
	}
	o := &as.rmObs
	key := stringToBytes(subject)
	ref, exists := as.retmsgs.Find(key)
	if exists && ref.sseq == seq {
		// The record the index already names. Only a LIVE entry may have its cached copy
		// hydrated from it — before the watermark test below, since a cold load of the
		// current record normally reads a sequence the observer has already committed. A
		// placeholder at this sequence is a deletion, and nothing resurrects it.
		if !ref.deleted && rm != nil {
			as.cacheRetainedLocked(subject, rm, origin)
		}
		return false
	}
	if origin != mqttRetainedFromObserver && seq <= o.applied {
		// The observer has committed the record at seq, or skipped it because the stream
		// had already superseded it with a record it delivers later. Either way the index
		// is at least this new.
		return false
	}
	if exists && seq < ref.sseq {
		return false
	}
	if rm != nil {
		if exists {
			ref.sseq, ref.deleted = seq, false
		} else {
			as.retmsgs.Insert([]byte(subject), mqttRetainedMsgRef{sseq: seq})
		}
		delete(o.placeholders, subject)
		as.cacheRetainedLocked(subject, rm, origin)
		return true
	}
	as.rmsCache.Delete(subject)
	if origin == mqttRetainedFromObserver {
		// W reaches seq in this same critical section, so every straggler at or below it
		// is refused by the watermark: no placeholder is needed to remember the deletion.
		if exists {
			as.retmsgs.Delete(key)
		}
		delete(o.placeholders, subject)
		return exists
	}
	if exists {
		ref.sseq, ref.deleted = seq, true
	} else {
		as.retmsgs.Insert([]byte(subject), mqttRetainedMsgRef{sseq: seq, deleted: true})
	}
	if o.placeholders == nil {
		o.placeholders = make(map[string]uint64)
	}
	o.placeholders[subject] = seq
	return true
}

// applyRetained is applyRetainedLocked for a caller that does not hold as.mu.
func (as *mqttAccountSessionManager) applyRetained(subject string, seq uint64, rm *mqttRetainedMsg, origin mqttRetainedOrigin) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.applyRetainedLocked(subject, seq, rm, origin)
}

// retainedNoMessage is retainedNoMessageLocked for a caller that does not hold as.mu.
func (as *mqttAccountSessionManager) retainedNoMessage(subject string, c uint64) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.retainedNoMessageLocked(subject, c)
}

// cacheRetainedLocked writes the cached copy for an application the index accepted. A
// load owns its bytes and populates a cold cache; the observer and the seed only replace
// a copy that is already warm, and copy the bytes they do not own. as.mu held.
func (as *mqttAccountSessionManager) cacheRetainedLocked(subject string, rm *mqttRetainedMsg, origin mqttRetainedOrigin) {
	if origin == mqttRetainedFromLoad {
		as.setCachedRetainedMsg(subject, rm, false, false)
		return
	}
	as.setCachedRetainedMsg(subject, rm, true, true)
}

// retainedNoMessageLocked applies an authoritative load that found NO record for subject.
// The answer carries no sequence — the record was TTL-reaped or removed out of band, and
// no observer delivery will ever arrive for it — so it can only act on the sequence the
// index itself recorded, c, which is 0 for a literal filter forced into the load while
// unindexed. as.mu held.
func (as *mqttAccountSessionManager) retainedNoMessageLocked(subject string, c uint64) {
	// Nothing servable exists, whatever the index says.
	as.rmsCache.Delete(subject)
	if c == 0 || as.retmsgs == nil {
		// Nothing of ours was read, so nothing here may be removed: a record seeded
		// between the read and now is newer than this answer.
		return
	}
	key := stringToBytes(subject)
	ref, ok := as.retmsgs.Find(key)
	if !ok || ref.deleted || ref.sseq != c {
		return
	}
	o := &as.rmObs
	if c <= o.applied {
		as.retmsgs.Delete(key)
		return
	}
	// Above W the entry is also the ordering protection against an older straggler — a
	// delayed seed or load of a record between W and c — which would otherwise be accepted
	// over nothing and re-index a superseded value. Keep the sequence as a placeholder.
	ref.deleted = true
	if o.placeholders == nil {
		o.placeholders = make(map[string]uint64)
	}
	o.placeholders[subject] = c
}

// pruneRetainedPlaceholdersLocked drops every placeholder W has passed: the watermark now
// refuses what it protected against. Only an entry that is still that same placeholder is
// removed — a live entry that replaced it is never touched. as.mu held.
func (as *mqttAccountSessionManager) pruneRetainedPlaceholdersLocked() {
	o := &as.rmObs
	for subject, seq := range o.placeholders {
		if seq > o.applied {
			continue
		}
		delete(o.placeholders, subject)
		key := stringToBytes(subject)
		if ref, ok := as.retmsgs.Find(key); ok && ref.deleted && ref.sseq == seq {
			as.retmsgs.Delete(key)
		}
	}
}

// retainedIndexedLive reports whether subject is a replay candidate: indexed, and not a
// placeholder.
func (as *mqttAccountSessionManager) retainedIndexedLive(subject string) bool {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.retmsgs == nil {
		return false
	}
	ref, ok := as.retmsgs.Find(stringToBytes(subject))
	return ok && !ref.deleted
}

// commitRetainedDelivery is the observer's commit: the generation and the delivery
// sequence are re-validated, the delivery bookkeeping advanced and the index applied in
// ONE critical section. Classification and the test gate run before it, outside the lock,
// so a parked observer application never blocks a seed — and an application parked across
// a reset is refused here rather than committed into the new generation.
func (as *mqttAccountSessionManager) commitRetainedDelivery(sub *subscription, sseq, dseq uint64, subject string, rm *mqttRetainedMsg, apply bool) {
	as.mu.Lock()
	defer as.mu.Unlock()
	o := &as.rmObs
	if sub != o.sub || o.resetting {
		return
	}
	if sseq == 0 || dseq != o.lastDseq+1 {
		// A lost delivery, or a duplicate or renumbering after a consumer leader change.
		// Every application is arbitrated, so resuming from W is always safe.
		o.markResettingLocked("gap")
		return
	}
	o.lastDseq = dseq
	o.lastArrival = time.Now()
	if apply {
		as.applyRetainedLocked(subject, sseq, rm, mqttRetainedFromObserver)
	}
	if sseq > o.applied {
		o.applied = sseq
	}
	as.pruneRetainedPlaceholdersLocked()
}

// commitRetainedHeartbeat handles an idle heartbeat. It travels the consumer's outbound
// queue behind every delivery it summarizes, so a Nats-Last-Consumer beyond the last
// committed delivery is a delivery that never arrived.
func (as *mqttAccountSessionManager) commitRetainedHeartbeat(sub *subscription, lastConsumer uint64) {
	as.mu.Lock()
	defer as.mu.Unlock()
	o := &as.rmObs
	if sub != o.sub || o.resetting {
		return
	}
	o.lastArrival = time.Now()
	if lastConsumer > o.lastDseq {
		o.markResettingLocked("heartbeat")
	}
}

// mqttRetainedIsHeartbeat reports whether a message with no reply is a JetStream idle
// heartbeat. nats.go decodes the status line into a Status header, which frameNATSMsg
// preserves.
func mqttRetainedIsHeartbeat(h []byte) bool {
	return string(sliceHeader("Status", h)) == "100"
}

// mqttRetainedObserverConfig is the observer consumer. startSeq <= 1 is the initial
// DeliverAll create; anything later resumes at that stream sequence.
func mqttRetainedObserverConfig(name, deliverSubject, owner string, startSeq uint64, hb time.Duration) *CreateConsumerRequest {
	cfg := ConsumerConfig{
		Name:              name,
		FilterSubject:     mqttRetainedMsgsStreamSubject + ">",
		DeliverSubject:    deliverSubject,
		ReplayPolicy:      ReplayInstant,
		AckPolicy:         AckNone,
		InactiveThreshold: 5 * time.Minute,
		Heartbeat:         hb,
		Metadata:          mqttEphemeralConsumerMetadata(owner, mqttEphemeralRetainedObserver),
	}
	if startSeq > 1 {
		cfg.DeliverPolicy = DeliverByStartSequence
		cfg.OptStartSeq = startSeq
	}
	return &CreateConsumerRequest{Stream: mqttRetainedMsgsStreamName, Config: cfg}
}

// subscribeRetainedObserver subscribes one generation's deliver subject on the manager's
// internal client, under a sid no other subscription of that client uses.
func (as *mqttAccountSessionManager) subscribeRetainedObserver(deliverSubject string, g uint64) (*subscription, error) {
	sub, _, err := as.jsa.c.processSub([]byte(deliverSubject), nil, []byte("rmobs."+strconv.FormatUint(g, 10)), as.processRetainedMsg, false)
	return sub, err
}

// retainedObserverLoop owns every reset after initialization. It is started only once
// mqttProvisionEphemeralConsumers has succeeded: before that, initialization owns the
// consumer, its rollback and its subscription cleanup.
func (as *mqttAccountSessionManager) retainedObserverLoop(s *Server, closeCh chan struct{}) {
	hb := as.retainedObserverHeartbeat()
	as.mu.Lock()
	as.rmObs.lastArrival = time.Now()
	wake := as.rmObs.wake
	as.mu.Unlock()
	tt := time.NewTicker(hb)
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
		case <-wake:
		case <-closeCh:
			return
		case <-s.quitCh:
			return
		}
		as.retainedObserverTick(hb)
	}
}

// retainedObserverTick decides whether a reset is due and eligible, and runs at most one
// attempt. Eligibility is checked on every wake AND every tick, so neither a gap storm nor
// a failing JetStream turns the loop hot.
func (as *mqttAccountSessionManager) retainedObserverTick(hb time.Duration) {
	if sm := as.retainedSeams(); sm != nil && sm.testRetainedObserverHold != nil && sm.testRetainedObserverHold() {
		return
	}
	now := time.Now()
	as.mu.Lock()
	o := &as.rmObs
	if !o.resetting && now.Sub(o.lastArrival) > mqttRetainedLivenessBeats*hb {
		// No delivery and no heartbeat: reaped by InactiveThreshold, deleted out of band,
		// or the connection is down.
		o.markResettingLocked("liveness")
	}
	if !o.resetting || (!o.lastAttempt.IsZero() && now.Sub(o.lastAttempt) < hb) {
		as.mu.Unlock()
		return
	}
	o.lastAttempt = now
	o.resets++
	as.mu.Unlock()
	as.resetRetainedObserver(hb)
}

// resetRetainedObserver re-creates the observer from W+1. The consumer name never
// changes, so the per-run cardinality argument is the initialization's: the name is proved
// ABSENT before it is created again. Every failure leaves the observer resetting for the
// next eligible tick; mid-run there is no initialization to fail closed, and the index
// keeps serving what it holds.
func (as *mqttAccountSessionManager) resetRetainedObserver(hb time.Duration) {
	srv := as.jsa.c.srv
	id, owner := srv.ephemeralIdentity()
	name := mqttRetainedObserverName(id)
	sm := as.retainedSeams()

	var derr error
	if sm != nil && sm.testRetainedResetDeleteAmbiguous != nil && sm.testRetainedResetDeleteAmbiguous() {
		derr = errMQTTTestRetainedResetDeleteAmbiguous
	} else {
		derr = as.jsa.deleteEphemeralConsumerDefinite(mqttRetainedMsgsStreamName, name)
	}
	if derr != nil {
		as.jsa.c.Warnf("MQTT: retained observer reset deferred: %v", derr)
		return
	}

	g := srv.mqtt.rmObsGen.Add(1)
	dsubj := mqttRetainedObserverDeliverSubject(id, g)
	sub, err := as.subscribeRetainedObserver(dsubj, g)
	if err != nil {
		as.jsa.c.Warnf("MQTT: retained observer reset deferred: subscribe %q: %v", dsubj, err)
		return
	}

	// The new generation is installed BEFORE the create: the consumer may deliver before
	// its create response is processed.
	as.mu.Lock()
	o := &as.rmObs
	old := o.sub
	o.sub = sub
	o.lastDseq = 0
	o.lastArrival = time.Now()
	start := o.applied + 1
	o.resetStart = start
	o.resetting = false
	// A lost replacement whose record expired before this resume has nothing left to
	// redeliver, and its predecessor may still be cached: from here every SUBSCRIBE reads
	// the stream.
	as.rmsCache.Range(func(k, _ any) bool {
		as.rmsCache.Delete(k)
		return true
	})
	as.mu.Unlock()
	if old != nil {
		// Best effort: a subscription that outlives this is harmless, since nothing it
		// delivers passes the commit's generation check.
		if err := as.jsa.c.processUnsub(old.sid); err != nil {
			as.jsa.c.Warnf("MQTT: retained observer reset: unsubscribe previous generation: %v", err)
		}
	}

	if err := as.createRetainedObserverConsumer(mqttRetainedObserverConfig(name, dsubj, owner, start, hb)); err != nil {
		as.mu.Lock()
		if as.rmObs.sub == sub {
			// The next attempt's definite delete also reconciles a create that landed
			// with its response lost. An errored create licenses no rollback.
			as.rmObs.markResettingLocked("create")
		}
		as.mu.Unlock()
		as.jsa.c.Warnf("MQTT: retained observer reset: create %q: %v", name, err)
	}
}

// createRetainedObserverConsumer is the reset's create. The test hook runs inside it, after
// the server has answered and before the reset sees the outcome.
func (as *mqttAccountSessionManager) createRetainedObserverConsumer(ccr *CreateConsumerRequest) error {
	_, err := as.jsa.createEphemeralConsumerOwned(ccr)
	if sm := as.retainedSeams(); sm != nil && sm.testRetainedCreateHook != nil {
		err = sm.testRetainedCreateHook(err)
	}
	return err
}

// retainedLogicalSubject is the logical retained subject of a stream subject.
func retainedLogicalSubject(streamSubject string) string {
	return strings.TrimPrefix(streamSubject, mqttRetainedMsgsStreamSubject)
}
