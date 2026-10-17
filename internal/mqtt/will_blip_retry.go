// Copyright 2025 The mqtt-nats-broker Authors

package mqtt

import (
	"errors"
	"sync"
)

// A Will that has no durable home, held across a service-connection blip.
//
// Three Will routes build a SYNTHETIC outbox record and publish it immediately
// rather than enqueueing it: the immediate path, the owed-Will path a teardown
// that lost the session gate takes, and the deferred-close discharge. None of
// them writes to $MQTT_willq, so none of them leaves anything a later attempt
// could find -- and under auth passthrough all three publish through the internal
// account client, whose deliverWillRecord refuses while the service connection's
// identity is unavailable.
//
// Without this, an ordinary NATS blip therefore DESTROYS an authorized Will: the
// caller logs and returns, and nothing retries. That is a regression the auth
// passthrough work introduced rather than an inherent limit -- before it, an
// immediate Will fanned out through the still-live per-client connection and its
// store rode nats.go's reconnect buffer. It also contradicts the rule the rest of
// this code states plainly: an UNKNOWN destination must defer consumption of a
// Will, never destroy it. A deny is permanent and refuses; an outage is not.
//
// Why in memory, and why that is not a half-measure. The only durable park is
// $MQTT_willq, which is exactly what is unreachable while the service connection
// is down -- that is docs/CONFORMANCE.md residual 8's argument and it still holds.
// So this restores ordinary-blip behaviour and claims nothing more: a restart, a
// shutdown, or the capacity bound below still lose the Will, and each is recorded
// as a residual rather than implied to be handled.
const (
	// mqttOwedWillMaxBytes bounds what one broker will hold across a blip. It is a
	// byte bound rather than a count because the cost is the payloads: a thousand
	// small Wills are cheap and a hundred large ones are not.
	mqttOwedWillMaxBytes = 4 << 20
)

// mqttOwedImmediateWill is one authorized Will awaiting the return of the account
// it was authorized against.
//
// It carries the account inside rec.Account, which deliverWillRecord compares on
// discharge -- so this type stores no second copy of that fact. The account is the
// identity here, not the connection or the client: both are gone by now.
type mqttOwedImmediateWill struct {
	as   *mqttAccountSessionManager
	rec  *mqttWillOutbox
	size int
}

type mqttOwedWillQueue struct {
	// dischargeMu admits ONE discharge pass at a time and is held for the whole
	// pass, publishes included. That is the simplification two review rounds paid
	// for: an earlier version took a batch out of the queue, published it with the
	// queue unlocked, and put back what it kept. Every defect those rounds found
	// lived in that bookkeeping -- capacity handed out against records still in
	// flight, and a discharge request dropped because another pass held the batch.
	// Holding the pass lock instead makes the charge exact by construction and makes
	// a lost wake-up impossible: a second caller BLOCKS here and then runs its own
	// full pass, which by definition sees everything the first one left.
	dischargeMu sync.Mutex

	mu   sync.Mutex
	owed []*mqttOwedImmediateWill
	// bytes charges exactly what the queue holds. A record is uncharged only when
	// it leaves, and it leaves only when it is resolved.
	bytes int
	// kickPending coalesces waiters to one. A kick that arrives while a pass is
	// running is not dropped -- the flag is cleared as soon as the waiting pass
	// takes dischargeMu, before it runs, so anything that arrives during the pass
	// schedules a fresh waiter.
	kickPending bool
	// dropped counts Wills refused for capacity since start, for the operator log
	// and for a test to assert the bound is real rather than nominal.
	dropped uint64
}

// parkOwedImmediateWill retains a Will whose destination account is not currently
// established, to be discharged when it returns.
//
// Returns false when the Will could NOT be retained -- shutdown has begun, or the
// capacity bound is reached -- and a false return is a loss the caller must report.
// It is deliberately not silent: this function's whole purpose is to stop a Will
// disappearing without anyone saying so.
//
// The recheck-and-kick at the end is load-bearing. The reconnect that would
// discharge this obligation can complete between the refusal that produced it and
// the insertion below, and a discharge that ran in that window saw an empty queue.
// Asking again after inserting closes it: either the account is still away, and the
// eventual reconnect finds this entry, or it is back and this call discharges it.
func (s *Server) parkOwedImmediateWill(as *mqttAccountSessionManager, rec *mqttWillOutbox) bool {
	if as == nil || rec == nil {
		return false
	}
	// The same admission gate delayed-Will dispatch uses. A closed gate means
	// shutdown Phase 2 has begun and nothing new may be taken on.
	if !s.admitWillWorker() {
		return false
	}
	size := len(rec.Msg) + len(rec.Props) + len(rec.Topic)
	q := &s.owedWills
	q.mu.Lock()
	if q.bytes+size > mqttOwedWillMaxBytes {
		q.dropped++
		dropped, held := q.dropped, q.bytes
		q.mu.Unlock()
		s.willWorkerWG.Done()
		s.Errorf("MQTT: dropping the Will on %q: %d bytes of Wills are already held awaiting the return of their authorized account (%d dropped so far)",
			rec.Topic, held, dropped)
		return false
	}
	q.owed = append(q.owed, &mqttOwedImmediateWill{as: as, rec: rec, size: size})
	q.bytes += size
	q.mu.Unlock()
	// Released BEFORE the kick, which takes its own token. Holding one across the
	// kick would make shutdown wait on this call for the whole discharge, and the
	// kick could then find the gate closed with our token still outstanding.
	s.willWorkerWG.Done()

	// The kick, and it asks first. Scanning the backlog on every insertion would be
	// quadratic in the number of Wills an outage produces, for no gain: while the
	// destination is away every entry will refuse exactly as this one just did. The
	// only reason to look is that the destination is available NOW, which is
	// precisely the window this exists to close.
	if _, ok := s.serviceAccountForWill(); ok {
		s.kickOwedImmediateWills()
	}
	return true
}

// kickOwedImmediateWills asks for a discharge pass WITHOUT blocking the caller.
//
// Both callers need that. Parking runs on a teardown path, and a reconnect
// discharge runs inside a nats.go connection callback, which serialises with the
// disconnect handler -- blocking either one for the length of a publish pass would
// be paying for this mechanism with the thing it is supposed to protect.
//
// At most one pass runs and one waits. A second waiter would do exactly what the
// first will, because a pass sees everything left by the pass before it.
func (s *Server) kickOwedImmediateWills() {
	q := &s.owedWills
	q.mu.Lock()
	if q.kickPending {
		q.mu.Unlock()
		return
	}
	q.kickPending = true
	q.mu.Unlock()

	// Admitted like every other Will worker. A reconnect callback can complete long
	// after shutdown closed the gate; refusing here leaves the records for
	// discardOwedImmediateWills rather than publishing through infrastructure that
	// is being torn down.
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
		// Cleared once this pass OWNS the lock and before it runs, so a kick
		// arriving while it runs schedules a fresh waiter instead of being folded
		// into a pass that has already looked.
		q.mu.Lock()
		q.kickPending = false
		q.mu.Unlock()
		s.dischargeOwedImmediateWills()
	}()
}

// dischargeOwedImmediateWills re-attempts every retained Will whose account is the
// one the service connection is now bound to. Synchronous, and NOT admission-gated:
// the gate belongs to kickOwedImmediateWills, which is the only production caller.
// Anything else calling this directly would escape the shutdown lifecycle.
//
// deliverWillRecord's own account fence remains the authority: this does not decide
// that a Will may be published, it only asks again. A Will whose account is still
// not the current one stays parked -- which is why each record is asked
// individually rather than the pass giving up on the first refusal. A broker that
// came back in a DIFFERENT account must publish none of these and discard none of
// them either; the original account may yet return.
func (s *Server) dischargeOwedImmediateWills() {
	q := &s.owedWills
	for i := 0; ; i++ {
		q.mu.Lock()
		if i >= len(q.owed) {
			q.mu.Unlock()
			return
		}
		o := q.owed[i]
		q.mu.Unlock()

		// Published with the queue UNLOCKED but the record still in it and still
		// charged, so a concurrent park cannot be admitted against space this record
		// occupies. Nothing else removes entries -- dischargeMu admits one pass -- so
		// the only movement under us is an append at the tail, which this loop
		// reaches on a later turn.
		err := o.as.publishWillRecord(s, o.rec)
		if errors.Is(err, errMQTTWillDestinationUnavailable) {
			continue // still away, or away for THIS record's account: leave it charged
		}
		if err != nil {
			// Any other error may have followed a partial effect -- a fan-out that
			// reached some subscribers, a store that landed. Re-attempting it would
			// duplicate that, so the Will is dropped and the loss is reported.
			s.Errorf("MQTT: the Will on %q could not be published and is not retryable: %v", o.rec.Topic, err)
		}
		q.mu.Lock()
		for j, x := range q.owed {
			if x == o {
				q.owed = append(q.owed[:j], q.owed[j+1:]...)
				q.bytes -= o.size
				if j <= i {
					i-- // the removal shifted everything after it down
				}
				break
			}
		}
		q.mu.Unlock()
	}
}

// discardOwedImmediateWills reports and releases everything still held. Shutdown
// calls it after the dispatch gate is closed, so nothing can be parked behind it.
//
// It logs rather than publishing: the service connection is not necessarily back,
// and a Will published during shutdown into an account nobody has re-identified is
// the failure the account fence exists to prevent.
func (s *Server) discardOwedImmediateWills() {
	q := &s.owedWills
	q.mu.Lock()
	pending := q.owed
	q.owed, q.bytes = nil, 0
	q.mu.Unlock()
	for _, o := range pending {
		s.Errorf("MQTT: shutting down while still holding the Will on %q for account %q; it is not published",
			o.rec.Topic, o.rec.Account)
	}
}

// The retry publishes through publishWillRecord rather than deliverWillRecord
// directly, so a Will re-attempted after an outage goes through the SAME vehicle a
// delayed one does -- one publication path, one place a test can stand in for it.
// With no seam installed it delegates straight to deliverWillRecord, so production
// behaviour is unchanged and the account fence is still the authority.

// retainOrReportOwedWill is what every synthetic-record Will site does with a
// delivery error, so the three of them cannot drift apart on the one question that
// matters: is this Will recoverable, or is it gone?
//
// Only errMQTTWillDestinationUnavailable is recoverable, and only because that
// refusal happens before any fan-out and before any store. Everything else is
// reported as a loss rather than retried, because it may have followed a partial
// effect and asking again would duplicate it.
func (s *Server) retainOrReportOwedWill(as *mqttAccountSessionManager, rec *mqttWillOutbox, err error, what string) {
	if !errors.Is(err, errMQTTWillDestinationUnavailable) {
		s.Errorf("MQTT: unable to publish %s on %q: %v", what, rec.Topic, err)
		return
	}
	if !s.parkOwedImmediateWill(as, rec) {
		s.Errorf("MQTT: %s on %q is LOST: its authorized account %q is unavailable and it could not be retained",
			what, rec.Topic, rec.Account)
		return
	}
	s.Warnf("MQTT: holding %s on %q until its authorized account %q is established again", what, rec.Topic, rec.Account)
}
