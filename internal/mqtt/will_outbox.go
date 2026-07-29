// Layer 2: cross-replica delayed-Will durable outbox.
//
// A delayed Will (MQTT 5.0 Will Delay Interval > 0 with a surviving session) is
// recorded on the replicated $MQTT_willq stream so it still fires from a
// surviving replica after the originating broker exits — the primary target
// being a Kubernetes rolling restart. The guarantee is at-least-once logical
// publication: per-subject JetStream CAS (JSExpectedLastSubjSeq) is the
// safety/linearization mechanism (only one contender wins each state
// transition), while the wall-clock ClaimUntil lease provides liveness/failure
// recovery only — clock skew can widen contention into additional, already
// permitted duplicates. Literal exactly-once external publish is impossible.
//
// Each Will event lives on its own subject
// <mqttWillQSubjectPrefix><domainTk><cidHash>.<willID> (MaxMsgsPer:1), so a
// generation that has won pending->publishing stays independently recoverable
// while a fresh reconnect+disconnect enqueues a new generation.
//
// Firing model: every account manager runs a claim loop (dedicated ticker). A
// pending record whose FireAt has passed is CAS-transitioned pending->publishing
// by whichever replica's scan wins, which then dispatches a publisher worker.
// Leases matter only in the publishing state: a claimant that dies leaves a
// publishing record whose lease expires, letting a survivor recover and
// republish (the at-least-once duplicate). A reconnect CAS-transitions the
// client's pending records to cancelled; fire and reconnect compete for the same
// pending sequence and the CAS winner is authoritative.
//
// Durable termination: a successful publish is made durably terminal
// (publishing->done, retried against the record's latest sequence) BEFORE any
// cleanup. Only then is the record deleted (at the done sequence). Because done is
// committed first, a failed delete cannot leave a re-fireable publishing record —
// the scanner reaps the done record instead. See finalizeWillPublish.
//
// Takeover suppression has two layers. The primary is a durable per-client
// cancellation watermark on the dedicated $MQTT_willcancel stream: every accepted
// CONNECT advances the client's marker to its session save sequence (a gating
// write — no success CONNACK until it lands), stamped with the $MQTT_sess
// incarnation epoch. Each delayed Will records the origin connection's session
// sequence and epoch (OriginSessionSeq / OriginSessEpoch); the claimant fence
// suppresses the Will when a current-epoch marker is strictly beyond it
// (SessionSeq > OriginSessionSeq — the connection's own Will still fires), or when
// the marker is from a different incarnation. This survives finite-disconnect and
// session-record deletion, closing the reconnect-suppression residual that a
// session-record fence alone could not. The secondary, unchanged layer is the
// numeric session-record fence, used only as a fallback when no marker is present
// (administrative marker loss): it suppresses iff a newer record (sequence >
// OriginSessionSeq) shows a still-connected generation (DisconnectedAt == 0). See
// willTakeoverFence and will_cancel.go.
//
// Residual (why literal exactly-once remains impossible):
//   - at-least-once duplicate: a crash between the external publish and the done
//     commit republishes after the lease expires. This is the sanctioned duplicate.
//
// The reconnect-suppression residual is closed by the watermark and reopens only
// under administrative loss of $MQTT_willcancel (documented as a degradation, not a
// supported migration). This is a first-release format: pre-Layer-2 in-record
// Wills, migrated outbox records, and a session stream without a valid generated
// epoch are unsupported and rejected rather than carried forward.

package mqtt

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/nats-io/nuid"
)

// mqttWillState is the lifecycle state of an outbox record. Transitions are
// per-subject CAS writes; deletion (by exact sequence) is not a linearization
// primitive, so cancellation is the cancelled state, not a delete.
type mqttWillState string

const (
	mqttWillPending    mqttWillState = "pending"    // awaiting its FireAt / a claimant
	mqttWillPublishing mqttWillState = "publishing" // a claimant is publishing it
	mqttWillCancelled  mqttWillState = "cancelled"  // a reconnect suppressed it before firing
	mqttWillDone       mqttWillState = "done"       // published; native records are then deleted
)

// Claim lease and scan cadence. Vars (not consts) so tests can shorten them.
// Workers do not renew the lease; it bounds crash recovery, not a live publish.
// The lease sits well above the publish (JSA request) timeout so a healthy
// publish finishes inside it, and above the scan interval so a crashed
// publisher's record is reclaimed by a later scan rather than while it is still
// live. The interval is the fire granularity for a due Will.
var (
	mqttWillClaimLease    = 30 * time.Second
	mqttWillClaimInterval = 5 * time.Second
)

// Bounded retry counts for the enqueue and cancel JSA operations, which sit on
// the disconnect and reconnect paths respectively: a transient JetStream error
// must not silently lose a delayed Will (enqueue) or let a suppressed Will fire
// after a successful reconnect (cancel).
const (
	mqttWillEnqueueAttempts = 3
	mqttWillCancelAttempts  = 3
	// Bounded retry for the terminal publishing->done CAS. A transient/ambiguous
	// terminal failure must not leave a published record able to re-fire, so the
	// commit is retried against the record's latest sequence before giving up and
	// leaving it publishing for lease-expiry recovery (at-least-once, never loss).
	mqttWillFinalizeAttempts = 3
)

// mqttWillOutbox is a single delayed-Will event persisted on $MQTT_willq. One
// record per subject (MaxMsgsPer:1); the subject carries WillID so overlapping
// generations for the same client hash never evict each other.
type mqttWillOutbox struct {
	WillID       string        `json:"wid"`
	ClientIDHash string        `json:"cid"`
	Topic        string        `json:"topic"`
	Msg          []byte        `json:"msg,omitempty"`
	QoS          byte          `json:"qos,omitempty"`
	Retain       bool          `json:"retain,omitempty"`
	Props        []byte        `json:"props,omitempty"` // encoded forwardable Will properties
	User         string        `json:"user,omitempty"`
	FireAt       int64         `json:"fire"` // unix nanoseconds to publish at
	State        mqttWillState `json:"state"`
	ClaimOwner   string        `json:"owner,omitempty"` // node name of the current publisher
	ClaimUntil   int64         `json:"until,omitempty"` // unix nanoseconds publish lease expiry
	Account      string        `json:"acct,omitempty"`  // diagnostic only
	// OriginSessionSeq and OriginSessEpoch identify the accepted generation that
	// created this delayed Will: the persisted session-record stream sequence and
	// the $MQTT_sess incarnation epoch captured at the disconnect (mqtt.go:1942).
	// A sequence is comparable only within one incarnation, so the fence compares
	// OriginSessionSeq against a watermark/session sequence only when the epochs are
	// equal. Every native record carries a nonzero sequence and a valid epoch; a
	// zero sequence or empty/malformed epoch is corrupt and defers (there is no
	// legacy/migrated compatibility path in the first release). See
	// willTakeoverFence.
	OriginSessionSeq uint64 `json:"oseq,omitempty"`
	OriginSessEpoch  string `json:"oep,omitempty"`
}

// leaseValid reports whether the publishing claim lease is still valid at now.
func (r *mqttWillOutbox) leaseValid(now time.Time) bool {
	return r.ClaimOwner != _EMPTY_ && r.ClaimUntil > now.UnixNano()
}

// mqttWillQSubject returns the per-Will subject for a client hash + Will id.
func mqttWillQSubject(domainTk, cidHash, willID string) string {
	return mqttWillQSubjectPrefix + domainTk + cidHash + "." + willID
}

// mqttWillQClientFilter matches every Will event for one client hash (reconnect
// cancellation).
func mqttWillQClientFilter(domainTk, cidHash string) string {
	return mqttWillQSubjectPrefix + domainTk + cidHash + ".*"
}

// mqttWillQClientSubjectPrefix is the literal (non-wildcard) prefix every Will
// subject for one client hash shares. Used to sweep local fire timers by client,
// which mqttWillQClientFilter cannot do: that is a NATS subject filter, not a
// string prefix.
func mqttWillQClientSubjectPrefix(domainTk, cidHash string) string {
	return mqttWillQSubjectPrefix + domainTk + cidHash + "."
}

// mqttWillQAccountFilter matches every Will event for the account (claim scan).
func mqttWillQAccountFilter(domainTk string) string {
	return mqttWillQSubjectPrefix + domainTk + ">"
}

// storeWillOutboxCAS writes rec to subject expecting expectSeq as the subject's
// current last sequence: 0 requires the subject to be empty (create-only), a
// non-zero value requires that exact record still to be current. A concurrent
// writer that changed the subject wins and this write is rejected, giving the
// state machine its linearization point. Returns the new stream sequence.
func (jsa *mqttJSA) storeWillOutboxCAS(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
	b, _ := json.Marshal(rec)
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(expectSeq, 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	bb.Write(b)
	resp, err := jsa.storeMsg(subject, hdr, bb.Bytes())
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// loadWillOutbox returns the current record on subject and its stream sequence,
// or (nil, 0, nil) when the subject has no record. A transient load error is
// returned so callers can distinguish "absent" from "indeterminate".
func (jsa *mqttJSA) loadWillOutbox(subject string) (*mqttWillOutbox, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttWillQStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, err
		}
		return nil, 0, nil
	}
	rec := &mqttWillOutbox{}
	if err := json.Unmarshal(smsg.Data, rec); err != nil {
		return nil, 0, err
	}
	return rec, smsg.Sequence, nil
}

// armDelayedWillTimer schedules a local fire timer for a confirmed-pending
// outbox record, so the Will is published at its Will Delay Interval rather than
// whenever the claim loop next scans. The scan interval (mqttWillClaimInterval,
// 5s) is the fire granularity without this, which makes a short delay late by up
// to a full interval.
//
// Promptness only: the durable outbox stays the authority. If the gate is closed
// (shutdown), if the reload fails, or if the claim is lost, nothing is written
// and the record remains pending for a later scan or a peer.
func (as *mqttAccountSessionManager) armDelayedWillTimer(s *Server, subject string, fireAt int64) {
	delay := time.Until(time.Unix(0, fireAt))
	s.scheduleWillTimer(subject, delay, func() {
		// Reload rather than closing over the enqueue-time snapshot: a reconnect
		// may have cancelled the record and a peer may have claimed it, so the
		// snapshot's state and sequence can both be stale. This mirrors what
		// scanWillOutbox does for a single record.
		rec, seq, err := as.jsa.loadWillOutbox(subject)
		if err != nil || rec == nil {
			// Transient load failure or already reaped; the scan retries.
			return
		}
		now := time.Now()
		if rec.State != mqttWillPending || rec.FireAt > now.UnixNano() {
			return
		}
		// tryFireWill owns the takeover fence, the pending->publishing CAS, worker
		// admission and dispatch. Its willScanAbort result only tells a multi-record
		// scan to stop, so it is not meaningful for this single record.
		_ = as.tryFireWill(s, subject, rec, seq, now)
	})
}

// enqueueDelayedWill writes a new pending outbox record for a delayed Will. A
// local fire timer publishes it at FireAt; the claim loops (on every replica,
// including this one) are the backstop and fire it once FireAt passes.
// Called from the disconnect path; the Will's publish permission was checked
// before this and a denied Will is never enqueued.
//
// The outbox is the only durable fire mechanism, so a transient store failure
// must not silently drop the Will: the create is retried (idempotently — the
// WillID, and thus the subject, is fixed across attempts, so a create-only CAS
// that fails because a prior attempt's write actually landed is treated as
// success). If the durable enqueue still cannot be confirmed after the bounded
// retries, the Will is published IMMEDIATELY as a fail-safe (trading the Will
// Delay for not losing the Will) rather than left with no authority to fire it.
func (as *mqttAccountSessionManager) enqueueDelayedWill(s *Server, cidHash string, will *mqttWill, props []byte, user string, fireAt int64, originSessionSeq uint64, originSessEpoch string) error {
	willID := nuid.Next()
	subject := mqttWillQSubject(as.domainTk, cidHash, willID)
	rec := &mqttWillOutbox{
		WillID:           willID,
		ClientIDHash:     cidHash,
		Topic:            string(will.topic),
		Msg:              will.message,
		QoS:              will.qos,
		Retain:           will.retain,
		Props:            props,
		User:             user,
		FireAt:           fireAt,
		State:            mqttWillPending,
		Account:          as.jsa.c.acc.GetName(),
		OriginSessionSeq: originSessionSeq,
		OriginSessEpoch:  originSessEpoch,
	}
	var err error
	for attempt := 0; attempt < mqttWillEnqueueAttempts; attempt++ {
		newSeq, serr := as.storeWillEnqueueCAS(subject, 0, rec)
		if serr == nil {
			// Durability CONFIRMED by the create CAS. From here NO failure may fall
			// back to the immediate-publish fail-safe below, or the durable pending
			// record would fire AND the fail-safe would publish. Run the best-effort
			// early cancel on the known pending snapshot; any failure inside it is
			// swallowed and the claimant-side fence remains the durable safety net.
			// Arm BEFORE the early cancel so a cancel that lands releases the timer
			// (earlyCancelIfSuperseded -> cancelWillTimer). Arming afterwards would
			// leave a timer for an already-cancelled record alive until its FireAt,
			// which a long Will Delay makes effectively permanent.
			as.armDelayedWillTimer(s, subject, rec.FireAt)
			as.earlyCancelIfSuperseded(s, subject, rec, newSeq)
			return nil
		}
		err = serr
		// The create-only CAS may have failed because our own earlier attempt's
		// write landed but its ack was lost: re-read to resolve the ambiguity.
		existing, exSeq, lerr := as.jsa.loadWillOutbox(subject)
		if lerr != nil || existing == nil || existing.WillID != willID {
			continue // enqueue UNCONFIRMED: retry, else fall to the fail-safe
		}
		// Enqueue CONFIRMED (durable). Reconcile by the record's current state.
		if existing.State == mqttWillPending {
			// Still ours to suppress: run the best-effort early cancel. If a
			// claimant has since advanced it to publishing/done/cancelled we must
			// NOT write cancelled over it (that would revoke an irrevocable claim),
			// which is exactly what the state guard here and inside the fence avoid.
			// This record is durable and still pending, so it needs the same fire
			// timer the clean-CAS path gets; without it a Will whose ack was merely
			// lost would silently keep the full scan-interval latency. Only for a
			// pending record: a claimant that already advanced it to
			// publishing/cancelled/done does not need an initial fire. Armed before
			// the early cancel for the same reason as the clean-CAS path above.
			as.armDelayedWillTimer(s, subject, existing.FireAt)
			as.earlyCancelIfSuperseded(s, subject, existing, exSeq)
		}
		return nil
	}
	// Fail-safe: durable enqueue not confirmed. Publish now so the Will is not
	// lost; the delay is sacrificed. (For QoS 0 this succeeds even if the willq
	// store failed; for QoS 1/2 it hits the same JetStream store, so a sustained
	// outage still fails — logged at Error.)
	s.Warnf("MQTT: could not durably enqueue delayed Will for client hash %q after %d attempts (%v); publishing immediately as a fail-safe", cidHash, mqttWillEnqueueAttempts, err)
	if perr := as.publishWillRecord(s, rec); perr != nil {
		s.Errorf("MQTT: fail-safe immediate publish of delayed Will for client hash %q also failed: %v", cidHash, perr)
		return perr
	}
	return nil
}

// storeWillEnqueueCAS performs the enqueue's create-only CAS store, honoring an
// instance-scoped test seam that can inject a store failure to drive the
// immediate-publish fail-safe. Nil seam in production delegates straight to the
// JSA. Only the enqueue path is routed through here so an injected failure never
// perturbs the cancel/fire/migration stores on the same account manager.
func (as *mqttAccountSessionManager) storeWillEnqueueCAS(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
	as.mu.RLock()
	hook := as.testEnqueueStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, rec)
	}
	return as.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
}

// earlyCancelIfSuperseded runs the takeover fence right after a confirmed enqueue
// and, if a live takeover already superseded this connection, CAS-cancels the
// just-created pending record. Best-effort optimization only: a cancel CAS that
// loses to a concurrent state advance is harmless, and a defer (indeterminate
// session read) leaves the record pending — the claimant-side fence in tryFireWill
// is the durable safety net. rec must be pending on entry and is at CAS sequence
// seq.
func (as *mqttAccountSessionManager) earlyCancelIfSuperseded(s *Server, subject string, rec *mqttWillOutbox, seq uint64) {
	if as.willTakeoverFence(rec) != willFenceCancel {
		return
	}
	rec.State = mqttWillCancelled
	if _, err := as.jsa.storeWillOutboxCAS(subject, seq, rec); err != nil {
		s.Debugf("MQTT: early-cancel of superseded delayed Will %q lost/failed (claimant fence will resolve): %v", rec.WillID, err)
		// The CAS outcome is ambiguous: the cancelled write may have landed with a
		// lost ack, or another writer may have advanced or deleted the record. In
		// either of those the record is no longer pending and the timer is dead
		// weight until FireAt. Re-read to tell them apart, and keep the timer only
		// when the record is confirmed still pending (or the re-read itself is
		// indeterminate, where the claimant fence remains the safety net).
		cur, _, lerr := as.jsa.loadWillOutbox(subject)
		if lerr == nil && (cur == nil || cur.State != mqttWillPending) {
			s.cancelWillTimer(subject)
		}
		return
	}
	// Cancelled: release the local fire timer rather than leaving it armed until
	// FireAt for a record that will never fire.
	s.cancelWillTimer(subject)
}

// cancelWillOutbox suppresses a reconnecting client's pending delayed Wills by
// CAS-transitioning each pending record to cancelled. A record already
// publishing is irrevocable (that generation is due) and is left alone; the
// claim loop later reaps cancelled native records. It is the authoritative
// reconnect suppression, so a transient failure must not be mistaken for a
// completed cancellation: the scan is retried a bounded number of times and an
// error is returned if it could not confirm every pending record was resolved
// (cancelled, or already firing). Runs without locks (JSA I/O).
func (as *mqttAccountSessionManager) cancelWillOutbox(s *Server, cidHash string) error {
	// Instance-scoped test observer: fires after this cancel pass so the takeover
	// late-enqueue test can confirm the displaced broker's async cancel completed on
	// the empty outbox before a late enqueue is released. Nil in production.
	as.mu.RLock()
	done := as.testCancelWillDone
	as.mu.RUnlock()
	if done != nil {
		defer done()
	}
	filter := mqttWillQClientFilter(as.domainTk, cidHash)
	prefix := mqttWillQClientSubjectPrefix(as.domainTk, cidHash)
	var err error
	for attempt := 0; attempt < mqttWillCancelAttempts; attempt++ {
		// Capture this client's armed fire timers BEFORE the scan. A record the
		// scan never returns (cancelled by a peer, then reaped by a claim loop)
		// leaves a timer whose exact subject this broker can no longer name, so it
		// needs a sweep — but the sweep must not reach a timer armed after the
		// capture. The remote-takeover path really does interleave that way: an
		// enqueue blocked behind this scan can land right after it and arm a timer
		// for a record that is still pending, and stopping it would drop that Will
		// back to the 5s scan latency.
		captured := s.snapshotWillTimersWithPrefix(prefix)
		if err = as.cancelWillOutboxOnce(s, filter); err == nil {
			// Conclusively completed: nothing captured before the scan can still be
			// wanted. Deliberately not done after a failed pass — an indeterminate
			// scan may have left records pending, and their timers must survive.
			s.stopWillTimers(captured)
			return nil
		}
	}
	s.Errorf("MQTT: could not confirm delayed-Will cancellation for client hash %q after %d attempts: %v", cidHash, mqttWillCancelAttempts, err)
	return err
}

// cancelWillOutboxOnce performs one full scan+cancel pass. It returns an error
// only on an indeterminate outcome (a transient scan/load failure, or a CAS
// write failure where the record is still pending); a CAS lost to a concurrent
// fire (the record is no longer pending) is a resolved, non-error outcome.
func (as *mqttAccountSessionManager) cancelWillOutboxOnce(s *Server, filter string) error {
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttWillQStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return err // transient scan failure: indeterminate
			}
			return nil // no (more) records: scan complete
		}
		seq = smsg.Sequence + 1
		rec := &mqttWillOutbox{}
		if err := json.Unmarshal(smsg.Data, rec); err != nil {
			continue
		}
		if rec.State != mqttWillPending {
			// Already resolved by a fire or a peer: nothing to cancel, but if this
			// broker enqueued it we still hold a useless fire timer.
			s.cancelWillTimer(smsg.Subject)
			continue
		}
		rec.State = mqttWillCancelled
		if _, werr := as.jsa.storeWillOutboxCAS(smsg.Subject, smsg.Sequence, rec); werr != nil {
			// CAS failed. Re-read: if the record is no longer pending (a fire won,
			// or a peer cancelled it) it is resolved; if it is still pending, the
			// write failed transiently and this pass is indeterminate.
			cur, _, lerr := as.jsa.loadWillOutbox(smsg.Subject)
			if lerr != nil {
				return lerr
			}
			if cur != nil && cur.State == mqttWillPending {
				return werr
			}
			s.cancelWillTimer(smsg.Subject)
			continue
		}
		// Cancelled durably. Release the local fire timer so a long Will Delay does
		// not retain it (and the account manager it captures) until FireAt. A no-op
		// on a broker that did not enqueue this Will. This covers both a local
		// reconnect and a remote takeover, since the origin broker runs this scan
		// on the remote-disconnect path too.
		s.cancelWillTimer(smsg.Subject)
	}
}

// willClaimLoop periodically scans the account's outbox and drives due Wills to
// publication. Tracked by willClaimWG so shutdown Phase 2 drains it before
// waiting on publisher workers; it stops on the dispatch-gate signal, the
// account close channel, or server quit.
func (as *mqttAccountSessionManager) willClaimLoop(s *Server, closeCh chan struct{}) {
	defer s.willClaimWG.Done()
	as.scanWillOutbox(s) // initial pass so due Wills fire promptly after (re)start
	tt := time.NewTicker(mqttWillClaimInterval)
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
			as.scanWillOutbox(s)
		case <-closeCh:
			return
		case <-s.willClaimStopCh:
			return
		case <-s.quitCh:
			return
		}
	}
}

// scanWillOutbox walks every outbox record for the account and acts per state:
// a due pending record (or a publishing record whose lease expired) is claimed
// via CAS pending/publishing->publishing and dispatched to a worker; a terminal
// record (cancelled/done) is deleted. A tryFireWill that returns willScanAbort
// (a transient/global cancel-watermark load failure) stops the scan after leaving
// the record pending, so a marker-stream outage costs at most one JSA timeout per
// scan rather than one per due record; the next interval retries. willClaimStopCh
// is checked between records so Phase-2 shutdown does not continue a long scan.
func (as *mqttAccountSessionManager) scanWillOutbox(s *Server) {
	filter := mqttWillQAccountFilter(as.domainTk)
	now := time.Now()
	for seq := uint64(1); ; {
		select {
		case <-s.willClaimStopCh:
			return
		case <-s.quitCh:
			return
		default:
		}
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttWillQStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT will-outbox scan ended: %v", err)
			}
			return
		}
		seq = smsg.Sequence + 1
		rec := &mqttWillOutbox{}
		if err := json.Unmarshal(smsg.Data, rec); err != nil {
			continue
		}
		switch rec.State {
		case mqttWillPending:
			if rec.FireAt <= now.UnixNano() {
				if as.tryFireWill(s, smsg.Subject, rec, smsg.Sequence, now) == willScanAbort {
					return
				}
			}
		case mqttWillPublishing:
			if !rec.leaseValid(now) {
				as.tryFireWill(s, smsg.Subject, rec, smsg.Sequence, now) // crashed-publisher recovery
			}
		case mqttWillCancelled, mqttWillDone:
			_ = as.jsa.deleteMsg(mqttWillQStreamName, smsg.Sequence, false)
		}
	}
}

// willFenceDecision is the outcome of the takeover fence.
type willFenceDecision int

const (
	willFenceProceed willFenceDecision = iota // fire: no superseding generation observed
	willFenceCancel                           // suppress: a newer generation superseded this connection
	willFenceDefer                            // indeterminate/corrupt for THIS record: resolve later
	willFenceAbort                            // transient/global watermark-load failure: stop the scan
)

// willScanResult tells scanWillOutbox whether to keep walking records after a
// pending-record claim attempt, or to stop because a transient/global
// cancel-watermark load failure would otherwise cost one JSA timeout per record.
type willScanResult int

const (
	willScanContinue willScanResult = iota
	willScanAbort
)

// loadFenceSession loads and decodes the persisted session record for a client
// hash, honoring the instance-scoped test seam. Returns (nil, 0, nil) when no
// record exists (absent). A load error, or a JSON decode failure (malformed
// record), is surfaced as an error so the fence treats it as indeterminate
// (defer) and never as "absent".
func (as *mqttAccountSessionManager) loadFenceSession(cidHash string) (*mqttPersistedSession, uint64, error) {
	as.mu.RLock()
	hook := as.testSessionLoad
	as.mu.RUnlock()
	if hook != nil {
		return hook(cidHash)
	}
	smsg, err := as.jsa.loadSessionMsg(as.domainTk, cidHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, err // indeterminate load
		}
		return nil, 0, nil // absent
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return nil, 0, err // malformed: indeterminate
	}
	return ps, smsg.Sequence, nil
}

// willTakeoverFence decides whether a delayed Will must be suppressed because a
// newer generation superseded the connection that created it. The durable
// per-client cancel watermark (advanced at every accepted CONNECT) is the
// authoritative signal; the session record is only a fallback when no usable
// watermark exists. Epoch is an identity: a sequence is comparable only within the
// incarnation named by its epoch.
//
//   - defer: the record's origin is invalid (zero sequence, or empty/malformed
//     epoch). Every native record is epoch-known; acting on an invalid origin could
//     lose a legitimate Will or publish a suppressed one. There is no legacy/migrated
//     compatibility path in the first release.
//   - abort: the watermark load was transiently/globally indeterminate. The caller
//     stops the scan so a marker outage costs one JSA timeout, not one per record.
//   - defer: the watermark record itself is corrupt (this record only).
//   - cancel: a valid current-epoch watermark proves a newer accepted generation —
//     same epoch with SessionSeq > OriginSessionSeq, or a different (other-
//     incarnation) origin epoch, which a current-epoch CONNECT necessarily supersedes.
//   - otherwise (absent/stale watermark, or same-epoch watermark not newer): fall
//     back to the session record via sessionRecordFence.
func (as *mqttAccountSessionManager) willTakeoverFence(rec *mqttWillOutbox) willFenceDecision {
	if rec.OriginSessionSeq == 0 || !mqttValidSessEpoch(rec.OriginSessEpoch) {
		return willFenceDefer
	}
	wm, _, status := as.loadWillCancelWatermark(rec.ClientIDHash)
	switch status {
	case mqttWillCancelIndeterminate:
		return willFenceAbort
	case mqttWillCancelCorrupt:
		return willFenceDefer
	case mqttWillCancelValid:
		// wmValid implies wm.SessEpoch == as.sessStreamEpoch. If the Will's origin
		// epoch differs, it is from another incarnation and is superseded; if it
		// matches, compare sequences within that incarnation.
		if wm.SessEpoch != rec.OriginSessEpoch {
			return willFenceCancel
		}
		if wm.SessionSeq > rec.OriginSessionSeq {
			return willFenceCancel
		}
		return as.sessionRecordFence(rec)
	default: // mqttWillCancelAbsent, mqttWillCancelStaleEpoch
		return as.sessionRecordFence(rec)
	}
}

// sessionRecordFence is the pre-watermark session-record fence, retained as the
// fallback when no usable watermark exists (absent or stale epoch). It reads the
// current session record: a newer record (sequence > OriginSessionSeq) with
// DisconnectedAt == 0 (a newer accepted connection without a later finite-disconnect
// stamp) is a takeover -> cancel; absent/unchanged/newer-but-disconnected -> proceed;
// an unreadable record -> defer. Its numeric comparison is only meaningful within
// one $MQTT_sess incarnation; when the marker stream and session stream are recreated
// together (the supported operation) the origin epoch keeps the watermark path exact,
// and this fallback carries the documented residual only for unsupported partial
// recreation.
func (as *mqttAccountSessionManager) sessionRecordFence(rec *mqttWillOutbox) willFenceDecision {
	ps, seq, err := as.loadFenceSession(rec.ClientIDHash)
	if err != nil {
		return willFenceDefer
	}
	if ps == nil || seq <= rec.OriginSessionSeq {
		return willFenceProceed
	}
	if ps.DisconnectedAt == 0 {
		return willFenceCancel
	}
	return willFenceProceed
}

// tryFireWill claims a due/recoverable record by CAS-transitioning it to
// publishing with a fresh lease, then dispatches a publisher worker through the
// shutdown-aware admission gate. A lost CAS (a reconnect cancelled it, or a peer
// claimed it) is a no-op. If the gate is closed (shutdown Phase 2), the record
// is left publishing for post-restart recovery. Returns willScanAbort only when the
// pending-record fence hit a transient/global watermark-load failure, so the caller
// stops the scan; every other outcome returns willScanContinue.
func (as *mqttAccountSessionManager) tryFireWill(s *Server, subject string, rec *mqttWillOutbox, seq uint64, now time.Time) willScanResult {
	// Fence only the initial pending->publishing claim. A record that is already
	// publishing (crashed-publisher recovery) is an irrevocable, due generation:
	// the fence must never cancel a record a peer was mid-publishing.
	if rec.State == mqttWillPending {
		switch as.willTakeoverFence(rec) {
		case willFenceCancel:
			rec.State = mqttWillCancelled
			if _, err := as.jsa.storeWillOutboxCAS(subject, seq, rec); err != nil {
				// Lost to a concurrent fire/cancel, or a transient write failure: a
				// later scan re-resolves. Harmless either way.
				s.Debugf("MQTT: unable to cancel superseded delayed Will %q: %v", rec.WillID, err)
			}
			return willScanContinue
		case willFenceDefer:
			return willScanContinue // indeterminate/corrupt for this record: leave pending
		case willFenceAbort:
			return willScanAbort // transient/global marker outage: stop the scan
		}
	}
	rec.State = mqttWillPublishing
	rec.ClaimOwner = s.NodeName()
	rec.ClaimUntil = now.Add(mqttWillClaimLease).UnixNano()
	newSeq, err := as.jsa.storeWillOutboxCAS(subject, seq, rec)
	if err != nil {
		return willScanContinue
	}
	if !s.admitWillWorker() {
		return willScanContinue
	}
	go as.willPublishWorker(s, subject, rec, newSeq)
	return willScanContinue
}

// willPublishWorker publishes a claimed Will and then finalizes its record.
// Tracked by willWorkerWG (admitted by tryFireWill). On a publish error the
// record is left publishing so its lease expiry drives a retry (at-least-once).
// On success the record is made durably terminal (publishing->done) BEFORE any
// cleanup: only then is the record deleted (at the done sequence). Committing done
// first means a failed delete can no longer leave a re-fireable publishing record —
// the scanner reaps the done record instead.
//
// The worker does not renew its lease: the lease (mqttWillClaimLease, 30s) is
// sized well above the publish timeout (the JSA request timeout, seconds), so a
// normal publish finishes within it and no peer reclaims. A pathologically slow
// publish that exceeds the lease can be reclaimed and republished concurrently —
// a duplicate that the at-least-once guarantee already permits.
func (as *mqttAccountSessionManager) willPublishWorker(s *Server, subject string, rec *mqttWillOutbox, seq uint64) {
	defer s.willWorkerWG.Done()
	if err := as.publishWillRecord(s, rec); err != nil {
		s.Warnf("MQTT: delayed Will %q publish failed, will retry after lease expiry: %v", rec.WillID, err)
		return
	}
	doneSeq, ok := as.finalizeWillPublish(s, subject, rec, seq)
	if !ok {
		// Terminal state could not be confirmed. Leave the record publishing so its
		// lease expiry drives recovery: a post-lease duplicate is permitted, message
		// loss is not.
		return
	}
	if doneSeq == 0 {
		return // record already gone (a peer finalized+deleted it)
	}
	if err := as.jsa.deleteMsg(mqttWillQStreamName, doneSeq, true); err != nil {
		s.Debugf("MQTT: unable to delete published Will %q: %v", rec.WillID, err)
	}
}

// finalizeWillPublish commits a just-published record publishing->done and returns
// the stream sequence of the durable done record (and ok==true) so the caller can
// clean up at the authoritative sequence rather than the now-stale claim sequence.
// On a CAS failure it re-reads and reconciles (same WillID throughout):
//   - done: the write landed (ours, or a peer that also published) -> success at
//     the re-read sequence.
//   - record absent: already finalized and deleted by a peer -> success with seq 0
//     (nothing left to delete).
//   - still publishing (our own failed/ambiguous CAS left it unchanged, or a peer
//     reclaimed it after lease expiry): retry the done CAS at the re-read sequence.
//   - a foreign WillID, or an unexpected state (cancelled/pending): abandon
//     (ok==false); the record is not ours to finalize.
//
// If done cannot be confirmed within the bounded budget, returns ok==false and the
// caller leaves the record publishing for lease-expiry recovery.
func (as *mqttAccountSessionManager) finalizeWillPublish(s *Server, subject string, rec *mqttWillOutbox, seq uint64) (uint64, bool) {
	rec.State = mqttWillDone
	for attempt := 0; attempt < mqttWillFinalizeAttempts; attempt++ {
		newSeq, err := as.storeWillFinalizeCAS(subject, seq, rec)
		if err == nil {
			return newSeq, true
		}
		// CAS failed (transiently, on a stale sequence, or via an ambiguous seam
		// that may have written anyway). Re-read to learn the durable truth.
		cur, curSeq, lerr := as.jsa.loadWillOutbox(subject)
		if lerr != nil {
			continue // indeterminate load: retry
		}
		if cur == nil {
			return 0, true // record already finalized+deleted by a peer
		}
		if cur.WillID != rec.WillID {
			s.Warnf("MQTT: delayed Will %q finalize found a foreign record on %q (state %q, wid %q); abandoning finalize", rec.WillID, subject, cur.State, cur.WillID)
			return 0, false
		}
		switch cur.State {
		case mqttWillDone:
			return curSeq, true // the done write landed (ours or a peer's)
		case mqttWillPublishing:
			seq = curSeq // retry the commit at the record's current sequence
		default:
			s.Warnf("MQTT: delayed Will %q finalize found unexpected state %q on %q; abandoning finalize", rec.WillID, cur.State, subject)
			return 0, false
		}
	}
	return 0, false
}

// storeWillFinalizeCAS performs the terminal publishing->done CAS store, honoring
// an instance-scoped test seam. Nil seam in production delegates straight to the
// JSA. Only the finalize path is routed through here so an injected failure never
// perturbs the enqueue/cancel/claim stores on the same account manager.
func (as *mqttAccountSessionManager) storeWillFinalizeCAS(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
	as.mu.RLock()
	hook := as.testFinalizeStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, rec)
	}
	return as.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
}

// publishWillRecord publishes a Will record, honoring the instance-scoped test
// seam. In production (nil seam) it delegates straight to deliverWillRecord; a
// test seam may replace the publication entirely, or call deliverWillRecord
// itself (e.g. to deliver for real and then report an ambiguous outcome).
func (as *mqttAccountSessionManager) publishWillRecord(s *Server, rec *mqttWillOutbox) error {
	as.mu.RLock()
	hook := as.testPublishWill
	as.mu.RUnlock()
	if hook != nil {
		return hook(rec)
	}
	return as.deliverWillRecord(s, rec)
}

// deliverWillRecord fans the Will out via a short-lived internal account client
// and the account JSA (mirroring the immediate-Will path), tagging nothing with
// a No Local origin since a Will is server-originated. The QoS1/2 store is bound
// by the JSA request timeout; this is the same publish core the immediate-Will
// path uses, so it inherits that path's bounding rather than blocking forever.
func (as *mqttAccountSessionManager) deliverWillRecord(s *Server, rec *mqttWillOutbox) error {
	subject, err := mqttTopicToNATSPubSubject([]byte(rec.Topic))
	if err != nil {
		// A malformed topic can never be published; drop it rather than retry
		// forever. Returning nil lets the caller finalize (delete).
		s.Warnf("MQTT: dropping delayed Will %q with invalid topic %q: %v", rec.WillID, rec.Topic, err)
		return nil
	}
	c := s.createInternalAccountClient()
	c.acc = as.jsa.c.acc
	c.opts.Username = rec.User
	pp := &mqttPublish{
		topic:   []byte(rec.Topic),
		subject: subject,
		msg:     rec.Msg,
		sz:      len(rec.Msg),
		flags:   rec.QoS << 1,
		props:   rec.Props,
	}
	if rec.Retain {
		pp.flags |= mqttPubFlagRetain
	}
	c.mqtt = &mqtt{pp: pp, asm: as, proto: mqttProtoLevel5}
	err = s.mqttInitiateMsgDeliveryJSA(c, &as.jsa, pp, _EMPTY_, _EMPTY_)
	c.flushClients(0)
	return err
}
