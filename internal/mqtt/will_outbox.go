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
	ClaimOwner   string        `json:"owner,omitempty"`    // node name of the current publisher
	ClaimUntil   int64         `json:"until,omitempty"`    // unix nanoseconds publish lease expiry
	Migrated     bool          `json:"migrated,omitempty"` // legacy-migrated: terminal records are tombstoned, not deleted
	Account      string        `json:"acct,omitempty"`     // diagnostic only
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

// enqueueDelayedWill writes a new pending outbox record for a delayed Will. The
// claim loops (on every replica, including this one) fire it once FireAt passes.
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
func (as *mqttAccountSessionManager) enqueueDelayedWill(s *Server, cidHash string, will *mqttWill, props []byte, user string, fireAt int64) error {
	willID := nuid.Next()
	subject := mqttWillQSubject(as.domainTk, cidHash, willID)
	rec := &mqttWillOutbox{
		WillID:       willID,
		ClientIDHash: cidHash,
		Topic:        string(will.topic),
		Msg:          will.message,
		QoS:          will.qos,
		Retain:       will.retain,
		Props:        props,
		User:         user,
		FireAt:       fireAt,
		State:        mqttWillPending,
		Account:      as.jsa.c.acc.GetName(),
	}
	var err error
	for attempt := 0; attempt < mqttWillEnqueueAttempts; attempt++ {
		if _, err = as.storeWillEnqueueCAS(subject, 0, rec); err == nil {
			return nil
		}
		// The create-only CAS may have failed because our own earlier attempt's
		// write landed but its ack was lost: if the subject already holds this
		// event, the enqueue succeeded.
		if existing, _, lerr := as.jsa.loadWillOutbox(subject); lerr == nil && existing != nil && existing.WillID == willID {
			return nil
		}
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

// cancelWillOutbox suppresses a reconnecting client's pending delayed Wills by
// CAS-transitioning each pending record to cancelled. A record already
// publishing is irrevocable (that generation is due) and is left alone; the
// claim loop later reaps cancelled native records. It is the authoritative
// reconnect suppression, so a transient failure must not be mistaken for a
// completed cancellation: the scan is retried a bounded number of times and an
// error is returned if it could not confirm every pending record was resolved
// (cancelled, or already firing). Runs without locks (JSA I/O).
func (as *mqttAccountSessionManager) cancelWillOutbox(s *Server, cidHash string) error {
	filter := mqttWillQClientFilter(as.domainTk, cidHash)
	var err error
	for attempt := 0; attempt < mqttWillCancelAttempts; attempt++ {
		if err = as.cancelWillOutboxOnce(s, filter); err == nil {
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
		if err := json.Unmarshal(smsg.Data, rec); err != nil || rec.State != mqttWillPending {
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
		}
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
// via CAS pending/publishing->publishing and dispatched to a worker; a native
// terminal record (cancelled/done) is deleted; migrated terminal records are
// tombstoned (kept) so a lagging migrator cannot resurrect a fired Will.
func (as *mqttAccountSessionManager) scanWillOutbox(s *Server) {
	filter := mqttWillQAccountFilter(as.domainTk)
	now := time.Now()
	for seq := uint64(1); ; {
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
				as.tryFireWill(s, smsg.Subject, rec, smsg.Sequence, now)
			}
		case mqttWillPublishing:
			if !rec.leaseValid(now) {
				as.tryFireWill(s, smsg.Subject, rec, smsg.Sequence, now) // crashed-publisher recovery
			}
		case mqttWillCancelled, mqttWillDone:
			if !rec.Migrated {
				_ = as.jsa.deleteMsg(mqttWillQStreamName, smsg.Sequence, false)
			}
		}
	}
}

// tryFireWill claims a due/recoverable record by CAS-transitioning it to
// publishing with a fresh lease, then dispatches a publisher worker through the
// shutdown-aware admission gate. A lost CAS (a reconnect cancelled it, or a peer
// claimed it) is a no-op. If the gate is closed (shutdown Phase 2), the record
// is left publishing for post-restart recovery.
func (as *mqttAccountSessionManager) tryFireWill(s *Server, subject string, rec *mqttWillOutbox, seq uint64, now time.Time) {
	rec.State = mqttWillPublishing
	rec.ClaimOwner = s.NodeName()
	rec.ClaimUntil = now.Add(mqttWillClaimLease).UnixNano()
	newSeq, err := as.jsa.storeWillOutboxCAS(subject, seq, rec)
	if err != nil {
		return
	}
	if !s.admitWillWorker() {
		return
	}
	go as.willPublishWorker(s, subject, rec, newSeq)
}

// willPublishWorker publishes a claimed Will and then finalizes its record.
// Tracked by willWorkerWG (admitted by tryFireWill). On a publish error the
// record is left publishing so its lease expiry drives a retry (at-least-once);
// on success a native record is deleted and a migrated record is tombstoned as
// done.
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
	if rec.Migrated {
		rec.State = mqttWillDone
		if _, err := as.jsa.storeWillOutboxCAS(subject, seq, rec); err != nil {
			s.Debugf("MQTT: unable to tombstone migrated Will %q: %v", rec.WillID, err)
		}
		return
	}
	if err := as.jsa.deleteMsg(mqttWillQStreamName, seq, true); err != nil {
		s.Debugf("MQTT: unable to delete published Will %q: %v", rec.WillID, err)
	}
}

// publishWillRecord fans the Will out via a short-lived internal account client
// and the account JSA (mirroring the immediate-Will path), tagging nothing with
// a No Local origin since a Will is server-originated. The QoS1/2 store is bound
// by the JSA request timeout; this is the same publish core the immediate-Will
// path uses, so it inherits that path's bounding rather than blocking forever.
func (as *mqttAccountSessionManager) publishWillRecord(s *Server, rec *mqttWillOutbox) error {
	// Instance-scoped test seam: replace the actual publication for this account
	// manager only (used to inject publish failures in recovery tests). Nil in
	// production.
	as.mu.RLock()
	hook := as.testPublishWill
	as.mu.RUnlock()
	if hook != nil {
		return hook(rec)
	}
	subject, err := mqttTopicToNATSPubSubject([]byte(rec.Topic))
	if err != nil {
		// A malformed topic can never be published; drop it rather than retry
		// forever. Returning nil lets the caller finalize (delete/tombstone).
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
	err = s.mqttInitiateMsgDeliveryJSA(c, &as.jsa, pp, _EMPTY_)
	c.flushClients(0)
	return err
}

// migrateWillsToOutbox converts delayed Wills persisted in legacy session records
// (mqttPersistedSession.Will, written by a pre-Layer-2 build) into outbox events,
// then clears the Will from the session record. Idempotent across replicas and
// restarts: the WillID is derived deterministically from the account, client
// hash, and FireAt, and creation is create-only CAS, so at most one replica wins
// and a terminal tombstone blocks re-creation of an already-fired Will. Runs once
// at account-manager start, before the claim loop's first scan.
func (as *mqttAccountSessionManager) migrateWillsToOutbox(s *Server) {
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT legacy-Will migration scan ended: %v", err)
			}
			return
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil || ps.Will == nil {
			continue
		}
		cidHash := getHash(ps.ID)
		w := ps.Will
		willID := "mig-" + getHash(as.domainTk+cidHash+strconv.FormatInt(w.FireAt, 10))
		rec := &mqttWillOutbox{
			WillID:       willID,
			ClientIDHash: cidHash,
			Topic:        w.Topic,
			Msg:          w.Msg,
			QoS:          w.QoS,
			Retain:       w.Retain,
			Props:        w.Props,
			User:         w.User,
			FireAt:       w.FireAt,
			State:        mqttWillPending,
			Migrated:     true,
			Account:      as.jsa.c.acc.GetName(),
		}
		// Create-only CAS. The legacy copy may be cleared ONLY once the outbox
		// durably holds this event — otherwise a transient store failure would
		// remove the sole durable copy of the Will. On a create error, verify the
		// subject already holds the matching event (a peer migrated it, or a
		// terminal tombstone from a fired migrated Will); if not (transient
		// failure), leave the legacy record for a later migration pass (a
		// subsequent account-manager start).
		subject := mqttWillQSubject(as.domainTk, cidHash, willID)
		if _, err := as.jsa.storeWillOutboxCAS(subject, 0, rec); err != nil {
			existing, _, lerr := as.jsa.loadWillOutbox(subject)
			if lerr != nil || existing == nil || existing.WillID != willID {
				continue // indeterminate: keep the legacy Will for a later pass
			}
		}
		as.clearLegacyWill(s, ps, cidHash, smsg.Sequence)
	}
}

// clearLegacyWill removes the Will from a legacy session record after migration:
// a clean-session record that existed only to carry the Will is deleted, else the
// Will field is cleared with an expected-sequence CAS (a concurrent writer wins).
func (as *mqttAccountSessionManager) clearLegacyWill(s *Server, ps *mqttPersistedSession, cidHash string, seq uint64) {
	if ps.Clean {
		if err := as.jsa.deleteMsg(mqttSessStreamName, seq, true); err != nil {
			s.Debugf("MQTT: unable to delete migrated clean-session Will record for %q: %v", ps.ID, err)
		}
		return
	}
	ps.Will = nil
	if err := as.storeSessionRecordCAS(ps, cidHash, seq); err != nil {
		s.Debugf("MQTT: unable to clear migrated legacy Will for %q: %v", ps.ID, err)
	}
}
