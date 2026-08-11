// Durable per-client delayed-Will cancellation watermark.
//
// Every accepted CONNECT advances a permanent per-client marker on the dedicated
// $MQTT_willcancel stream to the connection's session save sequence (a durable,
// gating write: no success CONNACK is sent until it lands). The takeover fence then
// suppresses any older-generation Will whose origin sequence is below the marker —
// even after the session record has been finite-disconnected or deleted, which the
// pre-existing session-record fence could not do. See willTakeoverFence.
//
// Sequences are only comparable within one $MQTT_sess incarnation, identified by a
// generated epoch (form "nuid:<NUID>") stored in the session stream metadata and
// carried on both the watermark (SessEpoch) and each native Will record
// (OriginSessEpoch). The epoch is an identity token: equality means "same
// incarnation, sequences comparable"; inequality means the record is from a prior
// incarnation and a current-epoch CONNECT supersedes it.

package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nuid"
)

// Bounded retry for the CONNECT-side watermark advance. A transient/ambiguous
// store failure is retried against the marker's latest sequence before failing the
// CONNECT (strict gating); an ambiguous write that actually landed is confirmed by
// re-read rather than retried into a spurious failure.
const mqttWillCancelAdvanceAttempts = 3

// mqttWillCancelWatermark is the durable per-client marker on $MQTT_willcancel.
// One record per client subject (MaxMsgsPer:1). SessionSeq is the highest accepted
// CONNECT session save sequence for the client within the SessEpoch incarnation.
type mqttWillCancelWatermark struct {
	ClientIDHash string `json:"cid"`
	SessionSeq   uint64 `json:"seq"`
	SessEpoch    string `json:"epoch"`
}

// mqttWillCancelStatus is the typed outcome of a watermark load, distinct from the
// fence decision so the caller can act precisely (self-heal, fall back, defer this
// record, or abort the scan).
type mqttWillCancelStatus int

const (
	mqttWillCancelAbsent        mqttWillCancelStatus = iota // no marker for this client
	mqttWillCancelValid                                     // decoded, correct cid, valid current-epoch marker
	mqttWillCancelStaleEpoch                                // decoded, valid form, but a different incarnation
	mqttWillCancelCorrupt                                   // decode failure / wrong cid / invalid epoch (seq known)
	mqttWillCancelIndeterminate                             // transient load failure
)

// mqttWillCancelSubject returns the per-client watermark subject.
func mqttWillCancelSubject(domainTk, cidHash string) string {
	return mqttWillCancelSubjectPrefix + domainTk + cidHash
}

// mqttGenSessEpoch generates a fresh session-stream incarnation epoch.
func mqttGenSessEpoch() string {
	return mqttSessEpochNUIDPrefix + nuid.Next()
}

// mqttValidSessEpoch reports whether s is a well-formed epoch: exactly
// "nuid:<22 base-62 chars>". Empty or any other form is invalid (it collapses no
// value into the identity space and must never be trusted or compared). The body
// must be 22 characters drawn from the NUID base-62 alphabet [0-9A-Za-z] — length
// alone is insufficient, since a same-length body of out-of-alphabet bytes would
// otherwise be treated as a valid identity and (having a different value than the
// current epoch) drive a spurious cross-incarnation cancel instead of a defer.
func mqttValidSessEpoch(s string) bool {
	if !strings.HasPrefix(s, mqttSessEpochNUIDPrefix) {
		return false
	}
	body := s[len(mqttSessEpochNUIDPrefix):]
	if len(body) != 22 { // nuid.Next() is 22 chars
		return false
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// validateWillCancelStream verifies a $MQTT_willcancel stream carries the exact
// permanent-marker configuration. Derived from what this owner does: a
// per-client watermark CAS (advance / self-heal at a known sequence) and
// loadLastMsgFor reads — and NOTHING else. Global message/byte/age limits would
// eventually evict a permanent marker; DiscardNew would reject the CAS
// replacement of a client's prior watermark; a source could occupy a client's
// marker slot; a size cap would reject the watermark write that gates every
// CONNECT.
//
// No operation ever deletes or purges on this stream, by design (a watermark
// is permanent; see docs/ARCHITECTURE.md), so DenyDelete and DenyPurge are
// audited NON-prerequisites — do not "finish" this list with them. For the
// same reason SubjectDeleteMarkerTTL is waived: with no delete, no delete
// marker can ever be created, so the check would not be derived from any owner
// operation. No consumer is ever created either, so MaxConsumers and
// ConsumerLimits are equally waived.
//
// wantReplicas is 0 when no explicit replica count is configured (skip that check).
func validateWillCancelStream(si *StreamInfo, domainTk string, wantReplicas int) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, mqttWillCancelSubjectPrefix+domainTk+">", wantReplicas); err != nil {
		return err
	}
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits): nothing consumes a watermark, so any other retention drops it immediately", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		return fmt.Errorf("discard is %v (want old): DiscardNew would reject the CAS replacement of a client's prior watermark", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		return fmt.Errorf("max_msgs_per_subject is %d (want 1): the per-client marker is a single-slot register its CAS depends on", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict a permanent marker", cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a client's marker slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject the watermark write that gates every CONNECT", cfg.MaxMsgSize)
	}
	return nil
}

// storeWillCancelCAS writes wm to subject expecting expectSeq as the subject's
// current last sequence (0 = create-only), mirroring storeWillOutboxCAS. Returns
// the new stream sequence.
func (jsa *mqttJSA) storeWillCancelCAS(subject string, expectSeq uint64, wm *mqttWillCancelWatermark) (uint64, error) {
	b, _ := json.Marshal(wm)
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

// loadWillCancelWatermark loads the current per-client watermark and classifies it,
// honoring the instance-scoped test seam. The subject stream sequence is returned
// on every non-absent outcome (available pre-decode) so the CONNECT-side advance can
// CAS-repair a corrupt/stale marker at its exact sequence.
func (as *mqttAccountSessionManager) loadWillCancelWatermark(cidHash string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus) {
	as.mu.RLock()
	hook := as.testCancelWatermarkLoad
	as.mu.RUnlock()
	if hook != nil {
		return hook(cidHash)
	}
	subject := mqttWillCancelSubject(as.domainTk, cidHash)
	smsg, err := as.jsa.loadLastMsgFor(mqttWillCancelStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, mqttWillCancelIndeterminate
		}
		return nil, 0, mqttWillCancelAbsent
	}
	wm := &mqttWillCancelWatermark{}
	if err := json.Unmarshal(smsg.Data, wm); err != nil {
		return nil, smsg.Sequence, mqttWillCancelCorrupt
	}
	if wm.ClientIDHash != cidHash || !mqttValidSessEpoch(wm.SessEpoch) {
		return wm, smsg.Sequence, mqttWillCancelCorrupt
	}
	if wm.SessEpoch != as.sessStreamEpoch {
		return wm, smsg.Sequence, mqttWillCancelStaleEpoch
	}
	return wm, smsg.Sequence, mqttWillCancelValid
}

// storeWillCancelWatermarkCAS performs the watermark CAS store, honoring the
// instance-scoped test seam. Only the advance path is routed through here so an
// injected failure never perturbs the fence's watermark loads.
func (as *mqttAccountSessionManager) storeWillCancelWatermarkCAS(subject string, expectSeq uint64, wm *mqttWillCancelWatermark) (uint64, error) {
	as.mu.RLock()
	hook := as.testCancelWatermarkStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, wm)
	}
	return as.jsa.storeWillCancelCAS(subject, expectSeq, wm)
}

// advanceWillCancelWatermark durably advances the client's watermark to
// max(current, candidate) within the current epoch, stamping the current epoch.
// candidate is the just-saved CONNECT session sequence (read under es.mu). It is a
// load / CAS / re-read loop:
//   - absent -> create-only CAS.
//   - valid & SessionSeq >= candidate -> already satisfied (the max rule tolerates
//     out-of-order replica writes).
//   - valid & SessionSeq < candidate -> CAS-replace at the marker's sequence.
//   - stale-epoch / corrupt -> CAS-overwrite at the known sequence (a newer epoch is
//     authoritative; self-heal).
//   - indeterminate / lost CAS -> reload and retry (bounded).
//   - ambiguous write (store errored but may have landed) -> a re-read showing a
//     valid current-epoch marker with SessionSeq >= candidate confirms success.
//
// It returns an error only when durability cannot be confirmed within the bounded
// attempts; the caller then rejects the CONNECT (strict gating) and runs the
// failure transaction. It never lowers or removes a marker.
func (as *mqttAccountSessionManager) advanceWillCancelWatermark(cidHash string, candidate uint64) error {
	subject := mqttWillCancelSubject(as.domainTk, cidHash)
	want := &mqttWillCancelWatermark{ClientIDHash: cidHash, SessionSeq: candidate, SessEpoch: as.sessStreamEpoch}
	var lastErr error
	for attempt := 0; attempt < mqttWillCancelAdvanceAttempts; attempt++ {
		wm, seq, status := as.loadWillCancelWatermark(cidHash)
		var expect uint64
		switch status {
		case mqttWillCancelValid:
			if wm.SessionSeq >= candidate {
				return nil // already at/above the candidate (max rule)
			}
			expect = seq
		case mqttWillCancelAbsent:
			expect = 0
		case mqttWillCancelStaleEpoch, mqttWillCancelCorrupt:
			expect = seq // overwrite the other-incarnation/corrupt marker at its sequence
		default: // mqttWillCancelIndeterminate
			lastErr = fmt.Errorf("watermark load indeterminate")
			continue
		}
		if _, err := as.storeWillCancelWatermarkCAS(subject, expect, want); err == nil {
			return nil
		} else {
			lastErr = err
			// The write may have landed (ambiguous), or lost a CAS to a concurrent
			// higher write; a re-read confirming the durable truth resolves both.
			if as.watermarkAtLeast(cidHash, candidate) {
				return nil
			}
		}
	}
	return fmt.Errorf("could not durably advance cancel watermark for client hash %q after %d attempts: %w", cidHash, mqttWillCancelAdvanceAttempts, lastErr)
}

// watermarkAtLeast reports whether the durable marker is a valid current-epoch
// watermark whose SessionSeq is at least candidate (i.e. a prior/ambiguous write
// achieved the intended suppression). Used to confirm an ambiguous advance.
func (as *mqttAccountSessionManager) watermarkAtLeast(cidHash string, candidate uint64) bool {
	wm, _, status := as.loadWillCancelWatermark(cidHash)
	return status == mqttWillCancelValid && wm.SessionSeq >= candidate
}

// rollbackResult classifies the durable outcome of the CONNECT-accept rollback.
type rollbackResult int

const (
	rollbackApplied       rollbackResult = iota // candidate is absent or an authoritative disconnected record
	rollbackSuperseded                          // a newer connected generation is authoritative (a legitimate success)
	rollbackIndeterminate                       // candidate still connected, or the record is unreadable
	// rollbackForked: a newer CONNECTED record exists that is PROVABLY a sibling of the
	// candidate — same parent, different identity — so it is no successor of it and
	// "a legitimate newer generation" is not established. Distinguished from
	// rollbackSuperseded because that verdict CANCELS the expiry timer, which is
	// durable state changed on an unsupported conclusion; and from
	// rollbackIndeterminate because nothing about the store was unavailable, so the
	// "session store unavailable" warning would be a report of a failure that did not
	// happen. See sess_fork_lineage.go.
	rollbackForked
)

// rollbackAcceptedSession is the CONNECT-accept watermark-failure transaction. The
// session was already durably saved (DisconnectedAt == 0) but the watermark advance
// could not be confirmed, so the CONNECT is rejected. removeSession alone cannot
// undo the durable record, so this makes the candidate generation non-falsely-
// connected and reconciles local state.
//
// Unconditional guarantees (the caller sends no success CONNACK): es.c == nil, the
// in-memory session is removed, and the rollback is classified applied / superseded
// / indeterminate. The persisted record being non-connected is NOT unconditional —
// under rollbackIndeterminate ($MQTT_sess unavailable) it may remain durably
// connected; recovery is then the periodic expiry sweep (finite sessions only) or a
// later reconnect. The prior connection's Will is not guaranteed to remain eligible:
// pre-watermark takeover effects (a remote observer's cancelWillOutbox, a displaced
// client's cleared Will) may already have suppressed it. The watermark itself is
// never touched (a monotonic value that may have landed cannot be safely lowered).
func (as *mqttAccountSessionManager) rollbackAcceptedSession(s *Server, es *mqttSession, cidHash string, candidate uint64, expiry uint32) {
	// Detach the rejected client so the (soon-removed) session object no longer
	// points at it. Neither clear() nor removeSession clears sess.c.
	es.mu.Lock()
	es.c = nil
	es.mu.Unlock()

	// Sequence-targeted write making the candidate record non-connected. Branch ONLY
	// on the resolved Session Expiry Interval (Clean Start / resume state is
	// orthogonal in MQTT 5, mirroring mqttHandleClosedClient's doClean = expiry==0):
	// zero expiry deletes the record (it ends at connection close); any positive
	// expiry re-saves it disconnected so a later reconnect can still resume it.
	//
	// Both are record writes, so they run in a record transaction like every other
	// (I8) — opened before the mutation the re-save persists. It is uncontended: the
	// CONNECT that owns this session released its accept transaction before calling
	// here, and the client is already detached.
	//
	// This path deliberately does NOT take a teardown claim. It removes only the
	// record, at an exact sequence, and its outcome is decided by the authoritative
	// re-read below — which is the substance of the release protocol — while it
	// deletes no consumers, so it has nothing it could wrongly delete. Widening it
	// into a full teardown would change guarantees this function documents.
	tok := es.beginRecordTx()
	defer func() {
		if terr := es.endRecordTx(tok); terr != nil {
			s.Errorf("MQTT: %v", terr)
		}
	}()
	if expiry == 0 {
		_ = as.rollbackDeleteSession(es, tok, candidate)
	} else {
		es.mu.Lock()
		es.disconnectedAt = time.Now().Unix()
		ps := es.buildPersistedSessionLocked()
		es.mu.Unlock()
		_ = as.rollbackStoreSession(es, tok, &ps, cidHash, candidate)
	}

	// Classify by an authoritative re-read. A successful re-save lands at a NEW
	// sequence, so we cannot key on "candidate seq"; instead: absent or disconnected
	// -> applied; connected & Sequence > candidate -> superseded (a newer CONNECT
	// won); connected & still at candidate, or unreadable -> indeterminate.
	ps, seq, lerr := as.rollbackLoadSession(cidHash)
	var result rollbackResult
	switch {
	case lerr != nil:
		result = rollbackIndeterminate
	case ps == nil:
		result = rollbackApplied
	case ps.DisconnectedAt != 0:
		result = rollbackApplied
	case seq > candidate:
		// A newer CONNECTED record. Sequence alone cannot say whether it SUCCEEDS the
		// candidate or merely shares its parent, and the two demand opposite treatment
		// of the expiry timer, so the lineage decides. es.wid/es.pwid are still the
		// candidate's: the re-save above either landed (in which case DisconnectedAt is
		// set and this arm is unreachable) or did not, and a rollback never adopts.
		es.mu.Lock()
		ackWID, ackPWID := es.wid, es.pwid
		es.mu.Unlock()
		if classifyRecordLineage(ps, ackWID, ackPWID) == mqttLineageForked {
			result = rollbackForked
		} else {
			result = rollbackSuperseded
		}
	default:
		result = rollbackIndeterminate
	}

	as.mu.Lock()
	// Invalidate any stale remote-expiry reconciliation worker BEFORE touching timers
	// or removing the session: advance the local reconcile watermark (only if an
	// entry already exists) so a worker parked on an older observation rejects its
	// snapshot rather than resuming after removeSession and overwriting our timer
	// decision. See reconcileRemoteSessionExpiry.
	if st := as.reconcileWM[cidHash]; st != nil {
		hi := candidate
		if seq > hi {
			hi = seq
		}
		if hi > st.latest {
			st.latest = hi
		}
	}
	// Reconcile the expiry timer from the authoritative re-read (the normal
	// cancelSessionExpiry on the accept path is skipped here).
	switch {
	case result == rollbackApplied && ps != nil && ps.DisconnectedAt != 0 &&
		ps.ExpiryInterval > 0 && ps.ExpiryInterval != mqttSessionNeverExpire:
		// Finite disconnected record: arm from its persisted absolute deadline so
		// cleanup follows it rather than waiting for the 15-minute sweep.
		remaining := time.Duration(ps.DisconnectedAt+int64(ps.ExpiryInterval)-time.Now().Unix()) * time.Second
		if remaining < 0 {
			remaining = 0
		}
		as.scheduleSessionExpiryLocked(s, cidHash, remaining)
	case result == rollbackIndeterminate:
		// Cannot prove the record's state: preserve any existing timer conservatively.
	case result == rollbackForked:
		// The surviving record is no successor of the candidate, so nothing has been
		// shown to own the deadline the existing timer is counting. Cancelling it here
		// would disarm a session nobody demonstrated had been replaced, which is what
		// rollbackSuperseded does and why a fork must not reach it.
		//
		// Preserving is not a guarantee of eventual expiry either: a sibling still
		// reading DisconnectedAt == 0 makes loadExpiringSessionRecord report not-due
		// when this one-shot fires, and nothing re-arms at that moment. What catches it
		// is sweepSessionExpiries, which stamps such a record and arms from it. The
		// residual is the DELAY and the sweep's origin gate; see docs/CONFORMANCE.md.
	default:
		// applied-absent / zero / never-expire, or superseded (the newer generation
		// owns its own scheduling): cancel any stale timer.
		as.cancelSessionExpiryLocked(cidHash)
	}
	as.removeSession(es, false)
	as.mu.Unlock()

	if result == rollbackIndeterminate {
		s.Warnf("MQTT: CONNECT rollback for client hash %q is indeterminate (session store unavailable); the rejected record may linger until the expiry sweep or a later reconnect", cidHash)
	}

	as.mu.RLock()
	done := as.testRollbackDone
	as.mu.RUnlock()
	if done != nil {
		done(es, result)
	}
}

// rollbackStoreSession performs the rollback re-save CAS, honoring the instance-
// scoped test seam so a test can fail the rollback without failing the initial
// es.save() or unrelated stores.
//
// tok authorizes the write, as for every record write (I8): a rollback from a
// terminated generation must not recreate the record.
func (as *mqttAccountSessionManager) rollbackStoreSession(es *mqttSession, tok recordTx, ps *mqttPersistedSession, idHash string, seq uint64) error {
	es.mu.Lock()
	terr := es.validateRecordTxLocked(tok)
	es.mu.Unlock()
	if terr != nil {
		return fmt.Errorf("refusing to roll back the record of session %q: %w", ps.ID, terr)
	}
	// The re-save is a record write, so it carries a lineage like any other: a fresh
	// identity parented to the candidate record it rolls back. Stamped BEFORE the test
	// seam, so a test observing the store sees the bytes production would store. The
	// session is removed by the caller and never adopts this pair. See
	// sess_fork_lineage.go.
	es.mu.Lock()
	ps.WID, ps.PWID = mqttNewRecordWID(), es.parentForNextWriteLocked()
	es.mu.Unlock()
	as.mu.RLock()
	hook := as.testRollbackSessionStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(ps, idHash, seq)
	}
	return as.storeSessionRecordCAS(ps, idHash, seq)
}

// rollbackDeleteSession removes the candidate record at its exact sequence, under
// the same authorization rule.
func (as *mqttAccountSessionManager) rollbackDeleteSession(es *mqttSession, tok recordTx, seq uint64) error {
	es.mu.Lock()
	terr := es.validateRecordTxLocked(tok)
	es.mu.Unlock()
	if terr != nil {
		return fmt.Errorf("refusing to roll back the record of session %q: %w", es.id, terr)
	}
	return as.jsa.deleteMsg(mqttSessStreamName, seq, true)
}

// rollbackLoadSession loads the current session record for the rollback
// classification, honoring the instance-scoped test seam. (nil, 0, nil) means
// absent; a non-nil error means unreadable (indeterminate).
func (as *mqttAccountSessionManager) rollbackLoadSession(cidHash string) (*mqttPersistedSession, uint64, error) {
	as.mu.RLock()
	hook := as.testRollbackSessionLoad
	as.mu.RUnlock()
	if hook != nil {
		return hook(cidHash)
	}
	smsg, err := as.jsa.loadSessionMsg(as.domainTk, cidHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, err
		}
		return nil, 0, nil
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return nil, 0, err
	}
	return ps, smsg.Sequence, nil
}
