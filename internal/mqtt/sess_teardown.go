// The distributed teardown claim.
//
// Discarding a session (clean start, a v5 DISCONNECT with expiry 0, session
// expiry, takeover-discard) deletes JetStream resources and then the session
// record. Nothing arbitrated that across brokers: the record transaction fence and
// lockSession are both in-process, so after broker A resolved against record
// sequence S, broker B could take over and save a NEWER record before A reached
// its deletions. A then deleted durables B had just restored, and finally deleted
// sequence S — which MaxMsgsPer:1 had already removed, so the delete reported
// "sequence not found", which clear() reads as success. B was left holding a valid
// record referencing deleted consumers, and (on the interest-retained streams) the
// messages they held were gone with them.
//
// Two things fix it, and the NAMING one is load-bearing:
//
//  1. Generation-scoped names (sess_incarnation.go). A claim alone cannot be made
//     safe, because re-reading the marker before each delete is a check-then-act: A
//     can verify its nonce, pause, let B finish the whole teardown, release the
//     marker and create a fresh session that recreates the same deterministic name,
//     and only then complete its delete. With generations, a late delete from a
//     retired incarnation can only name a resource that no longer exists.
//
//  2. This claim. Its remaining job is narrower but still necessary: keep two
//     brokers from tearing down and resuming the same record at once, and stop
//     treating a vanished record as proof of OUR OWN success. A lost claim CAS means
//     a newer owner exists, so delete nothing. A release is a sequence-targeted
//     delete whose no-op is classified by re-reading, never assumed to be ours.
//
// Every CONNECT that reads a marker FINISHES it rather than overwriting it: it
// adopts the teardown, repeats the (idempotent) deletions for the RETIRING
// generation, releases the record, and only then starts a fresh session with a
// fresh generation and Session Present false. That also makes a crashed teardown
// self-healing, which today leaks until the orphan sweep.

package mqtt

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nuid"
)

// mqttSessTeardown marks a session record as being torn down. Nonce identifies THIS
// attempt, so a broker can tell its own claim from a peer's when it classifies a
// release that removed nothing. Origin and StartedAt are diagnostic.
type mqttSessTeardown struct {
	Origin    string `json:"origin"`
	Nonce     string `json:"nonce"`
	StartedAt int64  `json:"started"`
}

// mqttTeardownTarget enumerates everything ONE retiring session incarnation owns,
// captured before the in-memory state is reset. Every name in it is either
// generation-scoped or already unique per incarnation, which is what makes the
// deletions safe to repeat and safe to issue late.
//
// sharedDurs is deliberately separate: a shared group durable is not owned by one
// session, and is released only after the record is gone, by the last-holder scan.
type mqttTeardownTarget struct {
	id         string
	idHash     string
	gen        string
	durs       []string // plain (and drain) durables on $MQTT_msgs
	sharedDurs []string
	pubRelDur  string
	q2AffGen   string
	q2AffDur   string
}

// mqttTeardownRelease is how a release attempt ended.
type mqttTeardownRelease int

const (
	// mqttTeardownReleased: the record is gone and it was ours to remove.
	mqttTeardownReleased mqttTeardownRelease = iota
	// mqttTeardownRetry: our marker is still current; the delete did not take.
	mqttTeardownRetry
	// mqttTeardownSuperseded: a newer owner holds the record (a different nonce, or
	// no marker at all). Touch nothing further.
	mqttTeardownSuperseded
	// mqttTeardownIndeterminate: the classifying re-read failed. Nothing may be
	// concluded, so nothing further may be deleted.
	mqttTeardownIndeterminate
)

func (r mqttTeardownRelease) String() string {
	switch r {
	case mqttTeardownReleased:
		return "released"
	case mqttTeardownRetry:
		return "retry"
	case mqttTeardownSuperseded:
		return "superseded"
	default:
		return "indeterminate"
	}
}

// mqttTeardownReleaseAttempts bounds the retry of a release whose delete did not
// take while our marker was still current. Exhausting it leaves a marked record,
// which the next CONNECT (or the orphan sweep) finishes — never a silent leak.
const mqttTeardownReleaseAttempts = 3

// mqttTeardownClaimAttempts bounds the retry of a claim rejected by transient
// clustered contention (10164), which is a rejection but NOT evidence that the
// subject moved.
const mqttTeardownClaimAttempts = 3

// mqttTeardownClaim is how a claim attempt ended.
type mqttTeardownClaim int

const (
	// mqttTeardownClaimWon: the marker is durably ours; the deletions may run.
	mqttTeardownClaimWon mqttTeardownClaim = iota
	// mqttTeardownClaimSuperseded: a newer owner (or another teardown) holds the
	// record. Delete NOTHING. This is a complete outcome, not a failure: there is
	// nothing left for this session to do.
	mqttTeardownClaimSuperseded
	// mqttTeardownClaimIndeterminate: the outcome is unknown. Delete nothing and
	// report it, so the caller fails closed rather than believing a teardown ran.
	mqttTeardownClaimIndeterminate
)

func (c mqttTeardownClaim) String() string {
	switch c {
	case mqttTeardownClaimWon:
		return "won"
	case mqttTeardownClaimSuperseded:
		return "superseded"
	default:
		return "indeterminate"
	}
}

// claimTeardown CASes a teardown marker onto this session's record at the sequence
// the session believes is current, and returns the sequence the marker landed at.
//
// Every rejection is resolved by RE-READING rather than classified directly, for
// the reason isQoS2CASRejection documents: 10164 means transient clustered
// contention, not "the subject moved", so treating a rejection as "somebody else
// owns this" would abandon teardowns that nothing else will finish. The re-read
// also recovers the case that matters most in the other direction — an
// indeterminate write whose marker actually landed, which the nonce identifies as
// ours.
//
// ps must be the record content as it stands, so the marker preserves it: any
// broker must be able to finish this teardown, and the orphan sweep must keep
// reading the durables it names as referenced.
func (sess *mqttSession) claimTeardown(tok recordTx, ps *mqttPersistedSession, seq uint64) (markerSeq uint64, nonce string, outcome mqttTeardownClaim, err error) {
	// The marker IS a record write, so it needs the same authorization as any other
	// (I8): a writer from a terminated generation must not be able to mark — and so
	// license the deletion of — a record it no longer owns.
	sess.mu.Lock()
	terr := sess.validateRecordTxLocked(tok)
	sess.mu.Unlock()
	if terr != nil {
		return 0, _EMPTY_, mqttTeardownClaimIndeterminate,
			fmt.Errorf("refusing to claim the teardown of session %q: %w", ps.ID, terr)
	}

	// One nonce across every attempt, so a write that landed with its response lost
	// is recognizable as ours on the re-read instead of looking like a peer's.
	nonce = nuid.Next()
	marked := *ps
	marked.Teardown = &mqttSessTeardown{
		Origin:    sess.jsa.id,
		Nonce:     nonce,
		StartedAt: time.Now().Unix(),
	}

	var lastErr error
	for attempt := 0; attempt < mqttTeardownClaimAttempts; attempt++ {
		markerSeq, err = mqttStoreSessionRecordCAS(sess.jsa, sess.domainTk, &marked, sess.idHash, seq)
		if err == nil {
			return markerSeq, nonce, mqttTeardownClaimWon, nil
		}
		lastErr = err

		smsg, lerr := sess.jsa.loadSessionMsg(sess.domainTk, sess.idHash)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return 0, nonce, mqttTeardownClaimIndeterminate,
					fmt.Errorf("unable to resolve the teardown claim of session %q: %w", ps.ID, lerr)
			}
			// The record is gone, so somebody else already released this session. There
			// is nothing to claim and nothing to delete on its behalf.
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		}
		cur := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, cur); uerr != nil {
			return 0, nonce, mqttTeardownClaimIndeterminate,
				fmt.Errorf("unable to parse the session record of %q while resolving its teardown claim: %w", ps.ID, uerr)
		}
		switch {
		case cur.Teardown != nil && cur.Teardown.Nonce == nonce:
			// Our write landed after all; only the response was lost.
			return smsg.Sequence, nonce, mqttTeardownClaimWon, nil
		case cur.Teardown != nil:
			// Another broker's teardown owns this record. It (or the next CONNECT to
			// read the marker) finishes it; we delete nothing.
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		case smsg.Sequence != seq:
			// The record moved: a newer owner exists.
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		default:
			// Still exactly the record we resolved against, so the rejection was
			// transient contention, not a lost race. Retry the same CAS.
		}
	}
	return 0, nonce, mqttTeardownClaimIndeterminate,
		fmt.Errorf("unable to claim the teardown of session %q at sequence %v after %d attempts: %w",
			ps.ID, seq, mqttTeardownClaimAttempts, lastErr)
}

// deleteTeardownTargets performs the destructive half of a teardown: everything the
// retiring incarnation owns alone. Safe to repeat (every delete tolerates "already
// gone") and safe to issue late (every name is generation-scoped or per-incarnation
// unique), which is what lets an adopting broker just run it again.
//
// Shared group durables are NOT here; they are released by the caller after the
// record is gone, so the last-holder scan does not count this session.
func (sess *mqttSession) deleteTeardownTargets(t *mqttTeardownTarget, noWait bool) error {
	for _, dur := range t.durs {
		if _, err := sess.jsa.deleteConsumer(mqttStreamName, dur, noWait); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete consumer %q for session %q: %v", dur, t.id, err)
		}
	}
	if t.pubRelDur != _EMPTY_ {
		_, err := sess.jsa.deleteConsumer(mqttOutStreamName, t.pubRelDur, noWait)
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete consumer %q for session %q: %v", t.pubRelDur, t.id, err)
		}
	}

	// Terminate this session's shared QoS 2 assignments BEFORE its record is
	// deleted (terminal cleanup precedes completion): a discarded claim is what
	// tells a contender the delivery died with its session rather than being
	// available to redistribute. The claims are marked, never released, and the
	// undelivered copies are dropped by $MQTT_out's interest retention when the
	// affine durable goes. Spec5 [MQTT-4.8.2-6].
	if t.q2AffGen != _EMPTY_ && sess.asm != nil {
		if err := sess.asm.discardQ2AffClaimsFor(t.idHash, t.q2AffGen); err != nil {
			return fmt.Errorf("unable to terminate the shared QoS 2 assignments of session %q: %w", t.id, err)
		}
	}
	if t.q2AffDur != _EMPTY_ {
		_, err := sess.jsa.deleteConsumer(mqttOutStreamName, t.q2AffDur, noWait)
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete affine QoS 2 consumer %q for session %q: %v", t.q2AffDur, t.id, err)
		}
	}

	// Destroy this incarnation's inbound QoS2 records ($MQTT_qos2in, subject
	// "$MQTT.qos2.in.<idHash>.<gen>.<PI>"; see mqttQoS2HoldSubject). Both record
	// states are covered — held PUBLISHes and terminal tombstones — which is what the
	// move protocol needs: a discarded session must leave neither behind. They
	// otherwise collide by PI with a later session for the same client ID and can
	// resurface stale payloads on PUBREL. Only on discard, never on resume: an
	// inbound QoS2 message pending PUBREL is session state (Spec5 [4.1]) that a
	// resumed session must keep.
	//
	// This ARBITRATES per subject rather than purging by filter, because a purge
	// competes with nothing: a PUBREL that had merely READ a held record went on to
	// fan out, and its finalize read the resulting absence as proof the exchange had
	// completed — a message delivered out of a session already reported discarded. So
	// the discard now claims each release before destroying its hold, and where a
	// releaser claimed first it waits briefly and then leaves both records rather than
	// destroying a payload a crashed releaser's retry still needs. It reads no
	// payloads at all. See qos2_release_claim.go.
	// There is deliberately NO blind filter purge left beside this. Every hold
	// subject of this incarnation is reached by the arbitration, and a filter purge
	// alongside it could only destroy what the arbitration decided to preserve: a
	// held payload whose releaser claimed it (and which its retry still needs after a
	// crash), or the claim records themselves — including the `discarded` markers
	// that are the only durable proof a late releaser must lose.
	if sess.asm != nil {
		if err := sess.asm.arbitrateQoS2ReleaseClaims(sess.asm.server(), t.idHash, t.gen); err != nil {
			return fmt.Errorf("unable to arbitrate the inbound QoS2 releases of session %q: %w", t.id, err)
		}
	}

	// Destroy this incarnation's OUTBOUND in-flight registers ($MQTT_inflight,
	// subject "$MQTT.inflight.<idHash>.<gen>.<PI>"). The session is being discarded,
	// so every identifier it was holding is void: Spec5 [4.1] makes an unacknowledged
	// outbound QoS 1/2 delivery session state, and a discard destroys it rather than
	// carrying it into whatever session next uses this client ID.
	//
	// A blind filter purge is CORRECT here, and the contrast with the arbitrated hold
	// walk above is the point. A register has no second party: nothing else claims it,
	// waits on it, or reads it as proof — it is one session's private note about its
	// own identifier space, whereas a hold record is a payload another broker's
	// releaser may still be committed to delivering. And the filter names the RETIRING
	// generation, which clear() re-mints before this runs, so even a badly delayed
	// purge can only name subjects no live session can ever occupy.
	if t.idHash != _EMPTY_ && t.gen != _EMPTY_ {
		filter := mqttInflightWalkFilter(t.idHash, t.gen)
		if err := sess.jsa.purgeStream(mqttInflightStreamName, filter, noWait); isErrorOtherThan(err, JSStreamNotFoundErr) {
			return fmt.Errorf("unable to purge the outbound in-flight registers of session %q: %v", t.id, err)
		}
	}
	return nil
}

// releaseTeardown deletes the marked record at the sequence the marker landed on
// and CLASSIFIES the outcome by re-reading, because a delete that removed nothing
// is not evidence of our own success — MaxMsgsPer:1 may have removed that sequence
// when a peer's newer record superseded it, and the peer's record is then the one
// that matters.
//
// It retries only the one outcome that is genuinely retryable (our marker still
// current), and returns the classification so the caller can decide whether it is
// still entitled to touch the shared durables.
//
// The record delete is ALWAYS confirmed, even where the caller asked for a no-wait
// teardown. A fire-and-forget delete only reaches a send queue, so whether the
// re-read observes it is a matter of send-queue timing: when it does not, our own
// marker is still there and an unlanded delete is misread as "somebody must retry
// this", burning the retries and reporting a failure on an ordinary disconnect. In
// practice the queued delete usually reaches the server first, which is precisely
// why this must not be left to chance — the re-read is what licenses everything
// downstream, and an unclassifiable release is not a release. The no-wait flag
// still applies to the consumer deletes and the hold purge, which need no
// classification.
func (sess *mqttSession) releaseTeardown(tok recordTx, t *mqttTeardownTarget, nonce string, markerSeq uint64) (mqttTeardownRelease, error) {
	// Removing the record is a record write like any other (I8). In particular this
	// is the write clear() must not be able to perform from a terminated
	// generation, which is what used to let a late writer recreate a dead session.
	sess.mu.Lock()
	terr := sess.validateRecordTxLocked(tok)
	sess.mu.Unlock()
	if terr != nil {
		return mqttTeardownIndeterminate, fmt.Errorf("refusing to release the teardown of session %q: %w", t.id, terr)
	}

	var lastErr error
	for attempt := 0; attempt < mqttTeardownReleaseAttempts; attempt++ {
		err := sess.jsa.deleteMsg(mqttSessStreamName, markerSeq, true)
		// Ignore the various errors indicating that the message (or sequence) is
		// already deleted, which can happen in a cluster. JetStream reports it either
		// as JSSequenceNotFoundErrF (the delete removed nothing) or as a generic
		// "message delete failed" (10057) carrying ErrStoreMsgNotFound as its
		// flattened cause. Neither is a failure of the CALL — but neither proves the
		// record we wanted gone is the record that went, which is what the re-read
		// below settles.
		if isErrorOtherThan(err, JSSequenceNotFoundErrF) &&
			!isJSErrCausedBy(err, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
			lastErr = fmt.Errorf("unable to delete session %q record at sequence %v: %v", t.id, markerSeq, err)
			continue
		}

		smsg, lerr := sess.jsa.loadSessionMsg(sess.domainTk, t.idHash)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				// Cannot classify: no conclusion, and therefore no further deletions.
				return mqttTeardownIndeterminate, fmt.Errorf("unable to verify the teardown release of session %q: %w", t.id, lerr)
			}
			return mqttTeardownReleased, nil // absent: done
		}
		ps := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
			// An unreadable record is not ours to interpret, and certainly not proof
			// that our teardown completed. Fail closed.
			return mqttTeardownIndeterminate, fmt.Errorf("unable to parse the session record of %q while releasing its teardown: %w", t.id, uerr)
		}
		if ps.Teardown != nil && ps.Teardown.Nonce == nonce {
			// Still our marker: the delete genuinely did not take. Retry it at the
			// sequence it now sits on (a resave can have moved it).
			markerSeq = smsg.Sequence
			lastErr = fmt.Errorf("session %q teardown marker still present at sequence %v", t.id, smsg.Sequence)
			continue
		}
		// A different nonce, or no marker at all: a newer owner exists. This teardown
		// is over and nothing further may be deleted on its behalf.
		return mqttTeardownSuperseded, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("session %q teardown release not confirmed after %d attempts", t.id, mqttTeardownReleaseAttempts)
	}
	return mqttTeardownRetry, lastErr
}

// releaseSharedTeardownDurables deletes the group durables this session was the
// last holder of. Runs only AFTER the record is gone, so the last-holder scan does
// not count this session's own record; sharedSubs was niled by the caller, so the
// scan's in-memory check is empty and only other sessions' records matter. Spec5
// [4.8.2].
func (sess *mqttSession) releaseSharedTeardownDurables(t *mqttTeardownTarget, noWait bool) error {
	if len(t.sharedDurs) == 0 {
		return nil
	}
	held := sess.sharedDurablesHeldByOthers(t.sharedDurs)
	for _, dur := range t.sharedDurs {
		if held[dur] {
			continue
		}
		if _, err := sess.jsa.deleteConsumer(mqttStreamName, dur, noWait); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete shared consumer %q for session %q: %v", dur, t.id, err)
		}
	}
	return nil
}

// finishAdoptedTeardown completes a teardown some broker CLAIMED and did not
// finish, so its record can be released and the client can start fresh.
//
// It never resumes the marked state and never overwrites the marker: it adopts the
// existing nonce, repeats the (idempotent, generation-scoped) deletions and
// releases the record. A concurrent finisher — including the original owner waking
// up — is harmless: both delete the same retired names, one wins the record delete,
// and the loser's classifying re-read reports released or superseded.
//
// This is also what makes a CRASHED teardown self-healing, which previously leaked
// until the orphan sweep.
//
// No lock held on entry; the client ID is in sessLocked.
func (as *mqttAccountSessionManager) finishAdoptedTeardown(ps *mqttPersistedSession, idHash string, seq uint64, opts *Options) error {
	target := mqttTeardownTargetFromRecord(ps, idHash)
	// A transient session, just enough to reach the JSA and the affine claim scan.
	// Created on the RETIRING generation so every name it deletes is that
	// incarnation's; nothing here provisions, so no re-mint is wanted.
	sess := mqttSessionCreate(&as.jsa, ps.ID, idHash, ps.Gen, seq, opts)
	sess.asm = as
	sess.domainTk = as.domainTk

	// A fresh transient session, so the fence is uncontended; it is opened all the
	// same, because every record write goes through one (I8).
	tok := sess.beginRecordTx()
	defer func() {
		if terr := sess.endRecordTx(tok); terr != nil {
			as.jsa.c.Errorf("MQTT: %v", terr)
		}
	}()

	if err := sess.deleteTeardownTargets(target, false); err != nil {
		return err
	}
	switch res, rerr := sess.releaseTeardown(tok, target, ps.Teardown.Nonce, seq); res {
	case mqttTeardownReleased:
	case mqttTeardownSuperseded:
		// Somebody else's record is there now. Nothing further is ours to delete —
		// and in particular the group durables its membership may hold are not.
		return nil
	default:
		return rerr
	}
	return sess.releaseSharedTeardownDurables(target, false)
}

// mqttTeardownTargetFromRecord builds the teardown target of the incarnation a
// persisted record describes. Used by the paths that adopt somebody else's marked
// record, which have no in-memory session state to read.
func mqttTeardownTargetFromRecord(ps *mqttPersistedSession, idHash string) *mqttTeardownTarget {
	t := &mqttTeardownTarget{id: ps.ID, idHash: idHash, gen: ps.Gen, q2AffGen: ps.Q2AffGen}
	for _, cc := range ps.Cons {
		// Cons never holds group consumers (buildPersistedSessionLocked excludes
		// them), but guard anyway: deleting one outright would break other members.
		if cc == nil || cc.DeliverGroup != _EMPTY_ {
			continue
		}
		t.durs = append(t.durs, cc.Durable)
	}
	// A draining durable belongs to this session too: it outlived an UNSUBSCRIBE
	// only because started deliveries were owed, and the session ending discharges
	// that. See unsub_drain.go.
	for _, pd := range ps.Drains {
		if pd != nil && pd.Cons != nil {
			t.durs = append(t.durs, pd.Cons.Durable)
		}
	}
	for key := range ps.SharedSubs {
		t.sharedDurs = append(t.sharedDurs, mqttSharedDurablesForKey(key)...)
	}
	if ps.PubRel != nil {
		t.pubRelDur = ps.PubRel.Durable
	}
	if ps.Q2Aff != nil {
		t.q2AffDur = ps.Q2Aff.Durable
	}
	return t
}
