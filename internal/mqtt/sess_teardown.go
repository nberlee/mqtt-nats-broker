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
//     retired incarnation cannot name a resource the FRESH incarnation owns. It can
//     still reach one a peer created under the retiring generation, which a resume
//     preserves — which is why naming is not a substitute for arbitration on a subject
//     a live peer can still write. On the inbound QoS 2 holds what supplies the rest is
//     issue #66's purge bound: a removal names a subject but destroys only up to the
//     sequence its remover could prove it was entitled to.
//
//  2. This claim. Its remaining job is narrower but still necessary: keep two
//     brokers from tearing down and resuming the same record at once, and stop
//     treating a vanished record as proof of OUR OWN success. A lost claim CAS means the
//     claim is not ours — not that a newer owner exists, which it cannot say — so delete
//     nothing. A release is a sequence-targeted
//     delete whose no-op is classified by re-reading, never assumed to be ours.
//
// Every CONNECT that can INTERPRET the record FINISHES a marker it reads rather than
// overwriting it: it adopts the teardown, repeats the deletions for the RETIRING
// generation, releases the record, and only then starts a fresh session with a fresh
// generation and Session Present false. (Repeating them is safe for names nothing can
// re-occupy; for the inbound QoS 2 hold walk it is bounded per walk, and for the
// in-flight reap it is bounded by that pass's own enumeration watermark — issue #38
// replaced the unbounded purge that used to sit there. issue #66 and issue #38 state those two; do
// not restate them here.) That also makes a crashed teardown
// self-healing, which today leaks until the orphan sweep. A record the restore
// boundary refuses is never adopted by the CONNECT: a reserved-namespace record whose
// generation is valid is still finished by the expiry path and the orphan sweep, while
// one with no valid generation is adopted by NOTHING — every deletion here is
// generation-scoped, so there is no safe name to issue — and waits for an operator to
// recreate the streams. See docs/ARCHITECTURE.md, teardown section.

package mqtt

import (
	"encoding/json"
	"errors"
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
// generation-scoped or already unique per incarnation, so no deletion here can reach a
// FRESH incarnation's resources however late it lands.
//
// The inbound QoS 2 hold walk is the one target here that a repeat can re-reach on a subject
// a live peer re-occupied, and it is bounded PER WALK rather than frozen at the first walk's
// watermark. What that bounds, what it deliberately does not, and why freezing it would be
// unsafe are stated once in issue #66 (https://github.com/nberlee/mqtt-nats-broker/issues/131); do not re-derive them
// here, and do not paraphrase the entitlement criterion — three review rounds were spent on
// restatements of it that were a shade too strong.
//
// It extends to `$MQTT.inflight.` too, since issue #38: the blind filter purge there was
// replaced by an enumerate-load-classify-delete pass whose entitlement is its own
// enumeration watermark, so a subject re-occupied since the enumeration is SPARED rather
// than destroyed.
//
// The shared group durables are deliberately NOT here: a group consumer is not owned by
// one session, and the closing releases this generation's ACQUISITIONS of it from the
// generation's sealed acquisition ledger (closeSharedGeneration), never from a list a
// teardown object derived from its own membership.
type mqttTeardownTarget struct {
	id     string
	idHash string
	gen    string
	// holdWalkWidth is how many inbound QoS 2 hold arbitrations the walk keeps in flight; zero
	// means mqttQoS2DiscardWalkWidth. Only a clean close's continuation sets it, to
	// mqttQoS2DiscardWalkWidthReleased, once its gate is released: from then no CONNECT waits on
	// this walk, and a wide one only competes with every other CONNECT's requests.
	holdWalkWidth int
	durs          []string // plain (and drain) durables on $MQTT_msgs
	pubRelDur     string
	q2AffGen      string
	q2AffDur      string
	// deferAffineDelete transfers this generation's affine consumer to the recovery pass
	// (issue #107, row 8). Set ONLY by the teardown's own classification, and only where that
	// generation's closure DISCARDS and its affine target is enrolled with both a resource
	// record and a cleanup duty — the durable facts that say the duty exists and someone will
	// come back for it.
	//
	// Everything else this target names is deleted as it always was: the shared group durables
	// stay with the synchronous helper for every population, and a generation whose closure does
	// not discard enrols no affine target, so its consumer stays this teardown's to remove.
	deferAffineDelete bool
	// transferLogical transfers this generation's SOURCE, drain and PUBREL consumers, and its
	// retained registers, to the recovery executor (issue #107 7d). Set ONLY by the teardown's own
	// classification, and only where that generation's committed closure DISCARDS: from then the
	// consumers are retired through their resources — disposal, the name's retirement, a
	// classified delete — and never deleted here, because a delete issued before the authorization
	// is exactly what the resource protocol exists to forbid.
	//
	// The VACUOUS class (no closure, no ledger) and a teardown with no persisted record keep the
	// direct delete: nothing was admitted or provisioned through the authority, so no obligation
	// holds those names and no ownership record names them; and every name is nuid- or
	// generation-scoped, so a late delete reaches no successor.
	transferLogical bool
}

// mqttTeardownRelease is how a release attempt ended.
type mqttTeardownRelease int

const (
	// mqttTeardownReleased: the record is gone and it was ours to remove.
	mqttTeardownReleased mqttTeardownRelease = iota
	// mqttTeardownRetry: our marker is still current; the delete did not take.
	mqttTeardownRetry
	// mqttTeardownSuperseded: the record is no longer the one this claim held (a different
	// nonce, or no marker at all). It does not name a newer owner — nothing here can.
	// Touch nothing further.
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
// which the next CONNECT that can interpret it (or the orphan sweep, which needs no
// CONNECT at all) finishes — never a silent leak.
const mqttTeardownReleaseAttempts = 3

// mqttTeardownClaim is how a claim attempt ended.
type mqttTeardownClaim int

const (
	// mqttTeardownClaimWon: the marker is durably ours; the deletions may run.
	mqttTeardownClaimWon mqttTeardownClaim = iota
	// mqttTeardownClaimSuperseded: the record moved past the base this claim held, or
	// carries a teardown marker that is not ours. Delete NOTHING. This is a complete
	// outcome, not a failure: there is nothing left for this session to do.
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

// claimTeardown makes this session's generation noncurrent and marks its record, in ONE batch
// (teardown_claim.go, plan 5.2), and returns the sequence the marker landed at.
//
// al is the discard boundary's own authority read (mqttPreparedClear.auth), never a fresher one,
// and ret the retirement it authorizes. Only an authority SERVING this generation is claimed; one
// serving another generation, or already closed, means another teardown or acceptance owns the
// record (Superseded); an absent one is Inconsistent, since every record is written by an
// acceptance that creates or advances its authority.
//
// Every rejection is resolved by RE-READING both the record and the authority, never classified
// from the error (isQoS2CASRejection): our marker on the record is Landed; the record at our base
// with the authority at ours is contention or our own batch in process, so the same frozen batch
// is re-sent; the record at our base with the authority past ours means the frozen batch can
// never land, so it is recomposed ONCE against the fresh authority, which must still serve this
// generation under the same ownership; the record past our base without our marker is the
// existing supersession; anything unreadable is Indeterminate. Re-sends and the recomposition
// share the claim's bounded allowance.
//
// ps must be the record content as it stands, so the marker preserves it: any broker must be able
// to finish this teardown, and the orphan sweep must keep reading the durables it names.
func (sess *mqttSession) claimTeardown(tok recordTx, ps *mqttPersistedSession, seq uint64, retiringWID string,
	al mqttAuthorityLoad, ret mqttRetirementRequest) (markerSeq uint64, nonce string, outcome mqttTeardownClaim, err error) {

	// The marker IS a record write, so it needs the same authorization as any other (I8): a
	// writer from a terminated generation must not be able to mark — and so license the deletion
	// of — a record it no longer owns.
	sess.mu.Lock()
	terr := sess.validateRecordTxLocked(tok)
	sess.mu.Unlock()
	if terr != nil {
		return 0, _EMPTY_, mqttTeardownClaimIndeterminate,
			fmt.Errorf("refusing to claim the teardown of session %q: %w", ps.ID, terr)
	}
	switch {
	case al.status == mqttOutboundAbsent:
		return 0, _EMPTY_, mqttTeardownClaimIndeterminate,
			fmt.Errorf("refusing to claim the teardown of session %q: its record stands with no authority, which no acceptance writes", ps.ID)
	case al.status != mqttOutboundValid || al.rec == nil:
		return 0, _EMPTY_, mqttTeardownClaimIndeterminate,
			fmt.Errorf("refusing to claim the teardown of session %q: its authority reads %v", ps.ID, al.status)
	case al.rec.State != mqttAuthorityServing || al.rec.Gen != ret.Gen:
		return 0, _EMPTY_, mqttTeardownClaimSuperseded, nil
	}

	// One nonce and one marker identity across every attempt, so a batch that landed with its
	// response lost is recognizable as ours on the re-read instead of looking like a peer's.
	nonce = nuid.Next()
	marked := *ps
	// The marker carries a lineage like any other record write: a fresh identity of its own,
	// parented to the record it was derived from. retiringWID is passed in because the snapshot
	// arrives with no lineage. See sess_fork_lineage.go.
	marked.WID, marked.PWID = mqttNewRecordWID(), retiringWID
	marked.Teardown = &mqttSessTeardown{Origin: sess.jsa.id, Nonce: nonce, StartedAt: time.Now().Unix()}
	markerBody, merr := json.Marshal(&marked)
	if merr != nil {
		return 0, nonce, mqttTeardownClaimIndeterminate,
			fmt.Errorf("unable to encode the teardown marker of session %q: %w", ps.ID, merr)
	}
	ret.Tenure, ret.OwnerTxn = al.rec.Tenure, al.rec.OwnerTxn
	req := mqttTeardownClaimRequest{
		ID: ps.ID, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(),
		Prev: al.rec, PrevSeq: al.seq, Retire: ret,
		RecSubj: mqttSessSubject(sess.domainTk, sess.idHash), RecSeq: seq,
		Marker: markerBody, Nonce: nonce, MarkerWID: marked.WID,
	}
	t, cerr := sess.jsa.composeTeardownClaim(req)
	if cerr != nil {
		// Composition reads the closing members and validates the targets this session's memory
		// captured, before any CAS could say the record moved. A moved record is the supersession
		// the CAS would have reported, so it is read before the refusal is classified.
		if smsg, lerr := sess.jsa.loadSessionMsg(sess.domainTk, sess.idHash); (lerr == nil && smsg.Sequence != seq) ||
			(lerr != nil && !isErrorOtherThan(lerr, JSNoMessageFoundErr)) {
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		}
		return 0, nonce, mqttTeardownClaimIndeterminate,
			fmt.Errorf("unable to compose the teardown claim of session %q: %w", ps.ID, cerr)
	}

	// THE CLAIM IS FOREGROUND AUTHORITY WORK (issue #133): it is the connection's own close, and
	// the background writers of the client's authority (a generation's retirement, the lifecycle
	// drive, anchor removals) would otherwise outrun its recomposition. Announced, it loses at
	// most the attempts admitted before it, which its derived budget outlasts.
	mqttTeardownClaimPoint(ps.ID, "before-announce")
	fg := sess.announceForegroundAuthority()
	defer fg.release()
	// THE FIRST ATTEMPT IS COMPOSED AGAINST A READ TAKEN UNDER THE ANNOUNCEMENT. The authority
	// above was read before it (prepareClear), so a background write admitted in between would
	// spend an attempt the allowance sampled here does not count: re-read, and recompose against
	// what stands, with the same supersession rules as a recomposition below.
	if fresh := sess.jsa.loadOutboundAuthority(ps.ID, sess.asm.sessStreamEpoch); fresh.seq != t.PrevSeq {
		switch {
		case fresh.status != mqttOutboundValid || fresh.rec == nil:
			return 0, nonce, mqttTeardownClaimIndeterminate,
				fmt.Errorf("unable to claim the teardown of session %q: its authority reads %v", ps.ID, fresh.status)
		case fresh.rec.State != mqttAuthorityServing || fresh.rec.Gen != ret.Gen ||
			fresh.rec.Tenure != ret.Tenure || fresh.rec.OwnerTxn != ret.OwnerTxn:
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		}
		req.Prev, req.PrevSeq, req.Txn = fresh.rec, fresh.seq, mqttNewInflightNonce()
		if t, cerr = sess.jsa.composeTeardownClaim(req); cerr != nil {
			return 0, nonce, mqttTeardownClaimIndeterminate,
				fmt.Errorf("unable to recompose the teardown claim of session %q: %w", ps.ID, cerr)
		}
	}
	var lastErr error
	for attempt := 0; attempt < fg.budget(mqttForegroundAcquireAttempts); attempt++ {
		mqttTeardownClaimPoint(ps.ID, "before-send")
		authSeq, err := sess.jsa.commitAuthorityTransition(t)
		if err == nil {
			// The marker commits the batch directly after the authority.
			return authSeq + 1, nonce, mqttTeardownClaimWon, nil
		}
		lastErr = err

		smsg, lerr := sess.jsa.loadSessionMsg(sess.domainTk, sess.idHash)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return 0, nonce, mqttTeardownClaimIndeterminate,
					fmt.Errorf("unable to resolve the teardown claim of session %q: %w", ps.ID, lerr)
			}
			// The record is gone, so this session was already released — by what, an absence
			// cannot say. There is nothing to claim and nothing to delete on its behalf.
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		}
		cur := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, cur); uerr != nil {
			return 0, nonce, mqttTeardownClaimIndeterminate,
				fmt.Errorf("unable to parse the session record of %q while resolving its teardown claim: %w", ps.ID, uerr)
		}
		switch {
		case cur.Teardown != nil && cur.Teardown.Nonce == nonce:
			// Our batch landed; only the response was lost. The batch is atomic, so the
			// authority carries our entry with it.
			return smsg.Sequence, nonce, mqttTeardownClaimWon, nil
		case cur.Teardown != nil, smsg.Sequence != seq:
			// The record moved past our base, or another teardown's marker holds it. Whoever wrote
			// it — or the next reader that can interpret it — finishes it; we delete nothing.
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		}
		// The record is still at our base: the authority decides.
		fresh := sess.jsa.loadOutboundAuthority(ps.ID, sess.asm.sessStreamEpoch)
		switch {
		case fresh.status != mqttOutboundValid || fresh.rec == nil:
			return 0, nonce, mqttTeardownClaimIndeterminate,
				fmt.Errorf("unable to resolve the teardown claim of session %q: its authority reads %v", ps.ID, fresh.status)
		case fresh.seq == t.PrevSeq:
			// Contention, or our own batch still in process: the same frozen batch.
			continue
		case fresh.rec.State != mqttAuthorityServing || fresh.rec.Gen != ret.Gen ||
			fresh.rec.Tenure != ret.Tenure || fresh.rec.OwnerTxn != ret.OwnerTxn:
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		}
		// Authority-only movement: the frozen batch can never land. Recompose against the fresh
		// authority, under a new transition, revalidating the generation and the caps; each such
		// movement is a pre-admitted background write, which the budget outlasts.
		req.Prev, req.PrevSeq, req.Txn = fresh.rec, fresh.seq, mqttNewInflightNonce()
		if t, cerr = sess.jsa.composeTeardownClaim(req); cerr != nil {
			return 0, nonce, mqttTeardownClaimIndeterminate,
				fmt.Errorf("unable to recompose the teardown claim of session %q: %w", ps.ID, cerr)
		}
	}
	return 0, nonce, mqttTeardownClaimIndeterminate,
		fmt.Errorf("unable to claim the teardown of session %q at sequence %v after %d attempts: %w",
			ps.ID, seq, fg.budget(mqttForegroundAcquireAttempts), lastErr)
}

// secureQ2AffPubRelProofs records the PUBREC proof carried by every stored PUBREL of the
// retiring incarnation onto the assignment it names, before the PUBREL consumer that
// retains those records is deleted.
//
// It runs for an ADOPTED teardown too, and it has to: the adopting broker is the one that
// will delete the consumer, so it is the one that owes the reconciliation. Everything it
// does is idempotent — the proof is a monotone fact recorded through the ordinary fenced
// transition — so a repeated run is a no-op rather than a second effect.
//
// FAIL CLOSED on anything it could not establish, with one deliberate exception:
//
//   - The walk could not be completed, or a reference named a claim this broker could not
//     READ: abort the teardown. The condition is transient and the teardown is re-driven,
//     so the cost is a retry, and proceeding would destroy evidence it could not read.
//   - A MALFORMED reference proceeds, with a diagnostic. It names no assignment this
//     broker can identify, so there is nothing to record and no retry that could ever
//     succeed; failing closed on it would wedge this session's teardown forever, trading
//     an unbounded liveness loss for a bounded diagnostic one. The claim it cannot name is
//     still `Discarded` by the walk below, which is what [MQTT-4.8.2-5] permits.
//
// No lock held on entry.
func (sess *mqttSession) secureQ2AffPubRelProofs(t *mqttTeardownTarget) error {
	return sess.secureQ2AffPubRelProofsCharged(t, nil)
}

// secureQ2AffPubRelProofsCharged is secureQ2AffPubRelProofs under a per-record CHARGE, for the
// recovery pass. The charge here is a CANCELLATION check between record units rather than an
// operation budget: this walk is account-wide and charging it per record would starve it.
//
// Exhausting the charge fails the whole act, and that is deliberate: partial work must never
// license the readiness assertion, because readiness is what permits the seal, and the seal is
// what permits deleting the consumer these records live in. What a partial run already committed
// — a proof written into an assignment — STANDS: those are durable, repeatable facts, and a later
// pass re-walks from the start and completes them.
func (sess *mqttSession) secureQ2AffPubRelProofsCharged(t *mqttTeardownTarget, charge func() bool) error {
	as := sess.asm
	if as == nil || t.idHash == _EMPTY_ || t.gen == _EMPTY_ {
		return nil
	}
	pubRelSubject, _, _ := mqttPubRelNames(t.idHash, t.gen)
	records, _, ok := sess.scanPubRelRecordsCharged(pubRelSubject, charge)
	if !ok {
		return fmt.Errorf("unable to read the stored PUBRELs of session %q before deleting the "+
			"consumer that retains them: their PUBREC proofs would be destroyed unread", t.id)
	}
	s := as.server()
	for _, r := range records {
		// CANCELLABLE BETWEEN RECORD UNITS, not only inside the enumeration. The calls below
		// read claims and write proofs, each a round trip of its own, so a check that covered
		// only the walk would let a shutdown wait for the whole record set to be processed. Once
		// past this check the unit runs to its end, nested retries included; the guarantee is a
		// bounded wait, not a write barrier.
		if charge != nil && !charge() {
			return fmt.Errorf("securing the PUBREC proofs of session %q was interrupted before "+
				"every stored PUBREL had been processed, so nothing may be concluded about the "+
				"ones it did not reach", t.id)
		}
		if r.shapeErr != nil {
			// UNREADABLE, so no field of it authorizes anything — including a well-formed
			// affine reference on a record whose identifier, owner or source correlation is
			// not. Acting on it here would record PubRecProven for an assignment on the
			// strength of a record nothing can attribute, and this path MUTATES a claim.
			//
			// Reported and destroyed with its consumer, which is the same disposition the
			// refused-reference arm below takes and for the same reason: there is no
			// assignment this record can be used to correct, and the teardown's obligation
			// is to say what it is destroying rather than to interpret it.
			if s != nil {
				s.Warnf("MQTT: tearing down session %q with a stored PUBREL at sequence %v whose "+
					"shape this broker does not write (%v): nothing may be proven from it, so it "+
					"is destroyed with its consumer", t.id, r.seq, r.shapeErr)
			}
			continue
		}
		if r.affRef.refused() {
			if s != nil {
				s.Warnf("MQTT: tearing down session %q with a stored PUBREL whose affine QoS 2 "+
					"reference (claim %q copy %q) is not a shape this broker writes: it names no "+
					"assignment that can be corrected, so the record is destroyed with its consumer",
					t.id, r.affRef.Claim, r.affRef.Copy)
			}
			continue
		}
		if !r.affRef.valid() {
			continue
		}
		claim, cerr := as.q2AffClaimForCarriedCopy(r.affRef.Claim, r.affRef.Copy)
		if cerr != nil {
			if errors.Is(cerr, errQ2AffCarriedCopySuperseded) {
				continue // positively a superseded incarnation; nothing to record
			}
			return fmt.Errorf("unable to resolve the affine QoS 2 assignment %q named by a stored "+
				"PUBREL of session %q before deleting the consumer that retains it: %w",
				r.affRef.Claim, t.id, cerr)
		}
		if claim == nil || claim.PubRecProven {
			continue // gone, or already durable
		}
		if verr := q2AffShapePrecheck(r.affRef.Claim)(claim); verr != nil {
			if s != nil {
				s.Warnf("MQTT: tearing down session %q without correcting affine QoS 2 assignment %q: "+
					"its record is not a shape this broker writes (%v)", t.id, r.affRef.Claim, verr)
			}
			continue
		}
		if as.proveQ2AffCopyDelivered(r.affRef.Claim, r.affRef.Copy,
			q2AffShapePrecheck(r.affRef.Claim), func(format string, v ...any) {
				if s != nil {
					s.Warnf(format, v...)
				}
			}, true) == nil {
			return fmt.Errorf("unable to record the PUBREC proof of affine QoS 2 assignment %q before "+
				"deleting the PUBREL consumer of session %q that retains its only evidence",
				r.affRef.Claim, t.id)
		}
	}
	return nil
}

// deleteTeardownTargets performs the destructive half of a teardown: everything the
// retiring incarnation owns alone. Safe to issue late (every name is generation-scoped
// or per-incarnation unique, so none reaches a fresh incarnation), which is what lets an
// adopting broker run it again.
//
// Tolerating "already gone" is not by itself what makes this repeatable. For the
// inbound QoS 2 hold walk, which purges by subject and so can name a subject re-occupied
// since the first run, what makes a second run safe is the bound each call carries (issue #66).
// The in-flight reap below carries the same kind of bound, since issue #38: its
// entitlement is the watermark its own enumeration returned, and it deletes only records
// at or below that, at exact sequences, after checking each occupant's identity.
//
// Shared group durables are NOT here; the generation's ACQUISITIONS of them are released by
// the closing from the sealed acquisition ledger, before this and before the record goes
// (closeSharedGeneration).
func (sess *mqttSession) deleteTeardownTargets(t *mqttTeardownTarget, noWait bool) error {
	if h := mqttTestTeardownDeletesHookV.Load(); h != nil {
		(*h)(t)
	}
	for _, dur := range t.durs {
		if t.transferLogical {
			break // retired by the recovery executor through its resource (issue #107 7d)
		}
		if _, err := sess.jsa.deleteConsumer(mqttStreamName, dur, noWait); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete consumer %q for session %q: %v", dur, t.id, err)
		}
	}
	if t.pubRelDur != _EMPTY_ {
		// BEFORE the consumer goes, the PUBREC proofs its records carry are reconciled
		// into their assignments. Deleting the consumer drops those records with it —
		// $MQTT_out is interest-retained — and they are the only evidence that the
		// client answered PUBREC for an assignment recovery may have already marked
		// Lost. Without this the discard below preserves that Lost, and reapable
		// refuses the resulting `Lost ∧ Discarded` claim forever: an authoritative
		// record calling a delivered message lost, permanently.
		//
		// This is the same ordering rule as everywhere else in this protocol —
		// persist the proof before destroying the thing that proves it.
		if err := sess.secureQ2AffPubRelProofs(t); err != nil {
			return err
		}
		// TRANSFERRED in a discarding generation (issue #107 7d): the executor disposes of it only once
		// the affine readiness — the assertion these proofs are secured — stands.
		if !t.transferLogical {
			_, err := sess.jsa.deleteConsumer(mqttOutStreamName, t.pubRelDur, noWait)
			if isErrorOtherThan(err, JSConsumerNotFoundErr) {
				return fmt.Errorf("unable to delete consumer %q for session %q: %v", t.pubRelDur, t.id, err)
			}
		}
	}

	// Terminate this session's shared QoS 2 assignments BEFORE its record is
	// deleted (terminal cleanup precedes completion): a discarded claim is what
	// tells a contender the delivery died with its session rather than being
	// available to redistribute. The claims are marked, never released, and the
	// undelivered copies are dropped by $MQTT_out's interest retention when the
	// affine durable goes. Spec5 [MQTT-4.8.2-5].
	if t.q2AffGen != _EMPTY_ && sess.asm != nil {
		if err := sess.asm.discardQ2AffClaimsFor(t.idHash, t.q2AffGen); err != nil {
			return fmt.Errorf("unable to terminate the shared QoS 2 assignments of session %q: %w", t.id, err)
		}
	}
	if t.q2AffDur != _EMPTY_ && !t.deferAffineDelete {
		_, err := sess.jsa.deleteConsumer(mqttOutStreamName, t.q2AffDur, noWait)
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete affine QoS 2 consumer %q for session %q: %v", t.q2AffDur, t.id, err)
		}
	}
	// DEFERRED, NOT SKIPPED. The proofs above were still secured and the assignments still
	// discarded — deferring the CONSUMER.DELETE waives neither, and the ordering that puts them
	// first is preserved for both classifications. What changes is who issues the delete: the
	// enrolled cleanup duty, whose executor authorizes it against the resource record and records
	// a classified outcome, rather than this call, which reports only an error.

	// Destroy this incarnation's inbound QoS2 records ($MQTT_qos2in, subject
	// "$MQTT.qos2.in.<idHash>.<gen>.<PI>"; see mqttQoS2HoldSubject). Both record
	// states are covered — held PUBLISHes and terminal tombstones — which is what the
	// move protocol needs: a discarded session must leave neither behind. They
	// otherwise collide by PI with a later EXCHANGE on this same incarnation and can
	// resurface stale payloads on PUBREL. (Not with "a later session": the subject
	// carries the incarnation generation, so a fresh incarnation names different
	// subjects.) Only on discard, never on resume: an
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
	// There is deliberately NO blind filter purge left beside this. Every hold subject
	// this incarnation had WHEN THE WALK ENUMERATED is reached by the arbitration, and a
	// filter purge alongside it could only destroy what the arbitration decided to
	// preserve: a held payload whose releaser claimed it (and which its retry still needs
	// after a crash), or the claim records themselves — including the `discarded` markers
	// that are the only durable proof a late releaser must lose.
	// The enumeration is a one-shot snapshot, and a peer still serving this incarnation can
	// write after it: a subject the walk never enumerated is never arbitrated, while one it
	// did enumerate can be re-occupied before the purge lands. Which finding owns which
	// direction is stated once, in issue #40 and issue #66 (https://github.com/nberlee/mqtt-nats-broker/issues/131); this
	// paragraph covers neither.
	if sess.asm != nil {
		width := t.holdWalkWidth
		if width == 0 {
			width = mqttQoS2DiscardWalkWidth
		}
		if err := sess.asm.arbitrateQoS2ReleaseClaims(sess.asm.server(), t.idHash, t.gen, width); err != nil {
			return fmt.Errorf("unable to arbitrate the inbound QoS2 releases of session %q: %w", t.id, err)
		}
	}

	// Destroy this incarnation's OUTBOUND in-flight registers (the $MQTT.inflight. space,
	// subject "$MQTT.inflight.<idHash>.<gen>.<PI>"). The session is being discarded,
	// so every identifier it was holding is void: Spec5 [4.1] makes an unacknowledged
	// outbound QoS 1/2 delivery session state, and a discard destroys it rather than
	// carrying it into whatever session next uses this client ID.
	//
	// A BOUNDED REAP is used here rather than the arbitrated walk above, and the contrast
	// is deliberate: on THIS broker nothing else is competing for a register — no claim
	// record, no marker — whereas a hold record is a payload another broker's releaser may
	// still be committed to delivering. (A register is not inert even so: a resuming
	// session reads one as durable authority, and the slot-only custody contest rewrites it
	// to take ownership, see claimRecoveredInflight.) Until issue #38 this was a blind FILTER PURGE; it is now
	// reapIncarnationRegisters, which enumerates, loads, checks each occupant's identity
	// and deletes at exact sequences at or below its own enumeration watermark. The
	// enumeration filter names the RETIRING generation, which clear() re-mints before this
	// runs, so a delayed pass cannot name a subject the NEXT incarnation occupies.
	//
	// That is the whole of what is established, and it is deliberately NOT a correctness
	// verdict. A resume preserves the generation, so a peer may still be serving this
	// incarnation and holding registers under these very subjects; "no second party" is a
	// statement about this broker, not about the cluster. issue #38 AUDITED what that costs:
	// the destruction itself is now bounded and identity-checked (below), so a register
	// this pass was not entitled to is spared rather than destroyed. What remains is
	// contention with a genuinely LIVE owner of the same register, which naming cannot
	// arbitrate and which is issue #40's predicate, not this pass's. The same premise's cost
	// on $MQTT_qos2in is issue #66.
	//
	// issue #38 REPLACED the blind filter purge here. It used to be
	// purgeStream(filter, upToSeq 0), which read nothing, and the two things wrong with
	// that needed two different mechanisms: it could not NAME what it destroyed, and it
	// had nothing to BOUND itself by because it enumerated nothing. The replacement
	// enumerates, loads in batches, checks each occupant's identity, skips anything above
	// the watermark the enumeration itself took, and deletes at exact sequences.
	//
	// It also changes this function's failure posture, deliberately: an unreadable or
	// indeterminate register now ABORTS the teardown rather than being purged blind, so
	// the record is not released while a register of unknown status remains. What can then
	// re-drive it is route-specific: a record-backed teardown keeps its marked record and
	// its generation's Outstanding entry, so a later attempt re-enumerates, while clear()'s
	// no-record branch (seq == 0) returns before the claim and has neither, and the
	// clean-start predecessor cleanup runs after its record is already displaced — there
	// nothing re-drives it and only the orphan sweep, under a retirement proof, can reap
	// what it left. A permanently unreadable register wedges this client's teardown until
	// an operator acts only while its marked record persistently cannot be displaced;
	// docs/CONFORMANCE.md states it per call site. That is the right trade against
	// releasing a record on a read that failed.
	//
	// noWait no longer applies to this phase: a conditional delete must receive and
	// classify its outcome, so the classification cannot be fire-and-forget.
	if t.idHash != _EMPTY_ && t.gen != _EMPTY_ {
		if sess.asm == nil {
			// Unreachable for a real session: every session carries an account manager.
			// Fail closed rather than skip the reap, since skipping it silently is what
			// the blind purge effectively did.
			return fmt.Errorf("cannot reap the outbound in-flight registers of session %q: no account manager", t.id)
		}
		if err := sess.asm.reapIncarnationRegisters(sess.jsa.c, t.idHash, t.gen, t.transferLogical); err != nil {
			return fmt.Errorf("unable to reap the outbound in-flight registers of session %q: %w", t.id, err)
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
// On mqttTeardownSuperseded it ALSO hands back the record it classified against and
// that record's sequence, which it has in hand and used to discard. That is the
// hollow survivor of the claim-won deletion window whenever it carries the retiring
// generation, and condemnRetiredSurvivor needs the exact sequence to CAS at. Every
// other outcome returns (nil, 0): released means there is no record, and retry and
// indeterminate mean nothing has been classified at all.
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
func (sess *mqttSession) releaseTeardown(tok recordTx, t *mqttTeardownTarget, nonce string, markerSeq uint64) (mqttTeardownRelease, *mqttPersistedSession, uint64, error) {
	// Removing the record is a record write like any other (I8). In particular this
	// is the write clear() must not be able to perform from a terminated
	// generation, which is what used to let a late writer recreate a dead session.
	sess.mu.Lock()
	terr := sess.validateRecordTxLocked(tok)
	sess.mu.Unlock()
	if terr != nil {
		return mqttTeardownIndeterminate, nil, 0, fmt.Errorf("refusing to release the teardown of session %q: %w", t.id, terr)
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
				return mqttTeardownIndeterminate, nil, 0, fmt.Errorf("unable to verify the teardown release of session %q: %w", t.id, lerr)
			}
			// Absent: the record is released. The lifecycle-scoped half records it; one that
			// does not land leaves the generation's release pending, which the sweep finishes
			// from the positive entry and the record's absence (plan 5.2).
			if sess.asm != nil {
				if rerr := sess.asm.releaseTeardownEntry(t.id, t.gen, nonce, nil); rerr != nil {
					sess.jsa.c.Warnf("MQTT: the release of session %q generation %q is not yet recorded: %v", t.id, t.gen, rerr)
				} else {
					// The generation's recovery visit may already have found this release
					// pending: the drive retires it now rather than at the next sweep.
					sess.asm.kickLifecycleRetirement(t.id)
				}
			}
			return mqttTeardownReleased, nil, 0, nil
		}
		ps := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
			// An unreadable record is not ours to interpret, and certainly not proof
			// that our teardown completed. Fail closed.
			return mqttTeardownIndeterminate, nil, 0, fmt.Errorf("unable to parse the session record of %q while releasing its teardown: %w", t.id, uerr)
		}
		if ps.Teardown != nil && ps.Teardown.Nonce == nonce {
			// Still our marker: the delete genuinely did not take. Retry it at the
			// sequence it now sits on (a resave can have moved it).
			markerSeq = smsg.Sequence
			lastErr = fmt.Errorf("session %q teardown marker still present at sequence %v", t.id, smsg.Sequence)
			continue
		}
		// A different nonce, or no marker at all: the marker this teardown held is gone.
		// This teardown is over and nothing further may be deleted on its behalf. The record and its
		// sequence go back to the caller, which is what lets the claim-won deletion
		// window be recognised and repaired instead of merely reported.
		return mqttTeardownSuperseded, ps, smsg.Sequence, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("session %q teardown release not confirmed after %d attempts", t.id, mqttTeardownReleaseAttempts)
	}
	return mqttTeardownRetry, nil, 0, lastErr
}

// finishAdoptedTeardown completes a teardown some broker CLAIMED and did not
// finish, so its record can be released and the client can start fresh.
//
// It never resumes the marked state and never overwrites the marker: it adopts the
// existing nonce, repeats the generation-scoped deletions and releases the record. A
// concurrent finisher — including the original owner waking up — converges on the RECORD:
// both delete the same retired names, one wins the record delete, and the loser's
// classifying re-read reports released or superseded.
//
// The inbound QoS 2 hold walk carries a bound of its own, per walk, which is what makes a
// repeat of it safe where naming alone would not have. The in-flight reap beside it
// carries the same kind of bound since issue #38. "Idempotent" is still not claimed for either:
// a repeat is SAFE, which is a weaker and sufficient statement. issue #66 states the first and
// issue #38 the second; neither is restated here.
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
	sess := mqttSessionCreate(&as.jsa, ps.ID, idHash, ps.Gen, seq, ps.WID, ps.PWID, opts)
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

	// The retirement is recorded BEFORE the adoption's first deletion, exactly as on
	// the owner's own path: an adopter deletes the same resources, so it needs the same
	// fence, and the marker it adopted is just as evictable by an unconditional write. The
	// adopted marker's sequence is the ordering fact — it IS where the claim landed.
	// A failure aborts the adoption with nothing deleted; the record stays marked and a
	// later adopter tries again. That now includes a record whose client ID does not
	// hash back to the subject it was read from: the fence is validated before it is
	// written, so such a record is refused here rather than having its durables deleted
	// on the strength of a name nothing can attribute. Two of the three adoption callers
	// already refused it earlier (the CONNECT restore compares ps.ID to the connecting
	// client, and the sweep runs mqttValidateTeardownTargets); the expiry path did not,
	// and this is where it fails closed.

	// SETTLE FIRST, on EVERY adoption (issue #107 8b). A proposal standing over a marked record can
	// only be ABORTED — its metadata commit CASes the record at a sequence the marker claimed
	// past — so settlement cancels a preparation back to its serving predecessor or closes an
	// aborted reservation. Without it a receipted close meets PREPARING and refuses forever, and
	// a receiptless one has nothing it may decide on. Expiry and the orphan sweep reach here
	// with no CONNECT ahead of them, so the adoption is the only settler they have. Bounded by
	// one request timeout, settlement arms only (the same run the discard boundary uses). A
	// proposal still standing withholds: the record stays MARKED and the next adopter repeats.
	auth, serr := as.settleAdoptedAuthority(ps.ID)
	if serr != nil {
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, serr)
	}
	// THE CLAIM'S OWN EVIDENCE (plan 5.2): a marker is committed with its generation's
	// Outstanding entry and its discarding closure, as one batch. The
	// adoption reads the entry naming this marker; a marker the authority does not carry is
	// Inconsistent, and nothing is deleted on its strength.
	if auth.status != mqttOutboundValid || auth.rec == nil || auth.rec.Life == nil ||
		!mqttOutstandingNamesMarker(auth.rec.Life, ps.Gen, ps.Teardown.Nonce) {
		return fmt.Errorf("adopting the teardown of session %q: the authority (%v) carries no outstanding entry for its marker",
			ps.ID, auth.status)
	}
	// The discarding closure the claim committed: its retirement is the recovery executor's,
	// promptly and independently of the shared closing below (issue #107 7d).
	if err := as.jsa.closeTeardownGeneration(ps.ID, as.sessStreamEpoch, ps.Gen); err != nil {
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, err)
	}
	as.kickRecoveryRoot(ps.ID, as.sessStreamEpoch, ps.Gen)
	// CLOSE THE SHARED ACQUISITIONS of the adopted generation through the ONE closing driver
	// (issue #107 8b), exactly as on the owner's path and before its first deletion: licensed by the
	// generation's closure record or a settled authority proving it never served, seal, release
	// everything the sealed ledger lists, and refuse to release the record until it reads
	// sealed and empty. Repeat-safe from the durable ledger.
	if err := as.jsa.closeRetiringGeneration(ps.ID, as.sessStreamEpoch, ps.Gen, mqttAuthorityNeverServed(auth, ps.Gen)); err != nil {
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, err)
	}
	// WHO OWNS THIS GENERATION'S CLEANUP, decided BEFORE the first deletion (issue #107, row 8).
	//
	// The release gate below asks its own question after the deletions have run, which is right
	// for a RELEASE decision and useless for a DELETION one: by then the affine consumer would be
	// gone whatever the answer. An INDETERMINATE reading refuses the adoption with nothing
	// deleted — fail-closed, and never the vacuous path, which is the one that licenses deleting
	// everything.
	own, cerr := as.classifyTeardownGeneration(ps.ID, as.sessStreamEpoch, ps.Gen, target)
	if cerr != nil {
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, cerr)
	}
	own.transfers(target)
	as.reportUnenrolledTargets(ps.ID, ps.Gen, own)
	if err := sess.deleteTeardownTargets(target, false); err != nil {
		return err
	}
	// The proofs are secured and the assignments discarded by the call above; this asserts that
	// and seals, so the transferred cleanup becomes actionable. A failure is not fatal — the
	// recovery pass finishes both from the same durable evidence — and what matters is that the
	// consumer was NOT deleted, which is already true.
	as.finishAffinePrerequisites(as.server(), ps.ID, as.sessStreamEpoch, ps.Gen, own, target)
	// The window between an adoption's deletions and its release, in which a peer's
	// record is still invisible to every check that ran before it. Both callers'
	// superseded dispositions are reached through here, so a test arms one broker in
	// exactly this window rather than racing the two for real.
	if h := mqttTestTeardownAdoptionWindowHookV.Load(); h != nil {
		(*h)(ps.ID, idHash, seq)
	}
	// THE METADATA RELEASE IS GATED ON DURABLE FACTS ABOUT THE OLD GENERATION (issue #107, row 8).
	//
	// Releasing here is what lets a fresh acceptance in. If the old generation still has duties
	// that nothing will traverse, this release makes that permanent — so the gate asks whether
	// the generation is CLOSED with a sealed inventory, and whether its surviving work is
	// enrolled for recovery or durably certified complete.
	//
	// VACUOUS for a generation with neither a ledger nor a closure record: it admitted nothing
	// through the authority, so there is no inventory for these facts to be about. That is what
	// lets the gate stand here while the routes that create inventory are still unconverted.
	//
	// THE DISCARD DISPOSITION IS NOT ASKED OF THIS CALLER, and that is the point of where it
	// lives: `gatherAdoptedReleaseFacts` reads it off the CLOSURE RECORD, which carries it as a
	// mandatory member and is written by the transition that closes the generation — at the
	// moment the protocol event is true, rather than re-derived here by a teardown that no
	// longer has it in front of it.
	//
	// WHAT THIS TEARDOWN NO LONGER WAITS FOR is the physical cleanup (issue #107 7d). In a discarding
	// generation the source, drain, PUBREL and affine consumers were TRANSFERRED to the recovery
	// executor above, so the gate asks only whether that owner is durable — a reachable root
	// under the discarding closure, or a certificate — never whether the deletes happened. The
	// shared group durables were released by the shared closing above, which is its own gate.
	// THE ACCOUNT'S RECOVERY EPOCH, not the broker's run id. Every issue #107 record is stamped with
	// the $MQTT_sess incarnation identity, and reading a record under the wrong epoch refuses it
	// as unattributable — which here would turn every read into an error rather than an answer.
	if facts, ferr := as.gatherAdoptedReleaseFacts(ps.ID, as.sessStreamEpoch, ps.Gen); ferr != nil {
		// AN UNRESOLVED READ IS NOT AN ABSENCE. Proceeding would take the vacuous path and
		// release exactly the generation the gate exists to protect.
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, ferr)
	} else if gerr := mqttAdoptedReleaseAuthorized(ps.ID, ps.Gen, facts); gerr != nil {
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, gerr)
	}
	switch res, _, _, rerr := sess.releaseTeardown(tok, target, ps.Teardown.Nonce, seq); res {
	case mqttTeardownReleased:
	case mqttTeardownSuperseded:
		// The record moved after this adoption's deletions ran: an acceptance of a fresh
		// generation, never a survivor of this one (plan 6.3b).
		// A record that is not ours is there now. Nothing further is ours to delete —
		// and in particular the group durables its membership may hold are not.
		//
		// SURFACED, not swallowed. Returning nil here told createOrRestoreSessionWithin the
		// adoption had completed, and it handed back a fresh base-0 object without
		// re-reading the record it had authoritatively been told exists — a write on a
		// base already known to be stale, which is R1's one production violation. The
		// two terminal callers are given this branch explicitly rather than inheriting
		// an error arm. See sess_takeover_redrive.go.
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, errMQTTTeardownAdoptionSuperseded)
	default:
		return rerr
	}
	// The shared group durables were released BEFORE the record went, from the sealed
	// acquisition ledger (closeSharedGeneration above).
	return nil
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
	// A draining durable belongs to this session too: it outlived the removal of its
	// membership (an UNSUBSCRIBE, or a QoS 0 re-SUBSCRIBE)
	// only because started deliveries were owed, and the session ending discharges
	// that. See unsub_drain.go.
	for _, pd := range ps.Drains {
		if pd != nil && pd.Cons != nil {
			t.durs = append(t.durs, pd.Cons.Durable)
		}
	}
	if ps.PubRel != nil {
		t.pubRelDur = ps.PubRel.Durable
	}
	if ps.Q2Aff != nil {
		t.q2AffDur = ps.Q2Aff.Durable
	}
	return t
}

// settleAdoptedAuthority reads the authority an adopted teardown's client stands under (issue
// #107 8b). Acceptances are one batch each (issue #133), so no proposal can stand to be settled;
// an authority the store cannot answer for is an error, and the adoption withholds.
func (as *mqttAccountSessionManager) settleAdoptedAuthority(id string) (mqttAuthorityLoad, error) {
	load := as.jsa.loadOutboundAuthority(id, as.sessStreamEpoch)
	switch load.status {
	case mqttOutboundValid, mqttOutboundAbsent:
		return load, nil
	default:
		return load, fmt.Errorf("%w: the authority reads %v", errMQTTClosingUnlicensed, load.status)
	}
}
