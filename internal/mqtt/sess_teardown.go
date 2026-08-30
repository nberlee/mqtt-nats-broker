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
//     F-062's purge bound: a removal names a subject but destroys only up to the
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
// `$MQTT_inflight` reap it is bounded by that pass's own enumeration watermark — F-034
// replaced the unbounded purge that used to sit there. F-062 and F-034 state those two; do
// not restate them here.) That also makes a crashed teardown
// self-healing, which today leaks until the orphan sweep. A record the restore
// boundary refuses is never adopted by the CONNECT: a reserved-namespace record whose
// generation is valid is still finished by the expiry path and the orphan sweep, while
// one with no valid generation is adopted by NOTHING — every deletion here is
// generation-scoped, so there is no safe name to issue — and waits for an operator to
// recreate the streams. See docs/ARCHITECTURE.md, teardown section.

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
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
// unsafe are stated once in F-062 (docs/findings/ha-arbitration.md); do not re-derive them
// here, and do not paraphrase the entitlement criterion — three review rounds were spent on
// restatements of it that were a shade too strong.
//
// It extends to `$MQTT_inflight` too, since F-034: the blind filter purge there was
// replaced by an enumerate-load-classify-delete pass whose entitlement is its own
// enumeration watermark, so a subject re-occupied since the enumeration is SPARED rather
// than destroyed.
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

// mqttTeardownClaimAttempts bounds the retry of a claim rejected by transient
// clustered contention (10164), which is a rejection but NOT evidence that the
// subject moved.
const mqttTeardownClaimAttempts = 3

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
func (sess *mqttSession) claimTeardown(tok recordTx, ps *mqttPersistedSession, seq uint64, retiringWID string) (markerSeq uint64, nonce string, outcome mqttTeardownClaim, err error) {
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
	// The marker IS a record write, so it carries a lineage like any other: a fresh
	// identity of its own, parented to the record it was derived from. retiringWID is
	// passed in because the snapshot arrives with no lineage — the builder mints none —
	// so a parent derived from ps would come out empty and the marker would look like a
	// first create. Every attempt below re-sends THESE bytes, so the identity is minted
	// once and a write that landed with its response lost stays recognisable.
	// See sess_fork_lineage.go.
	marked.WID, marked.PWID = mqttNewRecordWID(), retiringWID
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
			// The record is gone, so this session was already released — by what, an
			// absence cannot say. There is nothing to claim and nothing to delete on its
			// behalf.
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
			// A teardown marker that is not ours holds this record. Whoever wrote it — or
			// the next reader that can interpret the record, a CONNECT or a sweep —
			// finishes it; we delete nothing.
			return 0, nonce, mqttTeardownClaimSuperseded, nil
		case smsg.Sequence != seq:
			// The record moved past the sequence this claim resolved against.
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

// ---------------------------------------------------------------------------
// The durable generation-retirement watermark
// ---------------------------------------------------------------------------

// A won claim licenses the deletions, and the release that would tell us whether we
// still own the claim runs AFTER them — it has to, because it proves absence rather
// than authorship, and reordering it would let an admitted proposal recreate the same
// shape one step later. So the claim marker is the only durable record of the
// retirement while the deletions run, and the marker lives on the record's own
// subject: an UNCONDITIONAL foreign write can land after the marker did — a CAS
// excludes a competing conditional write off that base (the same-base boundary,
// docs/CONFORMANCE.md) but not one carrying no expectation — and MaxMsgsPer:1 then
// EVICTS the marker. What survives is a record
// naming exactly the resources the winner just deleted, with no marker for any
// adopter or the orphan sweep to find — a hollow survivor nothing repairs.
//
// The fix is to keep the retirement fact OFF the replaceable subject. Every teardown
// that wins its claim first records a permanent per-client watermark on
// $MQTT_teardown naming the generation it is retiring, and only then deletes. THREE
// DESTRUCTIVE readers consume it: the winner's own release-superseded arm, which
// condemns a same-generation survivor; the CONNECT restore, which refuses to resume a
// retired generation; and the orphan sweep, which condemns one with no CONNECT at all.
// A fourth route, the record-absent create, no longer draws a CONCLUSION from it: the
// write-lineage audit removed its inheritance of the retired record's identity, because
// every verdict that inheritance produced was a false fork, and the identity field
// itself was removed later still. What that route retains is a corrupt-status read
// ALONE — it refuses a create over a watermark it cannot read at all, and draws nothing
// from one it can.
//
// A generation whose retirement was ever RECORDED is retired, and stays fenced:
// nothing ever reaps a watermark, so no GC choice can widen the residual. The
// deliberate exception is a crash BETWEEN the won claim and the watermark write,
// which is non-destructive by construction — the write precedes every deletion, so
// such a generation has lost nothing and is legitimately resumable.
//
// The two escapes that remain are instances of the apply-time CAS hole itself: the
// watermark names only the LATEST retirement, so an older retired generation can
// reappear through a proposal that outlived a whole intervening session lifecycle,
// and a cross-term apply on $MQTT_teardown can regress the watermark. Both are stated
// in docs/CONFORMANCE.md.

// mqttGenRetirementAttempts bounds the retry of the watermark write. Same shape and
// same reason as the claim: a CAS rejection is contention, resolved by re-reading,
// never classified. Exhausting it fails the teardown CLOSED, before any deletion.
const mqttGenRetirementAttempts = 3

// mqttGenRetirement is the durable per-client retirement watermark on
// $MQTT_teardown, one record per client subject (MaxMsgsPer:1), permanent.
//
// MarkerSeq is the $MQTT_sess sequence this teardown's claim marker landed at, and
// it is the ORDERING field: replacement is decided by it alone and never by
// last-writer-wins, so a paused writer cannot make an older retirement final
// (feedback-durable-arbitration-not-dedup). A landed sequence is never 0, so 0 is
// how a malformed watermark is recognised. Sequences are comparable across the whole
// stream by construction, because validateTeardownStream refuses a $MQTT_teardown
// belonging to any other $MQTT_sess incarnation.
//
// Gen names the retired incarnation; ID lets every reader check the record hashes
// back to the subject it was read from. Nonce, Origin and StartedAt are diagnostic.
type mqttGenRetirement struct {
	ID        string `json:"id"`
	Gen       string `json:"gen"`
	MarkerSeq uint64 `json:"mseq"`
	Nonce     string `json:"nonce"`
	Origin    string `json:"origin"`
	StartedAt int64  `json:"started"`
}

// mqttRetirementStatus is the typed outcome of a watermark load. There is
// deliberately no stale-epoch state: a stream whose watermarks could carry another
// incarnation's sequences is refused wholesale at provisioning (validateTeardownStream),
// which is the boundary-compliant refusal, rather than classified per read.
type mqttRetirementStatus int

const (
	mqttRetirementAbsent        mqttRetirementStatus = iota // no watermark for this client
	mqttRetirementValid                                     // decoded and fully validated
	mqttRetirementCorrupt                                   // decode failure, or a field that fails validation
	mqttRetirementIndeterminate                             // transient load failure
)

// validateTeardownStream verifies a $MQTT_teardown stream carries the exact
// permanent-watermark configuration. Derived from what this owner does: a per-client
// watermark CAS (create-only, or replacement at a known sequence), exact-subject
// loadLastMsgFor reads, and one filtered walk by the sweep — and NOTHING else. Global
// message/byte/age limits would eventually evict a permanent fence; DiscardNew would
// reject the CAS replacement of a client's prior watermark; a source could occupy a
// client's slot; a size cap would reject the write that gates every deletion.
//
// No operation ever deletes or purges here, by design — a retirement fence is
// permanent, and an age- or delete-based reap of it would be an unfenced reap of the
// fence itself — so DenyDelete, DenyPurge and SubjectDeleteMarkerTTL are audited
// NON-prerequisites. No consumer is ever created, so
// MaxConsumers and ConsumerLimits are waived, and no Nats-TTL is ever sent, so
// AllowMsgTTL is inert.
//
// sessEpoch is the $MQTT_sess incarnation epoch this account adopted. A MarkerSeq is
// a $MQTT_sess sequence, so watermarks are comparable to each other only within one
// incarnation of that stream: $MQTT_teardown is epoch-COUPLED to it, stamped at
// creation and required to match here. That refuses the partial recreation that would
// otherwise mis-compare sequences across incarnations — $MQTT_sess recreated alone
// mints a fresh epoch while the permanent watermarks keep the old one — instead of
// classifying it per read. The other direction ($MQTT_teardown recreated alone) stamps
// the current epoch, validates clean, and admits with no historical fences; that loss
// is stated in docs/CONFORMANCE.md.
//
// wantReplicas is 0 when no explicit replica count is configured (skip that check).
func validateTeardownStream(si *StreamInfo, domainTk string, wantReplicas int, sessEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttTeardownSubjectPrefix + domainTk + ">"}, wantReplicas); err != nil {
		return err
	}
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits): nothing consumes a retirement watermark, so any other retention drops it immediately", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		return fmt.Errorf("discard is %v (want old): DiscardNew would reject the CAS replacement of a client's prior watermark", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		return fmt.Errorf("max_msgs_per_subject is %d (want 1): the per-client watermark is a single-slot register its CAS depends on", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict a permanent retirement fence, unfencing a generation whose resources are already deleted",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a client's watermark slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject the watermark write that must land before any teardown deletion", cfg.MaxMsgSize)
	}
	stamp := cfg.Metadata[mqttSessEpochMetaKey]
	if !mqttValidSessEpoch(stamp) {
		return fmt.Errorf("metadata %q is %q, not a well-formed session-stream incarnation epoch: a retirement watermark orders itself by a $MQTT_sess sequence, which is only comparable within the incarnation it came from",
			mqttSessEpochMetaKey, stamp)
	}
	if stamp != sessEpoch {
		return fmt.Errorf("metadata %q is %q but this account's session stream is incarnation %q: the marker sequences these watermarks carry belong to a different $MQTT_sess",
			mqttSessEpochMetaKey, stamp, sessEpoch)
	}
	return nil
}

// Test hooks over the watermark's two JetStream operations. Atomic for the reason
// every other seam here is: the sweep goroutine and a connection's readLoop both
// reach them, so a plain package variable would be a data race.
//
// The load hook returns the classified outcome directly, so a test can present a
// malformed or unreadable watermark to any reader; the store hook fails the write, so
// a test can prove the teardown aborts BEFORE its first deletion.
var (
	mqttTestRetirementLoadHookV  atomic.Pointer[func(idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus)]
	mqttTestRetirementStoreHookV atomic.Pointer[func(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error)]
)

// setMQTTTestRetirementLoadHook installs (or, with nil, clears) the watermark load
// hook and returns a function that restores the previous value.
func setMQTTTestRetirementLoadHook(h func(idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus)) func() {
	prev := mqttTestRetirementLoadHookV.Load()
	if h == nil {
		mqttTestRetirementLoadHookV.Store(nil)
	} else {
		mqttTestRetirementLoadHookV.Store(&h)
	}
	return func() { mqttTestRetirementLoadHookV.Store(prev) }
}

// setMQTTTestRetirementStoreHook installs (or, with nil, clears) the watermark store
// hook and returns a function that restores the previous value.
func setMQTTTestRetirementStoreHook(h func(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error)) func() {
	prev := mqttTestRetirementStoreHookV.Load()
	if h == nil {
		mqttTestRetirementStoreHookV.Store(nil)
	} else {
		mqttTestRetirementStoreHookV.Store(&h)
	}
	return func() { mqttTestRetirementStoreHookV.Store(prev) }
}

// mqttTeardownSubject returns the per-client retirement watermark subject.
func mqttTeardownSubject(domainTk, idHash string) string {
	return mqttTeardownSubjectPrefix + domainTk + idHash
}

// mqttTeardownIDHashFromSubject extracts the client-ID hash token from a watermark
// subject, requiring the exact shape this account writes. Derived from the SUBJECT
// rather than from record content for the reason mqttSessIDHashFromSubject documents:
// a record is not a trusted input, and a hash taken from its body would let one
// client's watermark speak for another's.
func mqttTeardownIDHashFromSubject(subject, domainTk string) (string, error) {
	prefix := mqttTeardownSubjectPrefix + domainTk
	if !strings.HasPrefix(subject, prefix) {
		return _EMPTY_, fmt.Errorf("subject does not belong to this account/domain")
	}
	hash := subject[len(prefix):]
	if !mqttValidIDHashToken(hash) {
		return _EMPTY_, fmt.Errorf("subject carries a malformed client-ID hash token %q", hash)
	}
	return hash, nil
}

// mqttValidateGenRetirement is the ONE definition of a watermark this broker may act
// on, shared by the writer and every reader. Sharing it is the point: the three
// destructive readers draw a conclusion from the watermark — condemn a live record,
// discard a record at CONNECT — so a reader with its own weaker subset would act on a
// fact the writer would have refused to record. Anything malformed is CORRUPT
// and fails closed everywhere: the writer aborts before deleting, the CONNECT is
// refused, and the sweep mutates nothing for that client. It is never repaired in
// place.
//
// A fourth reader, the record-absent CREATE (createOrRestoreSessionWithin), draws no
// destructive conclusion and no longer draws a PARENT either — the write-lineage audit
// deleted that inheritance — but it still reads the watermark for one outcome and still
// REFUSES the CONNECT on a corrupt one. Conceding there would admit a session into a
// store whose own later teardown can never record its retirement, because
// recordGenRetirement refuses to replace a watermark it cannot read, and the record would
// then stay MARKED for every future finisher.
func mqttValidateGenRetirement(r *mqttGenRetirement, idHash string) error {
	if r == nil {
		return fmt.Errorf("no retirement watermark")
	}
	if getHash(r.ID) != idHash {
		return fmt.Errorf("watermark names a client ID that does not hash back to %q", idHash)
	}
	if !mqttValidSessGen(r.Gen) {
		return fmt.Errorf("watermark carries a malformed retired generation %q", r.Gen)
	}
	if r.MarkerSeq == 0 {
		return fmt.Errorf("watermark carries no teardown marker sequence")
	}
	return nil
}

// storeGenRetirementCAS writes r to subject expecting expectSeq as the subject's
// current last sequence (0 = create-only), mirroring storeWillCancelCAS.
func (jsa *mqttJSA) storeGenRetirementCAS(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error) {
	b, _ := json.Marshal(r)
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

// loadGenRetirement is the ONE read every reader and the writer use, honoring the
// test seam so an injected watermark is seen identically by all of them.
func (jsa *mqttJSA) loadGenRetirement(domainTk, idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
	if h := mqttTestRetirementLoadHookV.Load(); h != nil {
		return (*h)(idHash)
	}
	return jsa.loadGenRetirementRecord(domainTk, idHash)
}

// storeGenRetirement is the writer's store, honoring the test seam. Separate from
// the raw CAS so a hook can delegate to it after observing the call.
func (sess *mqttSession) storeGenRetirement(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error) {
	if h := mqttTestRetirementStoreHookV.Load(); h != nil {
		return (*h)(subject, expectSeq, r)
	}
	return sess.jsa.storeGenRetirementCAS(subject, expectSeq, r)
}

// loadGenRetirementRecord reads a client's watermark and classifies it. The subject
// sequence is returned on every non-absent outcome (it is available pre-decode) so
// the writer can CAS at the exact sequence it read.
func (jsa *mqttJSA) loadGenRetirementRecord(domainTk, idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
	smsg, err := jsa.loadLastMsgFor(mqttTeardownStreamName, mqttTeardownSubject(domainTk, idHash))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, mqttRetirementIndeterminate
		}
		return nil, 0, mqttRetirementAbsent
	}
	r := &mqttGenRetirement{}
	if uerr := json.Unmarshal(smsg.Data, r); uerr != nil {
		return nil, smsg.Sequence, mqttRetirementCorrupt
	}
	if verr := mqttValidateGenRetirement(r, idHash); verr != nil {
		return r, smsg.Sequence, mqttRetirementCorrupt
	}
	return r, smsg.Sequence, mqttRetirementValid
}

// recordGenRetirement durably records that this teardown is retiring t.gen, and is
// called between the won claim and the first deletion. ALWAYS confirmed, even on a
// no-wait discard: a fence that only reached a send queue is not a fence.
//
// The advance is decided by MarkerSeq ORDERING ALONE — the generation plays no part
// in it. A generation-equality early return looks equivalent and is not: it would let
// a newer same-generation retirement be skipped, leaving a paused older writer's CAS
// to become the final watermark.
//
//   - absent -> create-only CAS.
//   - standing MarkerSeq >= ours -> done. The standing fence is at least as new as
//     our fact, and equality is our own write already landed.
//   - standing MarkerSeq < ours -> CAS-replace at the sequence we loaded.
//   - corrupt -> fail closed. Never overwritten: a watermark we cannot read may name a
//     retirement newer than ours, and replacing it would unfence that generation.
//   - rejection / indeterminate -> re-read and decide again (never classify a
//     rejection: 10164 is contention).
//
// The monotonicity is DECISION-time monotonicity. A CAS admitted before a leader
// transition applies unchecked, so apply-time regression is possible; that is the
// stated residual, not something this loop can prevent.
//
// retiredWID is the identity of the record this teardown is about to delete. It is NOT
// stored: the watermark used to carry it as an rwid field, and that field was removed
// because a write identity names a record's BYTES rather than one OCCURRENCE of them —
// storeFramedSessionMsg replays acknowledged bytes verbatim and restore adopts WID/PWID
// unvalidated, so one identity can stand at two sequences and no reader could tell which
// occurrence a retirement removed. See docs/ROADMAP.md's vanished-record bullet for the
// worked interleaving and for what an exact reader would need instead.
//
// What survives is the PRECONDITION it always doubled as, and it is load-bearing: a
// session record carrying NO identity cannot be retired by name, so its teardown fails
// CLOSED here, before any deletion, leaving the record marked for a later finisher.
// Nothing else in the teardown path checks WID or PWID — mqttValidateTeardownTargets
// does not — so removing this check with the field would have turned that refusal into a
// successful teardown of a record nothing can name. Threaded in rather than read off t
// because the target enumerates the RESOURCES an incarnation owns, while this is a fact
// about the claim: clear() captures it before its reset, and finishAdoptedTeardown takes
// the marker's own PWID, the record that teardown claimed rather than the marker the
// release deletes. See TestRetirementWIDlessRecordFailsClosed.
func (sess *mqttSession) recordGenRetirement(t *mqttTeardownTarget, nonce, retiredWID string, markerSeq uint64) error {
	if retiredWID == _EMPTY_ {
		// The precondition, checked where the removed rwid field's validator row used to
		// enforce it: before `want` is built and before any deletion, and wrapped by the
		// same refusal below so the failure still names the retirement it could not record.
		return fmt.Errorf("refusing to record the retirement of session %q: the record it "+
			"would retire carries no identity", t.id)
	}
	subject := mqttTeardownSubject(sess.domainTk, t.idHash)
	want := &mqttGenRetirement{
		ID:        t.id,
		Gen:       t.gen,
		MarkerSeq: markerSeq,
		Nonce:     nonce,
		Origin:    sess.jsa.id,
		StartedAt: time.Now().Unix(),
	}
	if verr := mqttValidateGenRetirement(want, t.idHash); verr != nil {
		// Refusing to write what we would refuse to read. Reached only if the teardown
		// target itself is malformed, which every caller's record already excludes.
		return fmt.Errorf("refusing to record the retirement of session %q: %w", t.id, verr)
	}
	var lastErr error
	for attempt := 0; attempt < mqttGenRetirementAttempts; attempt++ {
		cur, seq, status := sess.jsa.loadGenRetirement(sess.domainTk, t.idHash)
		var expect uint64
		switch status {
		case mqttRetirementValid:
			if cur.MarkerSeq >= markerSeq {
				return nil
			}
			expect = seq
		case mqttRetirementAbsent:
			expect = 0
		case mqttRetirementCorrupt:
			return fmt.Errorf("refusing to retire generation %q of session %q: its retirement watermark is unreadable",
				t.gen, t.id)
		default: // mqttRetirementIndeterminate
			lastErr = fmt.Errorf("retirement watermark load indeterminate")
			continue
		}
		if _, err := sess.storeGenRetirement(subject, expect, want); err == nil {
			return nil
		} else {
			lastErr = err
			// The write may have landed with its response lost, or lost the CAS to a
			// concurrent higher write. A re-read of the durable truth resolves both, and
			// either way the fence we needed is standing.
			if sess.retirementFenced(t.idHash, markerSeq) {
				return nil
			}
		}
	}
	return fmt.Errorf("unable to record the retirement of generation %q of session %q after %d attempts: %w",
		t.gen, t.id, mqttGenRetirementAttempts, lastErr)
}

// retirementFenced reports whether the durable watermark is a valid one whose
// MarkerSeq is at least markerSeq — i.e. whether the fence this teardown needed is
// standing, whoever wrote it. Used to resolve an ambiguous write.
func (sess *mqttSession) retirementFenced(idHash string, markerSeq uint64) bool {
	r, _, status := sess.jsa.loadGenRetirement(sess.domainTk, idHash)
	return status == mqttRetirementValid && r.MarkerSeq >= markerSeq
}

// condemnRetiredSurvivor marks the record that superseded this teardown's release
// when that record is the HOLLOW SURVIVOR of the deletion window: same generation,
// unmarked, naming resources this teardown has already destroyed.
//
// Called from BOTH release-superseded arms — clear()'s and finishAdoptedTeardown's —
// before they return their existing sentinels. It is a side repair, not a new
// outcome: no caller disposition changes.
//
// MARK ONLY. It writes a fresh teardown marker preserving the record's content and
// stops: a marked record is the fully-handled self-healing state, and every route out
// of it already exists. The survivor's broker observes the foreign marker on its
// session-record watch (or the committed-read backstop) and evicts, its client's
// reconnect adopts the marker and starts fresh with Session Present false, and with
// no client the expiry path or the sweep's marker-adoption class finishes it.
// Deleting or releasing here instead would remove the record before the survivor's
// broker could observe the move, which is the F-036 shape rather than a repair.
func (sess *mqttSession) condemnRetiredSurvivor(tok recordTx, t *mqttTeardownTarget,
	cur *mqttPersistedSession, curSeq uint64) {

	if cur == nil || curSeq == 0 {
		return
	}
	if cur.Gen != t.gen {
		// A DIFFERENT generation, so this teardown - which retires one generation, not a
		// client ID - has no business with it. That is a lineage fact and not a claim that
		// anyone holds the session (F-084); it does not need to be one, because the
		// conclusion drawn is the negative one: do not condemn. This comparison is what
		// keeps the repair from being collateral damage, so it is the one the mutation
		// matrix removes.
		return
	}
	if cur.Teardown != nil {
		// A foreign teardown already owns the survivor; its finisher handles it.
		return
	}
	// A marker LICENSES its adopters to delete every durable the record names, so a
	// record that fails the name-binding checks is condemned by nothing: it is left to
	// the unadoptable-record rules rather than marked. Readers 2 and 3 apply the same
	// check for the same reason.
	if verr := mqttValidateTeardownTargets(cur, t.idHash); verr != nil {
		sess.jsa.c.Warnf("MQTT: refusing to condemn the surviving record of session %q: %v", t.id, verr)
		return
	}
	_, _, outcome, err := sess.claimTeardown(tok, cur, curSeq, cur.WID)
	switch outcome {
	case mqttTeardownClaimWon:
		sess.jsa.c.Warnf("MQTT: condemned the record of session %q, which survived the teardown of generation %q and names resources it deleted",
			t.id, t.gen)
	default:
		// The subject moved again, or the outcome is unknown. The watermark still
		// stands, so readers 2 and 3 remain the backstop; one attempt is all this arm
		// takes.
		sess.jsa.c.Debugf("MQTT: could not condemn the surviving record of session %q (%v): %v", t.id, outcome, err)
	}
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
	as := sess.asm
	if as == nil || t.idHash == _EMPTY_ || t.gen == _EMPTY_ {
		return nil
	}
	pubRelSubject, _, _ := mqttPubRelNames(t.idHash, t.gen)
	records, ok := sess.scanPubRelRecords(pubRelSubject)
	if !ok {
		return fmt.Errorf("unable to read the stored PUBRELs of session %q before deleting the "+
			"consumer that retains them: their PUBREC proofs would be destroyed unread", t.id)
	}
	s := as.server()
	for _, r := range records {
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
// since the first run, what makes a second run safe is the bound each call carries (F-062).
// The `$MQTT_inflight` reap below carries the same kind of bound, since F-034: its
// entitlement is the watermark its own enumeration returned, and it deletes only records
// at or below that, at exact sequences, after checking each occupant's identity.
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
	// affine durable goes. Spec5 [MQTT-4.8.2-5].
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
	// direction is stated once, in F-036 and F-062 (docs/findings/ha-arbitration.md); this
	// paragraph covers neither.
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
	// A BOUNDED REAP is used here rather than the arbitrated walk above, and the contrast
	// is deliberate: on THIS broker nothing else is competing for a register — no claim
	// record, no marker — whereas a hold record is a payload another broker's releaser may
	// still be committed to delivering. (A register is not inert even so: a resuming
	// session reads one as durable authority and rewrites it to take ownership, see
	// claimRecoveredSharedInflight.) Until F-034 this was a blind FILTER PURGE; it is now
	// reapIncarnationRegisters, which enumerates, loads, checks each occupant's identity
	// and deletes at exact sequences at or below its own enumeration watermark. The
	// enumeration filter names the RETIRING generation, which clear() re-mints before this
	// runs, so a delayed pass cannot name a subject the NEXT incarnation occupies.
	//
	// That is the whole of what is established, and it is deliberately NOT a correctness
	// verdict. A resume preserves the generation, so a peer may still be serving this
	// incarnation and holding registers under these very subjects; "no second party" is a
	// statement about this broker, not about the cluster. F-034 AUDITED what that costs:
	// the destruction itself is now bounded and identity-checked (below), so a register
	// this pass was not entitled to is spared rather than destroyed. What remains is
	// contention with a genuinely LIVE owner of the same register, which naming cannot
	// arbitrate and which is F-036's predicate, not this pass's. The same premise's cost
	// on $MQTT_qos2in is F-062.
	//
	// F-034 REPLACED the blind filter purge here. It used to be
	// purgeStream(filter, upToSeq 0), which read nothing, and the two things wrong with
	// that needed two different mechanisms: it could not NAME what it destroyed, and it
	// had nothing to BOUND itself by because it enumerated nothing. The replacement
	// enumerates, loads in batches, checks each occupant's identity, skips anything above
	// the watermark the enumeration itself took, and deletes at exact sequences.
	//
	// It also changes this function's failure posture, deliberately: an unreadable or
	// indeterminate register now ABORTS the teardown rather than being purged blind, so
	// the record is not released while a register of unknown status remains. What can then
	// re-drive it is branch-specific: a record-backed teardown keeps its marked record and
	// its retirement watermark, so a later attempt re-enumerates, while clear()'s
	// no-record branch (seq == 0) returns before the claim and has neither — there nothing
	// re-drives it and the orphan sweep is the only cleanup. A permanently unreadable
	// register therefore wedges this client's teardown until an operator removes it, which
	// is stated per call site in docs/CONFORMANCE.md and is the right trade against
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
		if err := sess.asm.reapIncarnationRegisters(sess.jsa.c, t.idHash, t.gen); err != nil {
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
			return mqttTeardownReleased, nil, 0, nil // absent: done
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
// existing nonce, repeats the generation-scoped deletions and releases the record. A
// concurrent finisher — including the original owner waking up — converges on the RECORD:
// both delete the same retired names, one wins the record delete, and the loser's
// classifying re-read reports released or superseded.
//
// The inbound QoS 2 hold walk carries a bound of its own, per walk, which is what makes a
// repeat of it safe where naming alone would not have. The `$MQTT_inflight` reap beside it
// carries the same kind of bound since F-034. "Idempotent" is still not claimed for either:
// a repeat is SAFE, which is a weaker and sufficient statement. F-062 states the first and
// F-034 the second; neither is restated here.
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
	//
	// The retired record's identity is the MARKER'S PARENT, not the marker's own WID:
	// claimTeardown parents the marker to the record it claimed, and that record — not
	// the marker, which this release deletes — is the one this teardown's deletions
	// remove. It is no longer STORED on the watermark, and passing ps.WID here would
	// still be wrong: what it supplies is recordGenRetirement's PRECONDITION, and the
	// marker's own identity would satisfy it for a claimed record that has none. An empty
	// value is refused there, which is how a record carrying no lineage fails its
	// adoption closed. See recordGenRetirement.
	if err := sess.recordGenRetirement(target, ps.Teardown.Nonce, ps.PWID, seq); err != nil {
		return err
	}
	if err := sess.deleteTeardownTargets(target, false); err != nil {
		return err
	}
	// The window between an adoption's deletions and its release, in which a peer's
	// record is still invisible to every check that ran before it. Both callers'
	// superseded dispositions are reached through here, so a test arms one broker in
	// exactly this window rather than racing the two for real.
	if h := mqttTestTeardownAdoptionWindowHookV.Load(); h != nil {
		(*h)(ps.ID, idHash, seq)
	}
	switch res, cur, curSeq, rerr := sess.releaseTeardown(tok, target, ps.Teardown.Nonce, seq); res {
	case mqttTeardownReleased:
	case mqttTeardownSuperseded:
		// The claim-won deletion window, reached through the ADOPTION path: this
		// adoption's deletions ran legitimately and the record that survived them may be
		// the same generation's, naming what is now gone. Condemn it before returning the
		// sentinel below, which is unchanged.
		sess.condemnRetiredSurvivor(tok, target, cur, curSeq)
		// A record that is not ours is there now. Nothing further is ours to delete —
		// and in particular the group durables its membership may hold are not.
		//
		// SURFACED, not swallowed. Returning nil here told createOrRestoreSession the
		// adoption had completed, and it handed back a fresh base-0 object without
		// re-reading the record it had authoritatively been told exists — a write on a
		// base already known to be stale, which is R1's one production violation. The
		// two terminal callers are given this branch explicitly rather than inheriting
		// an error arm. See sess_takeover_redrive.go.
		return fmt.Errorf("adopting the teardown of session %q: %w", ps.ID, errMQTTTeardownAdoptionSuperseded)
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
