// Serializing a session discard against an inbound QoS 2 PUBREL release.
//
// The move protocol (qos2_move.go) delivers a held message onward FIRST and only
// then replaces the hold record with a terminal tombstone. That makes the move
// recoverable, but it puts the only arbitration point AFTER the irreversible act:
// a session discard purged the hold subjects by filter, which competes with
// nothing, so a PUBREL that had merely READ the held record went on to fan out,
// and its finalize then read the record's absence as mqttQoS2FinalizeGone — "the
// exchange completed". A message could therefore be delivered out of a session
// the broker had already reported discarded, and the broker recorded that as
// success. An inbound QoS 2 message pending PUBREL is session state (Spec5 [4.1])
// that a discard must destroy, not deliver.
//
// # Two fences, and neither substitutes for the other
//
// A RELEASE CLAIM is a create-only CAS on a sibling of the hold subject
// ("$MQTT.qos2.in.<idHash>.<gen>.<PI>.rel", see mqttQoS2ClaimSubject). The
// releaser writes `releasing`, a discard writes `discarded`, and exactly one
// create can succeed WHILE A RECORD OCCUPIES THE SUBJECT. The loser does nothing
// irreversible. Exclusivity is per-occupancy, not per-identifier for all time; what
// that costs is stated once, in F-062 (docs/findings/ha-arbitration.md).
//
//  1. The claim fences a DISCARD. A re-read of the hold record could not: read
//     then fan out is check-then-act, and the discard is on another broker.
//  2. An authoritative RE-READ of the hold record, after the claim and before
//     anything irreversible, fences an ALREADY-COMPLETED exchange. The claim
//     cannot: a peer that committed the tombstone and reaped both records leaves
//     nothing to contend with, so a stalled releaser's create-only claim would
//     SUCCEED and it would fan out a second time from the payload it still holds
//     in memory. verifyQoS2HoldForRelease requires the original sequence AND the
//     original generation before the caller may deliver.
//
// Because the claim is on a sibling subject, the hold record is never rewritten:
// its payload, its properties and its store time (which ages the Message Expiry
// Interval) all stay exactly as the PUBLISH left them, and the discard can
// enumerate a session's inbound QoS 2 state without reading a single payload.
//
// # Scope rules
//
//  1. DISCARD-EXCLUSIVE, NOT RELEASER-EXCLUSIVE. Two brokers releasing the same
//     exchange share one generation — the generation identifies the exchange, not
//     the broker — so the loser of the CAS reads its own generation back and
//     proceeds. Duplicate onward stores stay suppressed by the existing
//     Nats-Msg-Id; the duplicate live fan-out is residual 1 of the move protocol
//     and is unchanged. This claim is not a mutual exclusion between releasers and
//     must not be described as one.
//  2. EVERY CLAIM DELETE TARGETS THE EXACT SEQUENCE of the claim being reaped,
//     never a purge of the claim subject and never an unsequenced delete — which
//     is why claimQoS2Release returns a sequence alongside its status. A delayed
//     reaper would otherwise remove a LATER exchange's claim on a reused packet
//     identifier, re-exposing that exchange to the stale-loader fan-out fence 2
//     exists to stop. Same rule as releaseQoS2Record and invariant 2 of
//     qos2_move.go.
//     The discard's HOLD purge (purgeQoS2Hold) is not sequenced to one record, because
//     it must remove whatever occupies the subject — a held PUBLISH, a tombstone, or a
//     record it cannot read. It is nonetheless BOUNDED: every call carries the highest
//     sequence its caller can prove it was entitled to destroy, and anything above that
//     is spared (F-062). Naming a subject was never sufficient on its own — generation
//     scoping protects the NEXT incarnation, but a resume preserves the generation, so
//     it never protected a peer still serving the retiring one. Naming was never
//     sufficient for a CLAIM either, which is why every claim delete is sequenced.
//  3. A DISCARD NEVER REAPS ITS OWN `discarded` CLAIM. It is the only durable
//     proof that a late releaser must lose, so reaping it at teardown time re-opens
//     exactly the window this file closes — the same rule as discardQ2AffClaimsFor,
//     where claims are marked and never released. Nothing can license reaping it
//     THERE, because "no releaser is still in flight" is not provable.
//     The aged orphan sweep (orphan_qos2_sweep.go) may retire one, and only under
//     an ordering teardown cannot offer: it destroys the HOLD RECORD FIRST and
//     confirms it gone, then removes the claim. A late releaser that preloaded the
//     payload and resumes afterwards wins a fresh create-only claim against the
//     empty subject, but its mandatory authoritative re-read
//     (verifyQoS2HoldForRelease) then returns mqttQoS2VerifyGone, so it answers the
//     client and delivers nothing. Delete the claim before the hold and that same
//     re-read returns mqttQoS2VerifyHeld and the message is fanned out again.
//     Eligibility is a stable observation on the sweeper's monotonic clock; no stored
//     timestamp participates, and At is never consulted (rule 5).
//     A RELEASER may retire one, under the single exception this rule admits: the
//     marker records the Fence its writer was entitled to destroy, so a releaser whose
//     hold occupied the subject ABOVE that fence has durable proof the marker was never
//     about its exchange, and reaps it CONDITIONALLY at that exact sequence
//     (claimQoS2ReleaseInner, which carries the proof). Never unconditionally, and never
//     by a discard, which is what the rest of this rule still forbids.
//  4. NON-MUTATING DEFAULTS. Every switch over a claim state or a verdict resolves
//     an unhandled value to Indeterminate — suppress PUBCOMP, the client retries —
//     never to a verdict that answers the client or deletes anything. An omitted
//     case then costs a retry instead of a message.
//  5. `At` IS WRITE-ONLY FORENSIC DATA. It is stamped by mqttNewQoS2RelClaim and
//     never read: not for readability (a malformed diagnostic field must not be able
//     to drop a message), not for identity, and not for cleanup. Nor does any OTHER
//     timestamp take its place — a JetStream store time is assigned by whichever
//     server led that stream's Raft group, so no two are from one clock. The orphan
//     sweep instead requires a claim to be stably OBSERVED for its grace, and keys
//     that on the record's sequence. At is deliberately not range-checked anywhere,
//     because the only place a range check could live is the readability predicate it
//     must stay out of.
//
// # Residual
//
// A release that has already won its claim may still fan out after the discard
// completed. The claim orders the delivery DECISION against the discard, not the
// wire: a releaser that won its claim before the discard observed the register is
// committed, and its live core-NATS fan-out may reach subscribers after the
// discard released the session record. A discard that observes a claim waits
// briefly (awaitQoS2Release), which closes the window whenever the releasing
// broker is alive, and closeQoS2ReleaseAndDrain closes it outright on the same
// broker — but a broker that dies mid-release cannot be waited out. See
// docs/CONFORMANCE.md.
//
// What is no longer true: a PUBREL that had merely read the hold record before an
// unarbitrated purge went on to deliver, and its finalize read the resulting
// absence as proof the exchange completed. Both halves are gone.

package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// The two claim states. A claim is a two-party register, not a state machine:
	// whichever party creates it decides, once and for all.
	mqttQoS2RelStateReleasing = "releasing"
	mqttQoS2RelStateDiscarded = "discarded"

	// mqttQoS2ClaimOriginMax bounds the sanitized origin. See mqttQoS2SanitizeOrigin
	// for why it cannot be derived from the local node name.
	mqttQoS2ClaimOriginMax = 64

	// Bounded budgets. Every loop here re-reads and reconciles rather than blindly
	// retrying, so a small budget suffices; exhaustion is reported as a failure (no
	// PUBCOMP, or a failed clear) rather than guessed.
	mqttQoS2ClaimAttempts = 3

	// The discard's wait for a release it observed in flight. The budget is for the
	// WHOLE walk, not per identifier: a session may hold many inbound exchanges, and
	// a per-identifier bound would multiply into a teardown that takes seconds.
	mqttQoS2DiscardWaitBudget   = 300 * time.Millisecond
	mqttQoS2DiscardWaitInterval = 100 * time.Millisecond
)

// qos2RelCondLocked returns the release fence's condition variable, created on
// first use. sess.mu must be held.
func (sess *mqttSession) qos2RelCondLocked() *sync.Cond {
	if sess.qos2RelCond == nil {
		sess.qos2RelCond = sync.NewCond(&sess.mu)
	}
	return sess.qos2RelCond
}

// beginQoS2ReleaseOp admits one inbound QoS 2 release, or refuses because this
// session is being discarded. Every caller that is admitted must call
// endQoS2ReleaseOp.
//
// This is the in-process half of the serialization. The durable claim is what
// arbitrates across brokers, but on THIS broker a discard can do better than
// compete: it can refuse to admit a release it would then have to race.
func (sess *mqttSession) beginQoS2ReleaseOp() bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.qos2RelClosing {
		return false
	}
	sess.qos2RelInFlight++
	return true
}

// endQoS2ReleaseOp releases a release admitted by beginQoS2ReleaseOp.
func (sess *mqttSession) endQoS2ReleaseOp() {
	sess.mu.Lock()
	if sess.qos2RelInFlight > 0 {
		sess.qos2RelInFlight--
	}
	if sess.qos2RelInFlight == 0 && sess.qos2RelClosing {
		sess.qos2RelCondLocked().Broadcast()
	}
	sess.mu.Unlock()
}

// closeQoS2ReleaseAndDrain closes the release fence and waits for every admitted
// release to finish. Called by clear() BEFORE it captures state, so that by the
// time the discard walks the hold subjects no release on this broker can still be
// between its claim and its fan-out.
//
// The fence must stay closed for the WHOLE of clear(), not merely across the
// drain, which is why the caller pairs it with a deferred reopenQoS2ReleaseFence
// rather than reopening beside the other per-connection resets: a release admitted
// while the walk is running would be arbitrated correctly by the durable claim, but
// then the same-broker case would be no better than the cross-broker one and the
// fence would not be closing anything it uniquely can.
//
// sess.mu not held on entry.
func (sess *mqttSession) closeQoS2ReleaseAndDrain() {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.qos2RelClosing = true
	for sess.qos2RelInFlight > 0 {
		sess.qos2RelCondLocked().Wait()
	}
}

// reopenQoS2ReleaseFence admits releases again. clear() is not only a teardown:
// two paths REUSE the session object as a fresh session, and a permanently closed
// fence would make every later inbound QoS 2 exchange on it answer 0x92. Safe
// because the incarnation has been re-minted by then, so a newly admitted release
// works on the fresh generation's subjects, which this teardown cannot name.
//
// sess.mu not held on entry.
func (sess *mqttSession) reopenQoS2ReleaseFence() {
	sess.mu.Lock()
	sess.qos2RelClosing = false
	sess.mu.Unlock()
}

// mqttQoS2RelClaim is the release-claim record.
//
// State, Gen and Fence are its identity and are validated strictly
// (mqttQoS2RelClaimIdentityOK). Origin and At are forensic: neither may ever
// decide whether a claim is readable, because the unreadable disposition drops a
// message and a malformed diagnostic field must not be able to cause that.
type mqttQoS2RelClaim struct {
	State string `json:"state"`
	Gen   string `json:"gen,omitempty"` // the exchange generation: required for releasing, absent for discarded
	// Fence is the highest $MQTT_qos2in sequence the writer of a `discarded` marker
	// was entitled to destroy on the hold subject — the last sequence its enumeration
	// saw. Required for discarded, absent for releasing.
	//
	// It is IDENTITY, not forensics, because it is read and it decides: a later
	// exchange whose hold sits above it proves the marker is not about it (see
	// claimQoS2ReleaseInner) and an adopting discard purges to it rather than to its own
	// watermark (discardQoS2Hold). What it is NOT: an identity of the exchange the
	// marker competed for. It associates the marker with a RANGE of hold occupancies,
	// because the create-only CAS is not atomically tied to any one of them. The claim
	// remains discard-versus-release arbitration; a fence elects no releaser and confers
	// no ownership of the subject.
	Fence  uint64 `json:"fence,omitempty"`
	Origin string `json:"origin"`
	// At is WRITE-ONLY (scope rule 5): stamped here, read by nothing, and replaced by
	// no other timestamp either. A self-reported time from a possibly skewed broker
	// clock cannot be validated, and an implausible value must never be resolvable
	// into "reap it now"; the orphan sweep answers that question by stable
	// OBSERVATION instead, keyed on the record's sequence.
	At int64 `json:"at"`
}

// mqttQoS2RelClaimIdentityOK reports whether a decoded claim carries a usable
// identity.
//
// A `releasing` claim REQUIRES a well-formed exchange generation: it is what tells
// a later PUBREL whether the claim is its own (proceed) or a dead releaser's
// leftover (reap and retry), and a claim without one could be read as either.
// A `discarded` claim requires the generation to be ABSENT, because the discard
// never reads the hold record — that is what keeps payloads off the discard path —
// and a discard needs no generation to identify the exchange it competed for.
//
// The reason this comment used to give — that a discard "can only ever concern the one
// exchange live on that generation-scoped subject" — is FALSE; see F-062, mechanism 2.
// With no generation on the marker a stale one is indistinguishable from an applicable
// one, so the fence supplies that evidence instead, and a `discarded` marker therefore
// REQUIRES one. A fence-less marker is an unsupported persisted shape: it is refused
// here (classifying unreadable) rather than read as covering everything, which is what
// a zero fence would mean to the comparison in claimQoS2ReleaseInner.
//
// `releasing` requires the fence to be ABSENT for the mirror reason it requires a
// generation: a releaser destroys nothing by filter, so a bound it never uses could
// only be misread.
//
// A `discarded` fence must be one that can actually BOUND a purge — the same predicate
// purgeQoS2Hold accepts, so the two cannot drift. That excludes MaxUint64 as well as zero,
// and the reason is a liveness one rather than tidiness: a readable maximal fence would be
// ADOPTED by arm A2 and then rejected by the purge, failing the clear; every later CONNECT
// adoption and every sweep pass would repeat that same failure behind a marked session
// record, which is a permanent client-ID lockout with no route out but an operator. Refusing
// it here instead sends it down the unreadable arm that discardQoS2Hold already has, where
// the record is exact-deleted and the discard re-competes and writes a usable marker — the
// self-healing repair. The releaser side is unaffected either way: `Discarded` and
// `Unreadable` share one branch, so a maximal marker answered the same before and after.
func mqttQoS2RelClaimIdentityOK(c *mqttQoS2RelClaim) bool {
	if c == nil {
		return false
	}
	switch c.State {
	case mqttQoS2RelStateReleasing:
		return mqttValidQoS2Gen(c.Gen) && c.Fence == 0
	case mqttQoS2RelStateDiscarded:
		return c.Gen == _EMPTY_ && mqttQoS2UsableCutoff(c.Fence)
	default:
		return false
	}
}

// mqttQoS2SanitizeOrigin reduces a node name to at most mqttQoS2ClaimOriginMax
// bytes of [0-9A-Za-z._-].
//
// Both halves are load-bearing for the max_payload preflight. The origin comes
// from Server.NodeName, which is caller-supplied and therefore unbounded, and
// which DIFFERS PER BROKER — so deriving the bound from the local value at startup
// would be actively unsafe: a PUBLISH admitted on a broker with a short node name
// could produce a claim that exceeds max_payload when released on a broker with a
// longer one, wedging the exchange exactly as mqttQoS2TerminalRecordMax exists to
// prevent. Restricting the alphabet then makes the JSON encoding equal to the
// string's length, so mqttQoS2ClaimRecordMax is exact rather than an estimate that
// has to allow for escape expansion.
func mqttQoS2SanitizeOrigin(s string) string {
	var b strings.Builder
	for i := 0; i < len(s) && b.Len() < mqttQoS2ClaimOriginMax; i++ {
		c := s[i]
		if c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' ||
			c == '.' || c == '_' || c == '-' {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// mqttNewQoS2RelClaim builds a claim. gen must be empty for a discard and a valid
// exchange generation for a release; fence must be non-zero for a discard and zero
// for a release (mqttQoS2RelClaimIdentityOK enforces both pairings on the read side).
func mqttNewQoS2RelClaim(state, gen, origin string, fence uint64) *mqttQoS2RelClaim {
	return &mqttQoS2RelClaim{
		State:  state,
		Gen:    gen,
		Fence:  fence,
		Origin: mqttQoS2SanitizeOrigin(origin),
		At:     time.Now().Unix(),
	}
}

// buildQoS2RelClaimRecord encodes a claim with its expected-sequence CAS header.
// Sole writer of the claim's wire form, shared with mqttQoS2ClaimRecordMax so the
// encoder and its bound cannot drift.
func buildQoS2RelClaimRecord(expectSeq uint64, c *mqttQoS2RelClaim) (msg []byte, hdrLen int) {
	b, _ := json.Marshal(c)
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(expectSeq, 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdrLen = bb.Len()
	bb.Write(b)
	return bb.Bytes(), hdrLen
}

// mqttQoS2ClaimRecordMax is the upper bound on a claim record's stored size, from
// the real encoder at maximal field values. Like the terminal tombstone it does
// not depend on the PUBLISH at all, so for a small payload with a short topic it
// can be the largest form the exchange writes — and its failure mode is the worst
// available: a PUBLISH accepted at PUBREC time whose PUBREL can then never write
// its claim retries forever with the hold never released.
//
// The expected-sequence header is budgeted at MaxUint64 although every claim write
// is create-only (expectation 0); Gen and Fence are budgeted TOGETHER although the identity
// rule admits no record carrying both; and Fence is budgeted at MaxUint64 although the
// identity rule does not admit that value either. That deliberately over-budgets:
// over-budgeting only rejects a PUBLISH slightly earlier, while under-budgeting
// wedges an accepted exchange.
var mqttQoS2ClaimRecordMax = sync.OnceValue(func() int {
	msg, _ := buildQoS2RelClaimRecord(math.MaxUint64, &mqttQoS2RelClaim{
		// The two state literals are the same length, so either bounds both.
		State:  mqttQoS2RelStateReleasing,
		Gen:    strings.Repeat("z", mqttQoS2GenLen),
		Fence:  math.MaxUint64,
		Origin: strings.Repeat("z", mqttQoS2ClaimOriginMax),
		At:     math.MaxInt64,
	})
	return len(msg)
})

// storeQoS2RelClaimCAS writes a claim to subject expecting expectSeq as the
// subject's current last sequence (0 = create-only) and returns the sequence it
// landed on.
func (jsa *mqttJSA) storeQoS2RelClaimCAS(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
	msg, hdrLen := buildQoS2RelClaimRecord(expectSeq, c)
	resp, err := jsa.storeMsg(subject, hdrLen, msg)
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// loadQoS2RelClaim reads the current claim on subject. Its four outcomes are all
// distinct and none may be collapsed into another:
//
//	(nil, nil, nil)   the subject is empty
//	(nil, smsg, nil)  a record is present but UNREADABLE (the caller needs its
//	                  sequence to reap it, and its metadata to log)
//	(claim, smsg, nil) readable
//	(nil, nil, err)   INDETERMINATE, and never to be guessed into an absence:
//	                  absence authorizes a fresh claim, which is the one decision
//	                  that can violate the serialization this file provides.
func (jsa *mqttJSA) loadQoS2RelClaim(subject string) (*mqttQoS2RelClaim, *StoredMsg, error) {
	smsg, err := jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	if smsg == nil {
		return nil, nil, nil
	}
	c := &mqttQoS2RelClaim{}
	if err := json.Unmarshal(smsg.Data, c); err != nil || !mqttQoS2RelClaimIdentityOK(c) {
		// Unreadable is a DEFINITE fact about the record, not an indeterminate read.
		return nil, smsg, nil
	}
	return c, smsg, nil
}

// storeQoS2RelClaimCAS performs a claim create, honoring an instance-scoped test
// seam. Only claim writes are routed through here, so an injected fault never
// perturbs the hold store, the terminal commit or unrelated loads.
func (as *mqttAccountSessionManager) storeQoS2RelClaimCAS(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
	as.mu.RLock()
	hook := as.testQoS2ClaimStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, c)
	}
	return as.jsa.storeQoS2RelClaimCAS(subject, expectSeq, c)
}

// loadQoS2RelClaim reads a claim, honoring an instance-scoped test seam.
func (as *mqttAccountSessionManager) loadQoS2RelClaim(subject string) (*mqttQoS2RelClaim, *StoredMsg, error) {
	as.mu.RLock()
	hook := as.testQoS2ClaimLoad
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject)
	}
	return as.jsa.loadQoS2RelClaim(subject)
}

// mqttQoS2ClaimStatus is the outcome of a releaser's attempt to claim the release.
// An enum rather than a boolean for the same reason mqttQoS2FinalizeStatus is one:
// "a discard won" (answer the client, deliver nothing), "an unreadable record is
// in the way" (answer the client, deliver nothing, log) and "the backend failed"
// (do NOT answer the client) demand different actions.
type mqttQoS2ClaimStatus int

const (
	mqttQoS2ClaimClaimed        mqttQoS2ClaimStatus = iota // we won the claim
	mqttQoS2ClaimAlreadyClaimed                            // our own generation already holds it
	mqttQoS2ClaimDiscarded                                 // a discard won; nothing may be delivered
	mqttQoS2ClaimUnreadable                                // a record we cannot attribute holds the subject
	mqttQoS2ClaimIndeterminate                             // backend failure; the returned error is non-nil
)

func (st mqttQoS2ClaimStatus) String() string {
	switch st {
	case mqttQoS2ClaimClaimed:
		return "claimed"
	case mqttQoS2ClaimAlreadyClaimed:
		return "already claimed"
	case mqttQoS2ClaimDiscarded:
		return "discarded"
	case mqttQoS2ClaimUnreadable:
		return "unreadable"
	default:
		return "indeterminate"
	}
}

// claimQoS2Release takes the release claim for the exchange identified by gen on
// holdSubject, and is the LAST point at which a discard can still win. It must be
// called before anything irreversible, and its success must be followed by
// verifyQoS2HoldForRelease (fence 2) before the caller delivers.
//
// The returned sequence is the claim's own, for the claim we won OR the one we
// observed, and every reap of that claim must target it (scope rule 2). It is 0
// whenever no claim was observed.
//
// Any store error — not just a rejected CAS — is resolved by an authoritative
// re-read, because an ambiguous write may have landed. Reconciliation:
//
//   - absent                        -> contention; retry the create
//   - `releasing`, our generation    -> AlreadyClaimed (our earlier attempt, or a
//     peer releasing the same exchange); proceed
//   - `releasing`, foreign generation -> a claim left behind by the dead releaser
//     of an earlier exchange on this identifier; reap it at the sequence just
//     observed, CONFIRMED, and retry the create
//   - `discarded`, fencing our hold -> Discarded
//   - `discarded`, fenced BELOW our hold -> a marker that was never about this
//     exchange; reap it at the sequence just observed, CONFIRMED, and retry the create
//   - unreadable                    -> Unreadable
func (as *mqttAccountSessionManager) claimQoS2Release(s *Server, holdSubject, gen string, holdSeq uint64) (mqttQoS2ClaimStatus, uint64, error) {
	status, seq, err := as.claimQoS2ReleaseInner(s, holdSubject, gen, holdSeq)
	// Report the resolved status to any observer (tests only). A releaser that
	// correctly declines to deliver sends nothing, and on a takeover its connection
	// is already closed, so the wire carries no evidence of what it decided.
	as.mu.RLock()
	done := as.testQoS2ClaimDone
	as.mu.RUnlock()
	if done != nil {
		done(holdSubject, gen, status)
	}
	return status, seq, err
}

func (as *mqttAccountSessionManager) claimQoS2ReleaseInner(s *Server, holdSubject, gen string, holdSeq uint64) (mqttQoS2ClaimStatus, uint64, error) {
	// Reach into the load..claim window (tests only): the seam runs once, before the
	// first create, so a test can discard the exchange, or complete and reap it,
	// exactly where a real race would.
	as.mu.RLock()
	pre := as.testQoS2PreClaim
	as.mu.RUnlock()
	if pre != nil {
		if err := pre(holdSubject, gen, holdSeq); err != nil {
			return mqttQoS2ClaimIndeterminate, 0, err
		}
	}

	claimSubject := mqttQoS2ClaimSubjectFor(holdSubject)
	claim := mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, as.jsa.id, 0)
	var lastErr error
	for attempt := 0; attempt < mqttQoS2ClaimAttempts; attempt++ {
		seq, err := as.storeQoS2RelClaimCAS(claimSubject, 0, claim)
		if err == nil {
			return mqttQoS2ClaimClaimed, seq, nil
		}
		lastErr = err
		cur, smsg, lerr := as.loadQoS2RelClaim(claimSubject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		switch {
		case smsg == nil:
			continue // raced away; retry the create
		case cur == nil:
			s.Warnf("MQTT: unusable QoS 2 release claim (%s)",
				mqttQoS2CorruptDetail(claimSubject, smsg, "malformed claim record, or no usable claim identity"))
			return mqttQoS2ClaimUnreadable, smsg.Sequence, nil
		case cur.State == mqttQoS2RelStateDiscarded && holdSeq > cur.Fence:
			// A marker that was never about this exchange. The evidence is STRICTLY
			// INCREASING OCCUPANT SEQUENCES on one hold subject, within one $MQTT_qos2in
			// incarnation — not "monotone occupancy", which is false: occupancy alternates
			// between occupied and empty, and that is the point. It holds because a hold
			// write is a create-only CAS, so a new occupant can only land on an EMPTY
			// subject at the stream's next sequence; the terminal tombstone is not a create
			// but an exact-sequence CAS replacement, which under MaxMsgsPer 1 + DiscardOld
			// lands at a NEW, higher sequence; and every removal — an exact-sequence delete
			// or the teardown's and sweep's filtered purge — leaves the stream's last
			// sequence where it was, so no earlier occupant can return and no sequence is
			// handed out twice.
			//
			// Our record occupied the subject above cur.Fence. Every occupancy at or below
			// Fence — every occupancy the marker's writer was entitled to destroy — therefore
			// ended before ours began and cannot come back, so reaping the marker re-exposes
			// nothing: the only record it could still fence is ours, which it never covered.
			// The evidence is permanent, so a stale holdSeq does not weaken it.
			//
			// This is the sole exception to scope rule 3, and it is exactly as narrow as the
			// rule states: CONDITIONAL, at the EXACT observed sequence, and by a RELEASER
			// holding this evidence — never unconditional and never by a discard. It is also
			// not a shortcut past the fences: the retried create still has to win, and
			// verifyQoS2HoldForRelease still has to find our record at our sequence.
			s.Warnf("MQTT: reaping the QoS 2 discard marker on %q, fenced to %d and superseded by the hold at %d",
				claimSubject, cur.Fence, holdSeq)
			if st, rerr := as.releaseQoS2Record(smsg.Sequence); st != mqttQoS2Released {
				return mqttQoS2ClaimIndeterminate, 0, rerr
			}
		case cur.State == mqttQoS2RelStateDiscarded:
			return mqttQoS2ClaimDiscarded, smsg.Sequence, nil
		case cur.State == mqttQoS2RelStateReleasing && cur.Gen == gen:
			return mqttQoS2ClaimAlreadyClaimed, smsg.Sequence, nil
		case cur.State == mqttQoS2RelStateReleasing:
			// A foreign generation: the claim of an earlier exchange on this identifier
			// whose releaser never finished. Reaping it must be CONFIRMED — an
			// unconfirmed reap makes the retried create fail identically forever — and
			// must target the sequence just observed (scope rule 2).
			s.Warnf("MQTT: reaping the QoS 2 release claim of generation %q on %q, superseded by generation %q",
				cur.Gen, claimSubject, gen)
			if st, rerr := as.releaseQoS2Record(smsg.Sequence); st != mqttQoS2Released {
				return mqttQoS2ClaimIndeterminate, 0, rerr
			}
		default:
			// Unreachable: mqttQoS2RelClaimIdentityOK admits no other state, so an
			// unknown one is already unreadable. Non-mutating by scope rule 4.
			return mqttQoS2ClaimIndeterminate, 0,
				fmt.Errorf("QoS 2 release claim on %q carries an unhandled state %q", claimSubject, cur.State)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("QoS 2 release claim not resolved after %d attempts", mqttQoS2ClaimAttempts)
	}
	return mqttQoS2ClaimIndeterminate, 0, lastErr
}

// mqttQoS2VerifyStatus is the outcome of fence 2, the authoritative re-read of the
// hold record after the claim and before the onward delivery.
type mqttQoS2VerifyStatus int

const (
	mqttQoS2VerifyHeld          mqttQoS2VerifyStatus = iota // our record, at our sequence: deliver
	mqttQoS2VerifyDelivered                                 // our generation's tombstone: the exchange already completed
	mqttQoS2VerifyGone                                      // absent: the exchange terminated and was reaped
	mqttQoS2VerifySuperseded                                // a foreign generation, or our generation at another sequence
	mqttQoS2VerifyIndeterminate                             // backend failure; the returned error is non-nil
)

func (st mqttQoS2VerifyStatus) String() string {
	switch st {
	case mqttQoS2VerifyHeld:
		return "held"
	case mqttQoS2VerifyDelivered:
		return "delivered"
	case mqttQoS2VerifyGone:
		return "gone"
	case mqttQoS2VerifySuperseded:
		return "superseded"
	default:
		return "indeterminate"
	}
}

// verifyQoS2HoldForRelease is fence 2: it re-reads the hold record and proves the
// exchange the caller loaded is still the one on the subject, at the same
// sequence, before the caller does anything irreversible. Called only after the
// claim has been won or adopted.
//
// EVERY accepting outcome is generation-qualified, and `delivered` is not exempt.
// A releaser that skipped the check there would take a LATER exchange's tombstone
// on the same packet identifier as proof of its own completion and answer 0x00 for
// a message it never delivered. A corrupt record classifies with an empty
// generation, so it lands in Superseded and is left ALONE rather than disposed of:
// the sequence it now occupies may belong to that newer exchange, which is the
// same reason finalizeQoS2Move mutates nothing on a foreign generation.
//
// This does NOT apply to the tombstone branch on the PUBREL load path, which
// answers 0x00 from whatever tombstone it finds and must keep doing so: that
// PUBREL carries no earlier generation to compare against, so the identifier's
// state is whatever the record says. The check belongs to the re-verification
// alone, where a specific generation was loaded and is being carried forward.
//
// Holding the claim fences a discard that has not yet committed to this subject's
// purge. It does not fence one that has, nor the orphan sweep. The exceptions are
// enumerated once, in F-062 (docs/findings/ha-arbitration.md); do not re-derive them
// here.
func (as *mqttAccountSessionManager) verifyQoS2HoldForRelease(holdSubject, gen string, holdSeq uint64) (mqttQoS2VerifyStatus, uint64, error) {
	rec, curSeq, err := as.loadQoS2Record(holdSubject)
	if err != nil {
		return mqttQoS2VerifyIndeterminate, 0, err
	}
	if rec == nil {
		return mqttQoS2VerifyGone, 0, nil
	}
	if rec.gen != gen {
		return mqttQoS2VerifySuperseded, 0, nil
	}
	switch rec.state {
	case mqttQoS2Held:
		if curSeq != holdSeq {
			// Our generation is not rewritten by anything in this protocol, so a
			// different sequence for it is not a state we can prove ours: mutate nothing.
			return mqttQoS2VerifySuperseded, 0, nil
		}
		return mqttQoS2VerifyHeld, curSeq, nil
	case mqttQoS2Done:
		return mqttQoS2VerifyDelivered, curSeq, nil
	default:
		// Unreachable: a corrupt record has no generation, so the check above already
		// returned Superseded. Non-mutating by scope rule 4.
		return mqttQoS2VerifyIndeterminate, 0,
			fmt.Errorf("QoS 2 hold record on %q carries an unhandled state %q", holdSubject, rec.state)
	}
}

// reapQoS2RelClaim removes a claim at its exact sequence, asynchronously (scope
// rule 2 governs the sequence; the timing is safe for the same reason invariant 4
// of qos2_move.go allows the tombstone's reap to be asynchronous — a surviving
// `releasing` claim is inert, because the PUBREL branch order reaches the terminal
// tombstone before the claim and answers 0x00 from it).
//
// Callers must have won or adopted the claim: this is never used on a `discarded`
// claim (scope rule 3) nor on a foreign generation's, which claimQoS2Release reaps
// with a confirmed delete instead.
func (as *mqttAccountSessionManager) reapQoS2RelClaim(claimSeq uint64) {
	if claimSeq == 0 {
		return
	}
	_ = as.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, claimSeq, false)
}

// arbitrateQoS2ReleaseClaims is the discard half. For every inbound QoS 2 hold of
// one retiring session incarnation it competes for the release claim, and destroys
// the hold only where it won.
//
// It reads NO payloads: the subject set comes from STREAM.INFO's subjects_filter
// and each decision is made from the tiny claim record, so an ordinary disconnect
// with several exchanges in flight does not drag megabytes across the JS API.
//
// Called from clear() via deleteTeardownTargets, after the teardown claim is won
// and BEFORE the session record is deleted: reporting a session as cleaned up
// while a releaser can still deliver from it is the terminal-cleanup-before-
// completion mistake. An error therefore fails the clear.
//
// This is the ONLY thing a discard does to $MQTT_qos2in. There is no filter purge
// beside it, and there must not be: one would destroy exactly what the arbitration
// preserves.
func (as *mqttAccountSessionManager) arbitrateQoS2ReleaseClaims(s *Server, idHash, gen string) error {
	// Unreachable: a session without an incarnation generation cannot exist, since
	// mqttSessGenFromRecord rejects a record that carries none. Kept as a guard, and
	// deliberately NOT an error — a missing generation leaves records behind for the
	// orphan sweep, which merely leaks, whereas failing the clear would strand the
	// teardown marker and lock the client ID out of its next CONNECT. Nothing here
	// can lose a message, because the blind purge this replaced is gone.
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return nil
	}
	subjects, upTo, err := as.enumerateQoS2HoldsBounded(mqttQoS2HoldWalkFilter(idHash, gen))
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return err
		}
		return nil
	}
	holds := make([]string, 0, len(subjects))
	for subject := range subjects {
		// The walk filter cannot match a claim subject; the check is here so that
		// widening the filter later cannot silently turn claims into holds.
		if mqttQoS2IsClaimSubject(subject) {
			continue
		}
		holds = append(holds, subject)
	}
	// Deterministic order, so a teardown's log lines and a test's expectations do not
	// depend on map iteration.
	sort.Strings(holds)

	// One wait budget for the whole walk (see mqttQoS2DiscardWaitBudget).
	deadline := time.Now().Add(mqttQoS2DiscardWaitBudget)
	for _, holdSubject := range holds {
		if err := as.discardQoS2Hold(s, holdSubject, upTo, deadline); err != nil {
			return err
		}
	}
	return nil
}

// mqttQoS2UsableCutoff reports whether upTo can bound a hold purge. Sole definition, shared
// by the walk's pre-write validation, the `discarded` identity rule and purgeQoS2Hold's own
// refusal, so the value a marker may record and the value a purge will accept cannot drift —
// a marker carrying a fence the purge would refuse is exactly the shape that strands a
// teardown, so the two predicates are one.
func mqttQoS2UsableCutoff(upTo uint64) bool {
	return upTo != 0 && upTo != math.MaxUint64
}

// enumerateQoS2Holds lists one incarnation's inbound QoS 2 subjects with the stream's
// last sequence, honoring an instance-scoped test seam.
func (as *mqttAccountSessionManager) enumerateQoS2Holds(filter string) (map[string]uint64, uint64, error) {
	as.mu.RLock()
	hook := as.testQoS2Subjects
	as.mu.RUnlock()
	if hook != nil {
		return hook(filter)
	}
	return as.jsa.streamSubjects(mqttQoS2IncomingMsgsStreamName, filter)
}

// enumerateQoS2HoldsBounded enumerates until the response can BOUND a purge: either it
// matched nothing, or its last sequence is usable as a cutoff.
//
// The retry is not defensive padding. STREAM.INFO builds State before it fills
// State.Subjects from a separate store read, so a walk that started against an empty
// stream can legitimately be handed a non-empty subject map with a zero last sequence.
// Acting on that sample is the one thing this walk must never do: a zero cutoff would
// become a `discarded` marker's fence, the identity rule then classifies that marker
// unreadable, and the unreadable disposition on the PUBREL path deletes the
// acknowledged hold and answers 0x92 — manufacturing exactly the loss F-062 closes.
// So the sample is re-taken, and NOTHING is written in between, which is what makes
// retrying safe.
//
// A later sample resolves it, because once a response carries a record sequence 1 has
// landed and the last sequence never rewinds — not under a delete, a replacement, a
// filtered purge, a compact at or below the tail, or expiry (and this stream refuses
// MaxAge outright). Ordinary record churn therefore cannot keep the watermark
// unusable; exhausting the budget needs a test seam, a backend-contract violation, or
// stream recreation, which the incarnation precondition excludes.
//
// Exhaustion is an ERROR, which fails the clear. It cannot be swallowed the way the
// generationless guard above swallows its case: that guard has no identity to enumerate
// with and nothing it could arbitrate, whereas this walk has already found real
// generation-scoped holds and merely cannot bound its entitlement to them. Returning nil
// would let deleteTeardownTargets complete and releaseTeardown delete the session record
// while a releaser can still deliver from it. The cost is stated where it lands: the
// marked, partly torn-down session refuses that client ID's next CONNECT until an
// adoption obtains a usable watermark (docs/CONFORMANCE.md).
func (as *mqttAccountSessionManager) enumerateQoS2HoldsBounded(filter string) (map[string]uint64, uint64, error) {
	var lastSeen uint64
	for attempt := 0; attempt < mqttQoS2ClaimAttempts; attempt++ {
		subjects, lastSeq, err := as.enumerateQoS2Holds(filter)
		if err != nil {
			return nil, 0, err
		}
		if len(subjects) == 0 || mqttQoS2UsableCutoff(lastSeq) {
			return subjects, lastSeq, nil
		}
		lastSeen = lastSeq
	}
	return nil, 0, fmt.Errorf(
		"inbound QoS 2 holds match %q but the stream reported an unusable last sequence %d after %d attempts",
		filter, lastSeen, mqttQoS2ClaimAttempts)
}

// discardQoS2Hold competes for one hold's release claim and, where it wins,
// destroys the hold.
//
// The claim STRICTLY PRECEDES the purge, and that ordering is load-bearing twice:
// it is what makes the discard a competitor at all, and it is what lets a releaser
// holding the claim read the hold record's absence as "terminated" rather than
// "discarded" (verifyQoS2HoldForRelease).
//
// upTo is the highest sequence THIS caller can prove it is entitled to destroy: the
// walk's enumeration watermark for a session teardown, the exact re-confirmed record
// sequence for the orphan sweep. It bounds the purge where this call's own MARKER licenses
// it, and is deliberately NOT what bounds the arm that adopts a pre-existing marker, which
// uses that marker's fence instead (see below). An awaited release licenses no purge of its
// own at all: it returns to this competition rather than destroying anything.
func (as *mqttAccountSessionManager) discardQoS2Hold(s *Server, holdSubject string, upTo uint64, deadline time.Time) error {
	claimSubject := mqttQoS2ClaimSubjectFor(holdSubject)
	claim := mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, as.jsa.id, upTo)
	var lastErr error
	for attempt := 0; attempt < mqttQoS2ClaimAttempts; attempt++ {
		if _, err := as.storeQoS2RelClaimCAS(claimSubject, 0, claim); err == nil {
			return as.purgeQoS2Hold(holdSubject, upTo)
		} else {
			lastErr = err
		}
		cur, smsg, lerr := as.loadQoS2RelClaim(claimSubject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		switch {
		case smsg == nil:
			continue // raced away; retry the create
		case cur == nil:
			// An unreadable record cannot be proof that a releaser won, and destruction
			// is this party's job, so reap it — confirmed, at the sequence just observed —
			// and compete again. Metadata only in the log line: a claim body is small,
			// but the rule that these two log lines never carry record content is
			// absolute.
			s.Warnf("MQTT: reaping an unusable QoS 2 release claim during session discard (%s)",
				mqttQoS2CorruptDetail(claimSubject, smsg, "malformed claim record, or no usable claim identity"))
			if st, rerr := as.releaseQoS2Record(smsg.Sequence); st != mqttQoS2Released {
				return fmt.Errorf("unable to reap the unusable QoS 2 release claim on %q: %w", claimSubject, rerr)
			}
		case cur.State == mqttQoS2RelStateDiscarded:
			// Ours from an earlier attempt, or a peer discard's. Either way no releaser of
			// the exchange this marker was written for can win, so that hold is ours to
			// destroy — but only up to what THAT marker's writer was entitled to destroy,
			// never up to our own watermark. Adopting a marker means finishing an
			// already-durable discard decision, and its fence is the entitlement that
			// decision recorded: its writer enumerated at Fence, so every occupancy at or
			// below Fence predated its decision point, and it won the create-only CAS that
			// licensed acting on it.
			//
			// NOT min(cur.Fence, upTo). Our own watermark is not an upper bound on a
			// marker we merely adopted: the walk enumerates every subject up front but does
			// not load a sibling claim until it reaches it, so a CONCURRENT teardown can
			// enumerate higher than we did and win this marker in between. Clamping would
			// then spare a hold that marker's own decision covered while we report the walk
			// complete. It is also no defense against a corrupt-huge fence, where it
			// degrades to exactly the unbounded-by-our-watermark behavior it was meant to
			// prevent.
			return as.purgeQoS2Hold(holdSubject, cur.Fence)
		case cur.State == mqttQoS2RelStateReleasing:
			// Wait it out, then COMPETE AGAIN rather than treating the subject's emptiness as
			// our entitlement — the claim we waited on may have belonged to an earlier
			// exchange and been reaped by the CURRENT one's releaser, which leaves the subject
			// briefly empty without anything having terminated (see awaitQoS2Release).
			again, aerr := as.awaitQoS2Release(s, holdSubject, claimSubject, deadline)
			if aerr != nil || !again {
				return aerr
			}
			continue
		default:
			// Unreachable, as in claimQoS2Release. Fails the clear rather than deleting
			// anything (scope rule 4).
			return fmt.Errorf("QoS 2 release claim on %q carries an unhandled state %q", claimSubject, cur.State)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("QoS 2 release claim on %q not resolved after %d attempts", claimSubject, mqttQoS2ClaimAttempts)
	}
	return lastErr
}

// awaitQoS2Release waits, within the walk's remaining budget, for the claim subject to
// become free, and reports whether the caller should COMPETE FOR IT AGAIN.
//
// It destroys nothing, and that is the whole point. An earlier version read an empty
// claim subject as proof that the exchange this walk enumerated had terminated, on the
// grounds that "a releaser deletes its claim only after the terminal commit". **That
// premise is false, and this file contains its own counterexample**: a releaser that
// meets a FOREIGN generation's `releasing` claim reaps it at its exact sequence and only
// then retries its own create (claimQoS2ReleaseInner). The subject is empty in between.
//
// So the window is real and the loss it produced was the F-062 class again: E1 completes
// and leaves its claim to be reaped; E2 lands on the vacated hold subject and is answered
// PUBREC; the teardown enumerates with E2 at or below its watermark, meets E1's stale
// claim, and waits. E2's PUBREL then reaps that stale claim, and a poll landing in the gap
// saw absence, concluded "terminated", and purged E2 — an acknowledged message destroyed by
// a walk that had won no arbitration over it at all. The cutoff cannot help, because E2
// legitimately predates the enumeration.
//
// The fix is to stop inferring entitlement from absence. An empty subject licenses only one
// thing: entering the create-only competition again. If this walk wins, it holds a fresh
// marker carrying its own fence and destroys under arm A1's entitlement like any other
// winner; if a releaser wins, we are back to waiting, which is correct.
//
// It polls the CLAIM, never the hold record — a claim is tiny, while polling the hold would
// pull up to max_payload bytes per attempt on a disconnect path — and the hold is never
// deleted while a claim stands, because its payload is what a retry delivers if the
// releasing broker died between claim and store. When the budget runs out the records are
// left for the orphan sweep; the releasing broker is then presumed dead, and no bounded wait
// can prove otherwise.
func (as *mqttAccountSessionManager) awaitQoS2Release(s *Server, holdSubject, claimSubject string, deadline time.Time) (bool, error) {
	for {
		cur, smsg, err := as.loadQoS2RelClaim(claimSubject)
		if err == nil && cur == nil && smsg == nil {
			return true, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if remaining > mqttQoS2DiscardWaitInterval {
			remaining = mqttQoS2DiscardWaitInterval
		}
		time.Sleep(remaining)
	}
	s.Warnf("MQTT: leaving inbound QoS 2 hold %q to the orphan sweep: a release claimed it and did not complete within %v",
		holdSubject, mqttQoS2DiscardWaitBudget)
	return false, nil
}

// purgeQoS2Hold removes the records occupying one hold subject — a held PUBLISH, a
// terminal tombstone or a corrupt record — up to and INCLUDING sequence upTo, with a
// CONFIRMED purge of that exact subject.
//
// upTo is the highest sequence the caller can PROVE it is entitled to destroy, and
// there is deliberately no unbounded form of this function: generation scoping alone
// was never a bound, because a resume preserves the generation, so a peer still
// serving the retiring incarnation can accept and PUBREC-acknowledge a new exchange on
// this very subject after the caller stopped looking. Whatever lands above upTo is
// SPARED and left to a later walk or the orphan sweep (F-062, mechanism 1).
//
// Confirmed even where the caller asked for a no-wait teardown, for the reason
// releaseTeardown documents: an unclassifiable outcome is not an outcome, and this
// one licenses reporting the session's inbound QoS 2 state as destroyed. It reads no
// payload.
//
// Two values of upTo are REFUSED rather than sent, for two different reasons:
//
//   - 0 would emit Sequence 1, which both stores treat as an explicit no-op. The
//     hazard is not an over-broad purge but FALSE SUCCESS — a nil return here licenses
//     reporting the session's inbound QoS 2 state as destroyed when nothing was
//     removed, which is the terminal-cleanup-before-completion mistake.
//   - MaxUint64 would wrap upTo+1 to zero and emit the genuinely unbounded request.
//     This guard is what makes "no route can regress to an unbounded purge" true rather
//     than nearly true, and it is the only thing standing between a corrupt maximal
//     fence and that shape.
func (as *mqttAccountSessionManager) purgeQoS2Hold(holdSubject string, upTo uint64) error {
	if !mqttQoS2UsableCutoff(upTo) {
		return fmt.Errorf("refusing to purge the inbound QoS 2 hold on %q: unusable cutoff %d", holdSubject, upTo)
	}
	err := as.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, upTo+1, false)
	if isErrorOtherThan(err, JSStreamNotFoundErr) {
		return fmt.Errorf("unable to purge the inbound QoS 2 hold on %q up to %d: %w", holdSubject, upTo, err)
	}
	return nil
}
