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
// create can ever succeed. The loser does nothing irreversible.
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
//     The discard's HOLD purge (discardQoS2Hold) is different, and it only looks
//     inconsistent: it names an exact subject of a RETIRED incarnation, and
//     clear() re-mints the generation before the walk runs, so no later exchange
//     can ever occupy that subject and a late purge is harmless by naming. A claim
//     is reaped by releasers on a LIVE incarnation, where the same subject genuinely
//     is reused by a later exchange. Naming suffices for one; only sequencing
//     suffices for the other.
//  3. A DISCARD NEVER REAPS ITS OWN `discarded` CLAIM. It is the only durable
//     proof that a late releaser must lose, so reaping it re-opens exactly the
//     window this file closes — the same rule as discardQ2AffClaimsFor, where
//     claims are marked and never released. Nothing can license reaping it at
//     teardown time, because "no releaser is still in flight" is not provable. It
//     is left to the orphan sweep filed in docs/ROADMAP.md, which ages a claim by
//     mqttQoS2RelClaimAt.
//  4. NON-MUTATING DEFAULTS. Every switch over a claim state or a verdict resolves
//     an unhandled value to Indeterminate — suppress PUBCOMP, the client retries —
//     never to a verdict that answers the client or deletes anything. An omitted
//     case then costs a retry instead of a message.
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

	// mqttQoS2ClaimAtFloor is the oldest timestamp a claim's At may carry
	// (2020-01-01T00:00:00Z). A value below it is not a plausible claim time, so the
	// age falls back to the record's own store time. There is deliberately no
	// CEILING: an implausibly FUTURE value only delays the reap of a small record,
	// while a ceiling would have to guess how far a broker's clock may legitimately
	// lead the stream's.
	mqttQoS2ClaimAtFloor = 1577836800

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
// State and Gen are its identity and are validated strictly
// (mqttQoS2RelClaimIdentityOK). Origin and At are forensic: neither may ever
// decide whether a claim is readable, because the unreadable disposition drops a
// message and a malformed diagnostic field must not be able to cause that.
type mqttQoS2RelClaim struct {
	State  string `json:"state"`
	Gen    string `json:"gen,omitempty"` // the exchange generation: required for releasing, absent for discarded
	Origin string `json:"origin"`
	At     int64  `json:"at"`
}

// mqttQoS2RelClaimIdentityOK reports whether a decoded claim carries a usable
// identity.
//
// A `releasing` claim REQUIRES a well-formed exchange generation: it is what tells
// a later PUBREL whether the claim is its own (proceed) or a dead releaser's
// leftover (reap and retry), and a claim without one could be read as either.
// A `discarded` claim requires the generation to be ABSENT, because the discard
// never reads the hold record — that is what keeps payloads off the discard path —
// and a discard needs no generation: it can only ever concern the one exchange
// live on that generation-scoped subject, and it retires the incarnation, so no
// later exchange can appear there to be confused by it.
func mqttQoS2RelClaimIdentityOK(c *mqttQoS2RelClaim) bool {
	if c == nil {
		return false
	}
	switch c.State {
	case mqttQoS2RelStateReleasing:
		return mqttValidQoS2Gen(c.Gen)
	case mqttQoS2RelStateDiscarded:
		return c.Gen == _EMPTY_
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

// mqttQoS2RelClaimAt returns the age of a claim record: its own At when that is
// plausible, and otherwise the record's JetStream store time, which always exists,
// is assigned by one stream leader and cannot be forged. Without the fallback a
// malformed diagnostic field could make an accepted `discarded` claim permanently
// unreapable; with it, the sweep always has an answer.
func mqttQoS2RelClaimAt(c *mqttQoS2RelClaim, smsg *StoredMsg) time.Time {
	if c != nil && c.At >= mqttQoS2ClaimAtFloor {
		return time.Unix(c.At, 0)
	}
	return smsg.Time
}

// mqttNewQoS2RelClaim builds a claim. gen must be empty for a discard and a valid
// exchange generation for a release.
func mqttNewQoS2RelClaim(state, gen, origin string) *mqttQoS2RelClaim {
	return &mqttQoS2RelClaim{
		State:  state,
		Gen:    gen,
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
// is create-only (expectation 0). That deliberately over-budgets by at most 19
// bytes: over-budgeting only rejects a PUBLISH slightly earlier, while
// under-budgeting wedges an accepted exchange.
var mqttQoS2ClaimRecordMax = sync.OnceValue(func() int {
	msg, _ := buildQoS2RelClaimRecord(math.MaxUint64, &mqttQoS2RelClaim{
		// The two state literals are the same length, so either bounds both.
		State:  mqttQoS2RelStateReleasing,
		Gen:    strings.Repeat("z", mqttQoS2GenLen),
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
//   - `discarded`                   -> Discarded
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
	claim := mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, as.jsa.id)
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
// Once this returns Held nothing can remove the record under us: a discard must
// win the claim before it purges (discardQoS2Hold), and we hold the claim.
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
	subjects, err := as.jsa.streamSubjects(mqttQoS2IncomingMsgsStreamName, mqttQoS2HoldWalkFilter(idHash, gen))
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
		if err := as.discardQoS2Hold(s, holdSubject, deadline); err != nil {
			return err
		}
	}
	return nil
}

// discardQoS2Hold competes for one hold's release claim and, where it wins,
// destroys the hold.
//
// The claim STRICTLY PRECEDES the purge, and that ordering is load-bearing twice:
// it is what makes the discard a competitor at all, and it is what lets a releaser
// holding the claim read the hold record's absence as "terminated" rather than
// "discarded" (verifyQoS2HoldForRelease).
func (as *mqttAccountSessionManager) discardQoS2Hold(s *Server, holdSubject string, deadline time.Time) error {
	claimSubject := mqttQoS2ClaimSubjectFor(holdSubject)
	claim := mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, as.jsa.id)
	var lastErr error
	for attempt := 0; attempt < mqttQoS2ClaimAttempts; attempt++ {
		if _, err := as.storeQoS2RelClaimCAS(claimSubject, 0, claim); err == nil {
			return as.purgeQoS2Hold(holdSubject)
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
			// Ours from an earlier attempt, or a peer discard's. Either way no releaser
			// can ever win, so the hold is ours to destroy.
			return as.purgeQoS2Hold(holdSubject)
		case cur.State == mqttQoS2RelStateReleasing:
			return as.awaitQoS2Release(s, holdSubject, claimSubject, deadline)
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

// awaitQoS2Release waits, within the walk's remaining budget, for a release the
// discard observed in flight.
//
// It polls the CLAIM for absence, never the hold record: the releaser deletes its
// claim only after the terminal commit, so absence proves the exchange terminated
// — and a claim is a tiny record, while polling the hold would pull up to
// max_payload bytes per attempt on a disconnect path.
//
// The hold record is NOT deleted while a claim stands. Its payload is what a retry
// delivers if the releasing broker died between claim and store, so destroying it
// would turn this spec deviation into silent loss. When the budget runs out the
// records are left for the orphan sweep filed in docs/ROADMAP.md; the releasing
// broker is then presumed dead, and no bounded wait can prove otherwise.
func (as *mqttAccountSessionManager) awaitQoS2Release(s *Server, holdSubject, claimSubject string, deadline time.Time) error {
	for {
		cur, smsg, err := as.loadQoS2RelClaim(claimSubject)
		if err == nil && cur == nil && smsg == nil {
			return as.purgeQoS2Hold(holdSubject)
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
	return nil
}

// purgeQoS2Hold removes whatever record occupies one hold subject — a held
// PUBLISH, a terminal tombstone or a corrupt record — with a CONFIRMED purge of
// that exact subject.
//
// Confirmed even where the caller asked for a no-wait teardown, for the reason
// releaseTeardown documents: an unclassifiable outcome is not an outcome, and this
// one licenses reporting the session's inbound QoS 2 state as destroyed. It needs
// no sequence and reads no payload, and a late purge cannot reach a newer
// incarnation because the subject carries the retiring generation (scope rule 2).
func (as *mqttAccountSessionManager) purgeQoS2Hold(holdSubject string) error {
	err := as.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, false)
	if isErrorOtherThan(err, JSStreamNotFoundErr) {
		return fmt.Errorf("unable to purge the inbound QoS 2 hold on %q: %w", holdSubject, err)
	}
	return nil
}
