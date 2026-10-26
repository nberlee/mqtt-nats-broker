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
// pending record whose FireAt has passed is CAS-claimed pending->publishing by
// whichever replica's scan wins; the claimant then DECIDES the Will's fate and
// persists that decision with the CAS that licenses it. Leases matter only in the
// publishing state: a claimant that dies leaves a publishing record whose lease
// expires, letting a survivor recover — and a survivor that finds a persisted
// publish decision EXECUTES it rather than re-deciding, which is what stops one
// Will being published by one broker and suppressed by another.
//
// Durable termination: a successful publish is made durably terminal
// (publishing->done, retried against the record's latest sequence) BEFORE any
// cleanup. Only then is the record deleted (at the done sequence). Because done is
// committed first, a failed delete cannot leave a re-fireable publishing record —
// the scanner reaps the done record instead. See finalizeWillPublish.
//
// Takeover suppression is decided from the PERMANENT CONNECT ACCEPTANCE FACTS on
// $MQTT_sess (sess_accept_fact.go), not from a cancellation watermark and not from
// a reconnect-time cancel. Both of those are gone, and both for the same reason:
// neither could say WHEN the acceptance happened. MQTT 5.0 suppresses a delayed
// Will only for a reconnect that happens during the Will Delay ([MQTT-3.1.3-9]);
// once the delay has elapsed the Will is owed and must be published
// ([MQTT-3.1.2-8]). A claimant therefore scans this client's acceptance facts,
// bounded by the cutoff it sampled and persisted, and suppresses only for an
// acceptance that is this incarnation's, this exact client's, strictly newer than
// the connection that created the Will, and dated BEFORE the Will became due.
//
// Residual (why literal exactly-once remains impossible):
//   - at-least-once duplicate. The boundary is EXTERNAL PUBLICATION WITH NO DURABLE
//     EVIDENCE OF COMPLETION — not a crash, which is only one way to reach it. Two
//     triggers, both sanctioned and both with their own test:
//       1. Lease expiry with the publish externally done and done uncommitted.
//          Recovery cannot tell WHY the lease lapsed: a crash, a pause, a partition
//          and a live broker stalled in its own process are indistinguishable to it.
//          The last is reachable — the publish path waits on mqttSubsBarrier, bounded
//          by shutdown and not by the lease (see willPublishWorker). One such stall
//          yields TWO publications, not unboundedly many: the barrier is per account
//          manager, so the peer that reclaims is not stalled and commits done.
//       2. Enqueue ambiguity, with no lease expiry involved at all: retry exhaustion
//          leaves an UNCONFIRMED create, the fail-safe publishes immediately, and a
//          record that did land fires again later (publishWillFailSafe).
//     Republishing is the correct bias in both: the alternative is silence about a
//     Will that may be owed, or durably cancelling one that may already have gone
//     out. Neither is the terminal regression, where done COMMITTED and was then
//     overwritten — evidence the system HELD and discarded, rather than never had.
//
// This is a first-release format: pre-Layer-2 in-record Wills, records without an
// exact client identity or a well-formed decision shape, and a session stream
// without a valid generated epoch are unsupported and rejected rather than carried
// forward. Every decoded record is validated (validateWillOutboxRecord) and one
// that is not a supported record is REFUSED: never claimed, published, cancelled or
// deleted, and reported once with the operator remedy. Refusal is read-only, so
// nothing reclaims a refused record — an operator does.

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
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
const (
	mqttWillDefaultClaimLease    = 30 * time.Second
	mqttWillDefaultClaimInterval = 5 * time.Second
)

var (
	mqttWillClaimLease    = newMQTTKnob(mqttWillDefaultClaimLease)
	mqttWillClaimInterval = newMQTTKnob(mqttWillDefaultClaimInterval)
)

// How many times a claimant re-samples the acceptance cutoff before deciding. A
// fact that commits while the walk runs is above the cutoff the walk used, so it
// is invisible to it — and the persisted decision means no later claimant ever
// re-reads it. Chasing the cutoff examines those late arrivals, and is sound in
// only one direction: `At < FireAt` is what DATES eligibility, so a later cutoff
// can never suppress a Will an earlier one would have published — it can only
// find evidence this pass would otherwise have missed.
//
// The limit is what keeps the chase from becoming a wedge. An empty delta is NOT
// required to decide: a client reconnecting steadily after its Will fell due (or
// a hash-colliding one) produces facts forever, none of which can qualify, so a
// chase that insisted on an empty delta would defer an OWED Will on every pass
// and never fire it. Exhausting the limit therefore LICENSES under the cutoff
// actually verified rather than deferring; deferral stays reserved for the four
// conditions that make a decision unsafe rather than merely less informed —
// shutdown, an exhausted budget, an unreadable fact, and a lost renewal.
const mqttWillCutoffChases = 4

// Bounded retry count for the enqueue JSA operation, which sits on the disconnect
// path: a transient JetStream error must not silently lose a delayed Will.
const (
	mqttWillEnqueueAttempts = 3
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
	WillID       string `json:"wid"`
	ClientIDHash string `json:"cid"`
	// ClientID is the EXACT client identifier, of which ClientIDHash is only the
	// routing token. Suppression is decided by comparing this against an acceptance
	// fact's client id, because two distinct clients can share a hash and one
	// client's reconnect must never destroy another's delayed Will. Both durable
	// enqueue routes have the exact identity in hand; a record without one is an
	// unsupported shape and is refused.
	ClientID   string        `json:"cidx"`
	Topic      string        `json:"topic"`
	Msg        []byte        `json:"msg,omitempty"`
	QoS        byte          `json:"qos,omitempty"`
	Retain     bool          `json:"retain,omitempty"`
	Props      []byte        `json:"props,omitempty"` // encoded forwardable Will properties
	User       string        `json:"user,omitempty"`
	FireAt     int64         `json:"fire"` // unix nanoseconds to publish at
	State      mqttWillState `json:"state"`
	ClaimOwner string        `json:"owner,omitempty"` // node name of the current publisher
	ClaimUntil int64         `json:"until,omitempty"` // unix nanoseconds publish lease expiry
	// Account is the NATS SERVICE ACCOUNT this Will's authorization was decided
	// against, and it is load-bearing rather than diagnostic. A delayed Will is
	// published long after its connection is gone, by an internal client that has
	// no identity of its own, so the record IS the decision -- and a decision taken
	// for one account cannot license a publication into another. deliverWillRecord
	// refuses a record whose account is not the one the service connection is
	// currently bound to, and leaves it retryable rather than dropping it, because
	// a broker pointed back at the original account must still fire it.
	Account string `json:"acct,omitempty"`
	// OriginSessionSeq and OriginSessEpoch identify the accepted generation that
	// created this delayed Will: the persisted session-record stream sequence and
	// the $MQTT_sess incarnation epoch captured at the disconnect (mqttHandleWill, will_disposition.go).
	// A sequence is comparable only within one incarnation, so the fence compares
	// OriginSessionSeq against a watermark/session sequence only when the epochs are
	// equal. Every record carries a nonzero sequence and a valid epoch; a zero
	// sequence or empty/malformed epoch is unsupported and refused at decode (there
	// is no legacy/migrated compatibility path in the first release). See
	// validateWillOutboxBody.
	OriginSessionSeq uint64 `json:"oseq"`
	OriginSessEpoch  string `json:"oep"`
	// Gen is the enqueuing session INCARNATION's generation (mqttSession.gen; see
	// sess_incarnation.go). Every record carries a valid one; a generationless
	// record is unsupported and refused at decode, not treated as a distinct
	// unscoped bucket — "no identity" must not collapse into the identity space
	// (sess_incarnation.go:51-56), and an unscoped bucket cannot be arbitrated.
	//
	// It existed so the DISPLACED-broker cancel could scope itself to the Wills its
	// own evicted connection enqueued. That cancel is DELETED — a reconnect-time
	// cancel cannot say when the acceptance happened, which is the whole point of the
	// dated facts — so nothing reads this field today. It is still written and still
	// validated, and it is called out in docs/CONFORMANCE.md as a field whose reader
	// is gone rather than quietly left to look load-bearing.
	Gen string `json:"gen"`
	// Decision is the act this record has been LICENSED to perform, and it is
	// immutable once set. It exists because a fresh decision per claimant is
	// destructive without needing any leader transition at all: a claimant that
	// scanned under one cutoff, licensed a publish, published, and crashed before
	// the terminal commit would otherwise be recovered by a peer that samples a
	// LATER cutoff, sees an acceptance that landed in between, and cancels — the
	// same Will generation reinterpreted from published to suppressed after the
	// irreversible act. A recovering claimant reuses this decision and does not
	// scan at all.
	//
	// It also distinguishes an UNLICENSED publishing claim (a claimant is
	// scanning) from a LICENSED one (a claimant may publish), which the state
	// alone cannot say.
	Decision mqttWillDecision `json:"dec,omitempty"`
	// ScanFrom is the highest $MQTT_sess sequence a claimant has EXAMINED and rejected
	// for this Will: "no acceptance fact at or below this sequence qualifies". It is
	// monotone progress, not a decision, and it is permanent — the record's own
	// predicate inputs (client, epochs, origin sequence, FireAt) are immutable, and a
	// fact is never rewritten or removed, so a rejection stays a rejection.
	//
	// Without it a bounded scan cannot finish an unbounded history: every claimant
	// restarted at the same sequence, so a non-qualifying prefix long enough to exhaust
	// the scan budget wedged the Will permanently — neither published nor cancelled,
	// with every lease-expiry recovery re-reading the same prefix. That is a liveness
	// defect, not the accepted disk-growth residual.
	//
	// Advanced by the RENEWAL CAS, which already writes during a scan, so progress
	// costs nothing extra; carried forward untouched by every reclaim.
	//
	// It is a $MQTT_sess SEQUENCE, so it is meaningless outside the incarnation that
	// assigned it — which is why ScanEpoch travels with it and a floor from another
	// incarnation is IGNORED. Without that scoping a surviving floor is a live defect:
	// the walk skips the very facts a cross-incarnation Will qualifies on, and worse
	// than skipping, a floor above the new cutoff makes the loop run zero times and
	// report "nothing qualifies", which LICENSES a publication having read nothing at
	// all.
	//
	// That state USED TO be reachable through a supported operation, when $MQTT_willq
	// sat outside the coupled recreation unit and survived a $MQTT_sess recreation.
	// F-066 made all ten streams one recreation unit, so no supported operation
	// produces it any more; the scoping stays as defence in depth against
	// administrative deletion or a partial restore.
	ScanFrom uint64 `json:"sfrom,omitempty"`
	// ScanEpoch is the $MQTT_sess incarnation ScanFrom was recorded under. Set with
	// it and cleared with it; a floor whose epoch is not this broker's is discarded
	// and the walk restarts, which costs one re-read and is the conservative
	// direction.
	ScanEpoch string `json:"sepoch,omitempty"`
	// DecisionCutoff is the exact $MQTT_sess last sequence the decision was taken
	// under. It is a POINTER because presence is the thing being recorded and zero
	// is a legal value: a fresh or recreated session stream has LastSeq 0, so a
	// numeric-zero sentinel would collapse "no cutoff" into a valid cutoff.
	//
	// It carries NO incarnation companion, unlike ScanFrom, and deliberately so: it is
	// only ever compared for EQUALITY against the cutoff on the record this claimant
	// licensed (sameDecision), never against a newly derived sequence and never as an
	// ordering bound. Both sides of every such comparison were written by one licensing
	// CAS on one record, so no incarnation boundary can fall between them, and equality
	// does not depend on what the number counts. That is what separates it from
	// ScanFrom, where a stored sequence WAS compared against sequences derived later —
	// the defect this design has now been bitten by three times.
	DecisionCutoff *uint64 `json:"cut,omitempty"`
}

// mqttWillDecision is the act a licensing CAS grants a claimed record. Unset means
// no act is licensed; the two set values are terminal intentions, so a record
// carrying one may only ever perform that one.
type mqttWillDecision string

const (
	mqttWillDecisionUnset   mqttWillDecision = ""
	mqttWillDecisionPublish mqttWillDecision = "publish"
	mqttWillDecisionCancel  mqttWillDecision = "cancel"
)

// validateWillDecisionShape enforces the legal (state, decision, cutoff) matrix.
// Everything outside it is refused rather than interpreted, because each illegal
// combination is a claim about the protocol that the protocol does not make:
//
//	pending     decision unset, cutoff absent   (nothing licensed yet)
//	publishing  unset+absent, or publish+cutoff (scanning, or licensed to publish)
//	cancelled   cancel + cutoff                 (a suppression was licensed)
//	done        publish + cutoff                (a publication was licensed)
//
// publishing/cancel in particular must never be legal: a cancel is licensed
// straight to cancelled by the same CAS that records it, so a publishing record
// carrying cancel could only be a decision the entry branch would then try to
// EXECUTE — and a cancel executed from publishing is a state with two meanings.
func validateWillDecisionShape(state mqttWillState, dec mqttWillDecision, cut *uint64) error {
	switch dec {
	case mqttWillDecisionUnset, mqttWillDecisionPublish, mqttWillDecisionCancel:
	default:
		return fmt.Errorf("decision %q is not a known outbox decision", dec)
	}
	if (dec == mqttWillDecisionUnset) != (cut == nil) {
		return fmt.Errorf("decision %q and decision-cutoff presence (%v) disagree", dec, cut != nil)
	}
	switch state {
	case mqttWillPending:
		if dec != mqttWillDecisionUnset {
			return fmt.Errorf("a pending record carries decision %q", dec)
		}
	case mqttWillPublishing:
		if dec == mqttWillDecisionCancel {
			return fmt.Errorf("a publishing record carries a cancel decision, which is never executable")
		}
	case mqttWillCancelled:
		if dec != mqttWillDecisionCancel {
			return fmt.Errorf("a cancelled record carries decision %q (want cancel)", dec)
		}
	case mqttWillDone:
		if dec != mqttWillDecisionPublish {
			return fmt.Errorf("a done record carries decision %q (want publish)", dec)
		}
	}
	return nil
}

// usableScanFloor returns the persisted scan floor only when it was recorded under
// the incarnation the reader is running in. A floor from another incarnation names a
// sequence in a store that no longer exists, so it is discarded rather than compared.
func (r *mqttWillOutbox) usableScanFloor(currentEpoch string) uint64 {
	if r.ScanEpoch == _EMPTY_ || r.ScanEpoch != currentEpoch {
		return 0
	}
	return r.ScanFrom
}

// licensedToPublish reports whether this record carries the exact shape the
// claimant entry branch may EXECUTE without scanning. Phrased on publish
// specifically: a cancel decision is terminal, so no record a claimant could
// execute ever carries one.
func (r *mqttWillOutbox) licensedToPublish() bool {
	return r.State == mqttWillPublishing && r.Decision == mqttWillDecisionPublish && r.DecisionCutoff != nil
}

// sameDecision reports whether other carries the identical licensed decision — both
// the act and the cutoff it was taken under. The cutoff is part of the identity
// because two claimants can license the same act under different evidence, and only
// the persisted one may be finalized.
func (r *mqttWillOutbox) sameDecision(other *mqttWillOutbox) bool {
	if r.Decision != other.Decision {
		return false
	}
	if r.DecisionCutoff == nil || other.DecisionCutoff == nil {
		return r.DecisionCutoff == nil && other.DecisionCutoff == nil
	}
	return *r.DecisionCutoff == *other.DecisionCutoff
}

// leaseValid reports whether the publishing claim lease is still valid at now.
func (r *mqttWillOutbox) leaseValid(now time.Time) bool {
	return r.ClaimOwner != _EMPTY_ && r.ClaimUntil > now.UnixNano()
}

// mqttWillIdentTk reports whether s is a nonempty SINGLE subject token, the shape
// both identity fields must have because both are subject tokens: the subject is
// <prefix><domainTk><cid>.<wid>. A dotted (or wildcard) identity would still match
// its own subject by equality yet change the subject's token count, making the
// record visible to the account filter (<domainTk>>) but INVISIBLE to the
// reconnect-cancel filter (<domainTk><cid>.*, exactly one trailing token) — so it
// could fire after a reconnect that was entitled to suppress it. Both real mints
// are single tokens by construction (nuid.Next() and getHash are base-62), so this
// refuses only foreign shapes.
func mqttWillIdentTk(s string) bool {
	if s == _EMPTY_ {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case btsep, pwc, fwc, ' ':
			return false
		}
	}
	return true
}

// validateWillOutboxBody checks the format invariants a delayed-Will outbox
// record carries on its own, independent of where it is stored. This is the
// pre-release format contract: every field a decision is taken on must be
// present and well-formed, because there is no legacy bucket to fall back to and
// an unscoped record cannot be arbitrated (see the Compatibility boundary in
// docs/ARCHITECTURE.md and sess_incarnation.go:22-32).
//
// It applies at the persist and decode boundaries only. An outbox record used
// purely as a publication vehicle — mqttPublishOwedWill's, and the deferred-close
// immediate publication's — is built in memory, handed straight to
// errMQTTWillDestinationUnavailable is deliverWillRecord's account-fence refusal:
// the service connection is down, or is bound to an account other than the one this
// Will's authorization was decided against. It is a statement about the DESTINATION,
// never about the Will, which is what makes it the one refusal worth re-attempting
// when the destination comes back.
var errMQTTWillDestinationUnavailable = errors.New("the Will's authorized destination account is not currently established")

// deliverWillRecord (which reads message fields alone) and never persisted, so it
// carries no identity and is deliberately not validated.
//
// Not checked, deliberately: FireAt, which is dueness rather than format (a zero
// FireAt is simply a due Will), and any length/alphabet shape for the identity
// fields beyond token topology, since they are matched by equality and never
// parsed.
func validateWillOutboxBody(rec *mqttWillOutbox) error {
	if !mqttWillIdentTk(rec.WillID) {
		return fmt.Errorf("will id %q is not a single subject token", rec.WillID)
	}
	if !mqttWillIdentTk(rec.ClientIDHash) {
		return fmt.Errorf("client id hash %q is not a single subject token", rec.ClientIDHash)
	}
	// The exact client identity, and its binding to the routing token. Suppression
	// compares this against an acceptance fact's client id, so a record whose two
	// identities disagree could be suppressed by — or survive — the wrong client's
	// reconnect.
	if rec.ClientID == _EMPTY_ {
		return fmt.Errorf("record carries no exact client id")
	}
	if h := getHash(rec.ClientID); h != rec.ClientIDHash {
		return fmt.Errorf("exact client id hashes to %q, but the record's hash token is %q", h, rec.ClientIDHash)
	}
	if !mqttValidSessGen(rec.Gen) {
		return fmt.Errorf("session incarnation generation %q is not well-formed", rec.Gen)
	}
	if rec.OriginSessionSeq == 0 {
		return fmt.Errorf("origin session sequence is zero")
	}
	// Only a claimant renewal advances the scan floor, and a claimant is publishing by
	// then. A pending record carrying one would mean progress was recorded by
	// something that never held the record, which no writer does.
	if rec.State == mqttWillPending && (rec.ScanFrom != 0 || rec.ScanEpoch != _EMPTY_) {
		return fmt.Errorf("a pending record carries a scan floor %d under epoch %q", rec.ScanFrom, rec.ScanEpoch)
	}
	// The floor and the incarnation it was recorded under are one fact. A bare
	// sequence would be uncomparable, and a bare epoch says nothing.
	if (rec.ScanFrom == 0) != (rec.ScanEpoch == _EMPTY_) {
		return fmt.Errorf("scan floor %d and scan epoch %q disagree about presence", rec.ScanFrom, rec.ScanEpoch)
	}
	if rec.ScanEpoch != _EMPTY_ && !mqttValidSessEpoch(rec.ScanEpoch) {
		return fmt.Errorf("scan epoch %q is not well-formed", rec.ScanEpoch)
	}
	if !mqttValidSessEpoch(rec.OriginSessEpoch) {
		return fmt.Errorf("origin session epoch %q is not well-formed", rec.OriginSessEpoch)
	}
	switch rec.State {
	case mqttWillPending, mqttWillPublishing, mqttWillCancelled, mqttWillDone:
	default:
		return fmt.Errorf("state %q is not a known outbox state", rec.State)
	}
	return validateWillDecisionShape(rec.State, rec.Decision, rec.DecisionCutoff)
}

// validateWillOutboxRecord is validateWillOutboxBody plus the binding between the
// record and the subject it was stored on: the identities in the body must be the
// identities the subject is built from.
//
// Body-only validation is not enough at a decode boundary. A well-formed record
// carrying client B's identities, stored on client A's subject, would be fenced
// with B's watermark and session state and then claimed and published on A's
// subject; finalize's WillID equality only detects a post-claim REPLACEMENT, it
// never establishes that the claimed body matched its own subject.
//
// The writer validates the body alone: it builds the subject from those same
// fields, so the binding holds by construction.
func validateWillOutboxRecord(domainTk, subject string, rec *mqttWillOutbox) error {
	if err := validateWillOutboxBody(rec); err != nil {
		return err
	}
	if want := mqttWillQSubject(domainTk, rec.ClientIDHash, rec.WillID); subject != want {
		return fmt.Errorf("record identities name subject %q, but it is stored on %q", want, subject)
	}
	return nil
}

// mqttWillQSubject returns the per-Will subject for a client hash + Will id.
func mqttWillQSubject(domainTk, cidHash, willID string) string {
	return mqttWillQSubjectPrefix + domainTk + cidHash + "." + willID
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
//
// A record that decodes but is not a supported one is returned as an ERROR, never
// as a record and never as "absent": every point-load caller acts on what it gets
// back, and an unsupported record must be acted on by nobody. Validating here is
// what leaves no unvalidated semantic loader in the package; a test that needs to
// observe a refused record reads the stream's raw bytes instead.
func (jsa *mqttJSA) loadWillOutbox(domainTk, subject string) (*mqttWillOutbox, uint64, error) {
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
	if err := validateWillOutboxRecord(domainTk, smsg.Subject, rec); err != nil {
		return nil, 0, err
	}
	return rec, smsg.Sequence, nil
}

// decodeScannedWillOutbox decodes one scanned outbox message and refuses it if it
// is not a supported record: unsupported records are logged (once per subject) and
// skipped, never claimed, published, cancelled or deleted. Deletion is withheld
// deliberately — it is a mutation of state we have just declared we cannot
// interpret, and unlike a retained-message cache entry the outbox record is the
// only durable copy of a Will-shaped fact. Shared by both scans so the claim pass
// and the cancel pass refuse exactly the same set.
func (as *mqttAccountSessionManager) decodeScannedWillOutbox(s *Server, smsg *StoredMsg) (*mqttWillOutbox, bool) {
	rec := &mqttWillOutbox{}
	if err := json.Unmarshal(smsg.Data, rec); err != nil {
		as.refuseWillOutboxRecord(s, smsg.Subject, err)
		return nil, false
	}
	if err := validateWillOutboxRecord(as.domainTk, smsg.Subject, rec); err != nil {
		as.refuseWillOutboxRecord(s, smsg.Subject, err)
		return nil, false
	}
	return rec, true
}

// refuseWillOutboxRecord reports an unsupported outbox record and the operator
// remedy. Warn once per subject per account-manager lifetime, Debug thereafter:
// the claim scan meets every record every mqttWillClaimInterval, so warning each
// pass would bury the one actionable line under twelve identical ones a minute.
//
// The remedy is stated, and stated as URGENT, because nothing else resolves it: a
// refused record is reclaimed by NOTHING — no sweep, no fence and no reaper touches
// it — and the claim walk re-reads it every pass. A backlog costs one JetStream get
// per record per scan per broker, which delays valid due Wills. The log must not
// imply that leaving it alone either clears it or is free.
func (as *mqttAccountSessionManager) refuseWillOutboxRecord(s *Server, subject string, err error) {
	as.mu.Lock()
	_, warned := as.refusedWillRecords[subject]
	if !warned {
		if as.refusedWillRecords == nil {
			as.refusedWillRecords = map[string]struct{}{}
		}
		as.refusedWillRecords[subject] = struct{}{}
	}
	as.mu.Unlock()
	const format = "MQTT: refusing unsupported delayed-Will outbox record on %q: %v. It will never fire, be cancelled or be reaped, and the claim scan re-reads it every interval; delete that message from the %s stream PROMPTLY (recreating the stream is NOT an option: the broker-owned streams are one recreation unit, so the alternative is a whole-bundle reset), or an accumulating backlog will slow every scan and delay valid Wills"
	if warned {
		s.Debugf(format, subject, err, mqttWillQStreamName)
		return
	}
	s.Warnf(format, subject, err, mqttWillQStreamName)
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
		rec, seq, err := as.jsa.loadWillOutbox(as.domainTk, subject)
		if err != nil || rec == nil {
			// Transient load failure or already reaped; the scan retries.
			return
		}
		if rec.State != mqttWillPending || rec.FireAt > time.Now().UnixNano() {
			return
		}
		// tryFireWill owns the claim, the decision and the dispatch. Its willScanAbort
		// result only tells a multi-record scan to stop, so it is not meaningful for
		// this single record.
		_ = as.tryFireWill(s, subject, rec, seq)
	})
}

// mqttTestDelayedWillEnqueuedV, when set, observes every delayed-Will record whose
// create the enqueue CONFIRMED, on both confirmation exits. A test reads the record's
// origin identity here because a pending record may be cancelled ahead of its FireAt
// (cancelSupersededAhead) and reaped before any scan of the stream could see it. Nil
// in production.
var mqttTestDelayedWillEnqueuedV atomic.Pointer[func(rec *mqttWillOutbox)]

func mqttTestDelayedWillEnqueued(rec *mqttWillOutbox) {
	if h := mqttTestDelayedWillEnqueuedV.Load(); h != nil {
		(*h)(rec)
	}
}

// enqueueDelayedWill writes a new pending outbox record for a delayed Will. A
// local fire timer publishes it at FireAt; the claim loops (on every replica,
// including this one) are the backstop and fire it once FireAt passes.
// Called from the disconnect path, and only for a Will the caller has already
// established is authorized — by the decision captured at CONNECT, not by a check
// made here, which could not be truthful on a teardown path. So a record on this
// stream IS the durable evidence that authorization passed, which is what lets the
// replay publish through the internal client without re-deriving anything. F-079.
//
// The outbox is the only durable fire mechanism, so a transient store failure
// must not silently drop the Will: the create is retried (idempotently — the
// WillID, and thus the subject, is fixed across attempts, so a create-only CAS
// that fails because a prior attempt's write actually landed is treated as
// success). If the durable enqueue still cannot be confirmed after the bounded
// retries, the Will is published IMMEDIATELY as a fail-safe (trading the Will
// Delay for not losing the Will) rather than left with no authority to fire it.
//
// The record is validated before it is persisted, and a record that would be
// refused by every reader takes the same fail-safe rather than being written:
// persisting one converts a sacrificed delay into permanent loss PLUS a permanent
// operator diagnostic. No current caller can reach it — all of them pass a live
// session's generation, the startup-validated stream epoch, and the nonzero
// sequence of the CONNECT save a surviving session implies — so it is an
// invariant assert that happens to have a safe arm.
func (as *mqttAccountSessionManager) enqueueDelayedWill(s *Server, clientID, sessGen string, will *mqttWill, props []byte, user string, fireAt int64, originSessionSeq uint64, originSessEpoch string, authorizedAccount string, identity *mqttDelayedWillIdentity) (mqttWillEnqueueOutcome, error) {
	// THE IDENTITY IS ALLOCATED BEFORE THE RETAINABLE REFUSAL, not after it.
	//
	// A retained obligation re-attempts THIS enqueue, and its idempotence rests on
	// every attempt being the same create-only CAS on the same subject: the re-read
	// below rejects a stranger by `existing.WillID != willID`. Minting a fresh
	// WillID per attempt would make each retry a NEW record, so a first attempt
	// whose write landed while its ack was lost would be joined by a second -- two
	// records for one Will. Allocating after the unavailability check, which is what
	// this function used to do, made that unavoidable for the retained population,
	// because the refusal happened before an identity existed.
	//
	// The caller passes a pointer to an identity it owns. A ZERO one is FILLED IN
	// here and is then the caller's to retain, so a retained obligation can never
	// carry an identity different from the one the refused attempt would have used;
	// a populated one is reused verbatim, which is what a re-attempt does. Callers
	// with nothing to retain may pass nil.
	if identity == nil {
		identity = &mqttDelayedWillIdentity{}
	}
	if identity.WillID == _EMPTY_ {
		identity.WillID = nuid.Next()
	}
	if identity.CIDHash == _EMPTY_ {
		identity.CIDHash = getHash(clientID)
	}
	willID := identity.WillID
	cidHash := identity.CIDHash
	subject := mqttWillQSubject(as.domainTk, cidHash, willID)

	// The destination this Will was authorized for. The replay happens on an
	// internal client with no identity of its own, so the record has to carry it.
	//
	// TWO refusals, not one, and both are UNAVAILABLE rather than denied -- refused
	// BEFORE any enqueue effect, which is what makes retention safe here. An
	// established account that is not the one this Will was authorized against is
	// the same class: deliverWillRecord's own fence refuses it at fire time
	// (errMQTTWillDestinationUnavailable), so classifying it here moves that fence
	// earlier rather than adding a rule. Checking only that SOME account is
	// established would let a Will authorized for A persist under current B.
	svcAccount, svcOK := s.serviceAccountForWill()
	if !svcOK {
		return mqttWillEnqueueUnavailable, fmt.Errorf("cannot enqueue a delayed Will yet: the service connection's account is not currently established")
	}
	if authorizedAccount != _EMPTY_ && svcAccount != authorizedAccount {
		return mqttWillEnqueueUnavailable, fmt.Errorf("cannot enqueue a delayed Will yet: it was authorized for account %q but the service connection is on %q",
			authorizedAccount, svcAccount)
	}
	rec := &mqttWillOutbox{
		WillID:           willID,
		ClientIDHash:     cidHash,
		ClientID:         clientID,
		Gen:              sessGen,
		FireAt:           fireAt,
		State:            mqttWillPending,
		Account:          svcAccount,
		OriginSessionSeq: originSessionSeq,
		OriginSessEpoch:  originSessEpoch,
	}
	willOutboxMessage(rec, will, props, user)
	// Body only: the subject above is built from these same identities, so the
	// subject binding validateWillOutboxRecord adds holds by construction.
	if verr := validateWillOutboxBody(rec); verr != nil {
		s.Errorf("MQTT: refusing to enqueue an unsupported delayed-Will outbox record for client hash %q: %v; publishing immediately as a fail-safe", cidHash, verr)
		// The writer-side refusal: nothing was ever sent, so provably no record
		// exists. Whether the fail-safe publishes or errors, this exchange is
		// TERMINAL either way -- the record can never be written, so there is nothing
		// to retain and nothing to retry.
		if ferr := as.publishWillFailSafe(s, cidHash, rec); ferr != nil {
			return mqttWillEnqueueTerminalFailed, ferr
		}
		return mqttWillEnqueuePublishedInstead, nil
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
			mqttTestDelayedWillEnqueued(rec)
			as.earlyCancelIfSuperseded(s, subject, rec, newSeq)
			return mqttWillEnqueueConfirmed, nil
		}
		err = serr
		// The create-only CAS may have failed because our own earlier attempt's
		// write landed but its ack was lost: re-read to resolve the ambiguity.
		existing, exSeq, lerr := as.jsa.loadWillOutbox(as.domainTk, subject)
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
		mqttTestDelayedWillEnqueued(existing)
		return mqttWillEnqueueConfirmed, nil
	}
	s.Warnf("MQTT: could not durably enqueue delayed Will for client hash %q after %d attempts (%v); publishing immediately as a fail-safe", cidHash, mqttWillEnqueueAttempts, err)
	// UNCONFIRMED, not proven absent: a create may have landed while its ack was
	// lost. Whatever the fail-safe does, the caller may NOT retry and may NOT publish
	// again -- a record may exist AND the Will may have gone out.
	//
	// The error VALUE is deliberately not consulted here. The fail-safe can return
	// errMQTTWillDestinationUnavailable, and classifying on that sentinel would send
	// a possibly-durable obligation back into retention, where a re-attempt would
	// mint a second record. THE EXIT DECIDES, NEVER THE ERROR.
	if ferr := as.publishWillFailSafe(s, cidHash, rec); ferr != nil {
		return mqttWillEnqueueUnconfirmedAbandoned, ferr
	}
	return mqttWillEnqueueUnconfirmedPublished, nil
}

// publishWillFailSafe publishes a delayed Will immediately because this broker has
// no CONFIRMED durable record to fire it later: the Will Delay is sacrificed rather
// than the Will. Its two callers do NOT carry the same guarantee, and the
// difference is the difference between a lost delay and a duplicate.
//
//   - The writer-side refusal: nothing was ever sent, so provably no record exists.
//     A crash between the refusal and the publish loses the Will, which is inside
//     the stated guarantee — durability begins at acknowledged outbox persistence
//     and nothing was persisted.
//   - Retry exhaustion: UNCONFIRMED, not proven absent. A create can land while its
//     ack is lost, and if every bounded re-read is also indeterminate the loop
//     exits here with a durable pending record in place. That record fires later,
//     so the Will is published twice. This is pre-existing behavior, not something
//     the F-058 validation introduced, and it is the outbox's sanctioned
//     at-least-once duplicate reached by a second route: the alternative is
//     staying silent on an enqueue that may never have landed, which is loss.
//     TestMQTTWillOutboxEnqueueExhaustionAfterLandedWriteDuplicates pins it.
//
// A confirmed enqueue must never reach here — that would publish AND leave a record
// this broker knows will fire.
//
// (For QoS 0 the publish succeeds even if the willq store failed; for QoS 1/2 it
// hits the same JetStream store, so a sustained outage still fails — logged at
// Error.)
func (as *mqttAccountSessionManager) publishWillFailSafe(s *Server, cidHash string, rec *mqttWillOutbox) error {
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
	as.cancelIfSuperseded(s, subject, rec, seq, time.Time{})
}

// cancelIfSuperseded is the POSITIVE-ONLY suppression: it scans this Will's
// acceptance facts and, if one qualifies, licenses a cancel straight from pending
// with a single CAS. It never publishes and never persists a negative verdict.
//
// It takes no claim, and that is not an omission. A claim exists so that a claimant
// which may PUBLISH cannot have its decision re-taken after the irreversible act;
// this path never publishes, and its single CAS at the pending sequence is its own
// exclusion — a concurrent claimant either moves the record first (and this CAS
// fails) or does not (and this cancel is what it finds). Claiming here would also
// leave a publishing record behind whenever no fact qualified, and the recovery arm
// fires a publishing record regardless of FireAt: a best-effort optimization would
// have become a way to publish a Will before it was due.
//
// Best-effort throughout. Every failure leaves the record exactly as it was, and the
// claimant at FireAt remains the authority.
//
// bound caps the whole call. A caller that has its own budget passes it, and the
// effective deadline is the EARLIER of the two: a nested scan that could run to its
// own budget would make the caller's meaningless. The zero value means "no caller
// bound", used by the enqueue-time route, which runs on the disconnecting
// connection's own goroutine rather than on the claim loop.
//
// rec must be PENDING at sequence seq.
func (as *mqttAccountSessionManager) cancelIfSuperseded(s *Server, subject string, rec *mqttWillOutbox, seq uint64, bound time.Time) {
	deadline := time.Now().Add(as.scanBudget())
	if !bound.IsZero() && bound.Before(deadline) {
		deadline = bound
	}
	if !time.Now().Before(deadline) {
		return
	}
	// Sampled INSIDE the bound, for the reason tryFireWill states: the cutoff read is
	// itself a request, and a bound that excluded it would not bound the call.
	upTo, uerr := as.sessLastSeq()
	if uerr != nil {
		return
	}
	stop := as.claimStopChans(s)
	if probe := (&mqttFactScan{stop: stop}); probe.stopped() || !time.Now().Before(deadline) {
		return
	}
	sc := &mqttFactScan{
		upTo:       upTo,
		deadline:   deadline,
		renewEvery: as.renewCadence(),
		stop:       stop,
	}
	if as.scanAcceptFacts(rec, sc) != mqttFactQualified {
		return
	}
	// And once more immediately before the write. Everything between the last check
	// and here is a decision, not an act; this is the act, and it must not be the one
	// thing that outlives shutdown or the caller's bound.
	if sc.stopped() || !time.Now().Before(deadline) {
		return
	}
	cancelled := *rec
	cancelled.State = mqttWillCancelled
	cancelled.Decision = mqttWillDecisionCancel
	cutoff := upTo
	cancelled.DecisionCutoff = &cutoff
	if _, err := as.jsa.storeWillOutboxCAS(subject, seq, &cancelled); err != nil {
		s.Debugf("MQTT: cancel of superseded delayed Will %q lost/failed (the claimant fence will resolve): %v", rec.WillID, err)
		// The CAS outcome is ambiguous: the cancelled write may have landed with a
		// lost ack, or another writer may have advanced or deleted the record. In
		// either of those the record is no longer pending and the timer is dead
		// weight until FireAt. Re-read to tell them apart, and keep the timer only
		// when the record is confirmed still pending (or the re-read itself is
		// indeterminate, where the claimant fence remains the safety net).
		cur, _, lerr := as.jsa.loadWillOutbox(as.domainTk, subject)
		if lerr == nil && (cur == nil || cur.State != mqttWillPending) {
			s.cancelWillTimer(subject)
		}
		return
	}
	// Cancelled: release the local fire timer rather than leaving it armed until
	// FireAt for a record that will never fire. A no-op on any broker but the one
	// that enqueued it — which is why the scan's completed pass reconciles the rest.
	s.cancelWillTimer(subject)
}

// mqttTestWillInitialScanDoneV runs once the claim loop's initial outbox scan has returned, so a
// measurement of a broker start can wait for it. nil in production.
var mqttTestWillInitialScanDoneV atomic.Pointer[func(as *mqttAccountSessionManager)]

// willClaimLoop periodically scans the account's outbox and drives due Wills to
// publication. Tracked by willClaimWG so shutdown Phase 2 drains it before
// waiting on publisher workers; it stops on the dispatch-gate signal, the
// account close channel, or server quit.
func (as *mqttAccountSessionManager) willClaimLoop(s *Server, closeCh chan struct{}) {
	defer s.willClaimWG.Done()
	as.scanWillOutbox(s) // initial pass so due Wills fire promptly after (re)start
	if h := mqttTestWillInitialScanDoneV.Load(); h != nil {
		(*h)(as)
	}
	tt := time.NewTicker(mqttWillClaimInterval.Get())
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
// (a transient or global failure to read the evidence: the acceptance cutoff or an
// acceptance fact) stops the scan, leaving the record as it found it, so a $MQTT_sess
// outage costs at most one JSA timeout per SCAN rather than one per due record; the
// next interval retries. willClaimStopCh is checked between records so Phase-2
// shutdown does not continue a long scan.
func (as *mqttAccountSessionManager) scanWillOutbox(s *Server) {
	// Captured BEFORE the walk, for the same reason the per-client capture was: a
	// completed walk only proves no record was pending while it ran, and an enqueue
	// blocked behind it can land immediately after and arm a genuinely wanted timer.
	// Anything armed during or after the walk is excluded by construction.
	captured := s.snapshotWillTimersWithPrefix(mqttWillQSubjectPrefix + as.domainTk)
	// Subjects this walk saw still PENDING. Everything else the capture holds — a
	// record a peer cancelled and reaped, one that has gone terminal, one this broker
	// can no longer name at all — is a timer for a Will that will never fire from
	// here.
	stillPending := map[string]struct{}{}
	var future []futureWill
	complete := false

	filter := mqttWillQAccountFilter(as.domainTk)
walk:
	for seq := uint64(1); ; {
		select {
		case <-s.willClaimStopCh:
			break walk
		case <-s.quitCh:
			break walk
		default:
		}
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttWillQStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT will-outbox scan ended: %v", err)
				break walk
			}
			complete = true // no more records: the walk is conclusive
			break walk
		}
		seq = smsg.Sequence + 1
		// Sampled PER RECORD, never once for the walk. A single acceptance-fact scan
		// can consume the whole scan budget, so a timestamp taken at the top would by
		// then be minutes old — and it decides two things that must not be stale:
		// whether a record is due, and whether a claim's lease has expired.
		now := time.Now()
		rec, ok := as.decodeScannedWillOutbox(s, smsg)
		if !ok {
			// Refused: never claimed, dispatched or deleted — and never swept either.
			// A record this broker cannot interpret is not evidence that its timer is
			// unwanted, so the subject counts as pending for the sweep below.
			stillPending[smsg.Subject] = struct{}{}
			continue
		}
		switch rec.State {
		case mqttWillPending:
			if rec.FireAt <= now.UnixNano() {
				if as.tryFireWill(s, smsg.Subject, rec, smsg.Sequence) == willScanAbort {
					break walk
				}
				continue
			}
			// Not due. Its fate cannot be settled here — "no acceptance qualifies" is
			// an UNSTABLE verdict, since a later reconnect can still add one — so it is
			// deferred to the budgeted phase below, which only ever acts on the stable
			// half of the predicate.
			stillPending[smsg.Subject] = struct{}{}
			future = append(future, futureWill{subject: smsg.Subject, rec: rec, seq: smsg.Sequence})
		case mqttWillPublishing:
			stillPending[smsg.Subject] = struct{}{}
			if !rec.leaseValid(now) {
				// Crashed-publisher recovery. Its abort result is tested for the same
				// reason the pending arm's is, and NOT testing it was the defect: a
				// transient outage reading the acceptance evidence costs one JetStream
				// timeout per expired record instead of one per pass, and a backlog of
				// expired records turns a brief outage into a scan that never finishes.
				if as.tryFireWill(s, smsg.Subject, rec, smsg.Sequence) == willScanAbort {
					break walk
				}
			}
		case mqttWillCancelled, mqttWillDone:
			_ = as.jsa.deleteMsg(mqttWillQStreamName, smsg.Sequence, false)
		}
	}

	if !complete {
		// Inconclusive. Nothing below may run: a walk that stopped early proves
		// nothing about the records it never reached, and both the early cancellation
		// and the timer sweep are entitled to act only on what the walk established.
		return
	}
	as.cancelSupersededAhead(s, future)
	// Only now, and only over the pre-walk capture: a timer whose record this walk
	// saw still pending is wanted, and every other captured timer is for a record
	// that is gone, terminal, or claimed. This is the ONLY thing that releases the
	// enqueuing broker's timer when a PEER cancelled and reaped the record first —
	// that broker's own cancelWillTimer is a no-op on a subject it never sees again.
	// It decides no MQTT outcome; it releases a local promptness optimization after
	// durable state has proved the timer represents nothing.
	var stale []willTimerRef
	for _, ref := range captured {
		if _, keep := stillPending[ref.key]; !keep {
			stale = append(stale, ref)
		}
	}
	s.stopWillTimers(stale)
}

// futureWill is a pending record whose FireAt has not arrived, carried out of the
// walk so the early-cancellation phase runs AFTER every due Will has been handled.
type futureWill struct {
	subject string
	rec     *mqttWillOutbox
	seq     uint64
}

// cancelSupersededAhead suppresses not-yet-due Wills for which a qualifying
// acceptance ALREADY exists, rather than waiting for FireAt.
//
// It is sound because the suppression predicate is MONOTONE: every operand is fixed
// for a pending record (the fact's date and client, the epochs, the two sequences)
// and a fact is never rewritten or removed, so a QUALIFYING verdict reached now is
// the verdict FireAt would reach. The converse is not true, which is why this phase
// is positive-only: "nothing qualifies" is unstable — a later reconnect can add a
// fact — so it is never persisted and never licenses a publication.
//
// It is not a second decision mechanism. There is one suppression fact, one
// predicate and one licensing shape; the enqueue-time early cancel, this phase and
// the FireAt claimant are three CADENCES of the same positive test, and all three
// go through cancelIfSuperseded.
//
// Why it runs AFTER the walk rather than inline: a fact scan may consume up to the
// scan budget, and one taken inline for a Will due in a year would delay publishing
// a Will that is owed now. The phase has its own pass-wide budget, and a per-account
// cursor so successive passes make progress round-robin instead of restarting at the
// same records forever.
func (as *mqttAccountSessionManager) cancelSupersededAhead(s *Server, future []futureWill) {
	if len(future) == 0 {
		return
	}
	deadline := time.Now().Add(mqttWillAheadBudget())
	as.mu.Lock()
	start := as.willAheadCursor
	as.mu.Unlock()
	if start >= len(future) {
		start = 0
	}
	done := 0
ahead:
	for i := range future {
		// LABELLED: an unlabelled break inside a select leaves the SELECT, not the
		// loop, so the phase would carry on right through shutdown.
		select {
		case <-s.willClaimStopCh:
			break ahead
		case <-s.quitCh:
			break ahead
		default:
		}
		if !time.Now().Before(deadline) {
			break
		}
		f := future[(start+i)%len(future)]
		// Re-checked HERE, not at collection time: the walk may have taken a while,
		// and a record that has since fallen due belongs to the claimant, which is the
		// only path allowed to reach the unstable half of the predicate.
		if f.rec.FireAt <= time.Now().UnixNano() {
			done++
			continue
		}
		// The OUTER deadline is threaded in, not merely checked between records. A
		// budget that bounds only the loop and not the work each iteration starts is
		// not a budget: one client with a long acceptance history would hold the claim
		// loop for a full scan budget, and every Will that fell due meanwhile would
		// fire late on every replica without a local timer.
		as.cancelIfSuperseded(s, f.subject, f.rec, f.seq, deadline)
		done++
	}
	as.mu.Lock()
	as.willAheadCursor = (start + done) % len(future)
	as.mu.Unlock()
}

// mqttWillAheadBudget bounds the early-cancellation phase for one pass. Half the
// claim interval, so the phase cannot make the loop miss its own tick: this is
// cleanup, and a due Will always outranks it.
func mqttWillAheadBudget() time.Duration { return mqttWillClaimInterval.Get() / 2 }

// willScanResult tells scanWillOutbox whether to keep walking records after a
// claim attempt, or to stop because a transient/global failure reading the
// acceptance evidence would otherwise cost one JSA timeout per record.
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

// claimStopChans are the signals a claimant's fact scan must abandon on. Resolved
// per manager rather than read straight off the Server, so a test that QUIETS this
// account's background claim loop — by closing the server's willClaimStopCh — can
// still drive a claimant directly instead of having every scan defer at once. Without
// it a claimant test shares its account with a live loop that can reclaim any record
// whose lease has expired, which is precisely what every budget-exhaustion test
// leaves behind (the budget is two leases, so the lease always expires first).
func (as *mqttAccountSessionManager) claimStopChans(s *Server) []<-chan struct{} {
	as.mu.RLock()
	override := as.testClaimStop
	as.mu.RUnlock()
	if override != nil {
		return *override
	}
	return []<-chan struct{}{s.willClaimStopCh, s.quitCh}
}

// claimLease is this account manager's publish lease. It is resolved PER MANAGER
// rather than read from the package variable directly, because a test that needs a
// short lease must not reach across into every other manager's claim loop: the
// variable is read by a live loop on another goroutine, so assigning it is a data
// race, not merely a shared setting.
func (as *mqttAccountSessionManager) claimLease() time.Duration {
	as.mu.RLock()
	d := as.testClaimLease
	as.mu.RUnlock()
	if d <= 0 {
		return mqttWillClaimLease.Get()
	}
	return d
}

// renewCadence is how often a scanning claimant renews its lease. Derived from the
// lease rather than configured beside it, so shortening the lease cannot silently
// produce a cadence that never renews in time. A third of the lease leaves two
// cadences of margin, so a single lost renewal is survivable.
func (as *mqttAccountSessionManager) renewCadence() time.Duration { return as.claimLease() / 3 }

// scanBudget bounds the whole of one claimant's fact scan. A claimant that reaches it
// licenses NOTHING and defers: it leaves the record publishing and lets lease expiry
// re-drive. Two leases is deliberately generous — the budget is a backstop against a
// pathological scan, not the mechanism that keeps a scan short.
func (as *mqttAccountSessionManager) scanBudget() time.Duration { return 2 * as.claimLease() }

// mqttWillTimingInvariant asserts the relationships the claim protocol depends on,
// rather than leaving them as properties of whatever the constants happen to be:
//
//   - the renewal cadence must be strictly below the lease, or a renewal never
//     precedes expiry and a scanning claimant is reclaimed underneath itself;
//   - the JetStream request timeout must be below the lease, so no SINGLE request can
//     by itself consume an entire ownership window. That is the whole of what this
//     inequality delivers, and it is deliberately weaker than "a request completes
//     inside the lease it was licensed under", which it does NOT give: the publish
//     licensing CAS stamps a fresh lease and then spends up to a whole timeout being
//     acknowledged, so a timeout just under the lease leaves the worker a residue
//     approaching zero — `T < L` is not `2T < L`. The worker then also waits on
//     mqttSubsBarrier, which no timeout bounds at all. So the worker's deliberate lack
//     of renewal does not rest on this relationship; the overrun it leaves is the
//     residual docs/CONFORMANCE.md discloses, for which docs/ROADMAP.md carries a
//     phase-aware lease-ownership fix (see willPublishWorker);
//   - the claim interval must be below the lease, so a record is reclaimed by a LATER
//     scan rather than one whose lease is still valid. Note this is about lease
//     VALIDITY, not about whether the owning broker is alive — a live broker whose
//     lease expired is reclaimable, which is the point of a lease.
//
// It takes its inputs rather than reading the package variables, which is what lets
// one function serve two callers: a TEST asserting the relationship over the
// production defaults, and account-manager construction checking the EFFECTIVE
// JetStream request timeout against the DEFAULT lease. The second caller is the load
// -bearing one, because the timeout is an option an operator can set while the lease
// is a constant — and a timeout at or above the lease means EVERY request can outlive
// the ownership window, so a licensing ack can arrive after a peer has already
// reclaimed the record and published it. That is a misconfiguration this refuses
// outright, distinct from the disclosed barrier overrun, which no configuration
// prevents. The default lease is deliberately the reference:
// tests shorten the lease per-manager, and a two-second lease against a five-second
// request timeout is their intent rather than a misconfiguration.
func mqttWillTimingInvariant(lease, interval, jsaTimeout time.Duration) error {
	if c := lease / 3; c >= lease {
		return fmt.Errorf("renewal cadence %v is not below the claim lease %v", c, lease)
	}
	if jsaTimeout >= lease {
		return fmt.Errorf("the JetStream request timeout %v is not below the claim lease %v", jsaTimeout, lease)
	}
	if interval >= lease {
		return fmt.Errorf("the claim interval %v is not below the claim lease %v", interval, lease)
	}
	return nil
}

// tryFireWill drives one due or recoverable outbox record to a licensed act.
//
// The order is CLAIM FIRST, then decide, and that is the correction this protocol
// exists for. Deciding first and claiming afterwards leaves the decision unowned:
// two claimants can reach opposite verdicts from cutoffs that straddle one
// acceptance, and the loser's verdict is not even recorded anywhere to be
// contradicted. Deciding under a claim, and PERSISTING the decision with the very
// CAS that licenses it, makes the decision a property of the Will generation rather
// than of whichever broker happened to be looking.
//
//  1. Claim or reclaim (pending -> publishing, or publishing -> publishing at the
//     scanned sequence), carrying Decision and DecisionCutoff forward unchanged.
//  2. If the claimed record is already LICENSED TO PUBLISH, execute that decision.
//     No cutoff is sampled, no fact is read, and whatever acceptances have landed
//     since are irrelevant: the decision was taken and it is not re-taken.
//  3. Otherwise sample a cutoff, scan this Will's acceptance facts within it while
//     renewing the claim, and license the outcome with one CAS — publish (staying
//     publishing, with a refreshed lease) or cancel (straight to cancelled).
//
// Returns willScanAbort only for a transient, GLOBAL failure, so a stream outage
// costs one JetStream timeout per pass rather than one per record.
//
// It samples its OWN clock rather than taking one from the caller. A scan can spend
// its whole budget on one record's acceptance-fact walk, so a timestamp handed down
// from the top of a pass would mint a lease that had ALREADY expired — and a claim
// whose lease is in the past is reclaimable by a peer the instant it lands, which
// would put two brokers on one licensed publish. That is a defect rather than the
// sanctioned duplicate, and the distinction is not that a crash did or did not happen:
// it is that such a lease was never an ownership window at all. The sanctioned
// duplicate needs a lease minted correctly and then EXPIRED, with the publish
// externally done and `done` uncommitted.
func (as *mqttAccountSessionManager) tryFireWill(s *Server, subject string, rec *mqttWillOutbox, seq uint64) willScanResult {
	// Step 1. The claim carries the decision forward untouched — a reclaim that
	// cleared it would let a recovering claimant re-decide a Will another claimant
	// may already have published.
	claimed := *rec
	claimed.State = mqttWillPublishing
	claimed.ClaimOwner = s.NodeName()
	claimed.ClaimUntil = time.Now().Add(as.claimLease()).UnixNano()
	claimSeq, err := as.storeWillClaimCAS(subject, seq, &claimed)
	if err != nil {
		// Lost the claim (a peer took it, a cancel won) or a transient write failure.
		// A later scan re-resolves; nothing here is owed.
		return willScanContinue
	}

	// Step 2. A persisted publish decision is EXECUTED, never revisited.
	if claimed.licensedToPublish() {
		as.dispatchWillPublish(s, subject, &claimed, claimSeq)
		return willScanContinue
	}

	// Step 3. Undecided: sample the cutoff, then scan under it.
	//
	// The budget starts HERE, before the cutoff read, not after it. That read is a
	// JetStream request with its own timeout, so a budget that began afterwards left it
	// outside the bound entirely — and a claimant that spent its whole budget getting a
	// cutoff would then go on to license an act under it.
	deadline := time.Now().Add(as.scanBudget())
	// Resolved BEFORE the cutoff read, because that read is blocking: shutdown can
	// close while it is in flight, and a stop-aware scan built only afterwards would
	// never see it.
	//
	// There is deliberately NO check between the cutoff read and the scan. One was
	// written and then deleted: the scan's own pre-read check catches exactly the same
	// condition before it issues a request, so nothing could distinguish the two and a
	// mutation of either survived — three checks where two decide everything. What
	// remains is the scan's own bracket, and the final guard below for the one path the
	// scan cannot cover: a verdict reached without entering the loop at all.
	stop := as.claimStopChans(s)
	upTo, uerr := as.sessLastSeq()
	if uerr != nil {
		// The cutoff itself is unavailable. Global and transient, so it stops the pass
		// for the same reason a fact-load failure does. The record stays publishing and
		// its lease expiry re-drives it.
		s.Debugf("MQTT: delayed Will %q could not sample an acceptance cutoff: %v", rec.WillID, uerr)
		return willScanAbort
	}
	curSeq := claimSeq
	// Declared before the closure so the renewal can read the scan's own progress.
	sc := &mqttFactScan{
		upTo: upTo,
		// A floor from another incarnation is discarded: the sequence it names belongs
		// to a store that no longer exists.
		from:       claimed.usableScanFloor(as.sessStreamEpoch),
		deadline:   deadline,
		renewEvery: as.renewCadence(),
		// Armed once, HERE, and deliberately not re-armed per cutoff-chase pass: the
		// lease it protects runs from the claim, not from whichever pass is current.
		nextRenew: time.Now().Add(as.renewCadence()),
		stop:      stop,
	}
	sc.renew = func() error {
		renewed := claimed
		renewed.ClaimUntil = time.Now().Add(as.claimLease()).UnixNano()
		// The renewal carries the scan's PROGRESS, which is what makes a bounded
		// scan able to finish an unbounded history: a later claimant resumes here
		// instead of re-reading the same rejected prefix forever.
		//
		// Compared against the USABLE floor, not the stored number. A floor from
		// another incarnation is already ignored for reading, so the scan restarts
		// at the beginning — but comparing progress against a numerically larger
		// stale value would then persist nothing, and every pass would re-read the
		// same prefix and re-exhaust the same budget. That is the wedge this field
		// exists to prevent, reached by scoping only one of the two sides.
		if v := sc.verified; v > renewed.usableScanFloor(as.sessStreamEpoch) {
			renewed.ScanFrom, renewed.ScanEpoch = v, as.sessStreamEpoch
		}
		ns, rerr := as.storeWillClaimCAS(subject, curSeq, &renewed)
		if rerr != nil {
			// An UNCONFIRMED renewal is abandoned, not retried and not ignored. It
			// may be transient, or a successful renewal whose ack was lost, or proof
			// that a recovery claimant advanced the record — and only the last is a
			// verdict. Since the safe response to the verdict is to stop, and the
			// other two cost only a deferred Will, they are all treated alike.
			return rerr
		}
		curSeq = ns
		claimed.ScanFrom, claimed.ScanEpoch = renewed.ScanFrom, renewed.ScanEpoch
		return nil
	}
	verdict := as.scanAcceptFacts(rec, sc)
	// Chase the cutoff. The walk above examined everything at or below the cutoff it
	// was given, so an acceptance that committed WHILE it ran sits above that cutoff
	// and no reader ever returns to it: this claimant is bounded by `upTo` and every
	// later one executes the persisted decision without rescanning. Re-sampling and
	// examining only the delta narrows that to whatever commits after the final
	// sample. See mqttWillCutoffChases for why exhausting the limit decides instead
	// of deferring.
	for chase := 0; verdict == mqttFactNone && chase < mqttWillCutoffChases; chase++ {
		if sc.stopped() || !time.Now().Before(deadline) {
			return willScanContinue
		}
		fresh, ferr := as.sessLastSeq()
		if ferr != nil {
			// A narrowing, not a prerequisite: the cutoff already verified is a sound
			// basis for a decision, so an unavailable re-sample stops chasing rather
			// than abandoning a pass that has already read its evidence.
			break
		}
		if fresh <= sc.upTo {
			break // nothing committed while the walk ran
		}
		// Resume above the cutoff just finished. Sound for the same reason the
		// persisted floor is: every fact for this filter at or below it was examined
		// and rejected, and a fact is never rewritten.
		sc.from, sc.upTo = sc.upTo, fresh
		verdict = as.scanAcceptFacts(rec, sc)
	}
	switch verdict {
	case mqttFactDefer:
		// Shutdown, an exhausted budget, an unreadable fact, or a lost renewal. Act not
		// at all: the record stays publishing and lease expiry re-drives it.
		return willScanContinue
	case mqttFactAbort:
		return willScanAbort
	}

	// The budget AND the stop signals are checked again, immediately before the
	// licensing CAS. A renewed lease is not permission to act once the budget is gone:
	// the renewal proves this claimant still holds the record, while the budget is what
	// bounds how stale the cutoff it decided under may be. The stop check belongs here
	// too, and its omission was a real gap — a scan that returned a verdict without
	// entering its loop (a cutoff below the start) never checks a stop at all, and a
	// stop arriving after a verdict was never checked either.
	if !time.Now().Before(deadline) || sc.stopped() {
		return willScanContinue
	}

	// Step 4. License the act and persist the decision it was taken under, in one
	// CAS at the record's CURRENT sequence. A cancel goes straight to cancelled:
	// there is no licensed-cancel publishing state, which would be a state with two
	// meanings and an executable decision that must never be executed.
	licensed := claimed
	// The cutoff persisted is the one the scan actually VERIFIED through, which the
	// chase above may have advanced past the first sample. Recording the first sample
	// instead would understate the evidence a recovery claimant is told this decision
	// was taken under.
	cutoff := sc.upTo
	licensed.DecisionCutoff = &cutoff
	if verdict == mqttFactQualified {
		licensed.State = mqttWillCancelled
		licensed.Decision = mqttWillDecisionCancel
		if _, cerr := as.storeWillClaimCAS(subject, curSeq, &licensed); cerr != nil {
			s.Debugf("MQTT: unable to cancel superseded delayed Will %q: %v", rec.WillID, cerr)
			return willScanContinue
		}
		s.cancelWillTimer(subject)
		return willScanContinue
	}
	licensed.Decision = mqttWillDecisionPublish
	licensed.ClaimUntil = time.Now().Add(as.claimLease()).UnixNano()
	licSeq, lerr := as.storeWillClaimCAS(subject, curSeq, &licensed)
	if lerr != nil {
		return willScanContinue
	}
	as.dispatchWillPublish(s, subject, &licensed, licSeq)
	return willScanContinue
}

// dispatchWillPublish hands a record LICENSED to publish to a worker, through the
// shutdown-aware admission gate. A closed gate leaves the record publishing and
// licensed, which is exactly what post-restart recovery is meant to find: the
// decision survives, so the recovering claimant executes it without rescanning.
func (as *mqttAccountSessionManager) dispatchWillPublish(s *Server, subject string, rec *mqttWillOutbox, seq uint64) {
	if !s.admitWillWorker() {
		return
	}
	go as.willPublishWorker(s, subject, rec, seq)
}

// storeWillClaimCAS performs a claim, renewal or licensing CAS, honoring an
// instance-scoped test seam. Nil seam in production delegates straight to the JSA.
// Routed separately from the enqueue and finalize stores so an injected failure on
// one leg of the protocol never perturbs the others.
func (as *mqttAccountSessionManager) storeWillClaimCAS(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
	as.mu.RLock()
	hook := as.testClaimStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, rec)
	}
	return as.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
}

// willPublishWorker publishes a claimed Will and then finalizes its record.
// Tracked by willWorkerWG (admitted by tryFireWill). On a publish error the
// record is left publishing so its lease expiry drives a retry (at-least-once).
// On success the record is made durably terminal (publishing->done) BEFORE any
// cleanup: only then is the record deleted (at the done sequence). Committing done
// first means a failed delete can no longer leave a re-fireable publishing record —
// the scanner reaps the done record instead.
//
// The worker does not renew its lease. The construction check
// (mqttWillTimingInvariant) bounds ONE JetStream request below the lease, which is
// worth having — an operator-set timeout above the lease would make every publish
// outlive its ownership window — but it bounds a request, NOT this worker's whole
// licensed lifecycle, and must not be read as doing so.
//
// The gap is real and reachable, not pathological. mqttInitiateMsgDeliveryJSA fans
// the Will out to subscribers FIRST (irreversible, external), then waits on
// mqttSubsBarrier before storing the QoS 1/2 copy — and that wait is bounded by
// shutdown, not by the lease. The barrier is held for a whole processSubs, which
// creates a JetStream consumer per filter, so a CONNECT session restore with a few
// dozen subscriptions on a cluster timing requests out holds it past 30s. A peer
// then reclaims the still-`publishing` record, reuses the persisted publish
// decision, and publishes again with both brokers alive.
//
// That is the sanctioned at-least-once duplicate, because LEASE EXPIRY is the
// failure boundary: a crash, a pause, a partition and a live broker stalled in its
// own process are indistinguishable to recovery, which sees only "licensed publish,
// completion unknown". Bounding the barrier here would be worse, not better — the
// fan-out already happened, so abandoning would skip the store that protects the
// racing subscriber, trading a duplicate for a LOSS. End-to-end phase-aware
// ownership is a ROADMAP item; the honest statement today is that this worker can
// outlive its lease without anything crashing.
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
	// Only a LICENSED publish is finalizable. The previous form retried against any
	// current publishing record with the same WillID, which was already too broad and
	// becomes unsafe once a claim can be unlicensed: it would have committed done over
	// a record another claimant was still scanning under.
	if !rec.licensedToPublish() {
		s.Warnf("MQTT: refusing to finalize delayed Will %q on %q: it carries no publish licence", rec.WillID, subject)
		return 0, false
	}
	rec.State = mqttWillDone
	for attempt := 0; attempt < mqttWillFinalizeAttempts; attempt++ {
		newSeq, err := as.storeWillFinalizeCAS(subject, seq, rec)
		if err == nil {
			return newSeq, true
		}
		// CAS failed (transiently, on a stale sequence, or via an ambiguous seam
		// that may have written anyway). Re-read to learn the durable truth.
		cur, curSeq, lerr := as.jsa.loadWillOutbox(as.domainTk, subject)
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
		// The DECISION is part of what may be finalized, not just the Will. A record
		// carrying a different decision — or the same act under a different cutoff — is
		// a different licensing, and committing our terminal state over it would retire
		// a decision that was never ours. ClaimOwner is deliberately NOT required to
		// match: after a lease expires a peer may reclaim and republish the SAME
		// licensed decision, and finalizing it then is correct whoever published.
		if !cur.sameDecision(rec) {
			s.Warnf("MQTT: delayed Will %q finalize found a differently licensed record on %q (state %q); abandoning finalize", rec.WillID, subject, cur.State)
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

// willOutboxMessage fills in the message-shaped fields of an outbox record from
// a Will: what has to be published, independent of when or under whose claim.
// Shared by the durable delayed-Will enqueue and by the immediate publication a
// teardown makes when it has lost the session gate (mqttPublishOwedWill), so
// both describe the same Will the same way.
func willOutboxMessage(rec *mqttWillOutbox, will *mqttWill, props []byte, user string) {
	rec.Topic = string(will.topic)
	rec.Msg = will.message
	rec.QoS = will.qos
	rec.Retain = will.retain
	rec.Props = props
	rec.User = user
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
	// The record carries the account its authorization was decided against. If the
	// service connection is now bound elsewhere, this Will must not be published
	// through it: the decision never referred to that account's subjects, and the
	// internal client publishing it has no identity of its own to re-derive one
	// from. Returning an error leaves the record for a later attempt rather than
	// finalizing it, so a broker pointed back at the original account still fires
	// it.
	// EXACT equality, not "differs if set". Every writer of an outbox record
	// stamps the account it was authorized for, so an EMPTY one under auth
	// passthrough is a record no writer here produced -- and treating it as "no
	// opinion" would let exactly the records this check exists to stop straight
	// through. With auth passthrough off, serviceAccountForWill and every writer
	// agree on the empty string, so the comparison is a no-op there.
	if svcAccount, ok := s.serviceAccountForWill(); !ok || rec.Account != svcAccount {
		// Wrapped in a SENTINEL, because this refusal is the only one a caller may
		// retry. It happens before any fan-out and before any store, so nothing has
		// been partially applied and re-attempting it later duplicates nothing. A
		// generic delivery error is the opposite: it can follow a partial effect, so
		// the non-durable retry in will_blip_retry.go deliberately does not take it.
		return fmt.Errorf("%w: Will %q was authorized for account %q, and the service connection is bound to %q (established=%v)",
			errMQTTWillDestinationUnavailable, rec.WillID, rec.Account, svcAccount, ok)
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
	err = s.mqttInitiateMsgDeliveryJSA(c, &as.jsa, pp, _EMPTY_, _EMPTY_, _EMPTY_)
	c.flushClients(0)
	return err
}

// validateWillQStream verifies a $MQTT_willq stream can carry the delayed-Will
// outbox. Derived from what this owner does: a create-only CAS enqueue at a
// per-WillID subject, state-advance CAS transitions, loadLastMsgFor reads,
// filtered claim/cancel scans, and exact-sequence deletes (confirmed on the
// terminal path, no-wait in the scan's cleanup). It never purges and never
// creates a consumer, so DenyPurge, MaxConsumers and ConsumerLimits are
// audited NON-prerequisites.
//
// DenyDelete IS fatal, and the reason is unbounded growth, not re-publication:
// every outbox subject is per-WillID, so nothing is ever replaced in place and
// deletion is the only thing that removes a record. The durable
// publishing->done transition precedes the delete, so a denied delete cannot
// re-fire a Will — it strands one done record per fired Will forever, and
// every scan re-walks and re-attempts all of them.
func validateWillQStream(si *StreamInfo, domainTk string, wantReplicas int, bundleEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttWillQSubjectPrefix + domainTk + ">"}, wantReplicas); err != nil {
		return err
	}
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits): nothing consumes an outbox record, so any other retention drops it immediately", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		return fmt.Errorf("discard is %v (want old): DiscardNew would reject the state-advance CAS on a Will's subject", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		return fmt.Errorf("max_msgs_per_subject is %d (want 1): each per-Will subject is a single-record CAS slot the claim transitions depend on", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict a pending Will before it fires",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a Will's CAS slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject the enqueue of a Will the broker already accepted", cfg.MaxMsgSize)
	}
	if cfg.DenyDelete {
		return fmt.Errorf("deny_delete is set: per-WillID subjects make deletion the only removal, so the outbox would grow without bound and every scan would re-walk every Will ever fired")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages on outbox subjects would be parsed as Will records", cfg.SubjectDeleteMarkerTTL)
	}
	// The bundle stamp: this stream carries the account's recovery epoch, which is
	// what makes the ten broker-owned streams one recreation unit rather than ten
	// independently recoverable stores (F-066).
	if err := validateBundleEpochStamp(&cfg, bundleEpoch); err != nil {
		return err
	}
	return nil
}

// mqttWillFireAt is when a delayed Will falls due: its Will Delay Interval measured
// from the CLOSE, capped at the Session Expiry Interval because Spec5 [3.1.3.2.2]
// publishes at the delay or at the session's end, whichever is first.
// (mqttSessionNeverExpire, being the max uint32, never lowers the delay.)
//
// closeObservedAt is the instant the close was observed (mqtt.closeObservedAt).
// ZERO means the Will was still ARMED when it was consumed, so no close was
// separately observed and the consuming route's own action IS the close: now is the
// anchor. That is the only reading of zero this helper makes -- an OWED Will whose
// anchor is zero is an invariant violation and its caller classifies it BEFORE
// calling here, because the two cases are indistinguishable to this function.
//
// The expiry is passed as a value, deliberately: every caller holds c.mu, and
// reaching for the locking mqttSessionExpiry() getter from inside such a section
// would self-deadlock. That is part of this helper's contract, not the caller's
// discretion.
func mqttWillFireAt(will *mqttWill, sessionExpiry uint32, closeObservedAt int64, now time.Time) int64 {
	effDelay := will.props.willDelay
	if sessionExpiry < effDelay {
		effDelay = sessionExpiry
	}
	anchor := now
	if closeObservedAt != 0 {
		anchor = time.Unix(0, closeObservedAt)
	}
	return anchor.Add(time.Duration(effDelay) * time.Second).UnixNano()
}
