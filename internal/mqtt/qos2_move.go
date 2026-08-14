// Recoverable inbound QoS 2 move protocol.
//
// A QoS 2 PUBLISH is first HELD on $MQTT_qos2in under a per-(client, packet
// identifier) subject; PUBREL then MOVES it onto $MQTT_msgs for onward delivery.
// The move used to delete the hold record BEFORE the onward store, so three
// windows lost the message outright: a delete whose response was lost, a delete
// followed by a failed store, and a malformed record deleted before its parse
// failed. Reversing the order alone only trades loss for unbounded duplication.
//
// So the hold record is a two-state machine instead, modelled on the delayed-Will
// outbox: a create-only CAS writes `held`, and a CAS at the held record's exact
// sequence replaces it with a terminal `delivered` tombstone AFTER the onward
// store returned success. A PUBREL that finds `delivered` answers PUBCOMP 0x00
// without re-delivering, for as long as the tombstone exists — indefinitely, and
// independent of any deduplication window. A stable Nats-Msg-Id on the onward
// store narrows the remaining window in which a retry can duplicate the stored
// copy, but correctness does not rest on it.
//
// # The guarantee
//
// No silent loss on any PUBREL path. Once the terminal state commits, suppression
// of re-delivery is indefinite and independent of the deduplication window, the
// broker's lifetime, and the nats-server's. Across the *unresolved* interval
// between the onward store and that commit, the durable (QoS 1/2 subscriber) path
// is exactly-once only while deduplication survives; otherwise it is
// at-least-once. The live core-NATS fan-out is at-least-once in that same
// interval and cannot be made otherwise.
//
// Do not restate this as "exactly-once across nats-server restart". It is not.
//
// # Design invariants
//
// Five scoped rules govern every mutation here. Every other comment in this
// change must use these scopes rather than a broader paraphrase.
//
//  1. A CAS or replacement mutation may target only a record whose immutable
//     generation matches. On the HOLD RECORD this is finalizeQoS2Move alone. (The
//     release claim in qos2_release_claim.go is a different register on a sibling
//     subject, and carries its own scope rules.)
//  2. Cleanup may delete only the exact sequence loaded. Safe without a
//     generation check, because deleting an exact sequence cannot reach a newer
//     one — and required without one, since a malformed hold record may carry no
//     parseable generation at all. This is releaseQoS2Record, which therefore
//     takes no generation argument.
//  3. Nonterminal cleanup must be confirmed before PUBCOMP. The corrupt, malformed
//     and expired branches each leave a nonterminal record behind on failure
//     (`held` or `corrupt`), which would block or misclassify reuse of the packet
//     identifier.
//  4. Terminal tombstone cleanup may be asynchronous. The terminal delete may stay
//     a sendq push, because a surviving tombstone is inert: a later PUBREL answers
//     from it and a later PUBLISH reaps it.
//  5. Nothing may fan out or store onward before the release CLAIM commits and the
//     hold record is then PROVEN unchanged. The claim is what a session discard
//     competes for; the proof is what stops a releaser stalled since before a
//     peer's completion from delivering twice. Neither substitutes for the other,
//     and both live in qos2_release_claim.go.
//
// Why identity is a generation ID and not a stream sequence: mqttSession.clear()
// purges hold records by client hash, while the hold subject is keyed by client ID
// and packet identifier — so one subject is reused across session generations. A
// finalizer that re-read after a failed CAS and retried against the newly observed
// sequence could therefore CAS a NEWER generation's hold record into a tombstone
// and discard that message. finalizeQoS2Move reconciles by generation first and
// returns mqttQoS2FinalizeSuperseded without mutating anything, exactly as
// finalizeWillPublish abandons on a foreign WillID.
//
// # Residual
//
// Loss is eliminated: the record leaves `held` only after the onward delivery
// returned success, and no nonterminal cleanup is reported to the client until it
// is confirmed (invariant 3; terminal tombstone cleanup stays asynchronous by
// invariant 4). What remains:
//
//  1. At-least-once live fan-out. A failure between processInboundClientMsg and
//     the terminal commit re-runs the fan-out on the PUBREL retry, so QoS 0 MQTT
//     subscribers, plain NATS subscribers and the retained store may see the
//     message twice. "I fanned out" cannot be committed atomically with the
//     fan-out; the window is one JetStream round trip wide and cannot be closed.
//  2. Best-effort suppression of the duplicate STORED copy in that same window.
//     The Nats-Msg-Id window is authoritative across a broker restart and a
//     stream-leader failover (followers populate the dedup map on apply), but not
//     across a nats-server process restart in which the first copy had already
//     been consumed and acked — $MQTT_msgs is interest-retention, so the rebuild
//     cannot see it.
//  3. A delivered exchange whose tombstone was already reaped answers a later
//     PUBREL 0x92 rather than 0x00. Absence is indistinguishable from
//     never-existed.
//  4. A release that has already won its claim may still fan out after the discard
//     completed. The delivery DECISION is now arbitrated against a session discard
//     by a create-only claim taken before the onward store (qos2_release_claim.go),
//     so a discard that observes no claim can never be followed by a delivery. The
//     wire is not ordered by it: a releaser that claimed first is committed, and
//     its live fan-out may land after the discard released the session record. A
//     discard that observes a claim waits briefly, and on the same broker an
//     in-process fence closes the window outright.
//  5. A superseded PUBREL is answered 0x92 after its message was in fact
//     delivered. Honest about the identifier's state; no loss, and the
//     superseding generation's payload is untouched.

package mqtt

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/nats-io/nuid"
)

const (
	// mqttQoS2GenHeader carries the hold record's immutable generation ID. Stamped
	// once by mqttNewDeliverableMessage at PUBLISH time and never rewritten; it is
	// the exchange's identity for its whole lifetime.
	mqttQoS2GenHeader = "Nmqtt-Q2Gen"

	// mqttQoS2DoneHeader marks the terminal tombstone. Its contract is exactly
	// "1"; anything else is not a tombstone.
	mqttQoS2DoneHeader = "Nmqtt-Q2Done"
	mqttQoS2DoneValue  = "1"

	// mqttQoS2ReleaseMsgIdPrefix namespaces the deduplication key within the
	// $MQTT_msgs stream-wide Nats-Msg-Id space, so a generation ID can never
	// collide with a key some other writer might use.
	mqttQoS2ReleaseMsgIdPrefix = "mqtt-q2-"

	// mqttQoS2GenLen is the length of a nuid.Next() identity.
	mqttQoS2GenLen = 22

	// Bounded retry budgets. Both loops re-read and reconcile on failure rather
	// than blindly retrying, so a small budget is sufficient; exhaustion is
	// reported as a failure (no PUBREC / no PUBCOMP) and the client retries.
	mqttQoS2StoreAttempts    = 3
	mqttQoS2FinalizeAttempts = 3
)

// Fixed upper bounds for the headers this protocol adds, used BOTH by the
// encoders and by the max_payload preflight in mqttProcessPub. The preflight must
// budget the largest form the exchange will ever write: a PUBLISH accepted at
// PUBREC time that a later form then pushes past max_payload cannot make progress
// — the client retries the identical packet forever and its hold record is never
// released. Deriving both from the same constants is what keeps them from
// drifting.
const (
	// mqttQoS2MsgIdHeaderMax bounds "Nats-Msg-Id:<prefix><gen>\r\n" on the RELEASE
	// form.
	mqttQoS2MsgIdHeaderMax = len(JSMsgId) + 1 + len(mqttQoS2ReleaseMsgIdPrefix) + mqttQoS2GenLen + 2

	// mqttQoS2GenHeaderMax bounds "Nmqtt-Q2Gen:<gen>\r\n" on the HOLD form.
	mqttQoS2GenHeaderMax = len(mqttQoS2GenHeader) + 1 + mqttQoS2GenLen + 2

	// mqttQoS2HoldCASHeaderMax bounds the expected-sequence CAS header on the HOLD
	// form. Exact, not an estimate: the hold write is always create-only, so the
	// expectation is always the single digit 0. The header is NOT stripped before
	// storage (removeHeaderIfPrefixPresent runs only on the mirror/source/restore
	// paths), so it counts toward the stored message size.
	mqttQoS2HoldCASHeaderMax = len(JSExpectedLastSubjSeq) + 1 + 1 + 2
)

// The TERMINAL form has no constant here on purpose: its expected-sequence header
// carries the held record's live stream sequence (up to the decimal width of a
// uint64), so its bound is computed by mqttQoS2TerminalRecordMax from the real
// encoder rather than restated as arithmetic. Two independent statements of the
// same size are exactly how a bound and its encoder drift apart.

// mqttQoS2ReleaseMsgId returns the deduplication key for the onward store of the
// hold record identified by gen. Stable across every PUBREL retry of that
// generation by construction, since gen is never rewritten.
func mqttQoS2ReleaseMsgId(gen string) string {
	return mqttQoS2ReleaseMsgIdPrefix + gen
}

// mqttQoS2State classifies a record found on a hold subject.
type mqttQoS2State int

const (
	mqttQoS2Held    mqttQoS2State = iota // a PUBLISH awaiting its PUBREL
	mqttQoS2Done                         // terminal tombstone: the move committed
	mqttQoS2Corrupt                      // unusable: no valid generation, or a malformed done marker
)

func (st mqttQoS2State) String() string {
	switch st {
	case mqttQoS2Held:
		return "held"
	case mqttQoS2Done:
		return "delivered"
	default:
		return "corrupt"
	}
}

// mqttQoS2Record is a classified hold-subject record. gen is meaningful only when
// state is not mqttQoS2Corrupt: a corrupt record has no identity, which is
// precisely why cleanup keys on the loaded sequence and not on a generation.
type mqttQoS2Record struct {
	state  mqttQoS2State
	gen    string
	stored *StoredMsg // the raw record; nil is never returned for a classified record
}

// mqttValidQoS2Gen reports whether s is a well-formed generation ID: exactly 22
// characters from the NUID base-62 alphabet [0-9A-Za-z]. Length alone is
// insufficient, for the same reason mqttValidSessEpoch documents — a same-length
// body of out-of-alphabet bytes would otherwise be admitted into the identity
// space, where it would be compared against real generations and could drive a
// spurious supersede decision. Empty is invalid: it collapses "no identity" into
// the identity space.
func mqttValidQoS2Gen(s string) bool {
	if len(s) != mqttQoS2GenLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// mqttCountHeader returns how many times key appears as a header in hdr. Needed
// because getHeader returns only the FIRST occurrence: a record carrying two
// Nmqtt-Q2Gen headers would otherwise resolve silently to an arbitrary identity.
// The rule this feeds is deliberately count-based, not value-based — a repeated
// identity declaration is a malformed record whether or not the values agree, and
// comparing values invites an implementation that accepts the identical case.
func mqttCountHeader(key string, hdr []byte) int {
	n, off := 0, 0
	for off < len(hdr) {
		idx := getHeaderKeyIndex(key, hdr[off:])
		if idx == -1 {
			return n
		}
		n++
		off += idx + len(key)
	}
	return n
}

// mqttQoS2GenOf extracts and validates the generation ID from a stored record's
// headers, returning _EMPTY_ when the record has no usable identity (missing,
// malformed, or declared more than once).
func mqttQoS2GenOf(hdr []byte) string {
	if mqttCountHeader(mqttQoS2GenHeader, hdr) != 1 {
		return _EMPTY_
	}
	gen := string(getHeader(mqttQoS2GenHeader, hdr))
	if !mqttValidQoS2Gen(gen) {
		return _EMPTY_
	}
	return gen
}

// mqttClassifyQoS2Record classifies a raw hold-subject record.
//
// A record with no valid generation is mqttQoS2Corrupt, never mqttQoS2Held.
// Reading it as `held` would be the worst outcome available: a fresh PUBLISH on
// that packet identifier would be accepted as a duplicate and answered PUBREC
// although its payload was never stored, and the following PUBREL would release
// the OLD record and answer 0x92 — silently losing the fresh payload, which is
// the exact class of bug this protocol exists to eliminate. Treating it as a load
// error is also wrong (it would make the record unreleasable and wedge its packet
// identifier for the session's lifetime), so corrupt is an explicit third state
// with an explicit disposal path.
//
// A done marker without a valid generation is corrupt, not delivered: the
// tombstone would otherwise answer 0x00 for a record it cannot identify.
func mqttClassifyQoS2Record(smsg *StoredMsg) *mqttQoS2Record {
	rec := &mqttQoS2Record{stored: smsg}
	gen := mqttQoS2GenOf(smsg.Header)
	if gen == _EMPTY_ {
		rec.state = mqttQoS2Corrupt
		return rec
	}
	rec.gen = gen
	// Branch on the marker's OCCURRENCE COUNT, never on its value's length. An
	// empty value ("Nmqtt-Q2Done:") and an absent header are indistinguishable by
	// length — getHeader returns an empty slice for the first and nil for the
	// second — so a length test would classify a present-but-malformed marker as
	// `held`. That is the silent-loss path: a fresh PUBLISH on that identifier gets
	// its create CAS rejected, reconciliation sees `held`, and PUBREC is returned
	// although the fresh payload was never stored, while the malformed old record
	// stays behind to be released or delivered by a later PUBREL.
	switch doneCount := mqttCountHeader(mqttQoS2DoneHeader, smsg.Header); {
	case doneCount == 0:
		rec.state = mqttQoS2Held
	case doneCount == 1 && string(getHeader(mqttQoS2DoneHeader, smsg.Header)) == mqttQoS2DoneValue:
		rec.state = mqttQoS2Done
	default:
		// Present but not exactly the contract value, or declared more than once:
		// the record's state is ambiguous and it has no usable identity.
		rec.state = mqttQoS2Corrupt
		rec.gen = _EMPTY_
	}
	return rec
}

// mqttQoS2CorruptDetail renders metadata about a corrupt or malformed record for
// logging. It deliberately does NOT include the record body or headers: the body
// is an application payload, and a record can be as large as max_payload, so
// logging it would both leak user data and produce unbounded log lines whenever a
// fault repeats.
//
// The subject is included rather than the client ID and packet identifier
// separately, because it already contains both in the form an operator would
// actually grep for ("$MQTT.qos2.in.<client-id-hash>.<packet-identifier>") — and it
// carries the HASH, not the raw client ID.
//
// Every field here is metadata about the record, never its content. Anything added
// to this function must keep that property: it is the single place these two log
// lines are composed, so it is also the single place that invariant can be broken.
func mqttQoS2CorruptDetail(subject string, smsg *StoredMsg, reason string) string {
	return fmt.Sprintf("subject %q, seq %d, header %d bytes, body %d bytes, reason: %s",
		subject, smsg.Sequence, len(smsg.Header), len(smsg.Data), reason)
}

// storeQoS2HoldCAS writes an already-encoded held record to subject expecting
// expectSeq as the subject's current last sequence (0 = create-only). It adds ONLY
// the expected-sequence header: the generation was stamped into msg by
// mqttNewDeliverableMessage, and emitting it here too would double the header and
// break every size bound (see the header-ownership note in mqttComputeNatsMsgSize).
func (jsa *mqttJSA) storeQoS2HoldCAS(subject string, expectSeq uint64, hdrLen int, msg []byte) (uint64, error) {
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(expectSeq, 10))
	bb.WriteString(CR_LF)
	// Splice our header line in front of the encoded message's own headers by
	// dropping its hdrLine prefix and keeping the rest verbatim (including its
	// end-of-header CRLF), so the body is untouched.
	bb.Write(msg[len(hdrLine):hdrLen])
	newHdrLen := bb.Len()
	bb.Write(msg[hdrLen:])
	resp, err := jsa.storeMsg(subject, newHdrLen, bb.Bytes())
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// buildQoS2DoneRecord encodes the terminal tombstone: an empty body carrying the
// generation forward plus the done marker, CAS-guarded at expectSeq. It is the
// sole writer of the tombstone's headers, and is shared with
// mqttQoS2TerminalRecordMax so the encoder and its size bound cannot drift.
func buildQoS2DoneRecord(gen string, expectSeq uint64) (msg []byte, hdrLen int) {
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(expectSeq, 10))
	bb.WriteString(CR_LF)
	bb.WriteString(mqttQoS2GenHeader)
	bb.WriteString(":")
	bb.WriteString(gen)
	bb.WriteString(CR_LF)
	bb.WriteString(mqttQoS2DoneHeader)
	bb.WriteString(":")
	bb.WriteString(mqttQoS2DoneValue)
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	return bb.Bytes(), bb.Len()
}

// mqttQoS2TerminalRecordMax is the exact upper bound on the terminal tombstone's
// size. Its body is empty, so — unlike the hold and release forms — it does not
// depend on the PUBLISH at all: for a small payload with a short topic it can be
// the LARGEST of the three forms. It must be budgeted by the max_payload preflight
// because its failure mode is the worst available: the onward store succeeds, the
// terminal CAS is rejected for size, and every retry repeats the onward store
// before failing identically, forever.
//
// Computed from the real encoder at the maximal uint64 expectation, so the bound
// holds whatever sequence the held record turns out to occupy. Memoized because
// the max_payload preflight consults it on every QoS 2 PUBLISH and the value is a
// constant — every generation ID has the same length, so the sample one used to
// build it does not affect the result.
var mqttQoS2TerminalRecordMax = sync.OnceValue(func() int {
	msg, _ := buildQoS2DoneRecord(nuid.Next(), math.MaxUint64)
	return len(msg)
})

// storeQoS2DoneCAS commits the terminal tombstone at the held record's exact
// sequence.
func (jsa *mqttJSA) storeQoS2DoneCAS(subject, gen string, expectSeq uint64) (uint64, error) {
	msg, hdrLen := buildQoS2DoneRecord(gen, expectSeq)
	resp, err := jsa.storeMsg(subject, hdrLen, msg)
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// loadQoS2Record loads and classifies the current record on a hold subject.
// (nil, 0, nil) means the subject is empty; a non-nil error means the read was
// indeterminate and must never be guessed into an absence.
func (jsa *mqttJSA) loadQoS2Record(subject string) (*mqttQoS2Record, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, err
		}
		return nil, 0, nil
	}
	if smsg == nil {
		return nil, 0, nil
	}
	return mqttClassifyQoS2Record(smsg), smsg.Sequence, nil
}

// isQoS2CASRejection reports whether err is a rejected expected-last-subject-
// sequence CAS, i.e. "the subject is not at the sequence I expected". 10071 comes
// from the single-writer check, 10164 from the clustered inflight-contention
// branches; 10164 in particular means transient contention, NOT "occupied", so
// every rejection must be resolved by re-reading rather than classified directly
// into a decision.
//
// This is a structural code match, which is why the move protocol needs no
// equivalent of the brittle 10077 + ErrMaxMsgsPerSubject cause-text comparison
// the old duplicate-PUBLISH detection depended on.
func isQoS2CASRejection(err error) bool {
	return IsNatsErr(err, JSStreamWrongLastSequenceErrF) ||
		IsNatsErr(err, JSStreamWrongLastSequenceConstantErr)
}

// mqttQoS2FinalizeStatus is the outcome of the terminal commit. It is an enum and
// not a boolean because a superseding generation (must not be retried, must not be
// mutated, and the client must be told its identifier is gone) is a different
// thing from a retryable backend failure (the client must NOT be answered at all).
type mqttQoS2FinalizeStatus int

const (
	mqttQoS2FinalizeCommitted     mqttQoS2FinalizeStatus = iota // terminal state is durable
	mqttQoS2FinalizeGone                                        // our generation is already absent
	mqttQoS2FinalizeSuperseded                                  // a foreign generation holds the subject; nothing mutated
	mqttQoS2FinalizeIndeterminate                               // backend failure; the returned error is non-nil
)

func (st mqttQoS2FinalizeStatus) String() string {
	switch st {
	case mqttQoS2FinalizeCommitted:
		return "committed"
	case mqttQoS2FinalizeGone:
		return "gone"
	case mqttQoS2FinalizeSuperseded:
		return "superseded"
	default:
		return "indeterminate"
	}
}

// storeQoS2FinalizeCAS performs the terminal held->delivered CAS, honoring an
// instance-scoped test seam. Only the finalize path is routed through here so an
// injected failure never perturbs the hold store or unrelated loads.
func (as *mqttAccountSessionManager) storeQoS2FinalizeCAS(subject, gen string, expectSeq uint64) (uint64, error) {
	as.mu.RLock()
	hook := as.testQoS2FinalizeStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, gen, expectSeq)
	}
	return as.jsa.storeQoS2DoneCAS(subject, gen, expectSeq)
}

// storeQoS2HoldCAS performs the create-only hold CAS, honoring an instance-scoped
// test seam. Only the PUBLISH-side store is routed through here.
func (as *mqttAccountSessionManager) storeQoS2HoldCAS(subject string, expectSeq uint64, hdrLen int, msg []byte) (uint64, error) {
	as.mu.RLock()
	hook := as.testQoS2HoldStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, hdrLen, msg)
	}
	return as.jsa.storeQoS2HoldCAS(subject, expectSeq, hdrLen, msg)
}

// loadQoS2Record loads and classifies a hold-subject record, honoring an
// instance-scoped test seam.
func (as *mqttAccountSessionManager) loadQoS2Record(subject string) (*mqttQoS2Record, uint64, error) {
	as.mu.RLock()
	hook := as.testQoS2Load
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject)
	}
	return as.jsa.loadQoS2Record(subject)
}

// finalizeQoS2Move commits the held->delivered transition for the generation the
// caller loaded, at the sequence it loaded. Called ONLY after the onward store
// returned success.
//
// This function is the sole place a generation is compared (invariant 1). On a
// rejected CAS it re-reads to learn the durable truth and reconciles by generation
// FIRST, exactly as finalizeWillPublish abandons on a foreign WillID:
//
//   - absent                        -> Gone (our record was reaped; the onward
//     store already succeeded, so the exchange is complete)
//   - a different (or no) generation -> Superseded: warn and mutate NOTHING. A
//     newer session generation may own this subject, and CASing its record would
//     replace an undelivered payload with our tombstone.
//   - our generation, already done   -> Committed at the re-read sequence
//   - our generation, still held     -> retry the CAS at the re-read sequence
//
// Attempts exhausted, or a load we cannot resolve, is Indeterminate with a non-nil
// error: the caller must then suppress PUBCOMP so the client retries.
func (as *mqttAccountSessionManager) finalizeQoS2Move(s *Server, subject, gen string, seq uint64) (mqttQoS2FinalizeStatus, uint64, error) {
	status, newSeq, err := as.finalizeQoS2MoveInner(s, subject, gen, seq)
	// Report the resolved status to any observer (tests only). This is the only way
	// to assert what a finalize DECIDED when its client is not around to be
	// answered — a cross-broker takeover, for instance, closes the connection
	// regardless of how the finalize resolved, so the wire tells a test nothing.
	as.mu.RLock()
	done := as.testQoS2FinalizeDone
	as.mu.RUnlock()
	if done != nil {
		done(subject, gen, status)
	}
	return status, newSeq, err
}

func (as *mqttAccountSessionManager) finalizeQoS2MoveInner(s *Server, subject, gen string, seq uint64) (mqttQoS2FinalizeStatus, uint64, error) {
	// Reach into the load..commit window (tests only): the seam runs once, before
	// the first CAS attempt, so a test can purge or replace the held record exactly
	// where a real race would.
	as.mu.RLock()
	pre := as.testQoS2PreFinalize
	as.mu.RUnlock()
	if pre != nil {
		if err := pre(subject, gen, seq); err != nil {
			return mqttQoS2FinalizeIndeterminate, 0, err
		}
	}
	var lastErr error
	for attempt := 0; attempt < mqttQoS2FinalizeAttempts; attempt++ {
		newSeq, err := as.storeQoS2FinalizeCAS(subject, gen, seq)
		if err == nil {
			return mqttQoS2FinalizeCommitted, newSeq, nil
		}
		lastErr = err
		// The CAS failed transiently, on a stale sequence, or ambiguously (it may
		// have written anyway). A re-read resolves all three.
		cur, curSeq, lerr := as.loadQoS2Record(subject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		if cur == nil {
			return mqttQoS2FinalizeGone, 0, nil
		}
		if cur.gen != gen {
			// Includes a corrupt record, whose gen is _EMPTY_: we cannot prove it is
			// ours, so we must not write over it.
			s.Warnf("MQTT: QoS 2 move finalize for generation %q on %q found a %s record with generation %q; abandoning finalize",
				gen, subject, cur.state, cur.gen)
			return mqttQoS2FinalizeSuperseded, 0, nil
		}
		switch cur.state {
		case mqttQoS2Done:
			return mqttQoS2FinalizeCommitted, curSeq, nil // ours landed (or a peer's did)
		case mqttQoS2Held:
			seq = curSeq // retry the commit at the record's current sequence
		default:
			// Unreachable: a corrupt record has no generation, so the gen check above
			// already returned Superseded.
			return mqttQoS2FinalizeSuperseded, 0, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("terminal commit not confirmed after %d attempts", mqttQoS2FinalizeAttempts)
	}
	return mqttQoS2FinalizeIndeterminate, 0, lastErr
}

// mqttQoS2ReleaseStatus is the outcome of nonterminal cleanup.
type mqttQoS2ReleaseStatus int

const (
	mqttQoS2Released      mqttQoS2ReleaseStatus = iota // deleted, or confirmed already absent
	mqttQoS2ReleaseFailed                              // indeterminate; the record may still be there
)

// releaseQoS2Record deletes the record at exactly seq. It takes NO generation
// (invariant 2): deleting an exact sequence cannot reach a newer record, and a
// corrupt record has no parseable generation to check — yet must still be
// releasable. Generation reconciliation lives in finalizeQoS2Move and nowhere
// else.
//
// The three outcomes matter (invariant 3). Reporting success on an unconfirmed
// delete would free the client's packet identifier while a nonterminal record
// survives, and the next PUBLISH on that identifier would then be misread against
// the stale record — the same silent loss, reintroduced through the error path.
func (as *mqttAccountSessionManager) releaseQoS2Record(seq uint64) (mqttQoS2ReleaseStatus, error) {
	err := as.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, seq, true)
	if err == nil {
		return mqttQoS2Released, nil
	}
	// Confirmed absence is as good as a delete: someone else already removed it
	// (a concurrent PUBREL, a session discard purge, or a TTL drop).
	if !isErrorOtherThan(err, JSSequenceNotFoundErrF) ||
		isJSErrCausedBy(err, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
		return mqttQoS2Released, nil
	}
	return mqttQoS2ReleaseFailed, err
}

// validateQoS2InStream verifies a $MQTT_qos2in stream can actually run the move
// protocol. This is a broker-owned stream with no released compatibility contract
// (docs/ARCHITECTURE.md, "Compatibility boundary"), so the rule is to reject every
// setting that changes the ordinary write, CAS, subject, acknowledgement,
// retention, deletion, or purge semantics this protocol depends on, rather than
// reason about which combinations happen to survive. An unknown-good configuration
// is a startup failure, not something to accommodate.
//
// The failure these checks prevent is the unrecoverable one: onward delivery
// succeeds, the terminal CAS cannot commit, and every PUBREL retry repeats the
// delivery forever.
//
// Scope note: this list is derived from what THIS protocol does and must not be
// copied wholesale to another stream. DenyDelete/DenyPurge are fatal here because
// release, reap, terminal cleanup and the hold purge all remove records; they are
// not fatal to a stream that never deletes.
//
// Re-derived for the <idHash>.<gen>.<PI> subject layout and its .rel claim
// sibling: the field list is unchanged — the subject check is a ">" wildcard
// spanning both layouts, MaxMsgsPer:1 now makes each of the hold and the claim
// its own single-slot register, and the walk filter's paging bound is a server
// constant, not stream config.
//
// wantReplicas is 0 when no explicit replica count is configured (skip that check).
func validateQoS2InStream(si *StreamInfo, wantReplicas int) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"}, wantReplicas); err != nil {
		return err
	}

	// --- State shape: what makes the two-state machine a state machine. ---
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits)", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		// DiscardNew (with DiscardNewPer) was the pre-move configuration: it made the
		// held->delivered CAS a rejected write instead of a replacement.
		return fmt.Errorf("discard is %v (want old)", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		// Load-bearing, and not merely a limit: with more than one message per
		// subject allowed, the held->delivered CAS APPENDS instead of replacing, so
		// the old held record survives underneath. loadLastMsgFor then returns the
		// tombstone (so the delivered branch looks correct), terminal cleanup deletes
		// it, and the stale held record is re-exposed — a later PUBREL re-delivers it
		// and a fresh PUBLISH on that identifier is misclassified as a duplicate.
		return fmt.Errorf("max_msgs_per_subject is %d (want 1)", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict live session state",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a hold or claim CAS slot")
	}

	// --- Protocol prerequisites: settings that leave the shape intact but
	// withdraw something the protocol depends on. ---
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would independently reject the hold, release or terminal record even when the broker max-payload preflight passed", cfg.MaxMsgSize)
	}
	if cfg.DenyDelete {
		return fmt.Errorf("deny_delete is set: it blocks release, reap and terminal cleanup")
	}
	if cfg.DenyPurge {
		// The purge this protects is purgeQoS2Hold's CONFIRMED exact-subject
		// purge — the session discard's and the orphan sweep's only removal of a
		// hold subject. (The discard's blind filter purge is gone; see
		// qos2_release_claim.go.)
		return fmt.Errorf("deny_purge is set: it blocks the confirmed exact-subject hold purge the session discard and the orphan sweep perform")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages on hold subjects would be classified as records", cfg.SubjectDeleteMarkerTTL)
	}
	return nil
}
