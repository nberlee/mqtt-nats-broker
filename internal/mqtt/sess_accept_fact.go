// Permanent per-CONNECT acceptance facts.
//
// An accepted CONNECT records one durable, dated fact of its own acceptance on a
// subject nothing else ever writes: $MQTT.accept.<domainTk><cidHash>.<epoch>.<acceptID>.
// The fact and the session record are committed in ONE JetStream atomic batch on
// $MQTT_sess, so there is no interval in which a session has been accepted and the
// evidence of that acceptance does not yet exist — the split that defeated every
// watermark design before this one.
//
// What the fact replaces is the single-slot delayed-Will cancellation watermark.
// A slot is regressible: a sibling write erases the acceptance an earlier CONNECT
// recorded, and an erased acceptance can never be recovered because nothing else
// ever knew about it. A fact's subject carries its own acceptID, so a fact is
// never overwritten by anything, and the set of acceptances is therefore
// monotone. That is the whole of F-031.
//
// The fact is DATED (At, sampled by the accepting broker when it composes the
// attempt) because a delayed Will must be suppressed only by an acceptance that
// happened before the Will became due: MQTT 5.0 says a reconnect during the Will
// Delay Interval suppresses the Will ([MQTT-3.1.3-9]), and equally that the Will
// MUST be published once the delay has elapsed ([MQTT-3.1.2-8]). A watermark
// carrying no date could only answer the first.
//
// Ordering inside the batch is load-bearing: the fact is staged FIRST and the
// session record carries the commit. The batch's PubAck therefore reports the
// session record's sequence (the reply rides the commit entry), and the fact sorts
// strictly BELOW the session record of the connection that wrote it — so a
// connection can never suppress its own Will. A fact's sequence is never derived
// arithmetically from the ack; it is read back from the stream.
//
// Facts are permanent and never reaped: retiring one needs proof that no delayed
// Will it could still suppress remains, which nothing here has. See
// docs/CONFORMANCE.md for the storage residual that follows.

package mqtt

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nuid"
)

// mqttAcceptFact is one accepted CONNECT. Every field is part of a binding that is
// checked at decode: there is no field here a reader may take on trust.
type mqttAcceptFact struct {
	// ClientID is the EXACT client identifier, not its hash. The subject can only
	// carry the hash, and a hash collision between two clients would otherwise let
	// one client's reconnect suppress another client's delayed Will. Suppression is
	// decided on this field; the hash only routes.
	ClientID string `json:"cid"`
	// SessEpoch is the $MQTT_sess incarnation this acceptance belongs to. A
	// sequence is comparable only within one incarnation, so a fact from another
	// incarnation is not evidence about this one's sequences.
	SessEpoch string `json:"epoch"`
	// AcceptID makes the subject unique, which is what makes the fact permanent:
	// two CONNECTs of the same client in the same incarnation write two subjects,
	// never one slot twice.
	AcceptID string `json:"aid"`
	// At is when the accepting broker composed this attempt, in unix nanoseconds.
	// It is the date the Will-suppression predicate compares against FireAt.
	At int64 `json:"at"`
}

// mqttAcceptSubject returns the subject one acceptance fact is stored on.
func mqttAcceptSubject(domainTk, cidHash, epoch, acceptID string) string {
	return mqttAcceptSubjectPrefix + domainTk + cidHash + "." + epoch + "." + acceptID
}

// mqttAcceptClientEpochFilter matches every acceptance fact for one client hash
// within one incarnation — the exact set a delayed Will of that client, in that
// incarnation, can be suppressed by. Exact on both tokens that carry identity,
// wildcard only on the acceptID that makes each fact its own record.
func mqttAcceptClientEpochFilter(domainTk, cidHash, epoch string) string {
	return mqttAcceptSubjectPrefix + domainTk + cidHash + "." + epoch + ".*"
}

// mqttAcceptIdentTk reports whether s is a nonempty SINGLE subject token. Both
// tokens the fact contributes to its own subject must have this shape, for the
// reason mqttWillIdentTk states: a dotted or wildcard identity still matches its
// own subject by equality while changing the subject's token count, which makes
// the record visible to one filter and invisible to another.
func mqttAcceptIdentTk(s string) bool {
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

// mqttNewAcceptID mints the token that makes an acceptance fact's subject unique.
func mqttNewAcceptID() string { return nuid.Next() }

// validateAcceptFactRecord checks a decoded fact against the subject it was stored
// on and against the incarnation the reader is running in. Every binding is
// checked, because a fact is evidence used to DESTROY a Will: an unchecked binding
// is a way to make one client's acceptance suppress another's Will, or a previous
// incarnation's acceptance suppress this one's.
//
//   - the body's client id must hash to the subject's hash token
//   - the body's epoch must equal the subject's epoch token AND the incarnation the
//     reader adopted at startup
//   - the body's acceptID must equal the subject's acceptID token
//   - At must be present: a zero date would compare below every FireAt and suppress
//     unconditionally, which is precisely the undated behaviour this replaces
//
// A fact that fails any of these is REFUSED (the caller defers the Will rather than
// acting on evidence it cannot interpret). A fact that is entirely well-formed but
// names a DIFFERENT client that merely hashes the same is not refused — it is a
// valid fact about somebody else, and the predicate simply does not qualify it.
func validateAcceptFactRecord(domainTk, currentEpoch, subject string, f *mqttAcceptFact) error {
	if f.ClientID == _EMPTY_ {
		return fmt.Errorf("acceptance fact carries no client id")
	}
	if !mqttValidSessEpoch(f.SessEpoch) {
		return fmt.Errorf("acceptance fact epoch %q is not well-formed", f.SessEpoch)
	}
	if !mqttAcceptIdentTk(f.AcceptID) {
		return fmt.Errorf("acceptance id %q is not a single subject token", f.AcceptID)
	}
	if f.At == 0 {
		return fmt.Errorf("acceptance fact carries no acceptance time")
	}
	if currentEpoch == _EMPTY_ {
		return fmt.Errorf("this broker has adopted no session-stream incarnation epoch")
	}
	if f.SessEpoch != currentEpoch {
		return fmt.Errorf("acceptance fact epoch %q is not this broker's incarnation %q", f.SessEpoch, currentEpoch)
	}
	if want := mqttAcceptSubject(domainTk, getHash(f.ClientID), f.SessEpoch, f.AcceptID); subject != want {
		return fmt.Errorf("acceptance fact identities name subject %q, but it is stored on %q", want, subject)
	}
	return nil
}

// mqttDecodeAcceptFact decodes and validates one scanned acceptance fact.
func mqttDecodeAcceptFact(domainTk, currentEpoch string, smsg *StoredMsg) (*mqttAcceptFact, error) {
	f := &mqttAcceptFact{}
	if err := json.Unmarshal(smsg.Data, f); err != nil {
		return nil, err
	}
	if err := validateAcceptFactRecord(domainTk, currentEpoch, smsg.Subject, f); err != nil {
		return nil, err
	}
	return f, nil
}

// mqttAcceptFactQualifies is the suppression predicate, stated in one place and
// applied to every fact the walk returns. The filter and the walk's start sequence
// are narrowings that must never change this verdict, so both epoch terms are
// re-tested here rather than assumed from the filter.
//
//   - the fact must belong to the incarnation this broker is running in; a fact
//     from another incarnation says nothing comparable about this one
//   - the fact's client must be EXACTLY the Will's client; equal hashes are not
//     equal clients
//   - the acceptance must predate the Will becoming due. A reconnect during the
//     delay suppresses ([MQTT-3.1.3-9]); a reconnect after the delay has elapsed
//     does not, because the Will was already owed ([MQTT-3.1.2-8])
//   - within the Will's own incarnation the acceptance must be strictly newer than
//     the connection that created the Will, or the connection would suppress its
//     own Will. Across incarnations that comparison is meaningless and a
//     current-incarnation acceptance supersedes outright
func mqttAcceptFactQualifies(f *mqttAcceptFact, factSeq uint64, currentEpoch string, rec *mqttWillOutbox) bool {
	if f.SessEpoch != currentEpoch {
		return false
	}
	if f.ClientID != rec.ClientID {
		return false
	}
	if f.At >= rec.FireAt {
		return false
	}
	if rec.OriginSessEpoch != currentEpoch {
		return true
	}
	return factSeq > rec.OriginSessionSeq
}

// mqttFactVerdict is the outcome of a bounded acceptance-fact scan.
type mqttFactVerdict int

const (
	mqttFactNone      mqttFactVerdict = iota // no qualifying acceptance within the bound
	mqttFactQualified                        // an acceptance that suppresses this Will
	mqttFactDefer                            // undecidable for THIS Will: act not at all
	mqttFactAbort                            // transient/global failure: stop the whole pass
)

// mqttFactScan is the control a bounded scan runs under. It is separate from the
// scan itself because the two things that bound it — the claim the scanner holds
// and the shutdown it must not outlive — belong to the caller, not to the facts.
type mqttFactScan struct {
	// from is the persisted scan floor: a previous claimant already examined and
	// rejected every fact at or below it, so this scan resumes above it. Zero means
	// nothing has been verified yet.
	from uint64
	// verified is the highest sequence THIS scan has examined and rejected. The
	// renewal persists it, which is what lets a budgeted scan finish an unbounded
	// history instead of restarting at the same place forever.
	verified uint64
	// upTo is the $MQTT_sess last sequence this walk reads through, and the caller
	// persists it with whatever decision the walk licenses. Nothing above it is
	// read, so the decision is reproducible by a later reader holding the same
	// cutoff. The caller may CHASE it — re-sample and re-enter with a higher upTo
	// and a floor at the previous one — because a later cutoff can only add
	// evidence: `At < FireAt` is what dates eligibility, not this sequence.
	upTo uint64
	// deadline bounds the whole scan. A scan that reaches it licenses nothing: the
	// claimant defers and lease expiry re-drives. Checked before EVERY fact load,
	// and again by the caller immediately before the licensing write — a renewed
	// lease is not permission to act once the budget is gone.
	deadline time.Time
	// renew is called on cadence while scanning, and its failure ends the scan
	// with a defer. An unconfirmed renewal is not proof that ownership moved (it
	// may be transient, or a successful renewal whose ack was lost), so the safe
	// rule abandons on either rather than trying to tell them apart.
	renew func() error
	// renewEvery is the cadence at which renew is called.
	renewEvery time.Duration
	// nextRenew is when the next renewal is due. It lives on the scan rather than
	// inside it because the caller may re-enter scanAcceptFacts several times while
	// chasing the cutoff, and a deadline re-armed per entry measures the wrong thing:
	// several delta scans can each finish inside one cadence while collectively
	// outlasting the lease, so the claimant would renew NEVER and be reclaimed
	// underneath itself mid-decision. The cadence bounds the whole claim, not one pass.
	//
	// A zero value renews at the first opportunity, so a construction site that forgets
	// to arm it renews too often rather than too late.
	nextRenew time.Time
	// stop, when it selects, ends the scan with a defer: shutdown must not be
	// outlived by a claimant still deciding a Will's fate.
	stop []<-chan struct{}
}

// stopped reports whether any of the scan's stop signals has fired.
func (sc *mqttFactScan) stopped() bool {
	for _, ch := range sc.stop {
		select {
		case <-ch:
			return true
		default:
		}
	}
	return false
}

// scanAcceptFacts walks this Will's acceptance facts in [start, upTo] and reports
// whether one of them suppresses it.
//
// start is the Will's own origin sequence + 1 when the Will belongs to the
// incarnation this broker is running in, and the beginning of the stream otherwise:
// across incarnations the origin sequence names a record on a stream that no longer
// exists, so using it as a lower bound would skip real evidence. Never
// OriginSessionSeq + 1 unconditionally — that was the mutation T11 pairs with.
func (as *mqttAccountSessionManager) scanAcceptFacts(rec *mqttWillOutbox, sc *mqttFactScan) mqttFactVerdict {
	epoch := as.sessStreamEpoch
	if epoch == _EMPTY_ {
		return mqttFactDefer
	}
	filter := mqttAcceptClientEpochFilter(as.domainTk, rec.ClientIDHash, epoch)
	start := uint64(1)
	if rec.OriginSessEpoch == epoch {
		start = rec.OriginSessionSeq + 1
	}
	// Resume above whatever a previous claimant already rejected. Sound because a
	// rejection is permanent: this record's predicate inputs are immutable and a fact
	// is never rewritten or removed.
	if sc.from+1 > start {
		start = sc.from + 1
	}
	for seq := start; seq <= sc.upTo; {
		if sc.stopped() || !time.Now().Before(sc.deadline) {
			return mqttFactDefer
		}
		if sc.renew != nil && !time.Now().Before(sc.nextRenew) {
			if err := sc.renew(); err != nil {
				return mqttFactDefer
			}
			sc.nextRenew = time.Now().Add(sc.renewEvery)
			// A renewal is itself a JetStream request that runs to its own timeout, so
			// shutdown or an exhausted budget can arrive DURING it. Re-checked here for
			// the same reason as after a fact load below: otherwise the renewal buys one
			// more read past the bound, at a whole request timeout apiece.
			if sc.stopped() || !time.Now().Before(sc.deadline) {
				return mqttFactDefer
			}
		}
		smsg, err := as.loadNextAcceptFact(filter, seq)
		// Re-checked AFTER the read, not only before it. A JetStream request runs to
		// its own timeout and is not cancelable from here, so shutdown or an exhausted
		// caller budget can both arrive WHILE one is in flight — and a qualifying
		// answer arriving then would otherwise license a durable cancel on a broker
		// that is shutting down, or blow through a caller's bound by a whole request
		// timeout. The cheapest honest bound is to refuse to act on the answer.
		if sc.stopped() || !time.Now().Before(sc.deadline) {
			return mqttFactDefer
		}
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return mqttFactAbort // transient/global: one timeout per pass, not per record
			}
			return mqttFactNone // no more facts in range
		}
		if smsg.Sequence > sc.upTo {
			return mqttFactNone // beyond the cutoff this decision is taken under
		}
		seq = smsg.Sequence + 1
		f, derr := mqttDecodeAcceptFact(as.domainTk, epoch, smsg)
		if derr != nil {
			as.refuseAcceptFact(smsg.Subject, derr)
			// Undecidable for THIS Will: a fact we cannot interpret may be the
			// acceptance that suppresses it, so nothing may be licensed either way.
			return mqttFactDefer
		}
		if mqttAcceptFactQualifies(f, smsg.Sequence, epoch, rec) {
			return mqttFactQualified
		}
		// Examined and rejected: the renewal may persist this as the new floor.
		sc.verified = smsg.Sequence
	}
	return mqttFactNone
}

// loadNextAcceptFact reads the next acceptance fact at or after seq, honoring the
// instance-scoped test seam. The seam is what lets a test prove a claimant reused a
// persisted decision INSTEAD of rescanning: "it published" is also what a rescan
// that happened to find nothing produces, so the discriminating assertion is that
// no fact was loaded at all.
func (as *mqttAccountSessionManager) loadNextAcceptFact(filter string, seq uint64) (*StoredMsg, error) {
	as.mu.RLock()
	hook := as.testFactLoad
	as.mu.RUnlock()
	if hook != nil {
		return hook(filter, seq)
	}
	return as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
}

// sessLastSeq samples the $MQTT_sess last sequence — the cutoff a decision is taken
// under and persisted with. Sampled BEFORE the scan, so the scan reads a fixed
// range, and recorded with the decision, so a recovering claimant reproduces it
// rather than taking a later one.
func (as *mqttAccountSessionManager) sessLastSeq() (uint64, error) {
	as.mu.RLock()
	hook := as.testSessLastSeq
	as.mu.RUnlock()
	if hook != nil {
		return hook()
	}
	si, err := as.jsa.lookupStream(mqttSessStreamName)
	if err != nil {
		return 0, err
	}
	if si == nil {
		return 0, fmt.Errorf("stream %q not found", mqttSessStreamName)
	}
	return si.State.LastSeq, nil
}

// refuseAcceptFact reports an acceptance fact this broker cannot interpret, once
// per subject per account-manager lifetime. Nothing reclaims such a record: it is
// never rewritten (its subject is unique) and never deleted (facts are permanent),
// so every scan of that client meets it again and defers the Will each time. The
// operator remedy is the only exit, and the message says so.
func (as *mqttAccountSessionManager) refuseAcceptFact(subject string, err error) {
	as.mu.Lock()
	_, warned := as.refusedAcceptFacts[subject]
	if !warned {
		if as.refusedAcceptFacts == nil {
			as.refusedAcceptFacts = map[string]struct{}{}
		}
		as.refusedAcceptFacts[subject] = struct{}{}
	}
	as.mu.Unlock()
	s := as.jsa.c.srv
	if s == nil {
		return
	}
	const format = "MQTT: refusing unsupported CONNECT acceptance fact on %q: %v. Nothing reclaims it, and every delayed Will of that client now DEFERS instead of firing or being suppressed; delete that message from the %s stream PROMPTLY"
	if warned {
		s.Debugf(format, subject, err, mqttSessStreamName)
		return
	}
	s.Warnf(format, subject, err, mqttSessStreamName)
}

// mqttSessSubject returns the per-client session-record subject.
func mqttSessSubject(domainTk, cidHash string) string {
	return mqttSessStreamSubjectPrefix + domainTk + cidHash
}

// mqttComposeAcceptBatch frames the two messages of an accepted CONNECT's atomic
// batch: the acceptance fact staged first under a create-only CAS, then the session
// record at its own base sequence carrying the commit.
//
// The batch id is FRESH on every call, including a re-attempt of the same semantic
// write. Reusing one while a batch is still staged on the server leaves a gap in the
// staged sequence and abandons the batch, so byte-identical re-sending — the thing
// that settles an ordinary ambiguous write — is exactly wrong here. What must stay
// stable across attempts is the SEMANTIC attempt: both bodies, both CAS bases, the
// acceptID and the acceptance time. Only the framing is regenerated.
// It returns the id it minted, because the acknowledgement has to be bound to THIS
// batch: "some batch of two committed" is not the same claim as "the batch carrying
// these two bodies committed", and it is the second one the caller adopts a sequence
// from.
func mqttComposeAcceptBatch(factSubj string, factBody []byte, domainTk, cidHash string,
	baseSeq uint64, recBody []byte) (mqttBatchMsg, mqttBatchMsg, string) {

	batchID := nuid.Next()
	fb, fh := mqttFrameBatchWrite(0, factBody, batchID, 1, false)
	rb, rh := mqttFrameBatchWrite(baseSeq, recBody, batchID, 2, true)
	return mqttBatchMsg{subj: factSubj, hdr: fh, msg: fb},
		mqttBatchMsg{subj: mqttSessSubject(domainTk, cidHash), hdr: rh, msg: rb},
		batchID
}

// mqttAcceptFactReadback is what a read of an attempt's OWN acceptance-fact subject
// says about whether that attempt's batch ever committed.
//
// Unlike a session-record readback, this one is monotone: the subject carries a
// fresh acceptID per composed attempt, so nothing but that attempt ever writes it
// and nothing ever rewrites or removes it. "Absent" therefore means the batch has
// not committed, and "present" means it has — permanently, whatever the session
// record looks like now.
type mqttAcceptFactReadback uint8

const (
	// mqttAcceptFactUnreadable: the read itself failed. Never a verdict.
	mqttAcceptFactUnreadable mqttAcceptFactReadback = iota
	// mqttAcceptFactAbsent: the batch has not committed.
	mqttAcceptFactAbsent
	// mqttAcceptFactLanded: these are the attempt's exact fact bytes, so the batch
	// committed and the session record committed with it.
	mqttAcceptFactLanded
	// mqttAcceptFactCorrupt: something is on the attempt's own unique subject and it
	// is not what the attempt wrote. Nothing may be concluded from it: only this
	// attempt ever names that subject, so a foreign body there is an administrative
	// write or a store fault, and both are refusals rather than facts.
	mqttAcceptFactCorrupt
)

// readAcceptFact reads an attempt's acceptance fact and classifies it by CONTENT,
// never by presence alone.
func (jsa *mqttJSA) readAcceptFact(subject string, want [32]byte) mqttAcceptFactReadback {
	smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return mqttAcceptFactUnreadable
		}
		return mqttAcceptFactAbsent
	}
	if smsg == nil {
		return mqttAcceptFactAbsent
	}
	if mqttRecordDigest(smsg.Data) != want {
		return mqttAcceptFactCorrupt
	}
	return mqttAcceptFactLanded
}
