// Ambiguous session-record writes.
//
// sess.save() advances sess.seq only on a CONFIRMED response, so a write that
// LANDED with its response lost leaves the in-memory sequence stale. That is not
// self-correcting: a same-process reconnect reuses the very same session object,
// and its CONNECT accept save runs before any recovery, so the stale CAS fails and
// the reconnect is rejected — recovery is never reached.
//
// So an ambiguous failure is recorded rather than merely returned: the session is
// marked UNRESOLVED, together with the identity of the write it attempted — the CAS
// base sequence, a digest of the exact bytes, and those exact bytes themselves.
//
// Settling it is a classification, never "adopt whatever sequence is current", which
// would turn a stale local snapshot into a *valid* CAS and overwrite another broker's
// newer subscriptions, drains, claims or expiry state. And where the record is found
// still at the base sequence, the classification is not a READ at all: a read never
// arbitrates ownership. JetStream returns its contention rejection precisely while a
// write for the subject is IN PROCESS upstream, so a record at the base sequence is
// silent about a proposal that can commit a moment later. What settles that case is a
// CAS — the attempted bytes RE-SENT at the same base sequence, which arbitrates:
//
//	readSeq > baseSeq && digest matches     -> it landed; adopt readSeq
//	moved to anything else                  -> that record is AUTHORITATIVE and not
//	                                           ours: detach and evict, never write
//	readSeq == baseSeq                      -> nothing is known yet: RE-ATTEMPT the
//	                                           exact bytes. Landing them makes the
//	                                           attempt's content durable; a rejected
//	                                           re-attempt is classified by one more
//	                                           read; an ambiguous one keeps the
//	                                           identity for the next pass
//	the record is GONE (base > 0)           -> also not a verdict, and also
//	                                           RE-ATTEMPTED: a clustered apply never
//	                                           re-checks the CAS, so a proposal
//	                                           already accepted can land after a
//	                                           delete. The REJECTION KIND decides —
//	                                           10164 (a write is in process, possibly
//	                                           that proposal) stays unresolved, 10071
//	                                           is foreign FAIL-SAFE. Base 0 keeps the
//	                                           historical foreign classification: a
//	                                           headerless create is never re-attempted
//	the read itself failing                 -> still unresolved; write nothing,
//	                                           delete nothing, retry later
//
// "Our write did not land" is therefore never a verdict. An ambiguity ends as OURS or
// as FOREIGN, or it stays unresolved — so a mutation kept on the strength of "it may
// have landed" is never rolled back, and what a caller registers with it is the
// LANDED continuation alone. See retainOnLanded.
//
// While unresolved, save, clear and the record delete REFUSE AT ENTRY. That is the
// hard guarantee, and it is about teardown as much as about writes: the close path
// would otherwise issue a second stale disconnect save, or run clear() for expiry 0
// — deleting the shared durables and the record before its stale CAS could fail —
// or derive the delayed-Will takeover fence from a stale sequence.
//
// Refusing is not the same as dropping the client's MQTT obligations, though.
// Unresolved is NOT takeover: nobody else has taken responsibility for this
// session's expiry or its Will. So the close path records a DEFERRED DISCONNECT
// ACTION carrying the ORIGINAL deadlines — the disconnect time, the effective
// Session Expiry Interval, and the Will's absolute deadline plus the data it needs
// — and detaches the client without saving or clearing. On resolution the original
// disconnect time is persisted and only the REMAINING duration is armed, or the
// action fires at once if a deadline has already passed. Anything else would let a
// JetStream outage silently extend both the session lifetime and the Will Delay
// Interval.

package mqtt

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// mqttPendingWrite is the identity of an attempted record write whose outcome is
// unknown: the sequence it CAS'd against, a digest of the exact record bytes it
// tried to store, and those bytes as framed for the wire. The sequence alone cannot
// tell our write apart from another broker's, which is the whole point of the
// classification; the framed bytes are what lets the resolver SETTLE the ambiguity
// by re-attempting the very same write rather than by reading and guessing.
//
// The bytes are the attempt's, not memory's, and that is load-bearing: an uncertain
// caller KEPT its mutation and memory may have moved on since, so a re-marshal could
// send content whose digest no longer matches the identity a readback compares
// against — the original landing would then be unrecognizable as ours.
type mqttPendingWrite struct {
	baseSeq uint64
	digest  [sha256.Size]byte
	// framed is the CAS header plus body exactly as sent, with hdr its header
	// length — the two arguments storeSessionMsg takes.
	framed []byte
	hdr    int
	// voidedRegsRev is the void-register revision the attempt MARSHALLED, so a
	// readback that proves those exact bytes durable can credit exactly the
	// verdicts they carried — no more, whatever has been staged since. Every
	// landed write credits the revision it marshalled, wherever the proof comes
	// from; this is that revision travelling with the proof. See
	// voidRegNonceLocked and releaseSettledVoidReservationsLocked.
	voidedRegsRev uint64
	// wid / pwid are the lineage the ATTEMPT stamped, kept for the same reason the
	// bytes are: memory may have moved on, so a resolution that proves these bytes
	// durable must adopt the identity THEY carry and not whatever the session holds
	// by then. See sess_fork_lineage.go.
	wid, pwid string
	// factSubj / factBody / factDigest identify the permanent acceptance fact this
	// attempt committed atomically with the record; factSubj is empty on every
	// attempt that is not an accepted CONNECT's.
	//
	// They are what makes the ambiguity ANSWERABLE rather than merely arbitrable. A
	// session record can be overwritten, deleted and re-created, so "is our record
	// there?" is a question about the present. The fact cannot: its subject is
	// unique to this attempt, so its presence is a permanent, monotone answer to
	// "did this acceptance ever commit?" — which is the question the paired
	// classification asks first.
	//
	// factBody is kept unframed because a re-attempt must regenerate the batch
	// framing: a batch id may never be reused while a batch is staged, so
	// "verbatim" here means the semantic attempt (both bodies, both CAS bases),
	// never the bytes that carried it. See classifyAcceptFactReadback.
	factSubj   string
	factBody   []byte
	factDigest [sha256.Size]byte
}

// mqttUncertainResolution is how the resolver classified an ambiguous write.
type mqttUncertainResolution int

const (
	// mqttUncertainUnresolved: this session's durable state is not known, so
	// nothing may be written or deleted and the retry pass tries again. Two ways
	// to get here, and callers treat them identically: NOTHING WAS SETTLED — a read
	// that failed, a re-attempt whose own outcome is unknown, or a re-attempt
	// rejected while the subject is still in contention — so nothing was written and
	// any deferred action is still parked; or the ambiguity WAS settled and the work
	// it authorized — a landed continuation's write, a discharged deferred
	// disconnect — left the session uncertain AGAIN, so the deferred action was
	// consumed but a NEW attempt is now outstanding. See
	// resolutionAfterContinuations.
	mqttUncertainUnresolved mqttUncertainResolution = iota
	// mqttUncertainOurs: the record is ours, and the attempted write's content is
	// durable — landed as issued, or landed as the resolver's byte-identical
	// re-attempt. sess.seq is now correct and the session may write again.
	mqttUncertainOurs
	// mqttUncertainForeign: an authoritative record exists that is not ours. Detach
	// and evict; every obligation belongs to its owner.
	mqttUncertainForeign
	// mqttUncertainForked: a record exists that provably CLAIMS the parent of the one this
	// broker was acknowledged for, so our write's base was erased by something not
	// demonstrably a successor of ours. Disposed exactly as foreign — the session is not ours to
	// write and the surviving record is authoritative whoever wrote it — and reported
	// differently, because no takeover was demonstrated. See sess_fork_lineage.go.
	mqttUncertainForked
)

func (r mqttUncertainResolution) String() string {
	switch r {
	case mqttUncertainOurs:
		return "ours"
	case mqttUncertainForeign:
		return "foreign"
	case mqttUncertainForked:
		return "forked"
	default:
		return "unresolved"
	}
}

// mqttDeferredDisconnect is the MQTT work a close could not do because the
// session's record state was unresolved. Every deadline in it is ABSOLUTE, so
// resolution arms the remaining time rather than starting the interval again.
type mqttDeferredDisconnect struct {
	// disconnectedAt is the ORIGINAL disconnect time (Unix seconds), persisted on
	// resolution so the expiry deadline is the one the client actually earned.
	disconnectedAt int64
	// expiryInterval is the effective Session Expiry Interval at disconnect: 0
	// means the session ends now (a deferred clear()), mqttSessionNeverExpire means
	// it never expires, anything else is a finite deadline measured from
	// disconnectedAt.
	expiryInterval uint32
	// willDeadline is the absolute Unix time the Will becomes due (0 when there is
	// no Will pending). A reconnect BEFORE it cancels the Will; a reconnect after
	// it does not — the Will is still owed and is published on resolution.
	willDeadline int64
	// willSessionSeq / willSessEpoch are the delayed-Will takeover fence baseline
	// captured at the disconnect, so the enqueue is fenced against the incarnation
	// that actually disconnected rather than whatever sequence resolution finds.
	willSessionSeq uint64
	willSessEpoch  string
	// willGen is the enqueuing incarnation's generation, so a displaced-broker
	// cancel can scope itself. See sess_incarnation.go.
	willGen string
	// will / willProps / willUser are the Will itself, captured at the disconnect
	// because the client is closing and c.mqtt.cp will not be readable later. nil
	// will means there is nothing owed.
	//
	// Publish permission is evaluated at capture time — it is not serializable and
	// cannot be re-checked here — but that evaluation is currently VACUOUS:
	// pubAllowedFullCheck always returns true, so a Will denied by the account ACL
	// IS captured here and is later replayed through the internal client on the
	// service connection, live fan-out included. F-079.
	will      *mqttWill
	willProps []byte
	willUser  string
	// willDelayed distinguishes a Will that must go through the durable outbox (a
	// v5 Will Delay Interval on a surviving session) from one that is published
	// immediately.
	willDelayed bool
}

// mqttRecordDigest is the identity a readback compares against: the digest of the
// record BODY, which is what a load returns (the CAS header is a message header,
// not part of the stored data).
func mqttRecordDigest(body []byte) [sha256.Size]byte {
	return sha256.Sum256(body)
}

// markWriteUncertainLocked records that a record write may or may not have landed.
//
// A DEFINITE CAS rejection is not uncertainty and must not come here: it proves
// the write did not land, so sess.seq is already right and the session stays
// usable. Only an ambiguous failure — a timeout, a lost response, no responders —
// leaves an outcome nobody can name.
//
// It takes the ATTEMPT'S OWN DETAIL rather than a list of its parts, because everything
// pendingWrite holds is something a resolution must adopt from the bytes instead of from
// memory: d.body is the digest identity, d.framed/d.hdr are the same content as it went on
// the wire so the resolver can re-attempt this exact write rather than guessing at its fate,
// d.voidedRegsRev is the void-register revision it marshalled, and d.wid/d.pwid are the lineage it
// stamped; the record bytes the repair retains are d.body itself. Passing the detail is what makes it impossible for
// one of the four call sites to carry a field the other three carry.
//
// sess.mu must be held.
func (sess *mqttSession) markWriteUncertainLocked(d *mqttSessSaveDetail) {
	sess.seqUncertain = true
	sess.pendingWrite = &mqttPendingWrite{
		baseSeq: d.baseSeq, digest: mqttRecordDigest(d.body), framed: d.framed, hdr: d.hdr,
		voidedRegsRev: d.voidedRegsRev, wid: d.wid, pwid: d.pwid,
		factSubj: d.factSubj, factBody: d.factBody,
	}
	if d.factSubj != _EMPTY_ {
		sess.pendingWrite.factDigest = mqttRecordDigest(d.factBody)
	}
}

// isSeqUncertain reports whether this session has an unresolved record write.
func (sess *mqttSession) isSeqUncertain() bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.seqUncertain
}

// mqttIsRecordCASRejection reports whether err is a REJECTED expected-last-subject-
// sequence CAS — the subject was not at the sequence we required, so our write
// definitively did not land.
//
// 10071 comes from the single-writer check and 10164 from the clustered
// inflight-contention branches. Both mean "rejected, nothing stored", which is
// exactly what makes them certain: unlike a timeout, they leave sess.seq correct
// and the session usable. (Where an outcome must be turned into a DECISION rather
// than just an error — the teardown claim — 10164 is still re-read rather than
// read as "somebody else owns this", because it signals contention, not a lost
// race. Here only "did it land?" is being asked, and the answer is no.)
func mqttIsRecordCASRejection(err error) bool {
	return IsNatsErr(err, JSStreamWrongLastSequenceErrF) ||
		IsNatsErr(err, JSStreamWrongLastSequenceConstantErr)
}

// resolveUncertainWrite SETTLES an ambiguous write: it reads the session record
// back, classifies it against the attempted write's identity, RE-ATTEMPTS that
// write's exact bytes when the record is still at its CAS base, and reconciles
// memory to match — ALL INSIDE A RECORD TRANSACTION.
//
// Holding the fence across the whole thing is the point, not an implementation
// detail. Resolution is a record-state transition like any other — it can WRITE —
// and the things it does (decide, arbitrate by re-attempting, clear the uncertainty,
// run the retained continuations) have to be indivisible with respect to other
// writers:
//
//   - observing "no transaction is open" and then resolving is a check-then-act: an
//     off-read-loop finalizer takes the fence in between and its save() publishes
//     seqUncertain before it returns, so the resolver settles an ambiguity whose
//     writer has not yet classified it;
//   - the re-attempt is a record write at the attempt's own base sequence, so
//     another writer interleaving with it would be CAS'ing against a sequence this
//     resolver is about to move;
//   - two resolvers can otherwise both act on one attempt: the first settles it and
//     its continuation advances the record, and the second then reads a record that no
//     longer describes the attempt it asked about.
//
// It never waits for the fence. A caller may already hold a transaction on the same
// goroutine — the SUBSCRIBE packet does — so blocking here would deadlock the
// readLoop against itself. A refusal simply means "not now": the periodic pass and
// the redelivery clocks retry, and nothing is lost because an unresolved session can
// make no progress meanwhile anyway.
//
// The retained continuations receive the resolver's token: they must write under it,
// never open a transaction of their own.
//
// One single-shot pass, never a retry loop: the periodic pass is the retry cadence,
// and a loop would hold the fence across an unbounded number of round trips. The
// pass costs at most three JetStream requests (read, re-attempt, classifying
// re-read), plus whatever the landed continuations do — which is the pre-existing
// landed-arm cost, not a new one.
//
// No lock held on entry. Performs JetStream requests.
func (sess *mqttSession) resolveUncertainWrite() (mqttUncertainResolution, error) {
	if !sess.isSeqUncertain() {
		return mqttUncertainOurs, nil
	}
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		return mqttUncertainUnresolved, errMQTTResolveFenceBusy
	}
	res, err := sess.resolveUncertainWriteInTx(tok)
	if terr := sess.endRecordTx(tok); terr != nil {
		return res, terr
	}
	return res, err
}

// errMQTTResolveFenceBusy covers both refusal causes of the non-blocking acquisition:
// another record transaction is open, or the fence is free but reserved for an
// announced blocking writer under waiter priority (see sess_recordtx.go). The caller's
// arm is the same either way — not now, retry on the periodic pass — which is why one
// error serves both.
var errMQTTResolveFenceBusy = errors.New("the record fence is unavailable (a transaction is open, or a blocking writer is queued for it); resolution deferred")

// resolveUncertainWriteInTx is resolveUncertainWrite's body, with the fence held.
func (sess *mqttSession) resolveUncertainWriteInTx(tok recordTx) (mqttUncertainResolution, error) {
	sess.mu.Lock()
	if !sess.seqUncertain {
		sess.mu.Unlock()
		return mqttUncertainOurs, nil
	}
	pw := sess.pendingWrite
	domainTk, idHash := sess.domainTk, sess.idHash
	// The pair this broker was ACKNOWLEDGED for — not the attempt's, which is what a
	// sibling of our base would be compared against in vain. Sampled in the same
	// critical section as pw, and stable while the session is uncertain because every
	// save is refused at entry until it resolves. See sess_fork_lineage.go.
	ackWID, ackPWID := sess.wid, sess.pwid
	sess.mu.Unlock()
	if pw == nil {
		// Uncertain with no attempted-write identity: nothing can be classified, and
		// guessing is exactly what this exists to prevent.
		return mqttUncertainUnresolved, fmt.Errorf("session %q is uncertain with no recorded write identity", sess.id)
	}

	// An accepted CONNECT's attempt is TWO messages committed indivisibly, so its
	// ambiguity is read as a PAIR — the fact first, because it is the only monotone
	// half. The fact answers "did this batch ever commit?" permanently; the record
	// answers "who owns the session now?", which is a question about the present and
	// says nothing on its own about a batch that committed and was then superseded.
	factV := mqttAcceptFactLanded
	if pw.factSubj != _EMPTY_ {
		factV = sess.jsa.readAcceptFact(pw.factSubj, pw.factDigest)
	}

	smsg, err := sess.jsa.loadSessionMsg(domainTk, idHash)

	// Re-validate the attempt the readback was ABOUT. The load is a network round
	// trip; if the session was resolved and made uncertain again meanwhile, this
	// result describes a different attempt and applying it would be a guess.
	//
	// DEFENSIVE, deliberately. The fence serializes resolvers, so with foreign
	// disposition now ownership-gated, identity-checked and idempotent there is no
	// known state transition this prevents — a second verdict on a stale attempt has
	// nothing left to corrupt. It is kept because the cost is one comparison and the
	// alternative is applying a readback to a question nobody asked; a mutation of it
	// therefore SURVIVES, which is the honest signal that it guards nothing reachable
	// today rather than something untested.
	sess.mu.Lock()
	stillOurs := sess.seqUncertain && sess.pendingWrite == pw
	sess.mu.Unlock()
	if !stillOurs {
		return mqttUncertainOurs, nil
	}

	verdict, seq := classifyAttemptReadback(smsg, err, pw, ackWID, ackPWID)

	if pw.factSubj != _EMPTY_ {
		// TWO READS ARE NOT A SNAPSHOT. The batch can commit between them, and the
		// order of these two reads makes exactly one interleaving misleading: fact
		// absent, then a record that has MOVED. Read naively that is "somebody else
		// owns the session", which would dispose of an acceptance that in fact
		// committed. One forced re-read of the fact settles it, and it is forced only
		// on that pairing — every other pairing is already consistent with both reads
		// taken at either instant.
		if factV == mqttAcceptFactAbsent && (verdict == mqttAttemptForeign || verdict == mqttAttemptForked || verdict == mqttAttemptGone) {
			// Between the two reads, and BEFORE the second: shutdown must not be
			// outlived by another JetStream round trip taken purely to sharpen a
			// verdict nobody will act on. An unresolved session makes no progress
			// either way, and the periodic pass asks again after a restart.
			select {
			case <-sess.jsa.quitCh:
				return mqttUncertainUnresolved,
					fmt.Errorf("resolution of session %q deferred: shutting down between the paired acceptance reads", sess.id)
			default:
			}
			factV = sess.jsa.readAcceptFact(pw.factSubj, pw.factDigest)
		}
		if res, err, done := sess.resolveAcceptPair(tok, pw, factV, verdict, seq, ackWID, ackPWID); done {
			return res, err
		}
	}

	switch verdict {
	case mqttAttemptUnreadable:
		// Nothing is known: write nothing, delete nothing, and let a later pass ask
		// again.
		return mqttUncertainUnresolved, fmt.Errorf("unable to read back the record of session %q: %w", sess.id, err)
	case mqttAttemptOurs:
		// Byte-identical to what we tried to store, at a newer sequence: it landed,
		// and only the response was lost.
		return sess.settleLandedAttempt(tok, pw, seq), nil
	case mqttAttemptForeign:
		// A newer record with different content, or a sequence below our base. Either
		// way it is not the write we attempted, so it belongs to another owner. Do
		// NOT adopt its sequence: doing so would turn our stale snapshot into a valid
		// CAS and overwrite whatever that owner has since persisted.
		return mqttUncertainForeign, nil
	case mqttAttemptForked:
		// A body CLAIMING the parent of the record we were acknowledged for. Everything the
		// foreign arm does still applies — the attempt did not land, and its sequence
		// must not be adopted — but no takeover was demonstrated, so none is claimed.
		//
		// The capsule minted HERE, under the resolver's fence, is OUR side: the body this
		// broker was acknowledged for, against the parent body it was written from. It is a
		// CANDIDATE delta for the reconnect the 0x89 invites, not an account of what the
		// survivor did — nothing here establishes which write displaced our base, or how
		// many landed in between. See sess_fork_repair.go.
		sess.mintForkedGrant(ackWID, ackPWID)
		return mqttUncertainForked, nil
	default: // mqttAttemptAtBase, mqttAttemptGone
		// Neither answer settles anything on its own. At base says nothing about our
		// proposal, because JetStream reports contention while a write for the subject
		// is still in process; and an empty subject does not stop a proposal already
		// accepted into the log. Both are arbitrated by a CAS instead of a guess.
		return sess.reattemptUncertainWrite(tok, pw, domainTk, idHash, ackWID, ackPWID)
	}
}

// resolveAcceptPair applies the paired verdict of an accepted CONNECT's ambiguous
// batch. done==false hands the attempt back to the ordinary single-write arms,
// which is correct exactly where the fact says the batch never committed: nothing
// about a two-message attempt that stored nothing differs from a one-message
// attempt that stored nothing, so the same arbitration — re-attempt at the same
// base, dispose on a moved record — settles it.
//
// The two rows that are NOT handed back are the two the fact alone can decide, and
// the two that are structurally impossible. Refusing the impossible pairings rather
// than picking the more likely of the two reads is the whole reason the fact is
// committed atomically: if the pair can disagree, the guarantee this design rests on
// has failed, and continuing on a guess would turn a broken invariant into a
// destroyed Will or an adopted foreign record.
func (sess *mqttSession) resolveAcceptPair(tok recordTx, pw *mqttPendingWrite,
	factV mqttAcceptFactReadback, verdict mqttAttemptReadback, seq uint64,
	ackWID, ackPWID string) (mqttUncertainResolution, error, bool) {

	switch factV {
	case mqttAcceptFactUnreadable:
		return mqttUncertainUnresolved,
			fmt.Errorf("unable to read back the acceptance fact of session %q on %q", sess.id, pw.factSubj), true
	case mqttAcceptFactCorrupt:
		return mqttUncertainUnresolved,
			fmt.Errorf("the acceptance fact of session %q on %q is not the one this CONNECT wrote; refusing to classify its record write", sess.id, pw.factSubj), true
	case mqttAcceptFactLanded:
		switch verdict {
		case mqttAttemptOurs:
			return sess.settleLandedAttempt(tok, pw, seq), nil, true
		case mqttAttemptForeign:
			// The acceptance committed, and the record has since been replaced. The
			// acceptance stands permanently — that is what the fact is for — but this
			// broker's ownership does not, so the disposal is the ordinary foreign one.
			return mqttUncertainForeign, nil, true
		case mqttAttemptForked:
			sess.mintForkedGrant(ackWID, ackPWID)
			return mqttUncertainForked, nil, true
		case mqttAttemptGone:
			// Committed, then released by a teardown or an expiry. Not ours to resume.
			return mqttUncertainForeign, nil, true
		case mqttAttemptAtBase:
			// Impossible: our batch committed, so the subject advanced past the base it
			// CAS'd against, and stream sequences do not go backwards.
			return mqttUncertainUnresolved,
				fmt.Errorf("session %q: its acceptance fact is durable while its record is still at the write's base sequence %d; refusing to resolve", sess.id, pw.baseSeq), true
		default: // mqttAttemptUnreadable
			return mqttUncertainUnresolved,
				fmt.Errorf("session %q: its acceptance is durable but its record could not be read back", sess.id), true
		}
	default: // mqttAcceptFactAbsent
		if verdict == mqttAttemptOurs {
			// Impossible: the record bytes this attempt composed are durable while the
			// fact committed in the same indivisible batch is not.
			return mqttUncertainUnresolved,
				fmt.Errorf("session %q: its record write is durable while the acceptance fact committed with it is missing from %q; refusing to resolve", sess.id, pw.factSubj), true
		}
		return 0, nil, false
	}
}

// mqttAttemptReadback is what a record read says about the AMBIGUOUS ATTEMPT it was
// taken about, content included.
type mqttAttemptReadback uint8

const (
	// mqttAttemptUnreadable: the read itself is unavailable, so nothing can be
	// classified. Never a verdict about ownership.
	mqttAttemptUnreadable mqttAttemptReadback = iota
	// mqttAttemptForeign: the record is not this attempt's and never can be — the
	// subject has moved past the base sequence with other content, so the attempt's
	// CAS can no longer commit.
	mqttAttemptForeign
	// mqttAttemptGone: the subject is EMPTY. Somebody released this session, and the
	// attempt's CAS can no longer be accepted — but that is not the same as saying an
	// already-accepted proposal of ours cannot still land, so it is not foreign on its
	// own. Only reported for a non-zero base; see classifyAttemptReadback.
	mqttAttemptGone
	// mqttAttemptOurs: these are the attempt's exact bytes, at a sequence past its
	// base. The write landed and only its response was lost.
	mqttAttemptOurs
	// mqttAttemptAtBase: the record is still the one the attempt CAS'd against. That
	// is CONTENTION, not an outcome: the proposal may still be in process upstream.
	mqttAttemptAtBase
	// mqttAttemptForked: the surviving record provably CLAIMS the parent of the one this
	// broker was acknowledged for — same parent, different identity — so the attempt's base
	// is no longer current, and the body that replaced it does not demonstrably descend from
	// it. WHICH write displaced our base is NOT established: a legitimate child could have
	// landed first and been replaced in turn. Everything foreign does happens here too (the
	// attempt did not land and its sequence must not be adopted); what differs is that no
	// takeover is asserted. See sess_fork_lineage.go.
	mqttAttemptForked
)

// classifyAttemptReadback is the whole classification of a record read taken about
// an ambiguous write — the base/gone/moved decision of classifyRecordAgainstAttempt
// plus the CONTENT question only a caller holding the attempted bytes can answer.
//
// One function for BOTH reads the resolver takes (the initial readback and the one
// that follows a rejected re-attempt), so the two cannot drift apart. In particular
// the `Sequence > baseSeq` conjunct is stated once: classifyRecordAgainstAttempt
// calls every non-equal sequence "moved", and stream sequences are monotonic within
// a stream incarnation, so a record BELOW the base cannot be this attempt's landing
// however its bytes compare — a subject deleted and re-created lands higher, and a
// lower sequence means a reset or re-provisioned store. Such a record is classified
// foreign FAIL-SAFE (never adopted), not as a proof of another owner.
//
// The sequence returned is meaningful only for mqttAttemptOurs.
func classifyAttemptReadback(smsg *StoredMsg, lerr error, pw *mqttPendingWrite,
	ackWID, ackPWID string) (mqttAttemptReadback, uint64) {

	switch classifyRecordAgainstAttempt(smsg, lerr, pw.baseSeq) {
	case mqttRecordAttemptUnreadable:
		return mqttAttemptUnreadable, 0
	case mqttRecordAttemptGone:
		// Somebody released this session (a teardown, an expiry) — but an absent
		// subject does NOT prove the attempt is dead. In a clustered stream the
		// expected-sequence check runs only BEFORE the proposal
		// (`canConsistencyCheck := !isClustered`, nats-server v2.14.5
		// server/stream.go:6247), so a proposal already accepted into the log applies
		// whatever happened to the subject meanwhile — including an exact-sequence
		// delete, which only clear() now issues. F-030's conditional-write closure does
		// not reach this:
		// it is about a write conditioned on a superseded base, and a delete carries
		// no expectation at all. Reported as its own verdict so the
		// resolver ARBITRATES rather than disposing on a read.
		//
		// Base 0 reaches this arm like any other base, and that is F-029's doing. The
		// exclusion here existed because a first-ever create carried no header and could
		// therefore never be re-attempted — an unconditional write arbitrates nothing,
		// so the only safe reading of an absent subject was "somebody else's". Now the
		// create CASes at 0 too, so its re-attempt is exactly the arbitration this
		// verdict asks for: acceptance proves our create landed, and rejection falls to
		// the post-rejection classification below. The at-base arm stays unreachable at
		// 0 — stream sequences start at 1 — which is a property of the store, not an
		// assumption of ours. See sess_takeover_redrive.go.
		return mqttAttemptGone, 0
	case mqttRecordAttemptAtBase:
		return mqttAttemptAtBase, 0
	default: // mqttRecordAttemptMoved
		if smsg.Sequence > pw.baseSeq && mqttRecordDigest(smsg.Data) == pw.digest {
			return mqttAttemptOurs, smsg.Sequence
		}
		// Foreign — and the lineage question is asked HERE, on the branch that has
		// already reached that conclusion, and only above the base. A record BELOW it
		// stays foreign whatever its identity says: a lower sequence means a reset or
		// re-provisioned store, and that fail-safe is not the lineage rule's to relax.
		// Decoded lazily, because the two branches above compare raw bytes.
		if smsg.Sequence > pw.baseSeq &&
			classifyRecordLineage(mqttDecodeSessionRecord(smsg), ackWID, ackPWID) == mqttLineageForked {
			return mqttAttemptForked, 0
		}
		return mqttAttemptForeign, 0
	}
}

// reattemptUncertainWrite settles an ambiguity by RE-SENDING the attempt's exact
// bytes at its own CAS base — the only thing that can settle it, because a read of a
// record still at the base sequence is silent about a proposal that may still be in
// process upstream.
//
// It is an arbitration, not a repair. A success proves the attempt's CONTENT durable at
// the sequence it reports; a rejection proves the subject moved, and one further read
// says by whom.
//
// This comment used to argue that the original attempt and this re-attempt — two
// BRIDGE-AUTHORED CONDITIONAL writes off one base — could both commit across a leader
// transition, and that identical bytes plus MaxMsgsPer:1 made that harmless. **That
// argument is RETIRED**, not re-pointed at another producer: the same-base boundary in
// docs/CONFORMANCE.md closes it for exactly this pair. An external unconditional replay
// can put an indistinguishable body on the subject, but it is a different producer and
// is no evidence for the retired claim.
//
// Two properties the retired argument used as its safety net remain load-bearing for
// their OWN reasons, which is why nothing here changes:
//
//   - Exact-byte reissue, because a success or a digest match must credit the ORIGINAL
//     attempted body. Re-marshalling current memory would settle the ambiguity against
//     bytes the attempt never sent.
//   - $MQTT_sess being MaxMsgsPer:1, a validated stream prerequisite the single-slot
//     register depends on generally.
//
// Their coincidence — identical bytes landing twice being observationally benign — is
// kept only as generic defensive depth. It is NOT an argument about the unconditional
// route: that route supplies no second acknowledgement for this broker to adopt, so the
// surviving-sequence reasoning does not describe it.
//
// A FOREIGN write in this window is a different matter and is NOT solved here, and its
// direction is the OPPOSITE of what this comment used to say. An UNCONDITIONAL foreign
// write carries no expectation, so no CAS excludes it. Arriving BEFORE this re-attempt it
// is recorded in the stream's in-flight state and our expected-sequence re-attempt is
// REFUSED (and once it has applied, the moved subject sequence refuses us too). What is
// reachable is the other order: our re-attempt is admitted, and the foreign body lands
// afterwards and replaces it. For two bridge-authored CONDITIONAL writes neither ordering
// arises — the same-base boundary excludes the second. That is the external-write route
// in docs/CONFORMANCE.md, not a reattempt-specific residual, and it is not narrowed by
// anything this arm could do.
//
// Single-shot: the periodic pass is the retry cadence, and holding the record fence
// across a retry loop would starve every other writer on the session.
//
// The resolver's fence is held. sess.mu must not be held.
func (sess *mqttSession) reattemptUncertainWrite(tok recordTx, pw *mqttPendingWrite,
	domainTk, cidHash, ackWID, ackPWID string) (mqttUncertainResolution, error) {

	// Test seam, fired between the at-base classification and the re-attempt: it is
	// what lets a test land the ORIGINAL proposal inside the very window this arm
	// exists for, rather than approximating it with a sleep.
	if h := mqttTestResolveReattemptHookV.Load(); h != nil {
		(*h)(sess)
	}
	resp, err := sess.reattemptPendingStore(domainTk, cidHash, pw)
	if err == nil {
		return sess.settleLandedAttempt(tok, pw, resp.Sequence), nil
	}
	if !mqttIsRecordCASRejection(err) {
		// The re-attempt's own outcome is unknown. The identity stays EXACTLY as it
		// was — same base, same digest, same bytes — because it names whichever of the
		// two proposals lands. (The both-land case is retired for these two writes: they
		// are bridge-authored conditional writes off one base — see the same-base
		// boundary in docs/CONFORMANCE.md.)
		return mqttUncertainUnresolved,
			fmt.Errorf("the re-attempted record write of session %q is itself unresolved: %w", sess.id, err)
	}

	// Definitely rejected: this re-attempt is not the write that will decide, so the
	// record that IS there does.
	smsg, lerr := sess.jsa.loadSessionMsg(domainTk, cidHash)
	verdict, seq := classifyAttemptReadback(smsg, lerr, pw, ackWID, ackPWID)
	if pw.factSubj != _EMPTY_ {
		// Same pairing as the first readback, and needed for the same reason: the
		// ORIGINAL batch can commit while this re-attempt is being rejected, so a
		// record that has moved is not evidence about the acceptance. No forced
		// re-read is required here — this fact read is taken strictly AFTER the
		// session read that it has to be consistent with, which is the order the
		// first readback has to correct for.
		factV := sess.jsa.readAcceptFact(pw.factSubj, pw.factDigest)
		if res, rerr, done := sess.resolveAcceptPair(tok, pw, factV, verdict, seq, ackWID, ackPWID); done {
			return res, rerr
		}
	}
	switch verdict {
	case mqttAttemptOurs:
		// The original landed between the readback and the re-attempt.
		return sess.settleLandedAttempt(tok, pw, seq), nil
	case mqttAttemptForeign:
		return mqttUncertainForeign, nil
	case mqttAttemptForked:
		// A body claiming our acknowledged record's parent, reached through the re-attempt's
		// own rejection rather than the first readback. Same disposition, same silence
		// about a takeover nothing established, and its own mint: this is a SECOND
		// classification of the same session, and a resolver that reached the verdict only
		// here would otherwise leave the repair unrecorded. See sess_fork_repair.go.
		sess.mintForkedGrant(ackWID, ackPWID)
		return mqttUncertainForked, nil
	case mqttAttemptGone:
		// The subject is still empty, and the REJECTION KIND is the only thing that
		// says anything about our original. 10164 means a write for this subject is
		// IN PROCESS upstream — possibly that original, accepted before the delete and
		// still to apply — so nothing may be concluded and certainly nothing disposed.
		// 10071 means no write is in process, which is the best evidence available
		// that the original went with the record; it is a FAIL-SAFE classification,
		// not a proof, because an UNCONDITIONAL foreign write carries no expectation
		// for any check to compare and so is excluded by nothing — see the same-base
		// boundary in docs/CONFORMANCE.md, which closes the competing-CONDITIONAL-write
		// route this used to cite.
		if IsNatsErr(err, JSStreamWrongLastSequenceConstantErr) {
			return mqttUncertainUnresolved,
				fmt.Errorf("the record of session %q is gone while a write for it is in process: %w", sess.id, err)
		}
		return mqttUncertainForeign, nil
	default:
		// At base or unreadable. At base after a rejection means a write for the
		// subject is in process — possibly our own original — which is contention,
		// never a verdict.
		return mqttUncertainUnresolved,
			fmt.Errorf("the re-attempted record write of session %q was rejected and its outcome is not yet classifiable: %w", sess.id, err)
	}
}

// settleLandedAttempt adopts an ambiguous write PROVEN durable at seq, whichever of
// the two proposals carrying its bytes committed.
//
// Adopting the sequence, crediting the revision those bytes marshalled and running
// the landed continuations are one step under the resolver's fence: memory already
// matches the record, but every action a caller withheld for want of proof is now
// authorized. The credit happens here for the same reason a confirmed save credits
// its own — the proof is the durability boundary, not the intent of whoever wrote.
// See creditVoidedRegsLocked.
//
// No lock held on entry.
func (sess *mqttSession) settleLandedAttempt(tok recordTx, pw *mqttPendingWrite, seq uint64) mqttUncertainResolution {
	sess.clearUncertaintyLocked(seq, pw.wid, pw.pwid, pw.framed[pw.hdr:])
	sess.mu.Lock()
	sess.creditVoidedRegsLocked(pw.voidedRegsRev)
	sess.mu.Unlock()
	sess.commitUncertainWrites(tok)
	return sess.resolutionAfterContinuations(mqttUncertainOurs)
}

// mqttTestResolveReattemptHookV fires after an ambiguous write has been classified
// at-base and before its exact bytes are re-attempted. See reattemptUncertainWrite.
var mqttTestResolveReattemptHookV atomic.Pointer[func(sess *mqttSession)]

func setMQTTTestResolveReattemptHook(h func(sess *mqttSession)) func() {
	prev := mqttTestResolveReattemptHookV.Load()
	if h == nil {
		mqttTestResolveReattemptHookV.Store(nil)
	} else {
		mqttTestResolveReattemptHookV.Store(&h)
	}
	return func() { mqttTestResolveReattemptHookV.Store(prev) }
}

// resolutionAfterContinuations downgrades a resolved verdict to UNRESOLVED when the
// continuations this resolution just ran left the session uncertain AGAIN.
//
// A continuation may write — that is the point of handing it the resolver's token —
// and its write can itself come back ambiguous, which re-arms seqUncertain and
// registers a fresh continuation for the NEW attempt. Reporting the original verdict
// then tells the caller the session is writable when its next write will be refused,
// and for CONNECT that is destructive rather than merely stale: it binds, its accept
// save is refused, and mqtt.go's removeSession evicts the only object holding the new
// continuation — so a deferred disconnect's deadline and its void-register verdicts
// are lost with no crash anywhere.
//
// Unresolved is the honest answer and every caller already handles it: nothing is
// discharged, nothing evicted, CONNECT rejects and the client retries, and the
// periodic pass keeps watching a session that is once again in its candidate set.
//
// No lock held on entry.
func (sess *mqttSession) resolutionAfterContinuations(res mqttUncertainResolution) mqttUncertainResolution {
	if res == mqttUncertainOurs && sess.isSeqUncertain() {
		return mqttUncertainUnresolved
	}
	return res
}

// clearUncertainty marks the session resolved at seq, adopting the lineage AND the record bytes the
// proven bytes carry alongside it — the same pairing every other adoption site makes, because
// a sequence without its identity leaves the next save parenting itself to a record that is no
// longer the base, and an identity without its bytes leaves the next fork's delta measured
// against a record two writes back. See sess_fork_lineage.go and sess_fork_repair.go.
func (sess *mqttSession) clearUncertaintyLocked(seq uint64, wid, pwid string, body []byte) {
	sess.mu.Lock()
	sess.seqUncertain = false
	sess.pendingWrite = nil
	sess.seq = seq
	sess.wid, sess.pwid = wid, pwid
	sess.adoptGrantBodyLocked(body)
	sess.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Reconciling the state an ambiguous write left behind
// ---------------------------------------------------------------------------

// resolveUncertainNow attempts the readback immediately, for an off-readLoop writer
// whose ambiguous save would otherwise sit unresolved.
//
// The periodic pass does handle bound sessions, but only on its own schedule, so an
// ambiguity left for it sits unresolved in the meantime: the drain's claim neither
// restored nor acked, and every further save on the session refused. Resolving at the
// point the ambiguity arises shortens that window to nothing in the common case.
//
// Best effort by design: an unreadable record leaves the session unresolved for the
// periodic pass (or the next resume) to retry, which is the same posture as every
// other reader here. A foreign record is reported so the caller can stop.
//
// sess.mu must not be held.
func (sess *mqttSession) resolveUncertainNow(c *client) mqttUncertainResolution {
	// An UNBOUND session with a PARKED teardown has to resolve and discharge as
	// one step. Clearing the uncertainty first leaves a window in which a
	// reconnect binds, and that reconnect skips resolution precisely BECAUSE the
	// session is no longer uncertain — so nothing ever discharges the action and
	// it is stranded for good, taking its expiry and its Will with it.
	//
	// The ownership gate is what makes the pair atomic. Failing to take it is not
	// a problem to work around: it means a CONNECT is already in flight, and that
	// CONNECT still sees an uncertain session, so it resolves and discharges
	// through resolveUncertainSession itself. Reporting unresolved and leaving it
	// alone is therefore the correct outcome, not a lost update.
	// gated records that THIS call holds the ownership gate, which changes how a
	// foreign verdict must be disposed below: the wrapper form takes the gate
	// itself and would fail against the one already held here.
	gated := false
	if asm := sess.asm; asm != nil {
		// Claimed as the session's CURRENT owner — the bound client, or nobody for
		// an unbound one — which is the same claim reconcileResidentRecordState makes.
		// Sampling it cannot go stale unnoticed: lockSession re-checks the owner
		// under its own lock and fails if it moved.
		//
		// Taken UNCONDITIONALLY, not only when something is already parked.
		// Whether a teardown needs discharging is not knowable before the
		// resolution: a close holding this same gate can park one (and unbind the
		// session) at any point, so a decision made from a prior sample is a
		// check-then-act. The gate is what serializes this against that close.
		//
		// It never blocks — lockSession fails fast — and a failure means another
		// ownership-gated operation holds it, which for a CONNECT means the
		// session is still uncertain when it runs and it resolves and discharges
		// itself. Reporting unresolved is then the correct outcome.
		sess.mu.Lock()
		owner := sess.c
		sess.mu.Unlock()
		if err := asm.lockSession(sess, owner); err != nil {
			return mqttUncertainUnresolved
		}
		defer asm.unlockSession(sess)
		gated = true
	}
	res, err := sess.resolveUncertainWrite()
	if err != nil && c != nil {
		c.Debugf("MQTT: could not resolve the uncertain record write of session %q: %v", sess.id, err)
	}
	if res == mqttUncertainForeign || res == mqttUncertainForked {
		// Somebody else owns this session now — or, on the forked arm, nobody who can
		// be shown to. The DISPOSITION is identical either way and deliberately so:
		// the surviving record is authoritative, this object may not write, and every
		// protocol the eviction satisfies (the retained continuations, the parked
		// action, residency identity, CONNECT's verdict switch) stays satisfied by the
		// code that already satisfies it. Only the vocabulary differs.
		//
		// Disposition happens under the ownership gate — doing it ungated would race a
		// CONNECT that has already evicted this object and installed a replacement —
		// but this call already holds that gate, so it must NOT go through the
		// wrapper: lockSession fails fast on a gate it finds held, including its own,
		// which would silently skip the disposition and leave a stale client bound to
		// a session another broker owns. See disposeForeignSessionLocked.
		voc := mqttDisposalForForeignResolution(res)
		if gated {
			sess.disposeForeignSessionLocked(sess.asm, c, voc)
		} else {
			sess.disposeForeignSession(c, voc)
		}
		return res
	}
	if res == mqttUncertainOurs {
		// Resolved as ours — which is exactly when a teardown parked by an earlier
		// close becomes discharge-able, and exactly when nothing else will do it:
		// clearing the uncertainty removes this session from the periodic pass, and
		// the next CONNECT skips resolution for the same reason. Only
		// resolveUncertainSession used to discharge one, and only the paths that go
		// through it ever reached that. The gate taken above (when there was
		// anything to discharge) is still held, so no reconnect can have bound
		// since the resolution.
		sess.dischargeDeferredIfUnbound(c)
		// The discharge's own record write can be ambiguous, which leaves this
		// session unresolved again however cleanly the previous attempt classified.
		// Reported for the same reason resolveUncertainSession reports it: a caller
		// told "ours" would act on a session whose next write is refused. This site's
		// production callers ignore the value, so the honest return is what a test
		// discriminates rather than a behavior change here.
		return sess.resolutionAfterContinuations(res)
	}
	return res
}

// dischargeDeferredIfUnbound applies a parked deferred disconnect once its
// session's record state is known again, provided nobody has rebound in the
// meantime — a reconnect owns the decision itself and applies the reconnect
// rules (cancelling or retaining the Will), which is why resolveUncertainSession
// makes the same check.
//
// The caller must already hold the ownership gate for an unbound session with a
// parked action — resolveUncertainNow takes it precisely so that the resolution
// and this discharge cannot be split by a reconnect.
//
// No lock held on entry (other than that gate).
func (sess *mqttSession) dischargeDeferredIfUnbound(c *client) {
	asm := sess.asm
	if asm == nil || c == nil || c.srv == nil {
		return
	}
	sess.mu.Lock()
	bound, parked := sess.c != nil, sess.deferredDisconnect != nil
	sess.mu.Unlock()
	if bound || !parked {
		return
	}
	if action := sess.takeDeferredDisconnect(); action != nil {
		c.srv.applyDeferredDisconnect(asm, sess, action)
	}
}

// disposeForeignSessionLocked is the SINGLE foreign disposition: another owner holds
// this session, so every obligation — the expiry, the Will, the durables — is theirs.
//
// It detaches the stale client, drops the deferred close, evicts the session and
// CLOSES the connection. All four belong together:
//
//   - detaching without closing leaves a client still holding c.mqtt.sess, free to
//     publish, subscribe and ack against an evicted session while another broker is
//     authoritative;
//   - cancelling the expiry timer before the removal is known to apply disarms a
//     replacement session's deadline: the periodic sweep re-arms it eventually, but
//     not before that session has outlived the interval it was promised;
//   - evicting without an identity check deletes by client ID, so a CONNECT that has
//     already installed a restored replacement has that replacement removed instead;
//   - and doing any of it outside the ownership gate races that CONNECT directly.
//
// The caller must hold the session in asm.sessLocked, which is what serializes this
// against a concurrent CONNECT. Only the client this disposition observed is closed,
// so a client that rebound in the meantime is left alone.
//
// The takeover DISCONNECT is sent only to a connection that has actually been
// acknowledged. CONNACK must be the first packet a client sees
// ([MQTT-3.2.0-1]), so a connection whose success CONNACK was never written —
// one refused inside its own CONNECT, which mqttAbortAcceptedConnect leaves
// bound until the readLoop's close bookkeeping detaches it — must be closed
// silently. That is read from the connection itself rather than passed in by
// the caller: a resolver reaching a foreign verdict from the periodic pass has
// no idea which connection it is about to close, and got this wrong.
//
// The state disposal and the notify/close tail are split apart because they belong on
// different goroutines for different callers, NOT because either is optional: every caller
// that disposes state runs the tail. An off-readLoop caller (this one) must not close
// synchronously — it holds the ownership gate, and closeConnection unsubscribes and can
// drain — while the bound settlement on a session's own readLoop must, so that the
// connection is shut before its parser can look at another packet. See
// mqttGateDispatchOnRejection.
//
// No lock held on entry (other than the ownership gate).
func (sess *mqttSession) disposeForeignSessionLocked(asm *mqttAccountSessionManager, c *client,
	voc mqttDisposalVocabulary) {

	stale, disposed := sess.disposeForeignStateLocked(asm, c)
	if !disposed || stale == nil {
		return
	}
	stale.mqttNotifyForeignDisposal(sess.id, voc)
	go stale.closeConnection(voc.closeReason)
}

// disposeForeignStateLocked is the STATE half of the foreign disposition: it detaches the
// stale client, drops the deferred close and evicts the session. It reports the client it
// observed bound and whether the disposal APPLIED, so a caller cannot run the notify/close
// tail against a session it turned out to have no business disposing of.
//
// No lock held on entry (other than the ownership gate).
func (sess *mqttSession) disposeForeignStateLocked(asm *mqttAccountSessionManager, c *client) (*client, bool) {
	// The identity check comes BEFORE any mutation. A stale caller that discarded the
	// deferred close and only then noticed the binding had changed would have
	// destroyed the CURRENT session's obligation — the Will it still owes, the expiry
	// deadline it is still counting — on its way to deciding it had no business here.
	sess.mu.Lock()
	stale := sess.c
	if c != nil && stale != c {
		// Somebody rebound while this disposition was being decided. The gate means
		// that cannot be a CONNECT in flight, but it can be one that completed before
		// we took it — in which case the session is theirs and not ours to tear down.
		sess.mu.Unlock()
		return nil, false
	}
	sess.mu.Unlock()

	sess.takeDeferredDisconnect()
	sess.mu.Lock()
	sess.c = nil
	sess.seqUncertain, sess.pendingWrite = false, nil
	sess.mu.Unlock()

	if asm != nil {
		// Removal and expiry cancellation happen together, under asm.mu, and only if
		// this object is still the resident one: the timer is keyed by client hash, so
		// cancelling it unconditionally would disarm a REPLACEMENT session's deadline.
		asm.removeSessionIfCurrent(sess)
	}
	return stale, true
}

// mqttNotifyForeignDisposal tells a disposed connection why it is about to be closed. The
// close itself is the caller's: it is what differs between the off-readLoop callers and the
// settlement running on this connection's own readLoop.
//
// The notification itself is mqttNotifyTakenOver's, the one tail every displacement path
// shares: the CONNACK gate that used to be checked here lives there now, alongside the
// claim that keeps this from being the second DISCONNECT on the socket.
//
// No lock held on entry.
// The vocabulary decides what is CLAIMED. A demonstrated takeover says so; a record
// merely proven to have FORKED says only that ownership could not be established, and
// carries 0x89 rather than 0x8E so the client is invited to reconnect instead of being
// told to stop. See sess_fork_lineage.go.
func (c *client) mqttNotifyForeignDisposal(id string, voc mqttDisposalVocabulary) {
	c.Warnf("MQTT: session %q %s", id, voc.logReason)
	c.mqttNotifyDisplaced(voc)
}

// disposeForeignSession is disposeForeignSessionLocked for a caller that does not
// already hold the ownership gate. Failing to take it means a CONNECT is in
// progress: it wins, and it will rebuild from the authoritative record itself.
//
// No lock held on entry.
func (sess *mqttSession) disposeForeignSession(c *client, voc mqttDisposalVocabulary) {
	asm := sess.asm
	if asm == nil {
		return
	}
	sess.mu.Lock()
	owner := sess.c
	sess.mu.Unlock()
	if err := asm.lockSession(sess, owner); err != nil {
		return
	}
	defer asm.unlockSession(sess)
	sess.disposeForeignSessionLocked(asm, c, voc)
}

// retainOnLanded registers what a caller owes once an ambiguous write is PROVEN
// durable, for a mutation that was KEPT because that write may have landed.
//
// There is one continuation because there is one outcome to prepare for. An
// ambiguity is settled by re-attempting the exact bytes, so it ends as ours or as
// foreign and never as "it did not land": the kept mutation is never rolled back,
// and what a caller cannot do at failure time is only the part that needed PROOF —
// acking a drain, releasing a group durable, applying granted terms. That is what
// this registers. A caller with nothing to defer registers nothing; its mutation
// simply stands, exactly as it would have with the response not lost.
//
// It runs with sess.mu NOT held, and receives the RESOLVER'S record-transaction
// token. A continuation that needs to persist must write under it: opening a
// transaction of its own would deadlock against the resolver holding the fence, and
// would also let another writer interleave between the resolution and the
// reconciliation.
//
// sess.mu must not be held.
func (sess *mqttSession) retainOnLanded(onLanded func(recordTx)) {
	if onLanded == nil {
		return
	}
	sess.mu.Lock()
	sess.uncertainLanded = append(sess.uncertainLanded, onLanded)
	sess.mu.Unlock()
}

// takeUncertainActions removes and returns the registered continuations.
func (sess *mqttSession) takeUncertainActions() []func(recordTx) {
	sess.mu.Lock()
	r := sess.uncertainLanded
	sess.uncertainLanded = nil
	sess.mu.Unlock()
	return r
}

// commitUncertainWrites runs the retained continuations, oldest first: the record
// holds every mutation they were registered for, so the follow-ups they deferred are
// now owed.
//
// The walker takes the whole list and runs it to the end, which is correct only while
// at most ONE continuation per pending write can re-arm uncertainty. Register a
// second re-arming continuation and it must stop at the first re-arm and put the
// unprocessed tail back: continuations that follow it would write under a token whose
// session is uncertain again, and every such write is refused at entry — silently,
// since the walker looks at no outcome.
func (sess *mqttSession) commitUncertainWrites(tok recordTx) {
	for _, onLanded := range sess.takeUncertainActions() {
		onLanded(tok)
	}
}

// deferDisconnect records the MQTT obligations a close could not discharge because
// the session was unresolved, and detaches the client so nothing further can act on
// the stale state. Returns false if an action was already recorded (the first one
// holds the original deadlines and must win).
//
// No lock held on entry.
func (sess *mqttSession) deferDisconnect(action *mqttDeferredDisconnect) bool {
	parked, _ := sess.deferDisconnectIfUncertain(action, false)
	return parked
}

// deferDisconnectIfUncertain is deferDisconnect for a close that does NOT hold
// the ownership gate, so the uncertainty it is parking against can be resolved
// out from under it. The check and the park are ONE sess.mu critical section:
// checked separately, a gate-holding resolver can clear the uncertainty and run
// its final discharge scan between the two, and an action parked after that
// scan is stranded forever — the session is no longer uncertain, so neither the
// periodic pass nor the next CONNECT will ever resolve (and discharge) again.
//
// Returns (parked, stillUncertain). Not parked because the session resolved
// meanwhile (stillUncertain false) means the close must go back to taking the
// gate and performing an ordinary teardown; not parked because an action
// already exists (stillUncertain true) keeps the first action's deadlines, as
// always. requireUncertain false is the gated caller, for whom the check is
// the caller's own (it holds the gate, so nothing can resolve concurrently).
//
// No lock held on entry.
func (sess *mqttSession) deferDisconnectIfUncertain(action *mqttDeferredDisconnect, requireUncertain bool) (bool, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if requireUncertain && !sess.seqUncertain {
		return false, false
	}
	if sess.deferredDisconnect != nil {
		return false, true
	}
	sess.deferredDisconnect = action
	sess.c = nil
	return true, true
}

// retargetDeferredLifetime updates a PARKED deferred disconnect's Session
// Expiry Interval, for the one case that can invalidate it: a lifetime grant
// whose write was unresolved when the connection closed, and which a later
// readback proves LANDED.
//
// The close path snapshots the lifetime it can see, which while unresolved is
// deliberately the last CONFIRMED one — that is what keeps a grant that never
// landed from discarding the session. When the readback proves the opposite,
// the snapshot is the stale half: applyDeferredDisconnect decides discard and
// timer behavior from the action, not from the session, so without this the
// outcome would depend on whether the close or the readback ran first.
//
// prior is whatever was parked when the grant was attempted, and it is what
// makes this SAFE: an action is only retargeted when it is not that one. A
// deferred disconnect carries no owner identity, and one parked by an EARLIER
// connection can outlive its resolution — resolveUncertainNow classifies a write
// as ours without discharging a parked action, after which the periodic pass
// skips the session because it is no longer uncertain, and the next CONNECT
// skips resolution for the same reason. Retargeting blindly would then rewrite
// a previous connection's teardown terms with this one's. Pointer identity is
// the exact predicate: the close path installs one action and refuses to
// replace it, so a different pointer means this connection's close parked it.
//
// Only the interval moves. The action's Will is untouched and cannot be
// inconsistent with it here: a CONNECT refused before its CONNACK has already
// discarded its Will (mqttAbortAcceptedConnect), so a parked action from that
// path carries none.
//
// No lock held on entry.
func (sess *mqttSession) retargetDeferredLifetime(prior *mqttDeferredDisconnect, expiry uint32) {
	sess.mu.Lock()
	if cur := sess.deferredDisconnect; cur != nil && cur != prior {
		cur.expiryInterval = expiry
	}
	sess.mu.Unlock()
}

// takeDeferredDisconnect removes and returns the pending deferred action, if any.
func (sess *mqttSession) takeDeferredDisconnect() *mqttDeferredDisconnect {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	action := sess.deferredDisconnect
	sess.deferredDisconnect = nil
	return action
}

// expiryDeadline is the absolute Unix time this session expires, and whether it has
// one at all. A zero interval is "ends now"; mqttSessionNeverExpire has no deadline.
func (a *mqttDeferredDisconnect) expiryDeadline() (int64, bool) {
	if a.expiryInterval == 0 || a.expiryInterval == mqttSessionNeverExpire {
		return 0, false
	}
	return a.disconnectedAt + int64(a.expiryInterval), true
}

// endsAtDisconnect reports whether this session was to be discarded at connection
// close (a deferred clear()).
func (a *mqttDeferredDisconnect) endsAtDisconnect() bool { return a.expiryInterval == 0 }

// expiredBy reports whether the session's expiry deadline has already passed at
// now, so resolution must discard it immediately rather than arm a fresh interval.
func (a *mqttDeferredDisconnect) expiredBy(now time.Time) bool {
	deadline, ok := a.expiryDeadline()
	return ok && now.Unix() >= deadline
}

// willDue reports whether the Will's absolute deadline has passed at now. A
// reconnect arriving before it cancels the Will; after it, the Will is still owed.
func (a *mqttDeferredDisconnect) willDue(now time.Time) bool {
	return a.willDeadline > 0 && now.UnixNano() >= a.willDeadline
}

// mqttDeferDisconnectOwned captures everything a close would have done and parks
// it on the session, then detaches the client. Nothing is written and nothing is
// deleted: the session's record state is unresolved, so no teardown decision can
// be made from it yet.
//
// The Will is captured HERE rather than re-derived on resolution, for two reasons:
// the closing client's CONNECT packet is about to become unreachable, and its
// publish permission is not serializable, so it has to be evaluated while the
// client still exists. That evaluation is currently VACUOUS in exactly the same
// way mqttHandleWill's is: pubAllowedFullCheck always returns true, so a denied
// Will IS captured here and IS replayed later. F-079.
//
// OWNED is a precondition, not a description: the caller holds the session
// ownership gate. That is what entitles the park to clear this client ID from the
// flappers jail, and a caller that does not hold it must use
// mqttDeferDisconnectUngated instead. The two entry points exist so that
// requirement is visible at the call rather than carried by a boolean whose name
// is about something else.
//
// Runs from the client's readLoop. No lock held on entry.
func (s *Server) mqttDeferDisconnectOwned(c *client, asm *mqttAccountSessionManager, sess *mqttSession) {
	s.mqttDeferDisconnectEx(c, asm, sess, false)
}

// mqttDeferDisconnectUngated is the park for a close that could NOT take the
// ownership gate. Returns whether the teardown is now someone's responsibility
// (parked here, or already parked by an earlier close); false means the session
// resolved while this close was building the action, so the caller must retake the
// gate and tear down ordinarily — and the Will this function consumed has been put
// back exactly as it was, or that ordinary teardown would publish nothing.
func (s *Server) mqttDeferDisconnectUngated(c *client, asm *mqttAccountSessionManager, sess *mqttSession) bool {
	return s.mqttDeferDisconnectEx(c, asm, sess, true)
}

// mqttDeferDisconnectEx is the shared body of the two entry points above.
//
// ONE fact drives the parameter, and both of its consequences: this close does not
// hold the session ownership gate. Without the gate the park must be conditional on
// the session still being uncertain, because nothing else serializes it against the
// resolver that would discharge it; and without the gate the jail entry cannot be
// identified, because the producers the gate excludes are free to run. A future
// caller that gets this wrong changes what is deleted, which is why the callers name
// it rather than pass it.
func (s *Server) mqttDeferDisconnectEx(c *client, asm *mqttAccountSessionManager, sess *mqttSession, ungated bool) bool {
	now := time.Now()
	sess.mu.Lock()
	action := &mqttDeferredDisconnect{
		disconnectedAt: now.Unix(),
		expiryInterval: sess.expiryInterval,
		willSessionSeq: sess.seq,
		willGen:        sess.gen,
	}
	sess.mu.Unlock()
	action.willSessEpoch = asm.sessStreamEpoch

	c.mu.Lock()
	// Consume it: from here this deferred action owns the Will, and no other
	// teardown path may publish it as well. See mqttTakeWillLocked. taken/takenDisp
	// remember the pre-take state so a REFUSED park can undo the consumption.
	var taken *mqttWill
	var takenDisp mqttWillDisposition
	if will, disposition := c.mqttTakeWillLocked(); will != nil {
		taken, takenDisp = will, disposition
		// Same rule as mqttHandleWill: a v5 Will Delay on a session that survives
		// the connection defers publication, capped at the Session Expiry Interval;
		// anything else is due immediately. The close already classified it unless
		// it somehow arrived here still armed.
		delayed := disposition == mqttWillOwedDelayed
		if disposition == mqttWillArmed {
			delayed = mqttWillIsDelayed(c.mqtt.proto, will, action.expiryInterval)
		}
		if !c.pubAllowedFullCheck(string(will.subject), false, true) {
			// Intended: a denied Will is not captured, so it can never be published
			// later either. UNREACHABLE today -- pubAllowedFullCheck is hardcoded to
			// true, so this arm never runs and a denied Will is captured. F-079.
			will = nil
		}
		if will != nil {
			action.will = will
			action.willProps = mqttWillForwardProps(will.props)
			action.willUser = c.opts.Username
			action.willDelayed = delayed
			if delayed {
				effDelay := will.props.willDelay
				if action.expiryInterval < effDelay {
					effDelay = action.expiryInterval
				}
				action.willDeadline = now.Add(time.Duration(effDelay) * time.Second).UnixNano()
			} else {
				action.willDeadline = now.UnixNano()
			}
		}
	}
	c.mu.Unlock()

	parked, stillUncertain := sess.deferDisconnectIfUncertain(action, ungated)
	if !parked {
		if !stillUncertain {
			// Resolved while the action was being built: nothing may be parked
			// (nothing would ever discharge it), and the Will goes back where it
			// was so the ordinary teardown the caller now performs can publish it.
			if taken != nil {
				c.mu.Lock()
				if c.mqtt != nil && c.mqtt.cp != nil {
					c.mqtt.cp.will = taken
					c.mqtt.willDisposition = takenDisp
				}
				c.mu.Unlock()
			}
			return false
		}
		return true // an earlier close already parked its (original) deadlines
	}
	// Un-jail this client ID only where THIS CLOSE OWNS THE SESSION, which is what
	// makes the entry provably a stale one from an earlier displacement rather than a
	// live one somebody just installed. Under a held gate neither producer can run:
	// evictDecideOnce refuses outright (evictBusy) and discardTakenOverClient sits
	// downstream of a CONNECT that would need the same gate.
	//
	// Ungated, nothing here can be proven. The gate holder this close lost to may have
	// released between lockSessionForTeardown's last attempt and this park, leaving
	// evictDecideOnce free to detach the old client and jail this client ID in the
	// interval — and deleting THAT entry would cancel a displacement's jail. Leaving it
	// costs at most one mqttSessJailDur of reconnect ENFORCEMENT on an entry that may
	// have been stale — the entry can outlive that window in the map, but CONNECT
	// stops acting on it — which is the fail-closed direction.
	//
	// The lock is taken here rather than inherited: the owning callers hold the session
	// gate, not asm.mu. It is taken BARE — deferDisconnectIfUncertain released sess.mu
	// inside its own critical section, and this caller holds no record fence on any
	// route (the never-expiring one parks only after endRecordTx) — so it nests inside
	// nothing and adds no edge to the established as.mu -> sess.mu order.
	if !ungated {
		asm.mu.Lock()
		asm.removeSessFromFlappersLocked(sess.id)
		asm.mu.Unlock()
	}
	asm.afterParkedJailDecision(sess.id)
	c.Warnf("MQTT: session %q has an unresolved record write; deferring its disconnect handling until it resolves", sess.id)
	return true
}

// resolveUncertainSession classifies a parked session's ambiguous write and
// discharges whatever the close deferred. It is the single place that turns
// "unresolved" into an outcome, so CONNECT and the retry pass both go through it
// and exactly one of them acts.
//
// The caller must hold the session in asm.sessLocked (lockSession), which is what
// serializes it against a concurrent CONNECT.
//
// Returns the resolution. mqttUncertainUnresolved has the two meanings the enum
// states: the readback failed, in which case nothing was written, nothing was
// deleted and the deferred action is still parked; or the DISCHARGE re-armed
// uncertainty, in which case the action was applied and its own record write is now
// the outstanding one. Callers must treat both the same way — the session's durable
// state is unknown either way — and no caller may bind, resume or discard on it.
//
// No lock held on entry.
func (s *Server) resolveUncertainSession(asm *mqttAccountSessionManager, sess *mqttSession) mqttUncertainResolution {
	res, err := sess.resolveUncertainWrite()
	switch res {
	case mqttUncertainUnresolved:
		s.Debugf("MQTT: session %q is still unresolved: %v", sess.id, err)
		return res
	case mqttUncertainForeign, mqttUncertainForked:
		// Another owner holds this session, or — on the forked arm — the record moved
		// without anything establishing one. One disposition path for both resolvers,
		// so a session found foreign by the periodic pass has its stale connection
		// CLOSED too — this pass now examines bound sessions, and detaching one
		// without closing it leaves a live client acting on an evicted session. The
		// caller already holds the ownership gate.
		sess.disposeForeignSessionLocked(asm, nil, mqttDisposalForForeignResolution(res))
		return res
	}

	// Ours. Discharge the deferred close, if the session is still unbound: a
	// reconnect that got here first owns the session now and its CONNECT applies
	// the reconnect rules (cancelling or retaining the Will) itself.
	sess.mu.Lock()
	rebound := sess.c != nil
	sess.mu.Unlock()
	if rebound {
		return res
	}
	if action := sess.takeDeferredDisconnect(); action != nil {
		s.applyDeferredDisconnect(asm, sess, action)
		// The discharge writes the record ITSELF, outside resolveUncertainWriteInTx,
		// so it can leave the session uncertain again after the readback classified
		// the previous attempt as ours. Re-checking here is what stops a CONNECT
		// from binding on a stale "ours" and then evicting the session its refused
		// accept save runs into — taking the deferred deadline and the void-register
		// verdicts the discharge just staged with it.
		return sess.resolutionAfterContinuations(res)
	}
	return res
}

// applyDeferredDisconnect performs the close work that was parked, against the
// ORIGINAL deadlines: the session gets the lifetime it actually earned, not a
// fresh interval starting whenever JetStream came back.
//
// It is called with the action already CONSUMED, which is what makes the
// unresolved re-check below load-bearing rather than defensive: nothing here can
// land while the record state is unresolved — the surviving branch's save is
// refused at entry and clear() refuses at entry too — and there would be nothing
// left to retry the close with.
//
// No lock held on entry; the session is in the locked map.
func (s *Server) applyDeferredDisconnect(asm *mqttAccountSessionManager, sess *mqttSession, action *mqttDeferredDisconnect) {
	now := time.Now()

	// The resolution that authorized this discharge released the record fence before
	// getting here, and holding the ownership gate does not exclude an off-readLoop
	// writer: a register store settling meanwhile takes the fence with tryBeginRecordTx
	// and can leave its own write AMBIGUOUS. Put the action back untouched rather than
	// applying a close whose every write is refused — the session is uncertain again, so
	// the periodic pass and the next CONNECT resolve and discharge it, with its ORIGINAL
	// deadlines still intact. The surviving branch re-checks once more under its own
	// fence, which is the sound test; this one is the cheap early-out that also covers
	// the discard branch, whose clear() opens a fence of its own.
	if sess.isSeqUncertain() {
		s.reparkDeferredDisconnect(sess, action)
		return
	}
	// Test seam, fired in the window between that check and the fence the surviving branch
	// takes below: an ambiguity armed here is invisible to the check above, which is what
	// makes the in-fence re-check the sound one rather than a belt-and-braces copy.
	if h := mqttTestDischargeWindowHookV.Load(); h != nil {
		(*h)(sess)
	}

	switch {
	case action.endsAtDisconnect(), action.expiredBy(now):
		// The session ended at connection close, or its deadline passed while the
		// record was unresolved. Either way it is discarded now, not given a fresh
		// interval.
		//
		// clear() must own its own transaction — it ENDS the record generation — so this
		// branch cannot hold the fence across it the way the surviving branch does. What
		// makes the discard safe is the in-fence refusal: an ambiguity armed after the
		// entry check makes clear() fail before its first deletion rather than tear down
		// on a stale sequence. The recovery is then ours, because the action is already
		// consumed: put it back, with the session still RESIDENT, so the next resolution
		// discharges the discard and its Will.
		//
		// Keyed on the SENTINEL, never on "is the session uncertain now": any other
		// failure means a lost teardown claim or a partly-done teardown, and re-parking
		// there would resurrect an obligation that is either another owner's or already
		// half-discharged. Those keep the historical outcome — the object goes.
		if err := sess.clear(true); err != nil {
			switch {
			case errors.Is(err, errMQTTClearRefusedUnresolved):
				s.reparkDeferredDisconnect(sess, action)
				return
			case errors.Is(err, errMQTTClearSuperseded):
				// TERMINAL, in its own branch: the base this deferred discard claimed is
				// not current, so the discard is another owner's business and nothing was
				// ours to delete. NOT re-parked — the action is consumed and putting it
				// back would resurrect an obligation the current owner holds — and not
				// warned about, since it is the ordinary outcome of losing an
				// arbitration. The removeSession tail below still runs: this object is
				// finished either way.
				s.Debugf("MQTT: deferred discard of session %q stopped: %v", sess.id, err)
			default:
				s.Warnf("MQTT: deferred discard of session %q failed: %v", sess.id, err)
			}
		}
		asm.removeSession(sess, true)
	default:
		// The session SURVIVES, so this is the deferred form of the ordinary surviving
		// close and owes everything that close owes: the ORIGINAL disconnect time
		// persisted, only what remains of the interval armed, and the shared-group
		// pending deliveries purged so a resume cannot NAK-steal messages AckWait would
		// redeliver to other members and so the packet identifiers come back
		// (Spec5 [4.8.2], [4.9]).
		//
		// ONE record transaction and ONE classified write for both. The purge is not a
		// separate operation that happens to run first: when both register-stream rungs
		// fail it falls back to the session record, and an independent save of its own
		// can land ambiguously — after which every later write on this session is
		// refused, including the disconnect time's, with the parked action already
		// consumed and nothing left to retry it. The deadline would then exist in memory
		// only while the landed record said DisconnectedAt=0, i.e. connected: the local
		// timer fires, expireSession finds the record bumped past its stale sequence and
		// delegates to the record, which reads "connected, nothing due" and drops the
		// expiry — leaking the record until a reconnect that may never come.
		//
		// So the fence opens BEFORE the first mutation, the purge only STAGES (its
		// fallback verdicts go into cancelledRegNonces, its register-stream rungs need
		// no record), and one save carries the disconnect time and the verdicts
		// together.
		tok := sess.beginRecordTx()
		// Re-checked with the fence HELD, before the first mutation, and this is the check
		// that is sound: the early-out above is a check-then-act, because the fence is what
		// serializes writers and another one can go ambiguous while this call is still
		// waiting for it. Uncertainty cannot appear from here on — creating it requires a
		// save, and a save requires this fence.
		if sess.isSeqUncertain() {
			if terr := sess.endRecordTx(tok); terr != nil {
				s.Errorf("MQTT: %v", terr)
			}
			s.reparkDeferredDisconnect(sess, action)
			return
		}
		sess.mu.Lock()
		sess.disconnectedAt = action.disconnectedAt
		sess.mu.Unlock()
		if owed := sess.purgeSharedPendingStaged(nil); owed {
			s.Debugf("MQTT: the deferred close of session %q carries its void-register verdicts on the session record", sess.id)
		}
		outcome, err := sess.saveClassified(tok)
		switch outcome {
		case saveMaybeLanded:
			// Ambiguous: the deadline and the verdicts both ride the exact bytes this
			// write attempted, and those are what the resolver re-attempts — so the
			// settlement itself restores restart-survival and credits the revision they
			// marshalled. Nothing is owed on proof.
			s.Debugf("MQTT: the deferred disconnect of session %q was written ambiguously; the resolver will settle it: %v", sess.id, err)
		case saveDefinitelyFailed:
			// The in-memory purge stands, the verdicts stay staged for the next
			// convergence point, and the timer below is still armed. Only
			// restart-survival of the deadline is lost — the same posture as the
			// ordinary close's failed disconnect save, and worth a warning because a
			// lost purge verdict is not a routine debug event.
			s.Warnf("MQTT: unable to persist the deferred disconnect of session %q; its deadline will not survive a restart: %v", sess.id, err)
		}
		if terr := sess.endRecordTx(tok); terr != nil {
			s.Errorf("MQTT: %v", terr)
		}
		if deadline, ok := action.expiryDeadline(); ok {
			remaining := time.Until(time.Unix(deadline, 0))
			if remaining < 0 {
				remaining = 0
			}
			asm.scheduleSessionExpiry(s, sess.idHash, remaining)
		}
	}

	if action.will == nil {
		return
	}

	// RE-SAMPLE the clock. The branch above did JetStream work — the purge's register-stream
	// rungs and the classified save — and a Will deadline that passed during it must not be
	// read from the entry sample as still in the future: that would enqueue an already-DUE
	// Will, and an enqueued record is cancellable, so a reconnect arriving after the deadline
	// could suppress a Will that was already owed. The direct publication below is exactly
	// what makes that reconnect rule hold, so the decision has to be made on the time as it
	// is now, not as it was before the work.
	now = time.Now()

	// A Will that is already DUE — one with no delay, or a delayed one whose
	// original deadline passed while the record was unresolved — is published
	// directly rather than enqueued. That is what makes the reconnect rule hold:
	// a reconnect arriving BEFORE the original deadline cancels the Will through
	// the outbox as usual, but one arriving AFTER it must not, because the Will was
	// already owed. Enqueuing a past-due Will instead would race the reconnect's
	// own cancel scan and could silently suppress it.
	//
	// The publishing client is long gone, so this goes through the outbox's
	// client-free publisher rather than the immediate path (which writes into
	// c.mqtt.pp). Same publish core, same bounding.
	if !action.willDelayed || action.willDue(now) {
		rec := &mqttWillOutbox{
			ClientIDHash: sess.idHash,
			Gen:          action.willGen,
			Topic:        string(action.will.topic),
			Msg:          action.will.message,
			QoS:          action.will.qos,
			Retain:       action.will.retain,
			Props:        action.willProps,
			User:         action.willUser,
		}
		if err := asm.deliverWillRecord(s, rec); err != nil {
			s.Warnf("MQTT: deferred Will for session %q could not be published: %v", sess.id, err)
		}
		return
	}

	// Still within the Will Delay: hand it to the durable outbox with its ORIGINAL
	// absolute FireAt, so the delay is the one the client configured rather than
	// one restarted by the outage, and a timely reconnect can still cancel it.
	sess.mu.Lock()
	seq := action.willSessionSeq
	if seq == 0 {
		seq = sess.seq
	}
	sess.mu.Unlock()
	// The EXACT client id, not its hash: suppression compares it against an
	// acceptance fact's client id, and two clients can share a hash.
	_ = asm.enqueueDelayedWill(s, sess.id, action.willGen, action.will, action.willProps,
		action.willUser, action.willDeadline, seq, action.willSessEpoch)
}

// mqttTestDischargeWindowHookV fires after applyDeferredDisconnect's entry check and before
// the surviving branch opens its record transaction. See applyDeferredDisconnect.
var mqttTestDischargeWindowHookV atomic.Pointer[func(sess *mqttSession)]

func setMQTTTestDischargeWindowHook(h func(sess *mqttSession)) func() {
	prev := mqttTestDischargeWindowHookV.Load()
	if h == nil {
		mqttTestDischargeWindowHookV.Store(nil)
	} else {
		mqttTestDischargeWindowHookV.Store(&h)
	}
	return func() { mqttTestDischargeWindowHookV.Store(prev) }
}

// reparkDeferredDisconnect puts a consumed action back, for a discharge that found the
// record state unresolved again. Nothing is undone because nothing was applied, and the
// action still carries the ORIGINAL deadlines — which is the whole reason it is re-parked
// rather than rebuilt.
//
// A refusal means an action is already parked, which cannot happen for a session nobody
// can bind (the ownership gate is held), so it is reported rather than handled: the first
// action holds the earlier deadlines and must win, as always.
//
// No lock held on entry.
func (s *Server) reparkDeferredDisconnect(sess *mqttSession, action *mqttDeferredDisconnect) {
	if !sess.deferDisconnect(action) {
		s.Warnf("MQTT: the deferred disconnect of session %q could not be re-parked; another action is already pending", sess.id)
		return
	}
	s.Debugf("MQTT: the deferred disconnect of session %q stays parked; its record state is unresolved again", sess.id)
}

// reparkRefusedDiscard is the recovery for a discard that clear() REFUSED with
// errMQTTClearRefusedUnresolved — the ordinary clean close, session expiry and the
// CONNECT-accept rollback's zero-lifetime branch all take it. None may simply abandon the
// session: the refusal deleted nothing, so the record, the consumers, the shared group
// durables the discard owed a release, and the generation's in-flight registers are all
// still there.
//
// It parks the discard as a deferred disconnect with expiryInterval 0, so endsAtDisconnect()
// is true and the discharge takes applyDeferredDisconnect's discard branch — the same
// clear(), performed by a caller that has just proved the record state resolved. Then it
// restores RESIDENCY, which is what every resolver needs: reconcileResidentRecordState and CONNECT
// both scan as.sessions, and for an expiry-0 record sweepSessionExpiries looks at nothing at
// all. Park first, publish second — the obligation must exist before the object is reachable.
//
// "Restores" holds for the two callers that REMOVED the session before attempting the
// discard. The CONNECT rollback's session is still resident when it calls here — it removes
// only in its own tail, which the parked outcome skips — so for that caller addSession
// merely republishes the same two map entries. Idempotent, and the park is what the call is
// for either way.
//
// No WILL goes into the action, deliberately. A parked Will is memory-only and
// non-transferable: a remote takeover evicts the action under as.mu alone and nothing
// publishes it, which would regress a Will today's broker delivers. The close publishes its
// own by falling through to mqttHandleWill; expiry has none, its Will was discharged by the
// close that started the deadline.
//
// The caller must hold the ownership gate, and must key this on errMQTTClearRefusedUnresolved
// ALONE. Any other clear() failure means a lost teardown claim or a partly-done teardown,
// where re-parking would resurrect an obligation that is another owner's or already half
// discharged.
//
// Returns whether the discard is now someone's responsibility. False leaves the caller its
// historical outcome: the session stays evicted and the caller logs — or, for a caller whose
// session is still resident, it is removed by that caller's own tail. Ignoring a false return
// there would leave a resident object nobody owns.
//
// No lock held on entry.
func (s *Server) reparkRefusedDiscard(asm *mqttAccountSessionManager, sess *mqttSession, disconnectedAt int64) bool {
	parked, stillUncertain := sess.deferDisconnectIfUncertain(
		&mqttDeferredDisconnect{disconnectedAt: disconnectedAt, expiryInterval: 0}, true)
	switch {
	case !parked && !stillUncertain:
		// UNREACHABLE while the gate is held: seqUncertain is cleared only by
		// clearUncertaintyLocked — reached through resolveUncertainNow, which takes the
		// gate itself, or resolveUncertainSession, whose caller holds it — and by
		// disposeForeignSessionLocked, which also requires it. Reported rather than
		// handled, and returning false is what makes a future violation of that invariant
		// benign (the caller's historical outcome) rather than silent.
		s.Errorf("MQTT: the refused discard of session %q cannot be parked: the session resolved while the ownership gate was held", sess.id)
		return false
	case !parked:
		// An action is already parked, so this session already owes an end. This helper
		// ARBITRATES NOTHING. It recognises only the shape it would have written itself —
		// ends at disconnect AND carries no Will — and in that case the discard IS that
		// obligation, already recorded: the action is neither mutated nor replaced.
		//
		// Every other shape declines. A parked action carries no owner identity, so
		// nothing here can prove whose deadline or whose Will a retarget would overwrite;
		// declining keeps today's evict-and-log, which is provably no worse than today
		// because it IS today. Reachability of those shapes at this point is unknown and
		// stated as such in docs/CONFORMANCE.md.
		//
		// Read under sess.mu, as every other access to this field is.
		sess.mu.Lock()
		existing := sess.deferredDisconnect
		discardOnly := existing != nil && existing.endsAtDisconnect() && existing.will == nil
		sess.mu.Unlock()
		if !discardOnly {
			return false
		}
		s.Debugf("MQTT: the refused discard of session %q is already parked as a discard-only obligation", sess.id)
	}
	// RESIDENT again, so a resolver can find it. Two of the three callers removed it
	// before attempting the discard, and for the close that removal is what makes the
	// failure a leak: an expiry-0 record with no resident object has no reaper at all.
	// For the CONNECT rollback the session never left the maps, so this is a re-publish
	// of the same entries.
	asm.addSession(sess, true)
	return true
}

// reconcileResidentRecordState is the periodic pass over RESIDENT sessions whose
// record state is not settled. It resolves an ambiguous write, settles a definite
// rejection, and back-stops the void-verdict release handoff — three ways for a
// resident session's record state to need work that no traffic will bring.
//
// It cannot be folded into sweepSessionExpiries: that walks persisted RECORDS and
// skips ExpiryInterval == 0 or mqttSessionNeverExpire — exactly the two cases that
// carry a deferred clear() or a deferred Will, so the sessions that most need
// resolving are the ones it would never look at.
//
// It is serialized against CONNECT by the same ownership gate expiry uses
// (lockSession with a nil client, which fails while a client is bound or the ID is
// mid-CONNECT), so a reconnect simply wins and resolves the session itself.
//
// Runs from the periodic sweep goroutine. No lock held on entry.
func (as *mqttAccountSessionManager) reconcileResidentRecordState(s *Server) {
	// Snapshot under the manager lock and release it before any JetStream call.
	as.mu.RLock()
	candidates := make([]*mqttSession, 0, len(as.sessions))
	for _, sess := range as.sessions {
		candidates = append(candidates, sess)
	}
	as.mu.RUnlock()

	for _, sess := range candidates {
		sess.mu.Lock()
		uncertain := sess.seqUncertain
		owner := sess.c
		txOpen := sess.recordTxOpen
		// Sampled here and RE-READ inside the fence by the probe itself; this is only
		// the candidate test.
		rejected := sess.recordRejectionPendingLocked()
		sess.mu.Unlock()
		if txOpen {
			// A writer is mid-transaction. save() publishes seqUncertain BEFORE it
			// returns, so resolving now would settle an ambiguity whose writer has not
			// yet classified it or registered its continuations: the writer would then
			// read its own error as DEFINITE and roll back a mutation that landed, or
			// register a continuation into an already-consumed list. Both are silent.
			// The fence is exactly the interval in which mark-classify-register is
			// atomic, so wait for the next pass. See classifySave.
			continue
		}
		if uncertain {
			// A BOUND session must be resolved too, and it is the harder case: nothing
			// else will do it. Its own writers cannot — every save is refused while the
			// session is unresolved — and the connection can stay open indefinitely, so
			// "wait for the disconnect" is not a bound on anything. Left alone, such a
			// session stops being able to persist at all: an UNSUBSCRIBE whose response
			// was lost, an affine unwind.
			//
			// The gate is taken as the BOUND CLIENT rather than as nobody, which is the
			// same ownership claim its own readLoop would make: it still fails while a
			// CONNECT is in progress (sessLocked) or if the client changed underneath
			// us, so a reconnect continues to win. resolveUncertainSession itself
			// re-checks for a rebind before discharging any deferred close, and its
			// foreign branch detaches and evicts — which is exactly right for a bound
			// client holding a session somebody else now owns.
			if err := as.lockSession(sess, owner); err != nil {
				continue // a CONNECT is in progress, or the binding changed: it wins
			}
			// Re-validate AFTER taking the gate: a CONNECT may have resolved this
			// session between the snapshot and here.
			if sess.isSeqUncertain() {
				s.resolveUncertainSession(as, sess)
			}
			as.unlockSession(sess)
			continue
		}
		if rejected {
			as.settleRejectionFromSweep(s, sess)
			continue
		}
		// Nothing outstanding on the record, but possibly still owing a void-register
		// verdict: the BACKSTOP is what carries the release handoff's liveness past a
		// definite store failure. Not unconditional — the attempt it re-arms enters the
		// same pending predicate, so it defers again while a blocking writer is announced
		// for the fence (see sess_recordtx.go), and that waiter's own release is then the
		// next trigger.
		as.flushVoidVerdictGap(sess)
		// And the OWNERSHIP backstop: the session-record watch is a lossy push channel,
		// so a resident session whose record moved (or vanished) remotely may never have
		// been told. Reached only here, which is exactly the ordering this needs — an
		// open transaction, an unresolved write and an armed rejection each `continue`
		// above, so this never rediscovers a local ambiguity as if it were a peer's
		// write. See reconcileResidentOwnership.
		as.reconcileResidentOwnership(s, sess)
	}
}

// reconcileResidentOwnership is the committed-read backstop behind the session-record
// watch: one read per resident session per pass, feeding the SAME triggers a delivery
// feeds. It is what makes convergence a guarantee rather than a hope, because the
// watch's own deliveries can be dropped (AckNone, no flow control) and its eviction
// worker can refuse a held gate.
//
// A read can only ever be a TRIGGER here, never a verdict, and the asymmetry is the
// point: "the subject moved" is provable from committed state, while "the subject did
// NOT move" is not — JetStream returns its contention rejection while a write is in
// process upstream, so an unmoved read proves nothing (see sess_rejected_write.go).
// Every classification therefore still happens in the worker, under the record fence,
// with the digest arm ahead of it.
//
// Two shapes trigger, and the second is the one no sequence comparison can see:
//
//   - the record MOVED past our acknowledged sequence: feed the eviction trigger, as a
//     delivery would.
//   - the record is GONE while we hold a non-zero sequence: there is no newer sequence
//     to compare against, so this arms the ownership marker instead and lets the
//     rejection settlement — which already answers "the record moved out from under
//     me" — converge it. A record can only be removed by a party that first CLAIMED it
//     (a teardown claim is a full-body CAS at the sequence it believes current) or by
//     the expiry owner of a record we no longer own, so absence IS an ownership fact.
//     Locally the shape cannot be manufactured: clear() is the only deleter, and it
//     zeroes sess.seq before it claims or deletes anything — the CONNECT rollback's
//     zero-lifetime branch included, since it deletes through that same teardown.
//
// It also re-fires a trigger a refusal left undecided (evictLatest > evictDecided),
// which is the other half of the "an eviction can be lost entirely" fix.
//
// Runs from the periodic sweep goroutine, with no lock held and no session gate: it
// installs no state of its own beyond a trigger, and every decision it provokes takes
// the locks it needs for itself.
func (as *mqttAccountSessionManager) reconcileResidentOwnership(s *Server, sess *mqttSession) {
	as.mu.RLock()
	off := as.testSessRecordBackstopOff
	as.mu.RUnlock()
	if off {
		return
	}
	sess.mu.Lock()
	idHash, ourSeq := sess.idHash, sess.seq
	sess.mu.Unlock()
	if ourSeq == 0 {
		// Nothing was ever acknowledged for this session, so neither shape applies:
		// there is no sequence to have moved past, and an absent subject is the
		// ordinary state of a session whose first record has not landed.
		return
	}
	// Re-fire a trigger some refusal left undecided, before spending a read: this
	// needs no authoritative state, only the accounting.
	as.mu.Lock()
	if st := as.reconcileWM[idHash]; st != nil && st.evictLatest > st.evictDecided &&
		!st.evictRunning && s.admitWillWorker() {
		st.evictRunning = true
		st.inFlight++
		go func() {
			defer s.willWorkerWG.Done()
			as.evictDisplacedOnRemotePersist(s, idHash, st)
		}()
		as.mu.Unlock()
		return
	}
	as.mu.Unlock()

	smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash)
	if err != nil && isErrorOtherThan(err, JSNoMessageFoundErr) {
		// Unreadable establishes nothing in either direction; the next pass asks again.
		return
	}
	if smsg == nil {
		// GONE. Hand it to the settlement that already owns "the record moved out from
		// under me", rather than inventing a second verdict for the same question. The
		// marker's meaning is exactly this — the ownership question is outstanding —
		// and its point-of-use predicate still governs when a probe may run.
		sess.mu.Lock()
		if sess.seq == ourSeq && !sess.seqUncertain {
			sess.markWriteRejectedLocked(ourSeq)
		}
		sess.mu.Unlock()
		return
	}
	if smsg.Sequence <= ourSeq {
		return
	}
	// MOVED: identical to a delivery, except that NO record is passed — which
	// deliberately bypasses the callback's own-write spawn filter. That filter keys
	// on Origin alone and Origin proves no authorship, so a collision could make the
	// fast path skip a real peer's write forever; the backstop is what makes that
	// cost latency rather than correctness, and it can only do so by declining to
	// apply the same filter. It has already established the record moved past our own
	// acknowledged sequence, which is the fact the filter exists to approximate.
	as.deliverSessionRecordObservation(idHash, smsg.Sequence, nil)
}

// settleRejectionFromSweep probes one UNBOUND session's outstanding record rejection
// and follows the verdict.
//
// The gate is the sole unboundness check on this path: a bound session's marker simply has
// its claim refused and waits for the connection to end, at which point this same pass
// settles it. See lockSessionForRecordRejection.
//
// Runs from the periodic sweep goroutine. No lock held on entry.
func (as *mqttAccountSessionManager) settleRejectionFromSweep(s *Server, sess *mqttSession) {
	if err := as.lockSessionForRecordRejection(sess); err != nil {
		return
	}
	defer as.unlockSession(sess)
	switch sess.settleRecordRejection(as) {
	case mqttRejectionUnresolved:
		// The probe's own write is ambiguous. Resolve it in THIS iteration with the gate
		// still held — that is resolveUncertainSession's contract, and the wrapper form
		// would claim the same gate and fail against our own hold.
		s.resolveUncertainSession(as, sess)
	case mqttRejectionOurs:
		// The probe IS the backstop: every landed save credits the void-register
		// revision it marshalled, so a gap it covered is closed by it, and a nonce
		// staged after its marshal is the release handoff's to converge.
	case mqttRejectionForeign, mqttRejectionForked:
		// Disposed either way — settleRecordRejection disposes on both — so nothing
		// else may touch this session, backstop included. Named rather than left to
		// the default, which means "nothing was established" and would describe a
		// disposed session as one whose marker is still armed.
	default: // mqttRejectionKeep
		// Ownership was not established, so the backstop's save would be the same doomed
		// CAS. The marker stays armed and the next pass asks again.
	}
}

// flushVoidVerdictGap is the periodic BACKSTOP for the release handoff.
//
// The handoff rides lock releases, and one attempt per newly staged revision is what keeps
// it from retrying in a storm — but the same watermark means a DEFINITE store failure ends
// the sequence: the revision is no longer above it, so no release triggers again. An
// unbound, never-expiring session has no traffic and no timer either, so the verdict would
// stay memory-only until something else happened to stage a nonce, which may be never; a
// crash then restores the register as an ordinary live delivery, holding its identifier and
// its Receive Maximum slot. A dependency outage is exactly when that is likeliest.
//
// So the periodic pass re-arms it: the watermark is put back below the revision and the
// ordinary attempt runs, under the same ownership gate and the same trampoline as every
// other trigger. Re-arming rather than bypassing keeps ONE path to the store; the sweep
// interval is the retry cadence, which is the right one for a failure that is by definition
// not transient-looking.
//
// Runs from the periodic sweep goroutine, for sessions that are not unresolved (an
// unresolved one is the resolver's, and its own releases re-check afterwards). No lock held
// on entry.
func (as *mqttAccountSessionManager) flushVoidVerdictGap(sess *mqttSession) {
	sess.mu.Lock()
	// The pending condition minus the watermark: that is the conjunct being re-armed.
	//
	// Minus the WAITER conjunct too, deliberately, and that is load-bearing: re-arming
	// while a blocking writer is announced for the fence is exactly what lets that
	// waiter's eventual release trigger the handoff. Completing the predicate here —
	// adding recordTxWaiters.Load() == 0 to owed — would leave the watermark burned, so
	// the release finds nothing above it and the retry waits a whole further sweep
	// interval, lengthening the memory-only crash window for no gain. The attempt this
	// re-arm drives still tests the waiter conjunct, so nothing barges.
	owed := !sess.seqUncertain && sess.c == nil && !sess.recordTxOpen && !sess.voidHandoffActive &&
		sess.voidedRegsRev > sess.voidedRegsSavedRev
	if owed {
		sess.voidedRegsHandoffRev = sess.voidedRegsSavedRev
	}
	sess.mu.Unlock()
	if !owed {
		return
	}
	sess.attemptVoidVerdictHandoff(nil)
}

// ---------------------------------------------------------------------------
// Classifying a failed record write for the caller's IN-MEMORY state
// ---------------------------------------------------------------------------

// saveOutcome says what a failed save means for the state the caller mutated
// before it.
//
// The distinction is not cosmetic. An ambiguous write may have landed, so committing
// to "it did not land" at failure time is a guess made at the one moment nobody can
// know. Guess wrong and the next save overwrites what did land: a drain, a claim, a
// membership, an attempt counter.
//
// (The attempted write's identity is safe either way — its digest and its exact bytes
// are snapshotted into pendingWrite when the failure is recorded, so later memory
// changes cannot alter what the resolver compares against, or re-sends.)
//
// So: roll back only a DEFINITE failure. Keep the attempted state when the write may
// have landed — the resolver settles it forward, by re-attempting those very bytes —
// and register whatever the caller withheld for want of proof. See retainOnLanded.
type saveOutcome uint8

const (
	// saveLanded: the write is durable.
	saveLanded saveOutcome = iota
	// saveDefinitelyFailed: nothing was stored (a refused fence, a rejected CAS,
	// a refusal at entry). sess.seq is still right and the caller must undo its
	// mutation — inside the still-open transaction, or another writer can persist
	// the state it is undoing.
	saveDefinitelyFailed
	// saveMaybeLanded: the request went out and its outcome is unknown. The caller
	// must KEEP its mutation; the session is unresolved until the resolver settles
	// it, and no further write is permitted meanwhile.
	saveMaybeLanded
)

// classifySave turns a save error into an outcome. wasUncertain is
// isSeqUncertain() sampled BEFORE the write: without it, a save refused at entry
// because an EARLIER write is unresolved would be misread as this one having
// possibly landed, and the caller would keep a mutation that never left the
// process.
func (sess *mqttSession) classifySave(wasUncertain bool, err error) saveOutcome {
	if err == nil {
		return saveLanded
	}
	if !wasUncertain && sess.isSeqUncertain() {
		return saveMaybeLanded
	}
	return saveDefinitelyFailed
}

// saveClassified is save() plus the classification, for the common case where the
// caller does its own write.
func (sess *mqttSession) saveClassified(tok recordTx) (saveOutcome, error) {
	wasUncertain := sess.isSeqUncertain()
	err := sess.save(tok)
	return sess.classifySave(wasUncertain, err), err
}

// mqttForcedLeaveOwnership is what the 3.1.1 forced leave's commit concluded about this
// session's record state, sampled after its own cleanup writes and BEFORE it releases any group
// consumer.
//
// It exists because the commit performs both kinds of operation: session-record writes, which
// can leave the record unresolved or arm the rejection marker, and a CONSUMER.DELETE on an
// interest-retained stream, which destroys the backlog of every member that still holds the
// group. The second must not happen while the first is unsettled, and "notice afterwards and
// close the connection" is not a remedy — the delete has already happened.
type mqttForcedLeaveOwnership int

const (
	// mqttForcedLeaveOwnershipOurs: the record is still this session's, either because nothing
	// was left unsettled or because the resolution or the probe proved it. The only verdict that
	// licenses releasing a group consumer.
	mqttForcedLeaveOwnershipOurs mqttForcedLeaveOwnership = iota
	// mqttForcedLeaveOwnershipForeign: the record belongs to another owner (or to a sibling
	// nothing can attribute). Release nothing and stop serving.
	mqttForcedLeaveOwnershipForeign
	// mqttForcedLeaveOwnershipUnresolved: neither the resolution nor the probe could settle it.
	// Release nothing; the periodic pass settles it later.
	mqttForcedLeaveOwnershipUnresolved
)

func (v mqttForcedLeaveOwnership) String() string {
	switch v {
	case mqttForcedLeaveOwnershipOurs:
		return "ours"
	case mqttForcedLeaveOwnershipForeign:
		return "foreign"
	default:
		return "unresolved"
	}
}

// recordStateBlocksServing reports whether this session's own durable state forbids the
// connection proceeding: an unresolved write, or an armed rejection whose ownership question is
// still outstanding.
//
// It exists so a caller that has just run a compound operation does not have to enumerate which
// of that operation's writes might have failed and how. Every session-record write goes through
// persistRecord, which arms one or the other for any STORE error, so sampling the session
// afterwards covers writes the caller never named — including ones a later change adds. It does
// NOT cover a pre-store refusal (the save hook, a rejected transaction, an entry refusal for an
// earlier uncertainty), and it does not need to: none of those is evidence about ownership, and
// each is already visible to its own caller as a definite failure.
//
// ONE accessor rather than two calls, because the two predicates have different locking rules:
// recordRejectionGatesServingLocked requires sess.mu while isSeqUncertain takes it itself, so
// composing them naively is a lock error rather than merely untidy.
//
// No lock held on entry.
func (sess *mqttSession) recordStateBlocksServing() bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.seqUncertain || sess.recordRejectionGatesServingLocked()
}

// saveClassifiedDetail is saveClassified plus the attempted write's raw
// identity, for the callers that must turn a DEFINITE CAS rejection into a
// decision by kind — 10071 (the subject moved) versus 10164 (contention) — and
// retry or read back the exact bytes they sent. save() itself re-formats the
// store error and destroys the kind, which is why the detail is captured at the
// boundary.
//
// There are SEVERAL such callers, not one: the pre-CONNACK classification shared
// by the reconciliation phase and the lifetime grant, the shared-membership
// ownership write, and the 3.1.1 forced leave. And 10071 says the subject moved,
// which is weaker than "taken over" — see F-084. See
// classifyPreConnackSave in sess_resume_reconcile.go.
func (sess *mqttSession) saveClassifiedDetail(tok recordTx) (saveOutcome, mqttSessSaveDetail, error) {
	wasUncertain := sess.isSeqUncertain()
	detail, err := sess.saveWithDetail(tok)
	return sess.classifySave(wasUncertain, err), detail, err
}
