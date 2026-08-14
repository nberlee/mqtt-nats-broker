// A durable identity for the interval a client's session record subject is
// currently in — the one thing a create can inherit when no retirement history
// survives to name a parent.
//
// F-029b parents a record-absent create to the record whose deletion emptied the
// subject, read off the generation-retirement watermark's RWID (sess_teardown.go).
// That closes the create x create race wherever a valid watermark exists. Where the
// watermark read comes back ABSENT there is nothing to inherit, the create stays
// parentless, and classifyRecordLineage requires a non-empty acknowledged parent
// precisely so two unrelated first creates are not read as siblings — so the erased
// side concedes 0x8E and CANCELS its delayed Will, where a recognised fork would
// have reported 0x89 and preserved it (mqtt.go, sess_takeover_redrive.go). Absent is
// three shapes: a genuine first-ever interval, a client whose retirement history was
// LOST ($MQTT_teardown recreated alone, an administrative deletion, a partial
// restore), and a transient read.
//
// AN ABSENCE IS NOT A FACT, so the fix cannot be a reserved token stamped on that
// arm: a design that did was refuted, because it mints a positive claim out of
// missing evidence and its failure direction is a FALSE fork — the one direction
// F-029b exists to prevent, where every other row concedes. What is needed is a
// positive durable fact of the interval itself.
//
// WHY THE IDENTITY MUST ADVANCE, NOT MERELY PERSIST. A permanent per-client token
// would be shared by every interval of that client, and a shared parent pointer is a
// false fork the moment two records carrying it are classifier-visible to each other.
// Visibility is NOT durable co-existence: classifyRecordLineage compares a broker's
// in-memory ACKNOWLEDGED pair against whatever record exists now, so a broker whose
// own record has already been deleted is still a counterparty. One stranded owner
// plus one erased watermark is enough. The identity therefore has exactly one rule:
// it MOVES FORWARD at every interval boundary, and no two intervals ever hold the
// same value.
//
// THE VALUE IS A SEQUENCE, NOT A BODY FIELD. Every store gets a fresh sequence
// assigned at propose time and applied in log order (server/stream.go), so a delayed
// or duplicated writer replaying identical bytes produces a DIFFERENT token, never a
// reused one. A body field cannot promise that: identical bytes are exactly what a
// re-sent claim produces. The token pairs that sequence with the register stream's own
// incarnation epoch, because sequences restart at 1 in a new incarnation.
//
// WHY THE ADVANCE CARRIES NO IDEMPOTENCY GUARD. Duplicate finishers are ordinary here
// — finishAdoptedTeardown is built for the original owner and adopters to finish one
// marker concurrently and repeatedly — so an unconditional advance rotates the
// register more than once for a single retirement, and two creates straddling the
// second rotation derive different tokens. Three guards were designed and each was
// worse than that: MarkerSeq (one retirement occupies several $MQTT_sess sequences),
// the claim nonce (it proves duplicate claim BYTES, not a duplicate BOUNDARY, so a
// marker replayed after its release declines the advance and one token spans two
// intervals), and the retired record's WID (commutative within one retirement but it
// does not ORDER different ones, so a paused R1 finisher resuming after R2 released
// REGRESSES the register). The pattern is that any guard which can DECLINE a write can
// leave an earlier interval's identity standing, which is the fatal direction, while an
// unguarded advance can only ever mint a fresh sequence, which no earlier interval
// holds. So the divergence is taken and stated: two brokers deriving different tokens
// degrades to exactly today's missed recognition. See docs/CONFORMANCE.md.
//
// This is not a second mechanism stating the watermark's fact. The watermark says
// WHICH generation was retired and WHICH record its deletion removed, on a stream an
// operator can prune — and that loss is the case being closed here. The register says
// which INTERVAL the subject is in, on a stream whose sequences only move forward, so
// no erasure can hand an identity back. They are read by disjoint arms: RWID answers the create whose watermark is valid,
// the register answers the create whose watermark is absent.
//
// See docs/ARCHITECTURE.md and the residuals in docs/CONFORMANCE.md.

package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// mqttOriginAttempts bounds both the create-side establish (re-read after a rejected
// create-only CAS) and the teardown-side advance (re-attempt of a write that could
// not be confirmed). Same shape and reason as mqttGenRetirementAttempts: a bounded
// number of round trips inside a record fence, never an unbounded loop.
const mqttOriginAttempts = 3

// mqttOriginInterval is the per-client register body. It is a DIAGNOSTIC record with
// one load-bearing field.
//
// ID is load-bearing and must equal the connecting client's ID exactly, not merely
// hash back to the subject: getHash keeps eight SHA-derived base-62 characters, so it
// is collision-resistant and NOT injective, and two client IDs sharing a hash sharing
// one register would share one parent pointer — unrelated first creates reported as
// siblings, the fatal direction. The record path already makes the same strong
// comparison (errMQTTSessionCollision).
//
// Nonce, MarkerSeq, Gen, Origin and StartedAt say WHICH retirement last advanced the
// register, for a human reading it. NOTHING decides on them, and they are deliberately
// not validated: a diagnostic field that could fail validation would be able to brick
// a client's interval identity, and the token is derived from the message's SEQUENCE
// rather than from anything in here.
type mqttOriginInterval struct {
	ID        string `json:"id"`
	Nonce     string `json:"nonce"`
	MarkerSeq uint64 `json:"mseq"`
	Gen       string `json:"gen"`
	Origin    string `json:"origin"`
	StartedAt int64  `json:"started"`
}

// mqttOriginStatus is the typed outcome of a register load. Absent is a real,
// actionable outcome here rather than a hole — it is what the FIRST create of a client
// ID sees, and the one that establishes the register.
type mqttOriginStatus int

const (
	mqttOriginAbsent        mqttOriginStatus = iota // no register for this client hash
	mqttOriginValid                                 // decoded, and names a client hashing to this subject
	mqttOriginCorrupt                               // decode failure, or a name that does not hash back
	mqttOriginIndeterminate                         // transient load failure
)

// mqttOriginSubject returns the per-client register subject. Inside the reserved
// "$MQTT.>" namespace, which both the client SUBSCRIBE and PUBLISH guards deny
// wholesale — so the register is unforgeable by construction rather than by ACL
// configuration.
func mqttOriginSubject(domainTk, idHash string) string {
	return mqttOriginSubjectPrefix + domainTk + idHash
}

// mqttGenOriginEpoch generates a fresh $MQTT_origin incarnation epoch. Same form as
// the session-stream epoch, and validated by the same predicate: an epoch is an
// identity token compared for equality only.
func mqttGenOriginEpoch() string { return mqttGenSessEpoch() }

// mqttOriginToken derives the interval identity a create stamps as its record's
// parent: the register stream's incarnation epoch and the SEQUENCE the standing
// register message occupies.
//
// Both halves are required. The sequence is what advances — no two writes to the
// stream ever share one, whatever their bytes — and the epoch is what keeps two
// incarnations of the stream from handing out the same token, since sequences restart
// at 1 in a new one.
//
// Empty is returned for an unusable input rather than a partially-formed token, and
// empty CONCEDES at every call site: a malformed epoch means provisioning did not
// adopt one (it refuses the stream instead), and a landed sequence is never 0.
func mqttOriginToken(epoch string, seq uint64) string {
	if !mqttValidSessEpoch(epoch) || seq == 0 {
		return _EMPTY_
	}
	return epoch + "." + strconv.FormatUint(seq, 10)
}

// mqttValidateOriginInterval checks the ONE structural fact a register must carry:
// that it names a client hashing back to the subject it was read from. Everything
// else in the body is diagnostic, so validating it would let a cosmetic field refuse
// an identity nothing decides on.
//
// The exact client-ID comparison is deliberately NOT here: it needs the connecting
// client's ID, which only the create arm holds, and its outcome is different in kind
// — a well-formed register belonging to another client that shares this hash is not
// corrupt, it is somebody else's, and the create concedes to it rather than replacing
// it.
func mqttValidateOriginInterval(r *mqttOriginInterval, idHash string) error {
	if r == nil {
		return fmt.Errorf("no interval register")
	}
	if getHash(r.ID) != idHash {
		return fmt.Errorf("register names a client ID that does not hash back to %q", idHash)
	}
	return nil
}

// validateOriginStream verifies a $MQTT_origin stream carries the exact
// permanent-register configuration. Derived from what this owner does: a create-only
// per-client CAS (the establish), a plain confirmed publish that replaces the
// standing register (the advance), and exact-subject loadLastMsgFor reads — and
// NOTHING else. No scan, no consumer, no delete, no purge.
//
// DenyDelete and DenyPurge are audited NON-prerequisites, exactly as on the two
// permanent-watermark streams, and the reasoning that once required them here was
// wrong: a delete does not rewind the stream's sequence counter. An erased register
// sends the next create to the establish, which mints a FRESH sequence higher than
// anything issued before, so deletion can never hand back an identity an earlier
// interval held. What it costs is the opposite direction — the erased client's next
// record-absent create derives an identity no peer holds, so a pair straddling the
// erasure concedes. That is a recognition loss of exactly the kind administrative
// pruning of $MQTT_teardown already causes, stated in
// docs/CONFORMANCE.md rather than refused here; only stream RECREATION or a restore
// replays sequences, which the incarnation epoch is what guards.
//
// Global message/byte/age limits are rejected, and NOT for the reason an earlier
// revision gave: eviction cannot replay an identity any more than a delete can, since
// neither rewinds the sequence counter. They are rejected because they would reap
// registers CONTINUOUSLY, silently and without a diagnostic — disabling this
// recognition for every client whose register ages out, at a scale and a moment nobody
// chose. That is the same "permanent by design" ground the two watermark streams reject
// them on, and it is why it is not in tension with permitting a deliberate
// administrative prune, whose cost an operator takes knowingly and which is stated as a
// residual (R0f). DiscardNew would reject the advance instead of replacing the standing
// register. A source could occupy a client's slot. A size cap would reject the write
// that gates every deletion.
//
// sessEpoch is the $MQTT_sess incarnation epoch this account adopted, and this stream
// is epoch-COUPLED to it exactly as the two watermark streams are, so the detectable
// direction of a partial recreation is refused at startup. The other direction
// ($MQTT_origin recreated alone) stamps the current session epoch and validates
// clean; it is safe only under the mandatory restart the operational constraints
// already require, because as.originStreamEpoch is adopted once at initialization.
// See docs/ARCHITECTURE.md and docs/CONFORMANCE.md.
//
// wantReplicas is 0 when no explicit replica count is configured (skip that check).
func validateOriginStream(si *StreamInfo, domainTk string, wantReplicas int, sessEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttOriginSubjectPrefix + domainTk + ">"}, wantReplicas); err != nil {
		return err
	}
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits): nothing consumes an interval register, so any other retention drops it immediately", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		return fmt.Errorf("discard is %v (want old): DiscardNew would reject the advance that replaces a client's standing register", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		return fmt.Errorf("max_msgs_per_subject is %d (want 1): the per-client register is a single slot, and the create-only CAS that establishes it depends on that", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would silently reap interval registers, disabling cross-term create recognition for every client whose register ages out, with no diagnostic",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a client's register slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject the register advance that must land before any teardown deletion", cfg.MaxMsgSize)
	}
	stamp := cfg.Metadata[mqttOriginEpochMetaKey]
	if !mqttValidSessEpoch(stamp) {
		return fmt.Errorf("metadata %q is %q, not a well-formed stream-incarnation epoch: an interval identity is this stream's sequence, which is only unique within the incarnation that assigned it",
			mqttOriginEpochMetaKey, stamp)
	}
	sess := cfg.Metadata[mqttSessEpochMetaKey]
	if !mqttValidSessEpoch(sess) {
		return fmt.Errorf("metadata %q is %q, not a well-formed session-stream incarnation epoch: the register is coupled to the session stream whose records carry the identities it hands out",
			mqttSessEpochMetaKey, sess)
	}
	if sess != sessEpoch {
		return fmt.Errorf("metadata %q is %q but this account's session stream is incarnation %q: these registers were written against a different $MQTT_sess",
			mqttSessEpochMetaKey, sess, sessEpoch)
	}
	return nil
}

// Test hooks over the register's two JetStream operations, atomic for the same reason
// every other seam here is: a connection's readLoop and a teardown finisher both
// reach them.
//
// The load hook returns the classified outcome directly, so a test can present an
// absent, corrupt, foreign or unreadable register to any reader. The store hook
// receives cas=true for the create-side establish and cas=false for the teardown-side
// advance, which is what lets a test assert the advance is an UNCONDITIONAL publish
// rather than a conditional write — the property three refuted guards were removed to
// obtain.
var (
	mqttTestOriginLoadHookV  atomic.Pointer[func(idHash string) (*mqttOriginInterval, uint64, mqttOriginStatus)]
	mqttTestOriginStoreHookV atomic.Pointer[func(subject string, cas bool, r *mqttOriginInterval) (uint64, error)]
)

// setMQTTTestOriginLoadHook installs (or, with nil, clears) the register load hook
// and returns a function that restores the previous value.
func setMQTTTestOriginLoadHook(h func(idHash string) (*mqttOriginInterval, uint64, mqttOriginStatus)) func() {
	prev := mqttTestOriginLoadHookV.Load()
	if h == nil {
		mqttTestOriginLoadHookV.Store(nil)
	} else {
		mqttTestOriginLoadHookV.Store(&h)
	}
	return func() { mqttTestOriginLoadHookV.Store(prev) }
}

// setMQTTTestOriginStoreHook installs (or, with nil, clears) the register store hook
// and returns a function that restores the previous value.
func setMQTTTestOriginStoreHook(h func(subject string, cas bool, r *mqttOriginInterval) (uint64, error)) func() {
	prev := mqttTestOriginStoreHookV.Load()
	if h == nil {
		mqttTestOriginStoreHookV.Store(nil)
	} else {
		mqttTestOriginStoreHookV.Store(&h)
	}
	return func() { mqttTestOriginStoreHookV.Store(prev) }
}

// loadOriginInterval is the ONE read both the create arm and any future reader use,
// honoring the test seam so an injected register is seen identically by all of them.
func (jsa *mqttJSA) loadOriginInterval(domainTk, idHash string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
	if h := mqttTestOriginLoadHookV.Load(); h != nil {
		return (*h)(idHash)
	}
	return jsa.loadOriginIntervalRecord(domainTk, idHash)
}

// loadOriginIntervalRecord reads a client's register and classifies it. The subject
// sequence is returned on every non-absent outcome (it is available pre-decode)
// because it, and not the body, is the identity being read.
func (jsa *mqttJSA) loadOriginIntervalRecord(domainTk, idHash string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
	smsg, err := jsa.loadLastMsgFor(mqttOriginStreamName, mqttOriginSubject(domainTk, idHash))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, mqttOriginIndeterminate
		}
		return nil, 0, mqttOriginAbsent
	}
	r := &mqttOriginInterval{}
	if uerr := json.Unmarshal(smsg.Data, r); uerr != nil {
		return nil, smsg.Sequence, mqttOriginCorrupt
	}
	if verr := mqttValidateOriginInterval(r, idHash); verr != nil {
		return r, smsg.Sequence, mqttOriginCorrupt
	}
	return r, smsg.Sequence, mqttOriginValid
}

// storeOriginInterval is the ONE write, honoring the test seam. cas selects the two
// shapes this owner uses and nothing else: a create-only expected-last-subject-
// sequence CAS for the establish (so two racing first creates cannot both mint an
// identity), and a plain confirmed publish for the advance (so a rotation can never
// be DECLINED — see the file header).
func (jsa *mqttJSA) storeOriginInterval(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
	if h := mqttTestOriginStoreHookV.Load(); h != nil {
		return (*h)(subject, cas, r)
	}
	return jsa.storeOriginIntervalRecord(subject, cas, r)
}

// storeOriginIntervalRecord is the raw write, separate from the seam above for the same
// reason loadOriginIntervalRecord is: a hook that wants to OBSERVE a call and let it
// through needs a path that does not re-enter the hook. Clearing and restoring the
// pointer around a delegated call would be a race as soon as two brokers write
// concurrently, which is exactly what the register's own arbitration test does.
func (jsa *mqttJSA) storeOriginIntervalRecord(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
	b, _ := json.Marshal(r)
	if !cas {
		resp, err := jsa.storeMsg(subject, 0, b)
		if err != nil {
			return 0, err
		}
		return resp.Sequence, nil
	}
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":0")
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

// originIntervalToken derives the interval identity a record-absent create stamps as
// the parent of its first write, establishing the register when this is the first
// create this client ID has ever had.
//
// Called ONLY after the session-record subject has been proven empty AND the
// retirement watermark read has returned ABSENT. Both orderings are load-bearing: a
// watermark that is merely unread establishes no retirement fact, so deriving an
// identity there could stamp one interval's identity onto another.
//
// EMPTY MEANS CONCEDE, and every failure returns empty. Nothing here ever fails a
// CONNECT: the register is a recognition aid, and a create that cannot derive one
// behaves exactly as the broker did before this existed.
//
// The dispositions, each for a reason a shorter rule would get wrong:
//
//   - valid, and ours -> the token is the sequence it stands at.
//   - valid, but naming a DIFFERENT client that shares this hash -> concede, and do
//     NOT establish over it. The subject is occupied, and replacing it would hand
//     both clients one identity. Refusing the CONNECT instead is not available: the
//     register cannot be deleted, so a refusal would brick that client ID forever.
//     The two clients simply deny each other recognition.
//   - absent -> establish with a create-only CAS, and take the token from OUR OWN
//     confirmed sequence. A rejection is never read as "a register exists" (the
//     server folds a load error into 10071, and 10164 is contention), so it is
//     resolved by an authoritative re-read, bounded.
//   - corrupt -> concede without establishing, for the same occupied-subject reason.
//     The next teardown replaces it unconditionally.
//   - indeterminate, or an establish that failed for any other reason -> concede.
func (as *mqttAccountSessionManager) originIntervalToken(clientID, idHash string) string {
	// Every concession below is REPORTED. The CONNECT still succeeds — that is the
	// point of conceding — but a client whose identity cannot be derived silently loses
	// cross-term fork recognition and takes the takeover disposition instead, and a
	// stream permission fault or a corrupt register can leave it that way indefinitely.
	// Refusing automatic eviction on the ground that it degrades this feature "silently
	// and without a diagnostic" would be hollow if the runtime paths did the same.
	concede := func(why string, err error) string {
		if err != nil {
			as.jsa.c.Warnf("MQTT: no session-interval identity for client %q: %s: %v; this CONNECT is "+
				"accepted, but a concurrent create for this client cannot be recognised as a sibling "+
				"and would be told the session was taken over", clientID, why, err)
		} else {
			as.jsa.c.Warnf("MQTT: no session-interval identity for client %q: %s; this CONNECT is "+
				"accepted, but a concurrent create for this client cannot be recognised as a sibling "+
				"and would be told the session was taken over", clientID, why)
		}
		return _EMPTY_
	}
	for attempt := 0; attempt < mqttOriginAttempts; attempt++ {
		r, seq, status := as.jsa.loadOriginInterval(as.domainTk, idHash)
		switch status {
		case mqttOriginValid:
			if r.ID != clientID {
				// A hash collision, not corruption: the register is well-formed and
				// belongs to somebody else.
				return concede("its register subject is occupied by another client ID sharing the "+
					"same hash, which is left in place because it cannot be deleted", nil)
			}
			return mqttOriginToken(as.originStreamEpoch, seq)
		case mqttOriginCorrupt:
			return concede("its register is unreadable and is left in place rather than overwritten; "+
				"the next teardown of this client replaces it", nil)
		case mqttOriginIndeterminate:
			return concede("its register could not be read", nil)
		}
		// Absent: this client ID has no interval identity yet.
		want := &mqttOriginInterval{
			ID:        clientID,
			Origin:    as.jsa.id,
			StartedAt: time.Now().Unix(),
		}
		mine, err := as.jsa.storeOriginInterval(mqttOriginSubject(as.domainTk, idHash), true, want)
		if err == nil {
			return mqttOriginToken(as.originStreamEpoch, mine)
		}
		if !mqttIsRecordCASRejection(err) {
			// Not a lost race — a store that failed. Concede rather than retry into
			// an unbounded stall on a CONNECT path.
			return concede("its register could not be established", err)
		}
		// Rejected: somebody else established, or clustered contention. Neither is
		// evidence a register exists, so the next iteration's read is what settles it.
	}
	return concede("its register was contended for the whole retry bound", nil)
}

// advanceOriginInterval rotates the client's interval identity, and is called by
// recordGenRetirement between the won claim and the first deletion — after its own
// validation of the watermark it is about to write, and BEFORE the watermark decision
// loop, whose "a standing fence is already at or above ours" early return would
// otherwise skip the rotation on exactly the path that still deletes the record.
//
// Living inside recordGenRetirement rather than at its two callers is what makes it
// structurally impossible to record a retirement without rotating the identity, and
// any future writer inherits it.
//
// UNCONDITIONAL, by design: no read, no CAS, no equality guard. MaxMsgsPer:1 replaces
// the standing register and the identity every reader derives is the sequence this
// write lands at. A duplicate finisher therefore rotates a second time, which is
// harmless in the only direction that matters — a fresh sequence is one no earlier
// interval ever held — and that is precisely why three refuted guards were removed
// rather than repaired. See the file header.
//
// A write that cannot be CONFIRMED is re-attempted, bounded, and then fails, which
// aborts the teardown with NOTHING deleted — the same self-healing state the
// watermark write's own failure leaves. A re-attempt that lands twice is two
// rotations, which is the harmless direction again.
func (sess *mqttSession) advanceOriginInterval(t *mqttTeardownTarget, nonce string, markerSeq uint64) error {
	want := &mqttOriginInterval{
		ID:        t.id,
		Nonce:     nonce,
		MarkerSeq: markerSeq,
		Gen:       t.gen,
		Origin:    sess.jsa.id,
		StartedAt: time.Now().Unix(),
	}
	if verr := mqttValidateOriginInterval(want, t.idHash); verr != nil {
		// Refusing to write what we would refuse to read. Reached only if the teardown
		// target names a client that does not hash back to its own subject, which the
		// watermark validation ahead of this already refuses.
		return fmt.Errorf("refusing to advance the interval register of session %q: %w", t.id, verr)
	}
	subject := mqttOriginSubject(sess.domainTk, t.idHash)
	var lastErr error
	for attempt := 0; attempt < mqttOriginAttempts; attempt++ {
		if _, err := sess.jsa.storeOriginInterval(subject, false, want); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return fmt.Errorf("unable to advance the interval register of session %q after %d attempts: %w",
		t.id, mqttOriginAttempts, lastErr)
}
