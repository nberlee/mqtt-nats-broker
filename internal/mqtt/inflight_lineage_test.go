package mqtt

// The exchange LINEAGE: an identity that survives a replacement which inherits a client's
// obligation, where the register NONCE deliberately does not.
//
// The distinction these tests hold is the one the replacement arm already states in its own
// comments: it re-mints the nonce so the previous writer's cleanup is fenced off the new
// record, and in the same breath it rewrites the register alone "with no marker minted for an
// exchange the client may already hold" and preserves the occupant's QoS because that is
// "evidence about what the CLIENT may already be holding". So after that arm the record is a
// NEW allocation for an OLD obligation, and the lineage is what names the obligation.
//
// Every test here observes the PERSISTED record. The lineage is a durable fact; asserting it
// on the in-memory entry alone would pass against a writer that never wrote it.

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/nats-io/nuid"
)

// TestMQTTFreshAllocationStartsItsOwnLineage is also this file's POSITIVE CONTROL: it fails
// against an implementation that writes no lineage at all, so a "refuse everything" or
// "write nothing" mutation cannot pass the rest of the file by making every negative
// assertion trivially true.
func TestMQTTFreshAllocationStartsItsOwnLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fresh-lineage")
	dur := e.sess.idHash + "_" + nuid.Next()

	pi := e.track(t, dur, 100, 2)

	rec, smsg := e.loadRegister(t, pi)
	if rec == nil || smsg == nil {
		t.Fatalf("no register was stored for pi %d", pi)
	}
	if !mqttValidInflightNonce(rec.Lin) {
		t.Fatalf("a fresh allocation must carry a well-formed lineage, got %q", rec.Lin)
	}
	// A create-only CAS proves the subject was empty, so there was nothing to inherit from.
	if rec.Lin != rec.Nonce {
		t.Fatalf("a fresh allocation's lineage must be its own nonce, got lin=%q nonce=%q",
			rec.Lin, rec.Nonce)
	}
}

// TestMQTTInheritingReplacementCarriesTheLineage is the file's central assertion.
//
// The predecessor's lineage is CAPTURED FROM THE PERSISTED RECORD BEFORE the replacement
// runs. Taking it from the replacement's own result instead would be self-referential: a
// mutation that re-minted the lineage would be compared against the value it had just
// minted, and the test would pass while proving nothing.
func TestMQTTInheritingReplacementCarriesTheLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-inherit-lineage")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sseq = uint64(7)

	// OUR allocation first, so the store below meets the occupant on OUR subject. Seeding
	// the occupant under a different identifier would never reach the replacement arm --
	// the conflict is on the subject, and the subject is keyed by the identifier.
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	ourNonce := e.nonceOf(pi)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

	// THE OCCUPANT: the same delivery, written by another broker, with NO unsent marker.
	// Absent is deliberately not read as "transmitted" -- send intent deletes the marker
	// before the bytes leave -- so it means POSSIBLY-ATTEMPTED, which is the state that
	// carries a client obligation forward and the one this arm must inherit.
	predecessorNonce := mqttNewInflightNonce()
	predecessorLin := mqttNewInflightNonce() // not equal to its nonce: an heir of an heir
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, &mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: dur, Seq: sseq, QoS: 2,
		SourceAckReply: testInflightAckSubject(dur, sseq, 1),
		Nonce:          predecessorNonce, Lin: predecessorLin, Claim: mqttNewInflightNonce(),
	}); err != nil {
		t.Fatalf("seed the occupant: %v", err)
	}
	if load := e.asm.loadInflightMarker(mqttMarkerCallerStore, subject); load.kind != mqttInflightResponseAbsent {
		t.Fatalf("the occupant must have NO marker for the register-only arm, got kind=%v", load.kind)
	}

	testOpenStoreCustody(t, e.sess, pi)
	if !e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, ourNonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the replacement store refused")
	}

	heir, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || heir == nil {
		t.Fatalf("load the heir register: %v", err)
	}
	// ROUTE WITNESS: the replacement really ran, so the assertion below is about the
	// inheriting arm and not about a record nothing rewrote.
	if heir.Nonce != ourNonce {
		t.Fatalf("no replacement occurred: the record carries %q, not our nonce %q", heir.Nonce, ourNonce)
	}
	// And the marker was NOT minted, which is what distinguishes this arm from the pair one.
	if load := e.asm.loadInflightMarker(mqttMarkerCallerStore, subject); load.kind == mqttInflightResponsePresent {
		t.Fatal("the register-only arm must mint no marker for an exchange the client may hold")
	}
	if heir.Lin != predecessorLin {
		t.Fatalf("an inheriting replacement must carry the predecessor's lineage: got lin=%q want %q (heir nonce %q)",
			heir.Lin, predecessorLin, heir.Nonce)
	}
}

// A replacement over a PROVEN-UNSENT occupant -- its marker present and naming it -- starts a
// fresh lineage. Nothing was ever put on the socket under the occupant, so there is no
// client obligation to inherit and no durable fact can have been recorded about one.
func TestMQTTProvenUnsentReplacementStartsAFreshLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-unsent-lineage")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sseq = uint64(11)

	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	ourNonce := e.nonceOf(pi)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

	// The occupant is written as a PAIR, so its marker names it: proven-unsent.
	predecessorNonce := mqttNewInflightNonce()
	predecessorLin := mqttNewInflightNonce()
	if _, err := e.asm.jsa.storeInflightPair(subject, 0, 0, &mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: dur, Seq: sseq, QoS: 2,
		SourceAckReply: testInflightAckSubject(dur, sseq, 1),
		Nonce:          predecessorNonce, Lin: predecessorLin, Claim: mqttNewInflightNonce(),
	}); err != nil {
		t.Fatalf("seed the occupant pair: %v", err)
	}
	// Asserted rather than assumed: the marker is what selects the arm, and a fixture whose
	// marker had not landed would silently exercise the OTHER one.
	if load := e.asm.loadInflightMarker(mqttMarkerCallerStore, subject); load.kind != mqttInflightResponsePresent ||
		load.nonceOf() != predecessorNonce {
		t.Fatalf("the occupant's marker must be present and name it, got kind=%v nonce=%q",
			load.kind, load.nonceOf())
	}

	testOpenStoreCustody(t, e.sess, pi)
	if !e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, ourNonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the replacement store refused")
	}

	heir, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || heir == nil {
		t.Fatalf("load the heir register: %v", err)
	}
	if heir.Nonce != ourNonce {
		t.Fatalf("no replacement occurred: the record carries %q, not our nonce %q", heir.Nonce, ourNonce)
	}
	if heir.Lin == predecessorLin {
		t.Fatalf("a proven-unsent replacement must NOT inherit: lineage %q survived from the occupant", heir.Lin)
	}
	if heir.Lin != heir.Nonce {
		t.Fatalf("a proven-unsent replacement starts its own lineage: got lin=%q nonce=%q", heir.Lin, heir.Nonce)
	}
}

// THE FALLBACK. An attempt that selected an INHERITED lineage and then failed must not carry
// that lineage into its retry: the retry writes a create-only pair, and a create-only CAS
// proves the subject was empty, so the record it lands is a genuinely NEW exchange.
//
// This is the schedule that makes the fallback observable. Starting from an absent or
// cancelled occupant never contaminates the record in the first place, so the mutation
// "keep the inherited lineage across the reset" would pass against such a fixture while
// durably stamping a new exchange with an obligation the client already discharged.
func TestMQTTFailedInheritingReplacementDoesNotCarryItsLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fallback-lineage")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sseq = uint64(21)

	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	ourNonce := e.nonceOf(pi)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

	// An occupant with NO marker, so attempt 1 selects the INHERITING arm and contaminates
	// rec.Lin with the predecessor's lineage.
	predecessorNonce := mqttNewInflightNonce()
	predecessorLin := mqttNewInflightNonce()
	occSeq, err := e.asm.jsa.storeInflightCAS(subject, 0, &mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: dur, Seq: sseq, QoS: 2,
		SourceAckReply: testInflightAckSubject(dur, sseq, 1),
		Nonce:          predecessorNonce, Lin: predecessorLin, Claim: mqttNewInflightNonce(),
	})
	if err != nil {
		t.Fatalf("seed the occupant: %v", err)
	}

	// The REPLACEMENT CAS fails, and the occupant is removed with it, so the reread finds
	// ABSENCE and the next attempt falls back to a create-only pair.
	//
	// Keyed on the replacement's non-zero expected sequence, not on an attempt counter. The
	// pair write delegates to the single store hook when no pair hook is installed, so a
	// counter-keyed fixture intercepts the CREATE-ONLY first attempt instead and destroys
	// the occupant before the inheriting arm is ever reached -- which is exactly how the
	// first version of this test failed its own route witness.
	var replacementSeen, fallbackSeen bool
	var inheritedProposed bool
	e.asm.mu.Lock()
	// A pair hook that simply delegates keeps attempt 1 on the real store, where it fails
	// against the occupied subject as production would.
	e.asm.testInflightStorePair = func(subj string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
		if regExpect == 0 && replacementSeen {
			fallbackSeen = true
		}
		return e.asm.jsa.storeInflightPair(subj, regExpect, markerExpect, r)
	}
	e.asm.testInflightStore = func(subj string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if expectSeq == occSeq && !replacementSeen {
			replacementSeen = true
			// ROUTE WITNESS: the replacement really did select the inherited lineage.
			inheritedProposed = r.Lin == predecessorLin
			if derr := e.asm.jsa.deleteMsg(mqttInflightStreamName, occSeq, true); derr != nil {
				return 0, derr
			}
			return 0, fmt.Errorf("the replacement CAS lost its race")
		}
		return e.asm.jsa.storeInflightCAS(subj, expectSeq, r)
	}
	e.asm.mu.Unlock()

	testOpenStoreCustody(t, e.sess, pi)
	if !e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, ourNonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the store refused after its fallback")
	}
	if !replacementSeen || !inheritedProposed {
		t.Fatal("the inheriting replacement arm was never reached, so the fallback was never contaminated")
	}
	if !fallbackSeen {
		t.Fatal("no create-only fallback ran after the failed replacement")
	}

	rec, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil {
		t.Fatalf("load after the fallback: %v", err)
	}
	if rec.Nonce != ourNonce {
		t.Fatalf("the fallback did not land our allocation: nonce %q", rec.Nonce)
	}
	if rec.Lin == predecessorLin {
		t.Fatalf("a create-only fallback must NOT carry the failed replacement's inherited lineage %q", rec.Lin)
	}
	if rec.Lin != rec.Nonce {
		t.Fatalf("a create-only fallback starts its own lineage: got lin=%q nonce=%q", rec.Lin, rec.Nonce)
	}
}

// A CUSTODY REWRITE preserves the lineage, and it does so with no code of its own: every
// rewrite rebuilds from the record it read, so the field rides along. This test is what
// would catch a rewrite that reconstructed a body from parts instead.
func TestMQTTCustodyRewritePreservesTheLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-custody-lineage")
	dur := e.sess.idHash + "_" + nuid.Next()

	pi := e.track(t, dur, 42, 2)
	before, smsg := e.loadRegister(t, pi)
	if before == nil || smsg == nil {
		t.Fatal("the register was not stored")
	}
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

	// Rewrite custody the way a resume does: rebuild from the record read, mutate only the
	// claim fields, CAS at the observed sequence.
	rewritten := *before
	rewritten.Claim = mqttNewInflightNonce()
	rewritten.ClaimSeq = smsg.Sequence + 1
	if _, err := e.asm.jsa.storeInflightCAS(subject, smsg.Sequence, &rewritten); err != nil {
		t.Fatalf("custody rewrite: %v", err)
	}

	after, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || after == nil {
		t.Fatalf("load after the rewrite: %v", err)
	}
	if after.Claim == before.Claim {
		t.Fatal("no custody rewrite occurred, so this proves nothing about preservation")
	}
	if after.Lin != before.Lin {
		t.Fatalf("a custody rewrite must preserve the lineage: got %q want %q", after.Lin, before.Lin)
	}
	if after.Nonce != before.Nonce {
		t.Fatalf("a custody rewrite must preserve the nonce too: got %q want %q", after.Nonce, before.Nonce)
	}
}

// A cancellation tombstone written CREATE-ONLY lands only on a subject the CAS proved empty,
// so it has no occupant in hand and no historical lineage to recover: it starts its own.
func TestMQTTCancellationTombstoneStartsItsOwnLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-tombstone-lineage")
	dur := e.sess.idHash + "_" + nuid.Next()
	const pi = uint16(9)
	nonce := mqttNewInflightNonce()

	if err := e.sess.markInflightRegisterCancelledPending(e.c,
		mqttRegCancellation{pi: pi, durable: dur, sseq: 5, nonce: nonce, reply: testInflightAckSubject(dur, 5, 1)}, mqttTestLicence(e.sess, nil)); err != nil {
		t.Fatalf("write the cancellation tombstone: %v", err)
	}
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	rec, smsg, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("load the tombstone: %v", err)
	}
	if !rec.Cancelled {
		t.Fatalf("the record is not a cancellation tombstone: %+v", rec)
	}
	if rec.Lin != nonce {
		t.Fatalf("a create-only tombstone's lineage is its own allocation's: got %q want %q",
			rec.Lin, nonce)
	}
}

// The tombstone's OTHER arm: when the create conflicts with our own landed record, the
// rebuild starts from that record and must PRESERVE the lineage it actually carries — which
// may differ from the nonce, because the landed record may be an inheriting heir.
func TestMQTTCancellationTombstoneRebuildPreservesALandedLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-tombstone-rebuild")
	dur := e.sess.idHash + "_" + nuid.Next()
	const pi = uint16(4)
	nonce := mqttNewInflightNonce()
	inherited := mqttNewInflightNonce()

	// A landed record carrying OUR nonce but an INHERITED lineage: the shape a replacement
	// leaves behind. The create-only tombstone will conflict with it and rebuild.
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	landed := &mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: dur, Seq: 5, QoS: 2,
		SourceAckReply: testInflightAckSubject(dur, 5, 1),
		Nonce:          nonce, Lin: inherited, Claim: mqttNewInflightNonce(),
	}
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, landed); err != nil {
		t.Fatalf("seed the landed record: %v", err)
	}

	if err := e.sess.markInflightRegisterCancelledPending(e.c,
		mqttRegCancellation{pi: pi, durable: dur, sseq: 5, nonce: nonce, reply: testInflightAckSubject(dur, 5, 1)}, mqttTestLicence(e.sess, nil)); err != nil {
		t.Fatalf("mark the landed record cancelled: %v", err)
	}
	rec, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil {
		t.Fatalf("load after the rebuild: %v", err)
	}
	// ROUTE WITNESS: the rebuild arm ran, rather than the create-only arm.
	if !rec.Cancelled {
		t.Fatalf("the rebuild did not mark the record cancelled: %+v", rec)
	}
	if rec.Lin != inherited {
		t.Fatalf("the rebuild must preserve the LANDED lineage, not re-mint from the nonce: got %q want %q",
			rec.Lin, inherited)
	}
}

// TestMQTTAmbiguousLandedRegisterInstallsThePersistedLineage is an INJECTED
// BOUNDARY-CONTRACT test, and it says so rather than pretending to be a race.
//
// In the ordinary ambiguous-success race the proposal and the landed record agree on the
// lineage, so installing either would pass — there is no reachable schedule on that route
// that tells them apart. What is worth holding is the CONTRACT: the settlement threads the
// value it is given, and its caller gives it the LANDED record's, exactly as it does for
// QoS. So the differing value is injected at the caller's own boundary.
func TestMQTTAmbiguousLandedRegisterInstallsThePersistedLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-ambiguous-lineage")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sseq = uint64(3)

	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	ourNonce := e.nonceOf(pi)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

	// The landed record carries OUR nonce and a DIFFERENT lineage -- the shape an inheriting
	// replacement leaves when its response is lost. The two values must differ, or installing
	// the proposal and installing the landed record are indistinguishable and the test proves
	// nothing.
	landedLin := mqttNewInflightNonce()

	// "Landed, response lost": the write really happens, then the hook reports a failure the
	// caller cannot classify, so the store rereads, finds its own nonce, and settles through
	// the AMBIGUOUS-SUCCESS arm -- which is the caller whose argument this test is about.
	var landed bool
	e.asm.mu.Lock()
	e.asm.testInflightStorePair = func(subj string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
		if landed {
			return e.asm.jsa.storeInflightPair(subj, regExpect, markerExpect, r)
		}
		landed = true
		// The PAIR is landed for real -- register and marker together. Landing the register
		// alone would leave the marker absent beside a just-committed pair, which the
		// settlement correctly reads as unreadable and withholds, so the fixture would never
		// reach the ambiguous-success arm it exists to exercise.
		stored := *r
		stored.Lin = landedLin
		if _, err := e.asm.jsa.storeInflightPair(subj, regExpect, markerExpect, &stored); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("response lost")
	}
	e.asm.mu.Unlock()

	testOpenStoreCustody(t, e.sess, pi)
	if !e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, ourNonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the ambiguous store did not settle")
	}
	if !landed {
		t.Fatal("the ambiguous-success route was never reached")
	}

	// ROUTE WITNESS: the record on the subject is the one the hook landed, so the settlement
	// really did have a lineage differing from the proposal in front of it.
	rec, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil {
		t.Fatalf("load the landed record: %v", err)
	}
	if rec.Nonce != ourNonce || rec.Lin != landedLin {
		t.Fatalf("the landed record is not the ambiguous one: nonce=%q lin=%q", rec.Nonce, rec.Lin)
	}

	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	var gotLin string
	var pending bool
	if ack != nil {
		gotLin, pending = ack.ev.lin, ack.regPending
	}
	e.sess.mu.Unlock()
	if ack == nil || pending {
		t.Fatalf("the entry was not confirmed against the landed register: %+v", ack)
	}
	if gotLin != landedLin {
		t.Fatalf("the settlement must install the LANDED lineage, not the proposal: got %q want %q (our nonce %q)",
			gotLin, landedLin, ourNonce)
	}
}

// A register with no lineage, or a malformed one, is an UNSUPPORTED PERSISTED SHAPE and is
// refused at the real write boundary — never defaulted to the nonce, which is the value a
// FRESH exchange gets and would silently turn an inherited obligation into a new one.
//
// Driven through storeInflightCAS, the production CAS boundary, and NOT through the
// manager's test store hook, which bypasses validation and would prove nothing. Each case
// also asserts that NOTHING WAS STORED: a refusal that still wrote the record would be worse
// than no refusal at all.
func TestMQTTRegisterWithoutLineageIsRefused(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-lineage-refusal")
	dur := e.sess.idHash + "_" + nuid.Next()
	nonce := mqttNewInflightNonce()

	// A STORAGE OBSERVER, BELOW BOTH VALIDATORS. Reading an empty subject afterwards shows
	// the record is not THERE; it does not show nothing was WRITTEN, and those are different
	// claims — a boundary that stored on another subject, or stored and removed, passes a
	// presence check.
	//
	// It counts messages on the stream rather than hooking a wrapper, and that choice is the
	// point: an earlier version installed the manager's testInflightStore hook, which the
	// JSA boundaries never call — storeInflightCAS goes to storeMsg and storeInflightPair to
	// newAtomicBatchRequest — so the counter read zero even for the SUCCESSFUL controls and
	// asserted nothing at all. A stream count is path-independent and covers the batch write
	// as well as the single one.
	onStream := func() int {
		n := 0
		for seq := uint64(1); ; {
			smsg, err := e.asm.jsa.loadNextMsgFromSeq(mqttInflightStreamName,
				mqttInflightStreamSubjectPrefix+">", seq)
			if err != nil {
				return n
			}
			n++
			seq = smsg.Sequence + 1
		}
	}

	bodies := []struct {
		name string
		lin  string
	}{
		{"absent lineage", _EMPTY_},
		{"lineage from a foreign alphabet", "!!!!!!!!!!!!!!!!!!!!!!"},
		{"lineage of the wrong length", "short"},
	}

	// BOTH REAL WRITE BOUNDARIES, not just the CAS one. The pair boundary validates
	// independently, so a defaulting or bypass introduced there would survive a CAS-only
	// test — and the create-only PAIR is the mode a fresh allocation actually uses.
	for _, tc := range bodies {
		for _, boundary := range []struct {
			name  string
			store func(subject string, r *mqttInflightRecord) (uint64, error)
		}{
			{"CAS", func(subject string, r *mqttInflightRecord) (uint64, error) {
				return e.asm.jsa.storeInflightCAS(subject, 0, r)
			}},
			{"pair", func(subject string, r *mqttInflightRecord) (uint64, error) {
				return e.asm.jsa.storeInflightPair(subject, 0, 0, r)
			}},
		} {
			t.Run(tc.name+" via "+boundary.name, func(t *testing.T) {
				pi := uint16(600 + len(tc.name) + len(boundary.name)*40)
				subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
				before := onStream()
				_, err := boundary.store(subject, &mqttInflightRecord{
					State: mqttInflightPublish, PI: pi, Dur: dur, Seq: 1, QoS: 2,
					SourceAckReply: testInflightAckSubject(dur, 1, 1),
					Nonce:          nonce, Lin: tc.lin,
				})
				if err == nil {
					t.Fatalf("the %s boundary must refuse a register with no well-formed lineage", boundary.name)
				}
				// The refusal must be the LINEAGE's. A generic error would let this pass
				// against a boundary that refused for some unrelated earlier reason and
				// never looked at the field at all.
				if !strings.Contains(err.Error(), "lineage") {
					t.Fatalf("the refusal must name the lineage, got %v", err)
				}
				if after := onStream(); after != before {
					t.Fatalf("a refused register reached storage: %d message(s) landed on %s",
						after-before, mqttInflightStreamName)
				}
				rec, smsg, lerr := e.asm.jsa.loadInflightRecord(subject)
				if lerr != nil {
					t.Fatalf("read back after the refusal: %v", lerr)
				}
				if smsg != nil || rec != nil {
					t.Fatalf("a refused register must not be stored, found %+v", rec)
				}
			})
		}
	}

	// THE DECODER, which is the third boundary and the one a RESUME meets. A record that got
	// onto the stream some other way must be unreadable rather than silently defaulted.
	for _, tc := range bodies {
		t.Run(tc.name+" via decode", func(t *testing.T) {
			// `claim` and `acq` are both mandatory, so both are present in every body
			// here: without them the decode would be refused for the wrong reason and
			// the lineage assertion below would never be reached.
			const rest = `"claim":"yyyyyyyyyyyyyyyyyyyyyy","source_ack_reply":"$JS.ACK.$MQTT_msgs.d.1.41.41.0.0","acq":0}`
			body := `{"pi":7,"dur":"d","seq":41,"qos":2,"state":"publish",` +
				`"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"` + tc.lin + `",` + rest
			if tc.lin == _EMPTY_ {
				body = `{"pi":7,"dur":"d","seq":41,"qos":2,"state":"publish",` +
					`"nonce":"xxxxxxxxxxxxxxxxxxxxxx",` + rest
			}
			if r, derr := mqttDecodeInflightRecord([]byte(body)); derr == nil {
				t.Fatalf("the decoder must refuse a register with no well-formed lineage, got %+v", r)
			} else if !strings.Contains(derr.Error(), "lineage") {
				t.Fatalf("the decode refusal must name the lineage, got %v", derr)
			}
		})
	}

	// THE POSITIVE CONTROL, at all three boundaries: the same shape with a well-formed
	// lineage is accepted, so the refusals above are about the lineage and not about a
	// boundary that refuses everything.
	pi := uint16(699)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	good := &mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: dur, Seq: 1, QoS: 2,
		SourceAckReply: testInflightAckSubject(dur, 1, 1),
		Nonce:          nonce, Lin: nonce, Claim: e.sess.regClaim,
	}
	beforePair := onStream()
	if _, err := e.asm.jsa.storeInflightPair(subject, 0, 0, good); err != nil {
		t.Fatalf("the control body must be accepted by the pair boundary, refused with: %v", err)
	}
	// THE OBSERVER ITSELF IS CONTROLLED. If a successful write does not move it, every
	// "nothing was stored" assertion above is vacuous.
	if afterPair := onStream(); afterPair <= beforePair {
		t.Fatalf("the storage observer did not move on a successful pair write (%d -> %d), "+
			"so the zero-write assertions above prove nothing", beforePair, afterPair)
	}
	pi2 := uint16(698)
	good2 := *good
	good2.PI = pi2
	beforeCAS := onStream()
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, pi2), 0, &good2); err != nil {
		t.Fatalf("the control body must be accepted by the CAS boundary, refused with: %v", err)
	}
	if afterCAS := onStream(); afterCAS <= beforeCAS {
		t.Fatalf("the storage observer did not move on a successful CAS write (%d -> %d)", beforeCAS, afterCAS)
	}
	if r, derr := mqttDecodeInflightRecord([]byte(`{"pi":7,"dur":"d","seq":41,"qos":2,"state":"publish",` +
		`"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx",` +
		`"claim":"yyyyyyyyyyyyyyyyyyyyyy","source_ack_reply":"$JS.ACK.$MQTT_msgs.d.1.41.41.0.0","acq":0}`)); derr != nil || r == nil {
		t.Fatalf("the control body must decode, got %v", derr)
	}
}

// ---------------------------------------------------------------------------
// The lineage writer census
// ---------------------------------------------------------------------------

// mqttLineageWriteSinks is every call that PERSISTS a register body. A lineage decision is
// made at each of them, so this set is what the census scans for.
//
// storeInflightPair is here and is deliberately called out: the existing protected-sink
// families carry storeInflightCAS and casInflightSlot but NOT the pair write, so a census
// built on that set alone would be blind to every create-only PAIR — which is the mode a
// fresh allocation and every retry fallback use, i.e. the majority of lineage decisions.
var mqttLineageWriteSinks = map[string][]string{
	"register-write": {
		"storeInflightCAS",
		"storeInflightPair",
		"casInflightSlot",
	},
	// THE BOUNDED BYPASS FAMILY. The three wrappers above are the supported way to persist a
	// register, but nothing stops a new function from framing a body itself and going
	// straight to the stream -- which is exactly what the real boundaries do internally:
	// storeInflightCAS calls buildInflightRecord then storeMsg, and storeInflightPair frames
	// its two messages and calls newAtomicBatchRequest.
	//
	// THE REACH OF THIS GUARD, stated rather than implied: it is SYNTACTIC and NAME-BASED. It
	// catches a bypass that uses the register body builder or the atomic-batch request, which
	// is every route this package actually has. It does NOT prove that no other code path
	// anywhere can reach $MQTT_inflight -- a raw storeMsg on a hand-built subject with a
	// hand-marshalled body would not appear here, because deciding which storeMsg calls carry
	// a register would need subject analysis this scanner cannot do. That limit is the reason
	// the guard is described as bounded.
	"register-write-bypass": {
		"buildInflightRecord",
		"newAtomicBatchRequest",
	},
}

// mqttLineageWriterManifest is the AUDITED disposition of every function that persists a
// register body: which rule decides the lineage it writes.
//
// Keyed by function name, and the census below compares it against EVERY discovered site
// with no known-function filter. That filter is what makes the existing protected-sink
// coverage check a snapshot rather than a superset: a writer added in a NEW function is
// simply skipped there. Here an unlisted function is a failure, which is the only shape
// that catches the fourteenth writer rather than re-proving the thirteen known ones.
type mqttLineageSite struct {
	fn   string
	sink string
}

// mqttLineageSiteCounts is the audited count of register-write call sites PER (function,
// sink). The count is what keeps a SECOND write from appearing inside an already-audited
// function and inheriting its disposition without anyone deciding a lineage for it —
// the hole a holistic round found in the first version of this census.
//
// A changed count fails, in either direction. That is deliberate: a removed write is as much
// a change to the lineage story as an added one.
var mqttLineageSiteCounts = map[mqttLineageSite]int{
	// F-105's authority transition. It reaches the batch primitive because the authority
	// and everything it authorizes commit together, so the census sees it as a potential
	// register writer — but it WRITES NO REGISTER BODY of its own: its own members are the
	// authority, the permanent outcome and an optional closure, and every caller-supplied
	// member is refused if it names an F-105 arbitration subject. A register carried as a
	// caller member arrives ALREADY BUILT by the deciding writer, whose lineage disposition
	// is the one that applies. One call site, in commitAuthorityTransition.
	{fn: "commitAuthorityTransition", sink: "newAtomicBatchRequest"}: 1,

	// F-105's metadata commit. Reaches the batch primitive on $MQTT_sess — the session
	// record, the acceptance fact and the handoff receipt — and writes NO register body at
	// all: it does not touch $MQTT_inflight. One call site, in commitMetadataAttempt.
	{fn: "commitMetadataAttempt", sink: "newAtomicBatchRequest"}: 1,

	// F-105's shared-group lifecycle writer. Reaches the batch primitive for a two-member
	// join or release, and writes NO register body: its members are the group record and the
	// acquisition ledger under $MQTT.shared.{grp,shl}, neither of which carries a
	// register lineage. One call site, in commitSharedLifecycleBatch.
	{fn: "commitSharedLifecycleBatch", sink: "newAtomicBatchRequest"}: 1,

	// THE DECIDING WRITER, both of its modes.
	{fn: "storeInflightRegister", sink: "storeInflightPair"}: 1,
	{fn: "storeInflightRegister", sink: "storeInflightCAS"}:  1,

	// The cancellation tombstone store both cancellation writers route through (F-105 7b): the
	// slot-only plain CAS. The ordinary arm commits through opTombstoneRegister's authority
	// transition, which carries the same body.
	{fn: "storeCancellationTombstone", sink: "storeInflightCAS"}: 1,

	// Whole-record rebuilds.
	{fn: "casInflightSlot", sink: "storeInflightCAS"}:              1,
	{fn: "claimRecoveredInflight", sink: "storeInflightCAS"}:       1,
	{fn: "markInflightRegisterRetired", sink: "casInflightSlot"}:   1,
	{fn: "acquireActionFromPublish", sink: "casInflightSlot"}:      1,
	{fn: "mqttAcquirePubRelAction", sink: "casInflightSlot"}:       1,
	{fn: "startActionDrain", sink: "casInflightSlot"}:              1,
	{fn: "acquirePublishAction", sink: "casInflightSlot"}:          1,
	{fn: "advancePublishTerminal", sink: "casInflightSlot"}:        1,
	{fn: "intendPubRelSlotOnly", sink: "casInflightSlot"}:          1,
	{fn: "drainPublishAction", sink: "casInflightSlot"}:            1,
	{fn: "advanceFinishedSlotToTerminal", sink: "casInflightSlot"}: 1,
	{fn: "mqttTerminalizeOnPubComp", sink: "casInflightSlot"}:      1,
	{fn: "phaseSlotToPubRelReady", sink: "casInflightSlot"}:        1,
	// TWO: the slot-only class's publish -> pubrel_ready CAS, and the ordinary class's
	// canonical rebind from pubrel_intended after recordRecoveredPubRec has recorded the
	// PUBREC through the one arrow (F-105 PUBCOMP arm, consult 25). Both carry the loaded
	// record forward through casInflightSlot, so both preserve its lineage.
	{fn: "phaseRecoveredPublishSlotToReady", sink: "casInflightSlot"}: 2,
	{fn: "transferCustodyForAdmission", sink: "casInflightSlot"}:      1,
	// F-105 8c's certified clearance: ONE exact-sequence CAS of the register as read with its
	// live custody fields cleared (mqttCertifiedClearanceOf), deliberately NOT through
	// casInflightSlot, whose reload loop would rebase the licence onto a later state. The
	// body is the loaded record copied forward, so its lineage rides along unchanged.
	{fn: "clearCertifiedActionToken", sink: "storeInflightCAS"}: 1,

	// THE BOUNDARIES, and the BYPASS family they are the only legitimate users of.
	{fn: "storeInflightCAS", sink: "storeInflightCAS"}:       1,
	{fn: "storeInflightPair", sink: "storeInflightPair"}:     1,
	{fn: "storeInflightCAS", sink: "buildInflightRecord"}:    1,
	{fn: "storeInflightPair", sink: "newAtomicBatchRequest"}: 1,

	// NOT a register write, and audited as such rather than silently filtered: the accepted
	// CONNECT batch writes the session record and its siblings on other streams. It is
	// listed because the bypass guard is name-based and cannot tell one atomic batch from
	// another — naming it here is what keeps that limitation visible instead of hidden in a
	// skip rule.
	{fn: "storeAcceptBatch", sink: "newAtomicBatchRequest"}: 1,

	// NOT a register write either: the teardown claim batch writes the session MARKER (a
	// record write, lineage decided by claimTeardown) and its create-only teardown receipt
	// on $MQTT_sess, and touches no $MQTT_inflight register. Listed for the same reason
	// storeAcceptBatch is — the name-based bypass guard cannot tell one atomic batch from
	// another, so naming it keeps that limitation visible (F-105 Gap 1). One call site, in
	// storeTeardownClaimBatch.
	{fn: "storeTeardownClaimBatch", sink: "newAtomicBatchRequest"}: 1,
}

var mqttLineageWriterManifest = map[string]string{
	// F-105's authority transition: NO LINEAGE OF ITS OWN. It carries the arbitration
	// records and passes through whatever register body its caller already built, so the
	// lineage decision belongs to that builder and not here. This entry exists so the
	// pass-through is audited rather than invisible: if this writer ever constructed a
	// register body itself, the count above would change and the census would demand a
	// disposition for it.
	"commitAuthorityTransition": "carries: no register body of its own, so the caller's disposition applies",

	// F-105's metadata commit: NOT A REGISTER WRITER. It commits on $MQTT_sess and has no
	// register lineage to decide. The entry exists so the census sees it rather than a
	// future reader assuming an unlisted batch caller is unreachable.
	"commitMetadataAttempt": "none: writes on $MQTT_sess and carries no register body",

	// F-105's shared-group lifecycle writer: NOT A REGISTER WRITER. Its atomic batch carries
	// the group record and the acquisition ledger, neither of which is an in-flight register,
	// so it decides no register lineage. Listed so the batch caller is audited rather than
	// assumed unreachable.
	"commitSharedLifecycleBatch": "none: writes the shared group record and ledger, no register body",

	// THE DECIDING WRITER. Its lineage follows the write mode of each attempt: create-only
	// pair (fresh, its own nonce), replace-pair (fresh, occupant proven-unsent),
	// replace-register-only (INHERITED, occupant possibly-attempted).
	"storeInflightRegister": "decides: per-attempt write mode",

	// The cancellation tombstone store (F-105 7b). It builds no body: both cancellation writers
	// hand it theirs. markInflightRegisterCancelledPending's create-only body starts its own
	// lineage, because a create-only CAS proves the subject was empty, and its conflict arm
	// rebuilds from the landed record and carries whatever it holds; markInflightRegisterCancelled
	// is a whole-record rebuild that sets only Cancelled.
	"storeCancellationTombstone": "carries: the caller's body — the pending form decides (create-only starts its own, the rebuild preserves the landed one), the fallback preserves",

	// WHOLE-RECORD REBUILDS. Each starts from the record it READ and mutates selected
	// fields, so the lineage rides along with no code of its own. casInflightSlot's copy of
	// the loaded record is what makes that true for the whole family at once, and
	// TestMQTTCustodyRewritePreservesTheLineage is the behavioural proof of it.
	//
	// Every row below was DISCOVERED by this scan rather than recalled. The first draft of
	// this manifest listed five writers from memory and the census named seven more, which
	// is the entire reason it is a scan and not a list.
	"casInflightSlot":                  "preserves: copies the loaded record before mutating selected fields",
	"claimRecoveredInflight":           "preserves: whole-record rebuild, mutates only custody",
	"acquireActionFromPublish":         "preserves: action install through casInflightSlot",
	"mqttAcquirePubRelAction":          "preserves: action install through casInflightSlot",
	"advanceFinishedSlotToTerminal":    "preserves: terminal transition through casInflightSlot",
	"mqttTerminalizeOnPubComp":         "preserves: terminal transition through casInflightSlot",
	"phaseSlotToPubRelReady":           "preserves: phase CAS through casInflightSlot",
	"phaseRecoveredPublishSlotToReady": "preserves: phase CAS (slot-only) and canonical rebind (ordinary) through casInflightSlot",
	"markInflightRegisterRetired":      "preserves: whole-record rebuild, sets only Retired",
	"startActionDrain":                 "preserves: action drain through casInflightSlot",
	"acquirePublishAction":             "preserves: PUBLISH action acquisition through casInflightSlot",
	"advancePublishTerminal":           "preserves: the publish_terminal phase arrow through casInflightSlot",
	"intendPubRelSlotOnly":             "preserves: the pubrel_intended phase arrow through casInflightSlot (shared/affine)",
	"drainPublishAction":               "preserves: PUBLISH action drain through casInflightSlot",
	"clearCertifiedActionToken":        "preserves: the loaded record copied forward with only its live custody fields cleared, one exact-sequence CAS",
	"transferCustodyForAdmission":      "preserves: whole-record rebuild, mutates only custody",

	// THE WRITE BOUNDARIES themselves. They decide no lineage -- they VALIDATE the body
	// handed to them, which is where an absent or malformed lineage is refused rather than
	// defaulted. Listed because the scan sees the manager methods delegating to their jsa
	// namesakes, and a boundary silently missing from the manifest would be indistinguishable
	// from a writer nobody audited.
	"storeInflightCAS":  "boundary: validates, decides nothing",
	"storeInflightPair": "boundary: validates, decides nothing",
}

// TestMQTTLineageWriterCensus is an INDEPENDENT SCAN, not an enumeration of the writers this
// change happened to touch.
//
// The distinction is the whole point. A hand-maintained list of "the writers I know about"
// cannot prevent its own drift: it proves nothing about the writer somebody adds next. So
// the scan reads the SOURCE for every register-persisting call site, and every discovered
// site must have a disposition — with no restriction to functions the manifest already
// names, which is exactly the escape hatch the existing coverage check leaves open.
// itoaLineage keeps the count messages readable without pulling strconv into a file whose
// other assertions are all structural.
func itoaLineage(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func TestMQTTLineageWriterCensus(t *testing.T) {
	t.Parallel()
	sites, files, unresolved := censusScanSinkFamilies(t, mqttLineageWriteSinks)

	// ANTI-VACUITY. A silent parse failure or a renamed sink makes every assertion below
	// agree with nothing.
	if files < 10 {
		t.Fatalf("the scan read %d production files, which is too few to be a real scan", files)
	}
	if len(sites) == 0 {
		t.Fatal("the scan found no register-write sites at all, so this census proves nothing")
	}
	// Every sink in the family must actually appear, or a renamed one hides behind the
	// others. storeInflightPair is the one this census exists to stop losing.
	seenSink := map[string]bool{}
	for site := range sites {
		seenSink[site.sink] = true
	}
	for _, sink := range mqttLineageWriteSinks["register-write"] {
		if !seenSink[sink] {
			t.Errorf("the scan found no call site for %q: either it was renamed, or the scanner no longer sees it", sink)
		}
	}

	// The shared scanner also records raw map deletes, because its protected-map family is
	// unconditional. Those are memory bookkeeping, not register-body writes, and they belong
	// to the protected-map census rather than this one.
	for site := range sites {
		if strings.HasPrefix(site.sink, "delete:") {
			delete(sites, site)
		}
	}

	// PROPERTY: every DISCOVERED (function, sink, COUNT) triple is audited.
	//
	// The count is the part a holistic round caught missing. Keying only on the function
	// lets a SECOND write appear inside an already-listed function with no new disposition —
	// so adding another pair write to storeInflightRegister, writing a stale inherited
	// lineage, would pass. The manifest below therefore carries a count per site, and a
	// changed count is a failure that has to be re-audited rather than absorbed.
	var missing, miscounted []string
	for site, got := range sites {
		want, ok := mqttLineageSiteCounts[mqttLineageSite(site)]
		if !ok {
			missing = append(missing, site.fn+" / "+site.sink)
			continue
		}
		if want != got {
			miscounted = append(miscounted,
				site.fn+" / "+site.sink+" has "+itoaLineage(got)+" call site(s), audited for "+itoaLineage(want))
		}
	}
	sort.Strings(missing)
	sort.Strings(miscounted)
	for _, m := range missing {
		t.Errorf("a function persists a register body with no audited lineage disposition: %s. "+
			"Either give it one in mqttLineageWriterManifest and mqttLineageSiteCounts, or route it "+
			"through a writer that has one", m)
	}
	for _, m := range miscounted {
		t.Errorf("a register-write site count changed, so a write was added or removed without "+
			"an audited lineage decision: %s", m)
	}

	// PROPERTY: the manifest carries no stale rows, and it is checked PER (function, sink)
	// with ABSENCE READ AS ZERO.
	//
	// Keying this reverse check on the function alone was a hole a holistic round found:
	// removing the pair write from storeInflightRegister left its audited count of 1
	// unchecked, because the function was still discovered through its CAS write and pair
	// calls elsewhere satisfied a sink-presence test. The row then survived as an obsolete
	// audit that a later write could quietly occupy. "Fails in both directions" is only true
	// if the missing direction is checked at the same key the forward one uses.
	discovered := map[string]bool{}
	for site := range sites {
		discovered[site.fn] = true
	}
	for fn := range mqttLineageWriterManifest {
		if !discovered[fn] {
			t.Errorf("mqttLineageWriterManifest names %q, which persists no register body: the row is stale", fn)
		}
	}
	for want, n := range mqttLineageSiteCounts {
		got := sites[censusSite(want)] // absent == 0
		if got != n {
			t.Errorf("mqttLineageSiteCounts audits %q / %q for %s call site(s) but the scan found %s: "+
				"a write was removed or moved, so the row is stale and could absorb a later one",
				want.fn, want.sink, itoaLineage(n), itoaLineage(got))
		}
	}

	// PROPERTY 6, failing closed: a register-write sink taken as a VALUE is an indirection
	// this scanner cannot follow, so it is named rather than passed over.
	for _, u := range unresolved {
		t.Errorf("a register-write sink is taken as a value, so the census cannot see where it is called: %s", u)
	}
}
