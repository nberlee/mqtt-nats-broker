package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// Behavioural tests for the unsent marker (inflight_marker.go, F-072): the durable pair,
// the send state derived from it, and the three delete-owning operations. Each names the
// contract row it pins and the mutation that must make it fail. Tests that install a
// process-global hook are NOT parallel; the rest are.

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

func (e *testInflightEnv) regSubject(pi uint16) string {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
}

// loadMarker reads the unsent marker beside pi from the stream: nil, nil when absent.
func (e *testInflightEnv) loadMarker(t *testing.T, pi uint16) (*mqttInflightMarker, *StoredMsg) {
	t.Helper()
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, e.regSubject(pi)+"."+mqttInflightMarkerSuffix)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("load the marker for pi %d: %v", pi, err)
		}
		return nil, nil
	}
	if smsg == nil {
		return nil, nil
	}
	m, _ := mqttDecodeInflightMarker(smsg.Data)
	return m, smsg
}

func (e *testInflightEnv) waitMarkerAbsent(t *testing.T, pi uint16) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, smsg := e.loadMarker(t, pi); smsg == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the marker beside packet identifier %d is still present", pi)
}

// entryState is the send-state triple of one entry, under the lock.
func (e *testInflightEnv) entryState(pi uint16) (present, transmitted, opaque bool, markerSeq uint64) {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	ack := e.sess.pendingPublish[pi]
	if ack == nil {
		return false, false, false, 0
	}
	return true, ack.transmitted, ack.markerOpaque, ack.markerSeq
}

// transmit runs one send section to completion the way a sender does: open under the lock,
// record durable INTENT (the marker's confirmed delete) outside it, then the write outcome,
// then release. wrote is memory-only; the durable fact is the intent, made before it.
func (e *testInflightEnv) transmit(t *testing.T, pi uint16, dur string, sseq uint64, wrote bool) mqttSendToken {
	t.Helper()
	tok := e.openSection(t, pi, dur, sseq)
	intent := e.sess.recordSendIntent(e.c, pi, tok)
	if intent == mqttIntentDefiniteFailure || intent == mqttIntentAmbiguous {
		t.Fatalf("recordSendIntent for packet identifier %d: outcome %d", pi, intent)
	}
	e.sess.endPublishSend(e.c, pi, tok, intent, wrote)
	return tok
}

// openSection opens a send section on the session's own connection.
func (e *testInflightEnv) openSection(t *testing.T, pi uint16, dur string, sseq uint64) mqttSendToken {
	t.Helper()
	e.sess.mu.Lock()
	e.sess.c = e.c // the section is opened on the session's own connection
	tok, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatalf("beginSendLocked refused packet identifier %d", pi)
	}
	return tok
}

// bind makes e.c the session's connection, as a CONNECT would.
func (e *testInflightEnv) bind() {
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
}

// markerLog records every caller-tagged marker delete, reload and disposition while
// installed. Process-global, so a test using it is not parallel.
type markerLog struct {
	mu           sync.Mutex
	deletes      []markerDeleteCall
	reloads      []markerReloadCall
	dispositions []markerDispositionCall
	suppress     func(caller string) error
}

type markerDeleteCall struct {
	caller, subject string
	seq             uint64
}
type markerReloadCall struct{ caller, subject string }
type markerDispositionCall struct {
	caller string
	v      mqttMarkerVerdict
}

func installMarkerLog(t *testing.T) *markerLog {
	t.Helper()
	l := &markerLog{}
	r1 := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		l.mu.Lock()
		l.deletes = append(l.deletes, markerDeleteCall{caller, subject, seq})
		sup := l.suppress
		l.mu.Unlock()
		if sup != nil {
			return sup(caller)
		}
		return nil
	})
	r2 := setMQTTTestMarkerReloadHook(func(caller, subject string) {
		l.mu.Lock()
		l.reloads = append(l.reloads, markerReloadCall{caller, subject})
		l.mu.Unlock()
	})
	r3 := setMQTTTestMarkerDispositionHook(func(caller string, v mqttMarkerVerdict) {
		l.mu.Lock()
		l.dispositions = append(l.dispositions, markerDispositionCall{caller, v})
		l.mu.Unlock()
	})
	t.Cleanup(func() { r3(); r2(); r1() })
	return l
}

func (l *markerLog) deletesBy(caller string) []markerDeleteCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []markerDeleteCall
	for _, d := range l.deletes {
		if d.caller == caller {
			out = append(out, d)
		}
	}
	return out
}

// lastReloadCaller is the caller tag of the most recent marker reload. The reload hook
// fires BEFORE the read seam (loadInflightMarker), so a seam consulting this is deciding by
// WHO IS ASKING rather than by how many reads have gone past — a causal boundary instead of
// a count, which is what keeps a fixture from breaking when an unrelated probe is added.
func (l *markerLog) lastReloadCaller() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.reloads) == 0 {
		return _EMPTY_
	}
	return l.reloads[len(l.reloads)-1].caller
}

func (l *markerLog) reloadsBy(caller string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, r := range l.reloads {
		if r.caller == caller {
			n++
		}
	}
	return n
}

// lastDisposition is the last verdict the boundary returned to caller, if any.
func (l *markerLog) lastDisposition(caller string) (mqttMarkerVerdict, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.dispositions) - 1; i >= 0; i-- {
		if l.dispositions[i].caller == caller {
			return l.dispositions[i].v, true
		}
	}
	return mqttMarkerVerdict{}, false
}

func (l *markerLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.deletes, l.reloads, l.dispositions = nil, nil, nil
}

// waitFor polls cond for up to five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func absentResp() *JSApiMsgGetResponse {
	return &JSApiMsgGetResponse{ApiResponse: ApiResponse{
		Error: &ApiError{ErrCode: uint16(JSNoMessageFoundErr), Code: 404},
	}}
}

func indeterminateResp() *JSApiMsgGetResponse {
	return &JSApiMsgGetResponse{ApiResponse: ApiResponse{
		Error: &ApiError{ErrCode: uint16(JSStreamNotFoundErr), Code: 503, Description: "test: unreadable"},
	}}
}

func presentResp(t *testing.T, subject string, seq uint64, rec *mqttInflightRecord) *JSApiMsgGetResponse {
	t.Helper()
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	return &JSApiMsgGetResponse{Message: &StoredMsg{Subject: subject, Sequence: seq, Data: data}}
}

func isMarkerBatch(subjects []string) bool {
	if len(subjects) == 0 {
		return false
	}
	_, err := mqttParseInflightMarkerSubject(subjects[0])
	return err == nil
}

// ---------------------------------------------------------------------------
// T6: the pair is one atomic batch (N-2.2, N-2.7). Mutation M20: store the two records
// separately and the rejection subtests find one of them landed.
// ---------------------------------------------------------------------------

func TestMQTTInflightBatchIsAtomic(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-pair-atomic")
	dur := e.sess.idHash + "_" + nuid.Next()
	const pi = uint16(7)
	subject := e.regSubject(pi)
	rec := func(nonce string) *mqttInflightRecord {
		return freshRegisterLineage(&mqttInflightRecord{PI: pi, Dur: dur, Seq: 100, QoS: 1, State: mqttInflightPublish,
			Nonce: nonce, Claim: e.sess.regClaim, ClaimSeq: e.sess.seq})
	}
	n1, n2, n3 := mqttNewInflightNonce(), mqttNewInflightNonce(), mqttNewInflightNonce()

	// Create-only pair: both land, the marker names the register's nonce, and the
	// acknowledgement names the REGISTER.
	regSeq, err := e.asm.jsa.storeInflightPair(subject, 0, 0, rec(n1))
	if err != nil {
		t.Fatalf("create-only pair: %v", err)
	}
	cur, smsg := e.loadRegister(t, pi)
	if cur == nil || cur.Nonce != n1 || smsg.Sequence != regSeq {
		t.Fatalf("the acknowledgement must name the register: got %+v at %d, ack %d", cur, smsg.Sequence, regSeq)
	}
	m, msmsg := e.loadMarker(t, pi)
	if m == nil || m.Nonce != n1 {
		t.Fatalf("the marker must be present and name the register's nonce, got %+v", m)
	}
	markerSeq := msmsg.Sequence

	// Two NON-ZERO bases: the replacement CASes both at their observed sequences.
	regSeq2, err := e.asm.jsa.storeInflightPair(subject, regSeq, markerSeq, rec(n2))
	if err != nil {
		t.Fatalf("replace the pair at its observed sequences: %v", err)
	}
	if cur, _ = e.loadRegister(t, pi); cur == nil || cur.Nonce != n2 {
		t.Fatalf("the register must carry the replacement's nonce, got %+v", cur)
	}
	m, msmsg = e.loadMarker(t, pi)
	if m == nil || m.Nonce != n2 || msmsg.Sequence == markerSeq {
		t.Fatalf("the marker must be rewritten with the replacement's nonce at a new sequence, got %+v at %d", m, msmsg.Sequence)
	}
	markerSeq2 := msmsg.Sequence

	// Whole-batch rejection, both directions. A stale MARKER base: the register CAS on
	// its own would succeed, and only atomicity keeps it out.
	if _, err := e.asm.jsa.storeInflightPair(subject, regSeq2, markerSeq, rec(n3)); err == nil {
		t.Fatalf("a stale marker base must reject the whole batch")
	}
	if cur, _ = e.loadRegister(t, pi); cur == nil || cur.Nonce != n2 {
		t.Fatalf("a REJECTED batch stored the register anyway: %+v", cur)
	}
	if m, _ = e.loadMarker(t, pi); m == nil || m.Nonce != n2 {
		t.Fatalf("a rejected batch changed the marker: %+v", m)
	}
	// A stale REGISTER base with the correct marker base: the marker CAS on its own would
	// succeed, and must not.
	if _, err := e.asm.jsa.storeInflightPair(subject, regSeq, markerSeq2, rec(n3)); err == nil {
		t.Fatalf("a stale register base must reject the whole batch")
	}
	if m, msmsg = e.loadMarker(t, pi); m == nil || m.Nonce != n2 || msmsg.Sequence != markerSeq2 {
		t.Fatalf("a REJECTED batch stored the marker anyway: %+v at %d", m, msmsg.Sequence)
	}
	if cur, _ = e.loadRegister(t, pi); cur == nil || cur.Nonce != n2 {
		t.Fatalf("a rejected batch changed the register: %+v", cur)
	}

	// The marker's own create-only CAS: a marker with no register (a stale leftover) makes
	// the create of a fresh pair reject as a whole, which is what the store retry reaps.
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, regSeq2, true); err != nil {
		t.Fatalf("delete the register: %v", err)
	}
	if _, err := e.asm.jsa.storeInflightPair(subject, 0, 0, rec(n3)); err == nil {
		t.Fatalf("a leftover marker must reject a create-only pair")
	}
	if cur, _ = e.loadRegister(t, pi); cur != nil {
		t.Fatalf("a rejected create stored the register anyway: %+v", cur)
	}
}

// ---------------------------------------------------------------------------
// Rows 1 and T22b: the store installs the unsent entry, with the marker's sequence
// LOADED, never derived from the register's acknowledgement. Mutation M22: derive it as
// ack.Sequence-1 and the non-adjacent fixture's entry names a filler message.
// ---------------------------------------------------------------------------

func TestMQTTInflightMarkerSequenceIsLoadedNotDerived(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-marker-loaded")
	dur := e.sess.idHash + "_" + nuid.Next()

	// The ordinary store: the pair is adjacent, and the entry carries the marker's real
	// sequence with transmitted=false (row 1).
	pi := e.track(t, dur, 300, 1)
	_, msmsg := e.loadMarker(t, pi)
	present, transmitted, opaque, markerSeq := e.entryState(pi)
	if !present || transmitted || opaque || msmsg == nil || markerSeq != msmsg.Sequence {
		t.Fatalf("row 1: want an unsent entry holding marker %d, got present=%v transmitted=%v opaque=%v markerSeq=%d",
			msmsg.Sequence, present, transmitted, opaque, markerSeq)
	}

	// The non-adjacent store: a pair seam writes the marker, a FILLER on an unrelated
	// subject, then the register, so ack.Sequence-1 names the filler.
	e.asm.mu.Lock()
	e.asm.testInflightStorePair = func(subject string, regExpect, markerExpect uint64, r *mqttInflightRecord) (uint64, error) {
		body, err := mqttBuildInflightMarker(r.Nonce)
		if err != nil {
			return 0, err
		}
		if _, err := e.asm.jsa.storeMsg(subject+"."+mqttInflightMarkerSuffix, -1, body); err != nil {
			return 0, err
		}
		filler := mqttInflightSubject(e.sess.idHash, e.sess.gen, 60000)
		if _, err := e.asm.jsa.storeMsg(filler, -1, []byte(`{"filler":true}`)); err != nil {
			return 0, err
		}
		return e.asm.jsa.storeInflightCAS(subject, regExpect, r)
	}
	e.asm.mu.Unlock()
	pi2 := e.track(t, dur, 301, 1)
	e.asm.mu.Lock()
	e.asm.testInflightStorePair = nil
	e.asm.mu.Unlock()

	_, msmsg2 := e.loadMarker(t, pi2)
	_, rsmsg2 := e.loadRegister(t, pi2)
	if msmsg2 == nil || rsmsg2 == nil || rsmsg2.Sequence-1 == msmsg2.Sequence {
		t.Fatalf("the fixture must make the marker and the register non-adjacent, got marker %v register %v", msmsg2, rsmsg2)
	}
	_, _, _, markerSeq2 := e.entryState(pi2)
	if markerSeq2 != msmsg2.Sequence {
		t.Fatalf("T22b: the entry must carry the LOADED marker sequence %d, got %d (ack-1 is %d)",
			msmsg2.Sequence, markerSeq2, rsmsg2.Sequence-1)
	}
}

// ---------------------------------------------------------------------------
// Rows 19 / 19a: durable INTENT is recorded BEFORE the write by consuming the captured
// marker with a CONFIRMED delete at exactly its sequence; endPublishSend then records only
// in memory. A section that aborts before intent leaves the pair and the entry untouched.
// The confirmed operation is once per exchange: a none-held section (already
// possibly-attempted) records nothing. Mutation M1: record after the write; M16-adjacent
// (do not set transmitted); M59: make the intent delete fire-and-forget.
// ---------------------------------------------------------------------------

func TestMQTTInflightIntentConsumesTheMarkerBeforeTheWrite(t *testing.T) {
	// NOT parallel: installs the process-global marker hooks.
	e := testNewInflightEnv(t, "client-intent")
	log := installMarkerLog(t)
	dur := e.sess.idHash + "_" + nuid.Next()

	// A section that ABORTS BEFORE INTENT (a size refusal, a staged crash): nothing is
	// recorded anywhere, the pair stands, the next section still carries DUP=0.
	pi := e.track(t, dur, 400, 1)
	_, before := e.loadMarker(t, pi)
	tok := e.openSection(t, pi, dur, 400)
	if tok.dup || tok.markerSeq != before.Sequence {
		t.Fatalf("the first section must carry DUP=0 and the captured marker %d, got %+v", before.Sequence, tok)
	}
	e.sess.endPublishSend(e.c, pi, tok, mqttIntentNone, false)
	if _, ok := log.lastDisposition(mqttMarkerCallerSender); ok {
		t.Fatalf("a section that never reached intent consulted the boundary")
	}
	if n := len(log.deletesBy(mqttMarkerCallerSender)); n != 0 {
		t.Fatalf("a section that aborted before intent issued %d marker delete(s)", n)
	}
	if _, transmitted, _, mseq := e.entryState(pi); transmitted || mseq != before.Sequence {
		t.Fatalf("an aborted section must leave the entry proven-unsent, got transmitted=%v markerSeq=%d", transmitted, mseq)
	}
	if m, _ := e.loadMarker(t, pi); m == nil {
		t.Fatalf("the marker must survive a section that never recorded intent")
	}

	// INTENT (row 19): the delete is issued by the SENDER, CONFIRMED, at exactly the
	// captured sequence, BEFORE any write — the marker is gone when recordSendIntent
	// returns, and the entry records that only afterwards.
	log.reset()
	tok2 := e.openSection(t, pi, dur, 400)
	if tok2.dup {
		t.Fatalf("the second section opened before any intent and must still carry DUP=0")
	}
	if got := e.sess.recordSendIntent(e.c, pi, tok2); got != mqttIntentConfirmed {
		t.Fatalf("recordSendIntent: want Confirmed, got %d", got)
	}
	if v, ok := log.lastDisposition(mqttMarkerCallerSender); !ok || v.disposition != mqttMarkerDeleteMarker || v.row != "19" {
		t.Fatalf("row 19: want DeleteMarker for the sender, got %+v (%v)", v, ok)
	}
	dels := log.deletesBy(mqttMarkerCallerSender)
	if len(dels) != 1 || dels[0].seq != before.Sequence || dels[0].subject != e.regSubject(pi)+"."+mqttInflightMarkerSuffix {
		t.Fatalf("the sender must delete the captured marker %d exactly once, got %+v", before.Sequence, dels)
	}
	// CONFIRMED means gone NOW, before the section has written or recorded anything.
	if m, _ := e.loadMarker(t, pi); m != nil {
		t.Fatalf("a confirmed intent delete must have landed before recordSendIntent returned")
	}
	if _, transmitted, _, _ := e.entryState(pi); transmitted {
		t.Fatalf("the entry is updated by endPublishSend, not by recordSendIntent")
	}
	e.sess.endPublishSend(e.c, pi, tok2, mqttIntentConfirmed, true)
	if _, transmitted, _, mseq := e.entryState(pi); !transmitted || mseq != 0 {
		t.Fatalf("after confirmed intent the entry is possibly-attempted: transmitted, none held; got %v %d", transmitted, mseq)
	}

	// Row 19a: the next section is a RETRANSMISSION — DUP=1, none held — and records no
	// intent: the confirmed operation is once per exchange, never once per send.
	log.reset()
	tok3 := e.openSection(t, pi, dur, 400)
	if !tok3.dup || tok3.markerSeq != 0 {
		t.Fatalf("the section after intent must carry DUP=1 and none held, got %+v", tok3)
	}
	if got := e.sess.recordSendIntent(e.c, pi, tok3); got != mqttIntentNone {
		t.Fatalf("a none-held section owes no intent, got %d", got)
	}
	if _, ok := log.lastDisposition(mqttMarkerCallerSender); ok {
		t.Fatalf("a none-held section must not consult the boundary")
	}
	if n := len(log.deletesBy(mqttMarkerCallerSender)); n != 0 {
		t.Fatalf("a none-held section issued %d delete(s)", n)
	}
	e.sess.endPublishSend(e.c, pi, tok3, mqttIntentNone, true)
	if _, transmitted, _, _ := e.entryState(pi); !transmitted {
		t.Fatalf("a retransmission leaves the entry transmitted")
	}
}

// ---------------------------------------------------------------------------
// Rows 4 and 5: recovery derives the send state from the pair. Mutation M3: seed
// transmitted regardless of the marker and the intact-pair subtest fails; M6: free the
// identifier on a present marker and the entry is missing.
// ---------------------------------------------------------------------------

func TestMQTTInflightRecoveryDerivesSendStateFromTheMarker(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-recover-marker")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseqA, sseqB := e.publishSource(t, dur), e.publishSource(t, dur)

	// A: registered and never sent — the pair is intact. B: transmitted — the marker gone.
	piA := e.track(t, dur, sseqA, 1)
	piB := e.track(t, dur, sseqB, 1)
	e.transmit(t, piB, dur, sseqB, true)
	e.waitMarkerAbsent(t, piB)
	_, markerA := e.loadMarker(t, piA)

	e.forgetInMemory()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	present, transmitted, opaque, markerSeq := e.entryState(piA)
	if !present || transmitted || opaque || markerSeq != markerA.Sequence {
		t.Fatalf("row 4: an intact pair recovers UNSENT holding its marker %d, got present=%v transmitted=%v opaque=%v markerSeq=%d",
			markerA.Sequence, present, transmitted, opaque, markerSeq)
	}
	present, transmitted, opaque, markerSeq = e.entryState(piB)
	if !present || !transmitted || opaque || markerSeq != 0 {
		t.Fatalf("row 5: a register with no marker recovers TRANSMITTED holding none, got present=%v transmitted=%v opaque=%v markerSeq=%d",
			present, transmitted, opaque, markerSeq)
	}
	e.bind()
	e.sess.mu.Lock()
	tokA, okA := e.sess.beginSendLocked(e.c, piA, dur, sseqA)
	tokB, okB := e.sess.beginSendLocked(e.c, piB, dur, sseqB)
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if !okA || tokA.dup || !okB || !tokB.dup || incomplete {
		t.Fatalf("after recovery A sends DUP=0 and B DUP=1: A=%v/%+v B=%v/%+v incomplete=%v", okA, tokA, okB, tokB, incomplete)
	}
}

// ---------------------------------------------------------------------------
// T20d, rows 5-9 and the row-6/9 boundary (N-5.6, C22d): an absent marker is never
// interpreted against the walk's cached snapshot; the register's own subject is reread and
// the reread decides. Mutations M46 (install from the snapshot), M51 (install the cached
// PUBLISH on a same-nonce new sequence), M51a (row 6 loses `readable`).
// ---------------------------------------------------------------------------

func TestMQTTInflightMarkerAbsenceIsNotInterpretedAgainstAStaleSnapshot(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-stale-snapshot")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.transmit(t, pi, dur, sseq, true)
	e.waitMarkerAbsent(t, pi)
	walkRec, walkMsg := e.loadRegister(t, pi)
	subject := e.regSubject(pi)
	rival := mqttNewInflightNonce()

	moved := func(rec mqttInflightRecord, seq uint64) func() *JSApiMsgGetResponse {
		return func() *JSApiMsgGetResponse { return presentResp(t, subject, seq, &rec) }
	}
	sameAtNewSeq := *walkRec
	rivalBody := *walkRec
	rivalBody.Nonce = rival
	wrongPI := *walkRec
	wrongPI.PI = pi + 1

	for _, tc := range []struct {
		name  string
		truth func() *JSApiMsgGetResponse // what every read after the walk returns
		check func(t *testing.T)
	}{
		{"row 5: same exchange at the same sequence installs transmitted", moved(*walkRec, walkMsg.Sequence), func(t *testing.T) {
			present, transmitted, opaque, _ := e.entryState(pi)
			if !present || !transmitted || opaque {
				t.Fatalf("want a transmitted entry, got present=%v transmitted=%v opaque=%v", present, transmitted, opaque)
			}
		}},
		{"row 6: same exchange at a NEW sequence reclassifies from the reread", moved(sameAtNewSeq, walkMsg.Sequence+100), func(t *testing.T) {
			e.sess.mu.Lock()
			ack := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			if ack == nil || ack.regSeq != walkMsg.Sequence+100 || !ack.transmitted {
				t.Fatalf("the reclassified entry must name the NEW sequence %d, got %+v", walkMsg.Sequence+100, ack)
			}
		}},
		{"row 7: an absent reread reclassifies from the absence", func() *JSApiMsgGetResponse { return absentResp() }, func(t *testing.T) {
			present, _, _, _ := e.entryState(pi)
			e.sess.mu.Lock()
			incomplete := e.sess.piSpaceIncomplete
			e.sess.mu.Unlock()
			if present || incomplete {
				t.Fatalf("a proven absence installs nothing and withholds nothing: present=%v incomplete=%v", present, incomplete)
			}
		}},
		{"row 8: a different exchange reclassifies from that body", moved(rivalBody, walkMsg.Sequence+5), func(t *testing.T) {
			e.sess.mu.Lock()
			ack := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			if ack == nil || ack.regNonce != rival {
				t.Fatalf("the entry must be classified from the rival's body, got %+v", ack)
			}
		}},
		{"row 9: an indeterminate reread withholds", func() *JSApiMsgGetResponse { return indeterminateResp() }, func(t *testing.T) {
			present, transmitted, opaque, _ := e.entryState(pi)
			e.sess.mu.Lock()
			incomplete := e.sess.piSpaceIncomplete
			e.sess.mu.Unlock()
			if !present || transmitted || !opaque || !incomplete {
				t.Fatalf("want the marker-opaque reservation and an incomplete identifier space, got present=%v transmitted=%v opaque=%v incomplete=%v",
					present, transmitted, opaque, incomplete)
			}
		}},
		{"row 9, not 6: a same-nonce body at a new sequence naming the WRONG identifier is unreadable", moved(wrongPI, walkMsg.Sequence+7), func(t *testing.T) {
			present, transmitted, opaque, _ := e.entryState(pi)
			if !present || transmitted || !opaque {
				t.Fatalf("a body that does not name its subject is not readable, so row 9 withholds: present=%v transmitted=%v opaque=%v",
					present, transmitted, opaque)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e.forgetInMemory()
			e.sess.mu.Lock()
			e.sess.piSpaceIncomplete = false
			e.sess.mu.Unlock()
			var registerReads int
			e.asm.mu.Lock()
			e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
				// Only a batch that reads THIS register is the walk's or a reread: the inventory
				// traversal's duty batches (F-105 7b) and the marker batches read the stream as
				// it stands, or the walk's own read would be counted against another batch.
				if isMarkerBatch(subjects) || !slices.Contains(subjects, subject) {
					return e.asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
				}
				registerReads++
				out := make([]*JSApiMsgGetResponse, 0, len(subjects))
				for _, s := range subjects {
					switch {
					case s != subject:
						out = append(out, absentResp())
					case registerReads == 1:
						// The walk's own read: the snapshot as it stood.
						out = append(out, presentResp(t, subject, walkMsg.Sequence, walkRec))
					default:
						// Every read after it: the stream has moved on.
						out = append(out, tc.truth())
					}
				}
				return out, nil
			}
			e.asm.mu.Unlock()
			defer func() {
				e.asm.mu.Lock()
				e.asm.testInflightLoadMulti = nil
				e.asm.mu.Unlock()
			}()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			if registerReads < 2 {
				t.Fatalf("an absent marker must make recovery REREAD the register (%d register read(s))", registerReads)
			}
			tc.check(t)
		})
	}
}

// ---------------------------------------------------------------------------
// T21a, the two-walk case (N-5.6, C22e): a marker whose register the snapshot did not see
// loads its sibling authoritatively — present reclassifies, unresolved fails closed.
// Mutations M36 (treat it as orphaned), M47 (marker walk first).
// ---------------------------------------------------------------------------

func TestMQTTInflightRecoveryWalkRaceFailsClosed(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-two-walks")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	_, markerMsg := e.loadMarker(t, pi)

	// The register enumeration misses the pair (committed after it), the marker
	// enumeration sees it.
	installRace := func() {
		var calls int
		e.asm.mu.Lock()
		e.asm.testInflightEnumerate = func(filter string) (map[string]uint64, error) {
			calls++
			if calls == 1 {
				return map[string]uint64{}, nil
			}
			subjects, _, err := e.asm.jsa.streamSubjects(mqttInflightStreamName, filter)
			return subjects, err
		}
		e.asm.mu.Unlock()
	}
	clear := func() {
		e.asm.mu.Lock()
		e.asm.testInflightEnumerate, e.asm.testInflightLoad = nil, nil
		e.asm.mu.Unlock()
	}

	t.Run("a present sibling reclassifies and the pair is installed unsent", func(t *testing.T) {
		e.forgetInMemory()
		installRace()
		defer clear()
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		present, transmitted, opaque, markerSeq := e.entryState(pi)
		e.sess.mu.Lock()
		incomplete := e.sess.piSpaceIncomplete
		e.sess.mu.Unlock()
		if !present || transmitted || opaque || markerSeq != markerMsg.Sequence || incomplete {
			t.Fatalf("the pair committed between the walks must be installed unsent: present=%v transmitted=%v opaque=%v markerSeq=%d incomplete=%v",
				present, transmitted, opaque, markerSeq, incomplete)
		}
	})
	t.Run("an unresolved sibling read fails the identifier space closed", func(t *testing.T) {
		e.forgetInMemory()
		e.sess.mu.Lock()
		e.sess.piSpaceIncomplete = false
		e.sess.mu.Unlock()
		installRace()
		defer clear()
		e.asm.mu.Lock()
		e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
			return nil, nil, errors.New("test: the sibling cannot be read")
		}
		e.asm.mu.Unlock()
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		present, _, _, _ := e.entryState(pi)
		e.sess.mu.Lock()
		incomplete := e.sess.piSpaceIncomplete
		e.sess.mu.Unlock()
		if present || !incomplete {
			t.Fatalf("an unresolved sibling installs nothing and withholds allocation: present=%v incomplete=%v", present, incomplete)
		}
	})
}

// C22c: an enumerated marker whose batched read is indeterminate withholds the identifier
// space rather than guessing its register transmitted.
func TestMQTTInflightMarkerBatchReadFailureReservesTheIdentifierSpace(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-marker-batch-fail")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.forgetInMemory()

	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		if !isMarkerBatch(subjects) {
			return e.asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
		}
		out := make([]*JSApiMsgGetResponse, len(subjects))
		for i := range out {
			out[i] = indeterminateResp()
		}
		return out, nil
	}
	e.asm.mu.Unlock()
	defer func() {
		e.asm.mu.Lock()
		e.asm.testInflightLoadMulti = nil
		e.asm.mu.Unlock()
	}()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	present, _, _, _ := e.entryState(pi)
	e.sess.mu.Lock()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if present || !incomplete {
		t.Fatalf("an unreadable marker batch must install nothing and withhold allocation: present=%v incomplete=%v", present, incomplete)
	}
}

// T21b (N-9.6): a marker with no register reserves nothing and does not make the space
// incomplete; it is the sweep's.
func TestMQTTInflightOrphanMarkerReservesNoIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-orphan-marker")
	const pi = uint16(9)
	body, _ := mqttBuildInflightMarker(mqttNewInflightNonce())
	if _, err := e.asm.jsa.storeMsg(e.regSubject(pi)+"."+mqttInflightMarkerSuffix, -1, body); err != nil {
		t.Fatalf("plant the orphan marker: %v", err)
	}
	e.forgetInMemory()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	present, _, _, _ := e.entryState(pi)
	e.sess.mu.Lock()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if present || incomplete {
		t.Fatalf("an orphan marker reserves nothing: present=%v incomplete=%v", present, incomplete)
	}
	if m, _ := e.loadMarker(t, pi); m == nil {
		t.Fatalf("recovery must leave the orphan marker to the sweep")
	}
}

// ---------------------------------------------------------------------------
// T5c / T5d / T5e (N-5.7, N-5.8): terminal cleanup removes the register first and the
// marker second; a refused register half issues no marker delete; a moved register is
// preserved with its marker. Mutations M29 (marker first), M40 (authorize the marker delete
// from the register delete's success), M43 (revalidate before capturing).
// ---------------------------------------------------------------------------

func TestMQTTInflightTerminalCleanupRemovesRegisterThenMarker(t *testing.T) {
	// NOT parallel: installs process-global hooks.
	e := testNewInflightEnv(t, "client-cleanup-order")
	log := installMarkerLog(t)
	dur := e.sess.idHash + "_" + nuid.Next()

	t.Run("register then marker", func(t *testing.T) {
		pi := e.track(t, dur, 500, 1)
		_, mmsg := e.loadMarker(t, pi)
		regSubject := e.regSubject(pi)
		var mu sync.Mutex
		var order []string
		restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
			if stream == mqttInflightStreamName {
				// Named by subject: the admitted unsent register is withdrawn first, so its
				// delete acts on the tombstone's sequence rather than the admitted one.
				what := fmt.Sprintf("other@%d", seq)
				switch {
				case seq == mmsg.Sequence:
					what = "marker"
				case e.inflightSeqNames(seq, regSubject):
					what = "register"
				}
				mu.Lock()
				order = append(order, what)
				mu.Unlock()
			}
			return nil
		})
		defer restore()
		e.sess.mu.Lock()
		removed := e.sess.untrackPublishEntry(pi, testUntrackNoBinding)
		e.sess.mu.Unlock()
		if !e.sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil)) {
			t.Fatalf("the register half must be proven")
		}
		// The disposition and the delete order are settled SYNCHRONOUSLY by the call
		// above, so they are asserted BEFORE the eventual-absence polls: a boundary that
		// refused (row 26 PreserveMarker, the stranded-marker defect) then reports itself
		// as a wrong disposition instead of as an unexplained five-second timeout.
		if v, ok := log.lastDisposition(mqttMarkerCallerCleanup); !ok || v.disposition != mqttMarkerDeleteMarker {
			t.Fatalf("want DeleteMarker from the cleanup reap, got %+v (%v)", v, ok)
		}
		if dels := log.deletesBy(mqttMarkerCallerCleanup); len(dels) != 1 || dels[0].seq != mmsg.Sequence {
			t.Fatalf("the marker delete must be attributed to cleanup at the captured sequence, got %+v", dels)
		}
		mu.Lock()
		got := append([]string(nil), order...)
		mu.Unlock()
		if len(got) != 2 || got[0] != "register" || got[1] != "marker" {
			t.Fatalf("want the register deleted before the marker, got %v", got)
		}
		e.waitRegisterCount(t, 0)
		e.waitMarkerAbsent(t, pi)
	})

	t.Run("a refused register half issues no marker delete", func(t *testing.T) {
		log.reset()
		pi := e.track(t, dur, 501, 1)
		e.asm.mu.Lock()
		e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
			return nil, nil, errors.New("test: the register cannot be read")
		}
		e.asm.mu.Unlock()
		defer func() {
			e.asm.mu.Lock()
			e.asm.testInflightLoad = nil
			e.asm.mu.Unlock()
		}()
		e.sess.mu.Lock()
		removed := e.sess.untrackPublishEntry(pi, testUntrackNoBinding)
		e.sess.mu.Unlock()
		if e.sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil)) {
			t.Fatalf("an indeterminate register read must refuse the register half")
		}
		if n := len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
			t.Fatalf("no marker delete may follow a refused register half, got %d", n)
		}
		if m, _ := e.loadMarker(t, pi); m == nil {
			t.Fatalf("the marker must survive")
		}
	})

	t.Run("a moved register is preserved and so is its marker", func(t *testing.T) {
		log.reset()
		pi := e.track(t, dur, 502, 1)
		rec, rmsg := e.loadRegister(t, pi)
		// A same-identity rewrite moves the exchange to a new sequence; the entry still
		// names the old one.
		if _, err := e.asm.jsa.storeInflightCAS(e.regSubject(pi), rmsg.Sequence, rec); err != nil {
			t.Fatalf("rewrite the register: %v", err)
		}
		e.sess.mu.Lock()
		removed := e.sess.untrackPublishEntry(pi, testUntrackNoBinding)
		e.sess.mu.Unlock()
		if !e.sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil)) {
			t.Fatalf("a moved register is preserved and reported as progress")
		}
		if n := len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
			t.Fatalf("row 26: the sibling is this exchange at a new sequence, so no marker delete may follow, got %d", n)
		}
		if v, ok := log.lastDisposition(mqttMarkerCallerCleanup); !ok || v.disposition != mqttMarkerPreserveMarker || v.row != "26" {
			t.Fatalf("want PreserveMarker from row 26, got %+v (%v)", v, ok)
		}
		if m, _ := e.loadMarker(t, pi); m == nil {
			t.Fatalf("the marker must survive its register's rewrite")
		}
		if cur, _ := e.loadRegister(t, pi); cur == nil {
			t.Fatalf("the moved register must survive")
		}
	})
}

// ---------------------------------------------------------------------------
// T5h (N-5.9, rows 24a and 29): cleanup of a none-held entry RELOADS the marker and lets
// the boundary decide; none held alone never completes it. Mutations M56 (skip the reload),
// M57 (read an indeterminate reload as absent).
// ---------------------------------------------------------------------------

// noneHeldEntry produces a row-5 entry: transmitted, holding no marker sequence.
func (e *testInflightEnv) noneHeldEntry(t *testing.T, dur string) uint16 {
	t.Helper()
	sseq := e.publishSource(t, dur)
	pi := e.trackAdmitted(t, dur, sseq, 1)
	e.transmit(t, pi, dur, sseq, true)
	e.waitMarkerAbsent(t, pi)
	e.forgetInMemory()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if present, transmitted, opaque, markerSeq := e.entryState(pi); !present || !transmitted || opaque || markerSeq != 0 {
		t.Fatalf("precondition: a row-5 entry (transmitted, none held), got present=%v transmitted=%v opaque=%v markerSeq=%d",
			present, transmitted, opaque, markerSeq)
	}
	return pi
}

// ROWS 24a AND 29 ARE ABOUT THE MARKER BOUNDARY, and under F-098 they have to be reached
// through a release C-IV actually licenses (consult D-6 row (c), and D-7).
//
// `noneHeldEntry` TRANSMITS. Its earlier route — an identity-matched `mqttSlotDeleteComplete`
// — is exactly the second opinion C-IV removes, and the repair is emphatically NOT to hand
// the fixture a cancellation verdict: asserting a transmitted exchange was never transmitted
// fabricates the proof rather than supplying it. So the rows are reached the way production
// reaches them for an acknowledged PUBLISH: the genuine PUBACK-driven advance to
// `publish_terminal`, whose durable phase is positive authority in its own right (arm 3),
// released under `mqttSlotDeletePublishTerminal`.
//
// The old "the register half is independent of the marker's reload" property is REPLACED by
// the narrower pair D-6 established, because the unqualified version is false under arm 1:
//
//	(1) a marker failure during RELEASE-AUTHORITY EVALUATION blocks the deletion, and it
//	    blocks it even where a durable cancellation exists;
//	(2) a later failure at the MARKER-CLEANUP reload does not undo a register disposition
//	    that is already established.
//
// Mutations M56 (skip the reload), M57 (read an indeterminate reload as absent).

func TestMQTTInflightCleanupOfNoneHeldEntryReloadsAndOwesNoDelete(t *testing.T) {
	// NOT parallel: installs process-global hooks.
	e := testNewInflightEnv(t, "client-none-held-cleanup")
	log := installMarkerLog(t)

	// terminalRelease is the production route for an ACKNOWLEDGED PUBLISH: untrack the
	// entry, advance its slot to `publish_terminal`, and release it under that phase's own
	// disposition. Returning the advance separately keeps a failed advance from being
	// reported as a refused release.
	terminalRelease := func(t *testing.T, pi uint16) bool {
		t.Helper()
		e.sess.mu.Lock()
		removed := e.sess.untrackPublishEntry(pi, testUntrackNoBinding)
		e.sess.mu.Unlock()
		cur, smsg := e.loadRegister(t, pi)
		if cur == nil || smsg == nil {
			t.Fatal("the acknowledged PUBLISH must have its admitted register")
		}
		if _, err := e.sess.advanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Gen: e.sess.gen,
			Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
			Nonce: cur.Lin, To: mqttExchangeTerminal, Acq: cur.Acq,
			Register: cur, RegisterSeq: smsg.Sequence,
		}); err != nil {
			t.Fatalf("the acknowledged pair must reach Terminal: %v", err)
		}
		cur, smsg = e.loadRegister(t, pi)
		removed.slot = *mqttSlotRefFromRecord(e.regSubject(pi), smsg.Sequence, cur)
		removed.regSeq = smsg.Sequence
		// §6 adds a separate source premise to this marker-boundary test.
		// Real message absence with the consumer still present proves completion;
		// no marker hook or marker assertion is changed.
		if err := e.asm.jsa.deleteMsg(mqttStreamName, cur.Seq, true); err != nil {
			t.Fatalf("establish source completion: %v", err)
		}
		if e.asm.inflightSourceState(cur) != mqttInflightSourceComplete {
			t.Fatal("the source-completion premise must be proven")
		}
		return e.sess.deleteInflightRegisterFor(removed, mqttSlotDeletePublishTerminal, mqttTestLicence(e.sess, nil))
	}

	t.Run("authoritatively absent completes the cleanup (row 24a)", func(t *testing.T) {
		log.reset()
		dur := e.sess.idHash + "_" + nuid.Next()
		pi := e.noneHeldEntry(t, dur)
		if !terminalRelease(t, pi) {
			t.Fatalf("the register half must be proven")
		}
		if log.reloadsBy(mqttMarkerCallerCleanup) != 1 {
			t.Fatalf("cleanup of a none-held entry must RELOAD the marker exactly once, got %d", log.reloadsBy(mqttMarkerCallerCleanup))
		}
		if v, ok := log.lastDisposition(mqttMarkerCallerCleanup); !ok || v.disposition != mqttMarkerPreserveMarker || v.row != "24a" {
			t.Fatalf("want PreserveMarker from row 24a, got %+v (%v)", v, ok)
		}
		if n := len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
			t.Fatalf("nothing to delete, yet %d delete(s) were attributed to cleanup", n)
		}
		e.waitRegisterCount(t, 0)
	})

	// PROPERTY (1). The marker cannot be resolved when the authority is evaluated, so the
	// evidence is INDETERMINATE and arm 1 withholds before the terminal phase is even
	// considered. The register survives: an unresolved marker is never an absence, and a
	// release decided on one would be a release decided on a read that failed.
	t.Run("a marker unreadable from the outset BLOCKS the release", func(t *testing.T) {
		log.reset()
		dur := e.sess.idHash + "_" + nuid.Next()
		pi := e.noneHeldEntry(t, dur)
		e.asm.mu.Lock()
		e.asm.testInflightMarkerLoad = func(string) (*StoredMsg, error) { return nil, errors.New("test: unreadable") }
		e.asm.mu.Unlock()
		defer func() {
			e.asm.mu.Lock()
			e.asm.testInflightMarkerLoad = nil
			e.asm.mu.Unlock()
		}()
		if terminalRelease(t, pi) {
			t.Fatal("a release was reported while the marker could not be read: an unresolved " +
				"marker withholds authority (C-IV arm 1) whatever the phase says")
		}
		if n := len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
			t.Fatalf("a refused release issued %d marker delete(s)", n)
		}
		e.waitRegisterCount(t, 1)

		// AND THE BLOCK IS NOT PERMANENT: the same release, with the marker readable again,
		// completes. Without this the subtest would prove only that something refused, and
		// it would leave its register behind for the next subtest's allocation to collide
		// with — a fixture artefact that says nothing about the property.
		e.asm.mu.Lock()
		e.asm.testInflightMarkerLoad = nil
		e.asm.mu.Unlock()
		// Re-driven from the SLOT the advance left behind, which is the production
		// continuation for a `publish_terminal` record whose entry is already gone.
		subject := e.regSubject(pi)
		cur, smsg, lerr := e.asm.loadInflightRecord(subject)
		if lerr != nil || cur == nil || smsg == nil {
			t.Fatalf("fixture: the publish_terminal slot must still be there: rec=%+v err=%v", cur, lerr)
		}
		if !e.sess.recoverPublishTerminalSlot(e.c, subject, smsg.Sequence, cur) {
			t.Fatal("with the marker readable again the same terminal release must complete: " +
				"the refusal above was about the READ, not about the phase")
		}
		e.waitRegisterCount(t, 0)
	})

	// PROPERTY (2). The authority probe answers determinately and the register disposition is
	// ESTABLISHED; the marker reload that follows then fails. The disposition stands — the
	// register is gone — and the marker is refused and left to the sweep.
	//
	// The seam decides by CALLER, which is the causal boundary between the two reads: the
	// evidence probe asks first, the cleanup reload second. A read COUNT would encode the
	// current number of probes and break the moment one is added.
	t.Run("a failure at the cleanup reload does not undo an established disposition (row 29)", func(t *testing.T) {
		log.reset()
		dur := e.sess.idHash + "_" + nuid.Next()
		pi := e.noneHeldEntry(t, dur)
		e.asm.mu.Lock()
		e.asm.testInflightMarkerLoad = func(string) (*StoredMsg, error) {
			if log.lastReloadCaller() == mqttMarkerCallerCleanup {
				return nil, errors.New("test: unreadable")
			}
			return nil, nil // authoritatively absent, for the authority probe
		}
		e.asm.mu.Unlock()
		defer func() {
			e.asm.mu.Lock()
			e.asm.testInflightMarkerLoad = nil
			e.asm.mu.Unlock()
		}()
		if !terminalRelease(t, pi) {
			t.Fatal("the register disposition was established before the cleanup reload ran, " +
				"so a failure there must not turn it back into a refusal")
		}
		if v, ok := log.lastDisposition(mqttMarkerCallerCleanup); !ok || v.disposition != mqttMarkerRefuse || v.row != "29" {
			t.Fatalf("an indeterminate reload is never an absence: want Refuse from row 29, got %+v (%v)", v, ok)
		}
		if n := len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
			t.Fatalf("a refused reap issued %d delete(s)", n)
		}
		e.waitRegisterCount(t, 0)
	})

	// ROW 29's OTHER INPUT, tested where it lives. A marker PRESENT beside a none-held entry
	// names some other exchange — a sequence acquired after this register was removed — so
	// the reap refuses and leaves it. It is driven through reapMarkerNoneHeld directly
	// because the combined route cannot reach it: a marker naming another allocation is not
	// this allocation's marker, so the authority reads INDETERMINATE and property (1) stops
	// the release first. Both facts are asserted, in that order.
	t.Run("a present reload refuses and the marker survives (row 29)", func(t *testing.T) {
		log.reset()
		dur := e.sess.idHash + "_" + nuid.Next()
		pi := e.noneHeldEntry(t, dur)
		// Whoever's it is, a sequence acquired after the register was removed is not one
		// this cleanup may delete at.
		body, _ := mqttBuildInflightMarker(mqttNewInflightNonce())
		if _, err := e.asm.jsa.storeMsg(e.regSubject(pi)+"."+mqttInflightMarkerSuffix, -1, body); err != nil {
			t.Fatalf("plant the marker: %v", err)
		}
		if terminalRelease(t, pi) {
			t.Fatal("a marker naming ANOTHER allocation is not evidence about ours, so it " +
				"reads indeterminate and the release must withhold")
		}
		if d := e.asm.reapMarkerNoneHeld(e.c, mqttMarkerCallerCleanup, e.regSubject(pi)); d != mqttMarkerRefuse {
			t.Fatalf("reapMarkerNoneHeld = %v, want Refuse", d)
		}
		if v, ok := log.lastDisposition(mqttMarkerCallerCleanup); !ok || v.disposition != mqttMarkerRefuse || v.row != "29" {
			t.Fatalf("want Refuse from row 29, got %+v (%v)", v, ok)
		}
		if n := len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
			t.Fatalf("a refused reap issued %d delete(s)", n)
		}
		if m, _ := e.loadMarker(t, pi); m == nil {
			t.Fatalf("the planted marker must survive")
		}
	})
}

// ---------------------------------------------------------------------------
// T5g (reworked, user 2026-09-08): a Maximum Packet Size refusal of a RECOVERED
// possibly-attempted exchange RETAINS the identifier and its recoverable state. Refusing
// this attempt does not establish that earlier attempts wrote nothing — the receiver may
// hold the identifier. Safe disposal is preserved for an exchange PROVEN unsent, and an
// unresolved-intent entry is retained too. Mutation M60: allow disposal on none held.
// ---------------------------------------------------------------------------

func TestMQTTInflightRecoveredEntryOversizeRefusalRetainsTheIdentifier(t *testing.T) {
	// NOT parallel: installs process-global hooks.
	e := testNewInflightEnv(t, "client-oversize-none-held")
	log := installMarkerLog(t)
	dur := e.sess.idHash + "_" + nuid.Next()

	t.Run("possibly-attempted is retained", func(t *testing.T) {
		pi := e.noneHeldEntry(t, dur)
		e.bind()
		e.sess.mu.Lock()
		sseq := e.sess.pendingPublish[pi].sseq
		tok, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
		e.sess.mu.Unlock()
		if !ok || !tok.dup || tok.markerSeq != 0 {
			t.Fatalf("the section on a row-5 entry carries DUP=1 and none held, got ok=%v %+v", ok, tok)
		}
		if e.sess.oversizeDisposalAllowed(e.c, pi, tok) {
			t.Fatalf("a possibly-attempted exchange may not be disposed of on a size refusal")
		}
		log.reset()
		e.sess.endPublishSend(e.c, pi, tok, mqttIntentNone, false)
		if n := len(log.deletesBy(mqttMarkerCallerSender)) + len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
			t.Fatalf("%d delete(s) issued; a retained exchange owes none", n)
		}
		e.sess.mu.Lock()
		_, held := e.sess.sendsInFlight[pi]
		cur, tracked := e.sess.pendingPublish[pi]
		e.sess.mu.Unlock()
		if held || !tracked || !cur.transmitted {
			t.Fatalf("the section is released and the entry RETAINED as transmitted: held=%v tracked=%v cur=%+v", held, tracked, cur)
		}
		if rec, _ := e.loadRegister(t, pi); rec == nil {
			t.Fatalf("the register must survive: it is the identifier's durable hold")
		}
		// Retention is asserted by an allocation that would EXPOSE reuse, not by presence
		// alone: a new delivery must not be minted under this identifier.
		other := e.track(t, dur, sseq+1000, 1)
		if other == pi {
			t.Fatalf("the retained identifier %d was re-minted for a new delivery", pi)
		}
	})

	t.Run("unresolved intent is retained", func(t *testing.T) {
		pi := e.track(t, dur, 701, 1)
		e.sess.mu.Lock()
		e.sess.pendingPublish[pi].intentUnresolved = true
		e.sess.mu.Unlock()
		tok := e.openSection(t, pi, dur, 701)
		if !tok.intentUnresolved || tok.dup {
			t.Fatalf("the token must carry the unresolved intent, got %+v", tok)
		}
		if e.sess.oversizeDisposalAllowed(e.c, pi, tok) {
			t.Fatalf("an exchange whose intent is unresolved may not be disposed of: its marker may already be gone")
		}
		e.sess.endPublishSend(e.c, pi, tok, mqttIntentNone, false)
	})

	t.Run("proven-unsent may be disposed of", func(t *testing.T) {
		pi := e.track(t, dur, 702, 1)
		tok := e.openSection(t, pi, dur, 702)
		if tok.dup || tok.markerSeq == 0 {
			t.Fatalf("a fresh pair is proven-unsent, got %+v", tok)
		}
		if !e.sess.oversizeDisposalAllowed(e.c, pi, tok) {
			t.Fatalf("a proven-unsent exchange was never attempted and may be discarded")
		}
		e.sess.endPublishSend(e.c, pi, tok, mqttIntentNone, false)
	})
}

// ---------------------------------------------------------------------------
// T8 (N-7.7): a staged drain enrols transmitted entries and WITHDRAWS a confirmed
// never-transmitted one, whose register and marker go once the packet commits.
// Mutation M11: enrol every non-regPending entry again.
// ---------------------------------------------------------------------------

func TestMQTTInflightDrainWithdrawsConfirmedUnsentEntry(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-drain-withdraw")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	sent := e.track(t, dur, 600, 1)
	e.transmit(t, sent, dur, 600, true)
	e.waitMarkerAbsent(t, sent)
	unsent := e.track(t, dur, 601, 1)

	e.sess.mu.Lock()
	e.sess.cons = map[string]*ConsumerConfig{sid: cc}
	staged := e.sess.stageDrain(sid, cc, nil, mqttTestLicence(e.sess, nil))
	d := e.sess.drains[sid]
	_, unsentTracked := e.sess.pendingPublish[unsent]
	var enrolled []uint16
	if d != nil {
		for _, st := range d.seqs {
			enrolled = append(enrolled, st.pi)
		}
	}
	withdrawn := e.sess.commitDrains()
	e.sess.mu.Unlock()
	if !staged || d == nil || len(enrolled) != 1 || enrolled[0] != sent {
		t.Fatalf("the drain must enrol exactly the transmitted delivery %d, got staged=%v enrolled=%v", sent, staged, enrolled)
	}
	if unsentTracked {
		t.Fatalf("a confirmed never-transmitted entry must be withdrawn, not enrolled")
	}
	if len(withdrawn) != 1 || withdrawn[0].pi != unsent {
		t.Fatalf("the commit must hand back the withdrawn pair, got %+v", withdrawn)
	}
	e.sess.voidRegistersOnStream(e.c, withdrawn, mqttTestLicence(e.sess, nil))
	e.waitMarkerAbsent(t, unsent)
	waitFor(t, "the withdrawn register to go", func() bool {
		_, smsg := e.loadRegister(t, unsent)
		return smsg == nil
	})
	if cur, _ := e.loadRegister(t, sent); cur == nil {
		t.Fatalf("the enrolled delivery's register is the drain's to release, not the withdrawal's")
	}
}

// ---------------------------------------------------------------------------
// T25 / T21c (N-5.11): the generation teardown reaps registers, then markers, both under
// one watermark. Mutations M25 (registers only), M27b (markers first).
// ---------------------------------------------------------------------------

func TestMQTTInflightClearReapRemovesBothClasses(t *testing.T) {
	// NOT parallel: installs a process-global hook.
	e := testNewInflightEnv(t, "client-clear-reap")
	dur := e.sess.idHash + "_" + nuid.Next()
	pi1, pi2 := e.track(t, dur, 700, 1), e.track(t, dur, 701, 1)
	_, r1 := e.loadRegister(t, pi1)
	_, r2 := e.loadRegister(t, pi2)
	_, m1 := e.loadMarker(t, pi1)
	_, m2 := e.loadMarker(t, pi2)
	registers := map[uint64]bool{r1.Sequence: true, r2.Sequence: true}
	markers := map[uint64]bool{m1.Sequence: true, m2.Sequence: true}

	var mu sync.Mutex
	var order []uint64
	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream == mqttInflightStreamName {
			mu.Lock()
			order = append(order, seq)
			mu.Unlock()
		}
		return nil
	})
	defer restore()
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	if err := e.asm.reapIncarnationRegisters(e.c, idHash, gen, false); err != nil {
		t.Fatalf("reapIncarnationRegisters: %v", err)
	}
	e.waitRegisterCount(t, 0)
	e.waitMarkerAbsent(t, pi1)
	e.waitMarkerAbsent(t, pi2)
	mu.Lock()
	got := append([]uint64(nil), order...)
	mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("want four deletes, got %v", got)
	}
	for i, seq := range got {
		switch {
		case i < 2 && !registers[seq]:
			t.Fatalf("the register pass runs first: delete %d at position %d is not a register (%v)", seq, i, got)
		case i >= 2 && !markers[seq]:
			t.Fatalf("the marker pass runs after the register pass: delete %d at position %d is not a marker (%v)", seq, i, got)
		}
	}
}

// ---------------------------------------------------------------------------
// T17 / T24 / T18a (N-9.3): the sweep parses markers, captures, reads the sibling and
// executes rows 24-28. Mutations M23 (markers as malformed registers), M49 (an unreadable
// sibling as a different exchange), M49a (drop readable's subject condition).
// ---------------------------------------------------------------------------

func TestMQTTInflightMarkerSweepReapsAnOrphan(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-marker-sweep")
	dur := e.sess.idHash + "_" + nuid.Next()
	markerSubj := func(pi uint16) string { return e.regSubject(pi) + "." + mqttInflightMarkerSuffix }

	// (a) an orphan marker: no register at all
	orphan := uint16(11)
	body, _ := mqttBuildInflightMarker(mqttNewInflightNonce())
	if _, err := e.asm.jsa.storeMsg(markerSubj(orphan), -1, body); err != nil {
		t.Fatal(err)
	}
	// (b) a live pair
	live := e.track(t, dur, 800, 1)
	// (c) a marker whose sibling was rewritten by another exchange
	rivalled := e.track(t, dur, 801, 1)
	rrec, rmsg := e.loadRegister(t, rivalled)
	rrec.Nonce = mqttNewInflightNonce()
	if _, err := e.asm.jsa.storeInflightCAS(e.regSubject(rivalled), rmsg.Sequence, rrec); err != nil {
		t.Fatal(err)
	}
	// (d) an unreadable sibling
	unreadable := e.track(t, dur, 802, 1)
	if _, err := e.asm.jsa.storeMsg(e.regSubject(unreadable), -1, []byte("not a register")); err != nil {
		t.Fatal(err)
	}
	// (e) a sibling that decodes but names the WRONG identifier
	wrongPI := e.track(t, dur, 803, 1)
	wrec, _ := e.loadRegister(t, wrongPI)
	wrec.PI = wrongPI + 1
	wbody, _ := json.Marshal(wrec)
	if _, err := e.asm.jsa.storeMsg(e.regSubject(wrongPI), -1, wbody); err != nil {
		t.Fatal(err)
	}
	// (f) an orphan marker whose body does not decode
	garbage := uint16(12)
	if _, err := e.asm.jsa.storeMsg(markerSubj(garbage), -1, []byte("not a marker")); err != nil {
		t.Fatal(err)
	}

	// The grace goes to zero only now, after every fixture record exists: the account
	// manager's own sweep goroutine shares the quarantine, and a zero grace during the
	// seeding would let it take a first sighting the test's first pass then completes.
	zero := time.Duration(0)
	budget := 64
	e.asm.mu.Lock()
	e.asm.testOrphanGrace = &zero
	e.asm.testQoS2SweepRecordLimit = &budget
	e.asm.mu.Unlock()

	all := []uint16{orphan, live, rivalled, unreadable, wrongPI, garbage}
	pass := func() {
		t.Helper()
		_, markers, err := e.asm.collectInflightCandidates(e.s)
		if err != nil {
			t.Fatalf("collectInflightCandidates: %v", err)
		}
		// Every marker on the stream is PARSED as one (M23): none is logged away as an
		// unattributable register.
		seen := map[string]bool{}
		for _, m := range markers {
			seen[m.name.subject] = true
		}
		for _, pi := range all {
			if _, smsg := e.loadMarker(t, pi); smsg != nil && !seen[e.regSubject(pi)] {
				t.Fatalf("the marker beside %d is on the stream but the sweep did not parse it", pi)
			}
		}
		b := 16
		e.asm.reconcileOrphanMarkers(e.s, markers, &b)
	}
	pass() // the first sighting quarantines
	pass() // the second acts
	for _, tc := range []struct {
		name string
		pi   uint16
		kept bool
	}{
		{"row 24: an orphan marker is reaped", orphan, false},
		{"row 24: an undecodable orphan marker is reaped", garbage, false},
		{"row 26: a live pair's marker is kept", live, true},
		{"row 25: a marker whose sibling is another exchange is reaped", rivalled, false},
		{"row 27: an unreadable sibling keeps its marker", unreadable, true},
		{"row 27: a sibling naming the wrong identifier is not readable, and keeps its marker", wrongPI, true},
	} {
		if tc.kept {
			if m, smsg := e.loadMarker(t, tc.pi); smsg == nil {
				t.Errorf("%s: the marker was deleted (%+v)", tc.name, m)
			}
			continue
		}
		waitFor(t, tc.name, func() bool {
			_, smsg := e.loadMarker(t, tc.pi)
			return smsg == nil
		})
	}
}

// ---------------------------------------------------------------------------
// T20 / T20a / T20c (rows 2-3, N-7.2): a marker that cannot be read as ours or as absent
// after the pair committed installs the marker-opaque reservation; the next resume that
// reads the pair whole replaces it. Mutations M33 (lean on "in-memory wins"), M39 (read an
// absent marker as transmitted), M41 (ignore InstallTransmitted for an opaque reservation).
// ---------------------------------------------------------------------------

func TestMQTTInflightMarkerOpaqueReservation(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-marker-opaque")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)

	e.bind()
	// Admitted as production admits it (F-105 7b): the pair lands beside its exchange and duty
	// entry, and the admission's settlement is the step that reads the marker.
	e.admissionPrerequisites(t, dur, 1)
	e.acceptUnderServingAuthority(t)
	// Step 3 cannot read the marker: the settlement fails closed into the reservation.
	e.asm.mu.Lock()
	e.asm.testInflightMarkerLoad = func(string) (*StoredMsg, error) { return nil, errors.New("test: unreadable") }
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("allocate: pi=%d isNew=%v", pi, isNew)
	}
	if e.sess.admitInflightPublish(e.c, pi, 1, dur, sseq, nonce, _EMPTY_, _EMPTY_) {
		t.Fatalf("row 3: an unreadable marker must not release the identifier for sending")
	}
	e.asm.mu.Lock()
	e.asm.testInflightMarkerLoad = nil
	e.asm.mu.Unlock()
	present, transmitted, opaque, _ := e.entryState(pi)
	e.sess.mu.Lock()
	incomplete := e.sess.piSpaceIncomplete
	_, mapped := e.sess.cpending[dur][sseq]
	_, sendOK := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	rePI, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if !present || transmitted || !opaque || !incomplete || mapped || sendOK || rePI != 0 {
		t.Fatalf("the marker-opaque reservation: present=%v transmitted=%v opaque=%v incomplete=%v mapped=%v sendOK=%v redelivery pi=%d",
			present, transmitted, opaque, incomplete, mapped, sendOK, rePI)
	}
	if cur, _ := e.loadRegister(t, pi); cur == nil || cur.Nonce != nonce {
		t.Fatalf("the landed pair stands; nothing is repaired by creating or deleting: %+v", cur)
	}

	// The next resume reads the pair whole: the reservation is REPLACED by the unsent
	// entry, its mapping rebuilt, allocation resumed (N-7.2's install row; T20a).
	// Exercise the nonce-only licence independently of constructor completeness:
	// QoS is not a prerequisite for this replacement permission.
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi].qos = 0
	e.sess.mu.Unlock()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	_, mmsg := e.loadMarker(t, pi)
	present, transmitted, opaque, markerSeq := e.entryState(pi)
	e.sess.mu.Lock()
	incomplete = e.sess.piSpaceIncomplete
	_, mapped = e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if !present || transmitted || opaque || markerSeq != mmsg.Sequence || incomplete || !mapped {
		t.Fatalf("the resume must install the unsent entry over the reservation: present=%v transmitted=%v opaque=%v markerSeq=%d (want %d) incomplete=%v mapped=%v",
			present, transmitted, opaque, markerSeq, mmsg.Sequence, incomplete, mapped)
	}

	// T20c: the reservation again, then the marker gone before the resume (row 2 then
	// row 5): the resume installs TRANSMITTED and the next offer carries DUP=1.
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	ack.markerOpaque, ack.transmitted, ack.markerSeq = true, false, 0
	delete(e.sess.cpending[dur], sseq)
	e.sess.piSpaceIncomplete = true
	e.sess.mu.Unlock()
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, mmsg.Sequence, true); err != nil {
		t.Fatalf("delete the marker: %v", err)
	}
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	present, transmitted, opaque, markerSeq = e.entryState(pi)
	e.sess.mu.Lock()
	incomplete = e.sess.piSpaceIncomplete
	tok, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !present || !transmitted || opaque || markerSeq != 0 || incomplete || !ok || !tok.dup {
		t.Fatalf("T20c: want a transmitted entry sending DUP=1: present=%v transmitted=%v opaque=%v markerSeq=%d incomplete=%v ok=%v dup=%v",
			present, transmitted, opaque, markerSeq, incomplete, ok, tok.dup)
	}
}

// ---------------------------------------------------------------------------
// T6d (N-6.4, rows 15-17): a same-delivery occupant is replaced as a PAIR when its marker
// is present, as a register alone when it is absent, and not at all when it cannot be
// read. Mutations M42 (delete then create outside the batch), M44 (mint a marker on the
// register-only arm), M45 (drop the phase gate).
// ---------------------------------------------------------------------------

func TestMQTTInflightSameDeliveryOccupantIsReplacedAsAPair(t *testing.T) {
	// NOT parallel: installs process-global hooks.
	log := installMarkerLog(t)
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, e *testInflightEnv, pi uint16) // stage the occupant's marker
		check   func(t *testing.T, e *testInflightEnv, cB *client, sessB *mqttSession, pi uint16, nonceB string, stored bool)
	}{
		{"row 15: a present marker is replaced with the register in one batch",
			func(t *testing.T, e *testInflightEnv, pi uint16) {},
			func(t *testing.T, e *testInflightEnv, cB *client, sessB *mqttSession, pi uint16, nonceB string, stored bool) {
				if !stored {
					t.Fatalf("B's store must succeed")
				}
				cur, _ := e.loadRegister(t, pi)
				m, mmsg := e.loadMarker(t, pi)
				if cur == nil || cur.Nonce != nonceB || m == nil || m.Nonce != nonceB {
					t.Fatalf("both records must carry B's nonce: register %+v marker %+v", cur, m)
				}
				sessB.mu.Lock()
				ack := sessB.pendingPublish[pi]
				sessB.mu.Unlock()
				if ack == nil || ack.transmitted || ack.markerOpaque || ack.markerSeq != mmsg.Sequence {
					t.Fatalf("B's entry must be unsent holding the new marker %d, got %+v", mmsg.Sequence, ack)
				}
				if n := len(log.deletesBy(mqttMarkerCallerReplace)) + len(log.deletesBy(mqttMarkerCallerStore)); n != 0 {
					t.Fatalf("M42: the pair replacement must issue no marker delete inside the store interval, got %d", n)
				}
			}},
		{"row 16: an absent marker means a transmitted occupant; the register alone is rewritten and no marker is minted",
			func(t *testing.T, e *testInflightEnv, pi uint16) {
				_, mmsg := e.loadMarker(t, pi)
				if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, mmsg.Sequence, true); err != nil {
					t.Fatal(err)
				}
			},
			func(t *testing.T, e *testInflightEnv, cB *client, sessB *mqttSession, pi uint16, nonceB string, stored bool) {
				if !stored {
					t.Fatalf("B's store must succeed")
				}
				cur, _ := e.loadRegister(t, pi)
				if cur == nil || cur.Nonce != nonceB {
					t.Fatalf("the register must carry B's nonce, got %+v", cur)
				}
				if m, _ := e.loadMarker(t, pi); m != nil {
					t.Fatalf("M44: a register-only replacement must mint no marker, found %+v", m)
				}
				sessB.mu.Lock()
				ack := sessB.pendingPublish[pi]
				sessB.c = cB
				tok, ok := sessB.beginSendLocked(cB, pi, ack.jsDur, ack.sseq)
				sessB.mu.Unlock()
				if ack == nil || !ack.transmitted || ack.markerSeq != 0 || !ok || !tok.dup {
					t.Fatalf("B's entry must be transmitted, none held, and send DUP=1: %+v ok=%v dup=%v", ack, ok, tok.dup)
				}
			}},
		{"row 17: an unreadable marker withholds and writes nothing",
			func(t *testing.T, e *testInflightEnv, pi uint16) {
				if _, err := e.asm.jsa.storeMsg(e.regSubject(pi)+"."+mqttInflightMarkerSuffix, -1, []byte("not a marker")); err != nil {
					t.Fatal(err)
				}
			},
			func(t *testing.T, e *testInflightEnv, cB *client, sessB *mqttSession, pi uint16, nonceB string, stored bool) {
				if stored {
					t.Fatalf("B's store must be withheld")
				}
				cur, _ := e.loadRegister(t, pi)
				if cur == nil || cur.Nonce == nonceB {
					t.Fatalf("nothing may be written: the occupant must stand, got %+v", cur)
				}
				sessB.mu.Lock()
				ack := sessB.pendingPublish[pi]
				incomplete := sessB.piSpaceIncomplete
				sessB.mu.Unlock()
				if ack == nil || !ack.markerOpaque || !incomplete {
					t.Fatalf("B must hold the marker-opaque reservation with allocation withheld, got %+v incomplete=%v", ack, incomplete)
				}
				// It stands over the OCCUPANT, so it names the occupant's obligation — its
				// lineage and exchange nonce from the record the replacement arm decoded — never
				// the failed proposal's, and it acts for nothing (pending-entry closure, M14).
				if ack.kind != mqttPendingHold || ack.ev.lin != cur.Lin || ack.ev.nonce != cur.Nonce || ack.regNonce != cur.Nonce {
					t.Fatalf("the reservation must carry the occupant's evidence (lin %q nonce %q), got kind %d ev %+v nonce %q",
						cur.Lin, cur.Nonce, ack.kind, ack.ev, ack.regNonce)
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log.reset()
			e := testNewInflightEnv(t, "client-replace-pair")
			dur := e.sess.idHash + "_" + nuid.Next()
			const sseq = uint64(4300)
			pi := e.track(t, dur, sseq, 2)
			tc.prepare(t, e, pi)
			if err := testSessSave(t, e.sess); err != nil {
				t.Fatalf("persist A's session: %v", err)
			}
			_, sessB, cB := e.peer(t, "node2", "client-replace-pair")
			sessB.maxp = 100
			if err := testSessSave(t, sessB); err != nil {
				t.Fatalf("persist B's record: %v", err)
			}
			sessB.mu.Lock()
			piB, isNew := sessB.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
			nonceB := _EMPTY_
			if ack := sessB.pendingPublish[piB]; ack != nil {
				nonceB = ack.regNonce
			}
			sessB.mu.Unlock()
			if piB != pi {
				t.Skipf("this fixture needs B to allocate the occupied identifier %d, got %d", pi, piB)
			}
			if !isNew {
				t.Fatalf("B must allocate freshly to reach the replacement arm")
			}
			testOpenStoreCustody(t, sessB, piB)
			stored := sessB.storeInflightRegister(cB, piB, 2, dur, sseq, nonceB, _EMPTY_, _EMPTY_)
			tc.check(t, e, cB, sessB, pi, nonceB, stored)
		})
	}
}

// ---------------------------------------------------------------------------
// Row 21: a PUBACK is protocol evidence, and it cleans a CAPTURED marker at exactly its
// sequence, attributed to evidence. With intent recorded BEFORE the write, a transmitted
// exchange never holds a marker at PUBACK time, so the only live route here is a PUBACK the
// client sends for an identifier whose PUBLISH this broker never wrote — a client anomaly
// the broker still completes rather than leaving a pair behind. (The former fixture,
// "the sender's post-write delete was lost", stages a state the protocol now forbids.)
// Mutation M53-adjacent: attribute the delete to the sender, or delete none.
// ---------------------------------------------------------------------------

func TestMQTTInflightPubAckCleansTheCapturedMarker(t *testing.T) {
	// NOT parallel: installs process-global hooks.
	e := testNewInflightEnv(t, "client-puback-marker")
	log := installMarkerLog(t)
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	// A double ACK needs an actual JetStream offer. Merely fabricating an
	// ACK subject for an undelivered source is not settlement evidence.
	ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, dur)
	if err != nil || ci == nil {
		t.Fatalf("source consumer: %v", err)
	}
	sub, err := e.s.nrc.SubscribeSync(ci.Config.DeliverSubject)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatal(err)
	}
	offer, err := sub.NextMsg(5 * time.Second)
	if err != nil || offer == nil {
		t.Fatalf("source offer: %v", err)
	}
	if !mqttSourceAckReplyBinds(offer.Reply, mqttStreamName, dur, sseq) {
		t.Fatal("the offer must name this source")
	}
	pi := e.trackAdmitted(t, dur, sseq, 1, offer.Reply)
	_, mmsg := e.loadMarker(t, pi)
	if _, transmitted, _, mseq := e.entryState(pi); transmitted || mseq != mmsg.Sequence {
		t.Fatalf("precondition: a fresh pair is proven-unsent and holds its marker, got %v %d", transmitted, mseq)
	}

	e.c.mqtt = &mqtt{sess: e.sess, proto: mqttProtoLevel5}
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	if err := e.c.mqttProcessPublishReceived(pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	dels := log.deletesBy(mqttMarkerCallerEvidence)
	if len(dels) != 1 || dels[0].seq != mmsg.Sequence {
		t.Fatalf("row 21: the PUBACK must delete the captured marker %d attributed to protocol evidence, got %+v", mmsg.Sequence, dels)
	}
	e.waitMarkerAbsent(t, pi)
	e.waitRegisterCount(t, 0)
}

func TestMQTTInflightPubRecRollbackMarksTransmitted(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-retrack")
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	ack := &mqttPending{jsDur: "d", sseq: 1, regSeq: 4, regNonce: mqttNewInflightNonce(), markerSeq: 3}
	e.sess.retrackPublishLocked(5, ack)
	if !ack.transmitted || ack.markerSeq != 3 {
		t.Fatalf("a PUBREC rollback restores a TRANSMITTED entry carrying its marker sequence, got %+v", ack)
	}
}

// ---------------------------------------------------------------------------
// T27a (N-5.1, M53): every marker delete in production is issued by a function that asked
// the boundary, and the boundary is consulted only by the enumerated consumers. Both
// directions, in the shape of TestMQTTInflightRecordsAreDecodedThroughOneAPI.
// ---------------------------------------------------------------------------

func TestMQTTMarkerDecisionsGoThroughOneBoundary(t *testing.T) {
	t.Parallel()
	// The functions that may consult the boundary: the store path's derivation, the
	// replacement arm, the sender, the evidence consumers, the reap executor — and the
	// WRITE BOUNDARY, which re-asks it for every replacing register a transition carries
	// (F-105, row 1). That last one is the point of the enumeration rather than an exception
	// to it: the boundary must reach the same verdict as the composer, and it reaches it by
	// asking the same table rather than by restating it.
	consumers := map[string]bool{
		"mqttJSA.checkRegisterReplacements":                          true,
		"mqttSession.settleLandedRegister":                           true,
		"mqttSession.runInflightAdmission":                           true,
		"mqttSession.recordSendIntent":                               true,
		"mqttSession.recoverInflightRegistersOnce":                   true,
		"mqttAccountSessionManager.executeReap":                      true,
		"mqttAccountSessionManager.cleanMarkerAfterEvidence":         true,
		"mqttAccountSessionManager.cleanMarkerAfterEvidenceReloaded": true,
	}
	// The functions that may issue a marker delete: each consults the boundary in the
	// same body, so no delete is decided anywhere else.
	deleters := map[string]bool{
		"mqttSession.recordSendIntent":                               true,
		"mqttAccountSessionManager.executeReap":                      true,
		"mqttAccountSessionManager.cleanMarkerAfterEvidence":         true,
		"mqttAccountSessionManager.cleanMarkerAfterEvidenceReloaded": true,
	}
	consulted := map[string]int{}
	deleted := map[string]int{}
	declared := map[string]bool{}
	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, _ := decl.(*ast.FuncDecl)
			if fn == nil || fn.Body == nil {
				continue
			}
			name := testQualifiedFuncName(fn)
			declared[name] = true
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, _ := n.(*ast.CallExpr)
				if call == nil {
					return true
				}
				switch testCalleeName(call) {
				case "mqttDecideMarker":
					consulted[name]++
					if !consumers[name] && name != "mqttDecideMarker" {
						t.Errorf("%s: %s consults the marker boundary but is not an enumerated consumer (N-5.1)", fset.Position(call.Pos()), name)
					}
				case "deleteInflightMarker":
					deleted[name]++
					if !deleters[name] && name != "deleteInflightMarker" {
						t.Errorf("%s: %s issues a marker delete without a disposition from the boundary (M53)", fset.Position(call.Pos()), name)
					}
				}
				return true
			})
		}
	}
	for name := range consumers {
		if !declared[name] {
			t.Errorf("the consumer list names %q, which no production declaration matches", name)
		} else if consulted[name] == 0 {
			t.Errorf("the consumer list names %q, which no longer consults the boundary", name)
		}
	}
	for name := range deleters {
		if deleted[name] == 0 {
			t.Errorf("the deleter list names %q, which no longer issues a marker delete", name)
		}
		if consulted[name] == 0 {
			t.Errorf("%q issues marker deletes but consults no boundary in its own body", name)
		}
	}
	if len(consulted) == 0 || len(deleted) == 0 {
		t.Fatal("no boundary consult or marker delete was found at all, so this guard checks nothing")
	}
}

// ---------------------------------------------------------------------------
// The one marker-opaque decline every settling path shares.
// ---------------------------------------------------------------------------

func TestMQTTInflightMarkerOpaqueEntryIsDeclinedEverywhere(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-opaque-declines")
	dur := e.sess.idHash + "_" + nuid.Next()
	pi := e.track(t, dur, 900, 1)
	e.bind()
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	// The marker-opaque reservation as production builds it (mqttNewMarkerOpaque: a Hold), kept
	// at the fixture's pointer because the assertions below hold it.
	*ack = *mqttNewMarkerOpaque(ack, ack.ev, ack.regSeq, ack.qos, ack.slot, true)
	_, sendOK := e.sess.beginSendLocked(e.c, pi, dur, 900)
	rePI, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 900, 2))
	redeliverable := mqttPendingRedeliverable(ack)
	e.sess.mu.Unlock()
	if sendOK || rePI != 0 || redeliverable {
		t.Fatalf("a marker-opaque entry must be declined by the section, the redelivery and the NAK: send=%v pi=%d redeliverable=%v", sendOK, rePI, redeliverable)
	}
	if e.sess.deleteInflightRegisterFor(ack, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil)) {
		t.Fatalf("terminal cleanup must refuse to settle a marker-opaque reservation")
	}
	if cur, _ := e.loadRegister(t, pi); cur == nil {
		t.Fatalf("nothing may be destroyed under a marker-opaque reservation")
	}
	e.c.mqtt = &mqtt{sess: e.sess, proto: mqttProtoLevel5}
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	if err := e.c.mqttProcessPublishReceived(pi, false, 0); err != nil {
		t.Fatalf("the PUBACK decline must be silent, got %v", err)
	}
	present, _, opaque, _ := e.entryState(pi)
	if !present || !opaque {
		t.Fatalf("the reservation must stand after a declined acknowledgement, got present=%v opaque=%v", present, opaque)
	}
	_ = fmt.Sprintf
	_ = strings.Contains
}

// The send-path test hooks are exercised end-to-end by the fulltests e2e suite (T1, T2);
// these unit checks pin their setter/restore contract and their abort semantics in the
// default build, so the setters are not dead code there.
func TestMQTTInflightSendHooksAbortAndRestore(t *testing.T) {
	// NOT parallel: installs the process-global pre-send and send-window hooks.
	e := testNewInflightEnv(t, "client-send-hooks")
	dur := e.sess.idHash + "_" + nuid.Next()

	// The pre-send hook declines before tracking: no register is written and the source
	// is left for AckWait.
	restorePre := setMQTTTestInflightPreSendHook(func(string, string) bool { return false })
	if mqttInflightPreSendAdmits(dur, testInflightAckSubject(dur, 1, 1)) {
		t.Fatal("the pre-send hook must be able to decline")
	}
	restorePre()
	if !mqttInflightPreSendAdmits(dur, testInflightAckSubject(dur, 1, 1)) {
		t.Fatal("restoring the pre-send hook must re-admit")
	}

	// The reserved-window hook aborts a send section; every other window proceeds.
	restoreWin := setMQTTTestMarkerSendWindowHook(func(_ *mqttSession, _ uint16, window string) bool {
		return window != mqttMarkerSendReserved
	})
	if e.sess.markerSendWindowHook(1, mqttMarkerSendReserved) {
		t.Fatal("the reserved window must be abortable")
	}
	if !e.sess.markerSendWindowHook(1, mqttMarkerSendWrote) {
		t.Fatal("the wrote window always proceeds")
	}
	restoreWin()
	if !e.sess.markerSendWindowHook(1, mqttMarkerSendReserved) {
		t.Fatal("restoring the send-window hook must let the section proceed")
	}
}

// ---------------------------------------------------------------------------
// T5i / T5j: terminal cleanup's register delete is CONFIRMED before the marker reap reads
// the sibling. An unconfirmed delete is still present at that read, which classifies it as
// a live same-exchange sibling and PRESERVES the marker (row 26) — stranding a completed
// exchange's marker so a resume re-offers it as unsent. Fault injection proves the
// completion dependency without needing the original queue schedule: the async wrapper
// DISCARDS its delete error while the confirmed one retries and propagates it.
//
// Mutation M58: revert deleteInflightRegisterFor's mustConfirm to false. The injected
// first-attempt error is then dropped unsent, the sibling read finds the register, and
// T5i's disposition assertion fails with row 26; T5j's return-value assertion fails too.
// ---------------------------------------------------------------------------

// failRegisterDeletes intercepts stream deletes of the register on subject, answering the first
// `failures` attempts with an injected error and allowing every later attempt through to the real
// delete. It returns a restore func and a pointer to the attempt count.
//
// The register is named by its SUBJECT, not by a sequence captured at setup: an admitted unsent
// register is withdrawn before it is destroyed, so the delete acts on its tombstone's sequence.
func failRegisterDeletes(t *testing.T, e *testInflightEnv, subject string, failures int) (func(), *int32) {
	t.Helper()
	var attempts int32
	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream != mqttInflightStreamName || !e.inflightSeqNames(seq, subject) {
			return nil
		}
		if int(atomic.AddInt32(&attempts, 1)) <= failures {
			return errors.New("test: the register delete is rejected")
		}
		return nil
	})
	return restore, &attempts
}

// inflightSeqNames reports whether the in-flight stream's message at seq is on subject.
func (e *testInflightEnv) inflightSeqNames(seq uint64, subject string) bool {
	smsg, err := e.asm.jsa.loadMsg(mqttInflightStreamName, seq)
	return err == nil && smsg != nil && smsg.Subject == subject
}

func TestMQTTInflightTerminalCleanupConfirmsTheRegisterBeforeReapingTheMarker(t *testing.T) {
	// NOT parallel: installs process-global hooks.
	e := testNewInflightEnv(t, "client-cleanup-confirm")
	log := installMarkerLog(t)
	dur := e.sess.idHash + "_" + nuid.Next()
	pi := e.track(t, dur, 700, 1)
	_, mmsg := e.loadMarker(t, pi)

	// The first delete attempt is rejected; the retry is allowed through.
	restore, attempts := failRegisterDeletes(t, e, e.regSubject(pi), 1)
	defer restore()

	e.sess.mu.Lock()
	removed := e.sess.untrackPublishEntry(pi, testUntrackNoBinding)
	e.sess.mu.Unlock()
	if !e.sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil)) {
		t.Fatalf("the confirmed delete retries past a transient rejection, so cleanup is proven")
	}
	if n := atomic.LoadInt32(attempts); n < 2 {
		t.Fatalf("the register delete must be RETRIED past the injected failure, got %d attempt(s)", n)
	}
	// The sibling was read only after the register was proven gone, so the boundary saw an
	// absent register and completed the pair rather than preserving the marker.
	if v, ok := log.lastDisposition(mqttMarkerCallerCleanup); !ok || v.disposition != mqttMarkerDeleteMarker {
		t.Fatalf("want DeleteMarker once the register delete is confirmed, got %+v (%v)", v, ok)
	}
	if dels := log.deletesBy(mqttMarkerCallerCleanup); len(dels) != 1 || dels[0].seq != mmsg.Sequence {
		t.Fatalf("the marker delete must target the captured sequence %d, got %+v", mmsg.Sequence, dels)
	}
	e.waitRegisterCount(t, 0)
	e.waitMarkerAbsent(t, pi)
}

func TestMQTTInflightTerminalCleanupWithUnprovableRegisterDeleteReapsNoMarker(t *testing.T) {
	// NOT parallel: installs process-global hooks.
	e := testNewInflightEnv(t, "client-cleanup-exhausted")
	log := installMarkerLog(t)
	dur := e.sess.idHash + "_" + nuid.Next()
	pi := e.track(t, dur, 701, 1)

	// EVERY attempt is rejected: the delete is never proven, so nothing downstream may act
	// on an absence that was never established.
	restore, _ := failRegisterDeletes(t, e, e.regSubject(pi), 1<<30)
	defer restore()

	e.sess.mu.Lock()
	removed := e.sess.untrackPublishEntry(pi, testUntrackNoBinding)
	e.sess.mu.Unlock()
	if e.sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil)) {
		t.Fatalf("an unprovable register delete must not report the cleanup as proven")
	}
	if n := len(log.deletesBy(mqttMarkerCallerCleanup)); n != 0 {
		t.Fatalf("no marker delete may follow an unproven register half, got %d", n)
	}
	if m, _ := e.loadMarker(t, pi); m == nil {
		t.Fatalf("the marker must survive: its exchange's register was never proven gone")
	}
	if cur, _ := e.loadRegister(t, pi); cur == nil {
		t.Fatalf("the register must survive an exhausted delete")
	}
}

// ---------------------------------------------------------------------------
// The release gate (contract S-4): only a PROVEN-UNSENT entry may be discarded as never
// transmitted, by Message Expiry or an UNSUBSCRIBE drain's withdrawal. A possibly-attempted
// entry (marker gone) and an UNRESOLVED-intent entry (marker may be gone) are neither
// expirable nor withdrawn: the drain ENROLS them like a transmitted delivery. Mutation
// M64: gate on !transmitted alone — the unresolved case then leaks; M11: enrol nothing.
// ---------------------------------------------------------------------------

func TestMQTTInflightPossiblyAttemptedEntryIsNeitherExpirableNorWithdrawn(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-release-gate")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}

	// Three shapes: proven-unsent, possibly-attempted (intent confirmed, nothing written),
	// unresolved intent.
	unsent := e.track(t, dur, 800, 1)
	attempted := e.track(t, dur, 801, 1)
	tok := e.openSection(t, attempted, dur, 801)
	if got := e.sess.recordSendIntent(e.c, attempted, tok); got != mqttIntentConfirmed {
		t.Fatalf("recordSendIntent: %d", got)
	}
	e.sess.endPublishSend(e.c, attempted, tok, mqttIntentConfirmed, false) // aborted before the write
	unresolved := e.track(t, dur, 802, 1)
	e.sess.mu.Lock()
	e.sess.pendingPublish[unresolved].intentUnresolved = true
	e.sess.mu.Unlock()

	e.sess.mu.Lock()
	rel := map[string]bool{
		"proven-unsent":      e.sess.provenUnsentReleasableLocked(e.sess.pendingPublish[unsent]),
		"possibly-attempted": e.sess.provenUnsentReleasableLocked(e.sess.pendingPublish[attempted]),
		"unresolved intent":  e.sess.provenUnsentReleasableLocked(e.sess.pendingPublish[unresolved]),
	}
	e.sess.mu.Unlock()
	if !rel["proven-unsent"] || rel["possibly-attempted"] || rel["unresolved intent"] {
		t.Fatalf("release gate: want only proven-unsent releasable, got %v", rel)
	}

	// The drain WITHDRAWS the proven-unsent entry and ENROLS the other two.
	e.sess.mu.Lock()
	e.sess.cons = map[string]*ConsumerConfig{sid: cc}
	staged := e.sess.stageDrain(sid, cc, nil, mqttTestLicence(e.sess, nil))
	d := e.sess.drains[sid]
	enrolled := map[uint16]bool{}
	if d != nil {
		for _, st := range d.seqs {
			enrolled[st.pi] = true
		}
	}
	_, unsentTracked := e.sess.pendingPublish[unsent]
	withdrawn := e.sess.commitDrains()
	e.sess.mu.Unlock()
	if !staged || d == nil {
		t.Fatalf("the drain must stage")
	}
	if unsentTracked || len(withdrawn) != 1 || withdrawn[0].pi != unsent {
		t.Fatalf("exactly the proven-unsent entry is withdrawn, got tracked=%v withdrawn=%+v", unsentTracked, withdrawn)
	}
	if !enrolled[attempted] || !enrolled[unresolved] || len(enrolled) != 2 {
		t.Fatalf("the possibly-attempted and unresolved entries must be ENROLLED, got %v", enrolled)
	}
}

// ---------------------------------------------------------------------------
// The release gate's FOURTH caller (F-100): the resume reap. A pending entry whose source
// message is authoritatively gone from $MQTT_msgs has nothing left to redeliver, but the
// source vanishing is a fact about the BROKER's ability to continue, not about what the
// client holds. Only a PROVEN-UNSENT exchange may be released with its Receive Maximum
// slot; a possibly-attempted or unresolved-intent one stays RESERVED, with its source
// mapping kept so a callback already holding the payload cannot be given a second
// identifier for one source.
//
// Mutations: M1 release every candidate (the attempted and unresolved arms survive);
// M2 gate on !transmitted alone (the unresolved arm survives); M6 return before the reap
// block (the proven-unsent POSITIVE CONTROL never fires, so the fixture is proven to reach
// the route rather than assumed to).
// ---------------------------------------------------------------------------

func TestMQTTInflightResumeReapReservesAPossiblyAttemptedEntry(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-resume-reap")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Three shapes over sequences never published to $MQTT_msgs, so every one is a
	// CONFIRMED absence and every one is a reap candidate.
	const unsentSeq, attemptedSeq, unresolvedSeq = 900, 901, 902
	unsent := e.track(t, dur, unsentSeq, 1)
	attempted := e.track(t, dur, attemptedSeq, 1)
	tok := e.openSection(t, attempted, dur, attemptedSeq)
	if got := e.sess.recordSendIntent(e.c, attempted, tok); got != mqttIntentConfirmed {
		t.Fatalf("recordSendIntent: %d", got)
	}
	e.sess.endPublishSend(e.c, attempted, tok, mqttIntentConfirmed, false) // aborted before the write
	unresolved := e.track(t, dur, unresolvedSeq, 1)
	e.sess.mu.Lock()
	e.sess.pendingPublish[unresolved].intentUnresolved = true
	rel := map[string]bool{
		"proven-unsent":      e.sess.provenUnsentReleasableLocked(e.sess.pendingPublish[unsent]),
		"possibly-attempted": e.sess.provenUnsentReleasableLocked(e.sess.pendingPublish[attempted]),
		"unresolved intent":  e.sess.provenUnsentReleasableLocked(e.sess.pendingPublish[unresolved]),
	}
	snapshot := mqttPendingRefs(e.sess.pendingPublish)
	e.sess.mu.Unlock()
	// FIXTURE guard, not a verdict: if these three stop modelling the three shapes the
	// assertions below prove nothing about the gate.
	if !rel["proven-unsent"] || rel["possibly-attempted"] || rel["unresolved intent"] {
		t.Fatalf("fixture: want only proven-unsent releasable, got %v", rel)
	}

	e.sess.resendPendingRedelivery(e.c, snapshot, nil)

	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	mapped := func(sseq uint64) bool {
		_, ok := e.sess.cpending[dur][sseq]
		return ok
	}
	// THE POSITIVE CONTROL. Only the reap block can release this entry, so a fixture that
	// never reached it — no $MQTT_msgs stream, no ack subject, the ref outside the resume
	// snapshot — fails here instead of passing vacuously.
	if _, live := e.sess.pendingPublish[unsent]; live || mapped(unsentSeq) {
		t.Fatalf("the PROVEN-UNSENT entry must be released with its source mapping: the client never saw it (tracked=%v mapped=%v)", live, mapped(unsentSeq))
	}
	for name, pi := range map[string]uint16{"possibly-attempted": attempted, "unresolved intent": unresolved} {
		sseq := attemptedSeq
		if pi == unresolved {
			sseq = unresolvedSeq
		}
		if _, live := e.sess.pendingPublish[pi]; !live {
			t.Fatalf("the %s entry must stay RESERVED: the receiver may hold packet identifier %d from an attempt whose acknowledgement was lost", name, pi)
		}
		if !mapped(uint64(sseq)) {
			t.Fatalf("the %s entry's source mapping must be KEPT, so a callback still holding the payload reuses identifier %d rather than being given a second one", name, pi)
		}
	}
}

// The identity fence in the same reap: the scan runs with the session lock released, so an
// entry it classified can be untracked and its identifier re-minted for a DIFFERENT
// delivery before the final lock. A packet identifier is not a delivery identity —
// (durable, sequence) is — and acting on the identifier alone destroys a delivery this pass
// never looked at.
//
// The replacement is deliberately left regPending, which the release predicate accepts as
// proven-unsent, so a mutation keying on the identifier alone RELEASES it while the real
// check refuses to touch it. Mutation M5.
func TestMQTTInflightResumeReapRefusesAReplacedEntry(t *testing.T) {
	// NOT parallel: installs a process-global restore hook.
	e := testNewInflightEnv(t, "client-reap-replaced")
	dur := e.sess.idHash + "_" + nuid.Next()
	const candidateSeq, replacementSeq = 910, 911

	pi := e.track(t, dur, candidateSeq, 1)
	e.sess.mu.Lock()
	snapshot := mqttPendingRefs(e.sess.pendingPublish)
	e.sess.mu.Unlock()

	// Between the scan and the final lock: untrack the candidate, force the allocator back
	// onto the same identifier, and install a different source there through raw
	// trackPublish — never e.track, which would also persist a register.
	var staged bool
	restore := setMQTTTestRestorePause(func(at string) {
		if at != "reap-scanned" || staged {
			return
		}
		staged = true
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		lin := e.sess.pendingPublish[pi].ev.lin
		e.sess.untrackPublishEntry(pi, testUntrackNoBinding)
		// The identifier is reusable only once its exchange has durably ended (F-105 row 5:
		// custody holds it until then), so the candidate's exchange is observed ended — the
		// withdrawal a never-transmitted candidate's untrack stands for — before the re-mint.
		if at, ok := e.sess.custody.beginObservation(e.sess.gen, lin); !ok ||
			!e.sess.custody.applyExchangeObservation(e.sess.gen, lin, at, &mqttOutboundExchange{Phase: mqttExchangeWithdrawnUnsent}) ||
			e.sess.custody.holdsPI(pi) {
			t.Errorf("fixture: the candidate's custody must end with its exchange (lin %q)", lin)
		}
		e.sess.last_pi = pi - 1
		got, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, replacementSeq, 1))
		if got != pi || !isNew {
			t.Errorf("fixture: the replacement must reuse identifier %d, got %d isNew=%v", pi, got, isNew)
		}
	})
	defer restore()

	e.sess.resendPendingRedelivery(e.c, snapshot, nil)

	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if !staged {
		t.Fatal("fixture: the reap-scanned boundary was never reached, so nothing was interleaved")
	}
	ack := e.sess.pendingPublish[pi]
	if ack == nil {
		t.Fatalf("identifier %d now names a DIFFERENT delivery, which this pass never classified; releasing it destroyed a live one", pi)
	}
	if ack.sseq != replacementSeq {
		t.Fatalf("fixture: the entry at %d must be the replacement (%d), got %d", pi, uint64(replacementSeq), ack.sseq)
	}
	if !e.sess.provenUnsentReleasableLocked(ack) {
		t.Fatal("fixture: the replacement must be releasable on its own state, or a mutation keying on the identifier alone would not release it and M5 would go unkilled")
	}
	if _, ok := e.sess.cpending[dur][replacementSeq]; !ok {
		t.Fatalf("the replacement's source mapping must be untouched")
	}
}

// ---------------------------------------------------------------------------
// Holistic round 2, finding 2: inside ONE confirmed intent delete, an attempt that got no
// answer may have landed, and no later definite refusal can prove it did not. The operation's
// outcome is therefore AMBIGUOUS, the entry is held unresolved, and nothing may release it.
// Returning the last error would have read "lost, refused, refused" as a definite refusal
// and left a proven-unsent entry over a marker that may be gone. Mutation M66: return the
// last attempt's error.
// ---------------------------------------------------------------------------

func TestMQTTInflightAmbiguityDominatesLaterRefusalsWithinOneIntent(t *testing.T) {
	// NOT parallel: installs a process-global hook.
	e := testNewInflightEnv(t, "client-intent-mixed")
	dur := e.sess.idHash + "_" + nuid.Next()
	pi := e.track(t, dur, 900, 1)
	_, before := e.loadMarker(t, pi)

	var attempts int32
	restore := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller != mqttMarkerCallerSender {
			return nil
		}
		if atomic.AddInt32(&attempts, 1) == 1 {
			return errors.New("test: the first attempt's response was lost") // ambiguous
		}
		// Every later attempt: an explicit PRE-MUTATION refusal, the only definite shape.
		return &ApiError{Code: 400, ErrCode: uint16(JSBadRequestErr), Description: "test: refused before any mutation"}
	})
	defer restore()

	tok := e.openSection(t, pi, dur, 900)
	got := e.sess.recordSendIntent(e.c, pi, tok)
	if n := atomic.LoadInt32(&attempts); n != int32(mqttInflightStoreAttempts) {
		t.Fatalf("the confirmed delete must exhaust its %d attempts, saw %d", mqttInflightStoreAttempts, n)
	}
	if got != mqttIntentAmbiguous {
		t.Fatalf("an unanswered attempt followed by refusals is AMBIGUOUS for the whole operation, got %d", got)
	}
	e.sess.endPublishSend(e.c, pi, tok, got, false)
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	releasable := e.sess.provenUnsentReleasableLocked(ack)
	e.sess.mu.Unlock()
	if ack == nil || !ack.intentUnresolved || ack.transmitted || ack.markerSeq != before.Sequence {
		t.Fatalf("the entry must be held UNRESOLVED with its captured marker %d, got %+v", before.Sequence, ack)
	}
	if releasable {
		t.Fatalf("an unresolved entry must not be releasable by expiry or withdrawal")
	}
	// The marker itself was never touched by the refused attempts: it is still there.
	if m, _ := e.loadMarker(t, pi); m == nil {
		t.Fatalf("no attempt was forwarded to the stream, so the marker must survive")
	}
}

// ---------------------------------------------------------------------------
// Holistic round 3, finding 1: a generic delete failure is NOT proof the delete changed
// nothing — the file store can record the deletion and then fail (a compaction error
// persists as its write error and is reported on every later attempt too). Only an explicit
// pre-mutation refusal is definite; JSStreamMsgDeleteFailedF on every attempt is AMBIGUOUS
// and holds the entry unresolved. Mutation M68: classify every API error as definite.
// ---------------------------------------------------------------------------

func TestMQTTInflightStorageDeleteFailureIsAmbiguous(t *testing.T) {
	// NOT parallel: installs a process-global hook.
	e := testNewInflightEnv(t, "client-intent-storefail")
	dur := e.sess.idHash + "_" + nuid.Next()
	pi := e.track(t, dur, 910, 1)
	_, before := e.loadMarker(t, pi)
	restore := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller != mqttMarkerCallerSender {
			return nil
		}
		return &ApiError{Code: 500, ErrCode: uint16(JSStreamMsgDeleteFailedF), Description: "test: compaction failed after the deletion was recorded"}
	})
	defer restore()
	tok := e.openSection(t, pi, dur, 910)
	if got := e.sess.recordSendIntent(e.c, pi, tok); got != mqttIntentAmbiguous {
		t.Fatalf("a generic delete failure may follow a recorded deletion: want Ambiguous, got %d", got)
	}
	e.sess.endPublishSend(e.c, pi, tok, mqttIntentAmbiguous, false)
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	releasable := e.sess.provenUnsentReleasableLocked(ack)
	e.sess.mu.Unlock()
	if ack == nil || !ack.intentUnresolved || ack.markerSeq != before.Sequence || releasable {
		t.Fatalf("the entry must be held unresolved and unreleasable, got %+v releasable=%v", ack, releasable)
	}
}

// ---------------------------------------------------------------------------
// Holistic round 3, finding 2: a PUBLISH-phase register whose SOURCE is gone (acknowledged,
// or reaped by its Message Expiry TTL) is not thereby finished on the receiver's side.
// Recovery drops it only when the marker proves it never transmitted; a possibly-attempted
// one is RESERVED — installed transmitted, identifier held, no source mapping — because the
// receiver may hold that identifier from an attempt whose acknowledgement was lost.
// Mutation M69: drop every complete-source register regardless of its marker.
// ---------------------------------------------------------------------------

func TestMQTTInflightRecoveryReservesPossiblyAttemptedRegisterWhoseSourceIsGone(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-source-gone")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Possibly-attempted: intent recorded (marker gone), then the source disappears.
	sseqA := e.publishSource(t, dur)
	attempted := e.track(t, dur, sseqA, 1)
	e.transmit(t, attempted, dur, sseqA, true)
	e.waitMarkerAbsent(t, attempted)
	// Proven-unsent: the pair is intact, then the source disappears.
	sseqU := e.publishSource(t, dur)
	unsent := e.track(t, dur, sseqU, 1)
	for _, sseq := range []uint64{sseqA, sseqU} {
		if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
			t.Fatalf("remove the source %d: %v", sseq, err)
		}
	}
	e.forgetInMemory()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	held := e.sess.pendingPublish[attempted]
	_, unsentHeld := e.sess.pendingPublish[unsent]
	_, mapped := e.sess.cpending[dur][sseqA]
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if held == nil || !held.transmitted || held.markerOpaque || held.regForeign {
		t.Fatalf("the possibly-attempted identifier %d must be RESERVED as transmitted, got %+v", attempted, held)
	}
	if mapped || incomplete {
		t.Fatalf("a finished source gets no mapping (%v) and the space is complete (incomplete=%v)", mapped, incomplete)
	}
	if unsentHeld {
		t.Fatalf("the proven-unsent register over a finished source is dropped, not reserved")
	}
	if rec, _ := e.loadRegister(t, attempted); rec == nil {
		t.Fatalf("the reserved identifier's register is its durable hold and must survive")
	}
	waitFor(t, "the proven-unsent register to be consumed", func() bool {
		_, smsg := e.loadRegister(t, unsent)
		return smsg == nil
	})
	// Retention is asserted by an allocation that would EXPOSE reuse.
	other := e.track(t, dur, sseqU+1000, 1)
	if other == attempted {
		t.Fatalf("the reserved identifier %d was re-minted for a new delivery", attempted)
	}
}
