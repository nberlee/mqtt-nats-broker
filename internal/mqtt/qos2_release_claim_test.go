package mqtt

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// Tests for the inbound QoS 2 release claim (qos2_release_claim.go): the register
// that serializes a session discard against a PUBREL release, and the post-claim
// re-read that keeps a stalled releaser from delivering an exchange somebody else
// already completed.

// testRelClaimEnv is one broker over a real embedded JetStream, with a session
// whose incarnation is minted the way production mints it.
type testRelClaimEnv struct {
	s    *Server
	asm  *mqttAccountSessionManager
	sess *mqttSession
}

func testNewRelClaimEnv(t *testing.T, clientID string) *testRelClaimEnv {
	t.Helper()
	_, nc := runNATS(t)

	s := &Server{
		nrc:      nc,
		opts:     &Options{},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	t.Cleanup(func() { close(s.quitCh) })

	c := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	sess, _, err := asm.createOrRestoreSession(clientID, s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if !mqttValidSessGen(sess.gen) {
		t.Fatalf("a freshly created session must carry a minted incarnation generation, got %q", sess.gen)
	}
	return &testRelClaimEnv{s: s, asm: asm, sess: sess}
}

// hold stores a real held record for one packet identifier and returns its subject
// and sequence, exactly as mqttStoreQoS2MsgOnce would.
func (e *testRelClaimEnv) hold(t *testing.T, pi uint16, gen string, payload string) (string, uint64) {
	t.Helper()
	subject := mqttQoS2HoldSubject(e.sess.idHash, e.sess.gen, pi)
	pp := &mqttPublish{
		topic:   []byte("foo/bar"),
		subject: []byte("foo.bar"),
		msg:     []byte(payload),
		sz:      len(payload),
		pi:      pi,
		flags:   2 << 1,
	}
	msg, hdrLen := mqttNewDeliverableMessage(pp, true, 0, _EMPTY_, false, _EMPTY_, gen)
	seq, err := e.asm.storeQoS2HoldCAS(subject, 0, hdrLen, msg)
	if err != nil {
		t.Fatalf("store hold record for pi %d: %v", pi, err)
	}
	return subject, seq
}

// putClaim writes a claim directly, for setting up a state a test needs to observe.
func (e *testRelClaimEnv) putClaim(t *testing.T, holdSubject string, c *mqttQoS2RelClaim) uint64 {
	t.Helper()
	seq, err := e.asm.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(holdSubject), 0, c)
	if err != nil {
		t.Fatalf("store claim on %q: %v", holdSubject, err)
	}
	return seq
}

func (e *testRelClaimEnv) loadClaim(t *testing.T, holdSubject string) (*mqttQoS2RelClaim, *StoredMsg) {
	t.Helper()
	c, smsg, err := e.asm.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil {
		t.Fatalf("load claim on %q: %v", holdSubject, err)
	}
	return c, smsg
}

func (e *testRelClaimEnv) holdPresent(t *testing.T, holdSubject string) bool {
	t.Helper()
	rec, _, err := e.asm.jsa.loadQoS2Record(holdSubject)
	if err != nil {
		t.Fatalf("load hold record on %q: %v", holdSubject, err)
	}
	return rec != nil
}

// TestQoS2RelClaimCreateOnlyCASIsExclusive pins the dependency contract the whole
// design rests on: on one subject of $MQTT_qos2in, two create-only CAS writes
// cannot both succeed, and the loser's error is recognizable as a CAS rejection.
//
// Nothing else in the tree asserts this, and it is a property of the pinned
// nats-server rather than of this repo: if a future version made a create-only CAS
// against an occupied subject succeed, the discard and the releaser could both
// believe they won and the serialization would be silently gone.
func TestQoS2RelClaimCreateOnlyCASIsExclusive(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "cas-exclusive")
	holdSubject, _ := e.hold(t, 1, nuid.Next(), "payload")
	claimSubject := mqttQoS2ClaimSubjectFor(holdSubject)

	first := mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node1", 0)
	seq, err := e.asm.jsa.storeQoS2RelClaimCAS(claimSubject, 0, first)
	if err != nil {
		t.Fatalf("first create-only claim: %v", err)
	}

	// Every subsequent create-only write must be rejected for as long as the record
	// exists, whoever writes it and whatever it says.
	for _, second := range []*mqttQoS2RelClaim{
		mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node2", 1),
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node2", 0),
	} {
		_, err := e.asm.jsa.storeQoS2RelClaimCAS(claimSubject, 0, second)
		if err == nil {
			t.Fatalf("a second create-only claim (%s) succeeded on an occupied subject", second.State)
		}
		if !isQoS2CASRejection(err) {
			t.Fatalf("a rejected create-only claim must be classifiable by isQoS2CASRejection, got %v", err)
		}
	}

	// The winner is still the record on the subject, unaltered.
	cur, smsg := e.loadClaim(t, holdSubject)
	if cur == nil || cur.State != mqttQoS2RelStateReleasing || cur.Gen != first.Gen {
		t.Fatalf("claim = %+v, want the first writer's record", cur)
	}
	if smsg.Sequence != seq {
		t.Fatalf("claim sequence = %d, want %d: a losing create must not move the record", smsg.Sequence, seq)
	}
}

// TestQoS2ClaimSubjectNaming pins that a claim subject is distinguishable from a
// hold subject by its trailing TOKEN, never by counting tokens.
//
// Token depth is the wrong test even though today's two shapes happen to differ by
// one: any future change to the subject layout — another scoping token, a different
// hash form — would silently reclassify every record, and misclassifying a hold as
// a claim means the discard skips it while misclassifying a claim as a hold means
// it purges the proof a late releaser must lose against. The suffix is a property of
// the name itself and survives all of that.
func TestQoS2ClaimSubjectNaming(t *testing.T) {
	t.Parallel()
	const idHash = "HASH"
	gen := nuid.Next()

	if got, want := mqttQoS2ClaimSubject(idHash, gen, 7), mqttQoS2HoldSubject(idHash, gen, 7)+".rel"; got != want {
		t.Fatalf("claim subject = %q, want %q", got, want)
	}
	if got := mqttQoS2ClaimSubjectFor(mqttQoS2HoldSubject(idHash, gen, 7)); got != mqttQoS2ClaimSubject(idHash, gen, 7) {
		t.Fatalf("mqttQoS2ClaimSubjectFor disagrees with mqttQoS2ClaimSubject: %q", got)
	}

	// Literal subjects at other depths, standing in for any layout the classifier
	// might one day meet — including a record left in a development store by an
	// earlier subject shape. Depth must not decide.
	shallowHold := mqttQoS2IncomingMsgsStreamSubjectPrefix + idHash + ".7"
	deepHold := mqttQoS2IncomingMsgsStreamSubjectPrefix + idHash + "." + gen + ".extra.7"
	for _, tc := range []struct {
		subject string
		isClaim bool
	}{
		{mqttQoS2ClaimSubject(idHash, gen, 7), true},
		{mqttQoS2ClaimSubjectFor(shallowHold), true},
		{mqttQoS2ClaimSubjectFor(deepHold), true},
		{mqttQoS2HoldSubject(idHash, gen, 7), false},
		{shallowHold, false},
		{deepHold, false},
	} {
		if got := mqttQoS2IsClaimSubject(tc.subject); got != tc.isClaim {
			t.Fatalf("mqttQoS2IsClaimSubject(%q) = %v, want %v", tc.subject, got, tc.isClaim)
		}
	}

	// No packet identifier can render as the claim token, so a hold subject can
	// never be mistaken for a claim. (16-bit, so this is the whole space.)
	for pi := 0; pi <= 0xFFFF; pi++ {
		if strconv.Itoa(pi) == mqttQoS2ClaimSubjectToken {
			t.Fatalf("packet identifier %d renders as the claim token %q", pi, mqttQoS2ClaimSubjectToken)
		}
	}

	// The walk filter must reach holds and only holds.
	filter := mqttQoS2HoldWalkFilter(idHash, gen)
	if !testSubjectMatchesFilter(mqttQoS2HoldSubject(idHash, gen, 7), filter) {
		t.Fatalf("the walk filter %q does not match its own generation's hold subject", filter)
	}
	if testSubjectMatchesFilter(mqttQoS2ClaimSubject(idHash, gen, 7), filter) {
		t.Fatalf("the walk filter %q matches a claim subject; claims must not be walked as holds", filter)
	}
}

// TestQoS2RelClaimIdentityValidation pins the identity rules. Each rejected shape
// is one a real fault could produce, and admitting any of them would let a claim
// be attributed to an exchange that does not own it.
func TestQoS2RelClaimIdentityValidation(t *testing.T) {
	t.Parallel()
	gen := nuid.Next()
	for _, tc := range []struct {
		name  string
		claim *mqttQoS2RelClaim
		ok    bool
	}{
		{"releasing with a valid generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing, Gen: gen}, true},
		{"discarded with a fence", &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded, Fence: 7}, true},
		{"releasing without a generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing}, false},
		{"releasing with a short generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing, Gen: "abc"}, false},
		{"releasing with an out-of-alphabet generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing, Gen: strings.Repeat("!", mqttQoS2GenLen)}, false},
		{"discarded with a generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded, Gen: gen, Fence: 7}, false},
		// The four Fence pairings. A fence-less discard marker is the unsupported
		// persisted shape F-062 refuses: read as a claim it would fence NOTHING, so every
		// later exchange would reap it and the marker would stop proving that a late
		// releaser must lose. A fenced `releasing` claim is the mirror fault — a releaser
		// destroys nothing by filter, so a bound it never uses could only be misread.
		{"discarded without a fence", &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded}, false},
		// A maximal fence is refused for a LIVENESS reason, not tidiness: read as valid it
		// would be adopted by arm A2 and then rejected by the purge, failing the clear on
		// every later adoption and sweep pass behind a marked record — a permanent client-ID
		// lockout. Refused, it takes the discard's existing unreadable-repair arm instead.
		{"discarded with a maximal fence", &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded, Fence: math.MaxUint64}, false},
		{"releasing with a fence", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing, Gen: gen, Fence: 7}, false},
		{"unknown state", &mqttQoS2RelClaim{State: "releasingish", Gen: gen}, false},
		{"empty state", &mqttQoS2RelClaim{Gen: gen}, false},
		{"nil", nil, false},
	} {
		if got := mqttQoS2RelClaimIdentityOK(tc.claim); got != tc.ok {
			t.Fatalf("%s: identity ok = %v, want %v", tc.name, got, tc.ok)
		}
	}

	// A malformed body is unreadable, not indeterminate: the caller needs the
	// record's sequence to reap it, so the load must return the StoredMsg.
	e := testNewRelClaimEnv(t, "identity")
	holdSubject, _ := e.hold(t, 1, gen, "payload")
	claimSubject := mqttQoS2ClaimSubjectFor(holdSubject)
	if _, err := e.asm.jsa.storeMsg(claimSubject, 0, []byte("{not json")); err != nil {
		t.Fatalf("store malformed claim: %v", err)
	}
	cur, smsg, err := e.asm.jsa.loadQoS2RelClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading a malformed claim must not be an error, got %v", err)
	}
	if cur != nil {
		t.Fatalf("a malformed claim must not decode to a usable claim, got %+v", cur)
	}
	if smsg == nil || smsg.Sequence == 0 {
		t.Fatal("a malformed claim must be returned with its stored message, so its sequence can be reaped")
	}
}

// TestQoS2RelClaimAtAndOrigin pins the two forensic fields.
//
// At is stamped by every writer and read by NOTHING (scope rule 5). It must never
// decide whether a claim is readable — the unreadable disposition drops a message,
// so a malformed diagnostic field must not be able to cause that — and it must never
// decide eligibility either. Nor does any other timestamp take its place: the orphan
// sweep requires a claim to be stably OBSERVED for its grace, keyed on the record's
// sequence, so no stored time participates at all. There is deliberately no range
// check anywhere, because the only predicate a range check could live in is the
// readability one it has to stay out of.
func TestQoS2RelClaimAtAndOrigin(t *testing.T) {
	t.Parallel()
	before := time.Now().Unix()
	for _, c := range []*mqttQoS2RelClaim{
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node1", 0),
		mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node1", 1),
	} {
		if c.At < before {
			t.Fatalf("%s claim carries At=%d, want a current timestamp", c.State, c.At)
		}
	}

	// Any At at all — absent, implausible, negative, far future — leaves the claim
	// perfectly readable. Nothing else consults it.
	for _, tc := range []struct {
		name string
		at   int64
	}{
		{"plausible", 1_800_000_000},
		{"absent", 0},
		{"implausibly old", 42},
		{"negative", -1},
		{"far future", 1 << 60},
	} {
		// Otherwise a valid marker: the fence IS identity (it decides a purge bound and a
		// releaser's verdict), so the shape under test has to carry one for At to be the
		// only thing varying.
		c := &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded, Fence: 7, At: tc.at}
		if !mqttQoS2RelClaimIdentityOK(c) {
			t.Fatalf("%s: a claim must stay readable whatever its At", tc.name)
		}
	}

	// Origin is sanitized and truncated, which is what makes the record's maximal
	// encoding exact. It cannot be derived from the local node name: names differ per
	// broker, so a bound taken from this one would not hold on the broker that
	// releases the exchange.
	for _, tc := range []struct{ name, in string }{
		{"long", strings.Repeat("n", 4096)},
		{"needs escaping", `a"b\c` + "\n\t" + strings.Repeat("x", 200)},
		{"unicode", strings.Repeat("héllo-wörld", 40)},
	} {
		got := mqttQoS2SanitizeOrigin(tc.in)
		if len(got) > mqttQoS2ClaimOriginMax {
			t.Fatalf("%s: sanitized origin is %d bytes, want at most %d", tc.name, len(got), mqttQoS2ClaimOriginMax)
		}
		b, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("%s: marshal: %v", tc.name, err)
		}
		// len+2 for the quotes: no byte may expand under JSON escaping, or the exact
		// bound below would understate the record.
		if len(b) != len(got)+2 {
			t.Fatalf("%s: sanitized origin %q expands under JSON escaping to %s", tc.name, got, b)
		}
	}
}

// TestQoS2ClaimRecordMaxBoundsEveryRealClaim pins the admission bound against the
// real encoder. Under-budgeting it is the worst failure available: the PUBLISH is
// accepted at PUBREC time and its PUBREL can then never write the claim it must
// hold before delivering, so the client retries forever with the hold never
// released.
func TestQoS2ClaimRecordMaxBoundsEveryRealClaim(t *testing.T) {
	t.Parallel()
	bound := mqttQoS2ClaimRecordMax()
	for _, tc := range []struct {
		name  string
		claim *mqttQoS2RelClaim
	}{
		{"releasing", mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), strings.Repeat("z", 4096), 0)},
		// The largest ADMISSIBLE fence — MaxUint64 itself is refused by the identity rule — which
		// still encodes to the full 20 digits, so the bound is exercised at its real maximum.
		{"discarded", mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, strings.Repeat("z", 4096), math.MaxUint64-1)},
		{"maximal at", &mqttQoS2RelClaim{
			State:  mqttQoS2RelStateReleasing,
			Gen:    nuid.Next(),
			Origin: strings.Repeat("z", mqttQoS2ClaimOriginMax),
			At:     1<<62 - 1,
		}},
	} {
		// Every real write is create-only, so the widest CAS header a real claim can
		// carry is a single digit; the bound deliberately budgets the full width.
		msg, _ := buildQoS2RelClaimRecord(0, tc.claim)
		if len(msg) > bound {
			t.Fatalf("%s: encoded claim is %d bytes, above the bound %d", tc.name, len(msg), bound)
		}
	}

	// And the preflight budgets it, for a payload small enough that the claim form
	// dominates every payload-scaled one.
	pp := &mqttPublish{topic: []byte("a"), subject: []byte("a"), msg: []byte("x"), sz: 1}
	delivered := mqttComputeNatsMsgSize(pp, false, 0, _EMPTY_, false, _EMPTY_, _EMPTY_)
	if got := mqttMaxQoS2FormSize(pp, delivered, false, _EMPTY_); got < bound {
		t.Fatalf("mqttMaxQoS2FormSize = %d, below the claim bound %d: an accepted PUBLISH could not write its claim", got, bound)
	}
}

// TestQoS2ClaimVerdicts pins the releaser's reconciliation of a rejected create.
// Every row is a state a real race produces, and the two that answer the client
// (Discarded, Unreadable) must never be confused with the two that proceed.
func TestQoS2ClaimVerdicts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// setup writes whatever already occupies the claim subject. It receives the
		// hold's sequence because a `discarded` marker's verdict depends on where its
		// fence sits relative to that sequence.
		setup    func(t *testing.T, e *testRelClaimEnv, holdSubject, gen string, holdSeq uint64)
		want     mqttQoS2ClaimStatus
		wantSeq  bool // a claim sequence must be returned
		wantMine bool // afterwards the claim must be ours
	}{
		{
			name:     "uncontested",
			setup:    func(*testing.T, *testRelClaimEnv, string, string, uint64) {},
			want:     mqttQoS2ClaimClaimed,
			wantSeq:  true,
			wantMine: true,
		},
		{
			name: "our own generation already claimed",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, gen string, _ uint64) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "peer", 0))
			},
			want:     mqttQoS2ClaimAlreadyClaimed,
			wantSeq:  true,
			wantMine: true,
		},
		{
			name: "a discard won",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, _ string, holdSeq uint64) {
				// Fenced AT our hold's sequence, which is the boundary case: a discard that
				// enumerated at that sequence was entitled to destroy this very record, so
				// the marker legitimately covers it and equality must not read as superseded.
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "peer", holdSeq))
			},
			want:    mqttQoS2ClaimDiscarded,
			wantSeq: true,
		},
		// A marker fenced BELOW the hold is reaped and the create retried; that needs a
		// hold above sequence 1 to stage, so it lives in
		// TestQoS2TeardownStaleDiscardedClaimDoesNotPoisonReuse, which stages both
		// orderings.
		{
			name: "a dead releaser's foreign generation is reaped and retried",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, _ string, _ uint64) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "dead", 0))
			},
			want:     mqttQoS2ClaimClaimed,
			wantSeq:  true,
			wantMine: true,
		},
		{
			name: "an unreadable record holds the subject",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, _ string, _ uint64) {
				if _, err := e.asm.jsa.storeMsg(mqttQoS2ClaimSubjectFor(holdSubject), 0, []byte("{oops")); err != nil {
					t.Fatalf("store malformed claim: %v", err)
				}
			},
			want:    mqttQoS2ClaimUnreadable,
			wantSeq: true,
		},
		{
			name: "a fence-less discard marker is unreadable, never a covering one",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, _ string, _ uint64) {
				// The unsupported persisted shape, refused rather than read as fencing
				// everything — which is what a zero fence would mean to the comparison.
				body, _ := json.Marshal(&mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded, Origin: "peer"})
				if _, err := e.asm.jsa.storeMsg(mqttQoS2ClaimSubjectFor(holdSubject), 0, body); err != nil {
					t.Fatalf("store fence-less discard marker: %v", err)
				}
			},
			want:    mqttQoS2ClaimUnreadable,
			wantSeq: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRelClaimEnv(t, "verdict")
			gen := nuid.Next()
			holdSubject, holdSeq := e.hold(t, 1, gen, "payload")
			tc.setup(t, e, holdSubject, gen, holdSeq)

			status, claimSeq, err := e.asm.claimQoS2Release(e.s, holdSubject, gen, holdSeq)
			if err != nil {
				t.Fatalf("claimQoS2Release: %v", err)
			}
			if status != tc.want {
				t.Fatalf("status = %v, want %v", status, tc.want)
			}
			if tc.wantSeq && claimSeq == 0 {
				t.Fatal("a claim sequence must be returned, or the reap cannot target the exact record")
			}
			if tc.wantMine {
				cur, smsg := e.loadClaim(t, holdSubject)
				if cur == nil || cur.State != mqttQoS2RelStateReleasing || cur.Gen != gen {
					t.Fatalf("claim = %+v, want our own generation %q", cur, gen)
				}
				if smsg.Sequence != claimSeq {
					t.Fatalf("returned claim sequence %d, but the record is at %d", claimSeq, smsg.Sequence)
				}
			}
		})
	}
}

// TestQoS2ClaimIndeterminateIsNonMutating pins that an unresolvable claim neither
// answers the client nor deletes anything: an omitted case must cost a retry, not a
// message.
func TestQoS2ClaimIndeterminateIsNonMutating(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "claim-indeterminate")
	gen := nuid.Next()
	holdSubject, holdSeq := e.hold(t, 1, gen, "payload")

	boom := fmt.Errorf("store unavailable")
	e.asm.mu.Lock()
	e.asm.testQoS2ClaimStore = func(string, uint64, *mqttQoS2RelClaim) (uint64, error) { return 0, boom }
	e.asm.mu.Unlock()

	status, claimSeq, err := e.asm.claimQoS2Release(e.s, holdSubject, gen, holdSeq)
	if status != mqttQoS2ClaimIndeterminate {
		t.Fatalf("status = %v, want indeterminate", status)
	}
	if err == nil {
		t.Fatal("an indeterminate claim must return a non-nil error, or the caller cannot report it")
	}
	if claimSeq != 0 {
		t.Fatalf("claim sequence = %d, want 0: no claim was observed", claimSeq)
	}
	if cur, smsg := e.loadClaim(t, holdSubject); cur != nil || smsg != nil {
		t.Fatalf("an indeterminate claim must leave the subject untouched, found %+v", cur)
	}
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("an indeterminate claim must not remove the hold record")
	}
}

// TestQoS2VerifyHoldForRelease pins fence 2, the authoritative re-read between the
// claim and the fan-out.
//
// The claim cannot catch a releaser that stalled while a peer completed the whole
// exchange and reaped both records: its create-only claim then succeeds against an
// empty subject. Only this re-read stops it delivering a second time. Every
// accepting row is generation-qualified, INCLUDING the tombstone: a delayed PUBREL
// that skipped that check would take a later exchange's tombstone on the same
// packet identifier as proof of its own completion and answer 0x00 for a message
// it never delivered.
func TestQoS2VerifyHoldForRelease(t *testing.T) {
	t.Parallel()
	ourGen := nuid.Next()
	for _, tc := range []struct {
		name string
		// mutate rewrites the hold subject after the record was stored. It returns the
		// sequence the releaser originally loaded.
		mutate func(t *testing.T, e *testRelClaimEnv, holdSubject string, holdSeq uint64) uint64
		want   mqttQoS2VerifyStatus
	}{
		{
			name:   "our record at our sequence",
			mutate: func(_ *testing.T, _ *testRelClaimEnv, _ string, seq uint64) uint64 { return seq },
			want:   mqttQoS2VerifyHeld,
		},
		{
			name: "our own tombstone",
			mutate: func(t *testing.T, e *testRelClaimEnv, holdSubject string, seq uint64) uint64 {
				if _, err := e.asm.jsa.storeQoS2DoneCAS(holdSubject, ourGen, seq); err != nil {
					t.Fatalf("commit tombstone: %v", err)
				}
				return seq
			},
			want: mqttQoS2VerifyDelivered,
		},
		{
			name: "absent",
			mutate: func(t *testing.T, e *testRelClaimEnv, holdSubject string, seq uint64) uint64 {
				if err := e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, seq+1, false); err != nil {
					t.Fatalf("purge hold: %v", err)
				}
				return seq
			},
			want: mqttQoS2VerifyGone,
		},
		{
			name: "a later exchange's tombstone",
			mutate: func(t *testing.T, e *testRelClaimEnv, holdSubject string, seq uint64) uint64 {
				if _, err := e.asm.jsa.storeQoS2DoneCAS(holdSubject, nuid.Next(), seq); err != nil {
					t.Fatalf("commit a foreign tombstone: %v", err)
				}
				return seq
			},
			want: mqttQoS2VerifySuperseded,
		},
		{
			name: "a later exchange's held record",
			mutate: func(t *testing.T, e *testRelClaimEnv, holdSubject string, seq uint64) uint64 {
				if err := e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, seq+1, false); err != nil {
					t.Fatalf("purge hold: %v", err)
				}
				e.hold(t, 1, nuid.Next(), "newer")
				return seq
			},
			want: mqttQoS2VerifySuperseded,
		},
		{
			name: "our generation at another sequence",
			mutate: func(t *testing.T, e *testRelClaimEnv, holdSubject string, seq uint64) uint64 {
				// The releaser loaded a stale sequence: a state we cannot prove is the one
				// we read, so nothing may be delivered against it.
				return seq - 1
			},
			want: mqttQoS2VerifySuperseded,
		},
		{
			name: "a corrupt record",
			mutate: func(t *testing.T, e *testRelClaimEnv, holdSubject string, seq uint64) uint64 {
				if err := e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, seq+1, false); err != nil {
					t.Fatalf("purge hold: %v", err)
				}
				if _, err := e.asm.jsa.storeMsg(holdSubject, 0, []byte("no generation")); err != nil {
					t.Fatalf("store corrupt record: %v", err)
				}
				return seq
			},
			want: mqttQoS2VerifySuperseded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRelClaimEnv(t, "verify")
			holdSubject, holdSeq := e.hold(t, 1, ourGen, "payload")
			loadedSeq := tc.mutate(t, e, holdSubject, holdSeq)

			status, _, err := e.asm.verifyQoS2HoldForRelease(holdSubject, ourGen, loadedSeq)
			if err != nil {
				t.Fatalf("verifyQoS2HoldForRelease: %v", err)
			}
			if status != tc.want {
				t.Fatalf("status = %v, want %v", status, tc.want)
			}

			// A superseded verdict must mutate NOTHING: the record it found may belong to
			// a newer exchange, which is the same reason finalizeQoS2Move abandons.
			if tc.want == mqttQoS2VerifySuperseded {
				if !e.holdPresent(t, holdSubject) {
					t.Fatal("a superseded verification deleted the record it found")
				}
			}
		})
	}

	// An indeterminate read is never guessed into an absence, because absence
	// authorizes completing the exchange.
	t.Run("indeterminate", func(t *testing.T) {
		e := testNewRelClaimEnv(t, "verify-indeterminate")
		holdSubject, holdSeq := e.hold(t, 1, ourGen, "payload")
		e.asm.mu.Lock()
		e.asm.testQoS2Load = func(string) (*mqttQoS2Record, uint64, error) {
			return nil, 0, fmt.Errorf("stream unavailable")
		}
		e.asm.mu.Unlock()
		status, _, err := e.asm.verifyQoS2HoldForRelease(holdSubject, ourGen, holdSeq)
		if status != mqttQoS2VerifyIndeterminate || err == nil {
			t.Fatalf("status = %v, err = %v, want indeterminate with an error", status, err)
		}
	})
}

// TestQoS2ReleaseStatusStringsAreDistinct pins that every declared status renders
// distinctly, so the warning an operator reads to diagnose one of these races
// cannot name the wrong state — the failure mode qos2_move.go's own String()
// default would have had if a state were added to it.
func TestQoS2ReleaseStatusStringsAreDistinct(t *testing.T) {
	t.Parallel()
	claims := []mqttQoS2ClaimStatus{
		mqttQoS2ClaimClaimed, mqttQoS2ClaimAlreadyClaimed, mqttQoS2ClaimDiscarded,
		mqttQoS2ClaimUnreadable, mqttQoS2ClaimIndeterminate,
	}
	seen := map[string]bool{}
	for _, st := range claims {
		s := st.String()
		if seen[s] {
			t.Fatalf("claim status %d renders as %q, already used", st, s)
		}
		seen[s] = true
	}
	verifies := []mqttQoS2VerifyStatus{
		mqttQoS2VerifyHeld, mqttQoS2VerifyDelivered, mqttQoS2VerifyGone,
		mqttQoS2VerifySuperseded, mqttQoS2VerifyIndeterminate,
	}
	seen = map[string]bool{}
	for _, st := range verifies {
		s := st.String()
		if seen[s] {
			t.Fatalf("verify status %d renders as %q, already used", st, s)
		}
		seen[s] = true
	}
}

// TestQoS2DiscardArbitration pins the discard half: it competes for each release
// claim and destroys only what it won.
func TestQoS2DiscardArbitration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		setup     func(t *testing.T, e *testRelClaimEnv, holdSubject string, holdSeq uint64)
		wantHold  bool // the hold record must survive
		wantErr   bool
		wantClaim string // the claim state that must be on the subject afterwards ("" = any)
	}{
		{
			name:      "uncontested: the hold is destroyed",
			setup:     func(*testing.T, *testRelClaimEnv, string, uint64) {},
			wantClaim: mqttQoS2RelStateDiscarded,
		},
		{
			name: "a peer discard already claimed: still destroyed",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject string, holdSeq uint64) {
				// Fenced at this hold's sequence: the adopting walk purges to the MARKER's
				// fence, so a marker that covers this record is what makes "still destroyed"
				// the right outcome. One fenced below it is a different case, and
				// TestQoS2TeardownPurgeSparesAPostEnumerationHold owns it.
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "peer", holdSeq))
			},
			wantClaim: mqttQoS2RelStateDiscarded,
		},
		{
			name: "a releaser won: the hold is left alone",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject string, _ uint64) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer", 0))
			},
			wantHold:  true,
			wantClaim: mqttQoS2RelStateReleasing,
		},
		{
			name: "an unreadable claim is reaped and the discard competes again",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject string, _ uint64) {
				if _, err := e.asm.jsa.storeMsg(mqttQoS2ClaimSubjectFor(holdSubject), 0, []byte("{oops")); err != nil {
					t.Fatalf("store malformed claim: %v", err)
				}
			},
			wantClaim: mqttQoS2RelStateDiscarded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRelClaimEnv(t, "arbitrate")
			holdSubject, holdSeq := e.hold(t, 1, nuid.Next(), "payload")
			tc.setup(t, e, holdSubject, holdSeq)

			err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen)
			if tc.wantErr != (err != nil) {
				t.Fatalf("arbitrateQoS2ReleaseClaims error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := e.holdPresent(t, holdSubject); got != tc.wantHold {
				t.Fatalf("hold record present = %v, want %v", got, tc.wantHold)
			}
			if tc.wantClaim != _EMPTY_ {
				cur, _ := e.loadClaim(t, holdSubject)
				if cur == nil || cur.State != tc.wantClaim {
					t.Fatalf("claim = %+v, want state %q", cur, tc.wantClaim)
				}
			}
		})
	}

	// An indeterminate enumeration fails the clear rather than reporting a session
	// as cleaned up while a releaser can still deliver from it.
	t.Run("indeterminate enumeration fails closed", func(t *testing.T) {
		e := testNewRelClaimEnv(t, "arbitrate-fail")
		e.hold(t, 1, nuid.Next(), "payload")
		e.asm.mu.Lock()
		e.asm.testQoS2ClaimStore = func(string, uint64, *mqttQoS2RelClaim) (uint64, error) {
			return 0, fmt.Errorf("stream unavailable")
		}
		e.asm.mu.Unlock()
		if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err == nil {
			t.Fatal("an unresolvable claim must fail the discard, not be reported as cleaned up")
		}
	})
}

// TestQoS2DiscardClaimsBeforePurging pins the ordering the releaser's "absent means
// terminated" reading depends on. If the purge came first, a releaser holding the
// claim could read the hold record's absence as proof the exchange completed and
// answer 0x00 for a message the discard destroyed.
func TestQoS2DiscardClaimsBeforePurging(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "order")
	holdSubject, _ := e.hold(t, 1, nuid.Next(), "payload")

	var mu sync.Mutex
	var order []string
	e.asm.mu.Lock()
	e.asm.testQoS2ClaimStore = func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
		mu.Lock()
		if !e.holdPresent(t, holdSubject) {
			order = append(order, "claim-after-purge")
		} else {
			order = append(order, "claim")
		}
		mu.Unlock()
		return e.asm.jsa.storeQoS2RelClaimCAS(subject, expectSeq, c)
	}
	e.asm.mu.Unlock()

	if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
		t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) == 0 {
		t.Fatal("the discard wrote no claim at all")
	}
	for _, o := range order {
		if o != "claim" {
			t.Fatalf("the discard purged the hold before claiming it (%v)", order)
		}
	}
}

// TestQoS2DiscardReadsNoPayloads pins that a discard enumerates and decides without
// loading a single hold record. A session with several large inbound exchanges in
// flight would otherwise drag every payload across the JS API on an ordinary
// disconnect — which is the whole reason the claim lives on a sibling subject
// rather than inside the hold record.
func TestQoS2DiscardReadsNoPayloads(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "no-payloads")
	const n = 5
	for pi := uint16(1); pi <= n; pi++ {
		e.hold(t, pi, nuid.Next(), strings.Repeat("x", 4096))
	}

	var loads atomic.Int32 // the walk arbitrates its holds concurrently
	e.asm.mu.Lock()
	e.asm.testQoS2Load = func(subject string) (*mqttQoS2Record, uint64, error) {
		loads.Add(1)
		return e.asm.jsa.loadQoS2Record(subject)
	}
	e.asm.mu.Unlock()

	if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
		t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
	}
	if n := loads.Load(); n != 0 {
		t.Fatalf("the discard loaded %d hold record(s); it must decide from the claim register alone", n)
	}
	for pi := uint16(1); pi <= n; pi++ {
		subject := mqttQoS2HoldSubject(e.sess.idHash, e.sess.gen, pi)
		if e.holdPresent(t, subject) {
			t.Fatalf("hold for pi %d survived an uncontested discard", pi)
		}
	}
}

// TestQoS2ClaimReapsTargetExactSequence pins scope rule 2. A reaper holding a stale
// sequence must not be able to remove a LATER exchange's claim on a reused packet
// identifier: that would re-expose the later exchange to the stalled-loader
// fan-out this whole register exists to stop.
func TestQoS2ClaimReapsTargetExactSequence(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "reap-sequence")
	holdSubject, _ := e.hold(t, 1, nuid.Next(), "payload")

	// Exchange 1 claims, completes, and its claim is removed.
	staleSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node1", 0))
	if err := e.asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, staleSeq, true); err != nil {
		t.Fatalf("remove the first claim: %v", err)
	}

	// Exchange 2 on the same identifier claims.
	freshGen := nuid.Next()
	freshSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, freshGen, "node2", 0))
	if freshSeq == staleSeq {
		t.Fatal("the two claims must occupy different sequences for this test to mean anything")
	}

	// A reaper from exchange 1 finally runs, with the only sequence it ever knew.
	e.asm.reapQoS2RelClaim(staleSeq)

	cur, smsg := e.loadClaim(t, holdSubject)
	if cur == nil {
		t.Fatal("a delayed reaper removed a later exchange's claim; reaps must target the exact sequence")
	}
	if cur.Gen != freshGen || smsg.Sequence != freshSeq {
		t.Fatalf("claim = %+v at seq %d, want generation %q at seq %d", cur, smsg.Sequence, freshGen, freshSeq)
	}
}

// TestQoS2DiscardLeavesClaimsForTheSweep pins scope rule 3: a `discarded` claim is
// the only durable proof that a late releaser must lose, so the discard must never
// reap it — including through the unarbitrated filter purges, which must not name
// this incarnation's own generation.
func TestQoS2DiscardLeavesClaimsForTheSweep(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "sweep")
	holdSubject, _ := e.hold(t, 1, nuid.Next(), "payload")

	if err := e.sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}
	if e.holdPresent(t, holdSubject) {
		t.Fatal("an uncontested discard must destroy the hold record")
	}
	cur, _ := e.loadClaim(t, holdSubject)
	if cur == nil || cur.State != mqttQoS2RelStateDiscarded {
		t.Fatalf("claim = %+v, want the discard marker to survive for a late releaser to lose against", cur)
	}
}

// TestQoS2ClearPreservesAClaimedHold pins that a discard which loses the race
// leaves the payload alone. Destroying it would turn this spec deviation into
// silent loss: it is what a crashed releaser's retry delivers.
func TestQoS2ClearPreservesAClaimedHold(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "claimed-hold")
	holdSubject, _ := e.hold(t, 1, nuid.Next(), "payload")
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer", 0))

	start := time.Now()
	if err := e.sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}
	// The wait is bounded for the WHOLE walk, so an unfinished release delays a
	// teardown by a fixed budget rather than per identifier.
	if elapsed := time.Since(start); elapsed > 5*mqttQoS2DiscardWaitBudget {
		t.Fatalf("clear() waited %v on a claimed hold, far beyond the %v budget", elapsed, mqttQoS2DiscardWaitBudget)
	}
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("the discard destroyed a hold record whose release had already claimed it")
	}
}

// TestQoS2ClearWaitsOutACompletingRelease pins the other half: when the releasing broker
// finishes inside the budget, the discard sees the claim vanish, COMPETES FOR IT AGAIN, and
// completes the teardown under a marker of its own.
//
// It does not read the vacancy as its own entitlement — that inference is what F-062's A3
// race disproved — so the outcome is the hold gone and a fenced marker standing, exactly as
// on any other winning discard.
func TestQoS2ClearWaitsOutACompletingRelease(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "wait-out")
	// ONE generation for the hold and its claim. A releasing claim carrying a different
	// generation is not this exchange's releaser at all — it is the stale-foreign-claim shape
	// that TestQoS2AwaitedReleaseDoesNotInheritAStaleClaimsEntitlement stages deliberately —
	// so staging it here would name a completion that could not physically occur.
	gen := nuid.Next()
	holdSubject, holdSeq := e.hold(t, 1, gen, "payload")
	claimSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "peer", 0))

	// The releaser completes shortly after the discard starts waiting: it commits (here,
	// simply removes the hold) and then removes its claim, which is the order the real path
	// uses. Note what the resulting vacancy does NOT prove: claim absence is not evidence
	// that the exchange terminated, because a releaser also deletes a FOREIGN generation's
	// claim before retrying its own create. The waiter therefore treats the vacancy only as
	// permission to compete again, and the discard it then wins leaves its own fenced marker.
	go func() {
		time.Sleep(mqttQoS2DiscardWaitInterval / 2)
		_ = e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, holdSeq+1, false)
		_ = e.asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, claimSeq, true)
	}()

	if err := e.sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the hold survived a release that completed inside the discard's wait")
	}
	// This used to assert that NO claim was left behind. That expectation encoded the
	// defect F-062 round 9 found: it was only true because an awaited release purged
	// directly, treating an empty claim subject as its own entitlement — which is unsound,
	// since the claim waited on may belong to an earlier exchange and be reaped by the
	// current one's releaser. The walk now re-competes and destroys under a marker it won,
	// so a marker stands exactly as it does on every other winning discard. Asserting its
	// presence is strictly stronger than asserting its absence: it pins that the arbitration
	// was actually won rather than assumed.
	cur, _ := e.loadClaim(t, holdSubject)
	if cur == nil || cur.State != mqttQoS2RelStateDiscarded || !mqttQoS2UsableCutoff(cur.Fence) {
		t.Fatalf("claim = %+v, want a readable discard marker carrying a usable fence", cur)
	}
}

// TestQoS2ReleaseFenceRefusesDuringDiscard pins the in-process fence. Across
// brokers the durable claim is the only arbiter, but on THIS broker a discard can
// refuse to admit a release it would otherwise have to race — and it must reopen
// the fence afterwards, because two callers reuse the session object as a fresh
// session.
func TestQoS2ReleaseFenceRefusesDuringDiscard(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "fence")

	if !e.sess.beginQoS2ReleaseOp() {
		t.Fatal("a live session must admit a release")
	}
	e.sess.endQoS2ReleaseOp()

	// clear() drains and then reopens, so a reused session works again.
	if err := e.sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}
	if !e.sess.beginQoS2ReleaseOp() {
		t.Fatal("clear() left the release fence closed; every later exchange on a reused session would answer 0x92")
	}
	e.sess.endQoS2ReleaseOp()

	// While the fence is closed nothing is admitted, and a close waits for what it
	// already admitted.
	if !e.sess.beginQoS2ReleaseOp() {
		t.Fatal("a live session must admit a release")
	}
	drained := make(chan struct{})
	go func() {
		e.sess.closeQoS2ReleaseAndDrain()
		close(drained)
	}()
	select {
	case <-drained:
		t.Fatal("closeQoS2ReleaseAndDrain returned while a release was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	e.sess.endQoS2ReleaseOp()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("closeQoS2ReleaseAndDrain did not return after the in-flight release finished")
	}
	if e.sess.beginQoS2ReleaseOp() {
		t.Fatal("a closed fence admitted a release")
	}
	e.sess.reopenQoS2ReleaseFence()
}

// TestQoS2GenerationlessSessionIsRejected pins the reason the legacy hash-only
// path was removed rather than left partially arbitrated.
//
// Such a session has no identity to scope an arbitration with: its hold subjects
// carry no generation, so the discard could not enumerate or claim them and would
// have to fall back to a blind hash-wide purge — which destroys the held payload of
// a release that already claimed it, and whose retry after a crashed broker is the
// only thing that would ever have delivered it. That is silent loss, and strictly
// worse than the ordering deviation the claim exists to fix, so the record is
// refused at the boundary instead.
//
// Mutation: let mqttSessGenFromRecord admit an empty generation again and this
// fails — as does every name and filter downstream, which would then be built from
// nothing.
func TestQoS2GenerationlessSessionIsRejected(t *testing.T) {
	t.Parallel()
	if _, err := mqttSessGenFromRecord(_EMPTY_); err == nil {
		t.Fatal("a record with no incarnation generation must be rejected, not resumed on hash-only names")
	}

	e := testNewRelClaimEnv(t, "generationless")

	// No live session can reach the release path without a generation, which is what
	// makes the claim arbitrable for every session that exists.
	if !mqttValidSessGen(e.sess.gen) {
		t.Fatalf("a live session must carry a well-formed generation, got %q", e.sess.gen)
	}
	if err := e.sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}
	if !mqttValidSessGen(e.sess.gen) {
		t.Fatalf("clear() must re-mint a well-formed generation, got %q", e.sess.gen)
	}
}

// TestQoS2DiscardRunsNoBlindPurge pins that the per-subject arbitration is the ONLY
// thing a discard does to $MQTT_qos2in.
//
// A filter purge running alongside it would be indistinguishable in the ordinary
// case and catastrophic in the interesting one: it would destroy the very records
// the arbitration decided to preserve — a held payload whose releaser claimed it
// and may still be about to deliver it after a crash, and the claim records
// themselves, including the `discarded` markers that are the only durable proof a
// late releaser must lose.
//
// Two holds under one incarnation, one claimed and one not, is what makes this
// detectable: any wildcard purge over this generation would take both.
func TestQoS2DiscardRunsNoBlindPurge(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "no-blind-purge")
	claimed, _ := e.hold(t, 1, nuid.Next(), "claimed-payload")
	unclaimed, _ := e.hold(t, 2, nuid.Next(), "unclaimed-payload")
	e.putClaim(t, claimed, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer", 0))

	if err := e.sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}

	if !e.holdPresent(t, claimed) {
		t.Fatal("a blind purge destroyed the hold record of a release that had already claimed it")
	}
	if e.holdPresent(t, unclaimed) {
		t.Fatal("the arbitration failed to destroy an unclaimed hold record")
	}
	// Both claims survive: the peer's `releasing` (its exchange is still live) and
	// the discard's own marker on the unclaimed one.
	if cur, _ := e.loadClaim(t, claimed); cur == nil || cur.State != mqttQoS2RelStateReleasing {
		t.Fatalf("claim on the contested hold = %+v, want the releaser's", cur)
	}
	if cur, _ := e.loadClaim(t, unclaimed); cur == nil || cur.State != mqttQoS2RelStateDiscarded {
		t.Fatalf("claim on the destroyed hold = %+v, want the discard marker to survive", cur)
	}
}

// ---------------------------------------------------------------------------
// F-062: the hold purge is bounded, and the marker it leaves carries that bound
// ---------------------------------------------------------------------------

// vacate removes a hold record at its exact sequence, the way a completed exchange's
// terminal cleanup does. A fixture needs it because the hold store is create-only: a
// later exchange can land on the subject only once it is empty.
func (e *testRelClaimEnv) vacate(t *testing.T, seq uint64) {
	t.Helper()
	if st, err := e.asm.releaseQoS2Record(seq); st != mqttQoS2Released {
		t.Fatalf("vacating the hold at %d: status=%v err=%v", seq, st, err)
	}
}

// persist writes the session's record, which a clear()-driven fixture needs: the
// terminal-cleanup consequence it asserts is about the RECORD, and there is nothing to
// assert about one that was never written.
func (e *testRelClaimEnv) persist(t *testing.T) {
	t.Helper()
	tok := e.sess.beginRecordTx()
	err := e.sess.save(tok)
	if terr := e.sess.endRecordTx(tok); terr != nil && err == nil {
		err = terr
	}
	if err != nil {
		t.Fatalf("persisting the session record: %v", err)
	}
}

// holdSeqOf returns the sequence of whatever hold record occupies subject, or 0.
func (e *testRelClaimEnv) holdSeqOf(t *testing.T, holdSubject string) uint64 {
	t.Helper()
	_, seq, err := e.asm.jsa.loadQoS2Record(holdSubject)
	if err != nil {
		t.Fatalf("load the hold on %q: %v", holdSubject, err)
	}
	return seq
}

// releaseDelivers drives the two fences a real PUBREL drives and requires both to
// authorize the delivery. Asserting the hold record merely survived is not enough:
// what F-062 protects is the EXCHANGE, and an exchange whose claim is refused or whose
// re-read comes back Superseded delivers nothing however intact its record is.
func (e *testRelClaimEnv) releaseDelivers(t *testing.T, holdSubject, gen string, holdSeq uint64) {
	t.Helper()
	status, _, err := e.asm.claimQoS2Release(e.s, holdSubject, gen, holdSeq)
	if err != nil || status != mqttQoS2ClaimClaimed {
		t.Fatalf("claimQoS2Release = %v (err %v), want Claimed", status, err)
	}
	st, _, verr := e.asm.verifyQoS2HoldForRelease(holdSubject, gen, holdSeq)
	if verr != nil || st != mqttQoS2VerifyHeld {
		t.Fatalf("verifyQoS2HoldForRelease = %v (err %v), want Held", st, verr)
	}
}

// padStreamTail advances $MQTT_qos2in's last sequence with records on subjects OUTSIDE
// this session's walk filter, so they lift LastSeq without entering any walk's subject map.
// That is how a fixture gives a CONCURRENT teardown a legitimately higher watermark than
// ours: a fence has to be a sequence its writer could actually have enumerated, and in one
// stream incarnation a writer always enumerates BEFORE it writes its marker — so a fence
// above its own marker's sequence is not a concurrent teardown at all, it is the corrupt
// readable fence residual wearing its clothes.
func (e *testRelClaimEnv) padStreamTail(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		subject := mqttQoS2HoldSubject("PADHASH0", nuid.Next(), uint16(i+1))
		if _, err := e.asm.jsa.storeMsg(subject, 0, []byte("pad")); err != nil {
			t.Fatalf("pad the stream tail: %v", err)
		}
	}
}

// streamLastSeq reads the stream-wide last sequence, which is what an enumeration would
// have captured as its watermark at this moment.
func (e *testRelClaimEnv) streamLastSeq(t *testing.T) uint64 {
	t.Helper()
	_, last, err := e.asm.jsa.streamSubjects(mqttQoS2IncomingMsgsStreamName,
		mqttQoS2HoldWalkFilter(e.sess.idHash, e.sess.gen))
	if err != nil {
		t.Fatalf("read the stream tail: %v", err)
	}
	return last
}

// setSubjectsSeam installs an enumeration seam returning the real subject map with a
// caller-chosen last sequence, which is how a fixture stages a watermark the real
// stream would not hand it.
func (e *testRelClaimEnv) setSubjectsSeam(t *testing.T, seq func(realLast uint64) uint64) {
	t.Helper()
	e.asm.mu.Lock()
	defer e.asm.mu.Unlock()
	e.asm.testQoS2Subjects = func(filter string) (map[string]uint64, uint64, error) {
		subjects, realLast, err := e.asm.jsa.streamSubjects(mqttQoS2IncomingMsgsStreamName, filter)
		return subjects, seq(realLast), err
	}
}

// TestQoS2TeardownPurgeSparesAPostEnumerationHold is F-062 mechanism 1, asserted at
// the shared boundary: a teardown's purge never removes a hold record the teardown was
// not entitled to destroy.
//
// The hazard is silent loss of an ACKNOWLEDGED message. Broker B accepts a PUBLISH on
// the retiring incarnation and answers PUBREC — the client has been told the broker is
// holding it — after broker A's teardown enumerated and before its purge lands. A
// resume preserves the incarnation generation, so "a peer still serving this
// generation" is reachable and the generation scoping never bounded this.
//
// Five subtests over three arms: A1 (a fresh marker, twice — the second with a stale
// watermark), A3 (the awaited release, which purges nothing of its own and re-competes into
// A1), and both A2 variants (an adopted marker). Arm A4 is the orphan sweep's and is covered
// by the mutations against orphan_qos2_sweep_test.go.
func TestQoS2TeardownPurgeSparesAPostEnumerationHold(t *testing.T) {
	t.Parallel()

	// A1: the discard wins a fresh marker. The seam fires between the enumeration and
	// the purge, which is exactly the window a peer's PUBREC lands in.
	t.Run("A1 fresh discard marker", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "spare-a1")
		holdSubject, firstSeq := e.hold(t, 1, nuid.Next(), "first-payload")

		lateGen := nuid.Next()
		var lateSeq uint64
		// The walk arbitrates holds concurrently, so seam state is guarded even though this
		// fixture has one hold; reads after the walk are ordered by its join.
		var seamMu sync.Mutex
		e.asm.mu.Lock()
		e.asm.testQoS2ClaimStore = func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
			seamMu.Lock()
			defer seamMu.Unlock()
			if lateSeq == 0 {
				// E1 completes and vacates, then a peer accepts E2 and answers PUBREC. The
				// vacate is required: the hold store is create-only.
				e.vacate(t, firstSeq)
				_, lateSeq = e.hold(t, 1, lateGen, "acknowledged-payload")
			}
			return e.asm.jsa.storeQoS2RelClaimCAS(subject, expectSeq, c)
		}
		e.asm.mu.Unlock()

		if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
			t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
		}
		if lateSeq == 0 {
			t.Fatal("the seam never fired, so this fixture staged no race")
		}
		if got := e.holdSeqOf(t, holdSubject); got != lateSeq {
			t.Fatalf("the acknowledged hold is at %d, want it intact at %d", got, lateSeq)
		}
		// And the marker's fence is what licensed sparing it.
		cur, _ := e.loadClaim(t, holdSubject)
		if cur == nil || cur.State != mqttQoS2RelStateDiscarded || cur.Fence >= lateSeq {
			t.Fatalf("marker = %+v, want a discard fenced below the late hold at %d", cur, lateSeq)
		}
		e.releaseDelivers(t, holdSubject, lateGen, lateSeq)
	})

	// A3: the discard lost the claim to a releaser and is waiting it out. The vacancy that
	// follows licenses NO purge — that inference is the race F-062 round 9 disproved — so the
	// walk re-competes, wins a marker of its own, and the hold accepted after enumeration is
	// spared by that marker's cutoff. What this subtest measures is that chain.
	t.Run("A3 awaited release completes", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "spare-a3")
		// One generation for E1's hold and E1's claim: the releaser this walk waits out must be
		// the releaser OF the exchange it enumerated, or the history is not a completion.
		firstGen := nuid.Next()
		holdSubject, firstSeq := e.hold(t, 1, firstGen, "first-payload")
		claimSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, firstGen, "peer", 0))

		lateGen := nuid.Next()
		var lateSeq uint64
		var loads int
		var seamMu sync.Mutex // see A1
		e.asm.mu.Lock()
		e.asm.testQoS2ClaimLoad = func(subject string) (*mqttQoS2RelClaim, *StoredMsg, error) {
			seamMu.Lock()
			defer seamMu.Unlock()
			// The first load is discardQoS2Hold's, which must still see the `releasing`
			// claim so the walk dispatches into awaitQoS2Release. The second is the poll,
			// and by then the releaser has completed: claim gone, E1 vacated, E2 accepted.
			loads++
			if loads == 2 && lateSeq == 0 {
				e.vacate(t, claimSeq)
				e.vacate(t, firstSeq)
				_, lateSeq = e.hold(t, 1, lateGen, "acknowledged-payload")
			}
			return e.asm.jsa.loadQoS2RelClaim(subject)
		}
		e.asm.mu.Unlock()

		if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
			t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
		}
		if lateSeq == 0 {
			t.Fatal("the poll seam never fired, so this fixture staged no race")
		}
		if got := e.holdSeqOf(t, holdSubject); got != lateSeq {
			t.Fatalf("the acknowledged hold is at %d, want it intact at %d", got, lateSeq)
		}
		// A marker DOES stand now, and its presence is the fix rather than a leak: an awaited
		// release no longer licenses a purge, so the walk re-competed and destroys only under
		// its own freshly won marker. That marker must be fenced below the late hold, which is
		// what lets the late exchange prove it is not covered.
		cur, _ := e.loadClaim(t, holdSubject)
		if cur == nil || cur.State != mqttQoS2RelStateDiscarded {
			t.Fatalf("claim = %+v, want the marker the re-competing walk won", cur)
		}
		if cur.Fence >= lateSeq {
			t.Fatalf("marker fence %d covers the late hold at %d", cur.Fence, lateSeq)
		}
		e.releaseDelivers(t, holdSubject, lateGen, lateSeq)
	})

	// A2, variant 1: a marker left by an EARLIER partial teardown, fenced below a hold
	// accepted since. The current walk's watermark is above that hold, so a
	// cutoff-only fix destroys it here — this subtest is what keeps that fix red.
	t.Run("A2 marker from an earlier teardown", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "spare-a2-earlier")
		holdSubject, firstSeq := e.hold(t, 1, nuid.Next(), "first-payload")

		// The earlier walk enumerated at E1's sequence and left its marker without
		// purging, then died.
		e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "earlier-teardown", firstSeq))
		// E1 completes and vacates; a peer accepts E2 and answers PUBREC.
		e.vacate(t, firstSeq)
		lateGen := nuid.Next()
		_, lateSeq := e.hold(t, 1, lateGen, "acknowledged-payload")
		if lateSeq <= firstSeq {
			t.Fatalf("this fixture needs the later hold above the marker's fence: %d <= %d", lateSeq, firstSeq)
		}

		if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
			t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
		}
		if got := e.holdSeqOf(t, holdSubject); got != lateSeq {
			t.Fatalf("the acknowledged hold is at %d, want it intact at %d: the adopting walk "+
				"purged to its OWN watermark instead of the marker's fence", got, lateSeq)
		}
		e.releaseDelivers(t, holdSubject, lateGen, lateSeq)
	})

	// A1 with a STALE watermark: the non-snapshot gap's own direction, measured rather than
	// only asserted in the docs. STREAM.INFO builds State before it fills State.Subjects from
	// a separate store read, so a subject can appear in the map while its record sits ABOVE
	// the returned last sequence. The marker's CAS is then won DURING that hold's occupancy
	// and the hold is SPARED anyway — which is why winning the CAS is not on its own what makes
	// an occupancy discardable. What does is stated per arm in F-062; this fixture measures the
	// consequence rather than restating the rule. Direction is a leak, and the exchange stays
	// serviceable, so it belongs to F-036 rather than to any loss claim.
	t.Run("A1 spares a hold above a stale watermark", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "spare-a1-stale")
		holdSubject, firstSeq := e.hold(t, 1, nuid.Next(), "first-payload")
		e.vacate(t, firstSeq)
		aboveGen := nuid.Next()
		_, aboveSeq := e.hold(t, 1, aboveGen, "above-cutoff-payload")
		if aboveSeq <= firstSeq {
			t.Fatalf("this fixture needs the second hold above the stale watermark: %d <= %d", aboveSeq, firstSeq)
		}
		// The walk's State sample predates that hold; its Subjects sample includes it.
		e.setSubjectsSeam(t, func(uint64) uint64 { return firstSeq })

		if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
			t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
		}
		if got := e.holdSeqOf(t, holdSubject); got != aboveSeq {
			t.Fatalf("the hold above the cutoff is at %d, want it spared at %d", got, aboveSeq)
		}
		// A marker WAS won, during that hold's occupancy, and it is fenced below the hold.
		cur, _ := e.loadClaim(t, holdSubject)
		if cur == nil || cur.State != mqttQoS2RelStateDiscarded || cur.Fence >= aboveSeq {
			t.Fatalf("marker = %+v, want a discard fenced below the spared hold at %d", cur, aboveSeq)
		}
		// And the spared exchange is still serviceable: its PUBREL reaps that marker, because
		// its hold sits above the fence, and delivers.
		e.releaseDelivers(t, holdSubject, aboveGen, aboveSeq)
	})

	// A2, variant 2: a CONCURRENT teardown's marker, fenced ABOVE both the hold and
	// this walk's own watermark. Here the hold MUST be destroyed — the marker's writer
	// enumerated above it and won the CAS that licensed acting on it. The assertion is
	// inverted on purpose: it is what stops min(Fence, upTo) being reintroduced as the
	// safer-looking rule.
	t.Run("A2 concurrent teardown fenced higher", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "spare-a2-concurrent")
		holdSubject, firstSeq := e.hold(t, 1, nuid.Next(), "first-payload")
		e.vacate(t, firstSeq)
		coveredGen := nuid.Next()
		_, coveredSeq := e.hold(t, 1, coveredGen, "covered-payload")

		// T2's watermark has to be one a real enumeration could have taken, so advance the
		// tail with records outside the walk filter and read the tail T2 would have seen.
		e.padStreamTail(t, 4)
		t2Fence := e.streamLastSeq(t)
		markerSeq := e.putClaim(t, holdSubject,
			mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "concurrent-teardown", t2Fence))

		// The staging asserts its own LEGALITY before the walk runs, because an illegal
		// history here would silently turn this into the corrupt-fence residual's test and
		// it would still kill the min(Fence, upTo) mutation while proving nothing about the
		// concurrent-teardown arm. T1 enumerated below the hold; T2 enumerated at or above
		// it; and T2's fence precedes its own marker's sequence, which is what makes the
		// fence a value T2 could have held when it decided.
		if !(firstSeq < coveredSeq && coveredSeq <= t2Fence && t2Fence < markerSeq) {
			t.Fatalf("illegal single-incarnation staging: upTo=%d covered=%d fence=%d marker=%d",
				firstSeq, coveredSeq, t2Fence, markerSeq)
		}
		// T1 enumerated earlier, below the hold, and now adopts T2's marker.
		e.setSubjectsSeam(t, func(uint64) uint64 { return firstSeq })

		if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
			t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
		}
		if got := e.holdSeqOf(t, holdSubject); got != 0 {
			t.Fatalf("the hold at %d survived, but the adopted marker's fence covers it: "+
				"the adopting walk clamped to its own watermark", got)
		}
	})
}

// TestQoS2DiscardZeroWatermarkWritesNoMarker pins that an enumeration which cannot
// bound a purge is never acted on.
//
// STREAM.INFO builds State before it fills State.Subjects from a separate store read,
// so a walk that started against an empty stream can legitimately be handed a
// non-empty subject map with a zero last sequence. Acting on it would write a marker
// fenced at 0; the identity rule then classifies that marker unreadable, and the
// unreadable disposition on the PUBREL path deletes the acknowledged hold and answers
// 0x92 — manufacturing the exact loss F-062 closes.
func TestQoS2DiscardZeroWatermarkWritesNoMarker(t *testing.T) {
	t.Parallel()

	t.Run("a later sample resolves it", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "zero-watermark")
		holdSubject, holdSeq := e.hold(t, 1, nuid.Next(), "payload")

		var samples int
		e.setSubjectsSeam(t, func(realLast uint64) uint64 {
			samples++
			if samples == 1 {
				return 0
			}
			return realLast
		})
		var fences []uint64
		var fencesMu sync.Mutex // the walk arbitrates holds concurrently
		e.asm.mu.Lock()
		e.asm.testQoS2ClaimStore = func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
			fencesMu.Lock()
			fences = append(fences, c.Fence)
			fencesMu.Unlock()
			return e.asm.jsa.storeQoS2RelClaimCAS(subject, expectSeq, c)
		}
		e.asm.mu.Unlock()

		if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
			t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
		}
		if samples < 2 {
			t.Fatalf("the walk took %d sample(s); it must re-enumerate rather than act on an unusable one", samples)
		}
		for _, f := range fences {
			if f == 0 {
				t.Fatal("a marker was written with a zero fence: unreadable by the identity rule, " +
					"and the unreadable disposition destroys the acknowledged hold")
			}
		}
		if len(fences) != 1 || fences[0] < holdSeq {
			t.Fatalf("marker fences = %v, want exactly one at or above the hold at %d", fences, holdSeq)
		}
		if got := e.holdSeqOf(t, holdSubject); got != 0 {
			t.Fatalf("the hold at %d survived a walk that completed on a usable watermark", got)
		}
	})

	// Persistent incoherence. This is NOT reachable in contract: once a response carries
	// a record, sequence 1 has landed, and the last sequence never rewinds under a
	// delete, a replacement, a filtered purge, a compact at or below the tail, or expiry
	// (and this stream refuses MaxAge). It takes this very seam, a backend-contract
	// violation, or stream recreation. It is pinned anyway, because the alternative to
	// handling it is guessing.
	t.Run("exhaustion fails the clear", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "zero-watermark-forever")
		e.persist(t)
		holdSubject, holdSeq := e.hold(t, 1, nuid.Next(), "payload")
		e.setSubjectsSeam(t, func(uint64) uint64 { return 0 })

		// Driven through sess.clear(), not through the walk alone. Asserting only that the
		// helper returns an error cannot detect deleteTeardownTargets or clear() SWALLOWING
		// it and releasing the session record anyway — and that swallowing is the actual
		// terminal-cleanup-before-completion failure this case exists to pin.
		if err := e.sess.clear(false); err == nil {
			t.Fatal("clear() must fail when the walk cannot bound its purge: reporting the session " +
				"as cleaned up lets releaseTeardown delete the record while a releaser can still deliver")
		}
		// The record must still be there AND still marked: a released record would mean the
		// teardown reported completion, and an unmarked one would mean nobody will finish it.
		ps, _, perr := e.asm.loadFenceSession(e.sess.idHash)
		if perr != nil || ps == nil {
			t.Fatalf("a failed clear must leave the session record in place: ps=%v err=%v", ps, perr)
		}
		if ps.Teardown == nil {
			t.Fatal("a failed clear must leave the teardown marker on the record, or no later " +
				"CONNECT or sweep will finish it")
		}
		// Nothing was written and nothing was destroyed. The marked, partly torn-down
		// session refuses this client ID's next CONNECT until an adoption obtains a usable
		// watermark, which is the documented fail-closed cost (docs/CONFORMANCE.md).
		if cur, smsg := e.loadClaim(t, holdSubject); cur != nil || smsg != nil {
			t.Fatalf("a refused walk must write no claim, found %+v", cur)
		}
		rec, got, err := e.asm.jsa.loadQoS2Record(holdSubject)
		if err != nil || got != holdSeq || rec == nil {
			t.Fatalf("the hold must survive intact at %d: rec=%v seq=%d err=%v", holdSeq, rec, got, err)
		}
		if rec.state != mqttQoS2Held || rec.stored == nil || !strings.Contains(string(rec.stored.Data), "payload") {
			t.Fatalf("the held payload must be untouched, got state=%v", rec.state)
		}
	})
}

// TestQoS2TeardownStaleDiscardedClaimDoesNotPoisonReuse is F-062 mechanism 2, stated as
// the failure it WAS: a `discarded` marker was reaped by no teardown and no releaser path,
// so a LATER exchange on the same packet identifier read it, was answered 0x92 and had its
// hold released — losing an acknowledged message that the marker's writer never had any
// claim over. The conditional releaser reap this test drives is what closed it.
//
// Both orderings are staged, because the obvious repair (reap when the hold outranks
// the MARKER's own sequence) passes one and fails the other. What the releaser proves
// against is the fence, which is a property of the discard's ENTITLEMENT rather than of
// when its record happened to be written.
func TestQoS2TeardownStaleDiscardedClaimDoesNotPoisonReuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// stage returns the surviving exchange's generation and hold sequence, plus the
		// sequence the stale marker occupies.
		stage func(t *testing.T, e *testRelClaimEnv, holdSubject string, firstSeq uint64) (string, uint64, uint64)
	}{
		{
			name: "marker first: the later hold outranks the marker's record",
			stage: func(t *testing.T, e *testRelClaimEnv, holdSubject string, firstSeq uint64) (string, uint64, uint64) {
				e.vacate(t, firstSeq)
				markerSeq := e.putClaim(t, holdSubject,
					mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "retired-teardown", firstSeq))
				gen := nuid.Next()
				_, seq := e.hold(t, 1, gen, "acknowledged-payload")
				if seq <= markerSeq {
					t.Fatalf("this ordering needs the hold above the marker's record: %d <= %d", seq, markerSeq)
				}
				return gen, seq, markerSeq
			},
		},
		{
			// The discriminating one. E1 completes and vacates, the later exchange lands,
			// and only THEN does the retiring teardown write its marker — so the marker's
			// record sits ABOVE the hold it has no business fencing.
			name: "hold first: the marker's record outranks the later hold",
			stage: func(t *testing.T, e *testRelClaimEnv, holdSubject string, firstSeq uint64) (string, uint64, uint64) {
				e.vacate(t, firstSeq)
				gen := nuid.Next()
				_, seq := e.hold(t, 1, gen, "acknowledged-payload")
				markerSeq := e.putClaim(t, holdSubject,
					mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "retired-teardown", firstSeq))
				if markerSeq <= seq {
					t.Fatalf("this ordering needs the marker's record above the hold: %d <= %d", markerSeq, seq)
				}
				return gen, seq, markerSeq
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewRelClaimEnv(t, "stale-marker")
			holdSubject, firstSeq := e.hold(t, 1, nuid.Next(), "first-payload")
			gen, holdSeq, markerSeq := tc.stage(t, e, holdSubject, firstSeq)

			status, claimSeq, err := e.asm.claimQoS2Release(e.s, holdSubject, gen, holdSeq)
			if err != nil || status != mqttQoS2ClaimClaimed {
				t.Fatalf("claimQoS2Release = %v (err %v), want Claimed: a marker fenced at %d "+
					"has no business fencing a hold at %d", status, err, firstSeq, holdSeq)
			}
			if claimSeq == markerSeq {
				t.Fatal("the returned claim sits where the stale marker did: nothing was reaped")
			}
			// The reap targeted the marker's EXACT sequence, and what stands there now is
			// this exchange's own `releasing` claim.
			cur, smsg := e.loadClaim(t, holdSubject)
			if cur == nil || cur.State != mqttQoS2RelStateReleasing || cur.Gen != gen {
				t.Fatalf("claim = %+v, want this exchange's own releasing claim", cur)
			}
			if smsg.Sequence == markerSeq {
				t.Fatalf("a record still occupies the stale marker's sequence %d", markerSeq)
			}
			st, _, verr := e.asm.verifyQoS2HoldForRelease(holdSubject, gen, holdSeq)
			if verr != nil || st != mqttQoS2VerifyHeld {
				t.Fatalf("verifyQoS2HoldForRelease = %v (err %v), want Held: the reap must not be a "+
					"shortcut past fence 2", st, verr)
			}
		})
	}
}

// TestQoS2AwaitedReleaseDoesNotInheritAStaleClaimsEntitlement is the F-062 round-9 race, and
// the property it pins is the RELEASER'S VERDICT rather than the hold's survival.
//
// The interleaving: E1 completes and vacates the hold subject, leaving its `releasing` claim
// to be reaped. E2 lands on the freed subject and is answered PUBREC. The teardown enumerates
// with E2 AT OR BELOW its watermark, meets E1's stale claim, and waits. E2's PUBREL then reaps
// that foreign claim — a confirmed exact-sequence delete — and the subject is briefly empty
// before E2's own create lands.
//
// A discard that read that emptiness as "the exchange terminated" purged E2 while holding NO
// arbitration over it. The damage is not that E2's record went away: a teardown that enumerated
// E2 and wins the marker may legitimately destroy it. The damage is what E2's releaser is then
// told. With no marker on the subject its create SUCCEEDS, its re-read finds the hold gone, and
// `mqttQoS2VerifyGone` means "terminated and already reaped" — so the PUBREL answers 0x00,
// reporting success for a message that was destroyed and never delivered. Re-competing turns
// that into an honest 0x92: the walk wins a marker, and E2 is fenced rather than misinformed.
func TestQoS2AwaitedReleaseDoesNotInheritAStaleClaimsEntitlement(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "await-stale-claim")

	// E1 ran and completed, vacating the subject.
	holdSubject, firstSeq := e.hold(t, 1, nuid.Next(), "first-payload")
	e.vacate(t, firstSeq)
	// E2 lands on the freed identifier and is acknowledged.
	lateGen := nuid.Next()
	_, lateSeq := e.hold(t, 1, lateGen, "acknowledged-payload")
	// E1's releaser died before reaping its claim, so a FOREIGN generation's claim stands.
	staleClaimSeq := e.putClaim(t, holdSubject,
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "dead-releaser", 0))

	// E2's PUBREL reaps that foreign claim during the discard's poll, which is the window.
	// The first load is discardQoS2Hold's own and must still see the stale claim so the walk
	// dispatches into the wait; the second is the poll.
	var loads, reaped int
	var seamMu sync.Mutex // the walk arbitrates holds concurrently; reads after it are ordered by its join
	e.asm.mu.Lock()
	e.asm.testQoS2ClaimLoad = func(subject string) (*mqttQoS2RelClaim, *StoredMsg, error) {
		seamMu.Lock()
		defer seamMu.Unlock()
		loads++
		if loads == 2 && reaped == 0 {
			reaped = 1
			if st, err := e.asm.releaseQoS2Record(staleClaimSeq); st != mqttQoS2Released {
				t.Errorf("reaping the stale foreign claim: st=%v err=%v", st, err)
			}
		}
		return e.asm.jsa.loadQoS2RelClaim(subject)
	}
	e.asm.mu.Unlock()

	if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
		t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
	}
	if reaped == 0 {
		t.Fatal("the poll seam never fired, so this fixture staged no race")
	}

	// The walk must have WON a marker rather than inheriting the stale claim's absence, and
	// that marker must cover the exchange it destroyed — E2 was at or below its watermark.
	cur, _ := e.loadClaim(t, holdSubject)
	if cur == nil || cur.State != mqttQoS2RelStateDiscarded {
		t.Fatalf("claim = %+v, want a marker the walk won by competing after the wait", cur)
	}
	if cur.Fence < lateSeq {
		t.Fatalf("marker fence %d does not cover the hold at %d that the walk destroyed: "+
			"the walk purged something its own entitlement did not reach", cur.Fence, lateSeq)
	}

	// The decisive assertion. E2's releaser must be FENCED, never told the exchange completed.
	status, _, err := e.asm.claimQoS2Release(e.s, holdSubject, lateGen, lateSeq)
	if err != nil {
		t.Fatalf("claimQoS2Release: %v", err)
	}
	if status != mqttQoS2ClaimDiscarded {
		st, _, verr := e.asm.verifyQoS2HoldForRelease(holdSubject, lateGen, lateSeq)
		t.Fatalf("claim status = %v (verify = %v, err %v), want Discarded. Claimed+Gone is the "+
			"failure this test exists for: it answers 0x00 for an acknowledged message that was "+
			"destroyed and never delivered", status, st, verr)
	}
}

// TestQoS2DiscardRepairsAMaximalFenceMarker pins the liveness consequence of refusing a
// maximal fence at the identity rule rather than at the purge.
//
// Read as valid, such a marker is ADOPTED by arm A2 and its purge is then refused, so the
// clear fails; the record stays marked, and every later CONNECT adoption and every sweep pass
// repeats the same failure — a permanent client-ID lockout with no route out but an operator.
// Refused at the identity rule, it is an unreadable record, and the discard already knows how
// to repair one: exact-delete it and compete again. This asserts the repair, not the refusal.
func TestQoS2DiscardRepairsAMaximalFenceMarker(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "maximal-fence")
	holdSubject, holdSeq := e.hold(t, 1, nuid.Next(), "payload")

	// Written past the constructor, because the constructor's own callers can no longer
	// produce this shape — only a corrupt record can.
	body, _ := json.Marshal(&mqttQoS2RelClaim{
		State: mqttQoS2RelStateDiscarded, Fence: math.MaxUint64, Origin: "corrupt",
	})
	if _, err := e.asm.jsa.storeMsg(mqttQoS2ClaimSubjectFor(holdSubject), 0, body); err != nil {
		t.Fatalf("store a maximal-fence marker: %v", err)
	}

	// The classification is deliberately NOT asserted before the walk. Asserting it here would
	// make the mutation that accepts a maximal fence fail on the predicate and never reach the
	// walk at all — so the fixture would kill its mutation without ever exercising the liveness
	// consequence it is named for. That predicate is covered by
	// TestQoS2RelClaimIdentityValidation; what THIS test measures is the repair.
	//
	// The walk must COMPLETE rather than fail the clear.
	if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
		t.Fatalf("arbitrateQoS2ReleaseClaims: %v — a maximal fence must be repaired, not "+
			"stranded; that error IS the permanent client-ID lockout this refusal exists to "+
			"avoid, because every later adoption and sweep pass repeats it", err)
	}
	// The corrupt record is gone, replaced by a usable marker, and the hold is destroyed.
	cur, _ := e.loadClaim(t, holdSubject)
	if cur == nil || cur.State != mqttQoS2RelStateDiscarded {
		t.Fatalf("claim = %+v, want a fresh readable discard marker", cur)
	}
	if !mqttQoS2UsableCutoff(cur.Fence) || cur.Fence < holdSeq {
		t.Fatalf("replacement marker fence = %d, want a usable one at or above the hold at %d",
			cur.Fence, holdSeq)
	}
	if got := e.holdSeqOf(t, holdSubject); got != 0 {
		t.Fatalf("the hold at %d survived a completed discard", got)
	}
}

// TestQoS2HoldPurgeRefusesAnUnusableCutoff pins the two cutoff values that are
// refused rather than sent, for two different reasons.
//
// Zero would emit Sequence 1, which both stores treat as an explicit no-op: the hazard
// is FALSE SUCCESS, because a nil return licenses reporting the session's inbound QoS 2
// state as destroyed when nothing was removed. MaxUint64 would wrap upTo+1 to zero and
// emit the genuinely unbounded request, which is the shape the whole item removes.
//
// The fixture deliberately does NOT assert that no request reached the stream: a
// surviving record cannot prove it, since Sequence 1 is itself a successful no-op, and
// there is no observation seam on purgeStream. The error is what discriminates.
func TestQoS2HoldPurgeRefusesAnUnusableCutoff(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		upTo uint64
	}{
		{"zero would purge nothing and report success", 0},
		{"maximal would wrap to the unbounded request", math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewRelClaimEnv(t, "unusable-cutoff")
			holdSubject, holdSeq := e.hold(t, 1, nuid.Next(), "payload")

			if err := e.asm.purgeQoS2Hold(holdSubject, tc.upTo); err == nil {
				t.Fatalf("purgeQoS2Hold with cutoff %d returned success", tc.upTo)
			}
			if got := e.holdSeqOf(t, holdSubject); got != holdSeq {
				t.Fatalf("the hold is at %d, want it untouched at %d", got, holdSeq)
			}
		})
	}
}
