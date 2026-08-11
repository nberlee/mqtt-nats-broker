package mqtt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
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

	first := mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node1")
	seq, err := e.asm.jsa.storeQoS2RelClaimCAS(claimSubject, 0, first)
	if err != nil {
		t.Fatalf("first create-only claim: %v", err)
	}

	// Every subsequent create-only write must be rejected for as long as the record
	// exists, whoever writes it and whatever it says.
	for _, second := range []*mqttQoS2RelClaim{
		mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node2"),
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node2"),
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
		{"discarded without a generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded}, true},
		{"releasing without a generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing}, false},
		{"releasing with a short generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing, Gen: "abc"}, false},
		{"releasing with an out-of-alphabet generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateReleasing, Gen: strings.Repeat("!", mqttQoS2GenLen)}, false},
		{"discarded with a generation", &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded, Gen: gen}, false},
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
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node1"),
		mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node1"),
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
		c := &mqttQoS2RelClaim{State: mqttQoS2RelStateDiscarded, At: tc.at}
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
		{"releasing", mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), strings.Repeat("z", 4096))},
		{"discarded", mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, strings.Repeat("z", 4096))},
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
	if got := mqttMaxQoS2FormSize(pp, delivered, false); got < bound {
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
		// setup writes whatever already occupies the claim subject.
		setup    func(t *testing.T, e *testRelClaimEnv, holdSubject, gen string)
		want     mqttQoS2ClaimStatus
		wantSeq  bool // a claim sequence must be returned
		wantMine bool // afterwards the claim must be ours
	}{
		{
			name:     "uncontested",
			setup:    func(*testing.T, *testRelClaimEnv, string, string) {},
			want:     mqttQoS2ClaimClaimed,
			wantSeq:  true,
			wantMine: true,
		},
		{
			name: "our own generation already claimed",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, gen string) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "peer"))
			},
			want:     mqttQoS2ClaimAlreadyClaimed,
			wantSeq:  true,
			wantMine: true,
		},
		{
			name: "a discard won",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, _ string) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "peer"))
			},
			want:    mqttQoS2ClaimDiscarded,
			wantSeq: true,
		},
		{
			name: "a dead releaser's foreign generation is reaped and retried",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, _ string) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "dead"))
			},
			want:     mqttQoS2ClaimClaimed,
			wantSeq:  true,
			wantMine: true,
		},
		{
			name: "an unreadable record holds the subject",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject, _ string) {
				if _, err := e.asm.jsa.storeMsg(mqttQoS2ClaimSubjectFor(holdSubject), 0, []byte("{oops")); err != nil {
					t.Fatalf("store malformed claim: %v", err)
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
			tc.setup(t, e, holdSubject, gen)

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
				if err := e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, false); err != nil {
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
				if err := e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, false); err != nil {
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
				if err := e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, false); err != nil {
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
		setup     func(t *testing.T, e *testRelClaimEnv, holdSubject string)
		wantHold  bool // the hold record must survive
		wantErr   bool
		wantClaim string // the claim state that must be on the subject afterwards ("" = any)
	}{
		{
			name:      "uncontested: the hold is destroyed",
			setup:     func(*testing.T, *testRelClaimEnv, string) {},
			wantClaim: mqttQoS2RelStateDiscarded,
		},
		{
			name: "a peer discard already claimed: still destroyed",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject string) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "peer"))
			},
			wantClaim: mqttQoS2RelStateDiscarded,
		},
		{
			name: "a releaser won: the hold is left alone",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject string) {
				e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer"))
			},
			wantHold:  true,
			wantClaim: mqttQoS2RelStateReleasing,
		},
		{
			name: "an unreadable claim is reaped and the discard competes again",
			setup: func(t *testing.T, e *testRelClaimEnv, holdSubject string) {
				if _, err := e.asm.jsa.storeMsg(mqttQoS2ClaimSubjectFor(holdSubject), 0, []byte("{oops")); err != nil {
					t.Fatalf("store malformed claim: %v", err)
				}
			},
			wantClaim: mqttQoS2RelStateDiscarded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRelClaimEnv(t, "arbitrate")
			holdSubject, _ := e.hold(t, 1, nuid.Next(), "payload")
			tc.setup(t, e, holdSubject)

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

	var loads int
	e.asm.mu.Lock()
	e.asm.testQoS2Load = func(subject string) (*mqttQoS2Record, uint64, error) {
		loads++
		return e.asm.jsa.loadQoS2Record(subject)
	}
	e.asm.mu.Unlock()

	if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
		t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
	}
	if loads != 0 {
		t.Fatalf("the discard loaded %d hold record(s); it must decide from the claim register alone", loads)
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
	staleSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node1"))
	if err := e.asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, staleSeq, true); err != nil {
		t.Fatalf("remove the first claim: %v", err)
	}

	// Exchange 2 on the same identifier claims.
	freshGen := nuid.Next()
	freshSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, freshGen, "node2"))
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
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer"))

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

// TestQoS2ClearWaitsOutACompletingRelease pins the other half: when the releasing
// broker finishes inside the budget, the discard sees the claim vanish and leaves
// nothing behind.
func TestQoS2ClearWaitsOutACompletingRelease(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "wait-out")
	holdSubject, _ := e.hold(t, 1, nuid.Next(), "payload")
	claimSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer"))

	// The releaser completes shortly after the discard starts waiting: it commits
	// (here, simply removes the hold) and then removes its claim, which is the order
	// the real path uses and the reason claim-absence proves termination.
	go func() {
		time.Sleep(mqttQoS2DiscardWaitInterval / 2)
		_ = e.asm.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, holdSubject, false)
		_ = e.asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, claimSeq, true)
	}()

	if err := e.sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the hold survived a release that completed inside the discard's wait")
	}
	if cur, smsg := e.loadClaim(t, holdSubject); cur != nil || smsg != nil {
		t.Fatalf("a completed release must leave no claim behind, found %+v", cur)
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
	e.putClaim(t, claimed, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer"))

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
