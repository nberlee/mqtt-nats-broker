//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// Coverage for the recoverable inbound QoS 2 move protocol (qos2_move.go): the
// two-state hold record, its generation identity, the three cleanup paths that
// must confirm before answering the client, and the max-payload bounds for all
// three record forms.
//
// The interesting races all live in the window between loading the held record and
// committing its terminal state. Only testQoS2PreFinalize can reach that window —
// the deleteMsg hook cannot, because after the reorder deleteMsg runs after the
// commit — so the Gone/Superseded tests are driven through that seam and not
// through a hook that would silently exercise nothing.

// ---------------------------------------------------------------------------
// Unit-level: identity validation, classification, encoder/bound agreement.
// ---------------------------------------------------------------------------

// A generation ID is an identity token: anything that is not exactly 22
// characters of the NUID base-62 alphabet must be rejected, or a record carrying
// junk would be admitted into the identity space and compared against real
// generations. Mirrors the mqttValidSessEpoch rules.
func TestQoS2GenValidation(t *testing.T) {
	real := nuid.Next()
	for _, tc := range []struct {
		name string
		gen  string
		want bool
	}{
		{"real nuid", real, true},
		{"empty", "", false},
		{"21 chars", strings.Repeat("a", 21), false},
		{"23 chars", strings.Repeat("a", 23), false},
		{"22 out-of-alphabet bytes", strings.Repeat("-", 22), false},
		{"22 chars, one out-of-alphabet", strings.Repeat("a", 21) + "!", false},
		{"22 chars, high byte", strings.Repeat("a", 21) + "\xff", false},
		{"all digits", strings.Repeat("0", 22), true},
		{"mixed case", "AaBbCcDdEeFfGgHhIiJjKk", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mqttValidQoS2Gen(tc.gen); got != tc.want {
				t.Fatalf("mqttValidQoS2Gen(%q) = %v, want %v", tc.gen, got, tc.want)
			}
		})
	}

	// A record's generation is extracted with a strict single-occurrence rule:
	// getHeader returns only the first match, so two declarations would otherwise
	// resolve silently to an arbitrary identity. The rule is deliberately
	// count-based rather than value-based — a repeated identity declaration is a
	// malformed record whether or not the two values agree, and comparing values
	// invites an implementation that quietly accepts the identical case.
	other := nuid.Next()
	for _, tc := range []struct {
		name string
		hdr  string
		want string
	}{
		{"single valid", hdrLine + mqttQoS2GenHeader + ":" + real + CR_LF + CR_LF, real},
		{"absent", hdrLine + "Nmqtt-Pub:2" + CR_LF + CR_LF, _EMPTY_},
		{"invalid value", hdrLine + mqttQoS2GenHeader + ":short" + CR_LF + CR_LF, _EMPTY_},
		{
			"two differing values",
			hdrLine + mqttQoS2GenHeader + ":" + real + CR_LF + mqttQoS2GenHeader + ":" + other + CR_LF + CR_LF,
			_EMPTY_,
		},
		{
			// The case a value comparison would wrongly accept.
			"two identical values",
			hdrLine + mqttQoS2GenHeader + ":" + real + CR_LF + mqttQoS2GenHeader + ":" + real + CR_LF + CR_LF,
			_EMPTY_,
		},
	} {
		t.Run("extract/"+tc.name, func(t *testing.T) {
			if got := mqttQoS2GenOf([]byte(tc.hdr)); got != tc.want {
				t.Fatalf("mqttQoS2GenOf() = %q, want %q", got, tc.want)
			}
		})
	}
}

// A record with a done marker but no attributable generation must classify as
// corrupt, NOT delivered: the terminal branch would otherwise answer PUBCOMP 0x00
// for a record it cannot identify, claiming a delivery it cannot prove happened.
func TestQoS2CorruptDoneMarkerIsCorruptNotDone(t *testing.T) {
	gen := nuid.Next()
	for _, tc := range []struct {
		name string
		hdr  string
		want mqttQoS2State
	}{
		{
			"valid tombstone",
			hdrLine + mqttQoS2GenHeader + ":" + gen + CR_LF + mqttQoS2DoneHeader + ":1" + CR_LF + CR_LF,
			mqttQoS2Done,
		},
		{
			"done marker, no generation",
			hdrLine + mqttQoS2DoneHeader + ":1" + CR_LF + CR_LF,
			mqttQoS2Corrupt,
		},
		{
			"done marker, malformed generation",
			hdrLine + mqttQoS2GenHeader + ":nope" + CR_LF + mqttQoS2DoneHeader + ":1" + CR_LF + CR_LF,
			mqttQoS2Corrupt,
		},
		{
			// The marker's contract is exactly "1"; a forged or truncated value leaves
			// the record's state ambiguous, which is not the same as held.
			"done marker with the wrong value",
			hdrLine + mqttQoS2GenHeader + ":" + gen + CR_LF + mqttQoS2DoneHeader + ":0" + CR_LF + CR_LF,
			mqttQoS2Corrupt,
		},
		{
			// The row that pins the occurrence-count rule. An EMPTY value is
			// indistinguishable from an absent header by length, so classifying on
			// len(getHeader(...)) would call this `held` — and a fresh PUBLISH on that
			// identifier would then be answered PUBREC without its payload ever being
			// stored.
			"done marker with an empty value",
			hdrLine + mqttQoS2GenHeader + ":" + gen + CR_LF + mqttQoS2DoneHeader + ":" + CR_LF + CR_LF,
			mqttQoS2Corrupt,
		},
		{
			"done marker declared twice",
			hdrLine + mqttQoS2GenHeader + ":" + gen + CR_LF +
				mqttQoS2DoneHeader + ":1" + CR_LF + mqttQoS2DoneHeader + ":1" + CR_LF + CR_LF,
			mqttQoS2Corrupt,
		},
		{
			"done marker declared twice with differing values",
			hdrLine + mqttQoS2GenHeader + ":" + gen + CR_LF +
				mqttQoS2DoneHeader + ":1" + CR_LF + mqttQoS2DoneHeader + ":0" + CR_LF + CR_LF,
			mqttQoS2Corrupt,
		},
		{
			"generation only is held",
			hdrLine + mqttQoS2GenHeader + ":" + gen + CR_LF + CR_LF,
			mqttQoS2Held,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := mqttClassifyQoS2Record(&StoredMsg{Header: []byte(tc.hdr)})
			if rec.state != tc.want {
				t.Fatalf("state = %v, want %v", rec.state, tc.want)
			}
			if rec.state == mqttQoS2Corrupt && rec.gen != _EMPTY_ {
				t.Fatalf("a corrupt record must carry no generation, got %q", rec.gen)
			}
		})
	}
}

// The terminal record's size bound must never be smaller than what the encoder
// actually produces, at any expected sequence. This is the test that keeps the
// max-payload preflight and the encoder from drifting apart — and drift here is
// not a cosmetic bug: an under-budgeted terminal record means the onward store
// succeeds, the commit is rejected for size, and every PUBREL retry repeats the
// delivery forever.
func TestQoS2TerminalRecordBoundMatchesEncoder(t *testing.T) {
	bound := mqttQoS2TerminalRecordMax()
	seqs := []uint64{0, 1, 9, 10, math.MaxUint64}
	for p := uint64(10); p < math.MaxUint64/10; p *= 10 {
		seqs = append(seqs, p, p-1)
	}
	for _, seq := range seqs {
		msg, hdrLen := buildQoS2DoneRecord(nuid.Next(), seq)
		if len(msg) > bound {
			t.Fatalf("buildQoS2DoneRecord(seq=%d) is %d bytes, exceeds the bound %d", seq, len(msg), bound)
		}
		// The tombstone has no body: everything is header.
		if hdrLen != len(msg) {
			t.Fatalf("buildQoS2DoneRecord(seq=%d): hdrLen %d != len %d (the tombstone must have an empty body)", seq, hdrLen, len(msg))
		}
	}
	// Exact at the maximal sequence, so the bound is tight rather than merely safe.
	if msg, _ := buildQoS2DoneRecord(nuid.Next(), math.MaxUint64); len(msg) != bound {
		t.Fatalf("at MaxUint64 the encoder produces %d bytes but the bound is %d; the bound is not tight", len(msg), bound)
	}
}

// The header bounds used by the preflight must not be smaller than what the
// encoders emit, for the same reason.
func TestQoS2HeaderBoundsMatchEncoders(t *testing.T) {
	gen := nuid.Next()
	pp := &mqttPublish{topic: []byte("b"), subject: []byte("b"), msg: []byte("x"), sz: 1}

	base := mqttComputeNatsMsgSize(pp, false, 0, _EMPTY_, false, _EMPTY_, _EMPTY_)
	withID := mqttComputeNatsMsgSize(pp, false, 0, _EMPTY_, false, mqttQoS2ReleaseMsgId(gen), _EMPTY_)
	if got := withID - base; got != mqttQoS2MsgIdHeaderMax {
		t.Fatalf("Nats-Msg-Id header costs %d bytes, bound is %d", got, mqttQoS2MsgIdHeaderMax)
	}

	baseHold := mqttComputeNatsMsgSize(pp, true, 0, _EMPTY_, false, _EMPTY_, _EMPTY_)
	withGen := mqttComputeNatsMsgSize(pp, true, 0, _EMPTY_, false, _EMPTY_, gen)
	if got := withGen - baseHold; got != mqttQoS2GenHeaderMax {
		t.Fatalf("Nmqtt-Q2Gen header costs %d bytes, bound is %d", got, mqttQoS2GenHeaderMax)
	}

	// The create-only CAS header the hold store splices in. Exact, not an estimate:
	// the hold write always expects sequence 0.
	msg, hdrLen := mqttNewDeliverableMessage(pp, true, 0, _EMPTY_, false, _EMPTY_, gen)
	var bb bytes.Buffer
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":0")
	bb.WriteString(CR_LF)
	bb.Write(msg[len(hdrLine):hdrLen])
	bb.Write(msg[hdrLen:])
	if got := bb.Len() - len(msg); got != mqttQoS2HoldCASHeaderMax {
		t.Fatalf("hold CAS header costs %d bytes, bound is %d", got, mqttQoS2HoldCASHeaderMax)
	}
}

// The encoder and the sizer must agree byte-for-byte with the new headers in
// play, or headerLen is wrong and the stored message is corrupt.
func TestQoS2EncoderSizeMatchesComputed(t *testing.T) {
	gen := nuid.Next()
	pp := &mqttPublish{topic: []byte("a/b"), subject: []byte("a.b"), msg: []byte("payload"), sz: 7}
	for _, tc := range []struct {
		name         string
		encodePP     bool
		msgId, q2Gen string
		ttl          uint32
		origin       string
		retained     bool
	}{
		{name: "hold form", encodePP: true, q2Gen: gen},
		{name: "release form", msgId: mqttQoS2ReleaseMsgId(gen), ttl: 60, origin: "o"},
		{name: "release form retained", msgId: mqttQoS2ReleaseMsgId(gen), retained: true},
		{name: "neither", encodePP: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := mqttComputeNatsMsgSize(pp, tc.encodePP, tc.ttl, tc.origin, tc.retained, tc.msgId, tc.q2Gen)
			got, _ := mqttNewDeliverableMessage(pp, tc.encodePP, tc.ttl, tc.origin, tc.retained, tc.msgId, tc.q2Gen)
			if len(got) != want {
				t.Fatalf("encoded %d bytes, sizer said %d", len(got), want)
			}
		})
	}
}

// Only the two structural wrong-last-sequence codes mean "the subject is not at
// the sequence I expected". Classifying anything else as a CAS rejection would
// turn a real store failure into a silent retry.
func TestQoS2CASRejectionClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"10071 wrong last sequence", ApiErrors[JSStreamWrongLastSequenceErrF], true},
		{"10164 wrong last sequence constant", ApiErrors[JSStreamWrongLastSequenceConstantErr], true},
		{"10077 store failed", ApiErrors[JSStreamStoreFailedF], false},
		{"stream not found", ApiErrors[JSStreamNotFoundErr], false},
		{"no message found", ApiErrors[JSNoMessageFoundErr], false},
		{"nil", nil, false},
		{"plain error", fmt.Errorf("boom"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isQoS2CASRejection(tc.err); got != tc.want {
				t.Fatalf("isQoS2CASRejection(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Corrupt-record logging must carry enough to diagnose the fault and nothing of
// the payload: the body is application data, and a record can be as large as
// max_payload, so logging it would both leak user data and produce unbounded log
// lines whenever a fault repeats.
//
// This captures the REAL log lines emitted by the production branches, not the
// return value of the helper that composes them. Asserting on the helper alone
// would be circular: it is the call sites that could regress to logging
// rec.stored.Data, and a helper-only test stays green while they do.
func TestQoS2CorruptLoggingOmitsPayload(t *testing.T) {
	const secret = "SUPERSECRETPAYLOAD"
	// Long enough that a raw dump would also be a very large log line, which is the
	// second half of why the raw record must never be logged.
	body := strings.Repeat(secret, 200)

	for _, tc := range []struct {
		name string
		// seed writes the record that will trigger the branch under test.
		seed       func(t *testing.T, b *testQoS2Broker, subject string)
		wantReason string
	}{
		{
			// K: no valid generation at all.
			name: "corrupt record",
			seed: func(t *testing.T, b *testQoS2Broker, subject string) {
				hdr := hdrLine + CR_LF
				if _, err := b.jsa.storeQoS2HoldCAS(subject, 0, len(hdr), []byte(hdr+body)); err != nil {
					t.Fatalf("seeding the corrupt record: %v", err)
				}
			},
			wantReason: "no valid generation",
		},
		{
			// V: a valid generation, but publish headers we cannot use.
			name: "malformed publish headers",
			seed: func(t *testing.T, b *testQoS2Broker, subject string) {
				hdr := hdrLine + mqttQoS2GenHeader + ":" + nuid.Next() + CR_LF + CR_LF
				if _, err := b.jsa.storeQoS2HoldCAS(subject, 0, len(hdr), []byte(hdr+body)); err != nil {
					t.Fatalf("seeding the malformed record: %v", err)
				}
			},
			wantReason: "Nmqtt-Pub",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQoS2Setup(t, "log-"+strings.ReplaceAll(tc.name, " ", ""))
			pi := uint16(61)
			subject := b.holdSubject(t, pi)
			tc.seed(t, b, subject)

			errCh := make(chan string, 64)
			b.s.SetLogger(&captureErrorLogger{errCh: errCh}, false, false)

			testQoS2SendPubRel(t, b.cp, pi)
			if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
				t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonPacketIDNotFound, reason)
			}

			// Collect the line this branch emitted, then keep draining. Stopping at the
			// first match would leave a LATER statement unread — and "someone adds a
			// second log call that dumps the record" is exactly the regression this
			// test exists to catch, so every line must be inspected, not just the
			// expected one. captureErrorLogger.Errorf writes synchronously into the
			// buffered channel, so a bounded quiet period is enough to drain.
			var lines []string
			var found string
			deadline := time.After(testMQTTTimeout)
			for found == _EMPTY_ {
				select {
				case line := <-errCh:
					lines = append(lines, line)
					if strings.Contains(line, "QoS2 hold record") {
						found = line
					}
				case <-deadline:
					t.Fatal("no QoS2 hold-record error was logged")
				}
			}
			for draining, quiet := true, time.After(500*time.Millisecond); draining; {
				select {
				case line := <-errCh:
					lines = append(lines, line)
				case <-quiet:
					draining = false
				}
			}

			// The metadata an operator needs: the hash-bearing subject (which is what
			// you grep for and carries the client ID hash and packet identifier), the
			// stream sequence, both lengths, and why it was rejected.
			seq := uint64(1)
			for _, want := range []string{
				subject,
				fmt.Sprintf("seq %d", seq),
				fmt.Sprintf("body %d bytes", len(body)),
				"header ",
				tc.wantReason,
			} {
				if !strings.Contains(found, want) {
					t.Fatalf("log line %q does not mention %q", found, want)
				}
			}

			// No captured line may carry the payload or the raw headers.
			for _, line := range lines {
				if strings.Contains(line, secret) {
					t.Fatalf("a log line leaks the payload: %.300q...", line)
				}
				if strings.Contains(line, hdrLine) || strings.Contains(line, mqttQoS2GenHeader+":") {
					t.Fatalf("a log line leaks raw headers: %q", line)
				}
			}

			// The broker-composed DETAIL must identify the client by hash (via the
			// subject) and not by raw client ID. Assert against the detail alone, not
			// the whole line: c.Errorf prefixes every message with the client label,
			// which legitimately contains the raw ID, so a whole-line check either
			// always fires or — as an earlier version of this test did — dodges the
			// prefix with a pattern (b.pubID+".") that the raw ID never actually
			// matches, making the assertion vacuous.
			at := strings.Index(found, "(subject ")
			if at < 0 {
				t.Fatalf("could not locate the record detail in %q", found)
			}
			if detail := found[at:]; strings.Contains(detail, b.pubID) {
				t.Fatalf("the record detail embeds the raw client ID %q: %q", b.pubID, detail)
			}
		})
	}
}

// Every field validateQoS2InStream rejects, rejected independently — and a
// positive row for the exact configuration the create path produces, so the
// validator cannot reject its own output.
func TestValidateQoS2InStream(t *testing.T) {
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:       mqttQoS2IncomingMsgsStreamName,
			Subjects:   []string{mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
			Replicas:   3,
		}}
	}
	if err := validateQoS2InStream(good(), 3); err != nil {
		t.Fatalf("the create path's own configuration was rejected: %v", err)
	}
	if err := validateQoS2InStream(good(), 0); err != nil {
		t.Fatalf("unexpected rejection with no replica expectation: %v", err)
	}
	if err := validateQoS2InStream(nil, 0); err == nil {
		t.Fatal("expected nil stream info to be rejected")
	}

	for _, tc := range []struct {
		name  string
		mutid func(cfg *StreamConfig)
	}{
		// State shape.
		{"wrong subject", func(c *StreamConfig) { c.Subjects = []string{"other.>"} }},
		{"extra subject", func(c *StreamConfig) { c.Subjects = append(c.Subjects, "other.>") }},
		{"no subjects", func(c *StreamConfig) { c.Subjects = nil }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"interest retention", func(c *StreamConfig) { c.Retention = InterestPolicy }},
		{"discard new", func(c *StreamConfig) { c.Discard = DiscardNew }},
		{"max msgs per 0", func(c *StreamConfig) { c.MaxMsgsPer = 0 }},
		{"max msgs per 2", func(c *StreamConfig) { c.MaxMsgsPer = 2 }},
		{"global max msgs", func(c *StreamConfig) { c.MaxMsgs = 100 }},
		{"global max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"global max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} }},
		{"sources", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} }},
		{"wrong replicas", func(c *StreamConfig) { c.Replicas = 1 }},
		// Protocol prerequisites: settings that leave the shape intact but withdraw
		// something the move depends on.
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		{"deny delete", func(c *StreamConfig) { c.DenyDelete = true }},
		{"deny purge", func(c *StreamConfig) { c.DenyPurge = true }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) { c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"} }},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"allow msg counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
		{"async persist mode", func(c *StreamConfig) { c.PersistMode = AsyncPersistMode }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mutid(&si.Config)
			if err := validateQoS2InStream(si, 3); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}
}

// A pre-existing $MQTT_qos2in in the OLD (pre-move) configuration must make
// account-manager initialization FAIL, with the stop-all-brokers/recreate
// guidance — not be silently updated in place, and not be accepted.
//
// This is the end-to-end complement to TestValidateQoS2InStream: that test proves
// the validator's own logic, this one proves the validator is actually wired into
// startup and that its message reaches an operator. The distinction matters
// because the failure it prevents is silent data loss during a rolling upgrade —
// the moment one upgraded broker flipped the stream to DiscardOld, an old broker
// still running the plain hold store would overwrite held records instead of
// getting its duplicate rejection.
func TestQoS2InStreamMisconfigurationFailsStartup(t *testing.T) {
	_, nc := runNATS(t)

	s := &Server{
		nrc:      nc,
		opts:     &Options{},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	defer close(s.quitCh)

	// Pre-create the stream exactly as a pre-move development store has it:
	// DiscardNew + DiscardNewPer, which makes the terminal CAS a rejection rather
	// than a replacement.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:                 mqttQoS2IncomingMsgsStreamName,
		Subjects:             []string{mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"},
		Storage:              nats.FileStorage,
		Retention:            nats.LimitsPolicy,
		Discard:              nats.DiscardNew,
		DiscardNewPerSubject: true,
		MaxMsgsPerSubject:    1,
		Replicas:             1,
	}); err != nil {
		t.Fatalf("pre-create the old-configuration stream: %v", err)
	}

	c := s.createInternalAccountClient()
	_, err = s.getOrCreateMQTTAccountSessionManager(c)
	if err == nil {
		t.Fatal("expected initialization to fail against a pre-move $MQTT_qos2in, but it succeeded")
	}
	// The message must name the problem AND tell the operator what to do; a bare
	// "not usable" would leave a stuck deployment with no next step.
	for _, want := range []string{
		mqttQoS2IncomingMsgsStreamName,
		"discard is DiscardNew",
		"stop ALL brokers",
		"recreate",
		"not a rolling upgrade",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("startup error %q does not mention %q", err, want)
		}
	}

	// It must NOT have mutated the stream in place: an old broker still running
	// against it would silently overwrite held records once the discard policy
	// flipped, which is worse than refusing to start.
	si, ierr := js.StreamInfo(mqttQoS2IncomingMsgsStreamName)
	if ierr != nil {
		t.Fatalf("stream info after the refused startup: %v", ierr)
	}
	if si.Config.Discard != nats.DiscardNew {
		t.Fatalf("the refused startup mutated the stream: discard is now %v, want unchanged (new)", si.Config.Discard)
	}

	// Recreating it in the required shape clears the failure — the documented
	// recovery, proven rather than asserted.
	if derr := js.DeleteStream(mqttQoS2IncomingMsgsStreamName); derr != nil {
		t.Fatalf("delete the old-configuration stream: %v", derr)
	}
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	asm, rerr := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if rerr != nil {
		t.Fatalf("initialization still fails after recreating the stream: %v", rerr)
	}
	rsi, rierr := asm.jsa.lookupStream(mqttQoS2IncomingMsgsStreamName)
	if rierr != nil || rsi == nil {
		t.Fatalf("lookup the recreated stream: si=%v err=%v", rsi, rierr)
	}
	if verr := validateQoS2InStream(rsi, 0); verr != nil {
		t.Fatalf("the recreated stream does not satisfy the validator: %v", verr)
	}
}

// ---------------------------------------------------------------------------
// End-to-end: the state machine over a real broker.
// ---------------------------------------------------------------------------

// testQoS2Broker starts a v5 broker with a QoS 2 subscriber and a publisher.
type testQoS2Broker struct {
	s      *Server
	o      *Options
	cs, cp net.Conn
	rs, rp *mqttReader
	asm    *mqttAccountSessionManager
	jsa    *mqttJSA
	topic  string
	pubID  string
}

func testQoS2Setup(t *testing.T, name string) *testQoS2Broker {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })

	// The subscriber is a delivery witness, not a subject under test, so it speaks
	// 3.1.1: a v5 delivery prefixes a properties block that every payload assertion
	// would then have to strip. The publisher stays v5 because the branches under
	// test are distinguished by PUBCOMP reason codes, which 3.1.1 has no room for.
	topic := name + "/topic"
	cs, rs := testMQTTConnect(t, &mqttConnInfo{clientID: name + "-sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { cs.Close() })
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cs, rs, []*mqttFilter{{filter: topic, qos: 2}}, []byte{2})
	testMQTTFlush(t, cs, nil, rs)

	pubID := name + "-pub"
	b := &testQoS2Broker{s: s, o: o, cs: cs, rs: rs, topic: topic, pubID: pubID}
	b.cp, b.rp = b.connectPublisher(t)
	t.Cleanup(func() { b.cp.Close() })

	cl := testMQTTGetClient(t, s, pubID)
	b.asm, b.jsa = cl.mqtt.asm, cl.mqtt.sess.jsa
	return b
}

// connectPublisher opens (or reopens) the publisher connection. The Session
// Expiry Interval is essential rather than incidental: with the v5 default of 0, a
// broker-forced close discards the session, and sess.clear() then purges exactly
// the QoS 2 records these tests are asserting about — so every "the record
// survived the failure" check would pass or fail for the wrong reason.
func (b *testQoS2Broker) connectPublisher(t *testing.T) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   b.pubID,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	return c, r
}

// reconnectPublisher replaces the publisher connection after the broker closed it,
// resuming the same session.
func (b *testQoS2Broker) reconnectPublisher(t *testing.T) (net.Conn, *mqttReader) {
	t.Helper()
	b.cp.Close()
	c, r := b.connectPublisher(t)
	b.cp, b.rp = c, r
	t.Cleanup(func() { c.Close() })
	return c, r
}

// expectNoPubComp asserts the client was NOT told the exchange finished. A v5
// broker announces the close with a DISCONNECT before dropping the connection, so
// "no packet at all" would be the wrong assertion — what matters is that no
// PUBCOMP arrives, because a PUBCOMP is what frees the packet identifier. Anything
// else the broker sends on its way out is accepted and reported.
func (b *testQoS2Broker) expectNoPubComp(t *testing.T) {
	t.Helper()
	for {
		bt, _, ok := testMQTTReadPacketReady(b.rp, testMQTTTimeout)
		if !ok {
			return // connection closed or went quiet
		}
		switch pt := bt & mqttPacketMask; pt {
		case mqttPacketPubComp:
			t.Fatalf("Expected no PUBCOMP, but the exchange was reported as finished")
		case mqttPacketDisconnect:
			return // the broker's own close announcement
		default:
			t.Fatalf("Unexpected packet type %v while expecting no PUBCOMP", pt)
		}
	}
}

// holdSubject returns the hold subject for pi on the publisher's session.
func (b *testQoS2Broker) holdSubject(t *testing.T, pi uint16) string {
	t.Helper()
	return testMQTTGetClient(t, b.s, b.pubID).mqttQoS2InternalSubject(pi)
}

// setPreFinalize installs the pre-finalize seam for this broker's account manager.
func (b *testQoS2Broker) setPreFinalize(t *testing.T, fn func(subject, gen string, seq uint64) error) {
	t.Helper()
	b.asm.mu.Lock()
	b.asm.testQoS2PreFinalize = fn
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQoS2PreFinalize = nil
		b.asm.mu.Unlock()
	})
}

// consumeOne completes the subscriber's QoS 2 exchange for one expected payload.
func (b *testQoS2Broker) consumeOne(t *testing.T, payload string) {
	t.Helper()
	subPI := testMQTTCheckPubMsgNoAck(t, b.cs, b.rs, b.topic, mqttPubQoS2, []byte(payload))
	testMQTTSendPIPacket(mqttPacketPubRec, t, b.cs, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, b.rs, subPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, b.cs, subPI)
}

// THE regression test for the flaw that rejected the first design: a finalizer
// that re-read after a failed CAS and retried against the newly observed sequence
// could CAS a NEWER session generation's held record into a tombstone, discarding
// a message that was never delivered.
//
// The seam replaces the held record with a different generation in exactly the
// window a session discard plus a new PUBLISH would. The finalizer must classify
// that as superseded, mutate nothing, and answer 0x92 — and the replacement must
// still be deliverable afterwards.
func TestQoS2FinalizeDoesNotMutateNewGeneration(t *testing.T) {
	b := testQoS2Setup(t, "supersede")
	pi := uint16(21)
	subject := b.holdSubject(t, pi)

	var replacedGen string
	var replacedRaw []byte
	var once sync.Once
	b.setPreFinalize(t, func(gotSubject, gen string, seq uint64) error {
		if gotSubject != subject {
			return nil
		}
		once.Do(func() {
			// Model the race exactly: the old record is gone (as a discard purge would
			// leave it) and a NEW generation now occupies the same subject with a
			// payload that has never been delivered.
			if err := b.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, seq, true); err != nil {
				t.Errorf("test seam could not purge the held record: %v", err)
				return
			}
			replacedGen = nuid.Next()
			pp := &mqttPublish{topic: []byte(b.topic), subject: mqttTestTopicSubject(t, b.topic),
				msg: []byte("newer"), sz: len("newer"), flags: 2 << 1}
			msg, hdrLen := mqttNewDeliverableMessage(pp, true, 0, _EMPTY_, false, _EMPTY_, replacedGen)
			if _, err := b.jsa.storeQoS2HoldCAS(subject, 0, hdrLen, msg); err != nil {
				t.Errorf("test seam could not store the replacement record: %v", err)
				return
			}
			rec, _, err := b.jsa.loadQoS2Record(subject)
			if err != nil || rec == nil {
				t.Errorf("test seam could not read back the replacement: rec=%v err=%v", rec, err)
				return
			}
			replacedRaw = append([]byte(nil), rec.stored.Data...)
		})
		return nil
	})

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("original"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)

	// Superseded: this identifier's state is no longer ours to close.
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected reason 0x%x for a superseded finalize, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}

	// The replacement is byte-identical and still held: nothing was mutated.
	rec, _, err := b.jsa.loadQoS2Record(subject)
	if err != nil || rec == nil {
		t.Fatalf("Expected the replacement record to survive: rec=%v err=%v", rec, err)
	}
	if rec.state != mqttQoS2Held {
		t.Fatalf("Replacement record state = %v, want held (the finalizer overwrote a foreign generation)", rec.state)
	}
	if rec.gen != replacedGen {
		t.Fatalf("Replacement generation = %q, want %q", rec.gen, replacedGen)
	}
	if !bytes.Equal(rec.stored.Data, replacedRaw) {
		t.Fatalf("Replacement payload changed: got %q, want %q", rec.stored.Data, replacedRaw)
	}

	// The original was delivered (the onward store precedes the commit), and the
	// replacement's own PUBREL still delivers its payload exactly once.
	b.consumeOne(t, "original")
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the replacement's PUBREL to succeed, got 0x%x", reason)
	}
	b.consumeOne(t, "newer")
	testMQTTExpectNothing(t, b.rs)
}

// A held record that races away entirely before the commit is Gone, not an error:
// the onward store already succeeded, so the exchange is complete and the client
// is answered 0x00.
func TestQoS2FinalizeGoneAfterPurge(t *testing.T) {
	b := testQoS2Setup(t, "gone")
	pi := uint16(22)
	subject := b.holdSubject(t, pi)

	var once sync.Once
	b.setPreFinalize(t, func(gotSubject, gen string, seq uint64) error {
		if gotSubject != subject {
			return nil
		}
		once.Do(func() {
			if err := b.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, seq, true); err != nil {
				t.Errorf("test seam could not purge the held record: %v", err)
			}
		})
		return nil
	})

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("gone"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x when our generation is already absent, got 0x%x", mqttReasonSuccess, reason)
	}
	b.consumeOne(t, "gone")
	testMQTTExpectNothing(t, b.rs)
	testQoS2NoRecord(t, b.jsa, subject)
}

// The core guarantee. When terminal cleanup fails, the tombstone survives — and a
// retried PUBREL must be answered 0x00 from it with NO second delivery. This is
// what the state machine buys over a deduplication window: the suppression is
// indefinite and does not depend on any window still holding the message id.
func TestQoS2PubRelDoneRecordAnsweredWithoutRedelivery(t *testing.T) {
	b := testQoS2Setup(t, "tombstone")
	pi := uint16(23)
	subject := b.holdSubject(t, pi)

	// Block every delete on the hold stream so the tombstone cannot be reaped.
	var blocking atomic.Bool
	blocking.Store(true)
	restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
		if stream == mqttQoS2IncomingMsgsStreamName && blocking.Load() {
			return fmt.Errorf("terminal cleanup blocked by test")
		}
		return nil
	})
	t.Cleanup(restore)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("once"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
	}
	b.consumeOne(t, "once")

	// The tombstone is there because its cleanup was blocked. That is legitimate
	// (invariant 4: terminal cleanup may be asynchronous and may fail).
	checkFor(t, testMQTTTimeout, 20*time.Millisecond, func() error {
		rec, _, err := b.jsa.loadQoS2Record(subject)
		if err != nil {
			return err
		}
		if rec == nil || rec.state != mqttQoS2Done {
			return fmt.Errorf("expected a terminal record, got %v", rec)
		}
		return nil
	})

	// The retry a client sends when its PUBCOMP was lost.
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x from the terminal record, got 0x%x", mqttReasonSuccess, reason)
	}
	// No second delivery — not to the QoS 2 subscriber and not to anyone.
	testMQTTExpectNothing(t, b.rs)

	// With cleanup unblocked, a further retry reaps the tombstone.
	blocking.Store(false)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
	}
	testQoS2NoRecord(t, b.jsa, subject)
	testMQTTExpectNothing(t, b.rs)
}

// A leftover tombstone must NOT make a fresh PUBLISH on that identifier look like
// a duplicate. This is the loss path the two-state record introduces, and the
// single most important safety property of the PUBLISH side: answering PUBREC for
// a payload that was never stored means the following PUBREL finds a tombstone,
// answers 0x00, and the payload is gone.
func TestQoS2NewPublishReapsLeftoverDoneRecord(t *testing.T) {
	b := testQoS2Setup(t, "leftover")
	pi := uint16(24)
	subject := b.holdSubject(t, pi)

	for _, tc := range []struct {
		name string
		gen  string
	}{
		{"fresh generation", nuid.Next()},
		{"another generation", nuid.Next()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Seed the tombstone directly, at whatever sequence the subject is at.
			_, seq, err := b.jsa.loadQoS2Record(subject)
			if err != nil {
				t.Fatalf("reading the hold subject: %v", err)
			}
			if _, err := b.jsa.storeQoS2DoneCAS(subject, tc.gen, seq); err != nil {
				t.Fatalf("seeding the terminal record: %v", err)
			}

			payload := "after-" + tc.name
			testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte(payload))
			testMQTTReadPubRecV5(t, b.rp, pi)

			// The tombstone was reaped and OUR payload is what is held now.
			rec, _, err := b.jsa.loadQoS2Record(subject)
			if err != nil || rec == nil {
				t.Fatalf("Expected a held record after the fresh PUBLISH: rec=%v err=%v", rec, err)
			}
			if rec.state != mqttQoS2Held {
				t.Fatalf("Record state = %v, want held (the tombstone was misread as a duplicate)", rec.state)
			}
			if rec.gen == tc.gen {
				t.Fatal("The held record still carries the tombstone's generation")
			}
			if !bytes.Contains(rec.stored.Data, []byte(payload)) {
				t.Fatalf("Held payload = %q, want it to contain %q", rec.stored.Data, payload)
			}

			testQoS2SendPubRel(t, b.cp, pi)
			if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
				t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
			}
			b.consumeOne(t, payload)
			testMQTTExpectNothing(t, b.rs)
			testQoS2NoRecord(t, b.jsa, subject)
		})
	}
}

// A corrupt record on an unused identifier must not swallow a fresh PUBLISH.
// Classifying it as held would be the worst available outcome: PUBREC for a
// payload never stored, then a PUBREL that releases the OLD record and reports
// 0x92 while the fresh payload is lost.
func TestQoS2CorruptRecordDoesNotSwallowFreshPublish(t *testing.T) {
	b := testQoS2Setup(t, "corruptpub")
	pi := uint16(25)
	subject := b.holdSubject(t, pi)

	seedCorrupt := func(t *testing.T) {
		t.Helper()
		_, seq, err := b.jsa.loadQoS2Record(subject)
		if err != nil {
			t.Fatalf("reading the hold subject: %v", err)
		}
		if _, err := b.jsa.storeQoS2HoldCAS(subject, seq, len(hdrLine)+2,
			[]byte(hdrLine+CR_LF+"garbage-with-no-generation")); err != nil {
			t.Fatalf("seeding the corrupt record: %v", err)
		}
		rec, _, err := b.jsa.loadQoS2Record(subject)
		if err != nil || rec == nil || rec.state != mqttQoS2Corrupt {
			t.Fatalf("Expected a corrupt record, got rec=%v err=%v", rec, err)
		}
	}

	t.Run("reaped and superseded by the fresh payload", func(t *testing.T) {
		seedCorrupt(t)
		testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("fresh"))
		testMQTTReadPubRecV5(t, b.rp, pi)

		rec, _, err := b.jsa.loadQoS2Record(subject)
		if err != nil || rec == nil || rec.state != mqttQoS2Held {
			t.Fatalf("Expected our payload to be held after the reap, got rec=%v err=%v", rec, err)
		}
		if !bytes.Contains(rec.stored.Data, []byte("fresh")) {
			t.Fatalf("Held payload = %q, want it to contain %q", rec.stored.Data, "fresh")
		}
		testQoS2SendPubRel(t, b.cp, pi)
		if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
			t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
		}
		b.consumeOne(t, "fresh")
		testMQTTExpectNothing(t, b.rs)
	})

	t.Run("a failed reap withholds PUBREC entirely", func(t *testing.T) {
		seedCorrupt(t)
		restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
			if stream == mqttQoS2IncomingMsgsStreamName {
				return fmt.Errorf("reap blocked by test")
			}
			return nil
		})
		restoreOnce := sync.OnceFunc(restore)
		t.Cleanup(restoreOnce)

		testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("withheld"))
		// No PUBREC: the identifier's state is still unusable, so the client must not
		// be told its payload is held (invariant 3, one packet earlier). A v5 broker
		// announces the close with a DISCONNECT, which is not a PUBREC and is fine.
		for done := false; !done; {
			bt, _, ok := testMQTTReadPacketReady(b.rp, testMQTTTimeout)
			if !ok {
				break
			}
			switch pt := bt & mqttPacketMask; pt {
			case mqttPacketPubRec:
				t.Fatal("Expected no PUBREC while the reap fails, but the payload was reported as held")
			case mqttPacketDisconnect:
				done = true
			default:
				t.Fatalf("Unexpected packet type %v while expecting no PUBREC", pt)
			}
		}
		testQoS2ExpectClientGone(t, b.s, b.pubID)
		restoreOnce()
	})
}

// The corrupt PUBREL branch: a record we cannot identify is disposed of and
// answered 0x92, with the connection kept up. Seeded garbage carries no
// generation, so this exercises the corrupt branch — NOT the malformed-publish-
// headers branch, which presupposes a valid generation and is covered separately
// below.
func TestQoS2PubRelCorruptRecordReports92(t *testing.T) {
	b := testQoS2Setup(t, "corruptrel")
	pi := uint16(26)
	subject := b.holdSubject(t, pi)

	if _, err := b.jsa.storeQoS2HoldCAS(subject, 0, len(hdrLine)+2,
		[]byte(hdrLine+CR_LF+"garbage")); err != nil {
		t.Fatalf("seeding the corrupt record: %v", err)
	}

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected reason 0x%x for an unusable record, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}
	testMQTTExpectNothing(t, b.rs)
	testQoS2NoRecord(t, b.jsa, subject)
	// The connection stays up.
	testMQTTFlush(t, b.cp, nil, b.rp)
}

// The malformed-publish-headers branch proper: a record with a VALID generation
// whose Nmqtt-Pub/Nmqtt-Subject headers are unusable. It is provably ours, so it
// is released and answered 0x92 — but it is not corrupt, and conflating the two
// would leave this branch untested (which is what happened in an earlier draft).
func TestQoS2PubRelMalformedPublishHeaderReports92(t *testing.T) {
	b := testQoS2Setup(t, "malformed")
	pi := uint16(27)
	subject := b.holdSubject(t, pi)

	hdr := hdrLine + mqttQoS2GenHeader + ":" + nuid.Next() + CR_LF + CR_LF
	if _, err := b.jsa.storeQoS2HoldCAS(subject, 0, len(hdr), []byte(hdr+"body")); err != nil {
		t.Fatalf("seeding the malformed record: %v", err)
	}
	// Precondition: this really is the malformed branch, not the corrupt one.
	rec, _, err := b.jsa.loadQoS2Record(subject)
	if err != nil || rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("Expected a held record with a valid generation, got rec=%v err=%v", rec, err)
	}

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected reason 0x%x for a malformed record, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}
	testMQTTExpectNothing(t, b.rs)
	testQoS2NoRecord(t, b.jsa, subject)
	testMQTTFlush(t, b.cp, nil, b.rp)
}

// A failed NONterminal release must withhold the PUBCOMP: sending it would free
// the packet identifier while a held record survives, and the client's next
// PUBLISH on that identifier would then be matched against the stale record and
// lost. Invariant 3. Driven through the expiry branch, and mirrored for the
// malformed branch.
func TestQoS2ExpiredReleaseFailureBlocksPIReuse(t *testing.T) {
	for _, tc := range []struct {
		name string
		// seed puts the broker in the state whose release is about to fail.
		seed func(t *testing.T, b *testQoS2Broker, pi uint16, subject string)
		// wantReason is what the eventual successful release answers. The two
		// branches differ deliberately and must not be conflated: an expired message
		// was accepted and then aged out, which is a COMPLETED exchange (0x00), while
		// a record we cannot deliver leaves the identifier with no usable state
		// (0x92). Only the cleanup discipline under test is common to both.
		wantReason byte
	}{
		{
			name:       "expired",
			wantReason: mqttReasonSuccess,
			seed: func(t *testing.T, b *testQoS2Broker, pi uint16, subject string) {
				// Message Expiry Interval of 1s, then wait it out: the PUBREL finds an
				// expired hold and must release it. mqttV5PropsWithExpiry returns a
				// complete properties BLOCK (length varint included), so it is written
				// into the variable header directly rather than through a helper that
				// would prefix a second length.
				vh := newMQTTWriter(0)
				vh.WriteBytes([]byte(b.topic))
				vh.WriteUint16(pi)
				vh.Write(mqttV5PropsWithExpiry(1))
				vh.Write([]byte("expired"))
				w := newMQTTWriter(0)
				w.WriteByte(mqttPacketPub | (2 << 1))
				w.WriteVarInt(vh.Len())
				w.Write(vh.Bytes())
				if _, err := testMQTTWrite(b.cp, w.Bytes()); err != nil {
					t.Fatalf("Error writing PUBLISH: %v", err)
				}
				testMQTTReadPubRecV5(t, b.rp, pi)
				time.Sleep(1500 * time.Millisecond)
			},
		},
		{
			name:       "malformed",
			wantReason: mqttReasonPacketIDNotFound,
			seed: func(t *testing.T, b *testQoS2Broker, pi uint16, subject string) {
				hdr := hdrLine + mqttQoS2GenHeader + ":" + nuid.Next() + CR_LF + CR_LF
				if _, err := b.jsa.storeQoS2HoldCAS(subject, 0, len(hdr), []byte(hdr+"body")); err != nil {
					t.Fatalf("seeding the malformed record: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQoS2Setup(t, "relfail-"+tc.name)
			pi := uint16(28)
			subject := b.holdSubject(t, pi)
			tc.seed(t, b, pi, subject)

			restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
				if stream == mqttQoS2IncomingMsgsStreamName {
					return fmt.Errorf("release blocked by test")
				}
				return nil
			})
			restoreOnce := sync.OnceFunc(restore)
			t.Cleanup(restoreOnce)

			testQoS2SendPubRel(t, b.cp, pi)
			b.expectNoPubComp(t)
			testQoS2ExpectClientGone(t, b.s, b.pubID)
			testMQTTExpectNothing(t, b.rs)

			// The record survived, so nothing was silently freed.
			restoreOnce()
			if rec, _, err := b.jsa.loadQoS2Record(subject); err != nil || rec == nil {
				t.Fatalf("Expected the record to survive the failed release: rec=%v err=%v", rec, err)
			}

			// Retry with the fault cleared: the release completes, and the identifier is
			// usable again for a genuinely new message.
			cp2, rp2 := b.reconnectPublisher(t)

			testQoS2SendPubRel(t, cp2, pi)
			if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != tc.wantReason {
				t.Fatalf("Expected reason 0x%x once the release completes, got 0x%x", tc.wantReason, reason)
			}
			testQoS2NoRecord(t, b.jsa, subject)

			testMQTTPublishV5(t, cp2, 2, pi, b.topic, []byte("reused"))
			testMQTTReadPubRecV5(t, rp2, pi)
			testQoS2SendPubRel(t, cp2, pi)
			if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != mqttReasonSuccess {
				t.Fatalf("Expected the reused identifier's exchange to succeed, got 0x%x", reason)
			}
			b.consumeOne(t, "reused")
			testMQTTExpectNothing(t, b.rs)
		})
	}
}

// A failed onward store must suppress the PUBCOMP: reporting success would tell
// the client an exactly-once exchange completed for a message that exists
// nowhere. The record must still be held so the retry can complete.
//
// The assertion is deliberately NOT "nothing was delivered": the live core-NATS
// fan-out happens inside the delivery call, BEFORE the stream store, so a QoS 0
// subscriber legitimately sees the message. What must not happen is a delivery to
// a stream-backed (QoS 1/2) subscriber, whose copy comes from the store.
func TestQoS2PubRelStoreFailureSuppressesPubComp(t *testing.T) {
	b := testQoS2Setup(t, "storefail")
	pi := uint16(29)
	subject := b.holdSubject(t, pi)

	// A QoS 0 subscriber, which reads the live fan-out rather than the stream.
	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: "storefail-q0", cleanSess: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c0.Close()
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c0, r0, []*mqttFilter{{filter: b.topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, c0, nil, r0)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("stalled"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	restore := setMQTTTestStoreMsgHook(func(gotSubject string, _ int, _ []byte) error {
		if strings.HasPrefix(gotSubject, mqttStreamSubjectPrefix) {
			return fmt.Errorf("onward store blocked by test")
		}
		return nil
	})
	restoreOnce := sync.OnceFunc(restore)
	t.Cleanup(restoreOnce)

	testQoS2SendPubRel(t, b.cp, pi)
	b.expectNoPubComp(t)
	testQoS2ExpectClientGone(t, b.s, b.pubID)

	// The stream-backed subscriber got nothing: its copy comes from the store that
	// failed.
	testMQTTExpectNothing(t, b.rs)
	// The QoS 0 subscriber DID see it — the sanctioned live-fan-out duplicate. This
	// is asserted rather than ignored so the residual stays honest.
	testMQTTCheckPubMsgNoAck(t, c0, r0, b.topic, 0, []byte("stalled"))

	restoreOnce()
	if rec, _, err := b.jsa.loadQoS2Record(subject); err != nil || rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("Expected the record to still be held after the store failure: rec=%v err=%v", rec, err)
	}

	// The retry completes and the QoS 2 subscriber gets exactly one copy.
	cp2, rp2 := b.reconnectPublisher(t)
	testQoS2SendPubRel(t, cp2, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the retry to succeed, got 0x%x", reason)
	}
	b.consumeOne(t, "stalled")
	testMQTTExpectNothing(t, b.rs)
	// The QoS 0 subscriber saw the second fan-out: residual 1, made visible.
	testMQTTCheckPubMsgNoAck(t, c0, r0, b.topic, 0, []byte("stalled"))
}

// Delivery happened but the terminal commit did not: the PUBCOMP is suppressed,
// and the retry re-delivers. The stored copy is suppressed by the stable
// Nats-Msg-Id, so the QoS 2 subscriber still receives exactly one message while a
// QoS 0 subscriber receives two. This test IS residuals 1 and 2.
func TestQoS2PubRelFinalizeFailureRedeliversOnce(t *testing.T) {
	b := testQoS2Setup(t, "finfail")
	pi := uint16(30)

	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: "finfail-q0", cleanSess: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c0.Close()
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c0, r0, []*mqttFilter{{filter: b.topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, c0, nil, r0)

	var failing atomic.Bool
	failing.Store(true)
	b.asm.mu.Lock()
	b.asm.testQoS2FinalizeStore = func(subject, gen string, expectSeq uint64) (uint64, error) {
		if failing.Load() {
			return 0, fmt.Errorf("terminal commit blocked by test")
		}
		return b.asm.jsa.storeQoS2DoneCAS(subject, gen, expectSeq)
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQoS2FinalizeStore = nil
		b.asm.mu.Unlock()
	})

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("dup"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	b.expectNoPubComp(t)
	testQoS2ExpectClientGone(t, b.s, b.pubID)

	failing.Store(false)
	cp2, rp2 := b.reconnectPublisher(t)
	testQoS2SendPubRel(t, cp2, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the retry to succeed, got 0x%x", reason)
	}

	// Exactly one durable copy, despite two onward stores: the msgId deduplicated
	// the second (residual 2, within the window).
	b.consumeOne(t, "dup")
	testMQTTExpectNothing(t, b.rs)
	// Two live fan-outs: the irreducible duplicate (residual 1).
	testMQTTCheckPubMsgNoAck(t, c0, r0, b.topic, 0, []byte("dup"))
	testMQTTCheckPubMsgNoAck(t, c0, r0, b.topic, 0, []byte("dup"))
}

// The deduplication key must be stable across every PUBREL retry of one hold
// generation — that is what makes the retry non-duplicating — and must differ for
// a new generation on the same packet identifier, or a genuinely new message would
// be suppressed as a duplicate of the old one.
func TestQoS2MsgIdStableAcrossRetries(t *testing.T) {
	b := testQoS2Setup(t, "msgid")
	pi := uint16(31)

	var mu sync.Mutex
	var seen []string
	restore := setMQTTTestStoreMsgHook(func(subject string, hdrLen int, msg []byte) error {
		if !strings.HasPrefix(subject, mqttStreamSubjectPrefix) {
			return nil
		}
		mu.Lock()
		seen = append(seen, string(getHeader(JSMsgId, msg[:hdrLen])))
		mu.Unlock()
		return nil
	})
	t.Cleanup(restore)

	// Two PUBREL attempts on one generation: fail the first commit so the client
	// retries the same held record.
	var failing atomic.Bool
	failing.Store(true)
	b.asm.mu.Lock()
	b.asm.testQoS2FinalizeStore = func(subject, gen string, expectSeq uint64) (uint64, error) {
		if failing.Load() {
			return 0, fmt.Errorf("terminal commit blocked by test")
		}
		return b.asm.jsa.storeQoS2DoneCAS(subject, gen, expectSeq)
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQoS2FinalizeStore = nil
		b.asm.mu.Unlock()
	})

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("m1"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	b.expectNoPubComp(t)
	testQoS2ExpectClientGone(t, b.s, b.pubID)

	failing.Store(false)
	cp2, rp2 := b.reconnectPublisher(t)
	testQoS2SendPubRel(t, cp2, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the retry to succeed, got 0x%x", reason)
	}
	b.consumeOne(t, "m1")

	// A genuinely new message on the SAME identifier.
	testMQTTPublishV5(t, cp2, 2, pi, b.topic, []byte("m2"))
	testMQTTReadPubRecV5(t, rp2, pi)
	testQoS2SendPubRel(t, cp2, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the second exchange to succeed, got 0x%x", reason)
	}
	b.consumeOne(t, "m2")

	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 3 {
		t.Fatalf("Expected at least 3 onward stores (two attempts plus a new message), got %d: %q", len(seen), seen)
	}
	for i, id := range seen {
		if !strings.HasPrefix(id, mqttQoS2ReleaseMsgIdPrefix) {
			t.Fatalf("store %d carried msgId %q, want the %q prefix", i, id, mqttQoS2ReleaseMsgIdPrefix)
		}
	}
	if seen[0] != seen[1] {
		t.Fatalf("The two attempts on one generation used different msgIds (%q vs %q): the retry would duplicate", seen[0], seen[1])
	}
	if seen[len(seen)-1] == seen[0] {
		t.Fatal("A new generation reused the previous msgId: a genuinely new message would be suppressed as a duplicate")
	}
}

// A DUP PUBLISH reaches PUBREC through the CAS-rejection + re-read path, and the
// message is delivered exactly once.
func TestQoS2DupPublishReadsHeldRecord(t *testing.T) {
	b := testQoS2Setup(t, "duppath")
	pi := uint16(32)
	subject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("only"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	rec1, _, err := b.jsa.loadQoS2Record(subject)
	if err != nil || rec1 == nil {
		t.Fatalf("Expected a held record: rec=%v err=%v", rec1, err)
	}

	testMQTTPublishV5Dup(t, b.cp, 2, pi, b.topic, []byte("only"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	// The duplicate did NOT replace the record: same generation, same sequence.
	rec2, _, err := b.jsa.loadQoS2Record(subject)
	if err != nil || rec2 == nil {
		t.Fatalf("Expected the held record to survive the duplicate: rec=%v err=%v", rec2, err)
	}
	if rec2.gen != rec1.gen {
		t.Fatalf("The duplicate replaced the held record (generation %q -> %q)", rec1.gen, rec2.gen)
	}

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
	}
	b.consumeOne(t, "only")
	testMQTTExpectNothing(t, b.rs)
}

// The max-payload preflight must budget each of the three record forms, and a
// PUBLISH that would exceed one of them must be refused at PUBREC with 0x97
// rather than accepted and then wedged. A wedge is worse than the loss this
// protocol fixes: the client retries an identical packet forever.
func TestQoS2FormsRespectMaxPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 512)
	subj, err := mqttTopicToNATSPubSubject([]byte("mp"))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	pp := &mqttPublish{topic: []byte("mp"), subject: subj, msg: payload, sz: len(payload)}
	origin := mqttOriginMarker([]byte("k"), "id", "subj", []byte("p"))

	release := mqttComputeNatsMsgSize(pp, false, 0, origin, false, _EMPTY_, _EMPTY_) + mqttQoS2MsgIdHeaderMax
	hold := mqttComputeNatsMsgSize(pp, true, 0, _EMPTY_, false, _EMPTY_, _EMPTY_) +
		mqttQoS2GenHeaderMax + mqttQoS2HoldCASHeaderMax
	terminal := mqttQoS2TerminalRecordMax()

	largest := release
	for _, f := range []int{hold, terminal} {
		if f > largest {
			largest = f
		}
	}

	// At exactly the largest form the exchange must COMPLETE. One byte below it,
	// the PUBLISH must be refused at PUBREC with no backend write attempted.
	t.Run("accepted at the budgeted size", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		o.MaxPayload = int32(largest)
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "mp-ok", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTReadConnAckV5(t, r)
		pi := uint16(41)
		testMQTTPublishV5(t, c, 2, pi, "mp", payload)
		if reason, _ := testMQTTReadPubRecV5(t, r, pi); reason != mqttReasonSuccess {
			t.Fatalf("Expected PUBREC 0x%x at the budgeted size, got 0x%x", mqttReasonSuccess, reason)
		}
		testQoS2SendPubRel(t, c, pi)
		if reason, _ := testMQTTReadPubCompV5(t, r, pi); reason != mqttReasonSuccess {
			t.Fatalf("Expected the exchange to complete, got 0x%x", reason)
		}
	})

	t.Run("refused one byte below, with no backend write", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		o.MaxPayload = int32(largest - 1)
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		var wrote atomic.Bool
		restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
			if strings.HasPrefix(subject, mqttQoS2IncomingMsgsStreamSubjectPrefix) ||
				strings.HasPrefix(subject, mqttStreamSubjectPrefix) {
				wrote.Store(true)
			}
			return nil
		})
		defer restore()

		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "mp-rej", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTReadConnAckV5(t, r)
		pi := uint16(42)
		testMQTTPublishV5(t, c, 2, pi, "mp", payload)
		if reason, _ := testMQTTReadPubRecV5(t, r, pi); reason != mqttReasonQuotaExceeded {
			t.Fatalf("Expected PUBREC 0x%x one byte below the budget, got 0x%x", mqttReasonQuotaExceeded, reason)
		}
		if wrote.Load() {
			t.Fatal("A rejected PUBLISH must not write to either internal stream")
		}
		// Non-fatal: the connection stays up.
		testMQTTFlush(t, c, nil, r)
	})

	// The terminal form is the one an implementation is most likely to forget: its
	// body is empty, so for a small payload with a short topic it is the LARGEST of
	// the three, and its failure repeats the onward delivery on every retry.
	t.Run("terminal form is budgeted even when it dominates", func(t *testing.T) {
		tiny := &mqttPublish{topic: []byte("t"), subject: []byte("t"), msg: []byte("y"), sz: 1}
		tinyRelease := mqttComputeNatsMsgSize(tiny, false, 0, _EMPTY_, false, _EMPTY_, _EMPTY_) + mqttQoS2MsgIdHeaderMax
		tinyHold := mqttComputeNatsMsgSize(tiny, true, 0, _EMPTY_, false, _EMPTY_, _EMPTY_) +
			mqttQoS2GenHeaderMax + mqttQoS2HoldCASHeaderMax
		if terminal <= tinyRelease || terminal <= tinyHold {
			t.Skipf("terminal form (%d) does not dominate for a tiny publish (release %d, hold %d); "+
				"the ordering is header-size dependent and this case is then covered by the rows above",
				terminal, tinyRelease, tinyHold)
		}
		o := testMQTTDefaultOptionsV5()
		o.MaxPayload = int32(terminal - 1)
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		var wrote atomic.Bool
		restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
			if strings.HasPrefix(subject, mqttQoS2IncomingMsgsStreamSubjectPrefix) ||
				strings.HasPrefix(subject, mqttStreamSubjectPrefix) {
				wrote.Store(true)
			}
			return nil
		})
		defer restore()

		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "mp-term", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTReadConnAckV5(t, r)
		pi := uint16(43)
		testMQTTPublishV5(t, c, 2, pi, "t", []byte("y"))
		if reason, _ := testMQTTReadPubRecV5(t, r, pi); reason != mqttReasonQuotaExceeded {
			t.Fatalf("Expected PUBREC 0x%x when only the terminal form exceeds the budget, got 0x%x",
				mqttReasonQuotaExceeded, reason)
		}
		if wrote.Load() {
			t.Fatal("A PUBLISH rejected on the terminal form's account must not be stored")
		}
		testMQTTFlush(t, c, nil, r)
	})
}

// $MQTT_msgs must have a deduplication window: the stable Nats-Msg-Id is inert
// without one, and the window here is an inherited nats-server default (a version
// bump could change it), so this is the only thing that would notice.
func TestQoS2MsgsStreamHasDuplicateWindow(t *testing.T) {
	b := testQoS2Setup(t, "dupwin")
	si, err := b.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookup %s: si=%v err=%v", mqttStreamName, si, err)
	}
	if si.Config.Duplicates <= 0 {
		t.Fatalf("%s has no duplicate window (%v); the QoS 2 release msgId would be inert",
			mqttStreamName, si.Config.Duplicates)
	}
}

// mqttTestTopicSubject converts an MQTT topic to its NATS publish subject, failing
// the test rather than returning an error.
func mqttTestTopicSubject(t *testing.T, topic string) []byte {
	t.Helper()
	subj, err := mqttTopicToNATSPubSubject([]byte(topic))
	if err != nil {
		t.Fatalf("subject conversion for %q: %v", topic, err)
	}
	return subj
}
