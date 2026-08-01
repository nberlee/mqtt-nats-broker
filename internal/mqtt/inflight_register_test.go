package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// Tests for the outbound in-flight register (inflight_register.go): the durable
// record that makes an outbound QoS 1/2 packet identifier survive a broker restart
// or a failover, and the resume barrier that reads it back before any source
// interest exists.

// ---------------------------------------------------------------------------
// Naming, identity and codec
// ---------------------------------------------------------------------------

func TestMQTTInflightSubjectNaming(t *testing.T) {
	idHash, gen := getHash("client-a"), mqttNewSessGen()

	subject := mqttInflightSubject(idHash, gen, 1234)
	want := mqttInflightStreamSubjectPrefix + idHash + "." + gen + ".1234"
	if subject != want {
		t.Fatalf("subject %q, want %q", subject, want)
	}

	// Exactly one token past the generation, so the walk cannot reach another
	// incarnation's registers and a single request always returns the whole set.
	filter := mqttInflightWalkFilter(idHash, gen)
	if wantF := mqttInflightStreamSubjectPrefix + idHash + "." + gen + ".*"; filter != wantF {
		t.Fatalf("walk filter %q, want %q", filter, wantF)
	}
	if strings.Count(filter, ".*") != 1 || strings.Contains(filter, ">") {
		t.Fatalf("walk filter %q must be a single-token wildcard", filter)
	}

	n, err := mqttParseInflightSubject(subject)
	if err != nil {
		t.Fatalf("parse %q: %v", subject, err)
	}
	if n.idHash != idHash || n.gen != gen || n.pi != 1234 || n.subject != subject {
		t.Fatalf("parsed %+v, want (%q, %q, 1234, %q)", n, idHash, gen, subject)
	}
}

func TestMQTTInflightSubjectParseRejects(t *testing.T) {
	idHash, gen := getHash("client-a"), mqttNewSessGen()
	base := mqttInflightStreamSubjectPrefix

	for _, tc := range []struct {
		name    string
		subject string
	}{
		{"foreign prefix", mqttQoS2IncomingMsgsStreamSubjectPrefix + idHash + "." + gen + ".1"},
		{"too few tokens", base + idHash + "." + gen},
		{"too many tokens", base + idHash + "." + gen + ".1.rel"},
		{"malformed hash", base + "short." + gen + ".1"},
		{"malformed generation", base + idHash + ".notageneration.1"},
		{"packet identifier zero", base + idHash + "." + gen + ".0"},
		// Canonical decimal only: mqttInflightSubject formats with FormatUint, so a
		// leading zero is a subject this broker can never have written.
		{"non-canonical packet identifier", base + idHash + "." + gen + ".01"},
		{"packet identifier out of range", base + idHash + "." + gen + ".65536"},
		{"packet identifier not a number", base + idHash + "." + gen + ".rel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if n, err := mqttParseInflightSubject(tc.subject); err == nil {
				t.Fatalf("expected %q to be rejected, got %+v", tc.subject, n)
			}
		})
	}
}

func TestMQTTInflightRecordIdentity(t *testing.T) {
	ok := func() *mqttInflightRecord {
		return &mqttInflightRecord{PI: 7, Dur: "hash_nuid", Seq: 42, QoS: 2, Nonce: mqttNewInflightNonce()}
	}
	if !mqttInflightRecordIdentityOK(ok()) {
		t.Fatalf("a complete record must be usable")
	}
	if mqttInflightRecordIdentityOK(nil) {
		t.Fatalf("nil must not be usable")
	}
	// Every field is load-bearing, so a record missing any of them is unreadable
	// rather than partially usable.
	for _, tc := range []struct {
		name  string
		mutry func(*mqttInflightRecord)
	}{
		{"no packet identifier", func(r *mqttInflightRecord) { r.PI = 0 }},
		{"no source durable", func(r *mqttInflightRecord) { r.Dur = _EMPTY_ }},
		{"no source sequence", func(r *mqttInflightRecord) { r.Seq = 0 }},
		{"QoS 0", func(r *mqttInflightRecord) { r.QoS = 0 }},
		{"QoS 3", func(r *mqttInflightRecord) { r.QoS = 3 }},
		{"no nonce", func(r *mqttInflightRecord) { r.Nonce = _EMPTY_ }},
		{"short nonce", func(r *mqttInflightRecord) { r.Nonce = "abc" }},
		{"nonce out of alphabet", func(r *mqttInflightRecord) { r.Nonce = strings.Repeat("!", mqttInflightNonceLen) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ok()
			tc.mutry(r)
			if mqttInflightRecordIdentityOK(r) {
				t.Fatalf("%+v must not be usable", r)
			}
		})
	}
}

func TestMQTTInflightRecordCodec(t *testing.T) {
	rec := &mqttInflightRecord{
		PI: 9, Dur: "hash_nuid", Seq: 1234, QoS: 1,
		Nonce: mqttNewInflightNonce(), Q2AffClaim: "$MQTT.qos2.assign.G.7",
	}
	msg, hdrLen := buildInflightRecord(0, rec)
	hdr := string(msg[:hdrLen])
	if !strings.Contains(hdr, JSExpectedLastSubjSeq+":0") {
		t.Fatalf("header %q must carry a create-only expectation", hdr)
	}
	got := &mqttInflightRecord{}
	if err := json.Unmarshal(msg[hdrLen:], got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if *got != *rec {
		t.Fatalf("round trip gave %+v, want %+v", got, rec)
	}

	// A non-zero expectation names an exact sequence to replace.
	msg, hdrLen = buildInflightRecord(77, rec)
	if hdr = string(msg[:hdrLen]); !strings.Contains(hdr, JSExpectedLastSubjSeq+":77") {
		t.Fatalf("header %q must carry the expected sequence", hdr)
	}
}

// The source stream is DERIVED from the durable's prefix rather than stored, so an
// affine copy's $MQTT_out sequence is never checked against $MQTT_msgs.
func TestMQTTInflightSourceStream(t *testing.T) {
	idHash, gen := getHash("client-a"), mqttNewQ2AffGen()
	_, _, affDur := mqttQ2AffineNames(idHash, gen)
	if got := mqttInflightSourceStream(affDur); got != mqttOutStreamName {
		t.Fatalf("affine durable %q resolved to %q, want %q", affDur, got, mqttOutStreamName)
	}
	if got := mqttInflightSourceStream(idHash + "_" + nuid.Next()); got != mqttStreamName {
		t.Fatalf("plain durable resolved to %q, want %q", got, mqttStreamName)
	}
}

// ---------------------------------------------------------------------------
// Stream prerequisites
// ---------------------------------------------------------------------------

func TestMQTTValidateInflightStream(t *testing.T) {
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:       mqttInflightStreamName,
			Subjects:   []string{mqttInflightStreamSubjectPrefix + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
		}}
	}
	if err := validateInflightStream(good(), 0); err != nil {
		t.Fatalf("the shipped configuration must validate: %v", err)
	}
	if err := validateInflightStream(nil, 0); err == nil {
		t.Fatalf("missing stream info must be rejected")
	}
	// Replicas are only checked when the deployment asked for a specific count.
	if err := validateInflightStream(good(), 3); err == nil {
		t.Fatalf("a replica mismatch must be rejected when one is wanted")
	}
	si := good()
	si.Config.Replicas = 3
	if err := validateInflightStream(si, 3); err != nil {
		t.Fatalf("matching replicas must validate: %v", err)
	}

	for _, tc := range []struct {
		name string
		mut  func(*StreamConfig)
	}{
		{"wrong subjects", func(c *StreamConfig) { c.Subjects = []string{"foo.>"} }},
		{"extra subjects", func(c *StreamConfig) { c.Subjects = append(c.Subjects, "foo.>") }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"interest retention", func(c *StreamConfig) { c.Retention = InterestPolicy }},
		{"discard new", func(c *StreamConfig) { c.Discard = DiscardNew }},
		{"no per-subject singleton", func(c *StreamConfig) { c.MaxMsgsPer = 0 }},
		{"per-subject limit above one", func(c *StreamConfig) { c.MaxMsgsPer = 2 }},
		{"max msgs", func(c *StreamConfig) { c.MaxMsgs = 10 }},
		{"max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "other"} }},
		{"sources", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "other"}} }},
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		{"deny delete", func(c *StreamConfig) { c.DenyDelete = true }},
		{"deny purge", func(c *StreamConfig) { c.DenyPurge = true }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) {
			c.SubjectTransform = &SubjectTransformConfig{Source: ">", Destination: "x.>"}
		}},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: ">", Destination: "pub.>"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"message counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateInflightStream(si, 0); err == nil {
				t.Fatalf("%s must fail validation", tc.name)
			}
		})
	}

	// Deliberately NOT a prerequisite: nothing ever creates a consumer here, so a
	// stream-forced consumer inactivity limit cannot affect this owner. Listing a
	// prerequisite the owner does not have would fail startups for no reason.
	si = good()
	si.Config.ConsumerLimits.InactiveThreshold = time.Minute
	if err := validateInflightStream(si, 0); err != nil {
		t.Fatalf("a consumer inactivity limit is irrelevant to a stream with no consumers: %v", err)
	}
}

// ---------------------------------------------------------------------------
// A broker over a real JetStream
// ---------------------------------------------------------------------------

type testInflightEnv struct {
	s    *Server
	asm  *mqttAccountSessionManager
	sess *mqttSession
	c    *client
}

func testNewInflightEnv(t *testing.T, clientID string) *testInflightEnv {
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

	ic := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(ic)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	sess, _, err := asm.createOrRestoreSession(clientID, s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	sess.maxp = 100
	return &testInflightEnv{s: s, asm: asm, sess: sess, c: ic}
}

// ackSubject builds a JetStream ack reply subject for a (durable, sequence) with a
// given delivery count, the shape trackPublish parses.
func testInflightAckSubject(dur string, sseq uint64, deliveries int) string {
	return fmt.Sprintf("$JS.ACK.%s.%s.%d.%d.%d.0.0", mqttStreamName, dur, deliveries, sseq, sseq)
}

// track allocates an identifier the way a delivery callback does, then persists its
// register, and returns the identifier.
func (e *testInflightEnv) track(t *testing.T, dur string, sseq uint64, qos byte) uint16 {
	t.Helper()
	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish(%s/%d): pi=%d isNew=%v", dur, sseq, pi, isNew)
	}
	if !e.sess.storeInflightRegister(e.c, pi, qos, dur, sseq, _EMPTY_) {
		t.Fatalf("storeInflightRegister(%d, %s/%d) refused", pi, dur, sseq)
	}
	return pi
}

func (e *testInflightEnv) loadRegister(t *testing.T, pi uint16) (*mqttInflightRecord, *StoredMsg) {
	t.Helper()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	rec, smsg, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil {
		t.Fatalf("load register for pi %d: %v", pi, err)
	}
	return rec, smsg
}

// countRegisters returns how many registers this incarnation currently has.
func (e *testInflightEnv) countRegisters(t *testing.T) int {
	t.Helper()
	filter := mqttInflightWalkFilter(e.sess.idHash, e.sess.gen)
	n := 0
	for seq := uint64(1); ; {
		smsg, err := e.asm.jsa.loadNextMsgFromSeq(mqttInflightStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				t.Fatalf("walk registers: %v", err)
			}
			return n
		}
		seq = smsg.Sequence + 1
		n++
	}
}

// waitRegisterCount waits for this incarnation to hold exactly want registers.
//
// Polling rather than asserting once, because a register delete is deliberately
// ASYNCHRONOUS (R5: a failed delete costs a stale record, never a wedged identifier),
// so the store's own reconcile and every terminal path issue it fire-and-forget.
// Asserting immediately after one would be testing send-queue timing.
func (e *testInflightEnv) waitRegisterCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		if got = e.countRegisters(t); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d register(s) for this incarnation, got %d", want, got)
}

// The happy path: a first transmission records a register naming its source, and the
// pending entry is released for sending.
func TestMQTTInflightStoreConfirms(t *testing.T) {
	e := testNewInflightEnv(t, "client-store")
	dur := e.sess.idHash + "_" + nuid.Next()

	pi := e.track(t, dur, 100, 2)

	rec, smsg := e.loadRegister(t, pi)
	if rec == nil || smsg == nil {
		t.Fatalf("no register was stored for pi %d", pi)
	}
	if rec.PI != pi || rec.Dur != dur || rec.Seq != 100 || rec.QoS != 2 {
		t.Fatalf("register %+v does not describe the delivery", rec)
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if ack == nil || ack.regPending || ack.regSeq != smsg.Sequence || ack.regNonce != rec.Nonce {
		t.Fatalf("pending entry %+v was not released against the stored register at %d", ack, smsg.Sequence)
	}
}

// An ambiguous store — it landed, its response was lost — is resolved by re-reading
// the subject and matching the NONCE. The sequence cannot answer this, because the
// subject is reused by later exchanges on the same identifier.
func TestMQTTInflightStoreAmbiguousResolvedByNonce(t *testing.T) {
	e := testNewInflightEnv(t, "client-ambiguous")
	dur := e.sess.idHash + "_" + nuid.Next()

	var stored int
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		stored++
		// Let the write land, then report a failure the caller cannot classify.
		if _, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, r); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("response lost")
	}
	e.asm.mu.Unlock()

	pi := e.track(t, dur, 200, 1)
	if stored != 1 {
		t.Fatalf("expected exactly one store attempt, got %d", stored)
	}
	rec, smsg := e.loadRegister(t, pi)
	if rec == nil {
		t.Fatalf("the landed register was not adopted")
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if ack == nil || ack.regPending || ack.regSeq != smsg.Sequence || ack.regNonce != rec.Nonce {
		t.Fatalf("pending entry %+v did not adopt the landed register", ack)
	}
}

// A definite store failure sends NOTHING and releases the identifier, which is safe
// precisely because nothing else could have sent under it (R1).
func TestMQTTInflightStoreDefiniteFailureUntracks(t *testing.T) {
	e := testNewInflightEnv(t, "client-failure")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.asm.mu.Lock()
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("store refused")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, nil // the subject is empty: the write did not land
	}
	e.asm.mu.Unlock()

	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 300, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 300, _EMPTY_) {
		t.Fatalf("an unrecordable identifier must not be released for sending")
	}
	e.sess.mu.Lock()
	_, stillTracked := e.sess.pendingPublish[pi]
	sseqToPi := e.sess.cpending[dur]
	e.sess.mu.Unlock()
	if stillTracked {
		t.Fatalf("packet identifier %d must not stay tracked after a definite failure", pi)
	}
	if _, ok := sseqToPi[300]; ok {
		t.Fatalf("the source sequence must not stay mapped after a definite failure")
	}
}

// An identifier occupied by a register for a DIFFERENT, still-live delivery is
// refused rather than reaped: on another broker that record is that delivery's only
// durable identity.
func TestMQTTInflightStoreRefusesLiveForeignRegister(t *testing.T) {
	e := testNewInflightEnv(t, "client-foreign")
	dur := e.sess.idHash + "_" + nuid.Next()

	// A register for another delivery, whose source cannot be classified (no such
	// consumer exists), so it is Unknown rather than Complete.
	e.sess.mu.Lock()
	e.sess.last_pi = 41
	e.sess.mu.Unlock()
	foreign := &mqttInflightRecord{PI: 42, Dur: dur, Seq: 999, QoS: 2, Nonce: mqttNewInflightNonce()}
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, 42), 0, foreign); err != nil {
		t.Fatalf("seed a foreign register: %v", err)
	}

	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 500, 1))
	e.sess.mu.Unlock()
	if pi != 42 || !isNew {
		t.Fatalf("expected the allocation to land on 42, got pi=%d isNew=%v", pi, isNew)
	}
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, 500, _EMPTY_) {
		t.Fatalf("an identifier held by a live foreign register must be refused")
	}
	// The foreign record is untouched: it may be a live delivery's identity.
	rec, _ := e.loadRegister(t, 42)
	if rec == nil || rec.Nonce != foreign.Nonce {
		t.Fatalf("the foreign register was destroyed (%+v)", rec)
	}
}

// A register whose delivery is provably complete is a leftover of a lost delete, and
// its identifier may be reused once it has been reaped at its EXACT sequence.
func TestMQTTInflightStoreReapsProvablyCompleteRegister(t *testing.T) {
	e := testNewInflightEnv(t, "client-stale")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	e.sess.last_pi = 10
	e.sess.mu.Unlock()
	stale := &mqttInflightRecord{PI: 11, Dur: dur, Seq: 7, QoS: 1, Nonce: mqttNewInflightNonce()}
	staleSeq, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, 11), 0, stale)
	if err != nil {
		t.Fatalf("seed a stale register: %v", err)
	}

	// inflightSourceState consults the consumer first and never reads its absence as
	// completion, so the source consumer has to exist for the stale record to be
	// classifiable at all.
	ccfg := &CreateConsumerRequest{Stream: mqttStreamName, Config: ConsumerConfig{
		Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next(),
		AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "unused",
	}}
	if _, err := e.asm.jsa.createDurableConsumer(ccfg); err != nil {
		t.Fatalf("create the source consumer: %v", err)
	}

	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 600, 1))
	e.sess.mu.Unlock()
	if pi != 11 || !isNew {
		t.Fatalf("expected the allocation to land on 11, got pi=%d isNew=%v", pi, isNew)
	}
	// The stale record's source (sequence 7) is absent from an empty stream while its
	// consumer exists, so it is provably complete.
	if !e.sess.storeInflightRegister(e.c, pi, 1, dur, 600, _EMPTY_) {
		t.Fatalf("a stale register must not block its identifier")
	}
	rec, smsg := e.loadRegister(t, 11)
	if rec == nil || rec.Nonce == stale.Nonce {
		t.Fatalf("the stale register was not replaced (%+v)", rec)
	}
	if smsg.Sequence == staleSeq {
		t.Fatalf("the replacement must occupy a new sequence")
	}
	if rec.Seq != 600 {
		t.Fatalf("the register names source sequence %d, want 600", rec.Seq)
	}
}

// Withheld deliveries are never registered (R6): they were never transmitted, so
// there is no identifier to make durable.
func TestMQTTInflightDeferredNeverRegistered(t *testing.T) {
	e := testNewInflightEnv(t, "client-deferred")
	dur := e.sess.idHash + "_" + nuid.Next()
	e.sess.mu.Lock()
	e.sess.maxp = 1
	e.sess.mu.Unlock()

	pi := e.track(t, dur, 700, 1)
	if pi == 0 {
		t.Fatalf("the first delivery must be tracked")
	}
	// At the cap: the second is withheld before an identifier is even allocated.
	e.sess.mu.Lock()
	pi2, _, _, isNew2 := e.sess.trackPublish(dur, testInflightAckSubject(dur, 701, 1))
	deferredLen := len(e.sess.deferred)
	e.sess.mu.Unlock()
	if pi2 != 0 || isNew2 {
		t.Fatalf("a withheld delivery must allocate nothing: pi=%d isNew=%v", pi2, isNew2)
	}
	if deferredLen != 1 {
		t.Fatalf("expected the delivery to be deferred, got %d entries", deferredLen)
	}
	if n := e.countRegisters(t); n != 1 {
		t.Fatalf("expected exactly the transmitted delivery to be registered, got %d", n)
	}
}

// A terminal path deletes the register at its exact sequence.
func TestMQTTInflightTerminalDeletesRegister(t *testing.T) {
	e := testNewInflightEnv(t, "client-terminal")
	dur := e.sess.idHash + "_" + nuid.Next()

	pi := e.track(t, dur, 800, 1)
	e.sess.mu.Lock()
	removed := e.sess.untrackPublishEntry(pi)
	e.sess.mu.Unlock()
	if removed == nil || removed.regSeq == 0 {
		t.Fatalf("the removed entry must carry its register sequence: %+v", removed)
	}
	e.sess.deleteInflightRegisterFor(removed)
	e.waitRegisterCount(t, 0)
}

// A consumer teardown that abandons started deliveries without acking them takes
// their registers with it: otherwise the next resume would reserve identifiers for
// deliveries whose consumer no longer exists.
func TestMQTTInflightConsumerPurgeDeletesRegisters(t *testing.T) {
	e := testNewInflightEnv(t, "client-purge")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.track(t, dur, 900, 1)
	e.track(t, dur, 901, 2)
	if n := e.countRegisters(t); n != 2 {
		t.Fatalf("expected 2 registers, got %d", n)
	}

	e.sess.mu.Lock()
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	e.sess.mu.Unlock()
	if len(regs) != 2 {
		t.Fatalf("expected 2 register sequences to delete, got %d", len(regs))
	}
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)
	e.waitRegisterCount(t, 0)
}

// ---------------------------------------------------------------------------
// The resume barrier
// ---------------------------------------------------------------------------

// forgetInMemory drops the tracking maps the way a broker restart does, leaving only
// durable state behind.
func (e *testInflightEnv) forgetInMemory() {
	e.sess.mu.Lock()
	e.sess.pendingPublish = nil
	e.sess.pendingPubRel = nil
	e.sess.cpending = nil
	e.sess.last_pi = 0
	e.sess.mu.Unlock()
}

// The core guarantee: after a restart the barrier restores the ORIGINAL identifier
// for a delivery still in the PUBLISH phase, and the redelivery reports it started.
func TestMQTTInflightRecoveryRestoresIdentifier(t *testing.T) {
	e := testNewInflightEnv(t, "client-recover")
	dur := e.sess.idHash + "_" + nuid.Next()

	// A real consumer and a real source message, so the source classifies as live.
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	gotPi := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if ack == nil {
		t.Fatalf("identifier %d was not reserved", pi)
	}
	if ack.jsDur != dur || ack.sseq != sseq || ack.regSeq == 0 {
		t.Fatalf("restored entry %+v does not describe the delivery", ack)
	}
	if ack.jsAckSubject != _EMPTY_ {
		t.Fatalf("a restored entry has no per-attempt ack subject until its redelivery arrives")
	}
	if gotPi != pi {
		t.Fatalf("cpending maps the source to %d, want %d", gotPi, pi)
	}

	// The redelivery now reuses it, reports the delivery started, and allocates
	// nothing — which is the whole point.
	e.sess.mu.Lock()
	got, dup, started, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if got != pi || !dup || !started || isNew {
		t.Fatalf("redelivery after recovery: pi=%d (want %d) dup=%v started=%v isNew=%v", got, pi, dup, started, isNew)
	}
}

// publishSource stores a message on $MQTT_msgs and creates a durable consuming it,
// returning the message's stream sequence.
func (e *testInflightEnv) publishSource(t *testing.T, dur string) uint64 {
	t.Helper()
	subject := mqttStreamSubjectPrefix + "foo.bar"
	ccfg := &CreateConsumerRequest{Stream: mqttStreamName, Config: ConsumerConfig{
		Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next(),
		AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: subject,
	}}
	if _, err := e.asm.jsa.createDurableConsumer(ccfg); err != nil {
		t.Fatalf("create the source consumer: %v", err)
	}
	resp, err := e.asm.jsa.storeMsg(subject, -1, []byte("payload"))
	if err != nil {
		t.Fatalf("store the source message: %v", err)
	}
	return resp.Sequence
}

// A register whose source is provably complete is dropped rather than reserved: the
// exchange finished and only the delete was lost.
func TestMQTTInflightRecoveryDropsCompletedSource(t *testing.T) {
	e := testNewInflightEnv(t, "client-complete")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	// Remove the source message: with its consumer still present, absence proves the
	// delivery was acknowledged.
	if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if reserved {
		t.Fatalf("identifier %d must not be reserved for a completed delivery", pi)
	}
	e.waitRegisterCount(t, 0)
}

// A register whose source consumer is GONE keeps its identifier reserved. The
// delivery was lost rather than completed, so the client may still be holding the
// identifier and it must not be handed to another message.
func TestMQTTInflightRecoveryKeepsIdentifierWhenConsumerGone(t *testing.T) {
	e := testNewInflightEnv(t, "client-lost")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	if _, err := e.asm.jsa.deleteConsumer(mqttStreamName, dur, false); err != nil {
		t.Fatalf("delete the source consumer: %v", err)
	}
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPublish[pi]
	// It counts against Receive Maximum, and MUST: that limit bounds what the client is
	// holding unacknowledged, and a consumer disappearing does not prove the client
	// completed anything. Discounting it would let the broker send a second PUBLISH while
	// the client still holds the first (Spec5 [4.9]). A session stuck this way is a lost
	// message reported honestly. Only a provably never-transmitted delivery — a cancellation
	// tombstone — may free an identifier, and that never reaches here.
	inflight := e.sess.qos12InflightForCap()
	e.sess.mu.Unlock()
	if !reserved {
		t.Fatalf("identifier %d must stay reserved: its delivery was lost, not completed", pi)
	}
	if inflight != 1 {
		t.Fatalf("a possibly client-held delivery must count against Receive Maximum, got %d", inflight)
	}
	if n := e.countRegisters(t); n != 1 {
		t.Fatalf("the register must be retained, got %d", n)
	}
}

// A stored PUBREL reserves its identifier AND its source, so a redelivery of the
// source is suppressed by the usedForPubRel guard instead of re-sending a payload the
// client has already answered. This is also the latent collision the barrier closes:
// without the reservation a fresh allocation could mint the PUBREL's identifier.
func TestMQTTInflightRecoveryReservesPubRelIdentifiers(t *testing.T) {
	e := testNewInflightEnv(t, "client-pubrel")
	dur := e.sess.idHash + "_" + nuid.Next()
	const pi = uint16(5)
	const srcSeq = uint64(31)

	// The PUBREL consumer has to exist first: $MQTT_out is interest-retained, so a
	// store with no interest keeps nothing.
	e.subscribePubRel(t)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, dur, srcSeq, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, asRel := e.sess.pendingPubRel[pi]
	gotPi := e.sess.cpending[dur][srcSeq]
	// A fresh allocation must not be able to mint an identifier a PUBREL owns.
	e.sess.last_pi = pi - 1
	minted := e.sess.bumpPI()
	e.sess.mu.Unlock()
	if !asRel {
		t.Fatalf("identifier %d was not reserved as a pending PUBREL", pi)
	}
	if gotPi != pi {
		t.Fatalf("the PUBREL's source maps to %d, want %d", gotPi, pi)
	}
	if minted == pi {
		t.Fatalf("bumpPI minted %d, an identifier an unreplayed PUBREL still owns", minted)
	}

	// And a redelivery of the source is suppressed rather than re-sent.
	e.sess.mu.Lock()
	got, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, srcSeq, 2))
	e.sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("a source past PUBREC must not be re-sent, got pi=%d", got)
	}
}

// subscribePubRel establishes this session's PUBREL consumer and delivery
// subscription, so a stored PUBREL is actually retained.
func (e *testInflightEnv) subscribePubRel(t *testing.T) {
	t.Helper()
	if err := e.sess.ensurePubRelConsumerSubscription(e.c); err != nil {
		t.Fatalf("ensurePubRelConsumerSubscription: %v", err)
	}
}

// Where a register and a PUBREL record name the same source, the PUBREL owns the
// identifier (R2) — but what happens to the register depends on the SOURCE, and both
// branches matter.
//
// Source still LIVE means the handoff's source acknowledgement was never proven, so the
// source will be re-offered. The register must be RETIRED and kept: deleting it destroys
// the only record that can carry that fact, and once the client's PUBCOMP removes the
// PUBREL nothing is left to say the payload was already answered.
//
// Mutation check: delete the register on this branch (the original rule) and the retained
// -and-retired assertions fail.
func TestMQTTInflightRecoveryPubRelRetiresRegisterWhenSourceLive(t *testing.T) {
	e := testNewInflightEnv(t, "client-precedence")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.subscribePubRel(t)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, dur, sseq, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, asPub := e.sess.pendingPublish[pi]
	_, asRel := e.sess.pendingPubRel[pi]
	retired := e.sess.sourceRetiredLocked(dur, sseq)
	e.sess.mu.Unlock()
	if asPub {
		t.Fatalf("identifier %d must not be reserved as a PUBLISH: it is past PUBREC", pi)
	}
	if !asRel {
		t.Fatalf("identifier %d must be reserved as a pending PUBREL", pi)
	}
	if !retired {
		t.Fatalf("the source must be retired: its acknowledgement was never proven, so it will be re-offered")
	}
	// Kept AND marked, so the fact outlives the PUBREL the client is about to answer.
	e.waitRegisterCount(t, 1)
	rec, _ := e.loadRegister(t, pi)
	if rec == nil || !rec.Retired {
		t.Fatalf("the retained register must be marked retired, got %+v", rec)
	}
}

// Source provably COMPLETE means the handoff finished and only the register's delete was
// lost. Nothing can redeliver it, so the register is dropped rather than retired — which
// is what keeps a lost delete from leaving state behind forever.
func TestMQTTInflightRecoveryPubRelDropsRegisterWhenSourceComplete(t *testing.T) {
	e := testNewInflightEnv(t, "client-precedence-done")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.subscribePubRel(t)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, dur, sseq, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}
	// The source was acknowledged: with its consumer still present, absence proves it.
	if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, asRel := e.sess.pendingPubRel[pi]
	retired := e.sess.sourceRetiredLocked(dur, sseq)
	e.sess.mu.Unlock()
	if !asRel {
		t.Fatalf("identifier %d must be reserved as a pending PUBREL", pi)
	}
	if retired {
		t.Fatalf("a completed source must not be retired: nothing will ever re-offer it")
	}
	e.waitRegisterCount(t, 0)
}

// Two registers for one source delivery — a superseded broker wrote one — FAIL CLOSED.
//
// A stream sequence orders the WRITES, not the sends: by R1 each writer confirmed its
// register before transmitting, so the broker whose record landed later may well have
// sent first. Nothing here can say which identifier the client holds, so both are
// reserved, both records are kept, the source is mapped to NEITHER, and allocation is
// withheld — rather than restoring one by guesswork and possibly re-sending under an
// identifier the client never saw.
//
// Mutation check: restore the "keep the newest, drop the other" rule and the
// both-reserved, register-count and withheld-allocation assertions all fail.
func TestMQTTInflightRecoveryDuplicateSourceFailsClosed(t *testing.T) {
	e := testNewInflightEnv(t, "client-duplicate")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	older := &mqttInflightRecord{PI: 3, Dur: dur, Seq: sseq, QoS: 2, Nonce: mqttNewInflightNonce()}
	newer := &mqttInflightRecord{PI: 4, Dur: dur, Seq: sseq, QoS: 2, Nonce: mqttNewInflightNonce()}
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, 3), 0, older); err != nil {
		t.Fatalf("seed the older register: %v", err)
	}
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, 4), 0, newer); err != nil {
		t.Fatalf("seed the newer register: %v", err)
	}

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, kept3 := e.sess.pendingPublish[3]
	_, kept4 := e.sess.pendingPublish[4]
	mapped, sourceMapped := e.sess.cpending[dur][sseq]
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if !kept3 || !kept4 {
		t.Fatalf("both identifiers must be reserved so neither is reused (3=%v 4=%v)", kept3, kept4)
	}
	if sourceMapped {
		t.Fatalf("the source must map to no identifier, got %d: mapping one would re-send under a guess", mapped)
	}
	if !incomplete {
		t.Fatalf("an undeterminable identifier leaves the identifier space incomplete")
	}
	// Both records are kept: each is evidence that its identifier may be in use.
	e.waitRegisterCount(t, 2)

	// The source's redelivery is not re-sent under either identifier.
	e.sess.mu.Lock()
	got, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("the source was re-sent under identifier %d, which cannot be proven to be the one the client holds", got)
	}
}

// A RETIRED register whose source is provably complete is dropped rather than
// re-retired: the acknowledgement that could not be confirmed had in fact landed, so the
// retirement has nothing left to suppress.
//
// Without this a retirement never converges — every resume re-establishes it from its own
// record, and one accumulates per unproven acknowledgement for as long as the session
// lives.
//
// Mutation check: remove the completeness test from the retired branch of
// recoverInflightRegisters and both assertions fail.
func TestMQTTInflightRecoveryDropsRetiredRegisterWhenSourceComplete(t *testing.T) {
	e := testNewInflightEnv(t, "client-retired-converge")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	// Mark it retired the way the PUBREC path does.
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if _, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, ack.regSeq, ack.regNonce, _EMPTY_); !ok {
		t.Fatalf("could not mark the register retired")
	}
	// The source turns out to have been acknowledged after all: with its consumer still
	// present, absence proves it.
	if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	retired := e.sess.sourceRetiredLocked(dur, sseq)
	e.sess.mu.Unlock()
	if retired {
		t.Fatalf("a retirement whose source is complete must not be re-established")
	}
	e.waitRegisterCount(t, 0)
}

// An UNREADABLE register reserves its identifier, is kept rather than deleted, AND
// stops the session allocating anything new.
//
// The body is unusable but the subject is not: it parsed, so this incarnation wrote a
// register for that exact identifier, and by R1 a register exists only for a delivery
// that was about to go on the wire — so the client may be holding it.
//
// All three parts are needed, and the third is the one that is easy to miss. Reserving
// the identifier stops it being handed to a different message ([MQTT-2.3.1-4]) and
// keeping the record is what lets the next resume know to reserve it again. But the
// register's SOURCE is still on its stream, unacked, and its body is exactly what would
// have named it — so its redelivery finds no cpending entry and would take a FRESH
// identifier, putting the same message in flight under two. For QoS 2 that is two
// exchanges for one message. Only withholding allocation prevents it.
//
// Mutation checks: (a) delete the record and skip the reservation — the reservation
// assertion fails; (b) drop the regSeq exclusion from dropUnredeliverableRetained — the
// survival assertion fails; (c) do not set piSpaceIncomplete for an opaque register —
// the fresh-source-delivery assertion fails.
func TestMQTTInflightRecoveryReservesUnreadableRegister(t *testing.T) {
	e := testNewInflightEnv(t, "client-unusable")
	dur := e.sess.idHash + "_" + nuid.Next()

	const pi = uint16(8)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	msg, hdrLen := buildInflightRecord(0, &mqttInflightRecord{})
	if _, err := e.asm.jsa.storeMsg(subject, hdrLen, msg); err != nil {
		t.Fatalf("seed an unusable register: %v", err)
	}

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	ack, reserved := e.sess.pendingPublish[pi]
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if !reserved {
		t.Fatalf("an unreadable register must still reserve identifier %d: the client may be holding it", pi)
	}
	if ack.jsDur != _EMPTY_ || ack.sseq != 0 {
		t.Fatalf("an unreadable register names no source, got %+v", ack)
	}
	if ack.regSeq == 0 {
		t.Fatalf("the reservation must carry its register sequence, or the next resume mistakes it for a retained send")
	}
	if !incomplete {
		t.Fatalf("an unreadable register leaves the identifier space incomplete")
	}
	// Kept: it is the only evidence that the identifier is in use.
	e.waitRegisterCount(t, 1)

	// The reserved identifier is never handed out again...
	e.sess.mu.Lock()
	e.sess.last_pi = pi - 1
	minted := e.sess.bumpPI()
	e.sess.mu.Unlock()
	if minted == pi {
		t.Fatalf("bumpPI minted %d, an identifier an unreadable register still reserves", minted)
	}
	// ...and neither is any OTHER identifier, because the delivery that owns the
	// reserved one cannot be identified: a redelivery of it would otherwise go out
	// under a second identifier for a message already in flight.
	e.sess.mu.Lock()
	got, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1300, 2))
	e.sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("a source delivery was allocated identifier %d while an unreadable register's delivery was unidentifiable", got)
	}

	// And a later resume keeps the reservation rather than releasing it as a retained
	// send.
	e.sess.mu.Lock()
	e.sess.dropUnredeliverableRetained()
	_, stillReserved := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if !stillReserved {
		t.Fatalf("the reservation must survive dropUnredeliverableRetained")
	}

	// A SECOND resume of the same session object — a same-broker reconnect — must not
	// lift the restriction. The reservation from the first resume is already in
	// pendingPublish, so this pass installs nothing; a condition counting what it
	// installed rather than what it FOUND would read that as "nothing is opaque" and
	// re-enable allocation with the unreadable record still there.
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("second recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	incomplete = e.sess.piSpaceIncomplete
	_, keptReserved := e.sess.pendingPublish[pi]
	got, _, _, _ = e.sess.trackPublish(dur, testInflightAckSubject(dur, 1301, 2))
	e.sess.mu.Unlock()
	if !incomplete {
		t.Fatalf("a second resume must not lift the restriction while a register is still unreadable")
	}
	if !keptReserved {
		t.Fatalf("a second resume must keep identifier %d reserved", pi)
	}
	if got != 0 {
		t.Fatalf("a second resume re-enabled allocation: identifier %d was issued", got)
	}

	// Removing the unreadable record restores allocation on the next resume.
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, ack.regSeq, true); err != nil {
		t.Fatalf("remove the unreadable register: %v", err)
	}
	e.sess.mu.Lock()
	delete(e.sess.pendingPublish, pi)
	e.sess.mu.Unlock()
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	incomplete = e.sess.piSpaceIncomplete
	got, _, _, _ = e.sess.trackPublish(dur, testInflightAckSubject(dur, 1300, 2))
	e.sess.mu.Unlock()
	if incomplete {
		t.Fatalf("removing the unreadable register must restore the identifier space")
	}
	if got == 0 {
		t.Fatalf("allocation must resume once no register is unreadable")
	}
}

// In-memory state wins every conflict: a same-process reconnect already holds the
// live authority, and the barrier must not overwrite it.
func TestMQTTInflightRecoveryKeepsInMemoryState(t *testing.T) {
	e := testNewInflightEnv(t, "client-inmemory")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.sess.mu.Lock()
	live := e.sess.pendingPublish[pi]
	liveAck := testInflightAckSubject(dur, sseq, 1)
	live.jsAckSubject = liveAck
	e.sess.mu.Unlock()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	got := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if got != live {
		t.Fatalf("the live pending entry was replaced")
	}
	if got.jsAckSubject != liveAck {
		t.Fatalf("the live entry lost its ack subject (%q)", got.jsAckSubject)
	}
}

// An unreadable walk does not deny the resume; it withholds ALLOCATION, which is the
// only operation an unknown identifier space makes unsafe. Both halves of the walk
// behave the same way, and a later resume that completes clears the restriction.
func TestMQTTInflightRecoveryWithholdsAllocationOnUnreadableWalk(t *testing.T) {
	for _, tc := range []struct {
		name string
		// break installs the failure and returns its undo. Each targets ONE step of the
		// barrier: the register enumeration, one register load, or the PUBREL walk.
		brk func(*testInflightEnv) func()
	}{
		{"register enumeration", func(e *testInflightEnv) func() {
			e.asm.mu.Lock()
			e.asm.testInflightEnumerate = func(string) (map[string]uint64, error) {
				return nil, fmt.Errorf("enumeration failed")
			}
			e.asm.mu.Unlock()
			return func() {
				e.asm.mu.Lock()
				e.asm.testInflightEnumerate = nil
				e.asm.mu.Unlock()
			}
		}},
		{"PUBREL walk", func(e *testInflightEnv) func() {
			return setMQTTTestLoadNextMsgHook(func(stream, subject string, seq uint64) error {
				if stream == mqttOutStreamName {
					return fmt.Errorf("scan failed")
				}
				return nil
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "client-unreadable-"+strings.ReplaceAll(tc.name, " ", "-"))
			dur := e.sess.idHash + "_" + nuid.Next()

			restore := tc.brk(e)
			// DEFERRED as well as lifted explicitly below: a t.Fatalf between here and
			// there would otherwise leave the failure installed for every test that
			// follows in this package, which is exactly what happened once.
			defer restore()

			// The resume itself succeeds: taking a whole session offline over a
			// transient scan failure is worse than withholding new deliveries.
			if err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("an unreadable walk must not deny the resume: %v", err)
			}
			e.sess.mu.Lock()
			incomplete := e.sess.piSpaceIncomplete
			e.sess.mu.Unlock()
			if !incomplete {
				t.Fatalf("an unreadable walk must mark the identifier space incomplete")
			}
			// And nothing new may be allocated while it is unknown.
			e.sess.mu.Lock()
			pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1200, 1))
			e.sess.mu.Unlock()
			if pi != 0 {
				t.Fatalf("allocated packet identifier %d while the identifier space was unknown", pi)
			}

			// A resume that completes lifts the restriction.
			restore()
			if err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			e.sess.mu.Lock()
			incomplete = e.sess.piSpaceIncomplete
			pi, _, _, _ = e.sess.trackPublish(dur, testInflightAckSubject(dur, 1200, 1))
			e.sess.mu.Unlock()
			if incomplete {
				t.Fatalf("a completed scan must clear the restriction")
			}
			if pi == 0 {
				t.Fatalf("allocation must resume once the identifier space is known")
			}
		})
	}
}

// A session discard clears the incomplete-space restriction along with the identifier
// space it protected.
//
// Two paths REUSE the session object as a fresh session, and a fresh session reports
// Session Present false, so it never runs the resume barrier that would otherwise
// clear this. Left set, bumpPI returns 0 forever and the new session can never deliver
// a single outbound QoS 1/2 message — a permanent outage for that client.
//
// Mutation check: drop the piSpaceIncomplete reset from clear() and the allocation
// assertion below fails.
func TestMQTTInflightClearResetsIncompleteSpace(t *testing.T) {
	e := testNewInflightEnv(t, "client-clear-flag")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Reach the restricted state the way a failed scan does.
	e.sess.setPISpaceIncomplete(e.c, "test")
	e.sess.mu.Lock()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if !incomplete {
		t.Fatalf("the restriction must be in force before the discard")
	}

	if err := e.sess.clear(false); err != nil {
		t.Fatalf("clear: %v", err)
	}
	e.sess.mu.Lock()
	incomplete = e.sess.piSpaceIncomplete
	e.sess.maxp = 100
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1400, 1))
	e.sess.mu.Unlock()
	if incomplete {
		t.Fatalf("a discard retires the identifier space, so its restriction must go with it")
	}
	if pi == 0 || !isNew {
		t.Fatalf("the fresh session must be able to allocate: pi=%d isNew=%v", pi, isNew)
	}
}

// A register store that fails leaves the sequence marked as withheld, so its eventual
// FIRST transmission carries DUP=0.
//
// Nothing was put on the wire, but the AckWait redelivery arrives with a JetStream
// delivery count above one, and trackPublish derives dup from that count unless the
// sequence is known to have been withheld ([MQTT-3.3.1-1]).
//
// Mutation check: drop the deferred marker from abandonInflightRegister and the
// redelivery reports dup=true.
func TestMQTTInflightAbandonedRegisterKeepsFirstSendDupClear(t *testing.T) {
	e := testNewInflightEnv(t, "client-dup")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.asm.mu.Lock()
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("store refused")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, nil // the subject is empty: the write did not land
	}
	e.asm.mu.Unlock()

	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1500, 1))
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1500, _EMPTY_) {
		t.Fatalf("an unrecordable identifier must not be released for sending")
	}
	e.sess.mu.Lock()
	_, deferredNow := e.sess.deferred[mqttDeferredKey{dur, 1500}]
	e.sess.mu.Unlock()
	if !deferredNow {
		t.Fatalf("an abandoned allocation must be recorded as withheld: it was never transmitted")
	}

	// Let the store succeed, and take the redelivery: it is the FIRST transmission.
	e.asm.mu.Lock()
	e.asm.testInflightStore, e.asm.testInflightLoad = nil, nil
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	pi2, dup, started, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1500, 2))
	e.sess.mu.Unlock()
	if pi2 == 0 || !isNew || started {
		t.Fatalf("the redelivery must be a fresh allocation: pi=%d isNew=%v started=%v", pi2, isNew, started)
	}
	if dup {
		t.Fatalf("the first transmission of a never-sent message must carry DUP=0 [MQTT-3.3.1-1]")
	}
}

// A source retired by an unproven acknowledgement stays retired ACROSS PUBCOMP.
//
// This is the property an identifier-keyed suppression cannot have. Restoring the
// source's cpending link makes trackPublish's usedForPubRel guard suppress the
// redelivery — but only until PUBCOMP, because untrackPubRel removes the pendingPubRel
// entry that guard tests. From then on the payload of an exchange the client has
// already COMPLETED would simply be re-sent. Keying the fact by the source is what
// outlives the identifier's bookkeeping.
//
// Mutation check: key the suppression by identifier instead — restore
// cpending[dur][sseq] = pi and drop the srcAckOwed entry — and the post-PUBCOMP
// assertion fails.
func TestMQTTInflightRetiredSourceSurvivesPubComp(t *testing.T) {
	sess := &mqttSession{maxp: 100}
	const pi = uint16(7)
	const dur, sseq = "hash_nuid", uint64(42)

	// The state a PUBREC leaves behind when its source acknowledgement is unproven: the
	// identifier owes a PUBREL, the source's PUBLISH-phase entry is gone, and the source
	// is retired.
	sess.mu.Lock()
	sess.pendingPublish = map[uint16]*mqttPending{}
	sess.pendingPubRel = map[uint16]*mqttPending{pi: {jsDur: dur}}
	sess.cpending = map[string]map[uint64]uint16{}
	sess.retireSourceLocked(dur, sseq, &mqttRetiredSource{pi: pi})
	retiredBefore := sess.sourceRetiredLocked(dur, sseq)
	sess.mu.Unlock()
	if !retiredBefore {
		t.Fatalf("the source must be retired while the exchange is in its PUBREL phase")
	}

	// PUBCOMP: the identifier's bookkeeping goes away entirely.
	sess.mu.Lock()
	sess.untrackPubRel(pi)
	_, stillPubRel := sess.pendingPubRel[pi]
	retiredAfter := sess.sourceRetiredLocked(dur, sseq)
	// And nothing suppresses this source through the identifier any more, which is
	// exactly why the retirement has to be keyed by the source.
	got, _, _, _ := sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	sess.mu.Unlock()
	if stillPubRel {
		t.Fatalf("PUBCOMP must clear the pending PUBREL")
	}
	if !retiredAfter {
		t.Fatalf("the source must STILL be retired after PUBCOMP: its payload was already answered")
	}
	if got == 0 {
		t.Fatalf("this test is vacuous unless trackPublish would otherwise re-send the source after PUBCOMP")
	}

	// An unrelated source is unaffected.
	sess.mu.Lock()
	other := sess.sourceRetiredLocked(dur, 43)
	sess.mu.Unlock()
	if other {
		t.Fatalf("only the retired source may be suppressed")
	}
}

// Staging a drain CANCELS a delivery whose register is still in flight, rather than
// merely leaving it out of the drain.
//
// Skipping is not enough: the delivery callback is still inside storeInflightRegister, and
// once the session lock is released it would confirm its CAS and enqueue the PUBLISH —
// after this UNSUBSCRIBE has purged the consumer and its tracking, leaving the client
// holding an exchange nothing here remembers. Removing the entry hands the decision to the
// store's own ownership revalidation, which then refuses the send.
//
// Mutation check: `continue` without untracking (the original) and the entry survives, so
// the store confirms and the send proceeds.
func TestMQTTInflightStageDrainCancelsPendingRegister(t *testing.T) {
	e := testNewInflightEnv(t, "client-stage-cancel")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	// A delivery mid-registration: allocated, regPending, not yet confirmed.
	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1600, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}

	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	e.sess.mu.Lock()
	e.sess.cons = map[string]*ConsumerConfig{sid: cc}
	staged := e.sess.stageDrain(sid, cc, nil)
	_, stillTracked := e.sess.pendingPublish[pi]
	_, stillMapped := e.sess.cpending[dur][1600]
	e.sess.mu.Unlock()

	if staged {
		t.Fatalf("nothing was transmitted, so there is nothing to drain")
	}
	if stillTracked {
		t.Fatalf("packet identifier %d must be cancelled, not left for a callback to send under", pi)
	}
	if stillMapped {
		t.Fatalf("the source must not stay mapped to a cancelled identifier")
	}
	// EVERY entry was cancelled, so no drain is staged — but the withheld marker must
	// still be installed, or a rolled-back UNSUBSCRIBE costs the first real transmission
	// its DUP=0. This is the case an install placed after the "nothing staged" return
	// would miss entirely.
	e.sess.mu.Lock()
	_, withheld := e.sess.deferred[mqttDeferredKey{dur, 1600}]
	e.sess.mu.Unlock()
	if !withheld {
		t.Fatalf("an all-cancelled staging must still record the withheld marker")
	}

	// And the store's ownership revalidation refuses the send.
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1600, _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
}

// A cancelled registration is recorded as WITHHELD, so a rolled-back UNSUBSCRIBE does not
// cost the first transmission its DUP=0.
//
// Nothing was put on the wire, but the redelivery arrives with a JetStream delivery count
// above one, and trackPublish derives dup from that count unless the sequence is known to
// have been withheld ([MQTT-3.3.1-1]). A COMMITTED teardown drops the marker instead:
// the consumer is gone by then and nothing will ever re-offer the sequence.
//
// Mutation check: cancel without recording the marker and the DUP assertion fails; drop
// the commitDrains cleanup and the committed case leaves the marker behind.
func TestMQTTInflightStageDrainCancelKeepsFirstSendDupClear(t *testing.T) {
	e := testNewInflightEnv(t, "client-cancel-dup")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid, sseq = "foo", uint64(1800)
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}

	stage := func(t *testing.T) {
		t.Helper()
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		// One transmitted delivery so the drain stages at all, plus the one under test
		// whose registration is still in flight.
		pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq-1, 1))
		e.sess.pendingPublish[pi].regPending = false
		e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1)) //nolint:dogsled // regPending by design
		e.sess.cons = map[string]*ConsumerConfig{sid: cc}
		if !e.sess.stageDrain(sid, cc, nil) {
			t.Fatalf("the transmitted delivery must stage a drain")
		}
	}

	// ROLLED BACK: the marker survives, so the eventual first transmission is DUP=0.
	stage(t)
	e.sess.mu.Lock()
	_, withheld := e.sess.deferred[mqttDeferredKey{dur, sseq}]
	e.sess.mu.Unlock()
	if !withheld {
		t.Fatalf("a cancelled registration must be recorded as withheld, or its first send carries DUP=1")
	}
	e.sess.mu.Lock()
	_, dup, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if !isNew {
		t.Fatalf("the redelivery of a cancelled sequence is a fresh allocation")
	}
	if dup {
		t.Fatalf("the first transmission of a never-sent message must carry DUP=0 [MQTT-3.3.1-1]")
	}

	// COMMITTED: the marker goes, since nothing will re-offer that sequence.
	e.sess.mu.Lock()
	e.sess.drains, e.sess.drainByDur, e.sess.deferred = nil, nil, nil
	e.sess.pendingPublish, e.sess.cpending = nil, nil
	e.sess.mu.Unlock()
	stage(t)
	e.sess.mu.Lock()
	e.sess.commitDrains()
	_, stillWithheld := e.sess.deferred[mqttDeferredKey{dur, sseq}]
	e.sess.mu.Unlock()
	if stillWithheld {
		t.Fatalf("a committed teardown must not leave a withheld marker behind")
	}
}

// A drained delivery whose register is ALREADY GONE completes rather than withholding
// forever.
//
// If the release lands and the broker dies before persisting the claim removal, recovery
// restores the record's old reg_seq with no register left to repair it from. A retry that
// read "sequence not found" as a failure would withhold that exchange's completion for
// good.
//
// Mutation check: use the bare deleteMsg here instead of the confirmed helper and the
// completion is withheld.
func TestMQTTInflightDrainCompletesWhenRegisterAlreadyGone(t *testing.T) {
	e := testNewInflightEnv(t, "client-drain-gone")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	regSeq := e.sess.pendingPublish[pi].regSeq
	ackSubject := e.sess.pendingPublish[pi].jsAckSubject
	// A drain claiming it, with the register sequence recorded — and the register itself
	// already deleted, which is the post-crash state.
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	e.sess.drains = map[string]*mqttDrain{sid: {
		cc: cc, qos: 2,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true, pi: pi, regSeq: regSeq, ack: ackSubject}},
	}}
	e.sess.drainByDur = map[string]*mqttDrain{dur: e.sess.drains[sid]}
	e.sess.mu.Unlock()
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, regSeq, true); err != nil {
		t.Fatalf("pre-delete the register: %v", err)
	}

	if !e.sess.finalizeDrainDelivery(e.c, dur, sseq, ackSubject) {
		t.Fatalf("the drain must handle its own sequence")
	}
	e.sess.mu.Lock()
	claimed := e.sess.drains[sid] != nil && e.sess.drains[sid].seqs[sseq] != nil &&
		e.sess.drains[sid].seqs[sseq].claimed
	e.sess.mu.Unlock()
	if claimed {
		t.Fatalf("an already-absent register must not withhold the exchange's completion")
	}
}

// Two registers naming ONE drained delivery fail closed: a drain holds a single register
// sequence, so releasing one would orphan the rest as phantoms after its consumer goes.
//
// Mutation check: assign each register to the drain in turn (the original) and only one
// sequence survives, so the others are neither reserved nor released.
func TestMQTTInflightRecoveryDrainAmbiguousRegistersFailClosed(t *testing.T) {
	e := testNewInflightEnv(t, "client-drain-ambiguous")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	sseq := e.publishSource(t, dur)
	// Two registers, one source, different identifiers: a superseded broker wrote one.
	for _, pi := range []uint16{3, 4} {
		rec := &mqttInflightRecord{PI: pi, Dur: dur, Seq: sseq, QoS: 2, Nonce: mqttNewInflightNonce()}
		if _, err := e.asm.jsa.storeInflightCAS(
			mqttInflightSubject(e.sess.idHash, e.sess.gen, pi), 0, rec); err != nil {
			t.Fatalf("seed register %d: %v", pi, err)
		}
	}
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	e.sess.mu.Lock()
	e.sess.drains = map[string]*mqttDrain{sid: {
		cc: cc, qos: 2,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true, pi: 3, regSeq: 999}},
	}}
	e.sess.drainByDur = map[string]*mqttDrain{dur: e.sess.drains[sid]}
	e.sess.mu.Unlock()
	e.forgetInMemory()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, kept3 := e.sess.pendingPublish[3]
	_, kept4 := e.sess.pendingPublish[4]
	incomplete := e.sess.piSpaceIncomplete
	drainRegSeq := e.sess.drains[sid].seqs[sseq].regSeq
	e.sess.mu.Unlock()
	if !kept3 || !kept4 {
		t.Fatalf("both identifiers must be reserved (3=%v 4=%v)", kept3, kept4)
	}
	if !incomplete {
		t.Fatalf("an undeterminable drained identifier leaves the identifier space incomplete")
	}
	if drainRegSeq != 0 {
		t.Fatalf("the drain must not hold one of several sequences (%d): releasing it would orphan the others", drainRegSeq)
	}
	// Both records are kept: each is evidence that its identifier may be in use.
	e.waitRegisterCount(t, 2)
}

// A cancelled register whose delete cannot be proven is TOMBSTONED, and a later resume
// frees its identifier rather than reserving it.
//
// This is the durable evidence that separates the two cases a surviving register otherwise
// conflates. A delivery that was TRANSMITTED must keep its identifier and its Receive
// Maximum slot, because the client may be holding it. One that was never transmitted must
// keep neither — and only a tombstone can say which, since a confirmed register otherwise
// means "about to be sent" (R1).
//
// Mutation check: drop the markInflightRegisterCancelled fallback and the resume reserves
// the identifier and consumes the slot instead.
func TestMQTTInflightCancelledRegisterTombstoneFreesIdentifier(t *testing.T) {
	e := testNewInflightEnv(t, "client-tombstone")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 2000, 1))
	e.sess.mu.Unlock()

	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		seq, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		if r.Seq == 2000 && !r.Cancelled {
			// Cancel the entry while its store is in flight.
			e.sess.mu.Lock()
			e.sess.untrackPublishEntry(pi)
			e.sess.mu.Unlock()
		}
		return seq, err
	}
	// The delete cannot be proven, so the tombstone is the fallback.
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.mu.Unlock()

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 2000, _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	rec, _ := e.loadRegister(t, pi)
	if rec == nil || !rec.Cancelled {
		t.Fatalf("an unprovable cancellation delete must leave a tombstone, got %+v", rec)
	}

	// A resume then frees the identifier outright: the client never saw this PUBLISH.
	e.asm.mu.Lock()
	e.asm.testInflightStore, e.asm.testInflightDelete = nil, nil
	e.asm.mu.Unlock()
	e.forgetInMemory()
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPublish[pi]
	inflight := e.sess.qos12InflightForCap()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if reserved {
		t.Fatalf("identifier %d was never transmitted, so it must not be reserved", pi)
	}
	if inflight != 0 {
		t.Fatalf("a never-transmitted delivery must consume no Receive Maximum slot, got %d", inflight)
	}
	if incomplete {
		t.Fatalf("a tombstoned register is fully accounted for; allocation must not be withheld")
	}
	e.waitRegisterCount(t, 0)
}

// An owed cleanup is PERSISTED, so a restart cannot lose it.
//
// This is the last fallback, reached only when neither the confirmed delete nor the
// cancellation tombstone can be written — in practice a JetStream outage covering both. If
// the obligation were memory-only, an UNSUBSCRIBE that then completed its consumer delete
// would leave a register indistinguishable from a started delivery, whose identifier and
// Receive Maximum slot are consumed for the life of the session.
//
// Mutation check: drop OwedRegs from the persisted record (or from the restore) and the
// obligation does not survive.
func TestMQTTInflightOwedRegisterDeletePersists(t *testing.T) {
	e := testNewInflightEnv(t, "client-owed-persist")

	// Record an owed cleanup the way the doubly-failed cancellation path does.
	e.sess.oweRegisterDelete(e.c, 4242, _EMPTY_)
	e.sess.mu.Lock()
	owed := len(e.sess.owedRegisterDeletes)
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if owed != 1 {
		t.Fatalf("the obligation must be held in memory, got %d", owed)
	}

	// It is in the RECORD, which is what survives a restart.
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatalf("no session record")
	}
	if len(ps.OwedRegs) != 1 || ps.OwedRegs[0] != 4242 {
		t.Fatalf("the record must carry the owed cleanup, got %v", ps.OwedRegs)
	}

	// A restore — the restart — brings it back, so the retry can still discharge it.
	restored, _, err := e.asm.createOrRestoreSession("client-owed-persist", e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	restored.mu.Lock()
	back := restored.owedRegisterDeletes
	restored.mu.Unlock()
	if len(back) != 1 || back[0] != 4242 {
		t.Fatalf("a restored session must carry the owed cleanup, got %v", back)
	}
}

// A consumer teardown makes its cancellations durable ON THE REGISTER STREAM before the
// consumer goes, and holds the identifier until the store settles.
//
// These callers (a QoS downgrade's deferred consumer delete, a forced shared leave, a shared
// release) run AFTER their packet's save, so an intent recorded only on the session may not
// reach the record for a while — and a crash meanwhile leaves a landed register with nothing
// anywhere to explain it. A tombstone at the register's own subject needs no fence and no
// packet, so it is durable exactly when it has to be. The identifier stays reserved across
// the store because freeing it would let a new delivery write its own register at the same
// subject, which the tombstone must never be confused with.
//
// Mutation checks: drop the secureRegCancellations call from the teardown, or free the
// identifier at purge time instead of reserving it, and the assertions below fail.
func TestMQTTInflightConsumerPurgeSecuresCancellations(t *testing.T) {
	e := testNewInflightEnv(t, "client-purge-secure")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	var nonce string
	if ack := e.sess.pendingPublish[pi]; ack != nil {
		nonce = ack.regNonce
	}
	e.sess.mu.Unlock()
	if pi == 0 || !isNew || nonce == _EMPTY_ {
		t.Fatalf("trackPublish: pi=%d isNew=%v nonce=%q", pi, isNew, nonce)
	}

	// The teardown happens WHILE the register store is in flight, which is the whole
	// race: the hook runs in the store's place, tears the consumer down, and only then
	// lets the CAS through.
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		e.asm.mu.Lock()
		e.asm.testInflightStore = nil // once: the securing pass stores too
		e.asm.mu.Unlock()

		e.sess.mu.Lock()
		regs, cancels := e.sess.purgeConsumerPendingLocked(dur)
		held := e.sess.pendingPublish[pi]
		e.sess.mu.Unlock()
		if len(regs) != 0 {
			t.Errorf("an unconfirmed registration has no sequence to delete, got %v", regs)
		}
		if len(cancels) != 1 || cancels[0].pi != pi || cancels[0].nonce != nonce {
			t.Errorf("the teardown must report the cancelled registration, got %+v", cancels)
		}
		// The identifier is RESERVED, inert, until this store settles: nothing else may
		// take the subject the tombstone is about to occupy.
		if held == nil || !held.regPending || !held.regCancelled {
			t.Errorf("a cancelled registration must keep its identifier reserved, got %+v", held)
		}

		if !e.sess.secureRegCancellations(e.c, recordTx{}, cancels) {
			t.Errorf("the cancellation must be securable")
		}

		// The tombstone is on the register stream, so a restart needs nothing else.
		rec, smsg := e.loadRegister(t, pi)
		if rec == nil || smsg == nil {
			t.Errorf("no cancellation tombstone was written for pi %d", pi)
		} else if !rec.Cancelled || rec.Nonce != nonce {
			t.Errorf("tombstone %+v does not cancel this registration", rec)
		}
		// And the weaker record-borne intent is released against it.
		e.sess.mu.Lock()
		intents := len(e.sess.cancelledRegNonces)
		e.sess.mu.Unlock()
		if intents != 0 {
			t.Errorf("a tombstoned cancellation needs no session-record intent, %d left", intents)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()

	// The store settles: it refuses the send, removes the record, and releases the
	// reserved identifier.
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	e.sess.mu.Lock()
	_, stillHeld := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if stillHeld {
		t.Fatalf("the reservation must be released once the store settles")
	}
	e.waitRegisterCount(t, 0)
}

// A consumer teardown cancels its in-flight registrations in the SAME record write that
// removes the membership.
//
// Doing it in the teardown instead — after that write — cannot be made equivalent, and
// keeping the consumer does not redeem it: once the membership is gone there is no
// subscription left to redeliver through, so a register that lands afterwards holds its
// identifier and its Receive Maximum slot for the life of the session whatever becomes of the
// durable. Under a Receive Maximum of 1 that is a session that can no longer deliver.
//
// Mutation check: stage nothing (drop the cancelPendingRegistrationsLocked call from the
// UNSUBSCRIBE staging loop, or from this test) and the record does not carry the
// cancellation.
func TestMQTTInflightUnsubStagesCancellationWithMembershipRemoval(t *testing.T) {
	e := testNewInflightEnv(t, "client-staged-cancel")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	var nonce string
	if ack := e.sess.pendingPublish[pi]; ack != nil {
		nonce = ack.regNonce
	}
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	e.sess.cons = map[string]*ConsumerConfig{sid: cc}
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	// The delivery subscription the callback checks before tracking anything.
	deliverySub := &subscription{subject: []byte(cc.DeliverSubject), mqtt: &mqttSub{qos: 2}}
	e.c.mu.Lock()
	if e.c.subs == nil {
		e.c.subs = make(map[string]*subscription)
	}
	e.c.subs[cc.DeliverSubject] = deliverySub
	e.c.mu.Unlock()
	if nonce == _EMPTY_ {
		t.Fatalf("the allocation must carry a register nonce")
	}

	// The staging half of the UNSUBSCRIBE, inside the packet's transaction: the
	// membership goes and the registration is cancelled, then ONE save carries both. No
	// sid is drained here, which is the half stageDrain does not cover: a consumer this
	// packet DESTROYS (draining refused, or a shared group durable released).
	tok := e.sess.beginRecordTx()
	e.sess.mu.Lock()
	delete(e.sess.subs, "foo")
	cancels, closed, _ := e.sess.stageUnsubCancellationsLocked(e.c, []*mqttFilter{{filter: "foo", qos: 2}}, nil)
	held := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	// The delivery barrier must be down BEFORE the snapshot, or the snapshot is only a
	// moment in time: the callback stays live until the packet commits, so a delivery
	// arriving after it would allocate an identifier whose cancellation misses this write.
	if len(closed) != 1 || closed[0] != deliverySub {
		t.Fatalf("the doomed consumer's delivery subscription must be closed before staging, got %v", closed)
	}
	e.sess.subsMu.Lock()
	stillOpen := !deliverySub.mqtt.closed
	e.sess.subsMu.Unlock()
	if stillOpen {
		t.Fatalf("a doomed consumer must stop tracking new deliveries")
	}
	if len(cancels) != 1 || cancels[0].nonce != nonce {
		t.Fatalf("the staging must report the cancelled registration, got %+v", cancels)
	}
	// Inert, and holding its identifier until the store settles.
	if held == nil || !held.regPending || !held.regCancelled {
		t.Fatalf("a cancelled registration must keep its identifier reserved, got %+v", held)
	}
	if err := e.sess.save(tok); err != nil {
		t.Fatalf("persist the unsubscribe: %v", err)
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The write that removed the membership carries the cancellation. Nothing later has
	// to succeed for a restart to free this identifier.
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatalf("no session record")
	}
	if len(ps.CancelledRegs) != 1 || ps.CancelledRegs[0] != nonce {
		t.Fatalf("the membership-removal write must carry the cancellation, got %v", ps.CancelledRegs)
	}
}

// A TRANSMITTED delivery's register is voided by the write that abandons its consumer, and
// recovery frees its identifier instead of reserving it forever.
//
// This is what makes a lost register delete harmless on a teardown path. The confirmed delete
// and the owed sequence are both instructions, and an instruction nobody proved is an
// instruction nobody has: with JetStream unavailable for the delete AND for the record save, a
// crash leaves a register whose consumer is gone — which recovery deliberately does not read as
// complete — and its identifier and Receive Maximum slot are held for the life of the session.
//
// The verdict is keyed by the register's NONCE, not by its durable. A durable-scoped verdict
// cannot survive the one case that needs it: shared group durables are named deterministically,
// so a leave-then-rejoin brings the same name back, and no withdraw-and-restore dance makes
// "everything on this durable is void" mean the right thing on both sides of that boundary. A
// nonce is minted with its allocation and never reused.
//
// Mutation check: void only the regPending allocations (skip the transmitted branch) and the
// identifier stays reserved.
func TestMQTTInflightRecoveryDropsVoidedTransmittedRegister(t *testing.T) {
	const clientID = "client-voided-sent"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()
	burnDur := e.sess.idHash + "_" + nuid.Next()

	// A transmitted delivery whose source is LIVE — the state in which recovery would
	// otherwise reserve the identifier. One identifier is burned first ON A SEPARATE
	// DURABLE, so a fresh allocator could not produce this one by accident (the trap in
	// the file header) while the staging under test still names exactly one register.
	sseqBurn := e.publishSource(t, burnDur)
	e.track(t, burnDur, sseqBurn, 1)
	sseq2 := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq2, 2)
	if pi == 1 {
		t.Fatalf("the delivery under test must not hold the first identifier")
	}
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()

	// The teardown's own record write: the membership goes and the register is voided.
	tok := e.sess.beginRecordTx()
	e.sess.mu.Lock()
	_, sent := e.sess.voidRegistrationsLocked(dur)
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if len(sent) != 1 || sent[0] != nonce {
		t.Fatalf("the staging must report the voided transmitted register, got %v", sent)
	}
	if err := e.sess.save(tok); err != nil {
		t.Fatalf("persist the teardown: %v", err)
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil || len(ps.CancelledRegs) != 1 || ps.CancelledRegs[0] != nonce {
		t.Fatalf("the membership-removal write must carry the verdict, got %v", ps)
	}

	// The register delete never landed, and the broker restarted.
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	_, reserved := restored.pendingPublish[pi]
	inflight := restored.qos12InflightForCap()
	restored.mu.Unlock()
	if reserved {
		t.Fatalf("identifier %d belongs to an abandoned consumer and must not be reserved", pi)
	}
	// Exactly the burner's live delivery remains; the voided one holds no slot.
	if inflight != 1 {
		t.Fatalf("an abandoned delivery must consume no Receive Maximum slot, got %d in flight (want the burner alone)", inflight)
	}
	e.waitRegisterCount(t, 1) // the burner's register survives; the voided one is gone
}

// A rolled-back teardown WITHDRAWS its verdicts on transmitted registers.
//
// The asymmetry is the point. A cancelled allocation is never reinstated — nothing was sent,
// and the source is simply re-offered under a fresh allocation — but a transmitted delivery
// survives a rolled-back packet, subscription and all, and still owes its identifier. Leaving
// that verdict standing would have the next restart free an identifier the client is holding,
// which is the reverse of the bug the verdict exists to fix and a worse one.
//
// Mutation check: make unvoidRegNonces a no-op and the live delivery's identifier is freed.
func TestMQTTInflightRolledBackTeardownRestoresTransmittedRegister(t *testing.T) {
	const clientID = "client-unvoid"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()
	burnDur := e.sess.idHash + "_" + nuid.Next()

	sseqBurn := e.publishSource(t, burnDur)
	e.track(t, burnDur, sseqBurn, 1)
	sseq2 := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq2, 2)

	// Staged, then the packet's save fails and the rollback runs.
	e.sess.mu.Lock()
	_, sent := e.sess.voidRegistrationsLocked(dur)
	e.sess.mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("the staging must report the voided transmitted register, got %v", sent)
	}
	e.sess.unvoidRegNonces(sent)

	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the rolled-back session: %v", err)
	}
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	_, reserved := restored.pendingPublish[pi]
	restored.mu.Unlock()
	if !reserved {
		t.Fatalf("identifier %d belongs to a live delivery and must stay reserved", pi)
	}
}

// The deferred-cleanup helpers survive having no bound client.
//
// A surviving disconnect's shared purge calls them with a nil client, and every one of their
// log sites is on a FAILURE path — so a bare c.Warnf there panics the broker on exactly the
// transient JetStream failure it was trying to report, while a session is disconnecting.
//
// Mutation check: log through c directly in persistOwedRegisterDeletes (or in
// deleteInflightRegisters) and this panics.
func TestMQTTInflightCleanupHelpersTolerateNoClient(t *testing.T) {
	e := testNewInflightEnv(t, "client-no-logger")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	regSeq := e.sess.pendingPublish[pi].regSeq
	e.sess.mu.Unlock()

	// Every write this path attempts fails, which is what drives it onto the log sites.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.mu.Unlock()
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		return fmt.Errorf("test: session record unavailable")
	})
	defer restore()

	// No client, as purgeSharedPending has none.
	e.sess.deleteInflightRegisters(nil, recordTx{}, []uint64{regSeq})
	e.sess.secureRegCancellations(nil, recordTx{}, []mqttRegCancellation{
		{pi: pi, nonce: "nonce-x", durable: dur, sseq: sseq},
	})
	e.sess.retryOwedRegisterDeletes(nil)
	e.sess.oweRegisterDelete(nil, regSeq, _EMPTY_)

	// The obligation is still there, which is the point of surviving at all.
	e.sess.mu.Lock()
	owed := len(e.sess.owedRegisterDeletes)
	e.sess.mu.Unlock()
	if owed == 0 {
		t.Fatalf("the unproven removal must stay owed")
	}
}

// A teardown's register removals are PROVEN, and an unproven one becomes a persisted
// obligation rather than a lost request.
//
// The ordinary terminal deletes may be fire-and-forget: the record they leave behind names a
// source that stays live and classifiable, so the store's reconcile, the next resume or the
// orphan sweep removes it. A teardown's cannot, because the consumer that would classify
// those sources is about to be deleted — after which a surviving register is deliberately not
// read as complete, and one lost delete request reserves an identifier for the life of the
// session.
//
// Mutation check: use the unconfirmed helper (or drop the owed-delete recording) and the
// obligation is not kept.
func TestMQTTInflightTeardownProvesRegisterRemoval(t *testing.T) {
	e := testNewInflightEnv(t, "client-teardown-proof")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	regSeq := e.sess.pendingPublish[pi].regSeq
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	e.sess.mu.Unlock()
	if len(regs) != 1 || regs[0] != regSeq {
		t.Fatalf("the teardown must report the register of its transmitted delivery, got %v", regs)
	}

	// The delete cannot be proven.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.mu.Unlock()

	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)

	e.sess.mu.Lock()
	owed := append([]uint64(nil), e.sess.owedRegisterDeletes...)
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if len(owed) != 1 || owed[0] != regSeq {
		t.Fatalf("an unproven teardown delete must stay owed, got %v", owed)
	}
	// And durably, so a restart still discharges it.
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil || len(ps.OwedRegs) != 1 || ps.OwedRegs[0] != regSeq {
		t.Fatalf("the record must carry the owed removal, got %v", ps)
	}

	// It converges: the next convergence point discharges it.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = nil
	e.asm.mu.Unlock()
	e.sess.retryOwedRegisterDeletes(e.c)
	e.sess.mu.Lock()
	left := len(e.sess.owedRegisterDeletes)
	e.sess.mu.Unlock()
	if left != 0 {
		t.Fatalf("the owed removal must be discharged once the delete works, %d left", left)
	}
	e.waitRegisterCount(t, 0)
}

// A deferred consumer delete stages the same way for both halves of a multi-level filter,
// and its delivery barrier is REVERSIBLE.
//
// Both halves matter because the level-up companion of a "foo/#" filter is a consumer in its
// own right: its registrations would otherwise miss the membership-removal write. And the
// barrier must lift again, because a filter that fails keeps its original QoS 1/2
// membership — the final update simply skips it — so a barrier left down would silently stop
// that subscription delivering until the client reconnected.
//
// Mutation checks: skip the barrier in stageDeferredDeleteLocked, or make
// reopenTeardownDelivery a no-op, and the assertions below fail.
func TestMQTTInflightDeferredDeleteStagingIsReversible(t *testing.T) {
	e := testNewInflightEnv(t, "client-deferred-staging")

	stage := func(t *testing.T, dur string) (*subscription, []mqttRegCancellation) {
		t.Helper()
		cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
		sub := &subscription{subject: []byte(cc.DeliverSubject), mqtt: &mqttSub{qos: 2}}
		e.c.mu.Lock()
		if e.c.subs == nil {
			e.c.subs = make(map[string]*subscription)
		}
		e.c.subs[cc.DeliverSubject] = sub
		e.c.mu.Unlock()

		sseq := e.publishSource(t, dur)
		e.sess.mu.Lock()
		e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1)) //nolint:dogsled // regPending by design
		cancels, closed, _ := e.sess.stageDeferredDeleteLocked(e.c, cc)
		e.sess.mu.Unlock()
		if len(cancels) != 1 {
			t.Fatalf("the deferral must withdraw the pending registration, got %+v", cancels)
		}
		if closed != sub {
			t.Fatalf("the deferral must close the delivery subscription, got %v", closed)
		}
		e.sess.subsMu.Lock()
		stillOpen := !sub.mqtt.closed
		e.sess.subsMu.Unlock()
		if stillOpen {
			t.Fatalf("a deferred delete must stop the consumer tracking new deliveries")
		}
		return sub, cancels
	}

	// Both halves of "foo/#" — the filter's own consumer and its level-up companion.
	mainSub, _ := stage(t, e.sess.idHash+"_"+nuid.Next())
	fwSub, _ := stage(t, e.sess.idHash+"_"+nuid.Next())

	// The filter then fails, so its membership survives and both barriers lift.
	e.sess.reopenTeardownDelivery([]*subscription{mainSub, fwSub})
	e.sess.subsMu.Lock()
	mainClosed, fwClosed := mainSub.mqtt.closed, fwSub.mqtt.closed
	e.sess.subsMu.Unlock()
	if mainClosed || fwClosed {
		t.Fatalf("a rolled-back deferral must leave both subscriptions delivering (main closed=%v, level-up closed=%v)",
			mainClosed, fwClosed)
	}
}

// A cancellation tombstone never wedges its identifier: the next delivery to allocate it
// reaps the tombstone rather than refusing.
//
// One residue can outlive its owner — where the teardown wins the race to the entry, the
// store returns without ever writing, so nothing is left to delete what the securing pass
// wrote. Since a tombstone records a delivery that was never transmitted, its identifier is
// free by construction and any later writer of that subject may reap it.
//
// Mutation check: treat a cancelled occupant like any other (drop the case) and the store is
// refused.
func TestMQTTInflightStoreReapsCancelledOccupant(t *testing.T) {
	e := testNewInflightEnv(t, "client-reap-cancelled")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)

	// A tombstone left on the identifier a fresh allocation is about to take, naming a
	// source whose consumer is gone — so nothing else could ever classify it away.
	e.sess.mu.Lock()
	pi := e.sess.bumpPI()
	e.sess.last_pi = pi - 1 // hand the same identifier to the delivery below
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(idHash, gen, pi), 0, &mqttInflightRecord{
		PI: pi, Dur: e.sess.idHash + "_" + nuid.Next(), Seq: 4, QoS: 2,
		Nonce: mqttNewInflightNonce(), Cancelled: true,
	}); err != nil {
		t.Fatalf("write the tombstone: %v", err)
	}

	got := e.track(t, dur, sseq, 2)
	if got != pi {
		t.Fatalf("the delivery must take identifier %d, got %d", pi, got)
	}
	rec, _ := e.loadRegister(t, pi)
	if rec == nil || rec.Cancelled || rec.Seq != sseq {
		t.Fatalf("the register must describe the new delivery, got %+v", rec)
	}
}

// A cancelled allocation whose register store lands after the crash is still recognised as
// never transmitted.
//
// The in-memory handoff — confirmInflightRegister finds no entry, deletes the register and
// refuses the send — covers everything except a crash. The UNSUBSCRIBE persists its removal
// and deletes the consumer while the CAS is still on the wire, so if that CAS lands and the
// broker dies before the confirmation runs, no tombstone and no owed sequence was ever
// written. Recovery then finds an ordinary register whose consumer is gone, which it
// deliberately does not read as complete, and holds the identifier and its Receive Maximum
// slot for the life of the session.
//
// What closes it is the cancellation NONCE, recorded on the session record by the cancelling
// packet itself. It is keyed by nonce and not by identifier or source because the
// cancellation frees the identifier for reuse and the source is re-offered under a new one.
//
// Mutation checks: ignore the nonce set in the recovery classification, or drop CancelledRegs
// from the persisted record, and the identifier stays reserved with its slot consumed.
func TestMQTTInflightCancelledRegisterSurvivesCrashBeforeConfirm(t *testing.T) {
	const clientID = "client-cancel-crash"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	// A real consumer and a real source message, so while they exist the register's source
	// is LIVE — which is what makes recovery reserve the identifier without the intent.
	sseq := e.publishSource(t, dur)

	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	var nonce string
	if ack := e.sess.pendingPublish[pi]; ack != nil {
		nonce = ack.regNonce
	}
	e.sess.mu.Unlock()
	if pi == 0 || !isNew || nonce == _EMPTY_ {
		t.Fatalf("trackPublish: pi=%d isNew=%v nonce=%q", pi, isNew, nonce)
	}

	// The UNSUBSCRIBE cancels the allocation inside its own record transaction and
	// persists. That order is the whole point: its consumer delete comes after the save.
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	tok := e.sess.beginRecordTx()
	e.sess.mu.Lock()
	e.sess.cons = map[string]*ConsumerConfig{sid: cc}
	e.sess.stageDrain(sid, cc, nil)
	e.sess.mu.Unlock()
	if err := e.sess.save(tok); err != nil {
		t.Fatalf("persist the unsubscribe: %v", err)
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The CAS that was already on the wire LANDS, and the broker dies before its
	// confirmation runs: nothing marks the register, and no obligation names its sequence.
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, &mqttInflightRecord{
		PI: pi, Dur: dur, Seq: sseq, QoS: 2, Nonce: nonce,
	}); err != nil {
		t.Fatalf("land the in-flight register: %v", err)
	}
	// And the UNSUBSCRIBE's consumer delete completed, so nothing can classify the source.
	if _, err := e.asm.jsa.deleteConsumer(mqttStreamName, dur, true); err != nil {
		t.Fatalf("delete the source consumer: %v", err)
	}

	// The restart: the session comes back from the record alone.
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	restored.mu.Lock()
	_, reserved := restored.pendingPublish[pi]
	inflight := restored.qos12InflightForCap()
	incomplete := restored.piSpaceIncomplete
	intents := len(restored.cancelledRegNonces)
	restored.mu.Unlock()
	if reserved {
		t.Fatalf("identifier %d was never transmitted, so it must not be reserved", pi)
	}
	if inflight != 0 {
		t.Fatalf("a never-transmitted delivery must consume no Receive Maximum slot, got %d", inflight)
	}
	if incomplete {
		t.Fatalf("a register named by a cancellation is fully accounted for; allocation must not be withheld")
	}
	if n := e.countRegisters(t); n != 0 {
		t.Fatalf("the cancelled register must be removed, %d left", n)
	}
	// And the intent converges: its register is provably gone, so nothing is left to carry.
	if intents != 0 {
		t.Fatalf("a discharged cancellation must not stay on the record, %d left", intents)
	}
}

// An obligation recorded while another writer holds the record fence still reaches the
// record, at the next convergence point.
//
// The delivery goroutine that owns a cancelled register's cleanup is refused the fence for as
// long as the UNSUBSCRIBE that cancelled it holds one — and that transaction may already have
// marshalled its record, so it commits without the entry. Treating "I attempted a save" as
// durability, or treating "nothing was discharged" as "the record already says what it needs
// to", both leave the obligation in memory only, which a restart discards.
//
// Mutation check: skip the save in retryOwedRegisterDeletes when no obligation was
// discharged, and the record never learns about it.
func TestMQTTInflightOwedRegisterDeletePersistsAfterBusyFence(t *testing.T) {
	e := testNewInflightEnv(t, "client-owed-busy-fence")

	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.mu.Unlock()

	// Another writer holds the fence, exactly as an UNSUBSCRIBE does.
	tok := e.sess.beginRecordTx()
	e.sess.oweRegisterDelete(e.c, 5150, _EMPTY_)

	e.sess.mu.Lock()
	owed := len(e.sess.owedRegisterDeletes)
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if owed != 1 {
		t.Fatalf("a refused fence must still leave the obligation pending, got %d", owed)
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The next convergence point. Its deletes still fail, so nothing is discharged — and
	// that is precisely when the record must be brought up to date.
	e.sess.retryOwedRegisterDeletes(e.c)

	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatalf("no session record")
	}
	if len(ps.OwedRegs) != 1 || ps.OwedRegs[0] != 5150 {
		t.Fatalf("the record must carry an obligation the busy fence deferred, got %v", ps.OwedRegs)
	}
}

// An obligation whose save turns out NOT to have landed is re-saved by the readback, not
// left in memory.
//
// An ambiguous write is kept and resolved later, and for this obligation "resolved: it never
// landed" is the dangerous verdict: the entry is then durable nowhere, and nothing else will
// notice — the convergence points skip a session whose list has not changed since its last
// attempt. So the retry happens inside the resolver's own transaction, which is the only
// writer allowed at that moment and the reason continuations are handed a token at all.
//
// Mutation check: leave the not-landed continuation empty and the record never gets it.
func TestMQTTInflightOwedRegisterDeleteUnlandedSaveRetries(t *testing.T) {
	e := testNewInflightEnv(t, "client-owed-unlanded")
	// So sess.seq names the record the readback will find.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	e.sess.mu.Lock()
	idHash, base := e.sess.idHash, e.sess.seq
	// An attempted write that will not match the record: the readback compares the
	// record's sequence with this base and its digest with this body, so it can only
	// conclude that nothing landed.
	e.sess.markWriteUncertainLocked(base, []byte(`{"attempted":"never-landed"}`))
	// The obligation the ambiguous save was carrying.
	e.sess.owedRegisterDeletes = append(e.sess.owedRegisterDeletes, 7370)
	e.sess.owedRegsRev++
	rev := e.sess.owedRegsRev
	e.sess.mu.Unlock()

	// Exactly what persistOwedRegisterDeletes hands the readback on saveMaybeLanded.
	e.sess.retainOnUncertain(e.sess.owedRegsUncertainActions(e.c, rev))

	res, err := e.sess.resolveUncertainWrite()
	if err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}

	// The record carries it, because the not-landed verdict re-saved it.
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatalf("no session record")
	}
	if len(ps.OwedRegs) != 1 || ps.OwedRegs[0] != 7370 {
		t.Fatalf("an unlanded obligation save must be retried by the readback, got %v", ps.OwedRegs)
	}
	e.sess.mu.Lock()
	saved := e.sess.owedRegsSavedRev
	e.sess.mu.Unlock()
	if saved != rev {
		t.Fatalf("the obligation is durable, so it must be marked saved: rev %d, saved %d", rev, saved)
	}
}

// An unproven delete keeps its obligation VISIBLE, so a concurrent session save cannot
// persist a record that has forgotten it.
//
// Emptying the list before the deletes and putting the failures back afterwards looks
// equivalent and is not: any save falling in that window writes a record with no obligations
// in it, and the entry that comes back is then in memory only.
//
// Mutation check: clear owedRegisterDeletes before the delete attempts and restore the
// failures afterwards, and the concurrent save wipes the obligation from the record.
func TestMQTTInflightOwedRegisterDeleteSurvivesConcurrentSave(t *testing.T) {
	e := testNewInflightEnv(t, "client-owed-concurrent-save")

	e.sess.oweRegisterDelete(e.c, 6260, _EMPTY_)
	e.sess.mu.Lock()
	idHash := e.sess.idHash
	e.sess.mu.Unlock()

	// The delete fails, and while it is in flight an unrelated writer saves the session
	// record — the window the old ordering exposed.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error {
		tok, ok := e.sess.tryBeginRecordTx()
		if !ok {
			t.Errorf("the retry must not hold the record fence while deleting")
			return fmt.Errorf("test: delete unavailable")
		}
		if _, serr := e.sess.saveClassified(tok); serr != nil {
			t.Errorf("concurrent save: %v", serr)
		}
		if terr := e.sess.endRecordTx(tok); terr != nil {
			t.Errorf("endRecordTx: %v", terr)
		}
		return fmt.Errorf("test: delete unavailable")
	}
	e.asm.mu.Unlock()

	e.sess.retryOwedRegisterDeletes(e.c)

	e.sess.mu.Lock()
	owed := len(e.sess.owedRegisterDeletes)
	e.sess.mu.Unlock()
	if owed != 1 {
		t.Fatalf("an unproven delete must stay owed in memory, got %d", owed)
	}
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatalf("no session record")
	}
	if len(ps.OwedRegs) != 1 || ps.OwedRegs[0] != 6260 {
		t.Fatalf("a save racing an unproven delete must still record the obligation, got %v", ps.OwedRegs)
	}
}

// A cancelled register whose confirmed delete keeps failing stays OWED, and is retried at
// the next register store rather than forgotten.
//
// Mutation check: log and drop the obligation (the original) and the register survives.
func TestMQTTInflightOwedRegisterDeleteIsRetried(t *testing.T) {
	e := testNewInflightEnv(t, "client-owed")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1900, 1))
	e.sess.mu.Unlock()

	failDeletes := true
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r.Cancelled {
			// The tombstone cannot be written either, so the obligation has nowhere durable
			// to go and must be kept in memory.
			return 0, fmt.Errorf("test: cancellation mark unavailable")
		}
		seq, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		if r.Seq == 1900 {
			// Cancel the entry while its store is in flight.
			e.sess.mu.Lock()
			e.sess.untrackPublishEntry(pi)
			e.sess.mu.Unlock()
		}
		return seq, err
	}
	e.asm.testInflightDelete = func(seq uint64) error {
		if failDeletes {
			return fmt.Errorf("test: delete unavailable")
		}
		return e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		// Indeterminate, so the mark's reconcile cannot conclude the record is gone.
		return nil, nil, fmt.Errorf("test: load unavailable")
	}
	e.asm.mu.Unlock()

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1900, _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	e.sess.mu.Lock()
	owed := len(e.sess.owedRegisterDeletes)
	e.sess.mu.Unlock()
	if owed != 1 {
		t.Fatalf("an unproven cancellation delete must stay owed, got %d", owed)
	}

	// The next register store retries it, and this time the delete works.
	failDeletes = false
	e.asm.mu.Lock()
	e.asm.testInflightStore, e.asm.testInflightLoad = nil, nil
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	pi2, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1901, 1))
	e.sess.mu.Unlock()
	if !e.sess.storeInflightRegister(e.c, pi2, 1, dur, 1901, _EMPTY_) {
		t.Fatalf("the follow-up delivery must register normally")
	}
	e.sess.mu.Lock()
	owed = len(e.sess.owedRegisterDeletes)
	e.sess.mu.Unlock()
	if owed != 0 {
		t.Fatalf("the owed delete must be retried and cleared, %d left", owed)
	}
}

// Tearing a consumer down releases its RETIRED sources too, not just its cpending
// entries.
//
// A retirement is created after untrackPublishEntry has removed the source's tracking, so
// it is not in cpending at all. A teardown that only walked cpending left the retirement
// and its register behind while deleting the consumer that could have settled them:
// nothing could then re-offer the source, and the next resume — seeing a register whose
// consumer is gone — would reserve its identifier for the life of the session.
//
// Mutation check: drop the srcAckOwed loop from purgeConsumerPendingLocked and both
// assertions fail.
func TestMQTTInflightConsumerPurgeReleasesRetiredSources(t *testing.T) {
	e := testNewInflightEnv(t, "client-purge-retired")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	regSeq := ack.regSeq
	// The state an unproven source acknowledgement leaves: no cpending entry, a
	// retirement instead.
	e.sess.untrackPublishEntry(pi)
	e.sess.retireSourceLocked(dur, sseq, &mqttRetiredSource{
		pi: pi, regSeq: regSeq, nonce: ack.regNonce, durable: true,
	})
	_, mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if mapped {
		t.Fatalf("a retired source is deliberately not in cpending; the test would prove nothing")
	}

	e.sess.mu.Lock()
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	retired := e.sess.sourceRetiredLocked(dur, sseq)
	e.sess.mu.Unlock()
	if retired {
		t.Fatalf("the retirement must go with the consumer that could have settled it")
	}
	var found bool
	for _, seq := range regs {
		if seq == regSeq {
			found = true
		}
	}
	if !found {
		t.Fatalf("the retired register's sequence %d must be returned for deletion, got %v", regSeq, regs)
	}
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)
	e.waitRegisterCount(t, 0)
}

// Resume recovery reads in BATCHES, not one synchronous request per register.
//
// This is a scalability property, and it is load-bearing rather than cosmetic: the barrier
// runs before the CONNACK, and a session may legitimately hold up to 65_535 in-flight
// identifiers. One round trip per register — plus a consumer lookup and a message read per
// record — is upwards of 100_000 serial requests in front of every reconnect, which puts
// restart recovery past ordinary client timeouts.
//
// Measured by counting requests rather than by timing: one subject enumeration, one
// consumer lookup per DURABLE however many registers name it, and no per-record message
// walk.
//
// Mutation check: revert the enumeration to a per-record loadNextMsgFromSeq walk, or drop
// the consumer cache, and the counts below exceed their bounds.
func TestMQTTInflightRecoveryReadsInBatches(t *testing.T) {
	e := testNewInflightEnv(t, "client-batched")
	const registers = 40
	dur := e.sess.idHash + "_" + nuid.Next()

	// Many registers over ONE durable, all naming live sources.
	sseq := e.publishSource(t, dur)
	for i := 0; i < registers; i++ {
		rec := &mqttInflightRecord{
			PI: uint16(i + 1), Dur: dur, Seq: sseq, QoS: 2, Nonce: mqttNewInflightNonce(),
		}
		if _, err := e.asm.jsa.storeInflightCAS(
			mqttInflightSubject(e.sess.idHash, e.sess.gen, rec.PI), 0, rec); err != nil {
			t.Fatalf("seed register %d: %v", rec.PI, err)
		}
	}
	e.forgetInMemory()

	var enumerations, nextWalks int
	e.asm.mu.Lock()
	e.asm.testInflightEnumerate = func(filter string) (map[string]uint64, error) {
		enumerations++
		return e.asm.jsa.streamSubjects(mqttInflightStreamName, filter)
	}
	e.asm.mu.Unlock()
	restore := setMQTTTestLoadNextMsgHook(func(stream, subject string, seq uint64) error {
		if stream == mqttInflightStreamName {
			nextWalks++
		}
		return nil
	})
	defer restore()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	if enumerations != 1 {
		t.Fatalf("expected exactly one subject enumeration, got %d", enumerations)
	}
	if nextWalks != 0 {
		t.Fatalf("expected no per-record register walk, got %d request(s)", nextWalks)
	}
	// And every identifier was still restored: batching must not lose records.
	e.sess.mu.Lock()
	restored := len(e.sess.pendingPublish)
	e.sess.mu.Unlock()
	// All 40 registers name the SAME source, which is the duplicate-source case: it fails
	// closed, reserving every identifier and withholding allocation.
	if restored != registers {
		t.Fatalf("expected %d identifiers to be accounted for, got %d", registers, restored)
	}
}

// A batched message-get response distinguishes "no message found" from ANY OTHER API
// error.
//
// This is the one place batching could quietly weaken the rule the whole file rests on:
// absence is proven, never inferred. A failed read read as absence classifies a source as
// complete, which drops its register and frees an identifier the client may still be
// holding — and, on the register-load path, skips a register whose identifier then goes
// unreserved. Both batched paths share this one rule so neither can drift.
//
// Mutation check: treat any resp.ToError() as absence (as the first batched implementation
// of both paths did) and the read-failure cases below fail.
func TestMQTTClassifyMsgGetResponse(t *testing.T) {
	notFound := &JSApiMsgGetResponse{ApiResponse: ApiResponse{
		Error: &ApiError{ErrCode: uint16(JSNoMessageFoundErr), Code: 404},
	}}
	if got := mqttClassifyMsgGetResponse(notFound); got != mqttInflightResponseAbsent {
		t.Fatalf("no-message-found must be absence, got %v", got)
	}

	// Every OTHER API error is a failed read, not an absence.
	for _, tc := range []struct {
		name string
		resp *JSApiMsgGetResponse
	}{
		{"nil response", nil},
		{"stream not found", &JSApiMsgGetResponse{ApiResponse: ApiResponse{
			Error: &ApiError{ErrCode: uint16(JSStreamNotFoundErr), Code: 404},
		}}},
		{"success with no message", &JSApiMsgGetResponse{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mqttClassifyMsgGetResponse(tc.resp); got != mqttInflightResponseIndeterminate {
				t.Fatalf("%s must be indeterminate, never absence, got %v", tc.name, got)
			}
		})
	}

	present := &JSApiMsgGetResponse{Message: &StoredMsg{Sequence: 7}}
	if got := mqttClassifyMsgGetResponse(present); got != mqttInflightResponsePresent {
		t.Fatalf("a returned message must be present, got %v", got)
	}
}

// The source cache never reads a consumer's absence as a completed delivery.
func TestMQTTInflightSourceCacheConsumerGoneIsUnknown(t *testing.T) {
	e := testNewInflightEnv(t, "client-batch-consumer")
	cache := e.asm.newInflightSourceCache()
	cache.known["missing_durable"] = true // looked up, found nothing
	cache.resolve([]*mqttInflightRecord{{Dur: "missing_durable", Seq: 1}})
	if got := cache.state(&mqttInflightRecord{Dur: "missing_durable", Seq: 1}); got != mqttInflightSourceUnknown {
		t.Fatalf("a source whose consumer is gone must be unknown, never complete, got %v", got)
	}
}

// A register a DRAIN owns is neither rewritten nor deleted by recovery, and its sequence
// is repaired into the drain state.
//
// Retiring it would CAS the record to a new sequence while the drain's persisted RegSeq
// still named the old one, so the drain's later release would target a sequence that no
// longer exists and the real record would survive as a phantom. Repairing the field is
// also what brings drain records written before RegSeq existed up to date.
//
// Mutation check: drop the drainOwned check and the register is rewritten (its sequence
// moves) while the drain's RegSeq is left naming the old one.
func TestMQTTInflightRecoveryLeavesDrainOwnedRegisterAlone(t *testing.T) {
	e := testNewInflightEnv(t, "client-drain-owned")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	regSeq := e.sess.pendingPublish[pi].regSeq
	e.sess.mu.Unlock()

	// A drain claiming that delivery, with NO RegSeq — the shape of a record written
	// before the field existed.
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	e.sess.mu.Lock()
	e.sess.drains = map[string]*mqttDrain{sid: {
		cc: cc, qos: 2,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true, intent: true, pi: pi}},
	}}
	e.sess.drainByDur = map[string]*mqttDrain{dur: e.sess.drains[sid]}
	e.sess.mu.Unlock()

	// A stored PUBREL for the same source, so the exchange is past PUBREC.
	e.subscribePubRel(t)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, dur, sseq, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	// The record is untouched at its ORIGINAL sequence, and unretired.
	rec, smsg := e.loadRegister(t, pi)
	if rec == nil || smsg == nil {
		t.Fatalf("a drain-owned register must not be deleted")
	}
	if smsg.Sequence != regSeq {
		t.Fatalf("the register moved from %d to %d: the drain's saved sequence is now stale", regSeq, smsg.Sequence)
	}
	if rec.Retired {
		t.Fatalf("a drain-owned register must not be retired: the drain owns its release (I2b)")
	}
	// And the drain now knows the sequence it has to release.
	e.sess.mu.Lock()
	got := e.sess.drains[sid].seqs[sseq].regSeq
	e.sess.mu.Unlock()
	if got != regSeq {
		t.Fatalf("the drain's register sequence is %d, want %d", got, regSeq)
	}
}

// An enumeration that fails for ANY reason — including a missing stream — leaves the
// identifier space incomplete.
//
// Tolerating a stream-not-found would mean "this session holds no identifiers", which is
// not what it proves: the stream is created and validated at account startup, so its
// absence is an unexpected state. Reading it as emptiness reserves nothing and lets
// allocation hand out an identifier the client may be holding. (The orphan sweep tolerates
// the same error because there it means "nothing to clean up", which is safe.)
//
// Mutation check: tolerate JSStreamNotFoundErr here and the assertion fails.
func TestMQTTInflightRecoveryMissingStreamIsIncomplete(t *testing.T) {
	e := testNewInflightEnv(t, "client-no-stream")

	e.asm.mu.Lock()
	e.asm.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return nil, ApiErrors[JSStreamNotFoundErr]
	}
	e.asm.mu.Unlock()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("a missing stream must not deny the resume: %v", err)
	}
	e.sess.mu.Lock()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if !incomplete {
		t.Fatalf("a stream that cannot be enumerated is not proof that nothing is in flight")
	}
}

// A cancelled registration's register is removed with a CONFIRMED delete.
//
// The ordinary terminal delete may be fire-and-forget because its register names a source
// that stays live and classifiable. A cancelled one does not: its consumer is usually being
// deleted in the same breath, and a register whose consumer is gone is deliberately not
// read as complete — so a lost delete reserves an identifier for the life of the session
// with nothing able to resolve it.
//
// Mutation check: use the asynchronous deleteInflightRegister here and the delete is no
// longer observed as confirmed.
func TestMQTTInflightCancelledRegisterDeleteIsConfirmed(t *testing.T) {
	e := testNewInflightEnv(t, "client-cancel-confirm")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1700, 1))
	e.sess.mu.Unlock()

	var deleted []uint64
	e.asm.mu.Lock()
	// The cancellation happens WHILE the store is in flight — the UNSUBSCRIBE race. The
	// register really lands; the entry is gone by the time its confirmation runs.
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		seq, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		e.sess.mu.Lock()
		e.sess.untrackPublishEntry(pi)
		e.sess.mu.Unlock()
		return seq, err
	}
	e.asm.testInflightDelete = func(seq uint64) error {
		deleted = append(deleted, seq)
		if len(deleted) == 1 {
			// The first attempt fails with a TRANSIENT error — not "sequence not found",
			// which means the record is already gone and is a success. A confirmed delete
			// retries this; the fire-and-forget one does not, which is the difference under
			// test.
			return fmt.Errorf("test: transient delete failure")
		}
		// Delete for real, so the absence assertion below means something.
		return e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	e.asm.mu.Unlock()

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1700, _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	if len(deleted) < 2 {
		t.Fatalf("a cancelled allocation's register delete must be retried, got %d attempt(s)", len(deleted))
	}
	// Confirmed, so it is gone by the time this returns — no polling needed, which is
	// itself the property under test.
	if n := e.countRegisters(t); n != 0 {
		t.Fatalf("a confirmed delete must leave no register behind, got %d", n)
	}
}

// One consumer lookup per durable, however many registers name it.
func TestMQTTInflightRecoveryCachesConsumerLookups(t *testing.T) {
	e := testNewInflightEnv(t, "client-cache")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)

	// Ten registers, ten DISTINCT sources, one durable.
	for i := 0; i < 10; i++ {
		rec := &mqttInflightRecord{
			PI: uint16(i + 1), Dur: dur, Seq: sseq + uint64(i), QoS: 1, Nonce: mqttNewInflightNonce(),
		}
		if _, err := e.asm.jsa.storeInflightCAS(
			mqttInflightSubject(e.sess.idHash, e.sess.gen, rec.PI), 0, rec); err != nil {
			t.Fatalf("seed register %d: %v", rec.PI, err)
		}
	}
	e.forgetInMemory()

	cache := e.asm.newInflightSourceCache()
	recs := make([]*mqttInflightRecord, 0, 10)
	for i := 0; i < 10; i++ {
		recs = append(recs, &mqttInflightRecord{Dur: dur, Seq: sseq + uint64(i)})
	}
	cache.resolve(recs)

	if len(cache.known) != 1 {
		t.Fatalf("expected one consumer lookup for one durable, got %d", len(cache.known))
	}
	if len(cache.states) != 10 {
		t.Fatalf("expected all 10 sources classified, got %d", len(cache.states))
	}
	// The first source exists; the rest do not, and with the consumer present that
	// absence proves them acknowledged.
	if got := cache.state(&mqttInflightRecord{Dur: dur, Seq: sseq}); got != mqttInflightSourceLive {
		t.Fatalf("the stored source classified as %v, want live", got)
	}
	if got := cache.state(&mqttInflightRecord{Dur: dur, Seq: sseq + 5}); got != mqttInflightSourceComplete {
		t.Fatalf("an absent source with a live consumer classified as %v, want complete", got)
	}
	// An unknown durable is Unknown, never Complete.
	if got := cache.state(&mqttInflightRecord{Dur: "never_looked_up", Seq: 1}); got != mqttInflightSourceUnknown {
		t.Fatalf("an unresolved source classified as %v, want unknown", got)
	}
}

// ---------------------------------------------------------------------------
// Teardown and the orphan sweep
// ---------------------------------------------------------------------------

// A session discard destroys the registers of the incarnation it retires: every
// identifier it held is void.
func TestMQTTInflightTeardownPurgesRegisters(t *testing.T) {
	e := testNewInflightEnv(t, "client-teardown")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.track(t, dur, 1000, 1)
	e.track(t, dur, 1001, 2)
	retiring := e.sess.gen
	if n := e.countRegisters(t); n != 2 {
		t.Fatalf("expected 2 registers, got %d", n)
	}

	if err := e.sess.clear(false); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if e.sess.gen == retiring {
		t.Fatalf("clear must re-mint the incarnation generation")
	}
	// Count against the RETIRED generation: the live one is fresh and empty.
	filter := mqttInflightWalkFilter(e.sess.idHash, retiring)
	if _, err := e.asm.jsa.loadNextMsgFromSeq(mqttInflightStreamName, filter, 1); err == nil {
		t.Fatalf("the retired incarnation's registers must be purged")
	} else if isErrorOtherThan(err, JSNoMessageFoundErr) {
		t.Fatalf("walk the retired registers: %v", err)
	}
}

// seedOrphanRegister writes a register belonging to an incarnation no record and no
// live session claims.
func (e *testInflightEnv) seedOrphanRegister(t *testing.T, pi uint16) (string, uint64) {
	t.Helper()
	orphanHash, orphanGen := getHash("client-departed"), mqttNewSessGen()
	subject := mqttInflightSubject(orphanHash, orphanGen, pi)
	rec := &mqttInflightRecord{
		PI: pi, Dur: orphanHash + "_" + nuid.Next(), Seq: 3, QoS: 1, Nonce: mqttNewInflightNonce(),
	}
	seq, err := e.asm.jsa.storeInflightCAS(subject, 0, rec)
	if err != nil {
		t.Fatalf("seed an orphaned register: %v", err)
	}
	return subject, seq
}

func (e *testInflightEnv) registerPresent(t *testing.T, subject string) bool {
	t.Helper()
	_, smsg, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil {
		t.Fatalf("load the register on %q: %v", subject, err)
	}
	return smsg != nil
}

// The reconciliation itself: an orphaned register is retired only after a STABLE
// OBSERVATION, and never on the pass that first saw it — even at a zero grace.
//
// Driven through the reconcile rather than the whole sweep, because the account
// manager's own sweep goroutine runs concurrently and would supply observations of
// its own; the wiring is covered separately below.
func TestMQTTInflightOrphanReconcile(t *testing.T) {
	e := testNewInflightEnv(t, "client-orphan")
	subject, _ := e.seedOrphanRegister(t, 12)
	zero := time.Duration(0)
	e.asm.mu.Lock()
	e.asm.testOrphanGrace = &zero
	e.asm.mu.Unlock()

	pass := func() {
		t.Helper()
		cands, err := e.asm.collectInflightCandidates(e.s)
		if err != nil {
			t.Fatalf("collectInflightCandidates: %v", err)
		}
		refs, err := e.asm.buildSessionRefs()
		if err != nil {
			t.Fatalf("buildSessionRefs: %v", err)
		}
		budget := 16
		e.asm.reconcileOrphanInflight(e.s, cands, refs, &budget)
	}

	pass()
	if !e.registerPresent(t, subject) {
		t.Fatalf("the first sighting of an orphan must not delete it")
	}
	pass()
	if e.registerPresent(t, subject) {
		t.Fatalf("an orphaned register must be retired once stably observed")
	}
}

// The wiring: a full sweep pass reaches the in-flight class and converges.
func TestMQTTInflightOrphanSweepWired(t *testing.T) {
	e := testNewInflightEnv(t, "client-orphan-wired")
	subject, _ := e.seedOrphanRegister(t, 13)
	zero := time.Duration(0)
	e.asm.mu.Lock()
	e.asm.testOrphanGrace = &zero
	e.asm.mu.Unlock()

	// Several passes, because convergence deliberately takes more than one and the
	// first valid observation authorizes nothing.
	for i := 0; i < 4 && e.registerPresent(t, subject); i++ {
		e.asm.sweepOrphanedQoS2State(e.s)
	}
	if e.registerPresent(t, subject) {
		t.Fatalf("the sweep never reached the orphaned in-flight register")
	}
}

// A register belonging to a LIVE incarnation is never touched: absence from the
// authoritative reference set is the only thing that makes one an orphan.
func TestMQTTInflightOrphanSweepSparesLiveIncarnation(t *testing.T) {
	e := testNewInflightEnv(t, "client-live")
	dur := e.sess.idHash + "_" + nuid.Next()

	pi := e.track(t, dur, 1100, 2)
	if err := e.sess.save(e.sess.beginRecordTx()); err != nil {
		t.Fatalf("save the session record: %v", err)
	}
	zero := time.Duration(0)
	e.asm.mu.Lock()
	e.asm.testOrphanGrace = &zero
	e.asm.mu.Unlock()

	for i := 0; i < 4; i++ {
		e.asm.sweepOrphanedQoS2State(e.s)
	}
	if rec, _ := e.loadRegister(t, pi); rec == nil {
		t.Fatalf("a live incarnation's register must never be swept")
	}
	// And it holds no quarantine slot, so a live account cannot fill the shared table.
	q := e.asm.quarantine()
	q.mu.Lock()
	_, held := q.entries[mqttQuarantineInflightKey(mqttInflightSubject(e.sess.idHash, e.sess.gen, pi))]
	q.mu.Unlock()
	if held {
		t.Fatalf("a referenced register must not hold a quarantine slot")
	}
}
