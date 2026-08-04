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
		Nonce: mqttNewInflightNonce(), Claim: mqttNewInflightNonce(),
		Q2AffClaim: "$MQTT.qos2.assign.G.7",
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
		{"allow rollup", func(c *StreamConfig) { c.AllowRollup = true }},
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

// peer restores through a distinct account session manager while sharing the same
// JetStream. That is the cross-broker boundary: session-object claims differ even
// though the persisted session generation is the same.
func (e *testInflightEnv) peer(t *testing.T, node, clientID string) (*mqttAccountSessionManager, *mqttSession, *client) {
	t.Helper()
	s := &Server{nrc: e.s.nrc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: node}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	t.Cleanup(func() { close(s.quitCh) })
	c := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("%s getOrCreateMQTTAccountSessionManager: %v", node, err)
	}
	sess, present, err := asm.createOrRestoreSession(clientID, s.getOpts())
	if err != nil {
		t.Fatalf("%s createOrRestoreSession: %v", node, err)
	}
	if !present {
		t.Fatalf("%s must restore the existing session", node)
	}
	return asm, sess, c
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
	if !e.sess.storeInflightRegister(e.c, pi, qos, dur, sseq, e.nonceOf(pi), _EMPTY_) {
		t.Fatalf("storeInflightRegister(%d, %s/%d) refused", pi, dur, sseq)
	}
	return pi
}

// nonceOf reads an allocation's register nonce, the identity every reservation touch is
// keyed by. Reading it at the call site mirrors what the delivery callbacks do under the
// allocating lock.
func (e *testInflightEnv) nonceOf(pi uint16) string {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if ack := e.sess.pendingPublish[pi]; ack != nil {
		return ack.regNonce
	}
	return _EMPTY_
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
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 300, e.nonceOf(pi), _EMPTY_) {
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
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, 500, e.nonceOf(pi), _EMPTY_) {
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
	if !e.sess.storeInflightRegister(e.c, pi, 1, dur, 600, e.nonceOf(pi), _EMPTY_) {
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

// publishSourceGroup is publishSource for a SHARED group consumer: the durable is the
// deterministic mqttSharedNames one and the config carries a DeliverGroup, which is the
// property buildPersistedSessionLocked excludes from the record's Cons. Returns the
// source message's stream sequence.
func (e *testInflightEnv) publishSourceGroup(t *testing.T, dur, group, filter, publish string) uint64 {
	t.Helper()
	queue, _, wantDur := mqttSharedNames(group, filter)
	if dur != wantDur {
		t.Fatalf("group durable %q is not the deterministic name for %q/%q (%q)", dur, group, filter, wantDur)
	}
	ccfg := &CreateConsumerRequest{Stream: mqttStreamName, Config: ConsumerConfig{
		Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next(), DeliverGroup: queue,
		AckPolicy: AckExplicit, DeliverPolicy: DeliverNew,
		FilterSubject: mqttStreamSubjectPrefix + filter,
	}}
	if _, err := e.asm.jsa.createDurableConsumer(ccfg); err != nil {
		t.Fatalf("create the shared source consumer %q: %v", dur, err)
	}
	resp, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+publish, -1, []byte("payload"))
	if err != nil {
		t.Fatalf("store the shared source message: %v", err)
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
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1500, e.nonceOf(pi), _EMPTY_) {
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
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1600, e.nonceOf(pi), _EMPTY_) {
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

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 2000, e.nonceOf(pi), _EMPTY_) {
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

// A teardown whose register-stream rungs both fail leaves a PERSISTED nonce verdict, and
// recovery frees the identifier through it.
//
// The ladder is delete -> tombstone -> persisted nonce, and the first two share a storage
// domain ($MQTT_inflight), so they usually fail together — which is exactly when the session
// record must carry the verdict. Without it, a crash leaves a register whose consumer is
// gone, which recovery deliberately does not read as complete: its identifier and its
// Receive Maximum slot would be reserved for the life of the session.
//
// Mutation checks: skip the verdict on the double-failure branch of deleteInflightRegisters,
// or drop CancelledRegs from the persisted record, and the identifier stays reserved.
func TestMQTTInflightTeardownLadderPersistsVerdict(t *testing.T) {
	const clientID = "client-ladder-verdict"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()
	burnDur := e.sess.idHash + "_" + nuid.Next()

	// Burn one identifier on a separate durable so a fresh allocator could not produce the
	// one under test by accident (the trap in the file header).
	sseqBurn := e.publishSource(t, burnDur)
	e.track(t, burnDur, sseqBurn, 1)
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if len(regs) != 1 || regs[0].nonce != nonce {
		t.Fatalf("the teardown must report the register of its transmitted delivery, got %+v", regs)
	}

	// The whole register stream is unavailable: neither the delete nor the tombstone lands.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.mu.Unlock()

	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)

	// The verdict is durable on the session record.
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil || len(ps.CancelledRegs) != 1 || ps.CancelledRegs[0] != nonce {
		t.Fatalf("the record must carry the verdict, got %v", ps)
	}

	// The broker restarts with the register still there; recovery frees the identifier.
	e.asm.mu.Lock()
	e.asm.testInflightDelete, e.asm.testInflightStore, e.asm.testInflightLoad = nil, nil, nil
	e.asm.mu.Unlock()
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
	if reserved {
		t.Fatalf("identifier %d was given up by the teardown and must not be reserved", pi)
	}
	e.waitRegisterCount(t, 1) // the burner's register alone
}

// A teardown whose delete fails but whose TOMBSTONE lands needs no session-record verdict,
// and the verdict set stays bounded.
//
// The tombstone is on the register itself, which is strictly stronger than a nonce recorded
// beside it — and forgetting the nonce on that proof is what keeps CancelledRegs from
// growing without bound across a client that subscribes and unsubscribes forever.
//
// Mutation checks: skip the tombstone rung, or skip the forget on its success, and the
// record still carries a verdict it no longer needs.
func TestMQTTInflightTeardownLadderTombstoneBoundsVerdicts(t *testing.T) {
	e := testNewInflightEnv(t, "client-ladder-tombstone")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	// The staging half records the verdict — this is what the proven rung must retire.
	_, transmitted := e.sess.voidRegistrationsLocked(dur)
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if len(transmitted) != 1 || transmitted[0] != nonce {
		t.Fatalf("the staging must void the transmitted register, got %v", transmitted)
	}

	// Only the DELETE fails; the tombstone CAS still works.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.mu.Unlock()

	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)

	rec, smsg := e.loadRegister(t, pi)
	if rec == nil || smsg == nil || !rec.Cancelled || rec.Nonce != nonce {
		t.Fatalf("the register must carry its cancellation tombstone, got %+v", rec)
	}
	e.sess.mu.Lock()
	verdicts := len(e.sess.cancelledRegNonces)
	e.sess.mu.Unlock()
	if verdicts != 0 {
		t.Fatalf("a tombstoned register needs no session-record verdict, %d left", verdicts)
	}
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps != nil && len(ps.CancelledRegs) != 0 {
		t.Fatalf("the record must not accumulate spent verdicts, got %v", ps.CancelledRegs)
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
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, e.nonceOf(pi), _EMPTY_) {
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
// Mutation check: log through c directly in persistVoidedRegs (or in
// deleteInflightRegisters) and this panics.
func TestMQTTInflightCleanupHelpersTolerateNoClient(t *testing.T) {
	e := testNewInflightEnv(t, "client-no-logger")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	regSeq := e.sess.pendingPublish[pi].regSeq
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()

	// Every write this path attempts fails, which is what drives it onto the log sites.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.mu.Unlock()
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		return fmt.Errorf("test: session record unavailable")
	})
	defer restore()

	// No client, as purgeSharedPending has none.
	e.sess.deleteInflightRegisters(nil, recordTx{}, []mqttVoidedReg{
		{seq: regSeq, nonce: nonce, pi: pi, dur: dur, sseq: sseq},
	})
	e.sess.secureRegCancellations(nil, recordTx{}, []mqttRegCancellation{
		{pi: pi, nonce: "nonce-x", durable: dur, sseq: sseq},
	})
	e.sess.persistVoidedRegs(nil)

	// The verdict is still there, which is the point of surviving at all.
	e.sess.mu.Lock()
	_, held := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if !held {
		t.Fatalf("the unproven removal must keep its verdict")
	}
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

// A verdict recorded while another writer holds the record fence still reaches the record,
// at the next convergence point.
//
// The delivery goroutine that owns a cancelled register's verdict is refused the fence for as
// long as the UNSUBSCRIBE that cancelled it holds one — and that transaction may already have
// marshalled its record, so it commits without the entry. Treating "I attempted a save" as
// durability would leave the verdict in memory only, which a restart discards.
//
// Mutation check: mark the revision saved on a refused fence in persistVoidedRegs, and the
// record never learns about it.
func TestMQTTInflightVoidVerdictPersistsAfterBusyFence(t *testing.T) {
	e := testNewInflightEnv(t, "client-void-busy-fence")

	// Another writer holds the fence, exactly as an UNSUBSCRIBE does, while the verdict is
	// recorded and its first save attempted.
	tok := e.sess.beginRecordTx()
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked("nonce-busy-fence")
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("a refused fence must not report the verdict durable")
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The next convergence point (the packet tail, a register store, a resume) retries.
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("the convergence point must persist the deferred verdict")
	}
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil || len(ps.CancelledRegs) != 1 || ps.CancelledRegs[0] != "nonce-busy-fence" {
		t.Fatalf("the record must carry a verdict the busy fence deferred, got %v", ps)
	}
}

// A verdict whose save turns out NOT to have landed is re-saved by the readback, not left in
// memory.
//
// An ambiguous write is kept and resolved later, and for this verdict "resolved: it never
// landed" is the dangerous outcome: the entry is then durable nowhere, and nothing else will
// notice — the convergence points skip a session whose set has not changed since its last
// attempt. So the retry happens inside the resolver's own transaction, which is the only
// writer allowed at that moment and the reason continuations are handed a token at all.
//
// Mutation check: leave the not-landed continuation empty and the record never gets it.
func TestMQTTInflightVoidVerdictUnlandedSaveRetries(t *testing.T) {
	e := testNewInflightEnv(t, "client-void-unlanded")
	// So sess.seq names the record the readback will find.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	e.sess.mu.Lock()
	idHash, base := e.sess.idHash, e.sess.seq
	// An attempted write that will not match the record: the readback compares the
	// record's sequence with this base and its digest with this body, so it can only
	// conclude that nothing landed.
	e.sess.markWriteUncertainLocked(base, []byte(`{"attempted":"never-landed"}`), 0)
	// The verdict the ambiguous save was carrying.
	e.sess.voidRegNonceLocked("nonce-unlanded")
	rev := e.sess.voidedRegsRev
	e.sess.mu.Unlock()

	// Exactly what persistVoidedRegs hands the readback on saveMaybeLanded.
	e.sess.retainOnUncertain(e.sess.voidedRegsUncertainAction(e.c), nil)

	res, err := e.sess.resolveUncertainWrite()
	if err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}

	// The record carries it, because the not-landed continuation re-saved it.
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil || len(ps.CancelledRegs) != 1 || ps.CancelledRegs[0] != "nonce-unlanded" {
		t.Fatalf("an unlanded verdict save must be retried by the readback, got %v", ps)
	}
	e.sess.mu.Lock()
	saved := e.sess.voidedRegsSavedRev
	e.sess.mu.Unlock()
	if saved != rev {
		t.Fatalf("the verdict is durable, so it must be marked saved: rev %d, saved %d", rev, saved)
	}
}

// A cancelled register whose ladder keeps failing keeps its VERDICT, and the next register
// store persists it.
//
// This is the late-cancellation window: the entry was withdrawn while its store was in
// flight, and when the store settled neither the delete nor the tombstone could be written.
// The nonce verdict is the remaining home, and the register-store convergence point is what
// carries it to the record on a session that keeps publishing.
//
// Mutation check: drop the verdict on the double-failure branch of confirmInflightRegister
// (the original logged and forgot) and the record never learns the register is void.
func TestMQTTInflightLateCancellationVerdictIsPersisted(t *testing.T) {
	e := testNewInflightEnv(t, "client-late-cancel")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1900, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	idHash := e.sess.idHash
	e.sess.mu.Unlock()

	failDeletes := true
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r.Cancelled {
			// The tombstone cannot be written either, so the verdict has nowhere durable
			// to go but the session record.
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

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1900, e.nonceOf(pi), _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	e.sess.mu.Lock()
	_, held := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if !held {
		t.Fatalf("an unproven cancellation must keep its verdict")
	}

	// The next register store persists the verdict; the register itself is reaped by
	// the next recovery scan, not here.
	failDeletes = false
	e.asm.mu.Lock()
	e.asm.testInflightStore, e.asm.testInflightLoad = nil, nil
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	pi2, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1901, 1))
	e.sess.mu.Unlock()
	if !e.sess.storeInflightRegister(e.c, pi2, 1, dur, 1901, e.nonceOf(pi2), _EMPTY_) {
		t.Fatalf("the follow-up delivery must register normally")
	}
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatalf("no session record")
	}
	found := false
	for _, n := range ps.CancelledRegs {
		if n == nonce {
			found = true
		}
	}
	if !found {
		t.Fatalf("the register-store convergence point must persist the verdict, got %v", ps.CancelledRegs)
	}
}

// A cancelled reservation is held while its register store is UNSETTLED, however durable the
// verdict already is.
//
// Even with the nonce verdict on the record, the original create-only CAS may still be in
// flight: freeing the identifier lets a new allocation take the subject, reap what it finds
// there, and then race that old CAS onto the same subject — two writers for one identifier.
// The reservation drops only at a settle point (confirm/abandon), and this test pins the
// unsettled half.
//
// Mutation check: drop the regPending guard from releaseSettledVoidReservationsLocked and
// the identifier is freed while the store is still out.
func TestMQTTInflightStalledStoreHoldsReservation(t *testing.T) {
	e := testNewInflightEnv(t, "client-stalled-store")
	dur := e.sess.idHash + "_" + nuid.Next()

	// An allocation whose store never settles in this test: allocated, then withdrawn by a
	// teardown while "in flight".
	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 3100, 1))
	cancels, _ := e.sess.voidRegistrationsLocked(dur)
	e.sess.mu.Unlock()
	if len(cancels) != 1 {
		t.Fatalf("the staging must withdraw the pending registration, got %+v", cancels)
	}

	// The verdict becomes durable — and the identifier must STILL be held.
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("the verdict save must land")
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if ack == nil || !ack.regCancelled || !ack.regPending {
		t.Fatalf("a durable verdict alone must not release an unsettled reservation, got %+v", ack)
	}
}

// A settled-but-unproven cancellation keeps its identifier until the verdict's save lands.
//
// The settle point (the store's confirmation) can find every durable rung failing — the
// register stream down for the delete AND the tombstone, the session record down for the
// verdict. Releasing the identifier there hands it to a new delivery while nothing durable
// anywhere calls the old register void; a crash then resurrects it as an ordinary delivery
// holding a Receive Maximum slot forever. The release belongs to the save that proves the
// verdict.
//
// Mutation check: release the reservation unconditionally at the settle point and the
// identifier is freed with no rung proven.
func TestMQTTInflightUnprovenSettleKeepsReservation(t *testing.T) {
	e := testNewInflightEnv(t, "client-unproven-settle")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 3200, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	cancels, _ := e.sess.voidRegistrationsLocked(dur)
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	if len(cancels) != 1 || cancels[0].nonce != nonce {
		t.Fatalf("the staging must withdraw the pending registration, got %+v", cancels)
	}

	// The CAS that was in flight LANDS...
	seq, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(idHash, gen, pi), 0, &mqttInflightRecord{
		PI: pi, Dur: dur, Seq: 3200, QoS: 2, Nonce: nonce,
	})
	if err != nil {
		t.Fatalf("land the in-flight register: %v", err)
	}
	// ...and the settle point finds every durable rung failing.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.mu.Unlock()
	restoreSave := setMQTTTestSessSaveHook(func(*mqttSession) error {
		return fmt.Errorf("test: session record unavailable")
	})
	if e.sess.confirmInflightRegister(e.c, pi, dur, 3200, seq, nonce) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if ack == nil || !ack.regCancelled {
		t.Fatalf("an unproven settle must keep the reservation, got %+v", ack)
	}
	if ack.regPending {
		t.Fatalf("the settle point must mark the store over")
	}

	// The record heals; the save that proves the verdict is what releases the identifier.
	restoreSave()
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("the verdict save must land once the record heals")
	}
	e.sess.mu.Lock()
	_, stillHeld := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if stillHeld {
		t.Fatalf("a proven verdict on a settled store must release the identifier")
	}
}

// A RETIRED register is never tombstoned and never voided: its cleanup is delete-or-leave.
//
// The register is not a claim on an identifier — it is the durable suppression of a
// completed-but-unproven QoS 2 exchange. A Cancelled rewrite (or a nonce verdict) would have
// recovery drop it without reinstating that suppression, and if the teardown crashed before
// its consumer delete the source is still re-offerable: the completed payload would go out
// again. Left alone, recovery re-reads it as a retirement and the suppression survives.
//
// Mutation check: run the tombstone rung for a retired register (or void its nonce at
// staging) and the restart loses the retirement.
func TestMQTTInflightRetiredRegisterIsNeverTombstoned(t *testing.T) {
	const clientID = "client-retired-ladder"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	regSeq, nonce := ack.regSeq, ack.regNonce
	e.sess.untrackPublishEntry(pi)
	e.sess.retireSourceLocked(dur, sseq, &mqttRetiredSource{
		pi: pi, regSeq: regSeq, nonce: nonce, durable: true,
	})
	e.sess.mu.Unlock()
	// The record's copy carries the retirement mark, as secureRetiredSource would leave it.
	if newSeq, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, regSeq, nonce, _EMPTY_); !ok {
		t.Fatalf("the retirement must be durable for this test")
	} else {
		regSeq = newSeq
		e.sess.mu.Lock()
		if rs := e.sess.srcAckOwed[mqttDeferredKey{dur, sseq}]; rs != nil {
			rs.regSeq = newSeq
		}
		e.sess.mu.Unlock()
	}

	// The teardown stages and purges; the register delete fails.
	e.sess.mu.Lock()
	_, transmitted := e.sess.voidRegistrationsLocked(dur)
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	verdicts := len(e.sess.cancelledRegNonces)
	e.sess.mu.Unlock()
	if len(transmitted) != 0 || verdicts != 0 {
		t.Fatalf("a retirement must not be voided at staging, got %v (%d verdicts)", transmitted, verdicts)
	}
	found := false
	for _, v := range regs {
		if v.seq == regSeq && v.retired {
			found = true
		}
	}
	if !found {
		t.Fatalf("the purge must report the retired register as retired, got %+v", regs)
	}
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.mu.Unlock()
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)
	// The session record exists, so the restart below resumes THIS incarnation rather than
	// minting a new generation whose register walk would find nothing.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}

	// The register survives UNCHANGED: still a retirement, not a tombstone.
	rec, _ := e.loadRegister(t, pi)
	if rec == nil || !rec.Retired || rec.Cancelled {
		t.Fatalf("a retired register must survive a failed delete unchanged, got %+v", rec)
	}

	// And a restart reinstates the suppression rather than dropping it.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = nil
	e.asm.mu.Unlock()
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	rs := restored.srcAckOwed[mqttDeferredKey{dur, sseq}]
	restored.mu.Unlock()
	if rs == nil {
		t.Fatalf("the restart must reinstate the retirement, or the completed payload is resent")
	}
}

// A surviving disconnect's shared purge reaches a session RESTORED FROM ITS RECORD.
//
// Shared group consumer configs are deliberately excluded from the persisted Cons
// (buildPersistedSessionLocked): the group is arbitrated by SharedSubs and the configs are
// rebuilt by processSubs during post-CONNACK restoration. So a session that comes back from
// its record — a broker restart, a failover, a resume elsewhere, or a CONNECT refused before
// restoration ran at all — has NO shared entry in cons, while recoverInflightRegisters has
// already rebuilt its pending entries under the deterministic group durable names. A purge
// that enumerated cons walked nothing and freed nothing: the identifiers and Receive Maximum
// slots leaked for the life of the session, and the next resume NAK-stole group messages
// AckWait would have redelivered to other members. Spec5 [4.8.2].
//
// Mutation checks: revert the collection to the cons walk and the shared entries survive the
// purge; drop the shared-prefix filter and the plain delivery is purged with them; delete the
// group consumers and the presence window fails.
func TestMQTTInflightSharedPurgeReachesRestoredSession(t *testing.T) {
	const clientID = "client-shared-purge-restored"
	e := testNewInflightEnv(t, clientID)

	// A shared subscription on a multi-level filter is TWO group durables — the main
	// "foo.>" consumer and the level-up "foo" one — as processSubs creates them.
	const group, mainFilter, lvlFilter = "g", "foo.>", "foo"
	const key = "$share/" + group + "/foo/#"
	mainQ, _, mainDur := mqttSharedNames(group, mainFilter)
	lvlQ, _, lvlDur := mqttSharedNames(group, lvlFilter)
	// ...plus a PLAIN delivery, whose pending state the purge must leave alone: it is
	// this session's own, it resumes under its own identifier, and nothing else holds it.
	plainDur := e.sess.idHash + "_" + nuid.Next()

	// QoS 1 on the group durables: a shared QoS 2 delivery is moved to the session-affine
	// durable BEFORE trackPublish, so a QoS 2 register under a group durable is not a shape
	// the broker produces.
	mainSseq := e.publishSourceGroup(t, mainDur, group, mainFilter, "foo.bar")
	lvlSseq := e.publishSourceGroup(t, lvlDur, group, lvlFilter, lvlFilter)
	plainSseq := e.publishSource(t, plainDur)
	piMain := e.track(t, mainDur, mainSseq, 1)
	piLvl := e.track(t, lvlDur, lvlSseq, 1)
	piPlain := e.track(t, plainDur, plainSseq, 2)

	// The in-memory shape of a live session: membership for the shared key, a plain
	// subscription, and configs for all three. The save below is the real exclusion.
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"bar": 2}
	e.sess.sharedSubs = map[string]byte{key: 1}
	e.sess.cons = map[string]*ConsumerConfig{
		key: {Durable: mainDur, DeliverGroup: mainQ,
			DeliverSubject: mqttSubPrefix + nuid.Next()},
		key + mqttMultiLevelSidSuffix: {Durable: lvlDur, DeliverGroup: lvlQ,
			DeliverSubject: mqttSubPrefix + nuid.Next()},
		"bar": {Durable: plainDur, DeliverSubject: mqttSubPrefix + nuid.Next()},
	}
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}

	restored, present, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if !present {
		t.Fatalf("the record must be resumed, or the register walk finds nothing")
	}
	if err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	// The bug's precondition, taken from the real persistence path rather than asserted:
	// the restored session has the membership but no shared config to walk...
	restored.mu.Lock()
	if _, ok := restored.sharedSubs[key]; !ok {
		restored.mu.Unlock()
		t.Fatalf("the record must carry the shared membership, got %v", restored.sharedSubs)
	}
	for sid, cc := range restored.cons {
		if cc.DeliverGroup != _EMPTY_ {
			restored.mu.Unlock()
			t.Fatalf("a shared config must never be persisted; the test would prove nothing (%q)", sid)
		}
	}
	// ...while all three deliveries ARE pending again, keyed by durable and sequence.
	armed := map[string]uint16{}
	for _, arm := range []struct {
		dur  string
		sseq uint64
		pi   uint16
	}{{mainDur, mainSseq, piMain}, {lvlDur, lvlSseq, piLvl}, {plainDur, plainSseq, piPlain}} {
		if _, held := restored.pendingPublish[arm.pi]; !held {
			restored.mu.Unlock()
			t.Fatalf("recovery must reserve %d for %s/%d", arm.pi, arm.dur, arm.sseq)
		}
		if got := restored.cpending[arm.dur][arm.sseq]; got != arm.pi {
			restored.mu.Unlock()
			t.Fatalf("cpending[%s][%d] = %d, want %d", arm.dur, arm.sseq, got, arm.pi)
		}
		armed[arm.dur] = arm.pi
	}
	restored.mu.Unlock()
	if len(armed) != 3 {
		t.Fatalf("all three deliveries must be armed before the purge, got %v", armed)
	}

	restored.purgeSharedPending()

	restored.mu.Lock()
	_, heldMain := restored.pendingPublish[piMain]
	_, heldLvl := restored.pendingPublish[piLvl]
	_, mainMapped := restored.cpending[mainDur]
	_, lvlMapped := restored.cpending[lvlDur]
	_, heldPlain := restored.pendingPublish[piPlain]
	plainMapped := restored.cpending[plainDur][plainSseq]
	restored.mu.Unlock()
	if heldMain || heldLvl || mainMapped || lvlMapped {
		t.Fatalf("both group durables must be freed by the purge (main held=%v mapped=%v, "+
			"level-up held=%v mapped=%v)", heldMain, mainMapped, heldLvl, lvlMapped)
	}
	if !heldPlain || plainMapped != piPlain {
		t.Fatalf("the plain delivery is this session's own and must survive: held=%v cpending=%d",
			heldPlain, plainMapped)
	}
	// The two shared registers go; the plain one stays. The poll also gives a wrongly
	// issued asynchronous consumer delete time to land before the window below. (The
	// restored session shares this incarnation's hash and generation, so the env's
	// register walk covers it.)
	e.waitRegisterCount(t, 1)
	// The group durables themselves must survive — other members hold them, and this
	// session's membership stays on the record. Checked over a window, not once, because
	// the wrong code deletes them fire-and-forget.
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		for _, dur := range []string{mainDur, lvlDur} {
			if _, err := e.asm.jsa.lookupConsumer(mqttStreamName, dur); err != nil {
				t.Fatalf("purgeSharedPending must never delete the group consumer %q: %v", dur, err)
			}
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A takeover claims each shared register before installing it. Once that claim has
// moved the register, the stale owner's close can know only the retired sequence;
// after the new owner crashes, a third recovery still finds the ORIGINAL identifier.
//
// Mutation M9: drop the recovery claim pass and the stale exact-sequence delete
// removes the register, so the final recovery cannot reserve pi.
func TestMQTTInflightClaimFencesStaleClosePurge(t *testing.T) {
	const clientID = "client-shared-claim-stale-close"
	e := testNewInflightEnv(t, clientID)
	const group, filter = "g", "claim.stale"
	_, _, dur := mqttSharedNames(group, filter)
	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	pi := e.track(t, dur, sseq, 1)
	recA, before := e.loadRegister(t, pi)
	if recA == nil || before == nil {
		t.Fatal("the stale owner must know the original register sequence")
	}
	if recA.Claim != e.sess.regClaim {
		t.Fatalf("initial register claim=%q, want writer claim %q", recA.Claim, e.sess.regClaim)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if sessB.regClaim == e.sess.regClaim {
		t.Fatalf("distinct session objects reused claim %q", sessB.regClaim)
	}
	// A real takeover saves its CONNECT-accept record before the resume barrier.
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	subject := mqttInflightSubject(sessB.idHash, sessB.gen, pi)
	claimed, after, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || claimed == nil || after == nil {
		t.Fatalf("load claimed register: rec=%+v msg=%+v err=%v", claimed, after, err)
	}
	if after.Sequence == before.Sequence || claimed.Claim != sessB.regClaim {
		t.Fatalf("takeover did not move and claim the register: before=%d after=%d claim=%q want=%q",
			before.Sequence, after.Sequence, claimed.Claim, sessB.regClaim)
	}
	sessB.mu.Lock()
	bPending := sessB.pendingPublish[pi]
	sessB.mu.Unlock()
	if bPending == nil || bPending.regSeq != after.Sequence {
		t.Fatalf("new owner did not install the claimed sequence: %+v, want %d", bPending, after.Sequence)
	}

	// Pause point: takeover recovery is complete, then the stale close proceeds.
	e.sess.purgeSharedPending()
	claimed, survived, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || claimed == nil || survived == nil {
		t.Fatalf("stale close removed the claimed register: rec=%+v msg=%+v err=%v", claimed, survived, err)
	}
	if survived.Sequence != after.Sequence || claimed.Claim != sessB.regClaim {
		t.Fatalf("stale close changed B's register: seq=%d claim=%q, want %d/%q",
			survived.Sequence, claimed.Claim, after.Sequence, sessB.regClaim)
	}
	sessB.mu.Lock()
	_, stillPending := sessB.pendingPublish[pi]
	sessB.mu.Unlock()
	if !stillPending {
		t.Fatal("the stale close mutated the new owner's in-memory pending state")
	}

	// Crash B by dropping its object without a close, then recover elsewhere.
	asmB.removeSession(sessB, true)
	_, sessC, cC := e.peer(t, "node3", clientID)
	if err := sessC.recoverInflightRegisters(cC); err != nil {
		t.Fatalf("post-crash recovery: %v", err)
	}
	sessC.mu.Lock()
	recovered := sessC.pendingPublish[pi]
	gotPI := sessC.cpending[dur][sseq]
	sessC.mu.Unlock()
	if recovered == nil || gotPI != pi {
		t.Fatalf("original identifier was not recoverable after B crashed: pending=%+v cpending=%d want=%d",
			recovered, gotPI, pi)
	}
}

// If the stale close deletes after recovery loaded the register but before its
// claim CAS, the rejected claim re-reads absence as the durable decision and does
// not resurrect the identifier.
//
// Mutation M15: classify claim-pass absence as opaque and pi remains reserved.
func TestMQTTInflightClaimConsumesEarlierClose(t *testing.T) {
	const clientID = "client-shared-claim-close-first"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.close.first")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.close.first", "claim.close.first")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}

	claimAttempts := 0
	asmB.mu.Lock()
	asmB.testInflightStore = func(subject string, expectSeq uint64, rec *mqttInflightRecord) (uint64, error) {
		if rec.Claim != sessB.regClaim || !strings.HasPrefix(rec.Dur, mqttSharedConsumerDurablePrefix) {
			return asmB.jsa.storeInflightCAS(subject, expectSeq, rec)
		}
		claimAttempts++
		// Recovery has already loaded and classified the record. Let the stale close
		// consume that exact generation before the claim reaches JetStream.
		e.sess.purgeSharedPending()
		return asmB.jsa.storeInflightCAS(subject, expectSeq, rec)
	}
	asmB.mu.Unlock()
	if err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	if claimAttempts != 1 {
		t.Fatalf("the close-before-claim race was not armed exactly once, attempts=%d", claimAttempts)
	}
	sessB.mu.Lock()
	_, held := sessB.pendingPublish[pi]
	_, mapped := sessB.cpending[dur][sseq]
	sessB.mu.Unlock()
	if held || mapped {
		t.Fatalf("recovery resurrected a close-consumed identifier: held=%v mapped=%v", held, mapped)
	}
}

// When the stale close's exact delete fails, its nonce-keyed tombstone reconcile
// must stop at B's foreign claim. No tombstone and no session-record verdict may
// survive that lost arbitration.
//
// Mutation M10: allow the rung-2 re-target on nonce alone and B's record is
// rewritten Cancelled.
func TestMQTTInflightClaimFencesCancellationReconcile(t *testing.T) {
	const clientID = "client-shared-claim-cancel-gate"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.cancel")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.cancel", "claim.cancel")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}

	deleteAttempts, tombstoneAttempts := 0, 0
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error {
		deleteAttempts++
		return fmt.Errorf("test: stale delete unavailable")
	}
	e.asm.testInflightStore = func(subject string, expectSeq uint64, rec *mqttInflightRecord) (uint64, error) {
		tombstoneAttempts++
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, rec)
	}
	e.asm.mu.Unlock()
	e.sess.purgeSharedPending()
	if deleteAttempts == 0 || tombstoneAttempts != 1 {
		t.Fatalf("the rung-2 race was not armed: deletes=%d tombstones=%d", deleteAttempts, tombstoneAttempts)
	}

	rec, _, err := asmB.jsa.loadInflightRecord(mqttInflightSubject(sessB.idHash, sessB.gen, pi))
	if err != nil || rec == nil {
		t.Fatalf("load B register: rec=%+v err=%v", rec, err)
	}
	if rec.Claim != sessB.regClaim || rec.Cancelled {
		t.Fatalf("stale rung 2 modified B's decision: %+v", rec)
	}
	e.sess.mu.Lock()
	verdicts := len(e.sess.cancelledRegNonces)
	e.sess.mu.Unlock()
	if verdicts != 0 {
		t.Fatalf("lost arbitration reached the rung-3 verdict, got %d", verdicts)
	}
}

// A stalled first store may have landed before takeover, but its stale owner must
// not learn the claimed sequence from nonce reconciliation.
//
// Mutation M12: adopt on nonce alone and A confirms B's sequence for sending.
func TestMQTTInflightClaimFencesStoreReconcile(t *testing.T) {
	const clientID = "client-shared-claim-store-gate"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.store")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.store", "claim.store")
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	_, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}

	e.sess.mu.Lock()
	pi, _, _, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	storeAttempts := 0
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, rec *mqttInflightRecord) (uint64, error) {
		storeAttempts++
		if _, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, rec); err != nil {
			return 0, err
		}
		if err := sessB.recoverInflightRegisters(cB); err != nil {
			t.Fatalf("takeover recovery inside stalled store: %v", err)
		}
		return 0, fmt.Errorf("test: response lost after takeover claimed the write")
	}
	e.asm.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, sseq, nonce, _EMPTY_) {
		t.Fatal("stale store adopted the new owner's claimed sequence")
	}
	if storeAttempts != 1 {
		t.Fatalf("the ambiguous store race was not armed exactly once, attempts=%d", storeAttempts)
	}
	e.sess.mu.Lock()
	_, stalePending := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if stalePending {
		t.Fatal("stale allocation remained armed after losing register ownership")
	}
	sessB.mu.Lock()
	bPending := sessB.pendingPublish[pi]
	sessB.mu.Unlock()
	if bPending == nil || bPending.regSeq == 0 {
		t.Fatalf("takeover did not retain the allocation: %+v", bPending)
	}
}

// A last-gasp QoS 2 completion from the stale object cannot retire or adopt the
// sequence of a shared register claimed during takeover.
//
// Mutation M13: ignore the claim in retirement reconcile and the record is
// rewritten Retired (or its sequence adopted).
func TestMQTTInflightClaimFencesRetirementReconcile(t *testing.T) {
	const clientID = "client-shared-claim-retire-gate"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.retire")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.retire", "claim.retire")
	pi := e.track(t, dur, sseq, 1)
	recA, msgA := e.loadRegister(t, pi)
	if recA == nil || msgA == nil {
		t.Fatal("the stale retirement must start with an exact register identity")
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	if seq, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, msgA.Sequence, recA.Nonce, _EMPTY_); ok || seq != 0 {
		t.Fatalf("stale retirement unexpectedly succeeded at %d", seq)
	}
	rec, _, err := asmB.jsa.loadInflightRecord(mqttInflightSubject(sessB.idHash, sessB.gen, pi))
	if err != nil || rec == nil || rec.Claim != sessB.regClaim || rec.Retired {
		t.Fatalf("stale retirement modified B's register: rec=%+v err=%v", rec, err)
	}
}

// The create-first cancellation path has no sequence of its own. A nonce match is
// still insufficient to re-target it once recovery has established a foreign claim.
//
// Mutation M14: ignore the claim in pending-tombstone reconcile and B's live
// register becomes Cancelled.
func TestMQTTInflightClaimFencesPendingCancellation(t *testing.T) {
	const clientID = "client-shared-claim-pending-gate"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.pending")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.pending", "claim.pending")
	pi := e.track(t, dur, sseq, 1)
	recA, _ := e.loadRegister(t, pi)
	if recA == nil {
		t.Fatal("the stale cancellation must start with the register nonce")
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	rc := mqttRegCancellation{pi: pi, nonce: recA.Nonce, durable: dur, sseq: sseq}
	if err := e.sess.markInflightRegisterCancelledPending(e.c, rc); err != nil {
		t.Fatalf("lost arbitration is a completed cancellation outcome: %v", err)
	}
	rec, _, err := asmB.jsa.loadInflightRecord(mqttInflightSubject(sessB.idHash, sessB.gen, pi))
	if err != nil || rec == nil || rec.Claim != sessB.regClaim || rec.Cancelled {
		t.Fatalf("stale pending cancellation modified B's register: rec=%+v err=%v", rec, err)
	}
}

// The same-delivery replacement is a rewrite of a shared register too, and it is the one
// the nonce-match gates cannot cover: the occupant carries the OTHER writer's nonce.
//
// The three-owner interleaving: A's register N1 exists; A also holds a pending write N2
// for the same identifier and source (the restart shape — the register landed, the memory
// was lost, the re-offered source allocated the same identifier); B resumes and claims N1,
// moving it. A's create fails, and its re-read finds a record whose nonce is N1, not N2 —
// so every nonce-keyed gate is skipped — while the same-(Dur,Seq) branch would take the
// slot over AT THE CLAIMED SEQUENCE, overwriting B's claim. A's close then deletes the
// exchange's only durable identity, and after B crashes a third resume re-offers the
// delivery under a fresh identifier ([MQTT-4.4.0-1]).
//
// Mutation check: drop the claim condition from the same-delivery replacement branch and
// this test fails on B's register being rewritten.
func TestMQTTInflightClaimFencesSameDeliveryReplacement(t *testing.T) {
	const clientID = "client-shared-claim-replace-gate"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.replace")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.replace", "claim.replace")
	pi := e.track(t, dur, sseq, 1)
	recA, _ := e.loadRegister(t, pi)
	if recA == nil {
		t.Fatal("A's first register must exist")
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	subject := mqttInflightSubject(sessB.idHash, sessB.gen, pi)
	claimed, claimedMsg, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || claimed == nil || claimedMsg == nil {
		t.Fatalf("load claimed register: rec=%+v err=%v", claimed, err)
	}

	// A's second allocation for the same identifier and source: a fresh nonce, so the
	// occupant's nonce (N1) matches NEITHER a nonce gate nor A's pending write (N2) —
	// the armed precondition of the different-nonce branch.
	n2 := mqttNewInflightNonce()
	if n2 == recA.Nonce {
		t.Fatal("the second allocation must carry a different nonce")
	}
	if claimed.Nonce != recA.Nonce || claimed.Claim == e.sess.regClaim {
		t.Fatalf("the occupant must be A's allocation under B's claim: %+v", claimed)
	}
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{
		jsDur: dur, sseq: sseq, regPending: true, regNonce: n2,
	}
	e.sess.mu.Unlock()

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, sseq, n2, _EMPTY_) {
		t.Fatal("a stale same-delivery store must lose the arbitration, not take the slot")
	}
	rec, msg, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil || msg == nil {
		t.Fatalf("B's claimed register must survive: rec=%+v err=%v", rec, err)
	}
	if msg.Sequence != claimedMsg.Sequence || rec.Nonce != recA.Nonce || rec.Claim != sessB.regClaim {
		t.Fatalf("stale replacement rewrote B's claim: seq=%d nonce=%q claim=%q, want %d/%q/%q",
			msg.Sequence, rec.Nonce, rec.Claim, claimedMsg.Sequence, recA.Nonce, sessB.regClaim)
	}
	e.sess.mu.Lock()
	_, staleHeld := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if staleHeld {
		t.Fatal("the losing allocation must be abandoned")
	}

	// The three-owner ending: A's close purges nothing further, B crashes, and a third
	// resume still recovers the ORIGINAL identifier.
	e.sess.purgeSharedPending()
	asmB.removeSession(sessB, true)
	_, sessC, cC := e.peer(t, "node3", clientID)
	if err := sessC.recoverInflightRegisters(cC); err != nil {
		t.Fatalf("third-owner recovery: %v", err)
	}
	sessC.mu.Lock()
	recovered := sessC.pendingPublish[pi]
	gotPI := sessC.cpending[dur][sseq]
	sessC.mu.Unlock()
	if recovered == nil || gotPI != pi {
		t.Fatalf("original identifier was not recoverable by the third owner: pending=%+v cpending=%d want=%d",
			recovered, gotPI, pi)
	}
}

// Duplicate-source registers are claimed BEFORE the group fails closed, or the
// reservation protects nothing.
//
// Two registers naming one source is the shape recovery refuses to guess about: nothing
// can say which identifier the client holds, so every member is reserved and allocation
// is withheld. But a reservation is memory; the registers behind it are the durable
// state, and if they still sit at sequences a stale close knows, the close deletes one —
// after which the next resume sees a SINGLE register and installs it as authoritative,
// resolving by deletion the very ambiguity the group refused to guess about. If the
// client held the deleted identifier, the source resumes under the other one:
// [MQTT-4.4.0-1] broken, and for QoS 2 a possible duplicate exchange. So the claim pass
// runs on every readable live shared register before the duplicate grouping, moving
// every member of the group.
//
// Mutation check: skip claiming the live list (claim only reserve/retired, or nothing)
// and the stale purge below removes R1 — the register count and C's fail-closed state
// both fail.
func TestMQTTInflightClaimMovesDuplicateSourceRegisters(t *testing.T) {
	const clientID = "client-shared-claim-duplicate"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.duplicate")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.duplicate", "claim.duplicate")
	pi1 := e.track(t, dur, sseq, 1)
	// The superseded-broker shape: a second register for the SAME source on a different
	// identifier, written elsewhere with its own nonce.
	pi2 := pi1 + 1
	e.sess.mu.Lock()
	staleClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	if _, err := e.asm.jsa.storeInflightCAS(
		mqttInflightSubject(e.sess.idHash, e.sess.gen, pi2), 0, &mqttInflightRecord{
			PI: pi2, Dur: dur, Seq: sseq, QoS: 1, Nonce: mqttNewInflightNonce(), Claim: staleClaim,
		}); err != nil {
		t.Fatalf("seed the duplicate register: %v", err)
	}
	r1Before, m1Before := e.loadRegister(t, pi1)
	if r1Before == nil || m1Before == nil {
		t.Fatal("R1 must exist before the takeover")
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	if err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	// Armed preconditions: B failed closed on the pair, AND both registers moved under
	// B's claim — the group's durable state no longer sits at sequences A knows.
	sessB.mu.Lock()
	_, b1 := sessB.pendingPublish[pi1]
	_, b2 := sessB.pendingPublish[pi2]
	_, bMapped := sessB.cpending[dur][sseq]
	bIncomplete := sessB.piSpaceIncomplete
	sessB.mu.Unlock()
	if !b1 || !b2 || bMapped || !bIncomplete {
		t.Fatalf("B must fail closed on the duplicate pair: pi1=%v pi2=%v mapped=%v incomplete=%v",
			b1, b2, bMapped, bIncomplete)
	}
	for _, pi := range []uint16{pi1, pi2} {
		rec, msg, err := asmB.jsa.loadInflightRecord(mqttInflightSubject(sessB.idHash, sessB.gen, pi))
		if err != nil || rec == nil || msg == nil {
			t.Fatalf("load claimed duplicate %d: rec=%+v err=%v", pi, rec, err)
		}
		if rec.Claim != sessB.regClaim {
			t.Fatalf("duplicate member %d was not claimed: claim=%q want %q", pi, rec.Claim, sessB.regClaim)
		}
		if pi == pi1 && msg.Sequence == m1Before.Sequence {
			t.Fatalf("R1 did not move: still at %d", m1Before.Sequence)
		}
	}

	// The stale close proceeds; its exact-sequence delete must remove nothing.
	e.sess.purgeSharedPending()
	e.waitRegisterCount(t, 2)

	// B crashes; the third owner must still observe the fail-closed ambiguity, not a
	// single "authoritative" survivor.
	asmB.removeSession(sessB, true)
	_, sessC, cC := e.peer(t, "node3", clientID)
	if err := sessC.recoverInflightRegisters(cC); err != nil {
		t.Fatalf("third-owner recovery: %v", err)
	}
	sessC.mu.Lock()
	_, c1 := sessC.pendingPublish[pi1]
	_, c2 := sessC.pendingPublish[pi2]
	_, cMapped := sessC.cpending[dur][sseq]
	cIncomplete := sessC.piSpaceIncomplete
	sessC.mu.Unlock()
	if !c1 || !c2 || cMapped || !cIncomplete {
		t.Fatalf("the ambiguity must survive the stale close: pi1=%v pi2=%v mapped=%v incomplete=%v",
			c1, c2, cMapped, cIncomplete)
	}
}

// A SPLIT claim on a duplicate pair fails the whole group closed, not just the loser.
//
// Two takeover objects can race the same duplicate pair: this resumer claims one member
// while a concurrent resumer claims the other. The unclaimed member is not this session's
// to decide about — but it still NAMES the same source, and if its sibling were installed
// as an authoritative singleton, the duplicate would be resolved by exactly the guess the
// group refuses to make: the client may hold the identifier behind the unclaimed member,
// and retransmitting under the sibling breaks [MQTT-4.4.0-1] and can duplicate a QoS 2
// exchange. So a claim-failed LIVE member poisons its source's whole group: every member
// is reserved, none is installed, allocation is withheld.
//
// Mutation check: convert claim-failed members to bodyless reservations before the
// grouping (the pre-fix shape) and the sibling installs — the cpending assertion fails.
func TestMQTTInflightClaimSplitDuplicateFailsClosed(t *testing.T) {
	const clientID = "client-shared-claim-split"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.split")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.split", "claim.split")
	pi1 := e.track(t, dur, sseq, 1)
	pi2 := pi1 + 1
	e.sess.mu.Lock()
	staleClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	if _, err := e.asm.jsa.storeInflightCAS(
		mqttInflightSubject(e.sess.idHash, e.sess.gen, pi2), 0, &mqttInflightRecord{
			PI: pi2, Dur: dur, Seq: sseq, QoS: 1, Nonce: mqttNewInflightNonce(), Claim: staleClaim,
		}); err != nil {
		t.Fatalf("seed the duplicate register: %v", err)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	asmC, sessC, cC := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessC); err != nil {
		t.Fatalf("persist takeover: %v", err)
	}
	// One member's record becomes UNREADABLE between C's load and C's claim CAS — the
	// same load-to-CAS window TestMQTTInflightClaimConsumesEarlierClose arms for the
	// close. C's re-read then finds a record it cannot attribute: fail-closed, and the
	// whole group with it.
	splitArmed := 0
	subj1 := mqttInflightSubject(sessC.idHash, sessC.gen, pi1)
	asmC.mu.Lock()
	asmC.testInflightStore = func(subject string, expectSeq uint64, rec *mqttInflightRecord) (uint64, error) {
		if subject != subj1 || rec.Claim != sessC.regClaim {
			return asmC.jsa.storeInflightCAS(subject, expectSeq, rec)
		}
		splitArmed++
		if _, err := asmC.jsa.storeMsg(subject, -1, []byte("not-a-register")); err != nil {
			t.Fatalf("the corrupt write must land first: %v", err)
		}
		return asmC.jsa.storeInflightCAS(subject, expectSeq, rec)
	}
	asmC.mu.Unlock()
	if err := sessC.recoverInflightRegisters(cC); err != nil {
		t.Fatalf("split-claim recovery: %v", err)
	}
	if splitArmed != 1 {
		t.Fatalf("the split-claim race was not armed exactly once, attempts=%d", splitArmed)
	}

	sessC.mu.Lock()
	_, c1 := sessC.pendingPublish[pi1]
	_, c2 := sessC.pendingPublish[pi2]
	_, cMapped := sessC.cpending[dur][sseq]
	cIncomplete := sessC.piSpaceIncomplete
	sessC.mu.Unlock()
	if !c1 || !c2 {
		t.Fatalf("both identifiers must stay reserved under a split claim: pi1=%v pi2=%v", c1, c2)
	}
	if cMapped {
		t.Fatal("a split-claimed duplicate must not install its sibling as authoritative")
	}
	if !cIncomplete {
		t.Fatal("a split-claimed duplicate must withhold new allocation")
	}
	// Both records survive: pi1 unreadable and untouched, pi2 under C's claim.
	e.waitRegisterCount(t, 2)
	rec1, msg1, err := asmC.jsa.loadInflightRecord(subj1)
	if err != nil || msg1 == nil || rec1 != nil {
		t.Fatalf("the unattributable record must stand: rec=%+v msg=%+v err=%v", rec1, msg1, err)
	}
}

// A LIVE rival's claim observed mid-barrier refuses the resume outright.
//
// A readable record carrying this allocation's nonce under another object's claim is
// durable displacement proof: the rival claims only after its own accept save, so its
// record write is both durable and later than ours — and the shape is only observable
// against an ACTIVELY racing rival, since a dead rival's standing claim is re-claimed
// by the ordinary CAS at the loaded sequence. Completing the resume would bind a client
// to a superseded session holding foreign reservations: QoS 2 exchanges declined
// forever, allocation withheld, and only a missable takeover notification left to evict
// it. So the barrier aborts, nothing installs, and the CONNECT is refused.
//
// Mutation check: classify the displaced result as opaque instead and the error
// assertion fails (the resume completes with a foreign reservation).
func TestMQTTInflightClaimDisplacedResumeRefused(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-claim-displaced")
	_, _, dur := mqttSharedNames("g", "claim.displaced")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.displaced", "claim.displaced")
	pi := e.track(t, dur, sseq, 1)
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	previous := *rec
	previous.Claim = "previous-remote-owner"
	subj := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subj, msg.Sequence, &previous); err != nil {
		t.Fatalf("seed the previous owner's claim: %v", err)
	}

	const rivalClaim = "rival-resumer-claim"
	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if subject != subj || r.Claim != ownClaim {
			return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		rival := *r
		rival.Claim = rivalClaim
		if _, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, &rival); err != nil {
			t.Fatalf("the rival's claim must land first: %v", err)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()

	if err := e.sess.recoverInflightRegisters(e.c); err == nil {
		t.Fatal("a resume that observed an active rival's claim must be refused")
	}
	// Nothing was installed or demoted: the pre-existing live entry stands untouched
	// (the refused CONNECT is what removes this object from service), and the rival's
	// register and claim stand.
	e.sess.mu.Lock()
	entry := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if entry == nil || entry.regForeign || entry.jsDur != dur {
		t.Fatalf("a refused resume must not mutate the entry, got %+v", entry)
	}
	after, _, err := e.asm.jsa.loadInflightRecord(subj)
	if err != nil || after == nil || after.Claim != rivalClaim {
		t.Fatalf("the rival's claim must stand: rec=%+v err=%v", after, err)
	}
}

// A NEWER accept deterministically supersedes an OLDER owner's standing claim.
//
// Two simultaneous resumers can split a pair of registers; if each read the other's
// claim as displacement, both would abort and retry forever — the claim nonce alone is
// unordered. Accept saves give the order: record CAS makes every accept sequence
// unique, so the contest has exactly one newer side, and that side re-targets and takes
// every contested register while the older side reads displacement. Here the resumed
// object's accept is newer than the standing claim, so the resume must SUCCEED and own
// the register.
//
// Mutation check: treat every foreign live claim as displacement regardless of order
// (the pre-fix shape) and the resume is refused — the success assertion fails.
func TestMQTTInflightClaimSupersedesOlderOwner(t *testing.T) {
	const clientID = "client-shared-claim-supersede"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.supersede")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.supersede", "claim.supersede")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	// The record carries a previous remote owner's claim so the re-run must contest it.
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	previous := *rec
	previous.Claim = "previous-remote-owner"
	subj := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subj, msg.Sequence, &previous); err != nil {
		t.Fatalf("seed the previous owner's claim: %v", err)
	}
	e.sess.mu.Lock()
	ourSeq := e.sess.seq
	ownClaimAtArm := e.sess.regClaim
	e.sess.mu.Unlock()
	if ourSeq == 0 {
		t.Fatal("the resumed object's accept sequence must be newer than the rival's")
	}

	// The OLDER accepted owner's claim lands INSIDE the claim window — the only place
	// a contest exists; a standing claim outside the window is simply re-claimed by
	// the ordinary CAS at the loaded sequence.
	contested := 0
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if subject != subj || r.Claim != ownClaimAtArm || contested > 0 {
			return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		contested++
		older := *r
		older.Claim = "older-accepted-owner"
		older.ClaimSeq = 0
		if _, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, &older); err != nil {
			t.Fatalf("the older owner's claim must land first: %v", err)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("a newer accept must supersede an older owner's claim, got refusal: %v", err)
	}
	e.asm.mu.Lock()
	e.asm.testInflightStore = nil
	e.asm.mu.Unlock()
	after, _, err := e.asm.jsa.loadInflightRecord(subj)
	if err != nil || after == nil {
		t.Fatalf("load contested register: rec=%+v err=%v", after, err)
	}
	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	entry := e.sess.pendingPublish[pi]
	mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if after.Claim != ownClaim || after.ClaimSeq != ourSeq {
		t.Fatalf("the newer accept must own the register: claim=%q seq=%d want %q/%d",
			after.Claim, after.ClaimSeq, ownClaim, ourSeq)
	}
	if entry == nil || mapped != pi {
		t.Fatalf("the superseding owner must keep the delivery: entry=%+v mapped=%d", entry, mapped)
	}
}

// A chain of OLDER claims cannot strand the newest owner in an opaque reservation.
//
// The contest budget is fixed while ordered contenders are not: without the exhaustion
// rule, the newest accepted owner could fail its CAS three times behind three successive
// older takeovers and exit as an opaque reservation — the rightful owner completing
// CONNECT with its allocation withheld indefinitely. The contest instead keeps taking
// over strictly-older claims (each rival stops permanently at its first sight of a newer
// one, so the chain is finite), and if the generous budget somehow exhausts it reads
// DISPLACED — a refused resume whose retry meets a settled contest — never opaque.
//
// Mutation check: cap the contest at three attempts with exhaustion falling to opaque
// (the pre-fix shape) and the ownership assertions fail.
func TestMQTTInflightClaimOutlastsOlderClaimChain(t *testing.T) {
	const clientID = "client-shared-claim-chain"
	e := testNewInflightEnv(t, clientID)
	_, _, dur := mqttSharedNames("g", "claim.chain")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.chain", "claim.chain")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	previous := *rec
	previous.Claim = "previous-remote-owner"
	subj := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subj, msg.Sequence, &previous); err != nil {
		t.Fatalf("seed the previous owner's claim: %v", err)
	}
	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	ourSeq := e.sess.seq
	e.sess.mu.Unlock()
	if ourSeq == 0 {
		t.Fatal("the resumed object's accept sequence must dominate the chain")
	}

	// Three successive OLDER accepted owners land their claims ahead of each of our
	// first three attempts — the exact chain that exhausted a three-attempt budget.
	interventions := 0
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if subject != subj || r.Claim != ownClaim || interventions >= 3 {
			return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		interventions++
		older := *r
		older.Claim = fmt.Sprintf("older-owner-%d", interventions)
		older.ClaimSeq = 0
		if _, err := e.asm.jsa.storeInflightCAS(subject, expectSeq, &older); err != nil {
			t.Fatalf("older owner %d must land first: %v", interventions, err)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()

	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("the newest owner must outlast the older-claim chain, got refusal: %v", err)
	}
	if interventions != 3 {
		t.Fatalf("the chain was not armed: interventions=%d", interventions)
	}
	after, _, err := e.asm.jsa.loadInflightRecord(subj)
	if err != nil || after == nil || after.Claim != ownClaim || after.ClaimSeq != ourSeq {
		t.Fatalf("the newest owner must hold the register: rec=%+v err=%v", after, err)
	}
	e.sess.mu.Lock()
	entry := e.sess.pendingPublish[pi]
	mapped := e.sess.cpending[dur][sseq]
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if entry == nil || entry.regForeign || mapped != pi || incomplete {
		t.Fatalf("the newest owner must keep the delivery with allocation open: entry=%+v mapped=%d incomplete=%v",
			entry, mapped, incomplete)
	}
}

// A terminal path racing a same-object rebind cannot consume a Receive Maximum slot
// forever.
//
// The acknowledgement's two halves are split by the session lock: untracking happens
// under it, the register delete and source ack after it. A rebind's barrier re-run in
// that window sees a live register over a live source and legitimately reinstalls the
// entry — the exchange IS outstanding from the durable view if the broker crashes here.
// What must not happen is the paused durable half then completing the exchange
// invisibly: without the claim rewrite it deletes the register at its unchanged
// sequence, nothing ever clears the reinstalled entry, and the identifier counts
// against Receive Maximum forever, across every same-object rebind. The rewrite moves
// the sequence (the paused delete no-ops), and the next barrier run consumes the
// completed source against memory.
//
// Mutation checks: restore the own-claim fast path (skip the rewrite) and the paused
// delete lands — the final assertions fail; skip the complete-source memory
// reconciliation and the entry survives run 2 — the same assertions fail.
func TestMQTTInflightClaimTerminalRaceHealsPhantomEntry(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-claim-terminal-race")
	_, _, dur := mqttSharedNames("g", "claim.terminal.race")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.terminal.race", "claim.terminal.race")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	// The PUBACK's first half: untracked under the lock, durable half paused.
	e.sess.mu.Lock()
	removed := e.sess.untrackPublishEntry(pi)
	e.sess.mu.Unlock()
	if removed == nil || removed.regSeq == 0 {
		t.Fatalf("the terminal path must hold the pre-rewrite sequence, got %+v", removed)
	}

	// The rewrite's FIRST attempt fails without writing: the re-read then finds the
	// own-claimed record at the UNMOVED sequence, which proves nothing about the write
	// — the claim pass must retry until the sequence actually moves rather than adopt
	// the very target the paused delete holds.
	flaked := 0
	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r.Claim == ownClaim && strings.HasPrefix(r.Dur, mqttSharedConsumerDurablePrefix) && flaked == 0 {
			flaked++
			return 0, fmt.Errorf("test: claim response lost without landing")
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()

	// The rebind's barrier re-run: the register is legitimately reinstalled — and its
	// sequence must have MOVED out from under the paused delete.
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("same-object barrier re-run: %v", err)
	}
	e.sess.mu.Lock()
	entry := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if entry == nil || entry.regSeq == 0 || entry.regSeq == removed.regSeq {
		t.Fatalf("the reinstalled entry must carry the rewritten sequence: entry=%+v paused=%d",
			entry, removed.regSeq)
	}
	if flaked != 1 {
		t.Fatalf("the non-landing first attempt was not armed, flaked=%d", flaked)
	}
	e.asm.mu.Lock()
	e.asm.testInflightStore = nil
	e.asm.mu.Unlock()

	// The durable half releases: the delete aims at the moved-away sequence and the
	// source ack completes the exchange.
	e.sess.deleteInflightRegisterFor(removed)
	if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
		t.Fatalf("complete the source: %v", err)
	}
	e.waitRegisterCount(t, 1)

	// The next barrier run consumes the completed source against memory.
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("healing barrier re-run: %v", err)
	}
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	_, mapped := e.sess.cpending[dur][sseq]
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if held || mapped || incomplete {
		t.Fatalf("the finished exchange must release its slot: held=%v mapped=%v incomplete=%v",
			held, mapped, incomplete)
	}
	e.waitRegisterCount(t, 0)
}

// A claim fence that never lands refuses the resume — no reservation at an unfenced
// sequence.
//
// If every claim CAS fails without landing, the register never moves, and an opaque
// reservation would sit at exactly the sequence a paused terminal delete may hold: the
// delete-and-ack then completes the exchange with no durable record left for a later
// barrier to reconcile, and the bodyless reservation consumes a Receive Maximum slot
// forever. Exhaustion therefore refuses the resume; the retry meets a working store, and
// the paused terminal half completes an exchange nothing has resurrected.
//
// Mutation check: let exhaustion fall to an opaque reservation (the pre-fix shape) and
// the refusal assertion fails.
func TestMQTTInflightClaimUnfencedResumeRefused(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-claim-unfenced")
	_, _, dur := mqttSharedNames("g", "claim.unfenced")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.unfenced", "claim.unfenced")
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	// The terminal path unpacks under the lock and pauses before its durable half.
	e.sess.mu.Lock()
	removed := e.sess.untrackPublishEntry(pi)
	ownClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	if removed == nil || removed.regSeq == 0 {
		t.Fatalf("the terminal path must hold a sequence, got %+v", removed)
	}

	// Every claim write fails without landing: the fence never moves the register.
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r.Claim == ownClaim && strings.HasPrefix(r.Dur, mqttSharedConsumerDurablePrefix) {
			return 0, fmt.Errorf("test: claim store unavailable")
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	if err := e.sess.recoverInflightRegisters(e.c); err == nil {
		t.Fatal("a resume whose claim fence never landed must be refused")
	}
	e.asm.mu.Lock()
	e.asm.testInflightStore = nil
	e.asm.mu.Unlock()

	// Nothing was reinstalled, so the paused terminal half completes an exchange
	// nothing resurrected: no lasting reservation, no leaked register.
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if held {
		t.Fatal("a refused resume must not install a reservation at an unfenced sequence")
	}

	// The stronger outage: the claim write AND its verification re-read both fail —
	// sequence movement cannot be proven either way, and the refusal must hold.
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r.Claim == ownClaim && strings.HasPrefix(r.Dur, mqttSharedConsumerDurablePrefix) {
			return 0, fmt.Errorf("test: claim store unavailable")
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, fmt.Errorf("test: claim re-read unavailable")
	}
	e.asm.mu.Unlock()
	if err := e.sess.recoverInflightRegisters(e.c); err == nil {
		t.Fatal("a resume whose claim fence cannot be verified must be refused")
	}
	e.asm.mu.Lock()
	e.asm.testInflightStore = nil
	e.asm.testInflightLoad = nil
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	_, held = e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if held {
		t.Fatal("an unverifiable fence must not install a reservation either")
	}

	e.sess.deleteInflightRegisterFor(removed)
	e.waitRegisterCount(t, 0)
}

// A SAME-OBJECT barrier re-run must retire the live memory behind a poisoned group.
//
// "In-memory state wins" is the right install rule for records recovery would re-install,
// but a poisoned group is a durable fact saying the opposite: the source's owner cannot
// be determined, so no mapping for it may stand. A reused session object that kept its
// pre-recovery cpending entry would redeliver the source under its old identifier while
// the client may hold the sibling — the split-claim hazard surviving through memory
// instead of the stream.
//
// Mutation check: drop the poisoned-demotion loop from the apply phase and the mapping
// survives — the trackPublish assertion fails.
func TestMQTTInflightClaimPoisonedGroupRetiresLiveMemory(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-claim-poison-memory")
	_, _, dur := mqttSharedNames("g", "claim.poison.mem")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.poison.mem", "claim.poison.mem")
	pi1 := e.track(t, dur, sseq, 1)
	pi2 := pi1 + 1
	// The sibling was written elsewhere and carries that writer's claim.
	subj2 := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi2)
	if _, err := e.asm.jsa.storeInflightCAS(subj2, 0, &mqttInflightRecord{
		PI: pi2, Dur: dur, Seq: sseq, QoS: 1, Nonce: mqttNewInflightNonce(), Claim: "written-elsewhere",
	}); err != nil {
		t.Fatalf("seed the sibling register: %v", err)
	}

	// The armed precondition: this object's live memory maps the source to pi1.
	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	armedPi := e.sess.cpending[dur][sseq]
	_, armedHeld := e.sess.pendingPublish[pi1]
	e.sess.mu.Unlock()
	if armedPi != pi1 || !armedHeld {
		t.Fatalf("the live mapping must be armed before the re-run: cpending=%d held=%v", armedPi, armedHeld)
	}

	// The sibling's record becomes UNREADABLE between this object's load and its claim
	// CAS (a corrupt write landing at the subject), so the sibling is claim-failed —
	// fail-closed, not displacement proof — and the group poisons, while pi1's own
	// record carries this object's claim and is skipped without a write.
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, rec *mqttInflightRecord) (uint64, error) {
		if subject != subj2 || rec.Claim != ownClaim {
			return e.asm.jsa.storeInflightCAS(subject, expectSeq, rec)
		}
		if _, err := e.asm.jsa.storeMsg(subject, -1, []byte("not-a-register")); err != nil {
			t.Fatalf("the corrupt write must land first: %v", err)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, rec)
	}
	e.asm.mu.Unlock()
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("same-object barrier re-run: %v", err)
	}

	e.sess.mu.Lock()
	_, mapped := e.sess.cpending[dur][sseq]
	demoted := e.sess.pendingPublish[pi1]
	_, sibling := e.sess.pendingPublish[pi2]
	incomplete := e.sess.piSpaceIncomplete
	// The source's redelivery must find no identifier and be withheld, not resent
	// under pi1.
	gotPi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if mapped {
		t.Fatal("a poisoned group's source mapping must not survive a same-object re-run")
	}
	if demoted == nil || demoted.jsDur != _EMPTY_ {
		t.Fatalf("pi1 must be demoted to a bare reservation, got %+v", demoted)
	}
	if !sibling || !incomplete {
		t.Fatalf("the poisoned pair must fail closed: sibling=%v incomplete=%v", sibling, incomplete)
	}
	if gotPi != 0 {
		t.Fatalf("the poisoned source's redelivery must be withheld, got identifier %d", gotPi)
	}
}

// A SAME-OBJECT barrier re-run must consume a close decision against its live memory.
//
// The shape: a remote owner once claimed this delivery's register (a takeover), the
// client rebound to this broker's surviving object, and a stale close's delete wins the
// re-run's claim window. Absence at the exact loaded sequence is the durable decision
// that the delivery was released — and the reused object's live entry must go with it,
// or the session keeps redelivering a source the close durably ended, under an
// identifier whose register no longer exists (a crash later would lose it outright).
//
// Mutation check: drop the consumed-close reconciliation loop from the apply phase and
// both assertions below fail.
func TestMQTTInflightClaimConsumedCloseRetiresLiveMemory(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-claim-close-memory")
	_, _, dur := mqttSharedNames("g", "claim.close.mem")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.close.mem", "claim.close.mem")
	pi := e.track(t, dur, sseq, 1)
	// A remote owner's claim sits on the record (the takeover-and-rebind history), so
	// the re-run must attempt a claim CAS rather than skip its own.
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	remote := *rec
	remote.Claim = "previous-remote-owner"
	if _, err := e.asm.jsa.storeInflightCAS(
		mqttInflightSubject(e.sess.idHash, e.sess.gen, pi), msg.Sequence, &remote); err != nil {
		t.Fatalf("seed the remote claim: %v", err)
	}

	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	armedPi := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if armedPi != pi {
		t.Fatalf("the live mapping must be armed before the re-run: cpending=%d want %d", armedPi, pi)
	}

	// The stale close's exact-sequence delete wins inside the claim window.
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r.Claim != ownClaim || !strings.HasPrefix(r.Dur, mqttSharedConsumerDurablePrefix) {
			return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, expectSeq, true); err != nil {
			t.Fatalf("the close's delete must land first: %v", err)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("same-object barrier re-run: %v", err)
	}

	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	_, mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if held || mapped {
		t.Fatalf("a consumed close must retire the reused object's live entry: held=%v mapped=%v", held, mapped)
	}
	e.waitRegisterCount(t, 0)
}

// A reservation left by a LOST arbitration never settles the register behind it.
//
// The demoted entry's sequence is the RIVAL's: its claim CAS won between this object's
// load and CAS, and the re-read handed this object the rival's current sequence. A late
// PUBACK for that identifier then runs the terminal path, whose register delete carries
// no nonce or claim check — without the foreign mark it would delete the rival's only
// durable identity through the completion path, and after the rival crashes the delivery
// re-offers under a fresh identifier ([MQTT-4.4.0-1]).
//
// Mutation check: drop the regForeign gate from deleteInflightRegisterFor and the
// register-count assertion fails.
func TestMQTTInflightClaimLostMemberSurvivesTerminalAck(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-claim-lost-ack")
	_, _, dur := mqttSharedNames("g", "claim.lost.ack")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.lost.ack", "claim.lost.ack")
	pi := e.track(t, dur, sseq, 1)
	// The record carries a previous remote owner's claim, so the re-run must CAS.
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	previous := *rec
	previous.Claim = "previous-remote-owner"
	subj := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subj, msg.Sequence, &previous); err != nil {
		t.Fatalf("seed the previous owner's claim: %v", err)
	}

	// The record becomes UNREADABLE inside the claim window (a corrupt write): the
	// claim fails without displacement proof, the single-member group poisons, and
	// the live entry is demoted to a foreign reservation.
	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if subject != subj || r.Claim != ownClaim {
			return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		if _, err := e.asm.jsa.storeMsg(subject, -1, []byte("not-a-register")); err != nil {
			t.Fatalf("the corrupt write must land first: %v", err)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("same-object barrier re-run: %v", err)
	}

	// The armed shape: the entry is demoted, foreign, and carries a sequence this
	// object must never settle.
	e.sess.mu.Lock()
	demoted := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if demoted == nil || !demoted.regForeign || demoted.regSeq == 0 {
		t.Fatalf("the lost member must be a foreign reservation, got %+v", demoted)
	}

	// A late PUBACK settles the entry through the terminal path pair.
	e.sess.mu.Lock()
	removed := e.sess.untrackPublishEntry(pi)
	e.sess.mu.Unlock()
	e.sess.deleteInflightRegisterFor(removed)

	// The record this object does not own stands (unreadable, fail-closed).
	e.waitRegisterCount(t, 1)
	after, msgAfter, err := e.asm.jsa.loadInflightRecord(subj)
	if err != nil || msgAfter == nil || after != nil {
		t.Fatalf("the terminal path settled a record it does not own: rec=%+v msg=%+v err=%v", after, msgAfter, err)
	}
}

// A foreign reservation never advances a QoS 2 exchange: the real PUBREC path declines.
//
// The terminal-delete gate is only half the rule. A successful PUBREC converts the
// pending entry into a pendingPubRel and STORES a PUBREL — from a bodyless foreign
// reservation that would be an UNCORRELATED one (no source), which a later resume reads
// as identifier-first with no source mapping while the rival's still-live register
// conflicts with it: the unacknowledged source can then transmit under a fresh
// identifier, or the completed exchange's PUBLISH can resend — QoS 2 duplication either
// way. So the PUBREC path checks the reservation FIRST and declines: nothing tracked,
// nothing stored, the reservation stands, and the owner answers the client's
// retransmitted PUBREC.
//
// Mutation check: drop the regForeign decline from mqttProcessPublishReceived and every
// assertion below fails.
func TestMQTTInflightClaimForeignReservationDeclinesPubRec(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-claim-foreign-pubrec")
	_, _, dur := mqttSharedNames("g", "claim.foreign.pubrec")
	sseq := e.publishSourceGroup(t, dur, "g", "claim.foreign.pubrec", "claim.foreign.pubrec")
	pi := e.track(t, dur, sseq, 2)
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	previous := *rec
	previous.Claim = "previous-remote-owner"
	subj := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subj, msg.Sequence, &previous); err != nil {
		t.Fatalf("seed the previous owner's claim: %v", err)
	}

	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if subject != subj || r.Claim != ownClaim {
			return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		if _, err := e.asm.jsa.storeMsg(subject, -1, []byte("not-a-register")); err != nil {
			t.Fatalf("the corrupt write must land first: %v", err)
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("same-object barrier re-run: %v", err)
	}
	e.sess.mu.Lock()
	demoted := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if demoted == nil || !demoted.regForeign {
		t.Fatalf("the lost member must be a foreign reservation, got %+v", demoted)
	}

	// A late PUBREC arrives through the REAL path, on the bound client.
	e.c.mqtt = &mqtt{sess: e.sess, proto: mqttProtoLevel5}
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	if err := e.c.mqttProcessPublishReceived(pi, true, 0); err != nil {
		t.Fatalf("the decline must be silent, got %v", err)
	}
	e.sess.mu.Lock()
	e.sess.c = nil
	_, stillReserved := e.sess.pendingPublish[pi]
	_, asPubRel := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if !stillReserved || asPubRel {
		t.Fatalf("a foreign reservation must decline the PUBREC: reserved=%v pubrel=%v",
			stillReserved, asPubRel)
	}
	// No uncorrelated PUBREL was stored, and the record this object does not own stands.
	if n, err := e.asm.jsa.streamSubjects(mqttOutStreamName, e.sess.pubRelSubject); err == nil && len(n) != 0 {
		t.Fatalf("a foreign reservation must not store a PUBREL, found %v", n)
	}
	e.waitRegisterCount(t, 1)
	after, msgAfter, err := e.asm.jsa.loadInflightRecord(subj)
	if err != nil || msgAfter == nil || after != nil {
		t.Fatalf("the record this object does not own must stand: rec=%+v msg=%+v err=%v", after, msgAfter, err)
	}
}

// The purge reaches the strays a membership removal deliberately LEAVES BEHIND.
//
// The 3.1.1 forced leave runs before processSubs (so there is no shared cons entry), nils
// the membership, and stages only register verdicts: voidRegistrationsLocked leaves a
// TRANSMITTED delivery's cpending/pendingPublish entries "for the teardown's purge to
// remove". UNSUBSCRIBE's no-cons branch and purgeReservedSubs leave the same shape. After
// any of them BOTH views of the group are empty while the pending state is still live —
// which is why the purge derives its targets from that state instead.
//
// Mutation check: derive the targets from sharedSubs (or from any union of the two views)
// and this test fails; the strays are unreachable through either.
func TestMQTTInflightSharedPurgeReachesForcedLeaveStrays(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-purge-strays")
	const group, filter = "g", "foo.bar"
	_, _, dur := mqttSharedNames(group, filter)
	plainDur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	plainSseq := e.publishSource(t, plainDur)
	pi := e.track(t, dur, sseq, 1)
	piPlain := e.track(t, plainDur, plainSseq, 2)

	// Exactly what the forced leave stages: the verdict is recorded, the transmitted
	// entry is left, the membership goes, and no shared config ever existed.
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	_, transmitted := e.sess.voidRegistrationsLocked(dur)
	e.sess.sharedSubs = nil
	strayPi := e.sess.cpending[dur][sseq]
	sharedCons := 0
	for _, cc := range e.sess.cons {
		if cc.DeliverGroup != _EMPTY_ {
			sharedCons++
		}
	}
	e.sess.mu.Unlock()
	if len(transmitted) != 1 || transmitted[0] != nonce {
		t.Fatalf("the staging must void the transmitted register, got %v", transmitted)
	}
	if strayPi != pi || sharedCons != 0 {
		t.Fatalf("the stray must be armed with neither view naming it: cpending=%d (want %d), "+
			"shared cons=%d", strayPi, pi, sharedCons)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the staged verdict: %v", err)
	}

	e.sess.purgeSharedPending()

	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	_, mapped := e.sess.cpending[dur]
	verdicts := len(e.sess.cancelledRegNonces)
	_, heldPlain := e.sess.pendingPublish[piPlain]
	plainMapped := e.sess.cpending[plainDur][plainSseq]
	e.sess.mu.Unlock()
	if held || mapped {
		t.Fatalf("the close must free the stray: held=%v mapped=%v", held, mapped)
	}
	// Re-purging an already-voided entry is idempotent, and the delete that proves it is
	// gone retires the verdict rather than accumulating it.
	if verdicts != 0 {
		t.Fatalf("a proven delete must forget its verdict, %d left", verdicts)
	}
	if !heldPlain || plainMapped != piPlain {
		t.Fatalf("no other allocation may be touched: plain held=%v cpending=%d", heldPlain, plainMapped)
	}
	e.waitRegisterCount(t, 1)
}

// A surviving disconnect's shared purge walks the same ladder and never deletes a consumer.
//
// The path frees this session's shared packet identifiers without acking, so a resume does
// not NAK-steal group messages — but the group durable itself must survive (other members
// hold it, and this session's membership stays on the record). The never-deletes-consumer
// invariant is what the reconciliation analysis rests on; the ladder is what keeps a failed
// register delete from stranding an identifier: the verdict lands durably and the next
// restart frees it.
//
// It also covers the two group durables reachable through exactly ONE of the maps the
// collection reads: a RETIRED source, which is never in cpending at all, and a WITHHELD
// first transmission, which is only ever in deferred. Both fixtures drop the emptied outer
// cpending bucket their construction leaves behind, because a collection that read cpending
// keys alone would otherwise still find them and prove nothing.
//
// Mutation checks: delete the consumer in purgeSharedPending and the first assertion fails;
// skip the ladder (call the deletes fire-and-forget) and the verdict never reaches the
// record; skip the srcAckOwed keys and the retirement survives; skip the deferred keys and
// the withheld marker survives.
func TestMQTTInflightSharedPurgeLadderKeepsConsumer(t *testing.T) {
	e := testNewInflightEnv(t, "client-shared-purge")
	const group = "g"
	// The deterministic group name IS the fact the collection reads; nothing here is
	// populated by hand for it to find.
	_, _, dur := mqttSharedNames(group, "foo.bar")
	_, _, retDur := mqttSharedNames(group, "retired.src")
	_, _, defDur := mqttSharedNames(group, "deferred.src")

	sseq := e.publishSourceGroup(t, dur, group, "foo.bar", "foo.bar")
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	idHash := e.sess.idHash
	e.sess.mu.Unlock()

	// A retirement is created AFTER untrackPublishEntry removed the source's tracking, so
	// srcAckOwed is the only map naming its durable — once the emptied outer cpending
	// bucket untrackPublishEntry leaves behind is gone too.
	retSseq := e.publishSourceGroup(t, retDur, group, "retired.src", "retired.src")
	retPi := e.track(t, retDur, retSseq, 2)
	e.sess.mu.Lock()
	retAck := e.sess.pendingPublish[retPi]
	retRegSeq, retNonce := retAck.regSeq, retAck.regNonce
	e.sess.untrackPublishEntry(retPi)
	delete(e.sess.cpending, retDur)
	e.sess.retireSourceLocked(retDur, retSseq, &mqttRetiredSource{
		pi: retPi, regSeq: retRegSeq, nonce: retNonce, durable: true,
	})
	_, retOuter := e.sess.cpending[retDur]
	e.sess.mu.Unlock()
	if retOuter {
		t.Fatalf("the retirement durable must be reachable only through srcAckOwed")
	}

	// A withheld first transmission: trackPublish installs the outer cpending bucket
	// BEFORE the in-flight cap check that defers, so that key goes too.
	defSseq := e.publishSourceGroup(t, defDur, group, "deferred.src", "deferred.src")
	e.sess.mu.Lock()
	savedRmax := e.sess.rmax
	e.sess.rmax = 1 // at the cap, so the next NEW delivery is withheld rather than allocated
	defPi, _, _, defIsNew := e.sess.trackPublish(defDur, testInflightAckSubject(defDur, defSseq, 1))
	e.sess.rmax = savedRmax
	delete(e.sess.cpending, defDur)
	_, defArmed := e.sess.deferred[mqttDeferredKey{defDur, defSseq}]
	_, defOuter := e.sess.cpending[defDur]
	e.sess.mu.Unlock()
	if defPi != 0 || defIsNew {
		t.Fatalf("the at-cap delivery must be withheld, got pi=%d isNew=%v", defPi, defIsNew)
	}
	if !defArmed {
		t.Fatalf("the withheld marker must be armed before the purge")
	}
	if defOuter {
		t.Fatalf("the deferred durable must be reachable only through deferred")
	}

	// Both register-stream rungs fail; only the session record is reachable.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.mu.Unlock()

	e.sess.purgeSharedPending()

	// The consumer survives: other members may hold it, and the membership does.
	if _, err := e.asm.jsa.lookupConsumer(mqttStreamName, dur); err != nil {
		t.Fatalf("purgeSharedPending must never delete a consumer: %v", err)
	}
	// The identifier is freed for this session...
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if held {
		t.Fatalf("the purge must free the shared identifier")
	}
	// ...including the two durables only one map names.
	e.sess.mu.Lock()
	stillRetired := e.sess.sourceRetiredLocked(retDur, retSseq)
	_, stillDeferred := e.sess.deferred[mqttDeferredKey{defDur, defSseq}]
	e.sess.mu.Unlock()
	if stillRetired {
		t.Fatalf("a group durable named only by srcAckOwed must still be purged")
	}
	if stillDeferred {
		t.Fatalf("a group durable named only by deferred must still be purged")
	}
	// ...and the verdict reached the record, so a restart frees it too instead of
	// restoring a pending entry another member may complete.
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatalf("no session record")
	}
	found := false
	for _, n := range ps.CancelledRegs {
		if n == nonce {
			found = true
		}
	}
	if !found {
		t.Fatalf("the record must carry the verdict of the unremovable register, got %v", ps.CancelledRegs)
	}
}

// A proven rung-1 delete forgets its verdict, so the record's verdict set stays bounded.
//
// The verdict is recorded at staging (the membership-removal write carries it) and the
// register is deleted moments later; without the forget, every subscribe/unsubscribe cycle
// leaves one more nonce in CancelledRegs forever.
//
// Mutation check: skip forgetCancelledReg on the confirmed-delete rung and the record still
// carries a verdict its register no longer needs.
func TestMQTTInflightProvenDeleteBoundsVerdicts(t *testing.T) {
	e := testNewInflightEnv(t, "client-rung1-bounds")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	// The staging half: the transmitted register is voided in the membership write...
	_, transmitted := e.sess.voidRegistrationsLocked(dur)
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if len(transmitted) != 1 || transmitted[0] != nonce {
		t.Fatalf("the staging must void the transmitted register, got %v", transmitted)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the staged verdict: %v", err)
	}

	// ...and the teardown's delete then SUCCEEDS, which is the proof that retires it.
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)
	e.sess.mu.Lock()
	verdicts := len(e.sess.cancelledRegNonces)
	e.sess.mu.Unlock()
	if verdicts != 0 {
		t.Fatalf("a proven delete must forget its verdict, %d left", verdicts)
	}
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("the shortened verdict set must persist")
	}
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps != nil && len(ps.CancelledRegs) != 0 {
		t.Fatalf("the record must not accumulate spent verdicts, got %v", ps.CancelledRegs)
	}
	e.waitRegisterCount(t, 0)
}

// A verdict on an OLD register cannot touch a NEW delivery on the same reused durable name.
//
// Shared group durables are named deterministically, so a leave followed by a rejoin brings
// the same name back. This is the case that killed every durable-scoped rule this feature
// tried: "everything on this durable is void" either resurrects the abandoned register or
// destroys the genuine new one. A nonce is minted with its allocation and never reused, so
// the old verdict names exactly the old register, whatever the durable is called.
//
// Mutation check: key the recovery drop by DURABLE (drop every register whose durable has
// any voided nonce) and the rejoined delivery's identifier is freed while the client holds
// it.
func TestMQTTInflightSharedRejoinUnaffectedByOldVerdict(t *testing.T) {
	const clientID = "client-rejoin-nonce"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()

	// The OLD membership's delivery: transmitted, then the leave voids it and the delete
	// fails, so the verdict is what survives.
	sseqOld := e.publishSource(t, dur)
	piOld := e.track(t, dur, sseqOld, 2)
	e.sess.mu.Lock()
	nonceOld := e.sess.pendingPublish[piOld].regNonce
	_, transmitted := e.sess.voidRegistrationsLocked(dur)
	regs, _ := e.sess.purgeConsumerPendingLocked(dur)
	e.sess.mu.Unlock()
	if len(transmitted) != 1 || transmitted[0] != nonceOld {
		t.Fatalf("the leave must void the old register, got %v", transmitted)
	}
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, fmt.Errorf("test: register stream unavailable")
	}
	e.asm.mu.Unlock()
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)
	e.asm.mu.Lock()
	e.asm.testInflightDelete, e.asm.testInflightStore, e.asm.testInflightLoad = nil, nil, nil
	e.asm.mu.Unlock()

	// The REJOIN, same durable name, and a new delivery under a new nonce.
	sseqNew := e.publishSource(t, dur)
	piNew := e.track(t, dur, sseqNew, 2)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session: %v", err)
	}

	// The restart must free the OLD identifier and keep the NEW one.
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	_, oldHeld := restored.pendingPublish[piOld]
	_, newHeld := restored.pendingPublish[piNew]
	restored.mu.Unlock()
	if oldHeld {
		t.Fatalf("the old register was voided; identifier %d must not be reserved", piOld)
	}
	if !newHeld {
		t.Fatalf("the rejoined delivery's identifier %d must stay reserved: the old verdict cannot touch it", piNew)
	}
}

// A landed save releases only the reservations whose verdicts it provably SERIALIZED.
//
// The revision a save captures precedes its marshal, so a verdict recorded by a concurrent
// teardown after that capture is durable nowhere when the save lands — and a release keyed
// on set membership alone would free its identifier with a crash still able to resurrect the
// register. Each verdict therefore carries the revision of its own insertion, and release
// requires it to be at or below the revision proven saved.
//
// Mutation check: release on membership alone (drop the insertion-revision comparison) and
// the unserialized verdict's identifier is freed.
func TestMQTTInflightReleaseRequiresSerializedVerdict(t *testing.T) {
	e := testNewInflightEnv(t, "client-serialized-release")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Verdict A is recorded and provably saved.
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked("nonce-A")
	e.sess.mu.Unlock()
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("verdict A's save must land")
	}

	// Verdict B arrives AFTER that save, with a settled reservation waiting on it — the
	// state a concurrent teardown leaves between the marshal and the next save.
	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 4100, 1))
	cancels, _ := e.sess.voidRegistrationsLocked(dur)
	if ack := e.sess.pendingPublish[pi]; ack != nil {
		// Settled exactly as the settle points do it: regPending drops and the
		// reservation is parked in the index the proof edges release from.
		ack.regPending = false
		e.sess.parkSettledVoidReservationLocked(ack.regNonce, pi)
	}
	// The release pass runs on the strength of verdict A's landed save.
	e.sess.releaseSettledVoidReservationsLocked()
	_, held := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if len(cancels) != 1 {
		t.Fatalf("the staging must withdraw the registration, got %+v", cancels)
	}
	if !held {
		t.Fatalf("verdict B was never serialized by the landed save; its identifier must stay held")
	}

	// Once B's own save lands, the release follows.
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("verdict B's save must land")
	}
	e.sess.mu.Lock()
	_, held = e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if held {
		t.Fatalf("a serialized verdict on a settled store must release the identifier")
	}
}

// A reservation whose verdict was already proven by a register-stream rung is released at
// the settle point, not parked forever.
//
// A securing tombstone retires the nonce while the original store is still out. When that
// store then settles through ABANDONMENT (its retries exhausted, its reconcile unreadable),
// the save-keyed release can never fire — there is nothing left to save — so the settle
// point itself must recognise the retired verdict as proof. Withheld, the reservation and
// its Receive Maximum slot survive for the life of the session; at a Receive Maximum of 1
// that is a session that can no longer deliver, with every durable rung PROVEN.
//
// Mutation check: always park at the abandon settle point (drop the retired-verdict escape)
// and the reservation survives its own proof.
func TestMQTTInflightAbandonReleasesProvenReservation(t *testing.T) {
	e := testNewInflightEnv(t, "client-abandon-proven")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 4200, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()

	// The teardown withdraws the allocation and SECURES it: the tombstone lands, which
	// retires the nonce.
	e.sess.mu.Lock()
	regs, cancels := e.sess.purgeConsumerPendingLocked(dur)
	e.sess.mu.Unlock()
	if len(regs) != 0 || len(cancels) != 1 || cancels[0].nonce != nonce {
		t.Fatalf("the purge must report the pending cancellation, got %v / %v", regs, cancels)
	}
	if !e.sess.secureRegCancellations(e.c, recordTx{}, cancels) {
		t.Fatalf("the tombstone must land")
	}
	e.sess.mu.Lock()
	_, stillVoid := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if stillVoid {
		t.Fatalf("a landed tombstone must retire the nonce verdict")
	}

	// The original store settles by ABANDONMENT — and the proof must release it.
	e.sess.abandonInflightRegister(pi, dur, 4200, nonce)
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if held {
		t.Fatalf("a proven reservation must be released when its store settles")
	}
}

// A verdict whose register was not seen is KEPT by the scan, and retired only when the
// register finally lands and is deleted with proof.
//
// Scan-time absence cannot rule out a late create-only CAS: a superseded broker's store can
// land after a takeover's resume has already walked the stream. A verdict retired on that
// emptiness leaves the late register ordinary — an abandoned consumer's register nothing
// anywhere calls void — and every later resume reserves its identifier. Kept, the verdict is
// self-healing: the scan that finally sees the register deletes it and retires the verdict
// against that proof.
//
// The one place absence IS provable retires its own verdict: a store that was cancelled
// before any CAS was issued (releaseCancelledReservation) can never produce a register.
//
// Mutation checks: retire absent verdicts at scan (the removed cleanup) and the late
// register survives as an ordinary delivery; drop the no-CAS forget and a nonce no register
// can carry stays on the record.
func TestMQTTInflightAbsentVerdictSurvivesUntilItsRegisterLands(t *testing.T) {
	const clientID = "client-late-cas"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()

	// The verdict of a register the scan has never seen (the takeover shape: the old
	// broker's CAS is still in flight). A well-formed nonce, since an ill-formed one makes
	// the register unreadable rather than voided.
	nonce := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	pi := e.sess.bumpPI()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("the verdict must be durable")
	}

	// First resume: the register is absent, and the verdict must SURVIVE the scan.
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, kept := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if !kept {
		t.Fatalf("scan-time absence is not proof; the verdict must survive the scan")
	}

	// The late CAS lands...
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(idHash, gen, pi), 0, &mqttInflightRecord{
		PI: pi, Dur: dur, Seq: 4300, QoS: 2, Nonce: nonce,
	}); err != nil {
		t.Fatalf("land the late register: %v", err)
	}
	// ...and the next scan retires the verdict against the proof of its deletion.
	if err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters (second): %v", err)
	}
	e.sess.mu.Lock()
	_, kept = e.sess.cancelledRegNonces[nonce]
	_, reserved := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if kept {
		t.Fatalf("a register seen and deleted must retire its verdict")
	}
	if reserved {
		t.Fatalf("the late register was voided; identifier %d must not be reserved", pi)
	}
	e.waitRegisterCount(t, 0)

	// And the provable-absence path: a cancellation whose store never issued a CAS
	// retires its own verdict.
	e.sess.mu.Lock()
	pi2, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 4400, 1))
	nonce2 := e.sess.pendingPublish[pi2].regNonce
	e.sess.voidRegistrationsLocked(dur)
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi2, 1, dur, 4400, e.nonceOf(pi2), _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	e.sess.mu.Lock()
	_, kept2 := e.sess.cancelledRegNonces[nonce2]
	_, held2 := e.sess.pendingPublish[pi2]
	e.sess.mu.Unlock()
	if kept2 || held2 {
		t.Fatalf("no CAS was ever issued: the verdict and the reservation must both go (kept=%v held=%v)", kept2, held2)
	}
}

// A STALE store's settle cannot release a newer allocation's reservation.
//
// A store call can stall (its convergence-point persist is a round trip) across the
// cancellation of its own allocation, that reservation's release, the identifier's REUSE by
// a new allocation, and a second cancellation. When it finally reads the entry, the
// reservation it finds belongs to the NEW allocation — whose own CAS is still on the wire.
// Releasing it would free the identifier for a third allocation to race that CAS onto the
// same subject, and forgetting its verdict would leave whatever lands unexplained. Every
// reservation touch is therefore keyed by the toucher's own nonce.
//
// Mutation check: drop the nonce comparison from releaseCancelledReservation and the newer
// allocation's reservation (and verdict) are destroyed by the stale settle.
func TestMQTTInflightStaleStoreCannotReleaseNewerReservation(t *testing.T) {
	e := testNewInflightEnv(t, "client-stale-settle")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Allocation A, whose store is "stalled": trackPublish ran, storeInflightRegister has
	// not read the entry yet.
	e.sess.mu.Lock()
	piA, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5100, 1))
	nonceA := e.sess.pendingPublish[piA].regNonce
	// Its allocation is cancelled and the reservation settles and releases, freeing the
	// identifier...
	e.sess.voidRegistrationsLocked(dur)
	if ack := e.sess.pendingPublish[piA]; ack == nil || !ack.regCancelled {
		e.sess.mu.Unlock()
		t.Fatalf("the cancellation must leave a reservation")
	}
	delete(e.sess.pendingPublish, piA) // A's release, proven elsewhere
	e.sess.last_pi = piA - 1           // ...and REUSED by allocation B under the same number
	piB, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5200, 1))
	nonceB := e.sess.pendingPublish[piB].regNonce
	// B is cancelled too, with ITS store still out: the state the stale settle meets.
	e.sess.voidRegistrationsLocked(dur)
	e.sess.mu.Unlock()
	if piB != piA {
		t.Fatalf("the test needs the identifier reused, got %d then %d", piA, piB)
	}
	if nonceB == nonceA {
		t.Fatalf("nonces must differ across allocations")
	}

	// A's stale store finally settles. It must not touch B's reservation or verdict.
	if e.sess.storeInflightRegister(e.c, piA, 1, dur, 5100, nonceA, _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[piB]
	_, verdictHeld := e.sess.cancelledRegNonces[nonceB]
	e.sess.mu.Unlock()
	if ack == nil || !ack.regCancelled || ack.regNonce != nonceB {
		t.Fatalf("the newer allocation's reservation must survive a stale settle, got %+v", ack)
	}
	if !verdictHeld {
		t.Fatalf("the newer allocation's verdict must survive a stale settle")
	}
}

// The reservation release is ORDER-INDEPENDENT between the verdict's proof and the store's
// settlement, in both directions.
//
// The two race freely, and each ordering has a wedge if release is checked at only one edge.
// Verdict durable FIRST, settle second: the settle parks the reservation waiting for "the
// save that proves the verdict" — but that save already happened, and with the revision gap
// closed no later save comes round. Settle FIRST, stream-rung proof second: recovery (or a
// securing retry) retires the nonce, removing the very membership the save-keyed release
// requires, and nothing later re-examines the reservation. Either way the identifier and its
// Receive Maximum slot are consumed for the life of the session; at a Receive Maximum of 1,
// the whole session.
//
// Mutation checks: drop the savedRev comparison from voidVerdictProvenLocked and ordering A
// wedges; drop the release sweep from forgetCancelledReg and ordering B wedges.
func TestMQTTInflightReservationReleaseIsOrderIndependent(t *testing.T) {
	// Ordering A: the verdict's save lands BEFORE the store settles.
	t.Run("verdict-durable-then-settle", func(t *testing.T) {
		e := testNewInflightEnv(t, "client-order-a")
		dur := e.sess.idHash + "_" + nuid.Next()

		e.sess.mu.Lock()
		pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5300, 1))
		nonce := e.sess.pendingPublish[pi].regNonce
		e.sess.voidRegistrationsLocked(dur)
		e.sess.mu.Unlock()

		// The verdict becomes durable while the store is still out.
		if !e.sess.persistVoidedRegs(e.c) {
			t.Fatalf("the verdict save must land")
		}
		e.sess.mu.Lock()
		stillHeld := e.sess.pendingPublish[pi] != nil
		e.sess.mu.Unlock()
		if !stillHeld {
			t.Fatalf("an unsettled reservation must survive the save")
		}

		// The store then settles by ABANDONMENT. The proof already exists; parking here
		// waits for a save that will never come.
		e.sess.abandonInflightRegister(pi, dur, 5300, nonce)
		e.sess.mu.Lock()
		_, held := e.sess.pendingPublish[pi]
		e.sess.mu.Unlock()
		if held {
			t.Fatalf("a settle after a durable verdict must release the identifier")
		}
	})

	// Ordering B: the store settles, and a stream-rung proof then retires the nonce.
	t.Run("settle-then-rung-proof", func(t *testing.T) {
		e := testNewInflightEnv(t, "client-order-b")
		dur := e.sess.idHash + "_" + nuid.Next()

		e.sess.mu.Lock()
		pi, _, _, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5400, 1))
		nonce := e.sess.pendingPublish[pi].regNonce
		e.sess.voidRegistrationsLocked(dur)
		e.sess.mu.Unlock()

		// Settles first, with the verdict unproven (no save has landed).
		restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
			return fmt.Errorf("test: session record unavailable")
		})
		e.sess.abandonInflightRegister(pi, dur, 5400, nonce)
		restore()
		e.sess.mu.Lock()
		ack := e.sess.pendingPublish[pi]
		e.sess.mu.Unlock()
		if ack == nil || !ack.regCancelled || ack.regPending {
			t.Fatalf("an unproven settle must park the reservation, got %+v", ack)
		}

		// A stream-rung proof arrives after the settle: recovery deletes the register (or
		// a securing retry lands its tombstone) and retires the nonce. That retirement is
		// the last edge that will ever look at this reservation.
		e.sess.forgetCancelledReg(nonce)
		e.sess.mu.Lock()
		_, held := e.sess.pendingPublish[pi]
		e.sess.mu.Unlock()
		if held {
			t.Fatalf("a rung proof after the settle must release the identifier")
		}
	})
}

// A verdict whose register is FOUND tombstoned is retired against that register's confirmed
// deletion, not kept forever.
//
// This is the state a crash leaves between the teardown's tombstone rung and the forget's
// save: the verdict is on the record AND the Cancelled register is on the stream. The
// verdict-keeping absence policy does not apply — the register is right here, so proof needs
// no absence reasoning — and routing it to the fire-and-forget list instead would keep the
// nonce in CancelledRegs forever, one entry per such crash, growing the session record
// without bound.
//
// Mutation check: route a tombstoned in-set register to the fire-and-forget drop list (the
// original ordering) and the verdict survives its own proof.
func TestMQTTInflightTombstonedVerdictIsRetiredWithProof(t *testing.T) {
	const clientID = "client-tombstone-retire"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()

	// The crash residue: verdict durable, register tombstoned, forget's save never ran.
	nonce := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	pi := e.sess.bumpPI()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("the verdict must be durable")
	}
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(idHash, gen, pi), 0, &mqttInflightRecord{
		PI: pi, Dur: dur, Seq: 4500, QoS: 1, Nonce: nonce, Cancelled: true,
	}); err != nil {
		t.Fatalf("land the tombstone: %v", err)
	}

	// The restart's scan finds the register and retires the verdict against its deletion.
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	_, kept := restored.cancelledRegNonces[nonce]
	_, reserved := restored.pendingPublish[pi]
	restored.mu.Unlock()
	if kept {
		t.Fatalf("a tombstoned register is proof in hand; its verdict must be retired")
	}
	if reserved {
		t.Fatalf("a tombstoned register frees its identifier, %d must not be reserved", pi)
	}
	e.waitRegisterCount(t, 0)
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
	for _, v := range regs {
		if v.seq == regSeq {
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

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1700, e.nonceOf(pi), _EMPTY_) {
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
	// The sweep's delete is deliberately fire-and-forget, so absence is polled rather
	// than asserted on the send: asserting immediately races the send queue, which is
	// exactly the flake a paired soak run caught here.
	deadline := time.Now().Add(5 * time.Second)
	for e.registerPresent(t, subject) {
		if time.Now().After(deadline) {
			t.Fatalf("an orphaned register must be retired once stably observed")
		}
		time.Sleep(10 * time.Millisecond)
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
