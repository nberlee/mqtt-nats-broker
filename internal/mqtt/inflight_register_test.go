package mqtt

import (
	"bytes"
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

// freshRegisterLineage stamps a fixture register as a FRESH allocation: its lineage is its
// own nonce, which is exactly what storeInflightRegister writes for a create-only pair.
//
// It exists so a fixture says what it MEANS. Filling the field with a second
// mqttNewInflightNonce() would be legal — a lineage differing from the nonce is the ordinary
// shape after an inheriting replacement — and would therefore describe every one of these
// fixtures as an heir to some exchange that does not exist.
//
// A fixture that really does mean an heir sets Lin itself, to the PREDECESSOR's lineage, and
// does not come through here. That difference is the thing the inheritance tests observe.
func testRegisterSourceReply(r *mqttInflightRecord) {
	r.SourceAckReply = _EMPTY_
	if mqttOrdinarySourceRegister(r) {
		r.SourceAckReply = testInflightAckSubject(r.Dur, r.Seq, 1)
	}
}

func freshRegisterLineage(r *mqttInflightRecord) *mqttInflightRecord {
	if mqttOrdinarySourceRegister(r) && r.SourceAckReply == _EMPTY_ {
		r.SourceAckReply = testInflightAckSubject(r.Dur, r.Seq, 1)
	}
	r.Lin = r.Nonce
	if r.Claim == _EMPTY_ {
		// CUSTODY IS MANDATORY in every phase and at every QoS (C-III clause 1), so a
		// fixture that does not care about custody still has to carry some. Set here
		// rather than at each caller so a test that DOES care can still override it, and
		// so a legal base record stays legal for the fault each row is written to catch.
		r.Claim = mqttNewInflightNonce()
	}
	return r
}

func TestMQTTInflightSubjectNaming(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	ok := func() *mqttInflightRecord {
		return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "hash_nuid", Seq: 42, QoS: 2, Nonce: mqttNewInflightNonce()})
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
	t.Parallel()
	rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: 9, Dur: "hash_nuid", Seq: 1234, QoS: 1,
		Nonce: mqttNewInflightNonce(), Claim: mqttNewInflightNonce(),
		Q2AffClaim: "$MQTT.qos2.assign.G.7",
	})
	msg, hdrLen := buildInflightRecord(0, rec)
	hdr := string(msg[:hdrLen])
	if !strings.Contains(hdr, JSExpectedLastSubjSeq+":0") {
		t.Fatalf("header %q must carry a create-only expectation", hdr)
	}
	got := &mqttInflightRecord{State: mqttInflightPublish}
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
	t.Parallel()
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
	t.Parallel()
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:       mqttInflightStreamName,
			Subjects:   []string{mqttInflightStreamSubjectPrefix + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
			Metadata:   map[string]string{mqttSessEpochMetaKey: testBundleEpoch},
			// The PUBLISH-phase pair is one atomic batch (F-072).
			AllowAtomicPublish: true,
		}}
	}
	if err := validateInflightStream(good(), 0, testBundleEpoch); err != nil {
		t.Fatalf("the shipped configuration must validate: %v", err)
	}
	// T23, N-2.5: the stream must allow atomic publishes, or the pair cannot be committed.
	si := good()
	si.Config.AllowAtomicPublish = false
	if err := validateInflightStream(si, 0, testBundleEpoch); err == nil || !strings.Contains(err.Error(), "allow_atomic") {
		t.Fatalf("a stream without AllowAtomicPublish must be refused naming allow_atomic, got %v", err)
	}
	if err := validateInflightStream(nil, 0, testBundleEpoch); err == nil {
		t.Fatalf("missing stream info must be rejected")
	}
	// Replicas are only checked when the deployment asked for a specific count.
	if err := validateInflightStream(good(), 3, testBundleEpoch); err == nil {
		t.Fatalf("a replica mismatch must be rejected when one is wanted")
	}
	si = good()
	si.Config.Replicas = 3
	if err := validateInflightStream(si, 3, testBundleEpoch); err != nil {
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
			if err := validateInflightStream(si, 0, testBundleEpoch); err == nil {
				t.Fatalf("%s must fail validation", tc.name)
			}
		})
	}

	// Deliberately NOT a prerequisite: nothing ever creates a consumer here, so a
	// stream-forced consumer inactivity limit cannot affect this owner. Listing a
	// prerequisite the owner does not have would fail startups for no reason.
	si = good()
	si.Config.ConsumerLimits.InactiveThreshold = time.Minute
	if err := validateInflightStream(si, 0, testBundleEpoch); err != nil {
		t.Fatalf("a consumer inactivity limit is irrelevant to a stream with no consumers: %v", err)
	}

	// DenyPurge stopped being a prerequisite with F-034, and this asserts the removal
	// positively rather than leaving its absence to be inferred. The teardown's
	// per-incarnation FILTER PURGE was replaced by reapIncarnationRegisters, which deletes
	// at exact sequences — covered by the DenyDelete rejection above. Refusing DenyPurge
	// would reject a hardened stream this broker can use, for a purge that no longer
	// exists, so this test would fail if the rejection came back.
	si = good()
	si.Config.DenyPurge = true
	if err := validateInflightStream(si, 0, testBundleEpoch); err != nil {
		t.Fatalf("deny_purge must be accepted since F-034 replaced the filter purge with "+
			"exact-sequence deletes: %v", err)
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
	// pubRelSlots is the fixture's durable slot store, installed by
	// testReadyForPubRelDelivery so admission has something to acquire an action on.
	pubRelSlots map[string]*mqttInflightRecord
}

func testNewInflightEnv(t *testing.T, clientID string) *testInflightEnv {
	t.Helper()
	_, nc := runNATS(t)

	s := &Server{
		nrc:      nc,
		opts:     &Options{},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
		// Initialized exactly as the production constructor does. Without it any path that
		// ENQUEUES a delayed Will panics on a nil map inside scheduleWillTimer, so this env
		// could not reach the outbox route at all — a fixture that cannot reach the code it
		// names proves nothing about it.
		willTimers:      make(map[string]*time.Timer),
		willClaimStopCh: make(chan struct{}),
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
	e := &testInflightEnv{s: s, asm: asm, sess: sess, c: ic}
	testAcceptRigConnection(t, asm, sess, ic)
	return e
}

// testAcceptRigConnection accepts a rig's connection the way a CONNECT's accept loop does before
// any resume runs (F-105 7b): a serving authority stands (the existing one is kept, so a peer rig
// restoring the same session shares it), the connection records its entitlement, and the session
// object records the same as its accepted one (boundEnt). The connection is not BOUND as the
// session's (sess.c): rigs that need that call bind() themselves, as before.
func testAcceptRigConnection(t *testing.T, asm *mqttAccountSessionManager, sess *mqttSession, c *client) {
	t.Helper()
	epoch := asm.sessStreamEpoch
	a := &mqttOutboundAuthority{
		ID: sess.id, Epoch: epoch, Gen: sess.gen, Tenure: mqttNewInflightNonce(),
		State: mqttAuthorityServing, Rev: 1, Txn: "txn-boot", OwnerTxn: "txn-boot",
	}
	if al := asm.jsa.loadOutboundAuthority(sess.id, epoch); al.status == mqttOutboundValid &&
		al.rec != nil && al.rec.State == mqttAuthorityServing {
		a = al.rec
	} else {
		b, _ := json.Marshal(a)
		msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
		if _, err := asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(sess.id)), hdr, msg); err != nil {
			t.Fatalf("installing serving authority: %v", err)
		}
	}
	if c.mqtt == nil {
		c.mqtt = &mqtt{asm: asm, sess: sess}
	}
	ent := mqttOutboundEntitlement{Gen: a.Gen, Tenure: a.Tenure}
	c.setOutboundEntitlement(ent)
	sess.mu.Lock()
	sess.boundEnt, sess.boundEntOK = ent, true
	sess.mu.Unlock()
}

// peer restores through a distinct account session manager while sharing the same
// JetStream. That is the cross-broker boundary: session-object claims differ even
// though the persisted session generation is the same.
func (e *testInflightEnv) peer(t *testing.T, node, clientID string) (*mqttAccountSessionManager, *mqttSession, *client) {
	t.Helper()
	// The dispatch-stop channel exists before the account manager starts its
	// claim loop, which selects on it; assigning it afterwards is a data race.
	s := &Server{nrc: e.s.nrc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: node,
		willClaimStopCh: make(chan struct{})}
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
	testAcceptRigConnection(t, asm, sess, c)
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
	if mqttIsSharedDurable(dur) {
		// A shared register without its exchange is a shape this broker no longer writes (7a):
		// a shared delivery is admitted through the authority beside the acquisition it depends on.
		return e.trackSharedByGroupKey(t, dur, sseq, qos)
	}
	if !mqttDurIsSlotOnlyClass(dur) {
		// An ordinary register without its exchange is a shape this broker no longer writes, and
		// a resume refuses it as unadmitted (F-105 7b): the ordinary obligation is admitted as
		// production admits it, beside its exchange and its duty entry.
		return e.trackAdmitted(t, dur, sseq, qos)
	}
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish(%s/%d): pi=%d isNew=%v", dur, sseq, pi, isNew)
	}
	if !e.sess.storeInflightRegister(e.c, pi, qos, dur, sseq, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatalf("storeInflightRegister(%d, %s/%d) refused", pi, dur, sseq)
	}
	return pi
}

// trackAdmitted is track for an ORDINARY obligation as production admits it: through the
// outbound obligation authority, so the register stands beside its exchange.
//
// track's register has no exchange, and every acknowledgement of an ordinary slot without its
// exchange is refused (ackExchangeFor). A test of acknowledgement or completion behaviour needs
// the obligation production creates, which means the admission's own prerequisites first: a
// serving authority, the source consumer standing physically and claimed by name through the
// authority, and for QoS 2 the canonical PUBREL consumer. Each is created only if absent, so a
// fixture that already built one keeps its configuration. sseq is carried into the register
// unchanged.
func (e *testInflightEnv) trackAdmitted(t *testing.T, dur string, sseq uint64, qos byte, offeredReply ...string) uint16 {
	t.Helper()
	e.admissionPrerequisites(t, dur, qos)
	reply := testInflightAckSubject(dur, sseq, 1)
	if len(offeredReply) != 0 {
		reply = offeredReply[0]
		if !mqttSourceAckReplyBinds(reply, mqttStreamName, dur, sseq) {
			t.Fatal("offered reply does not bind the admitted source")
		}
	}
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, reply)
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish(%s/%d): pi=%d isNew=%v", dur, sseq, pi, isNew)
	}
	e.acceptUnderServingAuthority(t)
	if !e.sess.admitInflightPublish(e.c, pi, qos, dur, sseq, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatalf("admitInflightPublish(%d, %s/%d) refused", pi, dur, sseq)
	}
	return pi
}

// landAdmission lands the admission batch of an allocation DIRECTLY, with none of the session's
// settlement around it: the durable write a broker had on the wire when it died. It is the
// admitted world's form of a raw register CAS — the register, its exchange and its duty entry in
// one batch, under the connection's accepted entitlement — so a crash-cut fixture restores from
// the population production actually writes. It returns the landed register.
func (e *testInflightEnv) landAdmission(t *testing.T, pi uint16, qos byte, dur string, sseq uint64, nonce string) *mqttInflightRecord {
	t.Helper()
	e.admissionPrerequisites(t, dur, qos)
	e.acceptUnderServingAuthority(t)
	ent, _ := e.c.outboundEntitlement()
	epoch := e.asm.sessStreamEpoch
	deps, ok := e.sess.assembleAdmitDeps(e.c, dur, qos, epoch, ent.Gen)
	if !ok {
		t.Fatalf("the admission dependencies of %s/%d are not ready", dur, sseq)
	}
	rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: dur, Seq: sseq, QoS: qos, Nonce: nonce})
	if _, err := e.sess.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publish",
		Gen: ent.Gen, Tenure: ent.Tenure, Register: rec, Deps: deps,
	}); err != nil {
		t.Fatalf("land the admission of %d (%s/%d): %v", pi, dur, sseq, err)
	}
	landed, smsg := e.loadRegister(t, pi)
	if landed == nil || smsg == nil || landed.Nonce != nonce {
		t.Fatalf("the landed admission's register is not readable as this allocation's: %+v", landed)
	}
	return landed
}

// admissionPrerequisites builds what an ordinary admission depends on, each only if absent: a
// serving authority, the source consumer standing physically and claimed by name through the
// authority, and for QoS 2 the canonical PUBREL consumer.
func (e *testInflightEnv) admissionPrerequisites(t *testing.T, dur string, qos byte) {
	t.Helper()
	e.installServingAuthority(t)
	if ci, _ := e.asm.jsa.lookupConsumer(mqttStreamName, dur); ci == nil {
		ccfg := &CreateConsumerRequest{Stream: mqttStreamName, Config: ConsumerConfig{
			Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "foo.bar",
		}}
		if _, err := e.asm.jsa.createDurableConsumer(ccfg); err != nil {
			t.Fatalf("create the source consumer %q: %v", dur, err)
		}
	}
	epoch := e.asm.sessStreamEpoch
	if own, _, oerr := e.sess.jsa.loadNameOwnershipPhysical(epoch, mqttOwnedStreamMsgs, dur); oerr != nil {
		t.Fatalf("reading the source name ownership of %q: %v", dur, oerr)
	} else if own == nil {
		auth, _, aerr := e.sess.servingAuthority()
		if aerr != nil || auth == nil {
			t.Fatalf("no serving authority to claim %q under: %v", dur, aerr)
		}
		txn := mqttNewInflightNonce()
		if _, perr := e.sess.jsa.opProvisionResource(e.sess.id, epoch, txn, "op-"+txn,
			auth.Gen, auth.Tenure, mqttNewInflightNonce(), dur, mqttResourceSourceConsumer); perr != nil {
			t.Fatalf("claiming the source name %q: %v", dur, perr)
		}
	}
	if qos >= 2 {
		e.subscribePubRel(t)
	}
}

// acceptUnderServingAuthority records on the env's connection the entitlement the CONNECT accept
// loop records in production: the serving authority's generation and tenure. Admission is pinned
// to the connection's OWN accepted entitlement, so a unit env that never CONNECTs must be
// accepted before it admits. A connection already carrying one keeps it (it is set once). The
// env's connection is the internal account client, which carries no MQTT state until a fixture
// gives it some; the entitlement lives there, so it is given the minimal state first, as the
// fixtures that drive it as an MQTT connection already do.
func (e *testInflightEnv) acceptUnderServingAuthority(t *testing.T) {
	t.Helper()
	if _, ok := e.c.outboundEntitlement(); ok {
		return
	}
	if e.c.mqtt == nil {
		e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	}
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("no serving authority to accept the connection under: %+v", al)
	}
	e.c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: al.rec.Gen, Tenure: al.rec.Tenure})
	if _, ok := e.c.outboundEntitlement(); !ok {
		t.Fatal("the fixture connection did not take the accepted entitlement")
	}
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
	t.Parallel()
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
	t.Parallel()
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

	// The register store itself, driven directly as TestMQTTInflightStoreDefiniteFailureUntracks
	// drives it: e.track admits an ordinary obligation through the authority (F-105 7b), which
	// never reaches this door, and the door's only production caller is the affine delivery.
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 200, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	testOpenStoreCustody(t, e.sess, pi)
	if !e.sess.storeInflightRegister(e.c, pi, 1, dur, 200, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatalf("an ambiguous store that landed must be adopted, not refused")
	}
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
	t.Parallel()
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
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 300, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 300, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-foreign")
	dur := e.sess.idHash + "_" + nuid.Next()

	// A register for another delivery, whose source cannot be classified (no such
	// consumer exists), so it is Unknown rather than Complete.
	e.sess.mu.Lock()
	e.sess.last_pi = 41
	e.sess.mu.Unlock()
	foreign := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 42, Dur: dur, Seq: 999, QoS: 2, Nonce: mqttNewInflightNonce()})
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, 42), 0, foreign); err != nil {
		t.Fatalf("seed a foreign register: %v", err)
	}

	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 500, 1))
	e.sess.mu.Unlock()
	if pi != 42 || !isNew {
		t.Fatalf("expected the allocation to land on 42, got pi=%d isNew=%v", pi, isNew)
	}
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, 500, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatalf("an identifier held by a live foreign register must be refused")
	}
	// The foreign record is untouched: it may be a live delivery's identity.
	rec, _ := e.loadRegister(t, 42)
	if rec == nil || rec.Nonce != foreign.Nonce {
		t.Fatalf("the foreign register was destroyed (%+v)", rec)
	}
}

// A register whose delivery is provably complete AND PROVABLY NEVER TRANSMITTED is a
// leftover of a lost delete, and its identifier may be reused once it has been reaped at
// its EXACT sequence.
//
// The second conjunct is C-II (F-098) and it is the whole point of this test's shape.
// Source completion is a fact about the DELIVERY; the identifier belongs to the CLIENT
// exchange, and the two part company exactly when a PUBLISH went out and its
// acknowledgement did not come back. This test used to seed a marker-less register — a
// POSSIBLY-ATTEMPTED one — and assert its identifier was reused, which is a release of an
// identifier the receiver may still hold. The sibling below is the arm that now holds
// that line, and the two are deliberately one test: an arm asserting a reap proves
// nothing about the contract unless its twin asserts the refusal.
func TestMQTTInflightStoreReapsProvablyCompleteRegister(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-stale")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	e.sess.last_pi = 10
	e.sess.mu.Unlock()
	stale := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 11, Dur: dur, Seq: 7, QoS: 1, Nonce: mqttNewInflightNonce()})
	// SEEDED AS A PAIR, so the stale register carries its unsent marker and is therefore
	// PROVEN-UNSENT. A bare storeInflightCAS leaves no marker, which is the
	// possibly-attempted shape the sibling test covers.
	staleSeq, err := e.asm.jsa.storeInflightPair(mqttInflightSubject(e.sess.idHash, e.sess.gen, 11), 0, 0, stale)
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
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 600, 1))
	e.sess.mu.Unlock()
	if pi != 11 || !isNew {
		t.Fatalf("expected the allocation to land on 11, got pi=%d isNew=%v", pi, isNew)
	}
	// The stale record's source (sequence 7) is absent from an empty stream while its
	// consumer exists, so it is provably complete.
	testOpenStoreCustody(t, e.sess, pi)
	if !e.sess.storeInflightRegister(e.c, pi, 1, dur, 600, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
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

// TestMQTTInflightStoreReservesAPossiblyAttemptedCompleteRegister is C-II's arm at the
// allocation edge (F-098), and it is the twin of the reap above.
//
// THE SCHEDULE. A QoS 2 PUBLISH goes out under this identifier. Its unsent marker is
// deleted before the bytes leave, which is what makes the exchange durably
// possibly-attempted. The client receives it. The acknowledgement is lost, and so is the
// source acknowledgement's own record, so the SOURCE later reads complete while the
// CLIENT is still holding the identifier. A later allocation lands on the same identifier
// and meets the leftover register.
//
// Before F-098 this arm reaped it, because source completion was read as exchange
// completion — and `bumpPI` then re-minted an identifier the receiver still owned, or the
// freed Receive Maximum capacity let a DIFFERENT identifier transmit. The register must be
// RESERVED instead: possibly-attempted is exactly the state that carries a client
// obligation forward, and recovery's own rule has always said so.
//
// Paired with the mutation that restores the source-completion reap.
func TestMQTTInflightStoreReservesAPossiblyAttemptedCompleteRegister(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-possibly-attempted")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	e.sess.last_pi = 10
	e.sess.mu.Unlock()
	leftover := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 11, Dur: dur, Seq: 7, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 11)
	// NO MARKER, and that is the whole fixture: a bare register is the durable shape a
	// transmitted-then-unacknowledged exchange leaves behind. The twin above seeds a
	// PAIR, and the two differ in exactly this.
	leftoverSeq, err := e.asm.jsa.storeInflightCAS(subject, 0, leftover)
	if err != nil {
		t.Fatalf("seed the possibly-attempted register: %v", err)
	}
	if load := e.asm.loadInflightMarker(mqttMarkerCallerStore, subject); load.kind != mqttInflightResponseAbsent {
		t.Fatalf("the fixture must carry NO marker, got kind=%v", load.kind)
	}

	// The source consumer has to exist for the record to be classifiable at all:
	// inflightSourceState never reads a consumer's absence as completion.
	ccfg := &CreateConsumerRequest{Stream: mqttStreamName, Config: ConsumerConfig{
		Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next(),
		AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "unused",
	}}
	if _, err := e.asm.jsa.createDurableConsumer(ccfg); err != nil {
		t.Fatalf("create the source consumer: %v", err)
	}

	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 600, 1))
	e.sess.mu.Unlock()
	if pi != 11 || !isNew {
		t.Fatalf("expected the allocation to land on 11, got pi=%d isNew=%v", pi, isNew)
	}
	// The source at sequence 7 is absent from an empty stream while its consumer exists,
	// so it is PROVABLY COMPLETE — and that must not be enough.
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, 600, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatal("a possibly-attempted register must reserve its identifier even when its " +
			"source is provably complete: source completion is not exchange completion")
	}
	// THE OCCUPANT SURVIVES, at its own sequence. Asserted as well as the refusal,
	// because a refusal that also destroyed the record would free the identifier by the
	// other route and still pass the check above.
	rec, smsg := e.loadRegister(t, 11)
	if rec == nil || rec.Nonce != leftover.Nonce {
		t.Fatalf("the possibly-attempted register must be preserved, got %+v", rec)
	}
	if smsg == nil || smsg.Sequence != leftoverSeq {
		t.Fatalf("the preserved register must stay at its own sequence, got %+v", smsg)
	}
}

// Withheld deliveries are never registered (R6): they were never transmitted, so
// there is no identifier to make durable.
func TestMQTTInflightDeferredNeverRegistered(t *testing.T) {
	t.Parallel()
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
	pi2, isNew2 := e.sess.trackPublish(dur, testInflightAckSubject(dur, 701, 1))
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-terminal")
	dur := e.sess.idHash + "_" + nuid.Next()

	pi := e.track(t, dur, 800, 1)
	e.sess.mu.Lock()
	removed := e.sess.untrackPublishEntry(pi)
	e.sess.mu.Unlock()
	if removed == nil || removed.regSeq == 0 {
		t.Fatalf("the removed entry must carry its register sequence: %+v", removed)
	}
	e.sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil))
	e.waitRegisterCount(t, 0)
}

// A consumer teardown that abandons started deliveries without acking them takes
// their registers with it: otherwise the next resume would reserve identifiers for
// deliveries whose consumer no longer exists.
func TestMQTTInflightConsumerPurgeDeletesRegisters(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-purge")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.track(t, dur, 900, 1)
	e.track(t, dur, 901, 2)
	if n := e.countRegisters(t); n != 2 {
		t.Fatalf("expected 2 registers, got %d", n)
	}

	e.sess.mu.Lock()
	regs, _ := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	e.sess.mu.Unlock()
	if len(regs) != 2 {
		t.Fatalf("expected 2 register sequences to delete, got %d", len(regs))
	}
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs, mqttTestLicence(e.sess, nil))
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-recover")
	dur := e.sess.idHash + "_" + nuid.Next()

	// A real consumer and a real source message, so the source classifies as live.
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.forgetInMemory()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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

	// The redelivery now reuses it and allocates nothing — which is the whole point.
	// DUP is the ENTRY's transmitted bit, not a return of trackPublish, and here it is
	// FALSE: the fixture registered the delivery and never sent it, so the pair is
	// intact after the restart and recovery installs the entry unsent, holding the
	// marker its first transmission will delete (row 4, [MQTT-3.3.1-1]).
	e.sess.mu.Lock()
	got, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	var transmitted bool
	var markerSeq uint64
	if ack := e.sess.pendingPublish[got]; ack != nil {
		transmitted, markerSeq = ack.transmitted, ack.markerSeq
	}
	e.sess.mu.Unlock()
	if got != pi || transmitted || markerSeq == 0 || isNew {
		t.Fatalf("redelivery after recovery: pi=%d (want %d) transmitted=%v markerSeq=%d isNew=%v", got, pi, transmitted, markerSeq, isNew)
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

// publishSourceDelivered is publishSource with a REAL delivery: the source message is received on
// the consumer's deliver subject and its actual acknowledgement reply returned. An acknowledgement
// of a sequence the consumer never delivered is rejected by the server without confirmation, so a
// test whose settlement is confirmed needs the delivery itself, not a fabricated reply (F-105
// 7c-lc consult 111).
func (e *testInflightEnv) publishSourceDelivered(t *testing.T, dur string) (uint64, string) {
	t.Helper()
	subject := mqttStreamSubjectPrefix + "foo.bar"
	deliver := mqttSubPrefix + nuid.Next()
	sub, err := e.s.nrc.SubscribeSync(deliver)
	if err != nil {
		t.Fatalf("subscribing to the deliver subject: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	ccfg := &CreateConsumerRequest{Stream: mqttStreamName, Config: ConsumerConfig{
		Durable: dur, DeliverSubject: deliver,
		AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: subject,
	}}
	if _, err := e.asm.jsa.createDurableConsumer(ccfg); err != nil {
		t.Fatalf("create the source consumer: %v", err)
	}
	resp, err := e.asm.jsa.storeMsg(subject, -1, []byte("payload"))
	if err != nil {
		t.Fatalf("store the source message: %v", err)
	}
	m, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("receiving the source delivery: %v", err)
	}
	return resp.Sequence, m.Reply
}

// publishSourceGroup is publishSource for a SHARED group consumer: the durable is the
// deterministic mqttSharedNames one and the config carries a DeliverGroup, which is the
// property buildPersistedSessionLocked excludes from the record's Cons. Returns the
// source message's stream sequence.
func (e *testInflightEnv) publishSourceGroup(t *testing.T, dur, group, filter, publish string) uint64 {
	t.Helper()
	queue, _, wantDur := mqttSharedNames(group, filter, 1)
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
	t.Parallel()
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

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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

// A POSSIBLY-ATTEMPTED register whose source consumer is GONE keeps its identifier
// reserved. The delivery was lost rather than completed, so the client may still be
// holding the identifier and it must not be handed to another message.
//
// The fixture TRANSMITS first, which is not incidental. This test's subject is the
// Receive Maximum rule for a delivery that may be in the client's hands, and only a
// transmitted one is that. An earlier version tracked without transmitting, leaving the
// unsent marker in place — a provably never-transmitted exchange, which is the opposite
// population and which F-102 legitimately releases. The comment below always described
// the transmitted case; the fixture now matches it. The proven-unsent counterpart is
// TestMQTTInflightRecoveryConsumesAProvenUnsentRegisterOverALostSource.
func TestMQTTInflightRecoveryKeepsIdentifierWhenConsumerGone(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-lost")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	// Transmitted, so the intent's confirmed delete removes the marker and the pair reads
	// possibly-attempted. Asserted rather than assumed: with the marker still present this
	// test would be measuring the release arm instead.
	e.transmit(t, pi, dur, sseq, true)
	e.waitMarkerAbsent(t, pi)
	if _, mk := e.loadMarker(t, pi); mk != nil {
		t.Fatal("fixture: a transmitted exchange holds no unsent marker")
	}
	if _, err := e.asm.jsa.deleteConsumer(mqttStreamName, dur, false); err != nil {
		t.Fatalf("delete the source consumer: %v", err)
	}
	e.forgetInMemory()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPublish[pi]
	// It counts against Receive Maximum, and MUST: that limit bounds what the client is
	// holding unacknowledged, and a consumer disappearing does not prove the client
	// completed anything. Discounting it would let the broker send a second PUBLISH while
	// the client still holds the first (Spec5 [4.9]). A session stuck this way is a lost
	// message reported honestly. Only a provably never-transmitted delivery may free an
	// identifier, and TWO durable facts prove that: a cancellation tombstone, and an unsent
	// MARKER still naming the register (F-102). Neither is present here — this exchange was
	// transmitted — so nothing licenses a release.
	inflight := e.sess.receiveMaxOccupancyLocked()
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-pubrel")
	dur := e.sess.idHash + "_" + nuid.Next()
	const pi = uint16(5)
	const srcSeq = uint64(31)

	// The PUBREL consumer has to exist first: $MQTT_out is interest-retained, so a
	// store with no interest keeps nothing.
	e.subscribePubRel(t)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, "xxxxxxxxxxxxxxxxxxxxxx", dur, srcSeq, _EMPTY_, _EMPTY_, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}
	e.forgetInMemory()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
	got, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, srcSeq, 2))
	e.sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("a source past PUBREC must not be re-sent, got pi=%d", got)
	}
}

// subscribePubRel establishes this session's PUBREL consumer and delivery
// subscription, so a stored PUBREL is actually retained.
func (e *testInflightEnv) subscribePubRel(t *testing.T) {
	t.Helper()
	e.installServingAuthority(t)
	if err := e.sess.ensurePubRelConsumerSubscription(e.c); err != nil {
		t.Fatalf("ensurePubRelConsumerSubscription: %v", err)
	}
}

// installServingAuthority gives the env's session the SERVING outbound authority a real
// session already holds by the time it provisions a PUBREL or source consumer — minted at
// CONNECT in production (F-105 cutover). A unit env that never CONNECTs must install it so the
// claim-before-create provisioning (opProvisionResource) has a serving authority to admit
// under: there is deliberately no missing-authority fallback.
func (e *testInflightEnv) installServingAuthority(t *testing.T) {
	t.Helper()
	testInstallServingAuthority(t, e.asm, e.sess)
}

// testInstallServingAuthority installs the SERVING outbound authority a real session already
// holds by the time it provisions a PUBREL or source consumer — minted at CONNECT in
// production (F-105 cutover). A unit env that never CONNECTs must install it so the
// claim-before-create provisioning (opProvisionResource) has a serving authority to admit
// under: there is deliberately no missing-authority fallback.
func testInstallServingAuthority(t *testing.T, asm *mqttAccountSessionManager, sess *mqttSession) {
	t.Helper()
	epoch := asm.sessStreamEpoch
	a := &mqttOutboundAuthority{
		ID: sess.id, Epoch: epoch, Gen: sess.gen, Tenure: mqttNewInflightNonce(),
		State: mqttAuthorityServing, Rev: 1, Txn: "txn-boot", OwnerTxn: "txn-boot",
	}
	if al := asm.jsa.loadOutboundAuthority(sess.id, epoch); al.status == mqttOutboundValid &&
		al.rec != nil && al.rec.State == mqttAuthorityServing {
		a = al.rec
	} else {
		b, _ := json.Marshal(a)
		msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
		if _, err := asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(sess.id)), hdr, msg); err != nil {
			t.Fatalf("installing serving authority: %v", err)
		}
	}
	// AND THE FIXTURE CONNECTION IS ACCEPTED UNDER IT, as a real CONNECT's accept loop records
	// (F-105 row 2): an ordinary acquisition names the connection's own entitlement.
	sess.mu.Lock()
	c := sess.c
	// The session object records the same accepted entitlement (F-105 7b's boundEnt).
	if !sess.boundEntOK {
		sess.boundEnt, sess.boundEntOK = mqttOutboundEntitlement{Gen: a.Gen, Tenure: a.Tenure}, true
	}
	sess.mu.Unlock()
	c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: a.Gen, Tenure: a.Tenure})
}

// Where a PUBLISH slot and an exact PUBREL record name one exchange, recovery completes
// the durable handoff before source classification. What happens to its SOURCE still
// depends on whether that source may be re-offered.
//
// Source still LIVE means the handoff's source acknowledgement was never proven, so the
// source will be re-offered. The register must be RETIRED and kept: deleting it destroys
// the only record that can carry that fact, and once the client's PUBCOMP removes the
// PUBREL nothing is left to say the payload was already answered.
//
// Mutation check: delete the register on this branch (the original rule) and the retained
// -and-retired assertions fail.
func TestMQTTInflightRecoveryPubRelRetiresRegisterWhenSourceLive(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-precedence")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	// ADMITTED through the authority: an ordinary QoS 2 register stands beside its exchange,
	// and route B records the PUBREC the stored record proves on that exchange before it
	// rebinds the slot (consult 25). An exchange-less register is not a shape production writes.
	pi := e.trackAdmitted(t, dur, sseq, 2)
	e.subscribePubRel(t)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, e.nonceOf(pi), dur, sseq, _EMPTY_, _EMPTY_, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}
	e.forgetInMemory()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
	if rec == nil || !rec.Retired || rec.State != mqttInflightPubRelReady || rec.PubRelStreamSeq == 0 {
		t.Fatalf("the owner slot must be ready and marked retired, got %+v", rec)
	}
}

// A provably complete SOURCE does not mean the QoS 2 exchange is complete: a present
// PUBREL means the client still owes PUBCOMP. Route B therefore preserves and re-phases
// the owner slot, while correctly declining to install source-retirement bookkeeping.
func TestMQTTInflightRecoveryPubRelKeepsOwnerWhenSourceComplete(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-precedence-done")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	// ADMITTED through the authority: an ordinary QoS 2 register stands beside its exchange,
	// and route B records the PUBREC the stored record proves on that exchange before it
	// rebinds the slot (consult 25). An exchange-less register is not a shape production writes.
	pi := e.trackAdmitted(t, dur, sseq, 2)
	e.subscribePubRel(t)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, e.nonceOf(pi), dur, sseq, _EMPTY_, _EMPTY_, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}
	// The source was acknowledged: with its consumer still present, absence proves it.
	if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}
	e.forgetInMemory()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
	e.waitRegisterCount(t, 1)
	rec, _ := e.loadRegister(t, pi)
	if rec == nil || rec.State != mqttInflightPubRelReady || rec.PubRelStreamSeq == 0 {
		t.Fatalf("the source-complete exchange lost its continuing owner slot: %+v", rec)
	}
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-duplicate")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	older := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 3, Dur: dur, Seq: sseq, QoS: 2, Nonce: mqttNewInflightNonce()})
	newer := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 4, Dur: dur, Seq: sseq, QoS: 2, Nonce: mqttNewInflightNonce()})
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, 3), 0, older); err != nil {
		t.Fatalf("seed the older register: %v", err)
	}
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, e.sess.gen, 4), 0, newer); err != nil {
		t.Fatalf("seed the newer register: %v", err)
	}

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
	got, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("the source was re-sent under identifier %d, which cannot be proven to be the one the client holds", got)
	}
}

// A RETIRED register whose source is provably complete CONVERGES: the retirement is not
// re-established, because the acknowledgement that could not be confirmed had in fact
// landed and there is nothing left to suppress. Without that, every resume rebuilds the
// retirement from its own record and one accumulates per unproven acknowledgement for as
// long as the session lives.
//
// WHAT CONVERGENCE DOES NOT INCLUDE IS DROPPING THE REGISTER, and this assertion is the
// replacement for one that did (F-098, consult D-6 row (c), and D-7). `Retired` is written
// on the strength of the client's own PUBREC, so the PUBLISH provably reached the client and
// the client may still hold the identifier. The SOURCE's completion is a fact about the
// source alone and never discharges the client's half of the exchange — that is C-II — so
// this pass consumes the source and PRESERVES the register, keeping its identifier occupied.
//
// The old assertion required the register to disappear. Under C-IV that delete can never be
// licensed: arm 4 excludes a retired record whatever a marker or a cancellation says, and
// `publish` is not a terminal phase, so arm 3 does not apply either. Staging it produced the
// defect this test now pins — the refused delete left the durable record standing while
// nothing reserved its identifier, and a COLD resume has no in-memory entry to carry that
// obligation.
//
// The fourth assertion is what stops an unrelated capacity refusal masking a missing
// exclusion: allocation must still SUCCEED, on a different nonzero identifier.
//
// Mutation checks: (a) restore the staged mqttSlotDeleteComplete in the retired branch of
// recoverInflightRegisters — the preservation assertion fails; (b) install the occupancy
// exclusion inside the resident-entry branch instead of independently of it — the
// cold-resume exclusion assertions fail while everything else still passes; (c) remove the
// completeness test from that branch — the convergence assertion fails.
func TestMQTTInflightRecoveryPreservesARetiredRegisterWhoseSourceCompleted(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-retired-converge")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	// Mark it retired the way the PUBREC path does.
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if _, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, ack.regSeq, ack.regNonce, _EMPTY_, _EMPTY_); !ok {
		t.Fatalf("could not mark the register retired")
	}
	// The source turns out to have been acknowledged after all: with its consumer still
	// present, absence proves it.
	if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}

	// A COLD resume, which is the schedule the defect needed: `forgetInMemory` alone leaves
	// `sendsInFlight` behind, and an exclusion surviving there would satisfy the assertions
	// below without recovery having installed anything. Clearing it makes each pass start
	// from the durable records only, exactly as a restarted process does.
	forgetCold := func() {
		e.forgetInMemory()
		e.sess.mu.Lock()
		e.sess.sendsInFlight = nil
		e.sess.mu.Unlock()
	}
	forgetCold()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	retired := e.sess.sourceRetiredLocked(dur, sseq)
	e.sess.mu.Unlock()
	if retired {
		t.Fatalf("a retirement whose source is complete must not be re-established")
	}
	e.waitRegisterCount(t, 1)
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was allocatable after a cold resume, while its retired "+
			"register still stands: the client's PUBREC proves the PUBLISH reached it, so the "+
			"identifier may be held ([MQTT-2.3.1-4])", pi)
	}

	// AND AGAIN, from cold. The obligation lives on the durable record, so every pass that
	// finds that record must re-assert it; a pass that installed it once and then let a
	// later one release it would expose the identifier just as surely.
	forgetCold()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("second recoverInflightRegisters: %v", err)
	}
	e.waitRegisterCount(t, 1)
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d became allocatable on a repeated recovery pass", pi)
	}

	// THE IDENTIFIER SPACE IS STILL USABLE. Excluding one identifier must not read as
	// "allocation is refused", or the assertions above would pass against a session that
	// simply cannot allocate at all.
	e.sess.mu.Lock()
	other := e.sess.trackPublishRetained(2)
	e.sess.mu.Unlock()
	if other == 0 || other == pi {
		t.Fatalf("retained allocation returned %d, want a different nonzero identifier: the "+
			"exclusion must hold %d alone, not the whole space", other, pi)
	}
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
// assertion fails; (b) have the SECOND recovery drop the opaque reservation it finds rather
// than keeping it — the second-recovery assertions fail; (c) do not set piSpaceIncomplete for
// an opaque register — the fresh-source-delivery assertion fails.
func TestMQTTInflightRecoveryReservesUnreadableRegister(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-unusable")
	dur := e.sess.idHash + "_" + nuid.Next()

	const pi = uint16(8)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	msg, hdrLen := buildInflightRecord(0, &mqttInflightRecord{State: mqttInflightPublish})
	if _, err := e.asm.jsa.storeMsg(subject, hdrLen, msg); err != nil {
		t.Fatalf("seed an unusable register: %v", err)
	}

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
		t.Fatalf("the reservation must carry its register sequence: it is what says this hold stands on a register rather than being a retained send's register-less shape (R6)")
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
	got, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1300, 2))
	e.sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("a source delivery was allocated identifier %d while an unreadable register's delivery was unidentifiable", got)
	}

	// A SECOND resume of the same session object — a same-broker reconnect — must not
	// lift the restriction. The reservation from the first resume is already in
	// pendingPublish, so this pass installs nothing; a condition counting what it
	// installed rather than what it FOUND would read that as "nothing is opaque" and
	// re-enable allocation with the unreadable record still there.
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("second recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	incomplete = e.sess.piSpaceIncomplete
	_, keptReserved := e.sess.pendingPublish[pi]
	got, _ = e.sess.trackPublish(dur, testInflightAckSubject(dur, 1301, 2))
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
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	incomplete = e.sess.piSpaceIncomplete
	got, _ = e.sess.trackPublish(dur, testInflightAckSubject(dur, 1300, 2))
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-inmemory")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.sess.mu.Lock()
	live := e.sess.pendingPublish[pi]
	liveAck := testInflightAckSubject(dur, sseq, 1)
	live.jsAckSubject = liveAck
	e.sess.mu.Unlock()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
			pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1200, 1))
			e.sess.mu.Unlock()
			if pi != 0 {
				t.Fatalf("allocated packet identifier %d while the identifier space was unknown", pi)
			}

			// A resume that completes lifts the restriction.
			restore()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			e.sess.mu.Lock()
			incomplete = e.sess.piSpaceIncomplete
			pi, _ = e.sess.trackPublish(dur, testInflightAckSubject(dur, 1200, 1))
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
	t.Parallel()
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
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1400, 1))
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
	t.Parallel()
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
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1500, 1))
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1500, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
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
	pi2, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1500, 2))
	transmitted := pi2 != 0 && e.sess.pendingPublish[pi2].transmitted
	e.sess.mu.Unlock()
	if pi2 == 0 || !isNew {
		t.Fatalf("the redelivery must be a fresh allocation: pi=%d isNew=%v", pi2, isNew)
	}
	if transmitted {
		t.Fatalf("the first transmission of a never-sent message must carry DUP=0 [MQTT-3.3.1-1]: a fresh entry is not transmitted")
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
	t.Parallel()
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
	got, _ := sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
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
// after this UNSUBSCRIBE has given up this membership's tracking, leaving the client holding
// an exchange nothing here remembers. (The consumer itself may SURVIVE as a drain keeping
// its durable; what ends either way is this membership's claim on the delivery.) Removing the entry hands the decision to the
// store's own ownership revalidation, which then refuses the send.
//
// Mutation check: `continue` without untracking (the original) and the entry survives, so
// the store confirms and the send proceeds.
func TestMQTTInflightStageDrainCancelsPendingRegister(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-stage-cancel")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	// A delivery mid-registration: allocated, regPending, not yet confirmed.
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1600, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}

	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	e.sess.mu.Lock()
	e.sess.cons = map[string]*ConsumerConfig{sid: cc}
	staged := e.sess.stageDrain(sid, cc, nil, mqttTestLicence(e.sess, nil))
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
	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1600, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatalf("a cancelled allocation must not be released for sending")
	}
}

// A cancelled registration is recorded as WITHHELD, so a rolled-back UNSUBSCRIBE does not
// cost the first transmission its DUP=0.
//
// Nothing was put on the wire, but the redelivery arrives with a JetStream delivery count
// above one, and trackPublish derives dup from that count unless the sequence is known to
// have been withheld ([MQTT-3.3.1-1]). A COMMITTED teardown drops the marker instead,
// because the staging it protects is settled and no first transmission of that sequence
// remains to protect.
//
// Mutation check: cancel without recording the marker and the DUP assertion fails; drop
// the commitDrains cleanup and the committed case leaves the marker behind.
func TestMQTTInflightStageDrainCancelKeepsFirstSendDupClear(t *testing.T) {
	t.Parallel()
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
		pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq-1, 1))
		testConfirmLanded(e.sess, pi, true)                            // landed, then sent: endPublishSend records the write
		e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1)) //nolint:dogsled // regPending by design
		e.sess.cons = map[string]*ConsumerConfig{sid: cc}
		if !e.sess.stageDrain(sid, cc, nil, mqttTestLicence(e.sess, nil)) {
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
	rePI, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	transmitted := rePI != 0 && e.sess.pendingPublish[rePI].transmitted
	e.sess.mu.Unlock()
	if !isNew {
		t.Fatalf("the redelivery of a cancelled sequence is a fresh allocation")
	}
	if transmitted {
		t.Fatalf("the first transmission of a never-sent message must carry DUP=0 [MQTT-3.3.1-1]: a fresh entry is not transmitted")
	}

	// COMMITTED: the marker goes, because the staging it protects is settled and no first
	// transmission of that sequence remains to protect. Deliberately NOT "nothing will
	// re-offer that sequence": a committed drain KEEPS its consumer, so an unacknowledged
	// cancelled sequence can still be re-offered and is acknowledged by the delivery
	// callback's unclaimed arm (see unsub_drain.go's commitDrains). This assertion is about
	// marker removal and nothing more.
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
	t.Parallel()
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-drain-ambiguous")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	sseq := e.publishSource(t, dur)
	// Two registers, one source, different identifiers: a superseded broker wrote one.
	// Each admitted as production admits it (F-105 7b), beside its own exchange.
	for _, pi := range []uint16{3, 4} {
		e.landAdmission(t, pi, 2, dur, sseq, mqttNewInflightNonce())
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

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-tombstone")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 2000, 1))
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

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 2000, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
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
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPublish[pi]
	inflight := e.sess.receiveMaxOccupancyLocked()
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
	t.Parallel()
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
	regs, _ := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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

	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs, mqttTestLicence(e.sess, nil))

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
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-ladder-tombstone")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	// The staging half records the verdict — this is what the proven rung must retire.
	// e.track allocates and stores the register and never opens a send section, so this
	// entry is PROVEN UNSENT (its marker is present and it is not transmitted) and the
	// staging gate voids it. The fixture covers the never-sent population; it is not a
	// transmitted one, and F-101 corrected the label.
	_, abandoned := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	regs, _ := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if len(abandoned) != 1 || abandoned[0] != nonce {
		t.Fatalf("the staging must void the proven-unsent register, got %v", abandoned)
	}

	// Only the DELETE fails; the tombstone CAS still works.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: delete unavailable") }
	e.asm.mu.Unlock()

	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs, mqttTestLicence(e.sess, nil))

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
	t.Parallel()
	e := testNewInflightEnv(t, "client-purge-secure")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
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
		regs, cancels := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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

		if !e.sess.secureRegCancellations(e.c, recordTx{}, cancels, mqttTestLicence(e.sess, nil)) {
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
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-staged-cancel")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
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
	t.Parallel()
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
	_, sent := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	_, reserved := restored.pendingPublish[pi]
	inflight := restored.receiveMaxOccupancyLocked()
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
	t.Parallel()
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
	_, sent := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
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
	}, mqttTestLicence(e.sess, nil))
	e.sess.secureRegCancellations(nil, recordTx{}, []mqttRegCancellation{
		{pi: pi, nonce: "nonce-x", durable: dur, sseq: sseq, reply: testInflightAckSubject(dur, sseq, 1)},
	}, mqttTestLicence(e.sess, nil))
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
	t.Parallel()
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
		// convert=false: this test owns the DESTRUCTIVE staging, whose cancel-and-void
		// behavior is unchanged. The conversion candidate's staging is covered by the
		// QoS 0 downgrade tests.
		cancels, closed, _ := e.sess.stageDeferredDeleteLocked(e.c, cc, false)
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
	t.Parallel()
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
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(idHash, gen, pi), 0, freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: pi, Dur: e.sess.idHash + "_" + nuid.Next(), Seq: 4, QoS: 2,
		Nonce: mqttNewInflightNonce(), Cancelled: true,
	})); err != nil {
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
	t.Parallel()
	const clientID = "client-cancel-crash"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	// A real consumer and a real source message, so while they exist the register's source
	// is LIVE — which is what makes recovery reserve the identifier without the intent.
	sseq := e.publishSource(t, dur)

	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
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
	e.sess.stageDrain(sid, cc, nil, mqttTestLicence(e.sess, nil))
	e.sess.mu.Unlock()
	if err := e.sess.save(tok); err != nil {
		t.Fatalf("persist the unsubscribe: %v", err)
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The CAS that was already on the wire LANDS, and the broker dies before its
	// confirmation runs: nothing marks the register, and no obligation names its sequence.
	// Admitted as production admits it (F-105 7b): the write on the wire is the admission batch,
	// register, exchange and duty entry together, never a register alone.
	e.landAdmission(t, pi, 2, dur, sseq, nonce)
	// And the UNSUBSCRIBE's consumer delete completed, so nothing can classify the source.
	if _, err := e.asm.jsa.deleteConsumer(mqttStreamName, dur, true); err != nil {
		t.Fatalf("delete the source consumer: %v", err)
	}

	// The restart: the session comes back from the record alone.
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	restored.mu.Lock()
	_, reserved := restored.pendingPublish[pi]
	inflight := restored.receiveMaxOccupancyLocked()
	incomplete := restored.piSpaceIncomplete
	intents := len(restored.cancelledRegNonces)
	// The admission's exchange is custody too (F-105 row 5): the void must end it, or the
	// identifier stays held by the ledger whatever the maps say.
	custodyHeld := restored.custody.holdsPI(pi) || restored.custody.withholdsAllocation()
	restored.mu.Unlock()
	if custodyHeld {
		t.Fatalf("the cancelled admission's custody must end with the void: identifier %d is still held", pi)
	}
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
	t.Parallel()
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

// A verdict carried by an AMBIGUOUS save is durable once the resolver settles that write,
// and the revision it marshalled is credited with it.
//
// The dangerous outcome used to be "resolved: it never landed" — the entry was then durable
// nowhere, and nothing would notice, because the convergence points skip a session whose set
// has not changed since its last attempt. That verdict no longer exists: the resolver
// RE-ATTEMPTS the attempt's exact bytes, which carry this nonce, so settling the ambiguity is
// what makes the verdict durable. Crediting is the resolver's too — the proof is the
// durability boundary, not the intent of whoever wrote.
//
// The credit is asserted with the record fence still HELD. Releasing it first would let the
// void-verdict release handoff persist the gap through a second save, which would mask a
// resolver that credited nothing.
//
// Mutation check (M4): skip creditVoidedRegsLocked on the re-attempt's success and the
// revision stays uncredited.
func TestMQTTInflightVoidVerdictAmbiguousSaveIsSettled(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-void-unlanded")
	// So sess.seq names the record the resolver will find at its base sequence.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	e.sess.mu.Lock()
	idHash, base := e.sess.idHash, e.sess.seq
	// The verdict the ambiguous save was carrying, marshalled INTO the attempted
	// bytes exactly as saveWithDetail would have marshalled it.
	e.sess.voidRegNonceLocked("nonce-unlanded")
	rev := e.sess.voidedRegsRev
	ps := e.sess.buildPersistedSessionLocked()
	body, merr := json.Marshal(&ps)
	if merr != nil {
		e.sess.mu.Unlock()
		t.Fatalf("marshal the attempted record: %v", merr)
	}
	testMarkUncertainCarrying(e.sess, base, body, rev)
	e.sess.mu.Unlock()

	// The fence is held by this test, so the resolution's own credit is observed
	// before any release can converge the gap behind it.
	tok := e.sess.beginRecordTx()
	res, err := e.sess.resolveUncertainWriteInTx(tok)
	if err != nil {
		t.Fatalf("resolveUncertainWriteInTx: %v", err)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}
	e.sess.mu.Lock()
	saved := e.sess.voidedRegsSavedRev
	e.sess.mu.Unlock()
	if saved != rev {
		t.Fatalf("the re-attempted bytes are durable, so their revision must be credited: rev %d, saved %d", rev, saved)
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The record carries it, because the resolver re-attempted the very bytes that did.
	psRec, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if psRec == nil || len(psRec.CancelledRegs) != 1 || psRec.CancelledRegs[0] != "nonce-unlanded" {
		t.Fatalf("settling the ambiguity must make the verdict durable, got %v", psRec)
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-late-cancel")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1900, 1))
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

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1900, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
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
	pi2, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1901, 1))
	e.sess.mu.Unlock()
	testOpenStoreCustody(t, e.sess, pi2)
	if !e.sess.storeInflightRegister(e.c, pi2, 1, dur, 1901, e.nonceOf(pi2), _EMPTY_, _EMPTY_) {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-stalled-store")
	dur := e.sess.idHash + "_" + nuid.Next()

	// An allocation whose store never settles in this test: allocated, then withdrawn by a
	// teardown while "in flight".
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 3100, 1))
	cancels, _ := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 3200, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	cancels, _ := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	if len(cancels) != 1 || cancels[0].nonce != nonce {
		t.Fatalf("the staging must withdraw the pending registration, got %+v", cancels)
	}

	// The CAS that was in flight LANDS...
	seq, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(idHash, gen, pi), 0, freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: pi, Dur: dur, Seq: 3200, QoS: 2, Nonce: nonce,
	}))
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
	if e.sess.confirmInflightRegister(e.c, mqttLandedRegister{gen: e.sess.gen, pi: pi, jsDur: dur, sseq: 3200,
		regSeq: seq, nonce: nonce, lin: nonce, qos: 1}, 0, false) {
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

// A RETIRED register is never queued into consumer teardown at all.
//
// Retired proves the source crossed PUBREC, not that the slot reached an explicit
// past-PUBREC phase or that PUBCOMP arrived. Deleting the source consumer makes the source
// suppression unnecessary, but it does not license consumer teardown's raw deletion
// ladder. The source-side bookkeeping goes; the slot remains for recovery to reconcile.
//
// Mutation check: return the retired slot in the teardown batch and the assertion below
// catches the pre-PUBCOMP delete becoming reachable again.
func TestMQTTInflightRetiredRegisterIsNeverTombstoned(t *testing.T) {
	t.Parallel()
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
	if newSeq, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, regSeq, nonce, _EMPTY_, _EMPTY_); !ok {
		t.Fatalf("the retirement must be durable for this test")
	} else {
		regSeq = newSeq
		e.sess.mu.Lock()
		if rs := e.sess.srcAckOwed[mqttDeferredKey{dur, sseq}]; rs != nil {
			rs.regSeq = newSeq
		}
		e.sess.mu.Unlock()
	}

	// The teardown stages and purges. The retired slot is not one of its deletion targets.
	e.sess.mu.Lock()
	_, transmitted := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	regs, _ := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	verdicts := len(e.sess.cancelledRegNonces)
	e.sess.mu.Unlock()
	if len(transmitted) != 0 || verdicts != 0 {
		t.Fatalf("a retirement must not be voided at staging, got %v (%d verdicts)", transmitted, verdicts)
	}
	found := false
	for _, v := range regs {
		if v.seq == regSeq {
			found = true
		}
	}
	if found {
		t.Fatalf("consumer teardown queued the past-PUBREC owner slot for deletion: %+v", regs)
	}
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs, mqttTestLicence(e.sess, nil))
	// The session record exists, so the restart below resumes THIS incarnation rather than
	// minting a new generation whose register walk would find nothing.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}

	// The register survives UNCHANGED: still a retirement, not a tombstone or a
	// pre-PUBCOMP deletion.
	rec, _ := e.loadRegister(t, pi)
	if rec == nil || !rec.Retired || rec.Cancelled {
		t.Fatalf("a retired register must survive consumer teardown unchanged, got %+v", rec)
	}

	// With no exact PUBREL in this fixture, restart cannot manufacture pubrel_ready. It
	// still knows which identifier the retired source used, so it reserves that identifier
	// as a non-actionable PUBREL-side entry without declaring the whole identifier space
	// opaque. It must never turn the survivor back into a deliverable PUBLISH entry.
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	reserved := restored.pendingPubRel[pi]
	_, deliverable := restored.pendingPublish[pi]
	incomplete := restored.piSpaceIncomplete
	restored.mu.Unlock()
	if reserved == nil || deliverable || incomplete {
		t.Fatalf("restart did not reserve the retired publish-shaped slot safely: reserved=%+v deliverable=%v incomplete=%v",
			reserved, deliverable, incomplete)
	}
}

// A surviving disconnect's shared purge reaches a session RESTORED FROM ITS RECORD.
//
// Shared group consumer configs are deliberately excluded from the persisted Cons
// (buildPersistedSessionLocked): the group is arbitrated by SharedSubs and the configs are
// rebuilt by processSubs during restoration, which runs after register recovery. So a
// session that comes back from
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
	t.Parallel()
	const clientID = "client-shared-purge-restored"
	e := testNewInflightEnv(t, clientID)

	// A shared subscription on a multi-level filter is TWO group durables — the main
	// "foo.>" consumer and the level-up "foo" one — as processSubs creates them.
	const group, mainFilter, lvlFilter = "g", "foo.>", "foo"
	// The session key names the NATS subject, as production keys it (mqttParseSharedKey).
	const key = "$share/" + group + "/" + mainFilter
	mainQ, _, mainDur := mqttSharedNames(group, mainFilter, 1)
	lvlQ, _, lvlDur := mqttSharedNames(group, lvlFilter, 1)
	// ...plus a PLAIN delivery, whose pending state the purge must leave alone: it is
	// this session's own, it resumes under its own identifier, and nothing else holds it.
	plainDur := e.sess.idHash + "_" + nuid.Next()

	// QoS 1 on the group durables: a shared QoS 2 delivery is moved to the session-affine
	// durable BEFORE trackPublish, so a QoS 2 register under a group durable is not a shape
	// the broker produces.
	mainSseq := e.publishSourceGroup(t, mainDur, group, mainFilter, "foo.bar")
	lvlSseq := e.publishSourceGroup(t, lvlDur, group, lvlFilter, lvlFilter)
	plainSseq := e.publishSource(t, plainDur)
	// A shared delivery is admitted through the authority beside an exchange naming the exact
	// acquisition it depends on (7a), so the membership is JOINED — both groups of the key —
	// before its deliveries are admitted.
	e.joinSharedForTest(t, key, group, mainFilter, false)
	e.joinSharedForTest(t, key, group, lvlFilter, true)
	piMain := e.trackSharedAdmitted(t, mainDur, mainSseq)
	piLvl := e.trackSharedAdmitted(t, lvlDur, lvlSseq)
	piPlain := e.track(t, plainDur, plainSseq, 2)

	// The in-memory shape of a live session: membership for the shared key, a plain
	// subscription, and configs for all three. The save below is the real exclusion.
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"bar": 2}
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
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
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

	restored.purgeSharedPending(mqttTestLicence(restored, nil))

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

// C23: a SAME-OBJECT resume whose custody contest finds the close's exact-sequence delete
// already landed (the contest's Closed outcome) retires the reused object's live entry, not
// only the durable record. Absence is the durable decision; a resident entry left behind would
// redeliver the source under an identifier the close already freed.
//
// SLOT-ONLY (experimental affine): since F-105 7b the contest, and so its Closed outcome, runs
// for that class alone. The ordinary drop routes retire their resident entries through the drop
// loop's own handoff, which is why this is the input C23 needs.
//
// Mutation check: N21-C23, the consumed-entry removal dropped from the apply phase -> the
// resident entry survives the resume.
func TestMQTTInflightSlotOnlyConsumedCloseRetiresLiveMemory(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-slot-only-close-memory")
	// AN ACCEPTED RECORD, so the slot-only contest can order the remote claim below this
	// session's acceptance (as testSeedOwedSlot establishes it), saved twice so an older claim
	// sequence exists. The rig's accepted connection state is kept: the resume runs under it.
	if e.c.mqtt == nil {
		e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	}
	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)
	testPersistAcceptedRecord(t, e)
	e.sess.mu.Lock()
	ourSeq := e.sess.seq
	e.sess.mu.Unlock()
	if ourSeq < 2 {
		t.Fatalf("the fixture needs an acceptance sequence above 1, got %d", ourSeq)
	}
	dur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_close"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	// A remote owner's claim sits on the record, so the re-run must attempt a claim CAS.
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	remote := *rec
	remote.Claim, remote.ClaimSeq = "previous-remote-owner", ourSeq-1
	if _, err := e.asm.jsa.storeInflightCAS(e.regSubject(pi), msg.Sequence, &remote); err != nil {
		t.Fatalf("seed the remote claim: %v", err)
	}
	e.sess.mu.Lock()
	ownClaim := e.sess.regClaim
	armedPi := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if armedPi != pi {
		t.Fatalf("the live mapping must be armed before the re-run: cpending=%d want %d", armedPi, pi)
	}

	// The close's exact-sequence delete wins inside the claim window.
	var raced bool
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r.Claim == ownClaim && mqttDurIsSlotOnlyClass(r.Dur) && !raced {
			raced = true
			if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, expectSeq, true); err != nil {
				t.Errorf("the close's delete must land first: %v", err)
			}
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	defer func() {
		e.asm.mu.Lock()
		e.asm.testInflightStore = nil
		e.asm.mu.Unlock()
	}()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("same-object barrier re-run: %v", err)
	}
	if !raced {
		t.Fatal("fixture: the contest never wrote, so the close never raced it")
	}
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	_, mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if held || mapped {
		t.Fatalf("a consumed close must retire the reused object's live entry: held=%v mapped=%v", held, mapped)
	}
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-shared-claim-poison-memory")
	_, _, dur := mqttSharedNames("g", "claim.poison.mem", 1)
	sseq := e.publishSourceGroup(t, dur, "g", "claim.poison.mem", "claim.poison.mem")
	pi1 := e.track(t, dur, sseq, 1)
	pi2 := pi1 + 1
	// The sibling was written elsewhere and carries that writer's claim.
	subj2 := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi2)
	if _, err := e.asm.jsa.storeInflightCAS(subj2, 0, freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: pi2, Dur: dur, Seq: sseq, QoS: 1, Nonce: mqttNewInflightNonce(), Claim: "written-elsewhere",
	})); err != nil {
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
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("same-object barrier re-run: %v", err)
	}

	e.sess.mu.Lock()
	_, mapped := e.sess.cpending[dur][sseq]
	demoted := e.sess.pendingPublish[pi1]
	_, sibling := e.sess.pendingPublish[pi2]
	incomplete := e.sess.piSpaceIncomplete
	// The source's redelivery must find no identifier and be withheld, not resent
	// under pi1.
	gotPi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
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

// A never-transmitted reservation declines every acknowledgement of it. R1 makes all three
// undurable shapes illegitimate to acknowledge: an entry whose register has not confirmed
// (regPending), a settled-but-unproven cancellation placeholder (regCancelled, regPending
// cleared), and a foreign reservation (the sibling test above) were NONE of them transmitted,
// so no PUBACK/PUBREC of them can be real. Accepting one tears the entry down, acknowledges
// the source away — the lost message — and, on a successful PUBREC, hands off a bodyless entry
// carrying jsDur == pubRelDurable() and regSeq 0 that admission cannot tell from a live
// retained exchange, so a PUBREL later goes out with no durable action: F-092's own class.
//
// Mutation checks: drop `|| ack.regPending` from the decline at mqtt.go and both regPending
// arms fail; drop `|| ack.regCancelled` and the settled-cancelled arm fails (its regPending is
// already cleared, so that term is the only thing catching it).
func TestMQTTInflightUndurableReservationDeclinesAcknowledgement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		reason    byte
		cancelled bool // settled cancellation placeholder: regCancelled=true, regPending=false
	}{
		{"regPending successful PUBREC", 0, false},
		{"regPending failed PUBREC", 0x80, false},
		{"settled regCancelled successful PUBREC", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "undurable-decline")
			const dur = "SRCDUR"
			sseq := e.publishSource(t, dur)
			e.sess.mu.Lock()
			pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
			ack := e.sess.pendingPublish[pi]
			if pi == 0 || !isNew || ack == nil || !ack.regPending {
				e.sess.mu.Unlock()
				t.Fatalf("the allocation must be a regPending reservation: pi=%d isNew=%v ack=%+v", pi, isNew, ack)
			}
			if tc.cancelled {
				// The settled cancellation placeholder: its store settled so regPending is
				// cleared, but the allocation was cancelled and never proven, so it was
				// still never transmitted and the same decline must cover it.
				ack.regPending, ack.regCancelled = false, true
			}
			wantPending, wantCancelled := ack.regPending, ack.regCancelled
			e.sess.mu.Unlock()

			// A late acknowledgement arrives through the REAL path on the bound client.
			e.c.mqtt = &mqtt{sess: e.sess, proto: mqttProtoLevel5}
			e.sess.mu.Lock()
			e.sess.c = e.c
			e.sess.mu.Unlock()
			if err := e.c.mqttProcessPublishReceived(pi, true, tc.reason); err != nil {
				t.Fatalf("the decline must be silent, got %v", err)
			}

			e.sess.mu.Lock()
			e.sess.c = nil
			got := e.sess.pendingPublish[pi]
			_, asPubRel := e.sess.pendingPubRel[pi]
			e.sess.mu.Unlock()
			if got == nil || got.regPending != wantPending || got.regCancelled != wantCancelled {
				t.Fatalf("the undurable reservation must stand unchanged: got %+v (want regPending=%v regCancelled=%v)",
					got, wantPending, wantCancelled)
			}
			if asPubRel {
				t.Fatal("a never-transmitted reservation must not be handed off to a PUBREL entry")
			}
			// No PUBREL reached $MQTT_out: nothing durable was written, so admission has no
			// slot-less record it could later misclassify as retained and transmit.
			if n, _, err := e.asm.jsa.streamSubjects(mqttOutStreamName, e.sess.pubRelSubject); err == nil && len(n) != 0 {
				t.Fatalf("a never-transmitted reservation must not store a PUBREL, found %v", n)
			}
		})
	}
}

// The purge reaches the strays a membership removal deliberately LEAVES BEHIND.
//
// This fixture builds the shape by hand: the membership nil'd, no shared cons entry, and a
// TRANSMITTED delivery's cpending/pendingPublish entries left behind — which is what
// voidRegistrationsLocked does, recording the verdict and leaving the entry "for the
// teardown's purge to remove". BOTH views of the group are then empty while the pending state
// is still live, which is why the purge derives its targets from that state instead.
//
// Note on how the real forced leave reaches this shape, since it changed: since F-050 it runs
// AFTER processSubs, not before, and there is no shared cons entry because the pre-CONNACK
// restore excludes shared filters for a non-v5 connection — a different reason for the same
// absence. A RESIDENT session can still carry shared cons entries from an earlier v5
// connection, so "no cons entry" is a property of this fixture, not of every forced leave.
//
// Mutation check: derive the targets from sharedSubs (or from any union of the two views)
// and this test fails; the strays are unreachable through either.
func TestMQTTInflightSharedPurgeReachesForcedLeaveStrays(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-shared-purge-strays")
	const group, filter = "g", "foo.bar"
	_, _, dur := mqttSharedNames(group, filter, 1)
	plainDur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	plainSseq := e.publishSource(t, plainDur)
	pi := e.track(t, dur, sseq, 1)
	piPlain := e.track(t, plainDur, plainSseq, 2)

	// Exactly what the forced leave stages: the verdict is recorded, the transmitted
	// entry is left, the membership goes, and no shared config ever existed.
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	_, transmitted := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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

	e.sess.purgeSharedPending(mqttTestLicence(e.sess, nil))

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
	t.Parallel()
	e := testNewInflightEnv(t, "client-shared-purge")
	const group = "g"
	// The deterministic group name IS the fact the collection reads; nothing here is
	// populated by hand for it to find.
	_, _, dur := mqttSharedNames(group, "foo.bar", 1)
	_, _, retDur := mqttSharedNames(group, "retired.src", 1)
	_, _, defDur := mqttSharedNames(group, "deferred.src", 1)

	sseq := e.publishSourceGroup(t, dur, group, "foo.bar", "foo.bar")
	// QoS 1: supported shared delivery carries no QoS 2 (7a); the purge's ladder is QoS-blind.
	pi := e.track(t, dur, sseq, 1)
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	idHash := e.sess.idHash
	e.sess.mu.Unlock()

	// A retirement is created AFTER untrackPublishEntry removed the source's tracking, so
	// srcAckOwed is the only map naming its durable — once the emptied outer cpending
	// bucket untrackPublishEntry leaves behind is gone too.
	retSseq := e.publishSourceGroup(t, retDur, group, "retired.src", "retired.src")
	retPi := e.track(t, retDur, retSseq, 1)
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
	defPi, defIsNew := e.sess.trackPublish(defDur, testInflightAckSubject(defDur, defSseq, 1))
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

	e.sess.purgeSharedPending(mqttTestLicence(e.sess, nil))

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
	t.Parallel()
	e := testNewInflightEnv(t, "client-rung1-bounds")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	// The staging half: the transmitted register is voided in the membership write...
	_, transmitted := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	regs, _ := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	idHash := e.sess.idHash
	e.sess.mu.Unlock()
	if len(transmitted) != 1 || transmitted[0] != nonce {
		t.Fatalf("the staging must void the transmitted register, got %v", transmitted)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the staged verdict: %v", err)
	}

	// ...and the teardown's delete then SUCCEEDS, which is the proof that retires it.
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs, mqttTestLicence(e.sess, nil))
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
	t.Parallel()
	const clientID = "client-rejoin-nonce"
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()

	// The OLD membership's delivery: transmitted, then the leave voids it and the delete
	// fails, so the verdict is what survives.
	sseqOld := e.publishSource(t, dur)
	piOld := e.track(t, dur, sseqOld, 2)
	e.sess.mu.Lock()
	nonceOld := e.sess.pendingPublish[piOld].regNonce
	_, transmitted := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	regs, _ := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs, mqttTestLicence(e.sess, nil))
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
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
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
	t.Parallel()
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
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 4100, 1))
	cancels, _ := e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-abandon-proven")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 4200, 1))
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()

	// The teardown withdraws the allocation and SECURES it: the tombstone lands, which
	// retires the nonce.
	e.sess.mu.Lock()
	regs, cancels := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	e.sess.mu.Unlock()
	if len(regs) != 0 || len(cancels) != 1 || cancels[0].nonce != nonce {
		t.Fatalf("the purge must report the pending cancellation, got %v / %v", regs, cancels)
	}
	if !e.sess.secureRegCancellations(e.c, recordTx{}, cancels, mqttTestLicence(e.sess, nil)) {
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
	t.Parallel()
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
	e.sess.mu.Unlock()
	if !e.sess.persistVoidedRegs(e.c) {
		t.Fatalf("the verdict must be durable")
	}

	// First resume: the register is absent, and the verdict must SURVIVE the scan.
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	_, kept := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if !kept {
		t.Fatalf("scan-time absence is not proof; the verdict must survive the scan")
	}

	// The late write lands — as production writes it, the admission batch (F-105 7b)...
	e.landAdmission(t, pi, 2, dur, 4300, nonce)
	// ...and the next scan retires the verdict against the proof of its deletion.
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters (second): %v", err)
	}
	e.sess.mu.Lock()
	_, kept = e.sess.cancelledRegNonces[nonce]
	_, reserved := e.sess.pendingPublish[pi]
	// The late admission's exchange is custody too (F-105 row 5): the void must end it on this
	// same-object resume as it does across a restart.
	custodyHeld := e.sess.custody.holdsPI(pi) || e.sess.custody.withholdsAllocation()
	e.sess.mu.Unlock()
	if kept {
		t.Fatalf("a register seen and deleted must retire its verdict")
	}
	if custodyHeld {
		t.Fatalf("the voided admission's custody must end with the void: identifier %d is still held", pi)
	}
	if reserved {
		t.Fatalf("the late register was voided; identifier %d must not be reserved", pi)
	}
	e.waitRegisterCount(t, 0)

	// And the provable-absence path: a cancellation whose store never issued a CAS
	// retires its own verdict.
	e.sess.mu.Lock()
	pi2, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 4400, 1))
	nonce2 := e.sess.pendingPublish[pi2].regNonce
	e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi2, 1, dur, 4400, e.nonceOf(pi2), _EMPTY_, _EMPTY_) {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-stale-settle")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Allocation A, whose store is "stalled": trackPublish ran, storeInflightRegister has
	// not read the entry yet.
	e.sess.mu.Lock()
	piA, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5100, 1))
	nonceA := e.sess.pendingPublish[piA].regNonce
	// Its allocation is cancelled and the reservation settles and releases, freeing the
	// identifier...
	e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	if ack := e.sess.pendingPublish[piA]; ack == nil || !ack.regCancelled {
		e.sess.mu.Unlock()
		t.Fatalf("the cancellation must leave a reservation")
	}
	delete(e.sess.pendingPublish, piA) // A's release, proven elsewhere
	e.sess.last_pi = piA - 1           // ...and REUSED by allocation B under the same number
	piB, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5200, 1))
	nonceB := e.sess.pendingPublish[piB].regNonce
	// B is cancelled too, with ITS store still out: the state the stale settle meets.
	e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
	e.sess.mu.Unlock()
	if piB != piA {
		t.Fatalf("the test needs the identifier reused, got %d then %d", piA, piB)
	}
	if nonceB == nonceA {
		t.Fatalf("nonces must differ across allocations")
	}

	// A's stale store finally settles. It must not touch B's reservation or verdict.
	if e.sess.storeInflightRegister(e.c, piA, 1, dur, 5100, nonceA, _EMPTY_, _EMPTY_) {
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
		pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5300, 1))
		nonce := e.sess.pendingPublish[pi].regNonce
		e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
		pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5400, 1))
		nonce := e.sess.pendingPublish[pi].regNonce
		e.sess.voidRegistrationsLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
	t.Parallel()
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
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(idHash, gen, pi), 0, freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: pi, Dur: dur, Seq: 4500, QoS: 1, Nonce: nonce, Cancelled: true,
	})); err != nil {
		t.Fatalf("land the tombstone: %v", err)
	}

	// The restart's scan finds the register and retires the verdict against its deletion.
	restored, _, err := e.asm.createOrRestoreSession(clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if _, err := restored.recoverInflightRegisters(e.c); err != nil {
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

// Tearing a consumer down releases its in-memory SOURCE retirements too, not just its
// cpending entries, but preserves their whole-life owner slots.
//
// A retirement is created after untrackPublishEntry has removed the source's tracking, so
// it is not in cpending at all. A teardown that only walked cpending left the retirement
// behind while deleting the consumer that could have settled it. The register is a
// separate obligation: the consumer's disappearance does not prove PUBCOMP, so terminal
// cleanup—and only terminal cleanup—may remove that owner slot.
//
// Mutation check: drop the srcAckOwed loop from purgeConsumerPendingLocked and both
// assertions fail.
func TestMQTTInflightConsumerPurgeReleasesRetiredSources(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-purge-retired")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	regSeq, nonce := ack.regSeq, ack.regNonce
	e.sess.mu.Unlock()
	// durable:true below means the slot already carries the Retired source fact. Build
	// that production shape rather than labelling an unmarked PUBLISH record durable only
	// in memory; the latter made the assertion diagnose its own fixture.
	markedSeq, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, regSeq, nonce, _EMPTY_, _EMPTY_)
	if !ok {
		t.Fatal("could not persist the retired-source fixture")
	}
	regSeq = markedSeq
	e.sess.mu.Lock()
	// The state an unproven source acknowledgement leaves: no cpending entry, a
	// retirement instead.
	e.sess.untrackPublishEntry(pi)
	e.sess.retireSourceLocked(dur, sseq, &mqttRetiredSource{
		pi: pi, regSeq: regSeq, nonce: nonce, durable: true,
	})
	_, mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if mapped {
		t.Fatalf("a retired source is deliberately not in cpending; the test would prove nothing")
	}

	e.sess.mu.Lock()
	regs, _ := e.sess.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(e.sess, nil))
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
	if found {
		t.Fatalf("the retired owner slot %d must survive until PUBCOMP, got deletion batch %v", regSeq, regs)
	}
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs, mqttTestLicence(e.sess, nil))
	if rec, _ := e.loadRegister(t, pi); rec == nil || !rec.Retired {
		t.Fatalf("consumer purge removed or rewrote the past-PUBREC owner slot: %+v", rec)
	}
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
		rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
			PI: uint16(i + 1), Dur: dur, Seq: sseq, QoS: 2, Nonce: mqttNewInflightNonce(),
		})
		if _, err := e.asm.jsa.storeInflightCAS(
			mqttInflightSubject(e.sess.idHash, e.sess.gen, rec.PI), 0, rec); err != nil {
			t.Fatalf("seed register %d: %v", rec.PI, err)
		}
	}
	e.forgetInMemory()

	var enumerations, nextWalks int
	var filters []string
	e.asm.mu.Lock()
	e.asm.testInflightEnumerate = func(filter string) (map[string]uint64, error) {
		enumerations++
		filters = append(filters, filter)
		subjects, _, err := e.asm.jsa.streamSubjects(mqttInflightStreamName, filter)
		return subjects, err
	}
	e.asm.mu.Unlock()
	restore := setMQTTTestLoadNextMsgHook(func(stream, subject string, seq uint64) error {
		if stream == mqttInflightStreamName {
			nextWalks++
		}
		return nil
	})
	defer restore()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	// Exactly TWO bounded enumerations — the registers, then their unsent markers, in
	// that order (N-5.6) — and never a per-record walk.
	if enumerations != 2 || filters[0] != mqttInflightWalkFilter(e.sess.idHash, e.sess.gen) ||
		filters[1] != mqttInflightMarkerWalkFilter(e.sess.idHash, e.sess.gen) {
		t.Fatalf("expected the register enumeration then the marker enumeration, got %d: %q", enumerations, filters)
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
	t.Parallel()
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

// A consumer lookup that was ATTEMPTED but never completed leaves the source Unknown —
// never Complete, and never Lost either.
//
// The name and the message used to say "gone", which is the opposite of the invariant
// this fixture exercises: it sets `known` and leaves `resolved` false, which is the
// could-not-read case. An authoritative absence is Lost, and the sibling below pins that.
// A future reader taking the old name at face value would have concluded the cache treats
// a gone consumer as recoverable.
func TestMQTTInflightSourceCacheUnreadableConsumerIsUnknown(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-batch-consumer")
	cache := e.asm.newInflightSourceCache()
	cache.known["missing_durable"] = true // a lookup was attempted; it did not complete
	cache.resolve([]*mqttInflightRecord{{Dur: "missing_durable", Seq: 1}})
	if got := cache.state(&mqttInflightRecord{State: mqttInflightPublish, Dur: "missing_durable", Seq: 1}); got != mqttInflightSourceUnknown {
		t.Fatalf("a source whose consumer lookup did not complete must be unknown, never complete or lost, got %v", got)
	}
}

// The other half of that distinction: a lookup that COMPLETED and found nothing is an
// authoritative absence, which is Lost — knowably unrecoverable rather than uncertain.
// Pinning both directions is what stops the two being collapsed again.
func TestMQTTInflightSourceCacheAbsentConsumerIsLost(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-batch-consumer")
	cache := e.asm.newInflightSourceCache()
	cache.known["absent_durable"] = true
	cache.resolved["absent_durable"] = true // the lookup completed and reported not-found
	cache.resolve([]*mqttInflightRecord{{Dur: "absent_durable", Seq: 1}})
	if got := cache.state(&mqttInflightRecord{State: mqttInflightPublish, Dur: "absent_durable", Seq: 1}); got != mqttInflightSourceLost {
		t.Fatalf("a source whose consumer is authoritatively absent must be lost, got %v", got)
	}
}

// The two tests above pin the cache's ARMS. This pins the WIRING between the decoder and
// those arms, which is the part neither of them reaches.
//
// A holistic review found the gap: the decoder test proves the (info, settled) tuple, and
// the arm tests pre-seed resolved and prove the branch, but nothing proved the production
// mapping from one to the other. A regression that settled a successful nil ConsumerInfo
// could therefore select the terminal Lost verdict again while every one of those tests
// still passed. So this drives the real applyConsumerLookup with the exact tuples
// mqttClassifyConsumerInfoResponse produces, instead of reaching into the maps.
func TestMQTTInflightSourceCacheAppliesTheLookupContract(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-batch-wiring")

	for _, tc := range []struct {
		name string
		dur  string
		resp *JSApiConsumerInfoResponse
		want mqttInflightSourceState
		why  string
	}{
		{
			name: "authoritative not-found is Lost",
			dur:  "wiring_notfound",
			resp: &JSApiConsumerInfoResponse{ApiResponse: ApiResponse{Error: ApiErrors[JSConsumerNotFoundErr]}},
			want: mqttInflightSourceLost,
			why:  "the lookup completed and proved the consumer gone, which is terminal",
		},
		{
			name: "a success carrying no consumer is Unknown",
			dur:  "wiring_nilinfo",
			resp: &JSApiConsumerInfoResponse{},
			want: mqttInflightSourceUnknown,
			why: "the reply established nothing. Encoding it as an absence is what made the cache " +
				"conclude Lost - knowably unrecoverable - from a response that proved nothing",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cache := e.asm.newInflightSourceCache()
			cache.known[tc.dur] = true

			// The production decode, then the production application of its result.
			info, settled := mqttClassifyConsumerInfoResponse(tc.resp)
			cache.applyConsumerLookup(tc.dur, info, settled)

			cache.resolve([]*mqttInflightRecord{{Dur: tc.dur, Seq: 1}})
			if got := cache.state(&mqttInflightRecord{State: mqttInflightPublish, Dur: tc.dur, Seq: 1}); got != tc.want {
				t.Fatalf("state = %v, want %v: %s", got, tc.want, tc.why)
			}
		})
	}
}

// A stored exact PUBREL moves even a drain-owned PUBLISH slot through recovery route B.
// The drain's RegSeq is only a hint and terminal cleanup addresses the whole-life slot by
// identity, so preserving an obsolete PUBLISH phase would strand admission under the
// predecessor's custody after restart.
func TestMQTTInflightRecoveryCompletesDrainOwnedPubRelHandoff(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-drain-owned")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sid = "foo"

	sseq := e.publishSource(t, dur)
	// ADMITTED through the authority: an ordinary QoS 2 register stands beside its exchange,
	// and route B records the PUBREC the stored record proves on that exchange before it
	// rebinds the slot (consult 25). An exchange-less register is not a shape production writes.
	pi := e.trackAdmitted(t, dur, sseq, 2)
	// The register's OWN PUBLISH sequence, read from the store: an admission's PubAck names the
	// batch's last member, not the register, so the pending entry's hint is not this value.
	_, regMsg := e.loadRegister(t, pi)
	if regMsg == nil {
		t.Fatalf("the admitted register does not load")
	}
	regSeq := regMsg.Sequence

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
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, e.nonceOf(pi), dur, sseq, _EMPTY_, _EMPTY_, _EMPTY_)
	if _, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("store the PUBREL record: %v", err)
	}

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	// The slot is past PUBREC, canonical, and held for terminal cleanup rather than
	// released with the drain source.
	rec, smsg := e.loadRegister(t, pi)
	if rec == nil || smsg == nil {
		t.Fatalf("a drain-owned owner slot must not be deleted")
	}
	if smsg.Sequence == regSeq {
		t.Fatalf("route B did not move the slot from its PUBLISH sequence %d", regSeq)
	}
	if rec.State != mqttInflightPubRelReady || rec.PubRelStreamSeq == 0 {
		t.Fatalf("the drain-owned slot did not become canonical PUBREL authority: %+v", rec)
	}
	// The old hint may remain: it never selects or authorizes a release, and §10.5 skips
	// the drain-side slot release once intent proves this exchange is past PUBREC.
	e.sess.mu.Lock()
	got := e.sess.drains[sid].seqs[sseq].regSeq
	e.sess.mu.Unlock()
	if got != regSeq {
		t.Fatalf("the drain's non-authoritative register hint is %d, want original %d", got, regSeq)
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-no-stream")

	e.asm.mu.Lock()
	e.asm.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return nil, ApiErrors[JSStreamNotFoundErr]
	}
	e.asm.mu.Unlock()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-cancel-confirm")
	dur := e.sess.idHash + "_" + nuid.Next()

	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 1700, 1))
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

	if e.sess.storeInflightRegister(e.c, pi, 1, dur, 1700, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-cache")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)

	// Ten registers, ten DISTINCT sources, one durable.
	for i := 0; i < 10; i++ {
		rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
			PI: uint16(i + 1), Dur: dur, Seq: sseq + uint64(i), QoS: 1, Nonce: mqttNewInflightNonce(),
		})
		if _, err := e.asm.jsa.storeInflightCAS(
			mqttInflightSubject(e.sess.idHash, e.sess.gen, rec.PI), 0, rec); err != nil {
			t.Fatalf("seed register %d: %v", rec.PI, err)
		}
	}
	e.forgetInMemory()

	cache := e.asm.newInflightSourceCache()
	recs := make([]*mqttInflightRecord, 0, 10)
	for i := 0; i < 10; i++ {
		recs = append(recs, &mqttInflightRecord{State: mqttInflightPublish, Dur: dur, Seq: sseq + uint64(i)})
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
	if got := cache.state(&mqttInflightRecord{State: mqttInflightPublish, Dur: dur, Seq: sseq}); got != mqttInflightSourceLive {
		t.Fatalf("the stored source classified as %v, want live", got)
	}
	if got := cache.state(&mqttInflightRecord{State: mqttInflightPublish, Dur: dur, Seq: sseq + 5}); got != mqttInflightSourceComplete {
		t.Fatalf("an absent source with a live consumer classified as %v, want complete", got)
	}
	// An unknown durable is Unknown, never Complete.
	if got := cache.state(&mqttInflightRecord{State: mqttInflightPublish, Dur: "never_looked_up", Seq: 1}); got != mqttInflightSourceUnknown {
		t.Fatalf("an unresolved source classified as %v, want unknown", got)
	}
}

// ---------------------------------------------------------------------------
// Teardown and the orphan sweep
// ---------------------------------------------------------------------------

// A session discard destroys the registers of the incarnation it retires: every
// identifier it held is void.
//
// Named for the REAP rather than a purge since F-034: the removal is an
// enumerate-load-classify pass deleting at exact sequences, and this fixture asserts the
// outcome for the case where every register is readable and at or below the watermark.
// It does not assert that removal is unconditional — the reap spares an above-watermark
// record and refuses on an unreadable one, which the fixtures in
// inflight_identity_release_test.go cover.
func TestMQTTInflightTeardownReapsRegisters(t *testing.T) {
	t.Parallel()
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
		t.Fatalf("the retired incarnation's registers must be reaped")
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
	rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: pi, Dur: orphanHash + "_" + nuid.Next(), Seq: 3, QoS: 1, Nonce: mqttNewInflightNonce(),
	})
	// The RECLAMATION LICENCE (F-036 unit 2), planted BEFORE the register exists. A
	// register whose generation is absent from the reference walk is not thereby proven
	// dead: a session live on another broker whose record is absent still holds this
	// identifier and its Receive Maximum slot. So the sweep now requires a valid
	// retirement watermark naming the generation, and a fixture asserting reclamation
	// plants the fact that licenses it. The retention half is asserted separately, by
	// TestF036OrphanSweepRetainsRemoteLiveGenerationWithoutMatchingRetirement.
	//
	// The ORDER is load-bearing for the fixture, not just faithful to a real teardown
	// (which also records its retirement before leaving state behind): this account
	// manager runs its own periodic sweep goroutine, and these tests set the grace to
	// zero. Doing the watermark's round trips AFTER the register was stored put them
	// inside the window in which that background pass can take the register's FIRST
	// stable observation — which then makes the test's own first pass the second
	// sighting, and it deletes. That is what made TestMQTTInflightOrphanReconcile flake
	// on "the first sighting of an orphan must not delete it".
	e.retireClient(t, "client-departed", orphanGen)
	seq, err := e.asm.jsa.storeInflightCAS(subject, 0, rec)
	if err != nil {
		t.Fatalf("seed an orphaned register: %v", err)
	}
	return subject, seq
}

// seedOrphanRegisterWithoutLicence seeds an orphan register and deliberately plants NO
// retirement watermark — the state a session live on ANOTHER broker with no record
// leaves behind, which is what F-036 site 3 says absence does not distinguish from death.
func (e *testInflightEnv) seedOrphanRegisterWithoutLicence(t *testing.T, pi uint16) string {
	t.Helper()
	orphanHash, orphanGen := getHash("client-live-remote"), mqttNewSessGen()
	subject := mqttInflightSubject(orphanHash, orphanGen, pi)
	rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: pi, Dur: orphanHash + "_" + nuid.Next(), Seq: 3, QoS: 1, Nonce: mqttNewInflightNonce(),
	})
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, rec); err != nil {
		t.Fatalf("seed an unlicensed orphan register: %v", err)
	}
	return subject
}

// retireClient plants a valid retirement watermark for one client ID and generation.
func (e *testInflightEnv) retireClient(t *testing.T, clientID, gen string) {
	t.Helper()
	idHash := getHash(clientID)
	subject := mqttTeardownSubject(e.asm.domainTk, idHash)
	_, seq, status := e.asm.jsa.loadGenRetirement(e.asm.domainTk, idHash)
	var expect uint64
	if status != mqttRetirementAbsent {
		expect = seq
	}
	r := &mqttGenRetirement{
		ID: clientID, Gen: gen, MarkerSeq: 1, Nonce: nuid.Next(), Origin: "node1",
	}
	if _, err := e.asm.jsa.storeGenRetirementCAS(subject, expect, r); err != nil {
		t.Fatalf("planting the retirement watermark for %q: %v", clientID, err)
	}
	if e.asm.retirementProvenFor(idHash, gen) != mqttRetireProven {
		t.Fatalf("the planted retirement watermark does not prove generation %q retired", gen)
	}
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
	t.Parallel()
	e := testNewInflightEnv(t, "client-orphan")
	subject, _ := e.seedOrphanRegister(t, 12)
	zero := time.Duration(0)
	e.asm.mu.Lock()
	e.asm.testOrphanGrace = &zero
	e.asm.mu.Unlock()

	pass := func() {
		t.Helper()
		cands, _, err := e.asm.collectInflightCandidates(e.s)
		if err != nil {
			t.Fatalf("collectInflightCandidates: %v", err)
		}
		refs, _, err := e.asm.buildSessionRefs()
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
	t.Parallel()
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
	t.Parallel()
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

// TestMQTTInflightRecoveryQuarantinesIllegalPublishShapes is the read side of the ONE
// legal-shape matrix, and it exists because a holistic round found the read side checking
// the STATE alone.
//
// Every illegal `publish` shape the matrix names — a half-written action, a half-written
// PUBREL identity, half an affine pair, a QoS 1 record carrying QoS 2 fields — passed the
// identity check, satisfied `State == publish`, and went into source classification. There a
// partial affine pair becomes an ordinary exchange and loses its completion obligation, and
// a record whose phase fields disagree can license a payload retransmission from state the
// schema declares uninterpretable.
//
// The write boundary refuses all of these, so a record with one of these shapes came from
// somewhere else — which is exactly what the quarantine is for: the identifier is reserved
// and NOTHING is derived from the body. A quarantine is distinguishable from a legal
// restore by the source: a restored register names its `(durable, sequence)` and a
// quarantined one names nothing.
//
// TWO kinds of row, and the second kind is why this test survived the deletion of five
// fields. A row is either a typed mutation of a field the record still HAS, or an extra
// JSON key the record does not have at all. The five deleted tags moved to the second
// kind: their refusal no longer comes from a typed field declared in order to reject
// itself, it comes from the schema being CLOSED — which also covers the keys nobody
// predicted, and that is the property the five fields could not deliver.
//
// The legal control row is not decoration. Without it every assertion below would still
// pass against a recovery that quarantined the whole stream.
func TestMQTTInflightRecoveryQuarantinesIllegalPublishShapes(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-shapes")

	rows := []struct {
		name  string
		legal bool
		// mut mutates a field the record still has; extra injects a key it does not.
		// Exactly one of the two per row.
		mut   func(r *mqttInflightRecord)
		extra string
	}{
		{name: "the legal control", legal: true, mut: func(*mqttInflightRecord) {}},
		{name: "a claim sequence with no claimant", mut: func(r *mqttInflightRecord) {
			// The claim is CLEARED as well as the sequence set: custody is mandatory
			// now, so a base record carries one and setting only the sequence would be
			// an ordinary legal shape rather than the fault this row names.
			r.Claim, r.ClaimSeq = _EMPTY_, 9
		}},
		{name: "absent state", mut: func(r *mqttInflightRecord) { r.State = _EMPTY_ }},
		{name: "a deleted PUBREL state name", mut: func(r *mqttInflightRecord) {
			r.State = "pubrel_ready"
		}},
		{name: "half an affine reference", mut: func(r *mqttInflightRecord) {
			r.Q2AffClaim = "$MQTT.q2affclaim.h.1.c"
		}},
		{name: "the other half of an affine reference", mut: func(r *mqttInflightRecord) {
			r.Q2AffCopy = "$MQTT.q2aff.h.1.c.1"
		}},
		{name: "QoS 1 carrying an affine reference", mut: func(r *mqttInflightRecord) {
			r.QoS = 1
			r.Q2AffClaim, r.Q2AffCopy = "$MQTT.q2affclaim.h.1.c", "$MQTT.q2aff.h.1.c.1"
		}},
		{name: "QoS 1 marked retired", mut: func(r *mqttInflightRecord) {
			r.QoS = 1
			r.Retired = true
		}},
		{name: "a QoS that is neither 1 nor 2", mut: func(r *mqttInflightRecord) {
			r.QoS = 3
		}},
		// The five deleted tags, now unknown keys.
		{name: "the action token's tag", extra: `"action":"AAAAAAAAAAAAAAAAAAAAAA"`},
		{name: "the action claimant's tag", extra: `"action_claim":"someclaim"`},
		{name: "the action claim sequence's tag", extra: `"action_claimseq":4`},
		{name: "the canonical PUBREL durable's tag", extra: `"pubrel_dur":"$MQTT_out_dur"`},
		{name: "the canonical PUBREL sequence's tag", extra: `"pubrel_seq":12`},
		// And the rows the five-field approach could never have covered.
		{name: "a control field nobody predicted", extra: `"admission_epoch":4`},
		{name: "a plausible future phase field", extra: `"phase":"pubrel_ready"`},
	}

	// EVERY row gets its OWN live source, and that is not tidiness. Sharing one
	// `(durable, sequence)` across the rows made them duplicates of each other, and the
	// duplicate-source rule fails closed by deriving no source at all — so the "an illegal
	// shape derives NO source" assertion below would have been satisfied by the duplicate
	// rule rather than by the quarantine, and passed against a recovery with no matrix
	// check in it. The mutation that removes the check is what surfaced this.
	pis := make(map[string]uint16, len(rows))
	srcs := make(map[string]struct {
		dur  string
		sseq uint64
	}, len(rows))
	for i, row := range rows {
		pi := uint16(100 + i)
		dur := e.sess.idHash + "_" + nuid.Next()
		sseq := e.publishSource(t, dur)
		srcs[row.name] = struct {
			dur  string
			sseq uint64
		}{dur, sseq}
		if row.legal {
			// THE LEGAL CONTROL IS ADMITTED as production admits it (F-105 7b): a legal ordinary
			// register restores only beside the exchange its admission wrote, so a raw plant
			// would be refused as unadmitted and the control would prove nothing.
			pis[row.name] = e.track(t, dur, sseq, 2)
			continue
		}
		pis[row.name] = pi
		rec := freshRegisterLineage(&mqttInflightRecord{
			PI: pi, Dur: dur, Seq: sseq, Nonce: nuid.Next(),
			QoS: 2, State: mqttInflightPublish,
		})
		if row.mut != nil {
			row.mut(rec)
		}
		// Seeded with the RAW builder, which is deliberately not the write boundary: the
		// boundary refuses every illegal row, so there is no other way to plant one.
		msg, hdrLen := buildInflightRecord(0, rec)
		if row.extra != _EMPTY_ {
			// An unknown KEY cannot be expressed through the typed record at all, so the
			// body is edited after marshalling. The record it is injected into is the
			// legal one, which is what makes the row about the key alone.
			body := msg[hdrLen:]
			if body[len(body)-1] != '}' {
				t.Fatalf("%s: the marshalled body does not end in a brace: %q", row.name, body)
			}
			injected := append(append([]byte{}, body[:len(body)-1]...), ',')
			injected = append(injected, row.extra...)
			injected = append(injected, '}')
			msg = append(append([]byte{}, msg[:hdrLen]...), injected...)
		}
		// The row's premise, checked against the DECODER rather than assumed — and against
		// the decoder rather than the validator, because half these rows are now about a
		// key the validator never sees. A "legal" row the decoder refuses, or an illegal
		// one it accepts, would make the recovery assertion below prove the opposite of
		// what it claims.
		_, derr := mqttDecodeInflightRecord(msg[hdrLen:])
		if row.legal != (derr == nil) {
			t.Fatalf("%s: mqttDecodeInflightRecord = %v, want legal=%v", row.name, derr, row.legal)
		}
		subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
		if _, err := e.asm.jsa.storeMsg(subject, hdrLen, msg); err != nil {
			t.Fatalf("%s: seed the register: %v", row.name, err)
		}
	}
	e.forgetInMemory()

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	e.sess.mu.Lock()
	acks := make(map[uint16]*mqttPending, len(pis))
	for _, pi := range pis {
		if ack, ok := e.sess.pendingPublish[pi]; ok {
			acks[pi] = ack
		}
	}
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()

	for _, row := range rows {
		pi := pis[row.name]
		ack := acks[pi]
		if ack == nil {
			t.Errorf("%s: identifier %d was not reserved at all; the client may be holding it",
				row.name, pi)
			continue
		}
		if row.legal {
			if ack.jsDur != srcs[row.name].dur || ack.sseq != srcs[row.name].sseq {
				t.Errorf("%s: a LEGAL record must be restored with its source, got %q/%d",
					row.name, ack.jsDur, ack.sseq)
			}
			continue
		}
		if ack.jsDur != _EMPTY_ || ack.sseq != 0 {
			t.Errorf("%s: an illegal shape must derive NO source from its body, got %q/%d",
				row.name, ack.jsDur, ack.sseq)
		}
	}
	if !incomplete {
		t.Error("a quarantined register leaves the identifier space incomplete: the delivery " +
			"that owns the identifier cannot be identified, so no new one may be allocated")
	}
	// Every seeded record is KEPT. Deleting one would free an identifier that may be in
	// use, and the next resume would not even know to reserve it.
	e.waitRegisterCount(t, len(rows))
}

// TestMQTTInflightIllegalShapeWithholdsEveryDestructiveReader is the finding round 12
// actually raised, which was NOT that recovery was unvalidated — that was round 11 — but
// that validating recovery alone left every DESTRUCTIVE reader trusting the bytes.
//
// mqttSlotAdmitsDisposition checks identity and state; the loader checked identity. Between
// them an illegal but identity-valid `publish` record counted as readable AND deletable, so
// releaseInflightRegisterByIdentity could delete it and let drain finalization continue into
// irreversible claim, source and consumer cleanup — on a body the schema declares
// uninterpretable.
//
// Both readers are exercised per shape, and the assertion is the same on both: the record
// is RETAINED and progress is WITHHELD. Retained rather than deleted because the identifier
// may be one the client is holding; withheld rather than advanced because nothing about that
// exchange has been proven.
//
// The shapes cover BOTH ways a record can be unreadable, because a destructive reader must
// not care which: a known field in an illegal combination, and a key that is not in the
// schema at all. The second kind is what the five deleted fields used to cover for exactly
// five spellings.
func TestMQTTInflightIllegalShapeWithholdsEveryDestructiveReader(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-destructive")

	shapes := []struct {
		name  string
		mut   func(r *mqttInflightRecord)
		extra string
	}{
		{name: "half an affine reference", mut: func(r *mqttInflightRecord) {
			r.Q2AffCopy = "$MQTT.q2aff.h.1.c.1"
		}},
		{name: "a claim sequence with no claimant", mut: func(r *mqttInflightRecord) {
			// See the sibling row: custody is mandatory, so the claim is cleared too.
			r.Claim, r.ClaimSeq = _EMPTY_, 5
		}},
		{name: "a deleted PUBREL state name", mut: func(r *mqttInflightRecord) {
			r.State = "pubrel_sending"
		}},
		{name: "a deleted action tag", extra: `"action":"AAAAAAAAAAAAAAAAAAAAAA"`},
		{name: "a deleted canonical PUBREL tag", extra: `"pubrel_dur":"$MQTT_out_dur"`},
		{name: "an unrecognised control field", extra: `"admission_epoch":4`},
	}

	for i, sh := range shapes {
		pi := uint16(200 + i)
		dur := e.sess.idHash + "_" + nuid.Next()
		sseq := e.publishSource(t, dur)
		nonce := nuid.Next()
		rec := freshRegisterLineage(&mqttInflightRecord{
			PI: pi, Dur: dur, Seq: sseq, Nonce: nonce, QoS: 2, State: mqttInflightPublish,
		})
		if sh.mut != nil {
			sh.mut(rec)
		}
		subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
		msg, hdrLen := buildInflightRecord(0, rec)
		if sh.extra != _EMPTY_ {
			body := msg[hdrLen:]
			if body[len(body)-1] != '}' {
				t.Fatalf("%s: the marshalled body does not end in a brace: %q", sh.name, body)
			}
			injected := append(append([]byte{}, body[:len(body)-1]...), ',')
			injected = append(injected, sh.extra...)
			injected = append(injected, '}')
			msg = append(append([]byte{}, msg[:hdrLen]...), injected...)
		}
		if _, derr := mqttDecodeInflightRecord(msg[hdrLen:]); derr == nil {
			t.Fatalf("%s: the decoder accepts this shape, so the refusals below would prove nothing",
				sh.name)
		}
		resp, serr := e.asm.jsa.storeMsg(subject, hdrLen, msg)
		if serr != nil {
			t.Fatalf("%s: seed the register: %v", sh.name, serr)
		}

		// Reader one: the drain's identity release, naming the exchange EXACTLY. Every
		// addressing and matching precondition it has is satisfied — only the shape is
		// wrong — so a permissive reader would delete here.
		if got := e.asm.releaseInflightRegisterByIdentity(e.c, e.sess, subject, dur, sseq); got != mqttInflightUndecided {
			t.Errorf("%s: releaseInflightRegisterByIdentity = %v, want mqttInflightUndecided: an "+
				"illegal body may not authorize a delete or let the drain finalize", sh.name, got)
		}
		if _, smsg, lerr := e.asm.jsa.loadInflightRecord(subject); lerr != nil || smsg == nil {
			t.Fatalf("%s: the record must be RETAINED after the refused release (err=%v)", sh.name, lerr)
		}

		// Reader two: the generic gated reap, given a reference built from the record
		// itself — the most favourable input it can get.
		ref := mqttSlotRefFromRecord(subject, resp.Sequence, rec)
		if e.sess.reapRegisterWithVerdictGated(resp.Sequence, nonce, true, ref, mqttSlotDeleteComplete, mqttTestLicence(e.sess, nil)) {
			t.Errorf("%s: reapRegisterWithVerdictGated reported success; an illegal body is not an "+
				"absence and not a licence to delete", sh.name)
		}
		if _, smsg, lerr := e.asm.jsa.loadInflightRecord(subject); lerr != nil || smsg == nil {
			t.Fatalf("%s: the record must be RETAINED after the refused reap (err=%v)", sh.name, lerr)
		}
	}
}

// TestDuplicatePubRecReoffersTheStoredPubRel is round 20's finding, and it is a LIVENESS
// property rather than a safety one — which is why the earlier revision looked correct.
//
// A client retransmits PUBREC precisely because it did not receive the PUBREL
// ([MQTT-4.3.3]). The duplicate-classification branch correctly refuses to overwrite the
// live pendingPubRel entry with an empty placeholder — doing that would leave the handler
// with no source and no exchange identity, refuse the store, and terminate the connection
// on a legitimate duplicate. But it then RETURNED, answering nothing, so the client waited
// for JetStream's AckWait: a configurable interval that can be long, and at a Receive
// Maximum of 1 it stalls the session's entire outbound QoS 1/2 stream behind one exchange.
// Its comment claimed the retransmission was "answered by the exchange that already
// exists", which nothing performed.
//
// The answer is a NAK on the stored PUBREL's own delivery, so JetStream re-offers it at
// once and the ordinary delivery path retransmits it with its fencing intact.
func TestDuplicatePubRecReoffersTheStoredPubRel(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-dup-pubrec")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(3)
	const dur = "PUBREL_F089"
	relAck := testInflightAckSubject(dur, 55, 1)

	// Past PUBREC: the PUBLISH entry is gone and a live PUBREL entry holds the identifier,
	// carrying its own JetStream ack subject.
	e.sess.mu.Lock()
	e.sess.trackAsPubRel(pi, relAck)
	// The live PUBREL authority past a PUBREC is the successor of an ACTING PUBLISH entry; this
	// fixture has none behind it, so it states the kind that successor carries.
	e.sess.pendingPubRel[pi].kind = mqttPendingOrdinary
	rel := e.sess.pendingPubRel[pi]
	_, hasPub := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if rel == nil || rel.jsAckSubject != relAck {
		t.Fatalf("the fixture must leave a live PUBREL entry with its ack subject, got %+v", rel)
	}
	if hasPub {
		t.Fatal("the fixture must be PAST PUBREC: no PUBLISH entry may remain")
	}

	// Observe the record's own delivery subject: a NAK on it is what re-offers the PUBREL.
	obs, serr := e.s.nrc.SubscribeSync(relAck)
	if serr != nil {
		t.Fatalf("subscribe to the PUBREL's ack subject: %v", serr)
	}
	defer func() { _ = obs.Unsubscribe() }()
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The DUPLICATE PUBREC, through the real handler.
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess, proto: mqttProtoLevel5}
	if err := e.c.mqttProcessPublishReceived(pi, true, 0); err != nil {
		t.Fatalf("a duplicate PUBREC must not error: %v", err)
	}
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush after the duplicate: %v", err)
	}

	// ANSWERED: the stored PUBREL is re-offered at once.
	msg, aerr := obs.NextMsg(2 * time.Second)
	if aerr != nil {
		t.Fatalf("the duplicate PUBREC was not answered: the client retransmits because it did "+
			"not receive the PUBREL, so nothing here would reach it until AckWait expires (%v)", aerr)
	}
	if !bytes.Equal(msg.Data, AckNak) {
		t.Errorf("the answer must be a NAK, so the ordinary delivery path retransmits with its "+
			"fencing intact; got %q", msg.Data)
	}

	// And the live entry is UNTOUCHED: replacing it with a placeholder is the failure the
	// branch exists to prevent, and it must not have been traded for the liveness fix.
	e.sess.mu.Lock()
	after := e.sess.pendingPubRel[pi]
	_, pubBack := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if after != rel {
		t.Errorf("the duplicate must not replace the live PUBREL entry, got %+v", after)
	}
	if after == nil || after.jsAckSubject != relAck {
		t.Errorf("the entry must keep its own ack subject, got %+v", after)
	}
	if pubBack {
		t.Error("a duplicate PUBREC must not resurrect a PUBLISH entry")
	}
}

// TestDuplicatePubRecOnAPlaceholderSendsNoNak is the other half of that branch: the window
// between trackAsPubRel and the first delivery attempt, where the entry carries NO ack
// subject yet.
//
// There is nothing to re-offer — the original store is still in flight and will deliver —
// so the NAK's subject is empty and `mqttJSA.sendMsg` declines it at its own first line.
//
// This case is a PROPERTY guard rather than a mutation-checked clause, and the distinction
// is worth stating. An explicit branch here was written first, and a mutation removing it
// changed nothing observable, because sendMsg was already refusing the empty subject — two
// mechanisms stating one fact. The branch is gone; this test remains, because the property
// it asserts is about the placeholder window and holds whichever mechanism enforces it.
func TestDuplicatePubRecOnAPlaceholderSendsNoNak(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-dup-pubrec-placeholder")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(4)

	e.sess.mu.Lock()
	e.sess.trackAsPubRel(pi, _EMPTY_) // the placeholder: no ack subject yet
	rel := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if rel == nil || rel.jsAckSubject != _EMPTY_ {
		t.Fatalf("the fixture must be a placeholder with no ack subject, got %+v", rel)
	}

	// Anything published to an empty subject would be a protocol error on the wire, so the
	// observable property is that the connection stays usable and the entry is untouched.
	// A wildcard observer catches a NAK sent anywhere under $JS.ACK.
	obs, serr := e.s.nrc.SubscribeSync("$JS.ACK.>")
	if serr != nil {
		t.Fatalf("subscribe: %v", serr)
	}
	defer func() { _ = obs.Unsubscribe() }()
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess, proto: mqttProtoLevel5}
	if err := e.c.mqttProcessPublishReceived(pi, true, 0); err != nil {
		t.Fatalf("a duplicate PUBREC on a placeholder must not error: %v", err)
	}
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush after the duplicate: %v", err)
	}
	if msg, aerr := obs.NextMsg(250 * time.Millisecond); aerr == nil {
		t.Errorf("a placeholder has nothing to re-offer, so no NAK may be sent; got %q on %q",
			msg.Data, msg.Subject)
	}
	e.sess.mu.Lock()
	after := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if after != rel {
		t.Errorf("the placeholder must be left exactly as it was, got %+v", after)
	}
}

// TestMQTTStoreConflictReplacementRefusesAPastPubRecSlot covers census C02w. A
// same-source occupant is replaceable only while it is still a PUBLISH slot; once past
// PUBREC, overwriting it would erase the canonical record identity and reopen an answered
// payload as a fresh exchange.
func TestMQTTStoreConflictReplacementRefusesAPastPubRecSlot(t *testing.T) {
	e := testNewInflightEnv(t, "client-store-past-pubrec")
	dur := e.sess.idHash + "_STORE_PAST"
	const sourceSeq = uint64(8801)
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sourceSeq, 1))
	claim := e.sess.regClaim
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("allocating fixture identifier: pi=%d new=%v", pi, isNew)
	}
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	oldNonce := mqttNewInflightNonce()
	old := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPubRelReady, PI: pi, Dur: dur, Seq: sourceSeq, QoS: 2,
		Nonce: oldNonce, Claim: claim, ClaimSeq: 1,
		PubRelDurable: "PUBREL_STORE_PAST", PubRelStreamSeq: 91,
	})
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, old); err != nil {
		t.Fatalf("seeding past-PUBREC occupant: %v", err)
	}

	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sourceSeq, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatal("the new PUBLISH allocation replaced a past-PUBREC owner")
	}
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("loading preserved occupant: rec=%+v err=%v", cur, err)
	}
	if cur.Nonce != oldNonce || cur.State != mqttInflightPubRelReady || cur.PubRelStreamSeq != 91 {
		t.Fatalf("past-PUBREC occupant was rewritten: %+v", cur)
	}
}

// TestMQTTStoreConflictRetirementAdoptionRefusesAPubRelSlot covers census C56. A
// retirement on a PUBLISH register can suppress a re-offer, but a retired PUBREL slot is
// already the continuing owner and must not be reclassified into PUBLISH bookkeeping.
func TestMQTTStoreConflictRetirementAdoptionRefusesAPubRelSlot(t *testing.T) {
	e := testNewInflightEnv(t, "client-store-retired-pubrel")
	dur := e.sess.idHash + "_STORE_RETIRED"
	const sourceSeq = uint64(8802)
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sourceSeq, 1))
	claim := e.sess.regClaim
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("allocating fixture identifier: pi=%d new=%v", pi, isNew)
	}
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	oldNonce := mqttNewInflightNonce()
	old := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPubRelReady, PI: pi, Dur: dur, Seq: sourceSeq, QoS: 2,
		Nonce: oldNonce, Claim: claim, ClaimSeq: 1, Retired: true,
		PubRelDurable: "PUBREL_STORE_RETIRED", PubRelStreamSeq: 92,
	})
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, old); err != nil {
		t.Fatalf("seeding retired PUBREL occupant: %v", err)
	}

	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sourceSeq, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatal("the new PUBLISH allocation adopted a past-PUBREC retirement")
	}
	e.sess.mu.Lock()
	_, adopted := e.sess.srcAckOwed[mqttDeferredKey{dur, sourceSeq}]
	e.sess.mu.Unlock()
	if adopted {
		t.Fatal("a past-PUBREC slot was installed as PUBLISH-side retirement bookkeeping")
	}
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || cur.Nonce != oldNonce || cur.State != mqttInflightPubRelReady {
		t.Fatalf("retired PUBREL occupant was not preserved: rec=%+v err=%v", cur, err)
	}
}

// TestMQTTConfirmDropsOnlyItsOwnUnconfirmedAllocation covers the map edge C55m: a
// delayed confirmation may settle its durable register, but it must not remove a newer
// cancellation reservation that has reused the same identifier.
func TestMQTTConfirmDropsOnlyItsOwnUnconfirmedAllocation(t *testing.T) {
	e := testNewInflightEnv(t, "client-confirm-map-identity")
	dur := e.sess.idHash + "_CONFIRM_MAP"
	const sourceSeq = uint64(8803)
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sourceSeq, 1))
	oldNonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()
	newNonce := mqttNewInflightNonce()
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expect uint64, rec *mqttInflightRecord) (uint64, error) {
		seq, err := e.asm.jsa.storeInflightCAS(subject, expect, rec)
		if err == nil {
			e.sess.mu.Lock()
			e.sess.pendingPublish[pi] = &mqttPending{
				regNonce: newNonce, regPending: true, regCancelled: true,
			}
			e.sess.mu.Unlock()
		}
		return seq, err
	}
	e.asm.mu.Unlock()
	defer func() { e.asm.mu.Lock(); e.asm.testInflightStore = nil; e.asm.mu.Unlock() }()

	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sourceSeq, oldNonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the cancelled allocation was released for transmission")
	}
	e.sess.mu.Lock()
	cur := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if cur == nil || cur.regNonce != newNonce {
		t.Fatalf("the old confirmation removed the newer reservation: %+v", cur)
	}
}

// TestMQTTConfirmedDeleteIsIdentityAddressed covers C55's durable edge. Between a
// landed store and its confirmation the subject can move to another exchange; cleanup
// must preserve that occupant rather than deleting the remembered absolute sequence.
func TestMQTTConfirmedDeleteIsIdentityAddressed(t *testing.T) {
	e := testNewInflightEnv(t, "client-confirm-durable-identity")
	dur := e.sess.idHash + "_CONFIRM_DURABLE"
	const sourceSeq = uint64(8804)
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, sourceSeq, 1))
	oldNonce := e.sess.pendingPublish[pi].regNonce
	e.sess.mu.Unlock()
	foreignNonce := mqttNewInflightNonce()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(gotSubject string, expect uint64, rec *mqttInflightRecord) (uint64, error) {
		seq, err := e.asm.jsa.storeInflightCAS(gotSubject, expect, rec)
		if err != nil {
			return seq, err
		}
		foreign := *rec
		foreign.Nonce = foreignNonce
		foreign.Seq++
		testRegisterSourceReply(&foreign)
		if _, err := e.asm.jsa.storeInflightCAS(gotSubject, seq, &foreign); err != nil {
			return 0, err
		}
		e.sess.mu.Lock()
		e.sess.untrackPublishEntry(pi)
		e.sess.mu.Unlock()
		return seq, nil
	}
	e.asm.mu.Unlock()
	defer func() { e.asm.mu.Lock(); e.asm.testInflightStore = nil; e.asm.mu.Unlock() }()

	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sourceSeq, oldNonce, _EMPTY_, _EMPTY_) {
		t.Fatal("an allocation removed before confirmation was released for transmission")
	}
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || cur.Nonce != foreignNonce {
		t.Fatalf("cleanup destroyed the replacement occupant: rec=%+v err=%v", cur, err)
	}
}

// An admitted same-source replacement PRESERVES the occupant's QoS rather than
// overwriting it with the replacing writer's.
//
// The window is cross-broker and needs no crash: A writes a QoS 2 register for a
// delivery and transmits it; B's recovery scan missed that record, so B allocates the
// same identifier for the same source, and its store meets the occupant. B's own
// subscription may since have been re-granted at QoS 1 -- and taking B's value would
// record QoS 1 for an exchange the client is holding at QoS 2, so every later recovery
// on any broker retransmits it at the wrong QoS and its handshake is abandoned.
//
// B cannot prove the occupant was never transmitted: Cancelled is not exhaustive
// evidence, since a cancellation may live only as a nonce in the session record. So a
// live PUBLISH-phase occupant for this exact delivery is evidence about what the client
// may already hold, and its QoS is the only durable trace of it.
//
// Paired mutation: take the caller's QoS in the replacement arm instead of cur.QoS.
// Both assertions below fail under it -- the record and the thread-back -- and they are
// asserted against the value the mutation produces (1), never against a default.
func TestMQTTInflightSameSourceReplacementPreservesOccupantQoS(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-replace-qos")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sseq = uint64(4100)

	// A: the occupant, at QoS 2, for this exact delivery.
	pi := e.track(t, dur, sseq, 2)
	recA, smsgA := e.loadRegister(t, pi)
	if recA == nil || recA.QoS != 2 {
		t.Fatalf("the occupant must be a QoS 2 register, got %+v", recA)
	}

	// B: a distinct session OBJECT over the same JetStream, which is the cross-broker
	// boundary. Its recovery did not adopt A's register, so it allocates freshly.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist A's session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", "client-replace-qos")
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist B's accept record: %v", err)
	}
	sessB.maxp = 100

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
		t.Fatalf("B must allocate freshly for this fixture to reach the replacement arm")
	}

	// B stores at QoS 1 -- the value its own subscription would now compute. The
	// replacement arm must NOT take it.
	testOpenStoreCustody(t, sessB, piB)
	if !sessB.storeInflightRegister(cB, piB, 1, dur, sseq, nonceB, _EMPTY_, _EMPTY_) {
		t.Fatalf("B's store was refused; the replacement arm was never reached")
	}

	subject := mqttInflightSubject(sessB.idHash, sessB.gen, piB)
	recB, smsgB, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil {
		t.Fatalf("load the replacement register: %v", err)
	}
	if recB == nil {
		t.Fatalf("the replacement register is absent")
	}
	// It really is B's record, so the assertion below is about a replacement and not
	// about A's record having merely survived.
	if recB.Nonce != nonceB {
		t.Fatalf("the register must carry B's nonce after the replacement, got %q want %q", recB.Nonce, nonceB)
	}
	if smsgB.Sequence == smsgA.Sequence {
		t.Fatalf("the replacement must be a new stream sequence, got %d twice", smsgB.Sequence)
	}
	// THE assertion: the occupant's QoS, not the caller's.
	if recB.QoS != 2 {
		t.Fatalf("the replacement must preserve the occupant's QoS 2, got %d", recB.QoS)
	}

	// And the thread-back: B's own pending entry must carry what LANDED, or B would
	// transmit at one QoS while the register said another.
	sessB.mu.Lock()
	ackB := sessB.pendingPublish[piB]
	sessB.mu.Unlock()
	if ackB == nil {
		t.Fatalf("B's pending entry is gone")
	}
	if ackB.regPending {
		t.Fatalf("B's entry must be confirmed after a successful store")
	}
	if ackB.qos != 2 {
		t.Fatalf("B's entry must carry the QoS the record landed holding (2), got %d", ackB.qos)
	}
}

// The lost-response reconciliation arm must thread back the QoS of the record that
// ACTUALLY EXISTS, not zero and not the caller's.
//
// This is the arm reached when our own write landed but its response was lost: the
// reread matches our nonce, and confirmation adopts the landed sequence. It is a
// SEPARATE entry point from the successful CAS, and threading the QoS on only one of
// them leaves the other adopting a record whose QoS never reaches memory — the pending
// carrier stays zero and the delivery is then withheld for the life of the session.
//
// The schedule is what makes this discriminating, and it is forced rather than raced:
// the store hook lets the REPLACEMENT CAS land and only then reports a lost response,
// so the retry rereads into the same-nonce arm. Losing the initial conflicting create's
// response instead would not reach this branch at all.
//
// Paired mutation: pass 0 (or omit the thread-back) on the reconciliation arm. Passing
// rec.QoS there is NOT discriminating — the replacement arm has already set rec.QoS from
// the occupant, so both arguments read 2 — which is why that variant is recorded as
// non-discriminating rather than paired with this fixture.
func TestMQTTInflightReplacementLostResponseThreadsLandedQoS(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-replace-lost")
	dur := e.sess.idHash + "_" + nuid.Next()
	const sseq = uint64(4200)

	pi := e.track(t, dur, sseq, 2)
	if rec, _ := e.loadRegister(t, pi); rec == nil || rec.QoS != 2 {
		t.Fatalf("the occupant must be a QoS 2 register, got %+v", rec)
	}

	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist A's session: %v", err)
	}
	asmB, sessB, cB := e.peer(t, "node2", "client-replace-lost")
	sessB.maxp = 100
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist B's accept record: %v", err)
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

	// Let the REPLACEMENT land, then report its response lost. expectSeq != 0 is what
	// identifies the replacement attempt: the first, create-only attempt uses zero and
	// is the one that must be allowed to fail normally so the arm is reached.
	var replaced int
	asmB.mu.Lock()
	asmB.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if expectSeq == 0 {
			return asmB.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		replaced++
		if _, err := asmB.jsa.storeInflightCAS(subject, expectSeq, r); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("response lost")
	}
	asmB.mu.Unlock()

	testOpenStoreCustody(t, sessB, piB)
	if !sessB.storeInflightRegister(cB, piB, 1, dur, sseq, nonceB, _EMPTY_, _EMPTY_) {
		t.Fatalf("B's store was refused; the reconciliation arm was never reached")
	}
	asmB.mu.Lock()
	asmB.testInflightStore = nil
	asmB.mu.Unlock()
	if replaced != 1 {
		t.Fatalf("the replacement attempt must have run exactly once, got %d", replaced)
	}

	subject := mqttInflightSubject(sessB.idHash, sessB.gen, piB)
	recB, _, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || recB == nil {
		t.Fatalf("load the replacement register: rec=%v err=%v", recB, err)
	}
	if recB.Nonce != nonceB {
		t.Fatalf("the landed record must carry B's nonce, got %q", recB.Nonce)
	}
	if recB.QoS != 2 {
		t.Fatalf("the landed record must preserve the occupant's QoS 2, got %d", recB.QoS)
	}

	// THE assertion: the entry adopted the landed QoS through the RECONCILIATION arm.
	// Zero here means the carrier was never fed and every later offer withholds.
	sessB.mu.Lock()
	ackB := sessB.pendingPublish[piB]
	sessB.mu.Unlock()
	if ackB == nil {
		t.Fatalf("B's pending entry is gone")
	}
	if ackB.regPending {
		t.Fatalf("the reconciliation must confirm the entry, got regPending")
	}
	if ackB.qos != 2 {
		t.Fatalf("the reconciliation arm must thread back the landed QoS 2, got %d", ackB.qos)
	}
}
