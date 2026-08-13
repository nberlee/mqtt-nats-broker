package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestSessGenNamingIsScoped pins the property every generation-scoped name must
// have: a generation makes the name distinct from every other incarnation's, so a
// delete issued by a retired incarnation can only ever name a resource that no
// longer exists.
//
// There is no legacy counterpart to be compatible with. An absent generation is
// rejected at the record boundary (mqttSessGenFromRecord), so no live session can
// carry one, and no name here is ever built without one.
func TestSessGenNamingIsScoped(t *testing.T) {
	t.Parallel()
	const idHash = "abcdefghijklmnop"
	g1, g2 := mqttNewSessGen(), mqttNewSessGen()
	if g1 == g2 {
		t.Fatal("two minted generations collided")
	}
	for _, g := range []string{g1, g2} {
		if !mqttValidSessGen(g) {
			t.Fatalf("mqttNewSessGen produced %q, which mqttValidSessGen rejects", g)
		}
	}
	for _, bad := range []string{
		_EMPTY_,
		strings.Repeat("a", mqttSessGenLen-1),
		strings.Repeat("a", mqttSessGenLen+1),
		strings.Repeat(".", mqttSessGenLen), // right length, out of alphabet
		strings.Repeat("-", mqttSessGenLen),
	} {
		if mqttValidSessGen(bad) {
			t.Fatalf("mqttValidSessGen accepted %q", bad)
		}
	}

	s1, d1, du1 := mqttPubRelNames(idHash, g1)
	s2, d2, du2 := mqttPubRelNames(idHash, g2)
	for _, p := range []struct {
		what, a, b, unscoped string
	}{
		{"pubrel subject", s1, s2, mqttPubRelSubjectPrefix + idHash},
		{"pubrel delivery subject", d1, d2, mqttPubRelDeliverySubjectPrefix + idHash},
		{"pubrel durable", du1, du2, mqttPubRelConsumerDurablePrefix + idHash},
	} {
		if p.a == p.b {
			t.Fatalf("%s is shared across incarnations: %q", p.what, p.a)
		}
		if p.a == p.unscoped || p.b == p.unscoped {
			t.Fatalf("%s collides with the unscoped hash-only name %q", p.what, p.unscoped)
		}
		if !strings.HasPrefix(p.a, p.unscoped) {
			t.Fatalf("%s %q must extend the hash-only name %q so the reserved-namespace prefix checks still match",
				p.what, p.a, p.unscoped)
		}
	}

	// A hold subject and its release claim must both be scoped by generation, and
	// the discard's enumeration filter must reach one incarnation's holds and no
	// other's — it is the only thing that touches $MQTT_qos2in on a discard now, so
	// a filter that widened would destroy another incarnation's session state.
	if a, b := mqttQoS2HoldSubject(idHash, g1, 7), mqttQoS2HoldSubject(idHash, g2, 7); a == b {
		t.Fatalf("hold subject is shared across incarnations: %q", a)
	}
	if a, b := mqttQoS2ClaimSubject(idHash, g1, 7), mqttQoS2ClaimSubject(idHash, g2, 7); a == b {
		t.Fatalf("release claim subject is shared across incarnations: %q", a)
	}
	own := mqttQoS2HoldWalkFilter(idHash, g1)
	if !testSubjectMatchesFilter(mqttQoS2HoldSubject(idHash, g1, 7), own) {
		t.Fatalf("%q must match its own incarnation's hold subject", own)
	}
	if testSubjectMatchesFilter(mqttQoS2HoldSubject(idHash, g2, 7), own) {
		t.Fatalf("%q must NOT be able to reach another incarnation's hold subject", own)
	}
	if testSubjectMatchesFilter(mqttQoS2ClaimSubject(idHash, g1, 7), own) {
		t.Fatalf("%q must NOT match a release claim; claims are not walked as holds", own)
	}
}

// testSubjectMatchesFilter applies NATS subject-filter semantics ("*" matches
// exactly one token, ">" matches one or more trailing tokens). The server's own
// matcher lives in a package this one cannot import, and the purge filters here
// only ever use those two forms.
func testSubjectMatchesFilter(subject, filter string) bool {
	st, ft := strings.Split(subject, "."), strings.Split(filter, ".")
	for i, tok := range ft {
		if tok == ">" {
			return i < len(st) // ">" needs at least one token to consume
		}
		if i >= len(st) {
			return false
		}
		if tok != "*" && tok != st[i] {
			return false
		}
	}
	return len(st) == len(ft)
}

// TestSessGenContinuity is test 0f: a persistent session keeps its incarnation
// generation across a resume — so it still finds its own PUBRELs and QoS 2 holds —
// and gets a new one only when it is discarded. A record carrying no generation is
// refused outright.
func TestSessGenContinuity(t *testing.T) {
	t.Parallel()
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

	c := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	opts := s.getOpts()

	const clientID = "continuity"
	sess, existed, err := asm.createOrRestoreSession(clientID, opts)
	if err != nil || existed {
		t.Fatalf("createOrRestoreSession: existed=%v err=%v", existed, err)
	}
	gen := sess.gen
	if !mqttValidSessGen(gen) {
		t.Fatalf("a new session must mint a generation, got %q", gen)
	}
	wantSubj, wantDeliver, wantDur := mqttPubRelNames(sess.idHash, gen)
	if sess.pubRelSubject != wantSubj || sess.pubRelDeliverySubject != wantDeliver ||
		string(sess.pubRelDeliverySubjectB) != wantDeliver {
		t.Fatalf("session PUBREL names are not derived from its generation: %q/%q",
			sess.pubRelSubject, sess.pubRelDeliverySubject)
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save: %v", err)
	}

	// A resume must PRESERVE the generation, or the session would look for its
	// PUBRELs and QoS 2 holds under names nothing ever wrote.
	asm.removeSession(sess, true)
	resumed, existed, err := asm.createOrRestoreSession(clientID, opts)
	if err != nil || !existed {
		t.Fatalf("createOrRestoreSession (resume): existed=%v err=%v", existed, err)
	}
	if resumed.gen != gen {
		t.Fatalf("resume re-minted the generation: %q -> %q", gen, resumed.gen)
	}
	if resumed.pubRelSubject != wantSubj || resumed.pubRelDeliverySubject != wantDeliver {
		t.Fatalf("resumed PUBREL names drifted: %q/%q", resumed.pubRelSubject, resumed.pubRelDeliverySubject)
	}
	if _, _, dur := mqttPubRelNames(resumed.idHash, resumed.gen); dur != wantDur {
		t.Fatalf("resumed PUBREL durable drifted: %q -> %q", wantDur, dur)
	}

	// A discard re-mints, and everything derived from the generation moves with it.
	if err := resumed.clear(false); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if resumed.gen == gen {
		t.Fatal("clear() must re-mint the incarnation generation")
	}
	if !mqttValidSessGen(resumed.gen) {
		t.Fatalf("clear() minted an invalid generation %q", resumed.gen)
	}
	if resumed.pubRelSubject == wantSubj || resumed.pubRelDeliverySubject == wantDeliver ||
		string(resumed.pubRelDeliverySubjectB) == wantDeliver {
		t.Fatal("clear() re-minted the generation but left the derived PUBREL names on the retired one")
	}

	// A record written before generations existed is REJECTED rather than resumed on
	// hash-only names. Such a session has no identity to scope an arbitration with,
	// so its inbound QoS 2 release could not be arbitrated against a discard and the
	// discard would fall back to a blind hash-wide purge — destroying a held payload
	// whose releaser claimed it and then died. Refusing the record is the alternative
	// to that silent loss; the bridge is pre-release, so the store is recreated.
	const noGenID = "no-generation"
	ps := mqttPersistedSession{ID: noGenID, Subs: map[string]byte{"foo": 1}}
	b, err := json.Marshal(&ps)
	if err != nil {
		t.Fatalf("marshal generation-less record: %v", err)
	}
	if bytes := string(b); strings.Contains(bytes, `"gen"`) {
		t.Fatalf("an empty generation must be omitted from the record, got %s", bytes)
	}
	if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(noGenID), 0, b); err != nil {
		t.Fatalf("store generation-less session record: %v", err)
	}
	if _, _, err := asm.createOrRestoreSession(noGenID, opts); err == nil {
		t.Fatal("a session record with no incarnation generation must fail closed, not resume on hash-only names")
	}
}

// TestSessGenFromRecordFailsClosed pins that a stored incarnation generation is
// validated before any name or purge filter is built from it.
//
// The record is not a trusted input — a future broker or a hand edit can put
// anything there — and everything downstream is built by
// concatenation. A value carrying a wildcard or a subject separator would either
// produce an unusable durable name or, far worse, WIDEN the QoS 2 hold
// enumeration filter beyond this incarnation, reaching records the session has no
// claim on.
//
// EMPTY fails closed too, and that is deliberate rather than incidental: it is the
// shape of a record written before generations existed, and such a session has no
// identity to scope an arbitration with — so its inbound QoS 2 release could not be
// arbitrated against a discard, and the discard would fall back to a blind
// hash-wide purge that destroys a held payload whose releaser claimed it and then
// died. Rejecting the record is the alternative to that silent loss.
func TestSessGenFromRecordFailsClosed(t *testing.T) {
	t.Parallel()
	good := mqttNewSessGen()
	if got, err := mqttSessGenFromRecord(good); err != nil || got != good {
		t.Fatalf("a well-formed generation must be accepted, got %q err=%v", got, err)
	}
	for _, bad := range []string{
		_EMPTY_,
		"*",
		">",
		"a.b",
		good + ".>",
		good[:len(good)-1] + ".",
		good[:len(good)-1],
		good + "x",
		"!!!!!!!!!!!!!!!!!!!!!!",
	} {
		got, err := mqttSessGenFromRecord(bad)
		if err == nil {
			t.Errorf("generation %q was accepted; a malformed one must fail closed", bad)
		}
		if got != _EMPTY_ {
			t.Errorf("a rejected generation must not be coerced to a usable value, got %q", got)
		}
	}

	// The specific hazard, stated as the property it protects: an accepted
	// generation's enumeration filter must never reach outside this client's
	// namespace, nor past this one incarnation.
	f := mqttQoS2HoldWalkFilter("HASH", good)
	if !strings.HasPrefix(f, mqttQoS2IncomingMsgsStreamSubjectPrefix+"HASH.") {
		t.Fatalf("walk filter %q escapes the client's namespace", f)
	}
	if testSubjectMatchesFilter(mqttQoS2HoldSubject("HASH", mqttNewSessGen(), 1), f) {
		t.Fatalf("walk filter %q reaches another incarnation's hold subject", f)
	}
}

// TestClearResetsPerConnectionDeliveryFlags pins that clear() leaves no flag
// claiming a subscription to a subject it has just re-minted.
//
// clear() re-mints the incarnation, which changes the PUBREL subject, its delivery
// subject, and the QoS 2 hold subjects. A "already subscribed" flag surviving that
// refers to a subscription on the RETIRED delivery subject: the next
// ensurePubRelConsumerSubscription would skip processSub and then create a consumer
// delivering where nothing listens, and because $MQTT_out is interest-retained
// every PUBREL stored afterwards is silently dropped — the client can never
// complete a QoS 2 exchange for the life of that session.
//
// The CONNECT reuse path clears these again after calling clear(), so this is a
// robustness invariant rather than a live bug through that path. It is asserted
// here because clear() is what breaks it, is called from five places, and hands its
// session object back to be REUSED as a fresh session on two of them.
func TestClearResetsPerConnectionDeliveryFlags(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	s := &Server{nrc: nc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: "node1"}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	defer close(s.quitCh)

	c := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	sess, _, err := asm.createOrRestoreSession("clearflags", s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}

	// The state a connection that used both delivery paths leaves behind.
	sess.mu.Lock()
	genBefore := sess.gen
	pubRelDeliverBefore := sess.pubRelDeliverySubject
	sess.pubRelSubscribed = true
	sess.pubRelVerified = true
	sess.q2AffSubscribed = true
	sess.q2AffReady = true
	sess.mu.Unlock()

	if err := sess.clear(false); err != nil {
		t.Fatalf("sess.clear: %v", err)
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.gen == genBefore {
		t.Fatal("clear() must re-mint the incarnation")
	}
	if sess.pubRelDeliverySubject == pubRelDeliverBefore {
		t.Fatal("re-minting the incarnation must change the PUBREL delivery subject")
	}
	if sess.pubRelSubscribed {
		t.Fatalf("pubRelSubscribed survived clear() while the delivery subject changed from %q to %q: "+
			"a reused session skips subscribing and its PUBRELs are delivered nowhere",
			pubRelDeliverBefore, sess.pubRelDeliverySubject)
	}
	if sess.pubRelVerified {
		t.Fatal("pubRelVerified survived clear(): the retired consumer is not evidence for the new incarnation")
	}
	if sess.q2AffSubscribed {
		t.Fatal("q2AffSubscribed survived clear()")
	}
	if sess.q2AffReady {
		t.Fatal("q2AffReady survived clear()")
	}
}
