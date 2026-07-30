package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestSessGenNamingIsScopedAndLegacyCompatible pins the two properties every
// generation-scoped name must have: a generation makes the name distinct from
// every other incarnation's, and no generation reproduces the pre-upgrade name
// byte for byte (a legacy record must keep working, and an older broker's names
// must stay reachable).
func TestSessGenNamingIsScopedAndLegacyCompatible(t *testing.T) {
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

	legacySubj, legacyDeliver, legacyDur := mqttPubRelNames(idHash, _EMPTY_)
	if legacySubj != mqttPubRelSubjectPrefix+idHash ||
		legacyDeliver != mqttPubRelDeliverySubjectPrefix+idHash ||
		legacyDur != mqttPubRelConsumerDurablePrefix+idHash {
		t.Fatalf("an empty generation must reproduce the legacy names exactly, got %q/%q/%q",
			legacySubj, legacyDeliver, legacyDur)
	}

	s1, d1, du1 := mqttPubRelNames(idHash, g1)
	s2, d2, du2 := mqttPubRelNames(idHash, g2)
	for _, p := range []struct {
		what, a, b, legacy string
	}{
		{"pubrel subject", s1, s2, legacySubj},
		{"pubrel delivery subject", d1, d2, legacyDeliver},
		{"pubrel durable", du1, du2, legacyDur},
	} {
		if p.a == p.b {
			t.Fatalf("%s is shared across incarnations: %q", p.what, p.a)
		}
		if p.a == p.legacy || p.b == p.legacy {
			t.Fatalf("%s collides with the legacy name %q", p.what, p.legacy)
		}
		if !strings.HasPrefix(p.a, p.legacy) {
			t.Fatalf("%s %q must extend the legacy name %q so the reserved-namespace prefix checks still match",
				p.what, p.a, p.legacy)
		}
	}

	// A hold subject must be scoped by generation, and the legacy purge filter must
	// be unable to reach a generation-scoped one (that is the whole point: exactly
	// one token past the hash).
	if a, b := mqttQoS2HoldSubject(idHash, g1, 7), mqttQoS2HoldSubject(idHash, g2, 7); a == b {
		t.Fatalf("hold subject is shared across incarnations: %q", a)
	}
	genFilters := mqttQoS2HoldPurgeFilters(idHash, g1)
	if len(genFilters) != 2 {
		t.Fatalf("a generation-scoped purge needs its own range plus the legacy single-token range, got %v", genFilters)
	}
	legacyRange, ownRange := genFilters[0], genFilters[1]
	if !testSubjectMatchesFilter(mqttQoS2HoldSubject(idHash, _EMPTY_, 7), legacyRange) {
		t.Fatalf("%q must match the orphaned legacy hold subject", legacyRange)
	}
	if testSubjectMatchesFilter(mqttQoS2HoldSubject(idHash, g2, 7), legacyRange) {
		t.Fatalf("%q must NOT be able to reach another incarnation's hold subject", legacyRange)
	}
	if !testSubjectMatchesFilter(mqttQoS2HoldSubject(idHash, g1, 7), ownRange) {
		t.Fatalf("%q must match its own incarnation's hold subject", ownRange)
	}
	if testSubjectMatchesFilter(mqttQoS2HoldSubject(idHash, g2, 7), ownRange) {
		t.Fatalf("%q must NOT be able to reach another incarnation's hold subject", ownRange)
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
// and gets a new one only when it is discarded. A legacy record with no generation
// keeps working on the old names.
func TestSessGenContinuity(t *testing.T) {
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

	// A legacy record (written before generations existed) keeps working, on the
	// legacy names, until it is discarded.
	const legacyID = "legacy"
	legacyHash := getHash(legacyID)
	ps := mqttPersistedSession{ID: legacyID, Subs: map[string]byte{"foo": 1}}
	b, err := json.Marshal(&ps)
	if err != nil {
		t.Fatalf("marshal legacy record: %v", err)
	}
	if bytes := string(b); strings.Contains(bytes, `"gen"`) {
		t.Fatalf("an empty generation must be omitted from the record, got %s", bytes)
	}
	if _, err := asm.jsa.storeSessionMsg(asm.domainTk, legacyHash, 0, b); err != nil {
		t.Fatalf("store legacy session record: %v", err)
	}
	legacy, existed, err := asm.createOrRestoreSession(legacyID, opts)
	if err != nil || !existed {
		t.Fatalf("createOrRestoreSession (legacy): existed=%v err=%v", existed, err)
	}
	if legacy.gen != _EMPTY_ {
		t.Fatalf("a legacy record must not be given a generation on resume, got %q", legacy.gen)
	}
	if legacy.pubRelSubject != mqttPubRelSubjectPrefix+legacyHash ||
		legacy.pubRelDeliverySubject != mqttPubRelDeliverySubjectPrefix+legacyHash {
		t.Fatalf("a legacy session must keep the legacy PUBREL names, got %q/%q",
			legacy.pubRelSubject, legacy.pubRelDeliverySubject)
	}
	if err := legacy.clear(false); err != nil {
		t.Fatalf("clear legacy: %v", err)
	}
	if !mqttValidSessGen(legacy.gen) {
		t.Fatalf("a discarded legacy session must be re-minted onto generation-scoped names, got %q", legacy.gen)
	}
}

// TestSessGenFromRecordFailsClosed pins that a stored incarnation generation is
// validated before any name or purge filter is built from it.
//
// The record is not a trusted input — an older broker, a future one, or a hand
// edit can put anything there — and everything downstream is built by
// concatenation. A value carrying a wildcard or a subject separator would either
// produce an unusable durable name or, far worse, WIDEN the QoS 2 hold purge
// filter beyond this incarnation, deleting records the session has no claim on.
// Empty stays legal: that is the legacy hash-only form.
func TestSessGenFromRecordFailsClosed(t *testing.T) {
	good := mqttNewSessGen()
	if got, err := mqttSessGenFromRecord(good); err != nil || got != good {
		t.Fatalf("a well-formed generation must be accepted, got %q err=%v", got, err)
	}
	if got, err := mqttSessGenFromRecord(_EMPTY_); err != nil || got != _EMPTY_ {
		t.Fatalf("the legacy (empty) form must stay legal, got %q err=%v", got, err)
	}

	for _, bad := range []string{
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

	// The specific hazard, stated as the property it protects: whatever a record
	// carries, the purge filters must never reach outside this client's namespace.
	for _, gen := range []string{good, _EMPTY_} {
		for _, f := range mqttQoS2HoldPurgeFilters("HASH", gen) {
			if !strings.HasPrefix(f, mqttQoS2IncomingMsgsStreamSubjectPrefix+"HASH.") {
				t.Fatalf("purge filter %q escapes the client's namespace", f)
			}
		}
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
