package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

type testOrphanEnv struct {
	s    *Server
	asm  *mqttAccountSessionManager
	sess *mqttSession
}

func testNewOrphanEnv(t *testing.T, clientID string) *testOrphanEnv {
	t.Helper()
	// The account manager sweeps once at startup, on its own goroutine. That pass
	// advances the discovery cursor and would otherwise interleave with whatever a
	// test does next, so every env waits it out before returning. Tests here do not
	// run in parallel, so no other manager is starting concurrently.
	sweepsBefore := mqttOrphanSweepRuns.Load()
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
	// createOrRestoreSession neither registers nor persists; a live session is both,
	// and both are references the sweep must honor.
	asm.mu.Lock()
	asm.sessions[sess.id] = sess
	asm.sessByHash[sess.idHash] = sess
	asm.mu.Unlock()
	e := &testOrphanEnv{s: s, asm: asm, sess: sess}
	e.persist(t)
	deadline := time.Now().Add(10 * time.Second)
	for mqttOrphanSweepRuns.Load() <= sweepsBefore {
		if time.Now().After(deadline) {
			t.Fatal("the account manager never ran its startup orphan sweep")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return e
}

// persist writes the session's record, making it a live persisted generation.
func (e *testOrphanEnv) persist(t *testing.T) {
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

// forget detaches the session from the account manager's in-memory map and deletes
// its persisted record, which is exactly what makes its resources orphans: no live
// record and no local session reference either.
func (e *testOrphanEnv) forget(t *testing.T) {
	t.Helper()
	e.dropLocalSession()
	e.dropSessionRecord(t)
}

// dropLocalSession removes the session from BOTH in-memory indexes: `sessions` is
// keyed by client ID and `sessByHash` by the hash, and a reference surviving in
// either one legitimately protects the session's resources.
func (e *testOrphanEnv) dropLocalSession() {
	e.asm.mu.Lock()
	delete(e.asm.sessions, e.sess.id)
	delete(e.asm.sessByHash, e.sess.idHash)
	e.asm.mu.Unlock()
}

func (e *testOrphanEnv) dropSessionRecord(t *testing.T) {
	t.Helper()
	smsg, err := e.asm.jsa.loadSessionMsg(e.asm.domainTk, e.sess.idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("loading the session record: %v", err)
		}
		return
	}
	if err := e.asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("deleting the session record: %v", err)
	}
}

// testMustJSON encodes a record the way the broker stores it.
func testMustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// setGraces scopes the sweep's age policies to THIS account manager. Package-level
// knobs cannot be used from a test: the manager's own sweep goroutine reads them
// concurrently, so swapping one is a data race regardless of how it is restored.
func (e *testOrphanEnv) setGraces(t *testing.T, hold, release time.Duration) {
	t.Helper()
	e.asm.mu.Lock()
	e.asm.testOrphanGrace, e.asm.testQoS2ReleaseGrace = &hold, &release
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testOrphanGrace, e.asm.testQoS2ReleaseGrace = nil, nil
		e.asm.mu.Unlock()
	})
}

func (e *testOrphanEnv) setByteLimit(t *testing.T, n int64) {
	t.Helper()
	e.asm.mu.Lock()
	e.asm.testQoS2SweepByteLimit = &n
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQoS2SweepByteLimit = nil
		e.asm.mu.Unlock()
	})
}

func (e *testOrphanEnv) setRecordLimit(t *testing.T, n int) {
	t.Helper()
	e.asm.mu.Lock()
	e.asm.testQoS2SweepRecordLimit = &n
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQoS2SweepRecordLimit = nil
		e.asm.mu.Unlock()
	})
}

// testCollectCandidateSubjects runs one bounded discovery pass and returns the hold
// subjects it reported, so a test can watch the cursor advance across passes.
func testCollectCandidateSubjects(t *testing.T, e *testOrphanEnv) map[string]bool {
	t.Helper()
	cands, err := e.asm.collectInboundQoS2Candidates(e.s)
	if err != nil {
		t.Fatalf("collectInboundQoS2Candidates: %v", err)
	}
	out := make(map[string]bool, len(cands))
	for _, c := range cands {
		out[c.name.hold] = true
	}
	return out
}

// hold writes a held inbound QoS 2 PUBLISH on the session's own incarnation.
func (e *testOrphanEnv) hold(t *testing.T, pi uint16, gen, payload string) string {
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
	if _, err := e.asm.storeQoS2HoldCAS(subject, 0, hdrLen, msg); err != nil {
		t.Fatalf("store hold record for pi %d: %v", pi, err)
	}
	return subject
}

func (e *testOrphanEnv) putClaim(t *testing.T, holdSubject string, c *mqttQoS2RelClaim) uint64 {
	t.Helper()
	seq, err := e.asm.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(holdSubject), 0, c)
	if err != nil {
		t.Fatalf("store claim on %q: %v", holdSubject, err)
	}
	return seq
}

func (e *testOrphanEnv) holdPresent(t *testing.T, holdSubject string) bool {
	t.Helper()
	rec, _, err := e.asm.jsa.loadQoS2Record(holdSubject)
	if err != nil {
		t.Fatalf("load hold record on %q: %v", holdSubject, err)
	}
	return rec != nil
}

func (e *testOrphanEnv) claimPresent(t *testing.T, holdSubject string) bool {
	t.Helper()
	_, smsg, err := e.asm.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil {
		t.Fatalf("load claim on %q: %v", holdSubject, err)
	}
	return smsg != nil
}

// sweepNow runs the inbound half against a clock far enough ahead that every grace
// has elapsed. Store times are assigned by JetStream and cannot be backdated, so
// moving `now` forward is how a test ages a record.
func (e *testOrphanEnv) sweepInbound(t *testing.T) {
	t.Helper()
	cands, err := e.asm.collectInboundQoS2Candidates(e.s)
	if err != nil {
		t.Fatalf("collectInboundQoS2Candidates: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	e.asm.reconcileInboundQoS2(e.s, cands, refs)
}

// testAged is a cluster-clock reference far enough ahead that every grace has
// elapsed. Production takes this from sweepClockRef; a test supplies it directly,
// which is also what keeps these assertions independent of how fast the suite runs.
// settle runs the two inbound passes eligibility requires. The first records each
// candidate's identity in quarantine and authorizes nothing; only a later
// authoritative pass over an unchanged identity may act. Tests that assert a single
// pass does NOTHING call sweepInbound directly.
func (e *testOrphanEnv) settle(t *testing.T) {
	t.Helper()
	e.sweepInbound(t)
	e.sweepInbound(t)
}

// settleAffineConsumers is the same two-pass rule for the affine consumer path.
func (e *testOrphanEnv) settleAffineConsumers(t *testing.T, budget int) {
	t.Helper()
	for i := 0; i < 3; i++ {
		e.sweepAffineConsumers(t, budget)
	}
}

// ---------------------------------------------------------------------------
// Parsers and age policy
// ---------------------------------------------------------------------------

func TestOrphanSweepParsesInboundSubjectsStrictly(t *testing.T) {
	t.Parallel()
	const hash = "abcd1234"
	gen := nuid.Next()
	prefix := mqttQoS2IncomingMsgsStreamSubjectPrefix

	for _, tc := range []struct {
		name    string
		subject string
		ok      bool
		isClaim bool
	}{
		{"hold", prefix + hash + "." + gen + ".7", true, false},
		{"claim", prefix + hash + "." + gen + ".7.rel", true, true},
		{"max packet identifier", prefix + hash + "." + gen + ".65535", true, false},
		{"foreign prefix", "$MQTT.other." + hash + "." + gen + ".7", false, false},
		{"too few tokens", prefix + hash + "." + gen, false, false},
		{"too many tokens", prefix + hash + "." + gen + ".7.8", false, false},
		{"four tokens not ending in rel", prefix + hash + "." + gen + ".7.x", false, false},
		{"short hash", prefix + "abc." + gen + ".7", false, false},
		{"hash out of alphabet", prefix + "abcd123-" + "." + gen + ".7", false, false},
		{"malformed generation", prefix + hash + ".short.7", false, false},
		{"packet identifier zero", prefix + hash + "." + gen + ".0", false, false},
		{"packet identifier overflows", prefix + hash + "." + gen + ".65536", false, false},
		{"packet identifier not canonical", prefix + hash + "." + gen + ".07", false, false},
		{"packet identifier not decimal", prefix + hash + "." + gen + ".x", false, false},
	} {
		n, err := mqttParseQoS2InSubject(tc.subject)
		if tc.ok != (err == nil) {
			t.Fatalf("%s: parse err = %v, want ok = %v", tc.name, err, tc.ok)
		}
		if !tc.ok {
			continue
		}
		if n.isClaim != tc.isClaim {
			t.Fatalf("%s: isClaim = %v, want %v", tc.name, n.isClaim, tc.isClaim)
		}
		// Both kinds resolve to the SAME pair of names, which is what lets one walk
		// discover an identity from either of its records.
		if n.claim != mqttQoS2ClaimSubjectFor(n.hold) {
			t.Fatalf("%s: claim %q is not the sibling of hold %q", tc.name, n.claim, n.hold)
		}
		if n.idHash != hash || n.gen != gen {
			t.Fatalf("%s: parsed identity (%q,%q), want (%q,%q)", tc.name, n.idHash, n.gen, hash, gen)
		}
	}
}

func TestOrphanSweepParsesSessionSubjectsAndAffineDurables(t *testing.T) {
	t.Parallel()
	const hash = "abcd1234"
	gen := nuid.Next()

	for _, tc := range []struct {
		name, subject, domainTk string
		ok                      bool
	}{
		{"no domain", mqttSessStreamSubjectPrefix + hash, _EMPTY_, true},
		{"with domain", mqttSessStreamSubjectPrefix + "hub." + hash, "hub.", true},
		{"foreign domain", mqttSessStreamSubjectPrefix + "other." + hash, "hub.", false},
		{"malformed hash", mqttSessStreamSubjectPrefix + "xyz", _EMPTY_, false},
		{"extra token", mqttSessStreamSubjectPrefix + hash + ".more", _EMPTY_, false},
		{"foreign prefix", "$MQTT.other." + hash, _EMPTY_, false},
	} {
		got, err := mqttSessIDHashFromSubject(tc.subject, tc.domainTk)
		if tc.ok != (err == nil) {
			t.Fatalf("%s: err = %v, want ok = %v", tc.name, err, tc.ok)
		}
		if tc.ok && got != hash {
			t.Fatalf("%s: hash = %q, want %q", tc.name, got, hash)
		}
	}

	_, _, durable := mqttQ2AffineNames(hash, gen)
	gotHash, gotGen, err := mqttParseQ2AffDurable(durable)
	if err != nil || gotHash != hash || gotGen != gen {
		t.Fatalf("round trip of %q gave (%q,%q,%v), want (%q,%q,nil)", durable, gotHash, gotGen, err, hash, gen)
	}
	for _, bad := range []string{
		"$MQTT_Q2AFF_" + hash,
		"$MQTT_Q2AFF_" + hash + "_short",
		"$MQTT_Q2AFF_" + hash + "." + gen,
		"$MQTT_Q2AFF_xyz_" + gen,
		"$MQTT_PUBREL_" + hash + "_" + gen,
	} {
		if _, _, err := mqttParseQ2AffDurable(bad); err == nil {
			t.Fatalf("%q must not parse as an affine durable", bad)
		}
	}
}

// TestOrphanSweepQuarantineRule pins the rule that replaced age arithmetic.
//
// There is no clock to subtract against. A JetStream store time comes from whichever
// server led that stream's Raft group at that moment; four streams are four groups,
// and one stream spans several clocks across a leadership change. So the grace is a
// STABLE-OBSERVATION requirement measured on this process's monotonic clock, and a
// stored timestamp is never arithmetic — only identity is.
func TestOrphanSweepQuarantineRule(t *testing.T) {
	t.Parallel()
	t.Run("one observation never authorizes, even at zero grace", func(t *testing.T) {
		q := newMQTTSweepQuarantine()
		if q.observedOrphanFor("k", "fp", 0) {
			t.Fatal("a first sighting authorized a mutation; observation and authorization " +
				"must never collapse into one pass, or a resource glimpsed mid-construction is acted on")
		}
		if !q.observedOrphanFor("k", "fp", 0) {
			t.Fatal("a second sighting of an unchanged identity past a zero grace must qualify")
		}
	})

	t.Run("the wait is monotonic elapsed time, not an age", func(t *testing.T) {
		q := newMQTTSweepQuarantine()
		q.observedOrphanFor("k", "fp", time.Hour)
		if q.observedOrphanFor("k", "fp", time.Hour) {
			t.Fatal("a second sighting qualified before the grace elapsed")
		}
		// Rewind the observation rather than sleeping: what is being pinned is that the
		// span is measured locally, so a test can move only the local endpoint.
		q.mu.Lock()
		q.entries["k"].since = time.Now().Add(-2 * time.Hour)
		q.mu.Unlock()
		if !q.observedOrphanFor("k", "fp", time.Hour) {
			t.Fatal("an unchanged identity observed for longer than the grace did not qualify")
		}
	})

	t.Run("any identity change restarts the wait", func(t *testing.T) {
		q := newMQTTSweepQuarantine()
		q.observedOrphanFor("k", "seq-1", 0)
		if q.observedOrphanFor("k", "seq-2", 0) {
			t.Fatal("a replaced record inherited the previous record's quarantine")
		}
		if !q.observedOrphanFor("k", "seq-2", 0) {
			t.Fatal("the new identity never accumulated its own wait")
		}
	})

	t.Run("becoming referenced again restarts the wait", func(t *testing.T) {
		q := newMQTTSweepQuarantine()
		q.observedOrphanFor("k", "fp", 0)
		q.forget("k") // the session came back
		if q.observedOrphanFor("k", "fp", 0) {
			t.Fatal("a resource that was referenced again inherited the wait accumulated while it looked absent")
		}
	})

	t.Run("the table is bounded", func(t *testing.T) {
		q := newMQTTSweepQuarantine()
		q.max = 4
		for i := 0; i < 50; i++ {
			q.observedOrphanFor(fmt.Sprintf("k%d", i), "fp", 0)
		}
		q.mu.Lock()
		n := len(q.entries)
		q.mu.Unlock()
		if n > q.max {
			t.Fatalf("the quarantine table grew to %d entries, past its %d cap", n, q.max)
		}
	})

	t.Run("a backlog larger than the cap still makes progress", func(t *testing.T) {
		// The eviction trap: a deterministic scan revisits keys in the same order, so
		// evicting to admit each new key would evict the one due next, every call would
		// look like a first sighting, and NOTHING past the cap could ever qualify.
		q := newMQTTSweepQuarantine()
		q.max = 4
		keys := make([]string, 20)
		for i := range keys {
			keys[i] = fmt.Sprintf("k%d", i)
		}
		scan := func() int {
			qualified := 0
			for _, k := range keys {
				if q.observedOrphanFor(k, "fp", 0) {
					qualified++
					q.forget(k) // acted on: its slot is released
				}
			}
			return qualified
		}
		scan() // first sighting of the resident cohort; nothing qualifies yet
		if got := scan(); got == 0 {
			t.Fatal("no candidate ever qualified in a backlog larger than the cap; " +
				"the resident cohort is being evicted before it can be observed twice")
		}
		// And the whole backlog drains rather than a fixed prefix monopolizing the table.
		seen := 0
		for i := 0; i < 20 && seen < len(keys); i++ {
			seen += scan()
		}
		if seen < len(keys) {
			t.Fatalf("only %d of %d candidates ever qualified; the backlog does not drain", seen, len(keys))
		}
	})

	t.Run("a stale entry does not hold its slot forever", func(t *testing.T) {
		q := newMQTTSweepQuarantine()
		q.max, q.stale = 2, time.Hour
		q.observedOrphanFor("gone-a", "fp", 0)
		q.observedOrphanFor("gone-b", "fp", 0)
		if q.observedOrphanFor("newcomer", "fp", 0) {
			t.Fatal("a first sighting qualified")
		}
		q.mu.Lock()
		_, admitted := q.entries["newcomer"]
		for _, k := range []string{"gone-a", "gone-b"} {
			q.entries[k].lastSeen = time.Now().Add(-2 * time.Hour)
		}
		q.mu.Unlock()
		if admitted {
			t.Fatal("this test needs the table to be full before the stale prune")
		}
		q.observedOrphanFor("newcomer", "fp", 0)
		q.mu.Lock()
		_, admitted = q.entries["newcomer"]
		q.mu.Unlock()
		if !admitted {
			t.Fatal("entries for resources nobody has revisited in ages held their slots forever")
		}
	})
}

// TestOrphanSweepAdoptionValidatesTheAffinePair pins that teardown adoption applies
// the same affine-name validation the reference walk does.
//
// mqttTeardownTargetFromRecord copies the record's Q2Aff.Durable VERBATIM and the
// adoption deletes that consumer. A record naming a durable that is not this
// session's would therefore delete somebody else's $MQTT_out consumer and, because
// that stream is interest-retained, the copies it still held. Validating the
// generation alone is not enough, which is exactly how this was missed.
func TestOrphanSweepAdoptionValidatesTheAffinePair(t *testing.T) {
	e := testNewOrphanEnv(t, "adopt-validates-affine")
	e.setGraces(t, 0, 0)

	// A victim: a live affine consumer belonging to a different session entirely.
	victimHash, victimGen := getHash("adopt-victim"), mqttNewQ2AffGen()
	victimDur := testCreateAffineConsumer(t, e, victimHash, victimGen)

	// A marked record whose affine config points at the victim's durable.
	ps := &mqttPersistedSession{
		ID: e.sess.id, Gen: e.sess.gen,
		Q2AffGen: mqttNewQ2AffGen(),
		Q2Aff:    &ConsumerConfig{Durable: victimDur},
		Teardown: &mqttSessTeardown{Origin: "corrupt", Nonce: nuid.Next(), StartedAt: time.Now().Unix()},
	}
	subject := mqttSessStreamSubjectPrefix + e.asm.domainTk + e.sess.idHash
	if _, err := e.asm.jsa.storeMsg(subject, 0, testMustJSON(t, ps)); err != nil {
		t.Fatalf("writing the corrupt marked record: %v", err)
	}
	e.dropLocalSession()

	e.asm.adoptAbandonedTeardowns(e.s)
	e.asm.adoptAbandonedTeardowns(e.s)

	if !e.consumerExists(t, victimDur) {
		t.Fatal("adopting a teardown whose affine config names a foreign durable deleted " +
			"that consumer, taking its interest-retained copies with it")
	}
	// The marker is left for a path that can interpret it, not silently adopted.
	if cur, _, err := e.asm.loadFenceSession(e.sess.idHash); err != nil || cur == nil || cur.Teardown == nil {
		t.Fatal("the unvalidatable marked record was adopted anyway")
	}
}

// TestOrphanSweepPostSnapshotRereadsFailClosed pins that the re-reads which actually
// AUTHORIZE a mutation are no weaker than the reference walk they re-verify.
//
// A record written after the complete scan has not been through its validation, and
// `loadFenceSession` only decodes JSON. If such a record is uninterpretable — its ID
// does not hash back to the subject it sits on, its generation is malformed, or its
// affine pair is inconsistent — it must be INDETERMINATE. Reading it as "a valid
// record for a different generation" turns it into proof of absence, which discards
// a live assignment or deletes a referenced consumer along with its
// interest-retained copies.
func TestOrphanSweepPostSnapshotRereadsFailClosed(t *testing.T) {
	// Each case is a record that decodes cleanly but cannot be interpreted, and that
	// names a generation DIFFERENT from the candidate's — the shape that would
	// otherwise read as "this winner's incarnation is over".
	malformed := []struct {
		name   string
		record func(idHash string) *mqttPersistedSession
	}{
		{"ID does not hash back to the subject", func(string) *mqttPersistedSession {
			return &mqttPersistedSession{ID: "somebody-else", Gen: mqttNewSessGen(), Q2AffGen: mqttNewQ2AffGen()}
		}},
		{"malformed incarnation generation", func(string) *mqttPersistedSession {
			return &mqttPersistedSession{ID: "x", Gen: "short", Q2AffGen: mqttNewQ2AffGen()}
		}},
		{"affine config inconsistent with its generation", func(string) *mqttPersistedSession {
			return &mqttPersistedSession{
				ID: "x", Gen: mqttNewSessGen(), Q2AffGen: mqttNewQ2AffGen(),
				Q2Aff: &ConsumerConfig{Durable: "$MQTT_Q2AFF_somewhere_else"},
			}
		}},
		{"shared QoS 2 membership with no affine generation", func(string) *mqttPersistedSession {
			// The pre-affine shape. Its EMPTY generation is what makes it dangerous
			// here rather than merely malformed: emptiness differs from every claim's
			// generation, so the "valid record for a different generation" arm would
			// read it as proof the winner is gone.
			return &mqttPersistedSession{
				ID: "x", Gen: mqttNewSessGen(),
				SharedSubs: map[string]byte{"$share/g/reread.topic": 2},
			}
		}},
	}

	t.Run("a raced record never authorizes discarding an assignment", func(t *testing.T) {
		for _, tc := range malformed {
			t.Run(tc.name, func(t *testing.T) {
				e := testNewOrphanEnv(t, "reread-claim-"+strings.ReplaceAll(tc.name, " ", "-"))
				e.setGraces(t, 0, 0)
				idHash, gen := getHash("reread-winner"), mqttNewQ2AffGen()
				groupDur := mqttSharedConsumerDurablePrefix + "REREAD"
				subject := mqttQ2AffClaimSubject(groupDur, 91)
				c := testValidQ2AffClaim(idHash, gen, groupDur, 91)
				if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
					t.Fatalf("store claim: %v", err)
				}

				// The record arrives between the snapshot and the re-read.
				rec := tc.record(idHash)
				if rec.ID == "x" {
					rec.ID = "reread-winner" // hash back correctly where that is not the defect
				}
				e.asm.mu.Lock()
				e.asm.testSessionLoad = func(string) (*mqttPersistedSession, uint64, error) {
					return rec, 1, nil
				}
				e.asm.mu.Unlock()
				t.Cleanup(func() {
					e.asm.mu.Lock()
					e.asm.testSessionLoad = nil
					e.asm.mu.Unlock()
				})

				if got := e.asm.discardAbsentWinnerClaim(e.s, &mqttQ2AffInvEntry{subject: subject, claim: c}); got != mqttQoS2SweepDeferred {
					t.Fatalf("outcome = %v, want deferred", got)
				}
				cur, _, err := e.asm.loadQ2AffClaim(subject)
				if err != nil {
					t.Fatalf("load claim: %v", err)
				}
				if cur == nil || cur.Discarded {
					t.Fatalf("an uninterpretable raced record was read as proof the winner is gone, "+
						"discarding a possibly-live assignment: %+v", cur)
				}
			})
		}
	})

	t.Run("a raced record never authorizes deleting a consumer", func(t *testing.T) {
		for _, tc := range malformed {
			t.Run(tc.name, func(t *testing.T) {
				e := testNewOrphanEnv(t, "reread-cons-"+strings.ReplaceAll(tc.name, " ", "-"))
				e.setGraces(t, 0, 0)
				idHash, gen := getHash("reread-owner"), mqttNewQ2AffGen()
				durable := testCreateAffineConsumer(t, e, idHash, gen)

				rec := tc.record(idHash)
				if rec.ID == "x" {
					rec.ID = "reread-owner"
				}
				e.asm.mu.Lock()
				e.asm.testSessionLoad = func(string) (*mqttPersistedSession, uint64, error) {
					return rec, 1, nil
				}
				e.asm.mu.Unlock()
				t.Cleanup(func() {
					e.asm.mu.Lock()
					e.asm.testSessionLoad = nil
					e.asm.mu.Unlock()
				})

				e.settleAffineConsumers(t, 16)
				if !e.consumerExists(t, durable) {
					t.Fatal("an uninterpretable raced record was read as evidence that nothing " +
						"references this consumer, so it was deleted with its interest-retained copies")
				}
			})
		}
	})
}

// TestOrphanSweepRetainedAssignmentsHoldNoQuarantineSlot pins that the state which
// accumulates cannot switch the sweep off.
//
// `Lost` is permanent by design and a `Discarded` claim is retained until its source
// is proven closed — so those are exactly the claims an account collects. If they
// held quarantine slots they would fill the shared table, and it would then refuse
// every new hold, claim, consumer and teardown key across the whole account: the
// sweep would stop reconciling anything, permanently.
func TestOrphanSweepRetainedAssignmentsHoldNoQuarantineSlot(t *testing.T) {
	e := testNewOrphanEnv(t, "retained-no-slot")
	e.setGraces(t, 0, 0)
	groupDur := mqttSharedConsumerDurablePrefix + "NOSLOT"
	idHash, gen := getHash("noslot-winner"), mqttNewQ2AffGen()

	// A live group durable and a real source message, so these claims are genuinely
	// retained rather than reapable.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable: groupDur, FilterSubject: mqttStreamSubjectPrefix + "noslot.topic",
			DeliverSubject: mqttSharedSubDeliverPrefix + "noslot", DeliverGroup: "g",
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 64,
		},
	}); err != nil {
		t.Fatalf("create group durable: %v", err)
	}
	for i, terminal := range []string{"lost", "discarded"} {
		resp, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+"noslot.topic", 0, []byte("source"))
		if err != nil {
			t.Fatalf("store source: %v", err)
		}
		c := testValidQ2AffClaim(idHash, gen, groupDur, resp.Sequence)
		c.Phase = mqttQ2AffMaterialized
		switch terminal {
		case "lost":
			c.Lost = true
		case "discarded":
			c.Discarded = true
		}
		if _, err := e.asm.storeQ2AffClaimCAS(mqttQ2AffClaimSubject(groupDur, resp.Sequence), 0, c); err != nil {
			t.Fatalf("store claim %d: %v", i, err)
		}
	}

	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	budget := 16
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)

	q := e.asm.quarantine()
	q.mu.Lock()
	held := len(q.entries)
	q.mu.Unlock()
	if held != 0 {
		t.Fatalf("terminal and retained assignments are holding %d quarantine slot(s); "+
			"enough of them would fill the shared table and refuse every other candidate in the account", held)
	}
	// Both must still be there — this is about slots, not about reaping evidence.
	for _, e2 := range inv {
		if cur, _, err := e.asm.loadQ2AffClaim(e2.subject); err != nil || cur == nil {
			t.Fatalf("a retained assignment was reaped (cur=%v err=%v)", cur, err)
		}
	}
}

// TestOrphanSweepAdoptionValidatesEveryDeletionTarget pins that adoption checks all
// four durable fields a teardown would delete, not just the affine one.
//
// mqttTeardownTargetFromRecord copies Cons, Drains and PubRel durables verbatim
// alongside Q2Aff, and the adoption deletes every one of them. Validating a single
// field left three open.
func TestOrphanSweepAdoptionValidatesEveryDeletionTarget(t *testing.T) {
	victimHash := getHash("target-victim")
	for _, tc := range []struct {
		name    string
		corrupt func(ps *mqttPersistedSession)
	}{
		{"a plain subscription consumer belonging to another session", func(ps *mqttPersistedSession) {
			ps.Cons = map[string]*ConsumerConfig{"1": {Durable: victimHash + "_" + nuid.Next()}}
		}},
		{"a drain consumer belonging to another session", func(ps *mqttPersistedSession) {
			ps.Drains = map[string]*mqttPersistedDrain{"1": {Cons: &ConsumerConfig{Durable: victimHash + "_" + nuid.Next()}}}
		}},
		{"a PUBREL consumer belonging to another session", func(ps *mqttPersistedSession) {
			_, _, dur := mqttPubRelNames(victimHash, mqttNewSessGen())
			ps.PubRel = &ConsumerConfig{Durable: dur}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewOrphanEnv(t, "targets-"+strings.ReplaceAll(tc.name, " ", "-"))
			e.setGraces(t, 0, 0)
			ps := &mqttPersistedSession{
				ID: e.sess.id, Gen: e.sess.gen,
				Teardown: &mqttSessTeardown{Origin: "corrupt", Nonce: nuid.Next(), StartedAt: time.Now().Unix()},
			}
			tc.corrupt(ps)
			subject := mqttSessStreamSubjectPrefix + e.asm.domainTk + e.sess.idHash
			if _, err := e.asm.jsa.storeMsg(subject, 0, testMustJSON(t, ps)); err != nil {
				t.Fatalf("writing the corrupt marked record: %v", err)
			}
			e.dropLocalSession()

			e.asm.adoptAbandonedTeardowns(e.s)
			e.asm.adoptAbandonedTeardowns(e.s)

			cur, _, err := e.asm.loadFenceSession(e.sess.idHash)
			if err != nil {
				t.Fatalf("loading the record: %v", err)
			}
			if cur == nil || cur.Teardown == nil {
				t.Fatal("a record naming a durable that is not this session's was adopted, " +
					"so the teardown deleted a consumer belonging to somebody else")
			}
		})
	}
}

// TestOrphanSweepRevalidatesConsumerIdentityBeforeDelete pins the recreation race.
//
// A peer provisions an affine consumer BEFORE saving the record that references it —
// that create-before-save window is the whole reason the grace exists. If the sweep
// deletes by name using a fingerprint from an earlier listing, it destroys the
// peer's fresh consumer inside exactly that window, because the wait it accumulated
// belongs to a consumer that no longer exists.
func TestOrphanSweepRevalidatesConsumerIdentityBeforeDelete(t *testing.T) {
	e := testNewOrphanEnv(t, "consumer-recreated")
	e.setGraces(t, 0, 0)
	idHash, gen := getHash("recreated-owner"), mqttNewQ2AffGen()
	durable := testCreateAffineConsumer(t, e, idHash, gen)

	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	cons, err := e.asm.jsa.consumerList(mqttOutStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	budget := 16
	// One pass to quarantine the instance this listing saw.
	e.asm.deleteOrphanQ2AffConsumers(e.s, inv, cons, refs, &budget)

	// A peer tears it down and provisions it again under the same name, and has not
	// saved its record yet.
	if _, err := e.asm.jsa.deleteConsumer(mqttOutStreamName, durable, false); err != nil {
		t.Fatalf("deleting the consumer: %v", err)
	}
	time.Sleep(10 * time.Millisecond) // a distinguishable Created
	testCreateAffineConsumer(t, e, idHash, gen)

	// The sweep proceeds from its now-stale listing.
	e.asm.deleteOrphanQ2AffConsumers(e.s, inv, cons, refs, &budget)

	if !e.consumerExists(t, durable) {
		t.Fatal("a consumer recreated since the listing was deleted on the previous instance's " +
			"fingerprint, destroying a peer's fresh consumer inside the create-before-save window")
	}
}

// TestOrphanSweepBudgetIsChargedOnlyForWrites pins that already-discarded
// assignments cannot starve the ones behind them.
//
// A discarded claim whose source can still redeliver is RETAINED by design. If each
// such claim consumed a unit of the per-tick mutation budget, an inventory whose
// head held a budget's worth of them would burn the whole budget on the same prefix
// every pass, and assignments further down would never be reconciled at all.
func TestOrphanSweepBudgetIsChargedOnlyForWrites(t *testing.T) {
	e := testNewOrphanEnv(t, "budget-writes-only")
	e.setGraces(t, 0, 0)
	groupDur := mqttSharedConsumerDurablePrefix + "BUDGET"

	// Two claims that are already Discarded and not reapable (their group durable is
	// live and their source message still exists), followed by one that is not.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable: groupDur, FilterSubject: mqttStreamSubjectPrefix + "budget.topic",
			DeliverSubject: mqttSharedSubDeliverPrefix + "budget", DeliverGroup: "g",
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 64,
		},
	}); err != nil {
		t.Fatalf("create group durable: %v", err)
	}
	var sseqs []uint64
	for i := 0; i < 3; i++ {
		resp, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+"budget.topic", 0, []byte("source"))
		if err != nil {
			t.Fatalf("store source: %v", err)
		}
		sseqs = append(sseqs, resp.Sequence)
	}
	idHash, gen := getHash("budget-winner"), mqttNewQ2AffGen()
	for i, sseq := range sseqs {
		c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
		c.Phase = mqttQ2AffMaterialized
		c.Discarded = i < 2 // the first two are terminal on the discard axis
		if _, err := e.asm.storeQ2AffClaimCAS(mqttQ2AffClaimSubject(groupDur, sseq), 0, c); err != nil {
			t.Fatalf("store claim: %v", err)
		}
	}

	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	// A budget of exactly one: enough for the single real transition, and nothing to
	// spare if the two terminal claims are charged for their no-op.
	budget := 1
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)

	last := mqttQ2AffClaimSubject(groupDur, sseqs[2])
	cur, _, err := e.asm.loadQ2AffClaim(last)
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if cur == nil || !cur.Discarded {
		t.Fatalf("the assignment behind two already-discarded ones was never reconciled: %+v", cur)
	}
}

// TestOrphanSweepNoStoredTimestampDecidesEligibility is the regression guard for the
// bug this rule replaced.
//
// A single pass must never mutate anything, however old the records look, because
// "how old they look" is a number from a clock this process cannot vouch for. Only
// the second authoritative pass over an unchanged identity may act.
func TestOrphanSweepNoStoredTimestampDecidesEligibility(t *testing.T) {
	e := testNewOrphanEnv(t, "no-timestamp-arithmetic")
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")
	e.forget(t)
	e.setGraces(t, 0, 0)

	// Pass one: observation only.
	e.sweepInbound(t)
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("a single sweep pass destroyed a payload; observation and authorization " +
			"must be separate passes so a stale or skewed timestamp cannot license a deletion")
	}
	// Pass two: the identity is unchanged and the (zero) grace has elapsed.
	e.sweepInbound(t)
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the second authoritative pass did not act on a stable orphan")
	}

	// A record whose identity changes between passes starts over.
	other := e.hold(t, 2, nuid.Next(), "payload")
	e.sweepInbound(t)
	if _, err := e.asm.jsa.storeMsg(other, 0, []byte("rewritten")); err != nil {
		t.Fatalf("rewriting the record: %v", err)
	}
	e.sweepInbound(t)
	if !e.holdPresent(t, other) {
		t.Fatal("a record rewritten between passes was acted on with the previous record's quarantine")
	}
}

// ---------------------------------------------------------------------------
// The two grace policies stay independent
// ---------------------------------------------------------------------------

// TestOrphanSweepAppliesTheTwoGracesToTheirOwnClasses pins that a hold is aged by
// the construction grace while a release claim is aged by the much longer one. A
// claim arbitrates against a process that may be paused mid-release, which is not
// the timescale of a consumer created just before its record save.
func TestOrphanSweepAppliesTheTwoGracesToTheirOwnClasses(t *testing.T) {
	e := testNewOrphanEnv(t, "two-graces")
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node1"))
	e.forget(t)

	// The hold's quarantine is satisfied; the claim's is nowhere near. The claim still
	// fences, so nothing is destroyed however long the hold has been settled.
	e.setGraces(t, 0, time.Hour)
	e.settle(t)
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("the hold was destroyed while its claim was still inside the release-claim grace")
	}
	if !e.claimPresent(t, holdSubject) {
		t.Fatal("the claim was retired on the construction grace, not its own")
	}

	// Both satisfied.
	e.setGraces(t, 0, 0)
	e.settle(t)
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the hold survived a fully aged discarded claim")
	}
	if e.claimPresent(t, holdSubject) {
		t.Fatal("the claim survived once its hold was gone")
	}
}

// ---------------------------------------------------------------------------
// Inbound dispositions
// ---------------------------------------------------------------------------

// TestOrphanSweepFencesAnUnclaimedHoldBeforeDestroyingIt pins the discard-before-
// purge ordering for a hold nobody claimed, and that the marker it leaves behind
// survives its own, longer grace.
func TestOrphanSweepFencesAnUnclaimedHoldBeforeDestroyingIt(t *testing.T) {
	e := testNewOrphanEnv(t, "unclaimed-hold")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")
	e.forget(t)

	e.settle(t)
	if e.holdPresent(t, holdSubject) {
		t.Fatal("an aged unclaimed orphan hold was not destroyed")
	}
	claim, _ := func() (*mqttQoS2RelClaim, *StoredMsg) {
		c, smsg, err := e.asm.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
		if err != nil {
			t.Fatalf("load claim: %v", err)
		}
		return c, smsg
	}()
	if claim == nil || claim.State != mqttQoS2RelStateDiscarded {
		t.Fatalf("the hold must be fenced by a discarded marker before being purged, got %+v", claim)
	}

	// The marker is NOT reaped in the same pass: it answers to the release-claim
	// grace, which has not elapsed.
	if !e.claimPresent(t, holdSubject) {
		t.Fatal("the fresh discard marker was reaped before its own grace elapsed")
	}
	e.settle(t)
	if e.claimPresent(t, holdSubject) {
		t.Fatal("the aged discard marker was never reaped")
	}
}

// TestOrphanSweepRetiresAnAgedReleasingClaim covers the dead-releaser case: the
// claim keeps fencing while the payload is destroyed, and only then is it removed.
func TestOrphanSweepRetiresAnAgedReleasingClaim(t *testing.T) {
	e := testNewOrphanEnv(t, "dead-releaser")
	e.setGraces(t, 0, 0)
	gen := nuid.Next()
	holdSubject := e.hold(t, 1, gen, "payload")
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "node2"))
	e.forget(t)

	e.settle(t)
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the hold of a dead releaser was retained forever")
	}
	if e.claimPresent(t, holdSubject) {
		t.Fatal("the aged releasing claim was retained forever")
	}
}

// TestOrphanSweepRemovesAClaimWithNoHold covers the leftover a completed exchange
// can leave when its broker died between the terminal commit and the claim reap.
func TestOrphanSweepRemovesAClaimWithNoHold(t *testing.T) {
	e := testNewOrphanEnv(t, "claim-no-hold")
	e.setGraces(t, 0, 0)
	holdSubject := mqttQoS2HoldSubject(e.sess.idHash, e.sess.gen, 3)
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "node2"))
	e.forget(t)

	e.settle(t)
	if e.claimPresent(t, holdSubject) {
		t.Fatal("an aged orphan claim with no hold was retained forever")
	}
}

// TestOrphanSweepRetiresAnAgedUnreadableClaim pins the one explicit corrupt-record
// disposition: the unreadable record is removed at its exact sequence, and the hold
// it was standing in front of gets a fresh, READABLE fence before it is purged —
// never a purge with nothing in front of it.
func TestOrphanSweepRetiresAnAgedUnreadableClaim(t *testing.T) {
	e := testNewOrphanEnv(t, "unreadable-claim")
	e.setGraces(t, 0, time.Hour)
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")
	claimSubject := mqttQoS2ClaimSubjectFor(holdSubject)
	if _, err := e.asm.jsa.storeMsg(claimSubject, 0, []byte("{not json")); err != nil {
		t.Fatalf("store an unreadable claim: %v", err)
	}
	e.forget(t)

	// Inside the release-claim grace: an unreadable record is not a licence to hurry.
	e.settle(t)
	if !e.claimPresent(t, holdSubject) {
		t.Fatal("a young unreadable claim was retired on the construction grace")
	}
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("the hold was destroyed behind a young unreadable claim")
	}

	e.setGraces(t, 0, 0)
	// Past it, the pair retires the same way every other claimed pair does: the
	// unreadable record fences the purge, and is then removed at its exact sequence.
	// It is deliberately NOT replaced by a fresh marker first — see
	// TestOrphanSweepUnreadableClaimNeverLeavesTheHoldUnfenced for why.
	e.settle(t)
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the hold behind an aged unreadable claim was never destroyed")
	}
	if _, smsg, err := e.asm.jsa.loadQoS2RelClaim(claimSubject); err != nil || smsg != nil {
		t.Fatalf("the aged unreadable claim was not retired at its exact sequence (smsg=%v err=%v)", smsg, err)
	}
}

// TestOrphanSweepUnreadableClaimNeverLeavesTheHoldUnfenced is the ordering pin for
// the corrupt-record disposition.
//
// An unreadable record is still a FENCE: the claim write is a create-only CAS, which
// fails while any record occupies the subject. Deleting it first and creating a
// replacement marker would empty the subject while the payload is still there, and a
// releaser that preloaded that payload could win a claim in the gap and read its hold
// as still Held. So the hold is destroyed behind the unreadable record, and only then
// is that exact sequence removed.
func TestOrphanSweepUnreadableClaimNeverLeavesTheHoldUnfenced(t *testing.T) {
	e := testNewOrphanEnv(t, "unreadable-ordering")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")
	claimSubject := mqttQoS2ClaimSubjectFor(holdSubject)
	if _, err := e.asm.jsa.storeMsg(claimSubject, 0, []byte("{not json")); err != nil {
		t.Fatalf("store an unreadable claim: %v", err)
	}
	e.forget(t)

	// Observe the state of the hold at the instant the claim record is deleted. The
	// purge goes through STREAM.PURGE, so this hook sees the claim delete only.
	sawClaimDelete, holdGoneAtClaimDelete := false, true
	restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
		if stream == mqttQoS2IncomingMsgsStreamName {
			sawClaimDelete = true
			holdGoneAtClaimDelete = holdGoneAtClaimDelete && !e.holdPresent(t, holdSubject)
		}
		return nil
	})
	defer restore()

	e.settle(t)

	// The ordering assertion comes first, so a regression reports the reason rather
	// than one of its symptoms.
	if !sawClaimDelete {
		t.Fatal("the claim delete never happened, so this test asserted nothing about its ordering")
	}
	if !holdGoneAtClaimDelete {
		t.Fatal("the claim subject was emptied while the hold record still existed, " +
			"which lets a stalled releaser re-claim and fan out from an orphaned session")
	}
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the hold behind an aged unreadable claim was never destroyed")
	}
	if e.claimPresent(t, holdSubject) {
		t.Fatal("the aged unreadable claim was never retired")
	}
}

// TestOrphanSweepKeepsEverythingForALiveGeneration is the guard the whole file
// rests on: a session whose record is present owns its subjects, however old they
// are.
func TestOrphanSweepKeepsEverythingForALiveGeneration(t *testing.T) {
	e := testNewOrphanEnv(t, "live-generation")
	e.setGraces(t, 0, 0)
	gen := nuid.Next()
	holdSubject := e.hold(t, 1, gen, "payload")
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "node2"))

	e.settle(t)
	if !e.holdPresent(t, holdSubject) || !e.claimPresent(t, holdSubject) {
		t.Fatal("the sweep destroyed state belonging to a live session generation")
	}

	// Still live when only the in-memory session knows about it: a record save may be
	// in flight, and the local reference is what covers that window.
	e.dropSessionRecord(t)
	e.settle(t)
	if !e.holdPresent(t, holdSubject) || !e.claimPresent(t, holdSubject) {
		t.Fatal("the sweep destroyed state a local in-memory session still referenced")
	}
}

// TestOrphanSweepHoldDiesBeforeItsFence is the mutation-sensitive statement of
// invariant 10, and the sole licence for ever retiring a `discarded` marker.
//
// A releaser that preloaded a hold record and resumes after the sweep re-takes a
// create-only claim against the now-empty subject. What decides whether it delivers
// is its mandatory authoritative re-read: with the hold destroyed first it reads
// Gone and answers the client; with the marker removed while the hold survived it
// reads Held and fans the message out a second time, out of a session that no
// longer exists.
func TestOrphanSweepHoldDiesBeforeItsFence(t *testing.T) {
	t.Run("sweep order: the stalled releaser delivers nothing", func(t *testing.T) {
		e := testNewOrphanEnv(t, "fence-order-ok")
		e.setGraces(t, 0, 0)
		gen := nuid.Next()
		holdSubject := e.hold(t, 1, gen, "payload")
		// What a releaser paused between loading the hold and delivering it holds.
		_, preloadedSeq, err := e.asm.jsa.loadQoS2Record(holdSubject)
		if err != nil {
			t.Fatalf("preload the hold: %v", err)
		}
		e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node1"))
		e.forget(t)

		e.settle(t)
		if e.holdPresent(t, holdSubject) || e.claimPresent(t, holdSubject) {
			t.Fatal("the aged discarded pair was not reconciled")
		}
		// The releaser wakes: its claim succeeds against the empty subject...
		if _, err := e.asm.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(holdSubject), 0,
			mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "node2")); err != nil {
			t.Fatalf("the stalled releaser could not re-claim: %v", err)
		}
		// ...and its authoritative re-read refuses to authorize a delivery.
		st, _, err := e.asm.verifyQoS2HoldForRelease(holdSubject, gen, preloadedSeq)
		if err != nil {
			t.Fatalf("verifyQoS2HoldForRelease: %v", err)
		}
		if st != mqttQoS2VerifyGone {
			t.Fatalf("the stalled releaser saw %v; only Gone keeps it from delivering", st)
		}
	})

	t.Run("mutation: reaping the fence first re-exposes the payload", func(t *testing.T) {
		e := testNewOrphanEnv(t, "fence-order-broken")
		e.setGraces(t, 0, 0)
		gen := nuid.Next()
		holdSubject := e.hold(t, 1, gen, "payload")
		_, preloadedSeq, err := e.asm.jsa.loadQoS2Record(holdSubject)
		if err != nil {
			t.Fatalf("preload the hold: %v", err)
		}
		claimSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node1"))
		e.forget(t)

		// The forbidden order, performed by hand: the fence goes first.
		if st, err := e.asm.releaseQoS2Record(claimSeq); st != mqttQoS2Released {
			t.Fatalf("removing the marker: %v", err)
		}
		if _, err := e.asm.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(holdSubject), 0,
			mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "node2")); err != nil {
			t.Fatalf("the stalled releaser could not re-claim: %v", err)
		}
		st, _, err := e.asm.verifyQoS2HoldForRelease(holdSubject, gen, preloadedSeq)
		if err != nil {
			t.Fatalf("verifyQoS2HoldForRelease: %v", err)
		}
		if st != mqttQoS2VerifyHeld {
			t.Fatalf("this mutation is supposed to re-authorize a delivery; verify returned %v. "+
				"If the protocol changed so that it no longer does, the ordering test above is no longer proving anything.", st)
		}
	})
}

// TestOrphanSweepClaimReapTargetsTheExactSequence pins that a pass reasoning about
// one claim cannot delete the record that replaced it — the reused-packet-identifier
// hazard scope rule 2 exists for.
func TestOrphanSweepClaimReapTargetsTheExactSequence(t *testing.T) {
	e := testNewOrphanEnv(t, "exact-sequence")
	e.setGraces(t, 0, 0)
	gen := nuid.Next()
	holdSubject := e.hold(t, 1, gen, "payload")
	stale := mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node1")
	staleSeq := e.putClaim(t, holdSubject, stale)
	e.forget(t)

	// Between the sweep's read and its delete, the exchange completes and a LATER one
	// claims the same identifier.
	if err := e.asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, staleSeq, true); err != nil {
		t.Fatalf("removing the first claim: %v", err)
	}
	newerGen := nuid.Next()
	newerSeq := e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, newerGen, "node2"))
	if newerSeq == staleSeq {
		t.Fatal("this test needs the replacement claim to sit at a different sequence")
	}

	if e.asm.qos2ClaimUnchanged(mqttQoS2ClaimSubjectFor(holdSubject), stale, staleSeq) {
		t.Fatal("a replacement claim was accepted as the record the sweep reasoned about")
	}
	cur, smsg, err := e.asm.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil || smsg == nil || cur == nil || cur.Gen != newerGen {
		t.Fatalf("the later exchange's claim must be untouched, got %+v", cur)
	}
}

// TestOrphanSweepLoadsACounterpartTheBatchMissed pins that the bounded walk is
// DISCOVERY, never proof.
//
// With a budget that cannot fit both records of a pair, a pass sees only one of
// them. Reading "the batch contained no claim" as "there is no claim" would purge a
// payload with nothing fencing it. Every disposition therefore re-reads both
// subjects directly, so the pair still reconciles correctly in that pass.
func TestOrphanSweepLoadsACounterpartTheBatchMissed(t *testing.T) {
	e := testNewOrphanEnv(t, "split-batch")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "node1"))
	e.forget(t)

	// One record per pass: whichever half the walk lands on, the other must be
	// fetched directly before anything is destroyed.
	e.setRecordLimit(t, 1)
	e.asm.mu.Lock()
	e.asm.testQoS2SweepSeed = func(firstSeq, _ uint64) uint64 { return firstSeq }
	e.asm.mu.Unlock()

	if cands := testCollectCandidateSubjects(t, e); len(cands) != 1 {
		t.Fatalf("this test needs a budget that splits the pair, got %d candidate(s)", len(cands))
	}
	// A one-record budget means a given record is re-observed only once the cursor
	// wraps back to it, so quarantine needs more than the usual two passes here.
	for i := 0; i < 6; i++ {
		e.sweepInbound(t)
	}
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the pair was not reconciled from a batch that contained only one of its records")
	}
	if e.claimPresent(t, holdSubject) {
		t.Fatal("the claim survived once its hold was gone")
	}
}

// TestOrphanSweepRetainsUnattributableInboundState pins that a subject the parser
// cannot attribute is left exactly where it is. It has no provable owner, and
// guessing one means guessing about somebody's payload.
func TestOrphanSweepRetainsUnattributableInboundState(t *testing.T) {
	e := testNewOrphanEnv(t, "unattributable")
	e.setGraces(t, 0, 0)
	bad := mqttQoS2IncomingMsgsStreamSubjectPrefix + "nothash.short.0"
	if _, err := e.asm.jsa.storeMsg(bad, 0, []byte("payload")); err != nil {
		t.Fatalf("store an unattributable record: %v", err)
	}
	e.forget(t)

	e.settle(t)
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, bad)
	if err != nil || smsg == nil {
		t.Fatalf("an unattributable record must be retained, got %v (err %v)", smsg, err)
	}
}

// ---------------------------------------------------------------------------
// The session-reference snapshot fails closed
// ---------------------------------------------------------------------------

// TestOrphanSweepReferenceSnapshotFailsClosed pins the FATAL class: a record whose
// IDENTITY cannot be established hides references in EVERY class — the inbound and
// PUBREL references are derived from the id hash and the generation — so it aborts
// the whole destructive pass. A hidden reference is a deleted live resource.
//
// A record whose identity is sound but whose AFFINE PAIR does not validate is a
// different case and is not here: it hides affine references only, so it disables
// the affine phases alone. See
// TestOrphanSweepAffinePairFailureSkipsOnlyTheAffinePhases.
func TestOrphanSweepReferenceSnapshotFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, e *testOrphanEnv)
	}{
		{"undecodable record", func(t *testing.T, e *testOrphanEnv) {
			subject := mqttSessStreamSubjectPrefix + e.asm.domainTk + "aaaabbbb"
			if _, err := e.asm.jsa.storeMsg(subject, 0, []byte("{not json")); err != nil {
				t.Fatalf("store: %v", err)
			}
		}},
		{"record with no incarnation generation", func(t *testing.T, e *testOrphanEnv) {
			testStoreRawSession(t, e, "legacy", &mqttPersistedSession{ID: "legacy"})
		}},
		{"record with a malformed incarnation generation", func(t *testing.T, e *testOrphanEnv) {
			testStoreRawSession(t, e, "badgen", &mqttPersistedSession{ID: "badgen", Gen: "short"})
		}},
		{"record whose client ID does not hash back", func(t *testing.T, e *testOrphanEnv) {
			testStoreRawSession(t, e, "impostor", &mqttPersistedSession{ID: "somebody-else", Gen: nuid.Next()})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewOrphanEnv(t, "refs-"+strings.ReplaceAll(tc.name, " ", "-"))
			holdSubject := e.hold(t, 1, nuid.Next(), "payload")
			e.forget(t)
			tc.write(t, e)

			if _, _, err := e.asm.buildSessionRefs(); err == nil {
				t.Fatal("the reference snapshot reported success on a record whose identity it could not establish")
			}
			// And the pass that depends on it destroys nothing.
			e.asm.sweepOrphanedQoS2State(e.s)
			if !e.holdPresent(t, holdSubject) {
				t.Fatal("an orphan was destroyed on an incomplete reference snapshot")
			}
		})
	}
}

func testStoreRawSession(t *testing.T, e *testOrphanEnv, clientID string, ps *mqttPersistedSession) {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash(clientID)
	b := testMustJSON(t, ps)
	if _, err := e.asm.jsa.storeMsg(subject, 0, b); err != nil {
		t.Fatalf("store session record: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

// TestOrphanSweepCursorSeedsOnceThenAdvancesSequentially pins the bounded walk's
// scheduling: a random seed per process (so a restart loop does not rescan the same
// prefix forever) and sequential advance from there (so coverage inside one process
// is linear rather than a coupon collector's).
func TestOrphanSweepCursorSeedsOnceThenAdvancesSequentially(t *testing.T) {
	e := testNewOrphanEnv(t, "cursor")
	e.setGraces(t, 0, 0)
	const n = 8
	for pi := uint16(1); pi <= n; pi++ {
		e.hold(t, pi, nuid.Next(), "payload")
	}
	e.forget(t)

	// A deterministic seed, and a budget that forces several passes. Reseeding is
	// explicit: the startup sweep has already run (and seeded the cursor against an
	// empty stream), and this test is about what happens from a known start.
	e.asm.mu.Lock()
	e.asm.testQoS2SweepSeed = func(firstSeq, _ uint64) uint64 { return firstSeq }
	e.asm.qos2SweepSeeded = false
	e.asm.mu.Unlock()
	e.setRecordLimit(t, 3)

	first := testCollectCandidateSubjects(t, e)
	if len(first) != 3 {
		t.Fatalf("the record budget was not enforced: %d candidate(s)", len(first))
	}
	second := testCollectCandidateSubjects(t, e)
	if len(second) != 3 {
		t.Fatalf("the second pass returned %d candidate(s), want 3", len(second))
	}
	for subject := range second {
		if first[subject] {
			t.Fatalf("pass 2 revisited %q instead of resuming where pass 1 stopped", subject)
		}
	}

	// Keep going until the walk wraps and starts repeating: coverage must be complete.
	seen := map[string]bool{}
	for k := range first {
		seen[k] = true
	}
	for k := range second {
		seen[k] = true
	}
	for i := 0; i < 6 && len(seen) < n; i++ {
		for k := range testCollectCandidateSubjects(t, e) {
			seen[k] = true
		}
	}
	if len(seen) != n {
		t.Fatalf("sequential passes covered %d of %d hold subjects", len(seen), n)
	}
}

// TestOrphanSweepByteBudgetStopsAfterTheRecordThatCrossesIt pins the bound the byte
// budget actually claims.
//
// The message-get API returns whole records, so a hold's PUBLISH payload crosses
// the wire just to discover who owns it. The budget is checked AFTER a record is
// accounted, so it can be exceeded by at most one record — never by a whole pass.
func TestOrphanSweepByteBudgetStopsAfterTheRecordThatCrossesIt(t *testing.T) {
	e := testNewOrphanEnv(t, "byte-budget")
	e.setGraces(t, 0, 0)
	const n = 6
	const payload = 4096
	for pi := uint16(1); pi <= n; pi++ {
		e.hold(t, pi, nuid.Next(), strings.Repeat("x", payload))
	}
	e.forget(t)

	e.asm.mu.Lock()
	e.asm.testQoS2SweepSeed = func(firstSeq, _ uint64) uint64 { return firstSeq }
	e.asm.mu.Unlock()

	// A budget smaller than one record: the walk must still make progress (one
	// record), and must stop immediately after it.
	e.asm.mu.Lock()
	e.asm.qos2SweepSeeded = false
	e.asm.mu.Unlock()
	e.setByteLimit(t, 1)
	if got := len(testCollectCandidateSubjects(t, e)); got != 1 {
		t.Fatalf("a sub-record byte budget fetched %d record(s), want exactly 1 "+
			"(no progress at all would stall the sweep; more than one is an unbounded pass)", got)
	}

	// A budget that sits between the second and third record stops after the third —
	// the one that crosses it. (A record is its payload plus subject and headers, so
	// the limit is set at two and a half payloads rather than exactly two.)
	e.asm.mu.Lock()
	e.asm.qos2SweepSeeded = false
	e.asm.mu.Unlock()
	e.setByteLimit(t, payload*5/2)
	if got := len(testCollectCandidateSubjects(t, e)); got != 3 {
		t.Fatalf("a two-record byte budget fetched %d record(s), want 3 "+
			"(the budget is exceeded by at most the one record that crosses it)", got)
	}
}

// TestOrphanSweepCursorNormalizesWhenRetentionAdvances pins the one case that is
// ordinary progress rather than an unusable cursor: retention moving FirstSeq past
// us walks forward from the new floor, and does NOT reseed.
func TestOrphanSweepCursorNormalizesWhenRetentionAdvances(t *testing.T) {
	e := testNewOrphanEnv(t, "cursor-retention")
	e.setGraces(t, 0, 0)
	var seeds int
	e.asm.mu.Lock()
	e.asm.testQoS2SweepSeed = func(firstSeq, _ uint64) uint64 { seeds++; return firstSeq }
	e.asm.qos2SweepCursor, e.asm.qos2SweepSeeded = 5, true
	e.asm.mu.Unlock()

	if got := e.asm.qos2SweepStart(100, 200); got != 100 {
		t.Fatalf("a cursor behind FirstSeq must normalize forward to it, got %d", got)
	}
	if seeds != 0 {
		t.Fatal("retention advancing FirstSeq must not reseed the cursor")
	}

	// A cursor the stream can no longer contain at all IS unusable, and reseeds.
	e.asm.mu.Lock()
	e.asm.qos2SweepCursor = 9999
	e.asm.mu.Unlock()
	if got := e.asm.qos2SweepStart(100, 200); got != 100 || seeds != 1 {
		t.Fatalf("an out-of-range cursor must reseed (start %d, seeds %d)", got, seeds)
	}
}

// ---------------------------------------------------------------------------
// The sweep-only affine validator, and its boundary
// ---------------------------------------------------------------------------

func testValidQ2AffClaim(idHash, gen, groupDur string, sseq uint64) *mqttQ2AffClaim {
	hash := mqttQ2AffClaimHash(groupDur, sseq)
	_, _, durable := mqttQ2AffineNames(idHash, gen)
	return &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: sseq,
		ClaimHash:   hash,
		CopySubject: mqttQ2AffCopySubject(idHash, gen, hash),
		AffDurable:  durable,
		Phase:       mqttQ2AffClaimed,
	}
}

func TestOrphanSweepAffineValidatorChecksEveryDerivedName(t *testing.T) {
	t.Parallel()
	idHash, gen, groupDur := getHash("winner"), nuid.Next(), "$MQTT_SHARED_abc"
	const sseq = uint64(42)
	subject := mqttQ2AffClaimSubject(groupDur, sseq)

	if err := validateOrphanQ2AffClaim(subject, testValidQ2AffClaim(idHash, gen, groupDur, sseq)); err != nil {
		t.Fatalf("a well-formed claim must validate: %v", err)
	}
	for _, tc := range []struct {
		name    string
		subject string
		mutate  func(c *mqttQ2AffClaim)
	}{
		{"wrong subject", mqttQ2AffClaimSubject(groupDur, sseq+1), nil},
		{"wrong claim hash", subject, func(c *mqttQ2AffClaim) { c.ClaimHash = "0000000000000000" }},
		{"wrong copy subject", subject, func(c *mqttQ2AffClaim) { c.CopySubject = "$MQTT.out.q2.elsewhere" }},
		{"wrong affine durable", subject, func(c *mqttQ2AffClaim) { c.AffDurable = "$MQTT_Q2AFF_elsewhere" }},
		{"unknown phase", subject, func(c *mqttQ2AffClaim) { c.Phase = "teleported" }},
		{"both AffineDone and Lost", subject, func(c *mqttQ2AffClaim) { c.AffineDone, c.Lost = true, true }},
		{"malformed winner hash", subject, func(c *mqttQ2AffClaim) { c.Winner = "nothash" }},
	} {
		c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
		if tc.mutate != nil {
			tc.mutate(c)
		}
		if err := validateOrphanQ2AffClaim(tc.subject, c); err == nil {
			t.Fatalf("%s: the sweep validator accepted a claim it cannot trust", tc.name)
		}
	}
}

// TestOrphanSweepAffineValidatorDoesNotWidenTheSharedFloor is the boundary test.
//
// The derived-name checks live in the sweep and NOT in mqttQ2AffClaimIdentityOK,
// because that predicate gates session resume and clear(): one failure anywhere in
// its account-wide scan aborts the caller, and for clear() that fails the discard,
// strands the teardown marker and locks the client ID out of its next CONNECT. A
// derived-name mismatch must cost a deferred sweep pass, never that.
func TestOrphanSweepAffineValidatorDoesNotWidenTheSharedFloor(t *testing.T) {
	e := testNewOrphanEnv(t, "validator-boundary")
	e.setGraces(t, 0, 0)
	idHash, gen, groupDur := getHash("winner"), nuid.Next(), "$MQTT_SHARED_abc"
	const sseq = uint64(42)

	// Passes the shared attribution floor; fails a sweep-only derivation.
	c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
	c.AffDurable = "$MQTT_Q2AFF_somewhere_else"
	if !mqttQ2AffClaimIdentityOK(c) {
		t.Fatal("this test needs a claim that the SHARED floor accepts")
	}
	if _, err := e.asm.storeQ2AffClaimCAS(mqttQ2AffClaimSubject(groupDur, sseq), 0, c); err != nil {
		t.Fatalf("store claim: %v", err)
	}

	// The shared scanner still walks it, so resume and teardown are unaffected.
	if err := e.asm.scanQ2AffClaims(func(string, *mqttQ2AffClaim, uint64) error { return nil }); err != nil {
		t.Fatalf("the shared scan must not fail on a derived-name mismatch: %v", err)
	}
	if err := e.asm.discardQ2AffClaimsFor(idHash, gen); err != nil {
		t.Fatalf("a session teardown must not fail on a derived-name mismatch: %v", err)
	}

	// The sweep inventory does reject it, which protects every affine consumer for
	// this pass — an inventory that is not complete authorizes no deletion at all,
	// including of consumers that have nothing to do with the malformed claim.
	if _, err := e.asm.collectQ2AffInventory(); err == nil {
		t.Fatal("the sweep inventory accepted a claim whose derived names do not match")
	}
	orphanHash, orphanGen := getHash("bystander"), mqttNewQ2AffGen()
	durable := testCreateAffineConsumer(t, e, orphanHash, orphanGen)
	e.setGraces(t, 0, 0)
	e.asm.sweepOrphanedQoS2State(e.s)
	if !e.consumerExists(t, durable) {
		t.Fatal("an unrelated orphan consumer was deleted on the strength of an inventory that failed")
	}
}

// TestOrphanSweepKeepsADiscardedAssignmentWhoseSourceCouldRedeliver pins the rule
// that keeps [MQTT-4.8.2-6] intact through reconciliation.
//
// Discarding an assignment does not imply the group ack committed. If the source
// delivery can still be re-offered, deleting the claim would let that redelivery
// elect a fresh winner — handing a started delivery to a second client, which is
// the exact violation the assignment record exists to prevent.
func TestOrphanSweepKeepsADiscardedAssignmentWhoseSourceCouldRedeliver(t *testing.T) {
	e := testNewOrphanEnv(t, "source-redeliverable")
	e.setGraces(t, 0, 0)
	idHash, gen := getHash("redeliver-winner"), mqttNewQ2AffGen()
	groupDur := mqttSharedConsumerDurablePrefix + "REDELIVER"

	// A LIVE group durable, so the group-wide proof of unofferability does not apply,
	// and a real source message it still holds, so the per-message one does not either.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable: groupDur, FilterSubject: mqttStreamSubjectPrefix + "redeliver.topic",
			DeliverSubject: mqttSharedSubDeliverPrefix + "redeliver", DeliverGroup: "g",
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 64,
		},
	}); err != nil {
		t.Fatalf("create group durable: %v", err)
	}
	resp, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+"redeliver.topic", 0, []byte("source"))
	if err != nil {
		t.Fatalf("store the source message: %v", err)
	}
	sseq := resp.Sequence

	subject := mqttQ2AffClaimSubject(groupDur, sseq)
	c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
	c.Phase = mqttQ2AffMaterialized
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
		t.Fatalf("store claim: %v", err)
	}

	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	budget := 16
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)

	got, _, err := e.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if got == nil {
		t.Fatal("an assignment whose source could still be re-offered was deleted; " +
			"a redelivery can now elect a second winner for a started delivery")
	}
	if !got.Discarded {
		t.Fatalf("the assignment was not marked discarded: %+v", got)
	}
	if got.SourceAcked {
		t.Fatal("reconciliation asserted a group ack it never proved")
	}
}

// ---------------------------------------------------------------------------
// Affine assignments
// ---------------------------------------------------------------------------

// TestOrphanSweepDiscardsAbsentWinnerWithoutErasingProvenFacts pins that the
// reconciliation only ever ADDS to what a claim proves, and that it never
// manufactures the affine completion the write order of AffineDone exists to
// distinguish.
func TestOrphanSweepDiscardsAbsentWinnerWithoutErasingProvenFacts(t *testing.T) {
	e := testNewOrphanEnv(t, "absent-winner")
	e.setGraces(t, 0, 0)
	idHash, gen, groupDur := getHash("gone-winner"), nuid.Next(), "$MQTT_SHARED_absent"
	const sseq = uint64(7)
	subject := mqttQ2AffClaimSubject(groupDur, sseq)

	c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
	c.Phase, c.SourceAcked = mqttQ2AffMaterialized, true
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
		t.Fatalf("store claim: %v", err)
	}

	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	budget := 16
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)

	got, _, err := e.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if got == nil {
		// Reaped is a legitimate outcome once both sides are proven; what must never
		// happen is AffineDone appearing out of nowhere. Assert the surviving-record
		// case only.
		return
	}
	if !got.Discarded {
		t.Fatal("an absent winner's assignment was not marked discarded")
	}
	if !got.SourceAcked {
		t.Fatal("the reconciliation erased a previously proven SourceAcked")
	}
	if got.AffineDone {
		t.Fatal("the reconciliation manufactured affine completion for a delivery that never happened")
	}
	if got.Lost {
		t.Fatal("session loss was recorded as message loss")
	}
}

// TestOrphanSweepReapsWhenTheSourceCanNoLongerBeOffered is the other side of
// TestOrphanSweepKeepsADiscardedAssignmentWhoseSourceCouldRedeliver.
//
// `SourceAcked` asserts exactly one thing: the group delivery can never be
// re-offered. An absent group durable proves that for the whole group at once, so
// the existing proof path may complete it and reap a claim that is otherwise
// terminal. Without this, every claim whose broker died after the ack committed but
// before the CAS recorded it would be retained forever.
func TestOrphanSweepReapsWhenTheSourceCanNoLongerBeOffered(t *testing.T) {
	e := testNewOrphanEnv(t, "source-unofferable")
	e.setGraces(t, 0, 0)
	idHash, gen := getHash("unofferable-winner"), mqttNewQ2AffGen()
	// A group durable that does not exist: authoritatively absent, so no redelivery
	// can ever come from it.
	groupDur := mqttSharedConsumerDurablePrefix + "NEVEREXISTED"
	const sseq = uint64(77)
	subject := mqttQ2AffClaimSubject(groupDur, sseq)

	c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
	c.Phase, c.AffineDone = mqttQ2AffMaterialized, true
	if c.SourceAcked {
		t.Fatal("this test needs a claim whose source ack was never recorded")
	}
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
		t.Fatalf("store claim: %v", err)
	}

	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	budget := 16
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)

	got, _, err := e.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if got != nil {
		t.Fatalf("an assignment whose group durable is authoritatively gone was retained forever: %+v", got)
	}
}

// TestOrphanSweepNeverReapsALostAssignment pins invariant 8. The guard is the
// EXISTING reapable() predicate, and the sweep's job is simply never to acquire a
// deletion path that bypasses it.
func TestOrphanSweepNeverReapsALostAssignment(t *testing.T) {
	e := testNewOrphanEnv(t, "lost-assignment")
	e.setGraces(t, 0, 0)
	idHash, gen, groupDur := getHash("lost-winner"), nuid.Next(), "$MQTT_SHARED_lost"
	const sseq = uint64(11)
	subject := mqttQ2AffClaimSubject(groupDur, sseq)

	c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
	c.Phase, c.SourceAcked, c.Discarded, c.Lost = mqttQ2AffMaterialized, true, true, true
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
		t.Fatalf("store claim: %v", err)
	}
	if c.reapable(true) {
		t.Fatal("this test rests on reapable() refusing a Lost claim; it no longer does")
	}

	e.asm.sweepOrphanedQoS2State(e.s)
	got, _, err := e.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if got == nil {
		t.Fatal("the only durable evidence of a lost QoS 2 delivery was reaped")
	}
	if !got.Lost {
		t.Fatal("the Lost flag was cleared")
	}
}

// TestOrphanSweepDefersOnAnIndeterminateWinner pins that "I could not tell" never
// becomes "it is gone". A record carrying the claim's own affine generation defers
// whether it is live or merely uninterpretable.
func TestOrphanSweepDefersOnAnIndeterminateWinner(t *testing.T) {
	e := testNewOrphanEnv(t, "indeterminate-winner")
	e.setGraces(t, 0, 0)
	groupDur := "$MQTT_SHARED_defer"
	const sseq = uint64(3)

	for _, tc := range []struct {
		name string
		load func(cidHash string) (*mqttPersistedSession, uint64, error)
	}{
		{"backend error", func(string) (*mqttPersistedSession, uint64, error) {
			return nil, 0, fmt.Errorf("injected")
		}},
		{"matching affine generation with no validated config", func(cidHash string) (*mqttPersistedSession, uint64, error) {
			return &mqttPersistedSession{ID: "x", Gen: nuid.Next(), Q2AffGen: "GEN-UNDER-TEST"}, 1, nil
		}},
		{"malformed record generation", func(string) (*mqttPersistedSession, uint64, error) {
			return &mqttPersistedSession{ID: "x", Gen: "short", Q2AffGen: nuid.Next()}, 1, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idHash := getHash("defer-" + tc.name)
			gen := nuid.Next()
			subject := mqttQ2AffClaimSubject(groupDur, sseq)
			c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
			if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
				t.Fatalf("store claim: %v", err)
			}
			t.Cleanup(func() {
				_, seq, _ := e.asm.loadQ2AffClaim(subject)
				if seq != 0 {
					_ = e.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true)
				}
			})

			load := tc.load
			e.asm.mu.Lock()
			e.asm.testSessionLoad = func(cidHash string) (*mqttPersistedSession, uint64, error) {
				ps, seq, err := load(cidHash)
				if ps != nil && ps.Q2AffGen == "GEN-UNDER-TEST" {
					ps.Q2AffGen = gen
				}
				return ps, seq, err
			}
			e.asm.mu.Unlock()
			t.Cleanup(func() {
				e.asm.mu.Lock()
				e.asm.testSessionLoad = nil
				e.asm.mu.Unlock()
			})

			e2 := &mqttQ2AffInvEntry{subject: subject, claim: c, stored: time.Now()}
			if got := e.asm.discardAbsentWinnerClaim(e.s, e2); got != mqttQoS2SweepDeferred {
				t.Fatalf("outcome = %v, want deferred", got)
			}
			cur, _, err := e.asm.loadQ2AffClaim(subject)
			if err != nil {
				t.Fatalf("load claim: %v", err)
			}
			if cur == nil || cur.Discarded {
				t.Fatalf("an indeterminate winner authorized a mutation: %+v", cur)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Affine consumers
// ---------------------------------------------------------------------------

// testCreateAffineConsumer creates a real affine consumer on $MQTT_out for an
// identity, so consumer discovery and deletion run against JetStream rather than a
// fake.
func testCreateAffineConsumer(t *testing.T, e *testOrphanEnv, idHash, gen string) string {
	t.Helper()
	filter, deliver, durable := mqttQ2AffineNames(idHash, gen)
	ccr := &CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			Durable:        durable,
			FilterSubject:  filter,
			DeliverSubject: deliver,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			MaxAckPending:  1024,
		},
	}
	if _, err := e.asm.jsa.createDurableConsumer(ccr); err != nil {
		t.Fatalf("create affine consumer %q: %v", durable, err)
	}
	return durable
}

func (e *testOrphanEnv) consumerExists(t *testing.T, durable string) bool {
	t.Helper()
	ci, err := e.asm.jsa.lookupConsumer(mqttOutStreamName, durable)
	if err != nil {
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			t.Fatalf("lookup consumer %q: %v", durable, err)
		}
		return false
	}
	return ci != nil
}

func (e *testOrphanEnv) sweepAffineConsumers(t *testing.T, budget int) {
	t.Helper()
	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	cons, err := e.asm.jsa.consumerList(mqttOutStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	// Same order as the real pass: assignments are reconciled — and so observed —
	// before any consumer is considered for deletion.
	e.asm.reconcileOrphanQ2AffClaims(e.s, inv, refs, &budget)
	e.asm.deleteOrphanQ2AffConsumers(e.s, inv, cons, refs, &budget)
}

// TestOrphanSweepMarksAssignmentsDiscardedBeforeDeletingTheirConsumer is the
// central acceptance condition of the affine half.
//
// $MQTT_out is interest-retained, so deleting the consumer drops the copies it
// still holds. Doing that while a claim still described a live delivery would turn
// a session discard MQTT 5.0 [4.8.2] permits into a recorded Lost message.
func TestOrphanSweepMarksAssignmentsDiscardedBeforeDeletingTheirConsumer(t *testing.T) {
	e := testNewOrphanEnv(t, "consumer-ordering")
	e.setGraces(t, 0, 0)
	idHash, gen := getHash("aff-orphan"), mqttNewQ2AffGen()
	groupDur := "$MQTT_SHARED_ordering"
	const sseq = uint64(21)
	durable := testCreateAffineConsumer(t, e, idHash, gen)

	subject := mqttQ2AffClaimSubject(groupDur, sseq)
	c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
	c.Phase = mqttQ2AffMaterialized
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
		t.Fatalf("store claim: %v", err)
	}

	e.settleAffineConsumers(t, 16)

	if e.consumerExists(t, durable) {
		t.Fatal("an unreferenced, fully reconciled affine consumer was not deleted")
	}
	// The claim reached a terminal state before its copies could be dropped: either
	// it is still there carrying Discarded, or the two-sided predicate reaped it.
	// What must never be true is the consumer being gone while the claim still
	// described a live, undiscarded delivery.
	cur, _, err := e.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if cur != nil && !cur.Discarded {
		t.Fatal("the affine consumer was deleted while its assignment was not durably discarded, " +
			"which drops its copies with the claim still describing a live delivery")
	}
}

// TestOrphanSweepKeepsTheConsumerWhenTheDiscardCASFails is the mutation-sensitive
// half of the ordering above: if committing Discarded cannot be resolved, the
// consumer must survive. Deleting it anyway is exactly the step that would convert
// a permitted session discard into a recorded Lost message.
func TestOrphanSweepKeepsTheConsumerWhenTheDiscardCASFails(t *testing.T) {
	e := testNewOrphanEnv(t, "discard-cas-fails")
	e.setGraces(t, 0, 0)
	idHash, gen := getHash("cas-fail"), mqttNewQ2AffGen()
	groupDur := "$MQTT_SHARED_casfail"
	const sseq = uint64(22)
	durable := testCreateAffineConsumer(t, e, idHash, gen)

	subject := mqttQ2AffClaimSubject(groupDur, sseq)
	c := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
	c.Phase = mqttQ2AffMaterialized
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
		t.Fatalf("store claim: %v", err)
	}

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimStore = func(string, uint64, *mqttQ2AffClaim) (uint64, error) {
		return 0, fmt.Errorf("injected")
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQ2AffClaimStore = nil
		e.asm.mu.Unlock()
	})

	e.settleAffineConsumers(t, 16)
	if !e.consumerExists(t, durable) {
		t.Fatal("the affine consumer was deleted although its assignment could not be committed discarded")
	}
}

// TestOrphanSweepRetainsAConsumerItCannotFullyReconcile covers the three ways step
// 2-4 can come up short. Each must leave the consumer for a later pass rather than
// deleting it on a partial result.
func TestOrphanSweepRetainsAConsumerItCannotFullyReconcile(t *testing.T) {
	groupDur := "$MQTT_SHARED_retain"

	t.Run("a young assignment protects an otherwise-old consumer", func(t *testing.T) {
		// Store times come from JetStream, so the only way to give the consumer and its
		// assignment genuinely different ages is to create them at different times
		// against a short grace.
		e := testNewOrphanEnv(t, "retain-young")
		e.setGraces(t, 200*time.Millisecond, 200*time.Millisecond)
		idHash, gen := getHash("young-assign"), mqttNewQ2AffGen()
		durable := testCreateAffineConsumer(t, e, idHash, gen)
		time.Sleep(400 * time.Millisecond)
		subject := mqttQ2AffClaimSubject(groupDur, 31)
		if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, testValidQ2AffClaim(idHash, gen, groupDur, 31)); err != nil {
			t.Fatalf("store claim: %v", err)
		}
		// The consumer is past the grace; the assignment it owns is not.
		e.settleAffineConsumers(t, 16)
		if !e.consumerExists(t, durable) {
			t.Fatal("a consumer was deleted while it still owned an assignment younger than the grace")
		}
	})

	t.Run("an exhausted mutation budget retains the consumer", func(t *testing.T) {
		e := testNewOrphanEnv(t, "retain-budget")
		e.setGraces(t, 0, 0)
		idHash, gen := getHash("budget-assign"), mqttNewQ2AffGen()
		durable := testCreateAffineConsumer(t, e, idHash, gen)
		subject := mqttQ2AffClaimSubject(groupDur, 32)
		if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, testValidQ2AffClaim(idHash, gen, groupDur, 32)); err != nil {
			t.Fatalf("store claim: %v", err)
		}
		e.settleAffineConsumers(t, 0)
		if !e.consumerExists(t, durable) {
			t.Fatal("a consumer was deleted with its assignments left unreconciled by an exhausted budget")
		}
	})

	t.Run("a session that reappears keeps its consumer", func(t *testing.T) {
		e := testNewOrphanEnv(t, "retain-reappear")
		e.setGraces(t, 0, 0)
		idHash, gen := getHash("reappear"), mqttNewQ2AffGen()
		durable := testCreateAffineConsumer(t, e, idHash, gen)

		e.asm.mu.Lock()
		e.asm.testSessionLoad = func(string) (*mqttPersistedSession, uint64, error) {
			return &mqttPersistedSession{ID: "reappear", Gen: nuid.Next(), Q2AffGen: gen}, 1, nil
		}
		e.asm.mu.Unlock()
		t.Cleanup(func() {
			e.asm.mu.Lock()
			e.asm.testSessionLoad = nil
			e.asm.mu.Unlock()
		})

		e.settleAffineConsumers(t, 16)
		if !e.consumerExists(t, durable) {
			t.Fatal("a consumer was deleted although its session reappeared during final revalidation")
		}
	})
}

// TestOrphanSweepRetainsAConsumerOnALateClaim pins the verification barrier: a
// claim created between the authorizing inventory and the second scan retains the
// consumer, because the first inventory no longer describes the generation.
func TestOrphanSweepRetainsAConsumerOnALateClaim(t *testing.T) {
	e := testNewOrphanEnv(t, "late-claim")
	e.setGraces(t, 0, 0)
	idHash, gen := getHash("late-claim"), mqttNewQ2AffGen()
	groupDur := "$MQTT_SHARED_late"
	durable := testCreateAffineConsumer(t, e, idHash, gen)

	inv, err := e.asm.collectQ2AffInventory()
	if err != nil {
		t.Fatalf("collectQ2AffInventory: %v", err)
	}
	// Between the inventory and the verification scan, a contender elects this
	// generation for a new source message.
	subject := mqttQ2AffClaimSubject(groupDur, 41)
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, testValidQ2AffClaim(idHash, gen, groupDur, 41)); err != nil {
		t.Fatalf("store the late claim: %v", err)
	}

	cons, err := e.asm.jsa.consumerList(mqttOutStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	budget := 16
	e.asm.deleteOrphanQ2AffConsumers(e.s, inv, cons, refs, &budget)
	e.asm.deleteOrphanQ2AffConsumers(e.s, inv, cons, refs, &budget)

	if !e.consumerExists(t, durable) {
		t.Fatal("a consumer was deleted although a claim appeared after the authorizing inventory")
	}
}

// TestOrphanSweepLeavesForeignConsumersAlone pins the durable-prefix boundary. In
// particular $MQTT_PUBREL_* is deliberately out of scope: those hold undelivered
// outbound PUBREL state, so deleting one is not a free leak cleanup.
func TestOrphanSweepLeavesForeignConsumersAlone(t *testing.T) {
	e := testNewOrphanEnv(t, "foreign-consumers")
	e.setGraces(t, 0, 0)
	idHash, gen := getHash("pubrel-owner"), nuid.Next()
	subject, deliver, durable := mqttPubRelNames(idHash, gen)
	ccr := &CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			Durable: durable, FilterSubject: subject, DeliverSubject: deliver,
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 1024,
		},
	}
	if _, err := e.asm.jsa.createDurableConsumer(ccr); err != nil {
		t.Fatalf("create PUBREL consumer: %v", err)
	}
	e.forget(t)

	e.settleAffineConsumers(t, 16)
	if !e.consumerExists(t, durable) {
		t.Fatal("the sweep deleted a $MQTT_PUBREL_* consumer, which is out of scope for this item")
	}
}

// ---------------------------------------------------------------------------
// Teardown marker adoption
// ---------------------------------------------------------------------------

// TestOrphanSweepAdoptsAnAbandonedTeardown pins that a marked record does not pin
// its resources forever, and that a marker inside the grace still does.
func TestOrphanSweepAdoptsAnAbandonedTeardown(t *testing.T) {
	e := testNewOrphanEnv(t, "adopt-teardown")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")

	// Mark the record as a broker that claimed the teardown and then died.
	ps, seq, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil || ps == nil {
		t.Fatalf("loading the session record: %v", err)
	}
	ps.Teardown = &mqttSessTeardown{Origin: "node9", Nonce: nuid.Next(), StartedAt: time.Now().Unix()}
	// The marker's LINEAGE, exactly as claimTeardown stamps it: an identity of its own,
	// parented to the record it claimed. The finisher retires that parent BY NAME, so a
	// marker whose PWID names nothing is a record no teardown could have written — and
	// one whose retirement can never be recorded.
	ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
	subject := mqttSessStreamSubjectPrefix + e.asm.domainTk + e.sess.idHash
	if _, err := e.asm.jsa.storeMsg(subject, 0, testMustJSON(t, ps)); err != nil {
		t.Fatalf("writing the teardown marker: %v", err)
	}
	_ = seq
	e.dropLocalSession()

	// Inside the grace: the marked record still protects everything it names. The
	// reference is the real cluster clock, so the marker is genuinely young.
	e.setGraces(t, time.Hour, time.Hour)
	e.asm.adoptAbandonedTeardowns(e.s)
	cur, _, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil || cur == nil || cur.Teardown == nil {
		t.Fatal("a teardown marker inside the grace was adopted early")
	}
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("resources named by a young marked record were destroyed")
	}

	// Past it: the teardown is finished and the record released.
	e.setGraces(t, 0, 0)
	e.asm.adoptAbandonedTeardowns(e.s)
	e.asm.adoptAbandonedTeardowns(e.s)
	cur, _, err = e.asm.loadFenceSession(e.sess.idHash)
	if err != nil {
		t.Fatalf("loading the session record: %v", err)
	}
	if cur != nil {
		t.Fatalf("an aged abandoned teardown was not finished: %+v", cur.Teardown)
	}
	if e.holdPresent(t, holdSubject) {
		t.Fatal("the adopted teardown did not destroy the retired incarnation's inbound QoS 2 state")
	}
}

// TestOrphanSweepFailedAdoptionKeepsReferencesLive pins the fail-closed half: an
// adoption that could not complete leaves the marked record in place, and the
// reference snapshot then still counts it.
func TestOrphanSweepFailedAdoptionKeepsReferencesLive(t *testing.T) {
	e := testNewOrphanEnv(t, "failed-adoption")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")

	ps, _, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil || ps == nil {
		t.Fatalf("loading the session record: %v", err)
	}
	ps.Teardown = &mqttSessTeardown{Origin: "node9", Nonce: nuid.Next(), StartedAt: time.Now().Unix()}
	// The marker's LINEAGE, exactly as claimTeardown stamps it: an identity of its own,
	// parented to the record it claimed. The finisher retires that parent BY NAME, so a
	// marker whose PWID names nothing is a record no teardown could have written — and
	// one whose retirement can never be recorded.
	ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
	subject := mqttSessStreamSubjectPrefix + e.asm.domainTk + e.sess.idHash
	if _, err := e.asm.jsa.storeMsg(subject, 0, testMustJSON(t, ps)); err != nil {
		t.Fatalf("writing the teardown marker: %v", err)
	}
	e.dropLocalSession()
	e.setGraces(t, 0, 0)

	// A record whose re-read cannot be resolved is never adopted. The seam stays
	// installed across the whole sweep: the point is that a sweep which CANNOT finish
	// the teardown must still treat everything the marker names as referenced.
	e.asm.mu.Lock()
	e.asm.testSessionLoad = func(string) (*mqttPersistedSession, uint64, error) {
		return nil, 0, fmt.Errorf("injected")
	}
	e.asm.mu.Unlock()
	e.asm.sweepOrphanedQoS2State(e.s)
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("resources preserved by an unfinished teardown were classified as orphans")
	}
	e.asm.mu.Lock()
	e.asm.testSessionLoad = nil
	e.asm.mu.Unlock()

	cur, _, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil || cur == nil || cur.Teardown == nil {
		t.Fatal("a failed adoption removed or altered the marker")
	}
	// And the still-marked record is what counted as the reference.
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	if !refs.hasInbound(e.sess.idHash, cur.Gen) {
		t.Fatal("a teardown-marked record stopped counting as a reference after a failed adoption")
	}
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

// TestOrphanSweepRunsAtAccountManagerStartup pins the OTHER call site.
//
// The periodic ticker is the obvious one, but the startup pass is what converges a
// leak left by a broker that died while this one was down — and a broker that
// restarts more often than the tick interval would otherwise never sweep at all. It
// has to be observed through a package-level counter, because a fresh account
// manager starts sweeping before a test can reach into it.
func TestOrphanSweepRunsAtAccountManagerStartup(t *testing.T) {
	before := mqttOrphanSweepRuns.Load()
	testNewOrphanEnv(t, "startup-sweep")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if mqttOrphanSweepRuns.Load() > before {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("creating an account manager did not run the orphan sweep at startup")
}

// TestOrphanSweepRunsFromTheAccountSweepLoop pins that the reconciliation is
// actually reached from the account manager's periodic ticker, not merely callable.
func TestOrphanSweepRunsFromTheAccountSweepLoop(t *testing.T) {
	// The ticker interval is read once, when the account manager starts its sweep
	// goroutine, so it has to be in place before the env exists.
	defer setMQTTSessionExpirySweepInterval(100 * time.Millisecond)()

	e := testNewOrphanEnv(t, "sweep-loop")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "payload")
	e.forget(t)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !e.holdPresent(t, holdSubject) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the periodic account sweep never reconciled an orphaned inbound QoS 2 hold")
}

// ---------------------------------------------------------------------------
// Retirement condemnation and the PUBREL orphan class
// ---------------------------------------------------------------------------

// quarantined reports whether a key currently holds an observation slot. Asserted
// directly where SURVIVAL cannot discriminate: a candidate wrongly admitted here can
// still be rescued by an act-time re-read, which would mask a dropped reference class.
func (e *testOrphanEnv) quarantined(key string) bool {
	q := e.asm.quarantine()
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.entries[key] != nil
}

func (e *testOrphanEnv) retirement(t *testing.T, idHash string) (*mqttGenRetirement, mqttRetirementStatus) {
	t.Helper()
	r, _, status := e.asm.jsa.loadGenRetirementRecord(e.asm.domainTk, idHash)
	return r, status
}

func (e *testOrphanEnv) sessRecord(t *testing.T, idHash string) *mqttPersistedSession {
	t.Helper()
	ps, _, err := e.asm.loadFenceSession(idHash)
	if err != nil {
		t.Fatalf("loading the session record of %q: %v", idHash, err)
	}
	return ps
}

// testCreatePubRelConsumer provisions a session incarnation's PUBREL consumer with
// exactly the three names mqttPubRelNames derives, as the broker does.
func testCreatePubRelConsumer(t *testing.T, e *testOrphanEnv, idHash, gen string) string {
	t.Helper()
	subject, deliver, durable := mqttPubRelNames(idHash, gen)
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			Durable:        durable,
			FilterSubject:  subject,
			DeliverSubject: deliver,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			MaxAckPending:  1024,
		},
	}); err != nil {
		t.Fatalf("create PUBREL consumer %q: %v", durable, err)
	}
	return durable
}

// sweepPubRelConsumers runs one PUBREL pass in production's order: candidates are
// listed, then the authoritative reference snapshot is built.
func (e *testOrphanEnv) sweepPubRelConsumers(t *testing.T) {
	t.Helper()
	cons, err := e.asm.jsa.consumerList(mqttOutStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	e.asm.deleteOrphanPubRelConsumers(e.s, cons, refs)
}

// TestOrphanSweepCondemnsARetiredSurvivor is test 4, reader 3: with no CONNECT and no
// live winner, the sweep is what recognises the survivor of the claim-won deletion
// window — and it MARKS ONLY, leaving the finish to the marker-adoption class on a
// LATER pass.
//
// The ordering is the point. Finishing in the same pass would release the record
// moments after the marker landed, and a survivor broker whose lossy record watch
// dropped that brief observation would face an ABSENT record instead of a moved one —
// the F-036 shape. Deferring keeps it visibly MOVED for at least one backstop cadence.
//
// The watermark is produced by the REAL writer, never planted: a planted fence would
// let this test pass even if no teardown ever recorded one.
func TestOrphanSweepCondemnsARetiredSurvivor(t *testing.T) {
	e := testNewOrphanEnv(t, "sweep-condemn")
	e.setGraces(t, 0, 0)
	idHash := e.sess.idHash
	retiredGen := e.sess.gen

	base, baseSeq, err := e.asm.loadFenceSession(idHash)
	if err != nil || base == nil {
		t.Fatalf("loading the base record: %+v %v", base, err)
	}

	// Stage the window: the winner is parked between its claim and its deletions while
	// a same-base resume applies over the marker.
	gate := newTestHookGate(t)
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, _ string, markerSeq uint64) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if sess != e.sess {
			return
		}
		survivor := *base
		survivor.Teardown = nil
		survivor.WID, survivor.PWID = mqttNewRecordWID(), base.WID
		if _, serr := mqttStoreSessionRecordCAS(&e.asm.jsa, e.asm.domainTk, &survivor, idHash, markerSeq); serr != nil {
			t.Errorf("planting the surviving record: %v", serr)
		}
	})
	if cerr := e.sess.clear(false); !errors.Is(cerr, errMQTTClearSuperseded) {
		restore()
		t.Fatalf("clear() = %v, want the superseded sentinel", cerr)
	}
	restore()

	if r, status := e.retirement(t, idHash); status != mqttRetirementValid || r.Gen != retiredGen {
		t.Fatalf("the teardown recorded no usable fence: status=%v r=%+v", status, r)
	}
	// The winner's own arm condemned the survivor; a SECOND late resume erases that
	// marker too (matrix row 12), which is the state only the sweep can still repair.
	marked, markedSeq, err := e.asm.loadFenceSession(idHash)
	if err != nil || marked == nil || marked.Teardown == nil {
		t.Fatalf("expected the winner to have condemned the survivor, got %+v (err=%v)", marked, err)
	}
	again := *base
	again.Teardown = nil
	again.WID, again.PWID = mqttNewRecordWID(), base.WID
	if _, serr := mqttStoreSessionRecordCAS(&e.asm.jsa, e.asm.domainTk, &again, idHash, markedSeq); serr != nil {
		t.Fatalf("planting the second surviving record: %v", serr)
	}
	if baseSeq == 0 {
		t.Fatal("staging error: the base record was never persisted")
	}

	// Pass 1 first-sights the hollow pair and authorizes nothing.
	e.asm.sweepOrphanedQoS2State(e.s)
	if ps := e.sessRecord(t, idHash); ps == nil || ps.Teardown != nil {
		t.Fatalf("the first pass must authorize nothing, got %+v", ps)
	}

	// Pass 2 MARKS, and only marks.
	e.asm.sweepOrphanedQoS2State(e.s)
	ps := e.sessRecord(t, idHash)
	if ps == nil {
		t.Fatal("the sweep released the record in the pass that marked it: a survivor broker that " +
			"dropped the marker observation would see an absent record, not a moved one")
	}
	if ps.Teardown == nil {
		t.Fatal("the sweep did not condemn a record whose generation is durably retired")
	}
	if ps.Gen != retiredGen {
		t.Fatalf("the condemnation must preserve the record's content, got generation %q", ps.Gen)
	}

	// Pass 3 is the adoption class's own first sighting of that marker: still nothing.
	e.asm.sweepOrphanedQoS2State(e.s)
	if got := e.sessRecord(t, idHash); got == nil || got.Teardown == nil {
		t.Fatalf("a first marker observation must authorize no finish, got %+v", got)
	}

	// Pass 4 finishes it.
	e.asm.sweepOrphanedQoS2State(e.s)
	if got := e.sessRecord(t, idHash); got != nil {
		t.Fatalf("the marker-adoption class never finished the condemnation, got %+v", got)
	}
	// Nothing reaps a fence, so it is still there after every pass.
	if r, status := e.retirement(t, idHash); status != mqttRetirementValid || r.Gen != retiredGen {
		t.Fatalf("the retirement fence must be permanent: status=%v r=%+v", status, r)
	}
}

// TestOrphanSweepReapsAnOrphanedPubRelDurable is test 9: the companion half. A PUBREL
// durable on $MQTT_out whose session generation is authoritatively absent was
// referenced by NOTHING before this class existed — sweepOrphanedConsumers lists
// $MQTT_msgs only, and the affine pass deliberately drops every other prefix — so it
// held its interest-retained PUBRELs forever.
func TestOrphanSweepReapsAnOrphanedPubRelDurable(t *testing.T) {
	t.Run("an unreferenced durable is reaped, never on the first pass", func(t *testing.T) {
		e := testNewOrphanEnv(t, "pubrel-orphan")
		e.setGraces(t, 0, 0)
		durable := testCreatePubRelConsumer(t, e, getHash("pubrel-gone"), mqttNewSessGen())

		e.sweepPubRelConsumers(t)
		if !e.consumerExists(t, durable) {
			t.Fatal("the pass that first sighted the durable deleted it; one observation authorizes nothing")
		}
		e.sweepPubRelConsumers(t)
		if e.consumerExists(t, durable) {
			t.Fatal("an unreferenced PUBREL durable was never reaped")
		}
	})

	t.Run("one a record names never enters quarantine", func(t *testing.T) {
		e := testNewOrphanEnv(t, "pubrel-referenced")
		e.setGraces(t, 0, 0)
		idHash, gen := e.sess.idHash, e.sess.gen
		durable := testCreatePubRelConsumer(t, e, idHash, gen)
		_, _, cfgDurable := mqttPubRelNames(idHash, gen)
		e.sess.mu.Lock()
		e.sess.pubRelConsumer = &ConsumerConfig{Durable: cfgDurable}
		e.sess.mu.Unlock()
		e.persist(t)
		// Only the RECORD may reference it now.
		e.dropLocalSession()

		e.sweepPubRelConsumers(t)
		e.sweepPubRelConsumers(t)
		// Asserted against the candidate state, not against survival: the act-time
		// record re-read would keep a wrongly-admitted candidate alive and hide a
		// missing reference class.
		if e.quarantined(mqttQuarantinePubRelKey(durable)) {
			t.Fatal("a PUBREL durable a session record names was admitted as an orphan candidate")
		}
		if !e.consumerExists(t, durable) {
			t.Fatal("a referenced PUBREL durable was deleted")
		}
	})

	t.Run("one only a local session references is untouched", func(t *testing.T) {
		e := testNewOrphanEnv(t, "pubrel-local")
		e.setGraces(t, 0, 0)
		idHash, gen := e.sess.idHash, e.sess.gen
		durable := testCreatePubRelConsumer(t, e, idHash, gen)
		// The create-before-save window: the consumer exists, the record naming it does
		// not, and the only thing that can speak for it is the live session in memory.
		e.dropSessionRecord(t)

		e.sweepPubRelConsumers(t)
		e.sweepPubRelConsumers(t)
		if !e.consumerExists(t, durable) {
			t.Fatal("a PUBREL durable created by a live local session was reaped inside the " +
				"create-before-save window")
		}
	})

	t.Run("a record saved after the reference walk aborts the deletion", func(t *testing.T) {
		e := testNewOrphanEnv(t, "pubrel-late-save")
		e.setGraces(t, 0, 0)
		const owner = "pubrel-late-saver"
		idHash, gen := getHash(owner), mqttNewSessGen()
		durable := testCreatePubRelConsumer(t, e, idHash, gen)

		e.sweepPubRelConsumers(t) // quarantine the instance
		// The owner's save lands after the walk: it never passed through the reference
		// snapshot, and it does not change the consumer's Created either, so only the
		// final authoritative re-read can see it.
		e.asm.mu.Lock()
		e.asm.testSessionLoad = func(hash string) (*mqttPersistedSession, uint64, error) {
			if hash != idHash {
				return nil, 0, nil
			}
			return &mqttPersistedSession{ID: owner, Gen: gen}, 7, nil
		}
		e.asm.mu.Unlock()
		t.Cleanup(func() {
			e.asm.mu.Lock()
			e.asm.testSessionLoad = nil
			e.asm.mu.Unlock()
		})

		e.sweepPubRelConsumers(t)
		if !e.consumerExists(t, durable) {
			t.Fatal("a PUBREL durable was deleted although its record had come back by act time")
		}
	})

	t.Run("a durable recreated since the listing is not deleted", func(t *testing.T) {
		e := testNewOrphanEnv(t, "pubrel-recreated")
		e.setGraces(t, 0, 0)
		idHash, gen := getHash("pubrel-recreated-owner"), mqttNewSessGen()
		durable := testCreatePubRelConsumer(t, e, idHash, gen)

		cons, err := e.asm.jsa.consumerList(mqttOutStreamName)
		if err != nil {
			t.Fatalf("consumerList: %v", err)
		}
		refs, _, err := e.asm.buildSessionRefs()
		if err != nil {
			t.Fatalf("buildSessionRefs: %v", err)
		}
		e.asm.deleteOrphanPubRelConsumers(e.s, cons, refs) // quarantine this instance

		// A peer tears it down and provisions it again under the same name, and has not
		// saved its record yet.
		if _, derr := e.asm.jsa.deleteConsumer(mqttOutStreamName, durable, false); derr != nil {
			t.Fatalf("deleting the consumer: %v", derr)
		}
		time.Sleep(10 * time.Millisecond) // a distinguishable Created
		testCreatePubRelConsumer(t, e, idHash, gen)

		// The sweep proceeds from its now-stale listing.
		e.asm.deleteOrphanPubRelConsumers(e.s, cons, refs)
		if !e.consumerExists(t, durable) {
			t.Fatal("a PUBREL durable recreated since the listing was deleted on the previous " +
				"instance's fingerprint, destroying a peer's fresh consumer")
		}
	})
}

// TestOrphanSweepPubRelValidatesTheConsumerConfig is test 15. The durable NAME is what
// attributes a consumer to a session, so a config that disagrees with its own name
// cannot be attributed at all — and the check is applied at BOTH sites, because they
// answer different questions: discovery decides what may become a candidate, and the
// pre-delete lookup is what actually authorizes the delete.
func TestOrphanSweepPubRelValidatesTheConsumerConfig(t *testing.T) {
	t.Run("a mismatched consumer is never admitted at discovery", func(t *testing.T) {
		e := testNewOrphanEnv(t, "pubrel-badcfg-discovery")
		e.setGraces(t, 0, 0)
		idHash, gen := getHash("pubrel-badcfg"), mqttNewSessGen()
		_, deliver, durable := mqttPubRelNames(idHash, gen)
		if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
			Stream: mqttOutStreamName,
			Config: ConsumerConfig{
				Durable: durable,
				// Another incarnation's PUBREL subject: the name says one generation and
				// the filter says another, so which delivery path this is cannot be said.
				FilterSubject:  mqttPubRelSubjectPrefix + idHash + mqttSessGenTk(mqttNewSessGen(), "."),
				DeliverSubject: deliver,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				MaxAckPending:  1024,
			},
		}); err != nil {
			t.Fatalf("create mismatched PUBREL consumer: %v", err)
		}

		e.sweepPubRelConsumers(t)
		e.sweepPubRelConsumers(t)
		if e.quarantined(mqttQuarantinePubRelKey(durable)) {
			t.Fatal("a consumer whose config does not match its name was admitted as a candidate")
		}
		if !e.consumerExists(t, durable) {
			t.Fatal("a consumer we cannot attribute was deleted")
		}
	})

	t.Run("a same-instance mismatch is refused at the pre-delete lookup", func(t *testing.T) {
		e := testNewOrphanEnv(t, "pubrel-badcfg-predelete")
		e.setGraces(t, 0, 0)
		idHash, gen := getHash("pubrel-badcfg-late"), mqttNewSessGen()
		subject, deliver, durable := mqttPubRelNames(idHash, gen)
		if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
			Stream: mqttOutStreamName,
			Config: ConsumerConfig{
				Durable:        durable,
				FilterSubject:  subject,
				DeliverSubject: mqttPubRelDeliverySubjectPrefix + idHash + mqttSessGenTk(mqttNewSessGen(), "."),
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				MaxAckPending:  1024,
			},
		}); err != nil {
			t.Fatalf("create mismatched PUBREL consumer: %v", err)
		}
		real, err := e.asm.jsa.lookupConsumer(mqttOutStreamName, durable)
		if err != nil || real == nil {
			t.Fatalf("looking up the consumer just created: %+v %v", real, err)
		}

		// A listing that passes discovery, carrying the REAL instance's Created: the
		// instance re-lookup therefore agrees, and only the pre-delete config check can
		// still refuse the delete.
		listing := []*ConsumerInfo{{
			Name:    durable,
			Created: real.Created,
			Config: &ConsumerConfig{
				Durable:        durable,
				FilterSubject:  subject,
				DeliverSubject: deliver,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				MaxAckPending:  1024,
			},
		}}
		refs, _, err := e.asm.buildSessionRefs()
		if err != nil {
			t.Fatalf("buildSessionRefs: %v", err)
		}
		e.asm.deleteOrphanPubRelConsumers(e.s, listing, refs)
		e.asm.deleteOrphanPubRelConsumers(e.s, listing, refs)
		if !e.consumerExists(t, durable) {
			t.Fatal("a consumer whose live config does not match its name was deleted: the instance " +
				"fingerprint agreed, so only the pre-delete validation could refuse it")
		}
	})
}

// TestOrphanSweepPubRelSurvivesAnUnreadableAffineInventory pins the class boundary in
// the production pass, which the direct-call tests above cannot see.
//
// A malformed assignment claim aborts the affine INVENTORY, and that must protect the
// affine consumers and nothing else. It used to return from the whole sweep, so one
// persistent corrupt claim disabled PUBREL reconciliation account-wide and for good —
// on evidence about a different resource class. PUBREL eligibility rests on its own
// complete reference snapshot, the consumer listing, the grace, the final record
// re-read and the instance re-lookup; none of them is an assignment claim.
//
// The affine half is asserted as a POSITIVE CONTROL, not as a discriminator: an
// orphaned affine consumer is retained in the same pass, but that outcome is
// over-determined — the phase's own second complete inventory refuses it as well, so
// removing the guard beside this pass would not change it. What the control buys is
// that the fix is not a licence to delete affine consumers from a partial view.
func TestOrphanSweepPubRelSurvivesAnUnreadableAffineInventory(t *testing.T) {
	e := testNewOrphanEnv(t, "pubrel-vs-affine-inventory")
	e.setGraces(t, 0, 0)

	// A claim that fails validation: the winner hash no longer derives the copy subject
	// and affine durable the claim carries, so the inventory cannot be completed.
	groupDur := mqttSharedConsumerDurablePrefix + "CORRUPT"
	bad := testValidQ2AffClaim(getHash("affine-corrupt"), mqttNewQ2AffGen(), groupDur, 77)
	bad.Winner = "not-a-hash"
	if _, err := e.asm.jsa.storeQ2AffClaimCAS(mqttQ2AffClaimSubject(groupDur, 77), 0, bad); err != nil {
		t.Fatalf("planting a malformed assignment claim: %v", err)
	}
	if _, ierr := e.asm.collectQ2AffInventory(); ierr == nil {
		t.Fatal("staging error: the planted claim must make the affine inventory incomplete")
	}

	pubRelDur := testCreatePubRelConsumer(t, e, getHash("pubrel-past-a-corrupt-claim"), mqttNewSessGen())
	affDur := testCreateAffineConsumer(t, e, getHash("affine-past-a-corrupt-claim"), mqttNewQ2AffGen())

	// The REAL pass, twice: one observation authorizes nothing.
	e.asm.sweepOrphanedQoS2State(e.s)
	e.asm.sweepOrphanedQoS2State(e.s)

	if e.consumerExists(t, pubRelDur) {
		t.Fatal("an unreadable affine claim disabled the PUBREL class: the orphaned durable and its " +
			"interest-retained PUBRELs would accumulate indefinitely, on evidence about another class")
	}
	if !e.consumerExists(t, affDur) {
		t.Fatal("the affine consumer was deleted from an INCOMPLETE inventory: absence taken from a " +
			"partial view is not absence")
	}
}
