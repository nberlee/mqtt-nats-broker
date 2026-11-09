// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testSessWalkRecord persists a valid session record for clientID and returns its subject.
func testSessWalkRecord(t *testing.T, e *testOrphanEnv, clientID, gen string) string {
	t.Helper()
	testStoreRawSession(t, e, clientID, &mqttPersistedSession{ID: clientID, Gen: gen})
	return mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash(clientID)
}

// testSessWalkClients returns n client IDs whose session subjects sort in the order returned.
func testSessWalkClients(n int, prefix string) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = prefix + nuid.Next()
	}
	sort.Slice(ids, func(i, j int) bool { return getHash(ids[i]) < getHash(ids[j]) })
	return ids
}

// testSessStreamLastSeq is the session stream's last sequence.
func testSessStreamLastSeq(t *testing.T, e *testOrphanEnv) uint64 {
	t.Helper()
	_, last, err := e.asm.jsa.streamSubjects(mqttSessStreamName, mqttSessStreamSubjectPrefix+e.asm.domainTk+">")
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	return last
}

// testSessWalkVisit is one visit: the record's subject and sequence.
type testSessWalkVisit struct {
	subject string
	seq     uint64
}

// testSessWalkRequests records the session stream's next_by_subj start sequences and the
// session subjects loaded by subject, while armed.
type testSessWalkRequests struct {
	mu     sync.Mutex
	starts []uint64
	loads  []string
}

func testWatchSessWalkRequests(t *testing.T) *testSessWalkRequests {
	t.Helper()
	r := &testSessWalkRequests{}
	restore := setMQTTTestLoadNextMsgHook(func(stream, subject string, seq uint64) error {
		if stream == mqttSessStreamName && strings.HasPrefix(subject, mqttSessStreamSubjectPrefix) {
			r.mu.Lock()
			r.starts = append(r.starts, seq)
			r.mu.Unlock()
		}
		return nil
	})
	t.Cleanup(restore)
	h := func(stream, subject string) error {
		if stream == mqttSessStreamName && strings.HasPrefix(subject, mqttSessStreamSubjectPrefix) {
			r.mu.Lock()
			r.loads = append(r.loads, subject)
			r.mu.Unlock()
		}
		return nil
	}
	prev := mqttTestLoadLastMsgHookV.Load()
	mqttTestLoadLastMsgHookV.Store(&h)
	t.Cleanup(func() { mqttTestLoadLastMsgHookV.Store(prev) })
	return r
}

// reset forgets what was recorded so far.
func (r *testSessWalkRequests) reset() {
	r.mu.Lock()
	r.starts, r.loads = nil, nil
	r.mu.Unlock()
}

// check fails the test if a sequence walk started at or below watermark, or if no listed
// session subject in want was loaded by subject.
func (r *testSessWalkRequests) check(t *testing.T, pass string, watermark uint64, want []string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, seq := range r.starts {
		if seq <= watermark {
			t.Errorf("%s walked the session records by sequence from %d, at or below the listing watermark %d", pass, seq, watermark)
		}
	}
	loaded := make(map[string]bool, len(r.loads))
	for _, subj := range r.loads {
		loaded[subj] = true
	}
	for _, subj := range want {
		if !loaded[subj] {
			t.Errorf("%s never loaded the session record %q by its subject", pass, subj)
		}
	}
}

// Every sweep pass that needs all session records reads them by subject from the stream's
// subject index, never by a sequence walk from the start: on $MQTT_sess that walk crosses
// every block between two sparse session records, and a cold block is read whole (issue #179).
func TestSweepPassesWalkSessionRecordsFromTheirSubjects(t *testing.T) {
	e := testNewOrphanEnv(t, "walk-passes")
	var subjects []string
	for _, id := range testSessWalkClients(3, "walk-passes-") {
		subjects = append(subjects, testSessWalkRecord(t, e, id, nuid.Next()))
	}
	// An aged ordinary consumer no session references, so the orphan consumer sweep has a
	// candidate to carry into its record walk. Its name is not RETIRED, so nothing deletes it.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: "walkpasses_" + nuid.Next(), DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "walk.passes"}}); err != nil {
		t.Fatalf("create the candidate consumer: %v", err)
	}
	r := testWatchSessWalkRequests(t)
	for _, pass := range []struct {
		name string
		run  func()
	}{
		{"sweepSessionExpiries", func() { e.asm.sweepSessionExpiries(e.s) }},
		{"sweepOrphanedConsumersBefore", func() { e.asm.sweepOrphanedConsumersBefore(e.s, time.Now().Add(time.Hour)) }},
		{"adoptAbandonedTeardowns", func() { e.asm.adoptAbandonedTeardowns(e.s) }},
		{"buildSessionRefs", func() {
			if _, _, err := e.asm.buildSessionRefs(); err != nil {
				t.Fatalf("buildSessionRefs: %v", err)
			}
		}},
		{"sweepAcqReleaseVerdicts", func() { e.asm.sweepAcqReleaseVerdicts(e.s) }},
	} {
		watermark := testSessStreamLastSeq(t, e)
		r.reset()
		pass.run()
		r.check(t, pass.name, watermark, subjects)
	}
}

// A record written after the listing, a new one or a new version of one already visited, is
// visited by the tail walk above the watermark. The sequence walk visited both versions of a
// record rewritten behind its cursor, and the reference set is a union over them.
func TestSessionRecordWalkVisitsVersionsWrittenAfterTheListing(t *testing.T) {
	e := testNewOrphanEnv(t, "walk-after-listing")
	ids := testSessWalkClients(3, "walk-after-")
	a, b, c := ids[0], ids[1], ids[2]
	oldGen, newGen := nuid.Next(), nuid.Next()
	subjA := testSessWalkRecord(t, e, a, oldGen)
	subjB := testSessWalkRecord(t, e, b, nuid.Next())
	subjC := mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash(c)

	// B's visitor (A was visited before it) rewrites A under a new generation and creates C.
	var visits []testSessWalkVisit
	gens := map[string]bool{}
	if err := e.asm.walkSessionRecords(func(smsg *StoredMsg) error {
		visits = append(visits, testSessWalkVisit{smsg.Subject, smsg.Sequence})
		if smsg.Subject == subjB {
			testSessWalkRecord(t, e, a, newGen)
			testSessWalkRecord(t, e, c, nuid.Next())
		}
		if smsg.Subject == subjA {
			var ps mqttPersistedSession
			if err := json.Unmarshal(smsg.Data, &ps); err != nil {
				t.Fatalf("decode A: %v", err)
			}
			gens[ps.Gen] = true
		}
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !gens[oldGen] || !gens[newGen] {
		t.Fatalf("A's versions visited: %v, want both %q and %q (visits %v)", gens, oldGen, newGen, visits)
	}
	var sawC bool
	for _, v := range visits {
		sawC = sawC || v.subject == subjC
	}
	if !sawC {
		t.Fatalf("the record created after the listing was not visited: %v", visits)
	}

	// The same schedule through the reference set: both of A's generations are referenced.
	e2 := testNewOrphanEnv(t, "walk-after-listing-refs")
	ids2 := testSessWalkClients(2, "walk-after-refs-")
	a2, b2 := ids2[0], ids2[1]
	oldGen2, newGen2 := nuid.Next(), nuid.Next()
	testSessWalkRecord(t, e2, a2, oldGen2)
	subjB2 := testSessWalkRecord(t, e2, b2, nuid.Next())
	// The reference walk has no visitor to hook, so the writes ride B's load-by-subject, armed
	// only for this walk and only once A was loaded: the account's own record watch also loads
	// records by subject, and a trigger it fired would rewrite A before the walk began.
	subjA2 := mqttSessStreamSubjectPrefix + e2.asm.domainTk + getHash(a2)
	var mu sync.Mutex
	var armed, sawA2, wrote bool
	h2 := func(stream, subject string) error {
		if stream != mqttSessStreamName {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case !armed || wrote:
		case subject == subjA2:
			sawA2 = true
		case subject == subjB2 && sawA2:
			wrote = true
			testSessWalkRecord(t, e2, a2, newGen2)
		}
		return nil
	}
	prev := mqttTestLoadLastMsgHookV.Load()
	mqttTestLoadLastMsgHookV.Store(&h2)
	t.Cleanup(func() { mqttTestLoadLastMsgHookV.Store(prev) })
	mu.Lock()
	armed = true
	mu.Unlock()
	refs, _, err := e2.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	mu.Lock()
	if !wrote {
		mu.Unlock()
		t.Fatal("the rewrite never ran: B was not loaded by subject after A")
	}
	mu.Unlock()
	if !refs.hasInbound(getHash(a2), oldGen2) || !refs.hasInbound(getHash(a2), newGen2) {
		t.Fatalf("the reference set lost a generation of a record rewritten during the walk: old %v, new %v",
			refs.hasInbound(getHash(a2), oldGen2), refs.hasInbound(getHash(a2), newGen2))
	}
}

// A listed record deleted before its load is not visited, and the walk still succeeds: "no
// message" for a listed subject is a deletion since the listing.
func TestSessionRecordWalkSkipsARecordDeletedAfterTheListing(t *testing.T) {
	e := testNewOrphanEnv(t, "walk-deleted")
	ids := testSessWalkClients(2, "walk-deleted-")
	subjA := testSessWalkRecord(t, e, ids[0], nuid.Next())
	subjB := testSessWalkRecord(t, e, ids[1], nuid.Next())
	smsgB, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, subjB)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	var visited []string
	if err := e.asm.walkSessionRecords(func(smsg *StoredMsg) error {
		visited = append(visited, smsg.Subject)
		if smsg.Subject == subjA {
			if err := e.asm.jsa.deleteMsg(mqttSessStreamName, smsgB.Sequence, true); err != nil {
				t.Fatalf("delete B: %v", err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("the walk failed on a record deleted after the listing: %v", err)
	}
	for _, s := range visited {
		if s == subjB {
			t.Fatalf("a record deleted before its load was visited: %v", visited)
		}
	}
}

// A listing whose envelope cannot be vouched for is an error, and nothing is visited; the
// reference set that depends on it fails.
func TestSessionRecordWalkRefusesAnInvalidListing(t *testing.T) {
	e := testNewOrphanEnv(t, "walk-invalid")
	ids := testSessWalkClients(2, "walk-invalid-")
	subjA := testSessWalkRecord(t, e, ids[0], nuid.Next())
	testSessWalkRecord(t, e, ids[1], nuid.Next())
	// A short page: two subjects promised, one delivered.
	obs := func(filter string, offset int) (mqttSubjectPage, error) {
		return mqttSubjectPage{Subjects: map[string]uint64{subjA: 1}, Total: 2, Limit: 2, Offset: offset,
			LastSeq: testSessStreamLastSeq(t, e)}, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&obs)
	t.Cleanup(func() { e.asm.jsa.exchangeSubjectsObs.Store(nil) })

	var visits int
	if err := e.asm.walkSessionRecords(func(*StoredMsg) error { visits++; return nil }); err == nil {
		t.Fatal("the walk accepted a listing it could not vouch for")
	}
	if visits != 0 {
		t.Fatalf("the walk visited %d record(s) of a listing it could not vouch for", visits)
	}
	if _, _, err := e.asm.buildSessionRefs(); err == nil {
		t.Fatal("the reference set reported success on a listing it could not vouch for")
	}
}

// A listing that does not fit one page is not a census (a delete between pages shifts the
// offsets past a record that never left), so the walk falls back to the sequence walk from the
// start and visits every record, including one no page it read held.
func TestSessionRecordWalkWalksBySequenceWhenTheListingPages(t *testing.T) {
	e := testNewOrphanEnv(t, "walk-pages")
	ids := testSessWalkClients(3, "walk-pages-")
	r1 := testSessWalkRecord(t, e, ids[0], nuid.Next())
	r2 := testSessWalkRecord(t, e, ids[1], nuid.Next())
	r3 := testSessWalkRecord(t, e, ids[2], nuid.Next())
	watermark := testSessStreamLastSeq(t, e)
	if watermark == 0 {
		t.Fatal("the session stream reports no last sequence")
	}
	obs := func(filter string, offset int) (mqttSubjectPage, error) {
		return mqttSubjectPage{Subjects: map[string]uint64{r1: 1, r2: 1}, Total: 3, Limit: 2, Offset: offset,
			LastSeq: watermark}, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&obs)
	t.Cleanup(func() { e.asm.jsa.exchangeSubjectsObs.Store(nil) })
	r := testWatchSessWalkRequests(t)

	visited := map[string]bool{}
	if err := e.asm.walkSessionRecords(func(smsg *StoredMsg) error {
		visited[smsg.Subject] = true
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !visited[r1] || !visited[r2] || !visited[r3] {
		t.Fatalf("visited %v, want %q, %q and %q", visited, r1, r2, r3)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.starts) == 0 || r.starts[0] != 1 {
		t.Fatalf("the sequence walk did not start at the beginning of the stream: %v", r.starts)
	}
}

// A visitor ends the walk with errMQTTSessWalkStop, which is success; any other visitor error
// ends it and is returned as it was.
func TestSessionRecordWalkStopsWhenTheVisitorSaysSo(t *testing.T) {
	e := testNewOrphanEnv(t, "walk-stop")
	for _, id := range testSessWalkClients(3, "walk-stop-") {
		testSessWalkRecord(t, e, id, nuid.Next())
	}
	var visits int
	if err := e.asm.walkSessionRecords(func(*StoredMsg) error {
		visits++
		return errMQTTSessWalkStop
	}); err != nil {
		t.Fatalf("a stopped walk reported an error: %v", err)
	}
	if visits != 1 {
		t.Fatalf("the walk visited %d records after its visitor stopped it at the first", visits)
	}
	refused := errors.New("refused by the visitor")
	visits = 0
	err := e.asm.walkSessionRecords(func(*StoredMsg) error {
		visits++
		return refused
	})
	if !errors.Is(err, refused) {
		t.Fatalf("the walk returned %v, want the visitor's error", err)
	}
	if visits != 1 {
		t.Fatalf("the walk visited %d records after its visitor failed at the first", visits)
	}
}
