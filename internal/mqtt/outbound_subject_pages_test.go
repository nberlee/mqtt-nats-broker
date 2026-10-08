// Copyright 2026 The mqtt-nats-bridge Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The exchange enumeration reads every page (F-105 integrated gate, round 2). A generation keeps
// every exchange it ever admitted, so its history passes the server's subject-page size while its
// identifier space does not: the walk that decides whether an identifier is free must read the
// whole answer, page by page, and refuse any page it cannot vouch for.

// testSubjectUnder is a literal subject under a filter whose last token is a wildcard.
func testSubjectUnder(filter, tok string) string {
	return strings.TrimSuffix(filter, "*") + tok
}

// testUnvouchablePages answers a valid first page that reports more beyond it, then a PREMATURELY
// EMPTY second page: an answer the walk cannot vouch for.
func testUnvouchablePages(t *testing.T) func(string, int) (mqttSubjectPage, error) {
	t.Helper()
	return func(filter string, offset int) (mqttSubjectPage, error) {
		switch offset {
		case 0:
			return mqttSubjectPage{Subjects: map[string]uint64{
				testSubjectUnder(filter, "unvouched-a"): 1, testSubjectUnder(filter, "unvouched-b"): 2,
			}, Total: 3, Limit: 2}, nil
		case 2:
			return mqttSubjectPage{Subjects: map[string]uint64{}, Total: 3, Limit: 2, Offset: 2}, nil
		}
		return mqttSubjectPage{}, fmt.Errorf("test: no page scripted at offset %d", offset)
	}
}

// testPageScript answers the exchange walk of ONE filter page by page and records the offsets the
// walk requested. Any other filter is recorded, and the test asserts none was asked.
type testPageScript struct {
	filter string
	pages  map[int]mqttSubjectPage
	errs   map[int]error

	mu    sync.Mutex
	asked []int
	other []string
	// runaway records a walk that kept requesting past testPageScriptMaxAsks: one that cannot
	// terminate, such as a walk accepting a page that makes no progress.
	runaway bool
}

// testPageScriptMaxAsks bounds how many pages one scripted walk may request; every scripted answer
// ends in far fewer.
const testPageScriptMaxAsks = 64

func (s *testPageScript) obs() *func(string, int) (mqttSubjectPage, error) {
	f := func(filter string, offset int) (mqttSubjectPage, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if filter != s.filter {
			s.other = append(s.other, filter)
			return mqttSubjectPage{}, nil
		}
		if len(s.asked) >= testPageScriptMaxAsks {
			s.runaway = true
			return mqttSubjectPage{}, fmt.Errorf("test: the walk did not terminate")
		}
		s.asked = append(s.asked, offset)
		p, ok := s.pages[offset]
		if !ok {
			return mqttSubjectPage{}, fmt.Errorf("test: no page scripted at offset %d", offset)
		}
		return p, s.errs[offset]
	}
	return &f
}

// requireAsked asserts the walk requested exactly the offsets want, in order, and nothing else.
func (s *testPageScript) requireAsked(t *testing.T, what string, want ...int) {
	t.Helper()
	s.mu.Lock()
	asked, other, runaway := append([]int(nil), s.asked...), append([]string(nil), s.other...), s.runaway
	s.asked, s.other, s.runaway = nil, nil, false
	s.mu.Unlock()
	if runaway {
		t.Fatalf("%s: the walk did not terminate: it was still requesting pages after %d", what,
			testPageScriptMaxAsks)
	}
	if !reflect.DeepEqual(asked, want) {
		t.Fatalf("%s: the walk requested offsets %v, want %v", what, asked, want)
	}
	if len(other) != 0 {
		t.Fatalf("%s: the walk was also asked for filters the script does not answer: %v", what, other)
	}
}

// testPaged slices a sorted subject list into pages of limit, each reporting total.
func testPaged(subjects []string, total, limit int) map[int]mqttSubjectPage {
	pages := map[int]mqttSubjectPage{}
	for off := 0; off < len(subjects); off += limit {
		end := min(off+limit, len(subjects))
		m := map[string]uint64{}
		for i, s := range subjects[off:end] {
			m[s] = uint64(off + i + 1)
		}
		pages[off] = mqttSubjectPage{Subjects: m, Total: total, Limit: limit, Offset: off}
	}
	return pages
}

// testPagesSubjects is n literal subjects under filter, in sort order.
func testPagesSubjects(filter, prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = testSubjectUnder(filter, fmt.Sprintf("%s%03d", prefix, i))
	}
	return out
}

// testPagesExchangeBody is a valid exchange body, verified through the production decoder.
func testPagesExchangeBody(t *testing.T, e *testF105Env, gen, nonce string, pi uint16, phase mqttExchangePhase) []byte {
	t.Helper()
	x := &mqttOutboundExchange{ID: e.id, Epoch: e.epoch, Gen: gen, Nonce: nonce, PI: pi, QoS: 1,
		Phase: phase, AdmitTxn: "txn-seeded", Tenure: "tenure-seeded"}
	body, err := json.Marshal(x)
	if err != nil {
		t.Fatalf("encoding exchange %q: %v", nonce, err)
	}
	if _, derr := mqttDecodeOutboundExchange(body, e.id, e.epoch, gen, nonce); derr != nil {
		t.Fatalf("the seeded exchange %q is not a valid body: %v", nonce, derr)
	}
	return body
}

// testPagesAdmit runs one exchange admission claiming pi through the production commit path, and
// returns the admitted nonce with the commit's answer.
func testPagesAdmit(t *testing.T, e *testF105Env, txn string, pi uint16) (string, error) {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.rec == nil {
		t.Fatalf("no authority to admit under: %+v", al)
	}
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
		PI: pi, QoS: 2, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
	}
	tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
	if terr != nil {
		t.Fatalf("assembling admission %q failed: %v", txn, terr)
	}
	_, err := e.asm.jsa.commitAuthorityTransition(tr)
	return x.Nonce, err
}

// testPagesExchangePresent reports whether an exchange subject holds a record.
func testPagesExchangePresent(t *testing.T, e *testF105Env, gen, nonce string) bool {
	t.Helper()
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(getHash(e.id), gen, nonce))
	if err != nil {
		if ae := jsAPIError(err); ae != nil && IsNatsErr(ae, JSNoMessageFoundErr) {
			return false
		}
		t.Fatalf("reading exchange %q: %v", nonce, err)
	}
	return smsg != nil
}

// U-PAGE. Each case scripts the exchange walk page by page and asserts the offsets it requested.
// (a) and (b) are decided by a real admission; the walk-shape cases are asked of the walk itself
// and, for the refusals and successes, of an admission too.
func TestF105ExchangeWalkReadsEveryPage(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-subject-pages")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen
	filter := mqttExchangeWalkFilter(getHash(e.id), gen)
	jsa := &e.asm.jsa
	t.Cleanup(func() { jsa.exchangeSubjectsObs.Store(nil) })
	const limit, total = 4, 10

	script := func(pages map[int]mqttSubjectPage, errs map[int]error) *testPageScript {
		s := &testPageScript{filter: filter, pages: pages, errs: errs}
		jsa.exchangeSubjectsObs.Store(s.obs())
		return s
	}
	walkRefused := func(t *testing.T, s *testPageScript, what string, wantErr string, offsets ...int) {
		t.Helper()
		subs, complete, err := jsa.streamSubjectsComplete(mqttInflightStreamName, filter)
		switch {
		case wantErr != _EMPTY_:
			if err == nil || !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("%s: the walk answered %d subjects complete=%t err=%v, want the page's error %q",
					what, len(subs), complete, err, wantErr)
			}
		case err != nil || complete:
			t.Fatalf("%s: the walk answered complete=%t err=%v, want an incomplete answer", what, complete, err)
		}
		s.requireAsked(t, what+" (walk)", offsets...)
	}
	admitRefused := func(t *testing.T, s *testPageScript, what string, pi uint16, want string, offsets ...int) {
		t.Helper()
		nonce, err := testPagesAdmit(t, e, "txn-"+strings.ReplaceAll(what, " ", "-"), pi)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: the admission answered %v, want a refusal containing %q", what, err, want)
		}
		if testPagesExchangePresent(t, e, gen, nonce) {
			t.Fatalf("%s: the refused admission's exchange is on the store", what)
		}
		s.requireAsked(t, what+" (admission)", offsets...)
	}
	const unvouched = "a page of the answer could not be vouched for"

	t.Run("a 2.5 pages, all free", func(t *testing.T) {
		s := script(testPaged(testPagesSubjects(filter, "a", total), total, limit), nil)
		nonce, err := testPagesAdmit(t, e, "txn-a", 11)
		if err != nil {
			t.Fatalf("an admission over a complete, free three-page answer was refused: %v", err)
		}
		if !testPagesExchangePresent(t, e, gen, nonce) {
			t.Fatal("the admitted exchange is not on the store")
		}
		s.requireAsked(t, "(a)", 0, limit, 2*limit)
	})

	t.Run("b a collision only on page 3", func(t *testing.T) {
		const collision = "zcollision"
		if _, err := jsa.storeMsg(mqttExchangeSubject(getHash(e.id), gen, collision), -1,
			testPagesExchangeBody(t, e, gen, collision, 12, mqttExchangePublish)); err != nil {
			t.Fatalf("storing the outstanding exchange: %v", err)
		}
		subs := append(testPagesSubjects(filter, "b", total-1), mqttExchangeSubject(getHash(e.id), gen, collision))
		sort.Strings(subs)
		if subs[len(subs)-1] != mqttExchangeSubject(getHash(e.id), gen, collision) {
			t.Fatal("the fixture must place the collision on the last page")
		}
		s := script(testPaged(subs, total, limit), nil)
		admitRefused(t, s, "(b)", 12, collision, 0, limit, 2*limit)
	})

	t.Run("c1 a nonempty short middle page", func(t *testing.T) {
		pages := testPaged(testPagesSubjects(filter, "c", total), total, limit)
		short := pages[limit]
		for subj := range short.Subjects {
			delete(short.Subjects, subj)
			break
		}
		if len(short.Subjects) != limit-1 {
			t.Fatal("the fixture's middle page must be short and nonempty")
		}
		walkRefused(t, script(pages, nil), "(c1)", _EMPTY_, 0, limit)
		admitRefused(t, script(pages, nil), "(c1)", 13, unvouched, 0, limit)
	})

	t.Run("c2 a prematurely empty middle page", func(t *testing.T) {
		pages := testPaged(testPagesSubjects(filter, "d", total), total, limit)
		pages[limit] = mqttSubjectPage{Subjects: map[string]uint64{}, Total: total, Limit: limit, Offset: limit}
		walkRefused(t, script(pages, nil), "(c2)", _EMPTY_, 0, limit)
		admitRefused(t, script(pages, nil), "(c2)", 13, unvouched, 0, limit)
	})

	t.Run("d a page error under a well-formed envelope", func(t *testing.T) {
		pages := testPaged(testPagesSubjects(filter, "e", total), total, limit)
		errs := map[int]error{limit: errors.New("test: page 2 is unavailable")}
		walkRefused(t, script(pages, errs), "(d)", "test: page 2 is unavailable", 0, limit)
		admitRefused(t, script(pages, errs), "(d)", 13, "test: page 2 is unavailable", 0, limit)
	})

	t.Run("e a page echoing another offset", func(t *testing.T) {
		pages := testPaged(testPagesSubjects(filter, "f", total), total, limit)
		p := pages[limit]
		p.Offset = 0
		pages[limit] = p
		walkRefused(t, script(pages, nil), "(e)", _EMPTY_, 0, limit)
		admitRefused(t, script(pages, nil), "(e)", 13, unvouched, 0, limit)
	})

	t.Run("f a subject duplicated across a page boundary", func(t *testing.T) {
		// One subject inserted before the walk's position between pages shifts the list right:
		// the last subject of page 1 is returned again at the head of page 2, and the inserted
		// one is not returned at all.
		subs := testPagesSubjects(filter, "g", total)
		pages := map[int]mqttSubjectPage{}
		for _, pg := range []struct {
			off, total int
			subs       []string
		}{{0, total, subs[0:4]}, {4, total + 1, subs[3:7]}, {8, total + 1, subs[7:10]}} {
			m := map[string]uint64{}
			for i, s := range pg.subs {
				m[s] = uint64(pg.off + i + 1)
			}
			pages[pg.off] = mqttSubjectPage{Subjects: m, Total: pg.total, Limit: limit, Offset: pg.off}
		}
		s := script(pages, nil)
		got, complete, err := jsa.streamSubjectsComplete(mqttInflightStreamName, filter)
		if err != nil || !complete {
			t.Fatalf("(f): a shifted answer was refused: complete=%t err=%v", complete, err)
		}
		gotSet := make([]string, 0, len(got))
		for subj := range got {
			gotSet = append(gotSet, subj)
		}
		sort.Strings(gotSet)
		if !reflect.DeepEqual(gotSet, subs) {
			t.Fatalf("(f): the walk's unique subjects are %v, want the scripted set %v", gotSet, subs)
		}
		s.requireAsked(t, "(f) (walk)", 0, 4, 8)
		s = script(pages, nil)
		if _, err := testPagesAdmit(t, e, "txn-f", 14); err != nil {
			t.Fatalf("(f): an admission over a shifted, complete answer was refused: %v", err)
		}
		s.requireAsked(t, "(f) (admission)", 0, 4, 8)
	})

	t.Run("g the all-zero empty first answer", func(t *testing.T) {
		pages := map[int]mqttSubjectPage{0: {}}
		s := script(pages, nil)
		got, complete, err := jsa.streamSubjectsComplete(mqttInflightStreamName, filter)
		if err != nil || !complete || len(got) != 0 {
			t.Fatalf("(g): the empty answer gave %d subjects complete=%t err=%v, want complete and empty", len(got), complete, err)
		}
		s.requireAsked(t, "(g) (walk)", 0)
		s = script(pages, nil)
		if _, err := testPagesAdmit(t, e, "txn-g", 15); err != nil {
			t.Fatalf("(g): an admission over the empty answer was refused: %v", err)
		}
		s.requireAsked(t, "(g) (admission)", 0)
	})

	t.Run("h a positive total with page size zero", func(t *testing.T) {
		pages := map[int]mqttSubjectPage{0: {Subjects: map[string]uint64{}, Total: total, Limit: 0}}
		walkRefused(t, script(pages, nil), "(h)", _EMPTY_, 0)
		admitRefused(t, script(pages, nil), "(h)", 16, unvouched, 0)
	})
}

// U-CAS. The walk is complete only for subjects that existed when it began; a creator landing
// between pages may be missed, and what excludes it is the transition's own authority CAS. Here the
// competing admission lands after the first's enumeration and every boundary check have passed
// (testTransitionPresend runs after all of them), and the first's batch is refused by the store's
// CAS and commits nothing.
func TestF105ExchangeWalkConcurrentCreatorIsFencedByTheAuthorityCAS(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-subject-pages-cas")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen

	var raced atomic.Bool
	var secondNonce string
	var secondErr error
	hook := func(tr *mqttAuthorityTransition) {
		if tr.Attempt == nil || tr.Attempt.Txn != "txn-first" || !raced.CompareAndSwap(false, true) {
			return
		}
		secondNonce, secondErr = testPagesAdmit(t, e, "txn-second", 22)
	}
	e.asm.jsa.testTransitionPresend.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionPresend.Store(nil) })

	firstNonce, err := testPagesAdmit(t, e, "txn-first", 21)
	if !raced.Load() {
		t.Fatal("the first admission never reached its send, so nothing here is tested")
	}
	if secondErr != nil {
		t.Fatalf("the competing admission was refused: %v", secondErr)
	}
	if firstNonce == secondNonce {
		t.Fatal("the two admissions must carry distinct nonces")
	}
	if err == nil {
		t.Fatal("the first admission committed after a competing admission landed under the same authority")
	}
	if se := jsAPIError(err); se == nil || !IsNatsErr(se, JSStreamWrongLastSequenceErrF, JSStreamWrongLastSequenceConstantErr) {
		t.Fatalf("the first admission was refused by something other than the store's CAS: %v", err)
	}
	if testPagesExchangePresent(t, e, gen, firstNonce) {
		t.Fatal("the refused first admission's exchange is on the store")
	}
	if !testPagesExchangePresent(t, e, gen, secondNonce) {
		t.Fatal("the competing admission's exchange is not on the store")
	}
}

// testPagesEnvWithServer is testF105NewEnv that also returns the server, for a fixture that
// publishes straight into the store.
func testPagesEnvWithServer(t *testing.T, clientID string) (*testF105Env, *Server, func()) {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	epoch := asm.sessStreamEpoch
	asm.mu.RUnlock()
	stop := func() {
		c.Close()
		testMQTTShutdownServer(s)
	}
	if epoch == _EMPTY_ {
		stop()
		t.Fatal("the account has no recovery epoch, so nothing here can be attributed")
	}
	testF105ClearMintedAuthority(t, asm, clientID, stop)
	return &testF105Env{asm: asm, id: clientID, epoch: epoch}, s, stop
}

// R-PAGE. A generation whose exchange history passes the server's subject-page size, seeded
// directly into the store (successive admissions would scan the history quadratically). With the
// REAL enumeration: (a) an admission on a free identifier succeeds; (b) an outstanding exchange
// placed on a verified later page makes an admission claiming its identifier refused, naming it.
func TestF105ExchangeWalkPastTheServerPageSize(t *testing.T) {
	const seeded = 100_001
	e, srv, done := testPagesEnvWithServer(t, "f105-subject-pages-real")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen
	idHash := getHash(e.id)
	filter := mqttExchangeWalkFilter(idHash, gen)

	var pubErrs atomic.Int64
	nc := natsConnect(t, srv.ClientURL())
	defer nc.Close()
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096),
		nats.PublishAsyncErrHandler(func(nats.JetStream, *nats.Msg, error) { pubErrs.Add(1) }))
	if err != nil {
		t.Fatalf("JetStream context: %v", err)
	}
	start := time.Now()
	const batch = 10_000
	for from := 0; from < seeded; from += batch {
		for i := from; i < min(from+batch, seeded); i++ {
			nonce := fmt.Sprintf("rp%06d", i)
			body := testPagesExchangeBody(t, e, gen, nonce, uint16(i%65535)+1, mqttExchangeTerminal)
			if _, err := js.PublishAsync(mqttExchangeSubject(idHash, gen, nonce), body); err != nil {
				t.Fatalf("publishing seed %d: %v", i, err)
			}
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(2 * time.Minute):
			t.Fatalf("the seed batch from %d was not acknowledged", from)
		}
	}
	if n := pubErrs.Load(); n != 0 {
		t.Fatalf("%d seed publications failed", n)
	}
	t.Logf("seeded %d terminal exchanges in %v", seeded, time.Since(start))

	// THE PAGE BOUNDARY, read back: the server's page size, and the seeds on page 1 versus later.
	first, err := e.asm.jsa.streamSubjectsCompletePage(mqttInflightStreamName, filter, 0)
	if err != nil {
		t.Fatalf("reading the first page: %v", err)
	}
	if first.Total != seeded || first.Limit <= 0 || first.Limit >= seeded || len(first.Subjects) != first.Limit {
		t.Fatalf("the first page reports total=%d limit=%d with %d subjects, want %d seeded across more than one page",
			first.Total, first.Limit, len(first.Subjects), seeded)
	}
	lastSeed := mqttExchangeSubject(idHash, gen, fmt.Sprintf("rp%06d", seeded-1))
	if _, onFirst := first.Subjects[lastSeed]; onFirst {
		t.Fatalf("the last seed %q is on page 1, so nothing past it is a later page", lastSeed)
	}
	t.Logf("the server pages this filter at %d subjects; %d on page 1", first.Limit, len(first.Subjects))

	// (a) A FREE identifier is admitted through the production path with the real enumeration.
	if e.asm.jsa.exchangeSubjectsObs.Load() != nil {
		t.Fatal("the enumeration seam must be nil: this test is the real walk")
	}
	start = time.Now()
	nonceA, err := testPagesAdmit(t, e, "txn-rpage-a", 40_000)
	t.Logf("(a) admission over %d retained exchanges took %v", seeded, time.Since(start))
	if err != nil {
		t.Fatalf("(a) an admission on a free identifier was refused past the page size: %v", err)
	}
	if !testPagesExchangePresent(t, e, gen, nonceA) {
		t.Fatal("(a) the admitted exchange is not on the store")
	}

	// (b) AN OUTSTANDING exchange on a later page.
	const late = "zzzzzzzz-late"
	const k = 40_001
	if _, err := e.asm.jsa.storeMsg(mqttExchangeSubject(idHash, gen, late), -1,
		testPagesExchangeBody(t, e, gen, late, k, mqttExchangePublish)); err != nil {
		t.Fatalf("storing the outstanding exchange: %v", err)
	}
	first, err = e.asm.jsa.streamSubjectsCompletePage(mqttInflightStreamName, filter, 0)
	if err != nil {
		t.Fatalf("re-reading the first page: %v", err)
	}
	if _, onFirst := first.Subjects[mqttExchangeSubject(idHash, gen, late)]; onFirst || first.Total != seeded+2 {
		t.Fatalf("the outstanding exchange must be on a later page (on page 1=%t, total=%d)", onFirst, first.Total)
	}
	later, err := e.asm.jsa.streamSubjectsCompletePage(mqttInflightStreamName, filter, first.Limit)
	if err != nil {
		t.Fatalf("reading the later page: %v", err)
	}
	if _, onLater := later.Subjects[mqttExchangeSubject(idHash, gen, late)]; !onLater {
		t.Fatalf("the outstanding exchange is not on the page at offset %d: %v", first.Limit, later.Subjects)
	}
	start = time.Now()
	nonceB, err := testPagesAdmit(t, e, "txn-rpage-b", k)
	t.Logf("(b) admission over %d retained exchanges took %v", seeded+2, time.Since(start))
	if err == nil || !strings.Contains(err.Error(), late) {
		t.Fatalf("(b) an admission claiming %d beside the later-page obligation %q answered %v, want a refusal naming it",
			k, late, err)
	}
	if testPagesExchangePresent(t, e, gen, nonceB) {
		t.Fatal("(b) the refused admission's exchange is on the store")
	}
}
