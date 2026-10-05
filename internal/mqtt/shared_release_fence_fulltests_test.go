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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// F-105 7e: the shared acquisition release fence (plan-7e T2-T11). The tests that drive a schedule
// install the process-wide shared pause hook and so are not parallel.

// testSharedGates parks named mqttTestSharedPause points, each on its FIRST arrival only (a ladder
// retry reaching the same point again passes through).
type testSharedGates struct {
	mu    sync.Mutex
	gates map[string]*testSharedGate
}

type testSharedGate struct {
	first   sync.Once
	reached chan struct{}
	release chan struct{}
	opened  sync.Once
}

func testInstallSharedGates(t *testing.T, points ...string) *testSharedGates {
	t.Helper()
	g := &testSharedGates{gates: make(map[string]*testSharedGate)}
	for _, p := range points {
		g.gates[p] = &testSharedGate{reached: make(chan struct{}), release: make(chan struct{})}
	}
	hook := func(point string) {
		g.mu.Lock()
		gate := g.gates[point]
		g.mu.Unlock()
		if gate == nil {
			return
		}
		park := false
		gate.first.Do(func() { park = true })
		if !park {
			return
		}
		close(gate.reached)
		<-gate.release
	}
	if !mqttTestSharedPauseHookV.CompareAndSwap(nil, &hook) {
		t.Fatal("fixture error: another shared pause hook is installed")
	}
	t.Cleanup(func() {
		for p := range g.gates {
			g.open(p)
		}
		mqttTestSharedPauseHookV.Store(nil)
	})
	return g
}

func (g *testSharedGates) wait(t *testing.T, point string) {
	t.Helper()
	select {
	case <-g.gates[point].reached:
	case <-time.After(10 * time.Second):
		t.Fatalf("the schedule never reached %q", point)
	}
}

func (g *testSharedGates) reachedCh(point string) <-chan struct{} { return g.gates[point].reached }

func (g *testSharedGates) open(point string) {
	gate := g.gates[point]
	gate.opened.Do(func() { close(gate.release) })
}

// testAdmitInBackground allocates the rig's delivery and runs its admission with the CAPTURED
// dependency on another goroutine, as the delivery callback's admission ladder would.
func testAdmitInBackground(t *testing.T, r *testSharedAdmitRig, dep *mqttAdmitShared) (uint16, <-chan bool) {
	t.Helper()
	e := r.e
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(r.dur, testInflightAckSubject(r.dur, r.sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	nonce := e.nonceOf(pi)
	out := make(chan bool, 1)
	go func() { out <- e.sess.admitSharedInflightPublish(e.c, pi, 1, r.dur, r.sseq, nonce, dep) }()
	return pi, out
}

func testReleaseInBackground(r *testSharedAdmitRig) <-chan error {
	e := r.e
	out := make(chan error, 1)
	go func() {
		ok, err := e.sess.jsa.executeSharedRelease(e.sess.sharedEpoch(), e.sess.id, e.sess.gen, r.acq)
		if err == nil && !ok {
			err = errors.New("the release did not settle")
		}
		out <- err
	}()
	return out
}

func testRecv[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(15 * time.Second):
		t.Fatalf("%s did not finish", what)
	}
	var zero T
	return zero
}

func (r *testSharedAdmitRig) ledger(t *testing.T) *mqttSharedLedger {
	t.Helper()
	l := r.e.sess.jsa.loadSharedLedger(r.e.sess.id, r.e.sess.sharedEpoch(), r.e.sess.gen)
	if l.status != mqttOutboundValid {
		t.Fatalf("the ledger reads %v (%v)", l.status, l.err)
	}
	return l.rec
}

func (r *testSharedAdmitRig) groupRecord(t *testing.T) *mqttSharedGroupRecord {
	t.Helper()
	g := r.e.sess.jsa.loadSharedGroup(r.e.sess.sharedEpoch(), r.acq.Group)
	if g.status != mqttOutboundValid {
		t.Fatalf("the group record reads %v (%v)", g.status, g.err)
	}
	return g.rec
}

func (r *testSharedAdmitRig) closeAcq(t *testing.T, acq mqttSharedAcqRef) {
	t.Helper()
	e := r.e
	l := e.sess.jsa.loadSharedLedger(e.sess.id, e.sess.sharedEpoch(), e.sess.gen)
	b, err := mqttComposeSharedClose(e.sess.sharedEpoch(), e.sess.id, e.sess.gen, acq, l)
	if err != nil || b == nil {
		t.Fatalf("composing the close: %v", err)
	}
	if res := e.sess.jsa.commitSharedLifecycleBatch(b); res.Outcome != mqttSharedWriteLanded {
		t.Fatalf("the close did not land: %v %v", res.Outcome, res.Err)
	}
}

// T2: readiness refuses an acquisition its ledger lists as CLOSED, and only that one: another
// acquisition of the same incarnation is still ready. Mutation M6.
func TestSharedReadinessRefusesAClosedAcquisition(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t2")
	e := r.e
	group := "g" + getHashSize("fence-t2", 8)
	other, out, err := e.sess.acquireShared(e.c, group, "foo.bar", 1024)
	if err != nil || out != mqttSharedAcquired {
		t.Fatalf("acquiring a second holder: %v %v", out, err)
	}
	otherRef := mqttSharedAcqRef{Group: r.acq.Group, Inc: other.Inc, Acq: other.Acq}
	r.closeAcq(t, r.acq)
	dep := r.dep()
	rerr := e.sess.jsa.mqttSharedAcqReadyForDependency(e.sess.id, e.sess.sharedEpoch(), e.sess.gen, r.dur, dep)
	if rerr == nil || !strings.Contains(rerr.Error(), "closed to new dependencies") {
		t.Fatalf("readiness of a closed acquisition: %v", rerr)
	}
	otherDep := &mqttAdmitShared{Acq: otherRef, Tenure: dep.Tenure}
	if oerr := e.sess.jsa.mqttSharedAcqReadyForDependency(e.sess.id, e.sess.sharedEpoch(), e.sess.gen, r.dur, otherDep); oerr != nil {
		t.Fatalf("another acquisition of the same incarnation must stay ready: %v", oerr)
	}
	if pi, ok := r.admit(t, dep); ok {
		t.Fatal("a delivery was admitted against a closed acquisition")
	} else if _, x := r.exchangeOf(t, pi); x != nil {
		t.Fatal("a refused admission left an exchange")
	}
}

// T3: the fence kind's rule: a SERVING predecessor reinstalled exactly but for its revision,
// carrying no member and no closure.
func TestSharedReleaseFenceKindRule(t *testing.T) {
	prev := &mqttOutboundAuthority{ID: "fence-t3", Epoch: "ep", Gen: mqttNewSessGen(), Tenure: "ten",
		State: mqttAuthorityServing, Rev: 4, Txn: "t4", OwnerTxn: "own"}
	ok := mqttFenceSharedReleaseTransition(prev, 9, "txn-fence")
	if err := mqttCheckTransitionKindRules(ok); err != nil {
		t.Fatalf("the composed fence is refused: %v", err)
	}
	rows := map[string]func(tr *mqttAuthorityTransition){
		"a member": func(tr *mqttAuthorityTransition) {
			tr.Extra = []mqttTransitionMember{{subj: "x", body: []byte("{}")}}
		},
		"a changed generation": func(tr *mqttAuthorityTransition) { tr.Auth.Gen = mqttNewSessGen() },
		"a changed state":      func(tr *mqttAuthorityTransition) { tr.Auth.State = mqttAuthorityClosed },
		"a closure":            func(tr *mqttAuthorityTransition) { tr.Closure = &mqttGenClosure{} },
		"a non-serving predecessor": func(tr *mqttAuthorityTransition) {
			p := *tr.Prev
			p.State = mqttAuthorityPreparing
			tr.Prev = &p
			tr.Auth.State = mqttAuthorityPreparing
		},
	}
	for name, mut := range rows {
		tr := mqttFenceSharedReleaseTransition(prev, 9, "txn-fence")
		mut(tr)
		if err := mqttCheckTransitionKindRules(tr); err == nil {
			t.Errorf("a fence with %s was accepted", name)
		}
	}
}

// testBumpAuthority advances the authority's revision with a raw CAS write, standing for any
// committed authority write a concurrent operation makes.
func testBumpAuthority(t *testing.T, e *testInflightEnv) {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority reads %v", al.status)
	}
	next := *al.rec
	next.Rev++
	next.Txn = mqttNewInflightNonce()
	b, _ := json.Marshal(&next)
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.sess.id)), hdr, msg); err != nil {
		t.Fatalf("bumping the authority: %v", err)
	}
}

// T4: only a CONFIRMED advance past the sequence read is a fence. A landed fence is one attempt; a
// rejection with the authority unchanged is re-attempted and never counted as fenced (a 10164 says
// another batch is in process, not that one committed); a rejection behind a committed foreign
// advance is fenced with no further write. Mutations M9a, M9b.
func TestSharedReleaseFenceNeedsAConfirmedAdvance(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t4")
	e := r.e
	id, epoch, gen := e.sess.id, e.asm.sessStreamEpoch, e.sess.gen
	seq := func() uint64 { return e.asm.jsa.loadOutboundAuthority(id, epoch).seq }
	var attempts atomic.Int32
	injected := errors.New("injected: maximum messages per subject exceeded (10164)")
	setHook := func(h func(attempt int) (error, bool)) {
		fn := func(fid string, attempt int) (error, bool) {
			if fid != id {
				return nil, false
			}
			attempts.Add(1)
			return h(attempt)
		}
		mqttTestSharedFenceOutcomeHookV.Store(&fn)
	}
	defer mqttTestSharedFenceOutcomeHookV.Store(nil)

	// LANDED.
	attempts.Store(0)
	setHook(func(int) (error, bool) { return nil, false })
	before := seq()
	if err := e.sess.jsa.fenceSharedRelease(id, epoch, gen); err != nil {
		t.Fatalf("a landed fence: %v", err)
	}
	if attempts.Load() != 1 || seq() <= before {
		t.Fatalf("a landed fence took %d attempt(s) and moved the authority %d -> %d", attempts.Load(), before, seq())
	}

	// REJECTED, AUTHORITY UNCHANGED: never fenced.
	attempts.Store(0)
	setHook(func(int) (error, bool) { return injected, true })
	before = seq()
	if err := e.sess.jsa.fenceSharedRelease(id, epoch, gen); err == nil {
		t.Fatal("a fence whose every attempt was rejected over an unchanged authority reported FENCED")
	}
	if attempts.Load() != mqttSharedReleaseAttempts || seq() != before {
		t.Fatalf("the unconfirmed fence took %d attempt(s), want %d, and the authority moved %d -> %d",
			attempts.Load(), mqttSharedReleaseAttempts, before, seq())
	}

	// REJECTED BEHIND A COMMITTED FOREIGN ADVANCE: fenced, no further attempt.
	attempts.Store(0)
	setHook(func(int) (error, bool) {
		testBumpAuthority(t, e)
		return injected, true
	})
	if err := e.sess.jsa.fenceSharedRelease(id, epoch, gen); err != nil {
		t.Fatalf("a rejection behind a committed advance must be fenced: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("the fenced rejection re-attempted: %d attempt(s)", attempts.Load())
	}
}

// T5: an admission that observed readiness BEFORE the close cannot land after the release
// enumerated. The admission is parked after every readiness read; the release closes, fences and
// enumerates and parks before L1 with the acquisition still LISTED; the admission's CAS then fails
// and its retry is refused BY the close. Mutations M1, M2, M6.
func TestSharedReleaseFenceExcludesAnAdmissionThatSawReadiness(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t5")
	e := r.e
	dep := r.dep()
	admitPt, l1Pt := "admit:after-readiness:"+r.acq.Acq, "release:before-send:"+r.acq.Acq
	gates := testInstallSharedGates(t, admitPt, l1Pt)

	pi, admitted := testAdmitInBackground(t, r, dep)
	gates.wait(t, admitPt)
	released := testReleaseInBackground(r)
	gates.wait(t, l1Pt)

	gates.open(admitPt)
	if testRecv(t, "the parked admission", admitted) {
		t.Fatal("an admission that observed readiness before the close LANDED after the release enumerated")
	}
	// The release is still parked before L1: the acquisition is listed, and closed.
	if l := r.ledger(t); !l.isClosed(r.acq.Acq) {
		t.Fatalf("the release parked before L1 must have closed the acquisition: %+v", l)
	} else if _, listed := l.lists(r.acq.Acq); !listed {
		t.Fatalf("fixture error: the acquisition is no longer listed before L1: %+v", l)
	}
	rerr := e.sess.jsa.mqttSharedAcqReadyForDependency(e.sess.id, e.sess.sharedEpoch(), e.sess.gen, r.dur, dep)
	if rerr == nil || !strings.Contains(rerr.Error(), "closed to new dependencies") {
		t.Fatalf("what refuses the retry must be the close: %v", rerr)
	}

	gates.open(l1Pt)
	if err := testRecv(t, "the release", released); err != nil {
		t.Fatalf("the release: %v", err)
	}
	if _, x := r.exchangeOf(t, pi); x != nil {
		t.Fatalf("an exchange depends on the released acquisition: %+v", x)
	}
	if g := r.groupRecord(t); g.Live != nil {
		t.Fatalf("the last holder's release did not retire the incarnation: %+v", g)
	}
}

// T5b: the ORDER close-then-fence is the fence. One driver whatever the order: the release parks
// after its FIRST step; an admission with the captured dependency then runs, and is either refused
// or parks after its readiness; the release completes; the admission is resumed. Unmutated, the
// first step is the close and the admission is refused by it. Fence-first (M3), the admission reads
// the fenced authority and an open ledger, parks, and lands after L1.
func TestSharedReleaseFenceOrderIsCloseThenFence(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t5b")
	dep := r.dep()
	stepPt, admitPt := "release:after-step1:"+r.acq.Acq, "admit:after-readiness:"+r.acq.Acq
	gates := testInstallSharedGates(t, stepPt, admitPt)

	released := testReleaseInBackground(r)
	gates.wait(t, stepPt)
	pi, admitted := testAdmitInBackground(t, r, dep)
	var landed, done bool
	select {
	case landed = <-admitted:
		done = true
	case <-gates.reachedCh(admitPt):
	case <-time.After(15 * time.Second):
		t.Fatal("the admission neither finished nor parked")
	}
	gates.open(stepPt)
	if err := testRecv(t, "the release", released); err != nil {
		t.Fatalf("the release: %v", err)
	}
	gates.open(admitPt)
	if !done {
		landed = testRecv(t, "the admission", admitted)
	}
	if landed {
		t.Fatal("an admission composed against the release's window landed")
	}
	if _, x := r.exchangeOf(t, pi); x != nil {
		t.Fatalf("an exchange depends on the released acquisition: %+v", x)
	}
}

// testSharedSourceAcksStubbed makes a PUBACK's source settlement succeed without a live consumer
// reply, as TestSharedPubAckSettlesTheSourceThroughTheTerminalContinuation does.
func testSharedSourceAcksStubbed(f *testSharedHandBackFix) {
	f.e.asm.mu.Lock()
	f.e.asm.testInflightSourceAck = func(string) error { return nil }
	f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return mqttInflightSourceLive }
	f.e.asm.mu.Unlock()
}

// testSaveVerdict makes acq a DURABLE release verdict of the session, as the UNSUBSCRIBE's record
// write does.
func testSaveVerdict(t *testing.T, sess *mqttSession, acq mqttSharedAcqRef) {
	t.Helper()
	sess.mu.Lock()
	if sess.acqReleases == nil {
		sess.acqReleases = make(map[string]mqttSharedAcqRef)
	}
	sess.acqReleases[acq.Acq] = acq
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the verdict: %v", err)
	}
}

// T6: a dependant still owing delivery from the source WITHHOLDS the release — the acquisition
// stays listed and closed, the incarnation served, the consumer present — and the PUBACK that ends
// it re-triggers the owner, which releases and retires with no sweep running. Mutations M4, M8.
func TestSharedReleaseWithheldUntilTheDependantEnds(t *testing.T) {
	f := testSharedTransmitted(t, "fence-t6")
	sess := f.e.sess
	testSharedSourceAcksStubbed(f)
	testSaveVerdict(t, sess, f.acq)

	ok, err := sess.jsa.executeSharedRelease(sess.sharedEpoch(), sess.id, sess.gen, f.acq)
	if ok || !errors.Is(err, errMQTTSharedReleaseWithheld) {
		t.Fatalf("a release with a Publish dependant: ok=%v err=%v, want withheld", ok, err)
	}
	if l := f.ledger(t); !l.isClosed(f.acq.Acq) {
		t.Fatalf("the withheld acquisition must stay listed and closed: %+v", l)
	}
	if g := f.groupRecord(t); g.Live == nil || g.Live.Inc != f.acq.Inc {
		t.Fatalf("the withheld release moved the group: %+v", g)
	}
	if ci, lerr := sess.jsa.lookupConsumer(mqttStreamName, f.dur); lerr != nil || ci == nil {
		t.Fatalf("the withheld release lost the group consumer: %v", lerr)
	}

	if err := f.e.c.mqttProcessPublishReceived(f.pi, false, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "the re-triggered release removed the acquisition and retired the incarnation", func() bool {
		l := sess.jsa.loadSharedLedger(sess.id, sess.sharedEpoch(), sess.gen)
		if l.status != mqttOutboundValid {
			return false
		}
		if _, listed := l.rec.lists(f.acq.Acq); listed {
			return false
		}
		g := sess.jsa.loadSharedGroup(sess.sharedEpoch(), f.acq.Group)
		return g.status == mqttOutboundValid && g.rec.Live == nil
	})
	testCustodyEventually(t, "the settled verdict forgotten in memory", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		_, held := sess.acqReleases[f.acq.Acq]
		return !held
	})
}

// T6b: a delivery abandoned on an ACL deny owes nothing from the source, so it withholds nothing.
// Mutation M10.
func TestSharedReleaseIgnoresAnAbandonedDelivery(t *testing.T) {
	f := testSharedTransmitted(t, "fence-t6b")
	sess := f.e.sess
	if r := sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply); r != mqttAbandonDone {
		t.Fatalf("the deny established %v, want the durable abandonment", r)
	}
	if _, x := f.exchangeOf(t, f.pi); x == nil || x.Phase != mqttExchangeDeliveryAbandoned {
		t.Fatalf("fixture: the exchange after the deny: %+v", x)
	}
	ok, err := sess.jsa.executeSharedRelease(sess.sharedEpoch(), sess.id, sess.gen, f.acq)
	if !ok || err != nil {
		t.Fatalf("a release whose only dependant is abandoned: ok=%v err=%v", ok, err)
	}
	if g := f.groupRecord(t); g.Live != nil {
		t.Fatalf("the incarnation was not retired: %+v", g)
	}
}

// T8: an inventory with an UNCERTAIN subject — here the one exchange of the generation — withholds
// the release as an error; nothing is released. Mutation M5.
func TestSharedReleaseWithholdsOnAnUncertainInventory(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t8")
	e := r.e
	pi, ok := r.admit(t, r.dep())
	if !ok {
		t.Fatal("fixture: the delivery was not admitted")
	}
	reg, _ := r.exchangeOf(t, pi)
	// The exchange left Publish would withhold nothing; what is under test is the uncertainty alone,
	// so it is withdrawn first: an ended exchange that cannot be READ must still withhold.
	xsubj := mqttExchangeSubject(e.sess.idHash, e.sess.gen, reg.Lin)
	hook := func(subject string) bool { return subject == xsubj }
	mqttTestSharedInventoryUnreadableHookV.Store(&hook)
	defer mqttTestSharedInventoryUnreadableHookV.Store(nil)
	done, err := e.sess.jsa.executeSharedRelease(e.sess.sharedEpoch(), e.sess.id, e.sess.gen, r.acq)
	if done || err == nil || errors.Is(err, errMQTTSharedReleaseWithheld) || !strings.Contains(err.Error(), "uncertain") {
		t.Fatalf("a release over an uncertain inventory: done=%v err=%v", done, err)
	}
	if _, listed := r.ledger(t).lists(r.acq.Acq); !listed {
		t.Fatal("an uncertain inventory released the acquisition")
	}
}

// T9: the verdict sweep runs the same fence: a stable verdict with a Publish dependant is withheld
// by its pass; with no dependant it is released by its pass.
func TestSharedVerdictSweepRunsTheFence(t *testing.T) {
	f := testSharedTransmitted(t, "fence-t9")
	testSaveVerdict(t, f.e.sess, f.acq)
	srv := f.e.asm.server()
	f.e.asm.sweepAcqReleaseVerdicts(srv)
	f.e.asm.sweepAcqReleaseVerdicts(srv)
	if l := f.ledger(t); !l.isClosed(f.acq.Acq) {
		t.Fatalf("the sweep released (or never closed) an acquisition with a Publish dependant: %+v", l)
	}

	r := newTestSharedAdmitRig(t, "fence-t9-free")
	testSaveVerdict(t, r.e.sess, r.acq)
	rsrv := r.e.asm.server()
	r.e.asm.sweepAcqReleaseVerdicts(rsrv)
	r.e.asm.sweepAcqReleaseVerdicts(rsrv)
	if _, listed := r.ledger(t).lists(r.acq.Acq); listed {
		t.Fatal("the sweep did not release a stable verdict with no dependant")
	}
}

// T10: the fence has ONE route. The release and close composers and the fence op are called from
// executeSharedRelease alone, the fence's two helpers likewise.
func TestSharedReleaseFenceHasOneRoute(t *testing.T) {
	want := map[string]string{
		"mqttComposeSharedRelease": "executeSharedRelease",
		"mqttComposeSharedClose":   "executeSharedRelease",
		"fenceSharedRelease":       "executeSharedRelease",
		"sharedReleaseDependants":  "executeSharedRelease",
		"opFenceSharedRelease":     "fenceSharedRelease",
	}
	got := make(map[string]map[string]bool)
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				var callee string
				switch x := n.(type) {
				case *ast.CallExpr:
					switch fn := x.Fun.(type) {
					case *ast.Ident:
						callee = fn.Name
					case *ast.SelectorExpr:
						callee = fn.Sel.Name
					}
				case *ast.SelectorExpr:
					// A method VALUE (not called) is a route too.
					callee = x.Sel.Name
				}
				if _, watched := want[callee]; watched {
					if got[callee] == nil {
						got[callee] = make(map[string]bool)
					}
					got[callee][fd.Name.Name] = true
				}
				return true
			})
		}
	}
	for callee, caller := range want {
		var callers []string
		for c := range got[callee] {
			callers = append(callers, c)
		}
		sort.Strings(callers)
		if len(callers) != 1 || callers[0] != caller {
			t.Errorf("%s is reached from %v; the fence's one route is %s", callee, callers, caller)
		}
	}
}

// T11a: the re-trigger runs only DURABLE verdicts. A verdict standing in memory but not in the
// stored record — a membership removal whose save has not landed — is neither closed nor released.
// Mutation M14.
func TestSharedVerdictKickRunsOnlyDurableVerdicts(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t11a")
	sess := r.e.sess
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the record without the verdict: %v", err)
	}
	sess.mu.Lock()
	sess.acqReleases = map[string]mqttSharedAcqRef{r.acq.Acq: r.acq}
	gen := sess.gen
	sess.kickVerdictsLocked(gen)
	sess.mu.Unlock()
	testCustodyEventually(t, "the kicked run finished", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.verdictKick.running
	})
	l := r.ledger(t)
	if _, listed := l.lists(r.acq.Acq); !listed || l.isClosed(r.acq.Acq) {
		t.Fatalf("a prospective verdict was executed: %+v", l)
	}
}

// T11b: a run bound to one generation does nothing to its successor on the same object: it
// dispatches no release (the stored record is the successor's) and drops nothing from the
// successor's memory, and the successor's own kick still runs. Mutation M15.
func TestSharedVerdictKickIsBoundToItsGeneration(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t11b")
	sess := r.e.sess
	testSaveVerdict(t, sess, r.acq)
	sess.mu.Lock()
	g1 := sess.gen
	sess.mu.Unlock()

	var parkOnce sync.Once
	parked, resume := make(chan struct{}), make(chan struct{})
	pause := func(point, gen string) {
		if point == "before-read" && gen == g1 {
			parkOnce.Do(func() {
				close(parked)
				<-resume
			})
		}
	}
	mqttTestVerdictKickPauseHookV.Store(&pause)
	defer mqttTestVerdictKickPauseHookV.Store(nil)
	var mu sync.Mutex
	dispatched := map[string][]string{}
	exec := func(gen, acq string) {
		mu.Lock()
		dispatched[gen] = append(dispatched[gen], acq)
		mu.Unlock()
	}
	mqttTestVerdictKickExecHookV.Store(&exec)
	defer mqttTestVerdictKickExecHookV.Store(nil)

	sess.mu.Lock()
	sess.kickVerdictsLocked(g1)
	sess.mu.Unlock()
	testRecv(t, "the G1 run parking", parked)

	// THE SAME OBJECT, RE-MINTED: the successor generation and its own pending verdict, stored.
	g2 := mqttNewSessGen()
	x := mqttSharedAcqRef{Group: r.acq.Group, Inc: r.acq.Inc, Acq: mqttNewInflightNonce()}
	sess.mu.Lock()
	sess.gen = g2
	sess.acqReleases = map[string]mqttSharedAcqRef{x.Acq: x}
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the successor's record: %v", err)
	}

	close(resume)
	testCustodyEventually(t, "the G1 run finished", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !(sess.verdictKick.gen == g1 && sess.verdictKick.running)
	})
	mu.Lock()
	stale := append([]string(nil), dispatched[g1]...)
	mu.Unlock()
	if len(stale) != 0 {
		t.Fatalf("the G1 run dispatched %d release(s) after the object became G2's: %v", len(stale), stale)
	}
	sess.mu.Lock()
	_, kept := sess.acqReleases[x.Acq]
	sess.mu.Unlock()
	if !kept {
		t.Fatal("the G1 run dropped the successor's verdict from memory")
	}

	sess.mu.Lock()
	sess.kickVerdictsLocked(g2)
	sess.mu.Unlock()
	testCustodyEventually(t, "the successor's kick executed its own verdict", func() bool {
		mu.Lock()
		n := len(dispatched[g2])
		mu.Unlock()
		sess.mu.Lock()
		_, held := sess.acqReleases[x.Acq]
		sess.mu.Unlock()
		return n > 0 && !held
	})
}

// T11b': the end of a stale run never clears a successor's kick state: a G2 run active with a
// queued `again` is exactly as it was after the G1 run ends. Mutation M17.
func TestSharedVerdictKickStaleRunKeepsTheSuccessorsState(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t11b2")
	sess := r.e.sess
	sess.mu.Lock()
	g1 := sess.gen
	sess.mu.Unlock()
	var parkOnce sync.Once
	parked, resume := make(chan struct{}), make(chan struct{})
	pause := func(point, gen string) {
		if point == "before-read" && gen == g1 {
			parkOnce.Do(func() {
				close(parked)
				<-resume
			})
		}
	}
	mqttTestVerdictKickPauseHookV.Store(&pause)
	defer mqttTestVerdictKickPauseHookV.Store(nil)
	sess.mu.Lock()
	sess.acqReleases = map[string]mqttSharedAcqRef{r.acq.Acq: r.acq}
	sess.kickVerdictsLocked(g1)
	sess.mu.Unlock()
	testRecv(t, "the G1 run parking", parked)
	g2 := mqttNewSessGen()
	sess.mu.Lock()
	sess.gen = g2
	// A G2 run is active and another G2 dependant changed while it ran.
	sess.verdictKick = mqttVerdictKick{gen: g2, running: true, again: true}
	sess.mu.Unlock()
	close(resume)
	time.Sleep(300 * time.Millisecond)
	sess.mu.Lock()
	got := sess.verdictKick
	sess.mu.Unlock()
	if got != (mqttVerdictKick{gen: g2, running: true, again: true}) {
		t.Fatalf("the stale G1 run's end rewrote the successor's kick state to %+v", got)
	}
}

// T11c: a kick arriving while a run executes is not lost. Two Publish dependants on one acquisition:
// the first PUBACK's run withholds behind the second and is parked after executing; the second
// PUBACK's kick arrives meanwhile; the run loops and releases. Mutation M18.
func TestSharedVerdictKickCoalescesWithoutLosingAWakeup(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t11c")
	sess := r.e.sess
	f := &testSharedHandBackFix{testSharedAdmitRig: r}
	testSharedSourceAcksStubbed(f)
	// TWO transmitted deliveries of the group on the same acquisition (the Receive Maximum the
	// single-delivery fixture sets would refuse the second).
	transmit := func() uint16 {
		pi, ok := r.admit(t, r.dep())
		if !ok {
			t.Fatal("fixture: a delivery was not admitted")
		}
		subject := mqttInflightSubject(sess.idHash, sess.gen, pi)
		ml := r.e.asm.jsa.loadOccupantMarkerLoad(subject)
		if err := r.e.asm.deleteInflightMarker(mqttMarkerCallerSender, subject, ml.seq, true); err != nil {
			t.Fatalf("deleting a delivery's marker: %v", err)
		}
		sess.mu.Lock()
		sess.c = r.e.c
		if ent := sess.pendingPublish[pi]; ent != nil {
			ent.transmitted, ent.markerSeq = true, 0
		}
		sess.mu.Unlock()
		return pi
	}
	pi1 := transmit()
	r.sseq = r.e.publishSourceGroup(t, r.dur, "g"+getHashSize("fence-t11c", 8), "foo.bar", "foo.bar")
	pi2 := transmit()
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	testSaveVerdict(t, sess, r.acq)

	var parkOnce sync.Once
	parked, resume := make(chan struct{}), make(chan struct{})
	pause := func(point, g string) {
		if point == "after-execute" && g == gen {
			parkOnce.Do(func() {
				close(parked)
				<-resume
			})
		}
	}
	mqttTestVerdictKickPauseHookV.Store(&pause)
	defer mqttTestVerdictKickPauseHookV.Store(nil)

	if err := r.e.c.mqttProcessPublishReceived(pi1, false, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testRecv(t, "the first run parking after its withheld execution", parked)
	if _, listed := r.ledger(t).lists(r.acq.Acq); !listed {
		t.Fatal("fixture: the first run released despite the second dependant")
	}
	if err := r.e.c.mqttProcessPublishReceived(pi2, false, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "the second dependant's kick queued behind the running run", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.verdictKick.gen == gen && sess.verdictKick.again
	})
	close(resume)
	testCustodyEventually(t, "the looped run released the acquisition", func() bool {
		l := sess.jsa.loadSharedLedger(sess.id, sess.sharedEpoch(), sess.gen)
		if l.status != mqttOutboundValid {
			return false
		}
		_, listed := l.rec.lists(r.acq.Acq)
		return !listed
	})
}

// testPlantSharedAcq gives generation gen of the F-105 env one listed acquisition of a group, as a
// join would, and returns it.
func testPlantSharedAcq(t *testing.T, e *testF105Env, gen, group string) mqttSharedAcqRef {
	t.Helper()
	jsa := &e.asm.jsa
	qkey := mqttSharedGroupKey(group, "foo")
	g := jsa.loadSharedGroup(e.epoch, qkey)
	l := jsa.loadSharedLedger(e.id, e.epoch, gen)
	tenure := mqttSharedTenure{Seq: 1, ID: mqttNewInflightNonce()}
	if l.status == mqttOutboundValid {
		tenure = l.rec.Tenure
	} else {
		// Only an installation creates a ledger (F-105 8b): install the planted tenure first.
		if _, err := jsa.installSharedTenure(e.id, e.epoch, gen, tenure); err != nil {
			t.Fatalf("installing the planted tenure: %v", err)
		}
		l = jsa.loadSharedLedger(e.id, e.epoch, gen)
	}
	b, err := mqttComposeSharedJoin(e.epoch, e.id, gen, qkey, mqttNewInflightNonce(), tenure, g, l,
		mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending))
	if err != nil {
		t.Fatalf("composing the planted join: %v", err)
	}
	if res := jsa.commitSharedLifecycleBatch(b); res.Outcome != mqttSharedWriteLanded {
		t.Fatalf("the planted join did not land: %v %v", res.Outcome, res.Err)
	}
	return b.Acq
}

func testSharedLedgerClosedEmpty(e *testF105Env, gen string) error {
	l := e.asm.jsa.loadSharedLedger(e.id, e.epoch, gen)
	switch {
	case l.status != mqttOutboundValid:
		return fmt.Errorf("the shared ledger reads %v", l.status)
	case l.rec.State != mqttSharedLedgerSealed:
		return fmt.Errorf("the shared ledger is not sealed")
	case len(l.rec.Acqs) != 0:
		return fmt.Errorf("the shared ledger lists %d acquisition(s)", len(l.rec.Acqs))
	}
	return nil
}

// T7e(a) + T7f: a discarded generation's recovery owner seals and releases its shared ledger, and
// does not certify while it reads otherwise. Held closing first: every inventory duty done, the
// closing reports incomplete, no certificate and the root stays; released, it certifies with the
// ledger sealed and empty. Mutations M12, M13.
func TestF105RecoveryClosesTheDiscardedSharedLedgerBeforeCertifying(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-7e-recovery-closing")
	defer done()
	e.bootstrap(t, "txn-1")
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	gen := al.rec.Gen
	a1 := testPlantSharedAcq(t, e, gen, "g7e-a")
	a2 := testPlantSharedAcq(t, e, gen, "g7e-b")
	closed := testDiscardClose(t, e)
	if closed != gen {
		t.Fatalf("fixture: closed %q, planted under %q", closed, gen)
	}

	var held atomic.Bool
	held.Store(true)
	hook := func(root *mqttRecoveryRoot) (bool, bool) {
		if root.ID == e.id && root.Gen == gen && held.Load() {
			return false, true
		}
		return false, false
	}
	mqttTestRecoverySharedClosingHookV.Store(&hook)
	defer mqttTestRecoverySharedClosingHookV.Store(nil)
	for i := 0; i < 4*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}
	if testDiscardCertified(e, gen) {
		t.Fatal("the generation CERTIFIED while its shared closing reported incomplete")
	}
	held.Store(false)
	testDiscardSweepUntilCertified(t, e, gen, func() error { return testSharedLedgerClosedEmpty(e, gen) })
	for _, a := range []mqttSharedAcqRef{a1, a2} {
		g := e.asm.jsa.loadSharedGroup(e.epoch, a.Group)
		if g.status != mqttOutboundValid || g.rec.Live != nil {
			t.Fatalf("group %q still serves after the closing: %+v", a.Group, g.rec)
		}
	}
}

// T7e(b): a persistently failing shared closing never holds the discard back: every obligation is
// discarded across several sweeps while the closing fails, and the root certifies only once the
// closing succeeds. Mutations M13b, M13c.
func TestF105FailingSharedClosingDoesNotStopTheDiscard(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-7e-failing-closing")
	defer done()
	e.bootstrap(t, "txn-1")
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	gen := al.rec.Gen
	testPlantSharedAcq(t, e, gen, "g7e-fail")
	const n = 140
	nonces := make([]string, 0, n)
	for i := 0; i < n; i++ {
		inc := fmt.Sprintf("inc-%03d", i)
		testF105PlantResource(t, e, inc, mqttResourceSourceConsumer)
		nonce, _, _, _ := testF105AdmitQoS(t, e, fmt.Sprintf("txn-adm-%03d", i), uint16(i+1), 1, inc)
		nonces = append(nonces, nonce)
	}
	testDiscardClose(t, e)
	for _, nonce := range nonces {
		testDiscardResolveDutyRaw(t, e, gen, nonce)
	}
	var failing atomic.Bool
	failing.Store(true)
	hook := func(root *mqttRecoveryRoot) (bool, bool) {
		if root.ID == e.id && root.Gen == gen && failing.Load() {
			return false, true
		}
		return false, false
	}
	mqttTestRecoverySharedClosingHookV.Store(&hook)
	defer mqttTestRecoverySharedClosingHookV.Store(nil)
	allDiscarded := func() error {
		for _, nonce := range nonces {
			if x, _ := testF105LoadExchange(t, e, gen, nonce); x.Phase != mqttExchangeDiscarded {
				return fmt.Errorf("obligation %q stands at phase %d", nonce, x.Phase)
			}
		}
		return nil
	}
	// Discarded is not enough: the root must reach its retire-ready state — sweep finished, nothing
	// unresolved — which takes conclusion's ROLL back over the census entries whose discard work
	// remained when first classified. That roll decision must not wait on the failing closing.
	closure, _, _ := e.asm.jsa.loadGenClosure(e.id, e.epoch, gen)
	readyToRetire := func() error {
		root, _, rerr := e.asm.jsa.loadRecoveryRoot(closure.RootShard, closure.RootOrd)
		led, _, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, gen)
		if rerr != nil || lerr != nil || root == nil {
			return fmt.Errorf("root %v ledger %v", rerr, lerr)
		}
		if !mqttRootRetires(root, led) {
			return fmt.Errorf("the root is not ready to retire: resume %d bound %d unresolved %d",
				root.Resume, root.Bound, root.Unresolved)
		}
		return nil
	}
	var derr error
	for i := 0; i < 32*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		if testDiscardCertified(e, gen) {
			t.Fatalf("pass %d: CERTIFIED while the shared closing fails", i)
		}
		if derr = allDiscarded(); derr == nil {
			if derr = readyToRetire(); derr == nil {
				break
			}
		}
	}
	if derr != nil {
		t.Fatalf("the discard stalled behind a failing shared closing: %v", derr)
	}
	failing.Store(false)
	testDiscardSweepUntilCertified(t, e, gen, func() error { return testSharedLedgerClosedEmpty(e, gen) })
}

// T7e(b2) + T7e(c): the closing step is bounded and cancellable — an unaffordable episode is not
// started and a stop ends it after a complete one — and an inline closing racing the recovery's
// converges with no error.
func TestF105SharedClosingStepIsBoundedAndConverges(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-7e-closing-bounded")
	defer done()
	e.bootstrap(t, "txn-1")
	gen := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch).rec.Gen
	for i := 0; i < 3; i++ {
		testPlantSharedAcq(t, e, gen, fmt.Sprintf("g7e-bound-%d", i))
	}
	testDiscardClose(t, e)
	closure, _, _ := e.asm.jsa.loadGenClosure(e.id, e.epoch, gen)
	root, _, err := e.asm.jsa.loadRecoveryRoot(closure.RootShard, closure.RootOrd)
	if err != nil || root == nil {
		t.Fatalf("loading the root: %v", err)
	}
	var releases atomic.Int32
	compose := func(shape mqttSharedShape, _ string) {
		if shape == mqttSharedShapeRelease {
			releases.Add(1)
		}
	}
	mqttTestSharedComposeHookV.Store(&compose)
	defer mqttTestSharedComposeHookV.Store(nil)

	// UNAFFORDABLE: exactly enough for the closure read and the seal, not for one episode.
	b := newMqttRecoveryBudget()
	b.left = b.reserve + mqttSharedClosingClosureUnits + mqttSharedClosingSealUnits + mqttSharedClosingReleaseUnits - 1
	if e.asm.stepSharedClosing(e.asm.server(), root, b, nil) {
		t.Fatal("an unaffordable closing reported complete")
	}
	if n := releases.Load(); n != 0 {
		t.Fatalf("an unaffordable episode was dispatched: %d release(s)", n)
	}
	// STOP after one complete episode.
	episodes := 0
	stop := func() bool {
		episodes++
		return episodes > 1
	}
	if e.asm.stepSharedClosing(e.asm.server(), root, newMqttRecoveryBudget(), stop) {
		t.Fatal("a stopped closing reported complete")
	}
	if n := releases.Load(); n != 1 {
		t.Fatalf("a stop after one episode dispatched %d release(s)", n)
	}
	// CONVERGENCE with the inline closing.
	var wg sync.WaitGroup
	var inlineErr error
	var stepped bool
	wg.Add(2)
	go func() { defer wg.Done(); inlineErr = e.asm.jsa.closeSharedGeneration(e.id, e.epoch, gen) }()
	go func() {
		defer wg.Done()
		stepped = e.asm.stepSharedClosing(e.asm.server(), root, newMqttRecoveryBudget(), nil)
	}()
	wg.Wait()
	if inlineErr != nil {
		t.Fatalf("the inline closing racing the recovery's failed: %v", inlineErr)
	}
	if err := testSharedLedgerClosedEmpty(e, gen); err != nil {
		t.Fatalf("after both closings: %v (recovery step complete=%v)", err, stepped)
	}
}

// T7d: a SEALED ledger releases with no fence — a receiptless teardown (no discarding closure, the
// authority still serving) with a Publish dependant still closes. Mutation M7.
func TestSharedSealedClosingNeedsNoFence(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t7d")
	e := r.e
	if _, ok := r.admit(t, r.dep()); !ok {
		t.Fatal("fixture: the delivery was not admitted")
	}
	if err := e.sess.jsa.requireDiscardingClosure(e.sess.id, e.sess.sharedEpoch(), e.sess.gen); err == nil {
		t.Fatal("fixture: the generation must have no discarding closure")
	}
	if err := e.sess.jsa.closeSharedGeneration(e.sess.id, e.sess.sharedEpoch(), e.sess.gen); err != nil {
		t.Fatalf("the sealed closing of a receiptless teardown: %v", err)
	}
	if g := r.groupRecord(t); g.Live != nil {
		t.Fatalf("the sealed closing did not retire the incarnation: %+v", g)
	}
}

// T7 + T7b: the clean-start HANDOFF releases the predecessor generation's acquisitions — the
// pre-existing gap 7e closes. With the prompt drive parked the periodic pass does it, and the root
// does not certify before the ledger reads sealed and empty. Mutation M12.
func TestF105CleanStartHandoffReleasesThePredecessorsSharedAcquisitions(t *testing.T) {
	const clientID = "f105-7e-cs-shared"
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/g7e/foo", opts: 1}})
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	epoch := asm.sessStreamEpoch
	asm.mu.RUnlock()
	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("no serving authority: %v", al.status)
	}
	oldGen := al.rec.Gen
	l := asm.jsa.loadSharedLedger(clientID, epoch, oldGen)
	if l.status != mqttOutboundValid || len(l.rec.Acqs) != 1 {
		t.Fatalf("fixture: the predecessor must hold one acquisition: %v %+v", l.status, l.rec)
	}
	acq := l.rec.Acqs[0]
	c.Close()
	time.Sleep(150 * time.Millisecond)

	// THE PROMPT DRIVE PARKED: only the periodic pass may close it.
	park := func(id, gen string) bool { return !(id == clientID && gen == oldGen) }
	mqttTestRecoveryKickHookV.Store(&park)
	defer mqttTestRecoveryKickHookV.Store(nil)

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)

	env := &testF105Env{asm: asm, id: clientID, epoch: epoch}
	if err := testSharedLedgerClosedEmpty(env, oldGen); err == nil {
		t.Fatal("fixture: the predecessor's shared ledger closed before any recovery visit")
	}
	testDiscardSweepUntilCertified(t, env, oldGen, func() error { return testSharedLedgerClosedEmpty(env, oldGen) })
	g := asm.jsa.loadSharedGroup(epoch, acq.Group)
	if g.status != mqttOutboundValid || (g.rec.Live != nil && g.rec.Live.Inc == acq.Inc && g.rec.Live.Holders != 0) {
		// The successor holds no acquisition (a clean start re-subscribes nothing), so the
		// predecessor's was the last holder.
		t.Fatalf("the predecessor's incarnation is still held: %+v", g.rec)
	}
}

// T12 (H-1): a durable verdict whose owner run failed is still executed by the verdict sweep
// while a connected session keeps re-saving its record for unrelated reasons: the sweep's
// stability is the verdicts', never the record sequence's. Mutation M19.
func TestSharedVerdictSweepIgnoresUnrelatedRecordChurn(t *testing.T) {
	r := newTestSharedAdmitRig(t, "fence-t12")
	sess := r.e.sess
	testSaveVerdict(t, sess, r.acq)
	srv := r.e.asm.server()
	for pass := 0; pass < 3; pass++ {
		r.e.asm.sweepAcqReleaseVerdicts(srv)
		if _, listed := r.ledger(t).lists(r.acq.Acq); !listed {
			return
		}
		// An unrelated save between passes moves the record's sequence; the verdict stands.
		if err := testSessSave(t, sess); err != nil {
			t.Fatalf("re-saving the record: %v", err)
		}
	}
	t.Fatal("the verdict sweep never executed a durable verdict on a record whose sequence kept moving")
}
