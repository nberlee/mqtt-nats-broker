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

//go:build mqtt_fulltests

package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The client authority's foreground coordinator against the previous generation's cleanup: the
// account's recovery and the previous session object's workers yield to a reused client
// identifier's foreground claim, and resume once it returns (outbound_foreground_priority.go).

// testGoroutineID is the calling goroutine's runtime identifier: the actor an admission and an
// attempt are made by, since an actor is single-flight on one goroutine at a time.
func testGoroutineID() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		b = b[:i]
	}
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// testActorLedger pairs every transition attempt of one client with what licensed it: a standing
// announcement of the committing goroutine (foreground), or the admission that goroutine holds as a
// registered actor. One admission authorizes at most one attempt; a registered actor's attempt
// with no admission, or one consuming an admission granted after a standing announcement, is a
// violation, and so is any window in which more pre-announcement attempts commit than the
// announcement allowed.
type testActorLedger struct {
	mu        sync.Mutex
	id        string
	seq       int
	actors    map[uint64]int
	announced map[uint64]int
	pending   map[uint64]testLedgerAdmission
	windows   map[uint64]*testLedgerWindow // by announcement stamp, open ones only
	errs      []string
	// preAdmitted counts the attempts registered actors committed inside a window on an
	// admission granted before it.
	preAdmitted int
	// onActorAttempt, when set, runs outside the ledger's mutex.
	onActorAttempt func()
	// heldG, once set, is the goroutine whose attempt the test holds; heldRefused closes at the
	// first coordinator refusal that goroutine meets afterwards.
	heldG       uint64
	heldRefused chan struct{}
}

type testLedgerAdmission struct {
	adm uint64
	at  int
}

type testLedgerWindow struct {
	stamp     uint64
	opened    int
	allowance int
	attempts  int
}

func testInstallActorLedger(t *testing.T, id string) *testActorLedger {
	t.Helper()
	l := &testActorLedger{
		id: id, actors: map[uint64]int{}, announced: map[uint64]int{},
		pending: map[uint64]testLedgerAdmission{}, windows: map[uint64]*testLedgerWindow{},
		heldRefused: make(chan struct{}),
	}
	ev := func(e mqttForegroundEvent) {
		if e.ID != id {
			return
		}
		g := testGoroutineID()
		l.mu.Lock()
		defer l.mu.Unlock()
		l.seq++
		switch e.Kind {
		case mqttForegroundRegistered:
			l.actors[g]++
			delete(l.pending, g)
		case mqttForegroundDeregistered:
			l.actors[g]--
			delete(l.pending, g)
		case mqttForegroundAnnounced:
			l.announced[g]++
			l.windows[e.Stamp] = &testLedgerWindow{stamp: e.Stamp, opened: l.seq, allowance: e.Allowance}
		case mqttForegroundReleased:
			l.announced[g]--
			delete(l.windows, e.Stamp)
		case mqttForegroundRefused:
			if l.heldG != 0 && g == l.heldG {
				select {
				case <-l.heldRefused:
				default:
					close(l.heldRefused)
				}
			}
		case mqttForegroundAdmitted:
			if e.N != 0 || len(l.windows) != 0 {
				l.errs = append(l.errs, fmt.Sprintf("admission %d granted while %d announcement(s) stood", e.Admission, len(l.windows)))
			}
			// A NEW ADMISSION REPLACES AN UNUSED ONE: each authorizes at most one attempt.
			l.pending[g] = testLedgerAdmission{adm: e.Admission, at: l.seq}
		}
	}
	att := func(aid string, kind mqttTransitionKind, free bool) {
		if aid != id {
			return
		}
		g := testGoroutineID()
		l.mu.Lock()
		l.seq++
		switch {
		case l.announced[g] > 0:
			// FOREGROUND: licensed by its own announcement.
		case free:
			// AUTHORITY-FREE bookkeeping writes no authority, so no foreground claim can lose to
			// it and no admission is owed for it.
		case l.actors[g] > 0:
			p, ok := l.pending[g]
			delete(l.pending, g)
			if !ok {
				l.errs = append(l.errs, fmt.Sprintf("a registered actor's %s attempt carries no admission", kind))
			}
			hook := l.onActorAttempt
			if hook != nil {
				// THE ATTEMPT COMMITS AFTER THE HOOK: a held attempt is accounted where it lands.
				l.mu.Unlock()
				hook()
				l.mu.Lock()
			}
			for _, w := range l.windows {
				if ok && p.at > w.opened {
					l.errs = append(l.errs, fmt.Sprintf("a %s attempt consumed admission %d, granted after announcement %d", kind, p.adm, w.stamp))
				}
				w.attempts++
				if w.attempts > w.allowance {
					l.errs = append(l.errs, fmt.Sprintf("announcement %d lost %d attempts to admitted background work, allowance %d", w.stamp, w.attempts, w.allowance))
				}
			}
			if len(l.windows) > 0 && ok {
				l.preAdmitted++
			}
		}
		l.mu.Unlock()
	}
	prevEv := mqttTestForegroundEventV.Swap(&ev)
	prevAtt := mqttTestTransitionAttemptV.Swap(&att)
	t.Cleanup(func() {
		mqttTestForegroundEventV.Store(prevEv)
		mqttTestTransitionAttemptV.Store(prevAtt)
	})
	return l
}

func (l *testActorLedger) check(t *testing.T) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.errs {
		t.Error(e)
	}
	l.errs = nil
}

// testKickOutcomes counts the prompt drive's outcomes per generation of one client.
type testKickOutcomes struct {
	mu      sync.Mutex
	charged map[string]int
	refused map[string]int
	gone    map[string]int
}

func testRecordKickOutcomes(t *testing.T, id string) *testKickOutcomes {
	t.Helper()
	k := &testKickOutcomes{charged: map[string]int{}, refused: map[string]int{}, gone: map[string]int{}}
	h := func(kid, gen string, out mqttRecoveryVisitOutcome) {
		if kid != id {
			return
		}
		k.mu.Lock()
		defer k.mu.Unlock()
		switch out {
		case mqttRecoveryVisitCharged:
			k.charged[gen]++
		case mqttRecoveryVisitRefused:
			k.refused[gen]++
		case mqttRecoveryVisitGone:
			k.gone[gen]++
		}
	}
	prev := mqttTestRecoveryKickOutcomeV.Swap(&h)
	t.Cleanup(func() { mqttTestRecoveryKickOutcomeV.Store(prev) })
	return k
}

func (k *testKickOutcomes) get(gen string) (charged, refused, gone int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.charged[gen], k.refused[gen], k.gone[gen]
}

// testFastKickBackoff shortens the prompt drive's refusal backoff for the test's span.
func testFastKickBackoff(t *testing.T) {
	t.Helper()
	lo, hi := mqttRecoveryKickBackoffMin.Get(), mqttRecoveryKickBackoffMax.Get()
	mqttRecoveryKickBackoffMin.Set(time.Millisecond)
	mqttRecoveryKickBackoffMax.Set(2 * time.Millisecond)
	t.Cleanup(func() {
		mqttRecoveryKickBackoffMin.Set(lo)
		mqttRecoveryKickBackoffMax.Set(hi)
	})
}

// testKickQueued reports whether the prompt drive still holds a generation's root.
func testKickQueued(as *mqttAccountSessionManager, id, epoch, gen string) bool {
	rk := &as.recoveryKick
	rk.mu.Lock()
	defer rk.mu.Unlock()
	_, ok := rk.keys[mqttRecoveryKickKey{id: id, epoch: epoch, gen: gen}]
	return ok
}

// TestPubRelClaimYieldsPriorGenerationRecoveryAcrossSessionReplacement (witness A): a clean-session
// client's previous generation is being retired by account recovery when the identifier is reused.
// A recovery visit ENTERED before the new session object's claim announces — registered, its first
// duty step admitted and held at its commit — continues after the announcement over the duties
// that follow. Every recovery attempt carries its own admission, none is admitted while the claim
// stands, at most the announcement's allowance commit inside it, the QoS 2 SUBSCRIBE is granted, and
// the refused cleanup resumes through the prompt drive and completes once the claim returns. The
// visit is the prompt drive's, or the periodic pass's, whose refused visit kicks the root.
func TestPubRelClaimYieldsPriorGenerationRecoveryAcrossSessionReplacement(t *testing.T) {
	t.Run("kicked", func(t *testing.T) { testPriorGenerationRecoveryYields(t, "fg-prior-kicked", false) })
	t.Run("periodic", func(t *testing.T) { testPriorGenerationRecoveryYields(t, "fg-prior-periodic", true) })
}

func testPriorGenerationRecoveryYields(t *testing.T, id string, periodic bool) {
	release := testDiscardParkAll(t)
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Hour
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	if code := testPubRelSubscribeQoS2(t, c, r, "fgp/t"); code != 2 {
		t.Fatalf("fixture: the first generation's QoS 2 SUBSCRIBE was answered %#x", code)
	}
	old := testMQTTResidentSession(t, s, id)
	as := old.asm
	old.mu.Lock()
	oldGen := old.gen
	old.mu.Unlock()
	epoch := as.sessStreamEpoch
	c.Close()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if _, err := as.jsa.loadLastMsgFor(mqttSessStreamName,
			mqttSessStreamSubjectPrefix+as.domainTk+getHash(id)); err == nil {
			return fmt.Errorf("the session record of %q still stands", id)
		}
		return nil
	})

	ledger := testInstallActorLedger(t, id)
	outcomes := testRecordKickOutcomes(t, id)
	entered, announced := make(chan struct{}), make(chan struct{})
	var gateOnce, openOnce sync.Once
	var holdTimedOut atomic.Bool
	// A FAILED RUN RELEASES THE HELD ATTEMPT and stops the periodic driver below.
	t.Cleanup(func() { openOnce.Do(func() { close(announced) }) })
	ledger.mu.Lock()
	ledger.onActorAttempt = func() {
		first := false
		gateOnce.Do(func() { first = true })
		if first {
			// THE VISIT ENTERED: registered, its first step admitted, its attempt held at commit
			// until a foreground operation of the new session object stands.
			g := testGoroutineID()
			ledger.mu.Lock()
			ledger.heldG = g
			ledger.mu.Unlock()
			close(entered)
			select {
			case <-announced:
			case <-time.After(20 * time.Second):
			}
		}
	}
	ledger.mu.Unlock()

	if periodic {
		// THE PERIODIC PASS ENTERS, the prompt drive still parked; it keeps sweeping until the
		// held visit's goroutine is refused.
		stopSweeps := make(chan struct{})
		t.Cleanup(func() { close(stopSweeps) })
		go func() {
			for {
				select {
				case <-ledger.heldRefused:
					return
				case <-stopSweeps:
					return
				default:
				}
				as.sweepOutboundRecovery(s, nil)
				time.Sleep(time.Millisecond)
			}
		}()
	} else {
		release()
		as.kickRecoveryRoot(id, epoch, oldGen)
	}
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("no recovery visit of the previous generation reached an admitted attempt")
	}

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	succ := testMQTTResidentSession(t, s, id)
	if succ == old {
		t.Fatal("fixture: the reused identifier resumed the previous session object")
	}
	// THE FOREGROUND IS HELD OPEN at a seam outside the coordinator's lock: the new object's first
	// announcement releases the held recovery attempt, and the announcing operation stays
	// announced until the visit that attempt belongs to has met the coordinator's refusal.
	var holdOnce sync.Once
	hold := func(on bool, _ uint64) {
		if !on {
			return
		}
		holdOnce.Do(func() {
			openOnce.Do(func() { close(announced) })
			select {
			case <-ledger.heldRefused:
			// Well inside the SUBACK read's deadline, so a recovery that never yields is
			// reported as such rather than as a read timeout.
			case <-time.After(3 * time.Second):
				holdTimedOut.Store(true)
			}
		})
	}
	succ.testForegroundAnnounce.Store(&hold)
	defer succ.testForegroundAnnounce.Store(nil)
	if code := testPubRelSubscribeQoS2(t, c2, r2, "fgp/t"); code != 2 {
		t.Fatalf("the reused identifier's QoS 2 SUBSCRIBE was answered %#x while the previous generation's "+
			"recovery ran", code)
	}
	// THE LEDGER FIRST: what the claim's windows saw is the property; the fixture checks below
	// only say the scenario was reached.
	ledger.check(t)
	select {
	case <-announced:
	default:
		t.Fatal("fixture: the held recovery attempt was never released by an announcement")
	}
	if holdTimedOut.Load() {
		t.Fatal("the recovery visit entered before the claim was never refused while the claim stood")
	}
	ledger.mu.Lock()
	pre := ledger.preAdmitted
	ledger.mu.Unlock()
	if pre == 0 {
		t.Fatal("fixture: no pre-announcement recovery attempt committed inside a claim's window")
	}
	// THE VISIT CONTINUED past the announcement and was refused there, not charged: the prompt
	// drive saw the refusal, or the periodic pass handed the refused root to the prompt drive.
	checkFor(t, 20*time.Second, 20*time.Millisecond, func() error {
		if periodic {
			if !testKickQueued(as, id, epoch, oldGen) {
				return fmt.Errorf("the refused periodic visit did not kick the previous generation's root")
			}
			return nil
		}
		if _, refused, _ := outcomes.get(oldGen); refused == 0 {
			return fmt.Errorf("the previous generation's recovery was never refused by the claim")
		}
		return nil
	})
	if periodic {
		release()
		as.kickRecoveryRoot(id, epoch, oldGen)
	}
	// THE REFUSED CLEANUP RESUMES through the prompt drive alone (the periodic pass is parked).
	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		cert, err := as.jsa.loadGenCompletionCert(id, epoch, oldGen)
		if err != nil || cert == nil {
			return fmt.Errorf("the previous generation %q has not certified: %v", oldGen, err)
		}
		return nil
	})
	ledger.check(t)
}

// TestPubRelClaimRefusesRecoveryInItsContentionWait (witness C): the claim's first attempt loses
// to a legitimately pre-admitted background attempt; during the contention wait before its next
// attempt the claim is still announced, so recovery offering work there is refused, at its entry
// and at an attempt alike.
func TestPubRelClaimRefusesRecoveryInItsContentionWait(t *testing.T) {
	rig := newTestPubRelClaimRig(t, "fg-retry-gap")
	as, id := rig.sess.asm, rig.sess.id
	dereg := as.registerForegroundActor(id)
	defer dereg()
	if _, _, idle := as.admitForegroundOrIdle(id); idle != nil {
		t.Fatal("fixture: the background attempt could not be admitted before the claim")
	}
	var offers, admitted atomic.Int32
	var lost atomic.Bool
	as.mu.Lock()
	as.testAuthorityContentionWait = func(time.Duration) {
		if !lost.Load() {
			return
		}
		offers.Add(1)
		if !as.foregroundAnnounced(id) {
			admitted.Add(1)
		}
		if as.admitRecoveryAttempt(id, newMqttRecoveryBudget()) {
			admitted.Add(1)
		}
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testAuthorityContentionWait = nil
		as.mu.Unlock()
	})
	attempts := rig.presend(t, func(k int32) {
		if k == 1 {
			// THE PRE-ADMITTED ATTEMPT COMMITS inside the claim's first attempt.
			if err := rig.bump(_EMPTY_); err != nil {
				t.Errorf("committing the pre-admitted attempt: %v", err)
			}
			lost.Store(true)
		}
	})
	if code := testPubRelSubscribeQoS2(t, rig.c, rig.r, "fgc/t"); code != 2 {
		t.Fatalf("the QoS 2 SUBSCRIBE was answered %#x", code)
	}
	if n := attempts.Load(); n < 2 {
		t.Fatalf("fixture: the claim made %d attempt(s), so it never waited", n)
	}
	if offers.Load() == 0 {
		t.Fatal("fixture: recovery offered no work in the claim's contention wait")
	}
	if n := admitted.Load(); n != 0 {
		t.Fatalf("recovery was let in %d time(s) during the claim's contention wait", n)
	}
}

// testPlantRaw stores body at subject, create-only.
func testPlantRaw(t *testing.T, e *testOutboundEnv, subject string, body any) uint64 {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshalling %s: %v", subject, err)
	}
	msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
	resp, serr := e.asm.jsa.storeMsg(subject, hdr, msg)
	if serr != nil {
		t.Fatalf("planting %s: %v", subject, serr)
	}
	return resp.Sequence
}

// testPlantPoisedRoot enrols a discarded generation of the env's client whose sweep has finished:
// unresolved > 0 poises it to ROLL, 0 to CONCLUDE (its ledger sealed at the bound, nothing below
// the floor). Returns the root's sequence.
func testPlantPoisedRoot(t *testing.T, e *testOutboundEnv, gen string, ord, unresolved uint64) uint64 {
	t.Helper()
	testEnrolPlantedOutstanding(t, e, gen)
	shard := mqttDirShardFor(getHash(e.id))
	testPlantClosureAt(t, e, gen, shard, ord)
	testPlantRaw(t, e, mqttGenLedgerSubject(getHash(e.id), gen), &mqttGenLedger{
		Lo: 1, Del: 1, ID: e.id, Epoch: e.epoch, Gen: gen, State: mqttGenLedgerSealed, Hi: 2, SealedHi: 2,
	})
	return testPlantRoot(t, e, &mqttRecoveryRoot{
		Shard: shard, Ord: ord, ID: e.id, Epoch: e.epoch, Gen: gen,
		SweepStart: 1, Resume: 3, Bound: 2, Unresolved: unresolved,
	})
}

// testEnrolPlantedOutstanding gives a planted discarded generation the authority state its claim
// would have left (issue #133): the client's lifecycle closed, gen outstanding at phase open with
// its release landed. Without it the generation reads unresolved and its records are never written.
func testEnrolPlantedOutstanding(t *testing.T, e *testOutboundEnv, gen string) {
	t.Helper()
	if al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); al.status == mqttOutboundAbsent {
		e.bootstrap(t, "txn-boot")
	}
	_, ord, ok := mqttParseGenToken(gen)
	if !ok {
		t.Fatalf("fixture: %q is no generation token", gen)
	}
	testRewriteOutboundAuthority(t, e, func(a *mqttOutboundAuthority) {
		a.State, a.Life.Phase, a.Life.Pending = mqttAuthorityClosed, mqttLifecycleClosed, nil
		if a.Life.NextGenOrd <= ord {
			a.Life.NextGenOrd = ord + 1
		}
		if mqttOutstandingIndex(a.Life, ord) < 0 {
			a.Life.Outstanding = append(a.Life.Outstanding, mqttOutstandingGen{Ord: ord, Phase: mqttOutstandingOpen,
				Teardown: mqttOutstandingTeardown{Kind: mqttTeardownMarker, Nonce: "nonce-" + gen, MarkerWID: "wid-" + gen}})
		}
	})
}

// testSharedClosingComplete answers every shared closing of the client complete.
func testSharedClosingComplete(t *testing.T, id string) {
	t.Helper()
	h := func(root *mqttRecoveryRoot) (bool, bool) { return true, root.ID == id }
	prev := mqttTestRecoverySharedClosingHookV.Swap(&h)
	t.Cleanup(func() { mqttTestRecoverySharedClosingHookV.Store(prev) })
}

func testRootSeq(t *testing.T, e *testOutboundEnv, ord uint64) uint64 {
	t.Helper()
	r, seq, err := e.asm.jsa.loadRecoveryRoot(mqttDirShardFor(getHash(e.id)), ord)
	if err != nil {
		t.Fatalf("reading root %d: %v", ord, err)
	}
	if r == nil {
		return 0
	}
	return seq
}

// testCountAttempts counts every transition attempt of one client.
func testCountAttempts(t *testing.T, id string) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	h := func(aid string, _ mqttTransitionKind, _ bool) {
		if aid == id {
			n.Add(1)
		}
	}
	prev := mqttTestTransitionAttemptV.Swap(&h)
	t.Cleanup(func() { mqttTestTransitionAttemptV.Store(prev) })
	return &n
}

// TestRefusedKickedVisitsAreNeverCharged (witness E): two kicked roots of an announced client,
// one poised to ROLL and one poised to CONCLUDE, are visited far beyond the prompt drive's
// allowance while the announcement stands; no visit is charged and no root work happens. Released,
// with the periodic pass disabled, the prompt drive alone concludes the one and rolls the other.
func TestRefusedKickedVisitsAreNeverCharged(t *testing.T) {
	t.Cleanup(setMQTTSessionExpirySweepInterval(time.Hour))
	e, done := testNewEnv(t, "fg-poised")
	defer done()
	testFastKickBackoff(t)
	testSharedClosingComplete(t, e.id)
	concludeSeq := testPlantPoisedRoot(t, e, testGen, 1, 0)
	rollSeq := testPlantPoisedRoot(t, e, testGen2, 2, 1)
	outcomes := testRecordKickOutcomes(t, e.id)
	attempts := testCountAttempts(t, e.id)

	h, _ := e.asm.announceForeground(e.id)
	e.asm.kickRecoveryRoot(e.id, e.epoch, testGen)
	e.asm.kickRecoveryRoot(e.id, e.epoch, testGen2)
	beyond := mqttRecoveryKickMaxVisits + 8
	checkFor(t, 30*time.Second, 5*time.Millisecond, func() error {
		_, rc, _ := outcomes.get(testGen)
		_, rr, _ := outcomes.get(testGen2)
		if rc < beyond || rr < beyond {
			return fmt.Errorf("refused visits so far: conclude %d, roll %d; want %d each", rc, rr, beyond)
		}
		return nil
	})
	for _, gen := range []string{testGen, testGen2} {
		if ch, _, gone := outcomes.get(gen); ch != 0 || gone != 0 {
			t.Fatalf("generation %q: %d visits charged and %d gone under a standing announcement", gen, ch, gone)
		}
		if !testKickQueued(e.asm, e.id, e.epoch, gen) {
			t.Fatalf("generation %q left the prompt drive under a standing announcement", gen)
		}
	}
	if got := testRootSeq(t, e, 1); got != concludeSeq {
		t.Fatalf("the root poised to conclude was written (%d -> %d) under a standing announcement", concludeSeq, got)
	}
	if got := testRootSeq(t, e, 2); got != rollSeq {
		t.Fatalf("the root poised to roll was written (%d -> %d) under a standing announcement", rollSeq, got)
	}
	if testDiscardCertified(e, testGen) {
		t.Fatal("the generation certified under a standing announcement")
	}
	if n := attempts.Load(); n != 0 {
		t.Fatalf("%d transition attempts of the client were made under a standing announcement", n)
	}

	h.release()
	checkFor(t, 20*time.Second, 10*time.Millisecond, func() error {
		if !testDiscardCertified(e, testGen) || testRootSeq(t, e, 1) != 0 {
			return fmt.Errorf("the root poised to conclude has not concluded")
		}
		if got := testRootSeq(t, e, 2); got == rollSeq {
			return fmt.Errorf("the root poised to roll has not rolled")
		}
		return nil
	})
}

// TestRefusedPeriodicVisitKicksItsRoot (witness F): the periodic pass visits a discarded
// generation's root while its client is announced, with the prompt drive idle; the refused visit
// hands the root to the prompt drive, which concludes it once the announcement is released with
// the periodic pass disabled.
func TestRefusedPeriodicVisitKicksItsRoot(t *testing.T) {
	t.Cleanup(setMQTTSessionExpirySweepInterval(time.Hour))
	e, done := testNewEnv(t, "fg-periodic")
	defer done()
	testFastKickBackoff(t)
	testSharedClosingComplete(t, e.id)
	shard := mqttDirShardFor(getHash(e.id))
	testPlantPoisedRoot(t, e, testGen, 1, 0)
	testPlantShard(t, e, shard, 1)
	outcomes := testRecordKickOutcomes(t, e.id)

	var visits atomic.Int32
	vh := func() { visits.Add(1) }
	prevVisit := mqttTestRecoveryRootVisitHookV.Swap(&vh)
	t.Cleanup(func() { mqttTestRecoveryRootVisitHookV.Store(prevVisit) })
	h, _ := e.asm.announceForeground(e.id)
	for i := 0; i < 2*mqttDirShards && visits.Load() == 0; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}
	if visits.Load() == 0 {
		t.Fatal("fixture: the periodic pass never walked the root's shard")
	}
	if testDiscardCertified(e, testGen) {
		t.Fatal("the periodic pass concluded a root its client's foreground refused")
	}
	h.release()
	checkFor(t, 20*time.Second, 10*time.Millisecond, func() error {
		if !testDiscardCertified(e, testGen) {
			_, refused, _ := outcomes.get(testGen)
			return fmt.Errorf("the refused root never concluded (%d refused prompt visits)", refused)
		}
		return nil
	})
}

// TestFloorAdvancementAdmitsAfterThePositionDeletes (witness G): an announcement landing after a
// visit's entry and after its conclusion's position deletes, before the floor advancement, refuses
// that advancement; the root stays unconcluded and unrolled, and the next visit after the release
// advances the floor.
func TestFloorAdvancementAdmitsAfterThePositionDeletes(t *testing.T) {
	t.Cleanup(setMQTTSessionExpirySweepInterval(time.Hour))
	e, done := testFloorEnv(t, "fg-floor")
	defer done()
	jsa := &e.asm.jsa
	for pi := uint16(1); pi <= 2; pi++ {
		testEndAndResolve(t, e, testFloorAdmit(t, e, pi))
	}
	led, _ := testFloorLedger(t, e)
	for i := 0; i < 8; i++ {
		f, err := jsa.composeFloorBatch(e.id, e.epoch, testGen, led.Hi+1)
		if err != nil {
			t.Fatalf("composing a floor batch: %v", err)
		}
		if f == nil || len(f.Positions) == 0 {
			break
		}
		if err := jsa.opAdvanceFloor(f, mqttNewInflightNonce()); err != nil {
			t.Fatalf("advancing the floor: %v", err)
		}
	}
	led, _ = testFloorLedger(t, e)
	if led.Del >= led.Lo {
		t.Fatalf("fixture: nothing below the floor awaits deletion: %+v", led)
	}
	root := testEnvRoot(e, 1, led.Hi+1, led.Hi, 0)
	rootSeq := testPlantRoot(t, e, root)

	var fg atomic.Pointer[mqttForegroundHandle]
	seam := func(id, gen string) {
		if id != e.id || fg.Load() != nil {
			return
		}
		h, _ := e.asm.announceForeground(e.id)
		fg.Store(&h)
	}
	prev := mqttTestFloorAdvanceAdmitV.Swap(&seam)
	t.Cleanup(func() { mqttTestFloorAdvanceAdmitV.Store(prev) })
	var floors atomic.Int32
	att := func(id string, kind mqttTransitionKind, _ bool) {
		if id == e.id && kind == mqttTransitionAdvanceFloor {
			floors.Add(1)
		}
	}
	prevAtt := mqttTestTransitionAttemptV.Swap(&att)
	t.Cleanup(func() { mqttTestTransitionAttemptV.Store(prevAtt) })

	refused := e.asm.visitRecoveryRoot(e.asm.server(), root, rootSeq, newMqttRecoveryBudget(), nil)
	h := fg.Load()
	if h == nil {
		t.Fatal("fixture: the visit never reached the seam before its floor advancement")
	}
	if !refused {
		t.Fatal("a visit whose floor advancement met a standing announcement did not report itself refused")
	}
	if n := floors.Load(); n != 0 {
		t.Fatalf("%d floor advancements were attempted under the announcement", n)
	}
	after, _ := testFloorLedger(t, e)
	if after.Del != led.Del {
		t.Fatalf("the floor advanced (%d -> %d) under the announcement", led.Del, after.Del)
	}
	if got := testRootSeq(t, e, root.Ord); got != rootSeq {
		t.Fatalf("the root was written (%d -> %d) by a visit whose floor was refused", rootSeq, got)
	}
	h.release()
	if e.asm.visitRecoveryRoot(e.asm.server(), root, rootSeq, newMqttRecoveryBudget(), nil) {
		t.Fatal("the visit after the release was refused")
	}
	if after, _ = testFloorLedger(t, e); after.Del != after.Lo {
		t.Fatalf("the floor did not advance after the release: %+v", after)
	}
	if got := testRootSeq(t, e, root.Ord); got == rootSeq {
		t.Fatal("the root was not rolled after its floor completed")
	}
}

// TestUnreadableKickedClosureIsChargedUnderAnAnnouncement: a kicked root whose closure cannot be
// read is charged on every attempt — the read fails before the visit's entry, and a standing
// announcement exempts nothing — so the key exhausts its allowance and leaves the prompt drive.
func TestUnreadableKickedClosureIsChargedUnderAnAnnouncement(t *testing.T) {
	t.Cleanup(setMQTTSessionExpirySweepInterval(time.Hour))
	e, done := testNewEnv(t, "fg-bad-closure")
	defer done()
	testFastKickBackoff(t)
	msg, hdr := mqttFrameBatchWrite(0, []byte("{not a closure"), _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttGenClosureSubject(getHash(e.id), testGen), hdr, msg); err != nil {
		t.Fatalf("planting the unreadable closure: %v", err)
	}
	testKickExhaustsUnderAnnouncement(t, e)
}

// TestUnreadableKickedRootIsChargedUnderAnAnnouncement: as above, for a discarding closure whose
// root cannot be read.
func TestUnreadableKickedRootIsChargedUnderAnAnnouncement(t *testing.T) {
	t.Cleanup(setMQTTSessionExpirySweepInterval(time.Hour))
	e, done := testNewEnv(t, "fg-bad-root")
	defer done()
	testFastKickBackoff(t)
	shard := mqttDirShardFor(getHash(e.id))
	testPlantClosureAt(t, e, testGen, shard, 1)
	msg, hdr := mqttFrameBatchWrite(0, []byte("{not a root"), _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttRootSubject(shard, 1), hdr, msg); err != nil {
		t.Fatalf("planting the unreadable root: %v", err)
	}
	testKickExhaustsUnderAnnouncement(t, e)
}

func testKickExhaustsUnderAnnouncement(t *testing.T, e *testOutboundEnv) {
	t.Helper()
	outcomes := testRecordKickOutcomes(t, e.id)
	h, _ := e.asm.announceForeground(e.id)
	defer h.release()
	e.asm.kickRecoveryRoot(e.id, e.epoch, testGen)
	checkFor(t, 20*time.Second, 5*time.Millisecond, func() error {
		if testKickQueued(e.asm, e.id, e.epoch, testGen) {
			ch, rf, _ := outcomes.get(testGen)
			return fmt.Errorf("the key is still queued after %d charged and %d refused attempts", ch, rf)
		}
		return nil
	})
	ch, rf, gone := outcomes.get(testGen)
	if ch != mqttRecoveryKickMaxVisits || rf != 0 || gone != 0 {
		t.Fatalf("the unreadable key left after %d charged, %d refused and %d gone attempts; want exactly %d charged",
			ch, rf, gone, mqttRecoveryKickMaxVisits)
	}
}

// TestRefusedKickedRootKeepsItsSlotWhileVisited: while the prompt drive visits a root, kicks fill
// every other slot of its bounded set; the visit is then refused. The refused root, which has
// visits left, keeps the slot it held — the cap counts it while it is out of the queue — and is
// visited again once due, instead of being dropped to the periodic pass.
func TestRefusedKickedRootKeepsItsSlotWhileVisited(t *testing.T) {
	t.Cleanup(setMQTTSessionExpirySweepInterval(time.Hour))
	e, done := testNewEnv(t, "fg-kick-slot")
	defer done()
	testFastKickBackoff(t)
	inVisit, release := make(chan struct{}), make(chan struct{})
	var heldVisits atomic.Int32
	var once sync.Once
	restore := setMQTTTestRecoveryKickVisitOutcome(func(id, gen string) mqttRecoveryVisitOutcome {
		if gen != "held" {
			return mqttRecoveryVisitGone
		}
		if heldVisits.Add(1) == 1 {
			once.Do(func() { close(inVisit) })
			<-release
			return mqttRecoveryVisitRefused
		}
		return mqttRecoveryVisitGone
	})
	defer restore()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	e.asm.kickRecoveryRoot(e.id, e.epoch, "held")
	select {
	case <-inVisit:
	case <-time.After(10 * time.Second):
		t.Fatal("the prompt drive never visited the root")
	}
	for i := 0; i < mqttRecoveryKickCap+5; i++ {
		e.asm.kickRecoveryRoot(e.id, e.epoch, fmt.Sprintf("fill%03d", i))
	}
	rk := &e.asm.recoveryKick
	rk.mu.Lock()
	held := len(rk.keys)
	rk.mu.Unlock()
	if held != mqttRecoveryKickCap {
		t.Errorf("the drive holds %d roots while visiting one, want its cap %d", held, mqttRecoveryKickCap)
	}
	close(release)
	checkFor(t, 10*time.Second, 5*time.Millisecond, func() error {
		if n := heldVisits.Load(); n < 2 {
			if !testKickQueued(e.asm, e.id, e.epoch, "held") {
				return fmt.Errorf("the refused root left the prompt drive after %d visit(s)", n)
			}
			return fmt.Errorf("the refused root has been visited %d time(s), want a second", n)
		}
		return nil
	})
}
