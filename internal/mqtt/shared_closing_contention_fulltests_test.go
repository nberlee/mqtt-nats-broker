//go:build mqtt_fulltests

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
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A clean close's shared closing settles against a competing closer whose write is still in process
// (issue #178). JetStream answers 10164 while a proposal for the ledger subject is in process, and
// the ledger reads as it was until the entry is applied; a single server never produces that
// window on demand, so the send seam answers the closing's writes with the store's real refusal
// while the ledger stays as it stands, and the test performs the competitor's write itself. Every
// recovery caller is parked, so the closing under test is the only closer and every refusal and
// wait counted is its own.

// testContendedSends answers one shape's sends with a refusal until the competitor is applied.
type testContendedSends struct {
	mu        sync.Mutex
	shape     mqttSharedShape
	refusal   *ApiError
	armed     bool
	limit     int // refusals before the competitor is applied; 0 refuses for ever
	refusals  int
	byAcq     map[string]int
	competing atomic.Bool
	compete   func()
	// pass, when set, lets one send through instead of refusing it.
	pass func(acq string) bool
}

func (cs *testContendedSends) hook(shape mqttSharedShape, acq string) (*JSPubAckResponse, error, bool) {
	if cs.competing.Load() {
		return nil, nil, false
	}
	cs.mu.Lock()
	if !cs.armed || shape != cs.shape || (cs.pass != nil && cs.pass(acq)) {
		cs.mu.Unlock()
		return nil, nil, false
	}
	cs.refusals++
	cs.byAcq[acq]++
	fire := cs.limit > 0 && cs.refusals == cs.limit
	if fire {
		cs.armed = false
	}
	cs.mu.Unlock()
	if fire && cs.compete != nil {
		// The competitor's write is applied now; this send was refused while it was in process.
		cs.competing.Store(true)
		cs.compete()
		cs.competing.Store(false)
	}
	// The refusal arrives as the store answers it: inside the acknowledgement.
	return &JSPubAckResponse{Error: cs.refusal}, nil, true
}

func (cs *testContendedSends) arm() {
	cs.mu.Lock()
	cs.armed = true
	cs.mu.Unlock()
}

func (cs *testContendedSends) counts() (int, map[string]int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	by := make(map[string]int, len(cs.byAcq))
	for k, v := range cs.byAcq {
		by[k] = v
	}
	return cs.refusals, by
}

// testContend installs the send seam, a counted wait and a clock the waits advance, so the
// allowance is spent by the waits the closing takes and by nothing else.
func testContend(t *testing.T, shape mqttSharedShape, refusal *ApiError, limit int) (*testContendedSends, *atomic.Int64) {
	t.Helper()
	cs := &testContendedSends{shape: shape, refusal: refusal, limit: limit, byAcq: map[string]int{}}
	h := cs.hook
	prev := mqttTestSharedSendHookV.Load()
	mqttTestSharedSendHookV.Store(&h)
	waits := new(atomic.Int64)
	var elapsed atomic.Int64
	base := time.Now()
	mqttTestSharedContentionClockV.Set(func() time.Time { return base.Add(time.Duration(elapsed.Load())) })
	mqttTestSharedContentionWaitV.Set(func(d time.Duration) {
		waits.Add(1)
		elapsed.Add(int64(d))
	})
	t.Cleanup(func() {
		mqttTestSharedSendHookV.Store(prev)
		mqttTestSharedContentionClockV.Set(nil)
		mqttTestSharedContentionWaitV.Set(nil)
		mqttTestSharedContentionAllowanceV.Set(0)
	})
	return cs, waits
}

func testSessionRecordStands(f *testDiscardFix) bool {
	_, err := f.asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessStreamSubjectPrefix+f.asm.domainTk+getHash(f.id))
	return err == nil
}

func testLedgerSealedEmpty(f *testDiscardFix) error {
	l := f.asm.jsa.loadSharedLedger(f.id, f.epoch(), f.gen)
	if l.status != mqttOutboundValid || l.rec.State != mqttSharedLedgerSealed || len(l.rec.Acqs) != 0 {
		return fmt.Errorf("the ledger of %q does not read sealed and empty: %v %+v", f.gen, l.status, l.rec)
	}
	return nil
}

// testCompetingSeal is the competing closer's seal, written through the production composer.
func testCompetingSeal(t *testing.T, f **testDiscardFix) func() {
	return func() {
		fx := *f
		if _, err := fx.asm.jsa.sealSharedLedger(fx.id, fx.epoch(), fx.gen, nil); err != nil {
			t.Errorf("the competing seal: %v", err)
		}
	}
}

// The inline seal is refused six times — more than the four attempts an episode counts — while a
// competing closer's seal is in process, then the competitor's seal is applied: the closing reads
// it as its own seal and the teardown completes, releasing the record. Run against 10164 and
// against 10071, the two refusals the store answers at the two points of the competitor's commit.
//
// M1 (contention counted against the four attempts): the seal gives up, the record stays marked.
// M3 (10164 not classified as contention): the 10164 case gives up.
func TestTheClosingSealOutlastsACompetitorsInProcessWrite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		refusal *ApiError
	}{
		{"10164", ApiErrors[JSStreamWrongLastSequenceConstantErr]},
		{"10071", ApiErrors[JSStreamWrongLastSequenceErrF]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := testDiscardParkAll(t)
			const k = 6
			var fx *testDiscardFix
			cs, waits := testContend(t, mqttSharedShapeSeal, tc.refusal, k)
			cs.compete = testCompetingSeal(t, &fx)
			fx = testDiscardTearDownAfter(t, "seal-contended-"+tc.name, 1, false, func(f *testDiscardFix) {
				fx = f
				if !f.consumerExists(t, mqttStreamName, f.src) {
					t.Fatalf("fixture: the source consumer %q does not exist before the close", f.src)
				}
				cs.arm()
			})
			// The fixture returned only once the record was released: the teardown completed.
			if n, _ := cs.counts(); n != k {
				t.Fatalf("the closing's seal was refused %d time(s), want %d", n, k)
			}
			if w := waits.Load(); w != k {
				t.Fatalf("the closing took %d contention wait(s), want one per refusal (%d)", w, k)
			}
			if err := testLedgerSealedEmpty(fx); err != nil {
				t.Fatal(err)
			}
			// The consumers the teardown transferred are recovery's: resume it and they go.
			release()
			fx.asm.kickRecoveryRoot(fx.id, fx.epoch(), fx.gen)
			checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
				return fx.retired(t, mqttOwnedStreamMsgs, mqttStreamName, fx.src)
			})
		})
	}
}

// The competitor's seal is applied between the closing's ledger read and its batch: the write
// boundary refuses the batch for its stale prior. That refusal is contention, so the closing waits
// before the re-read that finds the seal standing.
//
// M2 (the stale prior not classified as contention): no wait is taken.
func TestAStalePriorSealRefusalWaitsBeforeItsReRead(t *testing.T) {
	testDiscardParkAll(t)
	_, waits := testContend(t, mqttSharedShapeSeal, nil, 0)
	var fx *testDiscardFix
	var competed atomic.Bool
	prev := mqttTestSharedPauseHookV.Load()
	pause := func(point string) {
		if point != "seal:before-send" || fx == nil {
			return
		}
		// Once, and not from inside the competitor's own seal, which passes this point too.
		if competed.CompareAndSwap(false, true) {
			if _, err := fx.asm.jsa.sealSharedLedger(fx.id, fx.epoch(), fx.gen, nil); err != nil {
				t.Errorf("the competing seal: %v", err)
			}
		}
	}
	mqttTestSharedPauseHookV.Store(&pause)
	t.Cleanup(func() { mqttTestSharedPauseHookV.Store(prev) })
	fx = testDiscardTearDownAfter(t, "seal-stale-prior", 1, false, func(f *testDiscardFix) { fx = f })
	if w := waits.Load(); w != 1 {
		t.Fatalf("the stale-prior refusal took %d contention wait(s), want 1", w)
	}
	if err := testLedgerSealedEmpty(fx); err != nil {
		t.Fatal(err)
	}
}

// testSharedCloser is a clean-session client holding shared subscriptions, beside a peer that
// holds the same groups, so a release decrements a group rather than emptying it.
type testSharedCloser struct {
	f    *testDiscardFix
	c    net.Conn
	qkey map[string]string // filter -> group key
}

func testConnectSharedCloser(t *testing.T, id string, filters ...string) *testSharedCloser {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Hour
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	subs := func(cid string) net.Conn {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		t.Cleanup(func() { c.Close() })
		if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted {
			t.Fatalf("fixture: CONNECT of %q answered %#x", cid, rc)
		}
		var fs []mqttV5SubFilter
		for _, f := range filters {
			fs = append(fs, mqttV5SubFilter{topic: f, opts: 1})
		}
		for i, code := range testMQTTSubV5(t, c, r, 1, fs) {
			if code != 1 {
				t.Fatalf("fixture: SUBSCRIBE %q of %q answered %#x", filters[i], cid, code)
			}
		}
		return c
	}
	subs(id + "-peer")
	c := subs(id)
	sess := testMQTTResidentSession(t, s, id)
	sc := &testSharedCloser{f: &testDiscardFix{s: s, asm: sess.asm, id: id}, c: c, qkey: map[string]string{}}
	sess.mu.Lock()
	sc.f.gen = sess.gen
	sess.mu.Unlock()
	l := sc.f.asm.jsa.loadSharedLedger(id, sc.f.epoch(), sc.f.gen)
	if l.status != mqttOutboundValid || len(l.rec.Acqs) != len(filters) {
		t.Fatalf("fixture: the ledger lists %d acquisition(s), want %d: %v %+v", mqttSharedLedgerLen(l), len(filters), l.status, l.rec)
	}
	for _, a := range l.rec.Acqs {
		g := sc.f.asm.jsa.loadSharedGroup(sc.f.epoch(), a.Group)
		if g.status != mqttOutboundValid || g.rec.Live == nil || g.rec.Live.Holders != 2 {
			t.Fatalf("fixture: group %q is not held by both clients: %v %+v", a.Group, g.status, g.rec)
		}
	}
	return sc
}

func (sc *testSharedCloser) acqs(t *testing.T) []mqttSharedAcqRef {
	t.Helper()
	l := sc.f.asm.jsa.loadSharedLedger(sc.f.id, sc.f.epoch(), sc.f.gen)
	if l.status != mqttOutboundValid {
		t.Fatalf("the ledger reads %v", l.status)
	}
	return l.rec.Acqs
}

func (sc *testSharedCloser) holders(t *testing.T, group string) uint64 {
	t.Helper()
	g := sc.f.asm.jsa.loadSharedGroup(sc.f.epoch(), group)
	if g.status != mqttOutboundValid || g.rec.Live == nil {
		t.Fatalf("group %q reads %v %+v", group, g.status, g.rec)
	}
	return g.rec.Live.Holders
}

// The seal lands; the release of the one acquisition is refused six times while a competing
// closer's release of the same entry is in process, then the competitor's release is applied. The
// closing reads the entry unlisted, the teardown completes, and the group lost exactly one holder.
//
// M1 / M7 (the release counts contention against its four attempts): the release gives up, and T4
// does not mask it because the competitor's release is not applied until the sixth refusal.
func TestTheClosingReleaseOutlastsACompetitorsInProcessRelease(t *testing.T) {
	testDiscardParkAll(t)
	sc := testConnectSharedCloser(t, "release-contended", "$share/g/contended/topic")
	acq := sc.acqs(t)[0]
	const k = 6
	cs, waits := testContend(t, mqttSharedShapeRelease, ApiErrors[JSStreamWrongLastSequenceConstantErr], k)
	cs.compete = func() {
		if ok, err := sc.f.asm.jsa.executeSharedRelease(sc.f.epoch(), sc.f.id, sc.f.gen, acq, nil); err != nil || !ok {
			t.Errorf("the competing release: %v %v", ok, err)
		}
	}
	cs.arm()
	sc.c.Close()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if testSessionRecordStands(sc.f) {
			return fmt.Errorf("the session record of %q still stands", sc.f.id)
		}
		return nil
	})
	if n, _ := cs.counts(); n != k {
		t.Fatalf("the closing's release was refused %d time(s), want %d", n, k)
	}
	if w := waits.Load(); w != k {
		t.Fatalf("the closing's release took %d contention wait(s), want %d", w, k)
	}
	if err := testLedgerSealedEmpty(sc.f); err != nil {
		t.Fatal(err)
	}
	if h := sc.holders(t, acq.Group); h != 1 {
		t.Fatalf("group %q has %d holder(s) after one release of two, want 1", acq.Group, h)
	}
}

// Under contention that never ends the closing's patience is one allowance: past it the refusals
// count again, the four counted attempts are spent and the closing fails, leaving the record
// marked for the adoption. Every refusal before the allowance ran out was waited on, and none after.
//
// M4 (no allowance: contention never counts): the closing never ends.
func TestContentionPastItsAllowanceEndsTheClosing(t *testing.T) {
	testDiscardParkAll(t)
	sc := testConnectSharedCloser(t, "seal-allowance", "$share/g/allowance/topic")
	cs, waits := testContend(t, mqttSharedShapeSeal, ApiErrors[JSStreamWrongLastSequenceConstantErr], 0)
	mqttTestSharedContentionAllowanceV.Set(100 * time.Millisecond)
	cs.arm()
	sc.c.Close()
	testWaitSendsSettle(t, cs)
	n, _ := cs.counts()
	w := waits.Load()
	if w == 0 || int64(n) != w+mqttSharedAcquireAttempts {
		t.Fatalf("the seal was refused %d time(s) and waited %d time(s): want the allowance's waits plus %d counted attempts",
			n, w, mqttSharedAcquireAttempts)
	}
	if !testSessionRecordStands(sc.f) {
		t.Fatal("the session record was released although its shared closing failed")
	}
}

// testWaitSendsSettle waits until the contended sends stop arriving: the closing has ended.
func testWaitSendsSettle(t *testing.T, cs *testContendedSends) {
	t.Helper()
	last, stable := -1, 0
	deadline := time.Now().Add(15 * time.Second)
	for stable < 10 {
		if time.Now().After(deadline) {
			n, _ := cs.counts()
			t.Fatalf("the closing never ended: %d refused send(s) and counting", n)
		}
		time.Sleep(50 * time.Millisecond)
		n, _ := cs.counts()
		if n > 0 && n == last {
			stable++
		} else {
			stable = 0
		}
		last = n
	}
}

// A recovery visit pays for each contention retry from its budget before taking it: with exactly k
// retries' worth past the counted price, it takes exactly k, leaves its reserve and reports the
// closing incomplete; a stop ends it after the retry in hand. Seal and release priced apart.
//
// M5 (the visit's pace ignores the budget, or the stop): the retry counts or the stop case differ.
func TestARecoveryVisitChargesEachContentionRetry(t *testing.T) {
	const k = 3
	visit := func(t *testing.T, sc *testSharedCloser, cs *testContendedSends, budget int, stop func() bool) (bool, *mqttRecoveryBudget, int) {
		t.Helper()
		before, _ := cs.counts()
		b := &mqttRecoveryBudget{left: budget, reserve: mqttRecoveryOpReserve}
		root := &mqttRecoveryRoot{ID: sc.f.id, Epoch: sc.f.epoch(), Gen: sc.f.gen}
		done := sc.f.asm.stepSharedClosing(sc.f.s, root, b, stop)
		after, _ := cs.counts()
		return done, b, after - before
	}

	t.Run("seal", func(t *testing.T) {
		testDiscardParkAll(t)
		sc := testConnectSharedCloser(t, "visit-seal", "$share/g/visitseal/topic")
		cs, waits := testContend(t, mqttSharedShapeSeal, ApiErrors[JSStreamWrongLastSequenceConstantErr], 0)
		// The inline closing fails with nothing written, leaving the discarding closure and an open
		// ledger for the visit.
		mqttTestSharedContentionAllowanceV.Set(time.Nanosecond)
		cs.arm()
		sc.c.Close()
		testWaitSendsSettle(t, cs)
		// Long enough for k paid retries many times over, short enough that a visit which never
		// pays still ends, with far more than k retries.
		mqttTestSharedContentionAllowanceV.Set(time.Second)

		w0 := waits.Load()
		budget := mqttRecoveryOpReserve + mqttSharedClosingClosureUnits + mqttSharedClosingSealUnits + k*mqttSharedSealAttemptUnits
		done, b, sends := visit(t, sc, cs, budget, nil)
		if done || sends != k+1 || waits.Load()-w0 != k || b.left != mqttRecoveryOpReserve {
			t.Fatalf("budget for %d retries: done %v, %d send(s), %d wait(s), %d left; want false, %d, %d, %d",
				k, done, sends, waits.Load()-w0, b.left, k+1, k, mqttRecoveryOpReserve)
		}

		w1 := waits.Load()
		stop := func() bool { return waits.Load() > w1 }
		done, _, sends = visit(t, sc, cs, mqttRecoveryOpBudget, stop)
		if done || sends != 2 || waits.Load()-w1 != 1 {
			t.Fatalf("stop after one retry: done %v, %d send(s), %d wait(s); want false, 2, 1", done, sends, waits.Load()-w1)
		}
	})

	t.Run("release", func(t *testing.T) {
		testDiscardParkAll(t)
		sc := testConnectSharedCloser(t, "visit-release", "$share/g/visitrelease/topic")
		cs, waits := testContend(t, mqttSharedShapeRelease, ApiErrors[JSStreamWrongLastSequenceConstantErr], 0)
		mqttTestSharedContentionAllowanceV.Set(time.Nanosecond)
		cs.arm()
		sc.c.Close()
		testWaitSendsSettle(t, cs)
		// Long enough for k paid retries many times over, short enough that a visit which never
		// pays still ends, with far more than k retries.
		mqttTestSharedContentionAllowanceV.Set(time.Second)
		if l := sc.f.asm.jsa.loadSharedLedger(sc.f.id, sc.f.epoch(), sc.f.gen); l.status != mqttOutboundValid ||
			l.rec.State != mqttSharedLedgerSealed || len(l.rec.Acqs) != 1 {
			t.Fatalf("fixture: the ledger does not read sealed with its acquisition listed: %v %+v", l.status, l.rec)
		}

		w0 := waits.Load()
		budget := mqttRecoveryOpReserve + mqttSharedClosingClosureUnits + mqttSharedClosingSealUnits +
			mqttSharedClosingReleaseUnits + k*mqttSharedReleaseAttemptUnits
		done, b, sends := visit(t, sc, cs, budget, nil)
		if done || sends != k+1 || waits.Load()-w0 != k || b.left != mqttRecoveryOpReserve {
			t.Fatalf("budget for %d retries: done %v, %d send(s), %d wait(s), %d left; want false, %d, %d, %d",
				k, done, sends, waits.Load()-w0, b.left, k+1, k, mqttRecoveryOpReserve)
		}
	})
}

// One allowance covers the whole closing. The first release is contended until the allowance is
// spent and then lands; the second release, contended for ever, must not wait at all — it gets
// only its four counted attempts — and the closing fails.
//
// M8 (a fresh allowance per release episode): the second release waits again.
func TestOneContentionAllowanceCoversTheWholeClosing(t *testing.T) {
	testDiscardParkAll(t)
	sc := testConnectSharedCloser(t, "allowance-shared", "$share/g/first/topic", "$share/g/second/topic")
	acqs := sc.acqs(t)
	first, second := acqs[0].Acq, acqs[1].Acq
	cs, waits := testContend(t, mqttSharedShapeRelease, ApiErrors[JSStreamWrongLastSequenceConstantErr], 0)
	mqttTestSharedContentionAllowanceV.Set(100 * time.Millisecond)
	var firstWaits, secondWaitsBefore atomic.Int64
	firstWaits.Store(-1)
	cs.pass = func(acq string) bool {
		// The first release lands once the allowance is spent and it has made one counted attempt.
		if acq == first && cs.byAcq[first] > 0 && firstWaits.Load() >= 0 {
			return true
		}
		if acq == first && cs.byAcq[first] > 0 && int64(cs.byAcq[first]) > waits.Load() {
			firstWaits.Store(waits.Load())
			secondWaitsBefore.Store(waits.Load())
			return true
		}
		return false
	}
	cs.arm()
	sc.c.Close()
	testWaitSendsSettle(t, cs)
	_, by := cs.counts()
	if firstWaits.Load() <= 0 {
		t.Fatalf("fixture: the first release never spent the allowance (waits %d, refusals %v)", firstWaits.Load(), by)
	}
	if extra := waits.Load() - secondWaitsBefore.Load(); extra != 0 || by[second] != mqttSharedReleaseAttempts {
		t.Fatalf("the second release waited %d more time(s) over %d refusal(s); want 0 waits and %d counted attempts",
			extra, by[second], mqttSharedReleaseAttempts)
	}
	if !testSessionRecordStands(sc.f) {
		t.Fatal("the session record was released although a shared release failed")
	}
}
