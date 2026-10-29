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
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Foreground priority over this broker's retained background writers (consults 104 and 105,
// outbound_foreground_priority.go), measured on the assembled MQTT v5 route.
//
// THE SCHEDULE IS FORCED, NOT TIMED. Each test parks background attempts that were ADMITTED BEFORE
// the foreground operation announced itself (at their batch, past their reads), then starts the
// foreground operation — a SUBSCRIBE's name claim. The claim's first attempt releases one parked
// attempt per attempt and lets it commit: that is the allowed collision. The claim then holds its
// announced window open until a background writer is observed WAITING at its admission (or a hold
// expires), and the test asserts that no other background attempt was admitted or committed inside
// the window, that the claim lost exactly one attempt per parked writer, that a contention wait was
// requested before each re-attempt, and that the background work completes after the release.

// testForegroundSchedule is one forced schedule.
type testForegroundSchedule struct {
	t        *testing.T
	clientID string
	sess     *mqttSession
	jsa      *mqttJSA

	mu     sync.Mutex
	events []string
	waits  []time.Duration
	// park lists, in release order, the background attempts to park at their batch: the first
	// presend whose Op matches each entry (exact, or a prefix ending in "-"). The entry "admit"
	// instead parks the next background attempt AT ITS ADMISSION, before its reads, so it reads
	// only once released; its batch is the next background presend after that release.
	park    []string
	parked  []chan struct{} // closed when entry k parked
	release []chan struct{} // closed to release entry k
	commit  []chan struct{} // closed when entry k's batch committed
	parkTxn []string
	trigger bool // the foreground under test is the next claim
	window  bool // the claim under test is announced
	// windowStamp is the window's announcement, by the handshake's count of announcements.
	windowStamp uint64
	claims      int
	fails       int
	inWindow    []string // background admissions and commits observed inside the window
	capture     int      // the admission-parked entry whose batch the next background presend is
	depth       int      // announcements standing; the window closes when it returns to zero
	// fails is how many claim attempts the schedule costs; by default one per parked entry.
	wantFails int
	// onClaim runs at the claim's first attempt, before the hold, for a schedule that releases
	// its parked attempt by other means.
	onClaim func()
	waited  chan struct{}
	sawWait bool
	hold    time.Duration
	// fgOp is the foreground operation under test, by its transition Op: the SUBSCRIBE name claim
	// by default. armOn, when set, arms the schedule at the first committed transition with that Op,
	// so the window opens at the NEXT announcement — the one that follows it, not an earlier
	// foreground operation of the same delivery. pinned says whether the foreground pins its first
	// read through testClaimPinned (the claim does; the send-section acquisition does not).
	fgOp   string
	armOn  string
	pinned bool
}

func testForegroundScheduleUp(t *testing.T, clientID string, sess *mqttSession, jsa *mqttJSA, as *mqttAccountSessionManager,
	park ...string) *testForegroundSchedule {
	t.Helper()
	sch := &testForegroundSchedule{t: t, clientID: clientID, sess: sess, jsa: jsa, park: park,
		waited: make(chan struct{}, 64), hold: 2 * time.Second, capture: -1, wantFails: len(park),
		fgOp: "op-provision-source", pinned: true}
	for range park {
		sch.parked = append(sch.parked, make(chan struct{}))
		sch.release = append(sch.release, make(chan struct{}))
		sch.commit = append(sch.commit, make(chan struct{}))
		sch.parkTxn = append(sch.parkTxn, _EMPTY_)
	}
	announce := func(on bool, stamp uint64) {
		sch.mu.Lock()
		defer sch.mu.Unlock()
		if on {
			sch.depth++
		} else {
			sch.depth--
		}
		switch {
		case on && sch.trigger && !sch.window && sch.claims == 0:
			sch.window = true
			sch.windowStamp = stamp
			sch.events = append(sch.events, "announce")
		case !on && sch.window && sch.depth == 0:
			// Overlapping foreground scopes keep the window open: background waits for all of them.
			sch.window = false
			sch.trigger = false
			sch.events = append(sch.events, "release")
		}
	}
	admit := func(stamp uint64) {
		sch.mu.Lock()
		// DECIDED INSIDE THE WINDOW, by the announcement count the admission read under the
		// handshake's lock: an admission decided just before the window's announcement may reach
		// this hook after it, and is the one attempt the window allows, not a breach.
		if sch.window && stamp >= sch.windowStamp {
			sch.inWindow = append(sch.inWindow, "admit")
		}
		for k, pat := range sch.park {
			if pat == "admit" && sch.parkTxn[k] == _EMPTY_ {
				sch.parkTxn[k] = "admission"
				sch.events = append(sch.events, "parked:admission")
				sch.mu.Unlock()
				close(sch.parked[k])
				<-sch.release[k]
				sch.mu.Lock()
				sch.capture = k
				sch.mu.Unlock()
				return
			}
		}
		sch.mu.Unlock()
	}
	wait := func() {
		sch.mu.Lock()
		w := sch.window
		sch.mu.Unlock()
		if w {
			select {
			case sch.waited <- struct{}{}:
			default:
			}
		}
	}
	pinned := func() {
		sch.mu.Lock()
		defer sch.mu.Unlock()
		if sch.window {
			sch.events = append(sch.events, "pinned")
		}
	}
	sess.testForegroundAnnounce.Store(&announce)
	sess.testBackgroundAdmit.Store(&admit)
	sess.testBackgroundWait.Store(&wait)
	sess.testClaimPinned.Store(&pinned)
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt == nil || at.Attempt.ID != clientID {
			return
		}
		if at.Attempt.Op == sch.fgOp {
			sch.mu.Lock()
			if !sch.window {
				sch.mu.Unlock()
				return
			}
			k := sch.claims
			sch.claims++
			sch.events = append(sch.events, "claim")
			sch.mu.Unlock()
			if k < len(sch.park) {
				// One allowed collision: release parked entry k and let it commit.
				close(sch.release[k])
				select {
				case <-sch.commit[k]:
				case <-time.After(10 * time.Second):
					t.Errorf("parked attempt %q never committed", sch.park[k])
				}
			}
			if k == 0 && sch.onClaim != nil {
				sch.onClaim()
			}
			if k == len(sch.park)-1 || (len(sch.park) == 0 && k == 0) {
				// Hold the announced window until a background writer waits at its admission.
				select {
				case <-sch.waited:
					sch.mu.Lock()
					sch.sawWait = true
					sch.mu.Unlock()
				case <-time.After(sch.hold):
				}
			}
			return
		}
		sch.mu.Lock()
		if k := sch.capture; k >= 0 {
			sch.parkTxn[k] = at.Attempt.Txn
			sch.capture = -1
			sch.mu.Unlock()
			return
		}
		for k, pat := range sch.park {
			if sch.parkTxn[k] != _EMPTY_ || pat == "admit" {
				continue
			}
			if at.Attempt.Op == pat || (strings.HasSuffix(pat, "-") && strings.HasPrefix(at.Attempt.Op, pat)) {
				sch.parkTxn[k] = at.Attempt.Txn
				sch.events = append(sch.events, "parked:"+at.Attempt.Op)
				sch.mu.Unlock()
				close(sch.parked[k])
				<-sch.release[k]
				return
			}
		}
		sch.mu.Unlock()
	}
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if at.Attempt == nil || at.Attempt.ID != clientID {
			return seq, err
		}
		sch.mu.Lock()
		defer sch.mu.Unlock()
		if sch.armOn != _EMPTY_ && at.Attempt.Op == sch.armOn && err == nil && !sch.window && sch.claims == 0 {
			sch.trigger = true
		}
		if at.Attempt.Op == sch.fgOp {
			if sch.window {
				if err != nil {
					sch.fails++
					sch.events = append(sch.events, "claim-fail")
				} else {
					sch.events = append(sch.events, "claim-ok")
				}
			}
			return seq, err
		}
		for k, txn := range sch.parkTxn {
			if txn == at.Attempt.Txn {
				close(sch.commit[k])
				return seq, err
			}
		}
		if sch.window && err == nil {
			sch.inWindow = append(sch.inWindow, "commit:"+at.Attempt.Op)
		}
		return seq, err
	}
	jsa.testTransitionPresend.Store(&presend)
	jsa.testTransitionCommit.Store(&commit)
	as.mu.Lock()
	as.testAuthorityContentionWait = func(d time.Duration) {
		sch.mu.Lock()
		if sch.window {
			sch.waits = append(sch.waits, d)
			sch.events = append(sch.events, "wait")
		}
		sch.mu.Unlock()
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		jsa.testTransitionPresend.Store(nil)
		jsa.testTransitionCommit.Store(nil)
		sess.testForegroundAnnounce.Store(nil)
		sess.testBackgroundAdmit.Store(nil)
		sess.testBackgroundWait.Store(nil)
		sess.testClaimPinned.Store(nil)
		as.mu.Lock()
		as.testAuthorityContentionWait = nil
		as.mu.Unlock()
	})
	return sch
}

// awaitParked waits for every parked entry.
func (sch *testForegroundSchedule) awaitParked() {
	sch.t.Helper()
	for k, ch := range sch.parked {
		select {
		case <-ch:
		case <-time.After(15 * time.Second):
			sch.t.Fatalf("background attempt %q never reached its batch", sch.park[k])
		}
	}
}

// arm marks the next claim as the foreground operation under test.
func (sch *testForegroundSchedule) arm() {
	sch.mu.Lock()
	sch.trigger = true
	sch.mu.Unlock()
}

// verify checks the window: one lost attempt per parked writer, a wait before each re-attempt, the
// announcement before the pinning read and held across every attempt, and no other background
// attempt admitted or committed while announced — and that a background writer DID wait in it.
func (sch *testForegroundSchedule) verify(sawWait bool) {
	sch.t.Helper()
	sch.mu.Lock()
	defer sch.mu.Unlock()
	n := sch.wantFails
	var want []string
	want = append(want, "announce")
	if sch.pinned {
		want = append(want, "pinned")
	}
	for k := 0; k < n; k++ {
		want = append(want, "claim", "claim-fail", "wait")
	}
	want = append(want, "claim", "claim-ok", "release")
	var got []string
	for _, e := range sch.events {
		if !strings.HasPrefix(e, "parked:") {
			got = append(got, e)
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		sch.t.Fatalf("the foreground claim ran as %v, want %v", got, want)
	}
	for i, d := range sch.waits {
		if d < mqttAuthorityContentionWaitMin || d >= mqttAuthorityContentionWaitMin+mqttAuthorityContentionWaitJitter {
			sch.t.Fatalf("wait %d was %v, outside [%v, %v)", i, d, mqttAuthorityContentionWaitMin,
				mqttAuthorityContentionWaitMin+mqttAuthorityContentionWaitJitter)
		}
	}
	if len(sch.inWindow) != 0 {
		sch.t.Fatalf("background attempts were admitted or committed while the foreground was announced: %v", sch.inWindow)
	}
	if sawWait && !sch.sawWait {
		sch.t.Fatal("no background writer waited at its admission inside the announced window: the " +
			"schedule never reached the gate under test")
	}
}

// testForegroundSubscribe sends the SUBSCRIBE under test and reads its SUBACK, tolerating retained
// PUBLISH frames of earlier subscriptions interleaved with it (each acknowledged); it returns the
// topics of those frames.
func testForegroundSubscribe(t *testing.T, c net.Conn, r *mqttReader, pi uint16, topic string) []string {
	t.Helper()
	testMQTTWriteSubV5(t, c, pi, []mqttV5SubFilter{{topic: topic, opts: 1}})
	var seen []string
	for {
		b, pl := testMQTTReadPacket(t, r)
		switch b & mqttPacketMask {
		case mqttPacketSubAck:
			rc := testMQTTParseSubAckV5(t, r, pl, pi)
			if len(rc) != 1 || rc[0] != 1 {
				t.Fatalf("the SUBSCRIBE under test was answered %v, want [1]", rc)
			}
			return seen
		case mqttPacketPub:
			tp, _, ppi := testMQTTParsePubV5(t, r, b, pl)
			seen = append(seen, tp)
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, ppi)
		default:
			t.Fatalf("unexpected packet %x", b)
		}
	}
}

// testForegroundServer starts a v5 server with the given retained copies and a persistent
// subscriber, and attaches to its session.
func testForegroundServer(t *testing.T, id string, retained map[string][]byte) (*testRetainedRig, net.Conn, *mqttReader) {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MaxPayload = 4 * 1024 * 1024
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { cp.Close() })
	testMQTTReadConnAckV5(t, rp)
	pi := uint16(1)
	for topic, payload := range retained {
		testMQTTPubV5Props(t, cp, rp, 1, true, pi, topic, payload, nil)
		pi++
	}
	ci := &mqttV5ConnInfo{clientID: id, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	testMQTTReadConnAckV5(t, r)
	rig := &testRetainedRig{s: s, o: o, id: id}
	rig.attach(t)
	return rig, c, r
}

// TestF105ForegroundAuthorityPriorityOverRetainedWorkers: for each retained background writer and
// each of its gated attempt sites, an attempt admitted before the announcement takes at most one
// claim attempt, every later attempt waits at its gate while the claim is announced, and the work
// completes after the release.
func TestF105ForegroundAuthorityPriorityOverRetainedWorkers(t *testing.T) {
	big := []byte(strings.Repeat("b", 200))
	multi := []byte(strings.Repeat("m", 9*mqttChunkMaxBytes+17))
	for _, tc := range []struct {
		name    string
		park    string // the driver attempt to park; the gate under test is the NEXT one
		payload []byte
	}{
		{name: "driver/chunks after reserve", park: "op-retained-reserve", payload: big},
		{name: "driver/second chunk batch", park: "op-retained-chunks", payload: multi},
		{name: "driver/admission after seal", park: "op-retained-seal", payload: big},
		{name: "driver/acquisition after admission", park: "op-admit-retained", payload: big},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig, c, r := testForegroundServer(t, "fg-"+strings.NewReplacer("/", "-", " ", "-").Replace(tc.name),
				map[string][]byte{"lv/big": tc.payload})
			sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, tc.park)
			if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/big", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
				t.Fatalf("SUBSCRIBE lv/big: %v", rc)
			}
			sch.awaitParked()
			sch.arm()
			seen := testForegroundSubscribe(t, c, r, 2, "lv/none")
			sch.verify(true)
			if len(seen) == 0 {
				testMQTTReadPubV5Props(t, c, r, "lv/big", tc.payload)
			}
		})
	}
	t.Run("driver/withdrawal after admission", func(t *testing.T) {
		rig, c, r := testForegroundServer(t, "fg-withdraw", map[string][]byte{"lv/big": big})
		var bigLin atomic.Value
		admitted := func(pi uint16) {
			reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, pi))
			if reg != nil {
				bigLin.Store(reg.Lin)
			}
			rig.sess.mu.Lock()
			cc := rig.sess.c
			rig.sess.mu.Unlock()
			cc.mu.Lock()
			cc.mqtt.maxPacketSize = 64
			cc.mu.Unlock()
		}
		rig.sess.mu.Lock()
		rig.sess.testRetainedAfterAdmit = admitted
		rig.sess.mu.Unlock()
		sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, "op-admit-retained")
		if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/big", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
			t.Fatalf("SUBSCRIBE lv/big: %v", rc)
		}
		sch.awaitParked()
		sch.arm()
		testForegroundSubscribe(t, c, r, 2, "lv/none")
		sch.verify(true)
		deadline := time.Now().Add(10 * time.Second)
		for {
			lin, _ := bigLin.Load().(string)
			if lin != _EMPTY_ {
				if x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, lin); x != nil &&
					x.Phase == mqttExchangeWithdrawnUnsent {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("the unservable copy was never withdrawn after the release")
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	t.Run("reclaimer/release after close", func(t *testing.T) {
		rig, c, r := testForegroundServer(t, "fg-reclaim", map[string][]byte{"lv/a": []byte("aa")})
		if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/a", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
			t.Fatalf("SUBSCRIBE lv/a: %v", rc)
		}
		_, pi, _ := testMQTTReadPubV5Raw(t, r, "lv/a", []byte("aa"))
		reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, pi))
		if reg == nil {
			t.Fatal("no retained register for lv/a")
		}
		sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, "op-reclaim-")
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		sch.awaitParked()
		sch.arm()
		testForegroundSubscribe(t, c, r, 2, "lv/none")
		sch.verify(true)
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, gone := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(rig.idHash, rig.gen, reg.Payload))
			if gone != nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("the copy was never reclaimed after the release")
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	t.Run("driver/close after departure", func(t *testing.T) {
		// The subscription departs while its copy's seal is parked: released, the driver finds the
		// departure at admission and CLOSES the never-admitted copy — a gated attempt of its own.
		rig, c, r := testForegroundServer(t, "fg-depart", map[string][]byte{"lv/big": big})
		// The UNSUBSCRIBE below also hands the subscription's SOURCE consumer to the retirement
		// owner (F-105 7c-lc): a background writer this schedule neither parks nor accounts for,
		// whose authority writes would also stale the parked seal's read. It is kept out of this
		// window; its own admission at the gate is TestF105SourceRetirementWaitsForTheForeground's.
		rig.sess.testSourceDisposalDrop.Store(true)
		sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, "op-retained-seal")
		if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/big", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
			t.Fatalf("SUBSCRIBE lv/big: %v", rc)
		}
		sch.awaitParked()
		testMQTTUnsubV5(t, c, r, 4, []string{"lv/big"})
		sch.arm()
		testForegroundSubscribe(t, c, r, 2, "lv/none")
		sch.verify(true)
		testMQTTExpectNothing(t, r)
	})
	t.Run("driver/retry after a lost CAS", func(t *testing.T) {
		// The parked reserve's read goes stale under another SUBSCRIBE's claim, so released it
		// LOSES its CAS and its step re-attempts: that re-attempt must pass the gate again.
		rig, c, r := testForegroundServer(t, "fg-stale", map[string][]byte{"lv/big": big})
		sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, "op-retained-reserve")
		sch.wantFails = 0
		if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/big", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
			t.Fatalf("SUBSCRIBE lv/big: %v", rc)
		}
		sch.awaitParked()
		if rc := testMQTTSubV5(t, c, r, 3, []mqttV5SubFilter{{topic: "lv/other", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
			t.Fatalf("SUBSCRIBE lv/other: %v", rc)
		}
		sch.arm()
		seen := testForegroundSubscribe(t, c, r, 2, "lv/none")
		sch.verify(true)
		if len(seen) == 0 {
			testMQTTReadPubV5Props(t, c, r, "lv/big", big)
		}
	})
	t.Run("both writers", func(t *testing.T) {
		rig, c, r := testForegroundServer(t, "fg-both", map[string][]byte{"lv/a": []byte("aa"), "lv/big": big})
		if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/a", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
			t.Fatalf("SUBSCRIBE lv/a: %v", rc)
		}
		_, pi, _ := testMQTTReadPubV5Raw(t, r, "lv/a", []byte("aa"))
		// Both writers park AT THEIR ADMISSION, so each reads only once released: the reclaimer first
		// (the PUBACK wakes it), then the driver (the SUBSCRIBE lv/big starts it; that SUBSCRIBE's
		// claim runs while the reclaimer is parked, and is not the claim under test).
		sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, "admit", "admit")
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		select {
		case <-sch.parked[0]:
		case <-time.After(15 * time.Second):
			t.Fatal("the reclaimer never reached its batch")
		}
		if rc := testMQTTSubV5(t, c, r, 3, []mqttV5SubFilter{{topic: "lv/big", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
			t.Fatalf("SUBSCRIBE lv/big: %v", rc)
		}
		sch.awaitParked()
		sch.arm()
		seen := testForegroundSubscribe(t, c, r, 2, "lv/none")
		sch.verify(true)
		if len(seen) == 0 {
			testMQTTReadPubV5Props(t, c, r, "lv/big", big)
		}
	})
}

// TestF105BackToBackSubscribeRetainedAuthorityContention (consult 104, revised for consult 105): two
// SUBSCRIBEs back to back, the second while the first's replay materializes, small and multi-batch:
// both granted, both copies delivered intact. The forced schedules above prove WHY; this is the
// client-visible pattern itself, run with no seams.
func TestF105BackToBackSubscribeRetainedAuthorityContention(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "small", size: 200},
		{name: "multi-chunk-batch", size: 9*mqttChunkMaxBytes + 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			big := []byte(strings.Repeat("b", tc.size))
			rig, c, r := testForegroundServer(t, "b2b-"+tc.name, map[string][]byte{"lv/big": big, "lv/small": []byte("ok")})
			_ = rig
			testMQTTWriteSubV5(t, c, 1, []mqttV5SubFilter{{topic: "lv/big", opts: 1}})
			testMQTTWriteSubV5(t, c, 2, []mqttV5SubFilter{{topic: "lv/small", opts: 1}})
			acks, gotBig, gotSmall := 0, false, false
			deadline := time.Now().Add(30 * time.Second)
			for acks < 2 || !gotBig || !gotSmall {
				if time.Now().After(deadline) {
					t.Fatalf("subacks %d big %v small %v", acks, gotBig, gotSmall)
				}
				b, pl := testMQTTReadPacket(t, r)
				switch b & mqttPacketMask {
				case mqttPacketSubAck:
					acks++
					rc := testMQTTParseSubAckV5(t, r, pl, uint16(acks))
					if len(rc) != 1 || rc[0] != 1 {
						t.Fatalf("SUBSCRIBE %d was answered %v", acks, rc)
					}
				case mqttPacketPub:
					topic, payload, pi := testMQTTParsePubV5(t, r, b, pl)
					switch topic {
					case "lv/big":
						if string(payload) != string(big) {
							t.Fatalf("lv/big arrived with %d bytes, want %d", len(payload), len(big))
						}
						gotBig = true
					case "lv/small":
						gotSmall = string(payload) == "ok"
					}
					testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
				default:
					t.Fatalf("unexpected packet %x", b)
				}
			}
			testMQTTFlush(t, c, nil, r)
		})
	}
}

// testMQTTWriteSubV5 writes a v5 SUBSCRIBE with empty properties and reads nothing.
func testMQTTWriteSubV5(t testing.TB, c net.Conn, pi uint16, filters []mqttV5SubFilter) {
	t.Helper()
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	vh.WriteVarInt(0)
	for _, f := range filters {
		vh.WriteBytes([]byte(f.topic))
		vh.WriteByte(f.opts)
	}
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing SUBSCRIBE: %v", err)
	}
}

// testMQTTParseSubAckV5 parses the body of a SUBACK already read by testMQTTReadPacket.
func testMQTTParseSubAckV5(t testing.TB, r *mqttReader, pl int, pi uint16) []byte {
	t.Helper()
	start := r.pos
	if rpi, err := r.readUint16("suback pi"); err != nil || rpi != pi {
		t.Fatalf("Expected SUBACK pi=%v, got %v (err=%v)", pi, rpi, err)
	}
	if _, err := r.readProperties(mqttPacketSubAck); err != nil {
		t.Fatalf("Error reading SUBACK properties: %v", err)
	}
	var codes []byte
	for r.pos-start < pl {
		rc, err := r.readByte("suback reason code")
		if err != nil {
			t.Fatalf("Error reading SUBACK reason code: %v", err)
		}
		codes = append(codes, rc)
	}
	return codes
}

// testMQTTParsePubV5 parses the body of a PUBLISH already read by testMQTTReadPacket.
func testMQTTParsePubV5(t testing.TB, r *mqttReader, b byte, pl int) (string, []byte, uint16) {
	t.Helper()
	start := r.pos
	qos := mqttGetQoS(b & mqttPacketFlagMask)
	topic, err := r.readBytes("topic", false)
	if err != nil {
		t.Fatalf("Error reading topic: %v", err)
	}
	var pi uint16
	if qos > 0 {
		if pi, err = r.readUint16("pi"); err != nil {
			t.Fatalf("Error reading pi: %v", err)
		}
	}
	if _, err := r.readProperties(mqttPropsContextPubOut); err != nil {
		t.Fatalf("Error reading PUBLISH properties: %v", err)
	}
	n := pl - (r.pos - start)
	if n < 0 || r.pos+n > len(r.buf) {
		t.Fatalf("Invalid payload length %d", n)
	}
	payload := append([]byte(nil), r.buf[r.pos:r.pos+n]...)
	r.pos += n
	return string(topic), payload, pi
}

// TestF105ForegroundAuthorityPriorityOverRetainedAbandonment: the driver's abandonment submission
// is a gated background attempt. Its first submission is admitted and parked, then refused before
// sending; while a SUBSCRIBE is announced the next submission waits at its admission, and the
// abandonment lands after the release.
func TestF105ForegroundAuthorityPriorityOverRetainedAbandonment(t *testing.T) {
	parked := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	rig := testRetainedAbandonUpExpiring(t, "fg-abandon", 1, 60, func(rig *testRetainedAbandonRig) {
		rig.asm.mu.Lock()
		rig.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if first.CompareAndSwap(false, true) {
				close(parked)
				<-release
				return mqttOpOutcome{}, errors.New("refused before it was sent")
			}
			return write()
		}
		rig.asm.mu.Unlock()
	})
	defer rig.done()
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the abandonment was never submitted")
	}
	sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm)
	sch.onClaim = func() { close(release) }
	sch.arm()
	testForegroundSubscribe(t, rig.c, rig.r, 1, "lv/none")
	sch.verify(true)
	rig.abandoned(t)
}

// TestF105ForegroundAuthorityAnnouncementBeforeRead (consult 105): the claim announces itself
// BEFORE its pinning authority read and stays announced across every re-attempt and wait: an
// announcement after the read would leave the read unprotected, and a release between attempts
// would admit background writers into the claim's budget.
func TestF105ForegroundAuthorityAnnouncementBeforeRead(t *testing.T) {
	big := []byte(strings.Repeat("b", 200))
	rig, c, r := testForegroundServer(t, "fg-order", map[string][]byte{"lv/big": big})
	sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, "op-retained-reserve")
	if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/big", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
		t.Fatalf("SUBSCRIBE lv/big: %v", rc)
	}
	sch.awaitParked()
	sch.arm()
	testForegroundSubscribe(t, c, r, 2, "lv/none")
	sch.mu.Lock()
	var got []string
	for _, e := range sch.events {
		if !strings.HasPrefix(e, "parked:") {
			got = append(got, e)
		}
	}
	sch.mu.Unlock()
	if len(got) < 2 || got[0] != "announce" || got[1] != "pinned" {
		t.Fatalf("the claim's announcement and pinning read ran as %v: the announcement must come first", got)
	}
	for i, e := range got[:len(got)-1] {
		if e == "release" {
			t.Fatalf("the claim released its announcement at step %d of %v, before its last attempt", i, got)
		}
	}
	if got[len(got)-1] != "release" {
		t.Fatalf("the claim never released its announcement: %v", got)
	}
	sch.verify(true)
}

// TestF105ForegroundAuthorityPriorityReleaseOnExhaustion (consult 105): a claim whose every attempt
// is defeated — here by a foreground writer outside the handshake, committing between each attempt's
// read and its batch — exhausts its budget, refuses the SUBSCRIBE, and still releases its
// announcement: a background admission proceeds at once afterwards.
func TestF105ForegroundAuthorityPriorityReleaseOnExhaustion(t *testing.T) {
	rig, c, r := testForegroundServer(t, "fg-exhaust", nil)
	jsa := &rig.asm.jsa
	var attempts int
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt == nil || at.Attempt.ID != rig.id || at.Attempt.Op != "op-provision-source" {
			return
		}
		attempts++
		auth, _, err := jsa.servingAuthority(rig.id, rig.epoch)
		if err != nil {
			t.Errorf("no serving authority: %v", err)
			return
		}
		inc := mqttNewInflightNonce()
		if _, perr := jsa.opProvisionResource(rig.id, rig.epoch, mqttNewInflightNonce(), "op-zz-collide", auth.Gen,
			auth.Tenure, inc, rig.idHash+"_"+inc, mqttResourceSourceConsumer); perr != nil {
			t.Errorf("the colliding write: %v", perr)
		}
	}
	jsa.testTransitionPresend.Store(&presend)
	defer jsa.testTransitionPresend.Store(nil)
	rig.asm.mu.Lock()
	rig.asm.testAuthorityContentionWait = func(time.Duration) {}
	rig.asm.mu.Unlock()
	defer func() {
		rig.asm.mu.Lock()
		rig.asm.testAuthorityContentionWait = nil
		rig.asm.mu.Unlock()
	}()
	if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "fg/x", opts: 1}}); len(rc) != 1 || rc[0] != mqttSubAckFailure {
		t.Fatalf("a claim defeated on every attempt was answered %v, want [0x80]", rc)
	}
	jsa.testTransitionPresend.Store(nil)
	if attempts != mqttSubscribeClaimAttempts {
		t.Fatalf("the claim made %d attempts, want its budget of %d", attempts, mqttSubscribeClaimAttempts)
	}
	rig.sess.foreground.mu.Lock()
	n := rig.sess.foreground.n
	rig.sess.foreground.mu.Unlock()
	if n != 0 {
		t.Fatalf("the exhausted claim left %d announcements standing", n)
	}
	done := make(chan bool, 1)
	go func() { done <- rig.sess.admitBackgroundAuthority() }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("a background admission after the exhausted claim was refused")
		}
	case <-time.After(time.Second):
		t.Fatal("a background admission after the exhausted claim still waits")
	}
}
