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

// A QoS 1/2 offer already routed to a connection that detaches is owed to the session
// [MQTT-4.4.0-1]: its delivery subscription is drained at the detach, and every drained offer is
// either recorded for the successor's resume or late-NAKed by its writer, so it is resent at once
// rather than after the consumer's AckWait (issue #170).
//
// NOT PARALLEL: the end-to-end fixtures install process-global delivery hooks. Every new seam is
// the session's own (sess.testDrainAtDetach). AckWait is a minute and every arrival is awaited
// within testMQTTTimeout, so an arrival here is never an AckWait re-offer.

package mqtt

import (
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// drainProbe records one session's drain-at-detach seams and holds the ones a schedule needs.
type drainProbe struct {
	mu        sync.Mutex
	cutoffs   []*client
	decisions []drainDecision
	issued    []drainIssue
	ops       []drainOp
	lateNAKs  []string

	cutoffCh chan struct{}
	issuedCh chan struct{}
	cutOnce  sync.Once
	issOnce  sync.Once

	// holdTeardownOf, when set, holds closeConnection of that client before its teardown;
	// teardownReached closes when it is held, releaseTeardown lets it go.
	holdTeardownOf  *client
	teardownReached chan struct{}
	releaseTeardown chan struct{}
	tdOnce          sync.Once

	// holdClaim, when set, holds the first claimed drain before Drain is called.
	holdClaim    bool
	claimReached chan struct{}
	releaseClaim chan struct{}
	claimOnce    sync.Once

	// holdLateNAK, when set, holds the first late NAK before it is published.
	holdLateNAK    bool
	lateNAKReached chan string
	releaseLateNAK chan struct{}
	lateOnce       sync.Once
}

type drainDecision struct {
	dur     string
	sseq    uint64
	origin  mqttOfferOrigin
	outcome mqttWithheldOutcome
}

type drainIssue struct {
	sub     *subscription
	err     error
	handoff []string
}

type drainOp struct {
	c   *client
	sub *subscription
	op  string
}

func newDrainProbe() *drainProbe {
	return &drainProbe{
		cutoffCh: make(chan struct{}), issuedCh: make(chan struct{}),
		teardownReached: make(chan struct{}), releaseTeardown: make(chan struct{}),
		claimReached: make(chan struct{}), releaseClaim: make(chan struct{}),
		lateNAKReached: make(chan string, 1), releaseLateNAK: make(chan struct{}),
	}
}

// install puts the probe on sess and releases every hold at cleanup, so a failing test never
// leaves a broker goroutine parked.
func (p *drainProbe) install(t *testing.T, sess *mqttSession) {
	t.Helper()
	h := &mqttDrainAtDetachTestHooks{
		beforeTeardown: func(c *client) {
			p.mu.Lock()
			hold := p.holdTeardownOf != nil && p.holdTeardownOf == c
			p.mu.Unlock()
			if hold {
				p.tdOnce.Do(func() { close(p.teardownReached) })
				<-p.releaseTeardown
			}
		},
		teardownOp: func(c *client, sub *subscription, op string) {
			p.mu.Lock()
			p.ops = append(p.ops, drainOp{c, sub, op})
			p.mu.Unlock()
		},
		claimed: func(sub *subscription) {
			if !p.holdClaim {
				return
			}
			first := false
			p.claimOnce.Do(func() { first = true; close(p.claimReached) })
			if first {
				<-p.releaseClaim
			}
		},
		issued: func(sub *subscription, err error, handoff []string) {
			p.mu.Lock()
			p.issued = append(p.issued, drainIssue{sub, err, append([]string(nil), handoff...)})
			p.mu.Unlock()
			if err == nil {
				p.issOnce.Do(func() { close(p.issuedCh) })
			}
		},
		cutoff: func(c *client) {
			p.mu.Lock()
			p.cutoffs = append(p.cutoffs, c)
			p.mu.Unlock()
			p.cutOnce.Do(func() { close(p.cutoffCh) })
		},
		decided: func(dur string, sseq uint64, origin mqttOfferOrigin, outcome mqttWithheldOutcome) {
			p.mu.Lock()
			p.decisions = append(p.decisions, drainDecision{dur, sseq, origin, outcome})
			p.mu.Unlock()
		},
		lateNAK: func(reply string) {
			p.mu.Lock()
			p.lateNAKs = append(p.lateNAKs, reply)
			p.mu.Unlock()
			if !p.holdLateNAK {
				return
			}
			first := false
			p.lateOnce.Do(func() { first = true })
			if first {
				p.lateNAKReached <- reply
				<-p.releaseLateNAK
			}
		},
	}
	sess.testDrainAtDetach.Store(h)
	t.Cleanup(func() {
		sess.testDrainAtDetach.Store(nil)
		for _, ch := range []chan struct{}{p.releaseTeardown, p.releaseClaim, p.releaseLateNAK} {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
}

func (p *drainProbe) decisionsFor(dur string, sseq uint64) []mqttWithheldOutcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []mqttWithheldOutcome
	for _, d := range p.decisions {
		if d.dur == dur && d.sseq == sseq {
			out = append(out, d.outcome)
		}
	}
	return out
}

func (p *drainProbe) decisionsFrom(dur string, sseq uint64, cc *client) []mqttWithheldOutcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []mqttWithheldOutcome
	for _, d := range p.decisions {
		if d.dur == dur && d.sseq == sseq && d.origin.cc == cc {
			out = append(out, d.outcome)
		}
	}
	return out
}

func (p *drainProbe) lateNAKCount(reply string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.lateNAKs {
		if r == reply {
			n++
		}
	}
	return n
}

func drainAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testMQTTTimeout):
		t.Fatalf("fixture: %s never happened", what)
	}
}

// drainRig is one broker with a persistent-session QoS 1 subscriber and a publisher.
type drainRig struct {
	t        *testing.T
	o        *Options
	s        *Server
	clientID string
	topic    string
	sess     *mqttSession
	c        net.Conn
	r        *mqttReader
	pub      net.Conn
	pubr     *mqttReader
	pubPI    uint16
}

func drainRigStart(t *testing.T, name string) *drainRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Minute // far above every await: an arrival is never an AckWait re-offer
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &drainRig{t: t, o: o, s: s, clientID: name + "-" + nuid.Next()[:8], topic: "dd/" + name}
	rig.c, rig.r = rig.connect(false)
	testMQTTSub(t, 1, rig.c, rig.r, []*mqttFilter{{filter: rig.topic, qos: 1}}, []byte{1})
	testMQTTFlush(t, rig.c, nil, rig.r)
	asm := testMQTTGetAccountSessionManager(t, s, rig.clientID)
	asm.mu.RLock()
	rig.sess = asm.sessions[rig.clientID]
	asm.mu.RUnlock()
	if rig.sess == nil {
		t.Fatal("no resident session for the subscriber")
	}
	rig.pub, rig.pubr = testMQTTConnect(t, &mqttConnInfo{clientID: rig.clientID + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.pub.Close() })
	testMQTTCheckConnAck(t, rig.pubr, mqttConnAckRCConnectionAccepted, false)
	return rig
}

// connect opens a persistent-session (Clean Session 0) connection for the subscriber.
func (rig *drainRig) connect(present bool) (net.Conn, *mqttReader) {
	rig.t.Helper()
	c, r := testMQTTConnect(rig.t, &mqttConnInfo{clientID: rig.clientID, cleanSess: false}, rig.o.MQTT.Host, rig.o.MQTT.Port)
	rig.t.Cleanup(func() { c.Close() })
	testMQTTCheckConnAck(rig.t, r, mqttConnAckRCConnectionAccepted, present)
	return c, r
}

func (rig *drainRig) publish(payload string) {
	rig.t.Helper()
	rig.pubPI++
	testMQTTPublish(rig.t, rig.pub, rig.pubr, 1, false, false, rig.topic, rig.pubPI, []byte(payload))
}

func (rig *drainRig) mine(sub *subscription) bool {
	return sub != nil && sub.client != nil && sub.client.mqtt != nil &&
		sub.client.mqtt.sess != nil && sub.client.mqtt.sess.id == rig.clientID
}

func (rig *drainRig) durable() string {
	rig.sess.mu.Lock()
	defer rig.sess.mu.Unlock()
	for _, cc := range rig.sess.cons {
		if cc.DeliverGroup == _EMPTY_ {
			return cc.Durable
		}
	}
	rig.t.Fatal("fixture: the subscriber has no ordinary consumer")
	return _EMPTY_
}

func (rig *drainRig) client() *client {
	rig.sess.mu.Lock()
	defer rig.sess.mu.Unlock()
	return rig.sess.c
}

// readPayloads reads n PUBLISH packets, acknowledging each, and returns them by payload.
func (rig *drainRig) readPayloads(c net.Conn, r *mqttReader, n int) map[string]detachedPublish {
	rig.t.Helper()
	got := make(map[string]detachedPublish, n)
	for i := 0; i < n; i++ {
		flags, pi, _, payload := testMQTTReadPubPacket(rig.t, r)
		got[string(payload)] = detachedPublish{pi: pi, dup: flags&mqttPubFlagDup != 0}
		if mqttGetQoS(flags) == 1 {
			testMQTTSendPIPacket(mqttPacketPubAck, rig.t, c, pi)
		}
	}
	return got
}

type detachedPublish struct {
	pi  uint16
	dup bool
}

func drainExpectPayloads(t *testing.T, got map[string]detachedPublish, want ...string) {
	t.Helper()
	var have []string
	for p := range got {
		have = append(have, p)
	}
	sort.Strings(have)
	sort.Strings(want)
	if fmt.Sprint(have) != fmt.Sprint(want) {
		t.Fatalf("received %v, want %v", have, want)
	}
}

// drainPark holds the next delivery callback of the rig's session before its lock.
type drainPark struct {
	parked  chan struct{}
	release chan struct{}
	mu      sync.Mutex
	sub     *subscription
	reply   string
}

func (rig *drainRig) parkNextDelivery() *drainPark {
	p := &drainPark{parked: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	pre := func(sub *subscription) {
		if !rig.mine(sub) {
			return
		}
		first := false
		once.Do(func() {
			first = true
			p.mu.Lock()
			p.sub = sub
			p.mu.Unlock()
			close(p.parked)
		})
		if first {
			<-p.release
		}
	}
	offered := func(sub *subscription, reply string) {
		if !rig.mine(sub) {
			return
		}
		p.mu.Lock()
		if p.reply == _EMPTY_ {
			p.reply = reply
		}
		p.mu.Unlock()
	}
	mqttTestDeliveryPreLockHookV.Store(&pre)
	mqttTestDeliveryOfferedHookV.Store(&offered)
	rig.t.Cleanup(func() {
		mqttTestDeliveryPreLockHookV.Store(nil)
		mqttTestDeliveryOfferedHookV.Store(nil)
		p.free()
	})
	return p
}

func (p *drainPark) free() {
	select {
	case <-p.release:
	default:
		close(p.release)
	}
}

func (p *drainPark) parkedSub() *subscription {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sub
}

func (p *drainPark) parkedReply() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reply
}

// waitQueued waits until sub's connection-layer queue holds at least n offers, the running one
// included.
func (rig *drainRig) waitQueued(sub *subscription, n int) {
	rig.t.Helper()
	deadline := time.Now().Add(testMQTTTimeout)
	for time.Now().Before(deadline) {
		if f, _ := sub.queued.Load().(func() int); f != nil && f() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	rig.t.Fatalf("fixture: the subscription never queued %d offers", n)
}

func (rig *drainRig) disconnect() {
	rig.t.Helper()
	testMQTTDisconnect(rig.t, rig.c, nil)
	rig.c.Close()
}

// T1: an offer queued behind a held callback when the subscriber disconnects is drained, not
// discarded, and both it and the held one are resent on the resume.
func TestQoS1OfferQueuedAtDisconnectIsResentOnResume(t *testing.T) {
	rig := drainRigStart(t, "t1")
	probe := newDrainProbe()
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	rig.publish("m1")
	drainAwait(t, park.parked, "the first offer reaching its callback")
	rig.publish("m2")
	rig.waitQueued(park.parkedSub(), 2)

	rig.disconnect()
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	park.free()

	drainExpectPayloads(t, rig.readPayloads(c2, r2, 2), "m1", "m2")
}

// T4: an offer its predecessor's callback decides after the successor's resume pass, with its
// subscription's drain issued, is NAKed by the writer and recorded nowhere.
func TestOfferDecidedAfterTheResumePassIsNAKedByItsWriter(t *testing.T) {
	rig := drainRigStart(t, "t4")
	dur := rig.durable()
	probe := newDrainProbe()
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	rig.publish("m1")
	drainAwait(t, park.parked, "the offer reaching its callback")
	sseq, _, _, _, _ := ackReplyInfo(park.parkedReply())
	if sseq == 0 {
		t.Fatal("fixture: the parked offer's reply was not observed")
	}

	rig.disconnect()
	drainAwait(t, probe.issuedCh, "the predecessor's drain being issued")
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	park.free()

	drainExpectPayloads(t, rig.readPayloads(c2, r2, 1), "m1")
	if got := probe.decisionsFor(dur, sseq); len(got) != 1 || got[0] != mqttWithheldNAKOwed {
		t.Fatalf("the late offer's writer decisions = %v, want exactly one NAK-owed", got)
	}
	if testHasWithheldMarker(rig.sess, dur, sseq) {
		t.Fatal("a late-NAKed offer also left a withheld marker")
	}
}

// T5: a predecessor that records after the successor's resume pass, because its connection was
// detached but its subscription not yet draining, has that marker NAKed when its drain starts.
func TestDrainStartReNAKsMarkersRecordedAfterTheResumePass(t *testing.T) {
	rig := drainRigStart(t, "t5")
	dur := rig.durable()
	probe := newDrainProbe()
	probe.mu.Lock()
	probe.holdTeardownOf = rig.client()
	probe.mu.Unlock()
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	rig.publish("m1")
	drainAwait(t, park.parked, "the offer reaching its callback")
	sseq, _, _, _, _ := ackReplyInfo(park.parkedReply())

	rig.disconnect()
	drainAwait(t, probe.teardownReached, "the predecessor's close held between its detach and its teardown")
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	park.free()
	testDrainWaitDecision(t, probe, dur, sseq, mqttWithheldRecorded)
	if !testHasWithheldMarker(rig.sess, dur, sseq) {
		t.Fatal("fixture: the not-yet-draining predecessor's offer left no marker")
	}
	close(probe.releaseTeardown)

	drainExpectPayloads(t, rig.readPayloads(c2, r2, 1), "m1")
	if testHasWithheldMarker(rig.sess, dur, sseq) {
		t.Fatal("the handoff left the marker it NAKed")
	}
}

func testDrainWaitDecision(t *testing.T, probe *drainProbe, dur string, sseq uint64, want mqttWithheldOutcome) {
	t.Helper()
	deadline := time.Now().Add(testMQTTTimeout)
	for time.Now().Before(deadline) {
		if got := probe.decisionsFor(dur, sseq); len(got) > 0 {
			if len(got) != 1 || got[0] != want {
				t.Fatalf("writer decisions for %s/%d = %v, want exactly %v", dur, sseq, got, want)
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("fixture: no writer decision for %s/%d", dur, sseq)
}

// T8: a predecessor offer decided between its subscription's drain CLAIM and the Drain call
// records (claimed licenses no NAK: the UNSUB is not out yet), and the handoff at the issue NAKs
// it.
func TestWriterBetweenClaimAndIssueRecordsAndTheHandoffNAKs(t *testing.T) {
	rig := drainRigStart(t, "t8")
	dur := rig.durable()
	probe := newDrainProbe()
	probe.holdClaim = true
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	rig.publish("m1")
	drainAwait(t, park.parked, "the offer reaching its callback")
	sseq, _, _, _, _ := ackReplyInfo(park.parkedReply())

	rig.disconnect()
	drainAwait(t, probe.claimReached, "the predecessor's drain claimed and not yet issued")
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	park.free()
	testDrainWaitDecision(t, probe, dur, sseq, mqttWithheldRecorded)
	close(probe.releaseClaim)
	drainAwait(t, probe.issuedCh, "the drain being issued")

	drainExpectPayloads(t, rig.readPayloads(c2, r2, 1), "m1")
	probe.mu.Lock()
	handoff := 0
	for _, is := range probe.issued {
		handoff += len(is.handoff)
	}
	probe.mu.Unlock()
	if handoff != 1 {
		t.Fatalf("the drain issue handed off %d late markers, want 1", handoff)
	}
}

// T2: a takeover's successor drains the displaced live predecessor's delivery subscription
// itself, so the predecessor's held and queued offers are resent while the predecessor's own
// close is still held.
func TestTakeoverResendsThePredecessorsQueuedOffer(t *testing.T) {
	rig := drainRigStart(t, "t2")
	probe := newDrainProbe()
	pred := rig.client()
	probe.mu.Lock()
	probe.holdTeardownOf = pred
	probe.mu.Unlock()
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	rig.publish("m1")
	drainAwait(t, park.parked, "the first offer reaching its callback")
	predSub := park.parkedSub()
	rig.publish("m2")
	rig.waitQueued(predSub, 2)

	c2, r2 := rig.connect(true) // the takeover: the predecessor is still connected
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	drainAwait(t, probe.teardownReached, "the predecessor's own close held before its teardown")
	park.free()

	// Decisive while the predecessor's own close is still held: only the successor can have
	// drained its subscription.
	drainExpectPayloads(t, rig.readPayloads(c2, r2, 2), "m1", "m2")
	probe.mu.Lock()
	byPred := false
	for _, is := range probe.issued {
		if is.sub == predSub && is.err == nil {
			byPred = true
		}
	}
	probe.mu.Unlock()
	if !byPred {
		t.Fatal("the predecessor's delivery subscription was never drained by the successor")
	}
	close(probe.releaseTeardown)
}

// T3: offers a non-shared subscription's callback HELD for its batch, and those still queued
// behind them, are all resent when the subscriber detaches while the holding callback is
// blocked.
func TestHeldOffersOfADrainedSubscriptionAreResent(t *testing.T) {
	rig := drainRigStart(t, "t3")
	probe := newDrainProbe()
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	holding, releaseHold := make(chan struct{}), make(chan struct{})
	var holdOnce sync.Once
	mqttTestHeldOffersHook.Set(func(sub *subscription, held int) {
		if !rig.mine(sub) {
			return
		}
		first := false
		holdOnce.Do(func() { first = true; close(holding) })
		if first {
			<-releaseHold
		}
	})
	t.Cleanup(func() {
		mqttTestHeldOffersHook.Set(nil)
		select {
		case <-releaseHold:
		default:
			close(releaseHold)
		}
	})

	rig.publish("m1")
	drainAwait(t, park.parked, "the first offer reaching its callback")
	for _, p := range []string{"m2", "m3", "m4"} {
		rig.publish(p)
	}
	rig.waitQueued(park.parkedSub(), 4) // a confirmed backlog behind the running callback
	park.free()
	// m1 goes out on the predecessor; m2's callback finds more queued and HOLDS it.
	drainExpectPayloads(t, rig.readPayloads(rig.c, rig.r, 1), "m1")
	drainAwait(t, holding, "a callback holding an offer for its batch")

	rig.disconnect()
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	close(releaseHold)

	drainExpectPayloads(t, rig.readPayloads(c2, r2, 3), "m2", "m3", "m4")
}

// T6: an allocation its predecessor's callback collected, abandoned after the successor's
// resume pass, is withheld as the ORIGIN's offer: with that origin's drain issued it is NAKed,
// never recorded as the successor's own withholding.
func TestAbandonedAllocationCarriesItsOrigin(t *testing.T) {
	rig := drainRigStart(t, "t6")
	dur := rig.durable()
	probe := newDrainProbe()
	probe.install(t, rig.sess)

	flushing, releaseFlush := make(chan struct{}), make(chan struct{})
	var flushOnce sync.Once
	mqttTestDeliveryFlushHook.Set(func(s *mqttSession, _ int) {
		if s != rig.sess {
			return
		}
		first := false
		flushOnce.Do(func() { first = true; close(flushing) })
		if first {
			<-releaseFlush
		}
	})
	t.Cleanup(func() {
		mqttTestDeliveryFlushHook.Set(nil)
		select {
		case <-releaseFlush:
		default:
			close(releaseFlush)
		}
	})

	rig.publish("m1")
	drainAwait(t, flushing, "the collector holding its allocation before admission")
	var sseq uint64
	rig.sess.mu.Lock()
	for pi, ack := range rig.sess.pendingPublish {
		if ack.jsDur == dur && ack.regPending {
			sseq = ack.sseq
			if ack.origin.cc == nil || ack.origin.sub == nil {
				rig.sess.mu.Unlock()
				t.Fatalf("allocation %d carries no origin", pi)
			}
		}
	}
	rig.sess.mu.Unlock()
	if sseq == 0 {
		t.Fatal("fixture: no pending allocation for the collected delivery")
	}

	rig.disconnect()
	drainAwait(t, probe.issuedCh, "the predecessor's drain being issued")
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	close(releaseFlush)

	drainExpectPayloads(t, rig.readPayloads(c2, r2, 1), "m1")
	if got := probe.decisionsFor(dur, sseq); len(got) == 0 || got[len(got)-1] != mqttWithheldNAKOwed {
		t.Fatalf("the abandoned allocation's writer decisions = %v, want it NAK-owed", got)
	}
}

// T7: a delivery withheld at the session's own in-flight cap is recorded and waits for a freed
// slot, after a resume pass as before it: capacity withholding never NAKs.
func TestCapacityWithholdingStillRecords(t *testing.T) {
	rig := drainRigStart(t, "t7")
	dur := rig.durable()
	probe := newDrainProbe()
	probe.install(t, rig.sess)

	rig.disconnect()
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the resume pass that sets the cutoff")
	succ := rig.client()
	rig.sess.mu.Lock()
	cut := rig.sess.resumeCutoffPassedLocked()
	rig.sess.rmax = 1 // the client's Receive Maximum: one in flight
	rig.sess.mu.Unlock()
	if !cut {
		t.Fatal("fixture: the cutoff is not in force for the bound connection")
	}

	rig.publish("m1")
	flags, pi1, _, payload := testMQTTReadPubPacket(t, r2)
	if string(payload) != "m1" || mqttGetQoS(flags) != 1 {
		t.Fatalf("first delivery = %q qos %d", payload, mqttGetQoS(flags))
	}
	rig.publish("m2")
	var sseq2 uint64
	deadline := time.Now().Add(testMQTTTimeout)
	for sseq2 == 0 && time.Now().Before(deadline) {
		probe.mu.Lock()
		for _, d := range probe.decisions {
			if d.dur == dur && d.origin.cc == succ {
				if d.outcome != mqttWithheldRecorded {
					probe.mu.Unlock()
					t.Fatalf("the capacity withholding decided %v, want recorded", d.outcome)
				}
				sseq2 = d.sseq
			}
		}
		probe.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	if sseq2 == 0 {
		t.Fatal("fixture: the second delivery was never withheld at the cap")
	}
	if !testHasWithheldMarker(rig.sess, dur, sseq2) {
		t.Fatal("the capacity withholding recorded no marker")
	}
	probe.mu.Lock()
	nLate := len(probe.lateNAKs)
	probe.mu.Unlock()
	if nLate != 0 {
		t.Fatalf("capacity withholding published %d late NAKs", nLate)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi1)
	drainExpectPayloads(t, rig.readPayloads(c2, r2, 1), "m2")
}

// T11: a shutdown is not a resume: the delivery subscription of a client closed by it is
// unsubscribed, never drained.
func TestShutdownCloseUnsubscribesDeliveryInterest(t *testing.T) {
	rig := drainRigStart(t, "t11")
	probe := newDrainProbe()
	probe.install(t, rig.sess)
	cli := rig.client()

	testMQTTShutdownServer(rig.s)

	probe.mu.Lock()
	defer probe.mu.Unlock()
	var seen []string
	for _, op := range probe.ops {
		if op.c == cli && op.sub.mqtt != nil && op.sub.mqtt.drainAtDetach {
			seen = append(seen, op.op)
		}
	}
	if len(seen) != 1 || seen[0] != "unsubscribe" {
		t.Fatalf("the shutdown's teardown of the delivery subscription = %v, want exactly one unsubscribe", seen)
	}
}

// T12: the late NAK's one stated duplicate. A NAK decided before the successor mapped the same
// source, and published after the successor sent it, re-offers an exchange the successor owns:
// the extra copy carries the same packet identifier and DUP, never a second identifier.
func TestDuplicateFromALateNAKCarriesTheSameIdentifierAndDUP(t *testing.T) {
	rig := drainRigStart(t, "t12")
	probe := newDrainProbe()
	probe.holdLateNAK = true
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	rig.publish("m1")
	drainAwait(t, park.parked, "the offer reaching its callback")
	rig.disconnect()
	drainAwait(t, probe.issuedCh, "the predecessor's drain being issued")
	c2, r2 := rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	park.free()

	var reply string
	select {
	case reply = <-probe.lateNAKReached:
	case <-time.After(testMQTTTimeout):
		t.Fatal("fixture: the late NAK was never decided")
	}
	// The successor maps and sends the source first: a re-offer already routed to it.
	rig.sess.jsa.sendMsg(reply, AckNak)
	flags, pi, _, payload := testMQTTReadPubPacket(t, r2)
	if string(payload) != "m1" || flags&mqttPubFlagDup != 0 {
		t.Fatalf("first copy = %q dup=%v, want a first transmission of m1", payload, flags&mqttPubFlagDup != 0)
	}
	close(probe.releaseLateNAK)
	flags2, pi2, _, payload2 := testMQTTReadPubPacket(t, r2)
	if string(payload2) != "m1" || pi2 != pi || flags2&mqttPubFlagDup == 0 {
		t.Fatalf("extra copy = %q pi %d dup=%v, want m1 under pi %d with DUP", payload2, pi2, flags2&mqttPubFlagDup != 0, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
}

// T15: each source gets at most one late NAK per cutoff. A drained offer that comes back to the
// still-draining predecessor (a NAK routed to interest a service reconnect re-sent) is decided
// again, and the second decision records a marker instead of NAKing again.
func TestLateNAKIsIssuedAtMostOncePerSource(t *testing.T) {
	rig := drainRigStart(t, "t15")
	dur := rig.durable()
	probe := newDrainProbe()
	probe.install(t, rig.sess)
	park := rig.parkNextDelivery()

	rig.publish("m1")
	drainAwait(t, park.parked, "the offer reaching its callback")
	predSub, predCli := park.parkedSub(), rig.client()
	reply := park.parkedReply()
	sseq, _, _, _, _ := ackReplyInfo(reply)

	rig.disconnect()
	drainAwait(t, probe.issuedCh, "the predecessor's drain being issued")
	_, _ = rig.connect(true)
	drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
	// The successor is at its Receive Maximum, so the re-offers stay unmapped.
	rig.sess.mu.Lock()
	rig.sess.rmax = 1
	rig.sess.maxp = 0
	rig.sess.mu.Unlock()

	// The first decision and a second for the same source, both through the predecessor's
	// detached arm.
	rig.sess.recordWithheldDelivery(predCli, predSub, reply)
	rig.sess.recordWithheldDelivery(predCli, predSub, reply)
	got := probe.decisionsFrom(dur, sseq, predCli)
	if len(got) != 2 || got[0] != mqttWithheldNAKOwed || got[1] != mqttWithheldRecorded {
		t.Fatalf("decisions for one source under one cutoff = %v, want NAK-owed then recorded", got)
	}
	if n := probe.lateNAKCount(reply); n != 1 {
		t.Fatalf("the source got %d late NAKs under one cutoff, want 1", n)
	}
	park.free()
}

// testDrainUnitEnv is a bound unit session with a referenced ordinary durable, a foreign
// predecessor connection and its draining delivery subscription, and the cutoff passed.
type testDrainUnitEnv struct {
	*testInflightEnv
	dur     string
	pred    *client
	predSub *subscription
}

func newTestDrainUnitEnv(t *testing.T) *testDrainUnitEnv {
	t.Helper()
	e, dur := testWithheldMarkerEnv(t)
	e.bind()
	pred := &client{}
	predSub := &subscription{client: pred, mqtt: &mqttSub{qos: 1, jsDur: dur, drainAtDetach: true,
		drainClaimed: true, drainIssued: true}}
	e.sess.mu.Lock()
	testReferenceDurableLocked(e.sess, dur)
	e.sess.setResumeCutoffLocked(e.sess.c)
	e.sess.mu.Unlock()
	return &testDrainUnitEnv{testInflightEnv: e, dur: dur, pred: pred, predSub: predSub}
}

func (u *testDrainUnitEnv) decide(sseq uint64, origin mqttOfferOrigin) mqttWithheldOutcome {
	u.sess.mu.Lock()
	defer u.sess.mu.Unlock()
	return u.sess.recordWithheldAttemptLocked(u.dur, sseq, testInflightAckSubject(u.dur, sseq, 2), origin)
}

// T10: the writer's rule, one condition at a time against the positive control.
func TestWithheldWriterRule(t *testing.T) {
	type tc struct {
		name  string
		setup func(u *testDrainUnitEnv) mqttOfferOrigin
		want  mqttWithheldOutcome
	}
	cases := []tc{
		{"positive control: foreign, after the cutoff, drain issued, referenced, unmapped",
			func(u *testDrainUnitEnv) mqttOfferOrigin { return mqttOfferOrigin{u.predSub, u.pred} }, mqttWithheldNAKOwed},
		{"mapped sequence", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.track(t, u.dur, 7, 1)
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRefused},
		{"unreferenced durable", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.sess.mu.Lock()
			u.sess.cons, u.sess.subs = nil, nil
			u.sess.mu.Unlock()
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRecorded},
		{"own origin", func(u *testDrainUnitEnv) mqttOfferOrigin {
			own := &subscription{client: u.c, mqtt: &mqttSub{qos: 1, jsDur: u.dur, drainAtDetach: true, drainClaimed: true, drainIssued: true}}
			return mqttOfferOrigin{own, u.c}
		}, mqttWithheldRecorded},
		{"zero origin (bookkeeping)", func(u *testDrainUnitEnv) mqttOfferOrigin { return mqttOfferOrigin{} }, mqttWithheldRecorded},
		{"nil binding, never resumed", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.sess.mu.Lock()
			u.sess.c, u.sess.resumeReadBy = nil, nil
			u.sess.mu.Unlock()
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRecorded},
		{"before the cutoff", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.sess.mu.Lock()
			u.sess.resumeReadBy = nil
			u.sess.mu.Unlock()
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRecorded},
		{"cutoff read by a different connection", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.sess.mu.Lock()
			u.sess.resumeReadBy = &client{}
			u.sess.mu.Unlock()
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRecorded},
		{"drain claimed, not issued", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.predSub.mqtt.drainIssued = false
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRecorded},
		{"not draining", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.predSub.mqtt.drainIssued, u.predSub.mqtt.drainClaimed = false, false
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRecorded},
		{"source already late-NAKed under this cutoff", func(u *testDrainUnitEnv) mqttOfferOrigin {
			u.sess.mu.Lock()
			u.sess.lateNAKed = map[mqttDeferredKey]struct{}{{u.dur, 7}: {}}
			u.sess.mu.Unlock()
			return mqttOfferOrigin{u.predSub, u.pred}
		}, mqttWithheldRecorded},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := newTestDrainUnitEnv(t)
			origin := c.setup(u)
			got := u.decide(7, origin)
			if got != c.want {
				t.Fatalf("decision = %v, want %v", got, c.want)
			}
			marked := testHasWithheldMarker(u.sess, u.dur, 7)
			if want := c.want == mqttWithheldRecorded; marked != want {
				t.Fatalf("marker present = %v, want %v", marked, want)
			}
		})
	}
}

// T13: a cutoff read by an EARLIER binding does not apply to the connection bound now: A's
// offers tagged after B's resume pass are not late under C before C's own pass, and C's pass
// starts a fresh late-NAK allowance.
func TestCutoffOfAnEarlierBindingDoesNotApply(t *testing.T) {
	u := newTestDrainUnitEnv(t) // bound: B (u.c), cutoff read by B
	notDraining := &subscription{client: u.pred, mqtt: &mqttSub{qos: 1, jsDur: u.dur, drainAtDetach: true}}
	if got := u.decide(7, mqttOfferOrigin{notDraining, u.pred}); got != mqttWithheldRecorded {
		t.Fatalf("fixture: A's offer under B's cutoff decided %v", got)
	}
	u.sess.mu.Lock()
	tagged := u.sess.deferredLate[mqttDeferredKey{u.dur, 7}].readBy == u.c
	cBind := &client{}
	u.sess.c = cBind // C binds; its resume pass has not run
	notDraining.mqtt.drainClaimed, notDraining.mqtt.drainIssued = true, true
	handoff := u.sess.handOffLateMarkersLocked(notDraining)
	u.sess.mu.Unlock()
	if !tagged {
		t.Fatal("fixture: the marker recorded after B's cutoff was not tagged")
	}
	if len(handoff) != 0 {
		t.Fatalf("A's drain issue under C handed off %v from B's cutoff", handoff)
	}
	if got := u.decide(8, mqttOfferOrigin{notDraining, u.pred}); got != mqttWithheldRecorded {
		t.Fatalf("A's offer after C bound, before C's pass, decided %v, want recorded", got)
	}
	u.sess.mu.Lock()
	u.sess.lateNAKed = map[mqttDeferredKey]struct{}{{u.dur, 9}: {}}
	u.sess.setResumeCutoffLocked(cBind)
	_, spent := u.sess.lateNAKed[mqttDeferredKey{u.dur, 9}]
	u.sess.mu.Unlock()
	if spent {
		t.Fatal("C's resume pass kept the earlier cutoff's late-NAK allowance")
	}
	if got := u.decide(9, mqttOfferOrigin{notDraining, u.pred}); got != mqttWithheldNAKOwed {
		t.Fatalf("A's offer after C's pass decided %v, want NAK-owed", got)
	}
}

// T14: the durables the ownership property honours are exactly the durables the rendered record
// names, and both equal an expectation WRITTEN here for each session shape.
func TestReferencedDurablesMatchTheRenderedRecord(t *testing.T) {
	e, _ := testWithheldMarkerEnv(t)
	sess := e.sess
	_, _, sharedMain := mqttSharedNames("g1", "sh.>", 3)
	_, _, sharedUp := mqttSharedNames("g1", "sh", 4)
	_, _, sharedOther := mqttSharedNames("g2", "other", 1)
	// The same groups' durables at incarnations the memberships do not hold: the level-up at
	// the main acquisition's incarnation, and another incarnation of a main.
	_, _, sharedUpAtMainInc := mqttSharedNames("g1", "sh", 3)
	_, _, sharedOtherInc := mqttSharedNames("g2", "other", 2)
	type shape struct {
		name  string
		build func()
		want  []string
		// not are durables the shape holds somewhere in memory that the record does NOT
		// reference, plus one it holds nowhere: the per-durable predicate must refuse each.
		not []string
	}
	reset := func() {
		sess.subs = map[string]byte{}
		sess.cons = map[string]*ConsumerConfig{}
		sess.drains, sess.drainByDur, sess.adoptedDrains, sess.sharedSubs = nil, nil, nil, nil
		sess.q2AffConsumer, sess.pubRelConsumer = nil, nil
	}
	shapes := []shape{
		{"ordinary membership", func() {
			sess.subs["a"] = 1
			sess.cons["a"] = &ConsumerConfig{Durable: "dur-a"}
		}, []string{"dur-a"}, []string{"dur-never"}},
		{"level-up sid normalised to its membership key", func() {
			sess.subs["b.>"] = 1
			sess.cons["b"+mqttMultiLevelSidSuffix] = &ConsumerConfig{Durable: "dur-b-fwc"}
			sess.cons["b2"+mqttMultiLevelSidSuffix] = &ConsumerConfig{Durable: "dur-b2-fwc"}
			sess.subs["b2"] = 1 // the level-up sid's membership key is "b2.>", not "b2"
		}, []string{"dur-b-fwc"}, []string{"dur-b2-fwc", "dur-never"}},
		{"consumer without a membership key", func() {
			sess.cons["c"] = &ConsumerConfig{Durable: "dur-c"}
		}, nil, []string{"dur-c", "dur-never"}},
		{"shared consumer in cons is not named through cons", func() {
			sess.subs["d"] = 1
			sess.cons["d"] = &ConsumerConfig{Durable: "dur-d", DeliverGroup: "q"}
		}, nil, []string{"dur-d", "dur-never"}},
		{"staged and active drains, removal-marked omitted", func() {
			sess.drains = map[string]*mqttDrain{
				"e1": {cc: &ConsumerConfig{Durable: "dur-e1"}, staged: true},
				"e2": {cc: &ConsumerConfig{Durable: "dur-e2"}},
				"e3": {cc: &ConsumerConfig{Durable: "dur-e3"}, removalMarked: true},
			}
		}, []string{"dur-e1", "dur-e2"}, []string{"dur-e3", "dur-never"}},
		{"adoption holding exactly cons[sid]", func() {
			cc := &ConsumerConfig{Durable: "dur-f"}
			sess.cons["f"] = cc
			sess.adoptedDrains = map[string]*mqttDrain{"f": {cc: cc}}
		}, []string{"dur-f"}, []string{"dur-never"}},
		{"adoption whose held config is not cons[sid]", func() {
			sess.cons["g"] = &ConsumerConfig{Durable: "dur-g"}
			sess.adoptedDrains = map[string]*mqttDrain{"g": {cc: &ConsumerConfig{Durable: "dur-g"}}}
		}, nil, []string{"dur-g", "dur-never"}},
		{"shared main and level-up memberships at their incarnations", func() {
			sess.sharedSubs = map[string]mqttSharedMembership{
				"$share/g1/sh.>":  {QoS: 1, Main: mqttSharedAcquisition{Inc: 3}, Up: &mqttSharedAcquisition{Inc: 4}},
				"$share/g2/other": {QoS: 1, Main: mqttSharedAcquisition{Inc: 1}},
			}
		}, []string{sharedMain, sharedUp, sharedOther}, []string{sharedUpAtMainInc, sharedOtherInc, "dur-never"}},
		{"affine and PUBREL", func() {
			sess.q2AffConsumer = &ConsumerConfig{Durable: "dur-aff"}
			sess.pubRelConsumer = &ConsumerConfig{Durable: "dur-pubrel"}
		}, []string{"dur-aff", "dur-pubrel"}, []string{"dur-never"}},
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			reset()
			sh.build()
			want := append([]string(nil), sh.want...)
			sort.Strings(want)
			var fromSet []string
			for d := range sess.recordReferencedDurablesLocked() {
				fromSet = append(fromSet, d)
			}
			sort.Strings(fromSet)
			rec := sess.buildPersistedSessionLocked()
			var rendered []string
			for _, cc := range rec.Cons {
				rendered = append(rendered, cc.Durable)
			}
			for _, d := range rec.Drains {
				rendered = append(rendered, d.Cons.Durable)
			}
			for key, m := range rec.SharedSubs {
				for _, n := range mqttSharedGroupNamesFor(key, m) {
					rendered = append(rendered, n.durable)
				}
			}
			if rec.Q2Aff != nil {
				rendered = append(rendered, rec.Q2Aff.Durable)
			}
			if rec.PubRel != nil {
				rendered = append(rendered, rec.PubRel.Durable)
			}
			sort.Strings(rendered)
			if fmt.Sprint(fromSet) != fmt.Sprint(want) {
				t.Fatalf("referenced durables = %v, want %v", fromSet, want)
			}
			if fmt.Sprint(rendered) != fmt.Sprint(want) {
				t.Fatalf("the rendered record names %v, want %v", rendered, want)
			}
			// The per-durable predicate the readers and the writer ask is pinned to the same
			// selection, one durable at a time.
			for _, d := range sh.want {
				if !sess.recordReferencesDurableLocked(d) {
					t.Errorf("recordReferencesDurableLocked(%q) = false, want true", d)
				}
			}
			for _, d := range sh.not {
				if sess.recordReferencesDurableLocked(d) {
					t.Errorf("recordReferencesDurableLocked(%q) = true, want false", d)
				}
			}
		})
	}
	sess.mu.Lock()
	reset()
	sess.mu.Unlock()
}

// testDrainAckSubjects subscribes to every acknowledgement published for dur on the service
// connection's server.
func testDrainAckSubjects(t *testing.T, nc *nats.Conn, dur string) *nats.Subscription {
	t.Helper()
	sub, err := nc.SubscribeSync("$JS.ACK." + mqttStreamName + "." + dur + ".>")
	if err != nil {
		t.Fatalf("subscribing to %s's acknowledgements: %v", dur, err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return sub
}

// testDrainNoNAKBefore publishes a sentinel on dur's acknowledgement subjects through the same
// queue the broker's NAKs use, and reports every acknowledgement published ahead of it: the
// queue is FIFO on one connection, so a NAK the pass published is there.
func testDrainNoNAKBefore(t *testing.T, sess *mqttSession, acks *nats.Subscription, dur string) []string {
	t.Helper()
	sentinel := "$JS.ACK." + mqttStreamName + "." + dur + ".sentinel"
	sess.jsa.sendMsg(sentinel, []byte("sentinel"))
	var before []string
	for {
		m, err := acks.NextMsg(testMQTTTimeout)
		if err != nil {
			t.Fatalf("fixture: the sentinel never arrived: %v", err)
		}
		if m.Subject == sentinel {
			return before
		}
		before = append(before, m.Subject)
	}
}

// testMarkerSkippedByEveryReader asserts the ownership property for one marker whose durable
// the record does not reference, with that absence settled: no reader acts on it, and each one
// that meets it deletes it. The marker is recorded again before each reader, so every reader
// is asked about it rather than only the first.
func testMarkerSkippedByEveryReader(t *testing.T, e *testInflightEnv, dur string, sseq uint64) {
	t.Helper()
	acks := testDrainAckSubjects(t, e.s.nrc, dur)
	key := mqttDeferredKey{dur, sseq}
	e.sess.mu.Lock()
	reply := e.sess.deferred[key]
	e.sess.mu.Unlock()
	if reply == _EMPTY_ {
		t.Fatal("fixture: no marker to read")
	}
	rerecord := func() {
		e.sess.mu.Lock()
		e.sess.recordWithheldAttemptLocked(dur, sseq, reply, mqttOfferOrigin{})
		e.sess.mu.Unlock()
		if !testHasWithheldMarker(e.sess, dur, sseq) {
			t.Fatal("fixture: the marker was not recorded again")
		}
	}
	pruned := func(reader string) {
		t.Helper()
		if testHasWithheldMarker(e.sess, dur, sseq) {
			t.Fatalf("the %s skipped the settled dead marker but did not delete it", reader)
		}
	}

	e.sess.mu.Lock()
	e.sess.maxp = 100
	naks := e.sess.deferredNaksLocked()
	e.sess.mu.Unlock()
	if len(naks) != 0 {
		t.Fatalf("a freed slot NAK'd %v for a durable the session no longer references", naks)
	}
	pruned("freed slot")

	rerecord()
	e.sess.mu.Lock()
	remains := e.sess.referencedMarkerRemainsLocked()
	e.sess.mu.Unlock()
	pruned("custody due check")

	rerecord()
	e.sess.mu.Lock()
	e.sess.custodyDemand = true
	e.sess.noteAllocationSucceededLocked()
	demand := e.sess.custodyDemand
	e.sess.mu.Unlock()
	if remains || demand {
		t.Fatalf("the custody due check counts the dead marker as a waiter (remains=%v, demand held=%v)", remains, demand)
	}
	pruned("custody demand release")

	rerecord()
	e.sess.resendPendingRedelivery(e.c, nil, nil)
	if got := testDrainNoNAKBefore(t, e.sess, acks, dur); len(got) != 0 {
		t.Fatalf("the resume pass NAK'd %v for a durable the session no longer references", got)
	}
	pruned("resume pass")
}

// T9: a predecessor's marker for a durable that has left the session is skipped by every
// reader: no NAK to the dead subject, no deferred allowance spent, no custody demand held. The
// committed UNSUBSCRIBE schedule end to end; the drain retirement and the shared release as the
// session states they leave.
func TestUnreferencedDurableMarkerIsSkippedByEveryReader(t *testing.T) {
	t.Run("committed UNSUBSCRIBE teardown with no started exchange", func(t *testing.T) {
		rig := drainRigStart(t, "t9a")
		dur := rig.durable()
		probe := newDrainProbe()
		probe.install(t, rig.sess)
		park := rig.parkNextDelivery()

		rig.publish("m1")
		drainAwait(t, park.parked, "the offer reaching its callback")
		sseq, _, _, _, _ := ackReplyInfo(park.parkedReply())
		rig.disconnect()
		drainAwait(t, probe.issuedCh, "the predecessor's drain being issued")
		c2, r2 := rig.connect(true)
		drainAwait(t, probe.cutoffCh, "the successor's resume pass reading the markers")
		testMQTTUnsub(t, 2, c2, r2, []*mqttFilter{{filter: rig.topic}})
		acks := testDrainAckSubjects(t, rig.s.nrc, dur)

		park.free()
		// The predecessor's drain is issued and the cutoff passed, but the durable is no
		// longer the session's: the writer records (permissively) and owes no NAK.
		testDrainWaitDecision(t, probe, dur, sseq, mqttWithheldRecorded)
		if !testHasWithheldMarker(rig.sess, dur, sseq) {
			t.Fatal("fixture: the late predecessor offer left no dead marker")
		}
		rig.sess.mu.Lock()
		rig.sess.maxp = 100
		naks := rig.sess.deferredNaksLocked()
		remains := rig.sess.referencedMarkerRemainsLocked()
		rig.sess.mu.Unlock()
		if len(naks) != 0 || remains {
			t.Fatalf("a reader acted on the dead marker: freed-slot NAKs %v, custody waiter %v", naks, remains)
		}
		// The unsubscribe committed and nothing is in flight, so the absence is settled and
		// the first reader that met the marker deleted it.
		if testHasWithheldMarker(rig.sess, dur, sseq) {
			t.Fatal("the settled dead marker outlived the reader pass that skipped it")
		}

		// A later resume pass, observed to its end, NAKs nothing for it.
		passDone := make(chan struct{})
		var doneOnce sync.Once
		restore := setMQTTTestRestorePause(func(at string) {
			if at == "resume-naks-done" {
				doneOnce.Do(func() { close(passDone) })
			}
		})
		defer restore()
		testMQTTDisconnect(t, c2, nil)
		c2.Close()
		_, _ = rig.connect(true)
		drainAwait(t, passDone, "the next resume pass ending")
		if got := testDrainNoNAKBefore(t, rig.sess, acks, dur); len(got) != 0 {
			t.Fatalf("the resume pass NAK'd %v for the unsubscribed durable", got)
		}
	})
	t.Run("UNSUBSCRIBE drain retired after its started exchange completed", func(t *testing.T) {
		e, dur := testWithheldMarkerEnv(t)
		e.bind()
		e.sess.mu.Lock()
		e.sess.drains = map[string]*mqttDrain{"retiring": {cc: &ConsumerConfig{Durable: dur}}}
		if !e.sess.recordReferencesDurableLocked(dur) {
			e.sess.mu.Unlock()
			t.Fatal("fixture: the active drain does not reference its durable")
		}
		delete(e.sess.drains, "retiring") // the retirement removed it
		e.sess.recordWithheldAttemptLocked(dur, 11, testInflightAckSubject(dur, 11, 2), mqttOfferOrigin{})
		e.sess.mu.Unlock()
		testMarkerSkippedByEveryReader(t, e, dur, 11)
	})
	t.Run("ownership ends while the resume pass is between its read and its NAKs", func(t *testing.T) {
		e, dur := testWithheldMarkerEnv(t)
		e.bind()
		e.sess.mu.Lock()
		testReferenceDurableLocked(e.sess, dur)
		e.sess.maxp = 100
		e.sess.recordWithheldAttemptLocked(dur, 13, testInflightAckSubject(dur, 13, 2), mqttOfferOrigin{})
		e.sess.mu.Unlock()
		acks := testDrainAckSubjects(t, e.s.nrc, dur)
		var ended bool
		restore := setMQTTTestRestorePause(func(at string) {
			if at != "reap-scanned" || ended {
				return
			}
			ended = true
			e.sess.mu.Lock()
			e.sess.cons, e.sess.subs = nil, nil // the teardown commits while the lock is released
			e.sess.mu.Unlock()
		})
		defer restore()
		e.sess.resendPendingRedelivery(e.c, nil, nil)
		if !ended {
			t.Fatal("fixture: the reap-scanned boundary was never reached")
		}
		if got := testDrainNoNAKBefore(t, e.sess, acks, dur); len(got) != 0 {
			t.Fatalf("the resume pass NAK'd %v for a durable whose ownership ended before its NAKs", got)
		}
		if testHasWithheldMarker(e.sess, dur, 13) {
			t.Fatal("the resume pass skipped the marker of a settled ended ownership but did not delete it")
		}
	})
	t.Run("shared release of the last membership", func(t *testing.T) {
		e, _ := testWithheldMarkerEnv(t)
		e.bind()
		key := "$share/rel/t9c"
		m := mqttSharedMembership{QoS: 1, Main: mqttSharedAcquisition{Inc: 2}}
		dur := mqttSharedGroupNamesFor(key, m)[0].durable
		e.sess.mu.Lock()
		e.sess.sharedSubs = map[string]mqttSharedMembership{key: m}
		if !e.sess.recordReferencesDurableLocked(dur) {
			e.sess.mu.Unlock()
			t.Fatal("fixture: the shared membership does not reference its group durable")
		}
		delete(e.sess.sharedSubs, key) // the last membership released
		e.sess.recordWithheldAttemptLocked(dur, 12, testInflightAckSubject(dur, 12, 2), mqttOfferOrigin{})
		e.sess.mu.Unlock()
		testMarkerSkippedByEveryReader(t, e, dur, 12)
	})
}

// T16: a marker whose durable a LIVE ownership other than an ordinary membership references
// still counts: a surviving UNSUBSCRIBE drain, an adopted drain, a shared level-up membership,
// the affine QoS 2 durable.
func TestMarkersOfLiveOwnershipStillCount(t *testing.T) {
	cases := []struct {
		name string
		own  func(sess *mqttSession) string
	}{
		{"surviving UNSUBSCRIBE drain", func(sess *mqttSession) string {
			sess.drains = map[string]*mqttDrain{"gone": {cc: &ConsumerConfig{Durable: "dur-drain-" + nuid.Next()}}}
			return sess.drains["gone"].cc.Durable
		}},
		{"adopted drain", func(sess *mqttSession) string {
			cc := &ConsumerConfig{Durable: "dur-adopted-" + nuid.Next()}
			sess.cons = map[string]*ConsumerConfig{"adopted": cc}
			sess.adoptedDrains = map[string]*mqttDrain{"adopted": {cc: cc}}
			return cc.Durable
		}},
		{"shared level-up membership", func(sess *mqttSession) string {
			key := "$share/lu/live.>"
			m := mqttSharedMembership{QoS: 1, Main: mqttSharedAcquisition{Inc: 5}, Up: &mqttSharedAcquisition{Inc: 6}}
			sess.sharedSubs = map[string]mqttSharedMembership{key: m}
			_, _, up := mqttSharedNames("lu", "live", 6)
			return up
		}},
		{"affine QoS 2 durable", func(sess *mqttSession) string {
			sess.q2AffConsumer = &ConsumerConfig{Durable: "dur-aff-" + nuid.Next()}
			return sess.q2AffConsumer.Durable
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e, _ := testWithheldMarkerEnv(t)
			e.bind()
			e.sess.mu.Lock()
			defer e.sess.mu.Unlock()
			dur := c.own(e.sess)
			reply := testInflightAckSubject(dur, 21, 2)
			e.sess.recordWithheldAttemptLocked(dur, 21, reply, mqttOfferOrigin{})
			e.sess.maxp = 100
			if !e.sess.referencedMarkerRemainsLocked() {
				t.Fatal("the custody due check does not count a live ownership's marker")
			}
			if naks := e.sess.deferredNaksLocked(); len(naks) != 1 || naks[0] != reply {
				t.Fatalf("a freed slot NAK'd %v, want the live ownership's marker %q", naks, reply)
			}
		})
	}
}

// T17: a marker whose membership an UNSUBSCRIBE removes only TENTATIVELY is skipped, never
// deleted, while the packet's save is in flight; the save fails definitely, the membership is
// restored, and the marker is still there for the next freed slot to NAK.
func TestRolledBackUnsubscribeKeepsItsMarker(t *testing.T) {
	rig := drainRigStart(t, "t17")
	dur := rig.durable()
	rig.sess.mu.Lock()
	rig.sess.maxp = 0 // every new delivery is withheld at the cap, nothing is in flight
	rig.sess.mu.Unlock()
	probe := newDrainProbe()
	probe.install(t, rig.sess)
	rig.publish("m1")
	var sseq uint64
	deadline := time.Now().Add(testMQTTTimeout)
	for sseq == 0 && time.Now().Before(deadline) {
		probe.mu.Lock()
		for _, d := range probe.decisions {
			if d.dur == dur && d.outcome == mqttWithheldRecorded {
				sseq = d.sseq
			}
		}
		probe.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	if sseq == 0 {
		t.Fatal("fixture: m1 was never withheld at the cap")
	}

	type midSave struct {
		naks             []string
		remains, kept    bool
		referenced, seen bool
	}
	var mid midSave
	restore := setMQTTTestSessSaveHook(func(s *mqttSession) error {
		if s != rig.sess {
			return nil
		}
		s.mu.Lock()
		if mid.seen {
			s.mu.Unlock()
			return nil
		}
		mid.seen = true
		mid.referenced = s.recordReferencesDurableLocked(dur)
		s.maxp = 100 // a slot frees while the save is in flight
		mid.naks = s.deferredNaksLocked()
		mid.remains = s.referencedMarkerRemainsLocked()
		_, mid.kept = s.deferred[mqttDeferredKey{dur, sseq}]
		s.maxp = 0
		s.mu.Unlock()
		return fmt.Errorf("injected definite save failure")
	})
	defer restore()

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(2 + 2 + len(rig.topic))
	w.WriteUint16(9)
	w.WriteBytes([]byte(rig.topic))
	if _, err := testMQTTWrite(rig.c, w.Bytes()); err != nil {
		t.Fatalf("writing UNSUBSCRIBE: %v", err)
	}
	deadline = time.Now().Add(testMQTTTimeout)
	restored := false
	for !restored && time.Now().Before(deadline) {
		rig.sess.mu.Lock()
		restored = mid.seen && rig.sess.recordReferencesDurableLocked(dur)
		rig.sess.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	restore()
	if !mid.seen {
		t.Fatal("fixture: the UNSUBSCRIBE never reached its save")
	}
	if mid.referenced {
		t.Fatal("fixture: the in-flight UNSUBSCRIBE had not removed the membership")
	}
	if len(mid.naks) != 0 || mid.remains {
		t.Fatalf("a reader acted on the tentatively unreferenced marker: NAKs %v, waiter %v", mid.naks, mid.remains)
	}
	if !mid.kept {
		t.Fatal("a reader deleted the marker of a membership the save could still restore")
	}
	if !restored {
		t.Fatal("the definite save failure did not restore the membership")
	}
	rig.sess.mu.Lock()
	rig.sess.maxp = 100
	naks := rig.sess.deferredNaksLocked()
	rig.sess.mu.Unlock()
	if len(naks) != 1 {
		t.Fatalf("the next freed slot NAK'd %v, want the restored membership's marker", naks)
	}
	if s, _, _, _, _ := ackReplyInfo(naks[0]); s != sseq {
		t.Fatalf("the next freed slot NAK'd sequence %d, want %d", s, sseq)
	}
}

// A marker whose durable the record no longer references is deleted, with its late tag, by
// the first reader pass that meets it once that absence is SETTLED: no record transaction is
// open and no record write is unresolved. While either is, the absence is tentative (the
// transaction that removed the membership may restore it) and every reader only skips it.
func TestSettledUnreferencedMarkerIsPrunedAtTheNextReaderPass(t *testing.T) {
	readers := []struct {
		name string
		// tagSurvivesOnlyByThePrune: the reader itself does not otherwise clear late tags (a
		// resume pass sets a new cutoff, which forgets every tag whatever this rule does).
		tagSurvivesOnlyByThePrune bool
		pass                      func(t *testing.T, u *testDrainUnitEnv)
	}{
		{"freed slot", true, func(t *testing.T, u *testDrainUnitEnv) {
			u.sess.mu.Lock()
			u.sess.maxp = 100
			naks := u.sess.deferredNaksLocked()
			u.sess.mu.Unlock()
			if len(naks) != 0 {
				t.Fatalf("a freed slot NAK'd %v for an unreferenced durable", naks)
			}
		}},
		{"custody due check", true, func(t *testing.T, u *testDrainUnitEnv) {
			u.sess.mu.Lock()
			remains := u.sess.referencedMarkerRemainsLocked()
			u.sess.mu.Unlock()
			if remains {
				t.Fatal("the custody due check counts an unreferenced marker as a waiter")
			}
		}},
		{"drain-issue handoff", true, func(t *testing.T, u *testDrainUnitEnv) {
			u.sess.mu.Lock()
			naks := u.sess.handOffLateMarkersLocked(u.predSub)
			u.sess.mu.Unlock()
			if len(naks) != 0 {
				t.Fatalf("the drain-issue handoff NAK'd %v for an unreferenced durable", naks)
			}
		}},
		{"resume pass", false, func(t *testing.T, u *testDrainUnitEnv) {
			acks := testDrainAckSubjects(t, u.s.nrc, u.dur)
			u.sess.resendPendingRedelivery(u.c, nil, nil)
			if got := testDrainNoNAKBefore(t, u.sess, acks, u.dur); len(got) != 0 {
				t.Fatalf("the resume pass NAK'd %v for an unreferenced durable", got)
			}
		}},
	}
	absences := []struct {
		name      string
		tentative func(s *mqttSession) (undo func())
	}{
		{"settled", nil},
		{"record transaction open", func(s *mqttSession) func() {
			s.recordTxOpen = true
			return func() { s.recordTxOpen = false }
		}},
		{"record write unresolved", func(s *mqttSession) func() {
			s.seqUncertain = true
			return func() { s.seqUncertain = false }
		}},
	}
	for _, r := range readers {
		for _, a := range absences {
			t.Run(r.name+"/"+a.name, func(t *testing.T) {
				u := newTestDrainUnitEnv(t)
				key := mqttDeferredKey{u.dur, 7}
				u.sess.mu.Lock()
				u.sess.cons, u.sess.subs = nil, nil // the ownership ended
				u.sess.mu.Unlock()
				// The predecessor's drain is issued and the cutoff passed, so the writer
				// records the unreferenced offer and tags it for the drain's handoff.
				if got := u.decide(7, mqttOfferOrigin{u.predSub, u.pred}); got != mqttWithheldRecorded {
					t.Fatalf("fixture: decision = %v, want recorded", got)
				}
				u.sess.mu.Lock()
				_, tagged := u.sess.deferredLate[key]
				var undo func()
				if a.tentative != nil {
					undo = a.tentative(u.sess)
				}
				u.sess.mu.Unlock()
				if !tagged {
					t.Fatal("fixture: the late marker carries no tag")
				}
				r.pass(t, u)
				u.sess.mu.Lock()
				if undo != nil {
					undo()
				}
				_, kept := u.sess.deferred[key]
				_, tagKept := u.sess.deferredLate[key]
				u.sess.mu.Unlock()
				wantKept := a.tentative != nil
				if kept != wantKept {
					t.Fatalf("marker kept = %v after the %s, want %v", kept, r.name, wantKept)
				}
				if r.tagSurvivesOnlyByThePrune && tagKept != wantKept {
					t.Fatalf("late tag kept = %v after the %s, want %v", tagKept, r.name, wantKept)
				}
			})
		}
	}
}

// A reader pass over markers of more distinct durables than its per-durable memo holds
// (mqttMarkerOwnershipMemo) switches to the referenced set and decides every marker as the
// per-durable predicate would: the referenced ones are NAKed, the others deleted.
func TestReaderPassOverManyDurablesDecidesEachMarkerByTheRecord(t *testing.T) {
	e, _ := testWithheldMarkerEnv(t)
	e.bind()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	e.sess.maxp = 1000
	want := map[string]bool{}
	for i := 0; i < 3*mqttMarkerOwnershipMemo; i++ {
		dur := fmt.Sprintf("%s_many_%d", e.sess.idHash, i)
		if i%2 == 0 {
			testReferenceDurableLocked(e.sess, dur)
		}
		reply := testInflightAckSubject(dur, uint64(100+i), 2)
		e.sess.recordWithheldAttemptLocked(dur, uint64(100+i), reply, mqttOfferOrigin{})
		want[reply] = i%2 == 0
	}
	naks := e.sess.deferredNaksLocked()
	got := map[string]bool{}
	for _, n := range naks {
		got[n] = true
	}
	for reply, referenced := range want {
		if got[reply] != referenced {
			t.Errorf("marker %q NAKed = %v, want %v (whether its durable is referenced)", reply, got[reply], referenced)
		}
	}
	if len(e.sess.deferred) != 0 {
		t.Errorf("%d markers remain after the pass, want none (unreferenced ones deleted, settled)", len(e.sess.deferred))
	}
}

// T18: a shared rejoin of a group incarnation another member kept alive, while the release is
// still TENTATIVE, makes the old marker eligible again, and it is NAKed once, to the group: an
// extra offer at most, by choice. Once the release is settled, the first reader pass deletes
// the marker, and a later rejoin finds nothing to NAK (the group's AckWait re-offers it).
func TestSharedRejoinNAKsToTheGroup(t *testing.T) {
	setup := func(t *testing.T) (*testInflightEnv, string, mqttSharedMembership, string) {
		e, _ := testWithheldMarkerEnv(t)
		e.bind()
		key := "$share/rejoin/t18"
		m := mqttSharedMembership{QoS: 1, Main: mqttSharedAcquisition{Inc: 7}}
		dur := mqttSharedGroupNamesFor(key, m)[0].durable
		reply := testInflightAckSubject(dur, 31, 2)
		e.sess.mu.Lock()
		e.sess.maxp = 100
		e.sess.sharedSubs = map[string]mqttSharedMembership{key: m}
		e.sess.recordWithheldAttemptLocked(dur, 31, reply, mqttOfferOrigin{})
		e.sess.mu.Unlock()
		return e, key, m, reply
	}
	t.Run("rejoin before the release settles", func(t *testing.T) {
		e, key, m, reply := setup(t)
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		e.sess.recordTxOpen = true // the release's own record transaction
		delete(e.sess.sharedSubs, key)
		if naks := e.sess.deferredNaksLocked(); len(naks) != 0 {
			t.Fatalf("a released group's marker was NAK'd: %v", naks)
		}
		e.sess.sharedSubs[key] = m // rejoined at the incarnation another member kept alive
		e.sess.recordTxOpen = false
		if naks := e.sess.deferredNaksLocked(); len(naks) != 1 || naks[0] != reply {
			t.Fatalf("the rejoined group's marker NAKs = %v, want exactly %q", naks, reply)
		}
		if naks := e.sess.deferredNaksLocked(); len(naks) != 0 {
			t.Fatalf("the rejoined group's marker was NAK'd again: %v", naks)
		}
	})
	t.Run("rejoin after the release settled", func(t *testing.T) {
		e, key, m, _ := setup(t)
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		delete(e.sess.sharedSubs, key) // released, and the release committed
		if naks := e.sess.deferredNaksLocked(); len(naks) != 0 {
			t.Fatalf("a released group's marker was NAK'd: %v", naks)
		}
		e.sess.sharedSubs[key] = m
		if naks := e.sess.deferredNaksLocked(); len(naks) != 0 {
			t.Fatalf("the settled release's marker survived to be NAK'd after the rejoin: %v", naks)
		}
	})
}

// testBenchMarkerSession is a session with a large subscription set: 2000 ordinary
// memberships, 200 shared ".>" memberships with level-up, and room for one NAK per pass.
func testBenchMarkerSession() *mqttSession {
	sess := &mqttSession{subs: map[string]byte{}, cons: map[string]*ConsumerConfig{},
		deferred: map[mqttDeferredKey]string{}, maxp: 1}
	for i := 0; i < 2000; i++ {
		f := fmt.Sprintf("bench.%d", i)
		sess.subs[f] = 1
		sess.cons[f] = &ConsumerConfig{Durable: fmt.Sprintf("dur-%d", i)}
	}
	sess.sharedSubs = map[string]mqttSharedMembership{}
	for i := 0; i < 200; i++ {
		sess.sharedSubs[fmt.Sprintf("$share/g%d/s.>", i)] = mqttSharedMembership{QoS: 1,
			Main: mqttSharedAcquisition{Inc: 1}, Up: &mqttSharedAcquisition{Inc: 1}}
	}
	return sess
}

// B1: the ownership property's cost under sess.mu, with a large subscription set and a backlog
// of markers none of which is referenced, the absence TENTATIVE (a record transaction open), so
// every pass scans the whole backlog and deletes nothing.
func BenchmarkReferencedDurablesUnderSessionLock(b *testing.B) {
	sess := testBenchMarkerSession()
	sess.recordTxOpen = true
	for i := 0; i < 500; i++ {
		sess.deferred[mqttDeferredKey{"dead-dur", uint64(i + 1)}] = "$JS.ACK.x"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.mu.Lock()
		if sess.referencedMarkerRemainsLocked() {
			b.Fatal("an unreferenced backlog counted as a waiter")
		}
		sess.mu.Unlock()
	}
}

// B2: sustained PUBACK-path reader passes (deferredNaksLocked) over 500 markers that outlived
// their durable's ownership, the absence settled: the first pass deletes them, and every later
// pass is the empty fast path.
func BenchmarkFreedSlotPassesOverLingeringUnreferencedMarkers(b *testing.B) {
	sess := testBenchMarkerSession()
	for i := 0; i < 500; i++ {
		sess.deferred[mqttDeferredKey{"dead-dur", uint64(i + 1)}] = "$JS.ACK.x"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.mu.Lock()
		if naks := sess.deferredNaksLocked(); len(naks) != 0 {
			b.Fatalf("a freed slot NAK'd unreferenced markers: %v", naks)
		}
		sess.mu.Unlock()
	}
}

// B3: sustained PUBACK-path reader passes over a live backlog of 500 markers of one referenced
// durable: each pass NAKs the lowest sequence (one free slot), which is recorded again so the
// backlog stays at 500.
func BenchmarkFreedSlotPassesOverAReferencedBacklog(b *testing.B) {
	sess := testBenchMarkerSession()
	for i := 0; i < 500; i++ {
		sess.deferred[mqttDeferredKey{"dur-7", uint64(i + 1)}] = "$JS.ACK.x"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.mu.Lock()
		naks := sess.deferredNaksLocked()
		if len(naks) != 1 {
			b.Fatalf("a freed slot NAK'd %d markers, want 1", len(naks))
		}
		sess.deferred[mqttDeferredKey{"dur-7", 1}] = naks[0]
		sess.mu.Unlock()
	}
}
