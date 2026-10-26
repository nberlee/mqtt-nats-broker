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
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"

	"github.com/nats-io/nats.go"
)

// The F-105 client-visible witnesses (WP-F step 4): an obligation whose PUBREC was taken on one
// broker, carried to a PEER broker by the client's next CONNECT, through the assembled production
// route with real clients.
//
// The two frontends share one JetStream — one embedded nats-server, two Servers each with its
// own NATS connection — which is the supported multi-frontend topology. A peer resume is not a
// same-process reconnect, so the owed PUBREL waits out AckWait there (CONFORMANCE "Outbound
// in-flight identifiers survive a restart and a failover", the promptness residual); both brokers
// run with a 1 s AckWait so the witnesses measure survival and completion, not that residual.

// testConsumerDeleteWatch observes every consumer delete in two layers.
//
// SYNCHRONOUS PROVENANCE: the request seam at the entry of jsa.deleteConsumer runs on the
// deleting goroutine before anything is sent, so the witness judges each delete against the state
// that stood at that moment — never against whatever an asynchronous observer reads later, when
// an authorization may already have committed.
//
// EXHAUSTIVENESS: a subscription on every CONSUMER.DELETE request the shared JetStream receives,
// whatever issued it. A delete the bus saw and the seam never did took a route around
// jsa.deleteConsumer. The two logs are compared by target and multiplicity only behind a barrier
// that drains each broker's own send queue (see barrier).
type testConsumerDeleteWatch struct {
	mu       sync.Mutex
	bus      map[string]int // target "<stream>.<durable>" -> requests the bus saw
	hooked   map[string]int // target -> requests jsa.deleteConsumer issued
	markers  map[string]bool
	judge    func(stream, durable string) error
	violated []string
	// busDelay, when set, delays the bus callback: the negative control for a late observer.
	busDelay time.Duration
}

const testConsumerDeletePrefix = "$JS.API.CONSUMER.DELETE."

func newTestConsumerDeleteWatch(t *testing.T, s *Server) *testConsumerDeleteWatch {
	t.Helper()
	w := &testConsumerDeleteWatch{bus: map[string]int{}, hooked: map[string]int{}, markers: map[string]bool{}}
	hook := func(stream, consumer string) {
		target := stream + "." + consumer
		w.mu.Lock()
		w.hooked[target]++
		judge := w.judge
		w.mu.Unlock()
		if judge == nil {
			return
		}
		if err := judge(stream, consumer); err != nil {
			w.mu.Lock()
			w.violated = append(w.violated, target+": "+err.Error())
			w.mu.Unlock()
		}
	}
	prev := mqttTestConsumerDeleteRequestHookV.Swap(&hook)
	t.Cleanup(func() { mqttTestConsumerDeleteRequestHookV.Store(prev) })

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connecting the delete observer: %v", err)
	}
	sub, err := nc.Subscribe(testConsumerDeletePrefix+">", func(m *nats.Msg) {
		w.mu.Lock()
		delay := w.busDelay
		w.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		target := strings.TrimPrefix(m.Subject, testConsumerDeletePrefix)
		w.mu.Lock()
		if strings.HasPrefix(target, "MARKER.") {
			w.markers[target] = true
		} else {
			w.bus[target]++
		}
		w.mu.Unlock()
	})
	if err != nil {
		nc.Close()
		t.Fatalf("subscribing the delete observer: %v", err)
	}
	if err := nc.Flush(); err != nil {
		nc.Close()
		t.Fatalf("flushing the delete observer: %v", err)
	}
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
		_ = nc.Drain()
	})
	return w
}

// setJudge installs the provenance rule each later delete is judged by, at request time.
func (w *testConsumerDeleteWatch) setJudge(judge func(stream, durable string) error) {
	w.mu.Lock()
	w.judge = judge
	w.mu.Unlock()
}

// barrier drains every named broker's send queue into the observer, then fails the test on any
// delete that reached the bus without passing jsa.deleteConsumer, or that the provenance rule
// refused.
//
// A noWait delete is only QUEUED by sendMsgReply and published later by sendJSAPIrequests, so a
// marker sent on any other connection could overtake it. Each marker is therefore enqueued through
// the broker's OWN queue: one queue publishes in order and one subscription's callbacks run in
// order, so once a broker's marker has been observed, every delete that broker issued before it
// has been published and observed. The caller quiesces the producers first.
func (w *testConsumerDeleteWatch) barrier(t *testing.T, what string, brokers ...*mqttAccountSessionManager) {
	t.Helper()
	if err := w.barrierErr(what, brokers...); err != nil {
		t.Fatal(err)
	}
}

// barrierErr is barrier reporting instead of failing, for the observer's own negative control.
func (w *testConsumerDeleteWatch) barrierErr(what string, brokers ...*mqttAccountSessionManager) error {
	var want []string
	for _, asm := range brokers {
		m := "MARKER." + nuid.Next()
		want = append(want, m)
		asm.jsa.sendMsg(testConsumerDeletePrefix+m, nil)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		w.mu.Lock()
		drained := true
		for _, m := range want {
			drained = drained && w.markers[m]
		}
		w.mu.Unlock()
		if drained {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: the observer never drained the brokers' markers %v", what, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for target, n := range w.bus {
		if w.hooked[target] < n {
			return fmt.Errorf("%s: %d delete(s) of %s reached JetStream, %d through jsa.deleteConsumer: a route bypassed it",
				what, n, target, w.hooked[target])
		}
	}
	if len(w.violated) != 0 {
		return fmt.Errorf("%s: a delete was requested without its licence: %v", what, w.violated)
	}
	return nil
}

// sawDelete reports whether a delete of durable on stream was requested through any route.
func (w *testConsumerDeleteWatch) sawDelete(stream, durable string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	target := stream + "." + durable
	return w.bus[target] > 0 || w.hooked[target] > 0
}

// testPeerWorld is one QoS 2 obligation past PUBREC on broker A, with broker B a peer frontend on
// the same JetStream.
type testPeerWorld struct {
	oA, oB  *Options
	sA, sB  *Server
	cid     string
	idHash  string
	epoch   string
	gen     string
	nonce   string
	pi      uint16
	src     string // the source consumer's durable
	srcSeq  uint64 // the source delivery's $MQTT_msgs stream sequence, as the register names it
	pubRel  string // the PUBREL consumer's durable
	relSubj string // the subject the canonical PUBREL is stored on
	srcInc  string
	relInc  string
	tenureA mqttOutboundEntitlement
	deletes *testConsumerDeleteWatch
	// asmA and asmB are the account's session managers on each broker.
	asmA, asmB *mqttAccountSessionManager
}

func testPeerWorldAtPubRec(t *testing.T, cid string) *testPeerWorld {
	t.Helper()
	return testPeerWorldAt(t, cid, 0)
}

// testPeerWorldAt builds the world; a nonzero v5Expiry makes the obligation's client an MQTT 5
// client with that Session Expiry Interval (seconds) instead of a 3.1.1 persistent session.
func testPeerWorldAt(t *testing.T, cid string, v5Expiry uint32) *testPeerWorld {
	t.Helper()
	return testPeerWorldBuild(t, cid, v5Expiry, nil)
}

// testPeerWorldBuild is testPeerWorldAt with beforePubRec, when non-nil, run against broker A's
// session manager once the 3.1.1 delivery is admitted and before its PUBREC is sent.
func testPeerWorldBuild(t *testing.T, cid string, v5Expiry uint32, beforePubRec func(asmA *mqttAccountSessionManager)) *testPeerWorld {
	t.Helper()
	if beforePubRec != nil && v5Expiry != 0 {
		t.Fatalf("fixture: beforePubRec is wired for the 3.1.1 delivery only")
	}
	w := &testPeerWorld{cid: cid, idHash: getHash(cid)}
	w.oA = testMQTTDefaultOptions()
	w.oA.MQTT.AckWait = time.Second
	w.sA = testMQTTRunServer(t, w.oA)
	t.Cleanup(func() { testMQTTShutdownServer(w.sA) }) // owns the shared nats-server: last
	w.oB = testMQTTDefaultOptions()
	w.oB.MQTT.AckWait = time.Second
	testSetStoreDir(w.oB, testStoreDir(w.oA))
	w.sB = testMQTTRunServer(t, w.oB)
	t.Cleanup(func() { w.sB.Shutdown() })
	w.deletes = newTestConsumerDeleteWatch(t, w.sA)
	// B's session manager, reached through a throwaway clean client before the witness's client
	// ever arrives there.
	probe, probeR := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-probe", cleanSess: true}, w.oB.MQTT.Host, w.oB.MQTT.Port)
	testMQTTCheckConnAck(t, probeR, mqttConnAckRCConnectionAccepted, false)
	w.asmB = testMQTTGetAccountSessionManager(t, w.sB, cid+"-probe")
	probe.Close()

	var (
		sub   net.Conn
		sessA *mqttSession
		pi    uint16
		nonce string
	)
	if v5Expiry == 0 {
		var subr *mqttReader
		sub, subr, sessA, pi, nonce = testF105QoS2Delivered(t, w.oA, w.sA, cid)
		if beforePubRec != nil {
			beforePubRec(sessA.asm)
		}
		testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	} else {
		var subr *mqttReader
		sub, subr = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
			props: mqttV5ConnPropsSessionExpiry(v5Expiry)}, w.oA.MQTT.Host, w.oA.MQTT.Port)
		t.Cleanup(func() { sub.Close() })
		if present, rc, _ := testMQTTReadConnAckV5(t, subr); rc != mqttConnAckRCConnectionAccepted || present {
			t.Fatalf("fixture CONNACK: rc %d present %v", rc, present)
		}
		testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})
		pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, w.oA.MQTT.Host, w.oA.MQTT.Port)
		testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, pc, pr, 2, false, false, "foo", 1, []byte("payload"))
		pc.Close()
		_, _, _, pi, _ = testMQTTReadPubV5Packet(t, subr)
		testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
		_, sessA = testResidentSession(t, w.sA, cid)
		sessA.mu.Lock()
		g := sessA.gen
		sessA.mu.Unlock()
		reg, _, lerr := sessA.jsa.loadInflightRecord(mqttInflightSubject(w.idHash, g, pi))
		if lerr != nil || reg == nil {
			t.Fatalf("fixture: the delivered register does not load back: %v", lerr)
		}
		nonce = reg.Lin
	}
	testF105ExchangePhaseIs(t, sessA, nonce, mqttExchangePubRecObserved)
	w.pi, w.nonce = pi, nonce

	sessA.mu.Lock()
	w.gen, w.epoch, w.tenureA = sessA.gen, sessA.asm.sessStreamEpoch, sessA.boundEnt
	for _, cc := range sessA.cons {
		w.src = cc.Durable
	}
	if sessA.pubRelConsumer != nil {
		w.pubRel = sessA.pubRelConsumer.Durable
	}
	sessA.mu.Unlock()
	w.relSubj, _, _ = mqttPubRelNames(w.idHash, w.gen)
	if w.src == _EMPTY_ || w.pubRel == _EMPTY_ {
		t.Fatalf("fixture: the session holds no source (%q) or PUBREL (%q) consumer", w.src, w.pubRel)
	}
	asm := sessA.asm
	w.asmA = asm
	for _, n := range []struct {
		stream mqttOwnedStream
		name   string
		inc    *string
	}{{mqttOwnedStreamMsgs, w.src, &w.srcInc}, {mqttOwnedStreamOut, w.pubRel, &w.relInc}} {
		own, _, err := asm.jsa.loadNameOwnershipPhysical(w.epoch, n.stream, n.name)
		if err != nil || own == nil {
			t.Fatalf("fixture: %q has no ownership record: %v", n.name, err)
		}
		*n.inc = own.OwnerInc
	}
	// The obligation's evidence stands before anything moves.
	if err := w.evidenceStands(asm); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	reg, _, err := asm.jsa.loadInflightRecord(mqttInflightSubject(w.idHash, w.gen, w.pi))
	if err != nil || reg == nil || reg.Dur != w.src || reg.Seq == 0 {
		t.Fatalf("fixture: the register does not name the source %q at a sequence: %+v (%v)", w.src, reg, err)
	}
	w.srcSeq = reg.Seq
	sub.Close()
	return w
}

// evidenceStands reports nil when everything the obligation's completion depends on is intact:
// the exchange at PubRecObserved, its register past PUBREC, the canonical PUBREL stored under the
// obligation's identifier, the PUBREL consumer present, and both resources holding the
// obligation as a dependency.
func (w *testPeerWorld) evidenceStands(asm *mqttAccountSessionManager) error {
	reg, _, err := asm.jsa.loadInflightRecord(mqttInflightSubject(w.idHash, w.gen, w.pi))
	if err != nil || reg == nil || !mqttInflightPastPubRec(reg.State) || reg.Lin != w.nonce {
		return fmt.Errorf("the register is not past PUBREC for the obligation: %+v (%v)", reg, err)
	}
	return w.obligationStands(asm)
}

// obligationStands is evidenceStands without the slot register. A discarding teardown removes the
// discarded generation's registers as session METADATA; that removal is not the obligation's
// ending, which only the exchange's own phase records (consult 129).
func (w *testPeerWorld) obligationStands(asm *mqttAccountSessionManager) error {
	x, _, err := asm.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, w.nonce)
	if err != nil || x == nil || x.Phase != mqttExchangePubRecObserved {
		return fmt.Errorf("the exchange does not stand at PubRecObserved: %+v (%v)", x, err)
	}
	recs, _, ok := f099RealPubRelScan(asm, w.relSubj)
	stored := false
	for _, r := range recs {
		stored = stored || r.pi == w.pi
	}
	if !ok || !stored {
		return fmt.Errorf("no canonical PUBREL stored for identifier %d on %q (%d records, ok=%v)", w.pi, w.relSubj, len(recs), ok)
	}
	// Both PHYSICAL consumers, read back: a dependency record does not prove its consumer exists,
	// and a server-side disappearance issues no delete request an observer could see.
	for _, c := range []struct{ stream, durable string }{{mqttOutStreamName, w.pubRel}, {mqttStreamName, w.src}} {
		if ci, lerr := asm.jsa.lookupConsumer(c.stream, c.durable); lerr != nil || ci == nil {
			return fmt.Errorf("consumer %q on %s is gone: %v", c.durable, c.stream, lerr)
		}
	}
	for _, inc := range []string{w.srcInc, w.relInc} {
		r, _, rerr := asm.jsa.loadOutboundResource(w.cid, w.epoch, w.gen, inc)
		if rerr != nil || r == nil {
			return fmt.Errorf("resource %q does not load: %v", inc, rerr)
		}
		if !testResourceDependsOn(r, w.nonce) {
			return fmt.Errorf("resource %q no longer holds the obligation: %+v", inc, r.Deps)
		}
	}
	return nil
}

func testResourceDependsOn(r *mqttOutboundResource, nonce string) bool {
	for _, d := range r.Deps {
		if d.Nonce == nonce {
			return true
		}
	}
	return false
}

// noDurableDeleted drains the named brokers' send queues into the delete observer, then fails the
// test if either of the obligation's consumers was asked to be deleted through any route.
func (w *testPeerWorld) noDurableDeleted(t *testing.T, when string, brokers ...*mqttAccountSessionManager) {
	t.Helper()
	w.deletes.barrier(t, when, brokers...)
	if w.deletes.sawDelete(mqttStreamName, w.src) || w.deletes.sawDelete(mqttOutStreamName, w.pubRel) {
		t.Fatalf("%s: a consumer the obligation depends on was deleted", when)
	}
}

// discardJudge is the provenance rule for a discarded generation's consumers, judged at the
// moment each delete is requested: legitimate only once the drivers were released, with the name
// RETIRED and its resource's deletion authorized (or already completed by a racing executor).
func (w *testPeerWorld) discardJudge(released *atomic.Bool, asm *mqttAccountSessionManager) func(stream, durable string) error {
	return func(stream, durable string) error {
		var owned mqttOwnedStream
		switch {
		case stream == mqttStreamName && durable == w.src:
			owned = mqttOwnedStreamMsgs
		case stream == mqttOutStreamName && durable == w.pubRel:
			owned = mqttOwnedStreamOut
		default:
			return nil
		}
		if !released.Load() {
			return fmt.Errorf("deleted while every recovery driver was parked")
		}
		own, _, err := asm.jsa.loadNameOwnershipPhysical(w.epoch, owned, durable)
		if err != nil || own == nil || own.State != mqttNameOwnRetired {
			return fmt.Errorf("deleted under ownership %+v (%v)", own, err)
		}
		r, _, err := asm.jsa.loadOutboundResource(w.cid, w.epoch, w.gen, own.OwnerInc)
		if err != nil || r == nil || (r.State != mqttResourceDeleteAuthorized && r.State != mqttResourceCleanupComplete) {
			return fmt.Errorf("deleted with its resource at %+v (%v)", r, err)
		}
		return nil
	}
}

// A QoS 2 obligation whose PUBREC was taken on broker A COMPLETES on peer B after the client
// resumes there: B takes a new tenure over the SAME generation, every piece of completion
// evidence survives the takeover and A's eviction, B re-sends the PUBREL under the identifier the
// client holds, and the client's PUBCOMPs end the exchange.
//
// Between the PUBREL and the PUBCOMP the message is never re-sent [MQTT-4.3.3]. The source
// acknowledgement is CONFIRMED at the PUBREC (mqttProcessPublishReceived's PUBREC arm), so the
// original source holds no pending delivery JetStream could re-offer: the witness establishes
// that on A before the takeover and reads the SAME consumer back through B after the resume
// (consult 132). A fresh redelivery of this source driving a PUBLISH on B is therefore
// unreachable here; the unconfirmed-acknowledgement route is the sibling witness below.
//
// "A peer winning" here is the tenure takeover a persistent resume performs: production writes
// no closure for a generation that served and is resumed (opFinalizeHandoff closes only on a
// discarding retire), so nothing about the obligation is retired — which is exactly what the
// witness asserts.
//
// Mutations, each must fail this test:
//   - WP-1: A's eviction disposes of the PUBREL consumer (a delete in evictDisplacedOnRemotePersist).
//   - WP-2: B never subscribes the PUBREL consumer on resume (ensurePubRelConsumerSubscription skipped).
//   - WP-3: recovery reads a past-PUBREC register as a delivery owed (PUBLISH where PUBREL is due).
//   - WP-4: the resumed CONNECT finalizes its handoff with a discarding retire.
func TestF105PreservedObligationCompletesOnThePeer(t *testing.T) {
	// Not parallel: the consumer-delete request seam is process-wide.
	w := testPeerWorldAtPubRec(t, "f105-peer-preserved")

	// (1) The PUBREC's source confirmation completed on A before anything moves. Polled: the slot
	// is re-phased before the confirmation runs, so the PUBREL the fixture read does not order it.
	var ciA *ConsumerInfo
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		ci, err := w.sourceAckConfirmed(w.asmA)
		ciA = ci
		return err
	})

	cB, rB, asmB, _ := w.resumeOnPeer(t)
	defer cB.Close()

	// (4) B reads back the SAME source consumer, its acknowledgement still covering the delivery.
	ciB, err := w.sourceAckConfirmed(asmB)
	if err != nil {
		t.Fatalf("after the peer takeover, through broker B: %v", err)
	}
	if !ciB.Created.Equal(ciA.Created) {
		t.Fatalf("broker B reads source consumer %q created at %v, broker A read it created at %v: not the same consumer",
			w.src, ciB.Created, ciA.Created)
	}
	t.Logf("source %s/%d through B: NumAckPending %d, AckFloor.Stream %d, NumRedelivered %d (corroboration only)",
		w.src, w.srcSeq, ciB.NumAckPending, ciB.AckFloor.Stream, ciB.NumRedelivered)

	// (5) B re-sends the PUBREL under the identifier the client holds, then THE PUBCOMP IS
	// WITHHELD for three AckWaits, every packet B sends consumed meanwhile.
	testPeerFirstPubRel(t, w, rB)
	trace, rels := testPeerHoldingPubRel(t, w, rB, time.Now().Add(3*w.oB.MQTT.AckWait))
	t.Logf("withheld-PUBCOMP window: %v", trace)

	// (6) and (7): one PUBCOMP per PUBREL held, and every later PUBREL answered, until the
	// exchange is Terminal and its register and dependency holds are released.
	done := testPeerCompleteAnsweringPubRels(t, w, cB, rB, 1+rels, func() error { return w.completed(asmB) })
	t.Logf("completion: %v", done)
	w.noDurableDeleted(t, "after completion", w.asmA, asmB)
}

// A QoS 2 obligation whose PUBREC's source acknowledgement was NOT confirmed on broker A — the
// real acknowledgement never reached JetStream — still never has its message re-sent on peer B
// [MQTT-4.3.3]. The original source stays pending until B takes over, so B ENCOUNTERS it, and the
// encounter is a settlement, never a delivery; the PUBREL exchange completes on B.
//
// A PUBLISH of the original message here is F-115 across brokers.
func TestF105AnUnconfirmedSourceAckStillNeverResendsThePublishOnThePeer(t *testing.T) {
	// Not parallel: the delivery offer seam is process-wide.
	var (
		mu       sync.Mutex
		attempts []string
	)
	w := testPeerWorldBuild(t, "f105-peer-unconfirmed", 0, func(asmA *mqttAccountSessionManager) {
		asmA.mu.Lock()
		// Stands in for JetStream's confirmation AND for the acknowledgement itself: nothing is
		// sent, so the source genuinely stays pending.
		asmA.testInflightSourceAck = func(ackSubject string) error {
			mu.Lock()
			attempts = append(attempts, ackSubject)
			mu.Unlock()
			return fmt.Errorf("test: the source acknowledgement was prevented from landing")
		}
		asmA.mu.Unlock()
	})

	// The targeted confirmation was attempted for THIS source delivery, and failed.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		mu.Lock()
		defer mu.Unlock()
		for _, a := range attempts {
			if mqttSourceAckReplyBinds(a, mqttStreamName, w.src, w.srcSeq) {
				return nil
			}
		}
		return fmt.Errorf("no confirmation of source %s/%d was attempted on A: %v", w.src, w.srcSeq, attempts)
	})

	offers := testPeerWatchSourceOffers(t, w)

	// The original source is still PENDING on A, read immediately before the takeover.
	if ci, err := w.sourceAckConfirmed(w.asmA); err == nil {
		t.Fatalf("the prevented acknowledgement of %s/%d landed anyway before B took over: %+v", w.src, w.srcSeq, ci)
	} else if ci == nil {
		t.Fatalf("before the takeover: %v", err)
	} else {
		t.Logf("before the takeover, through A: %v", err)
	}

	cB, rB, asmB, _ := w.resumeOnPeer(t)
	defer cB.Close()

	testPeerFirstPubRel(t, w, rB)
	trace, rels := testPeerHoldingPubRel(t, w, rB, time.Now().Add(3*w.oB.MQTT.AckWait))
	t.Logf("withheld-PUBCOMP window: %v", trace)

	// B ENCOUNTERED the pending source: its re-offer reached B's delivery callback, and was settled
	// rather than delivered.
	n := offers()
	if n == 0 {
		ci, lerr := asmB.jsa.lookupConsumer(mqttStreamName, w.src)
		t.Fatalf("the pending source %s/%d never reached broker B's delivery callback (consumer %+v, %v); packets: %v",
			w.src, w.srcSeq, ci, lerr, trace)
	}
	t.Logf("encounter: %d re-offer(s) of %s/%d reached broker B's delivery callback", n, w.src, w.srcSeq)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		_, err := w.sourceAckConfirmed(asmB)
		return err
	})

	done := testPeerCompleteAnsweringPubRels(t, w, cB, rB, 1+rels, func() error { return w.completed(asmB) })
	t.Logf("completion: %v", done)
	w.noDurableDeleted(t, "after completion", w.asmA, asmB)
}

// sourceAckConfirmed reads the obligation's ORIGINAL source consumer back through asm and reports
// nil when it exists, holds no delivery pending acknowledgement, and its ack floor covers the
// source delivery. The consumer info is returned whenever the lookup succeeded.
func (w *testPeerWorld) sourceAckConfirmed(asm *mqttAccountSessionManager) (*ConsumerInfo, error) {
	ci, err := asm.jsa.lookupConsumer(mqttStreamName, w.src)
	if err != nil || ci == nil {
		return nil, fmt.Errorf("source consumer %q on %s does not read back: %v", w.src, mqttStreamName, err)
	}
	if ci.NumAckPending != 0 || ci.AckFloor.Stream < w.srcSeq {
		return ci, fmt.Errorf("source consumer %q still owes delivery %d: NumAckPending %d, AckFloor.Stream %d",
			w.src, w.srcSeq, ci.NumAckPending, ci.AckFloor.Stream)
	}
	return ci, nil
}

// resumeOnPeer reconnects the client to broker B with a persistent session and asserts the
// takeover: B serves the SAME generation under a NEW tenure with no closure, A lets go of its
// resident session, recovery on both brokers gets its chance, and every piece of completion
// evidence stands with nothing deleted and B's PUBREL route subscribed.
func (w *testPeerWorld) resumeOnPeer(t *testing.T) (net.Conn, *mqttReader, *mqttAccountSessionManager, *mqttSession) {
	t.Helper()
	cB, rB := testInflightReconnect(t, w.oB, w.cid)
	t.Cleanup(func() { cB.Close() })

	asmB, sessB := testResidentSession(t, w.sB, w.cid)
	sessB.mu.Lock()
	genB, entB := sessB.gen, sessB.boundEnt
	sessB.mu.Unlock()
	if genB != w.gen {
		t.Fatalf("the resume on the peer serves generation %q, want the preserved %q", genB, w.gen)
	}
	if entB.Tenure == _EMPTY_ || entB.Tenure == w.tenureA.Tenure {
		t.Fatalf("the peer did not take a new tenure: A %q, B %q", w.tenureA.Tenure, entB.Tenure)
	}
	if c, _, st := asmB.jsa.loadGenClosure(w.cid, w.epoch, w.gen); st != mqttOutboundAbsent {
		t.Fatalf("the resumed generation was closed (%v): %+v", st, c)
	}
	asmA := w.asmA
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		asmA.mu.RLock()
		_, resident := asmA.sessions[w.cid]
		asmA.mu.RUnlock()
		if resident {
			return fmt.Errorf("broker A still holds the displaced session resident")
		}
		return nil
	})
	asmA.sweepOutboundRecovery(w.sA, nil)
	asmB.sweepOutboundRecovery(w.sB, nil)

	if err := w.evidenceStands(asmB); err != nil {
		t.Fatalf("after the peer takeover: %v", err)
	}
	w.noDurableDeleted(t, "after the peer takeover", asmA, asmB)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		sessB.mu.Lock()
		subscribed := sessB.pubRelSubscribed
		sessB.mu.Unlock()
		if !subscribed {
			return fmt.Errorf("the resumed session on B never subscribed its PUBREL consumer")
		}
		return nil
	})
	return cB, rB, asmB, sessB
}

// completed reports nil once the exchange is Terminal, its slot register is gone, and neither
// resource holds the obligation as a dependency any more.
func (w *testPeerWorld) completed(asm *mqttAccountSessionManager) error {
	x, _, err := asm.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, w.nonce)
	if err != nil || x == nil || x.Phase != mqttExchangeTerminal {
		return fmt.Errorf("the exchange is not terminal: %+v (%v)", x, err)
	}
	if reg, _, _ := asm.jsa.loadInflightRecord(mqttInflightSubject(w.idHash, w.gen, w.pi)); reg != nil && reg.Lin == w.nonce {
		return fmt.Errorf("the completed obligation's register still stands: %+v", reg)
	}
	for _, inc := range []string{w.srcInc, w.relInc} {
		if r, _, _ := asm.jsa.loadOutboundResource(w.cid, w.epoch, w.gen, inc); r != nil && testResourceDependsOn(r, w.nonce) {
			return fmt.Errorf("resource %q still holds the completed obligation", inc)
		}
	}
	return nil
}

// testPeerWatchSourceOffers counts the offers of the obligation's ORIGINAL source delivery that
// reach broker B's QoS 1/2 delivery callback: its source durable at its stream sequence, on a
// subscription of a client served by B. The seam is restored at cleanup on the test goroutine.
func testPeerWatchSourceOffers(t *testing.T, w *testPeerWorld) (count func() int) {
	t.Helper()
	var n atomic.Int64
	restore := setMQTTTestDeliveryOfferedHook(func(sub *subscription, reply string) {
		if sub == nil || sub.client == nil || sub.client.srv != w.sB {
			return
		}
		if mqttSourceAckReplyBinds(reply, mqttStreamName, w.src, w.srcSeq) {
			n.Add(1)
		}
	})
	t.Cleanup(restore)
	return func() int { return int(n.Load()) }
}

// testPeerFirstPubRel requires B's first packet to the resumed client to be the PUBREL under the
// identifier the client holds — never the PUBLISH.
func testPeerFirstPubRel(t *testing.T, w *testPeerWorld, rB *mqttReader) {
	t.Helper()
	pt, pl, ok := testMQTTReadPacketReady(rB, 10*time.Second)
	if !ok {
		t.Fatalf("the peer never re-sent the owed PUBREL")
	}
	if pt&mqttPacketMask == mqttPacketPub {
		flags, pi, topic, payload := testMQTTGetPubMsgExEx(t, nil, rB, pt, pl, _EMPTY_, nil)
		t.Fatalf("the peer sent PUBLISH (flags 0x%x, identifier %d, topic %q, payload %q) where the PUBREL for identifier %d is owed",
			flags, pi, topic, payload, w.pi)
	}
	if pt&mqttPacketMask != mqttPacketPubRel {
		t.Fatalf("the peer sent packet 0x%x where the PUBREL for identifier %d is owed", pt&mqttPacketMask, w.pi)
	}
	if rpi, err := rB.readUint16("packet identifier"); err != nil || rpi != w.pi {
		t.Fatalf("the peer's PUBREL names identifier %d (%v), want the original %d", rpi, err, w.pi)
	}
}

// testPeerReadHeld reads one packet B sends the client while it holds the obligation's PUBREL. A
// PUBLISH of the original message (topic "foo", payload "payload", whatever its flags or
// identifier) fails the test [MQTT-4.3.3], as does any other PUBLISH; a PUBREL must name the
// original identifier; any other packet, and the connection ending, are unexpected. It reports whether a PUBREL was read and
// whether anything was read at all, appending to trace.
func testPeerReadHeld(t *testing.T, w *testPeerWorld, rB *mqttReader, wait time.Duration, start time.Time, trace *[]string) (pubRel, read bool) {
	t.Helper()
	t0 := time.Now()
	b, pl, ok := testMQTTReadPacketReady(rB, wait)
	if !ok {
		// A read that gives up well before its wait did not time out: the connection ended.
		if time.Since(t0) < wait/2 {
			t.Fatalf("the client connection on B ended while it held the PUBREL; packets: %v", *trace)
		}
		return false, false
	}
	at := time.Since(start).Round(time.Millisecond)
	switch b & mqttPacketMask {
	case mqttPacketPub:
		flags, pi, topic, payload := testMQTTGetPubMsgExEx(t, nil, rB, b, pl, _EMPTY_, nil)
		*trace = append(*trace, fmt.Sprintf("+%v PUBLISH flags=0x%x pi=%d topic=%q", at, flags, pi, topic))
		if topic == "foo" && string(payload) == "payload" {
			t.Fatalf("the original message was sent again (flags 0x%x, identifier %d) while the client "+
				"held the PUBREL for identifier %d: packets %v", flags, pi, w.pi, *trace)
		}
		t.Fatalf("an unexpected PUBLISH reached the client holding the PUBREL: %v", *trace)
	case mqttPacketPubRel:
		rpi, err := rB.readUint16("packet identifier")
		*trace = append(*trace, fmt.Sprintf("+%v PUBREL pi=%d", at, rpi))
		if err != nil || rpi != w.pi {
			t.Fatalf("a repeated PUBREL names identifier %d (%v), want the original %d: %v", rpi, err, w.pi, *trace)
		}
		return true, true
	default:
		t.Fatalf("unexpected packet 0x%x while the client held the PUBREL: %v", b&mqttPacketMask, *trace)
	}
	return false, true
}

// testPeerHoldingPubRel consumes every packet B sends the client while it holds the obligation's
// PUBREL and withholds its PUBCOMP, until deadline. It returns the trace and how many (repeated)
// PUBRELs were read.
func testPeerHoldingPubRel(t *testing.T, w *testPeerWorld, rB *mqttReader, deadline time.Time) ([]string, int) {
	t.Helper()
	start := time.Now()
	var trace []string
	rels := 0
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return trace, rels
		}
		if rel, _ := testPeerReadHeld(t, w, rB, left, start, &trace); rel {
			rels++
		}
	}
}

// testPeerCompleteAnsweringPubRels sends one PUBCOMP for each of the owed PUBRELs the client has
// read, then keeps consuming: every further PUBREL is answered with its own PUBCOMP (MQTT 3.1.1
// §4.3.3), a PUBLISH still fails the test, and only completed reporting nil ends the wait — no
// quiet interval, and no PINGRESP, is taken as proof that nothing follows.
func testPeerCompleteAnsweringPubRels(t *testing.T, w *testPeerWorld, cB net.Conn, rB *mqttReader, owed int,
	completed func() error) []string {
	t.Helper()
	trace := make([]string, 0, owed)
	start := time.Now()
	for i := 0; i < owed; i++ {
		testMQTTSendPIPacket(mqttPacketPubComp, t, cB, w.pi)
		trace = append(trace, fmt.Sprintf("+%v sent PUBCOMP pi=%d", time.Since(start).Round(time.Millisecond), w.pi))
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := completed()
		if err == nil {
			return trace
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %d PUBCOMP(s): %v; packets: %v", owed, err, trace)
		}
		if rel, _ := testPeerReadHeld(t, w, rB, 50*time.Millisecond, start, &trace); rel {
			testMQTTSendPIPacket(mqttPacketPubComp, t, cB, w.pi)
			trace = append(trace, fmt.Sprintf("+%v sent PUBCOMP pi=%d", time.Since(start).Round(time.Millisecond), w.pi))
		}
	}
}

// testPeerLandedTransition is one authority transition a broker's seam saw commit.
type testPeerLandedTransition struct {
	broker  string
	kind    mqttTransitionKind
	txn     string
	seq     uint64
	closure *mqttGenClosure // the closure the batch wrote, if any
	// For an exchange-moving batch: what the generation's closure was, as the store held it, at
	// the moment the batch was sent.
	closureAtSend   *mqttGenClosure
	closureSeqAtSnd uint64
	// phase is the exchange phase the batch WROTE for the obligation (an exchange-moving batch).
	phase mqttExchangePhase
}

// testPeerRecordTransitions arms both brokers' transition seams to record every committed
// transition that writes the generation's closure or moves the obligation's exchange.
func (w *testPeerWorld) recordTransitions(t *testing.T, asmA, asmB *mqttAccountSessionManager) func() []testPeerLandedTransition {
	t.Helper()
	var mu sync.Mutex
	var landed []testPeerLandedTransition
	xsubj := mqttExchangeSubject(w.idHash, w.gen, w.nonce)
	arm := func(broker string, jsa *mqttJSA) {
		testArmTransitionSeam(t, jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			movesExchange := false
			var written mqttExchangePhase
			for _, m := range tr.Extra {
				if m.subj != xsubj {
					continue
				}
				movesExchange = true
				var x mqttOutboundExchange
				if json.Unmarshal(m.body, &x) == nil {
					written = x.Phase
				}
			}
			closes := tr.Closure != nil && tr.Closure.Gen == w.gen
			if !movesExchange && !closes {
				return send()
			}
			rec := testPeerLandedTransition{broker: broker, kind: tr.Attempt.Kind, txn: tr.Attempt.Txn, closure: tr.Closure, phase: written}
			if movesExchange {
				c, cseq, st := jsa.loadGenClosure(w.cid, w.epoch, w.gen)
				if st == mqttOutboundValid {
					rec.closureAtSend, rec.closureSeqAtSnd = c, cseq
				}
			}
			seq, err := send()
			if err == nil {
				rec.seq = seq
				mu.Lock()
				landed = append(landed, rec)
				mu.Unlock()
			}
			return seq, err
		})
	}
	arm("A", &asmA.jsa)
	arm("B", &asmB.jsa)
	return func() []testPeerLandedTransition {
		mu.Lock()
		defer mu.Unlock()
		return append([]testPeerLandedTransition(nil), landed...)
	}
}

// A QoS 2 obligation whose PUBREC was taken on broker A is ended only UNDER THE DISCARDING CLOSURE
// when the client's next CONNECT is a clean start on peer B.
//
// B's finalize closes A's generation with Discard; until a recovery executor acts, the obligation
// and every piece of its evidence stand. The obligation is then moved to Discarded by EXACTLY ONE
// committed transition, sent only once that closure stood, and its consumers are deleted only
// after their deletion was authorized. B never resumes the old exchange.
//
// Mutations, each must fail this test:
//   - WD-1: the boundary and the executor stop requiring the discarding closure (the ending can
//     land before the closure).
//   - WD-2: the executor ends the obligation with a non-discard ending.
//   - WD-3: the clean-start teardown raw-deletes the PUBREL consumer (transferLogical forced false).
//   - WD-4: B resumes the old generation's exchange on the clean start.
func TestF105DiscardedObligationEndsUnderTheDiscardingClosure(t *testing.T) {
	// Not parallel: parking the recovery drivers is process-wide.
	release := testDiscardParkAll(t)
	w := testPeerWorldAtPubRec(t, "f105-peer-discarded")
	asmA, asmB := w.asmA, w.asmB

	var released atomic.Bool
	w.deletes.setJudge(w.discardJudge(&released, asmB))

	// THE LICENCE. Before any clean start the generation is not closed, so the executor's own
	// step — the discard of this obligation — is refused by the boundary for want of a discarding
	// closure, and the obligation is untouched.
	if _, err := asmA.jsa.opDiscardExchange(w.cid, w.epoch, mqttNewInflightNonce(), w.gen, w.nonce); err == nil ||
		!strings.Contains(err.Error(), "only a committed discarding closure licenses its retirement") {
		t.Fatalf("discarding the obligation of an unclosed generation was not refused for want of the closure: %v", err)
	}
	if err := w.evidenceStands(asmA); err != nil {
		t.Fatalf("after the refused pre-closure discard: %v", err)
	}

	landed := w.recordTransitions(t, asmA, asmB)

	cB, rB := testMQTTConnect(t, &mqttConnInfo{clientID: w.cid, cleanSess: true}, w.oB.MQTT.Host, w.oB.MQTT.Port)
	defer cB.Close()
	testMQTTCheckConnAck(t, rB, mqttConnAckRCConnectionAccepted, false)

	// B's finalize closed the old generation with Discard, and B serves a new generation.
	_, sessB := testResidentSession(t, w.sB, w.cid)
	sessB.mu.Lock()
	genB, installedDeliver := sessB.gen, sessB.pubRelDeliverySubject
	sessB.mu.Unlock()
	if genB == w.gen {
		t.Fatalf("the clean start on the peer kept generation %q", w.gen)
	}
	// GENERATION ISOLATION: the PUBREL route the clean-started session installed is its OWN
	// generation's, so G's stored PUBRELs are never delivered to it (WD-4).
	_, deliverB, _ := mqttPubRelNames(w.idHash, genB)
	_, deliverG, _ := mqttPubRelNames(w.idHash, w.gen)
	if installedDeliver != deliverB || installedDeliver == deliverG {
		t.Fatalf("the clean-started session installed PUBREL delivery subject %q, want generation %q's %q "+
			"(the discarded generation's is %q)", installedDeliver, genB, deliverB, deliverG)
	}
	closure, closureSeq, st := asmB.jsa.loadGenClosure(w.cid, w.epoch, w.gen)
	if st != mqttOutboundValid || closure == nil || !closure.Discard {
		t.Fatalf("the clean start did not close generation %q with Discard: %+v (%v)", w.gen, closure, st)
	}
	var closer *testPeerLandedTransition
	for _, l := range landed() {
		if l.closure != nil {
			l := l
			closer = &l
		}
	}
	// The seam reports the sequence of the batch's LAST member; the closure is one of its members.
	if closer == nil || closer.broker != "B" || closer.txn != closure.Txn || closureSeq > closer.seq {
		t.Fatalf("the discarding closure was not written by B's finalize: closure txn %q seq %d, landed %+v",
			closure.Txn, closureSeq, closer)
	}

	// With every recovery driver parked, nothing has ended the obligation: its evidence stands.
	if err := w.obligationStands(asmB); err != nil {
		t.Fatalf("before any executor acted: %v", err)
	}
	w.noDurableDeleted(t, "before any executor acted", asmA, asmB)

	// B does not resume the old exchange: no PUBREL for its identifier reaches the new session.
	if pt, _, ok := testMQTTReadPacketReady(rB, 3*time.Second); ok {
		t.Fatalf("the clean-started session received packet 0x%x from the discarded generation", pt&mqttPacketMask)
	}

	// Release the drivers and let recovery retire the discarded generation. WD-2's mutant is armed
	// only from here (testWD2Armed), so the pre-closure refusal above ran unmodified.
	released.Store(true)
	testWD2Armed.Store(true)
	t.Cleanup(func() { testWD2Armed.Store(false) })
	release()
	requireOneDiscardEnding(t, landed, closure.Txn, func() { asmB.sweepOutboundRecovery(w.sB, nil) })
	f := &testDiscardFix{s: w.sB, asm: asmB, id: w.cid, gen: w.gen, nonce: w.nonce, src: w.src, pubRel: w.pubRel}
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		asmB.sweepOutboundRecovery(w.sB, nil)
		for _, e := range []error{
			f.obligationDiscarded(t),
			f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, w.src),
			f.retired(t, mqttOwnedStreamOut, mqttOutStreamName, w.pubRel),
			f.certified(t),
		} {
			if e != nil {
				return e
			}
		}
		return nil
	})

	// EXACTLY ONE committed transition moved the obligation, into Discarded, licensed by the
	// closure that already stood when it was sent.
	var moves []testPeerLandedTransition
	for _, l := range landed() {
		if l.closure == nil {
			moves = append(moves, l)
		}
	}
	if len(moves) != 1 {
		t.Fatalf("%d committed transitions moved the obligation, want exactly one: %+v", len(moves), moves)
	}
	m := moves[0]
	if m.kind != mqttTransitionDiscardExchange || m.phase != mqttExchangeDiscarded {
		t.Fatalf("the obligation was ended by a %v transition writing phase %d, want the discard", m.kind, m.phase)
	}
	if m.closureAtSend == nil || !m.closureAtSend.Discard || m.closureAtSend.Txn != closure.Txn || m.closureSeqAtSnd >= m.seq {
		t.Fatalf("the discard was sent without the discarding closure standing below it: closure %+v seq %d, discard seq %d",
			m.closureAtSend, m.closureSeqAtSnd, m.seq)
	}
	w.deletes.barrier(t, "after the discard completed", asmA, asmB)
}

// requireOneDiscardEnding waits for the first committed move of the obligation and requires it to
// be the discard, WRITING Discarded, sent with the discarding closure (txn) standing below it -
// asserted before any cleanup wait, so a wrong ending fails HERE and not at a cleanup timeout.
//
// drive is the recovery pass the witness runs on the executing broker each poll, exactly as its
// later cleanup wait does: with the prompt and periodic drivers parked until release, nothing else
// is guaranteed to reach the obligation within the wait.
func requireOneDiscardEnding(t *testing.T, landed func() []testPeerLandedTransition, closureTxn string, drive func()) {
	t.Helper()
	var moves []testPeerLandedTransition
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		drive()
		moves = moves[:0]
		for _, l := range landed() {
			if l.closure == nil {
				moves = append(moves, l)
			}
		}
		if len(moves) == 0 {
			return fmt.Errorf("no committed transition has moved the obligation yet")
		}
		return nil
	})
	m := moves[0]
	if m.kind != mqttTransitionDiscardExchange || m.phase != mqttExchangeDiscarded {
		t.Fatalf("the obligation was ended by a %v transition writing phase %d, want the discard writing Discarded", m.kind, m.phase)
	}
	if m.closureAtSend == nil || !m.closureAtSend.Discard || m.closureAtSend.Txn != closureTxn || m.closureSeqAtSnd >= m.seq {
		t.Fatalf("the discard was sent without the discarding closure standing below it: %+v", m)
	}
}

// testWD2Armed is read only by mutation WD-2's edits, which activate the non-discard ending and the
// kind/phase bypass together, after the pre-closure refusal has run unmodified.
var testWD2Armed atomic.Bool

// testPeerExpiryScheduledOn waits until broker asm holds a scheduled expiry for the client: it has
// observed the persisted finite expiry of the disconnected session.
func testPeerExpiryScheduledOn(t *testing.T, asm *mqttAccountSessionManager, idHash string) {
	t.Helper()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		asm.mu.Lock()
		_, ok := asm.pendingExpiries[idHash]
		asm.mu.Unlock()
		if !ok {
			return fmt.Errorf("broker B holds no scheduled expiry for %q", idHash)
		}
		return nil
	})
}

// testPeerSessionRecordGone waits for the session record's removal from $MQTT_sess.
func testPeerSessionRecordGone(t *testing.T, asm *mqttAccountSessionManager, idHash string) {
	t.Helper()
	checkFor(t, 15*time.Second, 50*time.Millisecond, func() error {
		if _, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessStreamSubjectPrefix+asm.domainTk+idHash); err == nil {
			return fmt.Errorf("the session record of %q still stands", idHash)
		}
		return nil
	})
}

// A QoS 2 obligation whose PUBREC was taken on broker A is ended only UNDER THE DISCARDING CLOSURE
// that PEER B's session expiry writes (consult 129). A is stopped once B holds the persisted
// expiry, so B must win the close; the obligation and its evidence stand until a recovery
// executor acts; the client reconnecting with Clean Start = false is told Session Present = false
// and never sees the old PUBREL; exactly one committed DiscardExchange ends the obligation.
//
// Mutations (outcomes recorded in the crosswalk): WDX-1 (B's expiry skips its discard), WDX-2
// (the expiry teardown raw-deletes the PUBREL consumer), WD-1 and WD-2 run against this arm too.
func TestF105ExpiredObligationEndsUnderThePeersDiscardingClosure(t *testing.T) {
	// Not parallel: parking the recovery drivers is process-wide. Both halves are installed before
	// either manager starts; the per-session expiry timer is not one of them.
	release := testDiscardParkAll(t)
	w := testPeerWorldAt(t, "f105-peer-expired", 4)
	asmA, asmB := w.asmA, w.asmB
	var released atomic.Bool
	w.deletes.setJudge(w.discardJudge(&released, asmB))

	if _, err := asmA.jsa.opDiscardExchange(w.cid, w.epoch, mqttNewInflightNonce(), w.gen, w.nonce); err == nil ||
		!strings.Contains(err.Error(), "only a committed discarding closure licenses its retirement") {
		t.Fatalf("discarding the obligation of an unexpired generation was not refused for want of the closure: %v", err)
	}
	landed := w.recordTransitions(t, asmA, asmB)

	// B holds the persisted finite expiry; A's observation window closes and A's frontend stops,
	// so B is the only broker that can expire the session.
	testPeerExpiryScheduledOn(t, asmB, w.idHash)
	w.noDurableDeleted(t, "before stopping A", asmA, asmB)
	w.sA.Shutdown()

	// B's expiry writes the discarding closure while every recovery driver stays parked.
	var closure *mqttGenClosure
	var closureSeq uint64
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		c, seq, st := asmB.jsa.loadGenClosure(w.cid, w.epoch, w.gen)
		if st != mqttOutboundValid || c == nil || !c.Discard {
			return fmt.Errorf("no discarding closure for generation %q yet: %+v (%v)", w.gen, c, st)
		}
		closure, closureSeq = c, seq
		return nil
	})
	var closer *testPeerLandedTransition
	for _, l := range landed() {
		if l.closure != nil {
			l := l
			closer = &l
		}
	}
	if closer == nil || closer.broker != "B" || closer.txn != closure.Txn || closureSeq > closer.seq {
		t.Fatalf("the discarding closure was not written by B's expiry: closure txn %q seq %d, landed %+v",
			closure.Txn, closureSeq, closer)
	}
	if err := w.obligationStands(asmB); err != nil {
		t.Fatalf("after B's expiry, before any executor acted: %v", err)
	}
	w.noDurableDeleted(t, "after B's expiry", asmB)
	testPeerSessionRecordGone(t, asmB, w.idHash)

	// The client comes back to B asking to resume: there is nothing to resume.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: w.cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(4)}, w.oB.MQTT.Host, w.oB.MQTT.Port)
	defer c2.Close()
	present, rc, _ := testMQTTReadConnAckV5(t, r2)
	if rc != mqttConnAckRCConnectionAccepted || present {
		t.Fatalf("the reconnect after expiry: rc 0x%x, Session Present %v; want accepted, not present", rc, present)
	}
	_, sess2 := testResidentSession(t, w.sB, w.cid)
	sess2.mu.Lock()
	gen2 := sess2.gen
	sess2.mu.Unlock()
	if gen2 == w.gen {
		t.Fatalf("the reconnect after expiry serves the expired generation %q", w.gen)
	}
	if pt, _, ok := testMQTTReadPacketReady(r2, 3*time.Second); ok {
		t.Fatalf("the reconnected session received packet 0x%x from the expired generation", pt&mqttPacketMask)
	}

	released.Store(true)
	testWD2Armed.Store(true)
	t.Cleanup(func() { testWD2Armed.Store(false) })
	release()
	requireOneDiscardEnding(t, landed, closure.Txn, func() { asmB.sweepOutboundRecovery(w.sB, nil) })
	f := &testDiscardFix{s: w.sB, asm: asmB, id: w.cid, gen: w.gen, nonce: w.nonce, src: w.src, pubRel: w.pubRel}
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		asmB.sweepOutboundRecovery(w.sB, nil)
		for _, e := range []error{
			f.obligationDiscarded(t),
			f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, w.src),
			f.retired(t, mqttOwnedStreamOut, mqttOutStreamName, w.pubRel),
			f.certified(t),
		} {
			if e != nil {
				return e
			}
		}
		return nil
	})
	if pt, _, ok := testMQTTReadPacketReady(r2, 3*time.Second); ok {
		t.Fatalf("after recovery, the reconnected session received packet 0x%x from the expired generation", pt&mqttPacketMask)
	}
	var moves []testPeerLandedTransition
	for _, l := range landed() {
		if l.closure == nil {
			moves = append(moves, l)
		}
	}
	if len(moves) != 1 || moves[0].kind != mqttTransitionDiscardExchange || moves[0].phase != mqttExchangeDiscarded {
		t.Fatalf("want exactly one committed DiscardExchange ending the obligation, got %+v", moves)
	}
	if m := moves[0]; m.closureAtSend == nil || !m.closureAtSend.Discard || m.closureAtSend.Txn != closure.Txn || m.closureSeqAtSnd >= m.seq {
		t.Fatalf("the discard was sent without B's expiry closure standing below it: %+v", m)
	}
	w.deletes.barrier(t, "after the discard completed", asmB)
}

// An OVERDUE session record is never resumed on a peer (WDX-1r's own fixture, consult 129): B's
// expiry is parked BEFORE its teardown, the deadline passes with the record still standing, and
// the client's Clean Start = false CONNECT to B must be answered Session Present = false with a
// new generation and no PUBREL from the expired one. The CONNECT, not the expiry pass, is what
// refuses the resume here.
func TestF105AnOverdueSessionIsNotResumedOnThePeer(t *testing.T) {
	const cid = "f105-peer-overdue"
	testDiscardParkAll(t)
	park := func(idHash string) bool { return idHash != getHash(cid) }
	prev := mqttTestExpiryFireHookV.Swap(&park)
	t.Cleanup(func() { mqttTestExpiryFireHookV.Store(prev) })

	w := testPeerWorldAt(t, cid, 2)
	asmA, asmB := w.asmA, w.asmB
	testPeerExpiryScheduledOn(t, asmB, w.idHash)
	w.noDurableDeleted(t, "before stopping A", asmA, asmB)
	w.sA.Shutdown()

	// The deadline passes; the parked expiry has torn nothing down, so the record stands OVERDUE.
	time.Sleep(3 * time.Second)
	if _, err := asmB.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessStreamSubjectPrefix+asmB.domainTk+w.idHash); err != nil {
		t.Fatalf("fixture: the overdue session record is gone before the CONNECT: %v", err)
	}

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: w.cid, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(2)}, w.oB.MQTT.Host, w.oB.MQTT.Port)
	defer c2.Close()
	present, rc, _ := testMQTTReadConnAckV5(t, r2)
	if rc != mqttConnAckRCConnectionAccepted || present {
		t.Fatalf("the CONNECT over an overdue record: rc 0x%x, Session Present %v; want accepted, not present", rc, present)
	}
	_, sess2 := testResidentSession(t, w.sB, w.cid)
	sess2.mu.Lock()
	gen2 := sess2.gen
	sess2.mu.Unlock()
	if gen2 == w.gen {
		t.Fatalf("the CONNECT resumed the overdue generation %q", w.gen)
	}
	if pt, _, ok := testMQTTReadPacketReady(r2, 3*time.Second); ok {
		t.Fatalf("the session that replaced an overdue one received packet 0x%x from it", pt&mqttPacketMask)
	}
}

// The delete observer's own negative control: a delete enqueued through a broker's send queue
// WITHOUT passing jsa.deleteConsumer, with BOTH its publication and the observer's callback
// delayed, is still caught by the per-queue marker barrier and the target comparison.
func TestF105TheDeleteObserverCatchesADeleteThatBypassedTheSeam(t *testing.T) {
	// Not parallel: the publication seam is process-wide.
	const bypass = testConsumerDeletePrefix + mqttOutStreamName + ".f105_bypass_durable"
	delayPublish := func(subject string) {
		if subject == bypass {
			time.Sleep(300 * time.Millisecond)
		}
	}
	prevPub := mqttTestJSSendPublishHookV.Swap(&delayPublish)
	t.Cleanup(func() { mqttTestJSSendPublishHookV.Store(prevPub) })
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	probe, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "f105-observer-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, "f105-observer-probe")
	probe.Close()

	w := newTestConsumerDeleteWatch(t, s)
	if err := w.barrierErr("control", asm); err != nil {
		t.Fatalf("a quiet observer reported a violation: %v", err)
	}
	w.mu.Lock()
	w.busDelay = 300 * time.Millisecond
	w.mu.Unlock()
	asm.jsa.sendMsg(bypass, nil)
	if err := w.barrierErr("bypass", asm); err == nil || !strings.Contains(err.Error(), "bypassed") {
		t.Fatalf("a delete that never passed jsa.deleteConsumer was not caught: %v", err)
	}
}
