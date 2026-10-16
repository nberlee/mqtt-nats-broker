package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// F-059: the retained index is ordered by $MQTT_rmsgs stream sequence across all of its
// writers, and a lost observer application is detected and repaired by resuming the
// observer from W+1. See retained_index.go. Every test here names the mutation it exists
// to kill.

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// retainedBrokerWith starts a broker with seams installed BEFORE Start: the observer
// begins replaying the stream during Start.
func retainedBrokerWith(t *testing.T, url, node string, setup func(sm *mqttSessionManager)) (*Server, string, *logCapture) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(nc.Close)
	s := NewServer(nc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, node)
	lc := &logCapture{}
	s.log.Store(slog.New(lc))
	if setup != nil {
		setup(&s.mqtt.sessmgr)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("start broker %s: %v", node, err)
	}
	t.Cleanup(s.Shutdown)
	return s, fmt.Sprintf("tcp://%s", s.Addr().String()), lc
}

// parkGate parks every call for one subject until the test hands it a release token, and
// announces each arrival. done frees everything at cleanup so shutdown never blocks
// behind a parked goroutine.
type parkGate struct {
	subject string
	entered chan bool
	release chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newParkGate(t *testing.T, subject string) *parkGate {
	g := &parkGate{subject: subject, entered: make(chan bool, 16), release: make(chan struct{}, 16), done: make(chan struct{})}
	t.Cleanup(g.free)
	return g
}

func (g *parkGate) free() { g.once.Do(func() { close(g.done) }) }

func (g *parkGate) park(subject string, tombstone bool) {
	if subject != g.subject {
		return
	}
	g.entered <- tombstone
	select {
	case <-g.release:
	case <-g.done:
	}
}

func (g *parkGate) requireEntered(t *testing.T, wantTombstone bool) {
	t.Helper()
	select {
	case got := <-g.entered:
		if got != wantTombstone {
			t.Fatalf("gate for %q entered with tombstone=%v, want %v", g.subject, got, wantTombstone)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for the gate on %q (tombstone=%v)", g.subject, wantTombstone)
	}
}

func (g *parkGate) releaseOne() { g.release <- struct{}{} }

// obsSnapshot is the observer state, read under the index lock.
type obsSnapshot struct {
	resetting   bool
	lastDseq    uint64
	applied     uint64
	resets      uint64
	resetReason string
	resetStart  uint64
	sub         *subscription
}

func observerState(asm *mqttAccountSessionManager) obsSnapshot {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	o := &asm.rmObs
	return obsSnapshot{o.resetting, o.lastDseq, o.applied, o.resets, o.resetReason, o.resetStart, o.sub}
}

func indexEntry(asm *mqttAccountSessionManager, subject string) (seq uint64, deleted, ok bool) {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	if asm.retmsgs == nil {
		return 0, false, false
	}
	ref, found := asm.retmsgs.Find([]byte(subject))
	if !found {
		return 0, false, false
	}
	return ref.sseq, ref.deleted, true
}

func waitApplied(t *testing.T, asm *mqttAccountSessionManager, seq uint64) {
	t.Helper()
	retainedCheckFor(t, 10*time.Second, 5*time.Millisecond, func() error {
		if got := observerState(asm).applied; got < seq {
			return fmt.Errorf("observer W is %d, want >= %d", got, seq)
		}
		return nil
	})
}

// streamSeq is the sequence of the subject's current record in $MQTT_rmsgs.
func streamSeq(t *testing.T, url, streamSubject string) uint64 {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := js.Stream(ctx, mqttRetainedMsgsStreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	m, err := st.GetLastMsgForSubject(ctx, streamSubject)
	if err != nil {
		t.Fatalf("last for %q: %v", streamSubject, err)
	}
	return m.Sequence
}

func forceObserverReset(asm *mqttAccountSessionManager) {
	asm.mu.Lock()
	asm.rmObs.markResettingLocked("test")
	asm.mu.Unlock()
}

const rsp = mqttRetainedMsgsStreamSubject

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

// T1 (kills M2: no watermark test for non-observer writers). A seed delayed past the
// observer's commit of a LATER tombstone must not re-index the value it seeds.
func TestRetainedDelayedOlderSeedCannotRestoreOverTombstone(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	var parked atomic.Bool
	seedEntered, seedRelease := make(chan struct{}, 1), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-seedRelease:
		default:
			close(seedRelease)
		}
	})
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "t1", func(sm *mqttSessionManager) {
		sm.testRetainedSeedGate = func(subject string) {
			if subject == "t1.topic" && parked.CompareAndSwap(false, true) {
				seedEntered <- struct{}{}
				<-seedRelease
			}
		}
	})
	asm := retainedASM(t, s)

	pub := connectClient(t, addr, "t1-pub")
	tok := pub.Publish("t1/topic", 1, true, "v1")
	<-seedEntered // v1 is stored; its seed is parked

	del := connectClient(t, addr, "t1-del")
	mustPublish(t, del, "t1/topic", 1, true, "")
	tSeq := streamSeq(t, srv.ClientURL(), rsp+"t1.topic")
	waitApplied(t, asm, tSeq)
	if _, _, ok := indexEntry(asm, "t1.topic"); ok {
		t.Fatal("fixture: the observer committed the tombstone, the index must have no entry")
	}

	close(seedRelease)
	if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		t.Fatalf("v1 publish did not complete: %v", tok.Error())
	}
	// Asserted BEFORE any SUBSCRIBE: a load could otherwise repair the index.
	if asm.retainedIndexedLive("t1.topic") {
		t.Fatal("a seed of v1 delayed past the committed tombstone re-indexed the deleted subject")
	}
	expectNoMsg(t, recvChan(t, connectClient(t, addr, "t1-wild"), "t1/#", 1), 500*time.Millisecond)
	expectNoMsg(t, recvChan(t, connectClient(t, addr, "t1-exact"), "t1/topic", 1), 500*time.Millisecond)
}

// T2 (kills M1: no per-entry sequence comparison). A delayed OLDER observer application
// must not overwrite a newer seeded entry.
func TestRetainedDelayedObserverCannotRestoreOlderValue(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	gate := newParkGate(t, "t2.topic")
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "t2", func(sm *mqttSessionManager) {
		sm.testRetainedApplyGate = gate.park
	})
	asm := retainedASM(t, s)

	pub := connectClient(t, addr, "t2-pub")
	mustPublish(t, pub, "t2/topic", 1, true, "v1")
	gate.requireEntered(t, false) // obs(v1) parked
	mustPublish(t, pub, "t2/topic", 1, true, "v2")
	s2 := streamSeq(t, srv.ClientURL(), rsp+"t2.topic")
	if seq, _, _ := indexEntry(asm, "t2.topic"); seq != s2 {
		t.Fatalf("fixture: the v2 seed must have indexed %d, index holds %d", s2, seq)
	}

	gate.releaseOne()
	// obs(v2) entering the gate proves obs(v1) returned (one subscription, one goroutine).
	gate.requireEntered(t, false)
	if seq, deleted, ok := indexEntry(asm, "t2.topic"); !ok || deleted || seq != s2 {
		t.Fatalf("delayed obs(v1) walked the index back: entry seq=%d deleted=%v ok=%v, want %d", seq, deleted, ok, s2)
	}
	gate.releaseOne()
	expectMsg(t, recvChan(t, connectClient(t, addr, "t2-wild"), "t2/#", 1), "v2")
}

// T3 (kills M3: a non-observer deletion deletes outright). A seed tombstone seen ahead
// of the observer leaves a placeholder, so a parked load of the older record cannot
// re-index it.
func TestRetainedDelayedSeedTombstoneHoldsPlaceholder(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	gate := newParkGate(t, "t3.topic")
	var loadParked atomic.Bool
	loadEntered, loadRelease := make(chan struct{}, 1), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-loadRelease:
		default:
			close(loadRelease)
		}
	})
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "t3", func(sm *mqttSessionManager) {
		sm.testRetainedApplyGate = gate.park
		sm.testRetainedLoadGate = func(subjects []string) {
			for _, ss := range subjects {
				if ss == rsp+"t3.topic" && loadParked.CompareAndSwap(false, true) {
					loadEntered <- struct{}{}
					<-loadRelease
				}
			}
		}
	})
	asm := retainedASM(t, s)

	pub := connectClient(t, addr, "t3-pub")
	mustPublish(t, pub, "t3/topic", 1, true, "v1")
	gate.requireEntered(t, false) // the observer is held at v1, so W < s1
	if !asm.retainedIndexedLive("t3.topic") {
		t.Fatal("fixture: the v1 seed must have indexed the subject")
	}

	// A cold authoritative load (the one a literal SUBSCRIBE issues) reads v1 and parks
	// before applying it. Driven from the test goroutine rather than through a SUBSCRIBE:
	// a parked SUBSCRIBE holds the subscribe barrier every later PUBLISH waits on.
	loadDone := make(chan map[string]*mqttRetainedMsg, 1)
	go func() { loadDone <- asm.loadRetainedMessages(map[string]uint64{"t3.topic": 0}, asm.jsa.c) }()
	<-loadEntered

	mustPublish(t, pub, "t3/topic", 1, true, "")
	if seq, deleted, ok := indexEntry(asm, "t3.topic"); !ok || !deleted {
		t.Fatalf("fixture: the tombstone seed must leave a placeholder, got seq=%d deleted=%v ok=%v", seq, deleted, ok)
	}

	close(loadRelease)
	if rms := <-loadDone; rms["t3.topic"] == nil {
		t.Fatal("fixture: the parked load must have read v1")
	}
	if asm.retainedIndexedLive("t3.topic") {
		t.Fatal("a load of v1 parked across the seeded tombstone re-indexed the deleted subject")
	}
	gate.free()
}

// T4 (kills M4: a decode-failure deletion bypasses arbitration). A delayed undecodable
// record must not evict a newer seeded value.
func TestRetainedDelayedCorruptRecordCannotEvictNewer(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	gate := newParkGate(t, "t4.topic")
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "t4", func(sm *mqttSessionManager) {
		sm.testRetainedApplyGate = gate.park
	})
	asm := retainedASM(t, s)

	injectRetainedRecord(t, srv.ClientURL(), rsp+"t4.topic", legacyJSONRetainedRecord(t, "t4/topic", "legacy"))
	gate.requireEntered(t, true) // the corrupt record's deletion is parked
	pub := connectClient(t, addr, "t4-pub")
	mustPublish(t, pub, "t4/topic", 1, true, "v2")
	s2 := streamSeq(t, srv.ClientURL(), rsp+"t4.topic")

	gate.releaseOne()
	gate.requireEntered(t, false) // obs(v2) entering proves the corrupt commit returned
	if seq, deleted, ok := indexEntry(asm, "t4.topic"); !ok || deleted || seq != s2 {
		t.Fatalf("a delayed corrupt record evicted the newer entry: seq=%d deleted=%v ok=%v, want %d", seq, deleted, ok, s2)
	}
	gate.releaseOne()
	expectMsg(t, recvChan(t, connectClient(t, addr, "t4-wild"), "t4/#", 1), "v2")
}

// T5 (kills M5: the loader does not index). With the observer's delivery lost and no
// reset allowed, an exact SUBSCRIBE's authoritative load is the only possible repairer,
// and a later wildcard SUBSCRIBE must see what it repaired.
func TestRetainedExactFilterLoadIndexesAbsentSubject(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	dropped := make(chan struct{}, 1)
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "t5", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHold = func() bool { return true }
		sm.testRetainedDeliveryDrop = func(subject string) bool {
			if subject == rsp+"t5.topic" {
				dropped <- struct{}{}
				return true
			}
			return false
		}
	})
	asm := retainedASM(t, s)
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"t5.topic", "t5/topic", "A")
	<-dropped
	if asm.retainedIndexedLive("t5.topic") {
		t.Fatal("fixture: the delivery was dropped, the subject must be absent")
	}
	expectMsg(t, recvChan(t, connectClient(t, addr, "t5-exact"), "t5/topic", 1), "A")
	if !asm.retainedIndexedLive("t5.topic") {
		t.Fatal("the exact filter's authoritative load did not index the subject it read")
	}
	expectMsg(t, recvChan(t, connectClient(t, addr, "t5-wild"), "t5/#", 1), "A")
}

// T6 (kills M6: a forced load's no-message answer removes unconditionally). The answer
// for a subject the load never saw indexed cannot remove a record seeded after the read.
func TestRetainedNoMessageForcedLoadKeepsNewerEntry(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	gate := newParkGate(t, "t6.topic") // holds the observer so it cannot repair a removal
	var loadParked atomic.Bool
	loadEntered, loadRelease := make(chan struct{}, 1), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-loadRelease:
		default:
			close(loadRelease)
		}
	})
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "t6", func(sm *mqttSessionManager) {
		sm.testRetainedApplyGate = gate.park
		sm.testRetainedLoadGate = func(subjects []string) {
			for _, ss := range subjects {
				if ss == rsp+"t6.topic" && loadParked.CompareAndSwap(false, true) {
					loadEntered <- struct{}{}
					<-loadRelease
				}
			}
		}
	})
	asm := retainedASM(t, s)

	// The load a literal SUBSCRIBE forces for an unindexed subject (collected sequence 0),
	// driven from the test goroutine for the reason T3 gives.
	loadDone := make(chan map[string]*mqttRetainedMsg, 1)
	go func() { loadDone <- asm.loadRetainedMessages(map[string]uint64{"t6.topic": 0}, asm.jsa.c) }()
	<-loadEntered // the read found no record and is parked

	mustPublish(t, connectClient(t, addr, "t6-pub"), "t6/topic", 1, true, "v1")
	gate.requireEntered(t, false)
	if !asm.retainedIndexedLive("t6.topic") {
		t.Fatal("fixture: the seed must have indexed the subject")
	}
	close(loadRelease)
	if rms := <-loadDone; rms["t6.topic"] != nil {
		t.Fatal("fixture: the parked load must have found no record")
	}
	if !asm.retainedIndexedLive("t6.topic") {
		t.Fatal("a no-message answer for an unindexed forced subject removed a newer seeded entry")
	}
	gate.free()
	expectMsg(t, recvChan(t, connectClient(t, addr, "t6-wild"), "t6/#", 1), "v1")
}

// bareRetainedASM is a manager with no server: enough for the arbitration functions.
func bareRetainedASM() *mqttAccountSessionManager {
	return &mqttAccountSessionManager{rmsCache: &sync.Map{}, rmObs: mqttRetainedObserverState{wake: make(chan struct{}, 1)}}
}

// T7 (kills M7: the no-message arm above W deletes instead of leaving a placeholder).
func TestRetainedNoMessageAboveWatermarkLeavesPlaceholder(t *testing.T) {
	t.Parallel()
	asm := bareRetainedASM()
	rm := &mqttRetainedMsg{Subject: "t7.x", Topic: "t7/x", Msg: []byte("v"), Flags: 1}
	asm.mu.Lock()
	defer asm.mu.Unlock()
	asm.rmObs.applied = 10
	if !asm.applyRetainedLocked("t7.x", 20, rm, mqttRetainedFromLoad) {
		t.Fatal("fixture: an entry above W must be accepted")
	}
	asm.retainedNoMessageLocked("t7.x", 20)
	if asm.applyRetainedLocked("t7.x", 15, rm, mqttRetainedFromSeed) {
		t.Fatal("a straggler between W and the vanished record's sequence was accepted")
	}
	if ref, ok := asm.retmsgs.Find([]byte("t7.x")); !ok || !ref.deleted || ref.sseq != 20 {
		t.Fatalf("want a placeholder at 20, got %+v ok=%v", ref, ok)
	}
	// And W passing it prunes it.
	asm.rmObs.applied = 20
	asm.pruneRetainedPlaceholdersLocked()
	if _, ok := asm.retmsgs.Find([]byte("t7.x")); ok {
		t.Fatal("a placeholder W has passed must be pruned")
	}
}

// T8 (kills M8: a JetStream marker falls to the decode-failure path). A marker is a
// deletion at its sequence, through the observer and through the loader, with no decode
// warning.
func TestRetainedSubjectDeleteMarkerIsADeletion(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	s, addr, lc := retainedBrokerWith(t, srv.ClientURL(), "t8", nil)
	asm := retainedASM(t, s)

	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"t8.a", "t8/a", "A")
	requireIndexed(t, asm, "t8.a", true)
	marker := nats.Header{}
	marker.Set(JSMarkerReason, "MaxAge")
	injectRetainedMsg(t, srv.ClientURL(), &nats.Msg{Subject: rsp + "t8.a", Header: marker})
	requireIndexed(t, asm, "t8.a", false)

	// Through the loader: an exact SUBSCRIBE over a marker the index never saw.
	injectRetainedMsg(t, srv.ClientURL(), &nats.Msg{Subject: rsp + "t8.b", Header: marker})
	waitApplied(t, asm, streamSeq(t, srv.ClientURL(), rsp+"t8.b"))
	expectNoMsg(t, recvChan(t, connectClient(t, addr, "t8-exact"), "t8/b", 1), 500*time.Millisecond)

	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, l := range lc.lines {
		if strings.Contains(l, "t8.") && (strings.Contains(l, "Rejecting retained message") || strings.Contains(l, "failed to decode retained message")) {
			t.Fatalf("a marker was treated as an undecodable record: %q", l)
		}
	}
}

// T9 (kills M17: the watermark test before equal-sequence hydration). A cold load of the
// record the observer has already committed must still populate the cache.
func TestRetainedEqualSequenceLoadHydratesCache(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "t9", nil)
	asm := retainedASM(t, s)
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"t9.topic", "t9/topic", "A")
	waitApplied(t, asm, streamSeq(t, srv.ClientURL(), rsp+"t9.topic"))
	if asm.getCachedRetainedMsg("t9.topic") != nil {
		t.Fatal("fixture: an observer-only record must not be cached yet")
	}
	expectMsg(t, recvChan(t, connectClient(t, addr, "t9-exact"), "t9/topic", 1), "A")
	if asm.getCachedRetainedMsg("t9.topic") == nil {
		t.Fatal("a cold load of the committed record did not hydrate the cache")
	}
}

// ---------------------------------------------------------------------------
// Loss
// ---------------------------------------------------------------------------

// dropOnce drops exactly one delivery for streamSubject and signals it.
func dropOnce(streamSubject string, dropped chan<- struct{}) func(string) bool {
	var done atomic.Bool
	return func(subject string) bool {
		if subject == streamSubject && done.CompareAndSwap(false, true) {
			dropped <- struct{}{}
			return true
		}
		return false
	}
}

// L1 (kills M9: no delivery-sequence gap check; and M12: resume at W+2). A delivery lost
// mid-stream is noticed at the next delivery and replayed from exactly W+1.
func TestRetainedLostMidStreamDeliveryIsRepaired(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	dropped := make(chan struct{}, 1)
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "l1", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = time.Minute // only the gap may detect this
		sm.testRetainedDeliveryDrop = dropOnce(rsp+"l1.a", dropped)
	})
	asm := retainedASM(t, s)
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l1.warm", "l1/warm", "W")
	waitApplied(t, asm, streamSeq(t, srv.ClientURL(), rsp+"l1.warm"))

	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l1.a", "l1/a", "A")
	<-dropped
	aSeq := streamSeq(t, srv.ClientURL(), rsp+"l1.a")
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l1.b", "l1/b", "B")

	requireIndexed(t, asm, "l1.a", true)
	requireIndexed(t, asm, "l1.b", true)
	st := observerState(asm)
	if st.resetReason != "gap" || st.resetStart != aSeq {
		t.Fatalf("want a gap reset resuming at the lost record %d, got reason=%q start=%d", aSeq, st.resetReason, st.resetStart)
	}
	wild := recvChan(t, connectClient(t, addr, "l1-wild"), "l1/#", 1)
	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case m := <-wild:
			got[m] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("wildcard replay incomplete: %v", got)
		}
	}
	if !got["A"] || !got["B"] || !got["W"] {
		t.Fatalf("wildcard replay: %v", got)
	}
}

// L2 (kills M10: the heartbeat's Nats-Last-Consumer is ignored). A lost LAST delivery
// has no later delivery to reveal it; the idle heartbeat does, while heartbeats keep
// the liveness timer from being the repairer.
func TestRetainedLostTailDeliveryIsRepaired(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	dropped := make(chan struct{}, 1)
	s, _, _ := retainedBrokerWith(t, srv.ClientURL(), "l2", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = 300 * time.Millisecond
		sm.testRetainedDeliveryDrop = dropOnce(rsp+"l2.a", dropped)
	})
	asm := retainedASM(t, s)
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l2.a", "l2/a", "A")
	<-dropped
	requireIndexed(t, asm, "l2.a", true)
	if st := observerState(asm); st.resetReason != "heartbeat" {
		t.Fatalf("the tail loss must be detected by the heartbeat, reset reason %q", st.resetReason)
	}
}

// L3 (kills M11: no missed-heartbeat liveness timer). A consumer reaped out from under
// the observer is re-created, under the same name, from W+1.
func TestRetainedObserverReapedIsRecreatedFromWatermark(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	s, _, _ := retainedBrokerWith(t, srv.ClientURL(), "l3", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = 300 * time.Millisecond
	})
	asm := retainedASM(t, s)
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l3.warm", "l3/warm", "W")
	wSeq := streamSeq(t, srv.ClientURL(), rsp+"l3.warm")
	waitApplied(t, asm, wSeq)

	id, _ := s.ephemeralIdentity()
	name := mqttRetainedObserverName(id)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := js.DeleteConsumer(ctx, mqttRetainedMsgsStreamName, name); err != nil {
		t.Fatalf("delete observer: %v", err)
	}
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l3.a", "l3/a", "A")
	requireIndexed(t, asm, "l3.a", true)
	st := observerState(asm)
	if st.resetReason != "liveness" || st.resetStart != wSeq+1 {
		t.Fatalf("want a liveness reset from W+1=%d, got reason=%q start=%d", wSeq+1, st.resetReason, st.resetStart)
	}
	ci, err := asm.jsa.lookupConsumer(mqttRetainedMsgsStreamName, name)
	if err != nil {
		t.Fatalf("the observer must exist again under the same name: %v", err)
	}
	if got := ci.Config.OptStartSeq; got != wSeq+1 {
		t.Fatalf("re-created observer starts at %d, want W+1=%d", got, wSeq+1)
	}
}

// L4a (kills M13a: no re-validation of the generation at commit). An application parked
// across a reset is refused even when its delivery sequence would be acceptable to the
// new generation.
func TestRetainedStaleGenerationApplicationIsRefused(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	gate := newParkGate(t, "l4.r")
	committed := make(chan string, 16)
	s, _, _ := retainedBrokerWith(t, srv.ClientURL(), "l4a", func(sm *mqttSessionManager) {
		sm.testRetainedApplyGate = gate.park
		sm.testRetainedCommitObserve = func(subject string) { committed <- subject }
	})
	asm := retainedASM(t, s)
	// R is the first record of a fresh stream: dseq 1 in generation g, and dseq 1 again
	// when g' replays it.
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l4.r", "l4/r", "R")
	gate.requireEntered(t, false) // g's application parked
	before := observerState(asm)

	forceObserverReset(asm)
	gate.requireEntered(t, false) // g' replayed R and is parked too
	mid := observerState(asm)
	if mid.sub == before.sub || mid.lastDseq != 0 || mid.resets != 1 {
		t.Fatalf("fixture: want one reset installed with nothing committed, got %+v", mid)
	}

	gate.releaseOne() // the FIRST parked one is g's
	select {
	case <-committed:
	case <-time.After(10 * time.Second):
		t.Fatal("the stale commit never returned")
	}
	after := observerState(asm)
	if after.lastDseq != 0 || after.resetting || after.sub != mid.sub || after.resets != 1 {
		t.Fatalf("a stale generation's application was committed into the new one: %+v", after)
	}
	gate.releaseOne()
	requireIndexed(t, asm, "l4.r", true)
}

// L4b (kills M13b: no re-validation of resetting at commit).
func TestRetainedApplicationWhileResettingIsRefused(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	gate := newParkGate(t, "l4b.r")
	committed := make(chan string, 16)
	s, _, _ := retainedBrokerWith(t, srv.ClientURL(), "l4b", func(sm *mqttSessionManager) {
		sm.testRetainedApplyGate = gate.park
		sm.testRetainedCommitObserve = func(subject string) { committed <- subject }
		sm.testRetainedObserverHold = func() bool { return true }
	})
	asm := retainedASM(t, s)
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l4b.r", "l4b/r", "R")
	gate.requireEntered(t, false)
	before := observerState(asm)
	forceObserverReset(asm)
	gate.releaseOne()
	select {
	case <-committed:
	case <-time.After(10 * time.Second):
		t.Fatal("the commit never returned")
	}
	after := observerState(asm)
	if after.lastDseq != before.lastDseq || after.applied != before.applied || asm.retainedIndexedLive("l4b.r") {
		t.Fatalf("an application was committed while resetting: before %+v after %+v", before, after)
	}
}

// L5 (kills M14: a deferred reset retried on every wake, or eligibility checked on wakes
// only). One attempt per heartbeat window, whatever wakes arrive and whenever the tick
// lands.
func TestRetainedAmbiguousResetDeleteRetriesOnCadence(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	hb := time.Second
	var ambiguous atomic.Int32
	ambiguous.Store(1)
	s, _, _ := retainedBrokerWith(t, srv.ClientURL(), "l5", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = hb
		sm.testRetainedResetDeleteAmbiguous = func() bool { return ambiguous.Add(-1) >= 0 }
	})
	asm := retainedASM(t, s)
	forceObserverReset(asm)
	retainedCheckFor(t, 5*time.Second, time.Millisecond, func() error {
		if observerState(asm).resets < 1 {
			return fmt.Errorf("no attempt yet")
		}
		return nil
	})
	first := time.Now()
	// Hammer the wake channel for most of the window; a tick lands inside it too.
	for time.Since(first) < hb*8/10 {
		asm.mu.Lock()
		select {
		case asm.rmObs.wake <- struct{}{}:
		default:
		}
		asm.mu.Unlock()
		if st := observerState(asm); st.resets != 1 {
			t.Fatalf("a deferred reset was re-attempted inside its heartbeat window: %d attempts", st.resets)
		}
		time.Sleep(5 * time.Millisecond)
	}
	retainedCheckFor(t, 5*hb, 10*time.Millisecond, func() error {
		if st := observerState(asm); st.resets < 2 || st.resetting {
			return fmt.Errorf("next eligible tick has not reset: %+v", st)
		}
		return nil
	})
}

// L6: a create that landed with its response lost is reconciled by the next attempt,
// and leaves exactly one consumer under the name.
func TestRetainedResetCreateResponseLostReconciles(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	var lose atomic.Int32
	lose.Store(1)
	s, _, _ := retainedBrokerWith(t, srv.ClientURL(), "l6", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = 300 * time.Millisecond
		sm.testRetainedCreateHook = func(err error) error {
			if err == nil && lose.Add(-1) >= 0 {
				return fmt.Errorf("mqtt test: create response lost")
			}
			return err
		}
	})
	asm := retainedASM(t, s)
	forceObserverReset(asm)
	retainedCheckFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if st := observerState(asm); st.resets < 2 || st.resetting {
			return fmt.Errorf("not reconciled yet: %+v", st)
		}
		return nil
	})
	id, _ := s.ephemeralIdentity()
	name := mqttRetainedObserverName(id)
	if ci, err := asm.jsa.lookupConsumer(mqttRetainedMsgsStreamName, name); err != nil || ci == nil {
		t.Fatalf("the reconciled observer %q does not exist: %v", name, err)
	}
	// A clustered CONSUMER.LIST may not yet report a consumer created moments ago (it is
	// reported as missing until it does), so the listing is read until it converges. A
	// second consumer under the name is a failure the moment any listing shows it.
	retainedCheckFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		got := testCountName(testConsumerNames(t, &asm.jsa, mqttRetainedMsgsStreamName), name)
		if got > 1 {
			t.Fatalf("want exactly one observer consumer, got %d", got)
		}
		if got != 1 {
			return fmt.Errorf("the listing does not report the observer yet")
		}
		return nil
	})
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l6.a", "l6/a", "A")
	requireIndexed(t, asm, "l6.a", true)
}

// L7 (kills M15: the generation installed after the create returns). The new consumer
// may deliver before its create returns, and those deliveries are committed.
func TestRetainedDeliveriesBeforeCreateReturnsAreCommitted(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	var blocked atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	s, _, _ := retainedBrokerWith(t, srv.ClientURL(), "l7", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = time.Minute
		sm.testRetainedCreateHook = func(err error) error {
			if blocked.CompareAndSwap(false, true) {
				entered <- struct{}{}
				<-release
			}
			return err
		}
	})
	asm := retainedASM(t, s)
	forceObserverReset(asm)
	<-entered // the server created the consumer; the create has not returned
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l7.a", "l7/a", "A")
	requireIndexed(t, asm, "l7.a", true)
	close(release)
}

// L8: a duplicate or renumbered delivery — what a consumer leader change looks like to
// this code — resets and applies nothing.
func TestRetainedDuplicateDeliveryResets(t *testing.T) {
	t.Parallel()
	for _, dseq := range []uint64{5, 3} {
		asm := bareRetainedASM()
		sub := &subscription{}
		asm.rmObs.sub, asm.rmObs.lastDseq, asm.rmObs.applied = sub, 5, 50
		rm := &mqttRetainedMsg{Subject: "l8.x", Topic: "l8/x", Msg: []byte("v"), Flags: 1}
		asm.commitRetainedDelivery(sub, 60, dseq, "l8.x", rm, true)
		st := observerState(asm)
		if !st.resetting || st.lastDseq != 5 || st.applied != 50 || asm.retainedIndexedLive("l8.x") {
			t.Fatalf("dseq %d after 5: want a reset and nothing applied, got %+v", dseq, st)
		}
	}
}

// L9 (kills M16: the resume does not flush the cache). A lost replacement whose record
// expired before the resume leaves nothing to redeliver; the cached predecessor must not
// be served after the resume.
func TestRetainedLostExpiringReplacementIsNotServedStale(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	var hold atomic.Bool
	hold.Store(true)
	dropped := make(chan struct{}, 1)
	var dropArmed atomic.Bool
	s, addr, _ := retainedBrokerWith(t, srv.ClientURL(), "l9", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = 300 * time.Millisecond
		sm.testRetainedObserverHold = hold.Load
		sm.testRetainedDeliveryDrop = func(subject string) bool {
			if subject == rsp+"l9.a" && dropArmed.CompareAndSwap(true, false) {
				dropped <- struct{}{}
				return true
			}
			return false
		}
	})
	asm := retainedASM(t, s)
	injectValidRetainedRecord(t, srv.ClientURL(), rsp+"l9.a", "l9/a", "v1")
	requireIndexed(t, asm, "l9.a", true)
	expectMsg(t, recvChan(t, connectClient(t, addr, "l9-warm"), "l9/a", 1), "v1")
	if asm.getCachedRetainedMsg("l9.a") == nil {
		t.Fatal("fixture: v1 must be cached")
	}

	dropArmed.Store(true)
	hdr := nats.Header{}
	hdr.Set(mqttNatsRetainedMessageTopic, "l9/a")
	hdr.Set(mqttNatsRetainedMessageFlags, "1")
	hdr.Set(mqttNatsRetainedMessageExpiry, strconv.FormatInt(time.Now().Add(time.Second).UnixNano(), 10))
	hdr.Set(JSMessageTTL, "1s")
	injectRetainedMsg(t, srv.ClientURL(), &nats.Msg{Subject: rsp + "l9.a", Header: hdr, Data: []byte("v2")})
	<-dropped
	retainedCheckFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if _, err := retainedStreamBody(t, srv.ClientURL(), rsp+"l9.a"); err == nil {
			return fmt.Errorf("v2 has not expired yet")
		}
		return nil
	})
	if st := observerState(asm); st.resets != 0 {
		t.Fatalf("fixture: no reset may run before v2 expired, got %+v", st)
	}
	if rm := asm.getCachedRetainedMsg("l9.a"); rm == nil || string(rm.Msg) != "v1" {
		t.Fatal("fixture: v1 must still be cached when the resume runs")
	}

	hold.Store(false)
	retainedCheckFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if st := observerState(asm); st.resets < 1 || st.resetting {
			return fmt.Errorf("no completed reset yet: %+v", st)
		}
		return nil
	})
	expectNoMsg(t, recvChan(t, connectClient(t, addr, "l9-exact"), "l9/a", 1), 500*time.Millisecond)
	expectNoMsg(t, recvChan(t, connectClient(t, addr, "l9-wild"), "l9/#", 1), 500*time.Millisecond)
	if asm.retainedIndexedLive("l9.a") {
		t.Fatal("the vanished subject is still a replay candidate after an authoritative load")
	}
}

// L10 (kills M18: the loop started before provisioning). A reset requested while
// initialization still owns the observer waits for the hand-off.
func TestRetainedNoResetBeforeHandoff(t *testing.T) {
	t.Parallel()
	srv := runJetStreamServerHA(t)
	hb := 200 * time.Millisecond
	var resetsDuringStall atomic.Int64
	resetsDuringStall.Store(-1)
	_, _, _ = retainedBrokerWith(t, srv.ClientURL(), "l10", func(sm *mqttSessionManager) {
		sm.testRetainedObserverHeartbeat = hb
		var once sync.Once
		sm.testRetainedInitHook = func(as *mqttAccountSessionManager) {
			once.Do(func() {
				forceObserverReset(as)
				time.Sleep(mqttRetainedLivenessBeats*hb + 4*hb)
				resetsDuringStall.Store(int64(observerState(as).resets))
			})
		}
	})
	if got := resetsDuringStall.Load(); got != 0 {
		t.Fatalf("a reset ran while initialization owned the observer: %d", got)
	}
}
