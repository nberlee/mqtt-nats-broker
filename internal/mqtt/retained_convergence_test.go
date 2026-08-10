package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Retained state is carried between brokers by the $MQTT_rmsgs stream alone:
// the header-encoded record is the value, an empty body is the deletion, and
// each broker's own consumer applies both to its volatile index. These tests
// exercise that path across two brokers and against records the broker cannot
// decode, which is the coverage F-056 needed before the development-era
// compatibility readers (JSON fallback, tombstone delete marker, the "RD"
// delete notification) could be removed.

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// logCapture records every log line a broker emits so a test can assert on a
// diagnostic the broker is required to produce.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (lc *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (lc *logCapture) Handle(_ context.Context, r slog.Record) error {
	lc.mu.Lock()
	lc.lines = append(lc.lines, r.Message)
	lc.mu.Unlock()
	return nil
}

func (lc *logCapture) WithAttrs([]slog.Attr) slog.Handler { return lc }
func (lc *logCapture) WithGroup(string) slog.Handler      { return lc }

// requireLine waits until some captured line contains every one of `parts`.
// The wait matters: the retained consumer callback runs on its own goroutine,
// so the observable effect a test synchronizes on (an index entry appearing or
// disappearing) can land marginally before or after the log write.
func (lc *logCapture) requireLine(t *testing.T, parts ...string) {
	t.Helper()
	retainedCheckFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		lc.mu.Lock()
		defer lc.mu.Unlock()
		for _, line := range lc.lines {
			matched := true
			for _, p := range parts {
				if !strings.Contains(line, p) {
					matched = false
					break
				}
			}
			if matched {
				return nil
			}
		}
		return fmt.Errorf("no log line containing %q in %d lines", parts, len(lc.lines))
	})
}

// retainedBroker starts an MQTT broker bound to an already-running NATS server,
// with its log captured. Unlike brokerOn it installs the capturing logger
// BEFORE Start and hands back the *Server, because these tests inspect the
// retained index directly and assert on warnings the broker emits while
// replaying the retained stream — which begins during Start.
func retainedBroker(t *testing.T, url, node string) (*Server, string, *logCapture) {
	t.Helper()
	return retainedBrokerGated(t, url, node, nil)
}

// retainedBrokerGated additionally installs the retained-apply test seam, which
// must also be in place before Start for the same reason as the logger.
func retainedBrokerGated(t *testing.T, url, node string, gate func(subject string, tombstone bool)) (*Server, string, *logCapture) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(nc.Close)
	s := NewServer(nc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, node)
	lc := &logCapture{}
	s.log.Store(slog.New(lc))
	s.mqtt.sessmgr.testRetainedApplyGate = gate
	if err := s.Start(); err != nil {
		t.Fatalf("start broker %s: %v", node, err)
	}
	t.Cleanup(s.Shutdown)
	return s, fmt.Sprintf("tcp://%s", s.Addr().String()), lc
}

// retainedCheckFor polls fn until it returns nil or totalWait elapses.
func retainedCheckFor(t *testing.T, totalWait, sleepDur time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	t.Fatal(err.Error())
}

// retainedASM returns the broker's single account session manager, which owns
// the retained index.
func retainedASM(t *testing.T, s *Server) *mqttAccountSessionManager {
	t.Helper()
	var asm *mqttAccountSessionManager
	retainedCheckFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		s.mqtt.sessmgr.mu.RLock()
		defer s.mqtt.sessmgr.mu.RUnlock()
		for _, m := range s.mqtt.sessmgr.sessions {
			asm = m
			return nil
		}
		return fmt.Errorf("no account session manager yet")
	})
	return asm
}

// requireIndexed waits for the broker's retained index to hold (or not hold) an
// entry for the given NATS subject. The index is what a wildcard subscription
// enumerates, so it is the state that decides whether a retained message is
// still replayable on this broker.
func requireIndexed(t *testing.T, asm *mqttAccountSessionManager, subject string, want bool) {
	t.Helper()
	retainedCheckFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		asm.mu.RLock()
		defer asm.mu.RUnlock()
		var found bool
		if asm.retmsgs != nil {
			_, found = asm.retmsgs.Find([]byte(subject))
		}
		if found != want {
			return fmt.Errorf("retained index entry for %q: got %v, want %v", subject, found, want)
		}
		return nil
	})
}

// injectRetainedRecord stores a raw record into $MQTT_rmsgs, bypassing the
// broker's encoder. Used to plant records in formats the broker must refuse.
func injectRetainedRecord(t *testing.T, url, subject string, body []byte) {
	t.Helper()
	injectRetainedMsg(t, url, &nats.Msg{Subject: subject, Data: body})
}

// injectValidRetainedRecord stores a record in the current header format
// directly into the stream, so that the consumer callback is its ONLY applier —
// no local seed runs for it, which is what makes the index/cache interaction
// deterministic.
func injectValidRetainedRecord(t *testing.T, url, subject, topic, payload string) {
	t.Helper()
	hdr := nats.Header{}
	hdr.Set(mqttNatsRetainedMessageTopic, topic)
	hdr.Set(mqttNatsRetainedMessageFlags, "1")
	injectRetainedMsg(t, url, &nats.Msg{Subject: subject, Header: hdr, Data: []byte(payload)})
}

func injectRetainedMsg(t *testing.T, url string, msg *nats.Msg) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("inject connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatalf("inject %q: %v", msg.Subject, err)
	}
}

// retainedStreamBody returns the body of the last stream record for a retained
// subject, or an error when the subject holds no record.
func retainedStreamBody(t *testing.T, url, subject string) ([]byte, error) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, mqttRetainedMsgsStreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

// legacyJSONRetainedRecord is a body in the development-era JSON format that
// mqttDecodeRetainedMessage used to accept when the Nmqtt-RFlags header was
// absent. It is written literally rather than by marshalling mqttRetainedMsg,
// because that struct no longer carries serialization tags — the point is that
// a body in this shape is no longer interpretable at all.
func legacyJSONRetainedRecord(t *testing.T, topic, payload string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"topic":  topic,
		"origin": "legacy-origin",
		"source": "legacy-source",
		"flags":  1,
		"msg":    []byte(payload),
	})
	if err != nil {
		t.Fatalf("marshal legacy record: %v", err)
	}
	return b
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestRetainedCrossBrokerConvergence pins the whole cross-broker retained
// contract on the stream alone: a broker that did not publish a retained
// message learns it, and learns its deletion, purely from its own consumer's
// view of $MQTT_rmsgs. It also pins the durable shape a deletion leaves behind
// — one record for the subject, and that record is the empty-body tombstone —
// which is why no explicit delete of the record it replaces is needed.
func TestRetainedCrossBrokerConvergence(t *testing.T) {
	srv := runJetStreamServerHA(t)
	_, addrA, _ := retainedBroker(t, srv.ClientURL(), "node-a")
	sB, addrB, _ := retainedBroker(t, srv.ClientURL(), "node-b")
	asmB := retainedASM(t, sB)

	pub := connectClient(t, addrA, "conv-pub")
	mustPublish(t, pub, "conv/topic", 1, true, "hello")

	// Broker B never saw the PUBLISH; it learns the retained message from the
	// stream, and a wildcard subscriber is served out of that index.
	requireIndexed(t, asmB, "conv.topic", true)
	subB := connectClient(t, addrB, "conv-sub-1")
	expectMsg(t, recvChan(t, subB, "conv/#", 1), "hello")

	// An empty payload deletes [MQTT-3.3.1-11]. B must converge on the deletion
	// the same way, with no notification of any kind from A.
	mustPublish(t, pub, "conv/topic", 1, true, "")
	requireIndexed(t, asmB, "conv.topic", false)
	subB2 := connectClient(t, addrB, "conv-sub-2")
	expectNoMsg(t, recvChan(t, subB2, "conv/#", 1), 500*time.Millisecond)

	// The durable residue of the deletion: the tombstone is the subject's last
	// record, and it is the ONLY record for that subject — MaxMsgsPer=1 evicted
	// the message it replaced when it was stored, empty body or not. This is the
	// premise the removal of the compaction delete rests on (F-056), so it is
	// asserted rather than assumed. A failure would not by itself resurrect the
	// predecessor — an ordered replay and a last-per-subject read both still land
	// on the tombstone — but it would mean the stream retains superseded records
	// against its own MaxMsgsPer invariant, and that the reasoning behind the
	// removal needs redoing.
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, mqttRetainedMsgsStreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	subject := mqttRetainedMsgsStreamSubject + "conv.topic"
	last, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		t.Fatalf("get last msg for %q: %v", subject, err)
	}
	if len(last.Data) != 0 {
		t.Fatalf("last record for %q has a %d-byte body, want the empty-body tombstone",
			subject, len(last.Data))
	}
	si, err := stream.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if n := si.State.Subjects[subject]; n != 1 {
		t.Fatalf("stream holds %d records for %q, want exactly 1 (the tombstone)", n, subject)
	}
}

// TestRetainedCorruptRecordFailsClosed pins the replacement for the removed
// "ignore what you cannot parse" behavior: a record in a format this broker
// does not write is rejected with a log naming it, and is never served. It runs
// the rejection through a broker's cold replay of the whole stream, which is
// also the restart-recovery path.
func TestRetainedCorruptRecordFailsClosed(t *testing.T) {
	srv := runJetStreamServerHA(t)
	sA, addrA, _ := retainedBroker(t, srv.ClientURL(), "node-a")

	// Plant the undecodable record FIRST, so that on the fresh broker's ordered
	// replay it is guaranteed to have been processed by the time the valid one
	// arrives. Without that ordering the assertions below could pass simply
	// because the subscription raced ahead of the corrupt record's callback.
	corruptSubject := mqttRetainedMsgsStreamSubject + "corrupt.topic"
	injectRetainedRecord(t, srv.ClientURL(), corruptSubject,
		legacyJSONRetainedRecord(t, "corrupt/topic", "legacy"))

	pub := connectClient(t, addrA, "corrupt-pub")
	mustPublish(t, pub, "valid/topic", 1, true, "good")
	pub.Disconnect(100)
	sA.Shutdown()

	// Restart: a fresh broker replays the stream from the beginning.
	sB, addrB, lcB := retainedBroker(t, srv.ClientURL(), "node-b")
	asmB := retainedASM(t, sB)

	// The valid record's arrival in the index is the barrier proving the earlier
	// corrupt record has already been through the callback.
	requireIndexed(t, asmB, "valid.topic", true)
	requireIndexed(t, asmB, "corrupt.topic", false)

	sub := connectClient(t, addrB, "corrupt-sub")
	ch := recvChan(t, sub, "#", 1)
	expectMsg(t, ch, "good")
	expectNoMsg(t, ch, 500*time.Millisecond)

	// The rejection must be diagnosable, not silent: that is the whole point of
	// removing the mechanism that required servers to ignore what they could not
	// parse.
	lcB.requireLine(t, corruptSubject, errMQTTRetainedMsgUnsupportedFormat.Error())
}

// TestRetainedCorruptReplacementEvictsCachedPredecessor covers the case a plain
// log-and-return would get wrong. When an undecodable record REPLACES a valid
// one, the stream no longer holds anything servable for that subject, so an
// index entry or cached copy left behind would keep serving a value the durable
// authority has lost — fail-open. The decode failure must evict.
func TestRetainedCorruptReplacementEvictsCachedPredecessor(t *testing.T) {
	srv := runJetStreamServerHA(t)
	s, addr, lc := retainedBroker(t, srv.ClientURL(), "node-a")
	asm := retainedASM(t, s)

	pub := connectClient(t, addr, "repl-pub")
	mustPublish(t, pub, "repl/topic", 1, true, "keep")
	requireIndexed(t, asm, "repl.topic", true)

	// Subscribing with an EXACT filter loads the record from the stream and puts
	// it in rmsCache, which is the copy that would outlive the index entry.
	warm := connectClient(t, addr, "repl-warm")
	expectMsg(t, recvChan(t, warm, "repl/topic", 1), "keep")

	// Replace it with a record the broker cannot decode. MaxMsgsPer=1 means the
	// valid record is gone from the stream at this point.
	corruptSubject := mqttRetainedMsgsStreamSubject + "repl.topic"
	injectRetainedRecord(t, srv.ClientURL(), corruptSubject,
		legacyJSONRetainedRecord(t, "repl/topic", "legacy"))

	requireIndexed(t, asm, "repl.topic", false)
	lc.requireLine(t, corruptSubject, errMQTTRetainedMsgUnsupportedFormat.Error())

	// Neither filter shape may serve the predecessor: the wildcard reads the
	// index, the exact filter reads the cache and then the stream.
	exact := connectClient(t, addr, "repl-exact")
	expectNoMsg(t, recvChan(t, exact, "repl/topic", 1), 500*time.Millisecond)
	wild := connectClient(t, addr, "repl-wild")
	expectNoMsg(t, recvChan(t, wild, "repl/#", 1), 500*time.Millisecond)
}

// TestRetainedEvictionClearsCacheWithoutIndexEntry covers the state in which the
// cached copy outlives the index entry. An exact-filter subscription caches
// unconditionally while only the consumer callback and the local seed ever
// index, so cache-present with index-absent is reachable — F-059's first arm.
// A decode-failure eviction that gave up as soon as the index had no entry
// would leave the predecessor cached, and a later valid add re-indexes the
// subject without refreshing the cache, making that predecessor servable again.
func TestRetainedEvictionClearsCacheWithoutIndexEntry(t *testing.T) {
	srv := runJetStreamServerHA(t)
	s, addr, lc := retainedBroker(t, srv.ClientURL(), "node-a")
	asm := retainedASM(t, s)

	pub := connectClient(t, addr, "evict-pub")
	mustPublish(t, pub, "evict/topic", 1, true, "v1")
	requireIndexed(t, asm, "evict.topic", true)

	// An exact-filter subscription loads the record and caches it.
	warm := connectClient(t, addr, "evict-warm")
	expectMsg(t, recvChan(t, warm, "evict/topic", 1), "v1")

	// Reproduce the absent-index arm directly: drop the index entry and leave
	// the cache alone. This is white-box because there is no deterministic way
	// to lose an AckNone delivery on demand, but the resulting state is exactly
	// what F-059 documents.
	asm.mu.Lock()
	asm.retmsgs.Delete([]byte("evict.topic"))
	asm.mu.Unlock()

	// A record the broker cannot decode replaces v1 in the stream. The eviction
	// must clear the cached predecessor even though the index has nothing.
	subject := mqttRetainedMsgsStreamSubject + "evict.topic"
	injectRetainedRecord(t, srv.ClientURL(), subject,
		legacyJSONRetainedRecord(t, "evict/topic", "legacy"))
	lc.requireLine(t, subject, errMQTTRetainedMsgUnsupportedFormat.Error())

	// A later valid record for the same subject, applied only by the consumer
	// callback: it re-indexes the subject without touching the cache.
	injectValidRetainedRecord(t, srv.ClientURL(), subject, "evict/topic", "v2")
	requireIndexed(t, asm, "evict.topic", true)

	// Subscribers must see the current record, never the evicted predecessor.
	exact := connectClient(t, addr, "evict-exact")
	expectMsg(t, recvChan(t, exact, "evict/topic", 1), "v2")
	wild := connectClient(t, addr, "evict-wild")
	expectMsg(t, recvChan(t, wild, "evict/#", 1), "v2")
}

// TestRetainedDelayedTombstoneKeepsNewerRecord pins the reason the compaction
// delete beside the removed RD notification had to go, rather than being carried
// forward as harmless. The two index writers are unarbitrated: when a tombstone's
// consumer callback is delayed past a newer live publish, the sequence it reads
// back out of the index is the NEWER record's. Acting on that observation with an
// explicit stream delete destroys the authoritative current record — durable data
// loss, not the volatile staleness F-059 otherwise leaves. With the delete gone,
// the same reordering costs only an index entry the next callback restores.
func TestRetainedDelayedTombstoneKeepsNewerRecord(t *testing.T) {
	srv := runJetStreamServerHA(t)
	release := make(chan struct{})
	// Every application of race.topic announces itself BEFORE it is applied, and
	// the tombstone additionally parks until the test releases it. The
	// announcement is what makes the ordering provable rather than merely
	// likely: index state cannot serve as a barrier here, because the local seed
	// moves it independently of the callbacks.
	//
	// The park has NO timeout on purpose. A self-releasing gate would let the
	// tombstone apply before the newer publish whenever the test goroutine was
	// slow, and in that ordering the compaction delete reads sequence 0 and does
	// nothing — so a mutation restoring it would PASS. A hang is a loud failure;
	// a soft release is a silent one, and this is the only regression test for a
	// durable data-loss window.
	entered := make(chan bool, 8)
	gate := func(subject string, tombstone bool) {
		if subject != "race.topic" {
			return
		}
		entered <- tombstone
		if tombstone {
			<-release
		}
	}
	requireEntered := func(wantTombstone bool) {
		t.Helper()
		select {
		case got := <-entered:
			if got != wantTombstone {
				t.Fatalf("callback entered with tombstone=%v, want %v", got, wantTombstone)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for a callback with tombstone=%v", wantTombstone)
		}
	}
	s, addr, _ := retainedBrokerGated(t, srv.ClientURL(), "node-a", gate)
	// Registered AFTER the broker so it runs BEFORE the broker's own shutdown
	// cleanup (cleanups are LIFO): a test that fails early must free the parked
	// callback first, or shutdown would block behind it.
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	asm := retainedASM(t, s)

	pub := connectClient(t, addr, "race-pub")
	mustPublish(t, pub, "race/topic", 1, true, "v1")
	requireEntered(false)
	requireIndexed(t, asm, "race.topic", true)

	// The tombstone's local seed clears the index immediately. Waiting for its
	// callback to reach the gate is what proves the delay is real: past this
	// point it cannot apply anything until release is closed.
	mustPublish(t, pub, "race/topic", 1, true, "")
	requireEntered(true)
	requireIndexed(t, asm, "race.topic", false)

	// A newer live publish while the tombstone callback is held. Its local seed
	// puts the NEW sequence into the index — the sequence the parked callback is
	// about to read back.
	mustPublish(t, pub, "race/topic", 1, true, "v2")
	requireIndexed(t, asm, "race.topic", true)

	releaseGate()

	// The newer record's callback announcing itself proves the tombstone
	// callback has already RETURNED, since one subscription's callbacks are
	// serialized. It removed the index entry it found, so the index is absent
	// again at this moment — which is what makes the wait below a real
	// transition barrier for the newer add rather than a condition already true.
	requireEntered(false)
	requireIndexed(t, asm, "race.topic", true)

	// The durable record must have survived the whole sequence. Poll rather than
	// sample once: a delete issued by the tombstone callback is no-wait, so a
	// single early read could miss it.
	subject := mqttRetainedMsgsStreamSubject + "race.topic"
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		body, err := retainedStreamBody(t, srv.ClientURL(), subject)
		if err != nil {
			t.Fatalf("newer retained record for %q was destroyed by the delayed tombstone: %v", subject, err)
		}
		if string(body) != "v2" {
			t.Fatalf("retained record for %q is %q, want v2", subject, body)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// And it is still servable.
	sub := connectClient(t, addr, "race-sub")
	expectMsg(t, recvChan(t, sub, "race/#", 1), "v2")
}
