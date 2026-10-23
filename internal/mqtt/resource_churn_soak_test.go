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

//go:build mqttport_fulltests

package mqtt

// The resource soak: churn a broker through connect, subscribe, publish, disconnect, expiry,
// abrupt drops, silent and failed connections, then let the work drain and require every
// resource the broker owns to stand at the value the completed work leaves — not at "whatever
// warm-up left", which would accept a fixed leak. Gated by MQTTNB_RESOURCE_SOAK=1 and run
// alone (`make resource-soak`): it counts goroutines across the process and reads process
// descriptors, which neighbouring tests would disturb.
//
// What it does NOT measure is stated in docs/findings/resource-lifetime.md: per-CONNECT
// durable records are JetStream storage, not process memory, and are a stated residual.

import (
	"fmt"
	"math"
	"net"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	resourceSoakEnv     = "MQTTNB_RESOURCE_SOAK"
	resourceSoakQuiesce = 90 * time.Second
	brokerPkgFrame      = "nats-broker/internal/mqtt."
	tlsReloadPkgFrame   = "nats-broker/internal/tlsreload."
)

// isBrokerFrame reports whether a function belongs to the broker: the MQTT core, or the
// listener's TLS material, which the broker owns and the soak measures with it.
func isBrokerFrame(fn string) bool {
	return strings.Contains(fn, brokerPkgFrame) || strings.Contains(fn, tlsReloadPkgFrame)
}

func requireResourceSoak(t *testing.T) {
	t.Helper()
	if os.Getenv(resourceSoakEnv) != "1" {
		t.Skipf("set %s=1 to run the resource soak (alone: make resource-soak)", resourceSoakEnv)
	}
}

var goroutineHeader = regexp.MustCompile(`^goroutine \d+ \[`)

// brokerGoroutines classifies every goroutine that runs broker code by its ROOT: the broker
// function it was created by, or failing that its bottom-most broker frame. The current top
// frame is never the key — the same worker is sampled in different helpers from one dump to
// the next.
func brokerGoroutines() map[string]int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]int{}
	for _, g := range strings.Split(string(buf), "\n\n") {
		lines := strings.Split(strings.TrimSpace(g), "\n")
		if len(lines) == 0 || !goroutineHeader.MatchString(lines[0]) {
			continue
		}
		var root, bottom string
		broker, test := false, false
		for i := 1; i < len(lines); i++ {
			l := lines[i]
			if strings.HasPrefix(l, "\t") {
				if strings.Contains(l, "_test.go:") {
					test = true
				}
				continue
			}
			if strings.HasPrefix(l, "created by ") {
				fn := strings.TrimPrefix(l, "created by ")
				if j := strings.Index(fn, " in goroutine"); j >= 0 {
					fn = fn[:j]
				}
				if isBrokerFrame(fn) {
					root = fn
				}
				continue
			}
			if isBrokerFrame(l) {
				broker = true
				fn := l
				if j := strings.LastIndex(fn, "("); j > 0 {
					fn = fn[:j]
				}
				bottom = fn
			}
		}
		if !broker || test {
			continue
		}
		if root == "" {
			root = bottom
		}
		out[root]++
	}
	return out
}

func goroutineDiff(want, got map[string]int) string {
	keys := map[string]struct{}{}
	for k := range want {
		keys[k] = struct{}{}
	}
	for k := range got {
		keys[k] = struct{}{}
	}
	var diffs []string
	for k := range keys {
		if want[k] != got[k] {
			diffs = append(diffs, fmt.Sprintf("%s: want %d, have %d", k, want[k], got[k]))
		}
	}
	sort.Strings(diffs)
	return strings.Join(diffs, "\n")
}

// awaitIdleGoroutines polls until two consecutive dumps a second apart agree: the idle
// boundary a baseline needs.
func awaitIdleGoroutines(t *testing.T) map[string]int {
	t.Helper()
	deadline := time.Now().Add(resourceSoakQuiesce)
	prev := brokerGoroutines()
	for {
		time.Sleep(time.Second)
		cur := brokerGoroutines()
		if goroutineDiff(prev, cur) == "" {
			return cur
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker goroutines never settled:\n%s", goroutineDiff(prev, cur))
		}
		prev = cur
	}
}

func openFDs(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("no /proc/self/fd: %v", err)
	}
	return len(ents)
}

func heapInuse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}

// brokerHeapInuse estimates the in-use heap allocated by BROKER code: the memory profile's
// records whose allocation stack has a non-test frame of this package (nats.go allocations
// made on the broker's behalf included), scaled from the sample to an estimate the way pprof
// scales them. The process heap also holds the embedded nats-server's file-store caches and
// subject index, which grow with the records JetStream stores and which in production belong
// to a different process; this is the share the broker owns.
func brokerHeapInuse() int64 {
	runtime.GC()
	runtime.GC()
	var recs []runtime.MemProfileRecord
	n, _ := runtime.MemProfile(nil, true)
	for {
		recs = make([]runtime.MemProfileRecord, n+50)
		var ok bool
		n, ok = runtime.MemProfile(recs, true)
		if ok {
			recs = recs[:n]
			break
		}
	}
	rate := float64(runtime.MemProfileRate)
	var total float64
	for i := range recs {
		r := &recs[i]
		count, bytes := r.InUseObjects(), r.InUseBytes()
		if count <= 0 || bytes <= 0 {
			continue
		}
		broker := false
		frames := runtime.CallersFrames(r.Stack())
		for {
			f, more := frames.Next()
			if isBrokerFrame(f.Function) && !strings.HasSuffix(f.File, "_test.go") {
				broker = true
				break
			}
			if !more {
				break
			}
		}
		if !broker {
			continue
		}
		scale := 1.0
		if rate > 1 {
			avg := float64(bytes) / float64(count)
			scale = 1 / (1 - math.Exp(-avg/rate))
		}
		total += float64(bytes) * scale
	}
	return int64(total)
}

// resourceCounts is every container the census names whose population must return to what
// completed work leaves.
type resourceCounts struct {
	clients, sessLocked, flappers, pendingExpiries int
	owedDrains, owedIntents, owedDisposals         int
	owedActions, owedTerminals, owedRetained       int
	owedWills, owedDelayedWills                    int
	jsaReplies, managerSubs, rmsCache              int
	sessions                                       []string
	willTimerKeys                                  []string
	// Per resident session, summed.
	cpending, pendingPublish, pendingPubRel, deferred, drains, disposals, subs int
	endedLineages                                                              map[string]int
}

func countResources(s *Server, as *mqttAccountSessionManager) resourceCounts {
	var rc resourceCounts
	s.mu.Lock()
	rc.clients = len(s.clients)
	s.mu.Unlock()
	lockLen := func(mu interface {
		Lock()
		Unlock()
	}, n func() int) int {
		mu.Lock()
		defer mu.Unlock()
		return n()
	}
	rc.owedDrains = lockLen(&s.owedDrainsMu, func() int { return len(s.owedDrains) })
	rc.owedIntents = lockLen(&s.owedIntentsMu, func() int { return len(s.owedIntents) })
	rc.owedDisposals = lockLen(&s.owedDisposalsMu, func() int { return len(s.owedDisposals) })
	rc.owedActions = lockLen(&s.owedActionsMu, func() int { return len(s.owedActions) })
	rc.owedTerminals = lockLen(&s.owedTerminalsMu, func() int { return len(s.owedTerminals) })
	rc.owedRetained = lockLen(&s.owedRetainedMu, func() int { return len(s.owedRetained) })
	rc.owedWills = lockLen(&s.owedWills.mu, func() int { return len(s.owedWills.owed) })
	rc.owedDelayedWills = lockLen(&s.owedDelayedWills.mu, func() int { return len(s.owedDelayedWills.owed) })
	s.willDispatchMu.Lock()
	for k := range s.willTimers {
		rc.willTimerKeys = append(rc.willTimerKeys, k)
	}
	s.willDispatchMu.Unlock()
	sort.Strings(rc.willTimerKeys)
	as.jsa.replies.Range(func(any, any) bool { rc.jsaReplies++; return true })
	if as.rmsCache != nil {
		as.rmsCache.Range(func(any, any) bool { rc.rmsCache++; return true })
	}
	as.jsa.c.mu.Lock()
	rc.managerSubs = len(as.jsa.c.subs)
	as.jsa.c.mu.Unlock()

	as.mu.RLock()
	rc.sessLocked, rc.flappers, rc.pendingExpiries = len(as.sessLocked), len(as.flappers), len(as.pendingExpiries)
	sessions := make([]*mqttSession, 0, len(as.sessions))
	for id, sess := range as.sessions {
		rc.sessions = append(rc.sessions, id)
		sessions = append(sessions, sess)
	}
	as.mu.RUnlock()
	sort.Strings(rc.sessions)
	rc.endedLineages = map[string]int{}
	for _, sess := range sessions {
		sess.mu.Lock()
		rc.cpending += len(sess.cpending)
		rc.pendingPublish += len(sess.pendingPublish)
		rc.pendingPubRel += len(sess.pendingPubRel)
		rc.deferred += len(sess.deferred)
		rc.drains += len(sess.drains)
		rc.disposals += len(sess.disposals)
		rc.subs += len(sess.subs)
		if sess.custody != nil {
			rc.endedLineages[sess.id] = len(sess.custody.endedLineages)
		}
		sess.mu.Unlock()
	}
	return rc
}

// soakWorkload drives one cycle mix against the broker. Every obligation it opens it also
// completes before disconnecting — that is what makes the zero expectations below exact.
type soakWorkload struct {
	t          *testing.T
	o          *Options
	tlsAddr    string
	persistent []string
}

func (w *soakWorkload) cycle(block, i int) {
	t := w.t
	tag := fmt.Sprintf("b%d-%d", block, i)

	// A publisher for this cycle, closed at its end.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-" + tag, cleanSess: true}, w.o.MQTT.Host, w.o.MQTT.Port)
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	defer pc.Close()

	// 1. A fresh clean session through QoS 0, 1 and 2.
	{
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "clean-" + tag, cleanSess: true}, w.o.MQTT.Host, w.o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		topic := "soak/clean/" + tag
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic + "/0", qos: 0}, {filter: topic + "/1", qos: 1}, {filter: topic + "/2", qos: 2}}, []byte{0, 1, 2})
		testMQTTFlush(t, c, nil, r)
		testMQTTPublish(t, pc, pr, 0, false, false, topic+"/0", 0, []byte("q0"))
		testMQTTCheckPubMsg(t, c, r, topic+"/0", 0, []byte("q0"))
		testMQTTPublish(t, pc, pr, 1, false, false, topic+"/1", 1, []byte("q1"))
		testMQTTCheckPubMsg(t, c, r, topic+"/1", mqttPubQos1, []byte("q1"))
		testMQTTPublish(t, pc, pr, 2, false, false, topic+"/2", 2, []byte("q2"))
		pi := testMQTTCheckPubMsgNoAck(t, c, r, topic+"/2", mqttPubQoS2, []byte("q2"))
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
		testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
		testMQTTDisconnect(t, c, nil)
		c.Close()
	}

	// 2. A persistent session: resume, a delivery left unacknowledged across an UNSUBSCRIBE
	// (the drain route), then completed.
	{
		id := w.persistent[i%len(w.persistent)]
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id}, w.o.MQTT.Host, w.o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
		topic := "soak/persist/" + tag
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
		testMQTTFlush(t, c, nil, r)
		testMQTTPublish(t, pc, pr, 1, false, false, topic, 3, []byte("persist"))
		pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQos1, []byte("persist"))
		testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: topic}})
		testMQTTFlush(t, c, nil, r)
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		testMQTTFlush(t, c, nil, r)
		testMQTTDisconnect(t, c, nil)
		c.Close()
	}

	// 3. Retained state on a fixed set of topics, overwritten every cycle.
	testMQTTPublish(t, pc, pr, 1, false, true, fmt.Sprintf("soak/retained/%d", i%5), 4, []byte(tag))

	// 4. A v5 session with a one-second expiry, left to expire.
	{
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "expire-" + tag, props: mqttV5PropsSessionExpiryReceiveMax(1, 16)},
			w.o.MQTT.Host, w.o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		c.Close()
	}

	// 5. A clean session dropped in the middle of a QoS 2 exchange: no DISCONNECT.
	{
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "drop-" + tag, cleanSess: true}, w.o.MQTT.Host, w.o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		topic := "soak/drop/" + tag
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 2}}, []byte{2})
		testMQTTFlush(t, c, nil, r)
		testMQTTPublish(t, pc, pr, 2, false, false, topic, 5, []byte("dropped"))
		pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQoS2, []byte("dropped"))
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		c.Close()
	}

	// 6. A connection that never sends CONNECT: the deadline must close it.
	if raw, err := net.Dial("tcp", net.JoinHostPort(w.o.MQTT.Host, fmt.Sprint(w.o.MQTT.Port))); err == nil {
		go func() {
			time.Sleep(3 * time.Second)
			raw.Close()
		}()
	}

	// 7. TLS: a handshake that fails, and one that stalls until the handshake deadline.
	if w.tlsAddr != "" {
		if raw, err := net.Dial("tcp", w.tlsAddr); err == nil {
			_, _ = raw.Write([]byte("this is not a ClientHello\r\n\r\n"))
			raw.Close()
		}
		if raw, err := net.Dial("tcp", w.tlsAddr); err == nil {
			go func() {
				time.Sleep(3 * time.Second)
				raw.Close()
			}()
		}
	}
}

// quiesce waits, with a deadline, until the work the workload opened has visibly completed:
// no client is connected, every finite-expiry record is gone, the flapper sweep and the
// retained-cache sweep have run since the last activity, and no JetStream request is pending.
func quiesce(t *testing.T, s *Server, as *mqttAccountSessionManager, persistent []string) resourceCounts {
	t.Helper()
	deadline := time.Now().Add(resourceSoakQuiesce)
	// The sweeps run on their own timers; wait past one full period of each.
	time.Sleep(2*mqttFlapCleanItvl + 2*mqttRetainedCacheTTL + 2*time.Second)
	for {
		rc := countResources(s, as)
		var pending []string
		if rc.clients != 0 {
			pending = append(pending, fmt.Sprintf("%d client(s) connected", rc.clients))
		}
		if len(rc.sessions) != len(persistent) {
			pending = append(pending, fmt.Sprintf("sessions %v, want only the persistent %v", rc.sessions, persistent))
		}
		if rc.jsaReplies != 0 {
			pending = append(pending, fmt.Sprintf("%d JetStream request(s) pending", rc.jsaReplies))
		}
		if rc.drains != 0 || rc.disposals != 0 {
			pending = append(pending, fmt.Sprintf("drains %d, disposals %d", rc.drains, rc.disposals))
		}
		if len(pending) == 0 {
			return rc
		}
		if time.Now().After(deadline) {
			t.Fatalf("work did not drain within %v:\n%s", resourceSoakQuiesce, strings.Join(pending, "\n"))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// assertExpected compares each count with the value completed work leaves.
func assertExpected(t *testing.T, block int, rc resourceCounts, persistent []string) {
	t.Helper()
	zero := map[string]int{
		"Server.clients": rc.clients, "sessLocked": rc.sessLocked, "flappers": rc.flappers,
		"pendingExpiries": rc.pendingExpiries, "owedDrains": rc.owedDrains, "owedIntents": rc.owedIntents,
		"owedDisposals": rc.owedDisposals, "owedActions": rc.owedActions, "owedTerminals": rc.owedTerminals,
		"owedRetained": rc.owedRetained, "owedWills": rc.owedWills, "owedDelayedWills": rc.owedDelayedWills,
		"JSA replies": rc.jsaReplies, "cpending buckets": rc.cpending, "pendingPublish": rc.pendingPublish,
		"pendingPubRel": rc.pendingPubRel, "deferred": rc.deferred, "drains": rc.drains,
		"disposals": rc.disposals, "session subscriptions": rc.subs,
	}
	for name, n := range zero {
		if n != 0 {
			t.Errorf("block %d: %s = %d, want 0 once the work completed", block, name, n)
		}
	}
	if fmt.Sprint(rc.sessions) != fmt.Sprint(persistent) {
		t.Errorf("block %d: resident sessions %v, want exactly the persistent %v", block, rc.sessions, persistent)
	}
	if rc.managerSubs != 3 {
		t.Errorf("block %d: the account manager holds %d service subscriptions, want 3", block, rc.managerSubs)
	}
	if rc.rmsCache > 5 {
		t.Errorf("block %d: the retained cache holds %d entries for 5 retained topics", block, rc.rmsCache)
	}
	perSession := map[string]int{}
	for _, k := range rc.willTimerKeys {
		if !strings.HasPrefix(k, "$MQTT_custody_tick.") {
			t.Errorf("block %d: a will timer that is not a custody tick: %q (the soak creates no Wills)", block, k)
			continue
		}
		perSession[strings.Split(strings.TrimPrefix(k, "$MQTT_custody_tick."), ".")[0]]++
	}
	for id, n := range perSession {
		if n > 2 {
			t.Errorf("block %d: session %q holds %d custody ticks, at most a firing arm and its replacement", block, id, n)
		}
	}
}

// assertEndedLineagesBounded requires each resident session's ended-lineage veto set to stay within
// the registers of its generation that still exist, counted by listing the register subjects
// directly, plus at most one prune batch. Pruning removes an entry once its register no longer
// names it, but only when the floor runs, and the floor is armed by an ending: the run an ending
// arms can still find that exchange's register standing (its deletion lands a moment later),
// keeps the entry, and goes quiescent until the next ending prunes it. So a quiescent session
// legitimately carries the entries its last run kept, which the next activity removes; what
// may not happen is the set growing with the traffic, which a disabled prune does (26 entries
// after one block). Polled past one floor tick.
func assertEndedLineagesBounded(t *testing.T, block int, as *mqttAccountSessionManager) {
	t.Helper()
	deadline := time.Now().Add(mqttFloorTick + 15*time.Second)
	for {
		as.mu.RLock()
		sessions := make([]*mqttSession, 0, len(as.sessions))
		for _, sess := range as.sessions {
			sessions = append(sessions, sess)
		}
		as.mu.RUnlock()
		var over []string
		for _, sess := range sessions {
			sess.mu.Lock()
			idHash, gen, ended := sess.idHash, sess.gen, 0
			if sess.custody != nil {
				ended = len(sess.custody.endedLineages)
			}
			sess.mu.Unlock()
			filter := mqttInflightStreamSubjectPrefix + idHash + mqttSessGenTk(gen, ".") + ".*"
			regs, err := as.jsa.streamSubjectsAllPages(mqttInflightStreamName, filter)
			if err != nil {
				t.Fatalf("block %d: listing %s's registers: %v", block, sess.id, err)
			}
			if ended > len(regs)+mqttFloorPruneBatch {
				over = append(over, fmt.Sprintf("%s: %d ended lineages, %d registers", sess.id, ended, len(regs)))
			}
		}
		if len(over) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("block %d: ended-lineage sets outlived their registers:\n%s", block, strings.Join(over, "\n"))
			return
		}
		time.Sleep(time.Second)
	}
}

func TestResourceChurnSingleBroker(t *testing.T) {
	requireResourceSoak(t)
	// Sample allocations finely enough that the broker's share is measured rather than
	// represented by one 512 KiB sample; set before the server allocates anything.
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = 4096
	t.Cleanup(func() { runtime.MemProfileRate = prevRate })
	prevJail, prevFlap, prevTTL := mqttSessJailDur, mqttFlapCleanItvl, mqttRetainedCacheTTL
	mqttSessJailDur, mqttFlapCleanItvl, mqttRetainedCacheTTL = 50*time.Millisecond, 250*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { mqttSessJailDur, mqttFlapCleanItvl, mqttRetainedCacheTTL = prevJail, prevFlap, prevTTL })

	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	o.MQTT.AuthTimeout = 0.5 // the CONNECT deadline, seconds
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A second, TLS, broker on its own store for the handshake failures.
	to := testMQTTDefaultOptions()
	to.MQTT.TLSConfig = testMQTTTLSMaterial(t).serverCfg
	to.MQTT.TLSTimeout = 1
	ts := testMQTTRunServer(t, to)
	defer testMQTTShutdownServer(ts)

	persistent := []string{"persist-0", "persist-1", "persist-2", "persist-3", "persist-4", "persist-5", "persist-6", "persist-7"}
	w := &soakWorkload{t: t, o: o, tlsAddr: net.JoinHostPort(to.MQTT.Host, fmt.Sprint(to.MQTT.Port)), persistent: persistent}
	// Create the persistent sessions (infinite expiry: 3.1.1 clean=false) before the baseline.
	for _, id := range persistent {
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTDisconnect(t, c, nil)
		c.Close()
	}
	as := testMQTTGetGlobalASM(t, s)
	quiesce(t, s, as, persistent)
	baseline := awaitIdleGoroutines(t)
	// Every session object the churn removes — clean disconnects, expiries, drops — must be
	// unreachable once its block has quiesced: a pinned object is a leak the counts above
	// cannot see, since it is no longer in any map they read.
	removed := captureRemovedSessions(t, "")

	const blocks, perBlock = 5, 100 // block 0 is the warm-up
	var heaps []uint64
	var brokerHeaps []int64
	var fds []int
	for b := 0; b < blocks; b++ {
		for i := 0; i < perBlock; i++ {
			w.cycle(b, i)
		}
		rc := quiesce(t, s, as, persistent)
		if b == 0 {
			continue
		}
		assertExpected(t, b, rc, persistent)
		assertEndedLineagesBounded(t, b, as)
		removed.awaitCollected(t, resourceSoakQuiesce)
		// The goroutines a broker with no client runs, per root, polled to the deadline.
		deadline := time.Now().Add(resourceSoakQuiesce)
		for {
			d := goroutineDiff(baseline, brokerGoroutines())
			if d == "" {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("block %d: broker goroutines did not return to the idle set:\n%s", b, d)
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		heaps = append(heaps, heapInuse())
		brokerHeaps = append(brokerHeaps, brokerHeapInuse())
		fds = append(fds, openFDs(t))
		profDir := os.Getenv("MQTTNB_RESOURCE_SOAK_DIR")
		if profDir == "" {
			profDir = t.TempDir()
		}
		if f, err := os.Create(fmt.Sprintf("%s/heap-block-%d.pprof", profDir, b)); err == nil {
			_ = pprof.WriteHeapProfile(f)
			f.Close()
		}
		t.Logf("block %d: broker heap in use ~%d bytes (process %d), %d descriptors, ended lineages %v",
			b, brokerHeaps[len(brokerHeaps)-1], heaps[len(heaps)-1], fds[len(fds)-1], rc.endedLineages)
	}
	// Sustained growth only, between the first and last measured blocks; the exact counts
	// above are the oracle. The heap assertion reads the BROKER's share: the process heap grows
	// with the embedded JetStream's file-store block caches and subject index as records
	// accumulate (measured: about 23 MB per block, none of it under a broker frame), which is
	// nats-server memory in production. It is logged, not asserted.
	const heapAllowance, fdAllowance = 8 << 20, 16
	if grew := brokerHeaps[len(brokerHeaps)-1] - brokerHeaps[0]; grew > heapAllowance {
		t.Errorf("broker-allocated heap in use grew ~%d bytes across %d blocks of completed work (allowance %d)",
			grew, blocks-2, heapAllowance)
	}
	if grew := fds[len(fds)-1] - fds[0]; grew > fdAllowance {
		t.Errorf("open descriptors grew by %d across %d blocks (allowance %d)", grew, blocks-2, fdAllowance)
	}
}

// After churn, a completed shutdown leaves nothing of the broker running: every goroutine with
// a broker root gone (the retained loops end on their next wake, so this polls), every
// subscription on the caller's service connection its own.
func TestResourceShutdownComplete(t *testing.T) {
	requireResourceSoak(t)
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	persistent := []string{"shut-0", "shut-1"}
	w := &soakWorkload{t: t, o: o, persistent: persistent}
	for _, id := range persistent {
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTDisconnect(t, c, nil)
		c.Close()
	}
	for i := 0; i < 50; i++ {
		w.cycle(0, i)
	}
	nc := s.nrc
	select {
	case <-s.BeginShutdown():
	case <-time.After(60 * time.Second):
		t.Fatal("shutdown did not complete")
	}
	deadline := time.Now().Add(2*mqttRetainedStepBackoff(100) + 5*time.Second)
	for {
		left := brokerGoroutines()
		if len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker goroutines outlived a completed shutdown:\n%s", goroutineDiff(nil, left))
		}
		time.Sleep(250 * time.Millisecond)
	}
	if n := nc.NumSubscriptions(); n != 0 {
		t.Fatalf("the service connection still holds %d broker subscriptions after shutdown", n)
	}
	testMQTTShutdownServer(s)
}
