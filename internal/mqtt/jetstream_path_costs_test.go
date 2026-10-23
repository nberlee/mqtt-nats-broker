//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// The JetStream path benchmark measures what each MQTT operation costs the broker's JetStream
// boundary — requests and bytes by class (the request census), how many of those requests are
// outstanding at once and how long each waits (the request observer), the server's JetStream API
// counter, and wall time — under connect, subscribe, publish, reconnect, retained-message and
// session-recovery loads, plus a broker start and one sweep tick, against a standalone broker and
// one on a three-node cluster. It is the measurement the roadmap's query-path changes are judged
// by: a change that claims to remove requests must move the census; one that claims to overlap
// them must move the observer's depth and leave the census unchanged.
//
// A row is reported only when its window closed cleanly: every worker the operation started has
// finished or settled, the census's fence came back behind everything the broker had published,
// every reply-bearing request the census saw was answered, and a quiet window afterwards saw no
// broker traffic at all. The quiet check is a validity guard, not proof that nothing was missed.
//
// Opt-in: MQTTNB_JS_PATH_BENCH=1 (make bench-jspaths). MQTTNB_JS_PATH_BENCH_OUT names a CSV file
// the rows are appended to (and <file>.census the per-class rows); MQTTNB_JS_PATH_BENCH_LABEL
// labels them with the tree being measured.

const (
	jsPathBenchEnv      = "MQTTNB_JS_PATH_BENCH"
	jsPathBenchOutEnv   = "MQTTNB_JS_PATH_BENCH_OUT"
	jsPathBenchLabelEnv = "MQTTNB_JS_PATH_BENCH_LABEL"
	jsPathBenchQuiet    = 300 * time.Millisecond
	// jsPathIdle is how long the broker's sender must be idle before a window closes: past the
	// reconcile delay its deferred continuations are armed with, so one the operation armed fires
	// inside the window.
	jsPathIdle = mqttActionReconcileDelay + 500*time.Millisecond
)

// jsPathRig is one broker under measurement, restartable against the same JetStream.
type jsPathRig struct {
	name    string
	s       *Server
	host    string
	port    int
	js      nats.JetStreamContext
	tapURL  string
	servers []*natsserver.Server
	r3      bool
	restart func(t *testing.T)
}

func (r *jsPathRig) asm(t *testing.T) *mqttAccountSessionManager {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r.s.mqtt.sessmgr.mu.RLock()
		asm := r.s.mqtt.sessmgr.sessions[globalAccountName]
		r.s.mqtt.sessmgr.mu.RUnlock()
		if asm != nil {
			return asm
		}
		if time.Now().After(deadline) {
			t.Fatal("the broker has no account manager")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *jsPathRig) inMsgs(t *testing.T) int64 {
	t.Helper()
	var n int64
	for _, ns := range r.servers {
		v, err := ns.Varz(nil)
		if err != nil {
			t.Fatalf("varz: %v", err)
		}
		n += v.InMsgs
	}
	return n
}

// jsPathAckWait is the rigs' delivery AckWait: short enough that a cold restore's redeliveries
// (which nothing NAKs: the restored session has no in-memory pending set) arrive within a read
// timeout, and far above any acknowledgement delay a load makes.
const jsPathAckWait = 2 * time.Second

func jsPathStandalone(t *testing.T) *jsPathRig {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = jsPathAckWait
	s := testMQTTRunServer(t, o)
	nc, js := jsClientConnect(t, s)
	t.Cleanup(nc.Close)
	r := &jsPathRig{name: "standalone", s: s, host: o.MQTT.Host, port: o.MQTT.Port, js: js, tapURL: s.ClientURL(),
		servers: []*natsserver.Server{testEmbedded.paired(s)}}
	r.restart = func(t *testing.T) {
		mark := jsPathMarkStart()
		defer jsPathWaitStarted(t, mark)
		r.s.Shutdown()
		r.s = testMQTTRestartServerSameStore(t, r.s, o)
		if err := r.s.Start(); err != nil {
			t.Fatalf("restart: %v", err)
		}
		r.port = o.MQTT.Port
	}
	t.Cleanup(func() { testMQTTShutdownServer(r.s) })
	return r
}

func jsPathR3(t *testing.T) *jsPathRig {
	t.Helper()
	cl := testNewJetStreamCluster(t, 3)
	ackWait := func(o *Options) { o.MQTT.AckWait = jsPathAckWait }
	b := cl.startBroker(t, 0, ackWait)
	nc, err := nats.Connect(strings.Join(cl.orderedURLs(1), ","))
	if err != nil {
		t.Fatalf("bench NATS connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("bench JetStream context: %v", err)
	}
	// The census taps the node the broker's service connection is on, where its publishes enter
	// the cluster.
	r := &jsPathRig{name: "r3", s: b.s, host: b.opts.MQTT.Host, port: b.opts.MQTT.Port, js: js,
		tapURL: b.s.nrc.ConnectedUrl(), r3: true}
	for _, n := range cl.nodes {
		r.servers = append(r.servers, n.ns)
	}
	r.restart = func(t *testing.T) {
		mark := jsPathMarkStart()
		defer jsPathWaitStarted(t, mark)
		b.cleanup()
		b = cl.startBroker(t, 0, ackWait)
		r.s, r.host, r.port = b.s, b.opts.MQTT.Host, b.opts.MQTT.Port
		r.tapURL = b.s.nrc.ConnectedUrl()
	}
	return r
}

// jsPathBench is one case: a rig, the census on it, and the worker accounting.
type jsPathBench struct {
	t   *testing.T
	rig *jsPathRig
	cs  *jsRequestCensus
}

// The worker seams are installed once, before any broker starts, so a broker's initial Will scan
// and its tenure settlements are always seen.
var (
	jsPathSettling  atomic.Int64 // tenure settlement jobs running
	jsPathWillScans atomic.Int64 // initial Will scans completed
)

func jsPathInstallWorkerSeams(t *testing.T) {
	start := func(gen, tenure string) bool { jsPathSettling.Add(1); return true }
	done := func(gen, tenure string) { jsPathSettling.Add(-1) }
	mqttTestTenureSettleStartV.Store(&start)
	mqttTestTenureSettleDoneV.Store(&done)
	scan := func(*mqttAccountSessionManager) { jsPathWillScans.Add(1) }
	mqttTestWillInitialScanDoneV.Store(&scan)
	t.Cleanup(func() {
		mqttTestTenureSettleStartV.Store(nil)
		mqttTestTenureSettleDoneV.Store(nil)
		mqttTestWillInitialScanDoneV.Store(nil)
	})
}

// jsPathStartMark is what a broker start must advance before its first window: the startup
// sweep pass (which ends with the QoS 2 collection, counted by mqttOrphanSweepRuns) and the
// claim loop's initial Will scan.
type jsPathStartMark struct{ sweeps, scans int64 }

func jsPathMarkStart() jsPathStartMark {
	return jsPathStartMark{mqttOrphanSweepRuns.Load(), jsPathWillScans.Load()}
}

func jsPathWaitStarted(t *testing.T, m jsPathStartMark) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for mqttOrphanSweepRuns.Load() <= m.sweeps || jsPathWillScans.Load() <= m.scans {
		if time.Now().After(deadline) {
			t.Fatal("a broker start's startup sweep or initial Will scan did not complete within 30s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newJSPathBench(t *testing.T, rig *jsPathRig) *jsPathBench {
	t.Helper()
	b := &jsPathBench{t: t, rig: rig}
	b.cs = startJSRequestCensus(t, rig.tapURL, []string{"$JS.API.>", mqttPrefix + ">", "$JS.ACK.>"}, jsCensusBrokerClass, nil)
	return b
}

// jsPathSettle waits for every worker an operation may have started: tenure settlements, and every
// resident session's custody (nothing running, nothing owed). With reclaim, also every session's
// identifier-exclusion reclaim disarmed: a delivery's own deferred work, which runs after the
// exchange ends and rearms on backoff (up to 30 s) until every exclusion is lifted. A session that
// legitimately holds exclusions (owed PUBRELs while offline) keeps it armed, so a pre-window settle
// does not wait for it.
func (b *jsPathBench) jsPathSettle(t *testing.T) { b.jsPathSettleWorkers(t, false) }

func (b *jsPathBench) jsPathSettleWorkers(t *testing.T, reclaim bool) {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	asm := b.rig.asm(t)
	for {
		ok := jsPathSettling.Load() == 0
		if ok {
			asm.mu.RLock()
			sessions := make([]*mqttSession, 0, len(asm.sessions))
			for _, sess := range asm.sessions {
				sessions = append(sessions, sess)
			}
			asm.mu.RUnlock()
			for _, sess := range sessions {
				// The identifier-exclusion reclaim is a delivery's own deferred work: it runs
				// after the exchange ends and rearms until every exclusion is lifted.
				sess.mu.Lock()
				reclaiming := reclaim && sess.exclReclaimArmed
				sess.mu.Unlock()
				if reclaiming || !testCustodySettled(sess) {
					ok = false
					break
				}
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			var why []string
			asm.mu.RLock()
			for id, sess := range asm.sessions {
				sess.mu.Lock()
				r := sess.exclReclaimArmed
				sess.mu.Unlock()
				why = append(why, fmt.Sprintf("%s reclaim=%v custody=%v", id, r, testCustodySettled(sess)))
			}
			asm.mu.RUnlock()
			t.Fatalf("the operation's workers did not settle within 40s (settlements running: %d): %v", jsPathSettling.Load(), why)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// jsPathSettledNow is jsPathSettleWorkers' predicate, read once.
func (b *jsPathBench) jsPathSettledNow(reclaim bool) bool {
	if jsPathSettling.Load() != 0 {
		return false
	}
	asm := b.rig.asm(b.t)
	asm.mu.RLock()
	sessions := make([]*mqttSession, 0, len(asm.sessions))
	for _, sess := range asm.sessions {
		sessions = append(sessions, sess)
	}
	asm.mu.RUnlock()
	for _, sess := range sessions {
		sess.mu.Lock()
		reclaiming := reclaim && sess.exclReclaimArmed
		sess.mu.Unlock()
		if reclaiming || !testCustodySettled(sess) {
			return false
		}
	}
	return true
}

// apiTotal reads the account's JetStream API counter, settled: in a cluster each server's count
// reaches the account by rate-limited usage updates, so the value is taken only once it has held
// across samples spanning three usage ticks (bounded). It returns the reading, how many API
// requests the readings themselves added, and whether it settled.
func (b *jsPathBench) apiTotal(t *testing.T) (uint64, int, bool) {
	t.Helper()
	ai, err := b.rig.js.AccountInfo()
	if err != nil {
		t.Fatalf("account info: %v", err)
	}
	if !b.rig.r3 {
		return ai.API.Total, 1, true
	}
	polls := 1
	last, since := ai.API.Total, time.Now()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		ai, err = b.rig.js.AccountInfo()
		if err != nil {
			t.Fatalf("account info: %v", err)
		}
		polls++
		// Each reading adds one request; a value that moved by exactly the readings is unchanged.
		if ai.API.Total == last+1 {
			last = ai.API.Total
			if time.Since(since) >= 4500*time.Millisecond {
				return last, polls, true
			}
			continue
		}
		last, since = ai.API.Total, time.Now()
	}
	return last, polls, false
}

// jsPathRow is one measured window.
type jsPathRow struct {
	rig, load  string
	ops        int
	snap       jsCensusSnapshot
	wall       time.Duration
	api        int64
	apiSettled bool
	inMsgs     int64
	sends      int
	maxDepth   int
	latP50     time.Duration
	latP99     time.Duration
	classes    map[string]*jsPathClassStats
	observed   bool
	invalid    string
}

// jsPathMeasureWindow runs body as one window of ops operations, then waits for wait (the load's own
// completion condition) and the workers, closes the census window and runs the quiet check.
// observe is false only where the broker's request observer cannot be attached before the work
// (a broker start).
func (b *jsPathBench) jsPathMeasureWindow(t *testing.T, load string, ops int, observe bool, body func(), wait func()) jsPathRow {
	t.Helper()
	row := jsPathRow{rig: b.rig.name, load: load, ops: ops, observed: observe}
	var o *testJSAObserver
	var m int
	if observe {
		o = testObserveJSA(t, &b.rig.asm(t).jsa)
	}
	b.cs.reset(t, b.rig.s.nrc.Flush)
	api0, _, settled0 := b.apiTotal(t)
	in0 := b.rig.inMsgs(t)
	if o != nil {
		m = o.mark()
	}
	start := time.Now()
	body()
	row.wall = time.Since(start)
	if wait != nil {
		wait()
	}
	if observe {
		// THE REMAINING ASYNCHRONOUS WORK. Beyond the workers settle names, an operation leaves
		// timers and continuations (terminal-source settlement, register reaps, reconciles) whose
		// end has no seam — most of them untracked timers at mqttActionReconcileDelay — and some of
		// them arm the workers settle waits for (an exchange that ends late installs an exclusion
		// and arms its reclaim). So the two waits alternate until they hold together: the workers
		// settled, then the sender idle for longer than that delay with nothing outstanding, then
		// the workers still settled. The quiet check guards the rest.
		for i := 0; ; i++ {
			b.jsPathSettleWorkers(t, true)
			jsPathWaitObserverIdle(t, o, jsPathIdle, 30*time.Second)
			if b.jsPathSettledNow(true) {
				break
			}
			if i == 20 {
				t.Fatal("the operation's workers kept re-arming: no quiescent point in 20 rounds")
			}
		}
	} else {
		time.Sleep(250 * time.Millisecond)
	}
	row.snap = b.cs.snapshot(t, b.rig.s.nrc.Flush)
	end := 0
	if o != nil {
		end = o.mark()
	}
	time.Sleep(jsPathBenchQuiet)
	quiet := b.cs.snapshot(t, b.rig.s.nrc.Flush)
	qn, _ := quiet.total("")
	sn, _ := row.snap.total("")
	tq, _ := quiet.total("test:")
	ts, _ := row.snap.total("test:")
	if qn-tq != sn-ts {
		var late []string
		for _, c := range quiet.classes() {
			if d := quiet.counts[c] - row.snap.counts[c]; d != 0 && !strings.HasPrefix(c, "test:") {
				late = append(late, fmt.Sprintf("%s+%d", c, d))
			}
		}
		row.invalid = fmt.Sprintf("the quiet check saw %d broker messages after the window closed: %v", (qn-tq)-(sn-ts), late)
	}
	if o != nil {
		if late := o.sentSubjects(end, func(string) bool { return true }); len(late) != 0 {
			row.invalid = fmt.Sprintf("the observer saw broker requests after the window closed: %v", late)
		}
	}
	for class, n := range row.snap.unanswered {
		if strings.HasPrefix(class, "api:") && n > 0 {
			row.invalid = fmt.Sprintf("%d %s requests were never answered", n, class)
		}
	}
	row.inMsgs = b.rig.inMsgs(t) - in0
	api1, polls1, settled1 := b.apiTotal(t)
	// The counter's delta less the readings' own requests. It is NOT the census's request count:
	// the server answers a successful message GET outside its API layer and never counts it
	// (nats-server jetstream_api.go, jsMsgGetRequest: "Don't send response through API layer for
	// this call"), so it counts every other request and every failed message GET; and the test's
	// own requests it counts are included.
	// Between the two returned readings the readings themselves made polls1 requests: the
	// baseline's last reading (which its own value does not include) and every after-phase
	// reading but the last. The baseline's earlier polls are already inside api0.
	row.api = int64(api1) - int64(api0) - int64(polls1)
	row.apiSettled = settled0 && settled1
	if o != nil {
		evs, ats := o.sinceAt(m, end)
		row.sends, row.maxDepth, row.latP50, row.latP99 = jsPathObserverStats(evs, ats)
		row.classes = jsPathObserverClassStats(o, evs, ats)
	}
	return row
}

// jsPathWaitObserverIdle waits until the observer has recorded no event for idle and no
// reply-bearing request is outstanding.
func jsPathWaitObserverIdle(t *testing.T, o *testJSAObserver, idle, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		o.mu.Lock()
		n := len(o.events)
		var last time.Time
		if n > 0 {
			last = o.at[n-1]
		}
		outstanding := 0
		seen := map[string]bool{}
		for _, ev := range o.events {
			switch ev.Kind {
			case mqttJSARequestSent:
				if ev.Reply != _EMPTY_ {
					seen[ev.Reply] = true
					outstanding++
				}
			case mqttJSAReplyDelivered:
				if seen[ev.Reply] {
					delete(seen, ev.Reply)
					outstanding--
				}
			}
		}
		o.mu.Unlock()
		if outstanding == 0 && (n == 0 || time.Since(last) >= idle) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the broker's sender did not go idle within %v (%d outstanding)", bound, outstanding)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// jsPathClassStats is one request class's latency and concurrency within a window.
type jsPathClassStats struct {
	sends    int
	maxDepth int
	p50, p99 time.Duration
}

// jsPathObserverClassStats derives, per request class (API verb and stream, from the request's
// subject), the requests sent, the most of that class outstanding at once, and send-to-delivery
// latency. Per class rather than per window: a window's overall maximum is set by whichever
// round is widest, which says nothing about the class a change targets.
func jsPathObserverClassStats(o *testJSAObserver, evs []mqttJSARequestEvent, ats []time.Time) map[string]*jsPathClassStats {
	type pending struct {
		class string
		at    time.Time
	}
	open := map[string]pending{}
	depth := map[string]int{}
	lat := map[string][]time.Duration{}
	out := map[string]*jsPathClassStats{}
	for i, ev := range evs {
		switch ev.Kind {
		case mqttJSARequestSent:
			if ev.Reply == _EMPTY_ {
				continue
			}
			class := jsCensusAPIClass(o.apiOf(ev.Reply))
			if out[class] == nil {
				out[class] = &jsPathClassStats{}
			}
			out[class].sends++
			open[ev.Reply] = pending{class, ats[i]}
			depth[class]++
			out[class].maxDepth = max(out[class].maxDepth, depth[class])
		case mqttJSAReplyDelivered:
			if p, ok := open[ev.Reply]; ok {
				delete(open, ev.Reply)
				depth[p.class]--
				lat[p.class] = append(lat[p.class], ats[i].Sub(p.at))
			}
		}
	}
	for class, l := range lat {
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		out[class].p50, out[class].p99 = l[len(l)/2], l[len(l)*99/100]
	}
	return out
}

// jsPathObserverStats derives, from a window's observer events, the reply-bearing requests sent,
// the most outstanding at once, and send-to-delivery latency.
func jsPathObserverStats(evs []mqttJSARequestEvent, ats []time.Time) (sends, maxDepth int, p50, p99 time.Duration) {
	sent := map[string]time.Time{}
	var lat []time.Duration
	depth := 0
	at := func(i int) time.Time { return ats[i] }
	for i, ev := range evs {
		switch ev.Kind {
		case mqttJSARequestSent:
			if ev.Reply == _EMPTY_ {
				continue
			}
			sends++
			sent[ev.Reply] = at(i)
			depth++
			maxDepth = max(maxDepth, depth)
		case mqttJSAReplyDelivered:
			if t0, ok := sent[ev.Reply]; ok {
				delete(sent, ev.Reply)
				depth--
				lat = append(lat, at(i).Sub(t0))
			}
		}
	}
	if len(lat) == 0 {
		return sends, maxDepth, 0, 0
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return sends, maxDepth, lat[len(lat)/2], lat[len(lat)*99/100]
}

// report logs a row and appends it to the CSV files, when named.
func (b *jsPathBench) report(t *testing.T, row jsPathRow) {
	t.Helper()
	label := os.Getenv(jsPathBenchLabelEnv)
	reqs, bytes := row.snap.total("api:")
	pubs, pubBytes := row.snap.total("pub:")
	acks, _ := row.snap.total("ack:")
	replies, replyBytes := row.snap.total("reply:")
	per := func(n int64) float64 { return float64(n) / float64(row.ops) }
	valid := row.invalid == _EMPTY_
	t.Logf("%s/%s: %d ops, %.1f ms/op; per op: %.2f API requests (%.0f B), %.2f stores/batch members (%.0f B), %.2f acks, %.2f replies (%.0f B); "+
		"server API %.2f/op (settled %v); observer: %d sends, max %d outstanding, send-to-delivery p50 %v p99 %v; valid %v %s\n%s%s",
		row.rig, row.load, row.ops, float64(row.wall.Microseconds())/1000/float64(row.ops),
		per(reqs), float64(bytes)/float64(row.ops), per(pubs), float64(pubBytes)/float64(row.ops), per(acks),
		per(replies), float64(replyBytes)/float64(row.ops), per(row.api), row.apiSettled, row.sends, row.maxDepth,
		row.latP50, row.latP99, valid, row.invalid, jsCensusReport(row.snap, row.ops), jsPathClassReport(row))
	if !valid {
		t.Errorf("%s/%s: the row is not valid: %s", row.rig, row.load, row.invalid)
	}
	out := os.Getenv(jsPathBenchOutEnv)
	if out == _EMPTY_ {
		return
	}
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("bench output: %v", err)
	}
	defer f.Close()
	fmt.Fprintf(f, "%s,%s,%s,%d,%.3f,%.3f,%.1f,%.3f,%.1f,%.3f,%.3f,%.1f,%.3f,%v,%d,%d,%.3f,%v\n",
		label, row.rig, row.load, row.ops, float64(row.wall.Microseconds())/1000/float64(row.ops),
		per(reqs), float64(bytes)/float64(row.ops), per(pubs), float64(pubBytes)/float64(row.ops), per(acks),
		per(replies), float64(replyBytes)/float64(row.ops), per(row.api), row.apiSettled, row.sends, row.maxDepth,
		per(row.inMsgs), valid)
	lf, err := os.OpenFile(out+".latency", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("bench latency output: %v", err)
	}
	defer lf.Close()
	for _, c := range jsPathClassNames(row) {
		s := row.classes[c]
		fmt.Fprintf(lf, "%s,%s,%s,%s,%d,%d,%.1f,%.1f\n", label, row.rig, row.load, c, s.sends, s.maxDepth,
			float64(s.p50.Microseconds()), float64(s.p99.Microseconds()))
	}
	cf, err := os.OpenFile(out+".census", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("bench census output: %v", err)
	}
	defer cf.Close()
	for _, c := range row.snap.classes() {
		fmt.Fprintf(cf, "%s,%s,%s,%s,%d,%d,%.3f\n", label, row.rig, row.load, c, row.snap.counts[c], row.snap.bytes[c],
			float64(row.snap.counts[c])/float64(row.ops))
	}
}

func jsPathClassNames(row jsPathRow) []string {
	var out []string
	for c := range row.classes {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func jsPathClassReport(row jsPathRow) string {
	var b strings.Builder
	for _, c := range jsPathClassNames(row) {
		s := row.classes[c]
		fmt.Fprintf(&b, "    latency %-44s %6d sends, max %3d outstanding, p50 %8v p99 %8v\n", c, s.sends, s.maxDepth, s.p50, s.p99)
	}
	return b.String()
}

// --- MQTT client helpers for the loads ---

type jsPathClient struct {
	t  *testing.T
	id string
	c  net.Conn
	r  *mqttReader
	pi uint16
}

func (b *jsPathBench) connect(t *testing.T, id string, clean bool, expiry uint32, rmax uint16) (*jsPathClient, bool) {
	t.Helper()
	props := mqttV5ConnPropsSessionExpiry(expiry)
	if rmax > 0 {
		props = mqttV5PropsSessionExpiryReceiveMax(expiry, rmax)
	}
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: clean, props: props}, b.rig.host, b.rig.port)
	present, rc, _ := testMQTTReadConnAckV5(t, r)
	if rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNECT %s refused: %d", id, rc)
	}
	return &jsPathClient{t: t, id: id, c: c, r: r}, present
}

func (c *jsPathClient) nextPI() uint16 {
	c.pi = c.pi%65535 + 1
	return c.pi
}

func (c *jsPathClient) fence() { testMQTTFlush(c.t, c.c, nil, c.r) }

func (c *jsPathClient) subscribe(topic string, qos byte) {
	testMQTTSubV5(c.t, c.c, c.r, c.nextPI(), []mqttV5SubFilter{{topic: topic, opts: qos}})
}

func (c *jsPathClient) disconnect() {
	if _, err := c.c.Write([]byte{mqttPacketDisconnect, 0}); err != nil {
		c.t.Fatalf("DISCONNECT: %v", err)
	}
	c.c.Close()
}

// publish1 sends a QoS 1 PUBLISH and reads its PUBACK.
func (c *jsPathClient) publish1(topic string, retain bool, payload []byte) {
	pi := c.nextPI()
	testMQTTSendPubV5Raw(c.t, c.c, 1, pi, retain, topic, nil, payload)
	testMQTTReadPubAckV5(c.t, c.r, pi)
}

// publish2 sends a QoS 2 PUBLISH and completes its exchange: PUBREC, PUBREL, PUBCOMP.
func (c *jsPathClient) publish2(topic string, payload []byte) {
	pi := c.nextPI()
	testMQTTSendPubV5Raw(c.t, c.c, 2, pi, false, topic, nil, payload)
	testMQTTReadPubRecV5(c.t, c.r, pi)
	if _, err := c.c.Write([]byte{mqttPacketPubRel | 0x2, 0x2, byte(pi >> 8), byte(pi)}); err != nil {
		c.t.Fatalf("PUBREL: %v", err)
	}
	testMQTTReadPubCompV5(c.t, c.r, pi)
}

// publish sends a PUBLISH at qos and completes its exchange.
func (c *jsPathClient) publish(qos byte, topic string, payload []byte) {
	if qos == 2 {
		c.publish2(topic, payload)
		return
	}
	c.publish1(topic, false, payload)
}

// receive reads one PUBLISH and completes its exchange as a client would.
func (c *jsPathClient) receive() {
	b, pl := testMQTTReadPacket(c.t, c.r)
	if b&mqttPacketMask != mqttPacketPub {
		c.t.Fatalf("expected PUBLISH, got %x", b)
	}
	qos := mqttGetQoS(b & mqttPacketFlagMask)
	start := c.r.pos
	if _, err := c.r.readBytes("topic", false); err != nil {
		c.t.Fatalf("topic: %v", err)
	}
	var pi uint16
	if qos > 0 {
		pi, _ = c.r.readUint16("pi")
	}
	c.r.pos = start + pl
	switch qos {
	case 1:
		testMQTTSendPubAck(c.t, c.c, pi)
	case 2:
		testMQTTSendPIPacket(mqttPacketPubRec, c.t, c.c, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, c.t, c.r, pi)
		testMQTTSendPIPacket(mqttPacketPubComp, c.t, c.c, pi)
	}
}

// readPublishPI reads one PUBLISH and returns its identifier, acknowledging nothing.
func (c *jsPathClient) readPublishPI() (byte, uint16) {
	b, pl := testMQTTReadPacket(c.t, c.r)
	if b&mqttPacketMask != mqttPacketPub {
		c.t.Fatalf("expected PUBLISH, got %x", b)
	}
	qos := mqttGetQoS(b & mqttPacketFlagMask)
	start := c.r.pos
	if _, err := c.r.readBytes("topic", false); err != nil {
		c.t.Fatalf("topic: %v", err)
	}
	var pi uint16
	if qos > 0 {
		pi, _ = c.r.readUint16("pi")
	}
	c.r.pos = start + pl
	return qos, pi
}

// disconnectedAt waits for a session record to record its disconnection, read on the test's own
// JetStream connection (classed test: by the census).
func (b *jsPathBench) waitDisconnectedAt(t *testing.T, id string) {
	t.Helper()
	asm := b.rig.asm(t)
	subj := mqttSessStreamSubjectPrefix + asm.domainTk + getHash(id)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if m, err := b.rig.js.GetLastMsg(mqttSessStreamName, subj); err == nil {
			var ps mqttPersistedSession
			if json.Unmarshal(m.Data, &ps) == nil && ps.DisconnectedAt != 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the session record of %s never recorded the disconnection", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- the loads ---

type jsPathLoad struct {
	name string
	run  func(t *testing.T, b *jsPathBench)
}

const jsPathOps = 20

var jsPathLoads = []jsPathLoad{
	// The accounting's own check: a window with no operation must read zero broker requests at
	// the census and zero adjusted requests at the server's counter, or the other rows' API
	// column carries a measurement artifact.
	{"idle", func(t *testing.T, b *jsPathBench) {
		row := b.jsPathMeasureWindow(t, "idle", 1, true, func() {}, nil)
		b.report(t, row)
		if n, _ := row.snap.total("api:"); n != 0 || row.api != 0 || !row.apiSettled {
			t.Errorf("an idle window read %d broker requests and %d adjusted server API requests (settled %v), want 0 and 0",
				n, row.api, row.apiSettled)
		}
	}},
	{"connect+disconnect", func(t *testing.T, b *jsPathBench) {
		var clients []*jsPathClient
		ids := make([]string, jsPathOps)
		for i := range ids {
			ids[i] = "bench-connect-" + nuid.Next()
		}
		b.report(t, b.jsPathMeasureWindow(t, "connect", jsPathOps, true, func() {
			for _, id := range ids {
				c, _ := b.connect(t, id, true, 300, 0)
				c.fence()
				clients = append(clients, c)
			}
		}, nil))
		b.report(t, b.jsPathMeasureWindow(t, "disconnect", jsPathOps, true, func() {
			for _, c := range clients {
				c.disconnect()
			}
		}, func() {
			for _, id := range ids {
				b.waitDisconnectedAt(t, id)
			}
		}))
	}},
	{"subscribe", func(t *testing.T, b *jsPathBench) {
		c, _ := b.connect(t, "bench-sub-"+nuid.Next(), true, 300, 0)
		for _, qos := range []byte{1, 2} {
			b.report(t, b.jsPathMeasureWindow(t, fmt.Sprintf("subscribe-q%d", qos), jsPathOps, true, func() {
				for i := 0; i < jsPathOps; i++ {
					c.subscribe(fmt.Sprintf("bench/sub/q%d/%d", qos, i), qos)
					c.fence()
				}
			}, nil))
		}
	}},
	{"publish", func(t *testing.T, b *jsPathBench) {
		for _, qos := range []byte{1, 2} {
			topic := fmt.Sprintf("bench/pub/q%d", qos)
			sub, _ := b.connect(t, "bench-pubsub-"+nuid.Next(), true, 300, 0)
			sub.subscribe(topic, qos)
			sub.fence()
			pub, _ := b.connect(t, "bench-pub-"+nuid.Next(), true, 0, 0)
			b.jsPathSettle(t)
			b.report(t, b.jsPathMeasureWindow(t, fmt.Sprintf("publish-q%d", qos), jsPathOps, true, func() {
				for i := 0; i < jsPathOps; i++ {
					pub.publish(qos, topic, []byte("payload"))
					sub.receive()
					sub.fence()
					pub.fence()
				}
			}, nil))
		}
	}},
	{"publish-window", func(t *testing.T, b *jsPathBench) {
		const window = 32
		topic := "bench/window"
		sub, _ := b.connect(t, "bench-winsub-"+nuid.Next(), true, 300, 64)
		sub.subscribe(topic, 1)
		sub.fence()
		pub, _ := b.connect(t, "bench-winpub-"+nuid.Next(), true, 0, 0)
		b.jsPathSettle(t)
		b.report(t, b.jsPathMeasureWindow(t, "publish-window", window, true, func() {
			for i := 0; i < window; i++ {
				pub.publish1(topic, false, []byte("payload"))
			}
			var pis []uint16
			for i := 0; i < window; i++ {
				_, pi := sub.readPublishPI()
				pis = append(pis, pi)
			}
			for _, pi := range pis {
				testMQTTSendPubAck(t, sub.c, pi)
			}
			sub.fence()
			pub.fence()
		}, nil))
	}},
	{"reconnect", func(t *testing.T, b *jsPathBench) {
		id := "bench-reconnect-" + nuid.Next()
		c, _ := b.connect(t, id, true, 3600, 0)
		for i := 0; i < 4; i++ {
			c.subscribe(fmt.Sprintf("bench/reconnect/%d", i), 1)
		}
		c.fence()
		c.disconnect()
		b.waitDisconnectedAt(t, id)
		b.jsPathSettle(t)
		for i := 0; i < jsPathOps; i++ {
			// Each round measures the resume alone; its DISCONNECT is settled outside the window.
			var c *jsPathClient
			b.reportAcc(t, "reconnect", i, b.jsPathMeasureWindow(t, "reconnect", 1, true, func() {
				var present bool
				c, present = b.connect(t, id, false, 3600, 0)
				if !present {
					t.Fatal("a persistent session's resume reported no session")
				}
				c.fence()
			}, nil))
			c.disconnect()
			b.waitDisconnectedAt(t, id)
			b.jsPathSettle(t)
		}
		b.flushAcc(t)
	}},
	{"retained", func(t *testing.T, b *jsPathBench) {
		pub, _ := b.connect(t, "bench-retpub-"+nuid.Next(), true, 0, 0)
		b.report(t, b.jsPathMeasureWindow(t, "retained-store", jsPathOps, true, func() {
			for i := 0; i < jsPathOps; i++ {
				pub.publish1(fmt.Sprintf("bench/retained/%d", i), true, []byte("retained"))
			}
			pub.fence()
		}, nil))
		var subs []*jsPathClient
		b.report(t, b.jsPathMeasureWindow(t, "retained-replay", jsPathOps, true, func() {
			for i := 0; i < jsPathOps; i++ {
				c, _ := b.connect(t, "bench-retsub-"+nuid.Next(), true, 0, 0)
				c.subscribe(fmt.Sprintf("bench/retained/%d", i), 1)
				c.receive()
				c.fence()
				subs = append(subs, c)
			}
		}, nil))
		for _, c := range subs {
			c.disconnect()
		}
	}},
	{"session-recovery", func(t *testing.T, b *jsPathBench) {
		for _, cold := range []bool{false, true} {
			load := "session-recovery"
			if cold {
				load = "session-recovery-cold"
			}
			const rounds = 5
			for round := 0; round < rounds; round++ {
				id := "bench-recover-" + nuid.Next()
				// Topics of this round's own, so no earlier round's session or publisher reaches it.
				q1, q2 := "bench/recover/"+id+"/q1", "bench/recover/"+id+"/q2"
				sub, _ := b.connect(t, id, true, 3600, 0)
				sub.subscribe(q1, 1)
				sub.subscribe(q2, 2)
				sub.fence()
				pub, _ := b.connect(t, "bench-recpub-"+nuid.Next(), true, 0, 0)
				// Two QoS 2 deliveries answered with PUBREC before the subscriber goes: PUBRELs owed.
				for i := 0; i < 2; i++ {
					pub.publish2(q2, []byte("owed"))
				}
				owed := 0
				for owed < 2 {
					bt, pl := testMQTTReadPacket(t, sub.r)
					switch bt & mqttPacketMask {
					case mqttPacketPub:
						start := sub.r.pos
						_, _ = sub.r.readBytes("topic", false)
						pi, _ := sub.r.readUint16("pi")
						sub.r.pos = start + pl
						testMQTTSendPIPacket(mqttPacketPubRec, t, sub.c, pi)
					case mqttPacketPubRel:
						sub.r.pos += pl
						owed++
					default:
						t.Fatalf("unexpected packet %x while owing PUBRELs", bt)
					}
				}
				sub.c.Close() // gone without a DISCONNECT
				b.waitDisconnectedAt(t, id)
				for i := 0; i < 8; i++ {
					pub.publish1(q1, false, []byte("offline"))
				}
				for i := 0; i < 2; i++ {
					pub.publish2(q2, []byte("offline"))
				}
				pub.fence()
				pub.disconnect()
				if cold {
					b.rig.restart(t)
					b.restartCensus(t)
				}
				b.jsPathSettle(t)
				var c *jsPathClient
				b.reportAcc(t, load, round, b.jsPathMeasureWindow(t, load, 1, true, func() {
					var present bool
					c, present = b.connect(t, id, false, 3600, 0)
					if !present {
						t.Fatal("the recovering session was not present")
					}
					// 8 QoS 1 and 2 QoS 2 redeliveries, and the 2 owed PUBRELs, in whatever order
					// the broker interleaves them: 8 PUBACKs and 4 PUBCOMPs complete the recovery.
					pubacks, pubcomps := 0, 0
					for pubacks < 8 || pubcomps < 4 {
						bt, pl := testMQTTReadPacket(t, c.r)
						switch bt & mqttPacketMask {
						case mqttPacketPub:
							qos := mqttGetQoS(bt & mqttPacketFlagMask)
							start := c.r.pos
							_, _ = c.r.readBytes("topic", false)
							pi, _ := c.r.readUint16("pi")
							c.r.pos = start + pl
							if qos == 1 {
								testMQTTSendPubAck(t, c.c, pi)
								pubacks++
							} else {
								testMQTTSendPIPacket(mqttPacketPubRec, t, c.c, pi)
							}
						case mqttPacketPubRel:
							start := c.r.pos
							pi, _ := c.r.readUint16("pi")
							c.r.pos = start + pl
							testMQTTSendPIPacket(mqttPacketPubComp, t, c.c, pi)
							pubcomps++
						case mqttPacketDisconnect:
							reason := byte(0)
							if pl > 0 {
								reason = c.r.buf[c.r.pos]
							}
							t.Fatalf("the broker sent DISCONNECT (reason 0x%02x) during recovery after %d PUBACKs and %d PUBCOMPs",
								reason, pubacks, pubcomps)
						default:
							t.Fatalf("unexpected packet %x during recovery", bt)
						}
					}
					// Drain to a PINGRESP, answering anything owed that arrives after the counted
					// completions (a PUBREL re-offered by its own timer is legal and is answered).
					if _, err := c.c.Write([]byte{mqttPacketPing, 0}); err != nil {
						t.Fatalf("PINGREQ: %v", err)
					}
					for {
						bt, pl := testMQTTReadPacket(t, c.r)
						if bt&mqttPacketMask == mqttPacketPingResp {
							break
						}
						if bt&mqttPacketMask == mqttPacketPubRel {
							start := c.r.pos
							pi, _ := c.r.readUint16("pi")
							c.r.pos = start + pl
							testMQTTSendPIPacket(mqttPacketPubComp, t, c.c, pi)
							t.Logf("recovery round %d: a further PUBREL for %d after the counted completions", round, pi)
							continue
						}
						t.Fatalf("unexpected packet %x after recovery", bt)
					}
				}, nil))
				c.disconnect()
			}
			b.flushAcc(t)
		}
	}},
	{"sweep-tick", func(t *testing.T, b *jsPathBench) {
		const sessions = 50
		for i := 0; i < sessions; i++ {
			id := fmt.Sprintf("bench-sweep-%d-%s", i, nuid.Next())
			c, _ := b.connect(t, id, true, 3600, 0)
			c.subscribe("bench/sweep/"+id, 1)
			c.fence()
			c.disconnect()
			b.waitDisconnectedAt(t, id)
		}
		b.jsPathSettle(t)
		asm := b.rig.asm(t)
		s := b.rig.s
		b.report(t, b.jsPathMeasureWindow(t, "sweep-tick", sessions, true, func() {
			// The ticker arm's seven calls, in its order (sess_manager.go).
			asm.reconcileResidentRecordState(s)
			asm.sweepSessionExpiries(s)
			asm.sweepOrphanedConsumers(s)
			asm.sweepSharedGroupRetirements(s)
			asm.sweepAcqReleaseVerdicts(s)
			asm.sweepOrphanedQoS2State(s)
			asm.sweepOutboundRecovery(s, func() bool { return false })
		}, nil))
	}},
	{"startup", func(t *testing.T, b *jsPathBench) {
		for i := 0; i < 3; i++ {
			// restart returns only once the start's sweep pass and initial Will scan completed.
			b.reportAcc(t, "startup", i, b.jsPathMeasureWindow(t, "startup", 1, false, func() {
				b.rig.restart(t)
			}, func() { b.jsPathSettle(t) }))
			b.restartCensus(t)
		}
		b.flushAcc(t)
	}},
}

// restartCensus re-attaches the census after a broker restart moved the service connection.
func (b *jsPathBench) restartCensus(t *testing.T) {
	t.Helper()
	if b.cs.nc.ConnectedUrl() == b.rig.tapURL {
		return
	}
	b.cs.nc.Close()
	b.cs = startJSRequestCensus(t, b.rig.tapURL, []string{"$JS.API.>", mqttPrefix + ">", "$JS.ACK.>"}, jsCensusBrokerClass, nil)
}

// Rows of a load measured as several one-operation windows are accumulated into one row.
var jsPathAcc struct {
	mu  sync.Mutex
	row *jsPathRow
}

func (b *jsPathBench) reportAcc(t *testing.T, load string, i int, row jsPathRow) {
	t.Helper()
	jsPathAcc.mu.Lock()
	defer jsPathAcc.mu.Unlock()
	a := jsPathAcc.row
	if a == nil || a.load != load {
		r := row
		r.snap = jsCensusSnapshot{counts: map[string]int64{}, bytes: map[string]int64{}, unanswered: map[string]int64{}}
		r.ops, r.wall, r.api, r.inMsgs, r.sends, r.maxDepth, r.latP50, r.latP99 = 0, 0, 0, 0, 0, 0, 0, 0
		r.apiSettled = true
		jsPathAcc.row = &r
		a = jsPathAcc.row
	}
	a.ops += row.ops
	a.wall += row.wall
	a.api += row.api
	a.apiSettled = a.apiSettled && row.apiSettled
	a.inMsgs += row.inMsgs
	a.sends += row.sends
	a.maxDepth = max(a.maxDepth, row.maxDepth)
	a.latP50 = max(a.latP50, row.latP50) // the worst round's percentiles: descriptive
	a.latP99 = max(a.latP99, row.latP99)
	if a.classes == nil {
		a.classes = map[string]*jsPathClassStats{}
	}
	for c, s := range row.classes {
		x := a.classes[c]
		if x == nil {
			x = &jsPathClassStats{}
			a.classes[c] = x
		}
		x.sends += s.sends
		x.maxDepth = max(x.maxDepth, s.maxDepth)
		x.p50, x.p99 = max(x.p50, s.p50), max(x.p99, s.p99)
	}
	for k, v := range row.snap.counts {
		a.snap.counts[k] += v
		a.snap.bytes[k] += row.snap.bytes[k]
	}
	for k, v := range row.snap.unanswered {
		a.snap.unanswered[k] += v
	}
	if row.invalid != _EMPTY_ {
		a.invalid = fmt.Sprintf("round %d: %s", i, row.invalid)
	}
}

func (b *jsPathBench) flushAcc(t *testing.T) {
	t.Helper()
	jsPathAcc.mu.Lock()
	a := jsPathAcc.row
	jsPathAcc.row = nil
	jsPathAcc.mu.Unlock()
	if a != nil {
		b.report(t, *a)
	}
}

// jsPathRefusalCosts measures, on whichever tree runs it, how many reads an admission's
// readiness makes for each class of refusal: the "today" and "with the single ownership read"
// columns of the readiness cost table come from running this on both arms. It uses only what both
// trees have.
func jsPathRefusalCosts(t *testing.T) {
	type fixture struct {
		e         *testInflightEnv
		o         *testJSAObserver
		dur       string
		epoch     string
		gen       string
		srcInc    string
		readiness map[string]bool
	}
	setup := func(t *testing.T, qos byte) *fixture {
		e := testNewInflightEnv(t, "bench-refusal-"+nuid.Next())
		f := &fixture{e: e, dur: e.sess.idHash + "_" + nuid.Next(), readiness: map[string]bool{}}
		e.admissionPrerequisites(t, f.dur, qos)
		e.acceptUnderServingAuthority(t)
		ent, _ := e.c.outboundEntitlement()
		f.epoch, f.gen = e.asm.sessStreamEpoch, ent.Gen
		add := func(owned mqttOwnedStream, stream, dur string) string {
			own, _, err := e.sess.jsa.loadNameOwnershipPhysical(f.epoch, owned, dur)
			if err != nil || own == nil {
				t.Fatalf("fixture: ownership of %q: %v", dur, err)
			}
			f.readiness[mqttNameOwnershipSubject(owned, dur)] = true
			f.readiness[JSApiPrefix+".STREAM.INFO."+stream] = true
			f.readiness[JSApiPrefix+".CONSUMER.INFO."+stream+"."+dur] = true
			f.readiness[mqttResourceSubject(e.sess.idHash, f.gen, own.OwnerInc)] = true
			return own.OwnerInc
		}
		f.srcInc = add(mqttOwnedStreamMsgs, mqttStreamName, f.dur)
		if qos >= 2 {
			_, _, rel := mqttPubRelNames(e.sess.idHash, f.gen)
			add(mqttOwnedStreamOut, mqttOutStreamName, rel)
		}
		f.o = testObserveJSA(t, &e.asm.jsa)
		return f
	}
	ownSubj := func(f *fixture) string { return mqttNameOwnershipSubject(mqttOwnedStreamMsgs, f.dur) }
	resSubj := func(f *fixture) string { return mqttResourceSubject(f.e.sess.idHash, f.gen, f.srcInc) }
	del := func(subj func(*fixture) string) func(*testing.T, *fixture) {
		return func(t *testing.T, f *fixture) {
			smsg, err := f.e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj(f))
			if err != nil {
				t.Fatalf("fixture: %v", err)
			}
			if err := f.e.asm.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); err != nil {
				t.Fatalf("fixture: %v", err)
			}
		}
	}
	rewriteOwn := func(change func(o *mqttNameOwnership)) func(*testing.T, *fixture) {
		return func(t *testing.T, f *fixture) {
			o, _, err := f.e.asm.jsa.loadNameOwnershipPhysical(f.epoch, mqttOwnedStreamMsgs, f.dur)
			if err != nil || o == nil {
				t.Fatalf("fixture: %v", err)
			}
			change(o)
			b, _ := json.Marshal(o)
			if _, err := f.e.asm.jsa.storeMsg(ownSubj(f), -1, b); err != nil {
				t.Fatalf("fixture: %v", err)
			}
		}
	}
	for _, tc := range []struct {
		name   string
		qos    byte
		mutate func(*testing.T, *fixture)
	}{
		{"proceeding", 1, nil},
		{"ownership-absent", 1, del(ownSubj)},
		{"ownership-foreign-client", 1, rewriteOwn(func(o *mqttNameOwnership) { o.ID = "client-other" })},
		{"ownership-retired", 1, rewriteOwn(func(o *mqttNameOwnership) { o.State = mqttNameOwnRetired })},
		{"resource-absent", 1, del(resSubj)},
		{"resource-other-consumer", 1, func(t *testing.T, f *fixture) {
			r, _, err := f.e.asm.jsa.loadOutboundResource(f.e.sess.id, f.epoch, f.gen, f.srcInc)
			if err != nil || r == nil {
				t.Fatalf("fixture: %v", err)
			}
			r.Name = f.e.sess.idHash + "_" + nuid.Next()
			// A second, client-checked ownership read follows the resource's NAME, so it reads
			// the replacement name's subject: that read is the readiness's too.
			f.readiness[mqttNameOwnershipSubject(mqttOwnedStreamMsgs, r.Name)] = true
			b, _ := json.Marshal(r)
			if _, err := f.e.asm.jsa.storeMsg(resSubj(f), -1, b); err != nil {
				t.Fatalf("fixture: %v", err)
			}
		}},
		{"stream-inactivity-limit", 1, func(t *testing.T, f *fixture) {
			js, err := f.e.s.nrc.JetStream()
			if err != nil {
				t.Fatalf("fixture: %v", err)
			}
			si, err := js.StreamInfo(mqttStreamName)
			if err != nil {
				t.Fatalf("fixture: %v", err)
			}
			cfg := si.Config
			cfg.ConsumerLimits.InactiveThreshold = time.Hour
			if _, err := js.UpdateStream(&cfg); err != nil {
				t.Fatalf("fixture: %v", err)
			}
		}},
		{"consumer-absent", 1, func(t *testing.T, f *fixture) {
			if _, err := f.e.asm.jsa.deleteConsumer(mqttStreamName, f.dur, false); err != nil {
				t.Fatalf("fixture: %v", err)
			}
		}},
		{"qos2-proceeding", 2, nil},
		{"qos2-source-ownership-absent", 2, del(ownSubj)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup(t, tc.qos)
			if tc.mutate != nil {
				tc.mutate(t, f)
			}
			m := f.o.mark()
			start := time.Now()
			_, ok := f.e.sess.assembleAdmitDeps(f.e.c, f.dur, tc.qos, f.epoch, f.gen)
			wall := time.Since(start)
			n := f.o.sends(m, func(s string) bool { return f.readiness[s] })
			if want := strings.HasSuffix(tc.name, "proceeding"); ok != want {
				t.Errorf("refusal %s: the readiness decided ready=%v, want %v", tc.name, ok, want)
			}
			t.Logf("refusal %s: ready=%v, %d readiness reads, %.2f ms", tc.name, ok, n, float64(wall.Microseconds())/1000)
			if out := os.Getenv(jsPathBenchOutEnv); out != _EMPTY_ {
				fh, err := os.OpenFile(out+".refusals", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
				if err != nil {
					t.Fatalf("bench output: %v", err)
				}
				defer fh.Close()
				fmt.Fprintf(fh, "%s,%s,%v,%d,%.3f\n", os.Getenv(jsPathBenchLabelEnv), tc.name, ok, n,
					float64(wall.Microseconds())/1000)
			}
		})
	}
}

func TestMQTTJetStreamPathCosts(t *testing.T) {
	if os.Getenv(jsPathBenchEnv) != "1" {
		t.Skipf("set %s=1 to run the JetStream path benchmark", jsPathBenchEnv)
	}
	// Nothing periodic runs inside a window: the claim loop and the sweep ticker are pushed out
	// past every case (their initial passes still run at each broker start, and are waited for).
	prevWill, prevLease := mqttWillClaimInterval, mqttWillClaimLease
	mqttWillClaimInterval, mqttWillClaimLease = 10*time.Minute, 20*time.Minute
	restoreSweep := setMQTTSessionExpirySweepInterval(time.Hour)
	t.Cleanup(func() {
		mqttWillClaimInterval, mqttWillClaimLease = prevWill, prevLease
		restoreSweep()
	})
	jsPathInstallWorkerSeams(t)
	only := os.Getenv("MQTTNB_JS_PATH_BENCH_LOADS")
	rigs := []struct {
		name  string
		start func(t *testing.T) *jsPathRig
	}{
		{"standalone", jsPathStandalone},
		{"r3", jsPathR3},
	}
	if only == _EMPTY_ || strings.Contains(","+only+",", ",refusals,") {
		t.Run("refusals", jsPathRefusalCosts)
	}
	for _, rig := range rigs {
		t.Run(rig.name, func(t *testing.T) {
			for _, load := range jsPathLoads {
				if only != _EMPTY_ && !strings.Contains(","+only+",", ","+load.name+",") {
					continue
				}
				t.Run(load.name, func(t *testing.T) {
					// A fresh broker per load, so no load inherits another's sessions or workers.
					mark := jsPathMarkStart()
					r := rig.start(t)
					jsPathWaitStarted(t, mark)
					b := newJSPathBench(t, r)
					r.asm(t)
					b.jsPathSettle(t)
					load.run(t, b)
				})
			}
		})
	}
}
