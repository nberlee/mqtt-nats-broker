//go:build mqtt_fulltests

package mqtt

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// jsRequestCensus counts, by class, what crosses a broker's JetStream boundary: every request
// and broker-owned publish it makes and every reply it receives, with their bytes. It is a plain
// subscriber on the account: core NATS hands it a copy of each matching message and it never
// replies, so it sees the broker's traffic without instrumenting the broker, identically
// whichever tree is measured. Comparing the classes per operation across two trees is what shows
// a change removed requests, or only overlapped them (the request observer, not this, tells
// those apart: a request and its reply come from different publishers and carry no common order
// here).
//
// ORDERED: every subscription feeds ONE channel on ONE connection, which nats.go fills from that
// connection's read loop in socket order, drained by one goroutine. The order is relied on only
// for the drain fence below, a publication of the tap's own that follows everything the server
// had already routed to it.
type jsRequestCensus struct {
	nc       *nats.Conn
	subs     []*nats.Subscription
	ch       chan *nats.Msg
	classify func(*nats.Msg) (string, bool)
	extra    func(*jsRequestCensus, *nats.Msg) // per-counted-message hook, under mu
	fence    string

	mu       sync.Mutex
	counts   map[string]int64
	bytes    map[string]int64
	awaiting map[string]string // a request's reply subject -> its class, until the reply is seen
	early    map[string]bool   // replies seen before their request's copy
	matched  int64
	fences   map[string]chan struct{}
	errs     []error
}

// jsRequestCensusFencePrefix is the census's own drain fence, published by the tap connection
// and never counted. Outside $MQTT., so no broker subscription can ever receive it.
const jsRequestCensusFencePrefix = "_CENSUS.fence."

func startJSRequestCensus(t *testing.T, url string, subjects []string,
	classify func(*nats.Msg) (string, bool), extra func(*jsRequestCensus, *nats.Msg)) *jsRequestCensus {
	t.Helper()
	cs := &jsRequestCensus{
		ch: make(chan *nats.Msg, 1<<20), classify: classify, extra: extra,
		fence:  jsRequestCensusFencePrefix + nuid.Next(),
		counts: map[string]int64{}, bytes: map[string]int64{},
		awaiting: map[string]string{}, early: map[string]bool{}, fences: map[string]chan struct{}{},
	}
	nc, err := nats.Connect(url, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		cs.mu.Lock()
		cs.errs = append(cs.errs, err)
		cs.mu.Unlock()
	}))
	if err != nil {
		t.Fatalf("census connect: %v", err)
	}
	t.Cleanup(nc.Close)
	cs.nc = nc
	for _, subject := range append(subjects, cs.fence+".>") {
		sub, err := nc.ChanSubscribe(subject, cs.ch)
		if err != nil {
			t.Fatalf("census subscribe %s: %v", subject, err)
		}
		// A channel subscription's limit is its channel: a full one drops, which the drain checks.
		cs.subs = append(cs.subs, sub)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("census flush: %v", err)
	}
	go cs.run()
	return cs
}

func (cs *jsRequestCensus) run() {
	for m := range cs.ch {
		cs.count(m)
	}
}

// jsMsgBytes is a message's size on the wire, near enough: subject, reply, header block, payload.
func jsMsgBytes(m *nats.Msg) int64 {
	n := len(m.Subject) + len(m.Reply) + len(m.Data)
	if len(m.Header) > 0 {
		n += len("NATS/1.0\r\n\r\n")
		for k, vs := range m.Header {
			for _, v := range vs {
				n += len(k) + len(": ") + len(v) + len("\r\n")
			}
		}
	}
	return int64(n)
}

func (cs *jsRequestCensus) count(m *nats.Msg) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if strings.HasPrefix(m.Subject, cs.fence+".") {
		if ch := cs.fences[m.Subject]; ch != nil {
			close(ch)
			delete(cs.fences, m.Subject)
		}
		return
	}
	class, ok := cs.classify(m)
	if !ok {
		return
	}
	cs.counts[class]++
	cs.bytes[class] += jsMsgBytes(m)
	// Pairing, order-independent: a reply can reach the tap before its request's copy.
	if strings.HasPrefix(m.Subject, mqttJSARepliesPrefix) {
		if _, ok := cs.awaiting[m.Subject]; ok {
			delete(cs.awaiting, m.Subject)
			cs.matched++
		} else {
			cs.early[m.Subject] = true
		}
	} else if strings.HasPrefix(m.Reply, mqttJSARepliesPrefix) {
		if cs.early[m.Reply] {
			delete(cs.early, m.Reply)
			cs.matched++
		} else {
			cs.awaiting[m.Reply] = class
		}
	}
	if cs.extra != nil {
		cs.extra(cs, m)
	}
}

// reset starts a new measurement window: counts, bytes and pairing are cleared, and the tap's
// queue is first fenced so nothing from before the window lands in it.
func (cs *jsRequestCensus) reset(t *testing.T, flush ...func() error) {
	t.Helper()
	cs.drainQueue(t, flush...)
	cs.mu.Lock()
	cs.counts, cs.bytes = map[string]int64{}, map[string]int64{}
	cs.awaiting, cs.early, cs.matched = map[string]string{}, map[string]bool{}, 0
	cs.mu.Unlock()
}

// drainQueue waits until every message the server had routed to the tap when flush returned has
// been counted: each flush makes a publisher's earlier publications processed by the server, so
// their copies are queued to the tap before the tap's own fence, which then arrives behind them.
func (cs *jsRequestCensus) drainQueue(t *testing.T, flush ...func() error) {
	t.Helper()
	for _, f := range flush {
		if err := f(); err != nil {
			t.Fatalf("census drain: flush: %v", err)
		}
	}
	subj := cs.fence + "." + nuid.Next()
	done := make(chan struct{})
	cs.mu.Lock()
	cs.fences[subj] = done
	cs.mu.Unlock()
	if err := cs.nc.Publish(subj, nil); err != nil {
		t.Fatalf("census fence: %v", err)
	}
	if err := cs.nc.Flush(); err != nil {
		t.Fatalf("census fence flush: %v", err)
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("census drain: the fence did not come back within 30s")
	}
	for _, sub := range cs.subs {
		if d, err := sub.Dropped(); err != nil || d != 0 {
			t.Fatalf("census drain: the tap dropped %d messages (%v)", d, err)
		}
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if len(cs.errs) > 0 {
		t.Fatalf("census drain: tap connection errors: %v", cs.errs)
	}
}

// jsCensusSnapshot is one window's counts.
type jsCensusSnapshot struct {
	counts, bytes map[string]int64
	unanswered    map[string]int64 // request classes whose reply was never seen
	unmatched     int64            // replies whose request was never seen
	matched       int64
}

// snapshot drains and returns the window's counts.
func (cs *jsRequestCensus) snapshot(t *testing.T, flush ...func() error) jsCensusSnapshot {
	t.Helper()
	cs.drainQueue(t, flush...)
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := jsCensusSnapshot{counts: map[string]int64{}, bytes: map[string]int64{}, unanswered: map[string]int64{},
		unmatched: int64(len(cs.early)), matched: cs.matched}
	for k, v := range cs.counts {
		out.counts[k] = v
	}
	for k, v := range cs.bytes {
		out.bytes[k] = v
	}
	for _, class := range cs.awaiting {
		out.unanswered[class]++
	}
	return out
}

func (s jsCensusSnapshot) total(prefix string) (n, b int64) {
	for k, v := range s.counts {
		if strings.HasPrefix(k, prefix) {
			n += v
			b += s.bytes[k]
		}
	}
	return n, b
}

func (s jsCensusSnapshot) classes() []string {
	var out []string
	for k := range s.counts {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// jsCensusBrokerClass classes a message the tap saw as the broker's, by API verb and stream, by
// MQTT publication family, by acknowledgement kind, or as a reply by its kind; and a test's own
// JetStream request (replying on _INBOX) as test:<verb>, so a server-side counter can be reconciled
// with it. The fence is never classed.
func jsCensusBrokerClass(m *nats.Msg) (string, bool) {
	subj, reply := m.Subject, m.Reply
	if strings.HasPrefix(subj, jsRequestCensusFencePrefix) {
		return _EMPTY_, false
	}
	if strings.HasPrefix(subj, mqttJSARepliesPrefix) {
		// $MQTT.JSA.<node>.<kind>.<nuid>
		if tk := strings.Split(subj, "."); len(tk) >= 5 {
			return "reply:" + tk[3], true
		}
		return "reply:?", true
	}
	// $MQTT.inbox. is the production service connection's CustomInboxPrefix, used only by nats.go's
	// own requests; the rigs do not set it, so this arm is not reached under test.
	if strings.HasPrefix(subj, mqttPrefix+"inbox.") {
		return "reply:inbox", true
	}
	if strings.HasPrefix(subj, "$JS.ACK.") {
		kind := string(m.Data)
		if i := strings.IndexByte(kind, ' '); i > 0 {
			kind = kind[:i]
		}
		if kind == _EMPTY_ {
			kind = "+ACK"
		}
		if strings.HasPrefix(reply, mqttJSARepliesPrefix) {
			return "ack:" + kind + "+confirmed", true
		}
		return "ack:" + kind, true
	}
	if api, ok := strings.CutPrefix(subj, "$JS.API."); ok {
		var keep []string
		for _, tk := range strings.Split(api, ".") {
			keep = append(keep, tk)
			if strings.HasPrefix(tk, "$") {
				break
			}
		}
		verb := strings.Join(keep, ".")
		if strings.HasPrefix(reply, "_INBOX.") {
			return "test:" + verb, true
		}
		if reply == _EMPTY_ || strings.HasPrefix(reply, mqttJSARepliesPrefix) || strings.HasPrefix(reply, mqttPrefix+"inbox.") {
			return "api:" + verb, true
		}
		return _EMPTY_, false
	}
	// A delivery subject carries JetStream's deliveries, idle heartbeats and flow control to the
	// broker: the server's traffic, not the broker's.
	if strings.HasPrefix(subj, mqttSubPrefix) {
		return _EMPTY_, false
	}
	if strings.HasPrefix(subj, mqttPrefix) && (reply == _EMPTY_ || strings.HasPrefix(reply, mqttJSARepliesPrefix)) {
		rest := strings.TrimPrefix(subj, mqttPrefix)
		family, _, _ := strings.Cut(rest, ".")
		class := "pub:" + family
		if m.Header.Get(JSBatchId) != _EMPTY_ {
			if m.Header.Get(JSBatchCommit) != _EMPTY_ {
				class += "+batch-commit"
			} else {
				class += "+batch-member"
			}
		}
		return class, true
	}
	return _EMPTY_, false
}

// jsCensusReport is the per-class table of a snapshot, per operation.
func jsCensusReport(s jsCensusSnapshot, ops int) string {
	var b strings.Builder
	for _, c := range s.classes() {
		fmt.Fprintf(&b, "    %-48s %8d %10d B  %8.2f/op\n", c, s.counts[c], s.bytes[c], float64(s.counts[c])/float64(ops))
	}
	return b.String()
}

// sinceAt returns the events recorded between two marks, with the time each was observed.
func (o *testJSAObserver) sinceAt(from, to int) ([]mqttJSARequestEvent, []time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]mqttJSARequestEvent(nil), o.events[from:to]...), append([]time.Time(nil), o.at[from:to]...)
}

// jsCensusAPIClass is a JS API subject's class, the verb and the stream it names, as the census
// classes it; any other subject is returned whole.
func jsCensusAPIClass(subject string) string {
	api, ok := strings.CutPrefix(subject, "$JS.API.")
	if !ok {
		return subject
	}
	var keep []string
	for _, tk := range strings.Split(api, ".") {
		keep = append(keep, tk)
		if strings.HasPrefix(tk, "$") {
			break
		}
	}
	return "api:" + strings.Join(keep, ".")
}

// apiOf is the API subject a reply answers.
func (o *testJSAObserver) apiOf(reply string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.api[reply]
}
