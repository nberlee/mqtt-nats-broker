package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// testJSAObserver records what one account's JetStream assistant sends and delivers, through
// its per-instance observer seam, and arms reply holds keyed by what a reply answers.
// A send is observed before it is published, so what a reply answers is always known by the time
// the reply arrives.
//
// Requests are matched by their TARGET: for a message GET, the subject it reads (or "seq:N" for a
// read by sequence), since every message GET on one stream shares one API subject; for anything
// else, the API subject itself.
type testJSAObserver struct {
	jsa    *mqttJSA
	mu     sync.Mutex
	events []mqttJSARequestEvent
	at     []time.Time       // when each event was observed
	subj   map[string]string // reply subject -> the target it answers
	api    map[string]string // reply subject -> the API subject it answers
}

// testJSARequestTarget is what a sent request reads, for matching.
func testJSARequestTarget(ev mqttJSARequestEvent) string {
	if strings.HasPrefix(ev.Subject, JSApiPrefix+".STREAM.MSG.GET.") {
		var req JSApiMsgGetRequest
		if json.Unmarshal(ev.Body, &req) == nil {
			if req.LastFor != _EMPTY_ {
				return req.LastFor
			}
			return fmt.Sprintf("seq:%d", req.Seq)
		}
	}
	return ev.Subject
}

func testObserveJSA(t *testing.T, jsa *mqttJSA) *testJSAObserver {
	t.Helper()
	o := &testJSAObserver{jsa: jsa, subj: map[string]string{}, api: map[string]string{}}
	f := func(ev mqttJSARequestEvent) {
		o.mu.Lock()
		defer o.mu.Unlock()
		if ev.Kind == mqttJSARequestSent {
			if ev.Reply != _EMPTY_ {
				o.api[ev.Reply] = ev.Subject
			}
			ev.Subject = testJSARequestTarget(ev)
			if ev.Reply != _EMPTY_ {
				o.subj[ev.Reply] = ev.Subject
			}
		}
		o.events = append(o.events, ev)
		o.at = append(o.at, time.Now())
	}
	jsa.testObserveRequest.Store(&f)
	t.Cleanup(func() {
		// A test that failed mid-hold must not leave a waiter parked.
		if h := jsa.testHoldReply.Swap(nil); h != nil {
			h.release(jsa, nil)
		}
		jsa.testObserveRequest.Store(nil)
	})
	return o
}

// subjectOf is the subject a reply answers, or empty for a reply this observer never saw sent.
func (o *testJSAObserver) subjectOf(reply string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.subj[reply]
}

// mark is a position in the event log, so a count can be scoped to what happened after it.
func (o *testJSAObserver) mark() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.events)
}

// since returns the events recorded at or after a mark.
func (o *testJSAObserver) since(m int) []mqttJSARequestEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]mqttJSARequestEvent(nil), o.events[m:]...)
}

// sends counts the requests sent since a mark whose subject satisfies match. No-wait
// publications (no reply subject) are not requests and are never counted here.
func (o *testJSAObserver) sends(m int, match func(subject string) bool) int {
	n := 0
	for _, ev := range o.since(m) {
		if ev.Kind == mqttJSARequestSent && ev.Reply != _EMPTY_ && match(ev.Subject) {
			n++
		}
	}
	return n
}

// sentSubjects lists, in send order, the subjects of the requests sent since a mark.
func (o *testJSAObserver) sentSubjects(m int, match func(subject string) bool) []string {
	var out []string
	for _, ev := range o.since(m) {
		if ev.Kind == mqttJSARequestSent && ev.Reply != _EMPTY_ && match(ev.Subject) {
			out = append(out, ev.Subject)
		}
	}
	return out
}

// delivered counts the replies delivered since a mark whose answered subject satisfies match.
func (o *testJSAObserver) delivered(m int, match func(subject string) bool) int {
	n := 0
	for _, ev := range o.since(m) {
		if ev.Kind == mqttJSAReplyDelivered && match(o.subjectOf(ev.Reply)) {
			n++
		}
	}
	return n
}

// waitSends waits, bounded, until at least n matching requests were sent since a mark.
func (o *testJSAObserver) waitSends(m, n int, match func(subject string) bool, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for {
		if o.sends(m, match) >= n {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// hold arms a reply hold for the replies to the requests whose subject satisfies match. Only one
// hold is armed at a time; release it before arming the next.
func (o *testJSAObserver) hold(match func(subject string) bool) *mqttJSAReplyHold {
	h := newMQTTJSAReplyHold(func(reply string) bool { return match(o.subjectOf(reply)) })
	o.jsa.testHoldReply.Store(h)
	return h
}

// release disarms a hold and delivers what it parked, each value first passed through transform
// when one is given.
func (o *testJSAObserver) release(h *mqttJSAReplyHold, transform func(*mqttJSAResponse)) {
	o.jsa.testHoldReply.CompareAndSwap(h, nil)
	h.release(o.jsa, transform)
}

func testSubjectIs(want string) func(string) bool {
	return func(s string) bool { return s == want }
}

func testSubjectHasPrefix(p string) func(string) bool {
	return func(s string) bool { return strings.HasPrefix(s, p) }
}

// The controls that make the observer and the reply hold a witness for overlap: a serial
// sequence of requests and a pipelined round issue the same kind of requests, and only the
// sends seen while replies are held tell them apart. Each control also has to COMPLETE once
// released, with the right answers: a hold that dropped replies instead of parking them would
// stall a serial caller exactly as a hold should, and only completion distinguishes the two.
func TestJSARequestObserverDepthControls(t *testing.T) {
	e := testNewInflightEnv(t, "client-observer-controls")
	jsa := &e.asm.jsa
	o := testObserveJSA(t, jsa)

	streams := []string{mqttSessStreamName, mqttWillQStreamName, mqttTeardownStreamName}
	durs := []string{"observer-control-a", "observer-control-b", "observer-control-c"}
	isStreamInfo := func(s string) bool {
		for _, st := range streams {
			if s == JSApiPrefix+".STREAM.INFO."+st {
				return true
			}
		}
		return false
	}
	isConsumerInfo := testSubjectHasPrefix(JSApiPrefix + ".CONSUMER.INFO." + mqttStreamName + ".observer-control-")

	h := o.hold(func(s string) bool { return isStreamInfo(s) || isConsumerInfo(s) })
	m := o.mark()

	type serialOut struct {
		names []string
		err   error
	}
	serialDone := make(chan serialOut, 1)
	go func() {
		var out serialOut
		for _, st := range streams {
			si, err := jsa.lookupStream(st)
			if err != nil {
				out.err = err
				break
			}
			out.names = append(out.names, si.Config.Name)
		}
		serialDone <- out
	}()
	type multiOut struct {
		ok  []bool
		err error
	}
	multiDone := make(chan multiOut, 1)
	go func() {
		_, ok, err := jsa.lookupConsumersMulti(mqttStreamName, durs)
		multiDone <- multiOut{ok, err}
	}()

	if !o.waitSends(m, 3, isConsumerInfo, 2*time.Second) {
		t.Fatalf("the pipelined round sent %d of its 3 requests while their replies were held",
			o.sends(m, isConsumerInfo))
	}
	if !o.waitSends(m, 1, isStreamInfo, 2*time.Second) {
		t.Fatal("the serial sequence never sent its first request")
	}
	// Give a serial caller every chance to send a second request it must not have.
	time.Sleep(100 * time.Millisecond)
	if n := o.sends(m, isStreamInfo); n != 1 {
		t.Fatalf("the serial sequence sent %d requests while its first reply was held, want 1", n)
	}
	if n := o.delivered(m, func(s string) bool { return isStreamInfo(s) || isConsumerInfo(s) }); n != 0 {
		t.Fatalf("%d held replies were delivered before the release", n)
	}
	releaseAt := o.mark()
	o.release(h, nil)

	select {
	case out := <-serialDone:
		if out.err != nil {
			t.Fatalf("the serial sequence failed after the release: %v", out.err)
		}
		if strings.Join(out.names, ",") != strings.Join(streams, ",") {
			t.Fatalf("the serial sequence answered %v, want %v", out.names, streams)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the serial sequence did not complete after the release: the hold lost a reply")
	}
	select {
	case out := <-multiDone:
		if out.err != nil {
			t.Fatalf("the pipelined round failed after the release: %v", out.err)
		}
		for i, ok := range out.ok {
			if !ok {
				t.Fatalf("durable %q: absence was not settled after the release", durs[i])
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the pipelined round did not complete after the release: the hold lost a reply")
	}

	// Causal order: every delivery follows its own send, and every held delivery follows the
	// release.
	sentAt := map[string]int{}
	for i, ev := range o.since(m) {
		switch ev.Kind {
		case mqttJSARequestSent:
			sentAt[ev.Reply] = m + i
		case mqttJSAReplyDelivered:
			s := o.subjectOf(ev.Reply)
			if !isStreamInfo(s) && !isConsumerInfo(s) {
				continue
			}
			at, ok := sentAt[ev.Reply]
			if !ok || at > m+i {
				t.Fatalf("a reply to %q was delivered before its request was sent", s)
			}
			if m+i < releaseAt {
				t.Fatalf("a held reply to %q was delivered before the release", s)
			}
		}
	}
	if n := o.delivered(m, isConsumerInfo); n != 3 {
		t.Fatalf("the pipelined round's replies were delivered %d times, want exactly 3", n)
	}
}

// newMQTTJSAReplyHold arms nothing by itself: a test stores it on the JSA it holds replies for.
func newMQTTJSAReplyHold(match func(reply string) bool) *mqttJSAReplyHold {
	return &mqttJSAReplyHold{match: match}
}

// release disarms the hold and delivers everything it parked, in arrival order, each value first
// passed through transform when one is given. Parking takes the same lock, so nothing is parked
// waitParked reports whether the hold has parked at least n replies within d.
func (h *mqttJSAReplyHold) waitParked(n int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		h.mu.Lock()
		got := len(h.parked)
		h.mu.Unlock()
		if got >= n {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// once release has begun.
func (h *mqttJSAReplyHold) release(jsa *mqttJSA, transform func(*mqttJSAResponse)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.released = true
	for _, p := range h.parked {
		if transform != nil {
			transform(p.r)
		}
		jsa.observeRequest(mqttJSARequestEvent{Kind: mqttJSAReplyDelivered, Reply: p.r.reply, Value: p.r.value})
		p.ch <- p.r
	}
	h.parked = nil
}
