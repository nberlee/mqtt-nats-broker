package mqtt

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The bundle preflight's ten lookups go out as one round.
func TestPreflightIssuesOneRound(t *testing.T) {
	e := testNewInflightEnv(t, "client-preflight-round")
	jsa := &e.asm.jsa
	o := testObserveJSA(t, jsa)
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)
	isBundle := func(s string) bool {
		for _, n := range names {
			if s == JSApiPrefix+".STREAM.INFO."+n {
				return true
			}
		}
		return false
	}
	h := o.hold(testSubjectIs(JSApiPrefix + ".STREAM.INFO." + names[0]))
	m := o.mark()
	done := make(chan error, 1)
	go func() { done <- mqttPreflightBrokerStreamBundle(jsa, specs) }()
	if !o.waitSends(m, len(names), isBundle, 2*time.Second) {
		t.Fatalf("with %s's reply held, %d of the %d bundle lookups were sent: serial", names[0],
			o.sends(m, isBundle), len(names))
	}
	o.release(h, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a complete bundle must be admitted: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the preflight did not complete after its round was released")
	}
}

// At a real startup the retired-origin lookup is ANSWERED before any bundle lookup is sent: its
// refusal stops startup before a bundle stream is read. Watched on the production path, through
// the sender's ordered send hook and the broker's own stream-info replies.
func TestPreflightRunsAfterTheOriginLookup(t *testing.T) {
	s1, c1, nc := newBringUpServer(t)
	var mu sync.Mutex
	replies := 0
	tap, err := nc.Subscribe(mqttJSARepliesPrefix+"*."+mqttJSAStreamLookup+".*", func(*nats.Msg) {
		mu.Lock()
		replies++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribing the reply tap: %v", err)
	}
	t.Cleanup(func() { _ = tap.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var order []string
	var repliesAtFirstBundle = -1
	hook := func(subject string) {
		if !strings.HasPrefix(subject, JSApiPrefix+".STREAM.INFO.") {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		order = append(order, strings.TrimPrefix(subject, JSApiPrefix+".STREAM.INFO."))
		if subject == JSApiPrefix+".STREAM.INFO."+mqttSessStreamName && repliesAtFirstBundle < 0 {
			repliesAtFirstBundle = replies
		}
	}
	mqttTestJSSendPublishHookV.Store(&hook)
	t.Cleanup(func() { mqttTestJSSendPublishHookV.Store(nil) })
	if _, err := s1.getOrCreateMQTTAccountSessionManager(c1); err != nil {
		t.Fatalf("a fresh account must start: %v", err)
	}
	mqttTestJSSendPublishHookV.Store(nil)
	mu.Lock()
	defer mu.Unlock()
	if len(order) == 0 || order[0] != mqttOriginStreamName {
		t.Fatalf("the first stream lookup was %v, want the retired-origin one", order)
	}
	if repliesAtFirstBundle < 1 {
		t.Fatalf("the bundle's first lookup was sent with %d stream-info replies received: the origin's "+
			"answer must come first", repliesAtFirstBundle)
	}
}

// The round is judged in TABLE order, so the stream an error names is the first undecided one in
// the table, whichever position failed and however the round answered.
func TestPreflightJudgesInTableOrder(t *testing.T) {
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)
	st := &testBundleLookupStub{info: map[string]*StreamInfo{}, err: map[string]error{
		names[3]: errors.New("failure at position 3"),
		names[6]: errors.New("failure at position 6"),
	}}
	err := mqttPreflightBrokerStreamBundle(st, specs)
	if err == nil || !strings.Contains(err.Error(), names[3]) || strings.Contains(err.Error(), names[6]) {
		t.Fatalf("two failing streams must report the first in table order (%s): %v", names[3], err)
	}
	st = &testBundleLookupStub{info: map[string]*StreamInfo{}, err: map[string]error{
		names[2]: errors.New("timeout after 5s"),
		names[7]: ApiErrors[JSStreamNotFoundErr],
	}, silent: map[string]bool{}}
	st.err[names[7]] = errors.New("an explicit error")
	if err := mqttPreflightBrokerStreamBundle(st, specs); err == nil || !strings.Contains(err.Error(), names[2]) {
		t.Fatalf("an earlier undecidable stream must be the one reported (%s): %v", names[2], err)
	}
}

// A failing round has still read every stream once, and no more: the bound the round's failure
// path may cost.
func TestPreflightEarlyFailureCost(t *testing.T) {
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)
	st := &testBundleLookupStub{info: map[string]*StreamInfo{}, err: map[string]error{names[0]: errors.New("unreadable")}}
	if err := mqttPreflightBrokerStreamBundle(st, specs); err == nil {
		t.Fatal("an undecidable first stream must fail the preflight")
	}
	if len(st.seen) != len(names) {
		t.Fatalf("the failing round read %d streams, want exactly the table's %d", len(st.seen), len(names))
	}
}
