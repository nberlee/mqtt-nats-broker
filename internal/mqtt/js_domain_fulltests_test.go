//go:build mqttport_fulltests

package mqtt

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestUpdateStreamHelperTakesDomainRoute: the out-of-band stream update the tagged tests
// impose limits with is a raw request, not a JSA one, so nothing in the JSA path applies
// the -js-domain prefix to it on its behalf. Under a domain it must still reach the domain's
// JetStream, or a test run against a domain-qualified deployment would update a different
// stream than the broker reads.
//
// Success alone proves nothing here: an unprefixed request is answered by the local
// JetStream behind the forwarder, so the update would land either way. The witness is the
// exact domain-qualified UPDATE subject crossing the forwarder. A total forwarded count is
// not enough either, because manager creation starts background sweeps whose requests cross
// the same forwarder.
func TestUpdateStreamHelperTakesDomainRoute(t *testing.T) {
	t.Parallel()
	const domain = "HUB"
	_, nc := runNATS(t)

	prefix := "$JS." + domain + ".API."
	var (
		mu        sync.Mutex
		forwarded []string
	)
	sub, err := nc.Subscribe(prefix+">", func(m *nats.Msg) {
		mu.Lock()
		forwarded = append(forwarded, m.Subject)
		mu.Unlock()
		fwd := nats.NewMsg(JSApiPrefix + "." + strings.TrimPrefix(m.Subject, prefix))
		fwd.Header = m.Header
		fwd.Data = m.Data
		resp, err := nc.RequestMsg(fwd, 5*time.Second)
		if err != nil {
			return
		}
		resp.Subject = m.Reply
		_ = nc.PublishMsg(resp)
	})
	if err != nil {
		t.Fatalf("Subscribe %s>: %v", prefix, err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	s := &Server{
		nrc:      nc,
		opts:     &Options{MQTT: MQTTOpts{JsDomain: domain}},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}

	const name = "UPDATE_DOMAIN_WITNESS"
	jsRawCreateStream(t, nc, &StreamConfig{
		Name:     name,
		Subjects: []string{"update.domain.witness"},
		Storage:  MemoryStorage,
		Replicas: 1,
		MaxMsgs:  -1,
	})
	cfg := StreamConfig{
		Name:     name,
		Subjects: []string{"update.domain.witness"},
		Storage:  MemoryStorage,
		Replicas: 1,
		MaxMsgs:  7,
	}
	if _, err := asm.jsa.updateStream(&cfg); err != nil {
		t.Fatalf("updateStream under domain %q: %v", domain, err)
	}

	want := prefix + "STREAM.UPDATE." + name
	mu.Lock()
	var took bool
	for _, subj := range forwarded {
		if subj == want {
			took = true
			break
		}
	}
	mu.Unlock()
	if !took {
		t.Fatalf("no request on %q crossed the domain forwarder: the update bypassed the domain route", want)
	}

	si, err := asm.jsa.lookupStream(name)
	if err != nil || si == nil {
		t.Fatalf("lookupStream(%q): %v", name, err)
	}
	if si.Config.MaxMsgs != 7 {
		t.Fatalf("MaxMsgs = %d after the update, want 7", si.Config.MaxMsgs)
	}
}

// TestNoWaitConsumerDeleteTakesDomainRoute: a no-wait consumer delete is published, not requested,
// so it goes through the publication path rather than the request path that prefixes every JS
// API subject. Under a domain it must still name the domain's API. Success proves nothing here
// (a no-wait delete reports none); the witness is the exact domain-qualified subject crossing the
// forwarder.
func TestNoWaitConsumerDeleteTakesDomainRoute(t *testing.T) {
	t.Parallel()
	const domain = "HUB"
	_, nc := runNATS(t)

	prefix := "$JS." + domain + ".API."
	seen := make(chan string, 64)
	sub, err := nc.Subscribe(prefix+"CONSUMER.DELETE.>", func(m *nats.Msg) { seen <- m.Subject })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	// The manager's own startup needs a JetStream behind the domain prefix: forward every request.
	fwd, err := nc.Subscribe(prefix+">", func(m *nats.Msg) {
		if m.Reply == _EMPTY_ {
			return // a no-wait publication: nothing to answer
		}
		out := nats.NewMsg(JSApiPrefix + "." + strings.TrimPrefix(m.Subject, prefix))
		out.Header = m.Header
		out.Data = m.Data
		resp, err := nc.RequestMsg(out, 5*time.Second)
		if err != nil {
			return
		}
		resp.Subject = m.Reply
		_ = nc.PublishMsg(resp)
	})
	if err != nil {
		t.Fatalf("Subscribe %s>: %v", prefix, err)
	}
	t.Cleanup(func() { _ = fwd.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	s := &Server{
		nrc:      nc,
		opts:     &Options{MQTT: MQTTOpts{JsDomain: domain}},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	const consumer = "nowait-domain-witness"
	if _, err := asm.jsa.deleteConsumer(mqttStreamName, consumer, true); err != nil {
		t.Fatalf("a no-wait delete reports no error: %v", err)
	}
	want := prefix + "CONSUMER.DELETE." + mqttStreamName + "." + consumer
	deadline := time.After(5 * time.Second)
	for {
		select {
		case subj := <-seen:
			if subj == want {
				return
			}
		case <-deadline:
			t.Fatalf("no publication on %q crossed the domain route: the no-wait delete bypassed the domain", want)
		}
	}
}
