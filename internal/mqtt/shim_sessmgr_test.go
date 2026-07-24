package mqtt

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TestSessionManagerBringUp drives the full account-session-manager creation
// against a live in-process JetStream server. This exercises the entire wired
// transport: createSubscription -> processSub, the sendJSAPIrequests send loop
// -> processInboundClientMsg, and jsa.createStream request/reply -> NATS ->
// processJSAPIReplies. Success means the MQTT streams actually exist in
// JetStream, created by the ported code over nats.go.
func TestSessionManagerBringUp(t *testing.T) {
	_, nc := runNATS(t)

	s := &Server{
		nrc:         nc,
		opts:        &Options{},
		quitCh:      make(chan struct{}),
		clusterName: "node1", // JSA reply routing id; must be non-empty & unique per instance
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)

	c := s.createInternalAccountClient()

	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	if asm == nil {
		t.Fatal("nil account session manager")
	}

	// Verify the core MQTT streams were created in JetStream.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, name := range []string{mqttSessStreamName, mqttStreamName, mqttRetainedMsgsStreamName} {
		if _, err := js.Stream(ctx, name); err != nil {
			t.Errorf("expected stream %q to exist: %v", name, err)
		}
	}

	// A second call must return the cached manager, not recreate it.
	asm2, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("second getOrCreate: %v", err)
	}
	if asm2 != asm {
		t.Error("expected cached account session manager on second call")
	}

	close(s.quitCh)
}
