package mqtt

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// TestV5SharedSubscription verifies MQTT 5.0 shared subscriptions ($share/group):
// two subscribers in the same group split the published messages — each message
// is delivered to exactly one member, with no duplicates and none lost.
func TestV5SharedSubscription(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)

	const total = 20
	var mu sync.Mutex
	countA, countB := 0, 0
	seen := make(map[string]int)
	done := make(chan struct{})
	var received int

	mkHandler := func(which *int) func(paho.PublishReceived) (bool, error) {
		return func(pr paho.PublishReceived) (bool, error) {
			mu.Lock()
			*which++
			seen[string(pr.Packet.Payload)]++
			received++
			if received == total {
				close(done)
			}
			mu.Unlock()
			return true, nil
		}
	}

	subA := v5Client(t, broker, "shared-a", mkHandler(&countA))
	subB := v5Client(t, broker, "shared-b", mkHandler(&countB))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, c := range []*paho.Client{subA, subB} {
		if _, err := c.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: "$share/g1/shared/topic", QoS: 1}},
		}); err != nil {
			t.Fatalf("shared subscribe: %v", err)
		}
	}

	pub := v5Client(t, broker, "shared-pub", nil)
	for i := 0; i < total; i++ {
		payload := []byte{byte('A' + i)}
		if _, err := pub.Publish(ctx, &paho.Publish{Topic: "shared/topic", QoS: 1, Payload: payload}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// The window is a throughput bound, not a correctness one: under a loaded -race suite the group
	// consumer was measured offering these about a second apart, every one exactly once.
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		mu.Lock()
		t.Fatalf("timed out: received %d/%d (A=%d B=%d)", received, total, countA, countB)
	}

	mu.Lock()
	defer mu.Unlock()
	if countA == 0 || countB == 0 {
		t.Errorf("expected both members to receive some messages, got A=%d B=%d", countA, countB)
	}
	for p, n := range seen {
		if n != 1 {
			t.Errorf("message %q delivered %d times, want exactly 1", p, n)
		}
	}
	if len(seen) != total {
		t.Errorf("distinct messages = %d, want %d", len(seen), total)
	}
}

// testQ2AffFixtureNonce is a valid claim INCARNATION for fixtures that are not about
// incarnations. Exactly 22 characters of the NUID alphabet, so it satisfies
// q2AffClaimUsableForDecision the way a real election's minted nonce does. A test
// that stages TWO incarnations of one claim subject must use distinct values instead
// — that distinction is the whole of F-065.
const testQ2AffFixtureNonce = "F065fixtureNonce000001"
