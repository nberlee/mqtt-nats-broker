// A subscription drained across a reconnect leaves no interest on the server.
//
// Delivery subscriptions are drained at detach ("Delivery subscriptions are drained at
// detach", docs/ARCHITECTURE.md), and a service-connection reconnect can fall inside that
// drain. The drain ends with a local removal and no UNSUB, so it relies on the reconnect not
// re-sending SUB for a draining subscription: otherwise the server keeps routing a detached
// session's JetStream push deliveries to a sid the client discards, each one coming back
// only at AckWait. nats.go gives that since v1.56.0 (nats-io/nats.go#2232); v1.55.0 re-sent
// the SUB and fails this test.

package mqtt

import (
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestDrainedSubscriptionLeavesNoServerInterestAcrossReconnect(t *testing.T) {
	srv, _ := runNATS(t)
	reconnected := make(chan struct{}, 4)
	nc, err := nats.Connect(srv.ClientURL(), nats.ReconnectWait(10*time.Millisecond),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const subject = "drain.reconnect"
	interest := func() int {
		t.Helper()
		sz, err := srv.Subsz(&natsserver.SubszOptions{Subscriptions: true, Test: subject})
		if err != nil {
			t.Fatal(err)
		}
		return sz.Total
	}
	flush := func() {
		t.Helper()
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// The first message holds the callback, so the drain cannot finish until it is released.
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	entered := make(chan struct{}, 1)
	delivered := make(chan string, 16)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		delivered <- string(m.Data)
		select {
		case entered <- struct{}{}:
			<-hold
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	flush()
	if got := interest(); got != 1 {
		t.Fatalf("server interest after SUB = %d, want 1", got)
	}
	if err := nc.Publish(subject, []byte("held")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the held message never reached the callback")
	}

	if err := sub.Drain(); err != nil {
		t.Fatal(err)
	}
	flush()
	if got := interest(); got != 0 {
		t.Fatalf("server interest after the drain's UNSUB = %d, want 0", got)
	}

	// The reconnect must fall inside the drain, or it tests nothing.
	if !sub.IsValid() {
		t.Fatal("the drain completed before the reconnect")
	}
	if err := nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect")
	}
	flush()
	if !sub.IsValid() {
		t.Fatal("the drain completed before the reconnect was observed")
	}
	if got := interest(); got != 0 {
		t.Fatalf("server interest after a reconnect inside the drain = %d, want 0", got)
	}

	release()
	for deadline := time.Now().Add(5 * time.Second); sub.IsValid(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the drain never completed")
		}
	}
	if got := <-delivered; got != "held" {
		t.Fatalf("the drain delivered %q, want the held message", got)
	}

	// Nothing routes to the drained sid: a later publish never reaches this connection.
	flush()
	if got := interest(); got != 0 {
		t.Fatalf("server interest after the drain completed = %d, want 0", got)
	}
	before := nc.Stats().InMsgs
	if err := nc.Publish(subject, []byte("after")); err != nil {
		t.Fatal(err)
	}
	flush()
	if after := nc.Stats().InMsgs; after != before {
		t.Fatalf("a publish after the drain reached the connection (InMsgs %d -> %d)", before, after)
	}
}
