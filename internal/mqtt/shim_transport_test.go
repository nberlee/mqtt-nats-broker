package mqtt

import (
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func runNATS(t *testing.T) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return srv, nc
}

// TestTransportPubSub verifies that processSub + processInboundClientMsg move a
// message (with NATS headers) through the real routing connection and invoke the
// delivery callback with the header/payload split msgParts expects. This is the
// plumbing the mqttJSA request/reply cycle rides on.
func TestTransportPubSub(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	s := &Server{nrc: nc, opts: &Options{}}
	c := &client{srv: s, nrc: nc, acc: s.globalAccount()}

	type delivery struct {
		subject, reply string
		hdr, msg       []byte
	}
	var mu sync.Mutex
	var got []delivery
	done := make(chan struct{}, 1)

	cb := func(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
		hdr, msg := pc.msgParts(rmsg)
		mu.Lock()
		got = append(got, delivery{subject, reply, append([]byte(nil), hdr...), append([]byte(nil), msg...)})
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	}

	sid := []byte("1")
	if _, _, err := c.processSub([]byte("test.subj"), nil, sid, cb, false); err != nil {
		t.Fatalf("processSub: %v", err)
	}
	// Give the async subscription a moment to register.
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Publish via processInboundClientMsg, framing the message the way the JSA
	// send loop does: [NATS/1.0 header block][payload] + trailing CRLF, with
	// c.pa describing the layout.
	framed := []byte(hdrLine + "My-Key: val" + _CRLF_ + _CRLF_)
	payload := []byte("hello")
	data := append(append([]byte(nil), framed...), payload...)
	wire := append(append([]byte(nil), data...), _CRLF_...)

	pub := &client{srv: s, nrc: nc}
	pub.pa.subject = []byte("test.subj")
	pub.pa.reply = []byte("test.reply")
	pub.pa.hdr = len(framed)
	pub.pa.size = len(data)
	if ok, _ := pub.processInboundClientMsg(wire, nil); !ok {
		t.Fatal("processInboundClientMsg returned not-ok")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("got %d deliveries, want 1", len(got))
	}
	d := got[0]
	if d.subject != "test.subj" {
		t.Errorf("subject = %q, want test.subj", d.subject)
	}
	if d.reply != "test.reply" {
		t.Errorf("reply = %q, want test.reply", d.reply)
	}
	if string(d.msg) != "hello" {
		t.Errorf("payload = %q, want hello", d.msg)
	}
	if len(d.hdr) == 0 {
		t.Error("expected non-empty header block from msgParts")
	}
}

// TestTransportUnsub verifies processUnsub stops delivery.
func TestTransportUnsub(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	s := &Server{nrc: nc, opts: &Options{}}
	c := &client{srv: s, nrc: nc, acc: s.globalAccount()}

	var count int
	var mu sync.Mutex
	cb := func(_ *subscription, _ *client, _ *Account, _, _ string, _ []byte) {
		mu.Lock()
		count++
		mu.Unlock()
	}
	sid := []byte("9")
	if _, _, err := c.processSub([]byte("u.subj"), nil, sid, cb, false); err != nil {
		t.Fatalf("processSub: %v", err)
	}
	nc.Flush()
	if err := c.processUnsub(sid); err != nil {
		t.Fatalf("processUnsub: %v", err)
	}
	nc.Flush()

	pub := &client{srv: s, nrc: nc}
	pub.pa.subject = []byte("u.subj")
	pub.pa.size = 3
	pub.processInboundClientMsg([]byte("hey"+_CRLF_), nil)
	nc.Flush()
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 0 {
		t.Errorf("delivery count = %d after unsub, want 0", count)
	}
}
