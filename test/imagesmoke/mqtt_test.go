package imagesmoke

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

const (
	packetConnack    = 0x20
	packetDisconnect = 0xe0

	// reasonServerShuttingDown is the MQTT 5.0 DISCONNECT reason the drain sends.
	reasonServerShuttingDown = 0x8b
)

// connectV5 is a minimal MQTT 5.0 CONNECT: clean start, no keep alive, no properties.
func connectV5(clientID string) []byte {
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0x02, 0, 0, 0}
	body = append(body, byte(len(clientID)>>8), byte(len(clientID)))
	body = append(body, clientID...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

// connectV311 is a minimal MQTT 3.1.1 CONNECT: clean session, no keep alive.
func connectV311(clientID string) []byte {
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x02, 0, 0}
	body = append(body, byte(len(clientID)>>8), byte(len(clientID)))
	body = append(body, clientID...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

// readPacket reads one MQTT control packet: its first byte and its body.
func readPacket(r *bufio.Reader) (byte, []byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var length, shift int
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		length |= int(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		if shift += 7; shift > 21 {
			return 0, nil, errors.New("malformed remaining length")
		}
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return first, body, nil
}

// event is something the watcher saw, timed when the read that returned it completed.
// That is an application observation, never earlier than the bytes' arrival and
// possibly later, so a timing assertion on it is sound only as a lower bound.
type event struct {
	disconnect bool // a DISCONNECT packet; otherwise the connection ended
	reason     byte
	at         time.Time
}

// watcher is a raw MQTT 5.0 connection that reads continuously and records the first
// DISCONNECT and the end of the stream. A library client is not used for this: paho's
// server-disconnect callback runs after its own teardown.
type watcher struct {
	mu         sync.Mutex
	disconnect *event
	end        *event
	done       chan struct{}
}

func dialWatcher(t *testing.T, addr, clientID string) *watcher {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(connectV5(clientID)); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	typ, body, err := readPacket(r)
	if err != nil || typ != packetConnack || len(body) < 2 || body[1] != 0 {
		t.Fatalf("watcher CONNACK: type %#x body %x err %v", typ, body, err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	w := &watcher{done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			typ, body, err := readPacket(r)
			now := time.Now()
			w.mu.Lock()
			if err != nil {
				w.end = &event{at: now}
				w.mu.Unlock()
				return
			}
			if typ&0xf0 == packetDisconnect && w.disconnect == nil {
				e := &event{disconnect: true, at: now}
				if len(body) > 0 {
					e.reason = body[0]
				}
				w.disconnect = e
			}
			w.mu.Unlock()
		}
	}()
	return w
}

// firstEvent is the earliest of the DISCONNECT and the end of stream, or nil.
func (w *watcher) firstEvent() *event {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.disconnect != nil {
		return w.disconnect
	}
	return w.end
}

// waitDisconnect waits for the stream to end and returns the DISCONNECT seen before it.
func (w *watcher) waitDisconnect(t *testing.T, within time.Duration) *event {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(within):
		t.Fatalf("watcher connection still open after %s", within)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.disconnect == nil {
		t.Fatalf("connection ended without a DISCONNECT")
	}
	return w.disconnect
}

// roundTripper is an MQTT 5.0 client subscribed QoS 1 to its own topic, so a publish
// it makes comes back to it through the broker and JetStream.
type roundTripper struct {
	c     *paho.Client
	topic string
	got   chan string
}

func dialRoundTripper(t *testing.T, addr, clientID string) *roundTripper {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rt := &roundTripper{topic: "smoke/" + clientID, got: make(chan string, 16)}
	rt.c = paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
			rt.got <- string(pr.Packet.Payload)
			return true, nil
		}},
	})
	t.Cleanup(func() { _ = rt.c.Disconnect(&paho.Disconnect{}) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca, err := rt.c.Connect(ctx, &paho.Connect{ClientID: clientID, CleanStart: true, KeepAlive: 60})
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("connect: %v %+v", err, ca)
	}
	if _, err := rt.c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: rt.topic, QoS: 1}}}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	return rt
}

// roundTrip publishes payload QoS 1 and waits for it to come back.
func (rt *roundTripper) roundTrip(t *testing.T, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.c.Publish(ctx, &paho.Publish{Topic: rt.topic, QoS: 1, Payload: []byte(payload)}); err != nil {
		t.Fatalf("publish %q: %v", payload, err)
	}
	for {
		select {
		case got := <-rt.got:
			if got == payload {
				return
			}
		case <-ctx.Done():
			t.Fatalf("publish %q never came back", payload)
		}
	}
}

// tlsConnack performs an MQTT 3.1.1 CONNECT over TLS and returns the CONNACK return code.
func tlsConnack(t *testing.T, addr string, cfg *tls.Config) byte {
	t.Helper()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(connectV311("smoke-tls")); err != nil {
		t.Fatal(err)
	}
	typ, body, err := readPacket(bufio.NewReader(conn))
	if err != nil || typ != packetConnack || len(body) != 2 {
		t.Fatalf("CONNACK: type %#x body %x err %v", typ, body, err)
	}
	return body[1]
}
