package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pmqtt "github.com/eclipse/paho.mqtt.golang"
)

// startWSBroker starts a broker with both listeners, the WebSocket one on an ephemeral port,
// and returns the TCP broker URL, the WebSocket host:port and the server.
func startWSBroker(t *testing.T, mod func(*Options)) (string, string, *Server) {
	t.Helper()
	_, nc := runNATS(t)
	opts := &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1, Websocket: WebsocketOpts{Host: "127.0.0.1", Port: -1}}}
	if mod != nil {
		mod(opts)
	}
	s := NewServer(nc, opts, "ws-node")
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	if s.WSAddr() == nil {
		t.Fatal("no WebSocket listener address")
	}
	return "tcp://" + s.Addr().String(), s.WSAddr().String(), s
}

// wsTestClient is a minimal RFC 6455 client presented as a net.Conn: Write sends one masked
// binary frame, Read yields binary payloads and ends at the server's Close. Raw is the socket,
// for writing control frames by hand.
type wsTestClient struct {
	Raw net.Conn
	br  *bufio.Reader
	buf []byte
	// pongs counts Pongs received.
	mu    sync.Mutex
	pongs int
}

func wsTestDial(t testing.TB, addr string, extra ...[2]string) *wsTestClient {
	t.Helper()
	c, err := wsTestDialErr(addr, extra...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Raw.Close() })
	return c
}

func wsTestDialErr(addr string, extra ...[2]string) (*wsTestClient, error) {
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return wsTestUpgrade(raw, addr, extra...)
}

// wsTestUpgrade runs the client side of the opening handshake over raw, which may be a TLS
// connection, and closes raw on failure.
func wsTestUpgrade(raw net.Conn, addr string, extra ...[2]string) (*wsTestClient, error) {
	req := wsValidRequest().set("Host", addr)
	for _, e := range extra {
		req.add(e[0], e[1])
	}
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := raw.Write(req.bytes()); err != nil {
		raw.Close()
		return nil, err
	}
	br := bufio.NewReader(raw)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		raw.Close()
		return nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 101 {
		raw.Close()
		return nil, fmt.Errorf("upgrade answered %d", resp.StatusCode)
	}
	_ = raw.SetDeadline(time.Time{})
	return &wsTestClient{Raw: raw, br: br}, nil
}

func (c *wsTestClient) Read(p []byte) (int, error) {
	for len(c.buf) == 0 {
		f, err := wsTestReadServerFrame(c.br)
		if err != nil {
			return 0, err
		}
		switch f.op {
		case wsBinaryMessage, wsContinuationFrame:
			c.buf = f.payload
		case wsPongMessage:
			c.mu.Lock()
			c.pongs++
			c.mu.Unlock()
		case wsCloseMessage:
			return 0, io.EOF
		}
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *wsTestClient) Write(p []byte) (int, error) {
	if _, err := c.Raw.Write(wsTestFrame{op: wsBinaryMessage, payload: p}.bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsTestClient) Close() error                       { return c.Raw.Close() }
func (c *wsTestClient) LocalAddr() net.Addr                { return c.Raw.LocalAddr() }
func (c *wsTestClient) RemoteAddr() net.Addr               { return c.Raw.RemoteAddr() }
func (c *wsTestClient) SetDeadline(t time.Time) error      { return c.Raw.SetDeadline(t) }
func (c *wsTestClient) SetReadDeadline(t time.Time) error  { return c.Raw.SetReadDeadline(t) }
func (c *wsTestClient) SetWriteDeadline(t time.Time) error { return c.Raw.SetWriteDeadline(t) }

// wsTestBridge hands a client library a real TCP socket and pumps its bytes into WebSocket
// frames as they are read, wherever that splits or merges packets [MQTT-6.0.0-2]. A library
// writing one packet as several Writes relies on the TCP socket's atomic vectored write; a
// net.Conn of our own could not offer it, and its pinger would interleave.
func wsTestBridge(t testing.TB, wsAddr string) net.Conn {
	t.Helper()
	return wsTestBridgeConn(t, wsTestDial(t, wsAddr))
}

func wsTestBridgeConn(t testing.TB, ws *wsTestClient) net.Conn {
	t.Helper()
	lib, pump := wsTestTCPPair(t)
	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, err := pump.Read(buf)
			if n > 0 {
				if _, werr := ws.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				_ = ws.Raw.Close()
				return
			}
		}
	}()
	go func() {
		_, _ = io.Copy(pump, ws)
		_ = pump.Close()
	}()
	return lib
}

// wsMQTTString is an MQTT UTF-8 string.
func wsMQTTString(s string) []byte {
	return append(binary.BigEndian.AppendUint16(nil, uint16(len(s))), s...)
}

// wsMQTTPacket frames an MQTT packet with its remaining length.
func wsMQTTPacket(first byte, body []byte) []byte {
	out := []byte{first}
	l := len(body)
	for {
		b := byte(l % 128)
		l /= 128
		if l > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if l == 0 {
			break
		}
	}
	return append(out, body...)
}

// wsConnect311 is an MQTT 3.1.1 CONNECT with a clean session and an optional QoS 0 Will.
func wsConnect311(id string, keepAlive uint16, willTopic, willMsg string) []byte {
	flags := byte(0x02)
	if willTopic != "" {
		flags |= 0x04
	}
	body := append(wsMQTTString("MQTT"), 4, flags)
	body = binary.BigEndian.AppendUint16(body, keepAlive)
	body = append(body, wsMQTTString(id)...)
	if willTopic != "" {
		body = append(body, wsMQTTString(willTopic)...)
		body = append(body, wsMQTTString(willMsg)...)
	}
	return wsMQTTPacket(0x10, body)
}

func wsSubscribe311(pid uint16, filter string, qos byte) []byte {
	body := binary.BigEndian.AppendUint16(nil, pid)
	body = append(body, wsMQTTString(filter)...)
	return wsMQTTPacket(0x82, append(body, qos))
}

// wsExpectConnAck reads a 3.1.1 CONNACK that accepted the connection.
func wsExpectConnAck(t testing.TB, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var b [4]byte
	if _, err := io.ReadFull(c, b[:]); err != nil {
		t.Fatalf("CONNACK: %v", err)
	}
	if b != [4]byte{0x20, 0x02, 0x00, 0x00} {
		t.Fatalf("CONNACK = %x", b)
	}
	_ = c.SetReadDeadline(time.Time{})
}

// wsClientCount is how many connections the server holds registered.
func wsClientCount(s *Server) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

func wsWaitClients(t testing.TB, s *Server, n int, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for wsClientCount(s) != n {
		if time.Since(start) > within {
			t.Fatalf("server holds %d connections after %v, want %d", wsClientCount(s), within, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Since(start)
}

func wsPahoClient(t *testing.T, broker, id string, clean bool, h pmqtt.MessageHandler) pmqtt.Client {
	t.Helper()
	o := pmqtt.NewClientOptions().AddBroker(broker).SetClientID(id).SetProtocolVersion(4).
		SetCleanSession(clean).SetConnectTimeout(5 * time.Second).SetAutoReconnect(false)
	if h != nil {
		o.SetDefaultPublishHandler(h)
	}
	c := pmqtt.NewClient(o)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s to %s: %v", id, broker, tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// MQTT 3.1.1 over WebSocket with a real client: QoS 1 and 2, retained, and a persistent
// session resumed over WebSocket.
func TestWSPahoV311(t *testing.T) {
	t.Parallel()
	_, wsAddr, _ := startWSBroker(t, nil)
	ws := "ws://" + wsAddr + "/mqtt"

	sub := wsPahoClient(t, ws, "ws-sub", true, nil)
	pub := wsPahoClient(t, ws, "ws-pub", true, nil)
	for _, qos := range []byte{1, 2} {
		ch := recvChan(t, sub, fmt.Sprintf("ws/q%d", qos), qos)
		mustPublish(t, pub, fmt.Sprintf("ws/q%d", qos), qos, false, fmt.Sprintf("hello-%d", qos))
		expectMsg(t, ch, fmt.Sprintf("hello-%d", qos))
	}

	mustPublish(t, pub, "ws/retained", 1, true, "kept")
	late := wsPahoClient(t, ws, "ws-late", true, nil)
	expectMsg(t, recvChan(t, late, "ws/retained", 1), "kept")

	ps := wsPahoClient(t, ws, "ws-persist", false, nil)
	if tok := ps.Subscribe("ws/persist", 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	ps.Disconnect(100)
	mustPublish(t, pub, "ws/persist", 1, false, "while-offline")
	got := make(chan string, 1)
	wsPahoClient(t, ws, "ws-persist", false, func(_ pmqtt.Client, m pmqtt.Message) { got <- string(m.Payload()) })
	expectMsg(t, got, "while-offline")
}

// MQTT 5.0 over WebSocket with a real client, a v5 property round-tripped.
func TestWSPahoV5(t *testing.T) {
	t.Parallel()
	_, wsAddr, _ := startWSBroker(t, nil)
	dial := func(id string, onPub func(paho.PublishReceived) (bool, error)) *paho.Client {
		cfg := paho.ClientConfig{Conn: wsTestBridge(t, wsAddr)}
		if onPub != nil {
			cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPub}
		}
		c := paho.NewClient(cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ca, err := c.Connect(ctx, &paho.Connect{ClientID: id, KeepAlive: 30, CleanStart: true})
		if err != nil || ca.ReasonCode != 0 {
			t.Fatalf("v5 connect over WebSocket: %v %+v", err, ca)
		}
		t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })
		return c
	}
	got := make(chan *paho.Publish, 1)
	sub := dial("ws-v5-sub", func(pr paho.PublishReceived) (bool, error) { got <- pr.Packet; return true, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "ws/v5", QoS: 1}}}); err != nil {
		t.Fatal(err)
	}
	pub := dial("ws-v5-pub", nil)
	if _, err := pub.Publish(ctx, &paho.Publish{Topic: "ws/v5", QoS: 1, Payload: []byte("v5"),
		Properties: &paho.PublishProperties{User: paho.UserProperties{{Key: "k", Value: "v"}}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if string(p.Payload) != "v5" || p.Properties == nil || p.Properties.User.Get("k") != "v" {
			t.Fatalf("got %q with properties %+v", p.Payload, p.Properties)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery")
	}
}

// MQTT 5.0 over WebSocket, the stateful half: QoS 2, retained delivery to a later
// subscriber, and a session with a nonzero expiry resumed over WebSocket with the message
// published while it was offline.
func TestWSPahoV5Session(t *testing.T) {
	t.Parallel()
	_, wsAddr, s := startWSBroker(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	expiry := uint32(300)
	dial := func(id string, clean bool, got chan<- *paho.Publish) (*paho.Client, *paho.Connack) {
		cfg := paho.ClientConfig{Conn: wsTestBridge(t, wsAddr)}
		if got != nil {
			cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) { got <- pr.Packet; return true, nil },
			}
		}
		c := paho.NewClient(cfg)
		ca, err := c.Connect(ctx, &paho.Connect{ClientID: id, KeepAlive: 30, CleanStart: clean,
			Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}})
		if err != nil || ca.ReasonCode != 0 {
			t.Fatalf("v5 connect %s over WebSocket: %v %+v", id, err, ca)
		}
		return c, ca
	}
	expect := func(ch <-chan *paho.Publish, topic, payload string) {
		t.Helper()
		select {
		case p := <-ch:
			if p.Topic != topic || string(p.Payload) != payload {
				t.Fatalf("got %s %q, want %s %q", p.Topic, p.Payload, topic, payload)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no delivery of %q on %s", payload, topic)
		}
	}

	pub, _ := dial("ws-v5s-pub", true, nil)
	defer func() { _ = pub.Disconnect(&paho.Disconnect{}) }()

	got := make(chan *paho.Publish, 8)
	sub, _ := dial("ws-v5s-sub", true, got)
	defer func() { _ = sub.Disconnect(&paho.Disconnect{}) }()
	if _, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "ws/v5s/q2", QoS: 2}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Publish(ctx, &paho.Publish{Topic: "ws/v5s/q2", QoS: 2, Payload: []byte("exactly-once")}); err != nil {
		t.Fatal(err)
	}
	expect(got, "ws/v5s/q2", "exactly-once")

	if _, err := pub.Publish(ctx, &paho.Publish{Topic: "ws/v5s/kept", QoS: 1, Retain: true, Payload: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	late := make(chan *paho.Publish, 8)
	lc, _ := dial("ws-v5s-late", true, late)
	defer func() { _ = lc.Disconnect(&paho.Disconnect{}) }()
	if _, err := lc.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "ws/v5s/kept", QoS: 1}}}); err != nil {
		t.Fatal(err)
	}
	expect(late, "ws/v5s/kept", "kept")

	ps, _ := dial("ws-v5s-persist", true, nil)
	if _, err := ps.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "ws/v5s/offline", QoS: 1}}}); err != nil {
		t.Fatal(err)
	}
	_ = ps.Disconnect(&paho.Disconnect{})
	// "Published while offline" means after the broker has processed the DISCONNECT, so wait
	// for that server-side fact: paho.golang's Disconnect returns once its bytes are written.
	// A publish racing that teardown is the residual docs/CONFORMANCE.md states ("A QoS 1
	// publish racing its subscriber's DISCONNECT waits for AckWait"), which this test
	// deliberately does not cover.
	for start := time.Now(); wsHasClient(s, "ws-v5s-persist"); time.Sleep(10 * time.Millisecond) {
		if time.Since(start) > 10*time.Second {
			t.Fatal("the disconnected session's connection is still registered")
		}
	}
	if _, err := pub.Publish(ctx, &paho.Publish{Topic: "ws/v5s/offline", QoS: 1, Payload: []byte("while-offline")}); err != nil {
		t.Fatal(err)
	}
	resumed := make(chan *paho.Publish, 8)
	rc, ca := dial("ws-v5s-persist", false, resumed)
	defer func() { _ = rc.Disconnect(&paho.Disconnect{}) }()
	if !ca.SessionPresent {
		t.Fatal("the session was not resumed over WebSocket (Session Present 0)")
	}
	expect(resumed, "ws/v5s/offline", "while-offline")
}

// The transport is below CONNECT: messages cross between TCP and WebSocket clients, and a
// WebSocket CONNECT takes over a TCP connection's client id.
func TestWSCrossTransportAndTakeover(t *testing.T) {
	t.Parallel()
	tcp, wsAddr, _ := startWSBroker(t, nil)
	ws := "ws://" + wsAddr + "/mqtt"
	tcpSub := wsPahoClient(t, tcp, "x-tcp-sub", true, nil)
	wsSub := wsPahoClient(t, ws, "x-ws-sub", true, nil)
	fromWS, fromTCP := recvChan(t, tcpSub, "x/from-ws", 1), recvChan(t, wsSub, "x/from-tcp", 1)
	mustPublish(t, wsPahoClient(t, ws, "x-ws-pub", true, nil), "x/from-ws", 1, false, "a")
	mustPublish(t, wsPahoClient(t, tcp, "x-tcp-pub", true, nil), "x/from-tcp", 1, false, "b")
	expectMsg(t, fromWS, "a")
	expectMsg(t, fromTCP, "b")

	lost := make(chan struct{})
	o := pmqtt.NewClientOptions().AddBroker(tcp).SetClientID("x-taken").SetProtocolVersion(4).
		SetAutoReconnect(false).SetConnectionLostHandler(func(pmqtt.Client, error) { close(lost) })
	old := pmqtt.NewClient(o)
	if tok := old.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	wsPahoClient(t, ws, "x-taken", true, nil)
	select {
	case <-lost:
	case <-time.After(10 * time.Second):
		t.Fatal("the TCP connection was not taken over by the WebSocket CONNECT")
	}
}

// A WebSocket Close without an MQTT DISCONNECT is a transport close, so the Will is
// published; DISCONNECT before the Close suppresses it.
func TestWSCloseFrameIsNotADisconnect(t *testing.T) {
	t.Parallel()
	tcp, wsAddr, _ := startWSBroker(t, nil)
	wills := recvChan(t, wsPahoClient(t, tcp, "will-watcher", true, nil), "will/+", 0)

	c := wsTestDial(t, wsAddr)
	_, _ = c.Write(wsConnect311("will-owner", 30, "will/a", "gone-a"))
	wsExpectConnAck(t, c)
	_, _ = c.Raw.Write(wsClose(wsStatus(1000)))
	expectMsg(t, wills, "gone-a")

	c2 := wsTestDial(t, wsAddr)
	_, _ = c2.Write(wsConnect311("will-owner-2", 30, "will/b", "gone-b"))
	wsExpectConnAck(t, c2)
	_, _ = c2.Write([]byte{0xE0, 0x00}) // DISCONNECT
	_, _ = c2.Raw.Write(wsClose(wsStatus(1000)))
	expectNoMsg(t, wills, time.Second)
}

// The core's inbound bound still judges a claim carried over WebSocket.
func TestWSOversizedConnectClaimRefused(t *testing.T) {
	t.Parallel()
	_, wsAddr, s := startWSBroker(t, nil)
	c := wsTestDial(t, wsAddr)
	wsWaitClients(t, s, 1, 5*time.Second)
	_, _ = c.Write([]byte{0x10, 0xFF, 0xFF, 0xFF, 0x7F})
	wsWaitClients(t, s, 0, 5*time.Second)
}

// WebSocket Pings do not keep a connection alive: before CONNECT the CONNECT deadline closes
// it, and after CONNECT the keep-alive bound does, whether or not the client reads its Pongs.
func TestWSPingsDoNotRefreshLiveness(t *testing.T) {
	t.Parallel()
	_, wsAddr, s := startWSBroker(t, func(o *Options) { o.MQTT.AuthTimeout = 0.5 })
	// op is a Ping, answered with a Pong (bounded by keep-alive itself), or an unsolicited
	// Pong, which writes nothing and so leaves keep-alive to the socket's read deadline alone.
	pinger := func(c *wsTestClient, stop chan struct{}, op wsOpCode) {
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
			if _, err := c.Raw.Write(wsTestFrame{op: op, payload: []byte("p")}.bytes()); err != nil {
				return
			}
			_, _ = c.Raw.Write(wsTestFrame{op: wsBinaryMessage}.bytes())
		}
	}

	c := wsTestDial(t, wsAddr)
	stop := make(chan struct{})
	defer close(stop)
	go pinger(c, stop, wsPingMessage)
	go func() { _, _ = io.Copy(io.Discard, c) }()
	if el := wsWaitClients(t, s, 0, 5*time.Second); el > 3*time.Second {
		t.Fatalf("a pinging, CONNECT-less connection lived %v past a 0.5 s CONNECT deadline", el)
	}

	c2 := wsTestDial(t, wsAddr)
	_, _ = c2.Write(wsConnect311("pinger", 1, "", ""))
	wsExpectConnAck(t, c2)
	stop2 := make(chan struct{})
	defer close(stop2)
	go pinger(c2, stop2, wsPingMessage)
	go func() { _, _ = io.Copy(io.Discard, c2) }()
	if el := wsWaitClients(t, s, 0, 6*time.Second); el > 4*time.Second {
		t.Fatalf("a pinging client with Keep Alive 1 lived %v", el)
	}

	c3 := wsTestDial(t, wsAddr)
	_, _ = c3.Write(wsConnect311("ponger", 1, "", ""))
	wsExpectConnAck(t, c3)
	stop3 := make(chan struct{})
	defer close(stop3)
	go pinger(c3, stop3, wsPongMessage)
	go func() { _, _ = io.Copy(io.Discard, c3) }()
	if el := wsWaitClients(t, s, 0, 6*time.Second); el > 4*time.Second {
		t.Fatalf("a client sending unsolicited Pongs with Keep Alive 1 lived %v", el)
	}
}

// A client that pings but never reads has its read side ended at the keep-alive bound, not at
// the write deadline: neither a blocked Pong nor a Pong waiting behind a blocked delivery
// outlasts keep-alive. What is measured is the start of the teardown (mqttHandleClosedClient
// entered), because that is what the transport owns; the teardown itself can then wait, on
// both transports alike, for a delivery holding c.mu in a blocked write, which the write
// deadline bounds (docs/ARCHITECTURE.md, "Every MQTT application write is deadline-bounded").
// Not parallel: it lengthens the package write deadline and installs a global hook.
func TestWSNonReadingPingerClosedAtKeepAlive(t *testing.T) {
	testShortWriteDeadline(t, 30*time.Second)
	for _, contended := range []bool{false, true} {
		t.Run(fmt.Sprintf("contended=%v", contended), func(t *testing.T) {
			tcp, wsAddr, s := startWSBroker(t, nil)
			entered := make(chan time.Time, 1)
			h := func(c *client) {
				if c.srv == s && c.mqtt != nil && c.mqtt.cid == "silent" {
					select {
					case entered <- time.Now():
					default:
					}
				}
			}
			mqttTestClosedClientEntryV.Store(&h)
			t.Cleanup(func() { mqttTestClosedClientEntryV.CompareAndSwap(&h, nil) })

			c := wsTestDial(t, wsAddr)
			_ = c.Raw.(*net.TCPConn).SetReadBuffer(4096)
			_, _ = c.Write(wsConnect311("silent", 1, "", ""))
			wsExpectConnAck(t, c)
			if contended {
				_, _ = c.Write(wsSubscribe311(1, "flood", 0))
				time.Sleep(200 * time.Millisecond)
				pub := wsPahoClient(t, tcp, "flooder", true, nil)
				big := bytes.Repeat([]byte("f"), 256<<10)
				for i := 0; i < 40; i++ {
					pub.Publish("flood", 0, false, big)
				}
				time.Sleep(300 * time.Millisecond)
			}
			stop := make(chan struct{})
			defer close(stop)
			start := time.Now()
			go func() {
				ping := wsPing(strings.Repeat("p", 125))
				for {
					select {
					case <-stop:
						return
					default:
					}
					if _, err := c.Raw.Write(ping); err != nil {
						return
					}
				}
			}()
			select {
			case at := <-entered:
				if el := at.Sub(start); el > 6*time.Second {
					t.Fatalf("read side ended after %v; keep-alive 1 s is 1.5 s, the write deadline 30 s", el)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("the non-reading pinger's read side did not end")
			}
		})
	}
}

// wsHasClient reports whether a connection with the given MQTT client id is registered.
func wsHasClient(s *Server, id string) bool {
	s.mu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()
	for _, c := range clients {
		c.mu.Lock()
		match := c.mqtt != nil && c.mqtt.cid == id
		c.mu.Unlock()
		if match {
			return true
		}
	}
	return false
}

// A peer stalled in the upgrade holds a registration, and shutdown still completes: the drain
// closes its socket. The handshake timeout is far longer than the assertion, so only the
// drain can be what ended it.
func TestWSShutdownDuringAStalledUpgrade(t *testing.T) {
	t.Parallel()
	_, wsAddr, s := startWSBroker(t, func(o *Options) { o.MQTT.Websocket.HandshakeTimeout = time.Minute })
	raw, err := net.Dial("tcp", wsAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_, _ = raw.Write([]byte("GET /mqtt HTTP/1.1\r\nHost: x\r\n"))
	wsWaitClients(t, s, 1, 5*time.Second)
	time.Sleep(100 * time.Millisecond)
	select {
	case <-s.BeginShutdown():
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete with an upgrade stalled")
	}
	if n := wsClientCount(s); n != 0 {
		t.Fatalf("%d connections still registered", n)
	}
	_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := raw.Read(make([]byte, 1)); err == nil || errors.Is(err, errors.ErrUnsupported) {
		t.Fatal("the stalled socket was not closed")
	}
}

// wsSetSetupStageHook observes one server's connection setup for one test. The hook is
// process-global, so its callers do not run in parallel (parallel_safety_test.go).
func wsSetSetupStageHook(t *testing.T, s *Server, fn func(c *client, stage string)) {
	h := func(c *client, stage string) {
		if c.srv == s {
			fn(c, stage)
		}
	}
	mqttTestSetupStageV.Store(&h)
	t.Cleanup(func() { mqttTestSetupStageV.CompareAndSwap(&h, nil) })
}

// wsFaultConn serves one upgrade request and fails the operation a test names.
type wsFaultConn struct {
	r          *bytes.Reader
	failSet    bool // SetDeadline with a nonzero time
	failClear  bool // SetDeadline with the zero time
	shortWrite bool // the 101
	mu         sync.Mutex
	closed     bool
}

func (c *wsFaultConn) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *wsFaultConn) Write(p []byte) (int, error) {
	if c.shortWrite {
		return len(p) / 2, errors.New("injected short write")
	}
	return len(p), nil
}
func (c *wsFaultConn) Close() error { c.mu.Lock(); c.closed = true; c.mu.Unlock(); return nil }
func (c *wsFaultConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
func (c *wsFaultConn) LocalAddr() net.Addr              { return testStubAddr{} }
func (c *wsFaultConn) RemoteAddr() net.Addr             { return testStubAddr{} }
func (c *wsFaultConn) SetReadDeadline(time.Time) error  { return nil }
func (c *wsFaultConn) SetWriteDeadline(time.Time) error { return nil }
func (c *wsFaultConn) SetDeadline(t time.Time) error {
	if (t.IsZero() && c.failClear) || (!t.IsZero() && c.failSet) {
		return errors.New("injected deadline failure")
	}
	return nil
}

// wsFaultListener hands out the given connections, then blocks until it is closed: the
// production accept loop, with a connection a test controls.
type wsFaultListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newWSFaultListener(conns ...net.Conn) *wsFaultListener {
	l := &wsFaultListener{conns: make(chan net.Conn, len(conns)), closed: make(chan struct{})}
	for _, c := range conns {
		l.conns <- c
	}
	return l
}

func (l *wsFaultListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *wsFaultListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *wsFaultListener) Addr() net.Addr { return testStubAddr{} }

// Every upgrade failure after registration ends with the socket closed, the client
// deregistered and no reader started. The connection arrives through acceptConnections, as
// production's do, so it holds a connSetupWG token as well as its clientWG one, and the
// shutdown that follows completing proves both were released. Not parallel: it installs a
// process-global hook.
func TestWSUpgradeFailuresReleaseSetup(t *testing.T) {
	for _, tc := range []struct {
		name string
		conn func() *wsFaultConn
	}{
		{"deadline install fails", func() *wsFaultConn { return &wsFaultConn{failSet: true} }},
		{"101 write torn", func() *wsFaultConn { return &wsFaultConn{shortWrite: true} }},
		{"deadline clear fails", func() *wsFaultConn { return &wsFaultConn{failClear: true} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, s := startWSBroker(t, nil)
			fc := tc.conn()
			fc.r = bytes.NewReader(wsValidRequest().bytes())
			var readers atomic.Int32
			wsSetSetupStageHook(t, s, func(c *client, stage string) {
				if stage == "reader" {
					readers.Add(1)
				}
			})
			cfg := s.mqttWSListenerConfig(&s.getOpts().MQTT)
			l := newWSFaultListener(fc)
			t.Cleanup(func() { _ = l.Close() })
			go s.acceptConnections(l, "fault", func(conn net.Conn) { s.createMQTTClient(conn, cfg) })

			deadline := time.Now().Add(5 * time.Second)
			for !fc.isClosed() {
				if time.Now().After(deadline) {
					t.Fatal("the failed setup did not close its socket")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if n := wsClientCount(s); n != 0 || readers.Load() != 0 {
				t.Fatalf("registered=%d readers=%d", n, readers.Load())
			}
			done := s.BeginShutdown()
			_ = l.Close() // shutdown closes only the listeners it owns
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not complete: a lifecycle token leaked")
			}
		})
	}
}

// Shutdown reaching a WebSocket connection at any setup stage still completes. Not
// parallel: it installs a process-global hook.
func TestWSShutdownAtEverySetupStage(t *testing.T) {
	for _, stage := range []string{"registered", "tls", "upgraded", "reader"} {
		t.Run(stage, func(t *testing.T) {
			_, wsAddr, s := startWSBroker(t, nil)
			reached, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			wsSetSetupStageHook(t, s, func(c *client, st string) {
				if st == stage {
					once.Do(func() { close(reached); <-release })
				}
			})
			go func() {
				if c, err := wsTestDialErr(wsAddr); err == nil {
					defer c.Raw.Close()
					_, _ = io.Copy(io.Discard, c.Raw)
				}
			}()
			<-reached
			done := s.BeginShutdown()
			close(release)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("shutdown parked at %q did not complete", stage)
			}
			if n := wsClientCount(s); n != 0 {
				t.Fatalf("%d connections still registered", n)
			}
		})
	}
}

// MaxConn bounds both listeners together.
func TestWSMaxConnCountsBothListeners(t *testing.T) {
	t.Parallel()
	tcp, wsAddr, _ := startWSBroker(t, func(o *Options) { o.MaxConn = 1 })
	wsPahoClient(t, tcp, "only-one", true, nil)
	if c, err := wsTestDialErr(wsAddr); err == nil {
		c.Raw.Close()
		t.Fatal("a WebSocket upgrade succeeded past MaxConn")
	}
}

// Start binds both listeners or neither, and shutdown closes both.
func TestWSListenerStartup(t *testing.T) {
	t.Parallel()
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mqttPort := free.Addr().(*net.TCPAddr).Port
	free.Close()

	_, nc := runNATS(t)
	s := NewServer(nc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: mqttPort,
		Websocket: WebsocketOpts{Host: "127.0.0.1", Port: occupied.Addr().(*net.TCPAddr).Port}}}, "ws-startup")
	if err := s.Start(); err == nil {
		s.Shutdown()
		t.Fatal("Start succeeded with the WebSocket port taken")
	}
	s.Shutdown()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", mqttPort))
	if err != nil {
		t.Fatalf("the MQTT port was left bound: %v", err)
	}
	l.Close()

	_, wsAddr, s2 := startWSBroker(t, nil)
	mqttAddr := s2.Addr().String()
	select {
	case <-s2.BeginShutdown():
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not complete")
	}
	for _, a := range []string{mqttAddr, wsAddr} {
		if c, err := net.DialTimeout("tcp", a, time.Second); err == nil {
			c.Close()
			t.Fatalf("%s still accepts after shutdown", a)
		}
	}
}
