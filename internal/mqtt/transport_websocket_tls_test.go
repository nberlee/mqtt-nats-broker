//go:build mqttport_fulltests

package mqtt

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pmqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/nberlee/mqtt-nats-broker/internal/tlsreload"
)

// startWSSBroker starts a broker whose MQTT and WebSocket listeners both serve src's material.
func startWSSBroker(t *testing.T, src *tlsreload.Source, noTLS bool) (mqttAddr, wsAddr string) {
	t.Helper()
	_, wsAddr, s := startWSBroker(t, func(o *Options) {
		o.MQTT.TLSConfig = src.ServerConfig()
		o.MQTT.Websocket.NoTLS = noTLS
	})
	return s.Addr().String(), wsAddr
}

// wssDial makes a full TLS handshake (the client config holds no session cache) and upgrades
// over it, returning the client and the TLS layer whose served certificate a test inspects.
func wssDial(t *testing.T, addr string, cfg *tls.Config) (*wsTestClient, *tls.Conn, error) {
	t.Helper()
	tc, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		return nil, nil, err
	}
	c, err := wsTestUpgrade(tc, addr)
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() { tc.Close() })
	return c, tc, nil
}

func wssConnect(t *testing.T, addr string, cfg *tls.Config, id string) (*wsTestClient, *tls.Conn) {
	t.Helper()
	c, tc, err := wssDial(t, addr, cfg)
	if err != nil {
		t.Fatalf("wss dial: %v", err)
	}
	_, _ = c.Write(wsConnect311(id, 30, "", ""))
	wsExpectConnAck(t, c)
	return c, tc
}

// wsPublish311 is a QoS 0 PUBLISH.
func wsPublish311(topic, payload string) []byte {
	return wsMQTTPacket(0x30, append(wsMQTTString(topic), payload...))
}

// wsExpectPublish reads a SUBACK-free stream until a QoS 0 PUBLISH of payload arrives.
func wsExpectPublish(t *testing.T, c *wsTestClient, payload string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = c.SetReadDeadline(time.Time{}) }()
	for {
		var h [2]byte
		if _, err := io.ReadFull(c, h[:]); err != nil {
			t.Fatalf("waiting for %q: %v", payload, err)
		}
		body := make([]byte, h[1])
		if _, err := io.ReadFull(c, body); err != nil {
			t.Fatal(err)
		}
		if h[0]&0xF0 == 0x30 {
			tl := int(body[0])<<8 | int(body[1])
			if got := string(body[2+tl:]); got != payload {
				t.Fatalf("PUBLISH payload %q, want %q", got, payload)
			}
			return
		}
	}
}

// MQTT 3.1.1 and 5.0 over wss:// with real clients, served from the reloading source.
func TestWSSPahoClients(t *testing.T) {
	a := newTLSGeneration(t)
	src, err := tlsreload.Load(newTLSProjection(t, a.cert, a.key, nil).files(false), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, wsAddr := startWSSBroker(t, src, false)
	cfg := tlsReloadClient(nil, a)

	o := pmqtt.NewClientOptions().AddBroker("wss://" + wsAddr + "/mqtt").SetClientID("wss-311").
		SetProtocolVersion(4).SetTLSConfig(cfg).SetConnectTimeout(5 * time.Second).SetAutoReconnect(false)
	c311 := pmqtt.NewClient(o)
	if tok := c311.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("3.1.1 over wss: %v", tok.Error())
	}
	defer c311.Disconnect(100)
	ch := recvChan(t, c311, "wss/x", 1)

	ws, _, err := wssDial(t, wsAddr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	v5 := paho.NewClient(paho.ClientConfig{Conn: wsTestBridgeConn(t, ws)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if ca, err := v5.Connect(ctx, &paho.Connect{ClientID: "wss-v5", KeepAlive: 30, CleanStart: true}); err != nil || ca.ReasonCode != 0 {
		t.Fatalf("5.0 over wss: %v %+v", err, ca)
	}
	defer func() { _ = v5.Disconnect(&paho.Disconnect{}) }()
	if _, err := v5.Publish(ctx, &paho.Publish{Topic: "wss/x", QoS: 1, Payload: []byte("from-v5")}); err != nil {
		t.Fatal(err)
	}
	expectMsg(t, ch, "from-v5")
}

// A renewed certificate reaches the next WSS handshake while a connection admitted under the
// previous one stays connected and keeps exchanging messages: the WebSocket listener rides the
// same reloading source as the MQTT listener.
func TestWSSReloadKeepsExistingConnections(t *testing.T) {
	a, b := newTLSGeneration(t), newTLSGeneration(t)
	proj := newTLSProjection(t, a.cert, a.key, nil)
	src, err := tlsreload.Load(proj.files(false), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, wsAddr := startWSSBroker(t, src, false)
	cfg := tlsReloadClient(nil, a, b)

	ca, tca := wssConnect(t, wsAddr, cfg, "wss-a")
	if got, want := tca.ConnectionState().PeerCertificates[0].SerialNumber.String(), tlsSerialOf(t, a.cert); got != want {
		t.Fatalf("first client served %s, want %s", got, want)
	}
	_, _ = ca.Write(wsSubscribe311(1, "wss/a", 0))

	proj.swap(b.cert, b.key, nil)
	if out := src.Check(); out != tlsreload.Applied {
		t.Fatalf("Check = %v, want applied", out)
	}
	cb, tcb := wssConnect(t, wsAddr, cfg, "wss-b")
	if got, want := tcb.ConnectionState().PeerCertificates[0].SerialNumber.String(), tlsSerialOf(t, b.cert); got != want {
		t.Fatalf("a client connecting after the reload was served %s, want the renewal %s", got, want)
	}
	time.Sleep(200 * time.Millisecond) // the SUBSCRIBE above is processed
	_, _ = cb.Write(wsPublish311("wss/a", "to a"))
	wsExpectPublish(t, ca, "to a")

	// A refused reload (a key that does not match its certificate) keeps the material.
	proj.swap(a.cert, b.key, nil)
	if out := src.Check(); out == tlsreload.Applied {
		t.Fatal("a mismatched key was applied")
	}
	_, tcc := wssConnect(t, wsAddr, cfg, "wss-c")
	if got, want := tcc.ConnectionState().PeerCertificates[0].SerialNumber.String(), tlsSerialOf(t, b.cert); got != want {
		t.Fatalf("after a refused reload a client was served %s, want %s", got, want)
	}
}

// A client-CA rollover refuses a WSS client whose certificate the removed CA issued, admits one
// from the new CA, and leaves a client admitted under the removed CA connected.
func TestWSSClientCARollover(t *testing.T) {
	a, b := newTLSGeneration(t), newTLSGeneration(t)
	proj := newTLSProjection(t, a.cert, a.key, a.ca.CertPEM)
	src, err := tlsreload.Load(proj.files(true), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, wsAddr := startWSSBroker(t, src, false)

	old, _ := wssConnect(t, wsAddr, tlsReloadClient(&a.client, a), "ca-old")
	_, _ = old.Write(wsSubscribe311(1, "roll/old", 0))

	proj.swap(a.cert, a.key, b.ca.CertPEM)
	if out := src.Check(); out != tlsreload.Applied {
		t.Fatalf("Check = %v, want applied", out)
	}
	if c, _, err := wssDial(t, wsAddr, tlsReloadClient(&a.client, a)); err == nil {
		// TLS 1.3 reports a client-certificate refusal on the first read, here the 101.
		c.Raw.Close()
		t.Fatal("a client certificate from the removed CA was admitted")
	}
	nc, _ := wssConnect(t, wsAddr, tlsReloadClient(&b.client, a), "ca-new")
	time.Sleep(200 * time.Millisecond)
	_, _ = nc.Write(wsPublish311("roll/old", "still here"))
	wsExpectPublish(t, old, "still here")
}

// -ws-no-tls serves plaintext WebSocket beside a TLS MQTT listener, for an ingress that
// terminates TLS.
func TestWSNoTLSBesideATLSListener(t *testing.T) {
	a := newTLSGeneration(t)
	src, err := tlsreload.Load(newTLSProjection(t, a.cert, a.key, nil).files(false), nil)
	if err != nil {
		t.Fatal(err)
	}
	mqttAddr, wsAddr := startWSSBroker(t, src, true)
	c := wsTestDial(t, wsAddr)
	_, _ = c.Write(wsConnect311("plain-ws", 30, "", ""))
	wsExpectConnAck(t, c)

	raw, err := net.Dial("tcp", mqttAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_, _ = raw.Write(wsConnect311("plain-tcp", 30, "", ""))
	_ = raw.SetReadDeadline(time.Now().Add(3 * time.Second))
	var b [4]byte
	if _, err := io.ReadFull(raw, b[:]); err == nil && b == [4]byte{0x20, 0x02, 0x00, 0x00} {
		t.Fatal("the MQTT listener accepted plaintext while TLS is configured")
	}
}
