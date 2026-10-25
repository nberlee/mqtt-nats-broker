package mqtt

import (
	"crypto/tls"
	"errors"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The F-052 boundary: every Options check runs through Start, the production entry point,
// against a real backend, so a mutant that skips or reorders validation starts a broker and
// fails the assertion instead of crashing on a missing connection.

// timeoutOverflowSeconds is the smallest float64 seconds value whose secondsToDuration product
// is 2^63, which overflows time.Duration; timeoutLargestSeconds is its float predecessor, whose
// product (2^63 - 1024) still fits.
var (
	timeoutOverflowSeconds = float64(0x1p63) / float64(time.Second)
	timeoutLargestSeconds  = math.Nextafter(timeoutOverflowSeconds, math.Inf(-1))
)

func validOptions() *Options {
	return &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}
}

func TestStartRefusesInvalidOptions(t *testing.T) {
	_, nc := runNATS(t)
	for _, tc := range []struct {
		name string
		mut  func(*Options)
		want string
	}{
		{"ack wait negative", func(o *Options) { o.MQTT.AckWait = -10 * time.Second }, errMQTTAckWaitMustBePositive.Error()},
		{"js api timeout negative", func(o *Options) { o.MQTT.JSAPITimeout = -10 * time.Second }, errMQTTJSAPITimeoutMustBePositive.Error()},
		{"topic alias maximum too high", func(o *Options) { o.MQTT.TopicAliasMaximum = 70000 }, "topic_alias_maximum must be in [-1..65535]"},
		{"topic alias maximum too low", func(o *Options) { o.MQTT.TopicAliasMaximum = -2 }, "topic_alias_maximum must be in [-1..65535]"},
		{"receive maximum too high", func(o *Options) { o.MQTT.ReceiveMaximum = 70000 }, "receive_maximum must be in [-1..65535]"},
		{"receive maximum too low", func(o *Options) { o.MQTT.ReceiveMaximum = -2 }, "receive_maximum must be in [-1..65535]"},
		{"keep alive maximum too high", func(o *Options) { o.MQTT.KeepAliveMaximum = 70000 }, "keep_alive_maximum must be in [0..65535]"},
		{"keep alive maximum wrapping width", func(o *Options) { o.MQTT.KeepAliveMaximum = 4294967296 }, "keep_alive_maximum must be in [0..65535]"},
		{"keep alive maximum negative", func(o *Options) { o.MQTT.KeepAliveMaximum = -1 }, "keep_alive_maximum must be in [0..65535]"},
		{"max conn negative", func(o *Options) { o.MaxConn = -1 }, "max_conn must be >= 0"},
		{"stream replicas negative", func(o *Options) { o.MQTT.StreamReplicas = -1 }, "stream_replicas must be >= 0"},
		{"auth timeout NaN", func(o *Options) { o.MQTT.AuthTimeout = math.NaN() }, "mqtt auth_timeout must be a finite number"},
		{"auth timeout +Inf", func(o *Options) { o.MQTT.AuthTimeout = math.Inf(1) }, "mqtt auth_timeout must be a finite number"},
		{"auth timeout -Inf", func(o *Options) { o.MQTT.AuthTimeout = math.Inf(-1) }, "mqtt auth_timeout must be a finite number"},
		{"auth timeout below 1ns", func(o *Options) { o.MQTT.AuthTimeout = 1e-10 }, "mqtt auth_timeout must be between 1ns"},
		{"auth timeout overflows", func(o *Options) { o.MQTT.AuthTimeout = timeoutOverflowSeconds }, "mqtt auth_timeout must be between 1ns"},
		{"top-level auth timeout NaN", func(o *Options) { o.AuthTimeout = math.NaN() }, "auth_timeout must be a finite number"},
		{"top-level auth timeout overflows", func(o *Options) { o.AuthTimeout = timeoutOverflowSeconds }, "auth_timeout must be between 1ns"},
		{"tls timeout NaN", func(o *Options) { o.MQTT.TLSTimeout = math.NaN() }, "mqtt tls_timeout must be a finite number"},
		{"tls timeout below 1ns", func(o *Options) { o.MQTT.TLSTimeout = 1e-10 }, "mqtt tls_timeout must be between 1ns"},
		{"pinned certificates", func(o *Options) { o.MQTT.TLSPinnedCerts = PinnedCertSet{"ab": {}} }, "pinning is not supported"},
		{"response information too long", func(o *Options) { o.MQTT.ResponseInformation = strings.Repeat("r", 0x10000) }, "response_information must be at most 65535 bytes"},
		{"response information invalid UTF-8", func(o *Options) { o.MQTT.ResponseInformation = "resp/\xff" }, "response_information is not a valid MQTT string"},
		{"response information NUL", func(o *Options) { o.MQTT.ResponseInformation = "resp/\x00" }, "response_information is not a valid MQTT string"},
		{"websocket port too low", func(o *Options) { o.MQTT.Websocket.Port = -2 }, "websocket port must be in [-1..65535]"},
		{"websocket port too high", func(o *Options) { o.MQTT.Websocket.Port = 65536 }, "websocket port must be in [-1..65535]"},
		{"websocket path without a listener", func(o *Options) { o.MQTT.Websocket.Path = "/x" }, "websocket listener is disabled"},
		{"websocket origins without a listener", func(o *Options) { o.MQTT.Websocket.AllowedOrigins = []string{"https://a"} }, "websocket listener is disabled"},
		{"websocket timeout without a listener", func(o *Options) { o.MQTT.Websocket.HandshakeTimeout = time.Second }, "websocket listener is disabled"},
		{"websocket host without a listener", func(o *Options) { o.MQTT.Websocket.Host = "127.0.0.1" }, "websocket listener is disabled"},
		{"websocket no-TLS without a listener", func(o *Options) { o.MQTT.Websocket.NoTLS = true }, "websocket listener is disabled"},
		{"websocket on the MQTT address", func(o *Options) {
			o.MQTT.Port = 18830
			o.MQTT.Websocket = WebsocketOpts{Host: "127.0.0.1", Port: 18830}
		}, "is the MQTT listener's address"},
		{"plaintext port too low", func(o *Options) { o.MQTT.TLSConfig = &tls.Config{}; o.MQTT.PlainPort = -2 }, "plaintext port must be in [-1..65535]"},
		{"plaintext port too high", func(o *Options) { o.MQTT.TLSConfig = &tls.Config{}; o.MQTT.PlainPort = 65536 }, "plaintext port must be in [-1..65535]"},
		{"plaintext port without TLS", func(o *Options) { o.MQTT.PlainPort = -1 }, "no TLS is configured"},
		{"plaintext port beside allow_non_tls", func(o *Options) {
			o.MQTT.TLSConfig = &tls.Config{}
			o.MQTT.PlainPort = -1
			o.AllowNonTLS = true
		}, "the TLS port stays TLS-only"},
		{"plaintext port on the MQTT address", func(o *Options) {
			o.MQTT.TLSConfig = &tls.Config{}
			o.MQTT.Port, o.MQTT.PlainPort = 18831, 18831
		}, "is the MQTT listener's address"},
		{"plaintext port on the websocket address", func(o *Options) {
			o.MQTT.TLSConfig = &tls.Config{}
			o.MQTT.PlainPort = 18832
			o.MQTT.Websocket = WebsocketOpts{Host: "127.0.0.1", Port: 18832}
		}, "is the websocket listener's address"},
		{"websocket path relative", func(o *Options) { o.MQTT.Websocket = WebsocketOpts{Port: -1, Path: "mqtt"} }, "websocket path"},
		{"websocket path with a query", func(o *Options) { o.MQTT.Websocket = WebsocketOpts{Port: -1, Path: "/mqtt?x"} }, "websocket path"},
		{"websocket path with a fragment", func(o *Options) { o.MQTT.Websocket = WebsocketOpts{Port: -1, Path: "/mqtt#x"} }, "websocket path"},
		{"websocket path not escaped", func(o *Options) { o.MQTT.Websocket = WebsocketOpts{Port: -1, Path: "/a b"} }, "websocket path"},
		{"websocket handshake timeout negative", func(o *Options) { o.MQTT.Websocket = WebsocketOpts{Port: -1, HandshakeTimeout: -time.Second} }, "handshake timeout must be >= 0"},
		{"websocket no-TLS without TLS", func(o *Options) { o.MQTT.Websocket = WebsocketOpts{Port: -1, NoTLS: true} }, "nothing to opt out of"},
		{"websocket origin with a path", func(o *Options) {
			o.MQTT.Websocket = WebsocketOpts{Port: -1, AllowedOrigins: []string{"https://a.example/"}}
		}, "allowed origin"},
		{"websocket origin ws scheme", func(o *Options) {
			o.MQTT.Websocket = WebsocketOpts{Port: -1, AllowedOrigins: []string{"ws://a.example"}}
		}, "allowed origin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := validOptions()
			tc.mut(o)
			s := NewServer(nc, o, "validate-refuse")
			err := s.Start()
			if s.Addr() != nil {
				s.Shutdown()
				t.Fatalf("Start opened a listener for an invalid configuration (err=%v)", err)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Start() = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestStartAcceptsBoundaryOptions(t *testing.T) {
	_, nc := runNATS(t)
	for _, tc := range []struct {
		name string
		mut  func(*Options)
	}{
		{"defaults", func(*Options) {}},
		{"topic alias maximum disabled", func(o *Options) { o.MQTT.TopicAliasMaximum = -1 }},
		{"topic alias maximum 65535", func(o *Options) { o.MQTT.TopicAliasMaximum = 0xFFFF }},
		{"receive maximum disabled", func(o *Options) { o.MQTT.ReceiveMaximum = -1 }},
		{"receive maximum 65535", func(o *Options) { o.MQTT.ReceiveMaximum = 0xFFFF }},
		{"keep alive maximum 65535", func(o *Options) { o.MQTT.KeepAliveMaximum = 0xFFFF }},
		{"max conn unlimited", func(o *Options) { o.MaxConn = 0 }},
		{"max conn 1", func(o *Options) { o.MaxConn = 1 }},
		{"stream replicas 1", func(o *Options) { o.MQTT.StreamReplicas = 1 }},
		{"auth timeout negative keeps the default", func(o *Options) { o.MQTT.AuthTimeout = -1 }},
		{"auth timeout 1ns", func(o *Options) { o.MQTT.AuthTimeout = 1e-9 }},
		{"auth timeout largest representable", func(o *Options) { o.MQTT.AuthTimeout = timeoutLargestSeconds }},
		{"tls timeout largest representable", func(o *Options) { o.MQTT.TLSTimeout = timeoutLargestSeconds }},
		{"empty pin set", func(o *Options) { o.MQTT.TLSPinnedCerts = PinnedCertSet{} }},
		{"response information at the length limit", func(o *Options) { o.MQTT.ResponseInformation = strings.Repeat("r", 0xFFFF) }},
		// -1 is an independent ephemeral allocation for each listener, never a collision.
		{"plaintext, TLS and websocket all ephemeral", func(o *Options) {
			o.MQTT.TLSConfig = &tls.Config{}
			o.MQTT.PlainPort = -1
			o.MQTT.Websocket = WebsocketOpts{Port: -1}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := validOptions()
			tc.mut(o)
			s := NewServer(nc, o, "validate-accept")
			if err := s.Start(); err != nil {
				s.Shutdown()
				t.Fatalf("Start() = %v, want a running broker", err)
			}
			s.Shutdown()
		})
	}
}

// TestStartValidatesBeforeAnySideEffect pins validation ahead of the two side effects Start
// has: the service identity request and the listener.
func TestStartValidatesBeforeAnySideEffect(t *testing.T) {
	srv, nc := runNATS(t)

	// (a) The service identity step runs when a dialer is set. A closed (not nil) service
	// connection makes that step fail cleanly, so validation running after it surfaces as an
	// identity error rather than the validation error.
	t.Run("before the service identity", func(t *testing.T) {
		closed, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		closed.Close()
		o := validOptions()
		o.MaxConn = -1
		s := NewServer(closed, o, "validate-identity")
		s.SetClientDialer(func(string, string, string) (*nats.Conn, error) {
			return nil, errors.New("the dialer must not be reached by a refused Start")
		})
		err = s.Start()
		if s.Addr() != nil {
			s.Shutdown()
		}
		if err == nil || !strings.Contains(err.Error(), "max_conn must be >= 0") {
			t.Fatalf("Start() = %v, want the validation error before the service identity", err)
		}
	})

	// (b) An ephemeral port, so the listener would bind if startMQTT ran first.
	t.Run("before the listener", func(t *testing.T) {
		o := &Options{MaxConn: -1, MQTT: MQTTOpts{Host: "127.0.0.1", Port: 0}}
		s := NewServer(nc, o, "validate-listener")
		err := s.Start()
		if s.Addr() != nil {
			s.Shutdown()
			t.Fatalf("Start opened a listener before refusing (err=%v)", err)
		}
		if err == nil || !strings.Contains(err.Error(), "max_conn must be >= 0") {
			t.Fatalf("Start() = %v, want the validation error", err)
		}
	})
}

// TestMaxConnRefusesBeyondTheBound proves -max-conns bounds admission. The third peer sends a
// valid CONNECT, and the CONNECT deadline is 30 s, so a close inside the 5 s read window can only
// be the admission refusal, never the deadline.
func TestMaxConnRefusesBeyondTheBound(t *testing.T) {
	s, addr := startConnectDeadlineBroker(t, func(o *Options) {
		o.MaxConn = 2
		o.MQTT.AuthTimeout = 30
	})
	first := dialSilent(t, addr)
	if _, err := first.Write(testConnectPacket("maxconn-1", 0)); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	expectConnAckAccepted(t, first)
	second := dialSilent(t, addr)
	if _, err := second.Write(testConnectPacket("maxconn-2", 0)); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	expectConnAckAccepted(t, second)

	third := dialSilent(t, addr)
	_, _ = third.Write(testConnectPacket("maxconn-3", 0)) // the broker may already have closed it
	_ = third.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [8]byte
	n, err := third.Read(buf[:])
	var ne net.Error
	switch {
	case err == nil && n > 0:
		t.Fatalf("a connection beyond MaxConn was served (first byte 0x%02x)", buf[0])
	case errors.As(err, &ne) && ne.Timeout():
		t.Fatal("a connection beyond MaxConn was left open, not refused")
	}

	// Freeing a slot readmits: wait for the broker to drop the first client, not merely for the
	// socket to close.
	_ = first.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		n := len(s.clients)
		s.mu.Unlock()
		if n <= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d clients still registered after the first disconnected", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	fourth := dialSilent(t, addr)
	if _, err := fourth.Write(testConnectPacket("maxconn-4", 0)); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	expectConnAckAccepted(t, fourth)
}
