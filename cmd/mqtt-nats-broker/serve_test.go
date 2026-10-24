package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nberlee/mqtt-nats-broker/internal/config"
	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
)

// tlsFiles mints a CA-signed server keypair on disk and returns the cert, key
// and CA paths.
func tlsFiles(t *testing.T) (certPath, keyPath, caPath string) {
	t.Helper()
	return tlsFilesValid(t, time.Now().Add(-time.Minute), time.Now().Add(time.Hour))
}

// tlsFilesValid is tlsFiles with the server certificate's validity window.
func tlsFilesValid(t *testing.T, notBefore, notAfter time.Time) (certPath, keyPath, caPath string) {
	t.Helper()
	dir := t.TempDir()
	ca, err := testcerts.NewAuthority("test-ca")
	if err != nil {
		t.Fatalf("NewAuthority: %v", err)
	}
	certPEM, keyPEM, err := ca.IssueWithValidity("server", notBefore, notAfter, "127.0.0.1", "localhost")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	write := func(name string, data []byte) string {
		t.Helper()
		p, err := testcerts.Write(dir, name, data)
		if err != nil {
			t.Fatalf("Write %s: %v", name, err)
		}
		return p
	}
	return write("server.pem", certPEM), write("server.key", keyPEM), write("ca.pem", ca.CertPEM)
}

// TestResolveTLSFailsClosedOnBadFiles is the regression test for the fail-open
// startup bug: an unreadable certificate or key used to be swallowed, leaving
// the broker serving plaintext on a listener configured for TLS.
func TestResolveTLSFailsClosedOnBadFiles(t *testing.T) {
	certPath, keyPath, _ := tlsFiles(t)
	missing := filepath.Join(t.TempDir(), "missing.pem")

	// wantSubstr pins the underlying cause, not merely "some error": a defensive
	// nil-config check downstream would otherwise mask a swallowed error and let
	// this test pass while the real cause was discarded.
	cases := []struct {
		name       string
		mutate     func(*config.Config)
		wantSubstr string
	}{
		{
			name:       "missing cert",
			mutate:     func(c *config.Config) { c.TLSCert, c.TLSKey = missing, keyPath },
			wantSubstr: "read TLS certificate",
		},
		{
			name:       "missing key",
			mutate:     func(c *config.Config) { c.TLSCert, c.TLSKey = certPath, missing },
			wantSubstr: "read TLS key",
		},
		{
			name:       "missing ca",
			mutate:     func(c *config.Config) { c.TLSCert, c.TLSKey, c.TLSCA = certPath, keyPath, missing },
			wantSubstr: "read TLS CA",
		},
		{
			// A directory stands in for an unreadable file: readable path, not a
			// certificate.
			name:       "unreadable cert",
			mutate:     func(c *config.Config) { c.TLSCert, c.TLSKey = t.TempDir(), keyPath },
			wantSubstr: "read TLS certificate",
		},
		{
			// Refused at startup as at reload: a leaf outside its validity window
			// is never served.
			name: "expired cert",
			mutate: func(c *config.Config) {
				c.TLSCert, c.TLSKey, _ = tlsFilesValid(t, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
			},
			wantSubstr: "expired",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			tc.mutate(cfg)
			got, err := resolveTLS(cfg, nil)
			if err == nil {
				t.Fatalf("resolveTLS = %v, nil error; want startup to fail for %q", got, tc.name)
			}
			if got != nil {
				t.Errorf("resolveTLS = %v on error, want nil", got)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("resolveTLS error = %q, want it to report the cause (%q)", err, tc.wantSubstr)
			}
		})
	}
}

func TestResolveTLSValid(t *testing.T) {
	certPath, keyPath, caPath := tlsFiles(t)

	cfg := config.Default()
	cfg.TLSCert, cfg.TLSKey, cfg.TLSCA = certPath, keyPath, caPath
	src, err := resolveTLS(cfg, nil)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if src == nil {
		t.Fatal("resolveTLS = nil, want a TLS source")
	}
	tc, _, _ := src.Active()
	if len(tc.Certificates) != 1 {
		t.Errorf("Certificates = %d, want 1", len(tc.Certificates))
	}
	if tc.ClientCAs == nil {
		t.Error("ClientCAs = nil, want the configured client CA")
	}
}

func TestResolveTLSNoneConfigured(t *testing.T) {
	src, err := resolveTLS(config.Default(), nil)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if src != nil {
		t.Errorf("resolveTLS = %v, want nil when no TLS is configured", src)
	}
}

func TestMQTTOptionsDoNotEnableExperimentalSharedQoS2(t *testing.T) {
	opts := mqttOptions(config.Default(), nil)
	if opts.MQTT.ExperimentalSharedQoS2 {
		t.Fatal("production options enabled experimental shared QoS2")
	}
}

// TestMQTTOptionsTLSApplied drives the real startup path (config -> resolveTLS
// -> mqttOptions) so the TLS config and the listener options cannot disagree.
func TestMQTTOptionsTLSApplied(t *testing.T) {
	certPath, keyPath, _ := tlsFiles(t)

	cfg := config.Default()
	cfg.TLSCert, cfg.TLSKey = certPath, keyPath
	src, err := resolveTLS(cfg, nil)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	opts := mqttOptions(cfg, src)
	if opts.MQTT.TLSConfig != src.ServerConfig() {
		t.Error("opts.MQTT.TLSConfig is not the source's listener config, so a reload would not reach the listener")
	}
	// Pin the exact bound, not merely "positive": a regression to e.g. 0.01 would
	// stay positive (so the core's <= 0 fallback would not rescue it) while making
	// normal handshakes fail.
	if opts.MQTT.TLSTimeout != mqttTLSHandshakeTimeout {
		t.Errorf("opts.MQTT.TLSTimeout = %v, want %v", opts.MQTT.TLSTimeout, float64(mqttTLSHandshakeTimeout))
	}
}

// TestMQTTTLSHandshakeTimeoutValue pins the handshake bound itself. The MQTT core
// applies its own default only when the option is non-positive, so a small
// positive value here would silently shorten every handshake.
func TestMQTTTLSHandshakeTimeoutValue(t *testing.T) {
	if mqttTLSHandshakeTimeout != 2 {
		t.Errorf("mqttTLSHandshakeTimeout = %v, want 2 seconds", float64(mqttTLSHandshakeTimeout))
	}
}

func TestMQTTOptionsNoTLS(t *testing.T) {
	cfg := config.Default()
	src, err := resolveTLS(cfg, nil)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	opts := mqttOptions(cfg, src)
	if opts.MQTT.TLSConfig != nil {
		t.Error("opts.MQTT.TLSConfig != nil, want plaintext when no TLS is configured")
	}
	if opts.MQTT.TLSTimeout != 0 {
		t.Errorf("opts.MQTT.TLSTimeout = %v, want 0 when no TLS is configured", opts.MQTT.TLSTimeout)
	}
	if opts.MQTT.Port != cfg.MQTTPort {
		t.Errorf("opts.MQTT.Port = %d, want %d", opts.MQTT.Port, cfg.MQTTPort)
	}
}

// TestMQTTOptionsWireMaxConns pins -max-conns to the field the accept path reads. It used
// to be written to an MQTTOpts field nothing read, so the flag was silently ignored (F-052).
func TestMQTTOptionsWireMaxConns(t *testing.T) {
	cfg := config.Default()
	cfg.MaxConns = 7
	if got := mqttOptions(cfg, nil).MaxConn; got != cfg.MaxConns {
		t.Fatalf("opts.MaxConn = %d, want %d", got, cfg.MaxConns)
	}
}

// TestMQTTOptionsRefusedByTheCoreBoundary drives the command's options through the same
// validation Start runs. The KeepAliveMax wraparound values are the reason the value
// reaches the check unnarrowed: int32(4294967296) is 0 and would pass.
func TestMQTTOptionsRefusedByTheCoreBoundary(t *testing.T) {
	for name, mut := range map[string]func(*config.Config){
		"keepalive negative":      func(c *config.Config) { c.KeepAliveMax = -1 },
		"keepalive too big":       func(c *config.Config) { c.KeepAliveMax = 70000 },
		"keepalive wraps to 0":    func(c *config.Config) { c.KeepAliveMax = 4294967296 },
		"keepalive wraps to 1":    func(c *config.Config) { c.KeepAliveMax = 4294967297 },
		"keepalive negative wrap": func(c *config.Config) { c.KeepAliveMax = -4294967295 },
		"max conns negative":      func(c *config.Config) { c.MaxConns = -1 },
		"stream replicas negative": func(c *config.Config) {
			c.StreamReps = -1
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config.Default()
			mut(cfg)
			if err := mqtt.ValidateOptions(mqttOptions(cfg, nil)); err == nil {
				t.Fatal("ValidateOptions = nil, want a refusal")
			}
		})
	}
	if err := mqtt.ValidateOptions(mqttOptions(config.Default(), nil)); err != nil {
		t.Fatalf("default configuration refused: %v", err)
	}
}

// The WebSocket listener rides the MQTT listener's TLS source, so one reload serves both.
func TestMQTTOptionsWebSocket(t *testing.T) {
	cfg := config.Default()
	if ws := mqttOptions(cfg, nil).MQTT.Websocket; ws.Port != 0 || ws.Host != "" || ws.HandshakeTimeout != 0 {
		t.Fatalf("WebSocket options with -ws-port unset = %+v, want disabled and empty", ws)
	}
	if err := mqtt.ValidateOptions(mqttOptions(cfg, nil)); err != nil {
		t.Fatalf("defaults refused: %v", err)
	}

	certPath, keyPath, _ := tlsFiles(t)
	cfg.TLSCert, cfg.TLSKey = certPath, keyPath
	cfg.WSPort, cfg.WSPath, cfg.WSAllowedOrigins = 8083, "/ws", "https://a.example, https://b.example"
	src, err := resolveTLS(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := mqttOptions(cfg, src)
	ws := opts.MQTT.Websocket
	if ws.Port != 8083 || ws.Host != cfg.MQTTHost || ws.Path != "/ws" || ws.HandshakeTimeout != cfg.WSHandshakeTimeout || ws.NoTLS {
		t.Fatalf("WebSocket options = %+v", ws)
	}
	if len(ws.AllowedOrigins) != 2 || ws.AllowedOrigins[1] != "https://b.example" {
		t.Fatalf("AllowedOrigins = %q", ws.AllowedOrigins)
	}
	if opts.MQTT.TLSConfig != src.ServerConfig() {
		t.Fatal("the WebSocket listener does not share the reloading TLS config")
	}
	if err := mqtt.ValidateOptions(opts); err != nil {
		t.Fatalf("refused: %v", err)
	}

	cfg.WSHost, cfg.WSNoTLS = "10.0.0.1", true
	if ws := mqttOptions(cfg, src).MQTT.Websocket; ws.Host != "10.0.0.1" || !ws.NoTLS {
		t.Fatalf("WebSocket options = %+v", ws)
	}
	// -ws-no-tls without TLS reaches the core, which refuses it.
	cfg.TLSCert, cfg.TLSKey = "", ""
	if err := mqtt.ValidateOptions(mqttOptions(cfg, nil)); err == nil {
		t.Fatal("ws-no-tls without TLS was accepted")
	}
}
