package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/nberlee/mqtt-nats-bridge/internal/config"
	"github.com/nberlee/mqtt-nats-bridge/internal/testcerts"
)

// tlsFiles mints a CA-signed server keypair on disk and returns the cert, key
// and CA paths.
func tlsFiles(t *testing.T) (certPath, keyPath, caPath string) {
	t.Helper()
	dir := t.TempDir()
	ca, err := testcerts.NewAuthority("test-ca")
	if err != nil {
		t.Fatalf("NewAuthority: %v", err)
	}
	certPEM, keyPEM, err := ca.Issue("server", "127.0.0.1", "localhost")
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
			wantSubstr: "load TLS keypair",
		},
		{
			name:       "missing key",
			mutate:     func(c *config.Config) { c.TLSCert, c.TLSKey = certPath, missing },
			wantSubstr: "load TLS keypair",
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
			wantSubstr: "load TLS keypair",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			tc.mutate(cfg)
			got, err := resolveTLS(cfg)
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
	tc, err := resolveTLS(cfg)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if tc == nil {
		t.Fatal("resolveTLS = nil, want a TLS config")
	}
	if len(tc.Certificates) != 1 {
		t.Errorf("Certificates = %d, want 1", len(tc.Certificates))
	}
	if tc.ClientCAs == nil {
		t.Error("ClientCAs = nil, want the configured client CA")
	}
}

func TestResolveTLSNoneConfigured(t *testing.T) {
	tc, err := resolveTLS(config.Default())
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if tc != nil {
		t.Errorf("resolveTLS = %v, want nil when no TLS is configured", tc)
	}
}

// TestMQTTOptionsTLSApplied drives the real startup path (config -> resolveTLS
// -> mqttOptions) so the TLS config and the listener options cannot disagree.
func TestMQTTOptionsTLSApplied(t *testing.T) {
	certPath, keyPath, _ := tlsFiles(t)

	cfg := config.Default()
	cfg.TLSCert, cfg.TLSKey = certPath, keyPath
	tlsCfg, err := resolveTLS(cfg)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	opts := mqttOptions(cfg, tlsCfg)
	if opts.MQTT.TLSConfig == nil {
		t.Error("opts.MQTT.TLSConfig = nil, want the resolved TLS config")
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
	tlsCfg, err := resolveTLS(cfg)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	opts := mqttOptions(cfg, tlsCfg)
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
