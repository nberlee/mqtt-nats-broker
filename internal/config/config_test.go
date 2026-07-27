package config

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/nberlee/mqtt-nats-bridge/internal/testcerts"
)

func TestLoadDrainDelay(t *testing.T) {
	// Default is 0 (drain immediately).
	c, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DrainDelay != 0 {
		t.Errorf("default DrainDelay = %s, want 0", c.DrainDelay)
	}

	// Flag parses a Go duration.
	c, err = Load([]string{"-drain-delay", "5s"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DrainDelay != 5*time.Second {
		t.Errorf("DrainDelay = %s, want 5s", c.DrainDelay)
	}

	// Env parses a Go duration.
	t.Setenv("MQTTNB_DRAIN_DELAY", "250ms")
	c, err = Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DrainDelay != 250*time.Millisecond {
		t.Errorf("DrainDelay = %s, want 250ms", c.DrainDelay)
	}
}

func TestLoadDrainDelayMalformedEnvErrors(t *testing.T) {
	// A present-but-unparseable env value is a hard error, not a silent fallback.
	t.Setenv("MQTTNB_DRAIN_DELAY", "notaduration")
	if _, err := Load(nil); err == nil {
		t.Fatal("expected error for malformed MQTTNB_DRAIN_DELAY, got nil")
	}
}

func TestValidateRejectsNegativeDrainDelay(t *testing.T) {
	if _, err := Load([]string{"-drain-delay", "-1s"}); err == nil {
		t.Fatal("expected error for negative drain-delay, got nil")
	}
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.MQTTPort != 1883 {
		t.Errorf("MQTTPort = %d, want 1883", c.MQTTPort)
	}
	if !c.AuthPassthrough {
		t.Error("AuthPassthrough should default to true")
	}
}

func TestLoadFlagsOverride(t *testing.T) {
	c, err := Load([]string{"-port", "8883", "-log-level", "debug", "-nats", "nats://a:4222,nats://b:4222"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.MQTTPort != 8883 {
		t.Errorf("MQTTPort = %d, want 8883", c.MQTTPort)
	}
	if got := c.NATSURLList(); len(got) != 2 {
		t.Errorf("NATSURLList = %v, want 2 entries", got)
	}
}

func TestLoadV5Knobs(t *testing.T) {
	c, err := Load([]string{"-keepalive-max", "60", "-response-info", "resp/base"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.KeepAliveMax != 60 {
		t.Errorf("KeepAliveMax = %d, want 60", c.KeepAliveMax)
	}
	if c.ResponseInfo != "resp/base" {
		t.Errorf("ResponseInfo = %q, want %q", c.ResponseInfo, "resp/base")
	}
	c2, err := Load([]string{"-nats-user", "svc", "-nats-pass", "secret"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c2.NATSUser != "svc" || c2.NATSPass != "secret" {
		t.Errorf("NATSUser/NATSPass = %q/%q, want svc/secret", c2.NATSUser, c2.NATSPass)
	}
	if _, err := Load([]string{"-keepalive-max", "70000"}); err == nil {
		t.Error("expected validation error for keepalive-max out of range")
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]func(*Config){
		"bad port":          func(c *Config) { c.MQTTPort = 0 },
		"empty nats":        func(c *Config) { c.NATSURLs = "" },
		"bad loglevel":      func(c *Config) { c.LogLevel = "trace" },
		"tls half-set":      func(c *Config) { c.TLSCert = "x" },
		"tls ca only":       func(c *Config) { c.TLSCA = "x" },
		"keepalive neg":     func(c *Config) { c.KeepAliveMax = -1 },
		"keepalive too big": func(c *Config) { c.KeepAliveMax = 70000 },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			c := Default()
			mut(c)
			if err := c.Validate(); err == nil {
				t.Errorf("expected validation error for %q", name)
			}
		})
	}
}

// tlsFixture writes a CA plus a matching server keypair into a temp dir and
// returns their paths, along with an unrelated keypair whose key does not match
// the server certificate.
type tlsFixture struct {
	caPath, certPath, keyPath string
	otherKeyPath              string
	emptyCAPath               string
}

func newTLSFixture(t *testing.T) tlsFixture {
	t.Helper()
	dir := t.TempDir()
	ca, err := testcerts.NewAuthority("test-ca")
	if err != nil {
		t.Fatalf("NewAuthority: %v", err)
	}
	certPEM, keyPEM, err := ca.Issue("server", "127.0.0.1", "localhost")
	if err != nil {
		t.Fatalf("Issue server: %v", err)
	}
	_, otherKeyPEM, err := ca.Issue("other", "127.0.0.1")
	if err != nil {
		t.Fatalf("Issue other: %v", err)
	}
	var f tlsFixture
	write := func(name string, data []byte) string {
		t.Helper()
		p, err := testcerts.Write(dir, name, data)
		if err != nil {
			t.Fatalf("Write %s: %v", name, err)
		}
		return p
	}
	f.caPath = write("ca.pem", ca.CertPEM)
	f.certPath = write("server.pem", certPEM)
	f.keyPath = write("server.key", keyPEM)
	f.otherKeyPath = write("other.key", otherKeyPEM)
	f.emptyCAPath = write("empty-ca.pem", []byte("not a certificate\n"))
	return f
}

func TestTLSConfigDisabled(t *testing.T) {
	c := Default()
	tc, err := c.TLSConfig()
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	if tc != nil {
		t.Errorf("TLSConfig = %v, want nil when no TLS is configured", tc)
	}
}

func TestTLSConfigValid(t *testing.T) {
	f := newTLSFixture(t)

	c := Default()
	c.TLSCert, c.TLSKey = f.certPath, f.keyPath
	tc, err := c.TLSConfig()
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	if tc == nil || len(tc.Certificates) != 1 {
		t.Fatalf("TLSConfig = %v, want one certificate", tc)
	}
	if tc.ClientAuth != tls.NoClientCert {
		t.Errorf("ClientAuth = %v, want NoClientCert without a client CA", tc.ClientAuth)
	}

	// Adding the client CA must turn on verified mTLS.
	c.TLSCA = f.caPath
	tc, err = c.TLSConfig()
	if err != nil {
		t.Fatalf("TLSConfig with CA: %v", err)
	}
	if tc.ClientCAs == nil {
		t.Error("ClientCAs = nil, want the configured client CA")
	}
	if tc.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", tc.ClientAuth)
	}
}

// TestTLSConfigErrors covers the failure modes that must abort startup: none of
// them may yield a usable (plaintext-implying) nil config with a nil error.
func TestTLSConfigErrors(t *testing.T) {
	f := newTLSFixture(t)
	missing := f.certPath + ".missing"

	cases := map[string]func(*Config){
		"missing cert": func(c *Config) { c.TLSCert, c.TLSKey = missing, f.keyPath },
		"missing key":  func(c *Config) { c.TLSCert, c.TLSKey = f.certPath, missing },
		"key mismatch": func(c *Config) { c.TLSCert, c.TLSKey = f.certPath, f.otherKeyPath },
		"missing ca": func(c *Config) {
			c.TLSCert, c.TLSKey, c.TLSCA = f.certPath, f.keyPath, missing
		},
		"ca without certificates": func(c *Config) {
			c.TLSCert, c.TLSKey, c.TLSCA = f.certPath, f.keyPath, f.emptyCAPath
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			c := Default()
			mut(c)
			tc, err := c.TLSConfig()
			if err == nil {
				t.Fatalf("TLSConfig = %v, nil error; want an error for %q", tc, name)
			}
			if tc != nil {
				t.Errorf("TLSConfig = %v on error, want nil", tc)
			}
		})
	}
}
