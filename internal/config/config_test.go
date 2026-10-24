package config

import (
	"testing"
	"time"
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
	if got := Default().NATSName; got != "mqtt-nats-broker" {
		t.Errorf("Default().NATSName = %q, want the product name %q", got, "mqtt-nats-broker")
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
	// The keepalive-max range is the MQTT core's, checked by mqtt.ValidateOptions
	// before startup (cmd/mqtt-nats-broker), not by Load.
}

func TestValidate(t *testing.T) {
	cases := map[string]func(*Config){
		"bad port":     func(c *Config) { c.MQTTPort = 0 },
		"empty nats":   func(c *Config) { c.NATSURLs = "" },
		"bad loglevel": func(c *Config) { c.LogLevel = "trace" },
		"tls half-set": func(c *Config) { c.TLSCert = "x" },
		"tls ca only":  func(c *Config) { c.TLSCA = "x" },
		"tls reload 0": func(c *Config) { c.TLSReloadInterval = 0 },
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

// TestValidateJSDomain pins the rule documented on validateJSDomain: a single
// subject token, rejecting the separator and wildcards plus every invisible
// character, while still accepting ordinary non-ASCII (which the NATS server
// itself accepts).
func TestValidateJSDomain(t *testing.T) {
	valid := []string{"", "HUB", "hub-1", "_x", "leaf_2", "京都"}
	for _, d := range valid {
		t.Run("valid/"+d, func(t *testing.T) {
			c := Default()
			c.JSDomain = d
			if err := c.Validate(); err != nil {
				t.Errorf("JSDomain %q: unexpected error: %v", d, err)
			}
		})
	}

	invalid := map[string]string{
		"dot":            "a.b",
		"star":           "a*",
		"gt":             "a>",
		"space":          "a b",
		"tab":            "a\tb",
		"newline":        "a\nb",
		"carriage":       "a\rb",
		"nul":            "a\x00b",
		"nbsp":           "a b",
		"leading space":  " HUB",
		"trailing space": "HUB ",
		"bad utf8":       "\xff\xfe",
	}
	for name, d := range invalid {
		t.Run("invalid/"+name, func(t *testing.T) {
			c := Default()
			c.JSDomain = d
			if err := c.Validate(); err == nil {
				t.Errorf("JSDomain %q: expected a validation error", d)
			}
		})
	}
}

// TestLoadJSDomain covers both configuration surfaces for the domain.
func TestLoadJSDomain(t *testing.T) {
	c, err := Load([]string{"-js-domain", "HUB"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.JSDomain != "HUB" {
		t.Errorf("JSDomain = %q, want HUB", c.JSDomain)
	}

	t.Setenv("MQTTNB_JS_DOMAIN", "SPOKE")
	c, err = Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.JSDomain != "SPOKE" {
		t.Errorf("JSDomain = %q, want SPOKE", c.JSDomain)
	}

	// The flag wins over the environment.
	c, err = Load([]string{"-js-domain", "HUB"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.JSDomain != "HUB" {
		t.Errorf("JSDomain = %q, want the flag to win over the env", c.JSDomain)
	}

	// Load validates, so a bad domain fails startup rather than surfacing later
	// as unroutable JS API subjects.
	if _, err := Load([]string{"-js-domain", "a.b"}); err == nil {
		t.Error("expected -js-domain a.b to be rejected at load")
	}
	t.Setenv("MQTTNB_JS_DOMAIN", "a b")
	if _, err := Load(nil); err == nil {
		t.Error("expected MQTTNB_JS_DOMAIN 'a b' to be rejected at load")
	}
}

func TestLoadTLSReloadInterval(t *testing.T) {
	c, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.TLSReloadInterval != 10*time.Second {
		t.Errorf("default TLSReloadInterval = %s, want 10s", c.TLSReloadInterval)
	}
	t.Setenv("MQTTNB_TLS_RELOAD_INTERVAL", "3s")
	if c, err = Load(nil); err != nil || c.TLSReloadInterval != 3*time.Second {
		t.Errorf("env TLSReloadInterval = %v (%v), want 3s", c, err)
	}
	if c, err = Load([]string{"-tls-reload-interval", "1m"}); err != nil || c.TLSReloadInterval != time.Minute {
		t.Errorf("flag TLSReloadInterval = %v (%v), want 1m", c, err)
	}
	if _, err := Load([]string{"-tls-reload-interval", "-1s"}); err == nil {
		t.Error("a negative tls-reload-interval was accepted")
	}
	t.Setenv("MQTTNB_TLS_RELOAD_INTERVAL", "soon")
	if _, err := Load(nil); err == nil {
		t.Error("a malformed MQTTNB_TLS_RELOAD_INTERVAL was accepted")
	}
}

func TestLoadWebSocket(t *testing.T) {
	c, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.WSPort != 0 || c.WSHost != "" || c.WSPath != "" || c.WSNoTLS || c.WSAllowedOrigins != "" || c.WSHandshakeTimeout != 5*time.Second {
		t.Errorf("WebSocket defaults = %+v", c)
	}
	t.Setenv("MQTTNB_WS_PORT", "8083")
	t.Setenv("MQTTNB_WS_HOST", "10.0.0.1")
	t.Setenv("MQTTNB_WS_PATH", "/ws")
	t.Setenv("MQTTNB_WS_HANDSHAKE_TIMEOUT", "3s")
	t.Setenv("MQTTNB_WS_NO_TLS", "true")
	t.Setenv("MQTTNB_WS_ALLOWED_ORIGINS", "https://a.example, https://b.example:8443")
	if c, err = Load(nil); err != nil {
		t.Fatalf("Load from env: %v", err)
	}
	if c.WSPort != 8083 || c.WSHost != "10.0.0.1" || c.WSPath != "/ws" || c.WSHandshakeTimeout != 3*time.Second || !c.WSNoTLS {
		t.Errorf("env WebSocket settings = %+v", c)
	}
	if got := c.WSAllowedOriginList(); len(got) != 2 || got[0] != "https://a.example" || got[1] != "https://b.example:8443" {
		t.Errorf("WSAllowedOriginList = %q", got)
	}
	c, err = Load([]string{"-ws-port", "9001", "-ws-path", "/mqtt-ws", "-ws-handshake-timeout", "1s", "-ws-no-tls=false", "-ws-allowed-origins", ""})
	if err != nil || c.WSPort != 9001 || c.WSPath != "/mqtt-ws" || c.WSHandshakeTimeout != time.Second || c.WSNoTLS || c.WSAllowedOriginList() != nil {
		t.Errorf("flag WebSocket settings = %+v (%v)", c, err)
	}
}

func TestValidateWebSocket(t *testing.T) {
	if _, err := Load([]string{"-ws-port", "8083", "-ws-host", "127.0.0.1", "-ws-path", "/x", "-ws-handshake-timeout", "1s", "-ws-no-tls", "-ws-allowed-origins", "https://a.example"}); err != nil {
		t.Fatalf("every WebSocket setting with -ws-port refused: %v", err)
	}
	for name, args := range map[string][]string{
		"port negative":          {"-ws-port", "-1"},
		"port too high":          {"-ws-port", "65536"},
		"timeout zero":           {"-ws-port", "8083", "-ws-handshake-timeout", "0s"},
		"timeout negative":       {"-ws-port", "8083", "-ws-handshake-timeout", "-1s"},
		"host without a port":    {"-ws-host", "127.0.0.1"},
		"path without a port":    {"-ws-path", "/x"},
		"timeout without a port": {"-ws-handshake-timeout", "1s"},
		"no-tls without a port":  {"-ws-no-tls"},
		"origins without a port": {"-ws-allowed-origins", "https://a.example"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(args); err == nil {
				t.Fatalf("Load(%q) accepted", args)
			}
		})
	}
	t.Setenv("MQTTNB_WS_HANDSHAKE_TIMEOUT", "soon")
	if _, err := Load(nil); err == nil {
		t.Error("a malformed MQTTNB_WS_HANDSHAKE_TIMEOUT was accepted")
	}
}
