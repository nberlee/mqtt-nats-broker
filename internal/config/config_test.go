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
