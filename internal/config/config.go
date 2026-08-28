// Package config defines the runtime configuration for the MQTT-NATS bridge and
// parses it from flags and environment variables.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Config is the full runtime configuration of the broker frontend.
type Config struct {
	// NATS backend.
	NATSURLs    string        // comma-separated NATS URLs (service connection)
	NATSCreds   string        // path to a NATS .creds file for the service connection
	NATSUser    string        // username for the service connection (user/password auth)
	NATSPass    string        // password for the service connection (user/password auth)
	NATSName    string        // connection name reported to NATS
	JSDomain    string        // optional JetStream domain; a single subject token (see validateJSDomain)
	JSTimeout   time.Duration // JetStream API request timeout
	StreamReps  int           // JetStream stream replicas (0 = auto)
	ConsumerRep int           // JetStream consumer replicas (0 = follow stream)

	// MQTT listener.
	MQTTHost string
	MQTTPort int
	MaxConns int
	// ConnectTimeout bounds how long an accepted connection may hold a socket, a
	// registration and a readLoop without having sent a CONNECT. Fail-closed:
	// there is no value that disables the bound, so 0 and negative are refused
	// rather than read as "unlimited".
	ConnectTimeout time.Duration

	// Shutdown.
	// DrainDelay is the grace period after SIGTERM before the broker disconnects
	// MQTT clients. Readiness flips to NotReady immediately; the delay lets a
	// load balancer / k8s Service deregister this instance before existing
	// connections are cut. 0 = drain immediately (correct for single-instance and
	// non-orchestrated deploys).
	DrainDelay time.Duration

	// MQTT 5.0 knobs.
	// KeepAliveMax caps the client-requested Keep Alive: when a v5 client requests
	// 0 or a value above this, the broker overrides it and returns the cap in a
	// CONNACK Server Keep Alive property. 0 = no override.
	KeepAliveMax int
	// ResponseInfo is the base response topic returned in a CONNACK Response
	// Information property when a v5 client sets Request Response Information = 1.
	// Empty = never sent.
	ResponseInfo string

	// Auth passthrough.
	// When true, MQTT username/password are used to dial a per-client NATS
	// connection so NATS enforces per-client authorization for SUBSCRIBE
	// admission, and so the broker can discover that client's account and
	// permissions and enforce them itself on everything that rides the service
	// connection. When a client connects without credentials,
	// DefaultUser/DefaultPass (if set) are used.
	//
	// The identity is discovered with $SYS.REQ.USER.INFO on the client's own
	// connection, so the MQTT identity must be permitted to publish that subject
	// and receive its reply.
	AuthPassthrough bool
	DefaultUser     string
	DefaultPass     string

	// TLS (MQTT listener).
	TLSCert string
	TLSKey  string
	TLSCA   string

	// Observability.
	LogLevel string // debug|info|warn|error
	HTTPAddr string // health/metrics bind address (empty = disabled)
}

// Default returns a Config populated with sensible defaults.
func Default() *Config {
	return &Config{
		NATSURLs:        "nats://127.0.0.1:4222",
		NATSName:        "mqtt-nats-bridge",
		JSTimeout:       5 * time.Second,
		MQTTHost:        "0.0.0.0",
		MQTTPort:        1883,
		MaxConns:        0,
		ConnectTimeout:  5 * time.Second,
		AuthPassthrough: true,
		LogLevel:        "info",
		HTTPAddr:        ":8080",
	}
}

// env returns the environment variable value or the provided fallback.
func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// envDuration reads a Go duration (e.g. "5s") from the environment. Unlike env,
// envInt, and envBool it does NOT silently fall back on a malformed value: a
// present-but-unparseable value is a configuration error the operator should see
// rather than have quietly ignored.
func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	if v, ok := os.LookupEnv(key); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", key, err)
		}
		return d, nil
	}
	return fallback, nil
}

// Load parses configuration from environment variables (MQTTNB_* prefix) first,
// then overlays any explicitly-provided command-line flags. Flags win over env.
func Load(args []string) (*Config, error) {
	c := Default()

	// Environment defaults (MQTTNB_ prefix).
	c.NATSURLs = env("MQTTNB_NATS_URLS", c.NATSURLs)
	c.NATSCreds = env("MQTTNB_NATS_CREDS", c.NATSCreds)
	c.NATSUser = env("MQTTNB_NATS_USER", c.NATSUser)
	c.NATSPass = env("MQTTNB_NATS_PASS", c.NATSPass)
	c.NATSName = env("MQTTNB_NATS_NAME", c.NATSName)
	c.JSDomain = env("MQTTNB_JS_DOMAIN", c.JSDomain)
	c.StreamReps = envInt("MQTTNB_STREAM_REPLICAS", c.StreamReps)
	c.ConsumerRep = envInt("MQTTNB_CONSUMER_REPLICAS", c.ConsumerRep)
	c.MQTTHost = env("MQTTNB_HOST", c.MQTTHost)
	c.MQTTPort = envInt("MQTTNB_PORT", c.MQTTPort)
	c.MaxConns = envInt("MQTTNB_MAX_CONNS", c.MaxConns)
	c.KeepAliveMax = envInt("MQTTNB_KEEPALIVE_MAX", c.KeepAliveMax)
	c.ResponseInfo = env("MQTTNB_RESPONSE_INFO", c.ResponseInfo)
	c.AuthPassthrough = envBool("MQTTNB_AUTH_PASSTHROUGH", c.AuthPassthrough)
	c.DefaultUser = env("MQTTNB_DEFAULT_USER", c.DefaultUser)
	c.DefaultPass = env("MQTTNB_DEFAULT_PASS", c.DefaultPass)
	c.TLSCert = env("MQTTNB_TLS_CERT", c.TLSCert)
	c.TLSKey = env("MQTTNB_TLS_KEY", c.TLSKey)
	c.TLSCA = env("MQTTNB_TLS_CA", c.TLSCA)
	c.LogLevel = env("MQTTNB_LOG_LEVEL", c.LogLevel)
	c.HTTPAddr = env("MQTTNB_HTTP_ADDR", c.HTTPAddr)
	var err error
	if c.DrainDelay, err = envDuration("MQTTNB_DRAIN_DELAY", c.DrainDelay); err != nil {
		return nil, err
	}
	if c.ConnectTimeout, err = envDuration("MQTTNB_CONNECT_TIMEOUT", c.ConnectTimeout); err != nil {
		return nil, err
	}

	fs := flag.NewFlagSet("mqtt-nats-bridge", flag.ContinueOnError)
	fs.StringVar(&c.NATSURLs, "nats", c.NATSURLs, "comma-separated NATS server URLs")
	fs.StringVar(&c.NATSCreds, "nats-creds", c.NATSCreds, "path to NATS .creds file")
	fs.StringVar(&c.NATSUser, "nats-user", c.NATSUser, "NATS username for the service connection")
	fs.StringVar(&c.NATSPass, "nats-pass", c.NATSPass, "NATS password for the service connection")
	fs.StringVar(&c.NATSName, "nats-name", c.NATSName, "NATS connection name")
	fs.StringVar(&c.JSDomain, "js-domain", c.JSDomain, "JetStream domain, a single subject token (optional; empty=the connected server's domain)")
	fs.IntVar(&c.StreamReps, "stream-replicas", c.StreamReps, "JetStream stream replicas (0=auto)")
	fs.IntVar(&c.ConsumerRep, "consumer-replicas", c.ConsumerRep, "JetStream consumer replicas (0=follow stream)")
	fs.StringVar(&c.MQTTHost, "host", c.MQTTHost, "MQTT listen host")
	fs.IntVar(&c.MQTTPort, "port", c.MQTTPort, "MQTT listen port")
	fs.IntVar(&c.MaxConns, "max-conns", c.MaxConns, "max concurrent MQTT clients (0=unlimited)")
	fs.IntVar(&c.KeepAliveMax, "keepalive-max", c.KeepAliveMax, "MQTT 5.0 Server Keep Alive cap in seconds (0=no override)")
	fs.StringVar(&c.ResponseInfo, "response-info", c.ResponseInfo, "MQTT 5.0 Response Information base topic (empty=never sent)")
	fs.BoolVar(&c.AuthPassthrough, "auth-passthrough", c.AuthPassthrough, "dial per-client NATS connection using MQTT credentials")
	fs.StringVar(&c.DefaultUser, "default-user", c.DefaultUser, "NATS user for anonymous MQTT clients")
	fs.StringVar(&c.DefaultPass, "default-pass", c.DefaultPass, "NATS password for anonymous MQTT clients")
	fs.StringVar(&c.TLSCert, "tls-cert", c.TLSCert, "TLS certificate file for the MQTT listener")
	fs.StringVar(&c.TLSKey, "tls-key", c.TLSKey, "TLS key file for the MQTT listener")
	fs.StringVar(&c.TLSCA, "tls-ca", c.TLSCA, "TLS client-CA file (enables mTLS)")
	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "log level: debug|info|warn|error")
	fs.StringVar(&c.HTTPAddr, "http-addr", c.HTTPAddr, "health/metrics bind address (empty=disabled)")
	fs.DurationVar(&c.DrainDelay, "drain-delay", c.DrainDelay, "grace period after SIGTERM before disconnecting MQTT clients (0=immediate; set behind a load balancer / k8s)")
	fs.DurationVar(&c.ConnectTimeout, "connect-timeout", c.ConnectTimeout, "how long an accepted connection may send no CONNECT before it is closed (must be > 0)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.NATSURLs) == "" {
		return fmt.Errorf("at least one NATS URL is required")
	}
	if c.MQTTPort <= 0 || c.MQTTPort > 65535 {
		return fmt.Errorf("invalid MQTT port %d", c.MQTTPort)
	}
	if c.KeepAliveMax < 0 || c.KeepAliveMax > 65535 {
		return fmt.Errorf("invalid keepalive-max %d (must be 0..65535, 0=no override)", c.KeepAliveMax)
	}
	if c.DrainDelay < 0 {
		return fmt.Errorf("invalid drain-delay %s (must be >= 0)", c.DrainDelay)
	}
	// The CONNECT bound is fail-closed, so "disabled" is not expressible: a 0 or
	// negative value would otherwise read as unlimited and restore exactly the
	// unbounded pre-CONNECT connection this bound exists to close.
	if c.ConnectTimeout <= 0 {
		return fmt.Errorf("invalid connect-timeout %s (must be > 0)", c.ConnectTimeout)
	}
	if err := validateJSDomain(c.JSDomain); err != nil {
		return err
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return fmt.Errorf("tls-cert and tls-key must be set together")
	}
	// A client CA on its own expresses mTLS intent but produces no TLS listener,
	// so accepting it would serve plaintext while the operator believes client
	// certificates are being verified.
	if c.TLSCA != "" && !c.TLSEnabled() {
		return fmt.Errorf("tls-ca requires tls-cert and tls-key")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid log level %q", c.LogLevel)
	}
	return nil
}

// validateJSDomain checks that a JetStream domain is usable as a single subject
// token. The value is interpolated into the JetStream API subjects
// ($JS.<domain>.API.*) and into the persisted MQTT session subject, so a bad one
// produces unroutable requests and a session namespace that can never be found
// again -- both of which fail silently and long after startup.
//
// This is deliberately narrower than the NATS server, which rejects ".", "*" and
// ">" but would accept a domain containing invisible whitespace or control
// characters. Such a domain looks correct in config and logs while not being
// what anyone typed. Ordinary non-ASCII is accepted, so a legitimate Unicode
// domain the server would take is not refused here.
func validateJSDomain(d string) error {
	if d == "" {
		return nil
	}
	const flags = "js-domain (MQTTNB_JS_DOMAIN)"
	if !utf8.ValidString(d) {
		return fmt.Errorf("invalid %s %q: must be valid UTF-8", flags, d)
	}
	for _, r := range d {
		switch {
		case r == '.' || r == '*' || r == '>':
			return fmt.Errorf("invalid %s %q: must be a single subject token, so it may not contain %q", flags, d, string(r))
		case unicode.IsSpace(r):
			return fmt.Errorf("invalid %s %q: may not contain whitespace (found %U)", flags, d, r)
		case unicode.IsControl(r):
			return fmt.Errorf("invalid %s %q: may not contain control characters (found %U)", flags, d, r)
		}
	}
	return nil
}

// TLSEnabled reports whether the MQTT listener should serve TLS.
func (c *Config) TLSEnabled() bool { return c.TLSCert != "" && c.TLSKey != "" }

// TLSConfig builds a *tls.Config for the MQTT listener from the configured files.
func (c *Config) TLSConfig() (*tls.Config, error) {
	if !c.TLSEnabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.TLSCert, c.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("load TLS keypair: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if c.TLSCA != "" {
		pem, err := os.ReadFile(c.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", c.TLSCA)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// NATSURLList returns the configured NATS URLs as a slice.
func (c *Config) NATSURLList() []string {
	parts := strings.Split(c.NATSURLs, ",")
	out := parts[:0]
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
