// Package config defines the runtime configuration for the MQTT-NATS broker and
// parses it from flags and environment variables.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
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
	NATSURLs   string        // comma-separated NATS URLs (service connection)
	NATSCreds  string        // path to a NATS .creds file for the service connection
	NATSUser   string        // username for the service connection (user/password auth)
	NATSPass   string        // password for the service connection (user/password auth)
	NATSName   string        // connection name reported to NATS
	JSDomain   string        // optional JetStream domain; a single subject token (see validateJSDomain)
	JSTimeout  time.Duration // JetStream API request timeout
	StreamReps int           // JetStream stream replicas (0 = auto)
	// NATSEmbeddedConfig, when set, is a native nats-server configuration file: the
	// broker runs that server in-process and reaches it without a URL, so NATSURLs does
	// not apply and an explicit one is refused.
	NATSEmbeddedConfig string

	// MQTT listener.
	MQTTHost string
	MQTTPort int
	// PlainPort, when nonzero, adds a plaintext MQTT listener on MQTTHost beside the TLS
	// one on MQTTPort, so one broker serves 1883 and 8883 together. It exists only beside
	// TLS: without TLS, MQTTPort is already the plaintext listener.
	PlainPort int
	MaxConns  int
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
	// TLSReloadInterval is how often the TLS files are re-read; a valid
	// replacement serves new handshakes from then on (internal/tlsreload).
	TLSReloadInterval time.Duration

	// MQTT over WebSocket listener. WSPort 0 disables it; the other settings are refused
	// without it (mqtt.ValidateOptions). It serves TLS from the MQTT listener's material
	// unless WSNoTLS.
	WSHost             string // empty = MQTTHost
	WSPort             int
	WSPath             string // request path; empty = /mqtt
	WSHandshakeTimeout time.Duration
	WSNoTLS            bool   // plaintext WebSocket beside a TLS MQTT listener (TLS-terminating ingress)
	WSAllowedOrigins   string // comma-separated scheme://host[:port]; empty = same-origin

	// Observability.
	LogLevel string // debug|info|warn|error
	HTTPAddr string // health/metrics bind address (empty = disabled)
}

// Default returns a Config populated with sensible defaults.
func Default() *Config {
	return &Config{
		NATSURLs:           "nats://127.0.0.1:4222",
		NATSName:           "mqtt-nats-broker",
		JSTimeout:          5 * time.Second,
		MQTTHost:           "0.0.0.0",
		MQTTPort:           1883,
		MaxConns:           0,
		ConnectTimeout:     5 * time.Second,
		AuthPassthrough:    true,
		TLSReloadInterval:  10 * time.Second,
		WSHandshakeTimeout: 5 * time.Second,
		LogLevel:           "info",
		HTTPAddr:           ":8080",
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
	_, natsURLsExplicit := os.LookupEnv("MQTTNB_NATS_URLS")
	c.NATSURLs = env("MQTTNB_NATS_URLS", c.NATSURLs)
	c.NATSEmbeddedConfig = env("MQTTNB_NATS_EMBEDDED_CONFIG", c.NATSEmbeddedConfig)
	c.NATSCreds = env("MQTTNB_NATS_CREDS", c.NATSCreds)
	c.NATSUser = env("MQTTNB_NATS_USER", c.NATSUser)
	c.NATSPass = env("MQTTNB_NATS_PASS", c.NATSPass)
	c.NATSName = env("MQTTNB_NATS_NAME", c.NATSName)
	c.JSDomain = env("MQTTNB_JS_DOMAIN", c.JSDomain)
	c.StreamReps = envInt("MQTTNB_STREAM_REPLICAS", c.StreamReps)
	c.MQTTHost = env("MQTTNB_HOST", c.MQTTHost)
	c.MQTTPort = envInt("MQTTNB_PORT", c.MQTTPort)
	c.PlainPort = envInt("MQTTNB_PLAIN_PORT", c.PlainPort)
	c.MaxConns = envInt("MQTTNB_MAX_CONNS", c.MaxConns)
	c.KeepAliveMax = envInt("MQTTNB_KEEPALIVE_MAX", c.KeepAliveMax)
	c.ResponseInfo = env("MQTTNB_RESPONSE_INFO", c.ResponseInfo)
	c.AuthPassthrough = envBool("MQTTNB_AUTH_PASSTHROUGH", c.AuthPassthrough)
	c.DefaultUser = env("MQTTNB_DEFAULT_USER", c.DefaultUser)
	c.DefaultPass = env("MQTTNB_DEFAULT_PASS", c.DefaultPass)
	c.TLSCert = env("MQTTNB_TLS_CERT", c.TLSCert)
	c.TLSKey = env("MQTTNB_TLS_KEY", c.TLSKey)
	c.TLSCA = env("MQTTNB_TLS_CA", c.TLSCA)
	c.WSHost = env("MQTTNB_WS_HOST", c.WSHost)
	c.WSPort = envInt("MQTTNB_WS_PORT", c.WSPort)
	c.WSPath = env("MQTTNB_WS_PATH", c.WSPath)
	c.WSNoTLS = envBool("MQTTNB_WS_NO_TLS", c.WSNoTLS)
	c.WSAllowedOrigins = env("MQTTNB_WS_ALLOWED_ORIGINS", c.WSAllowedOrigins)
	c.LogLevel = env("MQTTNB_LOG_LEVEL", c.LogLevel)
	c.HTTPAddr = env("MQTTNB_HTTP_ADDR", c.HTTPAddr)
	var err error
	if c.DrainDelay, err = envDuration("MQTTNB_DRAIN_DELAY", c.DrainDelay); err != nil {
		return nil, err
	}
	if c.ConnectTimeout, err = envDuration("MQTTNB_CONNECT_TIMEOUT", c.ConnectTimeout); err != nil {
		return nil, err
	}
	if c.TLSReloadInterval, err = envDuration("MQTTNB_TLS_RELOAD_INTERVAL", c.TLSReloadInterval); err != nil {
		return nil, err
	}
	if c.WSHandshakeTimeout, err = envDuration("MQTTNB_WS_HANDSHAKE_TIMEOUT", c.WSHandshakeTimeout); err != nil {
		return nil, err
	}

	fs := flag.NewFlagSet("mqtt-nats-broker", flag.ContinueOnError)
	registerFlags(fs, c)
	// The usage text (-h, or any parse error) prints each flag's default, and the defaults
	// registered above are the environment's values, so it would print MQTTNB_NATS_PASS,
	// MQTTNB_DEFAULT_PASS or a credential in MQTTNB_NATS_URLS before anything is validated.
	// It shows the built-in defaults instead; parsing reads only the flags' values.
	builtIn := flag.NewFlagSet("", flag.ContinueOnError)
	registerFlags(builtIn, Default())
	fs.VisitAll(func(f *flag.Flag) { f.DefValue = builtIn.Lookup(f.Name).DefValue })
	// The flag package's own diagnostics quote the argument they reject, value included
	// ("bad flag syntax: ---nats-pass=...", "invalid value ... for flag"), so they are
	// discarded and the error is restated without it. The usage text is safe to print.
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		fs.SetOutput(os.Stderr)
		fs.Usage()
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		return nil, commandLineError(fs, err)
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "nats" {
			natsURLsExplicit = true
		}
	})
	// NATSURLs has a default, so only an operator's own value says which server they
	// meant; with an embedded server it would be silently ignored.
	if c.NATSEmbeddedConfig != "" && natsURLsExplicit {
		return nil, fmt.Errorf("nats (MQTTNB_NATS_URLS) does not apply with nats-embedded-config: the embedded server is reached in-process")
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// commandLineError restates a flag parse error without any argument text: the kind of
// failure, and the flag's name only when it is one fs defines (an undefined "name" may
// be a mistyped value).
func commandLineError(fs *flag.FlagSet, err error) error {
	msg := err.Error()
	for _, kind := range []string{
		"bad flag syntax", "flag provided but not defined", "flag needs an argument",
		"invalid boolean value", "invalid boolean flag", "invalid value",
	} {
		if !strings.HasPrefix(msg, kind) {
			continue
		}
		if i := strings.LastIndex(msg, " -"); i >= 0 {
			name, _, _ := strings.Cut(msg[i+2:], ":")
			if fs.Lookup(name) != nil {
				return fmt.Errorf("invalid command line: %s (-%s)", kind, name)
			}
		}
		return fmt.Errorf("invalid command line: %s", kind)
	}
	return errors.New("invalid command line")
}

// registerFlags defines every flag on fs, each defaulting to and writing into c.
func registerFlags(fs *flag.FlagSet, c *Config) {
	fs.StringVar(&c.NATSURLs, "nats", c.NATSURLs, "comma-separated NATS server URLs")
	fs.StringVar(&c.NATSEmbeddedConfig, "nats-embedded-config", c.NATSEmbeddedConfig, "run an embedded nats-server from this native configuration file instead of connecting to -nats")
	fs.StringVar(&c.NATSCreds, "nats-creds", c.NATSCreds, "path to NATS .creds file")
	fs.StringVar(&c.NATSUser, "nats-user", c.NATSUser, "NATS username for the service connection")
	fs.StringVar(&c.NATSPass, "nats-pass", c.NATSPass, "NATS password for the service connection")
	fs.StringVar(&c.NATSName, "nats-name", c.NATSName, "NATS connection name")
	fs.StringVar(&c.JSDomain, "js-domain", c.JSDomain, "JetStream domain, a single subject token (optional; empty=the connected server's domain)")
	fs.IntVar(&c.StreamReps, "stream-replicas", c.StreamReps, "JetStream stream replicas (0=auto)")
	fs.StringVar(&c.MQTTHost, "host", c.MQTTHost, "MQTT listen host")
	fs.IntVar(&c.MQTTPort, "port", c.MQTTPort, "MQTT listen port")
	fs.IntVar(&c.PlainPort, "plain-port", c.PlainPort, "additional plaintext MQTT listen port beside the TLS -port (0=disabled; requires -tls-cert/-tls-key)")
	fs.IntVar(&c.MaxConns, "max-conns", c.MaxConns, "max concurrent MQTT clients (0=unlimited)")
	fs.IntVar(&c.KeepAliveMax, "keepalive-max", c.KeepAliveMax, "MQTT 5.0 Server Keep Alive cap in seconds (0=no override)")
	fs.StringVar(&c.ResponseInfo, "response-info", c.ResponseInfo, "MQTT 5.0 Response Information base topic (empty=never sent)")
	fs.BoolVar(&c.AuthPassthrough, "auth-passthrough", c.AuthPassthrough, "dial per-client NATS connection using MQTT credentials")
	fs.StringVar(&c.DefaultUser, "default-user", c.DefaultUser, "NATS user for anonymous MQTT clients")
	fs.StringVar(&c.DefaultPass, "default-pass", c.DefaultPass, "NATS password for anonymous MQTT clients")
	fs.StringVar(&c.TLSCert, "tls-cert", c.TLSCert, "TLS certificate file for the MQTT listener")
	fs.StringVar(&c.TLSKey, "tls-key", c.TLSKey, "TLS key file for the MQTT listener")
	fs.StringVar(&c.TLSCA, "tls-ca", c.TLSCA, "TLS client-CA file (enables mTLS)")
	fs.DurationVar(&c.TLSReloadInterval, "tls-reload-interval", c.TLSReloadInterval, "how often the TLS files are re-read for a replacement (must be > 0)")
	fs.StringVar(&c.WSHost, "ws-host", c.WSHost, "MQTT over WebSocket listen host (empty=the -host value)")
	fs.IntVar(&c.WSPort, "ws-port", c.WSPort, "MQTT over WebSocket listen port (0=disabled)")
	fs.StringVar(&c.WSPath, "ws-path", c.WSPath, "MQTT over WebSocket request path (empty=/mqtt)")
	fs.DurationVar(&c.WSHandshakeTimeout, "ws-handshake-timeout", c.WSHandshakeTimeout, "how long the WebSocket opening handshake may take (must be > 0)")
	fs.BoolVar(&c.WSNoTLS, "ws-no-tls", c.WSNoTLS, "serve plaintext WebSocket although TLS is configured (TLS terminated by an ingress; drops client-certificate verification for WebSocket clients)")
	fs.StringVar(&c.WSAllowedOrigins, "ws-allowed-origins", c.WSAllowedOrigins, "comma-separated browser origins allowed on the WebSocket listener, scheme://host[:port] (empty=same-origin only)")
	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "log level: debug|info|warn|error")
	fs.StringVar(&c.HTTPAddr, "http-addr", c.HTTPAddr, "health/metrics bind address (empty=disabled)")
	fs.DurationVar(&c.DrainDelay, "drain-delay", c.DrainDelay, "grace period after SIGTERM before disconnecting MQTT clients (0=immediate; set behind a load balancer / k8s)")
	fs.DurationVar(&c.ConnectTimeout, "connect-timeout", c.ConnectTimeout, "how long an accepted connection may send no CONNECT before it is closed (must be > 0)")

}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.NATSURLs) == "" {
		return fmt.Errorf("at least one NATS URL is required")
	}
	if err := validateNATSURLsCarryNoCredentials(c.NATSURLs); err != nil {
		return err
	}
	if c.MQTTPort <= 0 || c.MQTTPort > 65535 {
		return fmt.Errorf("invalid MQTT port %d", c.MQTTPort)
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
	// time.NewTicker panics on a nonpositive interval, and there is no "never
	// reload" setting to express with one.
	if c.TLSReloadInterval <= 0 {
		return fmt.Errorf("invalid tls-reload-interval %s (must be > 0)", c.TLSReloadInterval)
	}
	// The core reads -1 as an ephemeral port, which is not an operator setting.
	if c.PlainPort < 0 || c.PlainPort > 65535 {
		return fmt.Errorf("invalid plain-port %d (0 disables, else 1-65535)", c.PlainPort)
	}
	if c.PlainPort != 0 && !c.TLSEnabled() {
		return fmt.Errorf("plain-port requires tls-cert and tls-key: without TLS, port is already the plaintext listener")
	}
	if c.PlainPort != 0 && c.PlainPort == c.MQTTPort {
		return fmt.Errorf("plain-port %d is the TLS listener's port", c.PlainPort)
	}
	if c.WSPort < 0 || c.WSPort > 65535 {
		return fmt.Errorf("invalid ws-port %d (0 disables, else 1-65535)", c.WSPort)
	}
	// The core reads a nonpositive timeout as its default; a configured one must mean itself.
	if c.WSHandshakeTimeout <= 0 {
		return fmt.Errorf("invalid ws-handshake-timeout %s (must be > 0)", c.WSHandshakeTimeout)
	}
	// Only an enabled listener's settings reach the core, so without one they would be
	// dropped silently.
	if c.WSPort == 0 && (c.WSHost != "" || c.WSPath != "" || c.WSNoTLS || c.WSAllowedOrigins != "" ||
		c.WSHandshakeTimeout != Default().WSHandshakeTimeout) {
		return fmt.Errorf("ws-host, ws-path, ws-handshake-timeout, ws-no-tls and ws-allowed-origins require ws-port")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid log level %q", c.LogLevel)
	}
	return nil
}

// validateNATSURLsCarryNoCredentials refuses a NATS URL holding userinfo (user:password,
// or a token). nats.go presents a URL's userinfo in place of any credentials passed as
// options, and every per-client auth-passthrough connection dials these same URLs, so a
// credential here would authenticate every MQTT client as that one identity. It would
// also be inherited by every server the cluster advertises. The service identity is
// given by nats-user/nats-pass or nats-creds instead.
//
// Each entry is judged as nats.go reads it: split on commas, trimmed, a trailing slash
// dropped, and a scheme assumed when "://" is absent, since a scheme-less
// "user:pass@host:4222" still carries userinfo there. Errors name the entry by position,
// never by content: url.Parse's error quotes the whole URL, credential included.
func validateNATSURLsCarryNoCredentials(urls string) error {
	const flags = "nats (MQTTNB_NATS_URLS)"
	n := 0
	for _, entry := range strings.Split(urls, ",") {
		entry = strings.TrimSuffix(strings.TrimSpace(entry), "/")
		if entry == "" {
			continue
		}
		n++
		if !strings.Contains(entry, "://") {
			entry = "nats://" + entry
		}
		u, err := url.Parse(entry)
		if err != nil {
			return fmt.Errorf("%s URL #%d does not parse as a URL", flags, n)
		}
		if u.User != nil {
			return fmt.Errorf("%s URL #%d (%s://%s) carries credentials, which would replace every "+
				"auth-passthrough client's own; give the service identity with nats-user/nats-pass "+
				"(MQTTNB_NATS_USER/MQTTNB_NATS_PASS) or nats-creds (MQTTNB_NATS_CREDS)", flags, n, u.Scheme, u.Host)
		}
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

// WSAllowedOriginList returns the configured WebSocket origin allowlist as a slice.
func (c *Config) WSAllowedOriginList() []string {
	var out []string
	for _, p := range strings.Split(c.WSAllowedOrigins, ",") {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
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
