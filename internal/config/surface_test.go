package config

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// The configuration surface is pinned by testdata/surface.txt: every setting's flag,
// variable, kind and built-in default, and the variables the shipped embedded
// configurations reference. The tests render it and compare bytes; they never write it.
// `make config-surface-fixture` regenerates it, with the change to the surface it records
// (docs/ARCHITECTURE.md, "The configuration surface").
const surfaceFixture = "testdata/surface.txt"

// embeddedConfigs are the nats-server configurations the embedded image and the chart ship,
// whose $MQTTNB_ references nats-server reads from the environment.
var embeddedConfigs = []string{"../../embedded/nats.conf", "../../embedded/nats-cluster.conf"}

var embeddedReference = regexp.MustCompile(`\$(MQTTNB_[A-Za-z0-9_]+)`)

// embeddedConfigVariables returns every variable the embedded configurations reference,
// sorted. Comments are skipped; any other MQTTNB_ text that is not a complete $ reference is
// an error rather than a name quietly left out.
func embeddedConfigVariables() ([]string, error) {
	names := map[string]bool{}
	for _, file := range embeddedConfigs {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		for i, line := range strings.Split(string(data), "\n") {
			if c := strings.Index(line, "#"); c >= 0 {
				line = line[:c]
			}
			refs := embeddedReference.FindAllStringSubmatch(line, -1)
			if strings.Count(line, "MQTTNB_") != len(refs) {
				return nil, fmt.Errorf("%s:%d: MQTTNB_ outside a $MQTTNB_ reference: %q", file, i+1, line)
			}
			for _, m := range refs {
				names[m[1]] = true
			}
		}
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// renderSurface renders the fixture's content from Surface and the embedded configurations.
func renderSurface() ([]byte, error) {
	var b bytes.Buffer
	b.WriteString("# flag\tenv\tkind\tdefault\n")
	for _, s := range Surface() {
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\n", s.Flag, s.Env, s.Kind, s.Default)
	}
	b.WriteString("# embedded configuration variables (read by nats-server, not by the broker)\n")
	names, err := embeddedConfigVariables()
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		b.WriteString(n + "\n")
	}
	return b.Bytes(), nil
}

func TestConfigurationSurfaceIsPinned(t *testing.T) {
	got, err := renderSurface()
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(surfaceFixture)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the configuration surface differs from %s; if the change is intended, run "+
			"`make config-surface-fixture` and update docs/CONFIGURATION.md with it. Rendered:\n%s", surfaceFixture, got)
	}
}

// TestWriteConfigurationSurfaceFixture regenerates the fixture, only when asked to.
func TestWriteConfigurationSurfaceFixture(t *testing.T) {
	if os.Getenv("MQTTNB_WRITE_CONFIG_SURFACE_FIXTURE") != "1" {
		t.Skip("set MQTTNB_WRITE_CONFIG_SURFACE_FIXTURE=1 (make config-surface-fixture) to regenerate " + surfaceFixture)
	}
	got, err := renderSurface()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(surfaceFixture, got, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestEverySettingHasAFlagAndAVariable: each entry has both names, none shared, each a
// distinct Config field of a supported kind.
func TestEverySettingHasAFlagAndAVariable(t *testing.T) {
	flags, envs, fields := map[string]bool{}, map[string]bool{}, map[any]bool{}
	c := Default()
	for _, s := range settings {
		if s.flag == "" || strings.HasPrefix(s.flag, "-") || !strings.HasPrefix(s.env, "MQTTNB_") || s.usage == "" {
			t.Errorf("setting %+v: want a flag without its dash, an MQTTNB_ variable and a usage", s)
		}
		if flags[s.flag] || envs[s.env] {
			t.Errorf("setting %s / %s: name defined twice", s.flag, s.env)
		}
		flags[s.flag], envs[s.env] = true, true
		p := s.field(c)
		switch p.(type) {
		case *string, *int, *bool, *time.Duration:
		default:
			t.Errorf("setting %s: unsupported field kind %T", s.flag, p)
		}
		if fields[p] {
			t.Errorf("setting %s: writes a field another setting writes", s.flag)
		}
		fields[p] = true
	}
}

// TestConfigurationDocumentNamesEverySetting: the flag table in docs/CONFIGURATION.md lists
// exactly the settings, one per row, in the table's order. Defaults and descriptions are
// prose; the fixture pins the defaults.
func TestConfigurationDocumentNamesEverySetting(t *testing.T) {
	data, err := os.ReadFile("../../docs/CONFIGURATION.md")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	start := -1
	for i, l := range lines {
		if l == "## Flags and environment variables" {
			if start >= 0 {
				t.Fatal("the flag table's heading appears twice")
			}
			start = i
		}
	}
	if start < 0 {
		t.Fatal("docs/CONFIGURATION.md has no \"## Flags and environment variables\" heading")
	}
	flagCell := regexp.MustCompile("^`-([a-z0-9-]+)`$")
	envCell := regexp.MustCompile("^`(MQTTNB_[A-Z0-9_]+)`$")
	var rows [][2]string
	i := start + 1
	for i < len(lines) && !strings.HasPrefix(lines[i], "|") {
		if strings.HasPrefix(lines[i], "#") {
			t.Fatal("no table under the flag table's heading")
		}
		i++
	}
	if i+1 >= len(lines) || !strings.HasPrefix(lines[i+1], "|--") {
		t.Fatal("the flag table has no header row")
	}
	for i += 2; i < len(lines) && strings.HasPrefix(lines[i], "|"); i++ {
		cells := strings.Split(lines[i], "|")
		if len(cells) < 4 {
			t.Fatalf("row %q: want Flag and Env cells", lines[i])
		}
		f := flagCell.FindStringSubmatch(strings.TrimSpace(cells[1]))
		e := envCell.FindStringSubmatch(strings.TrimSpace(cells[2]))
		if f == nil || e == nil {
			t.Fatalf("row %q: the Flag and Env cells must each be exactly one backticked name", lines[i])
		}
		rows = append(rows, [2]string{f[1], e[1]})
	}
	var want [][2]string
	for _, s := range Surface() {
		want = append(want, [2]string{s.Flag, s.Env})
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("docs/CONFIGURATION.md lists\n%v\nwant, in order,\n%v", rows, want)
	}
}

// TestLoadRefusesMalformedEnvironment: a variable that does not parse stops Load, naming
// itself, even when its flag is also given; a string variable takes any value.
func TestLoadRefusesMalformedEnvironment(t *testing.T) {
	flagValue := map[string]string{"int": "2", "bool": "true", "duration": "2s"}
	for _, s := range Surface() {
		if s.Kind == "string" {
			continue
		}
		for _, bad := range []string{"abc", ""} {
			t.Run(s.Env+"="+bad, func(t *testing.T) {
				t.Setenv(s.Env, bad)
				for _, args := range [][]string{nil, {"-" + s.Flag + "=" + flagValue[s.Kind]}} {
					_, err := Load(args)
					if err == nil || !strings.HasPrefix(err.Error(), "invalid "+s.Env+": ") {
						t.Errorf("Load(%q) with %s=%q = %v; want it refused naming %s", args, s.Env, bad, err, s.Env)
					}
				}
			})
		}
	}
}

// TestEverySettingReachesItsField: each setting, given as a variable and as a flag, lands in
// the Config field written here by hand, independent of the settings table, and nowhere
// else. Each row carries the settings Validate requires beside it; every value is distinct,
// so a binding swapped between two settings misplaces one and fails.
func TestEverySettingReachesItsField(t *testing.T) {
	type input struct {
		flag, env, value string
		set              func(*Config)
	}
	in := map[string]input{}
	for _, v := range []input{
		{"nats", "MQTTNB_NATS_URLS", "nats://n1:4222", func(c *Config) { c.NATSURLs = "nats://n1:4222" }},
		{"nats-embedded-config", "MQTTNB_NATS_EMBEDDED_CONFIG", "/e.conf", func(c *Config) { c.NATSEmbeddedConfig = "/e.conf" }},
		{"nats-creds", "MQTTNB_NATS_CREDS", "/n.creds", func(c *Config) { c.NATSCreds = "/n.creds" }},
		{"nats-user", "MQTTNB_NATS_USER", "nuser", func(c *Config) { c.NATSUser = "nuser" }},
		{"nats-pass", "MQTTNB_NATS_PASS", "npass", func(c *Config) { c.NATSPass = "npass" }},
		{"nats-name", "MQTTNB_NATS_NAME", "nname", func(c *Config) { c.NATSName = "nname" }},
		{"js-domain", "MQTTNB_JS_DOMAIN", "dom", func(c *Config) { c.JSDomain = "dom" }},
		{"stream-replicas", "MQTTNB_STREAM_REPLICAS", "5", func(c *Config) { c.StreamReps = 5 }},
		{"host", "MQTTNB_HOST", "127.0.0.9", func(c *Config) { c.MQTTHost = "127.0.0.9" }},
		{"port", "MQTTNB_PORT", "1001", func(c *Config) { c.MQTTPort = 1001 }},
		{"max-conns", "MQTTNB_MAX_CONNS", "1002", func(c *Config) { c.MaxConns = 1002 }},
		{"connect-timeout", "MQTTNB_CONNECT_TIMEOUT", "11s", func(c *Config) { c.ConnectTimeout = 11 * time.Second }},
		{"drain-delay", "MQTTNB_DRAIN_DELAY", "12s", func(c *Config) { c.DrainDelay = 12 * time.Second }},
		{"auth-passthrough", "MQTTNB_AUTH_PASSTHROUGH", "false", func(c *Config) { c.AuthPassthrough = false }},
		{"max-ack-pending", "MQTTNB_MAX_ACK_PENDING", "1003", func(c *Config) { c.MaxAckPending = 1003 }},
		{"default-user", "MQTTNB_DEFAULT_USER", "duser", func(c *Config) { c.DefaultUser = "duser" }},
		{"default-pass", "MQTTNB_DEFAULT_PASS", "dpass", func(c *Config) { c.DefaultPass = "dpass" }},
		{"keepalive-max", "MQTTNB_KEEPALIVE_MAX", "1004", func(c *Config) { c.KeepAliveMax = 1004 }},
		{"response-info", "MQTTNB_RESPONSE_INFO", "resp/", func(c *Config) { c.ResponseInfo = "resp/" }},
		{"tls-cert", "MQTTNB_TLS_CERT", "/c.pem", func(c *Config) { c.TLSCert = "/c.pem" }},
		{"tls-key", "MQTTNB_TLS_KEY", "/k.pem", func(c *Config) { c.TLSKey = "/k.pem" }},
		{"plain-port", "MQTTNB_PLAIN_PORT", "1005", func(c *Config) { c.PlainPort = 1005 }},
		{"tls-ca", "MQTTNB_TLS_CA", "/ca.pem", func(c *Config) { c.TLSCA = "/ca.pem" }},
		{"tls-reload-interval", "MQTTNB_TLS_RELOAD_INTERVAL", "13s", func(c *Config) { c.TLSReloadInterval = 13 * time.Second }},
		{"ws-port", "MQTTNB_WS_PORT", "1006", func(c *Config) { c.WSPort = 1006 }},
		{"ws-host", "MQTTNB_WS_HOST", "127.0.0.8", func(c *Config) { c.WSHost = "127.0.0.8" }},
		{"ws-path", "MQTTNB_WS_PATH", "/ws", func(c *Config) { c.WSPath = "/ws" }},
		{"ws-handshake-timeout", "MQTTNB_WS_HANDSHAKE_TIMEOUT", "14s", func(c *Config) { c.WSHandshakeTimeout = 14 * time.Second }},
		{"ws-no-tls", "MQTTNB_WS_NO_TLS", "true", func(c *Config) { c.WSNoTLS = true }},
		{"ws-allowed-origins", "MQTTNB_WS_ALLOWED_ORIGINS", "https://o.example", func(c *Config) { c.WSAllowedOrigins = "https://o.example" }},
		{"log-level", "MQTTNB_LOG_LEVEL", "debug", func(c *Config) { c.LogLevel = "debug" }},
		{"http-addr", "MQTTNB_HTTP_ADDR", ":1007", func(c *Config) { c.HTTPAddr = ":1007" }},
		{"pprof-addr", "MQTTNB_PPROF_ADDR", "127.0.0.1:1008", func(c *Config) { c.PprofAddr = "127.0.0.1:1008" }},
	} {
		in[v.flag] = v
	}
	// requires names what Validate needs beside a setting; stream-replicas is needed by all.
	requires := map[string][]string{
		"tls-cert": {"tls-key"}, "tls-key": {"tls-cert"},
		"tls-ca": {"tls-cert", "tls-key"}, "plain-port": {"tls-cert", "tls-key"},
		"ws-host": {"ws-port"}, "ws-path": {"ws-port"}, "ws-handshake-timeout": {"ws-port"},
		"ws-no-tls": {"ws-port"}, "ws-allowed-origins": {"ws-port"},
	}
	var surface []string
	for _, s := range Surface() {
		surface = append(surface, s.Flag+" "+s.Env)
	}
	var written []string
	for _, v := range in {
		written = append(written, v.flag+" "+v.env)
	}
	sort.Strings(surface)
	sort.Strings(written)
	if !reflect.DeepEqual(surface, written) {
		t.Fatalf("this test's settings\n%v\ndiffer from the surface\n%v", written, surface)
	}
	for name := range in {
		group := append([]string{name}, requires[name]...)
		if name != "stream-replicas" {
			group = append(group, "stream-replicas")
		}
		want := Default()
		for _, g := range group {
			in[g].set(want)
		}
		t.Run(name+"/env", func(t *testing.T) {
			for _, s := range settings {
				t.Setenv(s.env, "") // records the value to restore
				os.Unsetenv(s.env)  //nolint:errcheck // a unset variable is not read
			}
			for _, g := range group {
				t.Setenv(in[g].env, in[g].value)
			}
			got, err := Load(nil)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Load from %v =\n%+v\nwant\n%+v", group, got, want)
			}
		})
		t.Run(name+"/flag", func(t *testing.T) {
			for _, s := range settings {
				t.Setenv(s.env, "") // records the value to restore
				os.Unsetenv(s.env)  //nolint:errcheck // a unset variable is not read
			}
			var args []string
			for _, g := range group {
				args = append(args, "-"+in[g].flag+"="+in[g].value)
			}
			got, err := Load(args)
			if err != nil {
				t.Fatalf("Load(%q): %v", args, err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Load(%q) =\n%+v\nwant\n%+v", args, got, want)
			}
		})
	}
}
