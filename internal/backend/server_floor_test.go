package backend

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// TestVersionAtLeast pins the ordering with literal pairs, independent of the pin, so the
// rules hold whatever release the floor currently names.
func TestVersionAtLeast(t *testing.T) {
	for _, tc := range []struct {
		version, floor string
		want           bool
	}{
		{"2.15.2", "2.15.2", true},
		{"v2.15.2", "2.15.2", true},
		{"2.15.2+build.7", "2.15.2", true},
		{"2.15.3", "2.15.2", true},
		{"2.16.0", "2.15.2", true},
		{"3.0.0", "2.15.2", true},
		{"2.15.1", "2.15.2", false},
		{"2.14.9", "2.15.2", false},
		{"1.99.99", "2.15.2", false},
		// Numeric, not lexical.
		{"2.15.10", "2.15.9", true},
		{"2.15.9", "2.15.10", false},
		// A prerelease is below its own release and above every lower core.
		{"2.15.2-RC.1", "2.15.2", false},
		{"2.15.3-RC.1", "2.15.2", true},
		{"2.15.2", "2.15.2-RC.1", true},
		{"2.15.2-RC.1", "2.15.2-RC.1", true},
	} {
		got, err := versionAtLeast(tc.version, tc.floor)
		if err != nil || got != tc.want {
			t.Errorf("versionAtLeast(%q, %q) = %v, %v; want %v, nil", tc.version, tc.floor, got, err, tc.want)
		}
	}

	// Prerelease precedence, each pair asserted in both directions so a comparator that
	// answers one way regardless fails: lo is strictly below hi.
	for _, p := range []struct{ lo, hi string }{
		{"2.15.2-RC.1", "2.15.2-RC.2"},
		{"2.15.2-RC.2", "2.15.2-RC.10"},             // numeric identifiers numerically
		{"2.15.2-1", "2.15.2-10000000000000000000"}, // at any length
		{"2.15.2-RC.1", "2.15.2-preview.1"},         // alphanumeric in ASCII order
		{"2.15.2-1", "2.15.2-alpha"},                // numeric below alphanumeric
		{"2.15.2-alpha", "2.15.2-alpha.1"},          // a prefix below the longer list
	} {
		if got, err := versionAtLeast(p.lo, p.hi); err != nil || got {
			t.Errorf("versionAtLeast(%q, %q) = %v, %v; want false, nil", p.lo, p.hi, got, err)
		}
		if got, err := versionAtLeast(p.hi, p.lo); err != nil || !got {
			t.Errorf("versionAtLeast(%q, %q) = %v, %v; want true, nil", p.hi, p.lo, got, err)
		}
	}

	// Not a semantic version: an error, as the version and as the floor alike, never an
	// ordering.
	for _, bad := range []string{
		"", "2.15", "x.y.z", "2.15.-1", "2.15.2-", "2.15.2-RC..1", "02.15.2",
		"2.15.3-RC.01", "2.15.3-!", "2.15.2+", "2.15.2+a..b", "vv2.15.2",
		"99999999999999999999.0.0",
	} {
		if got, err := versionAtLeast(bad, "2.15.2"); err == nil {
			t.Errorf("versionAtLeast(%q, floor) = %v, nil; want an error", bad, got)
		}
		if got, err := versionAtLeast("2.15.2", bad); err == nil {
			t.Errorf("versionAtLeast(version, %q) = %v, nil; want an error", bad, got)
		}
	}
}

// TestConnectedServerFloor: the floor is the linked module's own version, and the dialed
// server must also carry header support.
func TestConnectedServerFloor(t *testing.T) {
	if err := connectedServerMeetsFloor(natsserver.VERSION, true); err != nil {
		t.Fatalf("the pinned version itself: %v", err)
	}
	if err := connectedServerMeetsFloor(natsserver.VERSION, false); err == nil ||
		!strings.Contains(err.Error(), "no_header_support") {
		t.Fatalf("headers disabled: %v, want a refusal naming no_header_support", err)
	}
	f, err := parseVersion(natsserver.VERSION)
	if err != nil {
		t.Fatal(err)
	}
	below := []string{fmt.Sprintf("%d.%d.0", f.core[0]-1, f.core[1])}
	if f.core[1] > 0 {
		below = append(below, fmt.Sprintf("%d.%d.0", f.core[0], f.core[1]-1))
	}
	if len(f.pre) == 0 {
		below = append(below, natsserver.VERSION+"-RC.1")
	}
	for _, v := range below {
		err := connectedServerMeetsFloor(v, true)
		if err == nil || !strings.Contains(err.Error(), v) || !strings.Contains(err.Error(), natsserver.VERSION) {
			t.Errorf("%s: %v, want a refusal naming it and the floor", v, err)
		}
	}
}

func TestJetStreamAPILevelFloor(t *testing.T) {
	for _, level := range []int{natsserver.JSApiLevel, natsserver.JSApiLevel + 1} {
		if err := jetStreamAPILevelMeetsFloor(level); err != nil {
			t.Errorf("level %d: %v", level, err)
		}
	}
	for _, level := range []int{natsserver.JSApiLevel - 1, 0} {
		if err := jetStreamAPILevelMeetsFloor(level); err == nil {
			t.Errorf("level %d accepted", level)
		}
	}
}

// fakeServer speaks just enough of the NATS protocol to complete nats.go's handshake while
// announcing info, and records every line the client sends until it hangs up.
func fakeServer(t *testing.T, info map[string]any) (url string, lines func() []string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	info["host"] = "127.0.0.1"
	info["port"] = l.Addr().(*net.TCPAddr).Port
	infoJSON, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := fmt.Fprintf(c, "INFO %s\r\n", infoJSON); err != nil {
			return
		}
		r := bufio.NewReader(c)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			got = append(got, line)
			if line == "PING" {
				if _, err := c.Write([]byte("PONG\r\n")); err != nil {
					return
				}
			}
		}
	}()
	return "nats://" + l.Addr().String(), func() []string {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("fake server never saw the client hang up")
		}
		return got
	}
}

// TestConnect_RefusesAServerBelowTheVersionFloor: a dialed server below the floor is refused
// by name before the broker sends any JetStream request.
func TestConnect_RefusesAServerBelowTheVersionFloor(t *testing.T) {
	url, lines := fakeServer(t, map[string]any{
		"server_id": "FAKE", "server_name": "fake", "version": "2.14.0", "proto": 1,
		"headers": true, "max_payload": 1 << 20,
	})
	cfg := config.Default()
	cfg.NATSURLs = url
	cfg.JSTimeout = 2 * time.Second
	be, err := Connect(context.Background(), cfg, nil, slog.Default())
	if err == nil {
		be.Close()
		t.Fatal("Connect accepted nats-server 2.14.0")
	}
	if !strings.Contains(err.Error(), "nats-server 2.14.0 is below") || !strings.Contains(err.Error(), natsserver.VERSION) {
		t.Fatalf("Connect = %v, want the version refusal naming 2.14.0 and the floor", err)
	}
	all := lines()
	if !slices.Contains(all, "PING") {
		t.Fatalf("the fake server saw no handshake (%q): the refusal is not shown to follow the INFO", all)
	}
	for _, l := range all {
		if strings.HasPrefix(l, "PUB $JS.") || strings.HasPrefix(l, "HPUB $JS.") {
			t.Fatalf("a JetStream request was sent before the refusal: %q", l)
		}
	}
}

// TestConnect_RefusesAServerWithHeadersDisabled: a pinned server configured with
// no_header_support passes the version and a headerless probe, so header support is
// checked on its own, through both transports.
func TestConnect_RefusesAServerWithHeadersDisabled(t *testing.T) {
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoHeaderSupport: true,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)

	for _, inProcess := range []bool{false, true} {
		cfg := config.Default()
		cfg.NATSURLs = srv.ClientURL()
		cfg.JSTimeout = 2 * time.Second
		var ip nats.InProcessConnProvider
		if inProcess {
			ip = srv
		}
		be, err := Connect(context.Background(), cfg, ip, slog.Default())
		if err == nil {
			be.Close()
			t.Fatalf("in-process %v: Connect accepted a server with header support disabled", inProcess)
		}
		if !strings.Contains(err.Error(), "no_header_support") {
			t.Fatalf("in-process %v: Connect = %v, want the header refusal", inProcess, err)
		}
	}
}

// TestConnect_RefusesAJetStreamBelowTheAPILevelFloor: the API level the probe is answered
// with is checked, on the URL path's single probe and on the embedded path's wait alike,
// and a refusal is not waited on. The answer comes from a responder on a domain the server
// does not serve; the paired control answers at the floor and is accepted, so the refusal is
// the level's and not the responder's shape.
func TestConnect_RefusesAJetStreamBelowTheAPILevelFloor(t *testing.T) {
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)

	responder, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(responder.Close)
	var level, probes atomic.Int64
	if _, err := responder.Subscribe("$JS.floor.API.INFO", func(m *nats.Msg) {
		probes.Add(1)
		resp, _ := json.Marshal(map[string]any{
			"type":   "io.nats.jetstream.api.v1.account_info_response",
			"memory": 0, "storage": 0, "streams": 0, "consumers": 0,
			"limits": map[string]any{"max_memory": -1, "max_storage": -1, "max_streams": -1, "max_consumers": -1},
			"api":    map[string]any{"level": level.Load(), "total": 0, "errors": 0},
		})
		_ = m.Respond(resp)
	}); err != nil {
		t.Fatal(err)
	}
	if err := responder.Flush(); err != nil {
		t.Fatal(err)
	}

	old := jsReadyRetryWait
	jsReadyRetryWait = time.Minute
	t.Cleanup(func() { jsReadyRetryWait = old })

	for _, inProcess := range []bool{false, true} {
		connect := func() (*Backend, error) {
			cfg := config.Default()
			cfg.NATSURLs = srv.ClientURL()
			cfg.JSDomain = "floor"
			cfg.JSTimeout = 2 * time.Second
			var ip nats.InProcessConnProvider
			if inProcess {
				ip = srv
			}
			// Bounded, so a refusal that is waited on fails here instead of hanging.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return Connect(ctx, cfg, ip, slog.Default())
		}

		level.Store(int64(natsserver.JSApiLevel - 1))
		probes.Store(0)
		start := time.Now()
		be, err := connect()
		if err == nil {
			be.Close()
			t.Fatalf("in-process %v: Connect accepted API level %d", inProcess, natsserver.JSApiLevel-1)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("JetStream API level %d", natsserver.JSApiLevel-1)) {
			t.Fatalf("in-process %v: Connect = %v, want the API-level refusal", inProcess, err)
		}
		if n := probes.Load(); n != 1 {
			t.Fatalf("in-process %v: %d probes answered, want exactly one: the refusal must not be waited on", inProcess, n)
		}
		if d := time.Since(start); d >= jsReadyRetryWait {
			t.Fatalf("in-process %v: refusal took %v", inProcess, d)
		}

		level.Store(int64(natsserver.JSApiLevel))
		be, err = connect()
		if err != nil {
			t.Fatalf("in-process %v: Connect at the floor's API level: %v", inProcess, err)
		}
		be.Close()
	}
}
