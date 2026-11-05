package main

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestMQTTOptionsCarryMaxAckPending(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAckPending = 77
	if got := mqttOptions(cfg, nil).MQTT.MaxAckPending; got != 77 {
		t.Fatalf("MQTT MaxAckPending = %d, want 77", got)
	}
}

// The profiler serves on the loopback address it was given and nowhere else: the health mux
// does not answer the profile paths.
func TestProfilerServesOnlyItsOwnListener(t *testing.T) {
	srv, bound := startPprof("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if srv == nil {
		t.Fatal("the profiler did not bind")
	}
	t.Cleanup(func() { _ = srv.Close() })
	addr := bound.String()
	host, _, _ := net.SplitHostPort(addr)
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		t.Fatalf("the profiler bound %s, not a loopback address", addr)
	}
	resp, err := http.Get("http://" + addr + "/debug/pprof/heap?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(body) == 0 {
		t.Fatalf("heap profile: status %d, %d bytes", resp.StatusCode, len(body))
	}

	var ready atomic.Bool
	health := healthMux(nil, &ready)
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap"} {
		req, _ := http.NewRequest(http.MethodGet, path, nil)
		if _, pattern := health.Handler(req); pattern != "" {
			t.Errorf("the health mux answers %s (pattern %q)", path, pattern)
		}
	}
}
