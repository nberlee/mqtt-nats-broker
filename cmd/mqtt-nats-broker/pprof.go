package main

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// pprofMux serves the runtime profiles on a mux of its own. The net/http/pprof import also
// registers them on http.DefaultServeMux; nothing in the broker serves that mux, and the health
// listener has its own (healthMux), so the profiles are reachable only here.
func pprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// startPprof serves the profiles on addr, which config.Validate has confined to a literal
// loopback IP: profiles expose process memory, so they are reached with a port-forward, never
// from the network. The bind happens here, before startup goes on, so the address the operator
// asked for is the one served or the failure is logged; a profiler that cannot bind is
// diagnostics lost, not a reason to refuse service. Returns the server and the address it
// bound, or nil when it did not bind.
func startPprof(addr string, log *slog.Logger) (*http.Server, net.Addr) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("profiler listener could not bind", "addr", addr, "err", err)
		return nil, nil
	}
	// No write timeout: a CPU profile or trace streams for as long as it was asked to.
	srv := &http.Server{Handler: pprofMux(), ReadHeaderTimeout: 5 * time.Second}
	log.Info("profiler listening", "addr", ln.Addr().String())
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("profiler error", "err", err)
		}
	}()
	return srv, ln.Addr()
}
