package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/nberlee/mqtt-nats-broker/internal/operational"
)

// startingHealthMux is the health handler of a broker that has not served yet: no stop
// requested, not connected, no JetStream observation.
func startingHealthMux() *http.ServeMux {
	no := func() bool { return false }
	return healthMux(context.Background(), new(atomic.Int32), no, no)
}

// TestHealthEndpointsAnswerEachState drives the real handler through every combination of
// its inputs and compares with literal answers, not with the definition, so a definition
// and a handler that drift together still fail here.
func TestHealthEndpointsAnswerEachState(t *testing.T) {
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		state     operational.Readiness
		stop      bool
		connected bool
		jetStream bool
		status    int
		body      string
	}{
		{operational.Starting, false, false, false, 503, "starting"},
		{operational.Starting, false, false, true, 503, "starting"},
		{operational.Starting, false, true, false, 503, "starting"},
		{operational.Starting, false, true, true, 503, "starting"},
		{operational.Starting, true, false, false, 503, "draining"},
		{operational.Starting, true, false, true, 503, "draining"},
		{operational.Starting, true, true, false, 503, "draining"},
		{operational.Starting, true, true, true, 503, "draining"},
		{operational.Serving, false, false, false, 503, "nats disconnected"},
		{operational.Serving, false, false, true, 503, "nats disconnected"},
		{operational.Serving, false, true, false, 503, "jetstream not answering"},
		{operational.Serving, false, true, true, 200, "ready"},
		{operational.Serving, true, false, false, 503, "draining"},
		{operational.Serving, true, false, true, 503, "draining"},
		{operational.Serving, true, true, false, 503, "draining"},
		{operational.Serving, true, true, true, 503, "draining"},
	} {
		t.Run(fmt.Sprintf("state=%d/stopped=%t/connected=%t/jetstream=%t", tc.state, tc.stop, tc.connected, tc.jetStream), func(t *testing.T) {
			ctx := context.Background()
			if tc.stop {
				ctx = stopped
			}
			var readiness atomic.Int32
			readiness.Store(int32(tc.state))
			connected, jetStream := tc.connected, tc.jetStream
			mux := healthMux(ctx, &readiness, func() bool { return connected }, func() bool { return jetStream })
			get := func(p string) (int, string) {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
				return rec.Code, rec.Body.String()
			}
			if code, body := get("/readyz"); code != tc.status || body != tc.body {
				t.Errorf("/readyz = %d %q, want %d %q", code, body, tc.status, tc.body)
			}
			if code, body := get("/healthz"); code != 200 || body != "ok" {
				t.Errorf("/healthz = %d %q, want 200 \"ok\"", code, body)
			}
		})
	}
}

// TestStoppedAndStartingNeverConsultNATS: a stop or a broker that has not served answers
// without calling the connection or the JetStream observation, either of which can wait on
// nats.go's connection mutex through a reconnect.
func TestStoppedAndStartingNeverConsultNATS(t *testing.T) {
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		state operational.Readiness
		body  string
	}{
		{"stopped while serving", stopped, operational.Serving, "draining"},
		{"stopped while starting", stopped, operational.Starting, "draining"},
		{"starting", context.Background(), operational.Starting, "starting"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var readiness atomic.Int32
			readiness.Store(int32(tc.state))
			called := func(what string) func() bool {
				return func() bool { t.Errorf("%s consulted", what); return true }
			}
			mux := healthMux(tc.ctx, &readiness, called("the connection"), called("the JetStream observation"))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != 503 || rec.Body.String() != tc.body {
				t.Errorf("/readyz = %d %q, want 503 %q", rec.Code, rec.Body.String(), tc.body)
			}
		})
	}
}
