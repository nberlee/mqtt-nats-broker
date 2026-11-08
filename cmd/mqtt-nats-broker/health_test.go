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
// requested, not connected.
func startingHealthMux() *http.ServeMux {
	return healthMux(context.Background(), new(atomic.Int32), func() bool { return false })
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
		status    int
		body      string
	}{
		{operational.Starting, false, false, 503, "starting"},
		{operational.Starting, false, true, 503, "starting"},
		{operational.Starting, true, false, 503, "draining"},
		{operational.Starting, true, true, 503, "draining"},
		{operational.Serving, false, false, 503, "nats disconnected"},
		{operational.Serving, false, true, 200, "ready"},
		{operational.Serving, true, false, 503, "draining"},
		{operational.Serving, true, true, 503, "draining"},
	} {
		t.Run(fmt.Sprintf("state=%d/stopped=%t/connected=%t", tc.state, tc.stop, tc.connected), func(t *testing.T) {
			ctx := context.Background()
			if tc.stop {
				ctx = stopped
			}
			var readiness atomic.Int32
			readiness.Store(int32(tc.state))
			connected := tc.connected
			mux := healthMux(ctx, &readiness, func() bool { return connected })
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
