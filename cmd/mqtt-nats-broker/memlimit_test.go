package main

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

// The Go memory limit and the environment are process-global, so none of these
// tests runs in parallel, and each restores both through isolateGoMemoryLimit.

const mib = 1 << 20

// isolateGoMemoryLimit gives the test a clean slate: no GOMEMLIMIT or
// AUTOMEMLIMIT in the environment, source as the container limit, and the
// runtime limit, environment and source restored when the test ends. Every
// run() derives the process-wide limit, so any test calling it must isolate.
func isolateGoMemoryLimit(t *testing.T, source memlimit.Provider) {
	t.Helper()
	prior := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prior) })
	for _, key := range []string{"GOMEMLIMIT", "AUTOMEMLIMIT"} {
		t.Setenv(key, "") // registers the restore of the original value
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
	saved := goMemoryLimitSource
	goMemoryLimitSource = source
	t.Cleanup(func() { goMemoryLimitSource = saved })
}

// countingSource reports limit (or err) and counts how often it was asked.
type countingSource struct {
	mu    sync.Mutex
	calls int
	limit uint64
	err   error
}

func (s *countingSource) provide() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.limit, s.err
}

func (s *countingSource) called() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// recordingHandler keeps every log record's level and message.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// logged reports whether a record at level was written whose message contains
// fragment.
func (h *recordingHandler) logged(level slog.Level, fragment string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == level && strings.Contains(r.Message, fragment) {
			return true
		}
	}
	return false
}

// TestGoMemoryLimitIsNinetyPercentOfContainerLimit pins the ratio and that the
// product is truncated, never rounded up past the fraction of the ceiling.
func TestGoMemoryLimitIsNinetyPercentOfContainerLimit(t *testing.T) {
	for _, tc := range []struct {
		container uint64
		want      int64
	}{
		{container: 1000 * mib, want: 943718400},
		{container: 1001, want: 900}, // 900.9: truncated
	} {
		isolateGoMemoryLimit(t, memlimit.Limit(tc.container))
		applyGoMemoryLimit(slog.New(&recordingHandler{}))
		if got := debug.SetMemoryLimit(-1); got != tc.want {
			t.Errorf("container limit %d: Go memory limit = %d, want %d", tc.container, got, tc.want)
		}
	}
}

// TestGoMemoryLimitEnvironmentWins pins that an operator's GOMEMLIMIT is the
// whole answer: the container limit is not even read.
func TestGoMemoryLimitEnvironmentWins(t *testing.T) {
	src := &countingSource{limit: 1000 * mib}
	isolateGoMemoryLimit(t, src.provide)
	debug.SetMemoryLimit(321 * mib) // what the runtime parsed from GOMEMLIMIT
	t.Setenv("GOMEMLIMIT", "321MiB")

	applyGoMemoryLimit(slog.New(&recordingHandler{}))

	if n := src.called(); n != 0 {
		t.Errorf("container limit read %d times with GOMEMLIMIT set, want 0", n)
	}
	if got := debug.SetMemoryLimit(-1); got != 321*mib {
		t.Errorf("Go memory limit = %d, want the operator's %d", got, 321*mib)
	}
}

// TestGoMemoryLimitAutomemlimitOverride pins the library's own control: a ratio
// replaces 0.9, "off" disables the derivation, and a malformed value is logged
// and changes nothing rather than failing startup.
func TestGoMemoryLimitAutomemlimitOverride(t *testing.T) {
	t.Run("ratio", func(t *testing.T) {
		isolateGoMemoryLimit(t, memlimit.Limit(1000*mib))
		t.Setenv("AUTOMEMLIMIT", "0.5")
		applyGoMemoryLimit(slog.New(&recordingHandler{}))
		if got := debug.SetMemoryLimit(-1); got != 500*mib {
			t.Errorf("Go memory limit = %d, want %d", got, 500*mib)
		}
	})
	t.Run("off", func(t *testing.T) {
		src := &countingSource{limit: 1000 * mib}
		isolateGoMemoryLimit(t, src.provide)
		debug.SetMemoryLimit(123 * mib)
		t.Setenv("AUTOMEMLIMIT", "off")
		applyGoMemoryLimit(slog.New(&recordingHandler{}))
		if n := src.called(); n != 0 {
			t.Errorf("container limit read %d times with AUTOMEMLIMIT=off, want 0", n)
		}
		if got := debug.SetMemoryLimit(-1); got != 123*mib {
			t.Errorf("Go memory limit = %d, want it unchanged at %d", got, 123*mib)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		src := &countingSource{limit: 1000 * mib}
		isolateGoMemoryLimit(t, src.provide)
		debug.SetMemoryLimit(123 * mib)
		t.Setenv("AUTOMEMLIMIT", "bogus")
		h := &recordingHandler{}
		applyGoMemoryLimit(slog.New(h))
		if got := debug.SetMemoryLimit(-1); got != 123*mib {
			t.Errorf("Go memory limit = %d, want it unchanged at %d", got, 123*mib)
		}
		if !h.logged(slog.LevelError, "GOMEMLIMIT") {
			t.Error("malformed AUTOMEMLIMIT was not logged at error level")
		}
	})
}

// TestGoMemoryLimitUnlimitedContainer pins that a hierarchy with no concrete
// limit leaves the runtime explicitly unlimited, replacing any finite limit, and
// says so.
func TestGoMemoryLimitUnlimitedContainer(t *testing.T) {
	isolateGoMemoryLimit(t, func() (uint64, error) { return 0, memlimit.ErrNoLimit })
	debug.SetMemoryLimit(123 * mib)
	h := &recordingHandler{}

	applyGoMemoryLimit(slog.New(h))

	if got := debug.SetMemoryLimit(-1); got != math.MaxInt64 {
		t.Errorf("Go memory limit = %d, want unlimited (%d)", got, int64(math.MaxInt64))
	}
	if !h.logged(slog.LevelInfo, "GOMEMLIMIT") {
		t.Error("the change to an unlimited Go memory limit was not logged")
	}
}

// TestGoMemoryLimitUnreadableCgroupKeepsLimit pins that failing to read the
// container limit is reported and otherwise harmless: the runtime keeps the
// limit it had.
func TestGoMemoryLimitUnreadableCgroupKeepsLimit(t *testing.T) {
	isolateGoMemoryLimit(t, func() (uint64, error) { return 0, errors.New("permission denied") })
	debug.SetMemoryLimit(123 * mib)
	h := &recordingHandler{}

	applyGoMemoryLimit(slog.New(h))

	if got := debug.SetMemoryLimit(-1); got != 123*mib {
		t.Errorf("Go memory limit = %d, want it kept at %d", got, 123*mib)
	}
	if !h.logged(slog.LevelError, "GOMEMLIMIT") {
		t.Error("the unreadable container limit was not logged at error level")
	}
}

// TestRunDerivesGoMemoryLimitBeforeConnecting pins the startup placement: the
// limit is derived before NATS is dialed, and a failure to derive it does not
// stop startup. NATS is a closed port, so run() must end in the connect error.
func TestRunDerivesGoMemoryLimitBeforeConnecting(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  *countingSource
		want int64
	}{
		{name: "limited", src: &countingSource{limit: 1000 * mib}, want: 943718400},
		{name: "unreadable", src: &countingSource{err: errors.New("permission denied")}, want: 123 * mib},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateGoMemoryLimit(t, tc.src.provide)
			debug.SetMemoryLimit(123 * mib)

			err := run([]string{
				"-nats", "nats://" + reservedAddr(t),
				"-http-addr", reservedAddr(t),
			})
			if err == nil || !strings.Contains(err.Error(), "connect to NATS") {
				t.Fatalf("run() error = %v, want the NATS connect error: startup must "+
					"get past the memory limit whatever its outcome", err)
			}
			if n := tc.src.called(); n != 1 {
				t.Errorf("container limit read %d times before the NATS dial, want 1", n)
			}
			if got := debug.SetMemoryLimit(-1); got != tc.want {
				t.Errorf("Go memory limit = %d, want %d", got, tc.want)
			}
		})
	}
}
