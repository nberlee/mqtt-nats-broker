package main

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestWaitDrainGraceElapses verifies the grace period is actually honored (the
// regression: it must not return immediately because ctx was already cancelled).
func TestWaitDrainGraceElapses(t *testing.T) {
	start := time.Now()
	waitDrainGrace(40*time.Millisecond, make(chan os.Signal))
	if el := time.Since(start); el < 30*time.Millisecond {
		t.Fatalf("grace returned after %v, expected to wait ~40ms", el)
	}
}

// TestWaitDrainGraceSecondSignalCutsShort verifies a second signal ends the wait
// early instead of blocking for the full delay.
func TestWaitDrainGraceSecondSignalCutsShort(t *testing.T) {
	sigCh := make(chan os.Signal, 1)
	sigCh <- syscall.SIGTERM
	start := time.Now()
	waitDrainGrace(10*time.Second, sigCh)
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("second signal did not cut the grace short: waited %v", el)
	}
}

// TestWaitDrainGraceZeroReturnsImmediately verifies a 0 delay is a no-op.
func TestWaitDrainGraceZeroReturnsImmediately(t *testing.T) {
	start := time.Now()
	waitDrainGrace(0, make(chan os.Signal))
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("zero delay should return immediately, waited %v", el)
	}
}

// reservedAddr returns a 127.0.0.1 address that nothing is listening on, by
// binding an ephemeral port and immediately releasing it.
func reservedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	return addr
}

// TestRunFailsOnBadTLSBeforeStartup pins the ordering that makes the fail-closed
// guarantee meaningful: the TLS configuration is validated before the NATS
// connection is dialed and before the health listener is bound. NATS is pointed
// at a closed port, so if resolveTLS were moved below backend.Connect the error
// would name NATS instead of TLS, and if it were moved below startHTTP the health
// port would be left bound.
func TestRunFailsOnBadTLSBeforeStartup(t *testing.T) {
	natsAddr := reservedAddr(t)
	httpAddr := reservedAddr(t)
	missing := filepath.Join(t.TempDir(), "missing.pem")

	err := run([]string{
		"-nats", "nats://" + natsAddr,
		"-http-addr", httpAddr,
		"-tls-cert", missing,
		"-tls-key", missing,
	})
	if err == nil {
		t.Fatal("run() = nil, want a fatal TLS configuration error")
	}
	if !strings.Contains(err.Error(), "MQTT TLS configuration") {
		t.Errorf("run() error = %q, want the TLS configuration error; a NATS or "+
			"JetStream error means TLS is no longer validated first", err)
	}

	// Nothing may have been bound on the way to that error.
	if c, err := net.DialTimeout("tcp", httpAddr, 2*time.Second); err == nil {
		_ = c.Close() // best effort: the probe only had to establish that it connected
		t.Errorf("health server is listening on %s after a failed startup, want no listener", httpAddr)
	}
}
