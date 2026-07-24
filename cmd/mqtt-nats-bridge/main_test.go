package main

import (
	"os"
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
