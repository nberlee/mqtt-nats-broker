// Copyright 2026 The mqtt-nats-bridge Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// testRigServer is a session-capable rig server built by the production constructor, so it has
// everything a session's background workers touch: the Will dispatch gate and its tracked timer
// map, which the custody dispatcher's tick re-arms through on its exit. A hand-built &Server{}
// lacks the map, and a dispatcher that outlived its test panicked the whole tagged binary there.
//
// Cleanup stops the rig's Will dispatch and joins what it started: close the gate, stop the
// timers, close quitCh, then join the timers, claim loops and admitted workers. Unlike Shutdown it
// closes quitCh before the join, because a rig has no clients to drain first and some of its
// tests close or reopen the gate themselves; the joins are bounded so a stuck worker reports
// instead of hanging the binary.
func testRigServer(t testing.TB, nc *nats.Conn, node string) *Server {
	t.Helper()
	s := NewServer(nc, &Options{}, node)
	t.Cleanup(func() { testRigServerDown(t, s) })
	return s
}

// testRigServerDown is testRigServer's cleanup, for a rig that must stop before its test ends.
// Idempotent.
func testRigServerDown(t testing.TB, s *Server) {
	// The gate by hand rather than closeWillDispatch: a test may already have closed it, or
	// closed it and reopened it, and the claim-stop channel must be closed exactly once.
	s.willDispatchMu.Lock()
	s.willDispatchClosed = true
	select {
	case <-s.willClaimStopCh:
	default:
		close(s.willClaimStopCh)
	}
	timers := make([]willTimerRef, 0, len(s.willTimers))
	for key, tm := range s.willTimers {
		timers = append(timers, willTimerRef{key, tm})
	}
	s.willDispatchMu.Unlock()
	s.stopWillTimers(timers)
	testRigQuitOnce(s)
	joined := make(chan struct{})
	go func() {
		s.willTimerWG.Wait()
		s.willClaimWG.Wait()
		s.willWorkerWG.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(10 * time.Second):
		t.Errorf("rig server %q: a Will timer, claim loop or admitted worker did not stop within 10s", s.nodeName)
	}
}

var testRigQuitMu sync.Mutex

func testRigQuitOnce(s *Server) {
	testRigQuitMu.Lock()
	defer testRigQuitMu.Unlock()
	select {
	case <-s.quitCh:
	default:
		close(s.quitCh)
	}
}
