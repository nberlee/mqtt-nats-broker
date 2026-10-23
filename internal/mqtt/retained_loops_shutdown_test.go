// Copyright 2026 The mqtt-nats-broker Authors
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

// The retained background loops retry while their session object serves its generation. A
// persistent session is never removed at shutdown, so "serves" has to end with the server too,
// or in any process that outlives the server each loop wakes every few seconds forever,
// queueing a request nothing will ever answer.

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

// goroutinesRunning counts the goroutines whose stack has a frame matching fn.
func goroutinesRunning(fn string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		for _, line := range strings.Split(g, "\n") {
			// A frame line, not the "created by" line naming the spawner.
			if strings.Contains(line, fn) && !strings.HasPrefix(line, "created by ") {
				count++
				break
			}
		}
	}
	return count
}

// awaitGoroutines polls until fn's count satisfies ok, or fails at the deadline.
func awaitGoroutines(t *testing.T, fn string, d time.Duration, what string, ok func(int) bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		n := goroutinesRunning(fn)
		if ok(n) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d goroutine(s) running %s", what, n, fn)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// retainedLoopRig is a resident persistent session whose register reads fail, so a loop that
// reads one retries rather than finishing.
func retainedLoopRig(t *testing.T, clientID string) (*testInflightEnv, string) {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	if !e.sess.resident() {
		e.asm.addSession(e.sess, true)
	}
	if !e.sess.resident() {
		t.Fatal("the rig's session is not resident, so every loop would end at once and prove nothing")
	}
	e.asm.mu.Lock()
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, errors.New("register store unreadable (test)")
	}
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	return e, gen
}

// stopServer is what shutdown Phase 4 does to the account infrastructure.
func stopServer(s *Server) {
	s.mu.Lock()
	q := s.quitCh
	s.mu.Unlock()
	select {
	case <-q:
	default:
		close(q)
	}
}

// Not parallel: the oracle counts goroutines by function across the process, so a baseline
// is taken right before each loop starts and nothing else of the kind may start meanwhile.
func TestRetainedLoopsEndAtShutdown(t *testing.T) {
	const running, stopped = 3 * time.Second, 12 * time.Second

	t.Run("terminal cleanup", func(t *testing.T) {
		e, gen := retainedLoopRig(t, "loops-terminal")
		const fn = "continueRecoveredRetainedTerminals.func1"
		base := goroutinesRunning(fn)
		const pi = uint16(3)
		rec := &mqttInflightRecord{PI: pi, Nonce: "terminal-nonce", State: mqttInflightPublishTerminal}
		subject := mqttInflightSubject(e.sess.idHash, gen, pi)
		if mqttSlotRefFromRecord(subject, 5, rec) == nil {
			t.Fatal("the fixture record yields no slot reference, so the loop would skip it")
		}
		e.sess.continueRecoveredRetainedTerminals(e.c, []mqttRecoveredRetained{
			{kind: mqttRecoveredRetainedTerminal, subject: subject, seq: 5, rec: rec},
		})
		time.Sleep(300 * time.Millisecond)
		awaitGoroutines(t, fn, running, "the cleanup must be retrying while the server runs", func(n int) bool { return n > base })
		stopServer(e.s)
		awaitGoroutines(t, fn, stopped, "the cleanup outlived its server", func(n int) bool { return n <= base })
	})

	t.Run("manifest re-read", func(t *testing.T) {
		e, gen := retainedLoopRig(t, "loops-manifest")
		const fn = "wakeRetainedReclaimForPayload.func1"
		base := goroutinesRunning(fn)
		const payload = "payload-unreadable"
		// A manifest that cannot be decoded: not the store's "no message", so it is read again.
		if _, err := e.asm.jsa.storeMsg(mqttManifestSubject(getHash(e.sess.id), gen, payload), 0, []byte("{")); err != nil {
			t.Fatal(err)
		}
		e.sess.wakeRetainedReclaimForPayload(gen, payload)
		awaitGoroutines(t, fn, running, "the re-read must be retrying while the server runs", func(n int) bool { return n > base })
		stopServer(e.s)
		awaitGoroutines(t, fn, stopped, "the re-read outlived its server", func(n int) bool { return n <= base })
	})

	t.Run("reclaim", func(t *testing.T) {
		e, gen := retainedLoopRig(t, "loops-reclaim")
		const fn = ").runRetainedReclaim("
		base := goroutinesRunning(fn)
		// A copy whose owner record does not exist: withheld for a reason that is not "material
		// still needed", so it is never dropped while the session serves.
		e.sess.retainedReclaim.wake(e.sess, gen, "inc-without-an-owner")
		awaitGoroutines(t, fn, running, "the reclaimer must be retrying while the server runs", func(n int) bool { return n > base })
		time.Sleep(200 * time.Millisecond)
		stopServer(e.s)
		awaitGoroutines(t, fn, stopped, "the reclaimer outlived its server", func(n int) bool { return n <= base })
	})

	t.Run("abandonment continuation", func(t *testing.T) {
		e, gen := retainedLoopRig(t, "loops-abandon")
		const fn = ").runRetainedContinuations("
		base := goroutinesRunning(fn)
		const pi = uint16(4)
		e.sess.ownRetainedAbandonment(&mqttRetainedAbandonment{
			c: e.c,
			ev: mqttDenyEvidence{Gen: gen, Subject: mqttInflightSubject(e.sess.idHash, gen, pi),
				Nonce: "abandon-nonce", Lin: "abandon-lin", PI: pi},
			inc: "abandon-inc",
		})
		awaitGoroutines(t, fn, running, "the continuation must be retrying while the server runs", func(n int) bool { return n > base })
		time.Sleep(200 * time.Millisecond)
		stopServer(e.s)
		awaitGoroutines(t, fn, stopped, "the continuation outlived its server", func(n int) bool { return n <= base })
	})
}

// A clean disconnect or an expiry takes the session object out of the maps and can put the
// same object back (a drain timeout, a refused discard). An abandonment the continuation worker
// meets in that window is still owed: only a stopped server ends it.
func TestRetainedAbandonmentSurvivesTemporaryRemoval(t *testing.T) {
	e, gen := retainedLoopRig(t, "loops-abandon-removed")
	const pi = uint16(5)
	a := &mqttRetainedAbandonment{
		c: e.c,
		ev: mqttDenyEvidence{Gen: gen, Subject: mqttInflightSubject(e.sess.idHash, gen, pi),
			Nonce: "removed-nonce", Lin: "removed-lin", PI: pi},
		inc: "removed-inc",
	}
	e.sess.ownRetainedAbandonment(a)
	owned := func() bool {
		d := &e.sess.retainedDriver
		d.mu.Lock()
		defer d.mu.Unlock()
		for _, x := range d.abandons {
			if x == a {
				return true
			}
		}
		return false
	}
	// Let it step (and retry) at least once while resident.
	time.Sleep(300 * time.Millisecond)
	if !owned() {
		t.Fatal("fixture: the abandonment is not owned by the continuation worker")
	}
	e.asm.removeSession(e.sess, true)
	// Several of its backoff steps run while the object is out of the maps.
	time.Sleep(time.Second)
	e.asm.addSession(e.sess, true)
	if !owned() {
		t.Fatal("the abandonment was dropped while its session object was only temporarily out of the maps")
	}
	stopServer(e.s)
	awaitGoroutines(t, ").runRetainedContinuations(", 12*time.Second, "the continuation outlived its server",
		func(n int) bool { return !owned() })
}
