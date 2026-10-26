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

//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A SAME-SESSION BACKGROUND WRITER, made of the two things the foreground-priority handshake
// (outbound_foreground_priority.go) is about: the production admission, and a committed write of
// the session's authority record. Every authority transition of the session CASes that one record,
// so a commit landing between a foreground attempt's read and its batch defeats the batch AT THE
// STORE: no synthetic error, and the foreground attempt's own boundary checks and send run
// unmodified. The write stands in for a counted writer's operation body, not for its admission or
// its store effect.
//
// It is driven from the target operation's presend (after its reads, before its batch) in two
// modes. LIVE admits a fresh attempt in the window, so an unannounced target loses every attempt
// to it and an announced one holds it until the announcement is released. PRE-ADMITTED takes its
// admissions before the target starts, which is exactly the allowance the handshake concedes: one
// attempt per counted writer, committed after the announcement.

type testAuthorityContender struct {
	t    *testing.T
	sess *mqttSession
	jsa  *mqttJSA

	waiting chan struct{} // one token per background admission that started to wait
	commits atomic.Int32  // authority writes this contender committed
	held    atomic.Int32  // LIVE commits that found the session announced and waited
	join    sync.WaitGroup

	mu       sync.Mutex
	admitted []chan struct{} // PRE-ADMITTED writers, each waiting for its window
}

// testNewAuthorityContender installs the background-wait observer on sess for the test's life. The
// observer is non-blocking: a second wait while a token is pending is folded into it.
func testNewAuthorityContender(t *testing.T, sess *mqttSession) *testAuthorityContender {
	t.Helper()
	w := &testAuthorityContender{t: t, sess: sess, jsa: sess.jsa, waiting: make(chan struct{}, 1)}
	notify := func() {
		select {
		case w.waiting <- struct{}{}:
		default:
		}
	}
	sess.testBackgroundWait.Store(&notify)
	t.Cleanup(func() {
		sess.testBackgroundWait.Store(nil)
		w.release()
		w.join.Wait()
	})
	return w
}

// bump commits one authority write: the record as read, its revision advanced, CASed at the
// sequence read.
func (w *testAuthorityContender) bump() error {
	as := w.sess.asm
	al := w.jsa.loadOutboundAuthority(w.sess.id, as.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		return fmt.Errorf("the authority reads %v", al.status)
	}
	next := *al.rec
	next.Rev++
	next.Txn = mqttNewInflightNonce()
	b, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	if _, err := w.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(w.sess.id)), hdr, msg); err != nil {
		return fmt.Errorf("committing the contending authority write: %w", err)
	}
	w.commits.Add(1)
	return nil
}

// live is called from a presend window: one background attempt is admitted and commits, or finds
// the session announced and waits. It returns once either is observed, so the target proceeds to
// its batch behind the commit or with the writer held. A held writer commits after the
// announcement is released; the contender joins it at cleanup.
func (w *testAuthorityContender) live() (committed bool) {
	select {
	case <-w.waiting: // a stale token from an earlier wait
	default:
	}
	done := make(chan error, 1)
	w.join.Add(1)
	go func() {
		defer w.join.Done()
		if !w.sess.admitBackgroundAuthority() {
			done <- fmt.Errorf("the contender's admission was refused: shutdown")
			return
		}
		done <- w.bump()
	}()
	select {
	case err := <-done:
		if err != nil {
			w.t.Errorf("live contender: %v", err)
			return false
		}
		return true
	case <-w.waiting:
		w.held.Add(1)
		return false
	case <-time.After(10 * time.Second):
		w.t.Errorf("the live contender neither committed nor waited")
		return false
	}
}

// preAdmit takes n admissions now, before the target announces, each held by a writer that commits
// when fire hands it a window. It returns once every admission was granted.
func (w *testAuthorityContender) preAdmit(n int) {
	w.t.Helper()
	granted := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go_ := make(chan struct{})
		w.mu.Lock()
		w.admitted = append(w.admitted, go_)
		w.mu.Unlock()
		w.join.Add(1)
		go func() {
			defer w.join.Done()
			if !w.sess.admitBackgroundAuthority() {
				w.t.Errorf("a pre-admitted contender was refused")
				granted <- struct{}{}
				return
			}
			granted <- struct{}{}
			<-go_ // its window (fire commits on its admission), or cleanup
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case <-granted:
		case <-time.After(10 * time.Second):
			w.t.Fatalf("pre-admission %d of %d was never granted", i+1, n)
		}
	}
}

// fire hands the next pre-admitted writer its window: it commits now, inside the target's window,
// on the admission it took before the announcement. It reports false when none is left.
func (w *testAuthorityContender) fire() bool {
	w.mu.Lock()
	if len(w.admitted) == 0 {
		w.mu.Unlock()
		return false
	}
	go_ := w.admitted[0]
	w.admitted = w.admitted[1:]
	w.mu.Unlock()
	if err := w.bump(); err != nil {
		w.t.Errorf("pre-admitted contender: %v", err)
	}
	close(go_)
	return true
}

// release lets every pre-admitted writer that never got a window go.
func (w *testAuthorityContender) release() {
	w.mu.Lock()
	rest := w.admitted
	w.admitted = nil
	w.mu.Unlock()
	for _, ch := range rest {
		close(ch)
	}
}

// testPresendFor installs a presend hook on the session's store that calls window for every
// transition of the session whose operation is op and whose batch carries a member on subject
// (subject empty: any member), for the test's life.
func testPresendFor(t *testing.T, sess *mqttSession, op, subject string, window func(at *mqttAuthorityTransition)) {
	t.Helper()
	hook := func(at *mqttAuthorityTransition) {
		if at.Attempt == nil || at.Attempt.ID != sess.id || at.Attempt.Op != op {
			return
		}
		if subject != _EMPTY_ {
			found := false
			for _, m := range at.Extra {
				if m.subj == subject {
					found = true
					break
				}
			}
			if !found {
				return
			}
		}
		window(at)
	}
	sess.jsa.testTransitionPresend.Store(&hook)
	t.Cleanup(func() { sess.jsa.testTransitionPresend.Store(nil) })
}

// testCountContentionWaits counts the requested authority-contention waits instead of sleeping.
func testCountContentionWaits(t *testing.T, sess *mqttSession) *atomic.Int32 {
	t.Helper()
	var waits atomic.Int32
	as := sess.asm
	as.mu.Lock()
	as.testAuthorityContentionWait = func(time.Duration) { waits.Add(1) }
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testAuthorityContentionWait = nil
		as.mu.Unlock()
	})
	return &waits
}

// testLoseAtCommit refuses the first lose commits of the session's op (on subject, when given)
// with the production CAS rejection, after the batch's handle was frozen, and counts every such
// commit.
func testLoseAtCommit(t *testing.T, sess *mqttSession, op, subject string, lose int) *atomic.Int32 {
	t.Helper()
	var attempts atomic.Int32
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt == nil || at.Attempt.ID != sess.id || at.Attempt.Op != op {
			return send()
		}
		if subject != _EMPTY_ {
			found := false
			for _, m := range at.Extra {
				if m.subj == subject {
					found = true
					break
				}
			}
			if !found {
				return send()
			}
		}
		if int(attempts.Add(1)) <= lose {
			return 0, ApiErrors[JSStreamWrongLastSequenceErrF]
		}
		return send()
	}
	sess.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { sess.jsa.testTransitionCommit.Store(nil) })
	return &attempts
}
