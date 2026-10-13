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

import "sync"

// FOREGROUND PRIORITY over this broker's retained background writers (F-105 7c, consult 105).
//
// Every outbound authority transition of a session CASes the one authority record, so this
// broker's own writers serialize on it by construction. The retained driver (materialization,
// admission, acquisition, withdrawal, abandonment) and the reclaimer (one transition per
// reclamation step) produce BURSTS of such writes right after a SUBSCRIBE and after every retained
// acknowledgement, withdrawal and abandonment; a foreground operation whose bounded re-attempts all
// land inside such a burst fails although nothing contradicted it.
//
// THE HANDSHAKE. A foreground operation ANNOUNCES itself once, before its first relevant read, and
// stays announced across every attempt and contention wait until it returns. A background writer
// ADMITS each authority-transition attempt — one admission authorizes at most ONE attempt, checked
// again before every retry and every later transition, before that attempt's reads — and an
// admission waits while any foreground operation is announced. A background attempt admitted just
// before an announcement may still read and commit after it; that is allowed, and it is the whole
// of what background work can take from the foreground: with B background writers, each holding at
// most one admitted attempt, a continuously announced foreground operation loses at most B attempts
// to them.
//
// Foreground since F-112: the ordinary and shared delivery admission ladder
// (admitThroughAuthority), against the floor unit's background writes as well as the retained
// ones. With four background writers against that ladder's three attempts, the B-attempt
// allowance above can still exhaust it: a stated residual (docs/CONFORMANCE.md), re-offered at
// AckWait, whose remedy F-114 owns. What it does not cover, stated rather than implied: writes by another broker, by writers
// outside this protocol (the recovery pass, the custody observer's other units), and store
// failures. Background waits for as long as any foreground operation is announced, so continuous
// foreground traffic can delay it without bound; a foreground operation that never returns keeps
// background waiting (it is never cleared on a timer while it could still write). A waiter exits on
// server shutdown without being granted an attempt.
//
// The foreground side never waits on the background side, and neither side holds this mutex
// across a read, a commit, a sleep or any other lock.

// mqttForegroundAuthority is one session's announcement count.
type mqttForegroundAuthority struct {
	mu sync.Mutex
	n  int
	// idle is closed when the count returns to zero; a fresh one is made when it leaves zero.
	idle chan struct{}
}

// announceForegroundAuthority announces one foreground authority operation and returns its
// release, which the caller defers. The release is idempotent.
//
// No lock held on entry.
func (sess *mqttSession) announceForegroundAuthority() (release func()) {
	f := &sess.foreground
	f.mu.Lock()
	if f.n == 0 {
		f.idle = make(chan struct{})
	}
	f.n++
	f.mu.Unlock()
	if h := sess.testForegroundAnnounce.Load(); h != nil {
		(*h)(true)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			if f.n--; f.n == 0 {
				close(f.idle)
				f.idle = nil
			}
			f.mu.Unlock()
			if h := sess.testForegroundAnnounce.Load(); h != nil {
				(*h)(false)
			}
		})
	}
}

// admitBackgroundAuthority admits ONE background authority-transition attempt, waiting while any
// foreground operation is announced. It reports false only on server shutdown: the caller then
// makes no attempt.
//
// No lock held on entry; the caller holds no session, client, account or worker lock.
func (sess *mqttSession) admitBackgroundAuthority() bool {
	f := &sess.foreground
	var quit chan struct{}
	if s := sess.serverFor(nil); s != nil {
		quit = s.quitCh
	}
	for {
		f.mu.Lock()
		if f.n == 0 {
			f.mu.Unlock()
			if h := sess.testBackgroundAdmit.Load(); h != nil {
				(*h)()
			}
			return true
		}
		idle := f.idle
		f.mu.Unlock()
		if h := sess.testBackgroundWait.Load(); h != nil {
			(*h)()
		}
		select {
		case <-idle:
		case <-quit:
			return false
		}
	}
}
