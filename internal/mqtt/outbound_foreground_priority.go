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

import "sync"

// FOREGROUND PRIORITY over this broker's retained background writers (F-105 7c, consult 105).
//
// Every outbound authority transition of a session but the authority-free completions
// (mqttAttemptAuthorityFree) CASes the one authority record, so this broker's own writers
// serialize on it by construction. A completion contends only on the records it writes
// (exchange, register, resources), which the same background writers may also write, so it stays
// announced. The retained driver (materialization,
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
// ones. Its budget is mqttForegroundAcquireAttempts too (F-114), so the B-attempt allowance above
// cannot exhaust an admission to an empty identifier; the attempts the ladder spends classifying
// and cleaning up an occupied one are not covered by it (docs/CONFORMANCE.md).
//
// Foreground too: a delivery's send-section acquisition (acquirePublishAction), unless the
// section is the retained driver's own. Its budget, mqttForegroundAcquireAttempts, is one more
// than the writers that can each hold an attempt admitted before its announcement, so those
// attempts alone cannot exhaust it.
//
// And, with the same budget and the contention wait after an attempt whose batch reached its
// commit (a handle): the acknowledgement's terminal release, the PUBCOMP terminalization, the
// recovered PUBREC, the cancellation's withdrawal or tombstone (markInflightRegisterCancelled) and
// the PUBREL acquisition, the last two only when their caller passes no admission — an admitting
// caller is background and would wait on its own announcement. The PUBREL callback's redispatch
// (admitStoredPubRel) is announced across its whole loop, not only inside each write it reaches.
// The bound counts attempts lost to the pre-admitted writers only; attempts a loop spends on its
// own classification or cleanup are not covered by it. bounded_reattempt_census_test.go
// classifies every loop on either budget.
//
// What none of this covers, stated rather than implied: writes by another broker, by writers
// outside this protocol (the recovery pass, the custody observer's other units), writes background
// code makes WITHOUT an admission (the retained PUBREL action drain's completion, the denied
// delivery's disposal timer step), foreground operations of the same session contending with each
// other, and store failures. Background waits for as long as any foreground operation is
// announced, so continuous foreground traffic can delay it without bound; a foreground operation
// that never returns keeps background waiting (it is never cleared on a timer while it could still
// write). A waiter exits on server shutdown without being granted an attempt.
//
// The foreground side never waits on the background side, and neither side holds this mutex
// across a read, a commit, a sleep or any other lock.

// mqttBackgroundAuthorityWriters is how many attempts admitted BEFORE an announcement can still
// commit after it: one per background writer, each single-flight per session and admitting one
// attempt at a time. They are the retained driver (runRetainedDriver), the retained continuations
// (runRetainedContinuations), the reclaimer (runRetainedReclaim, which also steps the source
// retirement owner), the custody dispatcher (runCustodyDispatcher, which runs the floor unit) and
// exclusion reclaim (runExclusionReclaim). foreground_priority_census_test.go pins both halves of
// the claim: every admission belongs to one of them, and each is launched only behind its
// single-flight guard. A sixth writer, or a second attempt per writer, raises this number.
const mqttBackgroundAuthorityWriters = 5

// mqttForegroundAcquireAttempts is an announced foreground authority loop's budget: one attempt
// more than the background writers can take from it.
const mqttForegroundAcquireAttempts = mqttBackgroundAuthorityWriters + 1

// mqttForegroundAuthority is one session's announcement count.
type mqttForegroundAuthority struct {
	mu sync.Mutex
	n  int
	// idle is closed when the count returns to zero; a fresh one is made when it leaves zero.
	idle chan struct{}
	// announced counts announcements ever made. Test hooks stamp an announcement and an admission
	// with it, read under mu, so an observer can tell an admission decided before an announcement
	// from one decided after it however the two hooks interleave.
	announced uint64
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
	f.announced++
	stamp := f.announced
	f.mu.Unlock()
	if h := sess.testForegroundAnnounce.Load(); h != nil {
		(*h)(true, stamp)
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
				(*h)(false, stamp)
			}
		})
	}
}

// tryAdmitBackgroundAuthority admits ONE background authority-transition attempt only if no
// foreground operation is announced now, and never waits. It is the admission of a unit that runs
// on the session's shared custody dispatcher: waiting there would hold every other custody unit
// (hints, owed withdrawals, the hold releaser) behind the foreground for as long as it lasts. A
// refused unit backs off and is retried by the dispatcher instead. False also on server shutdown.
//
// No lock held on entry; the caller holds no session, client, account or worker lock.
func (sess *mqttSession) tryAdmitBackgroundAuthority() bool {
	if s := sess.serverFor(nil); s != nil {
		select {
		case <-s.quitCh:
			return false
		default:
		}
	}
	f := &sess.foreground
	f.mu.Lock()
	idle, stamp := f.n == 0, f.announced
	f.mu.Unlock()
	if !idle {
		if h := sess.testBackgroundWait.Load(); h != nil {
			(*h)()
		}
		return false
	}
	if h := sess.testBackgroundAdmit.Load(); h != nil {
		(*h)(stamp)
	}
	return true
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
		// A stopped server admits nothing, idle or not: an admitted attempt would only issue a
		// request that can no longer be answered, and a caller that loops on admission would
		// never learn the server is gone.
		if quit != nil {
			select {
			case <-quit:
				return false
			default:
			}
		}
		f.mu.Lock()
		if f.n == 0 {
			stamp := f.announced
			f.mu.Unlock()
			if h := sess.testBackgroundAdmit.Load(); h != nil {
				(*h)(stamp)
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
