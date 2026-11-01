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

import (
	"sync"
	"sync/atomic"
)

// FOREGROUND PRIORITY over this broker's background authority writers (F-105 7c, consult 105).
//
// Every outbound authority transition of a client but the authority-free completions
// (mqttAttemptAuthorityFree) CASes the one authority record, so this broker's own writers
// serialize on it by construction. A completion contends only on the records it writes
// (exchange, register, resources), which the same background writers may also write, so it stays
// announced. The retained driver (materialization, admission, acquisition, withdrawal,
// abandonment), the reclaimer (one transition per reclamation step) and the account's recovery
// of a previous generation (one transition per duty step) produce BURSTS of such writes — after a
// SUBSCRIBE, after every retained acknowledgement, withdrawal and abandonment, and after every
// close of a generation whose retirement recovery performs; a foreground operation whose bounded
// re-attempts all land inside such a burst fails although nothing contradicted it.
//
// ONE COORDINATOR PER CLIENT AUTHORITY. The authority record is per client identifier, so the
// handshake is too: the account manager holds one coordinator per client identifier
// (mqttAccountSessionManager.foreground), looked up by identifier at every use under ONE
// account-level mutex (fgMu) that also guards every counter of every entry, so a lookup, its
// check and its update are one critical section and no entry is used after the lock is dropped.
// A session object holds no reference to it: a clean-session reuse builds a fresh session object,
// and the previous object's workers and the previous generation's recovery must still yield to
// the new object's foreground. An entry exists only while it carries state — an announcement or a
// registered actor — and is deleted under fgMu the moment it carries none, so a deleted and
// recreated entry is indistinguishable from the one it replaced, whichever session object last
// touched it.
//
// THE HANDSHAKE. A foreground operation ANNOUNCES itself once, before its first relevant read, and
// stays announced across every attempt and contention wait until it returns. A background ACTOR
// REGISTERS with its client's coordinator for its lifetime and ADMITS each authority-transition
// attempt — one admission authorizes at most ONE attempt, checked again before every retry and
// every later transition, before that attempt's reads — and an admission is granted only while
// no foreground operation is announced. An attempt admitted just before an announcement may still
// read and commit after it; that is allowed, and it is the whole of what background work can take
// from the foreground. The announcement therefore SNAPSHOTS the registered actors as its
// ALLOWANCE: every actor is single-flight and holds at most one admitted attempt at a time, an
// actor registering after the snapshot is not admitted while the announcement stands, so a
// continuously announced foreground operation loses at most `allowance` attempts to admitted
// background work, however many session objects or recovery visits of the client are running.
//
// The actors: the five single-flight session workers (mqttBackgroundAuthorityWriters), registered
// at their entry for as long as they run, and each recovery visit of a root, registered for the
// visit (outbound_recovery_pass.go). foreground_priority_census_test.go pins both, and classifies
// every authority transition background code submits as admitted or as one of the exclusions
// below.
//
// RECOVERY NEVER WAITS. A visit checks its client's coordinator at its entry, before any cursor,
// roll, conclusion or duty, and does no root work at all while an announcement stands; past entry
// every authority attempt it makes — each duty step, each floor advancement in its conclusion —
// admits with the non-waiting form before its reads, and a refused attempt marks the visit
// REFUSED to its end. A refused visit is resumed, not charged: the prompt drive requeues it behind
// a backoff (outbound_recovery_kick.go), the periodic pass kicks a refused root into the prompt
// drive and otherwise revisits it on its next cycle.
//
// BUDGETS ARE DERIVED FROM THE ALLOWANCE. An announced loop on mqttForegroundAcquireAttempts or
// mqttSubscribeClaimAttempts runs handle.budget(base) = max(base, allowance + base -
// mqttBackgroundAuthorityWriters): never fewer attempts than before, and always the margin over
// the allowance the base had over the five session workers. bounded_reattempt_census_test.go
// classifies every loop on either budget.
//
// Foreground since F-112: the ordinary and shared delivery admission ladder
// (admitThroughAuthority), against the floor unit's background writes as well as the retained
// ones (F-114). The attempts the ladder spends classifying and cleaning up an occupied identifier
// are not covered (docs/CONFORMANCE.md). Foreground too: a delivery's send-section acquisition
// (acquirePublishAction), unless the section is the retained driver's own; and, with the same
// budget and the contention wait after an attempt whose batch reached its commit (a handle): the
// acknowledgement's terminal release, the PUBCOMP terminalization, the recovered PUBREC, the
// cancellation's withdrawal or tombstone (markInflightRegisterCancelled) and the PUBREL
// acquisition, the last two only when their caller passes no admission — an admitting caller is
// background and would wait on its own announcement. The PUBREL callback's redispatch
// (admitStoredPubRel) is announced across its whole loop, not only inside each write it reaches.
// The bound counts attempts lost to the pre-admitted actors only; attempts a loop spends on its own
// classification or cleanup are not covered by it.
//
// What none of this covers, stated rather than implied: writes by another broker; the one recovery
// exclusion, the adopted teardown's completion (finishAdoptedTeardown's settlement and close);
// writes background code makes WITHOUT an admission (the retained PUBREL action drain's completion,
// the denied delivery's disposal timer step, abandonOwnProposal's asynchronous settlement,
// record-driven expiry's clear through settleDiscardAuthority, the custody dispatcher's units
// other than the floor unit); foreground operations of the same
// client contending with each other; and store failures. Background waits, and recovery is refused,
// for as long as any foreground operation of its client is announced, so continuous foreground
// traffic on one client delays that client's background work and cleanup without bound; a
// foreground operation that never returns keeps them waiting (it is never cleared on a timer while
// it could still write). A waiter exits on server shutdown without being granted an attempt.
//
// The foreground side never waits on the background side, and neither side holds fgMu across a
// read, a commit, a sleep or any other lock.

// mqttBackgroundAuthorityWriters is how many single-flight session workers a client's session
// object runs, each admitting one attempt at a time: the retained driver (runRetainedDriver), the
// retained continuations (runRetainedContinuations), the reclaimer (runRetainedReclaim, which also
// steps the source retirement owner), the custody dispatcher (runCustodyDispatcher, which runs the
// floor unit) and exclusion reclaim (runExclusionReclaim). It is no longer the bound itself — the
// announcement's allowance is, counting every registered actor of every session object and every
// recovery visit of the client — but the base budgets were cut against it, so each budget's margin
// is its base minus this. foreground_priority_census_test.go pins that every admission belongs to
// a registered actor and that each worker is launched only behind its single-flight guard.
const mqttBackgroundAuthorityWriters = 5

// mqttForegroundAcquireAttempts is an announced foreground authority loop's base budget: one
// attempt more than the session workers of one session object can take from it.
const mqttForegroundAcquireAttempts = mqttBackgroundAuthorityWriters + 1

// mqttForegroundAuthority is one client authority's coordinator entry. Every field is guarded by
// the account's fgMu.
type mqttForegroundAuthority struct {
	// n counts the standing announcements.
	n int
	// idle is closed when n returns to zero; a fresh one is made when it leaves zero.
	idle chan struct{}
	// actors counts the registered background actors.
	actors int
}

// stateless reports whether the entry carries nothing a deleted entry would not.
func (f *mqttForegroundAuthority) stateless() bool {
	return f.n == 0 && f.actors == 0
}

// mqttForegroundHandle is one announcement: its release, which the caller defers and which is
// idempotent, and its allowance — the background actors registered when it was made, each of
// which may hold at most one attempt admitted before it.
type mqttForegroundHandle struct {
	release   func()
	allowance int
}

// budget is an announced loop's attempt budget for a base budget cut against the five session
// workers: never fewer than base, and the base's margin over those workers above the allowance.
func (h mqttForegroundHandle) budget(base int) int {
	return max(base, h.allowance+base-mqttBackgroundAuthorityWriters)
}

// mqttForegroundEventKind names one coordinator event a test seam observes.
type mqttForegroundEventKind uint8

const (
	mqttForegroundAnnounced mqttForegroundEventKind = iota + 1
	mqttForegroundReleased
	mqttForegroundAdmitted
	mqttForegroundRefused
	mqttForegroundRegistered
	mqttForegroundDeregistered
)

// mqttForegroundEvent is one coordinator event as it stood under fgMu.
type mqttForegroundEvent struct {
	Kind mqttForegroundEventKind
	ID   string
	// N and Actors are the entry's counts after the event; Entry reports whether the entry
	// still exists after it.
	N, Actors int
	Entry     bool
	// Stamp is the account's announcement sequence: the announcement's own for an announcement
	// or release, the latest made for any other event.
	Stamp uint64
	// Admission is an admitted attempt's identity, unique per account.
	Admission uint64
	// Allowance is an announcement's snapshot of the registered actors.
	Allowance int
}

// mqttTestForegroundEventV, when set, observes every coordinator event UNDER fgMu, so the order
// it sees is the order the coordinator decided in. It must not call back into the coordinator.
// Nil in production.
var mqttTestForegroundEventV atomic.Pointer[func(mqttForegroundEvent)]

// fgEventLocked reports one event to the test seam.
//
// fgMu held on entry.
func (as *mqttAccountSessionManager) fgEventLocked(kind mqttForegroundEventKind, id string,
	f *mqttForegroundAuthority, stamp, adm uint64, allowance int) {

	h := mqttTestForegroundEventV.Load()
	if h == nil {
		return
	}
	ev := mqttForegroundEvent{Kind: kind, ID: id, Stamp: stamp, Admission: adm, Allowance: allowance}
	if f != nil {
		ev.N, ev.Actors = f.n, f.actors
	}
	_, ev.Entry = as.foreground[id]
	(*h)(ev)
}

// fgEntryLocked returns id's entry, creating it.
//
// fgMu held on entry.
func (as *mqttAccountSessionManager) fgEntryLocked(id string) *mqttForegroundAuthority {
	if as.foreground == nil {
		as.foreground = make(map[string]*mqttForegroundAuthority)
	}
	f := as.foreground[id]
	if f == nil {
		f = &mqttForegroundAuthority{}
		as.foreground[id] = f
	}
	return f
}

// fgSettleLocked deletes id's entry if it carries no state.
//
// fgMu held on entry.
func (as *mqttAccountSessionManager) fgSettleLocked(id string, f *mqttForegroundAuthority) {
	if f.stateless() {
		delete(as.foreground, id)
	}
}

// announceForeground announces one foreground authority operation of client id.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) announceForeground(id string) (mqttForegroundHandle, uint64) {
	as.fgMu.Lock()
	f := as.fgEntryLocked(id)
	if f.n == 0 {
		f.idle = make(chan struct{})
	}
	f.n++
	as.fgAnnounced++
	stamp, allowance := as.fgAnnounced, f.actors
	as.fgEventLocked(mqttForegroundAnnounced, id, f, stamp, 0, allowance)
	as.fgMu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			as.fgMu.Lock()
			// THE ENTRY IS LOOKED UP AGAIN, never kept: it cannot have been deleted while this
			// announcement stood, and reading it by identifier is what makes that a fact rather
			// than an assumption about a pointer.
			f := as.fgEntryLocked(id)
			if f.n--; f.n == 0 {
				close(f.idle)
				f.idle = nil
			}
			as.fgSettleLocked(id, f)
			as.fgEventLocked(mqttForegroundReleased, id, f, stamp, 0, 0)
			as.fgMu.Unlock()
		})
	}
	return mqttForegroundHandle{release: release, allowance: allowance}, stamp
}

// registerForegroundActor registers one background actor of client id for its lifetime and
// returns its deregistration, which the actor defers. An actor that admits without being
// registered is outside the allowance, which is what the census forbids.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) registerForegroundActor(id string) (deregister func()) {
	as.fgMu.Lock()
	f := as.fgEntryLocked(id)
	f.actors++
	as.fgEventLocked(mqttForegroundRegistered, id, f, as.fgAnnounced, 0, 0)
	as.fgMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			as.fgMu.Lock()
			f := as.fgEntryLocked(id)
			f.actors--
			as.fgSettleLocked(id, f)
			as.fgEventLocked(mqttForegroundDeregistered, id, f, as.fgAnnounced, 0, 0)
			as.fgMu.Unlock()
		})
	}
}

// admitForegroundOrIdle admits ONE background attempt of client id if no announcement stands,
// returning its admission identity and the announcement stamp it was decided at; otherwise it
// admits nothing and returns the channel that closes when the client is next idle. It never
// creates an entry: a client with none has nothing announced.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) admitForegroundOrIdle(id string) (adm, stamp uint64, idle chan struct{}) {
	as.fgMu.Lock()
	defer as.fgMu.Unlock()
	f := as.foreground[id]
	if f != nil && f.n > 0 {
		as.fgEventLocked(mqttForegroundRefused, id, f, as.fgAnnounced, 0, 0)
		return 0, as.fgAnnounced, f.idle
	}
	as.fgAdmitted++
	as.fgEventLocked(mqttForegroundAdmitted, id, f, as.fgAnnounced, as.fgAdmitted, 0)
	return as.fgAdmitted, as.fgAnnounced, nil
}

// foregroundAnnounced reports whether a foreground operation of client id is announced now,
// admitting nothing. It is the recovery visit's ENTRY check, which grants nothing for the rest of
// the visit.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) foregroundAnnounced(id string) bool {
	as.fgMu.Lock()
	defer as.fgMu.Unlock()
	f := as.foreground[id]
	if f == nil || f.n == 0 {
		return false
	}
	as.fgEventLocked(mqttForegroundRefused, id, f, as.fgAnnounced, 0, 0)
	return true
}

// announceForegroundAuthority announces one foreground authority operation of this session's
// client and returns its handle, whose release the caller defers.
//
// A session object no account manager holds serves no connection and runs no worker, so it has no
// coordinator to announce in: its handle releases nothing and allows nothing.
//
// No lock held on entry.
func (sess *mqttSession) announceForegroundAuthority() mqttForegroundHandle {
	as := sess.asm
	if as == nil {
		return mqttForegroundHandle{release: func() {}}
	}
	h, stamp := as.announceForeground(sess.id)
	if hook := sess.testForegroundAnnounce.Load(); hook != nil {
		(*hook)(true, stamp)
	}
	var once sync.Once
	release := h.release
	h.release = func() {
		once.Do(func() {
			release()
			if hook := sess.testForegroundAnnounce.Load(); hook != nil {
				(*hook)(false, stamp)
			}
		})
	}
	return h
}

// registerForegroundActor registers the calling session worker with its client's coordinator for
// the worker's lifetime; the worker defers the returned deregistration at its entry.
//
// No lock held on entry.
func (sess *mqttSession) registerForegroundActor() (deregister func()) {
	if sess.asm == nil {
		return func() {}
	}
	return sess.asm.registerForegroundActor(sess.id)
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
	if sess.asm == nil {
		return true
	}
	_, stamp, idle := sess.asm.admitForegroundOrIdle(sess.id)
	if idle != nil {
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
// foreground operation of this session's client is announced. It reports false only on server
// shutdown: the caller then makes no attempt.
//
// No lock held on entry; the caller holds no session, client, account or worker lock.
func (sess *mqttSession) admitBackgroundAuthority() bool {
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
		if sess.asm == nil {
			return true
		}
		_, stamp, idle := sess.asm.admitForegroundOrIdle(sess.id)
		if idle == nil {
			if h := sess.testBackgroundAdmit.Load(); h != nil {
				(*h)(stamp)
			}
			return true
		}
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

// admitRecoveryAttempt admits ONE authority attempt of a recovery visit of client id's root,
// never waiting. A refusal is remembered for the rest of the visit (b.refused): the visit admits
// nothing more, and it ends REFUSED whatever its later bookkeeping does.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) admitRecoveryAttempt(id string, b *mqttRecoveryBudget) bool {
	if b.refused {
		return false
	}
	if _, _, idle := as.admitForegroundOrIdle(id); idle != nil {
		b.refused = true
		return false
	}
	return true
}
