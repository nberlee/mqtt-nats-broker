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
	"testing"
	"time"
)

// The client authority's foreground coordinator (outbound_foreground_priority.go): one entry per
// client identifier, deleted only when it carries nothing, and an announcement's allowance equal
// to the actors registered when it was made.

// TestForegroundCoordinatorEntryLivesExactlyAsLongAsItsState: an entry outlives every announcement
// and every registered actor, in either order of the last two, and is deleted the moment neither
// stands; a recreated entry starts where the deleted one ended.
func TestForegroundCoordinatorEntryLivesExactlyAsLongAsItsState(t *testing.T) {
	const id = "fg-coordinator"
	exists := func(t *testing.T, as *mqttAccountSessionManager, want bool, when string) {
		t.Helper()
		if n, actors, ok := testForegroundEntry(as, id); ok != want {
			t.Fatalf("%s: the entry exists=%v (n=%d actors=%d), want %v", when, ok, n, actors, want)
		}
	}

	t.Run("last deregister, then announce", func(t *testing.T) {
		as := &mqttAccountSessionManager{}
		dereg := as.registerForegroundActor(id)
		exists(t, as, true, "an actor registered")
		dereg()
		dereg()
		exists(t, as, false, "the last actor deregistered")
		h, _ := as.announceForeground(id)
		if h.allowance != 0 {
			t.Fatalf("an announcement after the last deregistration allows %d, want 0", h.allowance)
		}
		h.release()
		exists(t, as, false, "the announcement released")
	})

	t.Run("announce, then last deregister", func(t *testing.T) {
		as := &mqttAccountSessionManager{}
		dereg := as.registerForegroundActor(id)
		h, _ := as.announceForeground(id)
		if h.allowance != 1 {
			t.Fatalf("an announcement with one registered actor allows %d, want 1", h.allowance)
		}
		dereg()
		exists(t, as, true, "the actor deregistered under a standing announcement")
		if _, _, idle := as.admitForegroundOrIdle(id); idle == nil {
			t.Fatal("an attempt was admitted while the announcement stood")
		}
		h.release()
		exists(t, as, false, "the announcement released after the last actor")
	})

	t.Run("an actor's entry survives every release", func(t *testing.T) {
		as := &mqttAccountSessionManager{}
		dereg := as.registerForegroundActor(id)
		h1, _ := as.announceForeground(id)
		h1.release()
		exists(t, as, true, "a release while an actor stands")
		h2, _ := as.announceForeground(id)
		if h2.allowance != 1 {
			t.Fatalf("an announcement after a release allows %d with the actor still registered, want 1", h2.allowance)
		}
		h2.release()
		dereg()
		exists(t, as, false, "everything released")
	})

	t.Run("allowance counts every registered actor", func(t *testing.T) {
		as := &mqttAccountSessionManager{}
		var deregs []func()
		for i := 0; i < 7; i++ {
			deregs = append(deregs, as.registerForegroundActor(id))
		}
		other := as.registerForegroundActor("another-client")
		h, _ := as.announceForeground(id)
		if h.allowance != 7 {
			t.Fatalf("the allowance is %d with seven actors of the client registered, want 7", h.allowance)
		}
		// An actor registering AFTER the snapshot is not in the allowance and is not admitted.
		late := as.registerForegroundActor(id)
		if _, _, idle := as.admitForegroundOrIdle(id); idle == nil {
			t.Fatal("an actor registered after the announcement was admitted")
		}
		if _, _, idle := as.admitForegroundOrIdle("another-client"); idle != nil {
			t.Fatal("another client's actor waited on this client's announcement")
		}
		h.release()
		late()
		for _, d := range deregs {
			d()
		}
		other()
		exists(t, as, false, "everything released")
		if _, _, ok := testForegroundEntry(as, "another-client"); ok {
			t.Fatal("the other client's entry outlived its only actor")
		}
	})

	t.Run("admission identities are unique and never reused", func(t *testing.T) {
		as := &mqttAccountSessionManager{}
		seen := map[uint64]bool{}
		for i := 0; i < 5; i++ {
			adm, _, idle := as.admitForegroundOrIdle(id)
			if idle != nil || adm == 0 || seen[adm] {
				t.Fatalf("admission %d: identity %d (idle %v), seen before %v", i, adm, idle != nil, seen[adm])
			}
			seen[adm] = true
		}
		exists(t, as, false, "admissions alone")
	})
}

// testForegroundEvents records the coordinator's events for one client, in the order the
// coordinator decided them.
type testForegroundEvents struct {
	mu  sync.Mutex
	evs []mqttForegroundEvent
}

func testRecordForegroundEvents(t *testing.T, id string) *testForegroundEvents {
	t.Helper()
	r := &testForegroundEvents{}
	h := func(ev mqttForegroundEvent) {
		if ev.ID != id {
			return
		}
		r.mu.Lock()
		r.evs = append(r.evs, ev)
		r.mu.Unlock()
	}
	prev := mqttTestForegroundEventV.Swap(&h)
	t.Cleanup(func() { mqttTestForegroundEventV.Store(prev) })
	return r
}

// admittedSince counts the admissions decided after the announcement with stamp.
func (r *testForegroundEvents) admittedSince(stamp uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, after := 0, false
	for _, ev := range r.evs {
		if ev.Kind == mqttForegroundAnnounced && ev.Stamp == stamp {
			after = true
		}
		if after && ev.Kind == mqttForegroundAdmitted {
			n++
		}
	}
	return n
}

// TestRetiredSessionObjectsWorkerYieldsToItsSuccessorsForeground: a session object's worker held
// inside its work loop across that object's removal and its successor's creation is still an actor
// of the CLIENT: the coordinator entry outlives the object while the worker runs, and the worker's
// next attempt waits for the successor's announcement to be released.
func TestRetiredSessionObjectsWorkerYieldsToItsSuccessorsForeground(t *testing.T) {
	e := testNewInflightEnv(t, "fg-successor")
	old, as, id := e.sess, e.asm, e.sess.id
	evs := testRecordForegroundEvents(t, id)
	// RESIDENT, as a connected client's session is: a worker of a non-resident object stops.
	as.mu.Lock()
	prevResident := as.sessions[id]
	as.sessions[id] = old
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		if prevResident == nil {
			delete(as.sessions, id)
		} else {
			as.sessions[id] = prevResident
		}
		as.mu.Unlock()
	})

	held, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	step := func(mqttRetainedReclaimKey, mqttReclaimStep, error) {
		once.Do(func() {
			close(held)
			<-resume
		})
	}
	old.testReclaimAfterStep.Store(&step)
	waiting := make(chan struct{}, 16)
	wait := func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}
	old.testBackgroundWait.Store(&wait)
	defer old.testBackgroundWait.Store(nil)

	// Two copies, so the worker's loop offers a second step after the first is held. Neither
	// resource exists: each step answers withheld, which is all the loop needs to keep offering.
	rc := &old.retainedReclaim
	k1 := mqttRetainedReclaimKey{gen: old.gen, inc: "inc-absent-1"}
	k2 := mqttRetainedReclaimKey{gen: old.gen, inc: "inc-absent-2"}
	rc.mu.Lock()
	rc.pending = map[mqttRetainedReclaimKey]*mqttRetainedReclaimState{k1: {}, k2: {}}
	rc.mu.Unlock()
	rc.wake(old, k1.gen, k1.inc)
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the old object's reclaimer never took its first step")
	}

	// THE OBJECT IS REPLACED: removed from the account, a successor resident in its place.
	succ := &mqttSession{id: id, idHash: old.idHash, asm: as, jsa: old.jsa}
	as.mu.Lock()
	as.sessions[id] = succ
	as.mu.Unlock()
	probe := succ.announceForegroundAuthority()
	probe.release()
	if _, actors, ok := testForegroundEntry(as, id); !ok || actors < 1 {
		t.Fatalf("the client's coordinator entry did not survive the old object's removal while its "+
			"worker ran: exists=%v actors=%d", ok, actors)
	}
	h := succ.announceForegroundAuthority()
	if h.allowance < 1 {
		t.Fatalf("the successor's announcement allows %d, but the old object's worker is registered", h.allowance)
	}
	_, stamp := testLastAnnouncement(evs)
	close(resume)
	select {
	case <-waiting:
	case <-time.After(10 * time.Second):
		t.Fatalf("the old object's worker never waited on its successor's announcement (%d admissions after it)",
			evs.admittedSince(stamp))
	}
	if n := evs.admittedSince(stamp); n != 0 {
		t.Fatalf("%d background attempts were admitted while the successor's foreground stood", n)
	}
	h.release()
	// The worker, no longer resident, ends its run and deregisters: the entry carries nothing.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		n, actors, ok := testForegroundEntry(as, id)
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the entry still stands after the old object's worker ended: n=%d actors=%d", n, actors)
		}
	}
}

// testLastAnnouncement returns the latest announcement event's allowance and stamp.
func testLastAnnouncement(r *testForegroundEvents) (int, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.evs) - 1; i >= 0; i-- {
		if r.evs[i].Kind == mqttForegroundAnnounced {
			return r.evs[i].Allowance, r.evs[i].Stamp
		}
	}
	return 0, 0
}
