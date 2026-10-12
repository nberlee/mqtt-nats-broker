//go:build mqttport_fulltests

// Tests for an OWED-DELAYED Will meeting a same-client-ID takeover: the displaced
// connection has closed and classified its Will, its Will Delay Interval is running or has
// run out, and a CONNECT takes the session over before anything enqueued it.
//
// The takeover used to discard every Will it found that was not owed immediately, with no
// date test, so a delay that had already run out was destroyed. A takeover now destroys
// only an armed Will and consumes an owed-delayed one into the outbox, where the
// acceptance facts decide it: suppressed by a reconnect accepted before its deadline,
// published otherwise. Every interleaving below is FORCED with seams, because the window
// is a few statements wide and the fixture that first tested it excluded exactly the
// ordering that breaks it. Spec5 [3.1.2.5], [MQTT-3.1.2-8], [MQTT-3.1.3-9].

package mqtt

import (
	"net"
	"sync"
	"testing"
	"time"
)

// setMQTTTestDelayedWillEnqueuedHook installs the confirmed-enqueue observer
// (mqttTestDelayedWillEnqueuedV) and returns its restore.
func setMQTTTestDelayedWillEnqueuedHook(h func(rec *mqttWillOutbox)) func() {
	prev := mqttTestDelayedWillEnqueuedV.Load()
	if h == nil {
		mqttTestDelayedWillEnqueuedV.Store(nil)
	} else {
		mqttTestDelayedWillEnqueuedV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDelayedWillEnqueuedV.Store(prev) })
}

// testOwedDelayedFixture is one server with a Will witness, and the hooks every arm
// needs: the enqueued records for the client under test, and the seams.
//
// Not parallel: the confirmed-enqueue hook is package-global, so two fixtures installed at
// once would each see only the later one's records.
type testOwedDelayedFixture struct {
	t     *testing.T
	o     *Options
	s     *Server
	asm   *mqttAccountSessionManager
	topic string

	mu      sync.Mutex
	records []*mqttWillOutbox
}

func testNewOwedDelayedFixture(t *testing.T, topic string) *testOwedDelayedFixture {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	f := &testOwedDelayedFixture{t: t, o: o, s: s, asm: testMQTTGetGlobalASM(t, s), topic: topic}
	t.Cleanup(setMQTTTestDelayedWillEnqueuedHook(func(rec *mqttWillOutbox) {
		f.mu.Lock()
		f.records = append(f.records, rec)
		f.mu.Unlock()
	}))
	return f
}

// enqueued returns the confirmed outbox records written for clientID so far.
func (f *testOwedDelayedFixture) enqueued(clientID string) []*mqttWillOutbox {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*mqttWillOutbox
	for _, r := range f.records {
		if r.ClientID == clientID {
			out = append(out, r)
		}
	}
	return out
}

// connectOld connects the client under test with a delayed Will on a session that
// outlives the connection, and returns its socket plus the server-side session and client.
func (f *testOwedDelayedFixture) connectOld(id string, delay uint32) (net.Conn, *mqttSession, *client) {
	f.t.Helper()
	will := &mqttWill{topic: []byte(f.topic), message: []byte("owed-" + id), qos: 0}
	cw, rw := testMQTTConnectV5(f.t, &mqttV5ConnInfo{clientID: id, cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(delay), props: mqttV5ConnPropsSessionExpiry(30)},
		f.o.MQTT.Host, f.o.MQTT.Port)
	testMQTTReadConnAckV5(f.t, rw)
	sess, bound := testReDriveResident(f.asm, id)
	if sess == nil || !bound {
		f.t.Fatalf("the fixture needs a resident, bound session for %q", id)
	}
	sess.mu.Lock()
	oc := sess.c
	sess.mu.Unlock()
	return cw, sess, oc
}

// baseline is the session's (gen, seq) as the displaced connection left it.
func testOwedDelayedBaseline(sess *mqttSession) (string, uint64) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.gen, sess.seq
}

// willState reads the client's Will slot.
func testOwedDelayedWillState(c *client) (bool, mqttWillDisposition, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mqtt == nil || c.mqtt.cp == nil {
		return false, mqttWillDischarged, 0
	}
	return c.mqtt.cp.will != nil, c.mqtt.willDisposition, c.mqtt.closeObservedAt
}

// waitPastDeadline sleeps until closeObservedAt + delay has passed by a margin.
func testOwedDelayedWaitPast(closeAt int64, delay uint32) {
	due := time.Unix(0, closeAt).Add(time.Duration(delay)*time.Second + 300*time.Millisecond)
	if d := time.Until(due); d > 0 {
		time.Sleep(d)
	}
}

// replace runs a replacement CONNECT on its own goroutine and returns a channel that
// yields it once the CONNACK has been read.
func (f *testOwedDelayedFixture) replace(id string, cleanStart bool) <-chan net.Conn {
	done := make(chan net.Conn, 1)
	go func() {
		c, r := testMQTTConnectV5(f.t, &mqttV5ConnInfo{clientID: id, cleanStart: cleanStart,
			props: mqttV5ConnPropsSessionExpiry(30)}, f.o.MQTT.Host, f.o.MQTT.Port)
		testMQTTReadConnAckV5(f.t, r)
		done <- c
	}()
	return done
}

func (f *testOwedDelayedFixture) awaitConn(ch <-chan net.Conn) net.Conn {
	f.t.Helper()
	select {
	case c := <-ch:
		f.t.Cleanup(func() { c.Close() })
		return c
	case <-time.After(testMQTTTimeout):
		f.t.Fatal("the replacement CONNECT never completed")
		return nil
	}
}

// testOwedDelayedTakeoverAfterReturn drives the acceptance criterion's ordering: the
// takeover rebinds and parks before deciding the Will, the displaced connection then closes,
// classifies its Will owed-delayed, and its teardown RETURNS through the lost-owner exit,
// and only then does the takeover decide. between runs after the teardown returned and
// before the takeover is released.
func (f *testOwedDelayedFixture) takeoverAfterReturn(id string, delay uint32, cleanStart bool,
	between func(closeAt int64)) (string, uint64) {

	f.t.Helper()
	cw, sess, oc := f.connectOld(id, delay)
	gen, seq := testOwedDelayedBaseline(sess)

	rebound := make(chan struct{})
	releaseTakeover := make(chan struct{})
	var once sync.Once
	testMQTTSetTakeoverDiscardSeam(f.asm, func(cid string) {
		if cid != id {
			return
		}
		once.Do(func() { close(rebound) })
		<-releaseTakeover
	})
	f.t.Cleanup(func() { testMQTTSetTakeoverDiscardSeam(f.asm, nil) })
	returned := make(chan struct{})
	var rOnce sync.Once
	f.asm.mu.Lock()
	f.asm.testAfterTeardownOwnerLost = func(cid string) {
		if cid == id {
			rOnce.Do(func() { close(returned) })
		}
	}
	f.asm.mu.Unlock()
	f.t.Cleanup(func() {
		f.asm.mu.Lock()
		f.asm.testAfterTeardownOwnerLost = nil
		f.asm.mu.Unlock()
	})

	done := f.replace(id, cleanStart)
	select {
	case <-rebound:
	case <-time.After(testMQTTTimeout):
		f.t.Fatal("the replacement CONNECT never reached the takeover seam")
	}
	cw.Close()
	select {
	case <-returned:
	case <-time.After(testMQTTTimeout):
		close(releaseTakeover)
		f.t.Fatal("the displaced teardown never reached its lost-owner exit")
	}
	has, disp, closeAt := testOwedDelayedWillState(oc)
	if !has || disp != mqttWillOwedDelayed || closeAt == 0 {
		close(releaseTakeover)
		f.t.Fatalf("precondition: the returned teardown must leave an owed-delayed, anchored Will "+
			"(has=%v disposition=%v closeObservedAt=%d)", has, disp, closeAt)
	}
	if between != nil {
		between(closeAt)
	}
	close(releaseTakeover)
	f.awaitConn(done)
	return gen, seq
}

func TestMQTTTakeoverOwedDelayedWill(t *testing.T) {

	// The acceptance criterion: the displaced teardown has RETURNED, the delay has run
	// out, and only then does the takeover decide. The Will is owed and must be published,
	// once, through a confirmed outbox record (claim-and-decide), not around it.
	t.Run("elapsed, displaced teardown already returned", func(t *testing.T) {
		const topic, id = "wtod/t1", "wtod1"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		var released time.Time
		f.takeoverAfterReturn(id, 1, false, func(closeAt int64) {
			// M5's precondition: the displaced connection's own acceptance fact sits above
			// sequence 1, so a baseline of 1 genuinely admits it.
			if _, seqs := testFactsFor(t, f.asm, id); len(seqs) == 0 || seqs[len(seqs)-1] <= 1 {
				t.Fatalf("precondition: own acceptance fact above sequence 1, got %v", seqs)
			}
			testOwedDelayedWaitPast(closeAt, 1)
			released = time.Now()
		})
		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 publication of the elapsed Will, got %d", n)
		}
		recs := f.enqueued(id)
		if len(recs) != 1 {
			t.Fatalf("expected the Will to go through exactly one confirmed outbox record, got %d", len(recs))
		}
		if fireAt := time.Unix(0, recs[0].FireAt); fireAt.After(released) {
			t.Fatalf("the record's FireAt %v must be the close-anchored deadline, already past at %v", fireAt, released)
		}
	})

	// The other order: the takeover decides first, while the teardown that classified the
	// Will is still parked before the gate. The tail is the owner either way.
	t.Run("elapsed, takeover decides before the teardown observes the rebind", func(t *testing.T) {
		const topic, id = "wtod/t2", "wtod2"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		cw, _, oc := f.connectOld(id, 1)
		barrier := testMQTTNewTeardownBarrier(id)
		testMQTTSetTeardownSeam(f.asm, barrier.hook)
		defer testMQTTSetTeardownSeam(f.asm, nil)
		cw.Close()
		barrier.waitReached(t)
		_, disp, closeAt := testOwedDelayedWillState(oc)
		if disp != mqttWillOwedDelayed {
			barrier.unblock()
			t.Fatalf("precondition: owed-delayed before the gate, got %v", disp)
		}
		testOwedDelayedWaitPast(closeAt, 1)
		f.awaitConn(f.replace(id, false))
		barrier.unblock()
		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 publication, got %d", n)
		}
	})

	// Not elapsed: the reconnect is accepted during the delay, so the Will is suppressed —
	// by the outbox's fact scan, against a record written with the DISPLACED incarnation's
	// baseline, not by a discard.
	t.Run("not elapsed, resume takeover suppresses through the outbox", func(t *testing.T) {
		const topic, id = "wtod/t3", "wtod3"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		gen, seq := f.takeoverAfterReturn(id, 3, false, nil)
		recs := f.enqueued(id)
		if len(recs) != 1 {
			t.Fatalf("expected one confirmed outbox record for the owed-delayed Will, got %d", len(recs))
		}
		if recs[0].Gen != gen || recs[0].OriginSessionSeq != seq {
			t.Fatalf("the record must carry the displaced incarnation's baseline (gen %q seq %d), got (gen %q seq %d)",
				gen, seq, recs[0].Gen, recs[0].OriginSessionSeq)
		}
		if n := testMQTTCountPublishes(t, rwit, topic, 5*time.Second, 500*time.Millisecond); n != 0 {
			t.Fatalf("expected the reconnect within the delay to suppress the Will, got %d publications", n)
		}
	})

	// The clean-start takeover re-mints the session object before its tail runs, so the
	// baseline is the retiring incarnation's fence, not the re-minted object.
	t.Run("not elapsed, clean-start takeover uses the retiring fence", func(t *testing.T) {
		const topic, id = "wtod/t4", "wtod4"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		gen, seq := f.takeoverAfterReturn(id, 3, true, nil)
		recs := f.enqueued(id)
		if len(recs) != 1 {
			t.Fatalf("expected one confirmed outbox record, got %d", len(recs))
		}
		if recs[0].Gen != gen || recs[0].OriginSessionSeq != seq {
			t.Fatalf("the record must carry the RETIRING incarnation (gen %q seq %d), got (gen %q seq %d)",
				gen, seq, recs[0].Gen, recs[0].OriginSessionSeq)
		}
		if n := testMQTTCountPublishes(t, rwit, topic, 5*time.Second, 500*time.Millisecond); n != 0 {
			t.Fatalf("expected suppression, got %d publications", n)
		}
	})

	// Elapsed, but an EARLIER qualifying acceptance exists — standing for a remote
	// broker's reconnect during the delay that this broker never observed. Only the fact
	// scan can see it, which is why an elapsed deadline may not be published directly.
	t.Run("elapsed with an earlier qualifying acceptance is suppressed", func(t *testing.T) {
		const topic, id = "wtod/t6", "wtod6"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		f.takeoverAfterReturn(id, 1, false, func(closeAt int64) {
			testPlantAcceptFact(t, f.asm, id, closeAt+int64(100*time.Millisecond))
			testOwedDelayedWaitPast(closeAt, 1)
		})
		if n := testMQTTCountPublishes(t, rwit, topic, 3*time.Second, 500*time.Millisecond); n != 0 {
			t.Fatalf("expected the earlier acceptance to suppress the elapsed Will, got %d publications", n)
		}
		if len(f.enqueued(id)) != 1 {
			t.Fatal("the suppression must have been decided on an outbox record")
		}
	})
}

// TestMQTTUngatedParkTakesTheWillOnlyAtItsCommit pins the ungated park: a close that
// cannot take the gate builds a parked action while another party may rebind or detach
// the session, and the Will may leave its slot only at the park's commit. Taking it
// earlier hid it from the takeover tail or the disposal that consumed the slot, and the
// refused park's restore then left it on a connection nothing would visit again.
func TestMQTTUngatedParkTakesTheWillOnlyAtItsCommit(t *testing.T) {

	// drive closes the old connection while the TEST holds the session gate, so the
	// teardown fails it while still bound and takes the ungated park, parking at the seam
	// before the commit. during runs at the seam, with the gate still held by the test;
	// it returns true if it released the gate itself.
	drive := func(t *testing.T, f *testOwedDelayedFixture, id string,
		during func(sess *mqttSession, oc *client, closeAt int64)) {

		t.Helper()
		cw, sess, oc := f.connectOld(id, 1)
		atSeam := make(chan struct{})
		releaseSeam := make(chan struct{})
		var once sync.Once
		f.asm.mu.Lock()
		f.asm.testBeforeUngatedPark = func(cid string) {
			if cid != id {
				return
			}
			first := false
			once.Do(func() { first = true; close(atSeam) })
			if first {
				<-releaseSeam
			}
		}
		f.asm.mu.Unlock()
		t.Cleanup(func() {
			f.asm.mu.Lock()
			f.asm.testBeforeUngatedPark = nil
			f.asm.mu.Unlock()
		})
		// The CONNECT that accepted this client can still hold the gate just after its
		// CONNACK (its post-acceptance work), so wait for it.
		checkFor(t, testMQTTTimeout, 10*time.Millisecond, func() error {
			return f.asm.lockSession(sess, oc)
		})
		cw.Close()
		select {
		case <-atSeam:
		case <-time.After(testMQTTTimeout):
			f.asm.unlockSession(sess)
			t.Fatal("the closing teardown never reached the ungated park")
		}
		has, disp, closeAt := testOwedDelayedWillState(oc)
		if !has || disp != mqttWillOwedDelayed {
			f.asm.unlockSession(sess)
			close(releaseSeam)
			t.Fatalf("precondition: the Will is still in the slot, owed-delayed, at the seam (has=%v disposition=%v)", has, disp)
		}
		during(sess, oc, closeAt)
		close(releaseSeam)
	}

	// The takeover rebinds and its tail consumes the Will from the slot while the park is
	// paused; the commit then refuses on the binding with nothing taken.
	t.Run("a takeover settles the slot before the commit", func(t *testing.T) {
		const topic, id = "wupk/a", "wupka"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		drive(t, f, id, func(sess *mqttSession, oc *client, closeAt int64) {
			f.asm.unlockSession(sess)
			testOwedDelayedWaitPast(closeAt, 1)
			f.awaitConn(f.replace(id, false))
		})
		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 publication, got %d", n)
		}
	})

	// The same, with the SUCCESSOR uncertain when the commit runs: the commit must refuse
	// on the binding rather than park against the successor's uncertainty and detach it.
	t.Run("the commit does not park against a successor's uncertainty", func(t *testing.T) {
		const topic, id = "wupk/b", "wupkb"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		returned := make(chan struct{})
		var rOnce sync.Once
		f.asm.mu.Lock()
		f.asm.testAfterTeardownOwnerLost = func(cid string) {
			if cid == id {
				rOnce.Do(func() { close(returned) })
			}
		}
		f.asm.mu.Unlock()
		defer func() {
			f.asm.mu.Lock()
			f.asm.testAfterTeardownOwnerLost = nil
			f.asm.mu.Unlock()
		}()
		var succ *client
		var es *mqttSession
		drive(t, f, id, func(sess *mqttSession, oc *client, closeAt int64) {
			f.asm.unlockSession(sess)
			testOwedDelayedWaitPast(closeAt, 1)
			f.awaitConn(f.replace(id, false))
			es = sess
			sess.mu.Lock()
			succ = sess.c
			sess.seqUncertain = true
			sess.mu.Unlock()
		})
		// The correct teardown refuses the commit and leaves through its lost-owner exit. A
		// teardown that parked instead returns without reaching it, so a timeout here is
		// not itself the verdict: the assertions below say what happened to the successor.
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
		}
		es.mu.Lock()
		boundNow, parked := es.c, es.deferredDisconnect
		es.seqUncertain = false
		es.mu.Unlock()
		if boundNow != succ || succ == nil {
			t.Fatal("the displaced connection's park detached the successor")
		}
		if parked != nil {
			t.Fatal("the displaced connection parked an action on the successor's uncertainty")
		}
		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 publication, got %d", n)
		}
	})

	// A foreign disposal detaches the client and consumes the Will from the slot while the
	// park is paused; the commit then refuses on the binding with nothing taken.
	t.Run("a disposal consumes the slot before the commit", func(t *testing.T) {
		const topic, id = "wupk/c", "wupkc"
		f := testNewOwedDelayedFixture(t, topic)
		rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
		defer closeWit()
		drive(t, f, id, func(sess *mqttSession, oc *client, closeAt int64) {
			testOwedDelayedWaitPast(closeAt, 1)
			d := sess.disposeForeignStateLocked(f.asm, oc, false)
			d.applyWill(sess, f.asm)
			f.asm.unlockSession(sess)
		})
		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 publication (the disposal's), got %d", n)
		}
	})
}

// TestMQTTParkOnAnOccupiedSessionDischargesTheWill pins the existing-action refusal: a
// parked action of an EARLIER connection can coexist with a bound connection (a
// drain-timeout repark, then a resume), and when that connection closes under
// uncertainty its own park is refused because the first action keeps its deadlines. The
// Will is then taken at the commit against this connection's own baseline and discharged;
// it used to be taken and dropped.
func TestMQTTParkOnAnOccupiedSessionDischargesTheWill(t *testing.T) {
	const topic, id = "wocc/t8", "wocc8"
	f := testNewOwedDelayedFixture(t, topic)
	rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
	defer closeWit()
	cw, sess, _ := f.connectOld(id, 1)
	gen, seq := testOwedDelayedBaseline(sess)
	earlier := &mqttDeferredDisconnect{expiryInterval: 30, disconnectedAt: time.Now().Unix()}
	sess.mu.Lock()
	sess.deferredDisconnect = earlier
	sess.seqUncertain = true
	sess.mu.Unlock()
	defer func() {
		sess.mu.Lock()
		sess.seqUncertain = false
		if sess.deferredDisconnect == earlier {
			sess.deferredDisconnect = nil
		}
		sess.mu.Unlock()
	}()
	cw.Close()
	if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
		t.Fatalf("expected exactly 1 publication of the closing connection's Will, got %d", n)
	}
	recs := f.enqueued(id)
	if len(recs) != 1 || recs[0].Gen != gen || recs[0].OriginSessionSeq != seq {
		t.Fatalf("expected one record on the closing connection's own baseline (gen %q seq %d), got %+v", gen, seq, recs)
	}
	sess.mu.Lock()
	still := sess.deferredDisconnect
	sess.mu.Unlock()
	if still != earlier || earlier.will != nil {
		t.Fatal("the earlier connection's parked action must stay parked and unchanged")
	}
}

// TestMQTTReDriveT1bSettlesAnOwedDelayedWill pins the re-drive's T1b: a superseded
// clean-start discard abandons the resident object and runs the takeover tail on the
// captured client, and its applyWill runs BEFORE that tail. A fix in the tail alone would
// be bypassed there.
func TestMQTTReDriveT1bSettlesAnOwedDelayedWill(t *testing.T) {
	const topic, id = "wt1b/t7", "wt1b7"
	f := testNewOwedDelayedFixture(t, topic)
	rwit, closeWit := testMQTTWillWitness(t, f.o, id+"-w", topic)
	defer closeWit()
	cw, _, oc := f.connectOld(id, 1)
	barrier := testMQTTNewTeardownBarrier(id)
	testMQTTSetTeardownSeam(f.asm, barrier.hook)
	defer testMQTTSetTeardownSeam(f.asm, nil)
	cw.Close()
	barrier.waitReached(t)
	_, disp, closeAt := testOwedDelayedWillState(oc)
	if disp != mqttWillOwedDelayed {
		barrier.unblock()
		t.Fatalf("precondition: owed-delayed before the gate, got %v", disp)
	}
	testOwedDelayedWaitPast(closeAt, 1)
	tr := testTraceReDrive(t, f.asm)
	f.awaitConn(f.replace(id, true))
	barrier.unblock()
	if tr.winner() == _EMPTY_ {
		t.Fatal("the rival record never landed, so T1 never ran")
	}
	if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
		t.Fatalf("expected exactly 1 publication, got %d", n)
	}
}
