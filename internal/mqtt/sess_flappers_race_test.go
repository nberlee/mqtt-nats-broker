package mqtt

import (
	"net"
	"testing"
	"time"
)

// The flappers jail is asm.mu state: CONNECT reads it under that lock
// (mqttProcessConnect's CHECK label), the account manager's own cleanup timer
// ranges over it under that lock, and every displacement writes it under that
// lock. A close whose session-record write was unresolved parks its teardown and
// then clears this client ID from the jail — and that removal was the one runtime
// access in the tree taken with no lock at all (F-043).
//
// These tests are deliberately UNTAGGED. The defect is a data race, so the
// instrument that discriminates it is the race detector, and `make test` is the
// target that runs `-race`; `make full-suite` does not. Same reasoning as
// connect_deadline_test.go.
//
// One test per OWNERSHIP state at the park, not per source line. Three call sites
// reach mqttDeferDisconnectEx. Two go through mqttDeferDisconnectOwned with the
// session gate held — the never-expiring one parks after ending its own record
// transaction, which makes it identical here — and remove the entry under asm.mu.
// The third goes through mqttDeferDisconnectUngated, exists precisely because the
// gate could not be taken, and must leave the jail alone.

// flappersRaceFixture is one broker, one connected client, and the session object
// behind it, with the client ID already in the jail and the session's record write
// armed as unresolved — the two preconditions a parked close needs.
type flappersRaceFixture struct {
	s    *Server
	asm  *mqttAccountSessionManager
	sess *mqttSession
	conn net.Conn
	cid  string
	// reached is signalled by the jail seam once the park has decided about the
	// jail; release is what lets the closing goroutine leave the seam.
	reached chan string
	release chan struct{}
}

func newFlappersRaceFixture(t *testing.T, cid string) *flappersRaceFixture {
	t.Helper()
	_, s := startBroker(t)

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := conn.Write(testConnectPacket(cid, 0)); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	expectConnAckAccepted(t, conn)

	// The broker serves one account here, so the manager is taken from the map
	// rather than by account name — the test has no stake in which account it is.
	var asm *mqttAccountSessionManager
	s.mqtt.sessmgr.mu.RLock()
	for _, m := range s.mqtt.sessmgr.sessions {
		asm = m
		break
	}
	s.mqtt.sessmgr.mu.RUnlock()
	if asm == nil {
		t.Fatal("no account session manager after an accepted CONNECT")
	}
	asm.mu.RLock()
	sess := asm.sessions[cid]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for client ID %q after an accepted CONNECT", cid)
	}

	f := &flappersRaceFixture{
		s: s, asm: asm, sess: sess, conn: conn, cid: cid,
		reached: make(chan string, 4),
		release: make(chan struct{}),
	}

	// The seam. It touches no *testing.T, so it cannot panic the binary if it fires
	// after this test returns, and both channels stay usable: reached is buffered and
	// release is closed rather than sent to, so a late close parks nowhere.
	hook := func(clientID string) {
		if clientID != f.cid {
			return
		}
		select {
		case f.reached <- clientID:
		default:
		}
		<-f.release
	}
	asm.testAfterParkedJailDecision.Store(&hook)
	t.Cleanup(func() {
		asm.testAfterParkedJailDecision.Store(nil)
		f.releaseSeam()
	})

	// Seed the jail by ASSIGNING the entry, never through addSessToFlappersLocked:
	// that helper starts the cleanup timer, and a sweep that removes the entry on its
	// own would make every assertion below pass for a reason that has nothing to do
	// with the code under test. With no timer armed, only the production removal can
	// clear this.
	asm.mu.Lock()
	asm.flappers[cid] = time.Now()
	asm.mu.Unlock()
	f.requireJailed(t, "the fixture's own seed")

	// Arm the unresolved write the park is conditional on. The periodic pass that
	// would resolve it runs on the expiry sweep's cadence (15 minutes by default) and
	// its startup pass is long finished, so nothing clears this underneath the test.
	sess.mu.Lock()
	seq := sess.seq
	sess.mu.Unlock()
	testMarkUncertain(sess, seq, testRecordBody(t, sess))

	return f
}

// releaseSeam frees a closing goroutine parked in the seam, idempotently.
func (f *flappersRaceFixture) releaseSeam() {
	select {
	case <-f.release:
	default:
		close(f.release)
	}
}

// awaitJailDecision blocks until the park has decided about the jail and is held in
// the seam. It is the completion point both tests need: sess.deferredDisconnect
// becomes non-nil several frames EARLIER, so a fixture keyed on the park alone can
// inspect the jail before the code under test has run and pass against an
// implementation that removes the entry a moment later.
func (f *flappersRaceFixture) awaitJailDecision(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case <-f.reached:
	case <-time.After(within):
		t.Fatal("the close never reached the parked-jail decision: the fixture did not reach mqttDeferDisconnectEx")
	}
}

// requireJailed asserts the client ID is in the jail right now. It is called once on
// the seed, which is what stops "the jail is empty afterwards" from being a vacuous
// assertion: without it, a fixture that never seeded anything satisfies every
// absence check immediately.
func (f *flappersRaceFixture) requireJailed(t *testing.T, what string) {
	t.Helper()
	f.asm.mu.RLock()
	_, jailed := f.asm.flappers[f.cid]
	f.asm.mu.RUnlock()
	if !jailed {
		t.Fatalf("%s: client ID %q is not in the flappers jail, so nothing below tests a removal", what, f.cid)
	}
}

// startJailReader spins on the jail read CONNECT makes at mqttProcessConnect's
// CHECK label — the same map under the same lock — and returns a stop func.
//
// It runs on its own goroutine and shares nothing with the test goroutine, which is
// the whole point. The racing read has to be unordered with the removal, and every
// synchronisation with the closing goroutine destroys that: a read before the close
// acquires asm.mu in lockSession is ordered ahead of everything the close does
// afterwards, and a read after the deferred unlockSession releases asm.mu is ordered
// behind the removal. The seam is what makes a read land between them — it holds the
// closing goroutine still after the removal and before unlockSession, while this
// reader, which touches neither the seam's channels nor sess.mu, keeps reading.
//
// The first cut of this test tried to widen the window by having the TEST goroutine
// hold sess.mu and read the jail itself. It reproduced nothing — releasing sess.mu
// is a happens-before edge from the test to the close, and it ordered those reads
// ahead of the removal — and it could deadlock, because lockSession takes sess.mu
// while holding asm.mu. The mutation run and a review round caught the two halves.
func (f *flappersRaceFixture) startJailReader() (stop func()) {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
			}
			f.asm.mu.RLock()
			_ = f.asm.flappers[f.cid]
			f.asm.mu.RUnlock()
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

// TestDeferredDisconnectFlappersRemovalIsLocked covers the OWNED routes, and is the
// only race test: after the fix these are the only routes that write the map.
//
// The reproduction is staged, not spun at. The reader is already running when the
// close reaches the seam, and the seam holds the close there — past the removal,
// short of the deferred unlockSession that would order anything behind it. Every
// read the reader takes while the close is parked in the seam is therefore unordered
// with the removal, so an unlocked removal is a race the detector must report.
func TestDeferredDisconnectFlappersRemovalIsLocked(t *testing.T) {
	t.Parallel()
	f := newFlappersRaceFixture(t, "flap-owned")

	stop := f.startJailReader()

	// Abrupt, with no DISCONNECT packet: a graceful close would detour through a save
	// that an unresolved session refuses anyway, and the teardown is the path under
	// test either way.
	f.conn.Close()
	f.awaitJailDecision(t, 15*time.Second)

	// The close is held in the seam here, so these reads are the ones that matter.
	time.Sleep(20 * time.Millisecond)

	// The removal itself, asserted at the decision point rather than after a guess at
	// how long it takes: an owned close clears the jail exactly as the ordinary
	// teardown does. A "fix" that dropped the call instead of locking it fails here.
	f.asm.mu.RLock()
	_, jailed := f.asm.flappers[f.cid]
	f.asm.mu.RUnlock()

	stop()
	f.releaseSeam()

	if jailed {
		t.Fatalf("client ID %q is still in the flappers jail after an OWNED close decided", f.cid)
	}
}

// TestDeferredDisconnectUngatedParkKeepsTheJail covers the route that parks with NO
// gate: lockSessionForTeardown could not take it, so this close cannot prove the jail
// entry is stale rather than one a displacement installed while it was losing the
// gate, and must leave it alone.
//
// The test holds the gate itself, which is what forces that route. The assertion is
// taken at the seam — after the decision — rather than after a fixed wait, so an
// implementation that removes the entry cannot hide behind a descheduled goroutine.
func TestDeferredDisconnectUngatedParkKeepsTheJail(t *testing.T) {
	t.Parallel()
	f := newFlappersRaceFixture(t, "flap-ungated")

	f.sess.mu.Lock()
	owner := f.sess.c
	f.sess.mu.Unlock()
	if owner == nil {
		t.Fatal("the session is not bound to a client, so the gate cannot be taken as its owner")
	}
	// Retried: processConnect releases the gate in a deferred function that runs after
	// the CONNACK is on the wire, so the fixture can arrive while the accepting
	// CONNECT still holds it.
	var gateErr error
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if gateErr = f.asm.lockSession(f.sess, owner); gateErr == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if gateErr != nil {
		t.Fatalf("take the ownership gate: %v", gateErr)
	}
	gateHeld := true
	defer func() {
		if gateHeld {
			f.asm.unlockSession(f.sess)
		}
	}()

	// Holding the gate for the whole close is what forces the ungated route, and
	// keeping it held through the assertion is what keeps the route forced: the gate
	// is the reason lockSessionForTeardown fails all ten attempts.
	f.conn.Close()
	f.awaitJailDecision(t, 15*time.Second)

	f.requireJailed(t, "after an UNGATED close decided")

	f.releaseSeam()
	f.asm.unlockSession(f.sess)
	gateHeld = false
}
