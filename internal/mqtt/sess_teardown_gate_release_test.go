//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A clean close releases the client-ID gate once its retirement is durably owned, and the
// clean-start successor displaces the marked record instead of finishing the teardown inline.
// See destroyReleasing and sess_teardown_displace.go.

func testSetGateReleasedHook(t *testing.T, h func(sess *mqttSession) error) {
	t.Helper()
	prev := mqttTestTeardownGateReleasedHookV.Load()
	mqttTestTeardownGateReleasedHookV.Store(&h)
	t.Cleanup(func() { mqttTestTeardownGateReleasedHookV.Store(prev) })
}

func testSetMarkedRecordArmHook(t *testing.T, h func(clientID string, arm mqttMarkedRecordArm)) {
	t.Helper()
	prev := mqttTestMarkedRecordArmHookV.Load()
	mqttTestMarkedRecordArmHookV.Store(&h)
	t.Cleanup(func() { mqttTestMarkedRecordArmHookV.Store(prev) })
}

func testSetCleanCloseWillHook(t *testing.T, h func(sess *mqttSession)) {
	t.Helper()
	prev := mqttTestCleanCloseWillHookV.Load()
	mqttTestCleanCloseWillHookV.Store(&h)
	t.Cleanup(func() { mqttTestCleanCloseWillHookV.Store(prev) })
}

func testSetTeardownDeletesHook(t *testing.T, h func(tg *mqttTeardownTarget)) {
	t.Helper()
	prev := mqttTestTeardownDeletesHookV.Load()
	mqttTestTeardownDeletesHookV.Store(&h)
	t.Cleanup(func() { mqttTestTeardownDeletesHookV.Store(prev) })
}

// testGateReleasePark parks one client's clean-close continuation at the gate release.
type testGateReleasePark struct {
	parked     chan struct{}
	unpark     chan struct{}
	parkOnce   sync.Once
	unparkOnce sync.Once
	markerSeq  atomic.Uint64
}

func (p *testGateReleasePark) release() { p.unparkOnce.Do(func() { close(p.unpark) }) }

func (p *testGateReleasePark) waitParked(t *testing.T) {
	t.Helper()
	select {
	case <-p.parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the clean close never reached its gate release: the retirement was not durably owned")
	}
}

// testParkAtGateRelease installs the gate-release hook for clientID: it records the marked
// record's sequence, signals, and blocks until released.
func testParkAtGateRelease(t *testing.T, asm *mqttAccountSessionManager, clientID string) *testGateReleasePark {
	t.Helper()
	p := &testGateReleasePark{parked: make(chan struct{}), unpark: make(chan struct{})}
	t.Cleanup(p.release)
	testSetGateReleasedHook(t, func(sess *mqttSession) error {
		if sess.id != clientID {
			return nil
		}
		if _, seq := testCleanStartRecord(t, asm, sess.idHash); seq != 0 {
			p.markerSeq.Store(seq)
		}
		p.parkOnce.Do(func() { close(p.parked) })
		<-p.unpark
		return nil
	})
	return p
}

// testCleanStartRecord reads a client's session record, nil when the subject is empty.
func testCleanStartRecord(t *testing.T, asm *mqttAccountSessionManager, idHash string) (*mqttPersistedSession, uint64) {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("loading the session record: %v", err)
		}
		return nil, 0
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the session record: %v", err)
	}
	return ps, smsg.Sequence
}

// testCleanPredecessor is a connected v5 clean session (Session Expiry 0) holding inbound QoS 2
// exchanges at PUBREC.
type testCleanPredecessor struct {
	c     net.Conn
	r     *mqttReader
	sess  *mqttSession
	gen   string
	hash  string
	holds []string
}

func testConnectCleanPredecessor(t *testing.T, s *Server, o *Options, id string, holds int, will *mqttWill, props []byte) *testCleanPredecessor {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, will: will, props: props},
		o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("fixture: the predecessor CONNECT was answered %#x", rc)
	}
	p := &testCleanPredecessor{c: c, r: r, sess: testMQTTResidentSession(t, s, id)}
	p.sess.mu.Lock()
	p.gen, p.hash = p.sess.gen, p.sess.idHash
	p.sess.mu.Unlock()
	for i := 1; i <= holds; i++ {
		pi := uint16(i)
		testMQTTPublishV5(t, c, 2, pi, id+"/held", []byte(fmt.Sprintf("held-%d", i)))
		testMQTTReadPubRecV5(t, r, pi)
		p.holds = append(p.holds, mqttQoS2HoldSubject(p.hash, p.gen, pi))
	}
	return p
}

// testDropSessionUnretired removes a client's record and residency WITHOUT recording a
// retirement, unlike testOrphanDropSession: the closure is then the only fact that can license
// the sweep.
func testDropSessionUnretired(t *testing.T, asm *mqttAccountSessionManager, idHash string) {
	t.Helper()
	asm.mu.Lock()
	for id, ss := range asm.sessions {
		if ss.idHash == idHash {
			delete(asm.sessions, id)
		}
	}
	delete(asm.sessByHash, idHash)
	asm.mu.Unlock()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash)
	if err != nil {
		t.Fatalf("loading the session record: %v", err)
	}
	if err := asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("deleting the session record: %v", err)
	}
}

func testHoldsPresent(t *testing.T, asm *mqttAccountSessionManager, subjects []string) int {
	t.Helper()
	n := 0
	for _, subj := range subjects {
		if testOrphanHoldPresent(t, asm, subj) {
			n++
		}
	}
	return n
}

// The clean-start successor of a closing clean session is answered while the predecessor's
// reclamation is still parked: its CONNACK is a success, with Session Present false, a fresh
// generation, and the marked record displaced in place. The parked continuation then finishes
// without touching the successor.
//
// M1: the close never releases the gate early -> the successor's CONNACK misses the bound.
// M2: the marked-record arm always adopts inline -> the CONNECT blocks on the adoption's
// parked hold walk. M6: condemnRetiredSurvivor without its generation guard -> the
// continuation's superseded release marks the successor's record.
func TestMQTTCleanStartAnsweredDuringPredecessorTeardown(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "gate-release-successor"

	pred := testConnectCleanPredecessor(t, s, o, id, 3, nil, nil)
	park := testParkAtGateRelease(t, asm, id)
	// The adoption's hold walk is parked too, so a CONNECT that adopts inline cannot finish
	// before the continuation is released. The continuation reaches this hook only after it is
	// unparked, by which time the gate below is open.
	walkOpen := make(chan struct{})
	var walkOnce sync.Once
	t.Cleanup(func() { walkOnce.Do(func() { close(walkOpen) }) })
	testSetTeardownDeletesHook(t, func(tg *mqttTeardownTarget) {
		if tg.id == id && tg.gen == pred.gen {
			<-walkOpen
		}
	})
	var arms []mqttMarkedRecordArm
	var armsMu sync.Mutex
	testSetMarkedRecordArmHook(t, func(clientID string, arm mqttMarkedRecordArm) {
		if clientID == id {
			armsMu.Lock()
			arms = append(arms, arm)
			armsMu.Unlock()
		}
	})

	pred.c.Close()
	park.waitParked(t)
	markerSeq := park.markerSeq.Load()
	if marked, _ := testCleanStartRecord(t, asm, pred.hash); marked == nil || marked.Teardown == nil {
		t.Fatal("fixture: the parked teardown left no marked record for the successor to meet")
	}

	start := time.Now()
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	sessp, rc, _ := testMQTTReadConnAckV5(t, r2)
	took := time.Since(start)
	if rc != mqttConnAckRCConnectionAccepted || sessp {
		t.Fatalf("the successor was answered rc=%#x sessionPresent=%v, want a fresh success", rc, sessp)
	}
	if took > 3*time.Second {
		t.Fatalf("the successor's CONNACK took %v while its predecessor's reclamation was parked", took)
	}
	armsMu.Lock()
	gotArms := append([]mqttMarkedRecordArm(nil), arms...)
	armsMu.Unlock()
	if len(gotArms) != 1 || gotArms[0] != mqttMarkedRecordDisplaced {
		t.Fatalf("the marked record was handled %v, want displaced once", gotArms)
	}
	succ := testMQTTResidentSession(t, s, id)
	succ.mu.Lock()
	succGen := succ.gen
	succ.mu.Unlock()
	if succGen == pred.gen {
		t.Fatal("the successor resumed the predecessor's generation")
	}
	rec, seq := testCleanStartRecord(t, asm, pred.hash)
	if rec == nil || rec.Teardown != nil || rec.Gen != succGen || seq <= markerSeq {
		t.Fatalf("the successor's record is %+v at %d, want generation %q unmarked past the marker at %d",
			rec, seq, succGen, markerSeq)
	}
	if n := testHoldsPresent(t, asm, pred.holds); n != len(pred.holds) {
		t.Fatalf("fixture: %d of %d predecessor holds were reclaimed before the continuation ran", len(pred.holds)-n, len(pred.holds))
	}

	park.release()
	walkOnce.Do(func() { close(walkOpen) })
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if n := testHoldsPresent(t, asm, pred.holds); n != 0 {
			return fmt.Errorf("%d predecessor holds remain", n)
		}
		return nil
	})
	// The continuation's release meets the successor's record after the walk; it must leave
	// it exactly as it was.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec, _ := testCleanStartRecord(t, asm, pred.hash)
		if rec == nil || rec.Teardown != nil || rec.Gen != succGen {
			t.Fatalf("the predecessor's late cleanup changed the successor's record to %+v", rec)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The continuation releases the marked record before the successor's write lands: the
// successor's CAS at the marker's sequence is refused and the CONNECT re-drives into a
// record-absent create, still a fresh success.
func TestMQTTCleanStartRedrivesWhenPredecessorReleasesFirst(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "gate-release-redrive"

	pred := testConnectCleanPredecessor(t, s, o, id, 1, nil, nil)
	park := testParkAtGateRelease(t, asm, id)
	armReached, proceed := make(chan struct{}), make(chan struct{})
	var armOnce, proceedOnce sync.Once
	t.Cleanup(func() { proceedOnce.Do(func() { close(proceed) }) })
	testSetMarkedRecordArmHook(t, func(clientID string, arm mqttMarkedRecordArm) {
		if clientID != id || arm != mqttMarkedRecordDisplaced {
			return
		}
		first := false
		armOnce.Do(func() { first = true })
		if first {
			close(armReached)
			<-proceed
		}
	})

	pred.c.Close()
	park.waitParked(t)
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	select {
	case <-armReached:
	case <-time.After(10 * time.Second):
		t.Fatal("the successor never chose to displace the marked record")
	}
	park.release()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if rec, _ := testCleanStartRecord(t, asm, pred.hash); rec != nil {
			return fmt.Errorf("the predecessor's record still stands")
		}
		return nil
	})
	proceedOnce.Do(func() { close(proceed) })

	sessp, rc, _ := testMQTTReadConnAckV5(t, r2)
	if rc != mqttConnAckRCConnectionAccepted || sessp {
		t.Fatalf("the successor was answered rc=%#x sessionPresent=%v, want a fresh success", rc, sessp)
	}
	succ := testMQTTResidentSession(t, s, id)
	succ.mu.Lock()
	succGen := succ.gen
	succ.mu.Unlock()
	rec, _ := testCleanStartRecord(t, asm, pred.hash)
	if rec == nil || rec.Teardown != nil || rec.Gen != succGen || succGen == pred.gen {
		t.Fatalf("the successor's record is %+v, want its own fresh generation %q unmarked", rec, succGen)
	}
}

// testWillWitness subscribes to a Will topic and reports what arrives on it.
func testWillWitness(t *testing.T, o *Options, id, topic string) *mqttReader {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("fixture: the Will witness was answered %#x", rc)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 0}})
	return r
}

func testWillArrives(r *mqttReader, topic string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		got, _, _, _, ok := testMQTTTryReadPublishV5(r, left)
		if !ok {
			return false
		}
		if got == topic {
			return true
		}
	}
}

// A clean close acts on its Will while it still holds the client-ID gate and before its
// teardown, on every arm. The order is local: it is this broker's gate.
//
// M7: the Will back after clearInner -> the Will hook sees the gate already released, and the
// Will does not arrive while the continuation is parked.
func TestMQTTCleanCloseWillPrecedesGateRelease(t *testing.T) {
	type arm struct {
		name    string
		closeFn func(t *testing.T, p *testCleanPredecessor)
		props   []byte
		setup   func(t *testing.T, asm *mqttAccountSessionManager, p *testCleanPredecessor)
		willOut bool // the Will is published
		parks   bool // the teardown reaches its gate release
	}
	abrupt := func(t *testing.T, p *testCleanPredecessor) { p.c.Close() }
	arms := []arm{
		{name: "abrupt close", closeFn: abrupt, willOut: true, parks: true},
		{name: "normal DISCONNECT", closeFn: func(t *testing.T, p *testCleanPredecessor) {
			if _, err := testMQTTWrite(p.c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
				t.Fatalf("writing the DISCONNECT: %v", err)
			}
		}, willOut: false, parks: true},
		{name: "DISCONNECT with Will overriding the expiry to zero", props: mqttV5ConnPropsSessionExpiry(300),
			closeFn: func(t *testing.T, p *testCleanPredecessor) {
				if _, err := testMQTTWrite(p.c, mqttV5DisconnectSessionExpiry(mqttReasonDisconnectWithWill, 0)); err != nil {
					t.Fatalf("writing the DISCONNECT: %v", err)
				}
			}, willOut: true, parks: true},
		{name: "drain ceiling", closeFn: abrupt, willOut: true, parks: false,
			setup: func(t *testing.T, asm *mqttAccountSessionManager, p *testCleanPredecessor) {
				prev := mqttIncarnationDrainTimeout.Get()
				mqttIncarnationDrainTimeout.Set(250 * time.Millisecond)
				t.Cleanup(func() { mqttIncarnationDrainTimeout.Set(prev) })
				l, ok := p.sess.leases.acquire()
				if !ok {
					t.Fatal("fixture: the incarnation refused a lease")
				}
				t.Cleanup(l.release)
			}},
		{name: "superseded claim", closeFn: abrupt, willOut: true, parks: false,
			setup: func(t *testing.T, asm *mqttAccountSessionManager, p *testCleanPredecessor) {
				var once sync.Once
				h := func(sess *mqttSession, window string) {
					if sess != p.sess || window != mqttClearWindowPreEntry {
						return
					}
					once.Do(func() {
						rec, seq := testCleanStartRecord(t, asm, p.hash)
						if rec == nil {
							return
						}
						// A foreign write moves the record past the base the claim will name.
						rec.WID, rec.PWID = mqttNewRecordWID(), rec.WID
						if _, err := mqttStoreSessionRecordCAS(&asm.jsa, asm.domainTk, rec, p.hash, seq); err != nil {
							t.Errorf("fixture: moving the record: %v", err)
						}
					})
				}
				prev := mqttTestClearWindowHookV.Load()
				mqttTestClearWindowHookV.Store(&h)
				t.Cleanup(func() { mqttTestClearWindowHookV.Store(prev) })
			}},
		{name: "refused unresolved", closeFn: abrupt, willOut: true, parks: false,
			setup: func(t *testing.T, asm *mqttAccountSessionManager, p *testCleanPredecessor) {
				h := func(sess *mqttSession, window string) {
					if sess != p.sess || window != mqttClearWindowPreFence {
						return
					}
					sess.mu.Lock()
					sess.seqUncertain = true
					sess.mu.Unlock()
				}
				prev := mqttTestClearWindowHookV.Load()
				mqttTestClearWindowHookV.Store(&h)
				t.Cleanup(func() { mqttTestClearWindowHookV.Store(prev) })
			}},
	}
	for i, a := range arms {
		t.Run(a.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			t.Cleanup(func() { testMQTTShutdownServer(s) })
			asm := testMQTTGetGlobalASM(t, s)
			id := fmt.Sprintf("gate-release-will-%d", i)
			topic := id + "/will"
			witness := testWillWitness(t, o, id+"-witness", topic)

			p := testConnectCleanPredecessor(t, s, o, id, 0,
				&mqttWill{topic: []byte(topic), message: []byte("gone"), qos: 0}, a.props)
			if a.setup != nil {
				a.setup(t, asm, p)
			}
			park := testParkAtGateRelease(t, asm, id)
			var willCalls atomic.Int32
			var gateHeldAtWill atomic.Bool
			testSetCleanCloseWillHook(t, func(sess *mqttSession) {
				if sess.id != id {
					return
				}
				willCalls.Add(1)
				asm.mu.RLock()
				_, held := asm.sessLocked[id]
				asm.mu.RUnlock()
				gateHeldAtWill.Store(held)
			})

			a.closeFn(t, p)
			if a.parks {
				park.waitParked(t)
			}
			// While the continuation is parked (or the arm has ended the teardown), the Will
			// action has already happened.
			if got := testWillArrives(witness, topic, 3*time.Second); got != a.willOut {
				t.Fatalf("the Will arrived=%v, want %v", got, a.willOut)
			}
			if a.willOut && testWillArrives(witness, topic, 500*time.Millisecond) {
				t.Fatal("the Will was published twice")
			}
			checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
				if willCalls.Load() == 0 {
					return fmt.Errorf("the clean close never reached its Will action")
				}
				return nil
			})
			if n := willCalls.Load(); n != 1 {
				t.Fatalf("the clean close reached its Will action %d times", n)
			}
			if !gateHeldAtWill.Load() {
				t.Fatal("the Will action ran after the client-ID gate was released")
			}
		})
	}
}

// The Will action takes no record fence, so an off-readLoop writer holding the session's
// record transaction across it does not delay it.
func TestMQTTCleanCloseWillDoesNotWaitOnTheRecordFence(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	const id = "gate-release-will-fence"
	topic := id + "/will"
	witness := testWillWitness(t, o, id+"-witness", topic)
	p := testConnectCleanPredecessor(t, s, o, id, 0, &mqttWill{topic: []byte(topic), message: []byte("gone"), qos: 0}, nil)

	const fenceHeld = 1500 * time.Millisecond
	var held atomic.Bool
	testSetCleanCloseWillHook(t, func(sess *mqttSession) {
		if sess.id != id {
			return
		}
		tok, ok := sess.tryBeginRecordTx()
		if !ok {
			t.Error("fixture: the record fence was not free to take")
			return
		}
		held.Store(true)
		go func() {
			time.Sleep(fenceHeld)
			held.Store(false)
			if err := sess.endRecordTx(tok); err != nil {
				t.Errorf("fixture: releasing the record fence: %v", err)
			}
		}()
	})
	p.c.Close()
	if !testWillArrives(witness, topic, fenceHeld-300*time.Millisecond) {
		t.Fatal("the Will did not arrive while another writer held the record fence")
	}
	if !held.Load() {
		t.Fatal("fixture: the record fence was not held when the Will arrived")
	}
}

// Once the gate is released, the close never restores the retired object's residency, even
// if its teardown reports one of the two sentinels that do so before the claim.
//
// M9: drop the released-flag guard on those arms -> the object is resident again.
func TestMQTTCleanCloseKeepsResidencyAfterGateRelease(t *testing.T) {
	for _, sentinel := range []error{errMQTTClearDrainTimeout, errMQTTClearRefusedUnresolved} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			t.Cleanup(func() { testMQTTShutdownServer(s) })
			asm := testMQTTGetGlobalASM(t, s)
			const id = "gate-release-residency"
			p := testConnectCleanPredecessor(t, s, o, id, 0, nil, nil)
			returned := make(chan struct{})
			var once sync.Once
			testSetGateReleasedHook(t, func(sess *mqttSession) error {
				if sess.id != id {
					return nil
				}
				if errors.Is(sentinel, errMQTTClearRefusedUnresolved) {
					// The state the unresolved re-park needs, so that a missing guard would
					// actually re-park and restore residency rather than decline.
					sess.mu.Lock()
					sess.seqUncertain = true
					sess.mu.Unlock()
				}
				once.Do(func() { close(returned) })
				return fmt.Errorf("injected after the gate release: %w", sentinel)
			})
			p.c.Close()
			select {
			case <-returned:
			case <-time.After(10 * time.Second):
				t.Fatal("the clean close never reached its gate release")
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				asm.mu.RLock()
				resident := asm.sessions[id]
				asm.mu.RUnlock()
				if resident == p.sess {
					t.Fatal("the retired object was made resident again after its gate was released")
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

// errTestContinuationAbandoned ends a clean close's teardown at its gate release, leaving the
// durable state a crash there leaves.
var errTestContinuationAbandoned = errors.New("test: continuation abandoned at the gate release")

// A crash after the gate release leaves the predecessor's inbound QoS 2 holds with no marker
// once its successor displaced the record, and with no watermark once that successor's own
// retirement replaced the single slot. The predecessor's permanent discarding closure still
// licenses the orphan sweep; a live session's holds are untouched.
//
// M10: drop the closure arm of retirementProvenFor -> the holds are retained.
func TestMQTTOrphanSweepReapsDiscardedGenerationAfterWatermarkReplaced(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	testOrphanSetGraces(t, asm, 0, 0)
	const id = "gate-release-sweep"

	g1 := testConnectCleanPredecessor(t, s, o, id, 2, nil, nil)
	var abandoned atomic.Bool
	testSetGateReleasedHook(t, func(sess *mqttSession) error {
		if sess.id == id && abandoned.CompareAndSwap(false, true) {
			return errTestContinuationAbandoned
		}
		return nil
	})
	g1.c.Close()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if !abandoned.Load() {
			return fmt.Errorf("the first generation's close has not reached its gate release")
		}
		return nil
	})

	// The successor displaces the marker, then ends and retires its own generation.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	if sessp, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted || sessp {
		t.Fatalf("the successor was answered rc=%#x sessionPresent=%v", rc, sessp)
	}
	g2 := testMQTTResidentSession(t, s, id)
	g2.mu.Lock()
	g2Gen := g2.gen
	g2.mu.Unlock()
	c2.Close()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if rec, _ := testCleanStartRecord(t, asm, g1.hash); rec != nil {
			return fmt.Errorf("the successor's record still stands")
		}
		if r, _, st := asm.jsa.loadGenRetirement(asm.domainTk, g1.hash); st != mqttRetirementValid || r.Gen != g2Gen {
			return fmt.Errorf("the watermark does not name the successor yet")
		}
		return nil
	})

	// A live session's holds, which the sweep must not reach.
	const liveID = "gate-release-sweep-live"
	live := testConnectCleanPredecessor(t, s, o, liveID, 1, nil, mqttV5ConnPropsSessionExpiry(300))

	if n := testHoldsPresent(t, asm, g1.holds); n != len(g1.holds) {
		t.Fatalf("fixture: %d of the first generation's holds were reclaimed before the sweep", len(g1.holds)-n)
	}
	if asm.retirementMemoHas(g1.hash, g1.gen) {
		t.Fatal("fixture: the first generation's retirement was already remembered")
	}
	for i := 0; i < 6 && testHoldsPresent(t, asm, g1.holds) > 0; i++ {
		asm.sweepOrphanedQoS2State(s)
	}
	if n := testHoldsPresent(t, asm, g1.holds); n != 0 {
		t.Fatalf("%d of the discarded generation's holds were retained", n)
	}
	if n := testHoldsPresent(t, asm, live.holds); n != len(live.holds) {
		t.Fatal("the sweep reached a live session's inbound QoS 2 state")
	}
}

// Without a matching watermark, only a VALID closure of exactly this generation that carried an
// authorized discard licenses the sweep. Every other closure shape leaves the holds exactly
// where they are, and creates no discard claim in front of them.
//
// M12a: ignore Discard -> the preserving case reaps. M12b: skip the decoder's validation -> the
// wrong-epoch and wrong-identity cases reap. M12c: accept a non-valid load as proof -> the
// corrupt and unreadable cases reap.
func TestMQTTOrphanSweepRetainsWithoutDiscardingClosure(t *testing.T) {
	type shape struct {
		name   string
		plant  func(t *testing.T, asm *mqttAccountSessionManager, id, hash, gen string)
		reaped bool
	}
	plantBody := func(t *testing.T, asm *mqttAccountSessionManager, hash, gen string, body []byte) {
		t.Helper()
		msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
		if _, err := asm.jsa.storeMsg(mqttGenClosureSubject(hash, gen), hdr, msg); err != nil {
			t.Fatalf("planting the closure: %v", err)
		}
	}
	plantClosure := func(id, epoch, gen string, discard bool) func(*testing.T, *mqttAccountSessionManager, string, string, string) {
		return func(t *testing.T, asm *mqttAccountSessionManager, _, hash, holdGen string) {
			t.Helper()
			if epoch == _EMPTY_ {
				epoch = asm.sessStreamEpoch
			}
			if gen == _EMPTY_ {
				gen = holdGen
			}
			body, err := json.Marshal(&mqttGenClosure{
				ID: id, Epoch: epoch, Gen: gen, Txn: "txn-planted-closure", Discard: discard,
				// The shard is the BODY's client's, so a body naming another client is refused by
				// the hash binding alone, not by its shard.
				RootShard: mqttDirShardFor(getHash(id)), RootOrd: 1,
			})
			if err != nil {
				t.Fatalf("marshalling the closure: %v", err)
			}
			plantBody(t, asm, hash, holdGen, body)
		}
	}
	shapes := []shape{
		{"preserving", nil, false},
		{"absent", func(*testing.T, *mqttAccountSessionManager, string, string, string) {}, false},
		{"corrupt", func(t *testing.T, asm *mqttAccountSessionManager, _, hash, gen string) {
			plantBody(t, asm, hash, gen, []byte(`{"id":`))
		}, false},
		{"unreadable", nil, false},
		{"another generation", nil, false},
		{"another epoch", nil, false},
		{"another client", nil, false},
		{"discarding", nil, true},
	}
	for i, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			t.Cleanup(func() { testMQTTShutdownServer(s) })
			asm := testMQTTGetGlobalASM(t, s)
			testOrphanSetGraces(t, asm, 0, 0)
			id := fmt.Sprintf("closure-licence-%d", i)

			// Holds of a generation nothing references any more, with no watermark: the state a
			// broker that died before its teardown leaves.
			p := testConnectCleanPredecessor(t, s, o, id, 1, nil, mqttV5ConnPropsSessionExpiry(300))
			testDropSessionUnretired(t, asm, p.hash)
			if _, _, st := asm.jsa.loadGenRetirement(asm.domainTk, p.hash); st == mqttRetirementValid {
				t.Fatal("fixture: a watermark stands, so the closure is not what is under test")
			}

			plant := sh.plant
			switch sh.name {
			case "preserving":
				plant = plantClosure(id, _EMPTY_, _EMPTY_, false)
			case "discarding", "unreadable":
				plant = plantClosure(id, _EMPTY_, _EMPTY_, true)
			case "another generation":
				plant = func(t *testing.T, asm *mqttAccountSessionManager, _, hash, gen string) {
					// A discarding closure of ANOTHER generation of the same client.
					plantClosure(id, _EMPTY_, _EMPTY_, true)(t, asm, id, hash, mqttNewSessGen())
				}
			case "another epoch":
				plant = plantClosure(id, "another-epoch", _EMPTY_, true)
			case "another client":
				plant = plantClosure(id+"-other", _EMPTY_, _EMPTY_, true)
			}
			plant(t, asm, id, p.hash, p.gen)
			if sh.name == "unreadable" {
				closureSubj := mqttGenClosureSubject(p.hash, p.gen)
				restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
					if subject == closureSubj {
						return errors.New("injected: the closure read is indeterminate")
					}
					return nil
				})
				t.Cleanup(restore)
			}

			testOrphanSettle(asm, s)
			if got := testHoldsPresent(t, asm, p.holds) == 0; got != sh.reaped {
				t.Fatalf("the hold was reaped=%v, want %v", got, sh.reaped)
			}
			if !sh.reaped {
				claim, _, err := asm.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(p.holds[0]))
				if err != nil {
					t.Fatalf("loading the release claim: %v", err)
				}
				if claim != nil && claim.State == mqttQoS2RelStateDiscarded {
					t.Fatal("an unlicensed sweep fenced the hold with a discard claim")
				}
			}
		})
	}
}

// A CONNECT woken by a release just before its deadline, and delayed past the deadline before
// it can re-take as.mu, refuses rather than taking the now-free gate.
//
// M4b: drop the under-mutex deadline check -> the CONNECT takes the gate and is accepted.
func TestMQTTGateWaitRefusesAfterDeadlineAtWakeup(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "gate-wait-deadline"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("fixture: CONNECT answered %#x", rc)
	}
	cli := testMQTTGetClient(t, s, id)
	checkFor(t, time.Second, 10*time.Millisecond, func() error { return asm.lockSession(cli.mqtt.sess, cli) })

	const budget = time.Second
	asm.mu.Lock()
	asm.testOwnershipBudgetTotal = budget
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testOwnershipBudgetTotal = 0
		asm.mu.Unlock()
	})
	registered := make(chan struct{})
	var regOnce sync.Once
	reg := func() { regOnce.Do(func() { close(registered) }) }
	prevReg := mqttTestGateWaitRegisteredHookV.Load()
	mqttTestGateWaitRegisteredHookV.Store(&reg)
	t.Cleanup(func() { mqttTestGateWaitRegisteredHookV.Store(prevReg) })
	// Woken by the release, the CONNECT is held past its deadline before it re-takes as.mu:
	// the same schedule as a waiter blocked on the mutex.
	woken := func() { time.Sleep(budget + 250*time.Millisecond) }
	prevWoken := mqttTestGateWaitWokenHookV.Load()
	mqttTestGateWaitWokenHookV.Store(&woken)
	t.Cleanup(func() { mqttTestGateWaitWokenHookV.Store(prevWoken) })

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	select {
	case <-registered:
	case <-time.After(10 * time.Second):
		t.Fatal("the CONNECT never started waiting on the held gate")
	}
	asm.unlockSession(cli.mqtt.sess)
	// The gate refusal itself, named by its Reason String: a CONNECT that took the gate after
	// its deadline would be refused 0x89 too, later, by its exhausted ownership protocol.
	_, rc, props := testMQTTReadConnAckV5(t, r2)
	if rc != mqttReasonServerBusy || props == nil || props.reasonString != mqttReasonStrGateHeld {
		t.Fatalf("a CONNECT past its deadline at wakeup was answered %#x (%v), want the gate refusal", rc, props)
	}
}

// A CONNECT woken past its deadline into a freshly installed flapper entry is refused at its
// deadline rather than sitting out the flapper jail.
//
// M4c: check the deadline only after the flapper branch -> the CONNECT is jailed and closed
// without a CONNACK.
func TestMQTTGateWaitDeadlinePrecedesFlapperJail(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "gate-wait-flapper"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("fixture: CONNECT answered %#x", rc)
	}
	cli := testMQTTGetClient(t, s, id)
	checkFor(t, time.Second, 10*time.Millisecond, func() error { return asm.lockSession(cli.mqtt.sess, cli) })

	const budget = time.Second
	asm.mu.Lock()
	asm.testOwnershipBudgetTotal = budget
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testOwnershipBudgetTotal = 0
		asm.mu.Unlock()
	})
	registered := make(chan struct{})
	var regOnce sync.Once
	reg := func() { regOnce.Do(func() { close(registered) }) }
	prevReg := mqttTestGateWaitRegisteredHookV.Load()
	mqttTestGateWaitRegisteredHookV.Store(&reg)
	t.Cleanup(func() { mqttTestGateWaitRegisteredHookV.Store(prevReg) })
	// Woken by the release, the CONNECT is held past its deadline while a competing takeover
	// installs a flapper entry for the client ID.
	woken := func() {
		time.Sleep(budget + 250*time.Millisecond)
		// Installed AFTER the delay, so the entry is still inside its jail when the CONNECT
		// re-takes as.mu.
		asm.mu.Lock()
		asm.flappers[id] = time.Now()
		asm.mu.Unlock()
	}
	prevWoken := mqttTestGateWaitWokenHookV.Load()
	mqttTestGateWaitWokenHookV.Store(&woken)
	t.Cleanup(func() { mqttTestGateWaitWokenHookV.Store(prevWoken) })

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	select {
	case <-registered:
	case <-time.After(10 * time.Second):
		t.Fatal("the CONNECT never started waiting on the held gate")
	}
	asm.unlockSession(cli.mqtt.sess)
	_, rc, props := testMQTTReadConnAckV5(t, r2)
	if rc != mqttReasonServerBusy || props == nil || props.reasonString != mqttReasonStrGateHeld {
		t.Fatalf("a CONNECT past its deadline at wakeup, behind a flapper entry, was answered %#x (%v), want the gate refusal", rc, props)
	}
}

// A CONNECT's own release of the gate — the deferred release at the end of every CONNECT —
// wakes another CONNECT waiting on it.
//
// M3b: the CONNECT's release without its wakeup -> the waiter stays parked until its deadline.
func TestMQTTGateWaitWakesOnConnectRelease(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "gate-wait-connect-release"

	// The first successor holds the gate inside its CONNECT, parked at the marked-record arm.
	pred := testConnectCleanPredecessor(t, s, o, id, 0, nil, nil)
	park := testParkAtGateRelease(t, asm, id)
	armReached, proceed := make(chan struct{}), make(chan struct{})
	var armOnce, proceedOnce sync.Once
	t.Cleanup(func() { proceedOnce.Do(func() { close(proceed) }) })
	testSetMarkedRecordArmHook(t, func(clientID string, arm mqttMarkedRecordArm) {
		if clientID != id {
			return
		}
		first := false
		armOnce.Do(func() { first = true })
		if first {
			close(armReached)
			<-proceed
		}
	})
	pred.c.Close()
	park.waitParked(t)
	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c1.Close() })
	select {
	case <-armReached:
	case <-time.After(10 * time.Second):
		t.Fatal("the first successor never reached the marked record")
	}

	registered := make(chan struct{})
	var regOnce sync.Once
	reg := func() { regOnce.Do(func() { close(registered) }) }
	prevReg := mqttTestGateWaitRegisteredHookV.Load()
	mqttTestGateWaitRegisteredHookV.Store(&reg)
	t.Cleanup(func() { mqttTestGateWaitRegisteredHookV.Store(prevReg) })
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	select {
	case <-registered:
	case <-time.After(10 * time.Second):
		t.Fatal("the second CONNECT never waited on the first one's gate")
	}

	proceedOnce.Do(func() { close(proceed) })
	if _, rc, _ := testMQTTReadConnAckV5(t, r1); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the first successor was answered %#x", rc)
	}
	start := time.Now()
	if _, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the second CONNECT was answered %#x", rc)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the second CONNECT waited %v after the first one released the gate", took)
	}
}
