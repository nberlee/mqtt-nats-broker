//go:build mqtt_fulltests

// The re-drive transitions that need a REAL CONNECT. mqttProcessConnect can only be
// driven end to end through the fulltests harness (mqtt_test.go), which is this build tag,
// so these live here and run under `make full-suite`. The budget and record-level halves
// are in the default build; see sess_takeover_arbitration_test.go.

package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"

	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// F-029 (Slice B): the re-drive's BINDING transitions, which need a live CONNECT.
// The record-level half is in sess_takeover_arbitration_test.go.

// testReDriveTrace records what a CONNECT's record work actually did: the base of every
// save attempt, in order, and how many discards ran. Both are what distinguish a
// re-drive that restored authoritatively from one that carried on at a stale base.
type testReDriveTrace struct {
	mu sync.Mutex
	// everyDiscard makes the rival land on EVERY discard rather than only the first, so
	// a CONNECT can be driven to exhaust its cycle budget on T1 rather than converging.
	everyDiscard bool
	bases        []uint64
	discard      int
	staged       bool
	winnerWID    string
	winnerPWID   string
	winnerSeq    uint64
}

func (tr *testReDriveTrace) winnerSequence() uint64 {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.winnerSeq
}

func (tr *testReDriveTrace) saveBases() []uint64 {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]uint64(nil), tr.bases...)
}

func (tr *testReDriveTrace) discards() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.discard
}

func (tr *testReDriveTrace) winner() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.winnerWID
}

// testTraceReDrive records every save's CAS base and every discard, and lands a RIVAL
// record in the window between the FIRST discard's entry check and the fence it takes —
// so the teardown claim that follows is genuinely CASed against a sequence that is no
// longer current.
//
// Both live on one hook because clear's window hook is one slot; splitting them would
// silently disarm whichever was installed first. The rival lands ONCE: the re-drive's
// second discard must find the subject stable, or the test would be measuring an
// unbounded loop rather than the transition.
func testTraceReDrive(t *testing.T, asm *mqttAccountSessionManager, shape ...func(*mqttPersistedSession)) *testReDriveTrace {
	t.Helper()
	var tr *testReDriveTrace
	tr = testTraceReDriveStaged(t, func(sess *mqttSession) {
		if asm == nil {
			return
		}
		sess.mu.Lock()
		ps := sess.buildPersistedSessionLocked()
		seq, idHash := sess.seq, sess.idHash
		sess.mu.Unlock()
		// A rival broker's write off the SAME base: a fresh identity, parented to the
		// record this session holds. That is what a takeover from another frontend looks
		// like on the subject.
		ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
		ps.Origin = "rival-frontend"
		for _, sh := range shape {
			sh(&ps)
		}
		wseq, err := mqttStoreSessionRecordCAS(&asm.jsa, asm.domainTk, &ps, idHash, seq)
		if err != nil {
			t.Errorf("landing the rival record: %v", err)
			return
		}
		tr.mu.Lock()
		tr.winnerWID, tr.winnerPWID, tr.winnerSeq = ps.WID, ps.PWID, wseq
		tr.mu.Unlock()
	})
	return tr
}

// testTraceReDriveStaged is testTraceReDrive with the staging left to the caller: what a
// superseded discard MEETS is the variable the branch table turns on — a moved record, a
// teardown marker, or an absent subject — and each needs the same trace around it.
//
// stage runs in the pre-fence window of the FIRST discard only (unless everyDiscard is
// set), because a re-drive's second discard must find the subject stable or the test
// would be measuring an unbounded loop rather than the transition.
func testTraceReDriveStaged(t *testing.T, stage func(*mqttSession)) *testReDriveTrace {
	t.Helper()
	tr := &testReDriveTrace{}
	// Registered before the restore below, so the cleanups unwind in the order the
	// window needs: the restore stops new calls, then the gate waits out the one
	// that may already be inside stage. See testHookGate.
	gate := newTestHookGate(t)
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		sess.mu.Lock()
		base := sess.seq
		sess.mu.Unlock()
		tr.mu.Lock()
		tr.bases = append(tr.bases, base)
		tr.mu.Unlock()
		return nil
	})
	restoreClear := setMQTTTestClearWindowHook(func(sess *mqttSession, window string) {
		switch window {
		case mqttClearWindowPreEntry:
			tr.mu.Lock()
			tr.discard++
			tr.mu.Unlock()
			return
		case mqttClearWindowPreFence:
		default:
			return
		}
		tr.mu.Lock()
		done := tr.staged && !tr.everyDiscard
		tr.mu.Unlock()
		if done || stage == nil {
			return
		}
		// stage reports its failures through t, and a clear window can open on a
		// server goroutine after this test has returned — an expiry reconcile is
		// enough. Without the gate that report panics the whole binary and aborts
		// every test still to run.
		if !gate.enter() {
			return
		}
		defer gate.leave()
		stage(sess)
		tr.mu.Lock()
		tr.staged = true
		tr.mu.Unlock()
	})
	t.Cleanup(func() { restoreSave(); restoreClear() })
	return tr
}

// ---------------------------------------------------------------------------
// Test 3(a-c) / M2, M7: a superseded claim re-drives through the clean-start decision
// ---------------------------------------------------------------------------

// TestSessSupersededDiscardReDrivesTheCleanStart is the CONNECT-level half of B2.
//
// A clean-start CONNECT discards the session it found; its teardown claim is superseded
// because the record moved. The old code read that as a completed discard and went
// straight to its accept save — at base 0, because clear() had reset the sequence —
// which erased the winner. The re-drive instead restores the record that actually won
// and runs the whole discard cycle against IT.
//
// The three assertions are ordered so they distinguish the STALE base-0 create this
// item forbids from the LEGITIMATE one it ends with, which is why "no base-0 store at
// all" would be the wrong assertion: the winner's sequence is consumed by the second
// teardown claim, so the accept save that follows it is a create by construction.
func TestSessSupersededDiscardReDrivesTheCleanStart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "redrive-clean"

	// A persistent session first, so the reconnect below has a record to discard.
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	testMQTTDisconnectEx(t, c, nil, true)
	c.Close()

	tr := testTraceReDrive(t, asm)

	// The clean-start reconnect. It must be ACCEPTED: a re-drive is not a failure.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	winner := tr.winner()
	if winner == _EMPTY_ {
		t.Fatal("the rival record never landed: the claim was not superseded and nothing was under test")
	}
	// The whole discard cycle ran again against the record that won — the re-drive, as
	// opposed to binding the winner as if resuming it.
	if got := tr.discards(); got < 2 {
		t.Fatalf("a superseded discard must re-run the clean-start decision, got %d discard(s)", got)
	}

	// (c) The session this CONNECT ends up owning descends from NOTHING. This used to be
	// the lineage half of the re-drive claim — the create had to descend from the record
	// that WON rather than from the one the superseded discard claimed — and the
	// write-lineage audit removed the inheritance that carried it. The re-drive itself is
	// still proved, by the discard count above and the displace-in-place trace below; what is
	// asserted here is that the winner's identity, which IS available and is asserted
	// non-empty above, is not silently re-inherited.
	ps, _ := testLoadSessRecordByHash(t, asm, getHash(cid))
	if ps == nil {
		t.Fatal("the accepted CONNECT left no record")
	}
	if ps.PWID != _EMPTY_ {
		t.Fatalf("the accepted create descends from nothing: parent %q (the arbitration "+
			"winner was %q)", ps.PWID, winner)
	}

	// (a) NO save happened at base 0. F-105's clean start over a live predecessor displaces
	// the record IN PLACE through a discarding handoff conditioned on the predecessor's own
	// sequence (Codex consult 6); it never tears the record down and re-creates it at base 0.
	// A base-0 save anywhere would be the pre-F-105 clear-then-create stomp this item closed.
	bases := tr.saveBases()
	if len(bases) == 0 {
		t.Fatal("the CONNECT made no record write at all")
	}
	for i, b := range bases {
		if b == 0 {
			t.Fatalf("a base-0 store ran (attempt %d of %v): F-105 displaces the winner in "+
				"place at its own sequence and never creates at base 0", i, bases)
		}
	}
	// (b) and the final save DISPLACES THE WINNER IN PLACE: its CAS base is the sequence the
	// arbitration winner landed at, so the re-drive re-read the record that won and handed the
	// clean start off from it rather than stomping the base clear() reset. This is the F-105
	// form of the pre-cutover "final create at base 0" assertion: the winner's sequence is now
	// carried by the accept handoff rather than consumed by a separate teardown claim.
	if got, want := bases[len(bases)-1], tr.winnerSequence(); got != want {
		t.Fatalf("the accept save after a clean-start discard must displace the winner in "+
			"place: base %d, winner landed at %d, all %v", got, want, bases)
	}
}

// testLoadSessRecordByHash is testLoadSessRecord for a manager reached through a live
// server rather than through the two-broker fixture.
func testLoadSessRecordByHash(t *testing.T, asm *mqttAccountSessionManager, idHash string) (*mqttPersistedSession, uint64) {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("loadSessionMsg: %v", err)
		}
		return nil, 0
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		t.Fatalf("unmarshal session record: %v", uerr)
	}
	return ps, smsg.Sequence
}

// testRejectAcceptSave fails the first `times` record stores made by the session for
// clientID, with err, and reports how many it actually intercepted.
//
// It fires at the store boundary, so the CAS rejection reaches the accept save exactly
// as JetStream's would — including its TYPE, which is the whole point: 10071 and 10164
// demand opposite dispositions there.
func testRejectAcceptSave(t *testing.T, asm *mqttAccountSessionManager, clientID string, err error, times int) (fired func() int, restore func()) {
	t.Helper()
	var (
		mu sync.Mutex
		n  int
	)
	// F-105 cutover: the accepting CONNECT's save is the ownership metadata commit — a
	// three-member atomic batch (fact + COMMITTED receipt + record) — not the two-member
	// store the removed result hook rewrote. Its interception point is the ISSUING phase of
	// that batch, fired before the send, which reproduces the two-member path's CERTAIN
	// rejection: nothing lands, so sess.seq is still right and the accept loop classifies the
	// returned error exactly as it would a real store outcome (10164 -> create-retry,
	// 10071 -> re-drive cycle, anything else -> refusal). Matched by the record member's
	// subject, which is this client's session-record subject.
	recSubj := mqttSessSubject(asm.domainTk, getHash(clientID))
	restore = asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		if obs.Phase != mqttAcceptStoreIssuing || obs.RecSubj != recSubj {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if n >= times {
			return nil
		}
		n++
		return err
	})
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}, restore
}

// testClosePredecessorAuthority moves a client's SERVING outbound authority to CLOSED — the
// state an F-105 discarding teardown leaves behind (destroy closes the authority, writes the
// receipt and deletes the record). The redrive fixtures stage THIS rather than a bare record
// change because in F-105 a completed takeover or clean-start release closes the authority; a
// record mutated under a still-SERVING authority is a shape production never produces, so a
// re-drive meeting it can neither hand off (no predecessor) nor create (authority serving).
// See the Gap-2 finding: cancel-restore is correct, the fixture shape was pre-F-105.
func testClosePredecessorAuthority(t *testing.T, asm *mqttAccountSessionManager, cid string) {
	t.Helper()
	epoch := asm.sessStreamEpoch
	load := asm.jsa.loadOutboundAuthority(cid, epoch)
	if load.status != mqttOutboundValid || load.rec == nil || load.rec.State != mqttAuthorityServing {
		t.Fatalf("staging a CLOSED authority needs a SERVING predecessor for %q, got status=%v", cid, load.status)
	}
	a := load.rec
	if _, err := asm.jsa.opCloseGeneration(cid, epoch, mqttNewInflightNonce(), a.Gen, a.Tenure, a.OwnerTxn); err != nil {
		t.Fatalf("closing the predecessor authority for %q: %v", cid, err)
	}
}

// testResidentSession reports the session currently mapped for a client ID, and whether
// anything is bound to it. Both halves matter: R3 forbids leaving a re-minted object
// resident, and T2 forbids leaving the connecting client attached to one.
func testReDriveResident(asm *mqttAccountSessionManager, cid string) (*mqttSession, bool) {
	asm.mu.RLock()
	sess := asm.sessions[cid]
	asm.mu.RUnlock()
	if sess == nil {
		return nil, false
	}
	sess.mu.Lock()
	bound := sess.c != nil
	sess.mu.Unlock()
	return sess, bound
}

// ---------------------------------------------------------------------------
// Test 7 / M14: T2 does NOT dispose the connecting client
// ---------------------------------------------------------------------------

// TestReDriveT2KeepsTheConnectingClient is the regression the T0/T1/T2 split exists for.
//
// One disposal primitive for the whole re-drive would run the takeover tail here — and
// the client bound at the accept save is the one this CONNECT is accepting, so the tail
// would close the very connection it is trying to serve, and (if the replacement write
// still completed) leave this broker the durable owner with no serving client at all.
//
// The discriminators are: the connection survives, it is NOT jailed as a flapper, it is
// bound to the object that ends up resident, and it is still serving. "Received no
// DISCONNECT" is deliberately NOT one of them: mqttNotifyTakenOver already suppresses
// the notification before a success CONNACK, so it would hold against the takeover tail
// too.
func TestReDriveT2KeepsTheConnectingClient(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "redrive-t2"

	// A first connection, so an account session manager exists to inspect.
	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	defer c0.Close()
	asm := testMQTTGetAccountSessionManager(t, s, cid+"-probe")

	// The accept save is rejected 10071 exactly once, so the CONNECT re-drives through
	// T2 and then succeeds.
	fired, restore := testRejectAcceptSave(t, asm, cid, ApiErrors[JSStreamWrongLastSequenceErrF], 1)
	defer restore()

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	restore()

	if fired() != 1 {
		t.Fatalf("the accept save was not rejected, so T2 never ran: %d interception(s)", fired())
	}

	// Bound to the object that is actually resident — not to one T2 discarded.
	sess, bound := testReDriveResident(asm, cid)
	if sess == nil || !bound {
		t.Fatalf("the connecting client must be bound to the resident session after a re-drive: sess=%v bound=%v",
			sess != nil, bound)
	}
	// Not jailed: a flappers entry would delay this client's NEXT connect by a second
	// for a displacement that never happened.
	asm.mu.Lock()
	_, jailed := asm.flappers[cid]
	asm.mu.Unlock()
	if jailed {
		t.Fatal("a T2 re-drive must not add the connecting client to the flappers jail: nothing displaced it")
	}
	// And it is still serving: a round trip after the CONNACK proves the connection was
	// neither closed nor left half-bound.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "redrive/t2", qos: 0}}, []byte{0})
	testMQTTPublish(t, c, r, 0, false, false, "redrive/t2", 0, []byte("still here"))
	testMQTTCheckPubMsg(t, c, r, "redrive/t2", 0, []byte("still here"))
}

// ---------------------------------------------------------------------------
// Test 8 / M13: no seq == 0 session is left resident on a failure EXIT
// ---------------------------------------------------------------------------

// TestReDriveLeavesNoResidentAtBaseZero is R3's second half.
//
// A re-minted object is resident from addSession onward and carries sequence 0 until its
// first save lands. reconcileResidentOwnership returns early on ourSeq == 0 — an absent
// subject is the ordinary state of a session whose first record has not landed — so
// nothing reaps such an object. A failure exit that returns without removing it is
// therefore permanent split-brain, not a leak.
//
// Four exits are driven here: create-retry exhaustion (repeated 10164), cycle exhaustion
// (repeated 10071), the DEADLINE reached mid-flight, and a FAILED authoritative restore.
// The positive control is the test above, where a SUCCESSFUL re-drive does leave the
// replacement resident.
//
// The deadline case shares its return site with the counter cases rather than having one
// of its own — admitCycle and admitCreateRetry check the deadline themselves — so it is
// here to prove that by driving it, not because a separate branch was found. The restore
// case is the one exit whose cleanliness comes from ORDERING alone: T2 detaches before
// the restore it cannot know will succeed.
func TestReDriveLeavesNoResidentAtBaseZero(t *testing.T) {
	// Before the server. What this test asserts is that NOTHING reaps a resident at
	// sequence 0, so a periodic pass racing the assertion does not merely add jitter — it
	// can make a leak look reaped, which is the one direction that turns a real defect
	// into a passing run.
	testQuietExpirySweeps(t)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: "resident-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	defer c0.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "resident-probe")

	for _, tc := range []struct {
		name string
		cid  string
		err  error
	}{
		{"create retries exhausted", "exhaust-10164", ApiErrors[JSStreamWrongLastSequenceConstantErr]},
		{"re-drive cycles exhausted", "exhaust-10071", ApiErrors[JSStreamWrongLastSequenceErrF]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Far more rejections than either budget admits, so the CONNECT runs out.
			_, restore := testRejectAcceptSave(t, asm, tc.cid, tc.err, 100)
			defer restore()

			conn, _, _ := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: tc.cid, cleanSess: true},
				o.MQTT.Host, o.MQTT.Port, 0)
			if conn != nil {
				defer conn.Close()
			}
			restore()

			// Whatever the client was told, the broker must hold nothing for this ID.
			sess, bound := testReDriveResident(asm, tc.cid)
			if sess != nil {
				sess.mu.Lock()
				seq := sess.seq
				sess.mu.Unlock()
				t.Fatalf("a failed re-drive left a session resident (seq=%d bound=%v): "+
					"nothing reaps a resident at sequence 0", seq, bound)
			}
		})
	}

	// The DEADLINE, reached while attempts are still available. Driven through the
	// injected clock rather than by sleeping, and through a real CONNECT rather than by
	// calling the admission methods directly: what is under test is the CLEANUP at that
	// exit, which a direct call never reaches.
	t.Run("deadline reached mid-flight", func(t *testing.T) {
		const cid = "deadline-midflight"
		// The re-drive reads this clock once when it is constructed and once per
		// admission. So the budget is created inside the window and every admission after
		// it is outside — with both counters still untouched, which is what makes this the
		// deadline's exit and not a counter's.
		var (
			mu    sync.Mutex
			calls int
		)
		base := time.Now()
		restoreClock := setMQTTTestReDriveClock(func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return base
			}
			return base.Add(mqttRecordReDriveDeadline)
		})
		defer restoreClock()

		// Contention, not rejection: 10164 draws on the create-retry budget, which has
		// five admissions available and will not be what refuses.
		_, restore := testRejectAcceptSave(t, asm, cid, ApiErrors[JSStreamWrongLastSequenceConstantErr], 100)
		defer restore()

		conn, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: true},
			o.MQTT.Host, o.MQTT.Port, 0)
		if conn != nil {
			defer conn.Close()
		}
		restore()
		if cerr == nil {
			t.Fatal("a CONNECT whose re-drive ran past the admission deadline must be refused")
		}

		if sess, bound := testReDriveResident(asm, cid); sess != nil {
			t.Fatalf("the deadline exit left a session resident (bound=%v): it carries "+
				"sequence 0, which no reaper reaches", bound)
		}
	})

	// The AUTHORITATIVE RESTORE fails: T2 disposes BEFORE a restore it cannot know will
	// succeed, so that exit must be clean too. An undecodable record makes
	// createOrRestoreSessionWithin fail at its unmarshal, which is a real refusal shape
	// rather than an injected error.
	//
	// Observed IN FLIGHT, at a barrier inside the restore's own record load, and that is
	// the whole design of this case rather than a refinement of it. Asserting after the
	// CONNECT returns cannot see what this is about: the failed connection's teardown
	// removes the leaked object moments later, so a disposal that never happened and one
	// that happened at the right time are indistinguishable by then — measured, not
	// supposed. Blocking the load freezes the CONNECT between the disposal and the failure,
	// where the two are still distinguishable. The eventual assertion is kept below as the
	// regression check it is.
	t.Run("the authoritative restore fails", func(t *testing.T) {
		const cid = "restore-fails"
		var (
			mu       sync.Once
			loadOnce sync.Once
			smu      sync.Mutex
			t2Sess   *mqttSession
		)
		atBarrier, release := make(chan struct{}), make(chan struct{})
		subject := mqttSessStreamSubjectPrefix + asm.domainTk + getHash(cid)

		// The plant below writes this client's record subject, which this broker's own
		// session-record watch observes; its eviction worker then loads that subject
		// from another goroutine and would trip the barrier before the authoritative
		// restore ever ran. Drop the observation for this client only: the subtest is
		// about the re-drive, and the watch is a separate convergence channel with its
		// own tests.
		asm.mu.Lock()
		asm.testSessPersistDrop = func(h string) bool { return h == getHash(cid) }
		asm.mu.Unlock()
		defer func() {
			asm.mu.Lock()
			asm.testSessPersistDrop = nil
			asm.mu.Unlock()
		}()

		// F-105 cutover: the accept save is the ownership metadata commit, intercepted at its
		// ISSUING phase (before the send). The observer identifies the batch by the record
		// member's subject rather than by a session pointer, so the T2 object — the connecting
		// session, resident under its client ID at the save — is looked up here.
		recSubj := mqttSessSubject(asm.domainTk, getHash(cid))
		restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
			if obs.Phase != mqttAcceptStoreIssuing || obs.RecSubj != recSubj {
				return nil
			}
			var rejected bool
			mu.Do(func() {
				rejected = true
				asm.mu.RLock()
				t2 := asm.sessions[cid]
				asm.mu.RUnlock()
				smu.Lock()
				t2Sess = t2
				smu.Unlock()
				// Replace whatever the subject holds, so the record the re-drive is about to
				// read authoritatively cannot be parsed. The interception fires BEFORE the send,
				// so nothing of this attempt has landed and the standing sequence is the base the
				// plant must name (0 on a first clean start).
				var base uint64
				if smsg, lerr := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid)); lerr == nil {
					base = smsg.Sequence
				}
				framed, hdr := mqttFrameRecordWrite(base, []byte("{not-a-record"))
				if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(cid), hdr, framed); err != nil {
					t.Errorf("planting the undecodable record: %v", err)
				}
				// Armed only HERE, after the plant's own load, so the barrier lands on the
				// authoritative restore and on nothing before it.
				setMQTTTestLoadLastMsgHook(func(stream, subj string) error {
					if subj != subject {
						return nil
					}
					loadOnce.Do(func() {
						close(atBarrier)
						<-release
					})
					return nil
				})
			})
			if rejected {
				return ApiErrors[JSStreamWrongLastSequenceErrF]
			}
			return nil
		})
		defer restore()
		defer mqttTestLoadLastMsgHookV.Store(nil)

		var (
			cmu   sync.Mutex
			conn  net.Conn
			cerr  error
			ended = make(chan struct{})
		)
		go func() {
			defer close(ended)
			c, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: true},
				o.MQTT.Host, o.MQTT.Port, 0)
			cmu.Lock()
			conn, cerr = c, err
			cmu.Unlock()
		}()

		select {
		case <-atBarrier:
		case <-ended:
			t.Fatal("the CONNECT finished without its authoritative restore ever loading " +
				"the record: the T2 transition under test never ran")
		case <-time.After(10 * time.Second):
			close(release)
			<-ended
			t.Fatal("the authoritative restore never reached its record load")
		}

		// FROZEN between the disposal and the failure. T2 must already have let this object
		// go — out of BOTH maps, and detached from the client it was accepting — because the
		// restore it is about to attempt can fail, and a disposal that waited for the return
		// would leave a sequence-0 object resident and bound with no reaper that reaches it.
		smu.Lock()
		disposed := t2Sess
		smu.Unlock()
		if disposed == nil {
			close(release)
			<-ended
			t.Fatal("the accept save never ran, so there is no T2 object to assert on")
		}
		asm.mu.RLock()
		byID, byIDok := asm.sessions[cid]
		byHash, byHashok := asm.sessByHash[getHash(cid)]
		asm.mu.RUnlock()
		disposed.mu.Lock()
		stillBound := disposed.c != nil
		disposed.mu.Unlock()
		close(release)
		<-ended

		if byIDok && byID == disposed {
			t.Error("T2 left the discarded object mapped by client ID while its restore was " +
				"still in flight: a restore failure would strand it at sequence 0")
		}
		if byHashok && byHash == disposed {
			t.Error("T2 left the discarded object mapped by client-ID HASH: the eviction " +
				"paths resolve through that map, so an entry there is reachable state")
		}
		if stillBound {
			t.Error("T2 left the connecting client BOUND to the discarded object: its " +
				"readLoop gate checks the binding, not residency, so it would keep serving " +
				"packets against a session the restore is about to abandon")
		}

		cmu.Lock()
		gotConn, gotErr := conn, cerr
		cmu.Unlock()
		if gotConn != nil {
			defer gotConn.Close()
		}
		if gotErr == nil {
			t.Fatal("a CONNECT whose authoritative restore failed must be refused: " +
				"nothing may be resumed or created on a record that cannot be read")
		}
		// The eventual state, kept as the regression check it is rather than as this
		// subtest's evidence.
		if sess, bound := testReDriveResident(asm, cid); sess != nil {
			t.Fatalf("a failed restore left a session resident (bound=%v)", bound)
		}
	})
}

// ---------------------------------------------------------------------------
// Test 13 / M11: 10071 and 10164 take different branches at the accept save
// ---------------------------------------------------------------------------

// TestAcceptSaveRejectionKindsTakeDifferentBranches is why the accept save takes the
// detail rather than save()'s %v-formatted error.
//
// 10164 is contention: nothing is stored and ownership is not decided either way, so the
// WRITE is repeated, drawing on the create-retry budget. 10071 is a definite rejection,
// so the base is re-read authoritatively and a whole re-drive CYCLE is spent.
//
// The discriminator is the pair of unequal budgets. With rejections above the cycle cap
// but inside the create-retry cap, the two kinds must reach OPPOSITE outcomes: the
// contention run is accepted, and the definite run exhausts its cycles and is refused.
// A 10164 handled as a 10071 fails the first half; a 10071 handled as a 10164 fails the
// second.
func TestAcceptSaveRejectionKindsTakeDifferentBranches(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: "kinds-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	defer c0.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "kinds-probe")

	// Above the cycle cap, inside the create-retry cap. The constants are deliberately
	// unequal, and this is the window that exists only because they are.
	rejections := mqttRecordReDriveCycles + 1
	if rejections > mqttRecordCreateRetries {
		t.Fatalf("the budgets no longer leave a discriminating window: cycles=%d creates=%d",
			mqttRecordReDriveCycles, mqttRecordCreateRetries)
	}

	t.Run("contention retries the write and the CONNECT is accepted", func(t *testing.T) {
		const cid = "kind-10164"
		// The observer both injects the contention (ISSUING phase, before the send, so nothing
		// lands) and CAPTURES each fire, to prove the resend re-sends the SAME frozen attempt.
		recSubj := mqttSessSubject(asm.domainTk, getHash(cid))
		var (
			mu   sync.Mutex
			n    int
			seen []mqttAcceptStoreObs
		)
		restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
			if obs.Phase != mqttAcceptStoreIssuing || obs.RecSubj != recSubj {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, obs)
			if n < rejections {
				n++
				return ApiErrors[JSStreamWrongLastSequenceConstantErr]
			}
			return nil
		})
		defer restore()
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true},
			o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		restore()

		mu.Lock()
		got := append([]mqttAcceptStoreObs(nil), seen...)
		mu.Unlock()
		if len(got) != rejections+1 {
			t.Fatalf("the metadata commit issued %d time(s), want %d (%d contention rejections + 1 "+
				"success)", len(got), rejections+1, rejections)
		}
		// Proposal identity across retries (Codex consult 10): each resend re-sends the SAME
		// frozen attempt, so the acceptance fact, the session record and the COMMITTED receipt
		// are byte-identical every time and only the transport batch id is fresh. A retry that
		// re-composed instead (a fresh saveAcceptWithOwnership) would mint a new acceptID, so the
		// fact digest would change and the permanent slot of the old proposal would be orphaned.
		batchIDs := map[string]bool{}
		for i, obs := range got {
			if obs.BatchID == _EMPTY_ || batchIDs[obs.BatchID] {
				t.Fatalf("resend %d did not mint a fresh transport batch id (batch %q)", i, obs.BatchID)
			}
			batchIDs[obs.BatchID] = true
			if obs.FactDigest != got[0].FactDigest {
				t.Fatalf("resend %d changed the acceptance fact: the frozen attempt was re-composed, "+
					"not re-sent", i)
			}
			if obs.RecDigest != got[0].RecDigest {
				t.Fatalf("resend %d changed the session record bytes: the frozen attempt was "+
					"re-composed, not re-sent", i)
			}
			if obs.RecptDigest != got[0].RecptDigest {
				t.Fatalf("resend %d changed the COMMITTED receipt: the frozen attempt was re-composed, "+
					"not re-sent", i)
			}
		}
	})

	t.Run("a definite rejection spends a cycle and exhausts them", func(t *testing.T) {
		fired, restore := testRejectAcceptSave(t, asm, "kind-10071",
			ApiErrors[JSStreamWrongLastSequenceErrF], rejections)
		defer restore()
		conn, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "kind-10071", cleanSess: true},
			o.MQTT.Host, o.MQTT.Port, 0)
		if conn != nil {
			defer conn.Close()
		}
		restore()
		if err == nil {
			t.Fatalf("%d definite rejections exceed the %d-cycle budget, so the CONNECT must be refused; "+
				"accepting it means they were treated as contention", rejections, mqttRecordReDriveCycles)
		}
		// One rejection per admitted attempt: the initial one plus the cycles.
		if got, want := fired(), mqttRecordReDriveCycles+1; got != want {
			t.Fatalf("a definite rejection must be admitted %d times (the initial attempt is not a retry), got %d",
				want, got)
		}
	})
}

// TestAcceptAmbiguousMetadataCommitAdoptsByReadback covers the resolved-COMMITTED adoption
// path (F-105 cutover, Codex consult 10): a metadata commit whose three-member batch LANDED
// but whose acknowledgement this broker never saw. The accept executor resolves the permanent
// receipt slot to COMMITTED and adopts the landed record sequence by READBACK — matched by
// write identity — rather than leaving sess.seq at the stale base. Leaving it stale was the
// latent gap the old switch had: it returned success without adopting, so the accept's
// subsequent saves would CAS the wrong base. Staged by tearing the ACKED phase once, AFTER the
// batch has committed, so the fact, the receipt and the record are all durable while the
// caller sees a failure.
func TestAcceptAmbiguousMetadataCommitAdoptsByReadback(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: "ambig-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	defer c0.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "ambig-probe")

	const cid = "ambig-commit"
	recSubj := mqttSessSubject(asm.domainTk, getHash(cid))
	var (
		mu       sync.Mutex
		tornOnce bool
		ackedSeq uint64
	)
	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		if obs.RecSubj != recSubj || obs.Phase != mqttAcceptStoreAcked {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if tornOnce {
			return nil
		}
		tornOnce = true
		// The batch has ALREADY committed at this phase; obs.Seq is the landed record sequence,
		// and returning an error hands the caller a failure though the fact, receipt and record
		// are durable — the ambiguous outcome the readback adoption exists to resolve.
		ackedSeq = obs.Seq
		return fmt.Errorf("the metadata commit acknowledgement is torn for %q", cid)
	})
	defer restore()

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	restore()

	mu.Lock()
	torn, seq := tornOnce, ackedSeq
	mu.Unlock()
	if !torn {
		t.Fatal("the ACKED phase never fired, so the ambiguous-commit path was not exercised")
	}
	sess, bound := testReDriveResident(asm, cid)
	if sess == nil || !bound {
		t.Fatalf("an ambiguous-but-committed accept must leave its session resident and bound: "+
			"sess=%v bound=%v", sess != nil, bound)
	}
	sess.mu.Lock()
	adopted := sess.seq
	sess.mu.Unlock()
	if adopted != seq {
		t.Fatalf("the accept adopted session sequence %d but the committed record landed at %d: the "+
			"resolved-COMMITTED verdict did not adopt the landed sequence by readback", adopted, seq)
	}
}

// ---------------------------------------------------------------------------
// Tests 6 and 15 / M7, M19: T1 branches on what is actually attached
// ---------------------------------------------------------------------------

// TestReDriveT1BranchesOnTheOldBinding covers both T1 arms.
//
// Resident does not imply bound: a record-restored session is inserted with no client, a
// disconnected persistent session stays resident after its client detaches, and the
// uncertainty re-restore produces the same shape. Running the takeover tail
// unconditionally (M19) would jail a client ID whose previous connection had already
// gone, delaying that client's next connect by a second for a displacement that never
// happened — and would try to notify and close a nil client.
//
// T1a (unbound): no flappers entry. T1b (bound): the old connection is closed with
// DuplicateClientID and the ID IS jailed, exactly as an ordinary takeover.
func TestReDriveT1BranchesOnTheOldBinding(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	t.Run("T1a: resident and unbound tells nobody", func(t *testing.T) {
		const cid = "t1a-unbound"
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		asm := testMQTTGetAccountSessionManager(t, s, cid)
		// A clean disconnect leaves the persistent session RESIDENT with no client.
		testMQTTDisconnectEx(t, c, nil, true)
		c.Close()
		if sess, bound := testReDriveResident(asm, cid); sess == nil || bound {
			t.Fatalf("the fixture needs a resident, unbound session: sess=%v bound=%v", sess != nil, bound)
		}

		tr := testTraceReDrive(t, asm)
		c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

		if tr.winner() == _EMPTY_ {
			t.Fatal("the rival record never landed, so T1 never ran")
		}
		asm.mu.Lock()
		_, jailed := asm.flappers[cid]
		asm.mu.Unlock()
		if jailed {
			t.Fatal("T1a must add NO flappers entry: there was no connection to displace, " +
				"and jailing the ID delays this client's next connect for nothing")
		}
	})

	t.Run("T1b: an attached old client is taken over", func(t *testing.T) {
		const cid = "t1b-bound"
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		asm := testMQTTGetAccountSessionManager(t, s, cid)
		if sess, bound := testReDriveResident(asm, cid); sess == nil || !bound {
			t.Fatalf("the fixture needs a resident, BOUND session: sess=%v bound=%v", sess != nil, bound)
		}

		tr := testTraceReDrive(t, asm)
		c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

		if tr.winner() == _EMPTY_ {
			t.Fatal("the rival record never landed, so T1 never ran")
		}
		// The old connection is gone, and the ID is jailed — the ordinary takeover tail,
		// which a re-driven displacement is entitled to just as much as a plain one.
		asm.mu.Lock()
		_, jailed := asm.flappers[cid]
		asm.mu.Unlock()
		if !jailed {
			t.Fatal("T1b displaces a live connection, so it must run the full takeover tail")
		}
		if _, err := testMQTTRead(c); err == nil {
			t.Fatal("the displaced connection must be closed")
		}
	})
}

// ---------------------------------------------------------------------------
// Test 14 / M18: T2 hands the accept record transaction over cleanly
// ---------------------------------------------------------------------------

// TestReDriveT2HandsOverTheAcceptTransaction pins the fence, not the token.
//
// acceptTok belongs to the object it was opened on, and the deferred close ends whatever
// the accept-session variable names at defer time. Reassigning the session without
// ending the old transaction first would have that defer close the OLD token against the
// NEW object, leaking the discarded object's fence — after which nothing can ever take
// that object's record fence again.
//
// It must NOT assert that the two tokens differ: a recordTx is object-local
// {epoch, id, owner} and every session starts its counters at zero, so the two can
// legitimately be value-equal and such an assertion would be testing a coincidence.
func TestReDriveT2HandsOverTheAcceptTransaction(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "t2-handover"
	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	defer c0.Close()
	asm := testMQTTGetAccountSessionManager(t, s, cid+"-probe")

	// Capture every session object this CONNECT saved through, so the DISCARDED one can
	// be inspected after the CONNECT completes.
	var (
		mu   sync.Mutex
		seen []*mqttSession
	)
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id != cid {
			return nil
		}
		mu.Lock()
		if len(seen) == 0 || seen[len(seen)-1] != sess {
			seen = append(seen, sess)
		}
		mu.Unlock()
		return nil
	})
	defer restoreSave()

	fired, restore := testRejectAcceptSave(t, asm, cid, ApiErrors[JSStreamWrongLastSequenceErrF], 1)
	defer restore()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	restore()
	restoreSave()

	if fired() != 1 {
		t.Fatalf("the accept save was not rejected, so no handover happened: %d", fired())
	}
	mu.Lock()
	objects := append([]*mqttSession(nil), seen...)
	mu.Unlock()
	if len(objects) < 2 {
		t.Fatalf("a T2 handover replaces the session object, so at least two must have saved: %d", len(objects))
	}
	discarded, replacement := objects[0], objects[len(objects)-1]

	// The discarded object's fence must be CLOSED — a transaction left open on it can
	// never be ended, because nothing holds a reference to it any more. Taking a fresh
	// transaction on it is what proves the fence is free.
	tok, ok := discarded.tryBeginRecordTx()
	if !ok {
		t.Fatal("the discarded object's record fence was left OPEN: T2 must end its accept " +
			"transaction against the object it was opened on, before switching")
	}
	if err := discarded.endRecordTx(tok); err != nil {
		t.Fatalf("ending the probe transaction on the discarded object: %v", err)
	}
	// And the replacement is the one the CONNECT is actually using: it is resident.
	if sess, bound := testReDriveResident(asm, cid); sess != replacement || !bound {
		t.Fatalf("the replacement object must be the resident, bound one: resident=%p replacement=%p bound=%v",
			sess, replacement, bound)
	}
}

// ---------------------------------------------------------------------------
// Test 4 / M3: a superseded RELEASE re-drives too
// ---------------------------------------------------------------------------

// TestSupersededReleaseReDrivesTheCleanStart stages the OTHER superseded arm: the claim
// is WON, and the exact-sequence release then finds the marker REPLACED by a newer write.
//
// "Superseded" is a fact about the marker, not about who holds the session: a rival write
// landed after our claim, and nothing here demonstrates that its author still holds
// anything (F-084). What the re-drive rests on is our own claim's sequence no longer being
// current, which is enough on its own.
//
// It matters separately because the two arms say different things about what happened.
// The claim arm deleted nothing; this one ran the deletions first — legitimately, since
// the claim was won — so the sentinel must not promise "nothing was deleted", and the
// CONNECT must still re-drive rather than create at the base clear() reset.
// TestSupersededReleaseReDrivesTheCleanStart was RETIRED in the F-105 cutover (Codex
// consult 10). It staged the pre-F-105 claim-then-release flow: a teardown WON its marker
// (mqttTestTeardownClaimedHookV), a rival then replaced the marker, and the separate
// exact-sequence RELEASE removed nothing, so the clean start re-drove. Under F-105 a clean
// start over a SERVING authority takes the ATOMIC discarding handoff (opFinalizeHandoff) —
// there is no separate claim then release, so the teardown-claimed hook never fires on this
// path and the window it tested is unreachable. The surviving assertion — a superseded
// clean start re-evaluates its decision and displaces at the winner's sequence rather than
// writing at the base-0 reset — is covered by TestSessSupersededDiscardReDrivesTheCleanStart
// (the converted base-0 -> displace-in-place test). NOTE: finalization-loss recovery (a
// crash after the metadata committed but before finalize) is a DISTINCT F-105 scenario, owed
// separately; it must finalize the committed proposal without repeating metadata or
// restarting clean start, and is NOT what this retired fixture covered.

// ---------------------------------------------------------------------------
// Test 3(f) / M26: a moved winner that no longer needs discarding is RESUMED
// ---------------------------------------------------------------------------

// TestReDriveResumesAWinnerThatNeedsNoDiscard is the branch that proves the re-drive
// re-EVALUATES the clean-start decision instead of repeating it.
//
// The first pass discards because the session it found was itself clean. The record that
// wins is a persistent, non-overdue one, and the connecting client is persistent too — so
// the second pass must NOT discard: it resumes the winner, reports Session Present, and
// CASes its accept save at the winner's sequence rather than creating at base 0. Forcing
// clear() after restoring any record during a re-drive fails exactly here.
func TestReDriveResumesAWinnerThatNeedsNoDiscard(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "redrive-resume"
	// A CLEAN session first: it is discarded on the reconnect below because the session
	// found is clean, not because this CONNECT asks for it.
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, cid)

	// The winner is a PERSISTENT, connected record: nothing about it is overdue.
	tr := testTraceReDrive(t, asm, func(ps *mqttPersistedSession) {
		ps.Clean = false
		ps.ExpiryInterval = mqttSessionNeverExpire
		ps.DisconnectedAt = 0
	})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	// Session Present TRUE: the winner is a persistent session and this CONNECT resumes
	// it. A re-drive that discarded unconditionally would report false.
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	c.Close()

	if tr.winner() == _EMPTY_ {
		t.Fatal("the rival record never landed, so no re-drive happened")
	}
	if got := tr.discards(); got != 1 {
		t.Fatalf("the re-drive must RE-EVALUATE the decision against the record it restored, "+
			"and a persistent non-overdue winner needs no discard: got %d discard(s)", got)
	}
	// Its accept save CASes at the winner's sequence, not at base 0.
	bases := tr.saveBases()
	if len(bases) == 0 {
		t.Fatal("the CONNECT made no record write")
	}
	if last := bases[len(bases)-1]; last == 0 {
		t.Fatalf("resuming a winner CASes at ITS sequence; a base-0 create means the "+
			"decision was not re-evaluated: bases %v", bases)
	}
}

// TestReDriveT1ExhaustionLeavesNothingResident is the M13 invariant at the T1 exit
// (M15).
//
// T1 disposes BEFORE the budget check and before the fallible restore, and that ordering
// is the whole point: clear() has already reset the object to sequence 0 without
// detaching its client, and nothing reaps such an object. Disposing after the check
// instead means the exhaustion exit returns with it still resident and still bound.
func TestReDriveT1ExhaustionLeavesNothingResident(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "t1-exhaust"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	testMQTTDisconnectEx(t, c, nil, true)
	c.Close()

	// A rival on EVERY discard, so the CONNECT never converges and runs out of cycles.
	tr := testTraceReDrive(t, asm)
	tr.mu.Lock()
	tr.everyDiscard = true
	tr.mu.Unlock()

	conn, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: true},
		o.MQTT.Host, o.MQTT.Port, 0)
	if conn != nil {
		defer conn.Close()
	}
	if err == nil {
		t.Fatal("a CONNECT that loses the arbitration on every cycle must be refused")
	}
	if got := tr.discards(); got < mqttRecordReDriveCycles {
		t.Fatalf("the re-drive must spend its cycle budget before giving up, got %d discard(s)", got)
	}
	if sess, bound := testReDriveResident(asm, cid); sess != nil {
		sess.mu.Lock()
		seq := sess.seq
		sess.mu.Unlock()
		t.Fatalf("an exhausted T1 re-drive left a session resident (seq=%d bound=%v): clear() "+
			"re-minted it at sequence 0 and nothing reaps that, so the disposal must precede "+
			"the budget check", seq, bound)
	}
}

// ---------------------------------------------------------------------------
// Round-1 finding: a POST-RESET clear() failure must not leave a client bound
// ---------------------------------------------------------------------------

// TestFailedTeardownDetachesTheOldClient covers every ordinary clear() failure that
// happens AFTER the destructive reset — an indeterminate claim, a failed target
// deletion, an unclassifiable release, a shared-durable error.
//
// Both of clear()'s refusals happen before it destroys anything and carry
// errMQTTClearRefusedUnresolved, so any other error means the object is already wiped:
// state erased, sequence 0, generation re-minted. removeSession evicts it but does not
// DETACH it, and a readLoop's gate checks the binding rather than residency — so the old
// client kept serving packets against a session that existed nowhere, unreachable by
// every watch, backstop and sweep.
//
// The fault is injected through production behaviour rather than a hook: an undecodable
// record on the subject makes claimTeardown's resolving re-read fail to parse, which is
// its indeterminate arm. The CONNECT must fail (nothing may be resumed on an
// unclassifiable record) AND the old connection must be closed rather than left serving.
func TestFailedTeardownDetachesTheOldClient(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "teardown-fault"
	old, oldR := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer old.Close()
	testMQTTCheckConnAck(t, oldR, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, cid)

	sess, bound := testReDriveResident(asm, cid)
	if sess == nil || !bound {
		t.Fatal("the fixture needs a resident session with the old client bound")
	}

	// ISOLATE the fault from Slice A's convergence machinery. The undecodable record
	// planted below is itself a session-record write, so the watch observes it and
	// spawns an eviction worker that detaches this very client asynchronously — which
	// would mask the defect under test and make the paired mutation survive whenever the
	// worker won the race. Dropping the observation and disabling the backstop leaves
	// exactly one thing that can detach the old client: the code being tested.
	asm.mu.Lock()
	asm.testSessPersistDrop = func(string) bool { return true }
	asm.testSessRecordBackstopOff = true
	asm.mu.Unlock()

	// An undecodable record, landed at the session's own sequence so it MOVES the
	// subject: the teardown's claim CAS then fails and its re-read cannot parse what it
	// finds, which is claimTeardown's indeterminate outcome.
	sess.mu.Lock()
	seq, idHash, domainTk := sess.seq, sess.idHash, sess.domainTk
	sess.mu.Unlock()
	framed, hdr := mqttFrameRecordWrite(seq, []byte("{not-a-record"))
	if _, err := asm.jsa.storeSessionMsg(domainTk, idHash, hdr, framed); err != nil {
		t.Fatalf("landing the undecodable record: %v", err)
	}

	// A clean-start CONNECT now discards that session, and its teardown cannot classify.
	conn, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: true},
		o.MQTT.Host, o.MQTT.Port, 0)
	if conn != nil {
		defer conn.Close()
	}
	if err == nil {
		t.Fatal("a CONNECT whose teardown could not be classified must be refused: " +
			"nothing may be resumed or created on an unclassifiable record")
	}

	// The object is gone from the maps AND nothing is bound to it. Residency alone was
	// never the gap: the old client's readLoop does not consult it.
	if got, stillBound := testReDriveResident(asm, cid); got != nil || stillBound {
		t.Fatalf("a post-reset teardown failure must leave nothing resident: sess=%v bound=%v",
			got != nil, stillBound)
	}
	sess.mu.Lock()
	attached := sess.c != nil
	sess.mu.Unlock()
	if attached {
		t.Fatal("the old client is still BOUND to the wiped session: it will keep serving " +
			"packets against a session that exists nowhere, and no reaper can reach it")
	}
	// And the connection is actually closed, not merely detached.
	if _, rerr := testMQTTRead(old); rerr == nil {
		t.Fatal("the old connection must be closed: its session's state was destroyed by " +
			"a teardown that could not complete")
	}
}

// ---------------------------------------------------------------------------
// R1 at the deferred-discard door: a superseded discharge re-reads the record
// ---------------------------------------------------------------------------

// testParkDiscardOnAnAmbiguousWrite makes sess's NEXT discard refuse and re-park itself,
// by arming one ambiguous write in clear()'s pre-fence window — the window only a check
// made under the FENCE can still see, which is what makes the discard get as far as
// refusing. The bytes are the ones a real save would have stored, so the resolution that
// follows re-attempts a faithful write and classifies it as ours.
//
// `then` runs on every LATER pre-fence window on the same session: that is where a test
// stages what the DISCHARGE's own discard will meet. One hook slot serves both phases
// because there is only one — two hooks would silently overwrite each other.
//
// Returns the identity the armed bytes carry, which is the WID the record ends up holding
// and therefore the one a re-minted object would wrongly stamp as its parent.
func testParkDiscardOnAnAmbiguousWrite(t *testing.T, sess *mqttSession, then func(*mqttSession)) func() string {
	t.Helper()
	var (
		mu       sync.Mutex
		armedWID string
	)
	restore := setMQTTTestClearWindowHook(func(cur *mqttSession, window string) {
		if cur != sess || window != mqttClearWindowPreFence {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if armedWID != _EMPTY_ {
			if then != nil {
				then(cur)
			}
			return
		}
		cur.mu.Lock()
		ps := cur.buildPersistedSessionLocked()
		ps.WID, ps.PWID = mqttNewRecordWID(), cur.wid
		body, merr := json.Marshal(&ps)
		if merr == nil {
			testMarkUncertainLocked(cur, cur.seq, body)
			armedWID = ps.WID
		}
		cur.mu.Unlock()
		if merr != nil {
			t.Errorf("marshalling the ambiguous attempt: %v", merr)
		}
	})
	t.Cleanup(restore)
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return armedWID
	}
}

// testAwaitParkedDiscard waits for the state a refused discard leaves: the session still
// RESIDENT, its close obligation PARKED, and its record write still unresolved. That is
// what the reconnect has to resolve before it may decide anything, and asserting it here
// is what stops a later failure from being read as the mechanism under test.
func testAwaitParkedDiscard(t *testing.T, asm *mqttAccountSessionManager, sess *mqttSession) {
	t.Helper()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if !testSessionResident(asm, sess) {
			return fmt.Errorf("the refused discard must leave the session resident")
		}
		if testParkedAction(sess) == nil {
			return fmt.Errorf("the discard must be parked for the reconnect to discharge")
		}
		if !sess.isSeqUncertain() {
			return fmt.Errorf("the record write must still be unresolved")
		}
		return nil
	})
}

// TestReDriveDeferredDischargeIsAdmitted is the admission half of the same arm.
//
// The re-entry that arm performs is a CYCLE by this item's own definition — a fresh
// authoritative restore, and off it a whole teardown and accept save — so R3 must bound
// it. Jumping straight to the restore would begin exactly that work after the deadline,
// which is the guarantee CONFORMANCE states.
//
// The deadline is the only predicate that can refuse here, and that is a property worth
// stating rather than a limitation of the test: the arm is reachable at most once per
// CONNECT, since the restore it performs yields an object that is not uncertain, so the
// cycle CAP can never be what stops it.
func TestReDriveDeferredDischargeIsAdmitted(t *testing.T) {
	testQuietExpirySweeps(t)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "redrive-discharge-deadline"

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm, sess := testResidentSession(t, s, cid)

	// No second-phase staging: the discharge's discard COMPLETES here. It removes the
	// session either way, which is the whole reason this arm cannot tell the two outcomes
	// apart — and it means the admission is reached without any arbitration being lost.
	testParkDiscardOnAnAmbiguousWrite(t, sess, nil)

	c.Close()
	testAwaitParkedDiscard(t, asm, sess)

	// Constructed inside the window, every admission after it outside — with both counters
	// still untouched, so what refuses is unambiguously the deadline.
	var (
		mu    sync.Mutex
		calls int
	)
	base := time.Now()
	restoreClock := setMQTTTestReDriveClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return base
		}
		return base.Add(mqttRecordReDriveDeadline)
	})
	defer restoreClock()

	conn, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port, 0)
	if conn != nil {
		defer conn.Close()
	}
	if cerr == nil {
		t.Fatal("a CONNECT that would start a fresh re-drive cycle past the admission " +
			"deadline must be refused, not admitted unbounded")
	}
	// And the refusal is clean: the discharge removed the session before this arm ran, so
	// there is nothing left resident to reap.
	if got, bound := testReDriveResident(asm, cid); got != nil {
		t.Fatalf("the refused admission left a session resident (bound=%v)", bound)
	}
}

// TestReDriveReReadsAfterASupersededDeferredDiscard closes the one door into the re-drive
// that reports nothing.
//
// A CONNECT that finds its session's record write unresolved resolves it before deciding
// anything, and a resolution that comes back OURS discharges whatever the close parked.
// When that is a deferred DISCARD, the discharge runs clear() and CONSUMES its outcome:
// the session leaves the maps whether the teardown COMPLETED or was SUPERSEDED. So this
// frame cannot tell the two apart by asking, which is why it must not reuse the object it
// left behind — that object carries the sequence clear() reset to 0 after the base was
// already known stale.
//
// Staged on the ABSENT arm of a superseded claim, because that is the arm where the two
// dispositions differ DURABLY rather than only in path: with the record gone the base-0
// create LANDS either way, and only its lineage says which object wrote it. R2 requires a
// write's base and parent to come from one authoritative read, and the authoritative read
// here finds nothing — so this is a genuine first-create with an EMPTY parent. Stamping
// the retired record's WID instead would make the session read as a SIBLING of anything
// else parented to that record, and a false forked verdict suppresses delayed-Will
// cancellation (mqtt.go, `if !d.forked`), so this is not a vocabulary-only difference.
func TestReDriveReReadsAfterASupersededDeferredDiscard(t *testing.T) {
	// Before the server: reconcileResidentRecordState rides the expiry sweep's ticker and
	// discharges a parked action itself. If it wins, the CONNECT never enters the arm
	// under test and the fixture measures nothing while still passing.
	testQuietExpirySweeps(t)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "redrive-deferred-discard"

	// A clean session, so its close is a DISCARD rather than a survival.
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm, sess := testResidentSession(t, s, cid)

	// On the DISCHARGE's discard, the record is gone before the teardown claims it — what
	// a rival broker's own clean-start release leaves behind. claimTeardown reports an
	// absent subject as SUPERSEDED, and no marker lands, so the subject is empty for
	// whatever writes next.
	var (
		mu      sync.Mutex
		deleted bool
	)
	armedWID := testParkDiscardOnAnAmbiguousWrite(t, sess, func(cur *mqttSession) {
		mu.Lock()
		defer mu.Unlock()
		if deleted {
			return
		}
		cur.mu.Lock()
		seq := cur.seq
		cur.mu.Unlock()
		if seq == 0 {
			t.Error("the discharged discard must hold a record sequence to claim")
			return
		}
		// F-105 shape of the rival's completed clean-start release: the authority is CLOSED and
		// the record is gone, so the re-drive's authoritative read finds an absent subject over a
		// CLOSED generation and makes a genuine first create.
		testClosePredecessorAuthority(t, asm, cid)
		if derr := asm.jsa.deleteMsg(mqttSessStreamName, seq, true); derr != nil {
			t.Errorf("deleting the record the discharge is about to claim: %v", derr)
			return
		}
		deleted = true
	})

	c.Close()
	testAwaitParkedDiscard(t, asm, sess)

	// The reconnect: it resolves the write as ours, discharges the parked discard, and
	// that discard is superseded. It must be ACCEPTED — losing an arbitration is not a
	// failure of this CONNECT — and it must report no session present, because the record
	// it was going to resume is gone.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	mu.Lock()
	staged := deleted
	mu.Unlock()
	retired := armedWID()
	// Both halves of the staging are positive controls: without a retiring identity the
	// lineage assertion below could not fail, and without the deletion the discharge was
	// never superseded and nothing was under test.
	if retired == _EMPTY_ {
		t.Fatal("the ambiguous attempt never carried an identity, so nothing could be mis-parented")
	}
	if !staged {
		t.Fatal("the discharged discard never met an absent record: it was not superseded")
	}

	ps, _ := testLoadSessRecordByHash(t, asm, getHash(cid))
	if ps == nil {
		t.Fatal("the accepted CONNECT left no record")
	}
	if ps.WID == retired {
		t.Fatal("the CONNECT re-used the retired record's identity instead of creating one")
	}
	// The discriminator, and it is the RETIRED WID specifically: a create after an
	// authoritative read of an empty subject must not carry the identity the discharge
	// reset off the object, because that is a write whose base and parent came from two
	// different reads — exactly what R2 forbids. `retired` is asserted non-empty above,
	// so this arm can genuinely fail.
	if ps.PWID == retired {
		t.Fatalf("a create after an authoritative read of an ABSENT subject must not inherit the "+
			"RETIRED record's identity: parent %q", ps.PWID)
	}
	// And it inherits nothing else either: a create is parentless since the
	// write-lineage audit deleted every inheritance source.
	if ps.PWID != _EMPTY_ {
		t.Fatalf("a create descends from nothing: parent %q", ps.PWID)
	}
}

// ---------------------------------------------------------------------------
// Tests 3(d), 3(e), 3(g): the branches a superseded discard can land in
// ---------------------------------------------------------------------------

// testLoudRecorder captures WARN and ERROR messages from a live server. Both levels
// matter here: the two terminal callers below report a failed discard at different
// levels, and what the typed branch changes is that neither reports one at all.
type testLoudRecorder struct {
	slog.Handler
	mu   sync.Mutex
	msgs []string
}

func (h *testLoudRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *testLoudRecorder) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.msgs = append(h.msgs, r.Message)
		h.mu.Unlock()
	}
	return nil
}

// saw reports the first captured message containing substr, so a failure names the line
// the broker actually emitted rather than only that one existed.
func (h *testLoudRecorder) saw(substr string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if strings.Contains(m, substr) {
			return m, true
		}
	}
	return _EMPTY_, false
}

// testCaptureServerLogs is testCaptureWarnings for a live server rather than the
// two-broker record fixture.
func testCaptureServerLogs(t *testing.T, s *Server) *testLoudRecorder {
	t.Helper()
	h := &testLoudRecorder{Handler: slog.NewTextHandler(io.Discard, nil)}
	prev := s.log.Load()
	s.log.Store(slog.New(h))
	t.Cleanup(func() { s.log.Store(prev) })
	return h
}

// TestSessSupersededByAnotherMarkerAdoptsIt is test 3(d).
//
// The subject can be taken by another broker's teardown MARKER rather than by a record,
// and claimTeardown reports that as superseded too. The re-drive's authoritative read
// then finds a marker, so the right outcome is to ADOPT it — finish the deletions,
// release it, and create fresh — with NO second discard, because there is nothing left
// to discard.
//
// The discriminators are PATH-shaped, deliberately. The end state converges: a stale
// base-0 create against the marker is CAS-rejected by B1, T2 restores, adopts, and
// stamps the same parent the correct path stamps — so a final-record assertion alone
// would pass against the defect. What separates them is that the correct path never
// makes that write and never runs a second clear().
func TestSessSupersededByAnotherMarkerAdoptsIt(t *testing.T) {
	testQuietExpirySweeps(t)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "redrive-marker"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	testMQTTDisconnectEx(t, c, nil, true)
	c.Close()

	// The rival write is a CLAIMED TEARDOWN, not a plain record: another broker got as
	// far as claiming this session's record and then stopped. Staged by hand rather than
	// through the rival-record helper because the marker's PWID is the assertion — and
	// buildPersistedSessionLocked deliberately mints no lineage, so it has to be taken
	// from the identity the session actually holds, exactly as claimTeardown takes it.
	var (
		mmu                   sync.Mutex
		markerWID, markerPWID string
	)
	tr := testTraceReDriveStaged(t, func(sess *mqttSession) {
		sess.mu.Lock()
		ps := sess.buildPersistedSessionLocked()
		seq, idHash, held := sess.seq, sess.idHash, sess.wid
		sess.mu.Unlock()
		ps.WID, ps.PWID = mqttNewRecordWID(), held
		ps.Origin = "other-frontend"
		ps.Teardown = &mqttSessTeardown{
			Origin:    "other-frontend",
			Nonce:     nuid.Next(),
			StartedAt: time.Now().Unix(),
		}
		if _, err := mqttStoreSessionRecordCAS(&asm.jsa, asm.domainTk, &ps, idHash, seq); err != nil {
			t.Errorf("landing the teardown marker: %v", err)
			return
		}
		// The F-105 shape of a rival's claimed teardown: the marker stands on the record AND the
		// authority the rival was discarding is CLOSED. Closing it here is what lets the re-drive
		// adopt the standing teardown and create fresh over a CLOSED generation, rather than meet
		// a serving authority whose record now carries somebody else's marker.
		testClosePredecessorAuthority(t, asm, cid)
		mmu.Lock()
		markerWID, markerPWID = ps.WID, ps.PWID
		mmu.Unlock()
	})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	mmu.Lock()
	stagedWID, stagedPWID := markerWID, markerPWID
	mmu.Unlock()
	if stagedWID == _EMPTY_ || stagedPWID == _EMPTY_ {
		t.Fatalf("the teardown marker never landed with a lineage: wid=%q pwid=%q",
			stagedWID, stagedPWID)
	}
	// EXACTLY one discard. The marker is adopted, and an adoption is not a discard: a
	// second clear() here would be tearing down a session another broker already retired.
	if got := tr.discards(); got != 1 {
		t.Fatalf("adopting a marker must not run a second discard, got %d", got)
	}
	// No stale base-0 write before the authoritative read that found the marker.
	bases := tr.saveBases()
	if len(bases) == 0 {
		t.Fatal("the CONNECT made no record write at all")
	}
	for i, b := range bases[:len(bases)-1] {
		if b == 0 {
			t.Fatalf("a base-0 store ran before the marker was read (attempt %d of %v)", i, bases)
		}
	}

	ps, _ := testLoadSessRecordByHash(t, asm, getHash(cid))
	if ps == nil {
		t.Fatal("the accepted CONNECT left no record")
	}
	// R2's named exception is GONE with the rest of the create-parent sources: the
	// successor of an adopted marker descends from nothing, neither from the record that
	// teardown claimed nor from the marker itself. Both identities are asserted non-empty
	// above, so this arm can genuinely fail — the inputs a wrong implementation would
	// reach for are present.
	if ps.PWID != _EMPTY_ {
		t.Fatalf("the create after an adopted teardown descends from nothing: parent %q "+
			"(the claimed record was %q, the marker %q)", ps.PWID, stagedPWID, stagedWID)
	}
	if ps.WID == _EMPTY_ {
		t.Fatal("a parentless create still mints an identity of its OWN")
	}
}

// TestSessSupersededByAnAbsentSubjectCreatesFresh is test 3(e).
//
// claimTeardown reports superseded when the record is GONE as well as when it moved —
// what another broker's own completed clean-start release leaves behind. There is then
// no winner at all, so the re-drive's authoritative read finds nothing and the CONNECT
// makes a genuine FIRST create with an empty parent.
//
// This is the arm where a superseded-as-success defect differs DURABLY rather than only
// in path: with the subject empty the base-0 create lands either way. The LINEAGE no
// longer discriminates the two — every create is parentless since the write-lineage
// audit, so an object clear() reset and an authoritative read of an empty subject stamp
// the same thing — which is why the discard COUNT below is the discriminator, and why the
// retired-WID assertion is kept as a guard against an inheritance being reintroduced
// rather than as the proof.
func TestSessSupersededByAnAbsentSubjectCreatesFresh(t *testing.T) {
	testQuietExpirySweeps(t)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "redrive-absent"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	testMQTTDisconnectEx(t, c, nil, true)
	c.Close()

	var (
		rmu     sync.Mutex
		retired string
		removed bool
	)
	tr := testTraceReDriveStaged(t, func(sess *mqttSession) {
		sess.mu.Lock()
		seq, wid := sess.seq, sess.wid
		sess.mu.Unlock()
		if seq == 0 {
			t.Error("the discard must hold a record sequence to claim")
			return
		}
		// The F-105 shape of "another broker's completed clean-start release": the authority is
		// CLOSED and the record is gone. Closing the authority is what makes the re-drive create
		// fresh over a CLOSED generation rather than meet a serving one with no record.
		testClosePredecessorAuthority(t, asm, cid)
		if derr := asm.jsa.deleteMsg(mqttSessStreamName, seq, true); derr != nil {
			t.Errorf("removing the record the discard is about to claim: %v", derr)
			return
		}
		rmu.Lock()
		retired, removed = wid, true
		rmu.Unlock()
	})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	rmu.Lock()
	retiredWID, staged := retired, removed
	rmu.Unlock()
	if !staged {
		t.Fatal("the record was never removed, so the claim was not superseded by an absent subject")
	}
	if retiredWID == _EMPTY_ {
		t.Fatal("the retired record carried no identity, so the assertion below could not fail")
	}
	// No winner means nothing to discard a second time.
	if got := tr.discards(); got != 1 {
		t.Fatalf("an absent subject leaves nothing to discard again, got %d discard(s)", got)
	}

	ps, _ := testLoadSessRecordByHash(t, asm, getHash(cid))
	if ps == nil {
		t.Fatal("the accepted CONNECT left no record")
	}
	// As above: what must not be inherited is the RETIRED record's identity, asserted
	// non-empty above so this arm can genuinely fail. A genuine first-create descends
	// from nothing at all.
	if ps.PWID == retiredWID {
		t.Fatalf("a create after an authoritative read of an ABSENT subject must not inherit the "+
			"RETIRED record's identity: parent %q", ps.PWID)
	}
	if ps.PWID != _EMPTY_ {
		t.Fatalf("a create descends from nothing: parent %q", ps.PWID)
	}
}

// TestOverdueMovedWinnerReDrivesInsideTheRestore is test 3(g).
//
// An overdue record never reaches processConnect's clean-start decision: the restore
// intercepts it and discards it there, which is transition T0. Two things must hold that
// the outer transitions do not cover — the base-0 create is PARENTLESS, inheriting
// nothing from the record that discard retired, and the re-drive budget is the CONNECT's,
// not one the restore mints per call.
func TestOverdueMovedWinnerReDrivesInsideTheRestore(t *testing.T) {
	testQuietExpirySweeps(t)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	t.Run("the base-0 create after an overdue discard is parentless", func(t *testing.T) {
		const cid = "overdue-lineage"
		asm := testStageOverdueRecord(t, s, o, cid)

		// The staged record's identity, read BEFORE the CONNECT discards it: it is the
		// positive control for the parentless assertion below, which could not fail if
		// there had been no identity to inherit in the first place.
		overdue, _ := testLoadSessRecordByHash(t, asm, getHash(cid))
		if overdue == nil || overdue.WID == _EMPTY_ {
			t.Fatal("the staged overdue record carried no identity, so nothing could be mis-inherited")
		}

		tr := testTraceReDriveStaged(t, nil)
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		// Overdue means NOT resumed, whatever the client asked for. Spec5 [3.1.2.11.2].
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

		if got := tr.discards(); got != 1 {
			t.Fatalf("an overdue record is discarded exactly once, inside the restore, got %d", got)
		}
		ps, _ := testLoadSessRecordByHash(t, asm, getHash(cid))
		if ps == nil {
			t.Fatal("the accepted CONNECT left no record")
		}
		if ps.PWID != _EMPTY_ {
			t.Fatalf("the create after an overdue discard descends from nothing: parent %q "+
				"(the discarded record was %q)", ps.PWID, overdue.WID)
		}
		if ps.WID == _EMPTY_ || ps.WID == overdue.WID {
			t.Fatalf("a parentless create still mints an identity of its OWN: %q", ps.WID)
		}
	})

	// Superseding EVERY discard makes the restore's own loop run until the cycle cap stops
	// it. What this pins is that the loop is BOUNDED and that its failure exit is clean —
	// an unadmitted loop would not terminate at all.
	//
	// What it does NOT pin, stated because the obvious mutation was tried and SURVIVED:
	// that the budget is the CONNECT's rather than one the restore mints per call. The
	// whole exhaustion happens inside a single call, so a per-call budget exhausts exactly
	// as a shared one does and the two are indistinguishable here. The DEADLINE half of
	// the same threading is pinned end to end by the admission tests; the cycle counter's
	// is not pinned by any mutation.
	t.Run("the restore's own re-drive loop is bounded", func(t *testing.T) {
		const cid = "overdue-budget"
		asm := testStageOverdueRecord(t, s, o, cid)

		// The rival must stay OVERDUE. buildPersistedSessionLocked snapshots the session
		// as clear() left it — disconnected-at zeroed — so an unshaped rival would send the
		// next pass down the ordinary clean-start decision and the restore's own loop would
		// never be exercised again.
		tr := testTraceReDrive(t, asm, func(ps *mqttPersistedSession) {
			ps.ExpiryInterval, ps.DisconnectedAt = 1, time.Now().Unix()-3600
		})
		tr.mu.Lock()
		tr.everyDiscard = true
		tr.mu.Unlock()

		conn, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: false},
			o.MQTT.Host, o.MQTT.Port, 0)
		if conn != nil {
			defer conn.Close()
		}
		if cerr == nil {
			t.Fatal("a restore whose discard is superseded on every attempt must exhaust the " +
				"CONNECT's cycle budget and refuse, not re-drive forever")
		}
		if got := tr.discards(); got <= 1 {
			t.Fatalf("the restore must re-drive its discard, got %d discard(s)", got)
		}
		if got, bound := testReDriveResident(asm, cid); got != nil {
			t.Fatalf("the exhausted restore left a session resident (bound=%v)", bound)
		}
	})
}

// testStageOverdueRecord leaves a persistent session record for clientID whose expiry
// deadline has already passed, which is what makes the restore discard it rather than
// hand it to the clean-start decision.
func testStageOverdueRecord(t *testing.T, s *Server, o *Options, clientID string) *mqttAccountSessionManager {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	testMQTTDisconnectEx(t, c, nil, true)
	c.Close()

	// Backdate the disconnect past the interval. Written through the same CAS a real
	// writer uses, so the record stays well formed.
	ps, seq := testLoadSessRecordByHash(t, asm, getHash(clientID))
	if ps == nil {
		t.Fatal("the staged session must have a record")
	}
	ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
	ps.ExpiryInterval, ps.DisconnectedAt = 1, time.Now().Unix()-3600
	if _, err := mqttStoreSessionRecordCAS(&asm.jsa, asm.domainTk, ps, getHash(clientID), seq); err != nil {
		t.Fatalf("backdating the record: %v", err)
	}
	return asm
}

// ---------------------------------------------------------------------------
// Test 5, the two terminal clear() callers that need a live connection
// ---------------------------------------------------------------------------

// TestSupersededDiscardIsTerminalForTheConnectionCallers covers the two terminal callers
// the record fixture cannot reach: the ordinary close's doClean discard, and the deferred
// discard a resolution discharges.
//
// The DISCRIMINATOR is the absence of warning-level logging, because that is the only
// thing the typed branch changes. Losing an arbitration is a normal outcome; logged as a
// teardown failure it sends an operator hunting something that never went wrong. "The
// record is untouched" and "nothing was re-parked" are NOT discriminators here — both
// callers re-park only on errMQTTClearRefusedUnresolved, so both would hold anyway.
func TestSupersededDiscardIsTerminalForTheConnectionCallers(t *testing.T) {
	t.Run("the close path's clean-session discard", func(t *testing.T) {
		testQuietExpirySweeps(t)

		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		const cid = "terminal-close"
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		asm := testMQTTGetAccountSessionManager(t, s, cid)

		// A rival takes the subject in the window the close's discard is about to claim at.
		tr := testTraceReDrive(t, asm)
		logs := testCaptureServerLogs(t, s)

		c.Close()

		// The staging must actually have happened, or the silence below proves nothing.
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if tr.discards() == 0 {
				return fmt.Errorf("the close has not run its discard yet")
			}
			if tr.winner() == _EMPTY_ {
				return fmt.Errorf("the rival record has not landed yet")
			}
			if sess, _ := testReDriveResident(asm, cid); sess != nil {
				return fmt.Errorf("the close has not finished with the session yet")
			}
			return nil
		})
		// Untyped, this arm reaches c.Errorf("%s", err.Error()), whose text is the
		// sentinel's own wrapper.
		if msg, found := logs.saw(fmt.Sprintf("discarding session %q", cid)); found {
			t.Fatalf("a superseded discard is a normal outcome and must not be logged as a "+
				"failure, got %q", msg)
		}
	})

	t.Run("the deferred discard a resolution discharges", func(t *testing.T) {
		testQuietExpirySweeps(t)

		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		const cid = "terminal-discharge"
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		asm, sess := testResidentSession(t, s, cid)

		// Park the discard, then make the DISCHARGE's own discard meet an absent subject.
		var (
			dmu     sync.Mutex
			removed bool
		)
		testParkDiscardOnAnAmbiguousWrite(t, sess, func(cur *mqttSession) {
			dmu.Lock()
			defer dmu.Unlock()
			if removed {
				return
			}
			cur.mu.Lock()
			seq := cur.seq
			cur.mu.Unlock()
			if seq == 0 {
				return
			}
			// F-105 shape of a rival's completed release: CLOSED authority and an absent record.
			testClosePredecessorAuthority(t, asm, cid)
			if derr := asm.jsa.deleteMsg(mqttSessStreamName, seq, true); derr != nil {
				t.Errorf("removing the record the discharge is about to claim: %v", derr)
				return
			}
			removed = true
		})

		c.Close()
		testAwaitParkedDiscard(t, asm, sess)

		logs := testCaptureServerLogs(t, s)
		c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

		dmu.Lock()
		staged := removed
		dmu.Unlock()
		if !staged {
			t.Fatal("the discharged discard never met an absent record: it was not superseded")
		}
		// Untyped, this arm reaches s.Warnf("MQTT: deferred discard of session %q failed").
		if msg, found := logs.saw("deferred discard of session"); found {
			t.Fatalf("a superseded deferred discard is a normal outcome and must not be "+
				"logged as a failure, got %q", msg)
		}
	})
}

// ---------------------------------------------------------------------------
// Test 9(b) end to end: the two budgets stay independent across transitions
// ---------------------------------------------------------------------------

// TestReDriveBudgetsAreIndependentThroughAConnect is 9(b) driven through a real CONNECT
// rather than by calling the admission methods.
//
// The counters have to survive T0/T1/T2 re-entry, and a direct-call test cannot show
// that: it never crosses a transition. A mixed sequence is what discriminates a shared
// counter from two, and only because the caps differ — spend cycle budget first with
// 10071s, then prove the full create-retry budget is still there.
//
// Asserted on COUNTS, never on absolute sequences: the store-result seam rewrites a
// write's outcome AFTER it has been issued, so every "rejected" save has in fact landed
// and the subject moves underneath the CONNECT. That is what makes it the right seam for
// this — a rejection whose write landed is exactly the ambiguity the resolver exists for
// — and the wrong one to assert bases against.
func TestReDriveBudgetsAreIndependentThroughAConnect(t *testing.T) {
	testQuietExpirySweeps(t)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c0, r0 := testMQTTConnect(t, &mqttConnInfo{clientID: "budget-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r0, mqttConnAckRCConnectionAccepted, false)
	defer c0.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "budget-probe")

	const cid = "budget-mixed"
	// One short of the cycle cap, then the WHOLE create-retry budget. Under two counters
	// that is admissible and the CONNECT is accepted; under one shared counter the cycles
	// are already spent and the first or second retry is refused.
	cycles, retries := mqttRecordReDriveCycles-1, mqttRecordCreateRetries
	// F-105 cutover (Codex consult 10): the accepting CONNECT's save is the ownership metadata
	// commit — a three-member atomic batch — which returns BEFORE the removed two-member store
	// result hook, so the old hook fired zero times and the test proved nothing. The reachable
	// interception is the ISSUING phase of the metadata batch (fired before the send), matched by
	// this client's session-record subject: it reproduces a CERTAIN rejection (nothing lands),
	// which the accept loop classifies exactly as a real store outcome — 10071 spends a re-drive
	// CYCLE (a peer's record won), 10164 spends a create-RETRY (contention, the executor re-sends
	// the SAME frozen attempt). This fixture is SYNTHETIC contention: it proves the two counters
	// are INDEPENDENT (a run inside both budgets is accepted; one shared counter gives up early),
	// not an absolute ownership deadline and not real clustered contention.
	recSubj := mqttSessSubject(asm.domainTk, getHash(cid))
	var (
		mu   sync.Mutex
		n    int
		seen []string
	)
	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		if obs.Phase != mqttAcceptStoreIssuing || obs.RecSubj != recSubj {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case n < cycles:
			n++
			seen = append(seen, "10071")
			return ApiErrors[JSStreamWrongLastSequenceErrF]
		case n < cycles+retries:
			n++
			seen = append(seen, "10164")
			return ApiErrors[JSStreamWrongLastSequenceConstantErr]
		}
		seen = append(seen, "ok")
		return nil
	})
	defer restore()

	conn, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: true},
		o.MQTT.Host, o.MQTT.Port, 0)
	if conn != nil {
		defer conn.Close()
	}
	restore()

	mu.Lock()
	fired, order := n, append([]string(nil), seen...)
	mu.Unlock()
	if fired != cycles+retries {
		t.Fatalf("the staged sequence was not consumed: %d of %d interceptions fired (%v). "+
			"Under a SHARED counter the CONNECT gives up early, which is the defect this test "+
			"exists to catch — but it also fires when the staging itself is wrong",
			fired, cycles+retries, order)
	}
	if cerr != nil {
		t.Fatalf("a CONNECT that stayed inside BOTH budgets must be accepted: %v (sequence %v)", cerr, order)
	}
	// And it really did end up owning the session, rather than being accepted having
	// abandoned it.
	if sess, bound := testReDriveResident(asm, cid); sess == nil || !bound {
		t.Fatalf("the accepted CONNECT must leave its session resident and bound: sess=%v bound=%v",
			sess != nil, bound)
	}
}
