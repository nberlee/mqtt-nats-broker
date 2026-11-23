//go:build mqtt_fulltests

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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// An acceptance against a shared shard another client's write moved (issue #186): a creation's
// lifecycle registry shard, a Clean Start handoff's directory shard. The frozen batch can never
// land, so the CONNECT recomposes from fresh reads instead of re-sending it until an allowance runs
// out, or taking a record-moved re-drive for a record that did not move.

// testConnAckCode reads the CONNACK's return code, or reports that none arrived.
func testConnAckCode(t *testing.T, r *mqttReader) (byte, bool) {
	t.Helper()
	b, pl, ok := testMQTTReadPacketReady(r, 15*time.Second)
	if !ok || b&mqttPacketMask != mqttPacketConnectAck || pl != 2 {
		return 0, false
	}
	if _, err := r.readByte("connack flags"); err != nil {
		return 0, false
	}
	rc, err := r.readByte("connack return code")
	if err != nil {
		return 0, false
	}
	return rc, true
}

// testRestoreAt re-writes subject's last record at the sequence it stands at, moving it.
func testRestoreAt(jsa *mqttJSA, subject string) error {
	smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil || smsg == nil {
		return fmt.Errorf("reading %q to move it: %v", subject, err)
	}
	msg, hdr := mqttFrameBatchWrite(smsg.Sequence, smsg.Data, _EMPTY_, 0, false)
	if _, err := jsa.storeMsg(subject, hdr, msg); err != nil {
		return fmt.Errorf("moving %q: %w", subject, err)
	}
	return nil
}

func testIsCreationOf(tr *mqttAuthorityTransition, id string) bool {
	return tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAcceptCreate && tr.Attempt.ID == id
}

func testIsCleanStartHandoffOf(tr *mqttAuthorityTransition, id string) bool {
	return tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAcceptHandoff && tr.Attempt.ID == id && tr.Closure != nil
}

// testCountReDriveClock counts the CONNECT re-drive budget's clock reads. After a CONNECT has begun,
// only a re-drive cycle's or a create retry's admission reads it.
func testCountReDriveClock(t *testing.T) *atomic.Int32 {
	t.Helper()
	var reads atomic.Int32
	now := func() time.Time { reads.Add(1); return time.Now() }
	mqttTestReDriveClockV.Store(&now)
	t.Cleanup(func() { mqttTestReDriveClockV.Store(nil) })
	return &reads
}

// testPersistentSession leaves a persistent session of id standing, with no connection.
func testPersistentSession(t *testing.T, o *Options, id string) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()
}

// A peer's creation in the same registry shard lands between this creation's composition and its
// send. The store refuses the stale registry member (10071); the CONNECT recomposes and is
// accepted, with no record-moved re-drive. Mutations: shared-member movement never detected, or
// decided after the record arm (the record-moved re-drive is taken).
func TestCreationRecomposesOverAMovedRegistryShard(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "create-registry-moved"
	shard := mqttLifecycleRegistryShardFor(getHash(id))
	testSeedDirShard(t, s, asm, o, testClientInDirShard(t, "create-registry-seed", shard))

	reads := testCountReDriveClock(t)
	var creations atomic.Int32
	var after atomic.Int32
	pre := func(tr *mqttAuthorityTransition) {
		if !testIsCreationOf(tr, id) {
			return
		}
		if creations.Add(1) == 1 {
			if err := testRestoreAt(&asm.jsa, mqttLifecycleRegistrySubject(shard)); err != nil {
				t.Error(err)
			}
			after.Store(reads.Load())
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	defer asm.jsa.testTransitionPresend.Store(nil)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if rc, ok := testConnAckCode(t, r); !ok || rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the creation over a moved registry shard was answered %d (CONNACK %v), want accepted", rc, ok)
	}
	if n := creations.Load(); n != 2 {
		t.Fatalf("%d creation batches reached the store, want the frozen one and one recomposition", n)
	}
	if n := reads.Load() - after.Load(); n != 0 {
		t.Fatalf("the CONNECT admitted %d re-drive or create-retry step(s) after the registry moved, want none", n)
	}
}

// The first send meets 10164 after a peer's registry write already applied. Movement is
// observable, so the frozen batch is not re-sent and the create-retry allowance is not charged.
// Mutation: the 10164 arm without its shared check (a frozen re-send, charged to the allowance).
func TestAContendedCreationOverAMovedRegistryIsNotResent(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "create-registry-contended"
	shard := mqttLifecycleRegistryShardFor(getHash(id))
	testSeedDirShard(t, s, asm, o, testClientInDirShard(t, "create-contended-seed", shard))

	reads := testCountReDriveClock(t)
	var sends, after atomic.Int32
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if !testIsCreationOf(tr, id) {
			return send()
		}
		if sends.Add(1) == 1 {
			if err := testRestoreAt(&asm.jsa, mqttLifecycleRegistrySubject(shard)); err != nil {
				t.Error(err)
			}
			after.Store(reads.Load())
			return 0, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		}
		return send()
	})

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if rc, ok := testConnAckCode(t, r); !ok || rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the contended creation was answered %d (CONNACK %v), want accepted", rc, ok)
	}
	if n := sends.Load(); n != 2 {
		t.Fatalf("%d creation sends, want the contended one and one recomposition (no frozen re-send)", n)
	}
	if n := reads.Load() - after.Load(); n != 0 {
		t.Fatalf("the create-retry allowance was charged %d time(s) for a batch that could never land", n)
	}
}

// A recovery pass moves the directory shard a Clean Start handoff allocates in. The handoff
// recomposes, its predecessor's root at the moved header's next ordinal, and the CONNECT is
// accepted with no record-moved re-drive. Mutation: shared-member movement never detected (the
// store's refusal of the stale shard is taken for a moved record and the CONNECT re-drives).
func TestCleanStartHandoffRecomposesOverAMovedDirectoryShard(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "handoff-shard-moved"
	shard := mqttDirShardFor(getHash(id))
	testSeedDirShard(t, s, asm, o, testClientInDirShard(t, "handoff-shard-seed", shard))
	testPersistentSession(t, o, id)

	reads := testCountReDriveClock(t)
	var handoffs, after atomic.Int32
	var gotOrd, wantOrd atomic.Uint64
	pre := func(tr *mqttAuthorityTransition) {
		if !testIsCleanStartHandoffOf(tr, id) {
			return
		}
		switch handoffs.Add(1) {
		case 1:
			if _, err := testMoveDirShard(&asm.jsa, shard); err != nil {
				t.Error(err)
			}
			after.Store(reads.Load())
		case 2:
			hdr, _, err := asm.jsa.loadDirShardAt(shard)
			if err != nil || hdr == nil {
				t.Errorf("reading shard %d at the recomposed handoff: %v", shard, err)
				return
			}
			wantOrd.Store(hdr.Hi + 1)
			gotOrd.Store(tr.Closure.RootOrd)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	defer asm.jsa.testTransitionPresend.Store(nil)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if rc, ok := testConnAckCode(t, r); !ok || rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the Clean Start over a moved directory shard was answered %d (CONNACK %v), want accepted", rc, ok)
	}
	if n := handoffs.Load(); n != 2 {
		t.Fatalf("%d handoff batches reached the store, want the frozen one and one recomposition", n)
	}
	if n := reads.Load() - after.Load(); n != 0 {
		t.Fatalf("the CONNECT admitted %d re-drive or create-retry step(s) after the shard moved, want none", n)
	}
	if got, want := gotOrd.Load(), wantOrd.Load(); got == 0 || got != want {
		t.Fatalf("the recomposed handoff allocates root %d, want the moved header's next ordinal %d", got, want)
	}
	if hdr, _, err := asm.jsa.loadDirShardAt(shard); err != nil || hdr == nil || hdr.Hi < gotOrd.Load() {
		t.Fatalf("shard %d's header after the handoff: %+v (%v)", shard, hdr, err)
	}
}

// Movement proves the future, not the past: the frozen creation commits after the resolver's
// authority read and before its registry read, which then reads moved — by the creation itself.
// The resolver reads the authority again and adopts it, in the post-authority arm and in the 10164
// arm alike. Mutation: the authority re-read after the shared read skipped (a recomposition is
// asked for over this CONNECT's own landed creation).
func TestAMovedRegistryResolvesAnEarlierCreationCommitBeforeRecomposing(t *testing.T) {
	e, done := testNewEnv(t, "creation-earlier-commit")
	defer done()
	allow := func() error { return nil }
	for _, tc := range []struct {
		name string
		raw  error
	}{
		{"post-authority arm", errors.New("test: the acknowledgement was lost")},
		{"contention arm", ApiErrors[JSStreamWrongLastSequenceConstantErr]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "earlier-" + map[bool]string{true: "lost", false: "contended"}[tc.name == "post-authority arm"]
			tr := testComposeCreation(t, e, id, "txn-"+id)
			regSubj := tr.Registry.subj
			var armed atomic.Bool
			armed.Store(true)
			var commitErr atomic.Pointer[error]
			load := func(_, subject string) error {
				if subject == regSubj && armed.CompareAndSwap(true, false) {
					_, err := e.asm.jsa.commitAuthorityTransition(tr)
					commitErr.Store(&err)
				}
				return nil
			}
			mqttTestLoadLastMsgHookV.Store(&load)
			defer mqttTestLoadLastMsgHookV.Store(nil)
			sess, _, err := e.asm.createOrRestoreSession(id, e.asm.server().getOpts())
			if err != nil {
				t.Fatalf("a session object for %q: %v", id, err)
			}
			landed, retry, rerr := sess.resolveAcceptance(&e.asm.jsa,
				mqttSessSaveDetail{acceptShape: tr, rawErr: tc.raw}, tr.Attempt.Intent, allow, allow)
			if p := commitErr.Load(); p == nil || *p != nil {
				t.Fatalf("fixture: the frozen creation was not committed before the registry read (%v)", p)
			}
			if !landed || retry != mqttAcceptNoRetry || rerr != nil {
				t.Fatalf("a creation that landed before its registry read resolved landed=%v retry=%v err=%v, want landed",
					landed, retry, rerr)
			}
		})
	}
}

// A refusal that moved nothing re-sends the frozen creation after the contention backoff. Mutation:
// the backoff removed.
func TestAContendedFrozenCreationWaitsTheContentionBackoff(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	waits := testNoContentionSleep(t, asm)
	const id = "create-contended-backoff"

	var sends atomic.Int32
	var gaps [3]atomic.Int32
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if !testIsCreationOf(tr, id) {
			return send()
		}
		n := sends.Add(1)
		if n <= 3 {
			gaps[n-1].Store(waits.Load())
		}
		if n <= 2 {
			return 0, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		}
		return send()
	})
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if rc, ok := testConnAckCode(t, r); !ok || rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the contended creation was answered %d (CONNACK %v), want accepted", rc, ok)
	}
	if n := sends.Load(); n != 3 {
		t.Fatalf("%d creation sends, want two contended and one that lands", n)
	}
	if gaps[1].Load() <= gaps[0].Load() || gaps[2].Load() <= gaps[1].Load() {
		t.Fatalf("waits before the sends were %d, %d, %d: every frozen re-send must follow a backoff",
			gaps[0].Load(), gaps[1].Load(), gaps[2].Load())
	}
}

// The CONNECT's shared-shard allowance: while it stands, recompositions forced by a moved shard
// spend no attempt, so a Clean Start outlasts more movements than its budget; once spent they
// count and the CONNECT is refused within its budget; and a re-drive does not replenish it.
// Mutations: a shared recomposition charged to the budget (standing refused); the allowance never
// expiring (spent accepted); the allowance re-created per acceptance drive (the re-drive variant
// accepted).
func TestSharedShardMovementOutlastsTheAcceptanceBudgetUntilItsAllowanceIsSpent(t *testing.T) {
	moves := int32(3 * mqttForegroundAcquireAttempts)
	setup := func(t *testing.T, id string) (*Options, *mqttAccountSessionManager, uint64) {
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		t.Cleanup(func() { testMQTTShutdownServer(s) })
		asm := testMQTTGetGlobalASM(t, s)
		testNoContentionSleep(t, asm)
		shard := mqttDirShardFor(getHash(id))
		testSeedDirShard(t, s, asm, o, testClientInDirShard(t, id+"-seed", shard))
		testPersistentSession(t, o, id)
		return o, asm, shard
	}
	connect := func(t *testing.T, o *Options, id string) bool {
		// A refused CONNECT may close with no CONNACK at all, which counts as not accepted.
		c, r, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 0)
		if err != nil {
			return false
		}
		defer c.Close()
		rc, ok := testConnAckCode(t, r)
		return ok && rc == mqttConnAckRCConnectionAccepted
	}
	moving := func(t *testing.T, asm *mqttAccountSessionManager, id string, shard uint64, n *atomic.Int32) {
		pre := func(tr *mqttAuthorityTransition) {
			if testIsCleanStartHandoffOf(tr, id) && n.Add(1) <= moves {
				if _, err := testMoveDirShard(&asm.jsa, shard); err != nil {
					t.Error(err)
				}
			}
		}
		asm.jsa.testTransitionPresend.Store(&pre)
		t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
	}

	t.Run("allowance standing", func(t *testing.T) {
		const id = "accept-allowance-standing"
		o, asm, shard := setup(t, id)
		var n atomic.Int32
		moving(t, asm, id, shard, &n)
		if !connect(t, o, id) || n.Load() != moves+1 {
			t.Fatalf("after %d handoff batch(es) the CONNECT was not accepted, want accepted after %d movements", n.Load(), moves)
		}
	})
	t.Run("allowance spent", func(t *testing.T) {
		mqttTestSharedShardAllowanceV.Set(time.Nanosecond)
		t.Cleanup(func() { mqttTestSharedShardAllowanceV.Set(0) })
		const id = "accept-allowance-spent"
		o, asm, shard := setup(t, id)
		var n atomic.Int32
		moving(t, asm, id, shard, &n)
		if connect(t, o, id) || n.Load() >= moves {
			t.Fatalf("after %d handoff batch(es) the CONNECT was accepted, want refused within its budget", n.Load())
		}
	})
	t.Run("a re-drive does not replenish it", func(t *testing.T) {
		const id = "accept-allowance-redrive"
		o, asm, shard := setup(t, id)
		var offset atomic.Int64
		clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
		mqttTestSharedShardClockV.Set(clock)
		t.Cleanup(func() { mqttTestSharedShardClockV.Set(nil) })
		reads := testCountReDriveClock(t)
		var n, atRecordMove atomic.Int32
		recSubj := mqttSessSubject(asm.domainTk, getHash(id))
		pre := func(tr *mqttAuthorityTransition) {
			if !testIsCleanStartHandoffOf(tr, id) {
				return
			}
			switch k := n.Add(1); {
			case k == 1:
				// Starts the allowance, and spends no attempt.
				if _, err := testMoveDirShard(&asm.jsa, shard); err != nil {
					t.Error(err)
				}
			case k == 2:
				// The allowance is spent; an independent record write sends the CONNECT round its
				// re-drive.
				offset.Store(int64(time.Hour))
				if err := testRestoreAt(&asm.jsa, recSubj); err != nil {
					t.Error(err)
				}
				atRecordMove.Store(reads.Load())
			case k <= moves:
				if _, err := testMoveDirShard(&asm.jsa, shard); err != nil {
					t.Error(err)
				}
			}
		}
		asm.jsa.testTransitionPresend.Store(&pre)
		t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
		accepted := connect(t, o, id)
		if reads.Load() == atRecordMove.Load() {
			t.Fatal("fixture: the record write did not send the CONNECT round a re-drive")
		}
		if accepted || n.Load() >= moves {
			t.Fatalf("after a re-drive and %d handoff batch(es) the CONNECT was accepted, want refused: the re-drive must not replenish the allowance", n.Load())
		}
	})
}
