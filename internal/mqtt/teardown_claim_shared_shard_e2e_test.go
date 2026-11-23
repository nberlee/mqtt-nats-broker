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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A teardown claim against a directory shard another client's write moved (issue #186): the frozen
// batch can never land, so the claim recomposes from fresh reads instead of re-sending it until its
// budget runs out and stranding the session.

// testClientInDirShard returns a client ID beginning with prefix that hashes to directory shard
// shard (and so to the same lifecycle registry shard).
func testClientInDirShard(t *testing.T, prefix string, shard uint64) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		if mqttDirShardFor(getHash(id)) == shard {
			return id
		}
	}
	t.Fatalf("no client ID with prefix %q hashes to shard %d", prefix, shard)
	return _EMPTY_
}

// testSeedDirShard completes one clean close of client id, so its directory shard has a header.
func testSeedDirShard(t *testing.T, s *Server, asm *mqttAccountSessionManager, o *Options, id string) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTWaitForRecordAbsent(t, asm, getHash(id))
	shard := mqttDirShardFor(getHash(id))
	if hdr, _, err := asm.jsa.loadDirShardAt(shard); err != nil || hdr == nil {
		t.Fatalf("fixture: shard %d has no header after a completed closing (%v)", shard, err)
	}
}

// testMoveDirShard re-writes shard's header at the sequence it stands at, as a recovery pass
// recording its traversal position does, and returns the header.
func testMoveDirShard(jsa *mqttJSA, shard uint64) (*mqttDirShard, error) {
	hdr, seq, err := jsa.loadDirShardAt(shard)
	if err != nil || hdr == nil {
		return nil, fmt.Errorf("reading shard %d to move it: %v", shard, err)
	}
	if _, err := jsa.persistShard(hdr, seq); err != nil {
		return nil, fmt.Errorf("moving shard %d: %w", shard, err)
	}
	return hdr, nil
}

// testIsClaimOf reports whether tr is a teardown claim of client id.
func testIsClaimOf(tr *mqttAuthorityTransition, id string) bool {
	return tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionTeardownClaim && tr.Attempt.ID == id
}

// testUnresolvedClaimLog captures the clean close's unresolved-claim line for client id.
func testUnresolvedClaimLog(s *Server) chan string {
	l := &captureErrorLogger{errCh: make(chan string, 256)}
	s.SetLogger(l, false, false)
	return l.errCh
}

func testSawUnresolvedClaim(ch chan string, id string, within time.Duration) bool {
	deadline := time.After(within)
	for {
		select {
		case line := <-ch:
			if strings.Contains(line, "could not establish its teardown claim") && strings.Contains(line, id) {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// testNoContentionSleep replaces the authority-contention wait with a counter.
func testNoContentionSleep(t *testing.T, asm *mqttAccountSessionManager) *atomic.Int32 {
	t.Helper()
	var waits atomic.Int32
	asm.mu.Lock()
	asm.testAuthorityContentionWait = func(time.Duration) { waits.Add(1) }
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testAuthorityContentionWait = nil
		asm.mu.Unlock()
	})
	return &waits
}

// A recovery pass moves the closing client's directory shard after the claim composed. The frozen
// batch can never land, so the claim recomposes against the same authority and its root takes the
// moved header's next ordinal. Mutations: shared-member movement never detected, or the claim
// re-sending the frozen batch whenever its authority stands (the claim ends Indeterminate after
// its budget, the record is never released).
func TestTeardownClaimRecomposesOverAMovedDirectoryShard(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "claim-shard-moved"
	shard := mqttDirShardFor(getHash(id))
	testSeedDirShard(t, s, asm, o, testClientInDirShard(t, "claim-shard-seed", shard))

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	gen := testSessionGenOf(t, s, id)

	var claims atomic.Int32
	var gotOrd, wantOrd atomic.Uint64
	txns := make(chan string, 8)
	pre := func(tr *mqttAuthorityTransition) {
		if !testIsClaimOf(tr, id) {
			return
		}
		n := claims.Add(1)
		txns <- tr.Attempt.Txn
		switch n {
		case 1:
			if _, err := testMoveDirShard(&asm.jsa, shard); err != nil {
				t.Error(err)
			}
		case 2:
			hdr, _, err := asm.jsa.loadDirShardAt(shard)
			if err != nil || hdr == nil {
				t.Errorf("reading shard %d at the recomposed claim: %v", shard, err)
				return
			}
			wantOrd.Store(hdr.Hi + 1)
			gotOrd.Store(tr.Closure.RootOrd)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	defer asm.jsa.testTransitionPresend.Store(nil)

	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTWaitForRecordAbsent(t, asm, getHash(id))

	if n := claims.Load(); n != 2 {
		t.Fatalf("%d claim batches reached the store, want the frozen one and one recomposition", n)
	}
	if first, second := <-txns, <-txns; first == second {
		t.Fatalf("the recomposition reused transition %q", first)
	}
	if got, want := gotOrd.Load(), wantOrd.Load(); got == 0 || got != want {
		t.Fatalf("the recomposed claim allocates root %d, want the moved header's next ordinal %d", got, want)
	}
	if entry, auth := testOutstandingEntry(t, asm, id, gen); entry == nil || auth.State != mqttAuthorityClosed {
		t.Fatalf("the recomposed claim did not end generation %q: entry %+v, state %d", gen, entry, auth.State)
	}
	if hdr, _, err := asm.jsa.loadDirShardAt(shard); err != nil || hdr == nil || hdr.Hi < gotOrd.Load() {
		t.Fatalf("shard %d's header after the claim: %+v (%v), want its high-water at least %d", shard, hdr, err, gotOrd.Load())
	}
}

// The issue's witness: two clients hash to one directory shard, and the second's closing completes
// between the first's composition and its send. The first recomposes and allocates the ordinal
// after the second's; both sessions are released. Mutations as above.
func TestTeardownClaimRecomposesWhenAnotherClientsClosingAllocatesInItsShard(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const first = "claim-shard-first"
	shard := mqttDirShardFor(getHash(first))
	second := testClientInDirShard(t, "claim-shard-second", shard)

	ca, ra := testMQTTConnect(t, &mqttConnInfo{clientID: first, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, ra, mqttConnAckRCConnectionAccepted, false)
	cb, rb := testMQTTConnect(t, &mqttConnInfo{clientID: second, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, rb, mqttConnAckRCConnectionAccepted, false)

	var firstClaims atomic.Int32
	var secondOrd, firstOrd atomic.Uint64
	pre := func(tr *mqttAuthorityTransition) {
		switch {
		case testIsClaimOf(tr, second):
			secondOrd.Store(tr.Closure.RootOrd)
		case testIsClaimOf(tr, first):
			switch firstClaims.Add(1) {
			case 1:
				// The second client's own closing, driven to completion before this send.
				testMQTTDisconnect(t, cb, nil)
				cb.Close()
				deadline := time.Now().Add(10 * time.Second)
				for {
					if _, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(second)); err != nil && !isErrorOtherThan(err, JSNoMessageFoundErr) {
						return
					}
					if time.Now().After(deadline) {
						t.Errorf("the second client's closing did not complete")
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			case 2:
				firstOrd.Store(tr.Closure.RootOrd)
			}
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	defer asm.jsa.testTransitionPresend.Store(nil)

	testMQTTDisconnect(t, ca, nil)
	ca.Close()
	testMQTTWaitForRecordAbsent(t, asm, getHash(first))
	testMQTTWaitForRecordAbsent(t, asm, getHash(second))

	if n := firstClaims.Load(); n != 2 {
		t.Fatalf("the first client sent %d claim batches, want the frozen one and one recomposition", n)
	}
	if a, b := firstOrd.Load(), secondOrd.Load(); b == 0 || a != b+1 {
		t.Fatalf("the first client's recomposed root is %d and the second's %d, want consecutive ordinals", a, b)
	}
	if hdr, _, err := asm.jsa.loadDirShardAt(shard); err != nil || hdr == nil || hdr.Hi < firstOrd.Load() {
		t.Fatalf("shard %d's header after both closings: %+v (%v)", shard, hdr, err)
	}
}

// Movement proves the future, not the past: the claim's first send commits, but only after its
// first record read reported no marker, and before its shard read. The shard reads moved — by the
// claim's own batch — so the claim reads the record again and ends Won on its own marker, with no
// recomposition. Mutation: the record re-read after the shard read skipped (the recomposition meets
// its own sealed ledger and the claim ends Indeterminate).
func TestAMovedShardResolvesAnEarlierClaimCommitBeforeRecomposing(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	logs := testUnresolvedClaimLog(s)
	const id = "claim-earlier-commit"
	shardSubj := mqttShardSubject(mqttDirShardFor(getHash(id)))

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	var captured atomic.Pointer[func() (uint64, error)]
	var armed atomic.Bool
	var later atomic.Int32
	var commitErr atomic.Pointer[error]
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if !testIsClaimOf(tr, id) {
			return send()
		}
		if captured.CompareAndSwap(nil, &send) {
			armed.Store(true)
			return 0, errors.New("test: the acknowledgement was lost")
		}
		later.Add(1)
		return send()
	})
	load := func(_, subject string) error {
		if subject == shardSubj && armed.CompareAndSwap(true, false) {
			_, err := (*captured.Load())()
			commitErr.Store(&err)
		}
		return nil
	}
	mqttTestLoadLastMsgHookV.Store(&load)
	defer mqttTestLoadLastMsgHookV.Store(nil)

	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTWaitForRecordAbsent(t, asm, getHash(id))

	if p := commitErr.Load(); p == nil || *p != nil {
		t.Fatalf("fixture: the frozen claim was not committed between the reads (%v)", p)
	}
	if n := later.Load(); n != 0 {
		t.Fatalf("%d further claim batch(es) were sent after the earlier commit, want none", n)
	}
	if testSawUnresolvedClaim(logs, id, 200*time.Millisecond) {
		t.Fatal("the claim that landed was reported unresolved")
	}
}

// A claim whose sends are refused for a reason that is not movement ends Indeterminate, and the
// clean close reports it as the stated residual: the record stands unmarked, the authority serving.
// Mutation: the indeterminate claim error returned unwrapped (no residual line).
func TestAnUnresolvedTeardownClaimIsReportedAsTheReconnectResidual(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	logs := testUnresolvedClaimLog(s)
	testNoContentionSleep(t, asm)
	const id = "claim-unresolved"

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if testIsClaimOf(tr, id) {
			return 0, errors.New("test: the store refused the claim")
		}
		return send()
	})
	testMQTTDisconnect(t, c, nil)
	c.Close()

	if !testSawUnresolvedClaim(logs, id, 10*time.Second) {
		t.Fatal("the unresolved claim was not reported as the reconnect residual")
	}
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(id))
	if err != nil || smsg == nil {
		t.Fatalf("the record of an unresolved claim reads %v, want it standing", err)
	}
	if al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch); al.status != mqttOutboundValid || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the authority of an unresolved claim reads %v, want it serving", al.status)
	}
}

// A refusal that moved nothing re-sends the frozen batch after the contention backoff, so a
// competing proposal can apply. Mutation: the backoff removed (re-sends back to back).
func TestAContendedFrozenClaimWaitsTheContentionBackoff(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	waits := testNoContentionSleep(t, asm)
	const id = "claim-contended-backoff"

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	var sends atomic.Int32
	var gaps [3]atomic.Int32
	var txn atomic.Pointer[string]
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if !testIsClaimOf(tr, id) {
			return send()
		}
		n := sends.Add(1)
		if n <= 3 {
			gaps[n-1].Store(waits.Load())
		}
		if txn.CompareAndSwap(nil, &tr.Attempt.Txn); *txn.Load() != tr.Attempt.Txn {
			t.Errorf("the contended claim was recomposed (%q -> %q), want the same frozen batch", *txn.Load(), tr.Attempt.Txn)
		}
		if n <= 2 {
			return 0, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		}
		return send()
	})
	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTWaitForRecordAbsent(t, asm, getHash(id))

	if n := sends.Load(); n != 3 {
		t.Fatalf("%d claim sends, want two contended and one that lands", n)
	}
	if gaps[1].Load() <= gaps[0].Load() || gaps[2].Load() <= gaps[1].Load() {
		t.Fatalf("waits before the sends were %d, %d, %d: every frozen re-send must follow a backoff",
			gaps[0].Load(), gaps[1].Load(), gaps[2].Load())
	}
}

// Shared-shard movement is another client's write, which the claim's derived budget does not count:
// while the allowance stands, a recomposition it forces spends no attempt, so the claim outlasts
// more movements than its budget. Once the allowance is spent, they count, and the claim ends
// within its budget. Mutations: a shared recomposition charged to the budget (the standing half is
// refused); the allowance never expiring (the spent half lands).
func TestSharedShardMovementOutlastsTheClaimBudgetUntilItsAllowanceIsSpent(t *testing.T) {
	moves := int32(3 * mqttForegroundAcquireAttempts)
	run := func(t *testing.T, id string) (claims int32, released bool, logs chan string) {
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		t.Cleanup(func() { testMQTTShutdownServer(s) })
		asm := testMQTTGetGlobalASM(t, s)
		logs = testUnresolvedClaimLog(s)
		testNoContentionSleep(t, asm)
		shard := mqttDirShardFor(getHash(id))
		testSeedDirShard(t, s, asm, o, testClientInDirShard(t, id+"-seed", shard))
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		var n atomic.Int32
		pre := func(tr *mqttAuthorityTransition) {
			if testIsClaimOf(tr, id) && n.Add(1) <= moves {
				if _, err := testMoveDirShard(&asm.jsa, shard); err != nil {
					t.Error(err)
				}
			}
		}
		asm.jsa.testTransitionPresend.Store(&pre)
		t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
		testMQTTDisconnect(t, c, nil)
		c.Close()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(id)); err != nil && !isErrorOtherThan(err, JSNoMessageFoundErr) {
				return n.Load(), true, logs
			}
			if testSawUnresolvedClaim(logs, id, 20*time.Millisecond) {
				return n.Load(), false, logs
			}
		}
		return n.Load(), false, logs
	}
	t.Run("allowance standing", func(t *testing.T) {
		claims, released, _ := run(t, "claim-allowance-standing")
		if !released || claims != moves+1 {
			t.Fatalf("released=%v after %d claim(s), want released after %d movements and one landing", released, claims, moves)
		}
	})
	t.Run("allowance spent", func(t *testing.T) {
		mqttTestSharedShardAllowanceV.Set(time.Nanosecond)
		t.Cleanup(func() { mqttTestSharedShardAllowanceV.Set(0) })
		claims, released, _ := run(t, "claim-allowance-spent")
		if released || claims >= moves || claims < int32(mqttForegroundAcquireAttempts) {
			t.Fatalf("released=%v after %d claim(s), want the claim to end unresolved within its budget", released, claims)
		}
	})
}
