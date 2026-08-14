package mqtt

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// Tests for the per-client session-interval register: the identity a record-absent
// create inherits when the retirement watermark read comes back ABSENT.
//
// Two properties carry the whole design, and the fixtures below are built around
// them rather than around the code's shape:
//
//   - THE IDENTITY IS THE SEQUENCE. Every assertion names the exact
//     "<epoch>.<sequence>" value the test itself observed, never merely that two
//     brokers agreed: agreement alone passes against a derivation mutated to any
//     other stable value, including a body field.
//   - THE ADVANCE IS UNCONDITIONAL. Three refuted guards were removed, so the
//     regression barrier runs the other way: the mutations that matter here ADD an
//     equality guard, and the tests that catch them are the duplicate-finisher ones.
//
// Every lineage test drives the production create arm. No test picks a token and
// stages two records around it — a fixture that supplies the value its own assertion
// checks cannot kill a mutation at the production site.

// testLoadOrigin returns a client's interval register, its sequence and its
// classification, read through the real load path.
func testLoadOrigin(t *testing.T, env *testTeardownEnv, idHash string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
	t.Helper()
	return env.asm.jsa.loadOriginIntervalRecord(env.asm.domainTk, idHash)
}

// testOriginToken returns the exact token a reader would derive from the register
// standing now, and fails when there is none: a test comparing against an empty
// token would pass against a broker that derived nothing.
func testOriginToken(t *testing.T, env *testTeardownEnv, idHash string) string {
	t.Helper()
	_, seq, status := testLoadOrigin(t, env, idHash)
	if status != mqttOriginValid {
		t.Fatalf("expected a valid interval register, got status %v", status)
	}
	tok := mqttOriginToken(env.asm.originStreamEpoch, seq)
	if tok == _EMPTY_ {
		t.Fatalf("the register at sequence %v yielded no token (epoch %q)", seq, env.asm.originStreamEpoch)
	}
	return tok
}

// testStageQuietClaimedMarker stages a claimed teardown marker that NO expiry path will
// adopt, and every test below that drives an adoption by calling finishAdoptedTeardown
// itself uses it.
//
// testStageClaimedMarker deliberately makes its record OVERDUE so the record-driven
// expiry path admits it. That is right for the tests about adoption ELIGIBILITY and wrong
// here: an async adopter — the session-record watch on the other broker sees the marker
// land, reconciles the expiry it now reads as overdue, and finishes the teardown — is a
// legitimate second writer of the interval register, and it made the register assertions
// depend on nothing else getting there first. Under the full suite it sometimes did,
// landing between the store-hook snapshot and the register re-read. Every expiry path
// skips a record with ExpiryInterval == 0 or DisconnectedAt == 0 (mqtt.go:5432, 5762,
// 5906), so leaving both unset removes that writer by construction rather than by timing.
func testStageQuietClaimedMarker(t *testing.T, env *testTeardownEnv, clientID string) (*mqttPersistedSession, uint64) {
	t.Helper()
	sess := testProvisionSession(t, env, clientID)
	ps, seq := testLoadSessRecord(t, env, sess.idHash)
	if ps == nil {
		t.Fatal("the provisioned session must have a record to mark")
	}
	ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
	ps.Teardown = &mqttSessTeardown{
		Origin:    "test-abandoned-owner",
		Nonce:     nuid.Next(),
		StartedAt: time.Now().Unix(),
	}
	ps.ExpiryInterval, ps.DisconnectedAt = 0, 0
	markerSeq, err := mqttStoreSessionRecordCAS(&env.asm.jsa, env.asm.domainTk, ps, sess.idHash, seq)
	if err != nil {
		t.Fatalf("staging the teardown marker: %v", err)
	}
	return ps, markerSeq
}

// testCreateParent runs a production create and returns the parent it inherited.
func testCreateParent(t *testing.T, env *testTeardownEnv, clientID string) (*mqttSession, string) {
	t.Helper()
	sess, existed, err := env.asm.createOrRestoreSession(clientID, env.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if existed {
		t.Fatal("the fixture requires a record-absent CREATE, but a record was restored")
	}
	sess.mu.Lock()
	parent := sess.parentForNextWriteLocked()
	sess.mu.Unlock()
	return sess, parent
}

// testOriginCalls counts what a code path did to $MQTT_origin, delegating to the
// real implementation so the observation does not change the behavior. Scoped to one
// client hash, because the hooks are process-wide.
type testOriginCalls struct {
	mu     sync.Mutex
	loads  int
	stores int
	cas    []bool // one entry per store: true = conditional write, false = plain publish
}

func (c *testOriginCalls) snapshot() (loads, stores int, cas []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads, c.stores, append([]bool(nil), c.cas...)
}

// testObserveOrigin installs both register hooks for one client, recording every
// call and passing it through. The returned counter is safe to read after the test's
// production calls have returned.
func testObserveOrigin(t *testing.T, env *testTeardownEnv, idHash string) *testOriginCalls {
	t.Helper()
	calls := &testOriginCalls{}
	wantSubject := mqttOriginSubject(env.asm.domainTk, idHash)
	restoreLoad := setMQTTTestOriginLoadHook(func(h string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
		if h == idHash {
			calls.mu.Lock()
			calls.loads++
			calls.mu.Unlock()
		}
		return env.asm.jsa.loadOriginIntervalRecord(env.asm.domainTk, h)
	})
	restoreStore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
		if subject == wantSubject {
			calls.mu.Lock()
			calls.stores++
			calls.cas = append(calls.cas, cas)
			calls.mu.Unlock()
		}
		return env.asm.jsa.storeOriginIntervalRecord(subject, cas, r)
	})
	t.Cleanup(func() {
		restoreStore()
		restoreLoad()
	})
	return calls
}

// ---------------------------------------------------------------------------
// Test 1: the register read/establish dispositions, by exact token value
// ---------------------------------------------------------------------------

// TestOriginRegisterDispositions is test 1 (M5, M22, M24, M25, M6).
//
// Every row asserts the EXACT token, so a derivation mutated to a body field or to
// the loaded sequence in place of our own confirmed one fails here rather than
// merely producing "some token".
func TestOriginRegisterDispositions(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-dispositions"
	idHash := getHash(clientID)
	epoch := a.asm.originStreamEpoch
	if !mqttValidSessEpoch(epoch) {
		t.Fatalf("the manager must have adopted a well-formed register epoch, got %q", epoch)
	}

	rejected := ApiErrors[JSStreamWrongLastSequenceErrF]         // 10071
	contended := ApiErrors[JSStreamWrongLastSequenceConstantErr] // 10164

	valid := func(seq uint64) func(int) (*mqttOriginInterval, uint64, mqttOriginStatus) {
		return func(int) (*mqttOriginInterval, uint64, mqttOriginStatus) {
			return &mqttOriginInterval{ID: clientID}, seq, mqttOriginValid
		}
	}
	absent := func(int) (*mqttOriginInterval, uint64, mqttOriginStatus) {
		return nil, 0, mqttOriginAbsent
	}

	for _, tc := range []struct {
		name string
		// load is called with the zero-based read attempt.
		load func(int) (*mqttOriginInterval, uint64, mqttOriginStatus)
		// store is called with the zero-based establish attempt.
		store      func(int) (uint64, error)
		wantToken  string
		wantLoads  int
		wantStores int
	}{
		{
			name:      "valid register yields its own sequence",
			load:      valid(17),
			wantToken: mqttOriginToken(epoch, 17),
			wantLoads: 1,
		},
		{
			name:       "absent establishes and takes OUR confirmed sequence",
			load:       absent,
			store:      func(int) (uint64, error) { return 42, nil },
			wantToken:  mqttOriginToken(epoch, 42),
			wantLoads:  1,
			wantStores: 1,
		},
		{
			name: "10071 then present adopts the register that won",
			load: func(n int) (*mqttOriginInterval, uint64, mqttOriginStatus) {
				if n == 0 {
					return absent(n)
				}
				return valid(7)(n)
			},
			store:      func(int) (uint64, error) { return 0, rejected },
			wantToken:  mqttOriginToken(epoch, 7),
			wantLoads:  2,
			wantStores: 1,
		},
		{
			name: "10164 then present adopts the register that won",
			load: func(n int) (*mqttOriginInterval, uint64, mqttOriginStatus) {
				if n == 0 {
					return absent(n)
				}
				return valid(8)(n)
			},
			store:      func(int) (uint64, error) { return 0, contended },
			wantToken:  mqttOriginToken(epoch, 8),
			wantLoads:  2,
			wantStores: 1,
		},
		{
			name:       "10071 then absent for the whole bound concedes",
			load:       absent,
			store:      func(int) (uint64, error) { return 0, rejected },
			wantToken:  _EMPTY_,
			wantLoads:  mqttOriginAttempts,
			wantStores: mqttOriginAttempts,
		},
		{
			name:       "10164 then absent for the whole bound concedes",
			load:       absent,
			store:      func(int) (uint64, error) { return 0, contended },
			wantToken:  _EMPTY_,
			wantLoads:  mqttOriginAttempts,
			wantStores: mqttOriginAttempts,
		},
		{
			name: "10164 then corrupt concedes without establishing again",
			load: func(n int) (*mqttOriginInterval, uint64, mqttOriginStatus) {
				if n == 0 {
					return absent(n)
				}
				return nil, 3, mqttOriginCorrupt
			},
			store:      func(int) (uint64, error) { return 0, contended },
			wantToken:  _EMPTY_,
			wantLoads:  2,
			wantStores: 1,
		},
		{
			name: "10164 then indeterminate concedes without establishing again",
			load: func(n int) (*mqttOriginInterval, uint64, mqttOriginStatus) {
				if n == 0 {
					return absent(n)
				}
				return nil, 0, mqttOriginIndeterminate
			},
			store:      func(int) (uint64, error) { return 0, contended },
			wantToken:  _EMPTY_,
			wantLoads:  2,
			wantStores: 1,
		},
		{
			name:      "corrupt concedes and never establishes over an occupied subject",
			load:      func(int) (*mqttOriginInterval, uint64, mqttOriginStatus) { return nil, 4, mqttOriginCorrupt },
			wantToken: _EMPTY_,
			wantLoads: 1,
		},
		{
			name:      "indeterminate concedes",
			load:      func(int) (*mqttOriginInterval, uint64, mqttOriginStatus) { return nil, 0, mqttOriginIndeterminate },
			wantToken: _EMPTY_,
			wantLoads: 1,
		},
		{
			name:       "an establish that FAILED concedes without retrying",
			load:       absent,
			store:      func(int) (uint64, error) { return 0, errors.New("mqtt test: register store failed") },
			wantToken:  _EMPTY_,
			wantLoads:  1,
			wantStores: 1,
		},
		{
			name:       "a confirmed establish at sequence 0 is not a token",
			load:       absent,
			store:      func(int) (uint64, error) { return 0, nil },
			wantToken:  _EMPTY_,
			wantLoads:  1,
			wantStores: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			loads, stores := 0, 0
			restoreLoad := setMQTTTestOriginLoadHook(func(h string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
				if h != idHash {
					return a.asm.jsa.loadOriginIntervalRecord(a.asm.domainTk, h)
				}
				mu.Lock()
				n := loads
				loads++
				mu.Unlock()
				return tc.load(n)
			})
			defer restoreLoad()
			restoreStore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
				if subject != mqttOriginSubject(a.asm.domainTk, idHash) {
					return a.asm.jsa.storeOriginInterval(subject, cas, r)
				}
				if !cas {
					t.Errorf("the create-side establish must be a CREATE-ONLY CAS, not a plain publish: " +
						"two racing first creates would both mint an identity")
				}
				if r.ID != clientID {
					t.Errorf("the establish must name the connecting client exactly, got %q", r.ID)
				}
				mu.Lock()
				n := stores
				stores++
				mu.Unlock()
				if tc.store == nil {
					t.Error("this row must not reach the establish")
					return 0, errors.New("unexpected establish")
				}
				return tc.store(n)
			})
			defer restoreStore()

			got := a.asm.originIntervalToken(clientID, idHash)
			if got != tc.wantToken {
				t.Errorf("token = %q, want %q", got, tc.wantToken)
			}
			mu.Lock()
			gotLoads, gotStores := loads, stores
			mu.Unlock()
			if gotLoads != tc.wantLoads {
				t.Errorf("register reads = %d, want %d (the retry bound is part of the contract)", gotLoads, tc.wantLoads)
			}
			if gotStores != tc.wantStores {
				t.Errorf("establish attempts = %d, want %d", gotStores, tc.wantStores)
			}
		})
	}
}

// TestOriginRegisterLoaderClassifiesAForeignName is test 1's loader half (M10).
//
// The assertion is the loader's CLASSIFICATION, not the create's outcome: both a
// malformed body and a body naming a client that hashes elsewhere make the create
// concede, so an outcome-only assertion cannot tell the hash-back check from the
// exact-ID one and would pass with either removed.
func TestOriginRegisterLoaderClassifiesAForeignName(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-loader-foreign"
	idHash := getHash(clientID)

	// Well-formed JSON, every field plausible — and a name that hashes to a DIFFERENT
	// subject than the one it is stored on.
	foreign := &mqttOriginInterval{ID: "some-entirely-other-client", Nonce: "n", MarkerSeq: 5, Gen: mqttNewSessGen()}
	if getHash(foreign.ID) == idHash {
		t.Fatal("the fixture needs two client IDs with DIFFERENT hashes")
	}
	if _, err := a.asm.jsa.storeOriginInterval(mqttOriginSubject(a.asm.domainTk, idHash), true, foreign); err != nil {
		t.Fatalf("planting the foreign register: %v", err)
	}

	r, seq, status := testLoadOrigin(t, a, idHash)
	if status != mqttOriginCorrupt {
		t.Fatalf("a register naming a client that does not hash back to its own subject must be "+
			"CORRUPT, got status %v (r=%+v)", status, r)
	}
	if seq == 0 {
		t.Error("the sequence is available pre-decode and must be reported")
	}

	// And a malformed body is the other corrupt route, kept distinct so the two are
	// not conflated.
	if _, err := a.asm.jsa.storeMsg(mqttOriginSubject(a.asm.domainTk, getHash("origin-loader-garbage")), 0,
		[]byte("{not json")); err != nil {
		t.Fatalf("planting a malformed register: %v", err)
	}
	if _, _, status := testLoadOrigin(t, a, getHash("origin-loader-garbage")); status != mqttOriginCorrupt {
		t.Fatalf("an undecodable register must be corrupt, got %v", status)
	}
}

// ---------------------------------------------------------------------------
// Tests 3, 21: closure, and the arms that must not advance
// ---------------------------------------------------------------------------

// TestOriginHistoryLostStillRecognisesTheSibling is test 3.
//
// The subcase the item exists to close: interval 1 completes, its retirement
// watermark is then ERASED, and interval 2's create still inherits a parent — from
// the register, which lives on a different stream and which nothing can delete. The
// token must also DIFFER from interval 1's, or the two intervals would share one
// identity, which is the false-fork direction.
func TestOriginHistoryLostStillRecognisesTheSibling(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-history-lost"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	firstToken := testOriginToken(t, a, idHash)

	if err := sessA.clear(false); err != nil {
		t.Fatalf("clear(): %v", err)
	}
	secondToken := testOriginToken(t, a, idHash)
	if secondToken == firstToken {
		t.Fatalf("the teardown must ADVANCE the interval identity, both intervals hold %q", firstToken)
	}

	// Erase the retirement history, the exact loss this item closes. Deleting the
	// watermark message is what an administrative prune does; the register is on a
	// stream where that is denied.
	_, wseq, wstatus := testLoadRetirement(t, a, idHash)
	if wstatus != mqttRetirementValid {
		t.Fatalf("the teardown must have recorded a watermark, got %v", wstatus)
	}
	if err := a.asm.jsa.deleteMsg(mqttTeardownStreamName, wseq, true); err != nil {
		t.Fatalf("erasing the retirement watermark: %v", err)
	}
	if _, _, st := testLoadRetirement(t, a, idHash); st != mqttRetirementAbsent {
		t.Fatalf("the watermark must be gone for this fixture to mean anything, got %v", st)
	}

	_, parent := testCreateParent(t, a, clientID)
	if parent != secondToken {
		t.Fatalf("a create with no retirement history must inherit the INTERVAL identity: "+
			"parent=%q want %q (before this existed it was empty, and a parentless create concedes 0x8E "+
			"with its delayed Will cancelled)", parent, secondToken)
	}
}

// TestOriginNoRecordTeardownDoesNotAdvance is test 21 (M30).
//
// clear()'s seq == 0 arm has nothing to arbitrate and never reaches
// recordGenRetirement, so it must not move an identity either: the interval it would
// rotate is one no record ever occupied.
func TestOriginNoRecordTeardownDoesNotAdvance(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-no-record-teardown"
	idHash := getHash(clientID)

	// A create that never saved: the object exists, the subject is empty, seq == 0.
	sess, _ := testCreateParent(t, a, clientID)
	sess.mu.Lock()
	seq := sess.seq
	sess.mu.Unlock()
	if seq != 0 {
		t.Fatalf("the fixture needs an unsaved session, got sequence %v", seq)
	}
	before := testOriginToken(t, a, idHash) // established by the create itself

	calls := testObserveOrigin(t, a, idHash)
	if err := sess.clear(false); err != nil {
		t.Fatalf("clear() on an unsaved session: %v", err)
	}
	if _, stores, _ := calls.snapshot(); stores != 0 {
		t.Errorf("a teardown with no persisted record wrote the register %d times, want 0", stores)
	}
	if after := testOriginToken(t, a, idHash); after != before {
		t.Errorf("the identity moved from %q to %q on an arm that retires nothing", before, after)
	}
}

// ---------------------------------------------------------------------------
// Tests 5, 6, 16, 17: where the advance sits relative to everything else
// ---------------------------------------------------------------------------

// TestOriginAdvancePrecedesEveryDeletion is test 5 (M2, M14).
//
// Both callers, because folding the advance into recordGenRetirement rather than
// writing it at each call site is what makes the coverage structural — a mutation
// that advances at only one of them must fail here.
func TestOriginAdvancePrecedesEveryDeletion(t *testing.T) {
	t.Run("clear", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const clientID = "origin-before-delete-clear"
		sessA := testProvisionSession(t, a, clientID)
		idHash, retiredGen := sessA.idHash, sessA.gen
		_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)

		wantSubject := mqttOriginSubject(a.asm.domainTk, idHash)
		restore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
			if subject != wantSubject {
				return a.asm.jsa.storeOriginInterval(subject, cas, r)
			}
			return 0, errors.New("mqtt test: interval register advance forced to fail")
		})
		err := sessA.clear(false)
		restore()
		if err == nil || errors.Is(err, errMQTTClearSuperseded) {
			t.Fatalf("clear() = %v, want a plain failure: an identity that cannot be advanced must "+
				"abort the teardown", err)
		}
		if _, _, status := testLoadRetirement(t, a, idHash); status != mqttRetirementAbsent {
			t.Errorf("no watermark may be written when the identity could not advance, got %v", status)
		}
		ps, _ := testLoadSessRecord(t, a, idHash)
		if ps == nil || ps.Teardown == nil {
			t.Fatalf("the record must be left MARKED for a later adopter, got %+v", ps)
		}
		if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
			t.Error("a deletion ran although the interval identity never advanced")
		}
	})

	t.Run("adoption", func(t *testing.T) {
		testQuietExpirySweeps(t)
		a, b := testNewTeardownEnv(t)
		const clientID = "origin-before-delete-adopt"
		marker, markerSeq := testStageQuietClaimedMarker(t, a, clientID)
		idHash := getHash(clientID)
		_, _, pubRelDur := mqttPubRelNames(idHash, marker.Gen)

		wantSubject := mqttOriginSubject(b.asm.domainTk, idHash)
		restore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
			if subject != wantSubject {
				return b.asm.jsa.storeOriginInterval(subject, cas, r)
			}
			return 0, errors.New("mqtt test: interval register advance forced to fail")
		})
		err := b.asm.finishAdoptedTeardown(marker, idHash, markerSeq, b.s.getOpts())
		restore()
		if err == nil {
			t.Fatal("an adoption whose identity advance fails must abort with nothing deleted")
		}
		if _, _, status := testLoadRetirement(t, b, idHash); status != mqttRetirementAbsent {
			t.Errorf("no watermark may be written when the identity could not advance, got %v", status)
		}
		if got, _ := testLoadSessRecord(t, b, idHash); got == nil || got.Teardown == nil {
			t.Errorf("the marked record must survive an aborted adoption, got %+v", got)
		}
		if !testConsumerExists(t, b, mqttOutStreamName, pubRelDur) {
			t.Error("the adoption deleted although the interval identity never advanced")
		}
	})

	t.Run("the advance is durable before the first deletion", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const clientID = "origin-observed-before-delete"
		sessA := testProvisionSession(t, a, clientID)
		idHash, retiredGen := sessA.idHash, sessA.gen
		_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)
		before := testOriginToken(t, a, idHash)

		fired := false
		gate := newTestHookGate(t)
		restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, _ string, _ uint64) {
			if !gate.enter() {
				return
			}
			defer gate.leave()
			if sess != sessA {
				return
			}
			fired = true
			if now := testOriginToken(t, a, idHash); now == before {
				t.Errorf("the interval identity must have advanced before the first deletion, still %q", now)
			}
			if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
				t.Error("a deletion ran before the identity advanced")
			}
		})
		defer restore()
		if err := sessA.clear(false); err != nil {
			t.Fatalf("clear(): %v", err)
		}
		if !fired {
			t.Fatal("the claim-window hook never fired")
		}
	})
}

// TestOriginAdvanceSurvivesTheWatermarkEarlyReturn is test 6 (M3).
//
// recordGenRetirement returns early when a standing watermark already sits at or
// above this teardown's marker sequence — and the record is still DELETED on that
// path. An advance placed past that return is skipped exactly there, leaving an
// interval whose record is gone with its identity unmoved, which is identity reuse.
func TestOriginAdvanceSurvivesTheWatermarkEarlyReturn(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-early-return"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	before := testOriginToken(t, a, idHash)

	// A watermark already standing ABOVE anything this teardown will write, so the
	// decision loop takes its "the standing fence is at least as new as our fact"
	// return without storing.
	planted := testPlantRetirement(t, a, idHash, &mqttGenRetirement{
		ID: clientID, Gen: mqttNewSessGen(), MarkerSeq: 1 << 40, RWID: mqttNewRecordWID(),
	}, 0)
	if planted == 0 {
		t.Fatal("planting the standing watermark")
	}

	stores := 0
	wantSubject := mqttTeardownSubject(a.asm.domainTk, idHash)
	restoreW := setMQTTTestRetirementStoreHook(func(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error) {
		if subject == wantSubject {
			stores++
		}
		return a.asm.jsa.storeGenRetirementCAS(subject, expectSeq, r)
	})
	err := sessA.clear(false)
	restoreW()
	if err != nil {
		t.Fatalf("clear(): %v", err)
	}
	if stores != 0 {
		t.Fatalf("the fixture must exercise the early return, but the watermark was written %d times", stores)
	}
	if got, _ := testLoadSessRecord(t, a, idHash); got != nil {
		t.Fatalf("the record is still deleted on the early-return path, got %+v", got)
	}
	if after := testOriginToken(t, a, idHash); after == before {
		t.Fatalf("the identity must advance even when the watermark decision returns early, still %q", after)
	}
}

// TestOriginAdvanceFollowsLocalValidation is test 16 (M20).
//
// A malformed teardown target fails mqttValidateGenRetirement and deletes nothing.
// An advance placed BEFORE that validation would rotate the identity anyway, putting
// the register ahead of a teardown that never happened — after which a create derives
// a token no record carries.
func TestOriginAdvanceFollowsLocalValidation(t *testing.T) {
	testQuietExpirySweeps(t)
	a, b := testNewTeardownEnv(t)
	const clientID = "origin-after-validation"

	// A marker whose PWID is empty: the watermark validator refuses to name no
	// retired record, so the adoption fails its own validation before any write.
	marker, markerSeq := testStageQuietClaimedMarker(t, a, clientID)
	idHash := getHash(clientID)
	marker.PWID = _EMPTY_
	before := testOriginToken(t, a, idHash)

	calls := testObserveOrigin(t, b, idHash)
	err := b.asm.finishAdoptedTeardown(marker, idHash, markerSeq, b.s.getOpts())
	if err == nil {
		t.Fatal("a teardown whose target names no retired record must fail closed")
	}
	after := testOriginToken(t, a, idHash)
	// The store count is snapshotted AFTER the re-read, not before it: taken first, a
	// write that landed in between shows up as a moved identity with zero writes
	// recorded, which says nothing about who moved it. This ordering makes any second
	// writer name itself.
	_, stores, cas := calls.snapshot()
	if stores != 0 {
		t.Errorf("the register was written %d times (cas=%v) although the teardown failed its own "+
			"validation and retired nothing", stores, cas)
	}
	if after != before {
		t.Errorf("the identity moved from %q to %q although nothing was retired (%d observed writes)",
			before, after, stores)
	}
}

// TestOriginAbortedTeardownRetriedAdvancesAgain is test 17 (M18, M19).
//
// The abort is injected AFTER a successful advance — on the watermark write — so the
// retry meets a register this client has already rotated once. It must rotate again:
// any equality guard would decline the second write and leave the identity of an
// interval whose teardown never completed standing as the current one.
func TestOriginAbortedTeardownRetriedAdvancesAgain(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-retry-after-abort"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	before := testOriginToken(t, a, idHash)

	wantSubject := mqttTeardownSubject(a.asm.domainTk, idHash)
	restore := setMQTTTestRetirementStoreHook(func(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error) {
		if subject != wantSubject {
			return a.asm.jsa.storeGenRetirementCAS(subject, expectSeq, r)
		}
		return 0, errors.New("mqtt test: watermark store forced to fail after the advance")
	})
	err := sessA.clear(false)
	restore()
	if err == nil {
		t.Fatal("a teardown whose watermark cannot be written must abort")
	}
	first := testOriginToken(t, a, idHash)
	if first == before {
		t.Fatalf("the advance runs before the watermark write, so it must have landed: still %q", first)
	}
	if got, _ := testLoadSessRecord(t, a, idHash); got == nil || got.Teardown == nil {
		t.Fatalf("the aborted attempt must delete nothing and leave the record marked, got %+v", got)
	}

	// The retry: same client, same marker, same claim nonce, same everything an
	// equality guard could key on.
	ps, markerSeq := testLoadSessRecord(t, a, idHash)
	if err := a.asm.finishAdoptedTeardown(ps, idHash, markerSeq, a.s.getOpts()); err != nil {
		t.Fatalf("the retry must succeed: %v", err)
	}
	second := testOriginToken(t, a, idHash)
	if second == first {
		t.Fatalf("the retried teardown must rotate the identity again, both attempts hold %q — "+
			"an equality guard here leaves an earlier interval's identity standing, which is the "+
			"false-fork direction", second)
	}
}

// ---------------------------------------------------------------------------
// Tests 7-10, 19, 20: the create arm
// ---------------------------------------------------------------------------

// TestOriginRejectedEstablishNeverMintsAToken is test 7 (M5).
//
// Two fixtures, one per rejection kind, because they mean different things: 10071 is
// the single-writer check and 10164 is clustered contention, and NEITHER proves a
// register exists — the server folds a failed subject load into 10071. What a
// rejection licenses is a re-read.
//
// There is deliberately no teardown-side row: the advance is unconditional, so no
// rejection can be returned to it.
func TestOriginRejectedEstablishNeverMintsAToken(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"10071", ApiErrors[JSStreamWrongLastSequenceErrF]},
		{"10164", ApiErrors[JSStreamWrongLastSequenceConstantErr]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := testNewTeardownEnv(t)
			clientID := "origin-rejected-" + tc.name
			idHash := getHash(clientID)

			// The register this client's create will find on its re-read, planted by
			// the "winner" of the race.
			winner := &mqttOriginInterval{ID: clientID, Origin: "other-broker"}
			winnerSeq, err := a.asm.jsa.storeOriginInterval(mqttOriginSubject(a.asm.domainTk, idHash), true, winner)
			if err != nil {
				t.Fatalf("planting the winning register: %v", err)
			}

			// Our own establish is rejected. The first read must therefore report
			// absent, or the establish is never attempted at all.
			reads := 0
			restoreLoad := setMQTTTestOriginLoadHook(func(h string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
				if h != idHash {
					return a.asm.jsa.loadOriginIntervalRecord(a.asm.domainTk, h)
				}
				reads++
				if reads == 1 {
					return nil, 0, mqttOriginAbsent
				}
				return a.asm.jsa.loadOriginIntervalRecord(a.asm.domainTk, h)
			})
			defer restoreLoad()
			restoreStore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
				if subject != mqttOriginSubject(a.asm.domainTk, idHash) {
					return a.asm.jsa.storeOriginInterval(subject, cas, r)
				}
				return 0, tc.err
			})
			defer restoreStore()

			got := a.asm.originIntervalToken(clientID, idHash)
			want := mqttOriginToken(a.asm.originStreamEpoch, winnerSeq)
			if got != want {
				t.Fatalf("a rejected establish must adopt the register the RE-READ found, not mint one "+
					"from the write that was rejected: token=%q want %q", got, want)
			}
		})
	}
}

// TestOriginCorruptRegisterConcedesAndTheConnectSucceeds is test 8 (M11, M17).
//
// A corrupt register is somebody else's occupied subject as far as a create is
// concerned: it concedes, and the CONNECT still succeeds — refusing would brick the
// client ID forever, because the register cannot be deleted. The next teardown
// REPLACES it, which is the asymmetry against the retirement watermark: a watermark
// that cannot be read may name a retirement newer than ours, whereas the register is
// not a fence and must only move forward.
func TestOriginCorruptRegisterConcedesAndTheConnectSucceeds(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-corrupt-register"
	idHash := getHash(clientID)

	if _, err := a.asm.jsa.storeMsg(mqttOriginSubject(a.asm.domainTk, idHash), 0, []byte("{ not json")); err != nil {
		t.Fatalf("planting a corrupt register: %v", err)
	}
	if _, _, st := testLoadOrigin(t, a, idHash); st != mqttOriginCorrupt {
		t.Fatalf("the fixture needs a corrupt register, got %v", st)
	}

	warn := testCaptureWarnings(t, a)
	sess, parent := testCreateParent(t, a, clientID)
	if parent != _EMPTY_ {
		t.Errorf("a create must CONCEDE on a corrupt register rather than deriving a parent, got %q", parent)
	}
	// Reported, not silent. A concession costs this client cross-term fork recognition
	// for as long as the register stays unreadable, and refusing automatic eviction on
	// the ground that it degrades the feature without a diagnostic would be hollow if
	// the runtime path did exactly that.
	if !warn.sawWarning(t, "session-interval identity") {
		t.Error("conceding an interval identity must be REPORTED: an unreadable register can " +
			"disable fork recognition for this client indefinitely with nothing to see")
	}
	// And it must name the client, or an operator cannot act on it.
	if !warn.sawWarning(t, clientID) {
		t.Error("the diagnostic must name the client whose recognition is degraded")
	}
	if _, _, st := testLoadOrigin(t, a, idHash); st != mqttOriginCorrupt {
		t.Error("a create must not establish over an occupied subject")
	}

	// The teardown replaces it unconditionally, which is what makes the concession
	// self-healing rather than permanent.
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := sess.clear(false); err != nil {
		t.Fatalf("clear(): %v", err)
	}
	if _, _, st := testLoadOrigin(t, a, idHash); st != mqttOriginValid {
		t.Fatalf("the advance must REPLACE a corrupt register, got status %v", st)
	}
}

// TestOriginIndeterminateWatermarkTouchesNothing is test 9 (M8).
//
// A transient watermark read establishes no retirement fact, so it must not reach
// $MQTT_origin at all: an identity taken there could be stamped onto an interval
// whose retirement state merely could not be read.
func TestOriginIndeterminateWatermarkTouchesNothing(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-indeterminate-watermark"
	idHash := getHash(clientID)

	calls := testObserveOrigin(t, a, idHash)
	restore := setMQTTTestRetirementLoadHook(func(h string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
		if h != idHash {
			return a.asm.jsa.loadGenRetirementRecord(a.asm.domainTk, h)
		}
		return nil, 0, mqttRetirementIndeterminate
	})
	defer restore()

	_, parent := testCreateParent(t, a, clientID)
	if parent != _EMPTY_ {
		t.Errorf("a create over an unreadable watermark must stay parentless, got %q", parent)
	}
	loads, stores, _ := calls.snapshot()
	if loads != 0 || stores != 0 {
		t.Errorf("the indeterminate arm touched $MQTT_origin: %d reads, %d writes, want 0 and 0", loads, stores)
	}
}

// TestOriginValidWatermarkUsesRWID is test 10 (M9).
//
// The two arms are disjoint by design: RWID answers the create whose watermark is
// valid and the register answers the create whose watermark is absent. A create that
// consulted both would pay a read on every reconnect after a teardown, and the two
// arms of one interval could disagree.
func TestOriginValidWatermarkUsesRWID(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-valid-watermark"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	base, _ := testLoadSessRecord(t, a, idHash)
	if base == nil || base.WID == _EMPTY_ {
		t.Fatal("the record this teardown retires must carry an identity")
	}
	if err := sessA.clear(false); err != nil {
		t.Fatalf("clear(): %v", err)
	}
	r, _, status := testLoadRetirement(t, a, idHash)
	if status != mqttRetirementValid {
		t.Fatalf("watermark status = %v, want valid", status)
	}

	calls := testObserveOrigin(t, a, idHash)
	_, parent := testCreateParent(t, a, clientID)
	if parent != r.RWID {
		t.Errorf("a valid watermark supplies the parent: parent=%q want RWID %q", parent, r.RWID)
	}
	if parent != base.WID {
		t.Errorf("and it names the record the deletion REMOVED: %q want %q", parent, base.WID)
	}
	loads, stores, _ := calls.snapshot()
	if loads != 0 || stores != 0 {
		t.Errorf("the valid-watermark arm touched $MQTT_origin: %d reads, %d writes, want 0 and 0", loads, stores)
	}
}

// TestOriginExactClientIDNotTheHash is test 19 (M27).
//
// getHash keeps eight SHA-derived base-62 characters, so it is collision-resistant
// and NOT injective. Validating the register by hash-back alone would let two
// colliding client IDs share one register and therefore one parent pointer —
// unrelated first creates reported as siblings, which suppresses delayed-Will
// cancellation. The exact comparison converts that into mutual concession.
func TestOriginExactClientIDNotTheHash(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-exact-id"
	const other = "origin-exact-id-collides"
	idHash := getHash(clientID)

	// A well-formed register belonging to ANOTHER client, presented on this client's
	// subject: the shape a hash collision produces. The loader classifies it corrupt
	// only because the real hashes differ, so the load hook stands in for the
	// collision itself while the create arm's comparison is the thing under test.
	restore := setMQTTTestOriginLoadHook(func(h string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
		if h != idHash {
			return a.asm.jsa.loadOriginIntervalRecord(a.asm.domainTk, h)
		}
		return &mqttOriginInterval{ID: other, Origin: "other-broker"}, 11, mqttOriginValid
	})
	defer restore()

	establishes := 0
	restoreStore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
		if subject == mqttOriginSubject(a.asm.domainTk, idHash) {
			establishes++
		}
		return a.asm.jsa.storeOriginInterval(subject, cas, r)
	})
	defer restoreStore()

	got := a.asm.originIntervalToken(clientID, idHash)
	if got != _EMPTY_ {
		t.Fatalf("a register naming a DIFFERENT client must not be adopted: token=%q. Validating by "+
			"hash-back alone gives two colliding client IDs one identity, which reports unrelated "+
			"first creates as siblings", got)
	}
	if establishes != 0 {
		t.Errorf("the subject is occupied by another client's register; it must not be overwritten "+
			"by a create (%d writes)", establishes)
	}
}

// TestOriginReadFollowsTheAbsenceProof is test 20's ordering half (M28).
//
// The register read sits after the proof that the record subject is empty. Hoisting
// it above that proof would consult the register on every RESUME too — a serial
// round trip on the hot reconnect path, and a read of an identity a resume has no
// use for.
func TestOriginReadFollowsTheAbsenceProof(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "origin-read-ordering"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	if ps, _ := testLoadSessRecord(t, a, idHash); ps == nil {
		t.Fatal("the fixture needs a PRESENT record, so the create arm is never reached")
	}

	calls := testObserveOrigin(t, b, idHash)
	sess, existed, err := b.asm.createOrRestoreSession(clientID, b.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if !existed || sess == nil {
		t.Fatalf("the fixture must RESTORE, existed=%v", existed)
	}
	loads, stores, _ := calls.snapshot()
	if loads != 0 || stores != 0 {
		t.Errorf("a resume must not consult the interval register: %d reads, %d writes, want 0 and 0",
			loads, stores)
	}
}

// ---------------------------------------------------------------------------
// Tests 11-13: the epoch, and the stream it lives on
// ---------------------------------------------------------------------------

// TestOriginErasedRegisterYieldsAFreshIdentity pins the corrected reason the deletion
// APIs are audited NON-prerequisites here rather than required.
//
// An earlier revision required `DenyDelete` and `DenyPurge` on the ground that an erased
// register would "restore an earlier interval's identity for reuse". That is false, and a
// holistic review caught it: a delete does not rewind the stream's sequence counter. The
// next establish mints a sequence HIGHER than anything issued before, so an administrative
// erasure can only lose an identity, never replay one — which concedes, the safe
// direction, and is the same recognition loss pruning the watermark streams already
// carries. Only stream RECREATION or a restore replays sequences, and the incarnation
// epoch is what separates those (R0c).
//
// The assertion is the strict inequality, in both places it matters: the re-established
// identity is neither the erased one nor anything below it.
func TestOriginErasedRegisterYieldsAFreshIdentity(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-erased-register"
	idHash := getHash(clientID)

	_, before := testCreateParent(t, a, clientID)
	if before == _EMPTY_ {
		t.Fatal("the create must establish an identity")
	}
	_, erasedSeq, status := testLoadOrigin(t, a, idHash)
	if status != mqttOriginValid {
		t.Fatalf("the register must stand before it can be erased, got %v", status)
	}

	// The administrative erasure the deny flags used to forbid. It succeeds, which is
	// itself the point: nothing in the required configuration blocks it.
	if err := a.asm.jsa.deleteMsg(mqttOriginStreamName, erasedSeq, true); err != nil {
		t.Fatalf("erasing the register: %v", err)
	}
	if _, _, st := testLoadOrigin(t, a, idHash); st != mqttOriginAbsent {
		t.Fatalf("the register must be gone, got status %v", st)
	}

	_, after := testCreateParent(t, a, clientID)
	if after == _EMPTY_ {
		t.Fatal("the create following an erasure must establish a new identity")
	}
	if after == before {
		t.Fatalf("an erasure must not hand back the identity it removed, both %q", before)
	}
	_, freshSeq, st := testLoadOrigin(t, a, idHash)
	if st != mqttOriginValid {
		t.Fatalf("the re-established register must be valid, got %v", st)
	}
	if freshSeq <= erasedSeq {
		t.Fatalf("the re-established identity must sit STRICTLY above the erased one: %v vs %v — "+
			"a delete that rewound the sequence counter is the only way an erasure could replay an "+
			"identity, and this is the assertion that would catch it", freshSeq, erasedSeq)
	}

	// The same for a PURGE, which is the bulk operation the unbounded-retention note
	// actually points operators at, and which the delete row does not exercise.
	if err := a.asm.jsa.purgeStream(mqttOriginStreamName, mqttOriginSubject(a.asm.domainTk, idHash), false); err != nil {
		t.Fatalf("purging the register subject: %v", err)
	}
	if _, _, st := testLoadOrigin(t, a, idHash); st != mqttOriginAbsent {
		t.Fatalf("the purged register must be gone, got status %v", st)
	}
	_, afterPurge := testCreateParent(t, a, clientID)
	if afterPurge == _EMPTY_ || afterPurge == after || afterPurge == before {
		t.Fatalf("a purge must not hand back any identity it removed: %q (erased %q, %q)",
			afterPurge, before, after)
	}
	_, purgedFreshSeq, st := testLoadOrigin(t, a, idHash)
	if st != mqttOriginValid || purgedFreshSeq <= freshSeq {
		t.Fatalf("the identity re-established after a purge must sit STRICTLY above the purged one: "+
			"%v vs %v (status %v)", purgedFreshSeq, freshSeq, st)
	}
}

// TestOriginTokenBindsTheEpoch is test 11 (M7).
//
// Sequences restart at 1 in a new stream incarnation, so a token that is only a
// sequence lets a recreated $MQTT_origin hand out an identity an earlier interval
// already held. The epoch is what keeps two incarnations apart.
func TestOriginTokenBindsTheEpoch(t *testing.T) {
	e1, e2 := mqttGenOriginEpoch(), mqttGenOriginEpoch()
	if e1 == e2 {
		t.Fatal("two generated epochs must differ")
	}
	if got1, got2 := mqttOriginToken(e1, 5), mqttOriginToken(e2, 5); got1 == got2 {
		t.Fatalf("two incarnations at the same sequence must yield different identities, both %q", got1)
	}
	if got := mqttOriginToken(e1, 5); !strings.HasPrefix(got, e1) || !strings.HasSuffix(got, "5") {
		t.Errorf("the token must carry both halves, got %q", got)
	}
	// Unusable inputs concede rather than producing a partially-formed identity.
	if got := mqttOriginToken(e1, 0); got != _EMPTY_ {
		t.Errorf("a landed sequence is never 0, so it must yield no token, got %q", got)
	}
	if got := mqttOriginToken("not-a-nuid", 5); got != _EMPTY_ {
		t.Errorf("a malformed epoch must yield no token, got %q", got)
	}
}

// TestValidateOriginStream is test 12's validator half (M15, M26).
//
// Exercises every validateOriginStream branch against synthetic StreamInfo values,
// including the ones a single-node bring-up cannot reach. The accepted rows below are
// the audit: a setting absent from the list is an audited NON-prerequisite, not an
// oversight.
func TestValidateOriginStream(t *testing.T) {
	t.Parallel()
	const domainTk = ""
	sessEpoch, originEpoch := mqttGenSessEpoch(), mqttGenOriginEpoch()
	wantSubj := mqttOriginSubjectPrefix + domainTk + ">"
	base := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Subjects:   []string{wantSubj},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
			Metadata: map[string]string{
				mqttOriginEpochMetaKey: originEpoch,
				mqttSessEpochMetaKey:   sessEpoch,
			},
		}}
	}
	if err := validateOriginStream(base(), domainTk, 0, sessEpoch); err != nil {
		t.Fatalf("conformant stream must validate, got %v", err)
	}
	si := base()
	si.Config.Replicas = 3
	if err := validateOriginStream(si, domainTk, 3, sessEpoch); err != nil {
		t.Fatalf("matching replicas must validate, got %v", err)
	}

	for _, tc := range []struct {
		name     string
		mutate   func(si *StreamInfo)
		replicas int
	}{
		{name: "nil"},
		{name: "wrongsubject", mutate: func(si *StreamInfo) { si.Config.Subjects = []string{"bogus.>"} }},
		{name: "twosubjects", mutate: func(si *StreamInfo) { si.Config.Subjects = []string{wantSubj, "extra.>"} }},
		{name: "memstorage", mutate: func(si *StreamInfo) { si.Config.Storage = MemoryStorage }},
		{name: "interestretention", mutate: func(si *StreamInfo) { si.Config.Retention = InterestPolicy }},
		{name: "discardnew", mutate: func(si *StreamInfo) { si.Config.Discard = DiscardNew }},
		{name: "maxmsgsper", mutate: func(si *StreamInfo) { si.Config.MaxMsgsPer = 2 }},
		{name: "maxmsgs", mutate: func(si *StreamInfo) { si.Config.MaxMsgs = 10 }},
		{name: "maxbytes", mutate: func(si *StreamInfo) { si.Config.MaxBytes = 1 << 20 }},
		{name: "maxage", mutate: func(si *StreamInfo) { si.Config.MaxAge = time.Hour }},
		{name: "maxmsgsize", mutate: func(si *StreamInfo) { si.Config.MaxMsgSize = 1024 }},
		{name: "mirror", mutate: func(si *StreamInfo) { si.Config.Mirror = &StreamSource{Name: mqttSessStreamName} }},
		{name: "sources", mutate: func(si *StreamInfo) {
			si.Config.Sources = []*StreamSource{{Name: mqttSessStreamName}}
		}},
		{name: "replicamismatch", mutate: func(si *StreamInfo) { si.Config.Replicas = 1 }, replicas: 3},
		{name: "noack", mutate: func(si *StreamInfo) { si.Config.NoAck = true }},
		{name: "sealed", mutate: func(si *StreamInfo) { si.Config.Sealed = true }},
		{name: "subjecttransform", mutate: func(si *StreamInfo) {
			si.Config.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"}
		}},
		{name: "republish", mutate: func(si *StreamInfo) {
			si.Config.RePublish = &RePublish{Source: "a", Destination: "b"}
		}},
		{name: "allowmsgcounter", mutate: func(si *StreamInfo) { si.Config.AllowMsgCounter = true }},
		{name: "allowrollup", mutate: func(si *StreamInfo) { si.Config.AllowRollup = true }},
		{name: "asyncpersist", mutate: func(si *StreamInfo) { si.Config.PersistMode = AsyncPersistMode }},
		// Both epochs are real prerequisites, and each is checked separately: one
		// mutation of the metadata map cannot discriminate two keys.
		{name: "nometadata", mutate: func(si *StreamInfo) { si.Config.Metadata = nil }},
		{name: "nooriginepoch", mutate: func(si *StreamInfo) {
			delete(si.Config.Metadata, mqttOriginEpochMetaKey)
		}},
		{name: "malformedoriginepoch", mutate: func(si *StreamInfo) {
			si.Config.Metadata[mqttOriginEpochMetaKey] = "not-a-nuid"
		}},
		{name: "nosessepoch", mutate: func(si *StreamInfo) {
			delete(si.Config.Metadata, mqttSessEpochMetaKey)
		}},
		{name: "malformedsessepoch", mutate: func(si *StreamInfo) {
			si.Config.Metadata[mqttSessEpochMetaKey] = "not-a-nuid"
		}},
		{name: "foreignsessepoch", mutate: func(si *StreamInfo) {
			si.Config.Metadata[mqttSessEpochMetaKey] = mqttGenSessEpoch()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var si *StreamInfo
			if tc.mutate != nil {
				si = base()
				tc.mutate(si)
			}
			if err := validateOriginStream(si, domainTk, tc.replicas, sessEpoch); err == nil {
				t.Fatalf("expected validation failure for %q", tc.name)
			}
		})
	}

	for _, tc := range []struct {
		name   string
		mutate func(si *StreamInfo)
	}{
		{"subjectdeletemarkerttl is not a prerequisite: no delete ever occurs here",
			func(si *StreamInfo) { si.Config.SubjectDeleteMarkerTTL = time.Minute }},
		{"deny_delete is not a prerequisite: nothing here deletes, and a delete cannot replay an identity",
			func(si *StreamInfo) { si.Config.DenyDelete = true }},
		{"deny_purge is not a prerequisite: nothing here purges",
			func(si *StreamInfo) { si.Config.DenyPurge = true }},
		{"allow_msg_ttl is inert: this owner sends no Nats-TTL",
			func(si *StreamInfo) { si.Config.AllowMsgTTL = true }},
		{"a consumer cap cannot affect an owner that creates none",
			func(si *StreamInfo) { si.Config.MaxConsumers = 5 }},
		{"unrelated metadata keys are accepted",
			func(si *StreamInfo) { si.Config.Metadata["unrelated"] = "keepme" }},
		{"a dedup window is inert: this owner sets no Nats-Msg-Id",
			func(si *StreamInfo) { si.Config.Duplicates = time.Minute }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := base()
			tc.mutate(si)
			if err := validateOriginStream(si, domainTk, 0, sessEpoch); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}
}

// TestOriginStreamIsProvisionedAsRequired is test 12's readback half (M15).
//
// A validator-only table passes while the CREATION spec is broken: the two are
// different code. This asserts the configuration the broker actually created.
func TestOriginStreamIsProvisionedAsRequired(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	si, err := a.asm.jsa.lookupStream(mqttOriginStreamName)
	if err != nil || si == nil {
		t.Fatalf("the interval-register stream must be provisioned: %+v %v", si, err)
	}
	cfg := si.Config
	// Both denials are audited NON-prerequisites, and the provisioned default must not
	// set them: an earlier revision required them on a rationale a review round refuted
	// (a delete cannot rewind the sequence counter), and setting DenyPurge in particular
	// would block the bulk pruning the unbounded-retention note tells operators to use.
	if cfg.DenyDelete || cfg.DenyPurge {
		t.Errorf("the provisioned default must leave both denials unset, got deny_delete=%v deny_purge=%v",
			cfg.DenyDelete, cfg.DenyPurge)
	}
	if cfg.MaxMsgsPer != 1 {
		t.Errorf("max_msgs_per_subject = %d, want 1", cfg.MaxMsgsPer)
	}
	// Unset is normalized to -1 by the server, which is what the validator's own
	// "> 0" tests mean by unlimited; the assertion is that no limit is POSITIVE.
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 || cfg.MaxMsgSize > 0 {
		t.Errorf("no limit may evict a register: max_msgs=%d max_bytes=%d max_age=%v max_msg_size=%d",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge, cfg.MaxMsgSize)
	}
	if cfg.Retention != LimitsPolicy || cfg.Discard != DiscardOld {
		t.Errorf("retention=%v discard=%v, want limits and old", cfg.Retention, cfg.Discard)
	}
	if !mqttValidSessEpoch(cfg.Metadata[mqttOriginEpochMetaKey]) {
		t.Errorf("the stream must carry its own incarnation epoch, got %q", cfg.Metadata[mqttOriginEpochMetaKey])
	}
	if got := cfg.Metadata[mqttSessEpochMetaKey]; got != a.asm.sessStreamEpoch {
		t.Errorf("the stream must be stamped with the adopted session epoch: %q vs %q", got, a.asm.sessStreamEpoch)
	}
	// The adoption, without which the create arm has nothing to derive from.
	if a.asm.originStreamEpoch != cfg.Metadata[mqttOriginEpochMetaKey] {
		t.Errorf("the manager must adopt the AUTHORITATIVE epoch: %q vs the stream's %q",
			a.asm.originStreamEpoch, cfg.Metadata[mqttOriginEpochMetaKey])
	}
}

// TestOriginStreamCoupledRecreationGuidance is test 13's guidance half (M34).
//
// Every wrapper that names the one logical unit must name all four streams: a
// partial recreation is the hazard, and guidance that omits a member sends an
// operator to recreate three of them.
func TestOriginStreamCoupledRecreationGuidance(t *testing.T) {
	t.Parallel()
	as := &mqttAccountSessionManager{}
	as.sessStreamEpoch = mqttGenSessEpoch()
	specs := as.brokerStreamSpecs(&Server{}, "acc", 3, 3, mqttDefaultMaxAckPending, true, "rmsub", nil)

	unit := map[string]bool{
		mqttSessStreamName:     true,
		mqttTeardownStreamName: true,
		mqttOriginStreamName:   true,
	}
	seen := 0
	for _, spec := range specs {
		if !unit[spec.name] {
			continue
		}
		seen++
		// A shape the validator refuses, so the wrapper's guidance is what is returned.
		err := spec.validate(&StreamInfo{Config: StreamConfig{Subjects: []string{"bogus.>"}}})
		if err == nil {
			t.Fatalf("%s: a nonconformant stream must be refused", spec.name)
		}
		msg := err.Error()
		if !strings.Contains(msg, "one logical unit") {
			t.Errorf("%s: the guidance must say the streams are one logical unit: %v", spec.name, msg)
		}
		for name := range unit {
			if !strings.Contains(msg, name) {
				t.Errorf("%s: the coupled-recreation guidance omits %s: %v", spec.name, name, msg)
			}
		}
	}
	if seen != len(unit) {
		t.Fatalf("found %d of the %d coupled streams in the spec table", seen, len(unit))
	}
}

// TestOriginStreamIsEpochCoupledToTheSessionStream is test 13 (M16, M33).
//
// The two partial-recreation directions are deliberately different, and only one is
// detectable:
//
//   - the session stream recreated alone mints a fresh incarnation epoch while the
//     registers keep the old one, so they name intervals of a stream that no longer
//     exists. REFUSED at startup with the coupled guidance.
//   - $MQTT_origin recreated alone stamps the current session epoch, validates
//     clean, and admits with NO registers. The loss is the stated residual, and it is
//     safe only because a fresh incarnation epoch makes every token it later hands
//     out diverge from every old one — which holds after the mandatory restart the
//     operational constraints already require.
func TestOriginStreamIsEpochCoupledToTheSessionStream(t *testing.T) {
	t.Run("the session stream recreated alone is refused", func(t *testing.T) {
		s1, c1, nc := testRetirementBringUp(t)
		asm1, err := s1.getOrCreateMQTTAccountSessionManager(c1)
		if err != nil {
			t.Fatalf("first bring-up: %v", err)
		}

		// The retirement watermark stream goes too, so IT is recreated clean against the
		// fresh session epoch and $MQTT_origin is the stream left carrying the stale one.
		// Without this the refusal comes from an earlier entry and says nothing about
		// this stream's own guidance.
		js, ctx := testRetirementJS(t, nc)
		for _, name := range []string{mqttSessStreamName, mqttTeardownStreamName} {
			if derr := js.DeleteStream(ctx, name); derr != nil {
				t.Fatalf("delete %s: %v", name, derr)
			}
		}

		s2, c2, _ := testRetirementBringUpOn(t, nc, "node2")
		_, err = s2.getOrCreateMQTTAccountSessionManager(c2)
		if err == nil {
			t.Fatal("a $MQTT_origin stream from another session-stream incarnation must fail " +
				"initialization: its registers name intervals of a stream that no longer exists")
		}
		for _, want := range []string{
			mqttOriginStreamName, mqttSessStreamName, mqttTeardownStreamName,
			"one logical unit", mqttSessEpochMetaKey,
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal must name %q, got: %v", want, err)
			}
		}
		_ = asm1
	})

	t.Run("the register stream recreated alone admits with no registers", func(t *testing.T) {
		s1, c1, nc := testRetirementBringUp(t)
		asm1, err := s1.getOrCreateMQTTAccountSessionManager(c1)
		if err != nil {
			t.Fatalf("first bring-up: %v", err)
		}
		const clientID = "origin-recreated-unit"
		idHash := getHash(clientID)
		firstSeq, serr := asm1.jsa.storeOriginInterval(mqttOriginSubject(asm1.domainTk, idHash), true,
			&mqttOriginInterval{ID: clientID})
		if serr != nil {
			t.Fatalf("planting a register: %v", serr)
		}
		firstToken := mqttOriginToken(asm1.originStreamEpoch, firstSeq)
		if firstToken == _EMPTY_ {
			t.Fatal("the planted register must yield a token")
		}

		js, ctx := testRetirementJS(t, nc)
		if derr := js.DeleteStream(ctx, mqttOriginStreamName); derr != nil {
			t.Fatalf("delete the register stream: %v", derr)
		}

		s2, c2, _ := testRetirementBringUpOn(t, nc, "node2")
		asm2, err := s2.getOrCreateMQTTAccountSessionManager(c2)
		if err != nil {
			t.Fatalf("recreating the register stream alone must ADMIT: %v", err)
		}
		if asm2.sessStreamEpoch != asm1.sessStreamEpoch {
			t.Fatalf("the surviving session stream keeps its incarnation: %q vs %q",
				asm2.sessStreamEpoch, asm1.sessStreamEpoch)
		}
		// The residual, asserted rather than assumed.
		if _, _, status := asm2.jsa.loadOriginIntervalRecord(asm2.domainTk, idHash); status != mqttOriginAbsent {
			t.Fatalf("the recreated stream must be empty, got status %v", status)
		}
		// And the reason the loss is safe: a fresh incarnation epoch, so the sequences
		// that restart at 1 cannot reproduce an identity the old incarnation issued.
		if asm2.originStreamEpoch == asm1.originStreamEpoch {
			t.Fatal("the recreated stream must carry a FRESH incarnation epoch, or its restarted " +
				"sequences reproduce identities the previous incarnation already handed out")
		}
		secondSeq, serr := asm2.jsa.storeOriginInterval(mqttOriginSubject(asm2.domainTk, idHash), true,
			&mqttOriginInterval{ID: clientID})
		if serr != nil {
			t.Fatalf("establishing in the new incarnation: %v", serr)
		}
		if got := mqttOriginToken(asm2.originStreamEpoch, secondSeq); got == firstToken {
			t.Fatalf("two incarnations must not issue the same identity, both %q", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Tests 4 and 15: the two written LAST, backwards from their mutations
// ---------------------------------------------------------------------------

// TestOriginEveryAdvanceMintsAFreshToken is test 4 (M6, M31, M1).
//
// The identity is the SEQUENCE, and this is the test that pins it there. Three rows,
// and the third is the one that discriminates: a byte-identical register body still
// yields a different token, which no body field can do.
func TestOriginEveryAdvanceMintsAFreshToken(t *testing.T) {
	a, _ := testNewTeardownEnv(t)

	t.Run("two teardowns retiring different records", func(t *testing.T) {
		const clientID = "origin-fresh-two-teardowns"
		s1 := testProvisionSession(t, a, clientID)
		idHash := s1.idHash
		t0 := testOriginToken(t, a, idHash)
		if err := s1.clear(false); err != nil {
			t.Fatalf("first clear(): %v", err)
		}
		t1 := testOriginToken(t, a, idHash)
		s2 := testProvisionSession(t, a, clientID)
		if err := s2.clear(false); err != nil {
			t.Fatalf("second clear(): %v", err)
		}
		t2 := testOriginToken(t, a, idHash)
		if t0 == t1 || t1 == t2 || t0 == t2 {
			t.Fatalf("every interval must hold a distinct identity, got %q, %q, %q", t0, t1, t2)
		}
	})

	t.Run("two finishers of the SAME retirement", func(t *testing.T) {
		testQuietExpirySweeps(t)
		b, c := testNewTeardownEnv(t)
		const clientID = "origin-fresh-same-retirement"
		marker, markerSeq := testStageQuietClaimedMarker(t, b, clientID)
		idHash := getHash(clientID)
		before := testOriginToken(t, b, idHash)

		// The first finisher completes the adoption; the second is handed the same
		// marker at the same sequence with the same claim nonce, which is what
		// finishAdoptedTeardown is built for.
		if err := b.asm.finishAdoptedTeardown(marker, idHash, markerSeq, b.s.getOpts()); err != nil {
			t.Fatalf("first finisher: %v", err)
		}
		first := testOriginToken(t, b, idHash)
		if first == before {
			t.Fatalf("the first finisher must rotate the identity, still %q", first)
		}
		// Superseded or not, the SECOND finisher's rotation must have happened: it
		// runs before the release discovers the marker is gone.
		_ = c.asm.finishAdoptedTeardown(marker, idHash, markerSeq, c.s.getOpts())
		second := testOriginToken(t, b, idHash)
		if second == first {
			t.Fatalf("a duplicate finisher must rotate the identity again, both hold %q — an equality "+
				"guard on the claim nonce, the marker sequence, the generation or the retired record's "+
				"WID all decline here, and each leaves an earlier interval's identity standing", second)
		}
	})

	t.Run("a byte-identical body still yields a different identity", func(t *testing.T) {
		const clientID = "origin-fresh-identical-body"
		idHash := getHash(clientID)
		subject := mqttOriginSubject(a.asm.domainTk, idHash)
		// The same bytes twice, which is exactly what a re-sent claim produces. Any
		// derivation reading the BODY returns one value for both.
		body := &mqttOriginInterval{ID: clientID, Nonce: "identical", MarkerSeq: 9, Gen: mqttNewSessGen()}
		seq1, err := a.asm.jsa.storeOriginInterval(subject, false, body)
		if err != nil {
			t.Fatalf("first write: %v", err)
		}
		seq2, err := a.asm.jsa.storeOriginInterval(subject, false, body)
		if err != nil {
			t.Fatalf("second write: %v", err)
		}
		if seq1 == seq2 {
			t.Fatalf("every store receives a fresh sequence, got %v twice", seq1)
		}
		tok1, tok2 := mqttOriginToken(a.asm.originStreamEpoch, seq1), mqttOriginToken(a.asm.originStreamEpoch, seq2)
		if tok1 == tok2 {
			t.Fatalf("identical bytes must still yield different identities, both %q: the identity is "+
				"the sequence precisely because a body can be replayed", tok1)
		}
		// And the standing register is the second one, so a reader derives tok2.
		if got := testOriginToken(t, a, idHash); got != tok2 {
			t.Fatalf("a reader must derive the STANDING register's identity: %q want %q", got, tok2)
		}
		// Through the PRODUCTION create arm, which is the half the assertions above do
		// not reach: they compare mqttOriginToken against itself, so a create-side
		// derivation rewritten to read a body field satisfies every one of them. The
		// two writes above carry identical bodies, so a body-derived identity is stable
		// across them and cannot equal the second write's sequence.
		_, parent := testCreateParent(t, a, clientID)
		if parent != tok2 {
			t.Fatalf("the create arm must derive the STANDING register's SEQUENCE: parent=%q "+
				"want %q — a derivation reading any body field returns the same value for two "+
				"byte-identical writes", parent, tok2)
		}
	})
}

// TestOriginDuplicateFinisherDivergenceInBothDirections is test 15 (M18, M19, M21).
//
// The residual this design ACCEPTS, asserted in both directions so it cannot later be
// "fixed" by a guard that reintroduces identity reuse:
//
//	(b) a create pair STRADDLING a second rotation derives different identities and
//	    both concede — exactly today's behavior;
//	(c) a create pair within one interval derives ONE identity and is recognised.
//
// The direction is the whole content of the residual: a second rotation always mints
// a FRESH sequence, so no interleaving of advances can produce a shared identity
// between two intervals. The worst case is the missed recognition in (b).
//
// What this test pins is the IDENTITY divergence and the classifier verdict each pair
// produces; the DISPOSAL those verdicts turn into — 0x89 against 0x8E, over real sockets,
// on records that were actually written — is pinned separately by
// TestMQTTOriginDisposalDirectionsE2E. Keeping them apart is deliberate: the fixtures here
// build no session records, so a claim about what the connection is TOLD would be a claim
// this test cannot support.
func TestOriginDuplicateFinisherDivergenceInBothDirections(t *testing.T) {
	t.Run("the advance is an unconditional publish, and it repeats", func(t *testing.T) {
		testQuietExpirySweeps(t)
		a, b := testNewTeardownEnv(t)
		const clientID = "origin-divergence-writes"
		marker, markerSeq := testStageQuietClaimedMarker(t, a, clientID)
		idHash := getHash(clientID)

		callsA := testObserveOrigin(t, a, idHash)
		callsB := testObserveOrigin(t, b, idHash)
		if err := a.asm.finishAdoptedTeardown(marker, idHash, markerSeq, a.s.getOpts()); err != nil {
			t.Fatalf("first finisher: %v", err)
		}
		_ = b.asm.finishAdoptedTeardown(marker, idHash, markerSeq, b.s.getOpts())

		// Both hooks observe the same process-wide seam, so either counter sees every
		// write to this subject; the two are installed so neither broker's is missed.
		loads, stores, cas := callsA.snapshot()
		if l2, s2, c2 := callsB.snapshot(); s2 > stores {
			loads, stores, cas = l2, s2, c2
		}
		if stores < 2 {
			t.Fatalf("two finishers of one marker must each rotate the identity, got %d writes", stores)
		}
		for i, c := range cas {
			if c {
				t.Errorf("register write %d was CONDITIONAL: the advance must be a plain publish, or a "+
					"rotation can be DECLINED and an earlier interval's identity left standing", i)
			}
		}
		if loads != 0 {
			t.Errorf("the teardown side must not READ the register (%d reads): a read-then-write is a "+
				"guard, and every guard that can decline a write is the false-fork direction", loads)
		}
	})

	t.Run("a straddling pair diverges and concedes", func(t *testing.T) {
		testQuietExpirySweeps(t)
		a, b := testNewTeardownEnv(t)
		const clientID = "origin-divergence-straddle"
		marker, markerSeq := testStageQuietClaimedMarker(t, a, clientID)
		idHash := getHash(clientID)

		// Finisher 1 completes: the record is released and the subject is empty, so a
		// create no longer takes the adoption branch.
		if err := a.asm.finishAdoptedTeardown(marker, idHash, markerSeq, a.s.getOpts()); err != nil {
			t.Fatalf("first finisher: %v", err)
		}
		if got, _ := testLoadSessRecord(t, a, idHash); got != nil {
			t.Fatalf("the release must have emptied the subject, got %+v", got)
		}
		// Erase the watermark so both creates take the register arm rather than RWID:
		// this residual is about the register, and a valid watermark would mask it.
		_, wseq, wstatus := testLoadRetirement(t, a, idHash)
		if wstatus != mqttRetirementValid {
			t.Fatalf("the finisher must have written a watermark, got %v", wstatus)
		}
		if err := a.asm.jsa.deleteMsg(mqttTeardownStreamName, wseq, true); err != nil {
			t.Fatalf("erasing the watermark: %v", err)
		}

		_, parentC := testCreateParent(t, a, clientID)
		if parentC == _EMPTY_ {
			t.Fatal("the create before the second rotation must derive an identity")
		}

		// Finisher 2 arrives LATE, after the release, and rotates again.
		_ = b.asm.finishAdoptedTeardown(marker, idHash, markerSeq, b.s.getOpts())
		afterSecond := testOriginToken(t, b, idHash)
		if afterSecond == parentC {
			t.Fatalf("the fixture needs the second rotation to have happened, still %q", parentC)
		}

		_, parentD := testCreateParent(t, b, clientID)
		if parentD == _EMPTY_ {
			t.Fatal("the create after the second rotation must derive an identity")
		}
		if parentD == parentC {
			t.Fatal("this residual requires the two creates to DIVERGE; if they agree the fixture is " +
				"not exercising it")
		}
		// The direction: two records whose parents differ are NOT siblings, so each
		// concedes — never a false fork. That is the whole content of the residual.
		if classifyRecordLineage(&mqttPersistedSession{WID: mqttNewRecordWID(), PWID: parentD},
			mqttNewRecordWID(), parentC) != mqttLineageNotForked {
			t.Fatal("a straddling pair must CONCEDE, which is exactly today's behavior")
		}
	})

	t.Run("a non-straddling pair is recognised", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const clientID = "origin-divergence-same-interval"
		idHash := getHash(clientID)

		// Two creates in ONE interval, off one empty subject with no watermark.
		_, first := testCreateParent(t, a, clientID)
		_, second := testCreateParent(t, a, clientID)
		if first == _EMPTY_ || second == _EMPTY_ {
			t.Fatalf("both creates must derive an identity, got %q and %q", first, second)
		}
		if first != second {
			t.Fatalf("two creates in ONE interval must derive the SAME identity, got %q and %q", first, second)
		}
		if first != testOriginToken(t, a, idHash) {
			t.Fatal("and it must be the identity the standing register holds")
		}
		// Which is what makes the pair recognisable: same parent, different identities.
		ackWID := mqttNewRecordWID()
		sibling := &mqttPersistedSession{WID: mqttNewRecordWID(), PWID: second}
		if classifyRecordLineage(sibling, ackWID, first) != mqttLineageForked {
			t.Fatal("two creates of one interval are SIBLINGS: without that the erased side reports " +
				"0x8E 'session taken over' and cancels its delayed Will, which is what this item exists " +
				"to prevent")
		}
	})
}

// TestOriginBodyIsDiagnosticOnly pins the persisted key names, which are the contract
// a replicated round trip cannot check: every reader and writer in this tree would
// agree on a struct-tag typo.
func TestOriginBodyIsDiagnosticOnly(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "origin-raw-keys"
	sess := testProvisionSession(t, a, clientID)
	idHash := sess.idHash
	if err := sess.clear(false); err != nil {
		t.Fatalf("clear(): %v", err)
	}
	smsg, err := a.asm.jsa.loadLastMsgFor(mqttOriginStreamName, mqttOriginSubject(a.asm.domainTk, idHash))
	if err != nil {
		t.Fatalf("reading the register raw: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(smsg.Data, &raw); err != nil {
		t.Fatalf("the register body must be JSON: %v", err)
	}
	for _, key := range []string{"id", "nonce", "mseq", "gen", "origin", "started"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("the persisted body must carry %q, got %v", key, raw)
		}
	}
	if raw["id"] != clientID {
		t.Errorf("id = %v, want %q", raw["id"], clientID)
	}
}
