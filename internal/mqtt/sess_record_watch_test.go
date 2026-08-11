package mqtt

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

// errMQTTTestLostResponse is the store-result hook's "landed, response lost" half:
// the record is durable and the caller cannot know it, which is exactly the state
// that arms seqUncertain and leaves the acknowledged lineage pair stale.
var errMQTTTestLostResponse = errors.New("mqtt test: the store landed but its response was lost")

// The session-record watch and its committed-read backstop.
//
// The watch replaced an at-most-once notification that could simply be lost,
// stranding a displaced client on a session another broker owned. What replaced it
// is deliberately NOT a durable channel either — an ephemeral AckNone push consumer
// can duplicate or drop a delivery — so the guarantee lives in the PAIR: the watch
// owns latency, the periodic committed read owns convergence.
//
// Every test here therefore isolates ONE channel through the two seams
// (testSessPersistDrop, testSessRecordBackstopOff). With both live a test meaning to
// prove the watch converged a session would pass just as well if only the backstop
// did, which is the mutually-masking shape these seams exist to prevent.

// watchEnv is a rejection env with BOTH channels off by default, so each test turns
// on exactly the one it is about.
func testNewWatchEnv(t *testing.T, clientID string) *testRejectionEnv {
	t.Helper()
	e := testNewForkEnv(t, clientID)
	e.asm.mu.Lock()
	e.asm.testSessPersistDrop = func(string) bool { return true }
	e.asm.testSessRecordBackstopOff = true
	e.asm.mu.Unlock()
	return e
}

func (e *testRejectionEnv) setWatchDelivery(on bool) {
	e.asm.mu.Lock()
	if on {
		e.asm.testSessPersistDrop = nil
	} else {
		e.asm.testSessPersistDrop = func(string) bool { return true }
	}
	e.asm.mu.Unlock()
}

func (e *testRejectionEnv) setBackstop(on bool) {
	e.asm.mu.Lock()
	e.asm.testSessRecordBackstopOff = !on
	e.asm.mu.Unlock()
}

// runBackstopPass runs one periodic pass, which is the backstop's real cadence.
func (e *testRejectionEnv) runBackstopPass() {
	e.asm.reconcileResidentRecordState(e.s)
}

// reconcileEntry returns a copy of this client's reconciliation accounting.
func (e *testRejectionEnv) reconcileEntry() (present bool, latest, decided uint64, running bool, inFlight uint64) {
	e.asm.mu.RLock()
	defer e.asm.mu.RUnlock()
	st, ok := e.asm.reconcileWM[e.idHash]
	if !ok || st == nil {
		return false, 0, 0, false, 0
	}
	return true, st.evictLatest, st.evictDecided, st.evictRunning, st.inFlight
}

// ---------------------------------------------------------------------------
// Plan test 1/2: the two channels, isolated
// ---------------------------------------------------------------------------

// TestMQTTWatchConvergesWithoutTheBackstop is plan test 1's live half: with the
// backstop OFF, a peer's record write still evicts the displaced resident — so the
// convergence is attributable to the watch alone.
func TestMQTTWatchConvergesWithoutTheBackstop(t *testing.T) {
	t.Parallel()
	e := testNewWatchEnv(t, "wchlive")
	e.setWatchDelivery(true)
	done := e.armEvictObserver()

	e.remoteTakeover(t, "node2", "wchlive")

	if !testAwaitEvictDone(t, done) {
		t.Fatal("the watch must converge a displaced resident with no backstop pass at all")
	}
	if e.resident(e.sess) {
		t.Fatal("the displaced session must leave the residency maps")
	}
}

// TestMQTTBackstopConvergesWithoutTheWatch is plan test 1's dropped half, and the
// one that closes F-036: the delivery is LOST — the state the old at-most-once
// notification left behind for good — and one periodic pass still converges it.
func TestMQTTBackstopConvergesWithoutTheWatch(t *testing.T) {
	t.Parallel()
	e := testNewWatchEnv(t, "wchdrop")
	done := e.armEvictObserver()

	e.remoteTakeover(t, "node2", "wchdrop")

	// Nothing may have happened yet: the watch delivery was dropped, and with it the
	// only prompt signal. This is the precondition that makes the pass below the
	// cause rather than a bystander.
	// The precondition is about the EVICTION trigger, not about the watermark entry:
	// that entry is shared with the no-local expiry reconcile, which legitimately
	// creates one for any record write this manager observes before the test arms its
	// drop. What must be absent is any trigger the eviction side could act on.
	if _, latest, decided, _, _ := e.reconcileEntry(); latest > decided {
		t.Fatalf("precondition: a dropped delivery must leave no eviction trigger (evictLatest=%d evictDecided=%d)", latest, decided)
	}
	if !e.resident(e.sess) {
		t.Fatal("precondition: with the delivery dropped nothing else can have evicted yet")
	}

	e.setBackstop(true)
	e.runBackstopPass()

	if !testAwaitEvictDone(t, done) {
		t.Fatal("the backstop must converge a session whose watch delivery was lost")
	}
	if e.resident(e.sess) {
		t.Fatal("the displaced session must leave the residency maps")
	}
}

// ---------------------------------------------------------------------------
// Plan test 4: self-observation safety (the round-1 blocking case)
// ---------------------------------------------------------------------------

// TestMQTTWatchDoesNotEvictOnOwnAmbiguousLanding is the defect the digest arm
// exists for. A stream watch delivers this broker's OWN writes, which the channel
// it replaced never did. A save whose store LANDED but whose response was lost
// arms seqUncertain and leaves the acknowledged lineage pair stale — so the
// delivered record is strictly past sess.seq, and classifying it by LINEAGE would
// read our own bytes as unrelated and evict the rightful client over its own write.
//
// The positive control is the same staging with a genuinely foreign record at that
// sequence, which MUST evict: without it "nothing was evicted" would also hold for
// a broker that never evicts anything.
func TestMQTTWatchDoesNotEvictOnOwnAmbiguousLanding(t *testing.T) {
	t.Run("our own landing is recognised by digest and never evicts", func(t *testing.T) {
		e := testNewWatchEnv(t, "wchown")
		done := e.armEvictObserver()

		// A save that lands and loses its response: the store succeeds, the caller
		// sees an error it cannot classify, and the session goes unresolved holding
		// the attempt's identity.
		var landedSeq uint64
		restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if sess != e.sess || resp == nil || err != nil {
				return resp, err
			}
			landedSeq = resp.Sequence
			return nil, errMQTTTestLostResponse
		})
		_ = testSessSave(t, e.sess)
		restore()
		if landedSeq == 0 {
			t.Fatal("fixture: the ambiguous save never reached the store")
		}
		e.sess.mu.Lock()
		uncertain := e.sess.seqUncertain
		e.sess.mu.Unlock()
		if !uncertain {
			t.Fatal("fixture: a landed-but-unacknowledged save must leave the session unresolved")
		}

		// The watch delivers that very write back to us.
		e.deliverPersistObservation(t, landedSeq)

		if testAwaitEvictDone(t, done) {
			t.Fatal("a broker must never evict its own client over its own landed write")
		}
		if !e.resident(e.sess) {
			t.Fatal("the session must stay resident: nothing displaced it")
		}
		// The refusal decided nothing, so the trigger stays undecided for the backstop.
		present, latest, decided, _, _ := e.reconcileEntry()
		if !present || latest <= decided {
			t.Fatalf("a digest refusal must leave the trigger undecided (present=%v latest=%d decided=%d)", present, latest, decided)
		}
	})

	t.Run("positive control: a foreign record at that sequence does evict", func(t *testing.T) {
		e := testNewWatchEnv(t, "wchownctl")
		done := e.armEvictObserver()

		var landedSeq uint64
		restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if sess != e.sess || resp == nil || err != nil {
				return resp, err
			}
			landedSeq = resp.Sequence
			return nil, errMQTTTestLostResponse
		})
		_ = testSessSave(t, e.sess)
		restore()
		if landedSeq == 0 {
			t.Fatal("fixture: the ambiguous save never reached the store")
		}
		// Somebody else's record now occupies the subject, above the attempt.
		foreign := e.landRecord(t, func(ps *mqttPersistedSession) {
			ps.Gen = mqttNewSessGen() // a different incarnation: plainly not ours
			ps.WID, ps.PWID = mqttNewRecordWID(), mqttNewRecordWID()
		})
		e.deliverPersistObservation(t, foreign)

		if !testAwaitEvictDone(t, done) {
			t.Fatal("a genuinely foreign record must still evict while an attempt is unresolved")
		}
	})
}

// TestMQTTBackstopUnusableLoadCannotEvictOwnLanding is the holistic review's first
// finding. The backstop deliberately passes NO record, so the Origin filter cannot
// screen our own write, and the pass's seqUncertain check happens BEFORE the read —
// a save can land and lose its response in between. If the worker's own load then
// fails, there are no bytes for the digest arm, and conceding to the takeover
// vocabulary would evict the rightful client over its own write on the one occasion
// the digest cannot speak.
//
// The control is the same unusable load with NO unresolved attempt, which must still
// evict: the license rule is unchanged for the case the observation can only have
// come from a peer.
func TestMQTTBackstopUnusableLoadCannotEvictOwnLanding(t *testing.T) {
	t.Run("an unresolved attempt makes an unusable load refuse", func(t *testing.T) {
		e := testNewWatchEnv(t, "wchblind")
		done := e.armEvictObserver()

		var landedSeq uint64
		restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if sess != e.sess || resp == nil || err != nil {
				return resp, err
			}
			landedSeq = resp.Sequence
			return nil, errMQTTTestLostResponse
		})
		_ = testSessSave(t, e.sess)
		restore()
		if landedSeq == 0 {
			t.Fatal("fixture: the ambiguous save never reached the store")
		}

		// Delete the record so the worker's load finds nothing: the license path with
		// no bytes at all, which is what a transient read failure looks like here.
		if err := e.asm.jsa.deleteMsg(mqttSessStreamName, landedSeq, true); err != nil {
			t.Fatalf("delete the record to force an unusable load: %v", err)
		}
		e.deliverPersistObservation(t, landedSeq)

		if testAwaitEvictDone(t, done) {
			t.Fatal("with an unresolved attempt and no bytes to compare, the worker must refuse rather than concede")
		}
		if !e.resident(e.sess) {
			t.Fatal("the rightful client must not be evicted over its own write")
		}
	})

	t.Run("control: no unresolved attempt, an unusable load still evicts", func(t *testing.T) {
		e := testNewWatchEnv(t, "wchblindctl")
		done := e.armEvictObserver()

		e.sess.mu.Lock()
		seq := e.sess.seq
		uncertain := e.sess.seqUncertain
		e.sess.mu.Unlock()
		if uncertain {
			t.Fatal("fixture: this half needs a session with nothing outstanding")
		}
		// An observation above our sequence that no record backs: the license rule.
		e.deliverPersistObservation(t, seq+5)

		if !testAwaitEvictDone(t, done) {
			t.Fatal("the license rule is unchanged when no local write could be the trigger")
		}
	})
}

// ---------------------------------------------------------------------------
// Plan test 3: a refused eviction is re-fired, not lost
// ---------------------------------------------------------------------------

// TestMQTTWatchRefusedEvictionIsRefired pins the other half of "an eviction can be
// lost entirely". A worker that refuses because the ownership gate is HELD decides
// nothing; before this change its terminal cleanup deleted the entry, so the
// observation was gone for good and a quiet session stayed displaced forever. Now
// the entry survives while the session is resident, the trigger stays ahead of
// evictDecided, and the backstop re-fires it once the gate frees.
//
// The refusal is staged on the gate rather than on an unresolved write on purpose:
// the periodic pass resolves an ambiguity BEFORE it reaches the ownership backstop,
// so an uncertainty-shaped refusal would be settled by the same pass and could not
// show the re-fire at all.
func TestMQTTWatchRefusedEvictionIsRefired(t *testing.T) {
	t.Parallel()
	e := testNewWatchEnv(t, "wchrefire")
	done := e.armEvictObserver()

	// A record that plainly is not ours, landed above our acknowledged sequence.
	succ := e.landRecord(t, func(ps *mqttPersistedSession) {
		ps.Gen = mqttNewSessGen()
		ps.WID, ps.PWID = mqttNewRecordWID(), mqttNewRecordWID()
	})

	e.holdSessionGate()
	e.deliverPersistObservation(t, succ)
	if testAwaitEvictDone(t, done) {
		t.Fatal("a worker that never acquired the gate cannot have evicted anything")
	}
	if !e.resident(e.sess) {
		t.Fatal("the session must still be resident: nothing decided its fate")
	}
	present, latest, decided, running, inFlight := e.reconcileEntry()
	if !present {
		t.Fatal("the entry must survive a refusal while the session is resident: it carries the trigger")
	}
	if running || inFlight != 0 {
		t.Fatalf("a completed worker must leave no flag or counter behind (running=%v inFlight=%d)", running, inFlight)
	}
	if latest <= decided {
		t.Fatalf("a gate-refused worker decides nothing (latest=%d decided=%d)", latest, decided)
	}

	// The re-fire needs no new observation: the accounting alone is the signal.
	e.releaseSessionGate()
	e.setBackstop(true)
	e.runBackstopPass()
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the backstop must re-fire a trigger no decision ever consumed")
	}
	if e.resident(e.sess) {
		t.Fatal("the re-fired eviction must apply")
	}
}

// ---------------------------------------------------------------------------
// Plan test 9: a record that is GONE is displacement evidence too
// ---------------------------------------------------------------------------

// TestMQTTBackstopArmsOnDeletedRecord covers the shape no sequence comparison can
// see: a peer's teardown deleted the record outright, so there is no newer sequence
// to compare against and the moved-record trigger cannot fire. The backstop arms the
// ownership marker instead and the existing rejection settlement converges it.
//
// The control is a session whose record is PRESENT and unmoved: it must arm nothing,
// which is what distinguishes "keys on absence" from "arms on every backstop read".
func TestMQTTBackstopArmsOnDeletedRecord(t *testing.T) {
	t.Parallel()
	t.Run("deleted record arms the ownership marker", func(t *testing.T) {
		e := testNewWatchEnv(t, "wchgone")
		e.setBackstop(true)

		e.sess.mu.Lock()
		seq := e.sess.seq
		e.sess.mu.Unlock()
		if seq == 0 {
			t.Fatal("fixture: the session must hold an acknowledged record")
		}
		if err := e.asm.jsa.deleteMsg(mqttSessStreamName, seq, true); err != nil {
			t.Fatalf("delete the session record out from under the resident session: %v", err)
		}

		if e.markerPending(e.sess) {
			t.Fatal("precondition: nothing may have armed the marker yet")
		}
		e.runBackstopPass()
		if !e.markerPending(e.sess) {
			t.Fatal("an absent record for a resident session at a non-zero sequence must arm the ownership marker")
		}

		// And the settlement converges it: the probe's CAS cannot be accepted against
		// an empty subject, which is the foreign verdict the existing table already
		// returns for a record that is gone.
		e.runBackstopPass()
		if e.resident(e.sess) {
			t.Fatal("the rejection settlement must dispose a session whose record is gone")
		}
	})

	t.Run("control: a present, unmoved record arms nothing", func(t *testing.T) {
		e := testNewWatchEnv(t, "wchpresent")
		e.setBackstop(true)

		e.runBackstopPass()
		if e.markerPending(e.sess) {
			t.Fatal("the backstop must not arm on every read: the record is present and unmoved")
		}
		if !e.resident(e.sess) {
			t.Fatal("nothing may have disposed a healthy session")
		}
	})
}

// ---------------------------------------------------------------------------
// Plan test 10: the pruning rule's identity conjunct
// ---------------------------------------------------------------------------

// TestMQTTReconcileEntryPruningIsIdentitySafe stages the ONE shape in which only
// pointer identity can prevent a wrong delete: a stale holder still carries the old
// entry, the map already holds a REPLACEMENT entry for the same hash, no session is
// resident, and no worker is in flight — so the residency and inFlight conjuncts
// both permit the delete and cannot mask a missing identity check.
func TestMQTTReconcileEntryPruningIsIdentitySafe(t *testing.T) {
	t.Parallel()
	e := testNewWatchEnv(t, "wchprune")
	hash := e.idHash

	stale := &sessionReconcileState{evictLatest: 7}
	replacement := &sessionReconcileState{evictLatest: 9}

	e.asm.mu.Lock()
	e.asm.removeSession(e.sess, false)
	e.asm.reconcileWM[hash] = replacement
	e.asm.pruneReconcileEntryLocked(hash, stale)
	got := e.asm.reconcileWM[hash]
	e.asm.mu.Unlock()

	if got != replacement {
		t.Fatal("a stale holder's prune must not delete the entry a replacement installed")
	}

	// And the rule still prunes when the pointer IS the current one.
	e.asm.mu.Lock()
	e.asm.pruneReconcileEntryLocked(hash, replacement)
	_, present := e.asm.reconcileWM[hash]
	e.asm.mu.Unlock()
	if present {
		t.Fatal("with no session resident and no worker in flight the current entry must be pruned")
	}
}

// TestMQTTReconcileEntrySurvivesNoLocalWorker pins the interaction that used to
// erase a resident session's trigger: the no-local expiry worker and the eviction
// worker share ONE entry, and that worker's cleanup deleted it on "no worker in
// flight" alone.
func TestMQTTReconcileEntrySurvivesNoLocalWorker(t *testing.T) {
	t.Parallel()
	e := testNewWatchEnv(t, "wchshared")

	// A trigger the eviction side owns, left undecided.
	e.asm.mu.Lock()
	st := &sessionReconcileState{evictLatest: 42}
	e.asm.reconcileWM[e.idHash] = st
	st.inFlight++
	e.asm.mu.Unlock()

	// The no-local worker runs and completes against the same entry.
	e.asm.reconcileRemoteSessionExpiry(e.s, e.idHash, 1, st)

	if present, latest, decided, _, _ := e.reconcileEntry(); !present || latest <= decided {
		t.Fatal("the no-local worker must not erase the resident session's undecided eviction trigger")
	}
}

// ---------------------------------------------------------------------------
// Plan test 7: the consumer's delivery policy
// ---------------------------------------------------------------------------

// TestMQTTSessRecordWatchConsumerConfig pins DeliverNew directly. History concerns
// no resident session — one can only exist on this broker after the manager, and
// therefore the watch, exists — while DeliverAll would replay every record ever
// written into the no-local expiry reconcile at every manager start.
func TestMQTTSessRecordWatchConsumerConfig(t *testing.T) {
	t.Parallel()
	ccr := mqttSessRecordWatchConsumerConfig("", "$MQTT.sub.test")
	if ccr.Stream != mqttSessStreamName {
		t.Fatalf("the watch must consume the session stream, got %q", ccr.Stream)
	}
	if ccr.Config.DeliverPolicy != DeliverNew {
		t.Fatalf("deliver policy must be DeliverNew, got %v", ccr.Config.DeliverPolicy)
	}
	if ccr.Config.AckPolicy != AckNone {
		t.Fatalf("ack policy must be AckNone, got %v", ccr.Config.AckPolicy)
	}
	if ccr.Config.FilterSubject != mqttSessStreamSubjectPrefix+"*" {
		t.Fatalf("the filter must name the record space exactly, got %q", ccr.Config.FilterSubject)
	}
	if ccr.Config.InactiveThreshold != mqttSessRecordWatchInactiveThreshold {
		t.Fatalf("the consumer must carry its own inactivity threshold, got %v", ccr.Config.InactiveThreshold)
	}
	// With a domain configured the token carries its own separator, so the filter is
	// still exactly one trailing token.
	if got := mqttSessRecordWatchConsumerConfig("dom.", "$MQTT.sub.test").Config.FilterSubject; got != mqttSessStreamSubjectPrefix+"dom.*" {
		t.Fatalf("the domain form must stay exact, got %q", got)
	}
}

// TestMQTTSessRecordSubjectParsing covers the callback's own attribution: a record
// subject names exactly one client, and a deeper subject names none.
func TestMQTTSessRecordSubjectParsing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, domainTk, subject, want string
	}{
		{"no domain", "", mqttSessStreamSubjectPrefix + "abc", "abc"},
		{"with domain", "dom.", mqttSessStreamSubjectPrefix + "dom.abc", "abc"},
		{"wrong prefix", "", "$MQTT.other.abc", ""},
		{"domain mismatch", "dom.", mqttSessStreamSubjectPrefix + "other.abc", ""},
		{"deeper subject is attributed to nobody", "", mqttSessStreamSubjectPrefix + "abc.def", ""},
		{"empty hash", "", mqttSessStreamSubjectPrefix, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mqttSessRecordHashFromSubject(tc.domainTk, tc.subject); got != tc.want {
				t.Fatalf("hash for %q: got %q, want %q", tc.subject, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Plan test 12 / MA12: an undecodable record is still an observation
// ---------------------------------------------------------------------------

// TestMQTTWatchFeedsTriggerOnUndecodableRecord pins that a record the callback
// cannot decode still spawns the decision. Returning early there would discard the
// trigger for exactly the records most likely to matter; what a failed decode costs
// is only the own-write spawn filter, and the worker already has an
// unusable-record arm that decides at the observation level.
func TestMQTTWatchFeedsTriggerOnUndecodableRecord(t *testing.T) {
	t.Parallel()
	e := testNewWatchEnv(t, "wchgarbage")
	e.setWatchDelivery(true)
	done := e.armEvictObserver()

	e.sess.mu.Lock()
	seq := e.sess.seq
	e.sess.mu.Unlock()

	e.deliverThroughWatchCallback(t, seq+1, []byte("{not json"))

	if !testAwaitEvictDone(t, done) {
		t.Fatal("an undecodable record must still drive a decision: the observation is the license")
	}
}

// TestMQTTWatchSkipsOwnOriginForSpawn pins the fast-path filter and, with it, the
// reason the backstop must NOT apply the same filter: Origin proves no authorship,
// so the skip may only ever cost latency.
func TestMQTTWatchSkipsOwnOriginForSpawn(t *testing.T) {
	t.Parallel()
	e := testNewWatchEnv(t, "wchorigin")
	e.setWatchDelivery(true)

	e.sess.mu.Lock()
	seq := e.sess.seq
	e.sess.mu.Unlock()

	// A forbidden worker has to be caught by its ADMISSION, not by waiting to see
	// whether one shows up.
	//
	// Three weaker versions of this assertion were tried and each lost the evidence
	// it was reading. Presence of a reconcileWM entry is not this filter's business:
	// the no-local-session expiry branch creates one during setup, before the session
	// is registered in sessByHash, and pruneReconcileEntryLocked keeps it while the
	// session is resident — asserting absence failed about one run in eight with the
	// filter working perfectly. The entry's counters are worse: a worker admitted by
	// a broken filter EVICTS the session, and its cleanup then prunes the entry
	// precisely BECAUSE the session is no longer resident, so every field reads back
	// zero. And a timed wait on the completion seam is not a negative at all — a
	// worker still inside its JetStream load after the deadline lets residency look
	// intact here and then satisfies the backstop's own assertion below.
	//
	// Admission is synchronous: evictRunning and inFlight are set under as.mu before
	// the goroutine is launched, and the delivery is an ordinary call, so by the time
	// it returns the decision is already recorded. Parking any worker before its
	// first load is what stops it completing and erasing that record before it is
	// read.
	_, release := e.parkEvictWorker(t, false)

	body, err := json.Marshal(&mqttPersistedSession{ID: e.clientID, Origin: e.asm.jsa.id})
	if err != nil {
		t.Fatalf("marshal the record: %v", err)
	}
	e.deliverThroughWatchCallback(t, seq+1, body)

	e.asm.mu.RLock()
	var evictRunning bool
	var evictLatest uint64
	if st := e.asm.reconcileWM[e.idHash]; st != nil {
		evictRunning, evictLatest = st.evictRunning, st.evictLatest
	}
	_, resident := e.asm.sessByHash[e.idHash]
	e.asm.mu.RUnlock()

	if evictRunning || evictLatest != 0 {
		t.Fatalf("a record this broker wrote must not wake an eviction worker: "+
			"evictRunning=%v evictLatest=%d", evictRunning, evictLatest)
	}
	if !resident {
		t.Fatal("the own-origin record evicted the session that wrote it")
	}

	// Release the barrier and disarm it before the backstop half, whose worker must
	// run to completion. Closing release first makes the seam a no-op for any worker
	// that reaches it in between.
	close(release)
	e.asm.mu.Lock()
	e.asm.testEvictPauseBeforeLoad = nil
	e.asm.mu.Unlock()

	// The backstop refuses that shortcut, which is what keeps an Origin collision a
	// latency cost rather than a correctness one.
	e.setBackstop(true)
	done := e.armEvictObserver()
	foreign := e.landRecord(t, func(ps *mqttPersistedSession) {
		ps.Origin = e.asm.jsa.id // still claims to be ours
		ps.Gen = mqttNewSessGen()
		ps.WID, ps.PWID = mqttNewRecordWID(), mqttNewRecordWID()
	})
	_ = foreign
	e.runBackstopPass()
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the backstop must converge a moved record whatever Origin claims")
	}
}

// ---------------------------------------------------------------------------
// Initialization is failure-atomic for the per-broker ephemeral consumers
// ---------------------------------------------------------------------------

// TestMQTTFailedInitLeavesNoEphemeralConsumers pins the leak class the holistic review
// found twice, in two places. Both per-broker consumers are UNIQUELY named, so nothing
// later adopts or replaces one; manager initialization tears down only local
// subscriptions; and every MQTT connection retries the whole initialization. A
// consumer created before a step that fails is therefore deleted by nothing, and a
// persistent misconfiguration accumulates one per attempt.
//
// The test forces the watch create to fail and retries initialization several times.
// What it asserts is that the counts do not GROW — the property that distinguishes a
// stray consumer (which the inactivity threshold eventually reaps) from an unbounded
// per-retry accumulation (which it cannot keep up with).
func TestMQTTFailedInitLeavesNoEphemeralConsumers(t *testing.T) {
	e := testNewWatchEnv(t, "wchinitleak")

	countConsumers := func() int {
		t.Helper()
		sess, err := e.asm.jsa.consumerList(mqttSessStreamName)
		if err != nil {
			t.Fatalf("list session-stream consumers: %v", err)
		}
		ret, err := e.asm.jsa.consumerList(mqttRetainedMsgsStreamName)
		if err != nil {
			t.Fatalf("list retained-stream consumers: %v", err)
		}
		return len(sess) + len(ret)
	}

	before := countConsumers()

	restore := setMQTTTestWatchConsumerCreateFail(true)
	defer restore()

	// Each attempt provisions the retained consumer and then fails on the watch, which
	// is exactly the window the rollback covers.
	for i := 0; i < 3; i++ {
		srv := &Server{nrc: e.s.nrc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: "initfail"}
		srv.running.Store(true)
		srv.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
		c := srv.createInternalAccountClient()
		if _, err := srv.getOrCreateMQTTAccountSessionManager(c); err == nil {
			close(srv.quitCh)
			t.Fatal("fixture: initialization must fail while the watch create is forced to fail")
		}
		close(srv.quitCh)
	}

	after := countConsumers()
	if after > before {
		t.Fatalf("a failed initialization must leave no ephemeral consumer behind: %d before, %d after three attempts", before, after)
	}
	restore()

	// The AMBIGUOUS half: the watch create LANDS and its response is lost, so the
	// consumer exists under exactly the name the request carried. An error is not
	// proof that nothing was created, which is why the rollback deletes the failing
	// create's own name rather than only the previous consumer's.
	var landed atomic.Bool
	hookRestore := setMQTTTestConsumerCreateResultHook(func(stream, name string, err error) error {
		if stream == mqttSessStreamName && err == nil {
			landed.Store(true)
			return errMQTTTestLostResponse
		}
		return err
	})
	defer hookRestore()

	for i := 0; i < 3; i++ {
		srv := &Server{nrc: e.s.nrc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: "initambig"}
		srv.running.Store(true)
		srv.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
		c := srv.createInternalAccountClient()
		if _, err := srv.getOrCreateMQTTAccountSessionManager(c); err == nil {
			close(srv.quitCh)
			t.Fatal("fixture: initialization must fail when the watch create's response is lost")
		}
		close(srv.quitCh)
	}
	if !landed.Load() {
		t.Fatal("fixture: the ambiguous create never ran, so this half proves nothing")
	}
	if ambiguous := countConsumers(); ambiguous > before {
		t.Fatalf("a create whose response was lost must still be rolled back: %d before, %d after three attempts", before, ambiguous)
	}
}
