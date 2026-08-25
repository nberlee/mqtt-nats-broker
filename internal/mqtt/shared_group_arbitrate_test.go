// Unit coverage for F-077: a restored shared membership is arbitrated against the record.
//
// The oracle throughout is the OWNERSHIP WRITE ITSELF, counted at the store boundary and
// identified by its own attempt rather than by a total. That is not fussiness: several of
// the mutations this file has to kill produce the same client-visible outcome as the
// correct build — a read instead of a CAS still sees "foreign" when a newer record is
// already there, and a build that arbitrates on every CONNECT still refuses the ones that
// should be refused. Only "how many ownership writes did this CONNECT attempt, and against
// which base" separates them.
package mqtt

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// errMQTTTestDefiniteStoreFailure is a store error that is neither a CAS rejection nor an
// ambiguity: the write definitely did not land, so the primitive must classify it Local.
var errMQTTTestDefiniteStoreFailure = errors.New("mqtt test: record store refused outright")

// testOwnershipWrites counts record-store attempts made while the hook is installed, and
// records the CAS base of each.
//
// It counts ATTEMPTS at the store seam rather than successes, which is what makes it able
// to tell a CAS from a read: replacing the ownership write with a load leaves the count at
// zero while every downstream assertion still passes.
type testOwnershipWrites struct {
	mu    sync.Mutex
	bases []uint64
}

func (w *testOwnershipWrites) add(seq uint64) {
	w.mu.Lock()
	w.bases = append(w.bases, seq)
	w.mu.Unlock()
}

func (w *testOwnershipWrites) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.bases)
}

// testCountRecordStores installs the store-result seam, counts every record write attempt
// and applies fail to each outcome.
//
// Counting and injecting through ONE hook is deliberate: two hooks on the same package-level
// pointer would silently replace each other, and the second installer would win.
func testCountRecordStores(t *testing.T,
	fail func(*JSPubAckResponse, error) (*JSPubAckResponse, error)) *testOwnershipWrites {

	t.Helper()
	w := &testOwnershipWrites{}
	h := func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		sess.mu.Lock()
		base := sess.seq
		sess.mu.Unlock()
		w.add(base)
		if fail != nil {
			return fail(resp, err)
		}
		return resp, err
	}
	mqttTestSessStoreResultHookV.Store(&h)
	t.Cleanup(func() { mqttTestSessStoreResultHookV.Store(nil) })
	return w
}

// ---------------------------------------------------------------------------
// Case 6. The primitive's own verdict, asserted by calling it.
// ---------------------------------------------------------------------------

// TestSharedOwnershipUnresolvedKeepsMutation pins the AMBIGUOUS outcome, and it asserts the
// primitive's RETURNED VERDICT rather than anything downstream.
//
// That is the whole point of the fixture and it took two review rounds to get right.
// saveWithDetail registers the pending write and arms seqUncertain BEFORE the outcome is
// mapped, and saveClassifiedDetail only reports what already happened — so a build that
// maps saveMaybeLanded to Local leaves the refusal, the armed uncertainty, the same
// pendingWrite and the restored state all intact. Every indirect oracle passes against it.
// Only the enum distinguishes them (M7).
//
// The transaction is LEFT OPEN across the call, which is the idiom
// testSaveClassifiedHeld uses for resolveContendedRecordSave: the primitive classifies
// inside its caller's fence in production, and closing it first would test a different
// thing.
func TestSharedOwnershipUnresolvedKeepsMutation(t *testing.T) {
	for _, tc := range []struct {
		name string
		// preArm makes an EARLIER write of this session unresolved, so save() is refused at
		// entry and this attempt is DEFINITE rather than ambiguous.
		preArm bool
		fail   func(*JSPubAckResponse, error) (*JSPubAckResponse, error)
		want   mqttGroupOwnership
	}{
		{
			// A lost response. The request went out and its outcome is unknown — and that
			// is the DEFAULT for any non-CAS store error, deliberately: nothing about a
			// timeout or a refused storage layer proves the write did not land. An earlier
			// revision of this fixture expected Local here and was simply wrong.
			name: "ambiguous store",
			fail: func(*JSPubAckResponse, error) (*JSPubAckResponse, error) {
				return nil, errMQTTTestDefiniteStoreFailure
			},
			want: mqttGroupOwnershipUnresolved,
		},
		{
			// Refused at ENTRY because an earlier write is unresolved. Nothing was sent, so
			// this is the non-CAS route to a DEFINITE failure — and telling it apart from
			// the ambiguous case above is exactly what classifySave's wasUncertain sample
			// exists for.
			name:   "refused at entry while unresolved",
			preArm: true,
			fail: func(resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
				return resp, err
			},
			want: mqttGroupOwnershipLocal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "sharedown")
			sess := e.sess

			// Land one ordinary save first, so sess.seq is a real acknowledged base and the
			// ambiguity below is about THIS write rather than about a session that never
			// had a record.
			tok := sess.beginRecordTx()
			if err := sess.save(tok); err != nil {
				t.Fatalf("baseline save: %v", err)
			}
			if terr := sess.endRecordTx(tok); terr != nil {
				t.Fatalf("endRecordTx: %v", terr)
			}
			sess.mu.Lock()
			base := sess.seq
			sess.mu.Unlock()
			if base == 0 {
				t.Fatal("baseline save left sess.seq at 0, so the CAS below would be a create")
			}

			if tc.preArm {
				// An unresolved earlier write. save() refuses before the store, so the
				// detail comes back zero-valued — and the verdict must be Local, NOT a
				// rejection arm, which would classify a zero base against a real record and
				// dispose of the one object holding this session's pendingWrite.
				sess.mu.Lock()
				sess.seqUncertain = true
				sess.mu.Unlock()
			}

			// The fence is HELD across the call, exactly as it is in production.
			//
			// The store counter wraps the call and is the oracle a READ cannot satisfy:
			// replacing the CAS with a load leaves every verdict below reachable — a newer
			// record still looks foreign — while attempting no write at all. Only "one
			// store was attempted, against this session's acknowledged base" separates
			// arbitration from observation.
			w := testCountRecordStores(t, tc.fail)
			tok2 := sess.beginRecordTx()
			v := sess.proveSharedMembershipOwnership(tok2)
			if tc.preArm {
				// Refused at entry: nothing may reach the store at all.
				if got := w.count(); got != 0 {
					t.Fatalf("a save refused at entry attempted %d stores, want 0", got)
				}
			} else if got := w.count(); got != 1 {
				t.Fatalf("the ownership write attempted %d stores, want exactly 1: 0 means it "+
					"is a read rather than a CAS, and more than 1 means it retried a "+
					"non-contention failure", got)
			}
			if v.outcome != tc.want {
				t.Fatalf("ownership verdict = %v, want %v (err=%v)", v.outcome, tc.want, v.err)
			}
			if v.err == nil {
				t.Fatal("a non-Ours verdict must carry the store error for the refusal text")
			}
			// Foreign and Forked are the only outcomes that license a disposal vocabulary;
			// the others must not carry one, or a caller could dispose on a local failure.
			if v.voc != (mqttDisposalVocabulary{}) {
				t.Fatalf("outcome %v carried a disposal vocabulary %+v", v.outcome, v.voc)
			}
			if terr := sess.endRecordTx(tok2); terr != nil {
				t.Fatalf("endRecordTx: %v", terr)
			}

			// The AMBIGUOUS arm additionally owns the session until it is settled: the
			// mutation is kept and no further write is permitted.
			if tc.want == mqttGroupOwnershipUnresolved && !sess.isSeqUncertain() {
				t.Fatal("an unresolved ownership write must leave the session unresolved")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Case 12/13. The repairable verdict class.
// ---------------------------------------------------------------------------

// TestSharedPrePassRepairableDoesNotPoisonLaterTargets pins the pre-pass rule F-077 adds,
// in BOTH directions.
//
// The rule is stated as a class — terminal unless repairable — rather than as an
// `if absent` at each decision site, because F-076's batching seam took four rounds and
// closed only when the special cases were deleted. So the negative control matters as much
// as the positive one: a chunk-level transport failure is NOT a per-target verdict and must
// still stop the batch (M16 in reverse).
func TestSharedPrePassRepairableDoesNotPoisonLaterTargets(t *testing.T) {
	absent := mqttSharedGroupVerdict{err: errSharedGroupAbsent}
	indet := mqttSharedGroupVerdict{err: errSharedGroupIndeterminate}
	noncanon := mqttSharedGroupVerdict{err: fmt.Errorf("not the configuration this broker provisions")}
	usable := mqttSharedGroupVerdict{cfg: &ConsumerConfig{Durable: "d"}}

	// The pre-pass predicate: absence alone is non-terminal.
	for _, tc := range []struct {
		name     string
		v        mqttSharedGroupVerdict
		terminal bool
	}{
		{"absent is repairable", absent, false},
		{"indeterminate is terminal", indet, true},
		{"non-canonical is terminal", noncanon, true},
		{"usable is not terminal", usable, false},
	} {
		t.Run("pre-pass/"+tc.name, func(t *testing.T) {
			if got := mqttSharedGroupVerdictIsTerminal(tc.v); got != tc.terminal {
				t.Fatalf("mqttSharedGroupVerdictIsTerminal = %v, want %v", got, tc.terminal)
			}
		})
	}

	// The POST-pass predicate: every error ends the restore, absence included. This is the
	// asymmetry, and it is what stops the repairable class leaking into the second reading
	// (M17).
	for _, tc := range []struct {
		name string
		v    mqttSharedGroupVerdict
		ends bool
	}{
		{"absent ends the restore", absent, true},
		{"indeterminate ends the restore", indet, true},
		{"usable does not", usable, false},
	} {
		t.Run("post-pass/"+tc.name, func(t *testing.T) {
			if got := mqttSharedGroupVerdictEndsRestore(tc.v); got != tc.ends {
				t.Fatalf("mqttSharedGroupVerdictEndsRestore = %v, want %v", got, tc.ends)
			}
		})
	}

	// And the filter-failure decision moves with it. A pre-pass absence must leave the
	// filter RESTORABLE, or it never reaches processSubs at all and the repair arm is dead
	// code — which is precisely the defect round 4 found in an earlier revision of this
	// change.
	t.Run("the pre-pass leaves a repairable filter restorable", func(t *testing.T) {
		mk := func() []*mqttFilter {
			return []*mqttFilter{{shared: true, group: "g", filter: "a.topic", qos: 2}}
		}
		sid := mk()[0].sessionKey()

		pre := mk()
		failSharedGroupFilters(pre, map[string]mqttSharedGroupVerdict{sid: absent},
			mqttSharedGroupVerdictIsTerminal)
		if pre[0].qos >= mqttSubAckFailure {
			t.Fatal("the PRE-pass failed a repairable filter, so the repair arm is unreachable")
		}

		post := mk()
		failSharedGroupFilters(post, map[string]mqttSharedGroupVerdict{sid: absent},
			mqttSharedGroupVerdictEndsRestore)
		if post[0].qos < mqttSubAckFailure {
			t.Fatal("the POST-pass must fail an absent filter: the durable is being deleted " +
				"underneath this CONNECT, not waiting to be created")
		}
		if !strings.Contains(post[0].reasonStr, "does not exist") {
			t.Fatalf("post-pass refusal reason %q does not report the durable as absent", post[0].reasonStr)
		}
	})

	// A terminal verdict still stops the batch and judges everything unexamined
	// indeterminate; a repairable one does not. Asserted through the predicate the loop
	// uses, because the loop itself needs a live JetStream to drive.
	t.Run("only a terminal verdict stops the batch", func(t *testing.T) {
		if mqttSharedGroupVerdictIsTerminal(absent) {
			t.Fatal("a repairable verdict must not stop the batch: later healthy targets " +
				"would be judged indeterminate and refuse the CONNECT")
		}
		if !mqttSharedGroupVerdictIsTerminal(indet) {
			t.Fatal("a chunk-level transport failure arrives as indeterminate and must stop " +
				"the batch, or one lost response costs a timeout per remaining chunk")
		}
	})
}

// ---------------------------------------------------------------------------
// Case 9. The create census, as a drift guard.
// ---------------------------------------------------------------------------

// TestSharedGroupCreateCensus holds down the class F-077 arbitrates: every group-durable
// create must live inside processSharedJSConsumer, so a new create site cannot join the
// class without someone deciding whether it needs arbitration.
//
// A DRIFT GUARD, NOT CLOSURE, and the distinction is stated here rather than left for a
// reader to assume. The guard follows a directly-constructed request literal. A create
// whose CreateConsumerRequest arrives through a variable, or through a wrapper, escapes it
// — so this test failing means the class grew, while this test passing does NOT mean the
// class is closed.
func TestSharedGroupCreateCensus(t *testing.T) {
	fns, err := testCreateDurableConsumerCallers()
	if err != nil {
		t.Fatalf("scanning for createDurableConsumer callers: %v", err)
	}
	// Every function that can construct a config carrying a DeliverGroup inline.
	var offenders []string
	for fn, hasGroup := range fns {
		if hasGroup && fn != "processSharedJSConsumer" {
			offenders = append(offenders, fn)
		}
	}
	if len(offenders) != 0 {
		t.Fatalf("group-durable create(s) outside processSharedJSConsumer: %v\n"+
			"F-077 arbitrates ownership after every group create; a create elsewhere is "+
			"unarbitrated. Either move it, or extend the arbitration to cover it and update "+
			"this census.", offenders)
	}
	if !fns["processSharedJSConsumer"] {
		t.Fatal("processSharedJSConsumer no longer constructs a DeliverGroup config inline: " +
			"this guard has stopped following the code it claims to watch")
	}
}

// ---------------------------------------------------------------------------
// Case 11/14. The trigger fact is stack-local and filter-scoped.
// ---------------------------------------------------------------------------

// TestSharedOwnershipTriggerIsStackLocal proves the arbitration trigger cannot outlive the
// operation that raised it.
//
// A fact parked on mqttSession would govern the NEXT operation on the same object, which is
// exactly the stale-generation defect F-078 solved for its packet gate, by scoping it to the
// connection AND a per-session generation. The mutation
// this kills is the stale form specifically: a session field that is reset per invocation
// behaves correctly, so the mutation must omit the reset to be the defect (M3b).
func TestSharedOwnershipTriggerIsStackLocal(t *testing.T) {
	// The trigger is a local in processSubs and a field on the per-invocation result, so
	// the property is structural: assert it where a reviewer can see it fail.
	var jr mqttSharedJoinResult
	if jr.installed {
		t.Fatal("a zero mqttSharedJoinResult must not report installed interest")
	}
	// Two invocations, two results, no shared state.
	a := mqttSharedJoinResult{installed: true}
	b := mqttSharedJoinResult{}
	if b.installed {
		t.Fatal("a fresh result inherited the previous invocation's trigger, which is the " +
			"stale-fact defect: aggregate on the caller's stack, never on the session")
	}
	if !a.installed {
		t.Fatal("the trigger must survive within its own invocation")
	}
	// And the three facts must be independent: created without charged is the repair, and
	// installed without created is the verified-present resume (R1).
	repair := mqttSharedJoinResult{created: true, charged: 0, installed: true}
	if repair.charged != 0 || !repair.created || !repair.installed {
		t.Fatal("the repair shape is created-without-charge, and installed with both")
	}
	verified := mqttSharedJoinResult{created: false, charged: 0, installed: true}
	if verified.created || !verified.installed {
		t.Fatal("R1 installs interest without creating anything: that is what makes the " +
			"trigger 'interest was touched' rather than 'a durable was created'")
	}
}

// ---------------------------------------------------------------------------
// Cases 4 and 5. What a rejection LICENSES the broker to say.
// ---------------------------------------------------------------------------

// TestSharedOwnershipDirectRejectionClassifiesLineage pins the 10071 arm's own
// classification, which is the one arm that never consults the contention resolver.
//
// A CAS rejection says the subject moved. It does NOT say who moved it, and the difference
// is what the client is told: a record that is a legitimate SUCCESSOR of ours is a
// demonstrated takeover and says 0x8E "stop"; a record claiming the same PARENT as ours is
// a sibling — our write was erased by something that is not demonstrably our successor, and
// nobody was demonstrated to own the session — so it says 0x89 "reconnect" and mints a fork
// grant.
//
// Both halves are required. Without the successor case, a build that reported forked for
// ANY moved record would pass the fork case and be wrong about every ordinary takeover;
// without the fork case, collapsing 10071 to a bare takeover survives (which is exactly
// what an earlier run of this mutation showed).
func TestSharedOwnershipDirectRejectionClassifiesLineage(t *testing.T) {
	for _, tc := range []struct {
		name string
		land func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want mqttGroupOwnership
		voc  mqttDisposalVocabulary
	}{
		{"a sibling claiming our parent is FORKED", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttGroupOwnershipForked, mqttDisposalForked},
		{"a successor of our record is a TAKEOVER", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSuccessor(t, e, testMutateSibling)
		}, mqttGroupOwnershipForeign, mqttDisposalTakenOver},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "sharedlineage")
			sess := e.sess

			// TWO acknowledged saves, not one. The first is a parentless CREATE, and
			// classifyRecordLineage requires a non-empty acknowledged parent before it will
			// call anything a sibling — without that guard two unrelated first creates would
			// read as siblings of each other. So a single save leaves this fixture unable to
			// produce a fork at all, and it would pass by conceding.
			for i := 0; i < 2; i++ {
				tok := sess.beginRecordTx()
				if err := sess.save(tok); err != nil {
					t.Fatalf("baseline save %d: %v", i, err)
				}
				if terr := sess.endRecordTx(tok); terr != nil {
					t.Fatalf("endRecordTx: %v", terr)
				}
			}
			sess.mu.Lock()
			ackPWID := sess.pwid
			sess.mu.Unlock()
			if ackPWID == _EMPTY_ {
				t.Fatal("the acknowledged record still has no parent, so no sibling of it " +
					"can be classified forked and this fixture would prove nothing")
			}

			// Another writer lands a record on the subject, so our next CAS is rejected by
			// the store itself rather than by an injected error.
			tc.land(t, e)

			tok2 := sess.beginRecordTx()
			v := sess.proveSharedMembershipOwnership(tok2)
			if terr := sess.endRecordTx(tok2); terr != nil {
				t.Fatalf("endRecordTx: %v", terr)
			}

			if v.outcome != tc.want {
				t.Fatalf("ownership verdict = %v, want %v (err=%v)", v.outcome, tc.want, v.err)
			}
			if v.voc != tc.voc {
				t.Fatalf("disposal vocabulary = %+v, want %+v: this is what the client is "+
					"told, and 0x8E asserts a takeover the record may not support",
					v.voc, tc.voc)
			}
		})
	}
}

// TestSharedPostPassAbsenceStopsTheBatch pins the OTHER half of the pre/post asymmetry:
// the post-pass must stop CLASSIFYING at an absence, not merely fail the filter afterwards.
//
// The refusal is observable without this — applySharedGroupPostPass fails the absent filter
// either way — so a fixture that only checked the CONNACK cannot tell the two apart, and an
// earlier revision of this change wired the batch predicate to the pre-pass rule for both
// passes and no test noticed. What it costs is real though bounded: a CONNECT already doomed
// keeps issuing client-amplifiable lookups while its shared queue interest is installed,
// extending the window in which selected shared QoS 0 messages are dropped behind the
// restore gate.
//
// So the oracle is the number of LOOKUP BATCHES issued, which is the only place the
// difference shows.
func TestSharedPostPassAbsenceStopsTheBatch(t *testing.T) {
	e := testNewInflightEnv(t, "postpassbatch")

	// More targets than one chunk holds, so "stopped" and "walked on" differ in the number
	// of batched lookups issued.
	targets := make([]mqttSharedGroupTarget, 0, mqttInflightLoadBatch*2)
	for i := 0; i < cap(targets); i++ {
		sid := fmt.Sprintf("$share/g/postpassbatch.%d", i)
		targets = append(targets, mqttSharedGroupTarget{
			sid: sid, group: "g", subject: fmt.Sprintf("postpassbatch.%d", i),
			durable: fmt.Sprintf("$MQTT_SHARED_postpassbatch%d", i),
		})
	}

	// Every durable is absent: none of them was ever created.
	var batches int
	e.asm.mu.Lock()
	e.asm.testSharedGroupLookupBatch = func(int) { batches++ }
	e.asm.mu.Unlock()
	defer func() {
		e.asm.mu.Lock()
		e.asm.testSharedGroupLookupBatch = nil
		e.asm.mu.Unlock()
	}()

	// PRE-pass: absence is repairable, so classification walks every chunk.
	batches = 0
	e.asm.classifySharedGroups(targets, mqttSharedGroupVerdictIsTerminal)
	preBatches := batches
	if preBatches < 2 {
		t.Fatalf("the pre-pass issued %d lookup batches over %d targets; the fixture needs "+
			"more than one chunk for 'stopped' and 'walked on' to differ",
			preBatches, len(targets))
	}

	// POST-pass, through THE ENTRY POINT PRODUCTION USES. Calling classifySharedGroups
	// directly with the strict predicate would prove the predicate and not the wiring — and
	// the wiring is exactly what was wrong once and then unpinned once. applySharedGroupPostPass
	// takes targets rather than verdicts precisely so there is one entry point for both.
	batches = 0
	applySharedGroupPostPass(e.asm, e.sess, nil, targets)
	postBatches := batches
	if postBatches != 1 {
		t.Fatalf("the post-pass issued %d lookup batches, want 1: an absence after the "+
			"repair is terminal, so classification must stop rather than pay a lookup per "+
			"remaining chunk on a CONNECT that is already refused", postBatches)
	}
}
