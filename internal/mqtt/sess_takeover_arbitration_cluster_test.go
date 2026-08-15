//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

// F-029's arbitration over REAL replicated JetStream.
//
// Every other test of the conditional create runs against one embedded stream, where the
// expected-last-subject-sequence check is a single-writer store check. That is NOT the
// path that matters here: on a replicated stream the check runs in
// checkMsgHeadersPreClusteredProposal, BEFORE the proposal, against applied state plus an
// in-process map of accepted expected-subject proposals — a different mechanism from the
// single-writer store check, and the one a real deployment runs. Whether a given loser is
// refused by the applied state or by that map is an interleaving no test controls; what
// these assert is the property itself, on the replicated path.
//
// So these assert on a Replicas=3 $MQTT_sess with two real brokers in front of it.

// testClusterFreshSession builds the object a broker holds after finding NO record for a
// client: sequence 0, a fresh incarnation, no lineage — what both sides of a create x
// create race start from.
func testClusterFreshSession(t *testing.T, s *Server, clientID string) *mqttSession {
	t.Helper()
	asm := s.mqtt.sessmgr.sessions[globalAccountName]
	if asm == nil {
		t.Fatal("the broker has no account session manager yet")
	}
	sess := mqttSessionCreate(&asm.jsa, clientID, getHash(clientID),
		mqttNewSessGen(), 0, _EMPTY_, _EMPTY_, s.getOpts())
	sess.asm = asm
	sess.domainTk = asm.domainTk
	return sess
}

// TestMQTTTakeoverCreateRaceReplicated is B1 on the path B1 is actually about.
//
// Two brokers both find no record for one client and both create. Before F-029 the create
// carried no CAS header, so both landed, the later apply won under MaxMsgsPer:1, and the
// earlier broker's acknowledged record was erased with no rejection anywhere — a silent
// loss of a session's subscriptions, consumers and lifetime grant. With the header at
// sequence 0 exactly one create is admitted and the other is definitively rejected.
//
// The discriminators are all three together: exactly one save succeeds, the loser's error
// is a definite CAS rejection (so it KNOWS, rather than believing it owns the session),
// and the survivor's record is the winner's own bytes.
func TestMQTTTakeoverCreateRaceReplicated(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b0 := c.startBroker(t, 0)
	defer b0.cleanup()
	b1 := c.startBroker(t, 1)
	defer b1.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	const cid = "cluster-create-race"
	sessA := testClusterFreshSession(t, b0.s, cid)
	sessB := testClusterFreshSession(t, b1.s, cid)

	// Both CAS on "the subject must currently be empty", released together from a
	// barrier.
	//
	// WHAT THIS DOES NOT PROVE, stated because an earlier revision of this comment
	// claimed it: closing the barrier only makes both goroutines RUNNABLE. Either can
	// publish and apply before the other is scheduled, and there is no seam that holds a
	// proposal between clustered admission and apply — so this test does not put two
	// proposals inside the pre-proposal window, and **simultaneous pre-proposal
	// arbitration remains untested here**. It would pass on the applied-state rejection
	// alone.
	//
	// The three-way split of the evidence, since that is what keeps the claim honest:
	//
	//   - THIS repository proves the broker-visible PROPERTY on the real Replicas=3 path:
	//     one winner, a DEFINITE loser, and the winner's bytes surviving.
	//   - NATS SERVER owns and tests the pre-proposal MECHANISM. The in-process rejection
	//     is jetstream_batching.go:779 (v2.14.5); its direct guard coverage is
	//     jetstream_batching_test.go:1576-1586 ("expect-per-subj-in-process"), and
	//     jetstream_cluster_4_test.go:5158-5164 seeds expectedPerSubjectInProcess on an R3
	//     stream and requires the publish to be rejected with the constant-sequence error.
	//   - NEITHER makes this repository able to force that internal interleaving
	//     deterministically, which is a limit on what a test here can OBSERVE — not a gap
	//     in the broker's behaviour, and so not a conformance residual.
	//
	// Which code the loser gets is left unasserted: pinning 10071 or 10164 would be
	// asserting a scheduling accident.
	var (
		dA, dB     mqttSessSaveDetail
		errA, errB error
		start      = make(chan struct{})
		done       sync.WaitGroup
	)
	done.Add(2)
	go func() { defer done.Done(); <-start; dA, errA = testSaveDetail(t, sessA) }()
	go func() { defer done.Done(); <-start; dB, errB = testSaveDetail(t, sessB) }()
	close(start)
	done.Wait()

	won, lost := 0, 0
	for _, e := range []error{errA, errB} {
		if e == nil {
			won++
		} else {
			lost++
		}
	}
	if won != 1 || lost != 1 {
		t.Fatalf("exactly one create may be admitted from an empty subject, got %d won / %d lost "+
			"(A=%v B=%v): two winners means the record was erased silently", won, lost, errA, errB)
	}

	// The loser must KNOW. A create that failed for some other reason would leave the
	// broker believing it owns a session it never persisted.
	loserDetail, loserErr := dA, errA
	winner := sessB
	if errB != nil {
		loserDetail, loserErr, winner = dB, errB, sessA
	}
	if !mqttIsRecordCASRejection(loserDetail.rawErr) {
		t.Fatalf("the losing create must be a definite CAS rejection, got %v (%v)",
			loserDetail.rawErr, loserErr)
	}

	// And the surviving record is the winner's, at the winner's own sequence.
	asm := b0.s.mqtt.sessmgr.sessions[globalAccountName]
	smsg, lerr := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid))
	if lerr != nil || smsg == nil {
		t.Fatalf("the winner's record must be on the subject: %v", lerr)
	}
	winner.mu.Lock()
	wonSeq, wonWID := winner.seq, winner.wid
	winner.mu.Unlock()
	if smsg.Sequence != wonSeq {
		t.Fatalf("the surviving record is at %v, the winner was acknowledged at %v",
			smsg.Sequence, wonSeq)
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		t.Fatalf("decode the surviving record: %v", uerr)
	}
	if ps.WID != wonWID {
		t.Fatalf("the surviving record carries identity %q, the winner minted %q: "+
			"the winner's bytes were replaced", ps.WID, wonWID)
	}
}

// TestMQTTTakeoverSupersededClaimReplicated is the stomp, on replicated JetStream.
//
// Broker A owns the session. Broker B holds a stale view and runs a clean-start discard:
// its teardown claim is superseded, and before F-029 it went on to an accept save framed
// at sequence 0 — which landed unconditionally and erased A's record inside one leader
// term. Here the claim must report superseded and the create that used to follow it must
// be refused, with A's record byte-identical afterwards.
func TestMQTTTakeoverSupersededClaimReplicated(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b0 := c.startBroker(t, 0)
	defer b0.cleanup()
	b1 := c.startBroker(t, 1)
	defer b1.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	const cid = "cluster-superseded"
	asm0 := b0.s.mqtt.sessmgr.sessions[globalAccountName]

	// B's view, taken before A moves the record.
	sessB := testClusterFreshSession(t, b1.s, cid)
	if _, err := testSaveDetail(t, sessB); err != nil {
		t.Fatalf("B's first create: %v", err)
	}

	// A takes the session over off that same base.
	sessA := testClusterFreshSession(t, b0.s, cid)
	sessB.mu.Lock()
	base := sessB.seq
	sessB.mu.Unlock()
	sessA.mu.Lock()
	sessA.seq = base
	sessA.mu.Unlock()
	if _, err := testSaveDetail(t, sessA); err != nil {
		t.Fatalf("A's takeover: %v", err)
	}
	before, beforeSeq := testClusterRecord(t, asm0, cid)

	// B's discard now claims a base that is no longer current.
	if err := sessB.clear(false); !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("B's claim must be reported superseded, got %v", err)
	}
	// clear() re-minted B at sequence 0 without detaching it, which is exactly the state
	// that used to produce the unconditional create.
	sessB.mu.Lock()
	reset := sessB.seq
	sessB.mu.Unlock()
	if reset != 0 {
		t.Fatalf("clear() must re-mint at sequence 0, got %v", reset)
	}
	if _, err := testSaveDetail(t, sessB); err == nil {
		t.Fatal("the superseded broker's create must be REJECTED: landing it erases A's record")
	}

	after, afterSeq := testClusterRecord(t, asm0, cid)
	if afterSeq != beforeSeq || after.WID != before.WID {
		t.Fatalf("A's record was displaced: %q@%v -> %q@%v",
			before.WID, beforeSeq, after.WID, afterSeq)
	}
}

func testClusterRecord(t *testing.T, asm *mqttAccountSessionManager, cid string) (*mqttPersistedSession, uint64) {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid))
	if err != nil || smsg == nil {
		t.Fatalf("loadSessionMsg: %v", err)
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		t.Fatalf("decode the record: %v", uerr)
	}
	return ps, smsg.Sequence
}
