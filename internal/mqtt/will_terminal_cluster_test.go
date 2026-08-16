//go:build mqttport_fulltests

// The bridge-level sentinel for F-030's terminal Will state, over a genuine 3-node
// embedded JetStream cluster and the production CAS helpers.
//
// It is an INTEGRATION SENTINEL, not sole proof that the internal window was reached:
// the deterministic dependency canary for the supported server's refusal of a write
// whose per-subject expectation read a base a committed `done` has replaced lives in
// test/depproof, inside an isolated nats-server checkout. What this test adds is that
// the bridge's own writes go through that contract - both of them conditional - and
// that a refused reclaim leaves the terminal record standing and the Will unpublished.
//
// The paired mutation is named in the test: replacing the conditional reclaim with the
// unconditional store the negative pins use must make "no publication" fail.

package mqtt

import (
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nuid"
)

// TestMQTTWillOutboxReplicatedTerminalDoneStands: a licensed record is finalized to
// `done` across a real stream-leader transition, and a claimant write conditioned on
// the pre-`done` base is then refused. The terminal record stands, the scan reaps it,
// and nothing is published.
//
// Named to be picked up by the `^TestMQTTWillOutboxReplicated` filter the CI cluster
// job and `make cluster-failover-tests` use. Ungated: it drives one stepdown rather
// than killing a node, so it costs about what the R3 smoke test costs.
func TestMQTTWillOutboxReplicatedTerminalDoneStands(t *testing.T) {
	// Park the background claim loop's ticker beyond the test rather than signalling
	// it to stop: the record below is deliberately left with an EXPIRED lease, which
	// is exactly what the loop reclaims, and a reclaim by the loop would publish the
	// Will while the test is still asserting. Signalling the dispatch gate closed
	// would ALSO refuse the publisher worker, which is the very thing the paired
	// mutation has to be able to reach. Set before the broker starts, so the loop
	// never has a short tick; its one initial pass runs over an empty outbox.
	prevInterval := mqttWillClaimInterval
	mqttWillClaimInterval = 10 * time.Minute
	t.Cleanup(func() { mqttWillClaimInterval = prevInterval })

	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	asm := testMQTTGetGlobalASM(t, b.s)

	// Assert on the EXTERNAL act, not on what the record ends up saying.
	pubs := testCapturePublishes(t, asm)

	clientID := "terminal-" + nuid.Next()
	rec := testWillFor(asm, clientID, nuid.Next(), testFarFuture(), 1)
	subject, pendingSeq := testSeedWill(t, asm, rec)
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// The record the interleaving starts from: licensed to publish, its lease already
	// expired so a recovering claimant would EXECUTE the persisted decision. Written
	// with the production claim CAS at the pending record's sequence.
	licensed := *rec
	licensed.ClaimOwner = "peer-a"
	licensed.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
	testLicenseWill(&licensed, mqttWillPublishing, mqttWillDecisionPublish, 1)
	licensedSeq, err := asm.storeWillClaimCAS(subject, pendingSeq, &licensed)
	if err != nil {
		t.Fatalf("license the record: %v", err)
	}

	// A's ACTUAL external publication, through the production publish path. This is
	// what `done` is about to record, and without it this test could only observe a
	// FIRST publication later and mislabel it a republication.
	if err := asm.publishWillRecord(b.s, &licensed); err != nil {
		t.Fatalf("publish the licensed Will: %v", err)
	}
	if n := pubs.Load(); n != 1 {
		t.Fatalf("the licensed Will must have been published exactly once before it is "+
			"finalized, got %d", n)
	}

	// A real stream-leader transition, through the public JetStream endpoint. This is
	// the boundary the whole finding is about: it clears the in-process per-subject
	// state that makes two same-base writes impossible within one term.
	if _, err := b.nc.Request(
		fmt.Sprintf(natsserver.JSApiStreamLeaderStepDownT, mqttWillQStreamName), nil, 5*time.Second,
	); err != nil {
		t.Fatalf("stream leader stepdown: %v", err)
	}
	c.waitStreamLeader(t, testGlobalAccount, mqttWillQStreamName)
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// The publisher's terminal commit, through the production finalize path, so the
	// `done` record is written by the code that really writes it.
	finalized := licensed
	doneSeq, ok := asm.finalizeWillPublish(b.s, subject, &finalized, licensedSeq)
	if !ok || doneSeq == 0 {
		t.Fatalf("finalize to done: ok=%v seq=%d", ok, doneSeq)
	}

	// The reclaim, from the base the licensed record sat at - the write this finding
	// says must never take the record back to publishing. It goes through the same
	// production CAS helper the claim protocol uses.
	//
	// MUTATION (`test/depproof/run.sh --bridge`): the reclaim is replaced by the
	// unconditional store the negative pins use, and the publication assertion below
	// must then fail with a SECOND publication of a Will already published once. That
	// is what makes this test evidence about the server's contract rather than about
	// the bytes, and the harness scores the mutation only on that second publication.
	reclaim := licensed
	if _, rerr := asm.storeWillClaimCAS(subject, licensedSeq, &reclaim); rerr == nil {
		t.Fatal("a claimant write conditioned on the superseded base was ACCEPTED: the " +
			"terminal record is regressible and F-030's consequence is live")
	} else {
		// Either rejection is correct here and both are the server refusing the write:
		// a wrong-last-sequence rejection, or the in-process contention code, which
		// this repository resolves by re-reading rather than classifying.
		t.Logf("the superseded-base reclaim was refused: %v", rerr)
	}

	// The terminal record is the one standing, at the sequence the finalize returned.
	//
	// Reported rather than fatal, deliberately: the assertion this sentinel exists for
	// is the one below, about the external act, and a fatal here would stop the test
	// before it -- which is precisely how a sentinel ends up proving only that the
	// stored bytes look right. Under the mutation named above BOTH fire, and the
	// publication is the one that states the consequence.
	cur, curSeq := testReloadWill(t, asm, subject)
	if cur == nil || cur.State != mqttWillDone {
		t.Errorf("the terminal record must stand, got %+v", cur)
	} else if curSeq != doneSeq {
		t.Errorf("the terminal record moved: want seq %d, got %d", doneSeq, curSeq)
	}

	// A claimant pass over the account now: a terminal record is reaped, never
	// claimed and never published. Waiting for the reap rather than for a fixed
	// interval is what makes the publication assertion below conclusive - the pass
	// has demonstrably finished with this record.
	asm.scanWillOutbox(b.s)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asm); n != 0 {
			return fmt.Errorf("terminal record not reaped, %d left\n%s", n, c.describe())
		}
		return nil
	})
	// The decisive assertion, and the one the mutation must break: the Will was
	// published once, `done` recorded that, and nothing published it again.
	if n := pubs.Load(); n != 1 {
		t.Fatalf("a Will whose record committed `done` was published again: got %d "+
			"publications, want the single one `done` records", n)
	}
}
