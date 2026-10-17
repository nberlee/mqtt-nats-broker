// Repository-owned dependency regression for the accepted-CONNECT atomic-batch write
// path (mqtt-nats-broker, docs/ROADMAP.md §0.2).
//
// Every accepted CONNECT commits the acceptance fact and the session record as ONE
// AllowAtomicPublish batch on $MQTT_sess (mqttJSA.storeAcceptBatch). The bridge reads
// an acknowledgement of that batch as "the record committed WITH the fact", and the
// roadmap finding this fixture answers argued the opposite from four Raft-layer facts:
// ProposeMulti pushes entries individually, runAsLeader pops whatever prefix exists,
// each group rechecks leadership independently, and a batch has no Raft-level identity
// -- concluding that a FIFO prefix split can commit the fact without the record.
//
// The four facts are true. The consequence is not, because batch identity is preserved
// in the STREAM ENTRY encoding and enforced at APPLY rather than at Raft:
//
//	a committed non-commit member is COUNTED and BUFFERED, never applied
//	  (jetstream_cluster.go, the batchMsgOp arm: it returns without applyStreamMsgOp,
//	  and the containing CommittedEntry is retained at function exit)
//	only a gap-free batchCommitMsgOp APPLIES anything, replaying every buffered member
//	  under one mset.mu hold
//
// So a split prefix applies NOTHING. That is the claim measured here, and it is the
// one thing upstream's own coverage does not assert: v2.14.5 ships
// TestJetStreamAtomicBatchPublishRejectPartialBatchOnLeaderChange, which builds this
// exact shape -- R3, a hand-encoded uncommitted 10-member prefix through ProposeMulti,
// a per-server batchApply.count barrier, a real StepDown, and a per-server CLFS wait --
// but asserts only the CLFS accounting. It never looks at the target subject. This
// fixture reuses that construction and adds the assertion the bridge's claim rests on.
//
// The paired mutation is m-batch-apply-eager: applying each member as it is committed
// instead of buffering it must make the store assertions below fail.
package server

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

const (
	// The prefix is COMMITTED and PROCESSED on every replica, with the applied watermark
	// WITHHELD for retained batch state. Requiring it is what keeps the negative
	// assertion from being satisfied by a run in which nothing ever arrived: "no message
	// was stored" is only evidence about the batch if the members demonstrably reached
	// every replica and the applied watermark is withheld for retained batch state.
	//
	// Not "the members are being retained": the same counterexample applies here. A build
	// retaining only the batch id, count and maxApplied while dropping every
	// CommittedEntry container would satisfy this marker, both store assertions and the
	// CLFS check.
	//
	// IT DOES NOT ESTABLISH NON-APPLICATION, and the name is historical. The paired
	// m-batch-apply-eager mutation emits this marker too: it applies each member as it is
	// committed without touching batch.maxApplied, so the watermark stays withheld, the
	// barrier passes, and every one of that mutation's kills comes from the STORED marker
	// instead. Non-application is the store assertions' to prove, not this one's.
	//
	// (Through v2.14.5 this marker did witness the buffering, read under one batch.mu
	// acquisition off mset.batchApply. That state is a monitor-goroutine local from
	// v2.15.0-preview.1 and is not observable from a test at all; the count-versus-
	// retention distinction the old comment drew no longer has anything to read.)
	acceptBatchPrefixBufferedMarker = "ACCEPT-BATCH-PREFIX-BUFFERED"

	// A member of an uncommitted prefix reached the STORE. This is the failure the
	// mutation must produce, and the one the bridge's indivisibility claim forbids.
	acceptBatchPrefixStoredMarker = "ACCEPT-BATCH-PREFIX-STORED"
)

const (
	acceptBatchStream  = "TEST"
	acceptBatchSubject = "foo"
	acceptBatchPrefix  = 10
)

// TestJetStreamAcceptBatchPrefixStoresNothing: an atomic batch whose members are
// committed without their commit entry stores nothing on the target subject, on ANY
// replica, either while the applied watermark is still withheld or after the leader change that
// abandons it.
func TestJetStreamAcceptBatchPrefixStoresNothing(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               acceptBatchStream,
		Subjects:           []string{acceptBatchSubject},
		Storage:            FileStorage,
		Replicas:           3,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, acceptBatchStream)
	mset, err := sl.globalAccount().lookupStream(acceptBatchStream)
	require_NoError(t, err)

	// A batch whose commit entry is never proposed: the FIFO prefix the finding says
	// can be committed on its own. Encoded and proposed exactly as the upstream
	// leader-change test does, so this measures the real entry shape rather than a
	// reconstruction of it.
	var entries []*Entry
	for i := range uint64(acceptBatchPrefix) {
		esm := encodeStreamMsgAllowCompressAndBatch(
			acceptBatchSubject, _EMPTY_, nil, nil, i, 0, false, "b", i+1, false)
		entries = append(entries, newEntry(EntryNormal, esm))
	}
	n := mset.raftNode()

	// The PRE-PROPOSAL index, captured before anything is enqueued. The barrier below
	// requires every replica to advance PAST it, which is what stops the barrier passing
	// on the pre-proposal state: ProposeMulti only enqueues, and runAsLeader removes
	// entries from the proposal queue before sendAppendEntry advances pindex, so there is
	// a window in which prop.len() == 0 and pindex == processed still describe the log as
	// it was before the batch existed.
	rnl := n.(*raft)
	rnl.RLock()
	preIndex := rnl.pindex
	rnl.RUnlock()

	require_NoError(t, n.ProposeMulti(n.Term(), entries))

	// The barrier: every server has COMMITTED and PROCESSED the whole prefix. Without
	// this the store assertions below would be a race against replication.
	//
	// This is a RAFT-level barrier rather than a read of the buffered batch state, and
	// the reason is a dependency change rather than a preference. Through v2.14.5 the
	// batch-apply state hung off the stream (`mset.batchApply`) with its own mutex, so a
	// test could read the buffered member count directly; from v2.15.0-preview.1 it is a
	// monitor-goroutine LOCAL (`batch := &batchApply{}` in the stream monitor, threaded
	// into applyStreamEntries) with no field on *stream and no mutex, so nothing outside
	// that goroutine can observe it at all.
	//
	// Upstream re-pointed its own partial-batch test to a Raft barrier and this follows
	// that idiom, with two additions its context does not need. First, every replica must
	// advance PAST the pre-proposal index: prop.len() == 0 with pindex == processed is
	// also true of the log before ProposeMulti's entries reach it, because ProposeMulti
	// only enqueues and runAsLeader drains the queue before pindex moves. Second, applied
	// must be BEHIND processed, because a retained batch is reported through
	// n.Processed(ce.Index, min(maxApplied, ce.Index)) with batch.maxApplied pinned at the
	// applied value from before the batch's first member.
	//
	// WHAT THIS BARRIER PROVES, AND WHAT IT DOES NOT. It proves the whole prefix is
	// committed and processed on every replica and that the applied watermark is being
	// WITHHELD for retained batch state. It does NOT prove the members had no early side
	// effects, and the paired mutation is the proof that it cannot: m-batch-apply-eager
	// applies each member as it is committed without touching batch.maxApplied, so the
	// watermark is still held back, this barrier still passes, and the marker below is
	// still logged. Every one of that mutation's kills comes from the store assertions.
	//
	// So the two are separate evidence and both are needed: the barrier is what makes the
	// store assertions non-racy, and the store assertions are what establish that nothing
	// was applied. An earlier revision of this comment claimed the barrier alone witnessed
	// the buffering; it does not.
	var pindexMax uint64
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		pindexMax = 0
		for _, s := range c.servers {
			ms, err := s.globalAccount().lookupStream(acceptBatchStream)
			if err != nil {
				return err
			}
			rn, ok := ms.raftNode().(*raft)
			if !ok {
				return fmt.Errorf("server %q: stream has no raft node", s.Name())
			}
			rn.RLock()
			prop, pindex := rn.prop.len(), rn.pindex
			processed, applied := rn.processed, rn.applied
			rn.RUnlock()
			if prop > 0 {
				return fmt.Errorf("server %q still has inflight proposals", s.Name())
			}
			// PAST the pre-proposal index, not merely self-consistent with it.
			if pindex <= preIndex {
				return fmt.Errorf("server %q: pindex %d has not advanced past the "+
					"pre-proposal index %d", s.Name(), pindex, preIndex)
			}
			if pindex != processed {
				return fmt.Errorf("server %q: pindex %d != processed %d", s.Name(), pindex, processed)
			}
			// AND the applied watermark is being withheld, which is how retained batch
			// state shows at this layer: a batch reports through
			// n.Processed(ce.Index, min(maxApplied, ce.Index)) with batch.maxApplied pinned
			// at the pre-batch applied value. applied == processed here would mean no batch
			// state shows at all. See the note above for why this is not by itself proof
			// that the prefix is retained, or that the members were unapplied.
			if applied >= processed {
				return fmt.Errorf("server %q: applied %d is not behind processed %d, so no "+
					"batch is being held", s.Name(), applied, processed)
			}
			if pindexMax == 0 {
				pindexMax = pindex
			} else if pindex != pindexMax {
				return fmt.Errorf("server %q: pindex %d != max %d", s.Name(), pindex, pindexMax)
			}
		}
		return nil
	})
	t.Logf("%s: every replica has committed and processed all %d members of the "+
		"uncommitted prefix, with the applied watermark withheld for retained batch state",
		acceptBatchPrefixBufferedMarker, acceptBatchPrefix)

	// The claim itself. The barrier above established that every replica committed and
	// processed the prefix with its applied watermark withheld; THIS is what establishes
	// that nothing was applied, and it is the assertion the paired mutation breaks.
	acceptBatchAssertNothingStored(t, c, "with the applied watermark withheld")

	// And the abandonment, which is what a real leader change does to it. The CLFS
	// wait is the apply barrier -- StepDown() returning is not one.
	require_NoError(t, n.StepDown())
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			ms, err := s.globalAccount().lookupStream(acceptBatchStream)
			if err != nil {
				return err
			}
			if clfs := ms.getCLFS(); clfs != acceptBatchPrefix {
				return fmt.Errorf("server %q: expected clfs to be %d, got %d",
					s.Name(), acceptBatchPrefix, clfs)
			}
		}
		return nil
	})
	acceptBatchAssertNothingStored(t, c, "after the leader change abandoned the prefix")
}

// acceptBatchAssertNothingStored asserts on EVERY replica, not only on the former
// leader: each member goes separately to its own store, so a claim about what the
// batch left behind is a claim about all three.
//
// A load error that is not the specific message-not-found result is reported WITHOUT
// the stored marker. It is still a failure -- an inconclusive read cannot stand in for
// the negative -- but it is not evidence that a member was stored, and a marker that
// claimed it would let the mutation row score its kill on an unrelated fault.
func acceptBatchAssertNothingStored(t *testing.T, c *cluster, when string) {
	t.Helper()
	var smv StoreMsg
	for _, s := range c.servers {
		ms, err := s.globalAccount().lookupStream(acceptBatchStream)
		require_NoError(t, err)
		ms.mu.RLock()
		store := ms.store
		ms.mu.RUnlock()

		sm, lerr := store.LoadLastMsg(acceptBatchSubject, &smv)
		switch {
		case lerr == nil:
			t.Errorf("%s: server %q holds a message on %q at sequence %d %s: a member of an "+
				"uncommitted batch reached the store",
				acceptBatchPrefixStoredMarker, s.Name(), acceptBatchSubject, sm.seq, when)
		case !errors.Is(lerr, ErrStoreMsgNotFound):
			t.Errorf("server %q: the load of %q %s failed with %v, which settles nothing "+
				"about whether a member was stored", s.Name(), acceptBatchSubject, when, lerr)
		}

		// Paired with the subject load because the two fail differently: a member
		// stored under a subject transform, or one stored and then removed, moves this
		// count without moving the subject's last message. The stream is unlimited and
		// nothing else ever publishes to it, so the only messages it can hold are the
		// prefix's.
		if state := ms.state(); state.Msgs != 0 {
			t.Errorf("%s: server %q holds %d message(s) %s, having applied nothing but an "+
				"uncommitted batch prefix",
				acceptBatchPrefixStoredMarker, s.Name(), state.Msgs, when)
		}
	}
}
