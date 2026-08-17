// Repository-owned dependency regression for the accepted-CONNECT atomic-batch write
// path (mqtt-nats-bridge, docs/ROADMAP.md §0.2).
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
	// The prefix reached the apply layer and was BUFFERED there. Requiring it is what
	// keeps the negative assertion from being satisfied by a run in which nothing ever
	// arrived: "no message was stored" is only evidence about buffering if the members
	// were demonstrably committed and held.
	//
	// Both halves are required, and read under ONE batch.mu acquisition. The count
	// alone is not the observation: it is incremented in the batchMsgOp arm, while the
	// retention of the containing entry happens hundreds of lines later at function
	// exit, so a server that counted and then dropped the entries would satisfy a
	// count-only marker while storing nothing -- and the negative assertion would hold
	// for the wrong reason.
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
// replica, either while the prefix is still buffered or after the leader change that
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
	require_NoError(t, n.ProposeMulti(n.Term(), entries))

	// The barrier: every server has COMMITTED the whole prefix and is holding it.
	// Without this the store assertions below would be a race against replication.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			ms, err := s.globalAccount().lookupStream(acceptBatchStream)
			if err != nil {
				return err
			}
			ms.mu.RLock()
			batch := ms.batchApply
			ms.mu.RUnlock()
			if batch == nil {
				return fmt.Errorf("server %q has no batch apply state", s.Name())
			}
			batch.mu.Lock()
			count, held := batch.count, len(batch.entries)
			batch.mu.Unlock()
			if count != acceptBatchPrefix || held == 0 {
				return fmt.Errorf("server %q buffered %d of %d member(s) in %d retained entry container(s)",
					s.Name(), count, acceptBatchPrefix, held)
			}
		}
		return nil
	})
	t.Logf("%s: every replica has committed all %d members of the uncommitted prefix and "+
		"is holding their entries", acceptBatchPrefixBufferedMarker, acceptBatchPrefix)

	// The claim itself: committed, buffered, and nothing applied.
	acceptBatchAssertNothingStored(t, c, "while the prefix is buffered")

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
