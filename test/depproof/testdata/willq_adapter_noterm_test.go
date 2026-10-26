// Adapter for nats-server generations whose Raft proposal carries no term (before
// v2.14.4). It exists for one purpose: to prove the R-old fixture reaches the window
// nats-io/nats-server#8370 closed, so that the supported generation's refusal is a
// difference in the mechanism and not an artefact of the fixture.
//
// This generation is NOT supported by the broker. run.sh only ever selects it for the
// control run against an older checkout.
package server

// recordedProposal carries no term on this generation. The field is kept so the
// shared fixture compiles unchanged; proposalCarriesTerm reports it is meaningless.
type recordedProposal struct {
	term  uint64
	entry []byte
}

type proposalRecorder struct {
	RaftNode
	forward bool
	ch      chan recordedProposal
}

func (r *proposalRecorder) Propose(entry []byte) error {
	held := make([]byte, len(entry))
	copy(held, entry)
	select {
	case r.ch <- recordedProposal{entry: held}:
	default:
	}
	if r.forward {
		return r.RaftNode.Propose(entry)
	}
	return nil
}

func installProposalRecorder(mset *stream, forward bool) (chan recordedProposal, func()) {
	rec := &proposalRecorder{forward: forward, ch: make(chan recordedProposal, 8)}
	mset.mu.Lock()
	prev := mset.node
	rec.RaftNode = prev
	mset.node = rec
	mset.mu.Unlock()
	return rec.ch, func() {
		mset.mu.Lock()
		if mset.node == RaftNode(rec) {
			mset.node = prev
		}
		mset.mu.Unlock()
	}
}

// replayProposal ignores useCurrentTerm: there is no term to substitute, which is
// the property under test.
func replayProposal(n RaftNode, p recordedProposal, _ bool) error {
	return n.Propose(p.entry)
}

func proposalCarriesTerm() bool { return false }
