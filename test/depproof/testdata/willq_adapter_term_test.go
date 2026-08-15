// Adapter for nats-server generations whose Raft proposal carries the term the
// operation captured (v2.14.4 and later, nats-io/nats-server#8370). run.sh selects
// this file by inspecting the checkout's proposal signature, not by comparing
// version strings, so a rename or a backport is picked up by what the source says.
package server

// recordedProposal is a stream-data proposal captured at the boundary JetStream
// hands it to Raft, together with the term it was captured under.
type recordedProposal struct {
	term  uint64
	entry []byte
}

// proposalRecorder stands in for a stream's RaftNode so a test can hold, count or
// pass through the proposals the publish path makes. Everything other than Propose
// is the real node's, so installing one changes no other behaviour: the apply loop
// captured the real node when the stream's monitor started and is unaffected.
type proposalRecorder struct {
	RaftNode
	forward bool
	ch      chan recordedProposal
}

func (r *proposalRecorder) Propose(term uint64, entry []byte) error {
	held := make([]byte, len(entry))
	copy(held, entry)
	select {
	case r.ch <- recordedProposal{term: term, entry: held}:
	default:
	}
	if r.forward {
		return r.RaftNode.Propose(term, entry)
	}
	return nil
}

// installProposalRecorder swaps the stream's node for a recorder. With forward
// false the proposal is held rather than made, which is how a test stages an
// operation that read its base under one term and reaches Raft under another.
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

// replayProposal hands a held proposal to the node. useCurrentTerm is the mutation
// switch for JetStream's propagation of the captured term: substituting the node's
// current term must make the refusal assertion fail.
func replayProposal(n RaftNode, p recordedProposal, useCurrentTerm bool) error {
	term := p.term
	if useCurrentTerm {
		term = n.Term()
	}
	return n.Propose(term, p.entry)
}

// proposalCarriesTerm reports whether this generation refuses a proposal whose
// captured term is no longer current.
func proposalCarriesTerm() bool { return true }
