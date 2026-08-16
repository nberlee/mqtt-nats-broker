// Repository-owned dependency regressions for mqtt-nats-bridge finding F-030.
//
// These run inside an isolated nats-server checkout (package `server`), driven by
// test/depproof/run.sh in the bridge repository. They exist because the bridge's
// terminal Will-outbox state (`done` on $MQTT_willq) is only non-regressible if the
// JetStream server refuses a write whose per-subject expectation read a base that a
// committed `done` has already replaced. Two routes can carry such a write to a Raft
// proposal across a stream-leader transition, and each is exercised separately here.
// F-030's closure also rests on the pinned source and upstream maintainer confirmation:
//
//	R-old  an operation whose captured proposing term outlives that term. Refused by
//	       the term comparison in raft.proposeLocked, shipped in v2.14.4
//	       (nats-io/nats-server#8370). The pre-fence generation admits it, which is
//	       what the v2.14.3 control run measures.
//	R-new  a newly elected leader serving from applied state that does not yet
//	       include a committed `done`. Refused before it can happen: a node behind
//	       on applies does not campaign, a paused node defers a leadership transfer
//	       until it has replayed, and a new leader's stream does not accept writes
//	       until every entry in the log it was elected with has been processed.
//
// The primary assertion in each test is at the boundary the mechanism guards -- the
// proposal being refused, or no proposal being made at all. The record-state
// assertions that follow are defence in depth: they hold for additional reasons (the
// apply-time log-order comparison), so a mutation of the guard under test does not
// flip them, and a fixture that asserted only the record state would pass against a
// server with the guard removed.
package server

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// willqStoreBarrier is the deterministic seam the R-new readiness mutant needs: it
// holds ONE node's store application, so that node's applied view stays at the base a
// committed terminal record replaced.
//
// It BLOCKS rather than skips, and that distinction is the whole point. A barrier that
// skipped the write and returned success would let the apply loop call Applied(), which
// advances `processed` past `aflr` and opens the readiness gate by itself -- staging
// "applied, with the data silently lost" instead of "committed, not yet applied", and
// never exercising the mechanism under test. Blocking keeps raft's own view honest: the
// entry is committed, the upper layer has not finished with it, and `processed` does
// not move.
//
// Nothing reads it unless run.sh has applied m-freeze-store, which inserts the waits
// into the server's two store-application paths, so unmutated runs are unaffected.
// It is keyed by SERVER NAME rather than by the node's own state because the node under
// test is wrapped by the proposal recorder, and a condition reached through that wrapper
// would silently never fire -- which would score the mutant as a survivor.
var willqStoreBarrier struct {
	target atomic.Value // string: the server whose store application waits
	gate   atomic.Value // chan struct{}: closed to release it
}

// willqWaitStore is called from the server's store-application paths under
// m-freeze-store. It returns immediately for every node but the target.
func willqWaitStore(server string) {
	t, _ := willqStoreBarrier.target.Load().(string)
	if t == "" || t != server {
		return
	}
	if g, ok := willqStoreBarrier.gate.Load().(chan struct{}); ok {
		<-g
	}
}

// willqHoldStoreOn arms the barrier for one server and returns its release. The release
// is idempotent and MUST run before the cluster is shut down, or that node's apply
// goroutine never returns.
func willqHoldStoreOn(t *testing.T, server string) func() {
	t.Helper()
	gate := make(chan struct{})
	willqStoreBarrier.gate.Store(gate)
	willqStoreBarrier.target.Store(server)
	var once sync.Once
	release := func() {
		once.Do(func() {
			willqStoreBarrier.target.Store("")
			close(gate)
		})
	}
	// Registered AFTER the cluster's own cleanup, so it runs BEFORE it (t.Cleanup is
	// LIFO): a shutdown that ran first would wait on a goroutine still parked here.
	t.Cleanup(release)
	return release
}

const (
	willqFixtureStream  = "F030_WILLQ"
	willqFixtureSubject = "f030.willq.client.will"
)

// willqRecord is the shape of a $MQTT_willq record reduced to the two fields this
// proof turns on: the claim state and whether it is the terminal one.
type willqRecord struct {
	State string `json:"state"`
	Tag   string `json:"tag,omitempty"`
}

func willqBody(t *testing.T, state, tag string) []byte {
	t.Helper()
	b, err := json.Marshal(&willqRecord{State: state, Tag: tag})
	require_NoError(t, err)
	return b
}

// willqExpect builds the per-subject expectation header the bridge's
// storeWillOutboxCAS always sets. Every $MQTT_willq write is conditional, base 0
// included, so a fixture that stored unconditionally would not be testing the
// bridge's protocol at all.
func willqExpect(seq uint64) nats.Header {
	return nats.Header{JSExpectedLastSubjSeq: []string{strconv.FormatUint(seq, 10)}}
}

// willqFixtureCluster starts a three-node cluster carrying one stream configured
// exactly as the bridge configures $MQTT_willq (file storage, limits retention,
// one message per subject, R3), and seeds the record the routes then race against.
func willqFixtureCluster(t *testing.T) (*cluster, *nats.Conn, nats.JetStreamContext, uint64) {
	t.Helper()
	c := createJetStreamClusterExplicit(t, "F030", 3)
	t.Cleanup(c.shutdown)

	nc, js := jsClientConnect(t, c.randomServer())
	t.Cleanup(nc.Close)

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              willqFixtureStream,
		Subjects:          []string{"f030.willq.>"},
		Storage:           nats.FileStorage,
		Retention:         nats.LimitsPolicy,
		MaxMsgsPerSubject: 1,
		Replicas:          3,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, willqFixtureStream)

	ack, err := js.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(0),
		Data:    willqBody(t, "publishing", "base"),
	})
	require_NoError(t, err)
	return c, nc, js, ack.Sequence
}

func willqStreamOn(t *testing.T, s *Server) *stream {
	t.Helper()
	mset, err := s.GlobalAccount().lookupStream(willqFixtureStream)
	require_NoError(t, err)
	return mset
}

// willqApplyBacklog reports a node's applied-commit position and the highest commit
// it has been told about. They differ exactly while the node is holding entries it
// has not handed to the upper layer.
func willqApplyBacklog(t *testing.T, n RaftNode) (commit, hcommit uint64) {
	t.Helper()
	nr, ok := n.(*raft)
	require_True(t, ok)
	nr.RLock()
	defer nr.RUnlock()
	return nr.commit, nr.hcommit
}

// willqLastRecord returns the record currently standing on the outbox subject and
// its stream sequence.
func willqLastRecord(t *testing.T, js nats.JetStreamContext) (*willqRecord, uint64) {
	t.Helper()
	msg, err := js.GetLastMsg(willqFixtureStream, willqFixtureSubject)
	require_NoError(t, err)
	rec := &willqRecord{}
	require_NoError(t, json.Unmarshal(msg.Data, rec))
	return rec, msg.Sequence
}

// TestJetStreamWillqStaleTermReclaimIsRefused is route R-old.
//
// The stale operation is held at exactly the boundary the plan requires: after its
// per-subject expectation has read base S and before its Raft proposal. It is held
// by recording the proposal rather than by blocking inside Propose, because Propose
// runs under mset.clMu: a blocking hold there would also block the `done` write on
// the same stream, the apply path that clears the in-process per-subject entry, and
// the leader transition that resets clseq -- so the ordering this route is about
// (`done` commits first, the older operation is released second) cannot be staged
// that way at all.
//
// The re-election of the SAME process is load-bearing. Before the term comparison
// existed, proposeLocked still refused a proposal from a node that was no longer
// leader, so without the frozen process regaining leadership the older generation
// would refuse the replay for a reason that has nothing to do with the term, and
// the control run would prove nothing.
func TestJetStreamWillqStaleTermReclaimIsRefused(t *testing.T) {
	c, nc, js, base := willqFixtureCluster(t)

	leader := c.streamLeader(globalAccountName, willqFixtureStream)
	mset := willqStreamOn(t, leader)
	rn := mset.raftNode()
	require_True(t, rn != nil)
	termAtRead := rn.Term()

	// Hold the next stream-data proposal from this leader.
	captured, restore := installProposalRecorder(mset, false)

	// A core publish, so nothing waits for an acknowledgement that cannot come:
	// the proposal is being held, so this write is neither stored nor answered.
	require_NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(base),
		Data:    willqBody(t, "publishing", "stale-base-reclaim"),
	}))
	require_NoError(t, nc.Flush())

	var held recordedProposal
	select {
	case held = <-captured:
	case <-time.After(10 * time.Second):
		restore()
		t.Fatal("the stale-base write never reached the proposal boundary: the fixture " +
			"did not reach the mechanism it measures")
	}
	restore()
	if proposalCarriesTerm() {
		require_Equal(t, held.term, termAtRead)
	}

	// Cycle leadership so the held operation's term is no longer current, and bring
	// the SAME process back as leader.
	require_NoError(t, rn.StepDown())
	c.waitOnStreamLeader(globalAccountName, willqFixtureStream)
	for i := 0; c.streamLeader(globalAccountName, willqFixtureStream) != leader; i++ {
		if i >= 10 {
			t.Fatal("could not bring the original stream leader back: without a " +
				"same-process re-election this fixture cannot tell a term refusal " +
				"from an ordinary not-leader refusal")
		}
		cur := c.streamLeader(globalAccountName, willqFixtureStream)
		require_NoError(t, willqStreamOn(t, cur).raftNode().StepDown(rn.ID()))
		c.waitOnStreamLeader(globalAccountName, willqFixtureStream)
	}
	require_True(t, rn.Term() > termAtRead)
	require_Equal(t, rn.State(), Leader)

	// `done` commits first, from the same base the held operation read.
	doneAck, err := js.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(base),
		Data:    willqBody(t, "done", "terminal"),
	})
	require_NoError(t, err)
	require_True(t, doneAck.Sequence > base)

	// Release the held operation into the current leader. The term it carries is the
	// term its expectation was read under, which is no longer current.
	//
	// MUTATION M-captured-term: run.sh flips this to true, which substitutes the
	// node's current term for the captured one. The refusal below must then fail,
	// which is what proves the fixture depends on JetStream propagating the term it
	// captured rather than on Raft's plain not-leader check.
	useCurrentTerm := false
	replayErr := replayProposal(rn, held, useCurrentTerm)

	if proposalCarriesTerm() {
		// The boundary assertion, named so the harness can require this exact outcome:
		// MUTATION m-term-guard and m-captured-term must each make it fail, and a row
		// that fails anywhere else has not demonstrated the route.
		if replayErr != errNotLeader {
			t.Fatalf("%s: the older-term proposal was ADMITTED (err %v), so the captured "+
				"term is not being compared against the current one",
				willqOldTermAdmittedMarker, replayErr)
		}
	} else {
		// The control generation admits it: this is the window #8370 closed, and
		// reaching it is what licenses reading the supported run as a difference.
		require_NoError(t, replayErr)
	}

	// Defence in depth, and the reason the boundary assertion above is the primary
	// one: an admitted stale entry is still refused at apply, because it carries the
	// log position its expectation was read at and `done` has moved that on. The
	// terminal record therefore stands on both generations, which is exactly why a
	// fixture asserting only the record state could not discriminate the fence.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		rec, seq := willqLastRecord(t, js)
		if rec.State != "done" {
			return fmt.Errorf("the terminal record was regressed to %q", rec.State)
		}
		if seq != doneAck.Sequence {
			return fmt.Errorf("the terminal record moved from %d to %d", doneAck.Sequence, seq)
		}
		return nil
	})
	if proposalCarriesTerm() {
		// No second sequence was assigned at all: the refusal happened before the
		// proposal, not after it.
		si, err := js.StreamInfo(willqFixtureStream)
		require_NoError(t, err)
		require_Equal(t, si.State.LastSeq, doneAck.Sequence)
	}
}

// willqUnappliedStage is the state route R-new needs: one node holds a COMMITTED
// terminal record it has not finished applying, so its own applied view still names the
// base that record replaced.
type willqUnappliedStage struct {
	c         *cluster
	js        nats.JetStreamContext
	base      uint64
	doneSeq   uint64
	target    *Server // the node whose store application is held
	targetRN  RaftNode
	proposals chan recordedProposal // proposals the target's publish path makes
	release   func()                // frees the held store application
}

// willqStageUnappliedTerminal holds one follower's store application, commits `done`
// through the ordinary CAS path on the leader, and then pushes leadership toward the
// held node.
//
// Only the STORE is held. Appends, votes and raft-level commit all continue, so `done`
// reaches quorum normally and the held node's log contains it; what has not happened on
// that node is the upper layer finishing with the entry, which is exactly the state
// this route names.
func willqStageUnappliedTerminal(t *testing.T) *willqUnappliedStage {
	t.Helper()
	c, _, js, base := willqFixtureCluster(t)

	leader := c.streamLeader(globalAccountName, willqFixtureStream)
	lset := willqStreamOn(t, leader)
	lrn := lset.raftNode()

	st := &willqUnappliedStage{c: c, js: js, base: base}
	for _, s := range c.servers {
		if s != leader {
			st.target = s
			break
		}
	}
	require_True(t, st.target != nil)
	tset := willqStreamOn(t, st.target)
	// The real node, taken BEFORE the recorder is installed: the recorder stands in for
	// the node on the publish path, and the staging below drives the node itself.
	st.targetRN = tset.raftNode()
	// forward is true, so the target's behaviour is unchanged and this only OBSERVES
	// what it proposes -- the assertion this route turns on.
	ch, restore := installProposalRecorder(tset, true)
	t.Cleanup(restore)
	st.proposals = ch

	// The target must have applied the seed BEFORE its store is held, or it is behind
	// for a reason that has nothing to do with the terminal record: a publish needs
	// only the leader plus one peer, so a follower can still be at sequence zero here.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if got := tset.lastSeq(); got != base {
			return fmt.Errorf("%s is at sequence %d, want the seed at %d",
				st.target.Name(), got, base)
		}
		return nil
	})

	// TWO devices, because they hold the state at different moments and only one of
	// them exists unmutated.
	//
	// PauseApply is a real server API and is what stages the state HONESTLY on an
	// unmutated run: the node stops taking committed entries from its apply queue, so
	// its store stays at the base and `processed` stops advancing, which is exactly
	// "committed, not yet applied". That is the state the clean run asserts against.
	//
	// It does not survive an election, though: `tryCommit` calls `applyCommit` on the
	// LEADER path with no pause check, so a node that becomes leader processes its whole
	// log regardless. The barrier is what holds the store THROUGH the election, and it
	// only exists when m-freeze-store has inserted its waits -- which is why the
	// mutation rows need it and the clean row does not.
	require_NoError(t, st.targetRN.PauseApply())
	st.release = willqHoldStoreOn(t, st.target.Name())

	doneAck, err := js.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(base),
		Data:    willqBody(t, "done", "terminal"),
	})
	require_NoError(t, err)
	st.doneSeq = doneAck.Sequence

	// The premise, stated where it matters: the leader's applied view carries the
	// terminal record and the target's still names the base it replaced, which is the
	// state a write conditioned on that base would be measured against.
	require_Equal(t, lset.lastSeq(), st.doneSeq)
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if got := tset.lastSeq(); got != base {
			return fmt.Errorf("%s applied past the base to %d: the barrier is not holding",
				st.target.Name(), got)
		}
		// And in raft's own terms the node knows of a committed entry its upper layer
		// has not finished with. A PAUSED node keeps that in hcommit and leaves commit
		// where its applies stopped, so the backlog shows as hcommit > commit; once it
		// is unpaused (which is what the mutation rows arrange) it shows as
		// processed < commit. Either form is the same fact.
		if !willqHoldsUnprocessedCommit(t, st.targetRN) {
			return fmt.Errorf("%s reports nothing outstanding, so nothing is being held",
				st.target.Name())
		}
		return nil
	})

	// Take the node that could answer from applied state out of the running, and push
	// leadership toward the one that cannot.
	lrn.SetObserver(true)
	require_NoError(t, lrn.StepDown(st.targetRN.ID()))
	return st
}

// willqHoldsUnprocessedCommit reports whether a node knows of a committed entry its
// upper layer has not finished processing -- the condition the readiness envelope keys
// on. A paused node records that as hcommit > commit; an unpaused one as
// processed < commit.
func willqHoldsUnprocessedCommit(t *testing.T, n RaftNode) bool {
	t.Helper()
	nr, ok := n.(*raft)
	require_True(t, ok)
	nr.RLock()
	defer nr.RUnlock()
	return nr.processed < nr.commit || nr.hcommit > nr.commit
}

// leaderServingStale returns the server that holds stream leadership while its own
// applied view predates the committed terminal record, or nil.
//
// Both conditions matter. A node that leads with a CURRENT view is not this route's
// hazard, and flagging it would be a false kill.
func (st *willqUnappliedStage) leaderServingStale(t *testing.T) *Server {
	t.Helper()
	for _, s := range st.c.servers {
		mset := willqStreamOn(t, s)
		if mset.isLeader() && mset.lastSeq() < st.doneSeq {
			return s
		}
	}
	return nil
}

// raftLeaderBehind returns the target if it holds RAFT leadership while still behind on
// applies -- the first half of the readiness envelope, separately from whether the
// stream has been announced as serving.
func (st *willqUnappliedStage) raftLeaderBehind(t *testing.T) *Server {
	t.Helper()
	if st.targetRN.State() == Leader && willqStreamOn(t, st.target).lastSeq() < st.doneSeq {
		return st.target
	}
	return nil
}

// resumeAndSettle frees both holds, which is when the target finishes with the terminal
// entry, and waits for a leader to be serving again.
func (st *willqUnappliedStage) resumeAndSettle(t *testing.T) {
	t.Helper()
	st.release()
	st.targetRN.ResumeApply()
	st.c.waitOnStreamLeader(globalAccountName, willqFixtureStream)
}

// assertTerminalStands is the shared closing check: the write from the superseded base
// is refused for the ordinary reason once someone is serving from applied state, and
// the terminal record is the one standing.
func (st *willqUnappliedStage) assertTerminalStands(t *testing.T) {
	t.Helper()
	_, err := st.js.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(st.base),
		Data:    willqBody(t, "publishing", "stale-base-after-resume"),
	})
	require_Error(t, err)

	rec, seq := willqLastRecord(t, st.js)
	require_Equal(t, rec.State, "done")
	require_Equal(t, seq, st.doneSeq)
}

// TestJetStreamWillqNewLeaderCannotServeBeforeApplying is R-new's READINESS half: while
// a committed terminal record has not been applied on the node entitled to lead, that
// node neither takes leadership nor serves, and a write conditioned on the superseded
// base is neither answered nor proposed anywhere.
//
// The envelope has two halves and they are redundant with each other, so isolating it
// takes a higher-order mutant: m-behind-campaign lets a node behind on applies take
// leadership, m-premature-ready announces it as serving without waiting for its log to
// be processed, and m-freeze-store is the barrier that holds its applied view at the
// superseded base. Remove either half alone and the other still refuses, which is why
// the matrix runs the pair rather than one at a time.
//
// The kill this test must produce is a PROPOSAL carrying the superseded base, and it is
// marked so the harness can require that exact outcome rather than accept any failure.
func TestJetStreamWillqNewLeaderCannotServeBeforeApplying(t *testing.T) {
	st := willqStageUnappliedTerminal(t)

	// Recorded rather than fatal, so a mutated run reaches the write below and shows
	// the whole regressed shape at once. Stopping at the first would leave the decisive
	// part -- the proposal -- unproved.
	var ledBehind, servedStale *Server
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (ledBehind == nil || servedStale == nil) {
		if s := st.raftLeaderBehind(t); s != nil && ledBehind == nil {
			ledBehind = s
		}
		if s := st.leaderServingStale(t); s != nil && servedStale == nil {
			servedStale = s
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ledBehind != nil {
		t.Errorf("%s: %s took stream leadership while its applied view still named the "+
			"superseded base, so the campaign refusal for an unprocessed log is not holding",
			willqLedBehindMarker, ledBehind.Name())
	}
	if servedStale != nil {
		t.Errorf("%s: %s served the stream from applied state at %d, which predates the "+
			"committed terminal record at %d, so the applied-before-serving announcement "+
			"is not holding", willqServedStaleMarker, servedStale.Name(),
			willqStreamOn(t, servedStale).lastSeq(), st.doneSeq)
	}

	// A write from the superseded base, submitted into that window, must not be
	// answered, and above all must not be PROPOSED.
	_, err := st.js.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(st.base),
		Data:    willqBody(t, "publishing", "stale-base-in-window"),
	}, nats.AckWait(2*time.Second))
	if err == nil {
		t.Errorf("%s: a write conditioned on the superseded base was ACKNOWLEDGED inside "+
			"the window", willqAckedMarker)
	}

	// The held store application is freed; the target finishes with the terminal entry,
	// and whoever leads next serves from a view that contains it.
	st.resumeAndSettle(t)
	st.assertTerminalStands(t)

	// The proposal observation happens ONCE, here, after everything has settled -- not
	// as a snapshot taken right after the timed-out publish.
	//
	// A snapshot there raced the thing it measured: the publish times out on the client
	// while the cluster is still processing, so a proposal arriving just afterwards was
	// counted by a LATER, unmarked assertion, and the row scored its expected kill while
	// the claim it encodes had actually failed. Observing once, after the recorder has
	// drained, removes that gap by construction rather than papering over it with a
	// second marker. Nothing legitimate proposes in between: every write this fixture
	// submits is conditioned on the superseded base and must be refused before proposal.
	if n := len(st.proposals); n != 0 {
		t.Errorf("%s: %d proposal(s) carrying the superseded base were made from applied "+
			"state that predates the committed terminal record", willqStaleProposalMarker, n)
	}
}

// The two named failures this fixture can produce, one per half of the readiness
// envelope, so the harness can require the exact outcome a row claims rather than
// accept any failure.
//
// They are separate because the halves are separately removable and the difference is
// the evidence: removing the campaign refusal alone produces a node LEADING from a
// stale applied view while the stream still refuses to serve, and only removing the
// announcement as well produces a PROPOSAL carrying the superseded base.
const (
	willqOldTermAdmittedMarker = "R-OLD-TERM-ADMITTED"
	willqLedBehindMarker       = "R-NEW-LED-BEHIND"
	willqServedStaleMarker     = "R-NEW-SERVED-STALE"
	willqAckedMarker           = "R-NEW-ACKED"
	willqStaleProposalMarker   = "R-NEW-STALE-PROPOSAL"
)

// TestJetStreamWillqNewLeaderRefusesTheSupersededBase is R-new's second mechanism: once
// a leader IS serving, a write conditioned on the base a committed `done` replaced is
// refused BEFORE any proposal is made, because the expectation it is compared against
// is reconstructed from that leader's own applied state.
//
// The window above is what keeps a leader from serving with an applied view that
// predates the terminal record; this is what happens to the write once one is serving
// legitimately. MUTATION M-expected-subject bypasses exactly that comparison, and must
// make this fail at the boundary first -- the write acknowledged, and a proposal
// carrying the superseded base appearing -- with the terminal record's regression as
// the consequence behind it.
func TestJetStreamWillqNewLeaderRefusesTheSupersededBase(t *testing.T) {
	c, _, js, base := willqFixtureCluster(t)

	leader := c.streamLeader(globalAccountName, willqFixtureStream)
	doneAck, err := js.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(base),
		Data:    willqBody(t, "done", "terminal"),
	})
	require_NoError(t, err)

	// A real transition, through the same public stepdown the bridge sentinel uses.
	// This is the boundary the finding is about: it clears the in-process per-subject
	// state and zeroes the clustered sequence, so the new leader has to reconstruct
	// what the subject currently holds from its own applied state.
	require_NoError(t, willqStreamOn(t, leader).raftNode().StepDown())
	c.waitOnStreamLeader(globalAccountName, willqFixtureStream)
	next := c.streamLeader(globalAccountName, willqFixtureStream)
	require_True(t, next != leader)

	// Watch the new leader's proposals. forward is true, so behaviour is unchanged --
	// this only records what is proposed.
	nset := willqStreamOn(t, next)
	proposals, restore := installProposalRecorder(nset, true)
	defer restore()

	// The write the whole finding is about, conditioned on the superseded base.
	_, err = js.PublishMsg(&nats.Msg{
		Subject: willqFixtureSubject,
		Header:  willqExpect(base),
		Data:    willqBody(t, "publishing", "stale-base-reclaim"),
	}, nats.AckWait(5*time.Second))

	// Reported rather than fatal, so the BOUNDARY assertion below is reached and is
	// what a mutated run fails on. A fatal here would stop the test at "the write was
	// acknowledged", which says nothing about whether the refusal happened before the
	// proposal or after it -- and that distinction is the whole point of this fixture.
	if err == nil {
		t.Errorf("%s: a write conditioned on the superseded base was ACKNOWLEDGED",
			willqAckedMarker)
	}

	// The boundary assertion: refused before the proposal, not discarded after it.
	if n := len(proposals); n != 0 {
		t.Errorf("%s: %d proposal(s) carrying the superseded base were made by a leader "+
			"whose applied state already holds the terminal record", willqStaleProposalMarker, n)
	}

	// And the consequence: the terminal record is the one standing.
	rec, seq := willqLastRecord(t, js)
	require_Equal(t, rec.State, "done")
	require_Equal(t, seq, doneAck.Sequence)
}
