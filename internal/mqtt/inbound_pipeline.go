// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// The inbound acknowledgement pipeline. See docs/ARCHITECTURE.md, "The inbound
// acknowledgement pipeline".
//
// An inbound PUBLISH or PUBREL is answered only after its durable effect returned: the
// $MQTT_msgs store (QoS 1), the hold CAS (QoS 2 PUBLISH), or the onward store and the
// terminal CAS (PUBREL). Run on the readLoop, that caps one connection at one exchange per
// store round trip whatever window the client keeps. The pipeline lets a connection with
// work already outstanding overlap those round trips, while keeping every request the serial
// path makes and every order it promised:
//
//   - ONE IMPLEMENTATION, TWO DRIVERS. An exchange's work is a job with steps (the hold step,
//     a release's fences, the ordered issuance, the completion). DIRECT, the readLoop runs the
//     steps inline under its packet lease and writes the response itself; PIPELINED, the same
//     steps run on the pipe's goroutines. Direct iff the pipe is EMPTY (no job, no response
//     being written) and nothing more is buffered behind the packet: a client that publishes
//     and waits for each answer pays two checks over the serial path, and a direct packet can
//     neither overtake queued work nor be overtaken by it.
//   - ONE FIFO, positions reserved at receipt. The append is the reservation; every response
//     is written from the head, in receipt order, across PUBACK, PUBREC and PUBCOMP.
//   - ISSUANCE IN ORDER. One issuer takes jobs strictly at its cursor and submits each
//     $MQTT_msgs store without awaiting it; sendq has a single drainer, so the submission
//     order is the wire order. The live fan-out and the retained update precede the store in
//     the same step, so fan-out order is receipt order too ([MQTT-4.6.0-5]).
//   - SAME-IDENTIFIER CHAIN. A QoS 2 PUBLISH or PUBREL does not start while an earlier QoS 2
//     job on its identifier is unfinished: the serial path's order between a duplicate
//     PUBLISH's reconcile and the following release's terminal CAS.
//   - LEASES IN FIFO ORDER. A queued job takes its own incarnation lease at its first
//     session-observing step, and only when every earlier job has taken one or finished, so
//     the leased jobs are always a prefix of the FIFO and nothing leased waits on a job parked
//     on a closing tracker. A direct job runs under the readLoop's packet lease.
//   - ABORT. The connection's latch (closeTransport) releases every wait; unstarted jobs are
//     dropped, submitted stores are awaited (which releases their reply registrations), and
//     nothing is written afterwards.
//   - FAILURE KEEPS THE PREFIX. A job's failure is recorded at its position; earlier
//     responses are still written; at the failure the connection is told why and its
//     transport closed, and the readLoop runs the ordinary teardown.

// mqttInboundWindow bounds a connection's reserved positions. A PUBLISH or PUBREL arriving at
// a full pipe waits for space before its packet lease is taken (back-pressure on the socket).
// A variable only so a test can fill it.
var mqttInboundWindow = 1024

var (
	// errMQTTInboundAborted reports work refused or dropped because the connection's abort
	// latch fired: its transport is closed, or an earlier job failed. Nothing is answered.
	errMQTTInboundAborted = errors.New("mqtt: inbound pipeline aborted")

	// errMQTTInboundDrainFirst is the rejection gate's request to drain this connection's
	// pipe before it touches the session record transaction (mqttGateDispatchOnRejection).
	errMQTTInboundDrainFirst = errors.New("mqtt: drain the inbound pipeline before the rejection gate")
)

type mqttInboundKind uint8

const (
	mqttInboundQoS0 mqttInboundKind = iota
	mqttInboundQoS1
	mqttInboundQoS2Pub
	mqttInboundPubRel
	// mqttInboundAnswer is a response decided on the readLoop with nothing left to do: a
	// refusal, or a reserved-topic answer.
	mqttInboundAnswer
)

// mqttInboundResp is a response decided for a job. send false is a deliberate silence: QoS 0,
// or a release that may not be answered and is not a failure either.
type mqttInboundResp struct {
	send      bool
	pt        byte
	pi        uint16
	reason    byte
	reasonStr string
}

// mqttPubAnswer is the acknowledgement of a PUBLISH at qos: PUBACK, PUBREC, or nothing.
func mqttPubAnswer(qos byte, pi uint16, reason byte, reasonStr string) mqttInboundResp {
	switch qos {
	case 1:
		return mqttInboundResp{send: true, pt: mqttPacketPubAck, pi: pi, reason: reason, reasonStr: reasonStr}
	case 2:
		return mqttInboundResp{send: true, pt: mqttPacketPubRec, pi: pi, reason: reason, reasonStr: reasonStr}
	}
	return mqttInboundResp{}
}

// mqttPubCompAnswer is a release's PUBCOMP.
func mqttPubCompAnswer(pi uint16, reason byte, reasonStr string) mqttInboundResp {
	return mqttInboundResp{send: true, pt: mqttPacketPubComp, pi: pi, reason: reason, reasonStr: reasonStr}
}

// mqttInboundJob is one inbound PUBLISH or PUBREL. The fields above the line are set on the
// readLoop before the job is queued or run and never written again.
type mqttInboundJob struct {
	p     *mqttInboundPipe
	seq   uint64
	kind  mqttInboundKind
	pi    uint16
	trace bool

	// The publication (the parser's c.mqtt.pp, borrowed, while the job runs inline; an owned
	// copy from the moment it is queued: see run), its No Local stamp and
	// origin, and its receipt time, from which Message Expiry is aged.
	pp          *mqttPublish
	noLocal     string
	origin      string
	receiptNano int64

	// The QoS 2 stamp budget: non-zero when only the No Local stamp makes a NEW hold exceed
	// max_payload (mqttProcessPub).
	q2Stamped    int
	q2MaxPayload int32

	// counted: this job holds one unanswered count on its identifier (Receive Maximum).
	counted bool

	// --- guarded by p.mu ---
	hasOrdered bool // the issuer must run issueStep for it
	prereqDone bool // the issuer may take it at its cursor
	taken      bool // the issuer has claimed it: its ordered step is running or done
	issued     bool // the issuer has passed it
	ready      bool // response decided (or failure recorded)
	leaseDone  bool // acquired, finished, or never needed: the lease cursor may pass it
	resp       mqttInboundResp
	err        error

	// Owned by whichever goroutine runs the job's steps, in order.
	lease    *mqttLease
	issue    mqttIssued // valid when hasIssue
	hasIssue bool
	rel      *mqttInboundRelease
	direct   bool
}

// mqttInboundIdentifier is the Receive Maximum ledger's entry for one packet identifier.
type mqttInboundIdentifier struct {
	unanswered int  // PUBLISH jobs whose response has not been written
	held       bool // a success PUBREC was written and no PUBCOMP yet
}

// mqttInboundPipe is one connection's inbound acknowledgement pipeline.
type mqttInboundPipe struct {
	s     *Server
	c     *client
	abort <-chan struct{}

	mu sync.Mutex
	// changed is closed and replaced on every state change; waiters select on it and on the
	// abort latch, so no wait outlives the connection.
	changed chan struct{}
	jobs    []*mqttInboundJob // FIFO; jobs[0] is the emission head
	seq     uint64
	// nextIssue and nextLease are job sequence numbers: the issuer's cursor and the lease
	// cursor. Every job before nextIssue has been passed by the issuer; every job before
	// nextLease has acquired its lease, finished, or needed none.
	nextIssue     uint64
	nextLease     uint64
	issuerRunning bool
	emitting      bool
	failed        bool
	ledger        map[uint16]mqttInboundIdentifier // by value: an exchange allocates no entry

	// classify serializes this connection's releases' retirement classifications, which
	// would otherwise refuse each other the session record transaction (invariant 15).
	classify chan struct{}

	// wg counts every goroutine the pipe started; the readLoop joins it before teardown.
	wg sync.WaitGroup
}

// abortLatch returns the connection's abort latch, making it for a client not built by
// createMQTTClient.
func (m *mqtt) abortLatch() chan struct{} {
	m.inAbortMu.Lock()
	defer m.inAbortMu.Unlock()
	if m.inAbort == nil {
		m.inAbort = make(chan struct{})
	}
	return m.inAbort
}

// fireAbort closes the connection's abort latch, once.
func (m *mqtt) fireAbort() {
	m.inAbortMu.Lock()
	defer m.inAbortMu.Unlock()
	if m.inAbort == nil {
		m.inAbort = make(chan struct{})
	}
	if !m.inAbortFired {
		m.inAbortFired = true
		close(m.inAbort)
	}
}

// mqttInboundPipeFor returns c's pipe, creating it. readLoop only.
func (c *client) mqttInboundPipeFor(s *Server) *mqttInboundPipe {
	if p := c.mqtt.inPipe; p != nil {
		return p
	}
	p := &mqttInboundPipe{
		s:         s,
		c:         c,
		abort:     c.mqtt.abortLatch(),
		changed:   make(chan struct{}),
		seq:       0,
		nextIssue: 1,
		nextLease: 1,
		ledger:    map[uint16]mqttInboundIdentifier{},
		classify:  make(chan struct{}, 1),
	}
	c.mqtt.inPipe = p
	return p
}

// broadcastLocked wakes every waiter. p.mu held.
func (p *mqttInboundPipe) broadcastLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// waitLocked waits for the next state change or the abort, with p.mu held on entry and on
// return. It reports false when the abort fired.
func (p *mqttInboundPipe) waitLocked() bool {
	ch := p.changed
	p.mu.Unlock()
	select {
	case <-ch:
		p.mu.Lock()
		return true
	case <-p.abort:
		p.mu.Lock()
		return false
	}
}

// waitChangedLocked waits for the next state change only: the wait of a job whose remaining
// work is owned by a bounded, abort-aware step that always broadcasts. p.mu held.
func (p *mqttInboundPipe) waitChangedLocked() {
	ch := p.changed
	p.mu.Unlock()
	<-ch
	p.mu.Lock()
}

func (p *mqttInboundPipe) aborted() bool { return mqttLatchFired(p.abort) }

// emptyLocked: no job queued and no response being written. p.mu held.
func (p *mqttInboundPipe) emptyLocked() bool { return len(p.jobs) == 0 && !p.emitting }

// jobAtLocked returns the queued job with sequence seq, or nil. p.mu held.
func (p *mqttInboundPipe) jobAtLocked(seq uint64) *mqttInboundJob {
	if len(p.jobs) == 0 {
		return nil
	}
	i := int(seq - p.jobs[0].seq)
	if seq < p.jobs[0].seq || i >= len(p.jobs) {
		return nil
	}
	return p.jobs[i]
}

// ---------------------------------------------------------------------------------------------
// The Receive Maximum ledger (invariant 8). Both paths share it; p.mu guards it.

// admitIdentifier applies the server's inbound Receive Maximum to a QoS>0 PUBLISH. An
// identifier already outstanding (unanswered or held) is not a new send — a DUP, or a reuse —
// and never counts twice; a new one at the limit is the client's violation [MQTT-3.3.4-7]. A
// compliant client's own outstanding set always contains the server's, because the server
// releases an identifier BEFORE it writes the answer the client is waiting for.
func (p *mqttInboundPipe) admitIdentifier(pi uint16, recvMax uint16) (outstanding int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if recvMax == 0 {
		return len(p.ledger), true
	}
	if _, known := p.ledger[pi]; known {
		return len(p.ledger), true
	}
	return len(p.ledger), len(p.ledger) < int(recvMax)
}

// countLocked charges j's identifier one unanswered response. p.mu held.
func (p *mqttInboundPipe) countLocked(j *mqttInboundJob) {
	st := p.ledger[j.pi]
	st.unanswered++
	p.ledger[j.pi] = st
	j.counted = true
}

// settleLocked applies a job about to be answered (written true: its response is handed to
// the writer) or abandoned to the ledger. p.mu held.
func (p *mqttInboundPipe) settleLocked(j *mqttInboundJob, written bool) {
	st, ok := p.ledger[j.pi]
	if !ok {
		return
	}
	if j.counted {
		j.counted = false
		st.unanswered--
	}
	if written {
		switch j.resp.pt {
		case mqttPacketPubRec:
			// A success PUBREC leaves the client's quota slot occupied until the matching
			// PUBCOMP frees it. A DUP answered from the hold sets it again, idempotently.
			if j.resp.reason == mqttReasonSuccess {
				st.held = true
			}
		case mqttPacketPubComp:
			// A PUBCOMP (with any reason, 0x92 included) ends the exchange and returns the
			// slot. Spec5 [4.9].
			st.held = false
		}
	}
	if st.unanswered <= 0 && !st.held {
		delete(p.ledger, j.pi)
		return
	}
	p.ledger[j.pi] = st
}

// ---------------------------------------------------------------------------------------------
// Dispatch (readLoop).

// run dispatches a job built on the readLoop: inline when the pipe is empty and nothing more
// is buffered, otherwise queued. A response already decided (an answer) needs only an empty
// pipe to be written at once. Returns the error the readLoop's error exit acts on.
func (p *mqttInboundPipe) run(j *mqttInboundJob, more bool) error {
	p.mu.Lock()
	j.p = p
	switch j.kind {
	case mqttInboundQoS0, mqttInboundQoS1, mqttInboundPubRel:
		j.hasOrdered = true
	}
	// Every QoS>0 PUBLISH is unanswered until its PUBACK or PUBREC is written, a refusal
	// included; a release and a QoS 0 publication are not counted.
	switch {
	case j.kind == mqttInboundQoS1, j.kind == mqttInboundQoS2Pub,
		j.kind == mqttInboundAnswer && j.resp.send:
		p.countLocked(j)
	}
	direct := p.emptyLocked() && (!more || j.kind == mqttInboundAnswer)
	if h := mqttTestInboundSwitchV.Load(); h != nil {
		(*h)(p.c, j.kind, j.pi, direct)
	}
	if direct {
		p.mu.Unlock()
		j.direct = true
		return p.runInline(j)
	}
	// Queued: the job outlives this packet, so it takes ownership of its publication here,
	// before enqueueLocked makes it visible to any pipeline goroutine (invariant 4).
	if j.pp != nil {
		j.pp = mqttOwnedPublish(j.pp)
	}
	p.enqueueLocked(j)
	p.mu.Unlock()
	return nil
}

// answer dispatches a response decided on the readLoop (a refusal, a reserved-topic answer).
func (p *mqttInboundPipe) answer(pi uint16, trace bool, r mqttInboundResp, more bool) error {
	return p.run(&mqttInboundJob{kind: mqttInboundAnswer, pi: pi, trace: trace, resp: r}, more)
}

// runInline is the direct path: the readLoop runs every step under its packet lease and writes
// the response itself. A failure is returned to the readLoop, exactly as the serial path did.
func (p *mqttInboundPipe) runInline(j *mqttInboundJob) error {
	switch j.kind {
	case mqttInboundAnswer:
	case mqttInboundQoS2Pub:
		j.resp, j.err = j.holdStep()
	case mqttInboundPubRel:
		if !j.releaseFences() {
			break
		}
		j.issueStep()
		j.completeStep()
	default:
		j.issueStep()
		j.completeStep()
	}
	if j.err != nil {
		p.mu.Lock()
		p.settleLocked(j, false)
		p.mu.Unlock()
		return j.err
	}
	// Settled before the write, as emit does: the client may answer the response at once.
	p.mu.Lock()
	p.settleLocked(j, j.resp.send)
	p.mu.Unlock()
	if j.resp.send {
		p.c.mqttWriteInboundResponse(j.resp, j.trace)
	}
	return nil
}

// enqueueLocked reserves j's position and starts what it needs. p.mu held.
func (p *mqttInboundPipe) enqueueLocked(j *mqttInboundJob) {
	p.seq++
	j.seq = p.seq
	if len(p.jobs) == 0 {
		// The cursors live in sequence space; an empty pipe restarts them at the new job.
		p.nextIssue, p.nextLease = j.seq, j.seq
	}
	p.jobs = append(p.jobs, j)
	switch j.kind {
	case mqttInboundAnswer:
		j.prereqDone, j.ready, j.leaseDone = true, true, true
	case mqttInboundQoS0, mqttInboundQoS1:
		j.prereqDone = true
	case mqttInboundQoS2Pub:
		j.prereqDone = true
		p.wg.Add(1)
		go p.runHold(j)
	case mqttInboundPubRel:
		p.wg.Add(1)
		go p.runRelease(j)
	}
	p.advanceLeaseLocked()
	if !p.issuerRunning {
		p.issuerRunning = true
		p.wg.Add(1)
		go p.issuer()
	}
	p.broadcastLocked()
}

// waitSpace blocks the readLoop while the pipe is full. Called before the packet's lease.
func (p *mqttInboundPipe) waitSpace() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.jobs) >= mqttInboundWindow {
		if !p.waitLocked() {
			return errMQTTInboundAborted
		}
	}
	return nil
}

// drain waits until the pipe is empty, then joins its goroutines. Called by the readLoop
// before any packet that is not a PUBLISH or PUBREL, before that packet's lease. A failed or
// aborted pipe answers errMQTTInboundAborted: the connection is already closing.
func (p *mqttInboundPipe) drain() error {
	p.mu.Lock()
	for !p.emptyLocked() {
		if p.failed || !p.waitLocked() {
			p.mu.Unlock()
			return errMQTTInboundAborted
		}
	}
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

// close fires the abort latch and joins every pipe goroutine: unstarted work is dropped,
// submitted stores are awaited and completed, nothing more is written. The readLoop calls it
// on every exit, before mqttHandleClosedClient.
func (p *mqttInboundPipe) close() {
	p.c.mqtt.fireAbort()
	p.wg.Wait()
	if h := mqttTestInboundJoinedV.Load(); h != nil {
		(*h)(p.c)
	}
}

// pending reports whether the pipe holds work. Any goroutine.
func (p *mqttInboundPipe) pending() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.emptyLocked()
}

// ---------------------------------------------------------------------------------------------
// Leases (in FIFO order).

// advanceLeaseLocked moves the lease cursor past every job that needs no turn. p.mu held.
func (p *mqttInboundPipe) advanceLeaseLocked() {
	// A job leaves the FIFO only once ready, and ready implies its turn is over, so the cursor
	// is never behind the head; clamping keeps that true by construction.
	if len(p.jobs) > 0 && p.nextLease < p.jobs[0].seq {
		p.nextLease = p.jobs[0].seq
	}
	for {
		j := p.jobAtLocked(p.nextLease)
		if j == nil || !j.leaseDone {
			return
		}
		p.nextLease++
	}
}

// acquireLease takes j's incarnation lease at its turn: only when every earlier job has taken
// one or finished, so the leased jobs are a prefix of the FIFO. While the tracker is closing
// it parks holding nothing, as a packet does; once retired it is refused.
func (p *mqttInboundPipe) acquireLease(j *mqttInboundJob) error {
	mqttTestInboundPause(j, "lease")
	p.mu.Lock()
	for p.nextLease != j.seq {
		if h := mqttTestInboundLeaseDecisionV.Load(); h != nil {
			(*h)(j, "cursor")
		}
		if !p.waitLocked() {
			p.mu.Unlock()
			return errMQTTInboundAborted
		}
	}
	p.mu.Unlock()
	l, err := p.c.acquireInboundJobLease(p.abort, func() {
		if h := mqttTestInboundLeaseDecisionV.Load(); h != nil {
			(*h)(j, "parked")
		}
	})
	p.mu.Lock()
	j.lease = l
	j.leaseDone = true
	p.advanceLeaseLocked()
	p.broadcastLocked()
	p.mu.Unlock()
	if err == nil {
		if h := mqttTestInboundLeaseDecisionV.Load(); h != nil {
			(*h)(j, "acquired")
		}
	}
	return err
}

// acquireInboundJobLease is acquirePacketLease for a queued job: it parks on a closing
// tracker's decision as a packet does, and also returns when the connection aborts.
func (c *client) acquireInboundJobLease(abort <-chan struct{}, parked func()) (*mqttLease, error) {
	t := c.mqtt.tracker
	if t == nil {
		return nil, nil
	}
	for {
		if l, ok := t.acquire(); ok {
			return l, nil
		}
		isClosing, decided := t.closingAndDecided()
		if !isClosing {
			return nil, errMQTTTrackerRetired
		}
		parked()
		select {
		case <-decided:
		case <-abort:
			return nil, errMQTTInboundAborted
		}
		if !t.wasReopened(decided) {
			return nil, errMQTTTrackerRetired
		}
	}
}

// ---------------------------------------------------------------------------------------------
// The same-identifier chain.

// waitChain blocks j until no earlier QoS 2 job on its identifier is unfinished.
func (p *mqttInboundPipe) waitChain(j *mqttInboundJob) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		var blocker uint64
		for _, k := range p.jobs {
			if k.seq >= j.seq {
				break
			}
			if k.pi == j.pi && (k.kind == mqttInboundQoS2Pub || k.kind == mqttInboundPubRel) && !k.ready {
				blocker = k.seq
			}
		}
		if h := mqttTestInboundJobStartV.Load(); h != nil {
			(*h)(j, blocker)
		}
		if blocker == 0 {
			return nil
		}
		if !p.waitLocked() {
			return errMQTTInboundAborted
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Workers.

// issuer takes jobs strictly at its cursor, runs each one's ordered step, and exits when it
// has nothing to issue. It never skips an unready job.
func (p *mqttInboundPipe) issuer() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		var j *mqttInboundJob
		for {
			cur := p.jobAtLocked(p.nextIssue)
			// The issuer takes the job AT its cursor or nothing: an unready cursor job is
			// waited for, never skipped, which is what keeps submissions in receipt order.
			var chosen *mqttInboundJob
			if cur != nil && cur.prereqDone {
				chosen = cur
			}
			if h := mqttTestInboundIssuerDecisionV.Load(); h != nil {
				(*h)(p, p.decisionLocked(cur, chosen, func(k *mqttInboundJob) bool { return k.prereqDone && !k.issued }))
			}
			if cur == nil || p.aborted() {
				p.issuerRunning = false
				p.broadcastLocked()
				p.mu.Unlock()
				return
			}
			if chosen != nil {
				j = chosen
				break
			}
			if !p.waitLocked() {
				p.issuerRunning = false
				p.broadcastLocked()
				p.mu.Unlock()
				return
			}
		}
		ordered := j.hasOrdered
		// The claim, in the same critical section as the choice: from here the job's ordered
		// step belongs to the issuer, so no abort may drop it (see runRelease).
		j.taken = true
		p.mu.Unlock()

		if ordered {
			p.issueOrdered(j)
		}

		p.mu.Lock()
		j.issued = true
		p.nextIssue = j.seq + 1
		p.broadcastLocked()
		p.mu.Unlock()
		p.emit()
	}
}

// issueOrdered runs a job's ordered step on the issuer.
func (p *mqttInboundPipe) issueOrdered(j *mqttInboundJob) {
	switch j.kind {
	case mqttInboundQoS0, mqttInboundQoS1:
		if err := p.acquireLease(j); err != nil {
			p.markReady(j, mqttInboundResp{}, err)
			return
		}
		mqttTestInboundPause(j, "issue")
		j.issueStep()
		if j.kind == mqttInboundQoS0 || !j.hasIssue {
			j.completeStep()
			p.markReady(j, j.resp, j.err)
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			j.completeStep()
			p.markReady(j, j.resp, j.err)
		}()
	case mqttInboundPubRel:
		// Its lease is held by its own goroutine since its fences began.
		mqttTestInboundPause(j, "issue")
		j.issueStep()
		mqttTestInboundPause(j, "issued")
		p.mu.Lock()
		j.issued = true // the release's goroutine waits for this before completing
		p.broadcastLocked()
		p.mu.Unlock()
	}
}

// runHold is a QoS 2 PUBLISH's goroutine: the chain, the lease, the hold step.
func (p *mqttInboundPipe) runHold(j *mqttInboundJob) {
	defer p.wg.Done()
	if err := p.waitChain(j); err != nil {
		p.drop(j)
		return
	}
	if err := p.acquireLease(j); err != nil {
		p.markReady(j, mqttInboundResp{}, err)
		return
	}
	mqttTestInboundPause(j, "hold")
	resp, err := j.holdStep()
	p.markReady(j, resp, err)
}

// runRelease is a PUBREL's goroutine: the chain, the lease, the fences; then, once the issuer
// has issued its onward store, the completion.
func (p *mqttInboundPipe) runRelease(j *mqttInboundJob) {
	defer p.wg.Done()
	if err := p.waitChain(j); err != nil {
		p.drop(j)
		return
	}
	if err := p.acquireLease(j); err != nil {
		p.mu.Lock()
		j.hasOrdered, j.prereqDone = false, true
		p.mu.Unlock()
		p.markReady(j, mqttInboundResp{}, err)
		return
	}
	mqttTestInboundPause(j, "fences")
	if !j.releaseFences() {
		p.mu.Lock()
		j.hasOrdered, j.prereqDone = false, true
		p.broadcastLocked()
		p.mu.Unlock()
		p.markReady(j, j.resp, j.err)
		return
	}
	p.mu.Lock()
	j.prereqDone = true
	p.broadcastLocked()
	for !j.issued {
		if j.taken {
			// The issuer claimed it: its onward delivery may already be submitted, so this
			// release keeps its lease and its release op and completes it (awaiting the store,
			// committing the terminal state) whatever the abort says. The issuer's step is
			// bounded and abort-aware, and it broadcasts when it is done.
			if p.aborted() {
				mqttTestInboundPause(j, "kept")
			}
			p.waitChangedLocked()
			continue
		}
		if !p.waitLocked() {
			if j.taken {
				continue // claimed while this waited: see above
			}
			p.mu.Unlock()
			// The pre-store checkpoint, reached only while the issuer has NOT claimed the
			// release: nothing onward was submitted, so the hold and the claim stay, exactly as
			// an unresolved release leaves them.
			j.rel.end()
			p.drop(j)
			return
		}
	}
	p.mu.Unlock()
	j.completeStep()
	p.markReady(j, j.resp, j.err)
}

// drop abandons an unanswered job: no response, its lease released.
func (p *mqttInboundPipe) drop(j *mqttInboundJob) {
	p.markReady(j, mqttInboundResp{}, errMQTTInboundAborted)
}

// markReady records j's response (or failure), releases its lease first (its last effect is
// done), and writes whatever is now ready at the head.
func (p *mqttInboundPipe) markReady(j *mqttInboundJob, resp mqttInboundResp, err error) {
	if j.lease != nil {
		j.lease.release()
	}
	p.mu.Lock()
	j.resp, j.err, j.ready = resp, err, true
	if !j.leaseDone {
		j.leaseDone = true
		p.advanceLeaseLocked()
	}
	p.broadcastLocked()
	p.mu.Unlock()
	if h := mqttTestInboundResponseReadyV.Load(); h != nil {
		(*h)(j)
	}
	p.emit()
}

// emit writes ready responses from the head, in order. Single drainer without a goroutine:
// whoever finds no one emitting becomes the emitter until the head is not ready. The head
// leaves the FIFO only after its write returned, so an "empty" pipe has nothing in flight.
func (p *mqttInboundPipe) emit() {
	p.mu.Lock()
	if p.emitting {
		p.mu.Unlock()
		return
	}
	p.emitting = true
	for {
		var head *mqttInboundJob
		if len(p.jobs) > 0 {
			head = p.jobs[0]
		}
		// Only the head is ever written: a later ready response waits for it.
		var chosen *mqttInboundJob
		if head != nil && head.ready && head.issued && !p.failed && !p.aborted() {
			chosen = head
		}
		if h := mqttTestInboundEmitDecisionV.Load(); h != nil {
			(*h)(p, p.decisionLocked(head, chosen, func(k *mqttInboundJob) bool { return k.ready }))
		}
		if chosen == nil {
			break
		}
		if head.err != nil {
			// The failure position: everything before it was written. Nothing after it is.
			p.failed = true
			p.settleLocked(head, false)
			p.mu.Unlock()
			p.fail(head)
			p.mu.Lock()
			break
		}
		// The ledger is settled BEFORE the write, under the same lock the readLoop's Receive
		// Maximum check takes: a client that receives this answer may send its next PUBLISH
		// at once, and the identifier must already be free when that PUBLISH is checked. A
		// write refused at this point only happens on a connection that is going away. The
		// head itself is popped only after the write returned, so an "empty" pipe never has a
		// response in flight.
		p.settleLocked(head, head.resp.send)
		p.mu.Unlock()
		written := head.resp.send && p.c.mqttWriteInboundResponse(head.resp, head.trace)
		if h := mqttTestInboundEmitOutcomeV.Load(); h != nil && head.resp.send {
			(*h)(head, written)
		}
		p.mu.Lock()
		p.jobs = p.jobs[1:]
		p.broadcastLocked()
	}
	p.emitting = false
	p.broadcastLocked()
	p.mu.Unlock()
}

// fail is what the readLoop's error exit does before its close, at the failure position:
// tell a v5 client why, then close the transport. The readLoop's Read fails and it runs the
// ordinary teardown (mqttHandleClosedClient before closeConnection); no worker runs teardown.
func (p *mqttInboundPipe) fail(j *mqttInboundJob) {
	c := p.c
	err := j.err
	if errors.Is(err, errMQTTInboundAborted) {
		c.closeTransport()
		return
	}
	if err == ErrMaxPayload {
		c.Warnf("MQTT maximum payload exceeded")
	} else {
		c.Errorf("%v", err)
		if c.mqtt.proto == mqttProtoLevel5 {
			c.mqttEnqueueDisconnect(mqttDisconnectReasonFromErr(err), err.Error())
		}
	}
	c.closeTransport()
}

// classifyRelease runs a release's retirement classification, serialized among this
// connection's releases; each still takes the record transaction and reads the watermark
// fresh. Both drivers call it, so the direct path's classification is the same code.
func (p *mqttInboundPipe) classifyRelease(j *mqttInboundJob, sess *mqttSession, asm *mqttAccountSessionManager) (mqttQoS2RetireVerdict, error) {
	hook := mqttTestInboundClassifyV.Load()
	select {
	case p.classify <- struct{}{}:
	default:
		if hook != nil {
			(*hook)(j, "waiting", mqttQoS2RetireUnresolved)
		}
		select {
		case p.classify <- struct{}{}:
		case <-p.abort:
			return mqttQoS2RetireUnresolved, errMQTTInboundAborted
		}
	}
	verdict, err := sess.classifyQoS2ReleaseAgainstRetirement(asm)
	<-p.classify
	if hook != nil {
		(*hook)(j, "returned", verdict)
	}
	return verdict, err
}

// ---------------------------------------------------------------------------------------------
// Readloop helpers.

// mqttInboundDrain drains c's pipe, if it has one. readLoop only.
func (c *client) mqttInboundDrain() error {
	if p := c.mqtt.inPipe; p != nil {
		return p.drain()
	}
	return nil
}

// mqttInboundClose aborts and joins c's pipe, if it has one. readLoop only, on every exit.
func (c *client) mqttInboundClose() {
	if c.mqtt == nil {
		return
	}
	if p := c.mqtt.inPipe; p != nil {
		p.close()
		return
	}
	c.mqtt.fireAbort()
}

// mqttWriteInboundResponse writes a PUBACK, PUBREC or PUBCOMP for an inbound exchange, and
// reports whether it was written. It never follows a server-initiated DISCONNECT
// ([MQTT-3.14.4-1]): the refusal and the write are one critical section
// (mqttEnqueueProtoLocked). Traced at emission (invariant 14).
func (c *client) mqttWriteInboundResponse(r mqttInboundResp, trace bool) bool {
	buf := c.mqttPubResponseFrame(r.pt, r.pi, r.reason, r.reasonStr)
	if !c.mqttFitsClientMax(len(buf)) {
		// Unreachable in practice: a connected client's Maximum Packet Size is bounded
		// below by its success CONNACK. [MQTT-3.1.2-24].
		c.Debugf("Suppressing MQTT packet of %d bytes exceeding client Maximum Packet Size %d", len(buf), c.mqtt.maxPacketSize)
		return false
	}
	if !c.mqttEnqueueProtoLocked(buf, true) {
		return false
	}
	c.mqttTracePubResponse(r.pt, r.pi, r.reason, trace)
	return true
}

// mqttOwnedPublish deep-copies the parser's publication, which the next packet overwrites.
func mqttOwnedPublish(pp *mqttPublish) *mqttPublish {
	cp := func(b []byte) []byte {
		if b == nil {
			return nil
		}
		return append([]byte(nil), b...)
	}
	return &mqttPublish{topic: cp(pp.topic), subject: cp(pp.subject), mapped: cp(pp.mapped), msg: cp(pp.msg),
		sz: pp.sz, pi: pp.pi, flags: pp.flags, props: cp(pp.props)}
}

// mqttAgeQueuedExpiry ages a publication's Message Expiry Interval by the whole seconds it
// waited since receipt. An interval of 0 is left as it is (the serial path never aged at
// receipt, and the release already treats 0 as expired); fully elapsed reports expired.
// Spec5 [3.3.2.3.3].
func mqttAgeQueuedExpiry(props []byte, receiptNano int64) (out []byte, expired bool) {
	orig, ok := mqttMessageExpiry(props)
	if !ok || orig == 0 || receiptNano <= 0 {
		return props, false
	}
	elapsed := (mqttExpiryNow().UnixNano() - receiptNano) / int64(time.Second)
	if elapsed <= 0 {
		return props, false
	}
	if elapsed >= int64(orig) {
		return mqttSetMessageExpiry(props, 0), true
	}
	return mqttSetMessageExpiry(props, orig-uint32(elapsed)), false
}

// ---------------------------------------------------------------------------------------------
// Test seams. Nil in production.

// mqttInboundDecision is what a worker saw and chose at one evaluation: recorded after the
// choice, so a choice other than the cursor's is recorded as made.
type mqttInboundDecision struct {
	Cursor uint64   // the job at the decision point (issuer: its cursor; emitter: the head); 0 = none
	Ready  []uint64 // jobs eligible at decision time
	Chosen uint64   // the job taken; 0 = none
}

func (p *mqttInboundPipe) decisionLocked(cur, chosen *mqttInboundJob, eligible func(*mqttInboundJob) bool) mqttInboundDecision {
	var d mqttInboundDecision
	if cur != nil {
		d.Cursor = cur.seq
	}
	if chosen != nil {
		d.Chosen = chosen.seq
	}
	for _, k := range p.jobs {
		if eligible(k) {
			d.Ready = append(d.Ready, k.seq)
		}
	}
	return d
}

var (
	mqttTestInboundSwitchV         atomic.Pointer[func(c *client, kind mqttInboundKind, pi uint16, direct bool)]
	mqttTestInboundIssuerDecisionV atomic.Pointer[func(p *mqttInboundPipe, d mqttInboundDecision)]
	mqttTestInboundEmitDecisionV   atomic.Pointer[func(p *mqttInboundPipe, d mqttInboundDecision)]
	mqttTestInboundEmitOutcomeV    atomic.Pointer[func(j *mqttInboundJob, written bool)]
	mqttTestInboundResponseReadyV  atomic.Pointer[func(j *mqttInboundJob)]
	mqttTestInboundLeaseDecisionV  atomic.Pointer[func(j *mqttInboundJob, decision string)]
	mqttTestInboundJobStartV       atomic.Pointer[func(j *mqttInboundJob, blockedBy uint64)]
	mqttTestInboundClassifyV       atomic.Pointer[func(j *mqttInboundJob, phase string, v mqttQoS2RetireVerdict)]
	mqttTestInboundJoinedV         atomic.Pointer[func(c *client)]
	mqttTestInboundDispatchV       atomic.Pointer[func(c *client, pt byte, drain bool, pending bool)]
	mqttTestInboundPauseV          atomic.Pointer[func(j *mqttInboundJob, step string)]
)

// mqttTestInboundPause parks a job at a named step for a test. Nil in production.
func mqttTestInboundPause(j *mqttInboundJob, step string) {
	if h := mqttTestInboundPauseV.Load(); h != nil {
		(*h)(j, step)
	}
}

var (
	// mqttTestShutdownDisconnectV observes shutdownDisconnect (see there). Nil in production.
	mqttTestShutdownDisconnectV atomic.Pointer[func(c *client, phase string)]
	// mqttTestShutdownClientsJoinedV runs when shutdown Phase 1's clientWG wait returned. Nil in
	// production.
	mqttTestShutdownClientsJoinedV atomic.Pointer[func()]
	// mqttTestClosedClientEntryV runs on entry to mqttHandleClosedClient. Nil in production.
	mqttTestClosedClientEntryV atomic.Pointer[func(c *client)]
	// mqttTestReadLoopErrorV runs when the readLoop's handling of a packet ends the connection
	// with an error of its own (a protocol violation, a refusal that closes), before the earlier
	// packets are drained. Nil in production.
	mqttTestReadLoopErrorV atomic.Pointer[func(c *client, err error)]
)
