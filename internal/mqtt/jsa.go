// Copyright 2020-2026 The NATS Authors
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

// The JetStream request layer (mqttJSA): request/reply over the account's NATS connection,
// atomic batches, stream, consumer and message operations, the API reply demultiplexer, and
// the queued JS API sender.

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nuid"
)

type mqttJSAResponse struct {
	reply string // will be used to map to the original request in jsa.NewRequestExMulti
	value any
}

// mqttJSA is one account's JetStream assistant: its shared state lives in mqttJSACore, and the
// struct around it is a VIEW of that core. Every view of one account shares the same core —
// replies, send queue, seams — and differs only in the deadline its requests honour (F-105 8a).
//
// The view exists because the ownership protocol owes ONE absolute deadline across every
// request it makes, nested resolutions and retries included, and the ops are methods on this
// type. Handing a deadline to each op would leave every nested call that reacquires the
// assistant uncovered; handing the ops a view covers them by construction. A view is never
// stored on a session: it lives exactly as long as the protocol run that asked for it.
type mqttJSA struct {
	*mqttJSACore
	// deadline, when set, caps every request this view makes. Zero means the core's per-request
	// timeout alone, which is every view but the ownership protocol's.
	deadline time.Time
	// gate, when set, is asked once per request this view queues, immediately before it is
	// queued; a closed gate refuses that request with nothing sent (withRequestGate, F-119). Nil
	// on every view but a background caller's that owes per-request admission and accounting.
	gate func() bool
}

// mqttJSACore is the state every view of one account's assistant shares.
type mqttJSACore struct {
	mu      sync.Mutex
	id      string
	c       *client
	sendq   *ipQueue[*mqttJSPubMsg]
	rplyr   string
	replies sync.Map // [string]chan *mqttJSAResponse
	nuid    *nuid.NUID
	quitCh  chan struct{}
	// domain is the JetStream domain from -js-domain, or empty for the domain of
	// the NATS server we are connected to. When set it both prefixes the JS API
	// subjects (see prefixDomain) and is added to the session subject.
	domain  string
	timeout time.Duration

	// subsBarrier lets QoS 1/2 message stores wait out any SUBSCRIBE that is
	// still being processed in this account. See mqttSubsBarrier.
	subsBarrier mqttSubsBarrier

	// acceptObs is the accepted-CONNECT store observation seam. nil in production;
	// a test arms it to observe the acceptance batch AT the store boundary, which is
	// the only place the pair's indivisibility is a fact about this write rather than
	// an invariant over time — the session record's subject is a MaxMsgsPer:1
	// register, so a later legitimate save replaces what the batch committed.
	//
	// Scoped to the account's JetStream assistant rather than to the package, because
	// the sentinel that arms it runs several brokers in one process and must observe
	// exactly one of them. See mqttAcceptStoreObs.
	acceptObs atomic.Pointer[func(mqttAcceptStoreObs) error]

	// exchangeSubjectsObs is the F-105 obligation-enumeration seam. nil in production; a
	// test arms it to answer, page by page, the walk that decides whether a packet identifier
	// is free (streamSubjectsComplete).
	//
	// It exists because the shapes that matter are MULTI-PAGE answers — the server pages a
	// stream-info subject list at JSMaxSubjectDetails and reports the true total beside it —
	// and malformed pages, and a fixture cannot reach the page size without writing a hundred
	// thousand records. The seam answers one page per call, envelope included, and the answer
	// passes through the same validation as the server's.
	exchangeSubjectsObs atomic.Pointer[func(filter string, offset int) (mqttSubjectPage, error)]

	// resourceSubjectsPageObs answers one page of the settlement's resource walk (F-105 7f): the
	// paging and the insertion-between-pages shapes the walk must survive cannot be manufactured
	// below the server's cap. nil in production.
	resourceSubjectsPageObs atomic.Pointer[func(filter string, offset int) (map[string]uint64, int, error)]

	// testTransitionCommit wraps an authority transition's store comparisons and send
	// (commitAuthorityTransition). nil in production. It is the one place a test can land a
	// write between an operation's read of the authority and its commit, or follow a real
	// commit with a lost response — the two ambiguities the ownership reconciliation exists to
	// resolve, neither of which a fixture can produce by timing.
	testTransitionCommit atomic.Pointer[func(t *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error)]

	// testTransitionPresend runs after every boundary check of an authority transition has passed
	// and before its batch is sent: the one window where only the store's own CAS can refuse it.
	// nil in production.
	testTransitionPresend atomic.Pointer[func(t *mqttAuthorityTransition)]

	// testDropRequest, when it returns true for a request (its API subject and body), swallows
	// it: it is never queued, so its reply never comes and the waiter runs out its wait — the
	// store that cannot answer, which a fixture cannot otherwise produce. nil in production.
	testDropRequest atomic.Pointer[func(subject string, body []byte) bool]

	// testObserveRequest sees every message this account's sender publishes (before it is
	// published, in send order) and every reply it hands back (on arrival, and again on
	// delivery to the waiter). nil in production. It is the witness for an OVERLAP: a pipelined
	// round issues exactly the requests a serial one does, so only the order of sends against
	// deliveries shows which one ran — and that order exists only here, where the broker issues
	// and receives. A tap on the server cannot see it: a request and its reply come from
	// different publishers and carry no common order there.
	testObserveRequest atomic.Pointer[func(mqttJSARequestEvent)]

	// testHoldReply, when armed, parks the replies it matches until the test releases them, so a
	// round's breadth is observable without racing its replies. nil in production.
	testHoldReply atomic.Pointer[mqttJSAReplyHold]
}

// testDropped reports whether an armed seam swallows a request.
func (jsa *mqttJSA) testDropped(subject string, body []byte) bool {
	if h := jsa.testDropRequest.Load(); h != nil {
		return (*h)(subject, body)
	}
	return false
}

// mqttJSARequestEventKind names what the request observer saw.
type mqttJSARequestEventKind uint8

const (
	// mqttJSARequestSent: the sender is about to publish a message (a request, or a no-wait
	// publication with an empty reply).
	mqttJSARequestSent mqttJSARequestEventKind = iota + 1
	// mqttJSAReplyArrived: a reply was matched to its waiter and decoded.
	mqttJSAReplyArrived
	// mqttJSAReplyDelivered: the decoded reply is being handed to its waiter. For a held reply
	// that is its release, not its arrival, so a held reply still counts as outstanding.
	mqttJSAReplyDelivered
)

// mqttJSARequestEvent is one observation: the published subject (sends only) and the reply
// subject that pairs a send with its reply.
type mqttJSARequestEvent struct {
	Kind    mqttJSARequestEventKind
	Subject string
	Reply   string
	Body    []byte // the request's body (sends only): a message GET names what it reads here
	Value   any    // the decoded reply, on arrival and delivery
}

func (jsa *mqttJSA) observeRequest(ev mqttJSARequestEvent) {
	if h := jsa.testObserveRequest.Load(); h != nil {
		(*h)(ev)
	}
}

// mqttJSAReplyHold parks matching replies AFTER they are decoded, so nothing deferred reads the
// delivery client's per-message state, and never blocks the reply dispatcher. Every waiter
// channel is buffered for its whole round, so a release after the waiter gave up, or after
// shutdown, still never blocks; and a reply's correlation entry is deleted on arrival, so a
// parked reply is delivered exactly once.
type mqttJSAReplyHold struct {
	mu       sync.Mutex
	match    func(reply string) bool
	released bool
	parked   []mqttJSAParkedReply
}

type mqttJSAParkedReply struct {
	ch chan *mqttJSAResponse
	r  *mqttJSAResponse
}

// park takes the reply if the hold is armed and matches it.
func (h *mqttJSAReplyHold) park(ch chan *mqttJSAResponse, r *mqttJSAResponse) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released || !h.match(r.reply) {
		return false
	}
	h.parked = append(h.parked, mqttJSAParkedReply{ch: ch, r: r})
	return true
}

type mqttJSPubMsg struct {
	subj  string
	reply string
	hdr   int
	msg   []byte
}

func (jsa *mqttJSA) newRequest(kind, subject string, hdr int, msg []byte) (any, error) {
	responses, err := jsa.newRequestExMulti(kind, subject, []int{hdr}, [][]byte{msg})
	if err != nil {
		return nil, err
	}
	if len(responses) != 1 {
		return nil, fmt.Errorf("unreachable: invalid number of responses (%d)", len(responses))
	}
	return responses[0].value, nil
}

func (jsa *mqttJSA) prefixDomain(subject string) string {
	if jsa.domain != _EMPTY_ {
		// rewrite js api prefix with domain
		if sub := strings.TrimPrefix(subject, JSApiPrefix+"."); sub != subject {
			subject = fmt.Sprintf("$JS.%s.API.%s", jsa.domain, sub)
		}
	}
	return subject
}

// newRequestExMulti sends multiple messages on the same subject and waits for
// all responses. It returns the same number of responses in the same order as
// msgs parameter. In case of a timeout it returns an error as well as all
// responses received as a sparsely populated array, matching msgs, with nils
// for the values that have not yet been received.
//
// Note that each response may represent an error and should be inspected as
// such by the caller.
func (jsa *mqttJSA) newRequestExMulti(kind, subject string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	subjects := make([]string, len(msgs))
	for i := range subjects {
		subjects[i] = subject
	}
	return jsa.newRequestExMultiSubjects(kind, subjects, hdrs, msgs)
}

// newRequestExMultiSubjects is newRequestExMulti with a PER-REQUEST subject, which a
// batch of consumer-info lookups needs: each names its own consumer in the subject rather
// than in a body. Same pipelining, same all-or-timeout semantics; a nil response entry
// means that one request went unanswered.
func (jsa *mqttJSA) newRequestExMultiSubjects(kind string, subjects []string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	kinds := make([]string, len(msgs))
	for i := range kinds {
		kinds[i] = kind
	}
	return jsa.newRequestExMultiKinds(kinds, subjects, hdrs, msgs)
}

// mqttJSARoundRequest is one read of a pipelined round: its reply kind, its API subject and its
// body.
type mqttJSARoundRequest struct {
	kind    string
	subject string
	body    []byte
}

// requestRound issues one round of independent reads of possibly different kinds together and
// returns their responses positionally: nil where a position went unanswered, with the round's
// error. It is the ingress for a caller that composes reads the single-read loaders would have
// issued one at a time, and that caller judges each position exactly as the read it replaces.
func (jsa *mqttJSA) requestRound(reqs []mqttJSARoundRequest) ([]*mqttJSAResponse, error) {
	kinds := make([]string, len(reqs))
	subjects := make([]string, len(reqs))
	bodies := make([][]byte, len(reqs))
	for i, r := range reqs {
		kinds[i], subjects[i], bodies[i] = r.kind, r.subject, r.body
	}
	return jsa.newRequestExMultiKinds(kinds, subjects, make([]int, len(reqs)), bodies)
}

// mqttLastForRead is one round position reading a subject's last message on a stream, the read
// loadLastMsgFor makes alone.
func mqttLastForRead(streamName, subject string) mqttJSARoundRequest {
	req, _ := json.Marshal(&JSApiMsgGetRequest{LastFor: subject})
	return mqttJSARoundRequest{kind: mqttJSAMsgLoad, subject: fmt.Sprintf(JSApiMsgGetT, streamName), body: req}
}

// newRequestExMultiKinds is the fan-out with a PER-REQUEST kind as well as subject: one round
// of independent requests of different kinds (an admission's ownership read, its stream's info
// and its consumer's info) is queued before any reply is awaited, and each reply is decoded by
// its own kind, which its reply subject carries. Results are positional, so a caller judges them
// in whatever order its serial form judged them, whatever order they arrived in.
//
// An EMPTY round returns at once: zero replies never satisfy the wait below, so without this a
// caller with nothing to read would wait out the whole request timeout.
func (jsa *mqttJSA) newRequestExMultiKinds(kinds, subjects []string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	if len(hdrs) != len(msgs) || len(subjects) != len(msgs) || len(kinds) != len(msgs) {
		return nil, fmt.Errorf("unreachable: invalid number of messages (%d), kinds (%d), subjects (%d) or header offsets (%d)",
			len(msgs), len(kinds), len(subjects), len(hdrs))
	}
	if len(msgs) == 0 {
		return []*mqttJSAResponse{}, nil
	}
	wait, derr := jsa.requestWait()
	if derr != nil {
		return nil, derr
	}
	if gerr := jsa.admitRequests(len(msgs)); gerr != nil {
		return nil, gerr
	}
	responseCh := make(chan *mqttJSAResponse, len(msgs))

	// Generate and queue all outgoing requests, have all results reported to
	// responseCh, and store a map of reply subjects to the original subjects'
	// indices.
	r2i := map[string]int{}
	for i, msg := range msgs {
		hdr := hdrs[i]
		var sb strings.Builder
		// Either we use nuid.Next() which uses a global lock, or our own nuid object, but
		// then it needs to be "write" protected. This approach will reduce across account
		// contention since we won't use the global nuid's lock.
		jsa.mu.Lock()
		uid := jsa.nuid.Next()
		sb.WriteString(jsa.rplyr)
		jsa.mu.Unlock()

		sb.WriteString(kinds[i])
		sb.WriteByte(btsep)
		sb.WriteString(uid)
		reply := sb.String()

		// Add responseCh to the reply channel map. It will be cleaned out on
		// timeout (see below), or in processJSAPIReplies upon receiving the
		// response.
		jsa.replies.Store(reply, responseCh)

		jsa.queueRequest(subjects[i], reply, hdr, msg)
		r2i[reply] = i
	}

	// Wait for all responses to come back, or for the timeout to expire. We
	// don't want to use time.After() which causes memory growth because the
	// timer can't be stopped and will need to expire to then be garbage
	// collected.
	c := 0
	responses := make([]*mqttJSAResponse, len(msgs))
	start := time.Now()
	t := time.NewTimer(wait)
	defer t.Stop()
	for {
		select {
		case r := <-responseCh:
			i := r2i[r.reply]
			responses[i] = r
			c++
			if c == len(msgs) {
				return responses, nil
			}

		case <-jsa.quitCh:
			return nil, ErrServerNotRunning

		case <-t.C:
			var reply string
			now := time.Now()
			for reply = range r2i { // preserve the last value for Errorf
				jsa.replies.Delete(reply)
			}

			if len(msgs) == 1 {
				return responses, jsa.timeoutError(fmt.Errorf("timeout after %v: request type %q on %q (reply=%q)", now.Sub(start), kinds[0], subjects[0], reply))
			}
			// Name what did not answer, not the round's first subject: a stuck read is
			// diagnosed by its own subject.
			var unanswered []string
			for i, r := range responses {
				if r == nil {
					unanswered = append(unanswered, kinds[i]+" "+subjects[i])
				}
			}
			const named = 8
			more := _EMPTY_
			if len(unanswered) > named {
				more = fmt.Sprintf(" and %d more", len(unanswered)-named)
				unanswered = unanswered[:named]
			}
			return responses, jsa.timeoutError(fmt.Errorf("timeout after %v: got %d out of %d; unanswered: %s%s",
				now.Sub(start), c, len(msgs), strings.Join(unanswered, ", "), more))
		}
	}
}

// mqttBatchMsg is one message of an atomic batch: a subject and the already-framed
// bytes (batch headers, per-subject CAS header, body) with the header length the
// send loop needs.
type mqttBatchMsg struct {
	subj string
	hdr  int
	msg  []byte
}

// newAtomicBatchRequest sends msgs as ONE JetStream atomic batch and waits for the
// COMMIT's PubAck. Every message must already carry its batch headers; the last one
// must carry the commit header, and the caller owns that framing.
//
// Exactly one reply subject is registered, and it goes on the commit message alone.
// That is not tidiness: JetStream answers a NON-commit message that carries a reply
// with an empty ack the moment it is staged (server/stream.go, the !commit arm), so
// a reply on the staged message would consume the correlation entry and the real
// commit PubAck would arrive with nowhere to go. The staged message therefore goes
// out with an empty reply, and the price is that a failure DURING staging is
// reported to nobody and surfaces here as a timeout — an ambiguity the caller's
// paired readback already has to resolve, since a lost response is ambiguous
// whether one message or two produced it.
//
// The batch id must be fresh per attempt. Reusing one while a batch is still staged
// creates a sequence gap on the server's staging store, which abandons the batch.
func (jsa *mqttJSA) newAtomicBatchRequest(kind string, msgs []mqttBatchMsg) (any, error) {
	if len(msgs) == 0 {
		return nil, fmt.Errorf("unreachable: an atomic batch with no messages")
	}
	wait, derr := jsa.requestWait()
	if derr != nil {
		return nil, derr
	}
	if gerr := jsa.admitRequests(1); gerr != nil { // one atomic batch is one request
		return nil, gerr
	}
	var sb strings.Builder
	jsa.mu.Lock()
	uid := jsa.nuid.Next()
	sb.WriteString(jsa.rplyr)
	jsa.mu.Unlock()
	sb.WriteString(kind)
	sb.WriteByte(btsep)
	sb.WriteString(uid)
	reply := sb.String()

	responseCh := make(chan *mqttJSAResponse, 1)
	jsa.replies.Store(reply, responseCh)

	for i, m := range msgs {
		r := _EMPTY_
		if i == len(msgs)-1 {
			r = reply
		}
		if jsa.testDropped(m.subj, m.msg) {
			continue
		}
		jsa.sendq.push(&mqttJSPubMsg{
			subj:  jsa.prefixDomain(m.subj),
			reply: r,
			hdr:   m.hdr,
			msg:   m.msg,
		})
	}

	start := time.Now()
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case resp := <-responseCh:
		return resp.value, nil
	case <-jsa.quitCh:
		jsa.replies.Delete(reply)
		return nil, ErrServerNotRunning
	case <-t.C:
		jsa.replies.Delete(reply)
		return nil, jsa.timeoutError(fmt.Errorf("timeout after %v: atomic batch request type %q on %q (reply=%q)",
			time.Since(start), kind, msgs[len(msgs)-1].subj, reply))
	}
}

func (jsa *mqttJSA) sendAck(ackSubject string) {
	// Send to the ack subject with no payload.
	jsa.sendMsg(ackSubject, nil)
}

// A CONFIRMED acknowledgement (the JetStream "double ack") has no primitive here: it exists only
// as the split form the positive source-ACK gate submits (outbound_source_ack_gate.go,
// prepareAckConfirm), so no caller can confirm an acknowledgement the gate did not see.

func (jsa *mqttJSA) sendMsg(subj string, msg []byte) {
	jsa.sendMsgReply(subj, _EMPTY_, msg)
}

// sendMsgReply is sendMsg with a reply subject: the same queued transport class (a no-wait
// publication that is not a JS API request), used by a confirmed acknowledgement whose answer is
// awaited on that reply (outbound_source_ack_gate.go).
func (jsa *mqttJSA) sendMsgReply(subj, reply string, msg []byte) {
	if subj == _EMPTY_ {
		return
	}
	// We pass -1 for the hdr so that the send loop does not need to
	// add the "client info" header. This is not a JS API request per se.
	jsa.sendq.push(&mqttJSPubMsg{subj: subj, reply: reply, msg: msg, hdr: -1})
}

func (jsa *mqttJSA) createEphemeralConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiConsumerCreateT, cfg.Stream)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

func (jsa *mqttJSA) createDurableConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiDurableCreateT, cfg.Stream, cfg.Config.Durable)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

// if noWait is specified, does not wait for the JS response, returns nil
// mqttTestConsumerDeleteRequestHookV observes every consumer delete at its request, on the deleting
// goroutine and before anything is sent, so a test can judge the delete against the state that
// licensed it at that moment. nil in production.
var mqttTestConsumerDeleteRequestHookV atomic.Pointer[func(stream, consumer string)]

func (jsa *mqttJSA) deleteConsumer(streamName, consName string, noWait bool) (*JSApiConsumerDeleteResponse, error) {
	if h := mqttTestConsumerDeleteRequestHookV.Load(); h != nil {
		(*h)(streamName, consName)
	}
	subj := fmt.Sprintf(JSApiConsumerDeleteT, streamName, consName)
	if noWait {
		// sendMsg is the no-wait publication class and adds no domain, so the JS API subject
		// takes it here, as queueRequest gives it to every awaited request: under -js-domain an
		// unprefixed delete is answered by the connected server's own domain, not the
		// configured one.
		jsa.sendMsg(jsa.prefixDomain(subj), nil)
		return nil, nil
	}

	cdri, err := jsa.newRequest(mqttJSAConsumerDel, subj, 0, nil)
	if err != nil {
		return nil, err
	}
	cdr := cdri.(*JSApiConsumerDeleteResponse)
	return cdr, cdr.ToError()
}

func (jsa *mqttJSA) createStream(cfg *StreamConfig) (*StreamInfo, bool, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, false, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamCreate, fmt.Sprintf(JSApiStreamCreateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, false, err
	}
	scr := scri.(*JSApiStreamCreateResponse)
	return scr.StreamInfo, scr.DidCreate, scr.ToError()
}

func (jsa *mqttJSA) lookupStream(name string) (*StreamInfo, error) {
	slri, err := jsa.newRequest(mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, name), 0, nil)
	if err != nil {
		return nil, err
	}
	slr := slri.(*JSApiStreamInfoResponse)
	return slr.StreamInfo, slr.ToError()
}

// lookupStreams reads several streams' info in ONE round, positionally, each answer as
// lookupStream would have returned it: a position that went unanswered carries the round's error.
func (jsa *mqttJSA) lookupStreams(names []string) ([]*StreamInfo, []error) {
	kinds := make([]string, len(names))
	subjects := make([]string, len(names))
	for i, n := range names {
		kinds[i], subjects[i] = mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, n)
	}
	resps, rerr := jsa.newRequestExMultiKinds(kinds, subjects, make([]int, len(names)), make([][]byte, len(names)))
	infos := make([]*StreamInfo, len(names))
	errs := make([]error, len(names))
	for i := range names {
		infos[i], errs[i] = mqttRoundStreamInfo(resps, i, rerr)
	}
	return infos, errs
}

// streamSubjects returns the subjects in streamName matching filter, mapped to
// their message counts, together with the stream's LAST SEQUENCE, via
// STREAM.INFO's subjects_filter. It reads no message bodies at all, which is the
// whole reason the release claim lives on a sibling subject: the discard has to
// enumerate a session's inbound QoS 2 state without pulling every held payload
// (up to max_payload each) across the JS API on a disconnect path.
//
// The last sequence is what lets a caller BOUND a subsequent destructive purge to
// what it actually enumerated (F-062). The two values are NOT one snapshot: the
// server builds State first and fills State.Subjects afterwards from a separate
// store read, so a subject can appear in the map while its record sits ABOVE the
// returned sequence. That direction is benign for every caller here — such a record
// is SPARED by a bounded purge and left to the orphan sweep.
//
// What the pair does NOT support is any inference about where the returned subjects sit
// relative to the sequence. The map carries per-subject COUNTS, not sequences, and it is
// sampled after State, so its occupants may lie on either side of the watermark. LastSeq is
// solely the earlier State watermark, and its only use is to BOUND a destructive purge;
// reading it as "everything returned is at or below this" is false.
//
// A nil map means the filter matched nothing. Callers must keep their filter
// narrow enough that the response cannot be paged: JetStream truncates the list
// at JSMaxSubjectDetails and reports the remainder only through the paging
// fields, which this deliberately does not follow (see mqttQoS2HoldWalkFilter,
// whose subject space is bounded by the 16-bit packet identifier).
func (jsa *mqttJSA) streamSubjects(streamName, filter string) (map[string]uint64, uint64, error) {
	req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
	if err != nil {
		return nil, 0, err
	}
	slri, err := jsa.newRequest(mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, streamName), 0, req)
	if err != nil {
		return nil, 0, err
	}
	slr := slri.(*JSApiStreamInfoResponse)
	if err := slr.ToError(); err != nil {
		return nil, 0, err
	}
	if slr.StreamInfo == nil {
		return nil, 0, fmt.Errorf("stream info for %q unavailable", streamName)
	}
	return slr.State.Subjects, slr.State.LastSeq, nil
}

// streamSubjectsComplete lists every subject under filter, and whether the answer is ALL of them.
//
// The server caps a stream-info subject list at JSMaxSubjectDetails (100,000) and reports the
// true total in the paged envelope, sorting and slicing beyond it. A caller that reads one page
// cannot tell a complete answer from a first page, and for a caller deciding whether something is
// ABSENT that difference is the whole answer - so the walk reads page after page until a VALIDATED
// page reaches the total that page reported, and reports incomplete on any page it cannot vouch
// for. Reading one page and refusing past it was the integrated gate's round-2 finding: the
// obligations these callers enumerate are never deleted, so the cap was a ceiling on how long a
// generation could keep admitting work, not a bound on what it holds.
//
// "Complete" is completion of this walk, not an atomic snapshot. Each page is recomputed by the
// server; a subject created between pages may shift a later one onto a second page (read twice,
// merged once) and may itself be missed. No subject under these filters is ever deleted, so none
// that existed when the walk began can be skipped; a caller that must exclude concurrent creators
// does so with its own fence (the authority CAS, for the exchange walk).
func (jsa *mqttJSA) streamSubjectsComplete(streamName, filter string) (map[string]uint64, bool, error) {
	all := map[string]uint64{}
	offset := 0
	for {
		p, err := jsa.streamSubjectsCompletePage(streamName, filter, offset)
		if err != nil {
			return nil, false, err
		}
		next, done, ok := mqttSubjectPageAdvance(p, filter, offset)
		if !ok {
			return all, false, nil
		}
		for subj, n := range p.Subjects {
			all[subj] = n
		}
		if done {
			return all, true, nil
		}
		offset = next
	}
}

// mqttSubjectPage is one stream-info subject page as the server answered it: the subjects and the
// paged envelope beside them.
type mqttSubjectPage struct {
	Subjects map[string]uint64
	Total    int
	Limit    int
	Offset   int
}

// streamSubjectsCompletePage requests the subject page at offset.
func (jsa *mqttJSA) streamSubjectsCompletePage(streamName, filter string, offset int) (mqttSubjectPage, error) {
	if obs := jsa.exchangeSubjectsObs.Load(); obs != nil {
		return (*obs)(filter, offset)
	}
	req, err := json.Marshal(&JSApiStreamInfoRequest{ApiPagedRequest: ApiPagedRequest{Offset: offset},
		SubjectsFilter: filter})
	if err != nil {
		return mqttSubjectPage{}, err
	}
	slri, err := jsa.newRequest(mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, streamName), 0, req)
	if err != nil {
		return mqttSubjectPage{}, err
	}
	slr := slri.(*JSApiStreamInfoResponse)
	if err := slr.ToError(); err != nil {
		return mqttSubjectPage{}, err
	}
	if slr.StreamInfo == nil {
		return mqttSubjectPage{}, fmt.Errorf("stream info for %q unavailable", streamName)
	}
	return mqttSubjectPage{Subjects: slr.State.Subjects, Total: slr.Total, Limit: slr.Limit,
		Offset: slr.Offset}, nil
}

// mqttSubjectPageAdvance validates the page answered for a request at offset and says where the walk
// goes next: the next offset, whether this page completes the walk, and whether the page can be
// vouched for at all. Real and scripted pages pass through this one decision.
//
// The server answers a filter matching nothing with no subjects and every paged field ZERO; that
// is a complete, empty answer, and only as the first page - past it, the subjects already read
// cannot have vanished. Any other answer must echo the requested offset, carry a positive page
// size, and hold EXACTLY min(Limit, Total-Offset) subjects under the filter: a short page is not
// the end of the list, it is a page that did not say what it should have.
func mqttSubjectPageAdvance(p mqttSubjectPage, filter string, offset int) (next int, done, ok bool) {
	if p.Total == 0 && p.Limit == 0 && p.Offset == 0 && len(p.Subjects) == 0 {
		return 0, true, offset == 0
	}
	if p.Offset != offset || p.Limit <= 0 || p.Total <= offset {
		return 0, false, false
	}
	want := min(p.Limit, p.Total-offset)
	if len(p.Subjects) != want {
		return 0, false, false
	}
	for subj := range p.Subjects {
		if !mqttSubjectMatchesFilter(subj, filter) {
			return 0, false, false
		}
	}
	next = offset + want
	return next, next >= p.Total, true
}

// purgeStream removes all messages in streamName matching filter (a subject,
// possibly wildcarded), up to but NOT INCLUDING upToSeq. If noWait, the request is
// fired without waiting for a response (like deleteConsumer/deleteMsg's no-wait
// paths).
//
// upToSeq == 0 is the UNBOUNDED shape: the request then carries no sequence and the
// server removes whatever matches the filter when the purge APPLIES, which on a
// clustered stream is after the caller's own enumeration. Callers that must not
// destroy a record they never saw pass a bound; purgeQoS2Hold refuses to be called
// without one at all (F-062).
func (jsa *mqttJSA) purgeStream(streamName, filter string, upToSeq uint64, noWait bool) error {
	preq := JSApiStreamPurgeRequest{Subject: filter, Sequence: upToSeq}
	req, _ := json.Marshal(preq)
	subj := jsa.prefixDomain(fmt.Sprintf(JSApiStreamPurgeT, streamName))
	if noWait {
		jsa.sendq.push(&mqttJSPubMsg{
			subj: subj,
			msg:  req,
		})
		return nil
	}
	spri, err := jsa.newRequest(mqttJSAStreamPurge, subj, 0, req)
	if err != nil {
		return err
	}
	spr := spri.(*JSApiStreamPurgeResponse)
	return spr.ToError()
}

// lookupConsumer returns the ConsumerInfo for one durable, or an error. A
// JSConsumerNotFoundErr is an AUTHORITATIVE absence and is returned as such by
// the callers that need to tell "gone" from "could not tell"; every other error
// is indeterminate.
func (jsa *mqttJSA) lookupConsumer(streamName, consName string) (*ConsumerInfo, error) {
	cii, err := jsa.newRequest(mqttJSAConsumerLookup, fmt.Sprintf(JSApiConsumerInfoT, streamName, consName), 0, nil)
	if err != nil {
		return nil, err
	}
	cir := cii.(*JSApiConsumerInfoResponse)
	if err := cir.ToError(); err != nil {
		return nil, err
	}
	return cir.ConsumerInfo, nil
}

// lookupConsumersMulti fetches several consumers' info in ONE pipelined batch, in the
// order given. Each response is decoded by mqttClassifyConsumerInfoResponse, so ok is
// true only where the response SETTLED the question — the consumer was read, or it is
// authoritatively absent — and false wherever nothing was established, including a
// success carrying a nil ConsumerInfo. That is the distinction the resume path and the
// in-flight source cache turn into delivery and classification decisions, so it is
// decided in one tested place rather than re-derived per caller.
//
// The serial sibling above is fine for one lookup. This exists because the resume barrier
// needs one per DURABLE before the CONNACK, and a session with a few thousand
// subscriptions would otherwise pay a few thousand serial round trips to reconnect.
func (jsa *mqttJSA) lookupConsumersMulti(streamName string, durables []string) (infos []*ConsumerInfo, ok []bool, err error) {
	subjects := make([]string, 0, len(durables))
	hdrs := make([]int, 0, len(durables))
	msgs := make([][]byte, 0, len(durables))
	for _, dur := range durables {
		subjects = append(subjects, fmt.Sprintf(JSApiConsumerInfoT, streamName, dur))
		hdrs = append(hdrs, 0)
		msgs = append(msgs, nil)
	}
	all, err := jsa.newRequestExMultiSubjects(mqttJSAConsumerLookup, subjects, hdrs, msgs)
	infos = make([]*ConsumerInfo, len(durables))
	ok = make([]bool, len(durables))
	for i, v := range all {
		if i >= len(durables) || v == nil {
			continue // no response: not ok, so the caller treats it as indeterminate
		}
		cir, isCIR := v.value.(*JSApiConsumerInfoResponse)
		if !isCIR {
			continue
		}
		infos[i], ok[i] = mqttClassifyConsumerInfoResponse(cir)
	}
	return infos, ok, err
}

// mqttClassifyConsumerInfoResponse decodes ONE consumer-info response into the
// (info, settled) pair every batched caller reasons about. Extracted so the decision
// lives in one place and can be tested directly: it is the whole difference between
// "authoritatively gone" and "could not tell", and its callers turn that difference
// into terminal verdicts.
//
// settled=true means the response SETTLES the question — either the consumer is
// authoritatively absent (nil info) or it was read (non-nil info). settled=false means
// nothing was established and the caller must treat the durable as indeterminate.
//
// A successful response carrying a NIL ConsumerInfo is deliberately NOT settled, and
// that is the correction this function exists for. It used to be encoded exactly like
// an authoritative absence, so the two were indistinguishable — and the in-flight
// source cache reads a settled absence as mqttInflightSourceLost, a terminal verdict.
// Concluding "known to be gone and unrecoverable" from a reply that proved nothing is
// the shape docs/ARCHITECTURE.md forbids: an absence is not a fact unless something
// established it.
//
// A non-nil ConsumerInfo with a nil Config IS settled here — the consumer was read.
// Whether that shape is USABLE is a question about the config, and the callers that
// care answer it themselves rather than having this decode fail closed on their behalf.
func mqttClassifyConsumerInfoResponse(cir *JSApiConsumerInfoResponse) (*ConsumerInfo, bool) {
	if cir == nil {
		return nil, false
	}
	if aerr := cir.ToError(); aerr != nil {
		// Only "consumer not found" proves absence; anything else is a failed read.
		if isErrorOtherThan(aerr, JSConsumerNotFoundErr) {
			return nil, false
		}
		return nil, true // absent, authoritatively
	}
	if cir.ConsumerInfo == nil {
		return nil, false // a success that says nothing: not an absence
	}
	return cir.ConsumerInfo, true
}

// consumerList returns ConsumerInfo for the consumers of the stream that the paged
// CONSUMER.LIST API reports, walking it to the end. A walk across pages is not a snapshot:
// a create or delete between pages can make it miss or repeat a consumer (repeats are
// dropped), so a caller may rely on what it is given, never on what is absent from it.
func (jsa *mqttJSA) consumerList(streamName string) ([]*ConsumerInfo, error) {
	return mqttWalkConsumerList(func(offset int) (*JSApiConsumerListResponse, error) {
		return jsa.consumerListPage(streamName, offset)
	})
}

// consumerListPage requests one CONSUMER.LIST page at offset.
func (jsa *mqttJSA) consumerListPage(streamName string, offset int) (*JSApiConsumerListResponse, error) {
	req, err := json.Marshal(&JSApiConsumersRequest{ApiPagedRequest: ApiPagedRequest{Offset: offset}})
	if err != nil {
		return nil, err
	}
	cli, err := jsa.newRequest(mqttJSAConsumerList, fmt.Sprintf(JSApiConsumerListT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	clr := cli.(*JSApiConsumerListResponse)
	if clr.Error != nil {
		return nil, clr.ToError()
	}
	return clr, nil
}

// mqttConsumerListEntryName is the name a CONSUMER.LIST entry is reported under. A
// clustered all-peers-offline consumer is reported as a synthetic entry carrying only its
// assignment's config, with no top-level name; for an unnamed ephemeral even the config
// carries none, and the result is empty.
func mqttConsumerListEntryName(ci *ConsumerInfo) string {
	if ci == nil {
		return _EMPTY_
	}
	if ci.Name != _EMPTY_ {
		return ci.Name
	}
	if ci.Config == nil {
		return _EMPTY_
	}
	if ci.Config.Durable != _EMPTY_ {
		return ci.Config.Durable
	}
	return ci.Config.Name
}

// mqttWalkConsumerList pages CONSUMER.LIST through fetch and returns every consumer a page
// reported with its information, once each.
//
// The offset advances by the positions a page COVERED, which is neither the entries it
// returned nor the server's limit. The pinned handlers differ (nats-server v2.15.1): the
// standalone one examines positions until it has returned 256 infos, listing an offline
// consumer in Missing and silently skipping one closed under it; the clustered one selects
// 256 assignments and reports each as answered, as a synthetic offline entry that is ALSO
// in Missing, or as timed out and in Missing only. So a page's coverage is the number of
// distinct names across its entries and its Missing list: exactly the selected slice when
// clustered, and an under-count when standalone (a silently skipped position), which
// only re-reads an overlap the dedup absorbs. Never an over-count, so the walk never skips
// a position by its own arithmetic. Counted per page, independent of the dedup.
//
// A consumer named in Missing is not returned: the LIST could not report it, so it is not
// a candidate for anything until a later walk sees it. The walk ends on a page that covers
// nothing, or once the offset reaches a positive Total (the clustered handler answers an
// exhausted offset with Total 0, which the first rule ends).
func mqttWalkConsumerList(fetch func(offset int) (*JSApiConsumerListResponse, error)) ([]*ConsumerInfo, error) {
	var all []*ConsumerInfo
	returned := make(map[string]struct{})
	for offset := 0; ; {
		page, err := fetch(offset)
		if err != nil {
			return nil, err
		}
		missing := make(map[string]struct{}, len(page.Missing))
		covered := make(map[string]struct{}, len(page.Consumers)+len(page.Missing))
		for _, name := range page.Missing {
			missing[name] = struct{}{}
			covered[name] = struct{}{}
		}
		for _, ci := range page.Consumers {
			name := mqttConsumerListEntryName(ci)
			if name == _EMPTY_ {
				continue // unattributable; if it occupied a position, Missing counts it
			}
			covered[name] = struct{}{}
			if _, ok := missing[name]; ok {
				continue
			}
			if _, ok := returned[name]; ok {
				continue
			}
			returned[name] = struct{}{}
			all = append(all, ci)
		}
		if len(covered) == 0 {
			return all, nil
		}
		offset += len(covered)
		if page.Total > 0 && offset >= page.Total {
			return all, nil
		}
	}
}

func (jsa *mqttJSA) loadLastMsgFor(streamName string, subject string) (*StoredMsg, error) {
	if h := mqttTestLoadLastMsgHookV.Load(); h != nil {
		if err := (*h)(streamName, subject); err != nil {
			return nil, err
		}
	}
	mreq := &JSApiMsgGetRequest{LastFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

// loadMsgsMulti loads messages by EXACT sequence in one pipelined batch, preserving the
// order of seqs in the result. The by-subject sibling below cannot serve this: a source
// delivery is identified by its stream sequence, not by a subject.
//
// Used by the resume barrier, where the alternative is one synchronous round trip per
// in-flight delivery before the CONNACK. A nil entry means that request produced no
// response, which the caller must treat as indeterminate rather than as absence.
func (jsa *mqttJSA) loadMsgsMulti(streamName string, seqs []uint64) ([]*JSApiMsgGetResponse, error) {
	marshaled := make([][]byte, 0, len(seqs))
	headerBytes := make([]int, 0, len(seqs))
	for _, seq := range seqs {
		bb, err := json.Marshal(&JSApiMsgGetRequest{Seq: seq})
		if err != nil {
			return nil, err
		}
		marshaled = append(marshaled, bb)
		headerBytes = append(headerBytes, 0)
	}
	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), headerBytes, marshaled)
	responses := make([]*JSApiMsgGetResponse, len(all))
	for i, v := range all {
		if v != nil {
			responses[i] = v.value.(*JSApiMsgGetResponse)
		}
	}
	return responses, err
}

func (jsa *mqttJSA) loadLastMsgForMulti(streamName string, subjects []string) ([]*JSApiMsgGetResponse, error) {
	marshaled := make([][]byte, 0, len(subjects))
	headerBytes := make([]int, 0, len(subjects))
	for _, subject := range subjects {
		mreq := &JSApiMsgGetRequest{LastFor: subject}
		bb, err := json.Marshal(mreq)
		if err != nil {
			return nil, err
		}
		marshaled = append(marshaled, bb)
		headerBytes = append(headerBytes, 0)
	}

	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), headerBytes, marshaled)
	// all has the same order as subjects, preserve it as we unmarshal
	responses := make([]*JSApiMsgGetResponse, len(all))
	for i, v := range all {
		if v != nil {
			responses[i] = v.value.(*JSApiMsgGetResponse)
		}
	}
	return responses, err
}

// loadNextMsgFromSeq returns the first message on the given subject (which may
// contain wildcards) with a sequence >= seq, allowing a bounded walk over all
// messages of a subject filter.
func (jsa *mqttJSA) loadNextMsgFromSeq(streamName, subject string, seq uint64) (*StoredMsg, error) {
	if h := mqttTestLoadNextMsgHookV.Load(); h != nil {
		if err := (*h)(streamName, subject, seq); err != nil {
			return nil, err
		}
	}
	mreq := &JSApiMsgGetRequest{Seq: seq, NextFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

func (jsa *mqttJSA) loadMsg(streamName string, seq uint64) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{Seq: seq}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

func (jsa *mqttJSA) storeMsg(subject string, headers int, msg []byte) (*JSPubAckResponse, error) {
	p := jsa.submitStoreMsg(subject, headers, msg)
	return p.await()
}

// mqttPendingStore is a store that has been SUBMITTED and not yet answered. Submitting and
// awaiting are separate so that the inbound acknowledgement pipeline can issue one connection's
// stores in publication order without waiting for each (inbound_pipeline.go): sendq has a single
// drainer, so the order of submissions is the order on the wire. storeMsg is exactly
// submit().await(), so a store has one implementation whichever way it is driven.
type mqttPendingStore struct {
	jsa     *mqttJSA
	subject string
	reply   string
	ch      chan *mqttJSAResponse
	wait    time.Duration
	start   time.Time
	// err is set when the submission itself failed, in which case nothing was sent and no reply
	// is registered.
	err error
}

// submitStoreMsg registers the reply and queues the store request. The pre-store seam and the
// drop seam run here, at submission, where the request would leave. Returned by value: the
// caller holds it until it awaits, and a direct-path publication then allocates nothing for it.
func (jsa *mqttJSA) submitStoreMsg(subject string, headers int, msg []byte) mqttPendingStore {
	p := mqttPendingStore{jsa: jsa, subject: subject}
	if h := mqttTestStoreMsgHookV.Load(); h != nil {
		if err := (*h)(subject, headers, msg); err != nil {
			p.err = err
			return p
		}
	}
	wait, derr := jsa.requestWait()
	if derr != nil {
		p.err = derr
		return p
	}
	if gerr := jsa.admitRequests(1); gerr != nil {
		p.err = gerr
		return p
	}
	var sb strings.Builder
	jsa.mu.Lock()
	uid := jsa.nuid.Next()
	sb.WriteString(jsa.rplyr)
	jsa.mu.Unlock()
	sb.WriteString(mqttJSAMsgStore)
	sb.WriteByte(btsep)
	sb.WriteString(uid)
	p.reply = sb.String()
	p.ch = make(chan *mqttJSAResponse, 1)
	p.wait = wait
	p.start = time.Now()
	// The correlation entry is stored before the request enters sendq, so a fast reply always
	// finds it (see processJSAPIReplies).
	jsa.replies.Store(p.reply, p.ch)
	jsa.queueRequest(subject, p.reply, headers, msg)
	return p
}

// queueRequest queues one JS API request whose reply is already registered: the single
// producer of that transport class, for the fan-out (newRequestExMultiSubjects) and a
// submitted store alike. A dropped request (the test seam) is registered and never sent, so its
// waiter runs out its wait.
func (jsa *mqttJSA) queueRequest(subject, reply string, hdr int, msg []byte) {
	if jsa.testDropped(subject, msg) {
		return
	}
	jsa.sendq.push(&mqttJSPubMsg{
		subj:  jsa.prefixDomain(subject),
		reply: reply,
		hdr:   hdr,
		msg:   msg,
	})
}

// await waits for the submitted store's PubAck, under the same deadline, classification and
// result seam as every other request. Every exit that did not consume a reply deletes the
// registration, so an abandoned store leaves nothing behind in the reply map.
func (p *mqttPendingStore) await() (*JSPubAckResponse, error) {
	jsa := p.jsa
	if p.err != nil {
		return jsa.storeMsgResult(p.subject, nil, p.err)
	}
	t := time.NewTimer(p.wait)
	defer t.Stop()
	select {
	case r := <-p.ch:
		smr := r.value.(*JSPubAckResponse)
		return jsa.storeMsgResult(p.subject, smr, smr.ToError())
	case <-jsa.quitCh:
		jsa.replies.Delete(p.reply)
		return jsa.storeMsgResult(p.subject, nil, ErrServerNotRunning)
	case <-t.C:
		jsa.replies.Delete(p.reply)
		err := jsa.timeoutError(fmt.Errorf("timeout after %v: request type %q on %q (reply=%q)",
			time.Since(p.start), mqttJSAMsgStore, p.subject, p.reply))
		return jsa.storeMsgResult(p.subject, nil, err)
	}
}

// storeMsgResult applies the post-store fault seam. It exists because the
// pre-store hook above models the wrong half of the interesting failure: a store
// that never happened leaves nothing behind, whereas the case that actually
// wedges state is a store that LANDED (and may already have been delivered) whose
// response the caller never saw. Only a post-store seam can produce that.
func (jsa *mqttJSA) storeMsgResult(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
	if h := mqttTestStoreMsgResultHookV.Load(); h != nil {
		return (*h)(subject, resp, err)
	}
	return resp, err
}

func (jsa *mqttJSA) storeSessionMsg(domainTk, cidHash string, hdr int, msg []byte) (*JSPubAckResponse, error) {
	if mqttTestSessStoreSuppressV.Load() {
		// Test seam: the request never reaches the stream, and the caller sees an
		// error it cannot classify. This is the "issued but did not land" half of
		// ambiguity — the result hook models the other half (landed, response lost) —
		// and the two must be distinguishable, because the reconciliation the
		// readback owes differs completely between them.
		return nil, errors.New("mqtt test: session record store suppressed")
	}
	// Compute subject where the session is being stored
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash
	smri, err := jsa.newRequest(mqttJSASessPersist, subject, hdr, msg)
	if err != nil {
		return nil, err
	}
	smr := smri.(*JSPubAckResponse)
	return smr, smr.ToError()
}

func (jsa *mqttJSA) loadSessionMsg(domainTk, cidHash string) (*StoredMsg, error) {
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash
	return jsa.loadLastMsgFor(mqttSessStreamName, subject)
}

func (jsa *mqttJSA) deleteMsg(stream string, seq uint64, wait bool) error {
	if h := mqttTestDeleteMsgHookV.Load(); h != nil {
		if err := (*h)(stream, seq); err != nil {
			return err
		}
	}
	dreq := JSApiMsgDeleteRequest{Seq: seq, NoErase: true}
	req, _ := json.Marshal(dreq)
	subj := jsa.prefixDomain(fmt.Sprintf(JSApiMsgDeleteT, stream))
	if !wait {
		jsa.sendq.push(&mqttJSPubMsg{
			subj: subj,
			msg:  req,
		})
		return nil
	}
	dmi, err := jsa.newRequest(mqttJSAMsgDelete, subj, 0, req)
	if err != nil {
		return err
	}
	dm := dmi.(*JSApiMsgDeleteResponse)
	return dm.ToError()
}

// Returns true if `err` is not nil and does not match the api error with ErrorIdentifier id
func isErrorOtherThan(err error, id ErrorIdentifier) bool {
	return err != nil && !IsNatsErr(err, id)
}

// jsAPIError extracts the JetStream API error from err, seeing through %w
// wrapping. IsNatsErr does a bare type assertion and therefore does not.
func jsAPIError(err error) *ApiError {
	var ae *ApiError
	if errors.As(err, &ae) {
		return ae
	}
	return nil
}

// isJSErrCausedBy reports whether err is the JetStream API error identified by
// id whose interpolated cause is `cause`.
//
// A few JetStream API errors are generic containers: JSStreamStoreFailedF
// (10077) and JSStreamMsgDeleteFailedF (10057) both have Description "{err}",
// so the only record of what actually went wrong is the cause flattened into
// ApiError.Description. The error identity is matched structurally on ErrCode;
// the cause then needs a text comparison, which nats-server gives no way to
// avoid. Confining it here is the point of this helper: comparing the RENDERED
// ApiError.Error() ("<description> (<code>)") instead — as the QoS2 dedup path
// used to — breaks on any change to the outer formatting, and a bare substring
// match over-matches (a cause that merely mentions the sentinel text is not the
// sentinel). So: exact Description, or the ": "+cause suffix a cause wrapped
// before ApiError construction produces. Nothing else.
//
// A change to the flattened cause wording itself still defeats recognition.
func isJSErrCausedBy(err error, id ErrorIdentifier, cause error) bool {
	if err == nil || cause == nil {
		return false
	}
	ae := jsAPIError(err)
	if ae == nil {
		return false
	}
	want, ok := ApiErrors[id]
	if !ok || want == nil || ae.ErrCode != want.ErrCode {
		return false
	}
	ct := cause.Error()
	return ae.Description == ct || strings.HasSuffix(ae.Description, ": "+ct)
}

// Process JS API replies.
//
// Runs on ONE goroutine. Every reply for this account arrives on the single
// wildcard subscription `jsa.rplyr+">"` created in createSubscriptions, and
// nats-go dispatches a subscription's messages serially, so two invocations
// never race for the same reply subject. That is what makes the load-then-delete
// below safe without being atomic: the entry is consumed exactly once because
// only one goroutine can reach it.
//
// Upstream's comment here said the opposite ("can run from various go
// routines"), which is true where MQTT rides nats-server internals and false in
// this standalone port. It is corrected rather than kept because it describes
// the one execution model this function is NOT safe under.
//
// Splitting the wildcard into per-kind subscriptions -- floated in
// createSubscriptions -- keeps that safety only under all four of:
//
//   - the parts form an EXHAUSTIVE, non-overlapping partition of the reply
//     prefix. Non-overlap alone gives "at most one" matching callback, not
//     "exactly one"; a gap silently drops replies into the request timeout.
//   - each part still dispatches its callbacks serially.
//   - each part is subscribed before the send loop starts, on the same NATS
//     connection, so the server processes the SUB before any later PUB. (The
//     readiness Flush in processSub covers $MQTT.sub.* only, NOT $MQTT.JSA.*,
//     so a partition that loses same-connection ordering needs its own flush.)
//   - each correlation entry is stored before its request enters sendq, as
//     newRequestExMultiSubjects and newAtomicBatchRequest already do --
//     otherwise a fast reply finds no entry and is discarded.
//
// If any of those cannot hold, make the removal atomic (LoadAndDelete) first.
func (as *mqttAccountSessionManager) processJSAPIReplies(_ *subscription, pc *client, _ *Account, subject, _ string, msg []byte) {
	token := tokenAt(subject, mqttJSATokenPos)
	if token == _EMPTY_ {
		return
	}
	jsa := &as.jsa
	chi, ok := jsa.replies.Load(subject)
	if !ok {
		return
	}
	jsa.replies.Delete(subject)
	ch := chi.(chan *mqttJSAResponse)
	out := func(value any) {
		r := &mqttJSAResponse{reply: subject, value: value}
		jsa.observeRequest(mqttJSARequestEvent{Kind: mqttJSAReplyArrived, Reply: subject, Value: value})
		if h := jsa.testHoldReply.Load(); h != nil && h.park(ch, r) {
			return
		}
		jsa.observeRequest(mqttJSARequestEvent{Kind: mqttJSAReplyDelivered, Reply: subject, Value: value})
		ch <- r
	}
	// A status is the transport answering, not JetStream, so nothing in it is decoded.
	status := mqttJSAReplyStatus(pc, msg)
	decode := func(resp any, apiErr **ApiError) {
		if status != nil {
			*apiErr = status
		} else if err := json.Unmarshal(msg, resp); err != nil {
			*apiErr = NewJSInvalidJSONError(err)
		}
		out(resp)
	}
	switch token {
	case mqttJSAStreamCreate:
		resp := &JSApiStreamCreateResponse{}
		decode(resp, &resp.Error)
	case mqttJSAStreamLookup:
		resp := &JSApiStreamInfoResponse{}
		decode(resp, &resp.Error)
	case mqttJSAStreamPurge:
		resp := &JSApiStreamPurgeResponse{}
		decode(resp, &resp.Error)
	case mqttJSAConsumerCreate:
		resp := &JSApiConsumerCreateResponse{}
		decode(resp, &resp.Error)
	case mqttJSAConsumerDel:
		resp := &JSApiConsumerDeleteResponse{}
		decode(resp, &resp.Error)
	case mqttJSAMsgStore, mqttJSASessPersist:
		resp := &JSPubAckResponse{}
		decode(resp, &resp.Error)
	case mqttJSAMsgLoad:
		resp := &JSApiMsgGetResponse{}
		decode(resp, &resp.Error)
	case mqttJSAConsumerLookup:
		resp := &JSApiConsumerInfoResponse{}
		decode(resp, &resp.Error)
	case mqttJSAConsumerList:
		resp := &JSApiConsumerListResponse{}
		decode(resp, &resp.Error)
	case mqttJSAMsgDelete:
		resp := &JSApiMsgDeleteResponse{}
		decode(resp, &resp.Error)
	case mqttJSAAckConfirm:
		// A double-ack confirmation carries no body: the ARRIVAL of a reply that is not a
		// status is the answer. A status — no responders, because no consumer leader holds
		// the ack subscription — proves the acknowledgement reached nobody, so it is the
		// failure, never the confirmation.
		if status != nil {
			out(status)
			return
		}
		out(nil)
	default:
		pc.Warnf("Unknown reply code %q", token)
	}
}

// mqttJSAReplyStatus reports the transport status a JSA reply carries, or nil for an
// answer.
//
// Every JSA request is published with a reply subject, and nats.go always asks for
// no-responders notifications, so a request nothing receives — JetStream unavailable to
// the account, an unserved domain, a stream or consumer with no leader holding its
// subscription — is answered on our own reply subscription by a header-only "NATS/1.0 503".
// nats.go decodes the status line into a Status header, which frameNATSMsg preserves. What
// makes a reply a status is that header in the message's OWN header block, msg[:pa.hdr]:
// a header without it is an answer, and a body is never a header block. pc is the
// subscription's delivery client, whose pa.hdr processSub sets for this message before the
// callback, on the subscription's one dispatch goroutine.
//
// The error carries the status as its Code and NO JetStream error code. Every classifier in
// the broker matches an ApiError by error code, and none is zero, so no classifier can read
// a status as a definite answer: it lands where a timeout lands, which is what it is — the
// request was never answered by JetStream.
func mqttJSAReplyStatus(pc *client, msg []byte) *ApiError {
	hdr := pc.pa.hdr
	if hdr <= 0 || hdr > len(msg) {
		return nil
	}
	h := msg[:hdr]
	st := sliceHeader("Status", h)
	if len(st) == 0 {
		return nil
	}
	code, _ := strconv.Atoi(string(st))
	target := string(sliceHeader("Nats-Subject", h))
	if string(st) == "503" {
		return &ApiError{Code: code, Description: fmt.Sprintf(
			"no responders: nothing is subscribed to %q (JetStream unavailable to this account, or the stream or consumer has no leader)",
			target)}
	}
	desc := string(st)
	if d := sliceHeader("Description", h); len(d) > 0 {
		desc += " " + string(d)
	}
	return &ApiError{Code: code, Description: fmt.Sprintf("status %s answering %q", desc, target)}
}

// mqttJSARequestHeader builds the fixed Nats-Request-Info header prefixed to
// every JS API request for an account.
//
// The ClientInfo names the account and nothing else -- in particular it must
// never carry a Cluster. Upstream set ClientInfo.Cluster from its own cluster
// name so an embedded, clustered JetStream would place assets locally. This
// broker has no cluster of its own: the only value available here is its node id
// (a hostname, or a generated "mnb-<nuid>"), which is not a NATS cluster name
// and would be nonsense to a server reading it for placement.
//
// Nothing was ever actually misrouted by the old code, because
// processInboundClientMsg strips this header before publishing (it is a trusted
// server-internal header, meaningless from an external client). But serializing
// a fabricated cluster name into every request was dead weight waiting to be
// believed the moment that strip moved or a code path bypassed it. The broker is
// a plain client; the server places.
func mqttJSARequestHeader(accName string) []byte {
	b, _ := json.Marshal(ClientInfo{Account: accName})
	hdrStart := bytes.Buffer{}
	hdrStart.WriteString(hdrLine)
	// A bytes.Buffer write cannot fail, so neither can this.
	_ = http.Header{ClientInfoHdr: []string{string(b)}}.Write(&hdrStart)
	hdrStart.WriteString(CR_LF)
	hdrStart.WriteString(CR_LF)
	return hdrStart.Bytes()
}

// Loop to send JS API requests for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
// mqttTestJSSendPublishHookV runs in the send-queue publisher immediately before each queued
// message is published, in queue order, so a test can delay a publication it has already
// enqueued. nil in production.
var mqttTestJSSendPublishHookV atomic.Pointer[func(subject string)]

func (as *mqttAccountSessionManager) sendJSAPIrequests(s *Server, c *client, accName string, closeCh chan struct{}) {
	as.mu.RLock()
	sendq := as.jsa.sendq
	quitCh := as.jsa.quitCh
	jsa := as.jsa
	acc := c.acc
	as.mu.RUnlock()

	// The account session manager does not have a suhtdown API per-se, instead,
	// we will cleanup things when this go routine exits after detecting that the
	// server is shutdown or the initialization of the account manager failed.
	defer func() {
		as.mu.Lock()
		if as.flapTimer != nil {
			as.flapTimer.Stop()
			as.flapTimer = nil
		}
		// Stop any pending session-expiry timers and disable further
		// scheduling/firing; the nil map makes expireSessionFired a no-op for a timer
		// already mid-callback. Spec5 [3.1.2.11.2].
		for _, t := range as.pendingExpiries {
			t.timer.Stop()
		}
		as.pendingExpiries = nil
		// Drop reconciliation ordering state. Any worker still in flight holds its
		// own *sessionReconcileState pointer and decrements it directly, and its
		// map-identity check finds this nil map (delete on nil is a no-op), so
		// nilling here is safe. New workers cannot start: admitWillWorker is closed.
		as.reconcileWM = nil
		as.mu.Unlock()
	}()

	hdrb := mqttJSARequestHeader(accName)

	for {
		select {
		case <-sendq.ch:
			pmis := sendq.pop()
			for _, r := range pmis {
				if h := mqttTestJSSendPublishHookV.Load(); h != nil {
					(*h)(r.subj)
				}
				jsa.observeRequest(mqttJSARequestEvent{Kind: mqttJSARequestSent, Subject: r.subj, Reply: r.reply, Body: r.msg})
				var nsize int

				msg := r.msg
				// If r.hdr is set to -1, it means that there is no need for any header.
				if r.hdr != -1 {
					bb := bytes.Buffer{}
					if r.hdr > 0 {
						// This means that the header has been set by the caller and is
						// already part of `msg`, so simply set c.pa.hdr to the given value.
						c.pa.hdr = r.hdr
						nsize = len(msg)
						msg = append(msg, _CRLF_...)
					} else {
						// We need the ClientInfo header, so add it here.
						bb.Write(hdrb)
						c.pa.hdr = bb.Len()
						bb.Write(r.msg)
						nsize = bb.Len()
						bb.WriteString(_CRLF_)
						msg = bb.Bytes()
					}
					c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
					nsize = len(msg)
					msg = append(msg, _CRLF_...)
				}

				c.pa.subject = []byte(r.subj)
				c.pa.reply = []byte(r.reply)
				c.pa.size = nsize
				c.pa.szb = []byte(strconv.Itoa(nsize))
				c.pa.mapped = nil

				if acc.hasMappings() {
					if changed := c.selectMappedSubject(); changed {
						c.traceOutOp("MAPPINGS", fmt.Appendf(nil, "%s -> %s", c.pa.mapped, c.pa.subject))
					}
				}
				// The JSA send loop, not an application publication: there is no
				// MQTT publish to retain, and the retained update is never made here.
				c.processInboundClientMsg(msg)
				c.flushClients(0)
			}
			sendq.recycle(&pmis)

		case <-closeCh:
			return
		case <-quitCh:
			return
		}
	}
}
