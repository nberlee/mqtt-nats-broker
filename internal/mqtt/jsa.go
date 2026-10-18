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
}

// testDropped reports whether an armed seam swallows a request.
func (jsa *mqttJSA) testDropped(subject string, body []byte) bool {
	if h := jsa.testDropRequest.Load(); h != nil {
		return (*h)(subject, body)
	}
	return false
}

type mqttJSPubMsg struct {
	subj  string
	reply string
	hdr   int
	msg   []byte
}

func (jsa *mqttJSA) newRequest(kind, subject string, hdr int, msg []byte) (any, error) {
	return jsa.newRequestEx(kind, subject, _EMPTY_, hdr, msg)
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

func (jsa *mqttJSA) newRequestEx(kind, subject, cidHash string, hdr int, msg []byte) (any, error) {
	responses, err := jsa.newRequestExMulti(kind, subject, cidHash, []int{hdr}, [][]byte{msg})
	if err != nil {
		return nil, err
	}
	if len(responses) != 1 {
		return nil, fmt.Errorf("unreachable: invalid number of responses (%d)", len(responses))
	}
	return responses[0].value, nil
}

// newRequestExMulti sends multiple messages on the same subject and waits for
// all responses. It returns the same number of responses in the same order as
// msgs parameter. In case of a timeout it returns an error as well as all
// responses received as a sparsely populated array, matching msgs, with nils
// for the values that have not yet been received.
//
// Note that each response may represent an error and should be inspected as
// such by the caller.
func (jsa *mqttJSA) newRequestExMulti(kind, subject, cidHash string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	subjects := make([]string, len(msgs))
	for i := range subjects {
		subjects[i] = subject
	}
	return jsa.newRequestExMultiSubjectsCID(kind, cidHash, subjects, hdrs, msgs)
}

// newRequestExMultiSubjects is newRequestExMulti with a PER-REQUEST subject, which a
// batch of consumer-info lookups needs: each names its own consumer in the subject rather
// than in a body. Same pipelining, same all-or-timeout semantics; a nil response entry
// means that one request went unanswered.
func (jsa *mqttJSA) newRequestExMultiSubjects(kind string, subjects []string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	return jsa.newRequestExMultiSubjectsCID(kind, _EMPTY_, subjects, hdrs, msgs)
}

func (jsa *mqttJSA) newRequestExMultiSubjectsCID(kind, cidHash string, subjects []string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	if len(hdrs) != len(msgs) || len(subjects) != len(msgs) {
		return nil, fmt.Errorf("unreachable: invalid number of messages (%d), subjects (%d) or header offsets (%d)",
			len(msgs), len(subjects), len(hdrs))
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

		sb.WriteString(kind)
		sb.WriteByte(btsep)
		if cidHash != _EMPTY_ {
			sb.WriteString(cidHash)
			sb.WriteByte(btsep)
		}
		sb.WriteString(uid)
		reply := sb.String()

		// Add responseCh to the reply channel map. It will be cleaned out on
		// timeout (see below), or in processJSAPIReplies upon receiving the
		// response.
		jsa.replies.Store(reply, responseCh)

		if jsa.testDropped(subjects[i], msg) {
			r2i[reply] = i
			continue
		}
		jsa.sendq.push(&mqttJSPubMsg{
			subj:  jsa.prefixDomain(subjects[i]),
			reply: reply,
			hdr:   hdr,
			msg:   msg,
		})
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

			var first string
			if len(subjects) > 0 {
				first = subjects[0]
			}
			if len(msgs) == 1 {
				return responses, jsa.timeoutError(fmt.Errorf("timeout after %v: request type %q on %q (reply=%q)", now.Sub(start), kind, first, reply))
			} else {
				return responses, jsa.timeoutError(fmt.Errorf("timeout after %v: request type %q on %q: got %d out of %d", now.Sub(start), kind, first, c, len(msgs)))
			}
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
func (jsa *mqttJSA) newAtomicBatchRequest(kind, cidHash string, msgs []mqttBatchMsg) (any, error) {
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
	if cidHash != _EMPTY_ {
		sb.WriteString(cidHash)
		sb.WriteByte(btsep)
	}
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
		jsa.sendMsg(subj, nil)
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
	return slr.StreamInfo.State.Subjects, slr.StreamInfo.State.LastSeq, nil
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
	return mqttSubjectPage{Subjects: slr.StreamInfo.State.Subjects, Total: slr.Total, Limit: slr.Limit,
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

// consumerList returns ConsumerInfo for every consumer of the stream, walking
// the paged CONSUMER.LIST API to completion.
func (jsa *mqttJSA) consumerList(streamName string) ([]*ConsumerInfo, error) {
	var all []*ConsumerInfo
	for {
		req, err := json.Marshal(&JSApiConsumersRequest{ApiPagedRequest: ApiPagedRequest{Offset: len(all)}})
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
		all = append(all, clr.Consumers...)
		if len(all) >= clr.Total || len(clr.Consumers) == 0 {
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
	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), _EMPTY_, headerBytes, marshaled)
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

	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), _EMPTY_, headerBytes, marshaled)
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
	if h := mqttTestStoreMsgHookV.Load(); h != nil {
		if err := (*h)(subject, headers, msg); err != nil {
			return nil, err
		}
	}
	smri, err := jsa.newRequest(mqttJSAMsgStore, subject, headers, msg)
	if err != nil {
		return jsa.storeMsgResult(subject, nil, err)
	}
	smr := smri.(*JSPubAckResponse)
	return jsa.storeMsgResult(subject, smr, smr.ToError())
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

	// Passing cidHash will add it to the JS reply subject, so that we can use
	// it in the JS API reply handler.
	smri, err := jsa.newRequestEx(mqttJSASessPersist, subject, cidHash, hdr, msg)
	if err != nil {
		return nil, err
	}
	smr := smri.(*JSPubAckResponse)
	return smr, smr.ToError()
}

// storeAcceptBatch commits an accepted CONNECT's permanent acceptance fact and its
// session record in one atomic batch on $MQTT_sess, and returns the COMMIT's PubAck
// — whose sequence is the SESSION RECORD's, because the reply rides the commit
// entry and the session record is the message that carries it.
//
// The fact is staged first and the record commits. Both halves of that ordering are
// load-bearing: the ack then names the record (the sequence the session adopts), and
// the fact sorts strictly below the record of the connection that wrote it, so a
// connection can never suppress its own delayed Will.
//
// Both session-store test seams are honored here rather than in storeSessionMsg,
// because an accepted CONNECT no longer passes through it: mqttTestSessStoreSuppressV
// is the "issued but did not land" half of ambiguity (nothing reaches the stream) and
// mqttTestSessStoreResultHookV is the other half, and it must mean the WHOLE batch's
// commit result — including a re-attempt's — or a test could suppress one message of
// an indivisible pair.
func (jsa *mqttJSA) storeAcceptBatch(domainTk, cidHash, batchID string, fact, record mqttBatchMsg) (*JSPubAckResponse, error) {
	if mqttTestSessStoreSuppressV.Load() {
		return nil, errors.New("mqtt test: session record store suppressed")
	}
	// The observation seam, armed only by the sentinel that proves this pair is
	// indivisible over a real replicated stream. Inert and free when unarmed.
	if oerr := jsa.observeAcceptStore(mqttAcceptStoreIssuing, batchID, fact, record, 0); oerr != nil {
		return nil, oerr
	}
	bi, err := jsa.newAtomicBatchRequest(mqttJSASessPersist, cidHash, []mqttBatchMsg{fact, record})
	if err != nil {
		return nil, err
	}
	smr := bi.(*JSPubAckResponse)
	if aerr := smr.ToError(); aerr != nil {
		return smr, aerr
	}
	// Validated HERE, once, so the initial send and every ambiguity re-attempt are
	// held to the same proof. A success that is not bound to the batch this call sent
	// is not evidence about it: the caller adopts resp.Sequence as the SESSION
	// RECORD's, which is only true of an acknowledgement naming this batch and both
	// of its messages.
	if smr.PubAck == nil {
		return smr, fmt.Errorf("the acceptance batch was acknowledged with no publish acknowledgement")
	}
	if smr.BatchId != batchID || smr.BatchSize != mqttAcceptBatchSize {
		return smr, fmt.Errorf("the acceptance batch was acknowledged as %d message(s) under batch %q, but this broker sent %d under %q",
			smr.BatchSize, smr.BatchId, mqttAcceptBatchSize, batchID)
	}
	if smr.Stream != mqttSessStreamName {
		return smr, fmt.Errorf("the acceptance batch was acknowledged on stream %q (want %q)", smr.Stream, mqttSessStreamName)
	}
	// Fired only once the acknowledgement has been validated as THIS batch's, so the
	// sequence handed to an observer is the one the session is about to adopt and not
	// merely the one that came back.
	if oerr := jsa.observeAcceptStore(mqttAcceptStoreAcked, batchID, fact, record, smr.Sequence); oerr != nil {
		return smr, oerr
	}
	return smr, nil
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
//     newRequestExMultiSubjectsCID and newAtomicBatchRequest already do --
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
		ch <- &mqttJSAResponse{reply: subject, value: value}
	}
	switch token {
	case mqttJSAStreamCreate:
		var resp = &JSApiStreamCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamUpdate:
		var resp = &JSApiStreamUpdateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamLookup:
		var resp = &JSApiStreamInfoResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamPurge:
		var resp = &JSApiStreamPurgeResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerCreate:
		var resp = &JSApiConsumerCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerDel:
		var resp = &JSApiConsumerDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgStore, mqttJSASessPersist:
		var resp = &JSPubAckResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgLoad:
		var resp = &JSApiMsgGetResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerLookup:
		var resp = &JSApiConsumerInfoResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerList:
		var resp = &JSApiConsumerListResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgDelete:
		var resp = &JSApiMsgDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAAckConfirm:
		// A double-ack confirmation carries no body: its ARRIVAL is the answer.
		// Anything else would be a protocol change, so nothing is parsed here —
		// there is no error form to misread as a success.
		out(nil)
	default:
		pc.Warnf("Unknown reply code %q", token)
	}
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
	http.Header{ClientInfoHdr: []string{string(b)}}.Write(&hdrStart)
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
