// Copyright 2026 The mqtt-nats-bridge Authors
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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// A serving generation settles its displaced tenures (F-105 7f).
//
// A generation keeps SERVING across every resume, each accepted CONNECT under a fresh tenure, and
// only a session end discards it (7d). Every route that ends a subscription records its decision
// under its own entitlement and hands it to the session-resident retirement owner — custody that
// lives in memory. A crash, a takeover, or an entitlement that moved before the decision landed
// therefore left a source consumer LIVE and undecided, named by nothing, for the generation's
// whole life; and a decided one whose retirement the owner had not finished stood decided but
// never retired. Neither is visited by the recovery pass, whose licence is a closure.
//
// What licenses a decision here is the successor's ACCEPT. Once tenure T' of G is accepted, the
// displaced tenure T can write neither a session record (the accept advanced the record's
// sequence, and every save CASes it) nor a claim (every provisioning CASes the authority at
// exactly T). So "provisioned under a tenure that no longer serves AND named by nothing the
// serving tenure holds" is a fact no later write by T can falsify, and T' itself only ever names
// a consumer it already holds or one it claims under T'. The withdrawn 7c-lc rule — "no record
// names it" — lacked exactly this fence: under the SAME tenure a later save can still name the
// consumer after the read.
//
// The boundary checks the half the resource record carries: the deciding tenure is not the one
// that provisioned it (ProvTenure). The naming half is checked here, under a record transaction,
// because the session record is on another stream and no batch can carry it.

// mqttTenureSettleBackoffMax caps the retry cadence of an incomplete visit.
const mqttTenureSettleBackoffMax = 30 * time.Second

// The classes a settlement visit assigns, one per resource, reported through the test seam.
const (
	mqttSettleKindSkipped        = "kind-skipped"
	mqttSettleDecided            = "decided"
	mqttSettleSupersededUndecide = "superseded-undecided"
	mqttSettleOwnTenure          = "own-tenure"
	mqttSettleNamed              = "named"
	mqttSettleFinished           = "finished"
)

// mqttTestTenureSettleStartV gates a settlement job before its first visit; returning false
// suppresses the job. nil in production.
var mqttTestTenureSettleStartV atomic.Pointer[func(gen, tenure string) bool]

// startTenureSettlement runs the accepted resume's settlement of its generation, once, in the
// background: the CONNECT has nothing to wait for — nothing it serves depends on a consumer no
// subscription names.
//
// No lock held on entry.
func (sess *mqttSession) startTenureSettlement(c *client) {
	ent, ok := c.outboundEntitlement()
	if !ok || sess.jsa == nil || sess.asm == nil {
		return
	}
	if h := mqttTestTenureSettleStartV.Load(); h != nil && !(*h)(ent.Gen, ent.Tenure) {
		return
	}
	go sess.runTenureSettlement(ent)
}

// runTenureSettlement classifies every resource of the generation ONCE, retrying what it could
// not read, until the visit is complete or this tenure stops being the session's: a residency loss
// or a moved entitlement hands the generation to the next accepted tenure's own job, which is
// what makes it safe that nothing durable records this one.
func (sess *mqttSession) runTenureSettlement(ent mqttOutboundEntitlement) {
	classified := make(map[string]struct{})
	backoff := mqttActionReconcileDelay
	for {
		if !sess.servesGeneration(ent.Gen) || !sess.tenureStillServes(ent) {
			return
		}
		if sess.settleTenureVisit(ent, classified) {
			return
		}
		// SHUTDOWN ENDS THE WAIT: a stopping server's restart is a new accept, which runs its own job.
		var quit chan struct{}
		if srv := sess.serverFor(nil); srv != nil {
			quit = srv.quitCh
		}
		select {
		case <-time.After(backoff):
		case <-quit:
			return
		}
		if backoff *= 2; backoff > mqttTenureSettleBackoffMax {
			backoff = mqttTenureSettleBackoffMax
		}
	}
}

// tenureStillServes reports whether ent is still the session's entitlement: the standing authority
// serves at exactly it, or a handoff is PREPARING to displace exactly it. A preparation is not a
// move — a cancelled one restores this tenure, and no successor accept then exists to settle what
// this job abandoned — so only a finalized displacement ends the job; the retirement owner
// withholds its disposals meanwhile. A read that fails is not an answer either, so it reports
// true and the visit's own writes decide.
func (sess *mqttSession) tenureStillServes(ent mqttOutboundEntitlement) bool {
	auth, _, err := sess.jsa.requireAuthority(sess.id, sess.asm.sessStreamEpoch)
	if err != nil {
		return true
	}
	return auth != nil && (auth.State == mqttAuthorityServing || auth.State == mqttAuthorityPreparing) &&
		auth.Gen == ent.Gen && auth.Tenure == ent.Tenure
}

// settleTenureVisit walks the generation's resources and classifies each one not yet classified
// by this job. It reports whether every resource the walk found has been classified.
func (sess *mqttSession) settleTenureVisit(ent mqttOutboundEntitlement, classified map[string]struct{}) bool {
	jsa := sess.jsa
	id, epoch := sess.id, sess.asm.sessStreamEpoch
	subjects, err := jsa.streamSubjectsAllPages(mqttInflightStreamName, mqttResourceWalkFilter(getHash(id), ent.Gen))
	if err != nil {
		if jsa.c != nil {
			jsa.c.Debugf("MQTT: settling generation %q of %q: listing its resources: %v", ent.Gen, id, err)
		}
		return false
	}
	complete := true
	for _, subj := range subjects {
		inc := subj[strings.LastIndexByte(subj, '.')+1:]
		if _, done := classified[inc]; done {
			continue
		}
		r, _, lerr := jsa.loadOutboundResource(id, epoch, ent.Gen, inc)
		if lerr != nil || r == nil {
			// AN UNREADABLE RESOURCE IS NOT SETTLED, and it is not skipped either: the visit is
			// incomplete and the job comes back for it.
			complete = false
			continue
		}
		class, hand, cerr := sess.classifyForSettlement(ent, r)
		if cerr != nil {
			complete = false
			continue
		}
		classified[inc] = struct{}{}
		if h := sess.testSettleClassified.Load(); h != nil {
			(*h)(r.Name, class)
		}
		if hand != _EMPTY_ {
			sess.handSourceRetirement(&mqttSourceRetirement{gen: ent.Gen, name: r.Name, inc: r.Inc,
				reason: hand, ent: ent})
		}
	}
	return complete
}

// classifyForSettlement decides what one resource of the generation owes this tenure, and the
// disposal reason to hand the owner with, if any.
//
// It returns an error only when the naming half could not be decided (the record transaction was
// not available), so the job retries rather than guessing.
func (sess *mqttSession) classifyForSettlement(ent mqttOutboundEntitlement, r *mqttOutboundResource) (string, string, error) {
	// THE PUBREL CONSUMER IS NEVER SETTLED: it is one stable name per generation, never in Cons or
	// Drains, reused by the successor's ensure. Payload and affine kinds have their own retirement.
	if r.Kind != mqttResourceSourceConsumer {
		return mqttSettleKindSkipped, _EMPTY_, nil
	}
	if r.Disposal != nil {
		switch r.State {
		case mqttResourceLive, mqttResourceDeleteAuthorized:
			// DECIDED, its retirement unfinished: the decision is the whole licence, whoever took
			// it. The owner's step finds it standing and continues from the state that stands.
			return mqttSettleDecided, r.Disposal.Reason, nil
		}
		// CLEANUP_COMPLETE: the consumer is gone. Its duties' resolution is bookkeeping the
		// generation's discard performs (7d's census); resolving it here would cost a ledger walk
		// per resource on every resume.
		return mqttSettleFinished, _EMPTY_, nil
	}
	if r.State != mqttResourceLive {
		return mqttSettleFinished, _EMPTY_, nil
	}
	if r.ProvTenure == ent.Tenure {
		// THIS TENURE'S OWN CLAIM: its route still holds the decision, or will.
		return mqttSettleOwnTenure, _EMPTY_, nil
	}
	named, ok := sess.namesSourceConsumer(r.Name)
	if !ok {
		return _EMPTY_, _EMPTY_, fmt.Errorf("the record transaction of %q is held", sess.id)
	}
	if named {
		return mqttSettleNamed, _EMPTY_, nil
	}
	return mqttSettleSupersededUndecide, mqttDisposalTenureSuperseded, nil
}

// namesSourceConsumer reports whether anything the session holds names the durable: a
// subscription's consumer, a drain's, or a retained adoption's. Read under the record
// transaction, so no route is mid-transaction — a rollback restores only names its own
// transaction removed — and the answer is the state between transactions. Reports ok=false when
// the transaction could not be taken without blocking; the caller retries.
//
// No lock held on entry.
func (sess *mqttSession) namesSourceConsumer(durable string) (named, ok bool) {
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		return false, false
	}
	defer func() { _ = sess.endRecordTx(tok) }()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, cc := range sess.cons {
		if cc != nil && cc.Durable == durable {
			return true, true
		}
	}
	for _, d := range sess.drains {
		if d != nil && d.cc != nil && d.cc.Durable == durable {
			return true, true
		}
	}
	for _, d := range sess.adoptedDrains {
		if d != nil && d.cc != nil && d.cc.Durable == durable {
			return true, true
		}
	}
	return false, true
}

// streamSubjectsAllPages enumerates EVERY subject in streamName matching filter, following the
// stream-info pages the server truncates at JSMaxSubjectDetails.
//
// THE WALK ENDS WHEN THE OFFSET REACHES THE TOTAL THE ANSWERING PAGE REPORTED, advancing by the
// entries each page returned, duplicates across pages ignored. It never compares the collected
// count with a total. Why this cannot skip a subject that existed when the walk began: the server
// rebuilds and sorts the set on every request, and the one caller's subject space — a
// generation's resource records — is never deleted from (F-112), so between two requests the
// sorted list only GAINS entries. An insertion before the offset shifts later entries to higher
// indices, which re-reads one (a duplicate) and never skips one. IF RESOURCE RECORDS ARE EVER
// DELETED, this argument no longer holds and the walk must be revisited.
func (jsa *mqttJSA) streamSubjectsAllPages(streamName, filter string) ([]string, error) {
	seen := make(map[string]struct{})
	offset := 0
	for {
		subs, total, err := jsa.streamSubjectsPage(streamName, filter, offset)
		if err != nil {
			return nil, err
		}
		for s := range subs {
			seen[s] = struct{}{}
		}
		offset += len(subs)
		if len(subs) == 0 || offset >= total {
			break
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out, nil
}

// streamSubjectsPage is one page of a stream-info subject listing and the total the server
// reported beside it.
func (jsa *mqttJSA) streamSubjectsPage(streamName, filter string, offset int) (map[string]uint64, int, error) {
	if obs := jsa.resourceSubjectsPageObs.Load(); obs != nil {
		return (*obs)(filter, offset)
	}
	req, err := json.Marshal(&JSApiStreamInfoRequest{ApiPagedRequest: ApiPagedRequest{Offset: offset},
		SubjectsFilter: filter})
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
	return slr.State.Subjects, slr.Total, nil
}
