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
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// A COMPLETION LEAVES ITS HOLDS RELEASE-OWED (F-105 7c-lc, as amended).
//
// An admitted obligation takes an edge on its source consumer (and, at QoS 2, on the canonical
// PUBREL consumer). The client's answer that ENDS it (a PUBACK, a PUBCOMP, a refused PUBREC, the
// drain completion after one) does not release those edges in its own batch: the source
// resource is the record every next admission adds its edge to, so releasing there made each
// completion defeat the next admission's resource CAS. The release is a later, batched act of
// kind `release-holds`, and the ended obligation stands RELEASE-OWED meanwhile:
//
//   - durable and discoverable without any process memory: its exchange is at an identifier
//     ending, its holds index names a resource still listing its edge, and its delivery duty
//     stays PENDING, because a delivery duty may be written terminal only when every hold of an
//     ended exchange is absent in the state the batch leaves (checkDutyEndingIsEarned). Every
//     inventory traversal that resolves duties therefore finds it, in any generation;
//   - released by whichever comes first: the session's releaser (runReleaseUnit, while the
//     deliveries continue), the floor's bulk resolution, the recovery and discard executors, and
//     a source retirement waiting on exactly those edges. Each resolver composes through
//     opReleaseHolds, which releases what is still listed and resolves what the release earns.
//
// The other endings — withdrawn or superseded unsent, a generation discard — and a completion of
// an ABANDONED delivery (whose duty was already resolved by the abandonment, so no pending duty
// would carry the release) keep releasing in their own batch (appendHoldReleases).

// mqttHoldReleaseBatch bounds the obligations one release-holds batch considers.
const mqttHoldReleaseBatch = 64

// mqttReleaseOwedThreshold and mqttReleaseOwedDelay pace the session's releaser: it runs once
// this many completions are owed, or this long after the first, whichever comes first.
const (
	mqttReleaseOwedThreshold = 16
	mqttReleaseOwedDelay     = 200 * time.Millisecond
)

// mqttHoldReleaseOutcome is what one opReleaseHolds batch achieved.
type mqttHoldReleaseOutcome struct {
	// cleared names the obligations no hold of which is listed in the state the batch leaves:
	// nothing further is owed for them.
	cleared []string
	// resolved counts the delivery duties the batch wrote terminal.
	resolved int
}

// mqttCompletionReleasesInBatch reports whether an acknowledgement ending of x releases its
// holds in its own batch: only for an abandoned delivery, whose duty no longer stands pending to
// carry a release-owed ending. Every other completion leaves them release-owed.
func mqttCompletionReleasesInBatch(x *mqttOutboundExchange) bool {
	return x != nil && x.Abandoned != nil
}

// mqttHoldListed reports whether resource r still lists the hold edge (nonce, role). A resource
// at CleanupComplete lists nothing any more.
func mqttHoldListed(r *mqttOutboundResource, nonce string, role mqttResourceRole) bool {
	if r == nil || r.State == mqttResourceCleanupComplete {
		return false
	}
	for _, e := range r.Deps {
		if e.Nonce == nonce && e.Role == role {
			return true
		}
	}
	return false
}

// opReleaseHolds releases, in ONE authority-free batch, every hold still listed for the ended
// obligations it is given (by nonce, or as the delivery duty naming them), and writes terminal
// each given duty the release earns: the exchange owes no further delivery and, at an identifier
// ending, no hold of it is listed once the batch lands.
//
// One member per DISTINCT resource, each a CAS removing exact edges; a duty member per resolved
// duty. An obligation not yet ended, unreadable, or whose resource is not LIVE while listing
// its edge, is left for a later pass rather than failing the rest. Nothing to write is not an
// error when only nonces were given; given duties none of which resolves, it is (the caller
// counted on progress). known, when non-nil, supplies exchanges the caller already read: an
// ending is absorbing, so a read that showed one still shows it.
//
// THE EXCHANGES ARE READ ONCE, THE RESOURCES PER ATTEMPT. The source resource is the record
// every admission adds its edge to, so under a backlog a release composed over many exchange
// reads would lose its resource CAS to an admission nearly every time and fall behind for the
// burst's length. Only the resource-dependent half is recomposed on a stale expectation, from a
// fresh read, under a fresh transition id (a positively refused attempt wrote nothing).
func (jsa *mqttJSA) opReleaseHolds(id, epoch, gen string, nonces []string, duties []mqttSweptDuty,
	txn string, known map[string]*mqttOutboundExchange) (mqttHoldReleaseOutcome, error) {

	auth, _, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttHoldReleaseOutcome{}, err
	}
	if auth == nil {
		return mqttHoldReleaseOutcome{}, fmt.Errorf("client %q has no outbound authority, so no hold of its may be released", id)
	}
	exchanges := map[string]*mqttOutboundExchange{}
	var order []string
	consider := func(nonce string) {
		if _, seen := exchanges[nonce]; seen || len(exchanges) >= mqttHoldReleaseBatch {
			return
		}
		x := known[nonce]
		if x == nil || x.Nonce != nonce {
			var xerr error
			if x, _, xerr = jsa.loadOutboundExchange(id, epoch, gen, nonce); xerr != nil || x == nil {
				return
			}
		}
		exchanges[nonce] = x
		order = append(order, nonce)
	}
	for _, n := range nonces {
		consider(n)
	}
	for _, d := range duties {
		if d.Entry != nil {
			consider(d.Entry.Ref)
		}
	}
	var out mqttHoldReleaseOutcome
	for attempt := 0; attempt < mqttHoldReleaseAttempts; attempt++ {
		if attempt > 0 {
			txn = mqttNewInflightNonce()
		}
		out, err = jsa.releaseHoldsOnce(id, epoch, gen, auth, exchanges, order, duties, txn)
		if err == nil || !mqttIsStaleExpectation(err) {
			break
		}
	}
	return out, err
}

// mqttHoldReleaseAttempts bounds opReleaseHolds' recompositions on a stale expectation.
const mqttHoldReleaseAttempts = 8

// releaseHoldsOnce is one composition and commit of opReleaseHolds over exchanges already read.
func (jsa *mqttJSA) releaseHoldsOnce(id, epoch, gen string, auth *mqttOutboundAuthority,
	exchanges map[string]*mqttOutboundExchange, order []string, duties []mqttSweptDuty,
	txn string) (mqttHoldReleaseOutcome, error) {

	var out mqttHoldReleaseOutcome
	type held struct {
		r       *mqttOutboundResource
		seq     uint64
		changed bool
	}
	resources := map[string]*held{}
	for _, nonce := range order {
		x := exchanges[nonce]
		if !mqttExchangeReleasesHolds(x.Phase) {
			continue
		}
		for _, h := range x.Holds {
			hr := resources[h.Inc]
			if hr == nil {
				r, seq, lerr := jsa.loadOutboundResource(id, epoch, gen, h.Inc)
				if lerr != nil || r == nil {
					continue
				}
				hr = &held{r: r, seq: seq}
				resources[h.Inc] = hr
			}
			if hr.r.State != mqttResourceLive || !mqttHoldListed(hr.r, nonce, h.Role) {
				continue
			}
			next, rerr := mqttResourceReleaseDependency(hr.r, mqttResourceEdge{Nonce: nonce, Role: h.Role})
			if rerr != nil {
				continue
			}
			hr.r, hr.changed = next, true
		}
	}
	// AN OBLIGATION IS CLEARED when no hold of it is listed in the state this batch leaves.
	isCleared := func(x *mqttOutboundExchange) bool {
		for _, h := range x.Holds {
			hr := resources[h.Inc]
			if hr == nil || mqttHoldListed(hr.r, x.Nonce, h.Role) {
				return false
			}
		}
		return true
	}

	idHash := getHash(id)
	var members []mqttTransitionMember
	incs := make([]string, 0, len(resources))
	for inc, hr := range resources {
		if hr.changed {
			incs = append(incs, inc)
		}
	}
	sort.Strings(incs)
	for _, inc := range incs {
		body, merr := json.Marshal(resources[inc].r)
		if merr != nil {
			return out, merr
		}
		members = append(members, mqttTransitionMember{
			subj: mqttResourceSubject(idHash, gen, inc), expect: resources[inc].seq, body: body})
	}
	for _, d := range duties {
		if d.Entry == nil || d.Entry.Kind != mqttDutyExchange {
			continue
		}
		x := exchanges[d.Entry.Ref]
		if x == nil || !mqttExchangeDeliveryDutyEnded(x.Phase) ||
			(mqttExchangeReleasesHolds(x.Phase) && !isCleared(x)) {
			continue
		}
		cur, curSeq, lerr := jsa.loadDutyEntryAt(id, epoch, gen, d.Ord)
		if lerr != nil || cur == nil || mqttDutyResolved(cur.State) || cur.Ref != x.Nonce {
			continue
		}
		body, berr := mqttResolvedDutyBody(cur, mqttDutyOutcomeResolved)
		if berr != nil {
			return out, berr
		}
		members = append(members, mqttTransitionMember{
			subj: mqttDutySubject(idHash, gen, d.Ord), expect: curSeq, body: body})
		out.resolved++
	}
	for nonce, x := range exchanges {
		if mqttExchangeReleasesHolds(x.Phase) && isCleared(x) {
			out.cleared = append(out.cleared, nonce)
		}
	}
	if len(members) == 0 {
		if len(duties) > 0 {
			return out, fmt.Errorf("none of the %d duties of %q/%q given is resolvable now", len(duties), id, gen)
		}
		return out, nil
	}
	if _, cerr := jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn, Kind: mqttTransitionReleaseHolds,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Extra: members,
	}); cerr != nil {
		return mqttHoldReleaseOutcome{}, cerr
	}
	return out, nil
}

// noteReleaseOwed records an acknowledgement ending of (gen, nonce) for the session's releaser,
// and starts the dispatcher once the release is due. Memory only: the durable fact is the
// pending duty, and every traversal resolving duties finds it without this.
//
// Session lock NOT held on entry.
func (sess *mqttSession) noteReleaseOwed(gen, nonce string) {
	sess.mu.Lock()
	if sess.gen != gen || sess.custody == nil {
		sess.mu.Unlock()
		return
	}
	if sess.releaseOwed == nil {
		sess.releaseOwed = make(map[string]struct{})
	}
	sess.releaseOwed[nonce] = struct{}{}
	now := time.Now()
	start := false
	switch {
	case len(sess.releaseOwed) >= mqttReleaseOwedThreshold:
		start = !mqttCustodyDue(sess.releaseDueAt, now) || sess.releaseDueAt.IsZero()
		sess.releaseDueAt = now
	case sess.releaseDueAt.IsZero():
		// THE FIRST OWED COMPLETION ARMS THE DEADLINE, and the dispatcher must learn of it: an
		// idle dispatcher finds nothing due now and exits arming its tick at the deadline, which
		// is the only thing that runs a below-threshold release while no other custody work does.
		sess.releaseDueAt = now.Add(mqttReleaseOwedDelay)
		start = true
	}
	sess.mu.Unlock()
	if start {
		go sess.startCustodyDispatcher()
	}
}

// releaseDueLocked reports whether the releaser is due.
//
// Lock held on entry.
func (sess *mqttSession) releaseDueLocked(now time.Time) bool {
	return len(sess.releaseOwed) > 0 && mqttCustodyDue(sess.releaseDueAt, now)
}

// runReleaseUnit is ONE release-holds batch over the owed completions: not slack-gated and not
// yielding to foreground deliveries, because it is what keeps the source resource's dependents
// list from growing with the backlog. Its resource CAS competes with admissions' edge adds, once
// per batch rather than once per delivery.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runReleaseUnit(gen, epoch string) {
	if s := sess.custodyDriverSeams(); s.releaseHeld != nil && s.releaseHeld(gen) {
		sess.mu.Lock()
		if sess.gen == gen && len(sess.releaseOwed) > 0 {
			sess.releaseDueAt = time.Now().Add(100 * time.Millisecond)
		}
		sess.mu.Unlock()
		return
	}
	sess.mu.Lock()
	if sess.gen != gen {
		sess.mu.Unlock()
		return
	}
	nonces := make([]string, 0, min(len(sess.releaseOwed), mqttHoldReleaseBatch))
	for n := range sess.releaseOwed {
		if len(nonces) == mqttHoldReleaseBatch {
			break
		}
		nonces = append(nonces, n)
	}
	sess.mu.Unlock()
	if len(nonces) == 0 {
		return
	}
	out, err := sess.jsa.opReleaseHolds(sess.id, epoch, gen, nonces, nil, mqttNewInflightNonce(), nil)
	sess.mu.Lock()
	if sess.gen != gen {
		sess.mu.Unlock()
		return
	}
	now := time.Now()
	if err != nil {
		base, capd := sess.custodyDelaysLocked()
		sess.releaseDelay = mqttCustodyNextDelay(sess.releaseDelay, base, capd)
		sess.releaseDueAt = now.Add(mqttCustodyJitter(sess.releaseDelay))
		sess.mu.Unlock()
		return
	}
	// EVERY CONSIDERED NONCE LEAVES THE OWED SET once its batch landed: a cleared one is owed
	// nothing more, and one the batch could not release (a resource not LIVE, a read that failed)
	// is carried by its pending duty to the floor, the executors and the retirement, which all
	// compose through opReleaseHolds. This set is a pacing hint, never the record.
	for _, n := range nonces {
		delete(sess.releaseOwed, n)
	}
	sess.releaseDelay = 0
	switch {
	case len(sess.releaseOwed) >= mqttReleaseOwedThreshold:
		sess.releaseDueAt = now
	case len(sess.releaseOwed) > 0:
		sess.releaseDueAt = now.Add(mqttReleaseOwedDelay)
	default:
		sess.releaseDueAt = time.Time{}
	}
	sess.mu.Unlock()
	if len(out.cleared) > 0 {
		sess.wakeSourceRetirements()
	}
}
