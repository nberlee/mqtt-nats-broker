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
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

// A shared exchange depends on its exact acquisition (F-105 cutover, unit 7a; consult 82).
//
// A shared-subscription GROUP consumer is not a resource any generation owns: several
// generations hold it at once, through ACQUISITIONS listed in each generation's ledger on
// $MQTT_shared (shared_lifecycle.go). What keeps the consumer — and with it the interest-retained
// source message a member still owes its client — alive is that this generation's acquisition
// stays listed. So the obligation's dependency is that acquisition, by its exact identity
// {Group, Inc, Acq}, recorded ON THE EXCHANGE by the admission batch and never re-derived later:
// a register can be destroyed without ending its exchange, and a durable name identifies the
// incarnation, not which of this generation's acquisitions of it the delivery arrived under.
//
// 7a WRITES the dependency and admits only against a readiness observation taken after the
// authority read, so the admission batch's authority CAS fences it. The release side — marking
// the acquisition closed to new dependencies, advancing the authority, enumerating the exchanges
// that name it, and only then removing it from the ledger — is 7e's, in shared_release_fence.go
// (docs/ARCHITECTURE.md, "A shared exchange depends on its exact acquisition").

// mqttSharedAcqRefMembers is the closed member set of a persisted acquisition reference.
var mqttSharedAcqRefMembers = map[string]struct{}{"group": {}, "inc": {}, "acq": {}}

// mqttSharedDurableOf is the group durable an acquisition holds.
func mqttSharedDurableOf(ref mqttSharedAcqRef) string {
	return mqttSharedConsumerDurablePrefix + ref.Group + "_" + mqttSharedIncToken(ref.Inc)
}

// mqttIsSharedDurable reports whether a source durable is a shared-subscription group consumer.
func mqttIsSharedDurable(dur string) bool {
	return strings.HasPrefix(dur, mqttSharedConsumerDurablePrefix)
}

// mqttValidateSharedAcqRef refuses an acquisition reference this broker never writes.
func mqttValidateSharedAcqRef(ref *mqttSharedAcqRef) error {
	switch {
	case ref == nil:
		return fmt.Errorf("the shared acquisition reference is absent")
	case !mqttValidSharedGroupKey(ref.Group):
		return fmt.Errorf("the shared acquisition reference names a malformed group %q", ref.Group)
	case ref.Inc == 0:
		return fmt.Errorf("the shared acquisition reference names incarnation 0")
	case !mqttValidInflightNonce(ref.Acq):
		return fmt.Errorf("the shared acquisition reference carries a malformed acquisition %q", ref.Acq)
	}
	return nil
}

// errMQTTSharedAcqPair refuses a register and exchange that do not state one shared dependency.
var errMQTTSharedAcqPair = errors.New("the register and its exchange disagree about the shared acquisition")

// mqttCheckSharedAcqPair is THE pair rule, checked wherever a register and its exchange are both in
// hand: a register on a shared durable requires an exchange naming exactly the acquisition of that
// durable's incarnation, and any other register requires an exchange naming none. A contradiction is
// an unsupported persisted shape and is refused, never inferred or repaired.
func mqttCheckSharedAcqPair(reg *mqttInflightRecord, x *mqttOutboundExchange) error {
	if reg == nil || x == nil {
		return fmt.Errorf("%w: the pair is incomplete", errMQTTSharedAcqPair)
	}
	if !mqttIsSharedDurable(reg.Dur) {
		if x.SharedAcq != nil {
			return fmt.Errorf("%w: a register on %q has an exchange naming acquisition %q", errMQTTSharedAcqPair,
				reg.Dur, x.SharedAcq.Acq)
		}
		return nil
	}
	if x.SharedAcq == nil {
		return fmt.Errorf("%w: the shared register on %q has an exchange naming no acquisition", errMQTTSharedAcqPair, reg.Dur)
	}
	if err := mqttValidateSharedAcqRef(x.SharedAcq); err != nil {
		return fmt.Errorf("%w: %v", errMQTTSharedAcqPair, err)
	}
	if d := mqttSharedDurableOf(*x.SharedAcq); d != reg.Dur {
		return fmt.Errorf("%w: the register is on %q but its exchange depends on %q", errMQTTSharedAcqPair, reg.Dur, d)
	}
	return nil
}

// mqttSelectSharedAcq chooses, among the acquisitions a delivery's incarnation is held by, the one
// its exchange will depend on. Deterministic and independent of enumeration order: the smallest
// acquisition nonce among those with no pending release verdict. Any held acquisition is a correct
// dependency; the choice only has to be stable so that nothing about it depends on map order.
func mqttSelectSharedAcq(cands []mqttSharedAcqRef, pending map[string]struct{}) (mqttSharedAcqRef, bool) {
	var best mqttSharedAcqRef
	found := false
	for _, c := range cands {
		if _, releasing := pending[c.Acq]; releasing {
			continue
		}
		if !found || c.Acq < best.Acq {
			best, found = c, true
		}
	}
	return best, found
}

// sharedAcqForDeliveryLocked selects the acquisition a delivery on shared durable jsDur depends on,
// from the session's own memberships — Main and level-up alike, with the group key the join derives
// for each — excluding any acquisition with a pending release verdict. It is the admission's input
// only: the ledger check inside the attempt re-validates it against the durable ledger.
//
// Lock held on entry.
func (sess *mqttSession) sharedAcqForDeliveryLocked(jsDur string) (mqttSharedAcqRef, bool) {
	tail, ok := strings.CutPrefix(jsDur, mqttSharedConsumerDurablePrefix)
	if !ok {
		return mqttSharedAcqRef{}, false
	}
	qkey, inc, ok := mqttParseSharedIncName(tail)
	if !ok {
		return mqttSharedAcqRef{}, false
	}
	keys := make([]string, 0, len(sess.sharedSubs))
	for key := range sess.sharedSubs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var cands []mqttSharedAcqRef
	for _, key := range keys {
		m := sess.sharedSubs[key]
		group, subject, pok := mqttParseSharedKey(key)
		if !pok {
			continue
		}
		if m.Main.Inc == inc && mqttSharedGroupKey(group, subject) == qkey {
			cands = append(cands, mqttSharedAcqRef{Group: qkey, Inc: inc, Acq: m.Main.Acq})
		}
		if m.Up != nil && mqttNeedSubForLevelUp(subject) && m.Up.Inc == inc &&
			mqttSharedGroupKey(group, subject[:len(subject)-2]) == qkey {
			cands = append(cands, mqttSharedAcqRef{Group: qkey, Inc: inc, Acq: m.Up.Acq})
		}
	}
	pending := make(map[string]struct{}, len(sess.acqReleases))
	for _, r := range sess.acqReleases {
		pending[r.Acq] = struct{}{}
	}
	return mqttSelectSharedAcq(cands, pending)
}

// mqttAdmitShared is a shared delivery's dependency as its admission states it: the acquisition
// selected at the delivery callback and the SHARED tenure captured there. Neither is recaptured on a
// ladder retry.
type mqttAdmitShared struct {
	Acq    mqttSharedAcqRef
	Tenure mqttSharedTenure
}

// errMQTTSharedAcqNotReady refuses an attempt whose shared dependency cannot be shown ready. A
// non-nil write outcome: the ladder re-classifies and retries, re-running the whole ordered check
// against a freshly read authority, and after its bounded retries AckWait re-offers to the group.
var errMQTTSharedAcqNotReady = errors.New("the shared acquisition is not ready for a new dependency")

// mqttSharedAcqReadyForDependency is the shared readiness, taken INSIDE an admission attempt and
// AFTER its authority read, so the batch's CAS of that authority revision fences the observation:
// a release that marks the acquisition closed and then advances the authority (7e) makes every
// admission that observed readiness before it lose its CAS.
//
// In order: the generation's ledger is valid and open, carries the captured shared tenure, and lists
// exactly this acquisition; the group record serves this incarnation and is not retiring it; the
// physical consumer exists. Any gap refuses.
func (jsa *mqttJSA) mqttSharedAcqReadyForDependency(id, epoch, gen, jsDur string, s *mqttAdmitShared) error {
	if s == nil {
		return fmt.Errorf("%w: no shared dependency was supplied", errMQTTSharedAcqNotReady)
	}
	if err := mqttValidateSharedAcqRef(&s.Acq); err != nil {
		return fmt.Errorf("%w: %v", errMQTTSharedAcqNotReady, err)
	}
	if mqttSharedDurableOf(s.Acq) != jsDur {
		return fmt.Errorf("%w: acquisition %q holds %q, not the delivery's %q", errMQTTSharedAcqNotReady,
			s.Acq.Acq, mqttSharedDurableOf(s.Acq), jsDur)
	}
	if hook := mqttTestSharedReadinessHook.Load(); hook != nil {
		(*hook)(id)
	}
	l := jsa.loadSharedLedger(id, epoch, gen)
	if l.status != mqttOutboundValid {
		return fmt.Errorf("%w: the ledger of generation %q reads %v", errMQTTSharedAcqNotReady, gen, l.status)
	}
	// OPEN FOR A NEW DEPENDENCY: the ledger is not sealed, and the acquisition is not CLOSED — the
	// release fence's first step (F-105 7e), which this check observing is what the fence's
	// authority advance makes exclusive.
	if l.rec.State != mqttSharedLedgerOpen {
		return fmt.Errorf("%w: the ledger of generation %q is sealed", errMQTTSharedAcqNotReady, gen)
	}
	if l.rec.isClosed(s.Acq.Acq) {
		return fmt.Errorf("%w: acquisition %q of generation %q is closed to new dependencies: its release is under way",
			errMQTTSharedAcqNotReady, s.Acq.Acq, gen)
	}
	if l.rec.Tenure != s.Tenure {
		return fmt.Errorf("%w: the ledger of generation %q carries tenure %+v, not the delivery's %+v",
			errMQTTSharedAcqNotReady, gen, l.rec.Tenure, s.Tenure)
	}
	listed, ok := l.rec.lists(s.Acq.Acq)
	if !ok || listed != s.Acq {
		return fmt.Errorf("%w: the ledger of generation %q does not list acquisition %+v", errMQTTSharedAcqNotReady, gen, s.Acq)
	}
	g := jsa.loadSharedGroup(epoch, s.Acq.Group)
	if g.status != mqttOutboundValid {
		return fmt.Errorf("%w: the group record of %q reads %v", errMQTTSharedAcqNotReady, s.Acq.Group, g.status)
	}
	if g.rec.Live == nil || g.rec.Live.Inc != s.Acq.Inc {
		return fmt.Errorf("%w: group %q does not serve incarnation %d", errMQTTSharedAcqNotReady, s.Acq.Group, s.Acq.Inc)
	}
	for _, r := range g.rec.Retiring {
		if r == s.Acq.Inc {
			return fmt.Errorf("%w: group %q is retiring incarnation %d", errMQTTSharedAcqNotReady, s.Acq.Group, s.Acq.Inc)
		}
	}
	if ci, lerr := jsa.lookupConsumer(mqttStreamName, jsDur); lerr != nil || ci == nil {
		return fmt.Errorf("%w: the group consumer %q is not usable: %v", errMQTTSharedAcqNotReady, jsDur, lerr)
	}
	// A schedule seam AFTER every readiness read and before the caller composes and sends the
	// admission batch: the window the release fence has to close.
	mqttTestSharedPause("admit:after-readiness:" + s.Acq.Acq)
	return nil
}

// mqttTestSharedReadinessHook observes one shared readiness check, AFTER the attempt's authority read
// and BEFORE its ledger read, with the client ID so a parallel test can ignore other sessions. Nil
// in production.
var mqttTestSharedReadinessHook atomic.Pointer[func(id string)]
