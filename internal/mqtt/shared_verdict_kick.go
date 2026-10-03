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
	"sync/atomic"
)

// The owner's re-trigger of a withheld shared release (F-105 7e).
//
// A release withheld behind a dependant still owing delivery from the source becomes possible the
// moment that dependant leaves Publish — a PUBACK, a withdrawal, an abandonment, a discard. The
// verdict sweep would find it eventually, but only on a record stable across two passes, which a
// connected session's record rarely is; so the session that observes the dependant's change runs
// its verdicts again.
//
// FROM THE DURABLE RECORD ONLY. The in-memory verdict map also holds PROSPECTIVE verdicts between a
// membership removal and its save, and executing one would close — and could release — an
// acquisition the stored record still holds, with nothing durable left to recover it. So the run
// reads the session's record on $MQTT_sess and executes exactly the verdicts it carries.
//
// BOUND TO A GENERATION. The session object is reused across generations, so a run carries the
// generation that kicked it: it executes nothing unless the record is still that generation's,
// drops settled nonces from memory only while the session still is, and at its end touches the
// kick state only while it still names that generation. A successor's kick replaces a stale state
// outright. Within one generation a kick that arrives while a run is active is never lost: it sets
// `again`, and the run loops before it clears `running`.

// mqttVerdictKick is one session's re-trigger state. The zero value is idle.
type mqttVerdictKick struct {
	gen     string
	running bool
	again   bool
}

// mqttTestVerdictKickPauseHookV pauses a run at a named point ("before-read", "after-execute"),
// with the run's generation. Nil in production.
var mqttTestVerdictKickPauseHookV atomic.Pointer[func(point, gen string)]

// mqttTestVerdictKickExecHookV observes every release a run dispatches, with the run's generation
// and the nonce, so a test can assert that a stale run dispatched none. Nil in production.
var mqttTestVerdictKickExecHookV atomic.Pointer[func(gen, acq string)]

func mqttTestVerdictKickPause(point, gen string) {
	if h := mqttTestVerdictKickPauseHookV.Load(); h != nil {
		(*h)(point, gen)
	}
}

// kickVerdictsForObservedLocked kicks a run when an observed exchange of the current generation
// carries a shared acquisition and no longer owes delivery from the source, and the session owes
// at least one release (the run decides from the record which).
//
// Lock held on entry.
func (sess *mqttSession) kickVerdictsForObservedLocked(gen string, xs []*mqttOutboundExchange) {
	if gen != sess.gen || len(sess.acqReleases) == 0 {
		return
	}
	for _, x := range xs {
		if x != nil && x.SharedAcq != nil && x.Phase != mqttExchangePublish {
			sess.kickVerdictsLocked(gen)
			return
		}
	}
}

// kickVerdictsLocked starts gen's run, or marks the active one to go again.
//
// Lock held on entry.
func (sess *mqttSession) kickVerdictsLocked(gen string) {
	if sess.verdictKick.gen == gen && sess.verdictKick.running {
		sess.verdictKick.again = true
		return
	}
	sess.verdictKick = mqttVerdictKick{gen: gen, running: true}
	go sess.runVerdictKick(gen)
}

// runVerdictKick executes gen's durable verdicts until no kick arrived during the last execution.
//
// Lock NOT held on entry.
func (sess *mqttSession) runVerdictKick(gen string) {
	for {
		sess.executeDurableVerdicts(gen)
		sess.mu.Lock()
		if sess.verdictKick.gen != gen {
			// A successor's kick owns the state now.
			sess.mu.Unlock()
			return
		}
		if sess.verdictKick.again {
			sess.verdictKick.again = false
			sess.mu.Unlock()
			continue
		}
		sess.verdictKick.running = false
		sess.mu.Unlock()
		return
	}
}

// executeDurableVerdicts executes the verdicts the session's STORED record carries, if that record
// is still generation gen's, and forgets the ones that settled while the session still is gen.
//
// Lock NOT held on entry.
func (sess *mqttSession) executeDurableVerdicts(gen string) {
	mqttTestVerdictKickPause("before-read", gen)
	sess.mu.Lock()
	id, idHash, domainTk, epoch := sess.id, sess.idHash, sess.domainTk, sess.sharedEpoch()
	sess.mu.Unlock()
	if epoch == _EMPTY_ || sess.jsa == nil {
		return
	}
	smsg, err := sess.jsa.loadSessionMsg(domainTk, idHash)
	if err != nil {
		return
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil || ps.ID != id || ps.Gen != gen {
		return
	}
	var settled []string
	for _, r := range ps.AcqReleases {
		if h := mqttTestVerdictKickExecHookV.Load(); h != nil {
			(*h)(gen, r.Acq)
		}
		if ok, _ := sess.jsa.executeSharedRelease(epoch, id, gen, r); ok {
			settled = append(settled, r.Acq)
		}
	}
	mqttTestVerdictKickPause("after-execute", gen)
	if len(settled) == 0 {
		return
	}
	sess.mu.Lock()
	if sess.gen == gen {
		for _, a := range settled {
			delete(sess.acqReleases, a)
		}
		if len(sess.acqReleases) == 0 {
			sess.acqReleases = nil
		}
	}
	sess.mu.Unlock()
}
