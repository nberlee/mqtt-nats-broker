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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Generation retirement (issue #133): a noncurrent generation whose work is finished is
// reclaimed, every record it named purged, and its Outstanding entry removed.
//
// Three authority controls, each moving ONE Outstanding entry ONE step:
//
//   - open -> fenced, licensed by the generation's finished work: its completion certificate
//     stands, its recovery root is retired, and its teardown release landed. From here the
//     write boundary refuses everything scoped to the generation (generation_boundary.go), so
//     nothing can add a record the purge would miss.
//   - fenced -> bounded, recording the stream's last sequence read after the fence committed:
//     every record of the generation lies at or below it.
//   - bounded -> removed, after the purge of the generation's filters below bound + 1.
//
// Each control's ambiguity resolves from the monotone phase it sets: a driver that cannot tell
// whether its step landed re-reads the authority and continues from the phase it finds.

const mqttTransitionRetireGeneration mqttTransitionKind = "retire-generation"

// mqttGenerationPurgeFilters are every subject filter of generation gen of the client of idHash.
// Generation identities are never reissued, so the filters need no class list: the exact
// records keyed by the generation (closure, ledger, completion certificate, readiness, teardown
// binding), everything keyed below it (duties, resources, exchanges, actions, close
// certificates, material, canonical PUBRELs, generation-scoped outcomes), and its shared ledger.
func mqttGenerationPurgeFilters(idHash, gen string) []string {
	ob := mqttOutboundRecordSubjectPrefix() + "*." + idHash + "." + gen
	return []string{ob, ob + ".>", mqttSharedLedgerSubject(idHash, gen)}
}

// mqttComposeRetireGenerationStep composes the next control for generation ordinal ord over prev,
// or reports that its entry is gone (nil, nil): the generation is retired.
func mqttComposeRetireGenerationStep(prev *mqttOutboundAuthority, prevSeq uint64, txn string, ord, bound uint64) (*mqttAuthorityTransition, error) {
	if prev == nil || prev.Life == nil {
		return nil, fmt.Errorf("retiring a generation requires the authority that carries it")
	}
	i := mqttOutstandingIndex(prev.Life, ord)
	if i < 0 {
		return nil, nil
	}
	nl := *prev.Life
	nl.Outstanding = append([]mqttOutstandingGen(nil), prev.Life.Outstanding...)
	switch o := &nl.Outstanding[i]; o.Phase {
	case mqttOutstandingOpen:
		o.Phase = mqttOutstandingFenced
	case mqttOutstandingFenced:
		if bound == 0 {
			return nil, fmt.Errorf("bounding generation %d requires the stream's last sequence", ord)
		}
		o.Phase, o.Bound = mqttOutstandingBounded, bound
	case mqttOutstandingBounded:
		nl.Outstanding = append(nl.Outstanding[:i], nl.Outstanding[i+1:]...)
		if len(nl.Outstanding) == 0 {
			nl.Outstanding = nil
		}
	default:
		return nil, fmt.Errorf("generation %d stands at unsupported phase %d", ord, o.Phase)
	}
	next := *prev
	next.Life, next.Rev, next.Txn = &nl, prev.Rev+1, txn
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Scope: mqttLifeScope(prev.Life), Op: "op-" + txn,
			Kind: mqttTransitionRetireGeneration, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq, Auth: &next,
	}, nil
}

// mqttCheckRetireGenerationRules is the control's kind rule: an authority-only transition in
// which exactly one Outstanding entry moves exactly one step and nothing else changes.
func mqttCheckRetireGenerationRules(t *mqttAuthorityTransition) error {
	prev, next := t.Prev, t.Auth
	if prev == nil || next == nil || prev.Life == nil || next.Life == nil {
		return fmt.Errorf("a retire-generation transition requires a predecessor and a lifecycle")
	}
	if t.Closure != nil || t.Record != nil || t.Registry != nil || len(t.Extra) != 0 {
		return fmt.Errorf("a retire-generation transition writes nothing but the authority")
	}
	if _, err := mqttRetireGenerationStepOf(prev, next); err != nil {
		return err
	}
	return nil
}

// mqttRetireGenerationStepOf returns the Outstanding entry, as it stood in prev, that the step
// from prev to next moves, or why the pair is no single control step.
func mqttRetireGenerationStepOf(prev, next *mqttOutboundAuthority) (mqttOutstandingGen, error) {
	var moved mqttOutstandingGen
	po, no := prev.Life.Outstanding, next.Life.Outstanding
	// EVERYTHING BUT THE ONE ENTRY IS BYTE-IDENTICAL: the authority's ownership, state and
	// generation, the acceptance, the pending acceptance, the anchors and every other entry.
	strip := func(a *mqttOutboundAuthority, keep []mqttOutstandingGen) []byte {
		c := *a
		l := *a.Life
		l.Outstanding = keep
		c.Life, c.Rev, c.Txn = &l, 0, _EMPTY_
		b, _ := json.Marshal(&c)
		return b
	}
	switch {
	case len(no) == len(po):
		diff := -1
		for i := range po {
			if po[i] != no[i] {
				if diff >= 0 {
					return moved, fmt.Errorf("a retire-generation transition moves more than one generation")
				}
				diff = i
			}
		}
		if diff < 0 {
			return moved, fmt.Errorf("a retire-generation transition moves no generation")
		}
		p, n := po[diff], no[diff]
		step := p
		switch p.Phase {
		case mqttOutstandingOpen:
			step.Phase = mqttOutstandingFenced
		case mqttOutstandingFenced:
			step.Phase, step.Bound = mqttOutstandingBounded, n.Bound
			if n.Bound == 0 {
				return moved, fmt.Errorf("a retire-generation transition bounds generation %d at no sequence", p.Ord)
			}
		default:
			return moved, fmt.Errorf("a retire-generation transition moves generation %d from phase %d", p.Ord, p.Phase)
		}
		if n != step {
			return moved, fmt.Errorf("a retire-generation transition moves generation %d to %+v, not its next step", p.Ord, n)
		}
		moved = p
	case len(no) == len(po)-1:
		gone := -1
		for i, j := 0, 0; i < len(po); i++ {
			if j < len(no) && po[i] == no[j] {
				j++
				continue
			}
			if gone >= 0 {
				return moved, fmt.Errorf("a retire-generation transition removes more than one generation")
			}
			gone = i
		}
		if gone < 0 || po[gone].Phase != mqttOutstandingBounded {
			return moved, fmt.Errorf("a retire-generation transition removes a generation that is not bounded")
		}
		moved = po[gone]
	default:
		return moved, fmt.Errorf("a retire-generation transition changes the number of generations by %d", len(no)-len(po))
	}
	if !bytes.Equal(strip(prev, nil), strip(next, nil)) {
		return moved, fmt.Errorf("a retire-generation transition changes the authority beyond its generation's phase")
	}
	return moved, nil
}

// checkGenerationFenceLicence is the fence's licence, read from the store at the write boundary:
// a generation is fenced only when its completion certificate stands and its recovery root no
// longer names it. (That its teardown release landed is the authority's own validation: no
// authority holds a fenced generation whose release is pending.) Without it a hand-built control
// would purge live work.
func (jsa *mqttJSA) checkGenerationFenceLicence(t *mqttAuthorityTransition) error {
	moved, err := mqttRetireGenerationStepOf(t.Prev, t.Auth)
	if err != nil || moved.Phase != mqttOutstandingOpen {
		return err
	}
	a := t.Attempt
	gen, gerr := mqttGenToken(t.Prev.Life.L, moved.Ord)
	if gerr != nil {
		return gerr
	}
	if cert, cerr := jsa.loadGenCompletionCert(a.ID, a.Epoch, gen); cerr != nil || cert == nil {
		return fmt.Errorf("transition %q fences generation %q with no completion certificate standing (%v)", a.Txn, gen, cerr)
	}
	closed, _, cst := jsa.loadGenClosure(a.ID, a.Epoch, gen)
	if cst != mqttOutboundValid {
		return fmt.Errorf("transition %q fences generation %q, whose closure reads %v", a.Txn, gen, cst)
	}
	root, _, rerr := jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd)
	if rerr != nil {
		return fmt.Errorf("transition %q fences generation %q, whose recovery root cannot be read: %w", a.Txn, gen, rerr)
	}
	if root != nil && root.ID == a.ID && root.Epoch == a.Epoch && root.Gen == gen {
		return fmt.Errorf("transition %q fences generation %q, whose recovery root still stands", a.Txn, gen)
	}
	return nil
}

// errMQTTGenerationNotRetirable: the generation's work is not finished yet, so its retirement
// waits; a later visit tries again.
var errMQTTGenerationNotRetirable = errors.New("the generation is not retirable yet")

// mqttGenerationRetireSteps bounds one drive: three controls and a purge, with room for re-reads.
const mqttGenerationRetireSteps = 8

// retireGeneration drives generation gen of client id to retirement from whatever phase it
// stands at. admit is asked before every authority write (consult 12). It returns nil once the
// generation's entry is gone, errMQTTGenerationNotRetirable while its work is unfinished.
func (jsa *mqttJSA) retireGeneration(id, epoch, gen string, admit func() bool) error {
	l, ord, ok := mqttParseGenToken(gen)
	if !ok {
		return fmt.Errorf("retiring %q: not a generation token", gen)
	}
	idHash := getHash(id)
	for step := 0; step < mqttGenerationRetireSteps; step++ {
		al := jsa.loadOutboundAuthority(id, epoch)
		if al.status != mqttOutboundValid || al.rec.Life == nil || al.rec.Life.L != l {
			return fmt.Errorf("retiring %q of %q: the authority reads %v", gen, id, al.status)
		}
		i := mqttOutstandingIndex(al.rec.Life, ord)
		if i < 0 {
			return nil
		}
		var bound uint64
		switch o := al.rec.Life.Outstanding[i]; o.Phase {
		case mqttOutstandingOpen:
			if o.Teardown.Pending {
				return errMQTTGenerationNotRetirable
			}
		case mqttOutstandingFenced:
			// THE BOUND IS READ AFTER THE FENCE COMMITTED: nothing scoped to the generation can be
			// written past it, so every one of its records lies at or below it.
			_, last, serr := jsa.streamSubjects(mqttSessStreamName, mqttOutboundAuthoritySubject(idHash))
			if serr != nil {
				return serr
			}
			bound = last
		case mqttOutstandingBounded:
			for _, f := range mqttGenerationPurgeFilters(idHash, gen) {
				if perr := jsa.purgeStream(mqttSessStreamName, f, o.Bound+1, false); perr != nil {
					return fmt.Errorf("purging %q of %q: %w", f, gen, perr)
				}
			}
		}
		tr, cerr := mqttComposeRetireGenerationStep(al.rec, al.seq, mqttNewInflightNonce(), ord, bound)
		if cerr != nil || tr == nil {
			return cerr
		}
		if admit != nil && !admit() {
			return errMQTTGenerationNotRetirable
		}
		if _, err := jsa.commitAuthorityTransition(tr); err != nil {
			if mqttRetireGenerationNotLicensed(err) {
				return errMQTTGenerationNotRetirable
			}
			// AMBIGUOUS OR CONTENDED: the next pass re-reads the phase, which is the answer.
			continue
		}
	}
	return fmt.Errorf("retiring %q of %q did not converge in %d steps", gen, id, mqttGenerationRetireSteps)
}

// mqttRetireGenerationNotLicensed reports a fence refused for unfinished work.
func mqttRetireGenerationNotLicensed(err error) bool {
	return err != nil && errors.Is(err, errMQTTGenerationFenceUnlicensed)
}

// errMQTTGenerationFenceUnlicensed marks the fence licence's refusal.
var errMQTTGenerationFenceUnlicensed = errors.New("the generation's work is not finished")
