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
	"fmt"
)

// The shared acquisition ledger is LAZY and FENCED by the authority (issue #133, plan 8).
//
// A generation's ledger shl.<idHash>.<g> exists only once the generation acquired: the first
// shared SUBSCRIBE installs it, in a batch that also sets the authority's GenAcqs, and a
// generation's discard copies that bit into its Outstanding entry. So an absent ledger means
// "no acquisitions" exactly when the authority says acqs is false; and after the generation is
// discarded no ledger can appear, because every ledger-carrying batch carries the authority:
//
//   - new work (an installation, a join) only while the generation is current and serving under
//     the tenure of the lifecycle's current acceptance;
//   - every other step (a bump, a close, a release, a seal) only while the generation is current
//     or outstanding at phase open, so nothing of a fenced or retired generation is written.
//
// The authority is rewritten as read (no revision moves), with GenAcqs set by the installation.

// errMQTTSharedNotCurrent: a ledger step's generation or tenure is not the authority's; the
// step is refused before it is sent, and a re-read cannot change that.
var errMQTTSharedNotCurrent = errors.New("the shared ledger's generation or tenure is not the authority's")

// mqttSharedAuthorityFence returns the authority body a ledger-carrying batch rewrites, or why the
// authority as read refuses the batch.
func mqttSharedAuthorityFence(b *mqttSharedBatch, al mqttAuthorityLoad) (*mqttOutboundAuthority, error) {
	if al.status != mqttOutboundValid || al.rec.Life == nil {
		return nil, fmt.Errorf("the authority of %q reads %v", b.ID, al.status)
	}
	a := al.rec
	if a.Life.Phase == mqttLifecycleRetiring {
		return nil, fmt.Errorf("%w: lifecycle %d of %q is retiring", errMQTTSharedNotCurrent, a.Life.L, b.ID)
	}
	next := *a
	switch b.Shape {
	case mqttSharedShapeInstall, mqttSharedShapeJoin:
		acc := a.Life.Accept
		if a.State != mqttAuthorityServing || a.Gen != b.Gen || acc.Tenure != b.Tenure.ID || acc.Ord+1 != b.Tenure.Seq {
			return nil, fmt.Errorf("%w: a %s of generation %q under tenure %+v while the authority serves %q under "+
				"acceptance %d of tenure %q", errMQTTSharedNotCurrent, b.Shape, b.Gen, b.Tenure, a.Gen, acc.Ord, acc.Tenure)
		}
		if b.Shape == mqttSharedShapeInstall && b.LedgerPrior == nil {
			// THE LEDGER'S CREATION: the generation has acquired, which its discard records.
			l := *a.Life
			l.GenAcqs = true
			next.Life = &l
		}
	default:
		s, entry, _ := mqttGenStandingIn(al, b.Gen)
		if s != mqttGenCurrent && !(s == mqttGenOutstanding && entry.Phase == mqttOutstandingOpen) {
			return nil, fmt.Errorf("%w: a %s of generation %q, which reads %v", errMQTTSharedNotCurrent, b.Shape, b.Gen, s)
		}
	}
	return &next, nil
}

// generationAcquired reports whether generation gen of client id ever acquired: the authority's
// GenAcqs while the generation is current, its Outstanding entry's acqs once discarded. Anything
// else is unresolved.
func (jsa *mqttJSA) generationAcquired(id, epoch, gen string) (bool, error) {
	al := jsa.loadOutboundAuthority(id, epoch)
	s, entry, _ := mqttGenStandingIn(al, gen)
	switch s {
	case mqttGenCurrent:
		return al.rec.Life.GenAcqs, nil
	case mqttGenOutstanding:
		return entry.Acqs, nil
	}
	return false, fmt.Errorf("generation %q of %q reads %v under an authority that reads %v, so whether it acquired is unknown",
		gen, id, s, al.status)
}
