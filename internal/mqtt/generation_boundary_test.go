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
	"strings"
	"testing"
)

// testClosedOver is testTransition's authority after testGen was made noncurrent: closed at its
// last generation, with testGen outstanding at phase, and the transition scoped to testGen.
func testClosedOver(phase mqttOutstandingPhase) *mqttAuthorityTransition {
	tr := testTransition()
	tr.Attempt.Kind = mqttTransitionDisposeResource
	for _, auth := range []*mqttOutboundAuthority{tr.Prev, tr.Auth} {
		auth.State = mqttAuthorityClosed
		auth.Life.Phase = mqttLifecycleClosed
		auth.Life.Outstanding = []mqttOutstandingGen{{Ord: 0, Phase: phase,
			Teardown: mqttOutstandingTeardown{Kind: mqttTeardownMarker, Nonce: "nonce-x", MarkerWID: "wid-x"}}}
	}
	return tr
}

// A generation-scoped transition is written only while its generation is current or open:
// a fenced or bounded generation's retirement has begun, and a retired one is gone.
func TestAGenerationScopedTransitionNeedsAnOpenGeneration(t *testing.T) {
	if err := mqttCheckGenerationBoundary(testTransition()); err != nil {
		t.Fatalf("a transition scoped to the current generation was refused: %v", err)
	}
	if err := mqttCheckGenerationBoundary(testClosedOver(mqttOutstandingOpen)); err != nil {
		t.Fatalf("a transition scoped to an open outstanding generation was refused: %v", err)
	}
	for name, tr := range map[string]*mqttAuthorityTransition{
		"fenced":  testClosedOver(mqttOutstandingFenced),
		"bounded": testClosedOver(mqttOutstandingBounded),
		"retired": func() *mqttAuthorityTransition {
			tr := testClosedOver(mqttOutstandingOpen)
			tr.Prev.Life.Outstanding, tr.Auth.Life.Outstanding = nil, nil
			return tr
		}(),
	} {
		err := mqttCheckGenerationBoundary(tr)
		if err == nil || !strings.Contains(err.Error(), "only a current or open generation") {
			t.Fatalf("a transition scoped to a %s generation was not refused by the scope rule: %v", name, err)
		}
	}
}

// A retiring lifecycle takes no transition, lifecycle-scoped or generation-scoped.
func TestARetiringLifecycleTakesNoTransition(t *testing.T) {
	for _, kind := range []mqttTransitionKind{mqttTransitionAdmitExchange, mqttTransitionWillAnchor} {
		tr := testTransition()
		tr.Attempt.Kind = kind
		if kind == mqttTransitionWillAnchor {
			tr.Attempt.Scope = mqttLifeScope(tr.Prev.Life)
		}
		if err := mqttCheckGenerationBoundary(tr); err != nil {
			t.Fatalf("a %s transition over a serving lifecycle was refused: %v", kind, err)
		}
		tr.Prev.Life.Phase = mqttLifecycleRetiring
		err := mqttCheckGenerationBoundary(tr)
		if err == nil || !strings.Contains(err.Error(), "which is retiring") {
			t.Fatalf("a %s transition over a retiring lifecycle was not refused by the phase rule: %v", kind, err)
		}
	}
}

// A transition that ends the current generation closes it and enrols it outstanding in the
// same batch, whatever its kind: a noncurrent generation is a discarded one.
func TestMakingAGenerationNoncurrentIsADiscard(t *testing.T) {
	closing := func(closure, enrol bool) *mqttAuthorityTransition {
		tr := testTransition()
		tr.Attempt.Kind = mqttTransitionTeardownClaim
		tr.Attempt.Scope = mqttLifeScope(tr.Prev.Life)
		tr.Auth.State = mqttAuthorityClosed
		if closure {
			tr.Closure = &mqttGenClosure{ID: testID, Epoch: testEpoch, Gen: testGen, Txn: tr.Attempt.Txn}
		}
		if enrol {
			tr.Auth.Life.Outstanding = []mqttOutstandingGen{{Ord: 0, Phase: mqttOutstandingOpen}}
		}
		return tr
	}
	if err := mqttCheckGenerationBoundary(closing(true, true)); err != nil {
		t.Fatalf("a close that closes and enrols its generation was refused: %v", err)
	}
	if err := mqttCheckGenerationBoundary(closing(false, true)); err == nil ||
		!strings.Contains(err.Error(), "without closing it") {
		t.Fatalf("a close writing no closure was not refused by the discard rule: %v", err)
	}
	if err := mqttCheckGenerationBoundary(closing(true, false)); err == nil ||
		!strings.Contains(err.Error(), "without enrolling it outstanding") {
		t.Fatalf("a close enrolling nothing outstanding was not refused by the discard rule: %v", err)
	}
	// A REPLACEMENT is a noncurrency too: serving a successor ends the predecessor.
	replaced := testTransition()
	replaced.Attempt.Kind = mqttTransitionAcceptHandoff
	replaced.Attempt.Scope = mqttLifeScope(replaced.Prev.Life)
	replaced.Auth.Gen = testGen2
	if err := mqttCheckGenerationBoundary(replaced); err == nil || !strings.Contains(err.Error(), "without closing it") {
		t.Fatalf("a replacement writing no closure was not refused by the discard rule: %v", err)
	}
}
