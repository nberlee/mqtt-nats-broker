//go:build mqtt_fulltests

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

// The generation's resource enumeration (F-105, row 8), which the teardown classification and
// the recovery executor read before anything a discard transfers is retired.

// TestF105ATruncatedResourcePageIsNotAnAnswer is round I-6's third finding.
//
// The enumeration answers which resources a generation has. A walk that stops at a page it cannot
// vouch for omits whichever resource sits past it — and a reader then misses a resource that
// still names a dependent, or an enrolment a discard must transfer, which is an absence read as
// a fact in the one place this mechanism exists to stop it.
func TestF105ATruncatedResourcePageIsNotAnAnswer(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-resource-truncated")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)

	// A VALID FIRST PAGE that reports more beyond it, then a prematurely EMPTY second page: the
	// walk cannot vouch for the rest.
	truncated := testUnvouchablePages(t)
	e.asm.jsa.exchangeSubjectsObs.Store(&truncated)
	if _, _, err := e.asm.enumerateGenerationResources(e.id, e.epoch, testF105Gen); err == nil ||
		!strings.Contains(err.Error(), "could not be enumerated completely") {
		t.Fatalf("an incompletely enumerated answer was read as this generation's whole resource inventory: %v", err)
	}

	// THE EMPTY ANSWER, which the server gives a filter matching nothing (every paged field zero),
	// is complete — so what refused above is the unvouched page and not the emptiness.
	complete := func(string, int) (mqttSubjectPage, error) {
		return mqttSubjectPage{}, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&complete)
	if _, _, err := e.asm.enumerateGenerationResources(e.id, e.epoch, testF105Gen); err != nil {
		t.Fatalf("a complete answer holding nothing was refused: %v", err)
	}
	e.asm.jsa.exchangeSubjectsObs.Store(nil)
}
