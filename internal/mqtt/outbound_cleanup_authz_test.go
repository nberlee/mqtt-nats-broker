//go:build mqttport_fulltests

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

import "testing"

// The generation's resource enumeration (F-105, row 8), which the teardown classification and
// the recovery executor read before anything a discard transfers is retired.

// TestF105ATruncatedResourcePageIsNotAnAnswer is round I-6's third finding.
//
// The enumeration answers which resources a generation has. A first page read as the whole
// answer omits whichever resource sits past the cut — and a reader then misses a resource that
// still names a dependent, or an enrolment a discard must transfer, which is an absence read as
// a fact in the one place this mechanism exists to stop it.
func TestF105ATruncatedResourcePageIsNotAnAnswer(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-resource-truncated")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)

	truncated := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 100_001, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&truncated)
	if _, _, err := e.asm.enumerateGenerationResources(e.id, e.epoch, testF105Gen); err == nil {
		t.Fatalf("a first page was read as this generation's whole resource inventory")
	}

	// THE SAME EMPTY PAGE, REPORTED COMPLETE, is an answer — so what refused above is the
	// truncation and not the emptiness.
	complete := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 0, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&complete)
	if _, _, err := e.asm.enumerateGenerationResources(e.id, e.epoch, testF105Gen); err != nil {
		t.Fatalf("a complete answer holding nothing was refused: %v", err)
	}
	e.asm.jsa.exchangeSubjectsObs.Store(nil)
}
