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
	"fmt"
	"testing"
)

// The acceptance fixtures only the tagged suite reaches.

// testWillToken is a well-formed outbox identity naming anchor ord of lifecycle 1, for a
// record that is only stored, loaded or refused, never decided.
func testWillToken(t testing.TB, ord uint64) string {
	t.Helper()
	id, err := mqttWillToken(1, ord)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// testRetireGenerationUnderLater is testRetireGeneration for a generation the authority serves,
// with a LATER generation of the lifecycle installed after it and torn down as well, so the
// closed authority's last generation is not gen: the state a successor's Clean Start and its own
// close leave.
func testRetireGenerationUnderLater(t testing.TB, asm *mqttAccountSessionManager, clientID, gen string) {
	t.Helper()
	testRetireGenerationAs(t, asm, clientID, gen, true)
}

// testGenNoncurrent reports whether gen reads outstanding or retired for clientID: the fact a
// teardown or a Clean Start leaves, and what the retirement watermark used to record.
func testGenNoncurrent(asm *mqttAccountSessionManager, clientID, gen string) bool {
	s, _ := asm.genStanding(clientID, gen)
	return s == mqttGenOutstanding || s == mqttGenRetired
}

// testCloseGenerationOp is testCloseGeneration in the shape of the deleted operation's callers:
// the recovery epoch named (and required to be the account's), an outcome carrying nothing.
func testCloseGenerationOp(asm *mqttAccountSessionManager, id, epoch, gen, tenure, ownerTxn string) (mqttOpOutcome, error) {
	if epoch != asm.sessStreamEpoch {
		return mqttOpOutcome{}, fmt.Errorf("closing %q under epoch %q, not the account's %q", id, epoch, asm.sessStreamEpoch)
	}
	return mqttOpOutcome{}, testCloseGeneration(asm, id, gen, tenure, ownerTxn)
}

// mqttLifeOf is the lifecycle section a rewrite of prev carries forward, or none for no
// predecessor.
func mqttLifeOf(prev *mqttOutboundAuthority) *mqttLifecycleState {
	if prev == nil {
		return nil
	}
	return prev.Life
}

// testCloseGeneration closes the generation (gen, tenure, ownerTxn) of id through the claim
// batch over the standing authority, refusing as the composer does when that ownership is not
// the one standing.
func testCloseGeneration(asm *mqttAccountSessionManager, id, gen, tenure, ownerTxn string) error {
	al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid {
		return fmt.Errorf("the authority of %q reads %v", id, al.status)
	}
	t, err := testComposeTeardownClaim(asm, id, al.rec, al.seq, mqttNewInflightNonce(),
		mqttRetirementRequest{Gen: gen, Tenure: tenure, OwnerTxn: ownerTxn}, nil)
	if err != nil {
		return err
	}
	_, err = asm.jsa.commitAuthorityTransition(t)
	return err
}

// testNextGenOf is the generation a's lifecycle would install next, for a fixture that rewrites
// the authority to serve a successor directly; without a lifecycle section it is a fresh name.
func testNextGenOf(a *mqttOutboundAuthority) string {
	if a == nil || a.Life == nil {
		return mqttNewSessGen()
	}
	gen, err := mqttGenToken(a.Life.L, a.Life.NextGenOrd)
	if err != nil {
		panic(err)
	}
	return gen
}

// testEnrolOutstandingGen enrols gen outstanding at phase open with its release landed, as the
// clean-start handoff that replaced it would: a rewritten authority that displaces a generation
// must leave it noncurrent and open, not retired (issue #133).
func testEnrolOutstandingGen(a *mqttOutboundAuthority, gen string) {
	l, ord, ok := mqttParseGenToken(gen)
	if a.Life == nil || !ok || l != a.Life.L || mqttOutstandingIndex(a.Life, ord) >= 0 {
		return
	}
	life := *a.Life
	life.Outstanding = append(append([]mqttOutstandingGen(nil), a.Life.Outstanding...), mqttOutstandingGen{
		Ord: ord, Phase: mqttOutstandingOpen, Teardown: mqttOutstandingTeardown{Kind: mqttTeardownHandoffDiscard},
	})
	a.Life = &life
}

// testAdoptGenLife keeps a rewritten authority's lifecycle section consistent with the generation
// the fixture made it serve: its allocator moves past that generation.
func testAdoptGenLife(a *mqttOutboundAuthority, gen string) {
	if a.Life == nil {
		return
	}
	l, ord, ok := mqttParseGenToken(gen)
	if !ok || l != a.Life.L {
		return
	}
	life := *a.Life
	if ord >= life.NextGenOrd {
		life.NextGenOrd = ord + 1
	}
	a.Life = &life
}
