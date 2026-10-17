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

import "testing"

// Fixture helpers for the pending-entry kinds. Production reaches kind and evidence only through
// the constructors in outbound_pending_kind.go; a fixture that PLANTS a state names it here, so a
// planted Ordinary entry is visibly a claim the fixture makes rather than an accident of a literal.

// testOpenStoreCustody opens the allocation's admission custody, as production does before any
// ordinary register store (the authority admission opens it, openAdmissionCustodyLocked). A test
// that drives the standalone store door — whose only production caller is the affine delivery —
// with an ORDINARY durable states that precondition here; without it the landing promotes
// nothing, because an Ordinary entry exists only under custody.
func testOpenStoreCustody(t *testing.T, sess *mqttSession, pi uint16) {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	p := sess.pendingPublish[pi]
	if p == nil || !sess.openAdmissionCustodyLocked(sess.gen, pi, p.regNonce) {
		t.Fatalf("fixture: the allocation at %d could not open its admission custody", pi)
	}
}

// testConfirmLanded is a fixture's "the register landed and the PUBLISH was sent": production's
// confirmation PROMOTES the allocation (mqttPromoteLanded), which is what makes it an acting
// entry, so a fixture standing for that state promotes too rather than clearing regPending in
// place (which would leave the allocation's Hold). Planted directly: such fixtures have no ledger.
func testConfirmLanded(sess *mqttSession, pi uint16, transmitted bool) {
	p := sess.pendingPublish[pi]
	sess.pendingPublish[pi] = mqttPromoteLanded(p, mqttLandedRegister{gen: sess.gen, pi: pi, jsDur: p.jsDur,
		sseq: p.sseq, regSeq: 1, nonce: p.regNonce, lin: p.regNonce, qos: 1}, 0, transmitted, mqttInflightSlotRef{}, false)
}
