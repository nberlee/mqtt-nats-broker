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

// testSubjectMatches is NATS subject matching: "*" one token, ">" one or more trailing tokens.
func testSubjectMatches(subject, filter string) bool {
	st, ft := strings.Split(subject, "."), strings.Split(filter, ".")
	for i, f := range ft {
		if f == ">" {
			return len(st) > i
		}
		if i >= len(st) || (f != "*" && f != st[i]) {
			return false
		}
	}
	return len(st) == len(ft)
}

// The reclamation filters (issue #133) reach exactly their scope: a generation's filters reach
// its records and no other generation's, nor anything of its lifecycle that outlives it; a
// lifecycle's filters reach its client's records and nothing keyed by no client (name ownership,
// the registry, the recovery directory, the shared groups) or by another client.
func TestReclamationFiltersReachOnlyTheirScope(t *testing.T) {
	idHash, other := getHash("client-a"), getHash("client-b")
	l, _, _ := mqttParseGenToken(testGen)
	life, err := mqttLifecycleScope(l)
	if err != nil {
		t.Fatal(err)
	}
	matches := func(subject string, filters []string) bool {
		for _, f := range filters {
			if testSubjectMatches(subject, f) {
				return true
			}
		}
		return false
	}
	gen := mqttGenerationPurgeFilters(idHash, testGen)
	for _, s := range []string{
		mqttGenClosureSubject(idHash, testGen), mqttGenLedgerSubject(idHash, testGen),
		mqttDutySubject(idHash, testGen, 3),
		mqttExchangeSubject(idHash, testGen, "nonce-x"), mqttTransitionOutcomeSubject(idHash, testGen, "txn-x"),
		mqttCloseCertSubject(idHash, testGen, "run", "conn"), mqttSharedLedgerSubject(idHash, testGen),
	} {
		if !matches(s, gen) {
			t.Fatalf("the generation's filters %v miss its record %q", gen, s)
		}
	}
	for _, s := range []string{
		mqttGenClosureSubject(idHash, testGen2), mqttTransitionOutcomeSubject(idHash, testGen2, "txn-x"),
		mqttTransitionOutcomeSubject(idHash, life, "txn-x"), mqttOutboundAuthoritySubject(idHash),
		mqttGenClosureSubject(other, testGen), mqttSharedLedgerSubject(idHash, testGen2),
	} {
		if matches(s, gen) {
			t.Fatalf("the generation's filters %v reach %q, which is not its record", gen, s)
		}
	}
	lc := mqttLifecyclePurgeFilters(idHash)
	for _, s := range []string{
		mqttOutboundAuthoritySubject(idHash), mqttTransitionOutcomeSubject(idHash, life, "txn-x"),
		mqttGenClosureSubject(idHash, testGen2), mqttSharedLedgerSubject(idHash, testGen),
	} {
		if !matches(s, lc) {
			t.Fatalf("the lifecycle's filters %v miss its record %q", lc, s)
		}
	}
	for _, s := range []string{
		mqttNameOwnershipSubject(mqttOwnedStreamMsgs, "consumer"), mqttNameOwnershipSubject(mqttOwnedStreamOut, "consumer"),
		mqttLifecycleRegistrySubject(3), mqttRootSubject(3, 7), mqttSharedGroupSubject("group-key"),
		mqttOutboundAuthoritySubject(other), mqttGenClosureSubject(other, testGen),
	} {
		if matches(s, lc) {
			t.Fatalf("the lifecycle's filters %v reach %q, which is no record of the client", lc, s)
		}
	}
	// THE NAME-OWNERSHIP TOKEN IS NEVER A CLIENT'S: an owned stream's name cannot be an idHash.
	for _, st := range []mqttOwnedStream{mqttOwnedStreamMsgs, mqttOwnedStreamOut} {
		if mqttValidIDHashToken(string(st)) {
			t.Fatalf("owned stream %q reads as a client hash, so a lifecycle purge could reach name ownership", st)
		}
	}
}
