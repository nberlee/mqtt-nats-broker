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

import (
	"strings"
	"testing"
)

// TestF105AnUnreadableResourceRefusesTheTeardownClassification pins that a discarding generation
// one of whose resource records cannot be interpreted is NOT classified. The classification
// decides what the teardown transfers to recovery and what it deletes itself, and a record it
// cannot read may be exactly the enrolment it is asking about: "I could not read it" is never "it
// is not there". The refusal carries no class, no affine target and no transfer, and nothing is
// deleted.
func TestF105AnUnreadableResourceRefusesTheTeardownClassification(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-part-unreadable")
	defer cleanup()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	gen, aff := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	// A REAL affine consumer at the enrolled name, so "nothing deleted" is a fact about the store.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			Durable: durable, FilterSubject: mqttOutSubjectPrefix + "partunreadable",
			AckPolicy: AckExplicit, DeliverSubject: mqttSubPrefix + durable, MaxAckPending: 1024,
		},
	}); err != nil {
		t.Fatalf("creating the affine consumer on %s: %v", mqttOutStreamName, err)
	}
	newTarget := func() *mqttTeardownTarget {
		return &mqttTeardownTarget{
			id: e.id, idHash: getHash(e.id), gen: gen,
			q2AffDur: durable, q2AffGen: affGen, pubRelDur: pubRelDur,
		}
	}

	// THE POSITIVE CONTROL: the same generation, every record readable, classifies as discarding
	// with its enrolled affine target - so the refusal below is caused by the unreadable record.
	if own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, newTarget()); err != nil ||
		own.Class != mqttTeardownOwnerDiscarding || own.Affine == nil || own.Affine.Inc != aff.Inc {
		t.Fatalf("fixture: the readable generation did not classify as discarding with its affine "+
			"target: %+v (%v)", own, err)
	}

	// One resource record of the generation whose body this broker cannot interpret.
	corrupt := mqttResourceSubject(getHash(e.id), gen, "inc-unreadable")
	if _, err := e.asm.jsa.storeMsg(corrupt, 0, []byte("{not a resource record")); err != nil {
		t.Fatalf("planting the unreadable resource record: %v", err)
	}
	if _, unreadable, err := e.asm.enumerateGenerationResources(e.id, e.epoch, gen); err != nil || unreadable != 1 {
		t.Fatalf("fixture: the enumeration does not report exactly the planted record unreadable: "+
			"%d (%v)", unreadable, err)
	}

	target := newTarget()
	own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, target)
	if err == nil {
		t.Fatalf("a discarding generation with an unreadable resource record was classified as %v", own.Class)
	}
	if !strings.Contains(err.Error(), "could not be interpreted") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	// NOTHING TRANSFERRED: the refusal is the zero classification, which a caller that ignored the
	// error would still read as "no owner", never as a transfer.
	if own.Class != mqttTeardownOwnerUnknown || own.Affine != nil || len(own.Unenrolled) != 0 {
		t.Fatalf("the refused classification still carried a decision: %+v", own)
	}
	if target.deferAffineDelete || target.transferLogical {
		t.Fatalf("the refused classification transferred the target: %+v", target)
	}
	// AND NOTHING DELETED: the enrolled resource and its consumer stand.
	if ci, lerr := e.asm.jsa.lookupConsumer(mqttOutStreamName, durable); lerr != nil || ci == nil {
		t.Fatalf("the affine consumer was deleted across a refused classification: %v", lerr)
	}
	if r, _, rerr := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, aff.Inc); rerr != nil || r == nil ||
		r.State != aff.State {
		t.Fatalf("the enrolled affine resource moved across a refused classification: %+v (%v)", r, rerr)
	}
}
