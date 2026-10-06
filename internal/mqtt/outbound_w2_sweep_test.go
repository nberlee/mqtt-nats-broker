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
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// TestF105PlainSweepDeletesOnlyARetiredName pins that the plain orphan sweep's DELETE is
// authorized by a name's RETIRED ownership and by nothing else (F-105 §5.1 points 5 and 6, WP-F
// W2 row D3). Three consumers stand in ONE pass, each unreferenced by any session and aged past
// the grace window, so the absence of a reference - the sweep's original, wrong authority - would
// license all three:
//   - a LIVE name carrying an admitted QoS 1 obligation's dependency, which must survive: deleting
//     it destroys the redelivery the obligation is owed;
//   - a name with NO ownership record, which must survive: a missing record means no claim landed,
//     never that the name is free;
//   - a RETIRED name resurrected by a late CREATE, the positive control that must go, so a pass that
//     deleted nothing cannot satisfy the two survival assertions vacuously.
func TestF105PlainSweepDeletesOnlyARetiredName(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "w2-sweep-retired-only")
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch

	// LIVE: claimed through the production claim, then depended on by an admitted QoS 1 delivery.
	live := testClaimedSourceConsumer(t, e, "w2.sweep.live")
	pi := e.trackAdmitted(t, live.name, 1, 1)
	if pi == 0 {
		t.Fatal("fixture: the QoS 1 delivery over the LIVE source was not admitted")
	}
	if own, _, oerr := jsa.loadNameOwnershipPhysical(epoch, mqttOwnedStreamMsgs, live.name); oerr != nil ||
		own == nil || own.State != mqttNameOwnLive || own.OwnerInc != live.inc {
		t.Fatalf("fixture: the LIVE source name is not LIVE for its incarnation: %+v (%v)", own, oerr)
	}
	if r, _, rerr := jsa.loadOutboundResource(e.sess.id, epoch, live.gen, live.inc); rerr != nil || r == nil ||
		r.State != mqttResourceLive || len(r.Deps) == 0 {
		t.Fatalf("fixture: the LIVE source carries no admitted dependency: %+v (%v)", r, rerr)
	}

	// MISSING: a session-shaped durable no claim ever landed for.
	missing := e.sess.idHash + "_" + nuid.Next()
	if _, err := jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: missing, DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "w2.sweep.missing"}}); err != nil {
		t.Fatalf("creating the unowned durable: %v", err)
	}
	if own, _, oerr := jsa.loadNameOwnershipPhysical(epoch, mqttOwnedStreamMsgs, missing); oerr != nil || own != nil {
		t.Fatalf("fixture: the unowned durable has an ownership record: %+v (%v)", own, oerr)
	}

	// RETIRED control: retired through the production retirement route, then resurrected by a late
	// CREATE under the spent name.
	retired := testClaimedSourceConsumer(t, e, "w2.sweep.retired")
	if step, err := testStepUntil(t, e.sess, testRetireItem(retired), 10); step != mqttReclaimDone {
		t.Fatalf("fixture: the control's retirement did not finish: %v %v", step, err)
	}
	testRequireSourceRetired(t, e, retired.gen, retired.inc, retired.name)
	if _, err := jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: retired.name, DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "w2.sweep.retired"}}); err != nil {
		t.Fatalf("the late CREATE under the retired name: %v", err)
	}

	// None of the three is referenced by a local session or a session record.
	e.sess.mu.Lock()
	for _, cc := range e.sess.cons {
		if cc.Durable == live.name || cc.Durable == missing || cc.Durable == retired.name {
			e.sess.mu.Unlock()
			t.Fatalf("fixture: %q is referenced by the local session, so the sweep would never judge it", cc.Durable)
		}
	}
	e.sess.mu.Unlock()
	for _, name := range []string{live.name, missing, retired.name} {
		if !testConsumerPresent(t, e, name) {
			t.Fatalf("fixture: %q does not stand before the sweep", name)
		}
	}

	// ONE pass, with every consumer past the grace cutoff.
	e.asm.sweepOrphanedConsumersBefore(e.s, time.Now().Add(time.Minute))

	if testConsumerPresent(t, e, retired.name) {
		t.Fatal("the plain sweep left a resurrected consumer whose name is RETIRED: the pass " +
			"deleted nothing, so the survivals below would prove nothing")
	}
	if !testConsumerPresent(t, e, live.name) {
		t.Fatal("the plain sweep deleted a LIVE source consumer an admitted obligation depends on: " +
			"an absent reference is not a retired name")
	}
	if !testConsumerPresent(t, e, missing) {
		t.Fatal("the plain sweep deleted a durable with no ownership record: a missing owner is " +
			"never permission")
	}
}
