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
)

// METADATA RELEASE RESTS ON DURABLE FACTS, AND THE CLEANUP THEY PROMISE SURVIVES A RESTART (F-105
// WP-F W2, row D1 and §7.3 scenario 8).
//
// A discarding teardown releases the session's metadata while its physical cleanup is still
// outstanding: the source and PUBREL consumers still exist and their duties are open. That is only
// safe because the release gate's facts hold at that moment - the generation is closed with its
// ledger sealed at a bound, the closure carries the discard, and the generation's recovery root is
// ENROLLED and reachable in its directory shard - so some broker serving the account will come
// back for the work. This test makes the promise literal: the broker that tore the session down is
// shut down with every recovery caller parked, a new broker starts on the same store, and the
// recovery pass alone - no kick, no session, no teardown - retires both consumers, discards the
// obligation and certifies the generation.
//
// Mutations (D1): the executor withholds the source and (separately) the PUBREL consumer's
// retirement; a release-gate mutant that does not check durable recovery ownership is recorded
// against the gate's own tests.
func TestF105ARestartedBrokerRetiresADiscardedGenerationsConsumers(t *testing.T) {
	// NOT parallel: the recovery kick and the sweep interval are process-wide.
	testDiscardParkAll(t) // never released: only the restarted broker's recovery pass may act
	f, o := testDiscardTearDownOpts(t, "w2-restart-discard", 2, true, nil)
	if f.pubRel == _EMPTY_ {
		t.Fatal("fixture: the QoS 2 session has no PUBREL consumer")
	}

	// BEFORE THE RESTART, with the cleanup outstanding.
	if _, err := f.asm.jsa.loadLastMsgFor(mqttSessStreamName,
		mqttSessStreamSubjectPrefix+f.asm.domainTk+getHash(f.id)); err == nil {
		t.Fatal("the session metadata was not released")
	}
	consumers := []struct {
		stream mqttOwnedStream
		js     string
		name   string
	}{{mqttOwnedStreamMsgs, mqttStreamName, f.src}, {mqttOwnedStreamOut, mqttOutStreamName, f.pubRel}}
	for _, c := range consumers {
		if !f.consumerExists(t, c.js, c.name) {
			t.Fatalf("fixture: consumer %q is already gone, so no cleanup is outstanding", c.name)
		}
		own := f.ownership(t, c.stream, c.name)
		if own == nil {
			t.Fatalf("fixture: consumer %q has no ownership record", c.name)
		}
		ord, err := f.asm.jsa.resourceDutyOrdinal(f.id, f.epoch(), f.gen, own.OwnerInc, mqttDutyCleanup)
		if err != nil {
			t.Fatalf("locating the cleanup duty of %q: %v", c.name, err)
		}
		d, _, derr := f.asm.jsa.loadDutyEntryAt(f.id, f.epoch(), f.gen, ord)
		if derr != nil || d == nil || d.State == mqttDutyTerminal {
			t.Fatalf("fixture: the cleanup duty of %q is not open: %+v (%v)", c.name, d, derr)
		}
	}
	// The release gate's facts, on the persisted state.
	facts, ferr := f.asm.gatherAdoptedReleaseFacts(f.id, f.epoch(), f.gen)
	if ferr != nil {
		t.Fatalf("gathering the release facts: %v", ferr)
	}
	// (1) committed closure, ledger sealed at a bound.
	if facts.Closed == nil || facts.Ledger == nil || facts.Ledger.State != mqttGenLedgerSealed || facts.Ledger.SealedHi == 0 {
		t.Fatalf("fact 1: no committed closure over a ledger sealed at a bound: closure=%+v ledger=%+v", facts.Closed, facts.Ledger)
	}
	// (2) the discard closure.
	if facts.Discard != mqttFactApplies {
		t.Fatalf("fact 2: the closure does not carry the discard (disposition %v)", facts.Discard)
	}
	// (3) durable recovery ownership - here the enrolled, reachable root: no certificate can stand
	// while the cleanup is outstanding.
	if !facts.RootEnrolled || facts.Cert != nil {
		t.Fatalf("fact 3: want an enrolled reachable root and no certificate yet: enrolled=%v cert=%+v", facts.RootEnrolled, facts.Cert)
	}
	// (4) the affine target is enrolled - VACUOUS: this generation has no affine consumer at all.
	res, unreadable, rerr := f.asm.enumerateGenerationResources(f.id, f.epoch(), f.gen)
	if rerr != nil || unreadable != 0 || len(res) == 0 {
		t.Fatalf("enumerating the generation's resources: %d resources, %d unreadable (%v)", len(res), unreadable, rerr)
	}
	for _, r := range res {
		if r.Kind == mqttResourceAffineConsumer {
			t.Fatalf("fact 4 is not vacuous here: the generation carries affine resource %q", r.Inc)
		}
	}
	if blocks := mqttAdoptedReleaseBlockedBy(f.id, f.gen, facts); len(blocks) != 0 {
		t.Fatalf("the release gate blocks on the persisted state: %v", blocks)
	}

	// THE RESTART on the same store, every recovery caller still parked.
	oldAsm, epoch := f.asm, f.epoch()
	f.s.Shutdown()
	s2 := testMQTTRestartServerSameStore(t, f.s, o)
	t.Cleanup(func() { testMQTTShutdownServer(s2) })
	if err := s2.Start(); err != nil {
		t.Fatalf("restarting the broker: %v", err)
	}
	probe, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "w2-restart-probe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { probe.Close() })
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	asm2 := testMQTTGetAccountSessionManager(t, s2, "w2-restart-probe")
	if asm2 == oldAsm {
		t.Fatal("fixture: the restarted broker resolved the old session manager")
	}
	if asm2.sessStreamEpoch != epoch {
		t.Fatalf("the restarted broker reads recovery epoch %q, want the store's %q", asm2.sessStreamEpoch, epoch)
	}
	f.s, f.asm = s2, asm2

	done := func() error {
		if err := f.obligationDiscarded(t); err != nil {
			return err
		}
		for _, c := range consumers {
			if err := f.retired(t, c.stream, c.js, c.name); err != nil {
				return err
			}
		}
		return f.certified(t)
	}
	// ROUTE WITNESS: nothing was finished before the pass this test drives.
	if done() == nil {
		t.Fatal("fixture: the discarded generation was finished before any recovery pass ran on the restarted broker")
	}
	for _, c := range consumers {
		if !f.consumerExists(t, c.js, c.name) {
			t.Fatalf("fixture: consumer %q was deleted before any recovery pass ran on the restarted broker", c.name)
		}
	}
	var last error
	for pass := 0; pass < 6*mqttDirShards; pass++ {
		asm2.sweepOutboundRecovery(s2, nil)
		if last = done(); last == nil {
			break
		}
	}
	if last != nil {
		t.Fatalf("the restarted broker's recovery pass did not finish the discarded generation: %v", last)
	}
}
