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
	"strings"
	"testing"
	"time"
)

// testClientRecordCount counts every record the client of idHash still has on the session
// stream under the reclaimed classes: everything keyed by its idHash in the outbound and shared
// spaces, and its session record.
func testClientRecordCount(t *testing.T, asm *mqttAccountSessionManager, idHash string) (int, []string) {
	t.Helper()
	filters := append(mqttLifecyclePurgeFilters(idHash), mqttSessSubject(asm.domainTk, idHash))
	n := 0
	var names []string
	for _, f := range filters {
		subjects, _, err := asm.jsa.streamSubjects(mqttSessStreamName, f)
		if err != nil {
			t.Fatalf("listing %q: %v", f, err)
		}
		for s, c := range subjects {
			n += int(c)
			names = append(names, s)
		}
	}
	return n, names
}

// testDriveRetirement runs the recovery pass and the lifecycle pass until done reports nil.
func testDriveRetirement(t *testing.T, s *Server, asm *mqttAccountSessionManager, done func() error) {
	t.Helper()
	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		asm.sweepOutboundRecovery(s, nil)
		asm.sweepLifecycles(s)
		return done()
	})
}

// T-churn, the prober's path (issue #133): fresh-ID clean sessions with a plain QoS 0
// subscription connect and disconnect; once the recovery and lifecycle passes are quiescent,
// none of them leaves a record keyed by its client, and each lifecycle reads RETIRED in its
// registry shard.
func TestProberSessionsLeaveNoRecords(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	const probes = 5
	type probe struct {
		id, idHash string
		l          uint64
	}
	var all []probe
	for i := 0; i < probes; i++ {
		id := fmt.Sprintf("probe-%d-%d", i, time.Now().UnixNano())
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "probe/" + id, opts: 0}}); len(rc) != 1 || rc[0] != 0 {
			t.Fatalf("SUBSCRIBE: %v", rc)
		}
		al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
		if al.status != mqttOutboundValid {
			t.Fatalf("probe %q has no authority: %v", id, al.status)
		}
		testMQTTDisconnect(t, c, nil)
		c.Close()
		all = append(all, probe{id: id, idHash: getHash(id), l: al.rec.Life.L})
	}

	testDriveRetirement(t, s, asm, func() error {
		for _, p := range all {
			if n, names := testClientRecordCount(t, asm, p.idHash); n != 0 {
				return fmt.Errorf("probe %q still has %d record(s): %s", p.id, n, strings.Join(names, ", "))
			}
			reg := asm.jsa.loadLifecycleRegistryShard(asm.sessStreamEpoch, mqttLifecycleRegistryShardFor(p.idHash))
			if reg.status != mqttOutboundValid || reg.rec.classify(p.idHash, p.l) != mqttLifecycleRetired {
				return fmt.Errorf("probe %q's lifecycle %d is not retired in its registry shard (%v)", p.id, p.l, reg.status)
			}
		}
		return nil
	})
}

// A reclaimed client reconnecting with the same ID starts a NEW lifecycle: its retired one is
// never reissued, and the new one serves.
func TestARetiredClientReconnectsIntoANewLifecycle(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "retired-returns"
	idHash := getHash(id)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	first := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch).rec.Life.L
	testMQTTDisconnect(t, c, nil)
	c.Close()
	testDriveRetirement(t, s, asm, func() error {
		if n, names := testClientRecordCount(t, asm, idHash); n != 0 {
			return fmt.Errorf("%d record(s) remain: %s", n, strings.Join(names, ", "))
		}
		return nil
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if _, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the reconnect was refused: %d", rc)
	}
	al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec.Life.L == first || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the reconnect serves lifecycle %d (status %v), want a new one after %d", al.rec.Life.L, al.status, first)
	}
}

// A generation discarded by a Clean Start handoff is reclaimed while its successor serves: its
// records are purged and its Outstanding entry removed, and the successor's own records and
// lifecycle stay. Mutation: the generation purge skipped (its records survive; the lifecycle
// purge cannot mask it, because the lifecycle is not retiring).
func TestADiscardedGenerationIsReclaimedWhileItsSuccessorServes(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "gen-reclaim"
	idHash := getHash(id)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	g1 := testSessionGenOf(t, s, id)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	g2 := testSessionGenOf(t, s, id)
	if g2 == g1 {
		t.Fatalf("the Clean Start did not install a successor generation")
	}
	g1Records := func() (int, []string) {
		n := 0
		var names []string
		for _, f := range mqttGenerationPurgeFilters(idHash, g1) {
			subjects, _, err := asm.jsa.streamSubjects(mqttSessStreamName, f)
			if err != nil {
				t.Fatalf("listing %q: %v", f, err)
			}
			for sj, cnt := range subjects {
				n += int(cnt)
				names = append(names, sj)
			}
		}
		return n, names
	}
	if n, _ := g1Records(); n == 0 {
		t.Fatal("fixture: the discarded generation wrote no records to reclaim")
	}
	testDriveRetirement(t, s, asm, func() error {
		al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
		if al.status != mqttOutboundValid || len(al.rec.Life.Outstanding) != 0 {
			return fmt.Errorf("generation %q is still outstanding: %v", g1, al.status)
		}
		if n, names := g1Records(); n != 0 {
			return fmt.Errorf("%d record(s) of %q remain: %s", n, g1, strings.Join(names, ", "))
		}
		return nil
	})
	al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
	if al.rec.State != mqttAuthorityServing || al.rec.Gen != g2 || al.rec.Life.Phase != mqttLifecycleServing {
		t.Fatalf("the successor no longer serves: %+v", al.rec)
	}
	if s, _ := asm.genStanding(id, g1); s != mqttGenRetired {
		t.Fatalf("generation %q reads %v, want retired", g1, s)
	}
}

// T-frozen-create (issue #133): a creation batch frozen before its client's lifecycle retired and
// completed is refused when it is replayed afterwards: the authority and the record are gone,
// but the registry shard it CASed has moved, and no retired lifecycle is reissued. Mutation M1:
// the creation without its registry member (the replay lands a lifecycle over a retired one).
func TestAFrozenCreationIsRefusedAfterItsLifecycleRetired(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "frozen-create"
	var frozen *mqttAuthorityTransition
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.ID == id && tr.Attempt.Kind == mqttTransitionAcceptCreate && frozen == nil {
			frozen = tr
		}
		return send()
	})
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	if frozen == nil {
		t.Fatal("fixture: the CONNECT committed no creation batch")
	}
	testDriveRetirement(t, s, asm, func() error {
		if n, names := testClientRecordCount(t, asm, getHash(id)); n != 0 {
			return fmt.Errorf("%d record(s) remain: %s", n, strings.Join(names, ", "))
		}
		return nil
	})
	if _, err := asm.jsa.commitAuthorityTransition(frozen); err == nil {
		t.Fatal("a creation frozen before its lifecycle retired was replayed onto the retired client")
	}
	if al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch); al.status != mqttOutboundAbsent {
		t.Fatalf("the refused replay left an authority (%v)", al.status)
	}
}

// T-disconnect-anchor (issue #133): a normal DISCONNECT of a connection with a delay-capable
// Will removes its Will anchor, so its lifecycle retires and leaves no record. Delay-capable
// needs a session expiry, and the DISCONNECT sets it to zero so the session ends with it. Mutation M15: no
// anchor removal on DISCONNECT (the anchor pins the lifecycle, which never retires).
func TestANormalDisconnectReleasesItsWillAnchor(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "anchor-disconnect"
	will := &mqttWill{topic: []byte("will/anchor"), message: []byte("bye"), qos: 0}
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(30), props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || len(al.rec.Life.Anchors) != 1 {
		t.Fatalf("fixture: the delay-capable CONNECT holds %d anchor(s) (%v), want 1", len(al.rec.Life.Anchors), al.status)
	}
	l := al.rec.Life.L
	if _, err := c.Write(mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
		t.Fatal(err)
	}
	c.Close()
	testDriveRetirement(t, s, asm, func() error {
		reg := asm.jsa.loadLifecycleRegistryShard(asm.sessStreamEpoch, mqttLifecycleRegistryShardFor(getHash(id)))
		if reg.status != mqttOutboundValid || reg.rec.classify(getHash(id), l) != mqttLifecycleRetired {
			return fmt.Errorf("lifecycle %d of %q is not retired (%v)", l, id, reg.status)
		}
		if n, names := testClientRecordCount(t, asm, getHash(id)); n != 0 {
			return fmt.Errorf("%d record(s) remain: %s", n, strings.Join(names, ", "))
		}
		return nil
	})
}

// T-tail-paused-retirement / T-anchor (issue #133): a connection lost with a delayed Will leaves
// its anchor OWED, and the lifecycle is not retired while it stands, however often the sweep
// runs; once the Will fires and its anchor is discharged, the lifecycle retires. Mutation M4:
// the anchors ignored by lifecycle retirement (the lifecycle retires under an owed Will).
func TestAnOwedWillAnchorHoldsItsLifecycle(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "anchor-owed"
	will := &mqttWill{topic: []byte("will/owed"), message: []byte("bye"), qos: 0}
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(2), props: mqttV5ConnPropsSessionExpiry(3)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	// Delay-capable needs a session expiry: the Will fires at 2s, the session ends at 3s.
	al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || len(al.rec.Life.Anchors) != 1 {
		t.Fatalf("fixture: the delay-capable CONNECT holds %d anchor(s) (%v), want 1", len(al.rec.Life.Anchors), al.status)
	}
	l := al.rec.Life.L
	c.Close() // lost: the delayed Will is owed
	retired := func() bool {
		reg := asm.jsa.loadLifecycleRegistryShard(asm.sessStreamEpoch, mqttLifecycleRegistryShardFor(getHash(id)))
		return reg.status == mqttOutboundValid && reg.rec.classify(getHash(id), l) == mqttLifecycleRetired
	}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		asm.sweepOutboundRecovery(s, nil)
		asm.sweepLifecycles(s)
		if al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch); al.status == mqttOutboundValid && len(al.rec.Life.Anchors) > 0 && retired() {
			t.Fatal("the lifecycle retired while its Will anchor stands")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if retired() {
		t.Fatal("the lifecycle retired before its delayed Will was resolved")
	}
	testDriveRetirement(t, s, asm, func() error {
		if !retired() {
			return fmt.Errorf("lifecycle %d of %q is not retired", l, id)
		}
		return nil
	})
}
