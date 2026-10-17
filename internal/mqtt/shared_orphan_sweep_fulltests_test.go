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

//go:build mqttport_fulltests

package mqtt

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestMQTTSharedSubscriptionOrphanSweepRetiresUnderTheGroupRecord is the orphaned-consumer
// sweep over $MQTT_msgs end to end: its grace window, the live consumers it spares, the
// consumer that is not MQTT's, the pass a restart runs, and the exact set it leaves.
//
// It ports TestMQTTv5SharedSubscriptionOrphanSweep, which no target ran (its harness was the
// unported cluster one), onto the rules the sweep now enforces. Nothing is deleted for being
// unreferenced any more; each class is deleted only under the record that owns its name:
//
//   - a SHARED group consumer under its GROUP RECORD: an incarnation the record allocated that
//     is neither live nor retiring is DERIVED retired, so a consumer bearing its name is a
//     delayed CREATE after the group retired, and is deleted. A consumer whose group record
//     is absent is retained — absence attributes nothing.
//   - a PLAIN session durable under its NAME OWNERSHIP: a RETIRED name is spent, so a consumer
//     under it is an orphan, and is deleted. A name no ownership record claims is retained.
//
// The deletable orphans are produced by the production routes — the last holder's UNSUBSCRIBE
// retires the group incarnation, a plain UNSUBSCRIBE retires the source consumer through the
// retirement owner — and then resurrected by a CREATE, the population both rules exist for.
//
// NOT parallel: the restart arm sets mqttOrphanSweepGrace, which the restarted broker's
// startup sweep reads.
func TestMQTTSharedSubscriptionOrphanSweepRetiresUnderTheGroupRecord(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	// A live member: its shared durable and plain QoS 1 durable are referenced and their
	// names live. The session expiry keeps its record (and durables) across the restart.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "sweepLive", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1},
		{topic: "bar", opts: 1}}); len(codes) != 2 || codes[0] != 1 || codes[1] != 1 {
		t.Fatalf("SUBACK for the live member: %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	asm := testMQTTGetAccountSessionManager(t, s, "sweepLive")
	liveShared := testSharedLiveDurable(t, asm, "g", "foo")
	livePlain, _ := testDowngradeDurable(t, testMQTTResidentSession(t, s, "sweepLive"), "bar")

	// The departed member: it joins group "gone" and holds a plain QoS 1 subscription, then
	// leaves both. Its UNSUBSCRIBE is the last holder's, so the group incarnation retires, and
	// the plain consumer is retired through the retirement owner.
	gc, gr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "sweepGone", cleanStart: true},
		o.MQTT.Host, o.MQTT.Port)
	defer gc.Close()
	testMQTTReadConnAckV5(t, gr)
	if codes := testMQTTSubV5(t, gc, gr, 1, []mqttV5SubFilter{{topic: "$share/gone/foo", opts: 1},
		{topic: "baz", opts: 1}}); len(codes) != 2 || codes[0] != 1 || codes[1] != 1 {
		t.Fatalf("SUBACK for the departing member: %v", codes)
	}
	testMQTTFlush(t, gc, nil, gr)
	retiredShared := testSharedLiveDurable(t, asm, "gone", "foo")
	retiredPlain, _ := testDowngradeDurable(t, testMQTTResidentSession(t, s, "sweepGone"), "baz")
	if codes := testMQTTUnsubV5(t, gc, gr, 2, []string{"$share/gone/foo", "baz"}); len(codes) != 2 ||
		codes[0] != 0 || codes[1] != 0 {
		t.Fatalf("UNSUBACK for the departing member: %v", codes)
	}
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if ci, _ := asm.jsa.lookupConsumer(mqttStreamName, retiredShared); ci != nil {
			return fmt.Errorf("the retired group consumer %q still exists", retiredShared)
		}
		own, _, _ := asm.jsa.loadNameOwnershipPhysical(asm.sessStreamEpoch, mqttOwnedStreamMsgs, retiredPlain)
		if own == nil || own.State != mqttNameOwnRetired {
			return fmt.Errorf("the plain consumer name %q is not RETIRED yet: %+v", retiredPlain, own)
		}
		if ci, _ := asm.jsa.lookupConsumer(mqttStreamName, retiredPlain); ci != nil {
			return fmt.Errorf("the retired plain consumer %q still exists", retiredPlain)
		}
		return nil
	})

	// The orphans, as delayed CREATEs and crashes leave them: the two retired names
	// resurrected, a shared-shaped consumer whose group record does not exist, a plain one no
	// ownership record claims — plus a consumer that is not MQTT's, which the sweep must never
	// touch.
	_, _, absentShared := mqttSharedNames("absent", "foo", 1)
	const unownedPlain, notMQTT = "deadbeef_orphan", "not_mqtt"
	resurrect := func() {
		t.Helper()
		testCreateSharedShapedConsumer(t, asm, "gone", "foo", retiredShared)
		testCreatePlainShapedConsumer(t, asm, retiredPlain, "baz")
	}
	resurrect()
	testCreateSharedShapedConsumer(t, asm, "absent", "foo", absentShared)
	testCreatePlainShapedConsumer(t, asm, unownedPlain, "foo")
	if _, err := asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: notMQTT, AckPolicy: AckExplicit,
			FilterSubject: mqttStreamSubjectPrefix + "foo"}}); err != nil {
		t.Fatalf("add non-MQTT consumer: %v", err)
	}
	// Each fixture reached the classification it stands for.
	if disp, _, _ := asm.classifySharedConsumer(retiredShared); disp != mqttSharedConsumerRetired {
		t.Fatalf("fixture: the resurrected group consumer %q classifies %d, want derived retired", retiredShared, disp)
	}
	if disp, _, _ := asm.classifySharedConsumer(absentShared); disp != mqttSharedConsumerRetain {
		t.Fatalf("fixture: the absent group's consumer %q classifies %d, want retained", absentShared, disp)
	}
	if own, _, oerr := asm.jsa.loadNameOwnershipPhysical(asm.sessStreamEpoch, mqttOwnedStreamMsgs,
		unownedPlain); oerr != nil || own != nil {
		t.Fatalf("fixture: %q must have no ownership record: %+v (%v)", unownedPlain, own, oerr)
	}

	survivors := []string{liveShared, livePlain, notMQTT, absentShared, unownedPlain}
	requireExactly := func(what string, want []string) {
		t.Helper()
		checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
			got := testMsgsConsumerNames(t, asm)
			w := append([]string(nil), want...)
			sort.Strings(w)
			if strings.Join(got, ",") != strings.Join(w, ",") {
				return fmt.Errorf("%s: expected exactly %d consumers %v, got %d %v", what, len(w), w, len(got), got)
			}
			return nil
		})
	}

	// Within the grace window nothing may be deleted, not even an orphan under a spent name.
	asm.sweepOrphanedConsumers(s)
	requireExactly("within the grace window", append([]string{retiredShared, retiredPlain}, survivors...))

	// Grace elapsed: exactly the two orphans under a record that retired their names go.
	asm.sweepOrphanedConsumersBefore(s, time.Now().Add(time.Minute))
	requireExactly("after the grace window", survivors)

	// A restart sweeps at startup: resurrect again, restart, and the restarted broker's own
	// pass reaps them.
	resurrect()
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	c.Close()
	gc.Close()
	s.Shutdown()
	// Restored in a Cleanup, which runs after every defer — so after the restarted broker's
	// shutdown, when no sweep goroutine can still read it.
	oldGrace := mqttOrphanSweepGrace
	t.Cleanup(func() { mqttOrphanSweepGrace = oldGrace })
	mqttOrphanSweepGrace = 0
	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "sweepLive",
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	asm = testMQTTGetAccountSessionManager(t, s, "sweepLive")
	requireExactly("after the restart sweep", survivors)
}

// testSharedLiveDurable names the consumer of the incarnation a group record currently serves,
// read through the production decoder: the name the broker mounted, not one derived here.
func testSharedLiveDurable(t *testing.T, asm *mqttAccountSessionManager, group, subject string) string {
	t.Helper()
	g := asm.jsa.loadSharedGroup(asm.sessStreamEpoch, mqttSharedGroupKey(group, subject))
	if g.status != mqttOutboundValid || g.rec == nil || g.rec.Live == nil {
		t.Fatalf("group %q on %q serves no live incarnation: %+v (status %v)", group, subject, g.rec, g.status)
	}
	_, _, durable := mqttSharedNames(group, subject, g.rec.Live.Inc)
	if ci, _ := asm.jsa.lookupConsumer(mqttStreamName, durable); ci == nil {
		t.Fatalf("the live incarnation's consumer %q of group %q does not exist", durable, group)
	}
	return durable
}

// testCreateSharedShapedConsumer creates a consumer under a shared group durable name with the
// configuration the join path builds for that name.
func testCreateSharedShapedConsumer(t *testing.T, asm *mqttAccountSessionManager, group, subject, durable string) {
	t.Helper()
	tail := strings.TrimPrefix(durable, mqttSharedConsumerDurablePrefix)
	if _, err := asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: durable, DeliverSubject: mqttSharedSubDeliverPrefix + tail,
			DeliverGroup: mqttSharedQueueName(group, subject), AckPolicy: AckExplicit,
			FilterSubject: mqttStreamSubjectPrefix + subject}}); err != nil {
		t.Fatalf("creating shared-shaped consumer %q: %v", durable, err)
	}
}

// testCreatePlainShapedConsumer creates a plain session durable, delivering to "$MQTT.sub.".
func testCreatePlainShapedConsumer(t *testing.T, asm *mqttAccountSessionManager, durable, subject string) {
	t.Helper()
	if _, err := asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: durable, DeliverSubject: mqttSubPrefix + durable,
			AckPolicy: AckExplicit, FilterSubject: mqttStreamSubjectPrefix + subject}}); err != nil {
		t.Fatalf("creating plain-shaped consumer %q: %v", durable, err)
	}
}

// testMsgsConsumerNames lists every consumer on $MQTT_msgs, sorted.
func testMsgsConsumerNames(t *testing.T, asm *mqttAccountSessionManager) []string {
	t.Helper()
	cons, err := asm.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("listing the consumers of %s: %v", mqttStreamName, err)
	}
	names := make([]string, 0, len(cons))
	for _, ci := range cons {
		if ci != nil {
			names = append(names, ci.Name)
		}
	}
	sort.Strings(names)
	return names
}
