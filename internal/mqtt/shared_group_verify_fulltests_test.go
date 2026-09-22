//go:build mqttport_fulltests

// End-to-end coverage for F-076: a resume must PROVE its shared group durable exists and
// is usable before it restores the membership into it.
//
// Every test here has to reach the RESIDENT arm, and that is the hard part. A fresh
// connect finds no cons entry and takes the join path, which creates the durable and
// therefore proves it — a fixture that connects fresh exercises none of this change and
// would pass against the defect. So each test connects, subscribes, disconnects WITHOUT a
// clean start so the session object survives in the manager, and only then reconnects.
//
// Membership is QoS 1 throughout so these required tests exercise the supported group
// durable without entering the parked affine path. QoS 0 would use the raw queue
// subscription and could pass whether the durable exists or not.
package mqtt

import (
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

// residentReconnect resumes a session and returns the CONNACK's reason code instead of
// asserting success, which is what lets a test assert a REFUSAL.
func testSharedResidentReconnect(t *testing.T, b *testSharedBroker, id string, expiry uint32) (net.Conn, *mqttReader, byte, *mqttProperties) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(expiry),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	_, reason, props := testMQTTReadConnAckV5(t, r)
	return c, r, reason, props
}

// testSharedResidentSession proves the next reconnect will take the resident arm rather
// than the join path. Asserted rather than assumed: if the session object were evicted
// between the disconnect and the reconnect, every test in this file would silently
// exercise the create path instead and pass for the wrong reason.
func testSharedResidentSession(t *testing.T, b *testSharedBroker, id, durable string) {
	t.Helper()
	sess := testMQTTGetClient(t, b.s, id).mqtt.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for sid, cc := range sess.cons {
		if cc != nil && cc.Durable == durable {
			return
		}
		_ = sid
	}
	t.Fatalf("session %q holds no resident cons entry for group durable %q, so a reconnect would "+
		"take the join path and this fixture would prove nothing", id, durable)
}

// T1. The defect itself: a group durable deleted out of band between a disconnect and a
// resume. Before F-076 the reconnect was ACCEPTED with Session Present = 1 and the client
// held a subscription on a subject nothing publishes to, forever and silently.
//
// F-076 REFUSED that resume. F-077 REPAIRS it, and this fixture was rewritten rather than
// relaxed: the assertion it used to make — CONNACK 0x83, reason "does not exist" — pinned a
// behaviour F-076's own docs/CONFORMANCE.md entry called a residual and assigned to F-077
// ("Making recovery the broker's own job is F-077"). What it asserts now is strictly MORE
// than before: the resume succeeds, the durable exists again carrying the canonical
// configuration, and the ownership CAS that licenses serving it actually ran.
//
// The two verdicts that are NOT repairable keep their refusals, and the fixtures below
// prove it: a non-canonical durable may be serving other members and is not this broker's
// to normalize, and an indeterminate reading establishes nothing at all.
func TestMQTTSharedGroupAbsentDurableRepairsTheResume(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "absent")
	ca, _ := b.member(t, "absentA", 300)
	b.bind(t, "absentA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "absentA", durable)
	ca.Close()

	if _, err := b.jsa.deleteConsumer(mqttStreamName, durable, false); err != nil {
		t.Fatalf("deleting the group durable out of band: %v", err)
	}

	c, _, reason, _ := testSharedResidentReconnect(t, b, "absentA", 300)
	defer c.Close()
	if reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("resume with an absent group durable got CONNACK reason 0x%x, want 0x%x: F-077 "+
			"recreates the durable rather than refusing the session",
			reason, mqttConnAckRCConnectionAccepted)
	}
	// The repair is the point, so the durable's EXISTENCE is the oracle — an accepted
	// CONNACK alone would also be produced by a build that skipped the verification
	// entirely, which is the F-076 defect this file exists to hold down.
	ci, lerr := b.jsa.lookupConsumer(mqttStreamName, durable)
	if lerr != nil || ci == nil {
		t.Fatalf("the resume was accepted but the group durable %q was not recreated: %v", durable, lerr)
	}
	if ci.Config == nil {
		t.Fatalf("recreated group durable %q carries no configuration", durable)
	}
	// Canonical, not merely present: a repair that provisioned a shape this broker would
	// refuse on the next resume would leave the session refusing forever.
	if ci.Config.DeliverGroup == _EMPTY_ || ci.Config.AckPolicy != AckExplicit ||
		ci.Config.MaxDeliver != -1 || ci.Config.InactiveThreshold != 0 {
		t.Fatalf("recreated group durable %q is not the canonical shape: %+v", durable, ci.Config)
	}
}

// T2 / T2b. The present arm is a READ. It must not write to a live group durable — the
// durable is shared, and a write from one member rewrites the configuration every other
// member is running under — and it must adopt what the server actually reports rather
// than the configuration this connection remembered.
func TestMQTTSharedGroupPresentDurableIsAdoptedAndNotRewritten(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "present")
	ca, _ := b.member(t, "presentA", 300)
	b.bind(t, "presentA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "presentA", durable)

	before, err := b.jsa.lookupConsumer(mqttStreamName, durable)
	if err != nil || before == nil || before.Config == nil {
		t.Fatalf("reading the group durable: info=%v err=%v", before, err)
	}
	ca.Close()

	// Another member's broker moves a legitimately variable field. This is what makes the
	// remembered config and the live config genuinely differ: with the options unchanged
	// the two are equal and the adoption assertion below would pass vacuously.
	rewritten := *before.Config
	rewritten.AckWait = before.Config.AckWait + 7*time.Second
	if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName, Config: rewritten,
	}); err != nil {
		t.Fatalf("rewriting the group durable as another member: %v", err)
	}

	c, _, reason, _ := testSharedResidentReconnect(t, b, "presentA", 300)
	defer c.Close()
	if reason != mqttReasonSuccess {
		t.Fatalf("resume with a present, canonical group durable was refused: 0x%x", reason)
	}

	after, err := b.jsa.lookupConsumer(mqttStreamName, durable)
	if err != nil || after == nil || after.Config == nil {
		t.Fatalf("re-reading the group durable: info=%v err=%v", after, err)
	}
	// The resume wrote nothing: the other member's value survives untouched.
	if after.Config.AckWait != rewritten.AckWait {
		t.Fatalf("the resume rewrote the live group durable's ack_wait to %v, want %v left alone: "+
			"a shared durable's configuration is not one member's to normalize",
			after.Config.AckWait, rewritten.AckWait)
	}
	if !after.Created.Equal(before.Created) {
		t.Fatal("the resume recreated the group durable instead of reading it")
	}
	// And the session adopted what the server reports, not what it remembered.
	sess := testMQTTGetClient(t, b.s, "presentA").mqtt.sess
	sess.mu.Lock()
	var adopted *ConsumerConfig
	for _, cc := range sess.cons {
		if cc != nil && cc.Durable == durable {
			adopted = cc
		}
	}
	sess.mu.Unlock()
	if adopted == nil {
		t.Fatal("the resume left no cons entry for the group durable")
	}
	if adopted.AckWait != rewritten.AckWait {
		t.Fatalf("the session kept its remembered ack_wait %v instead of adopting the live %v",
			adopted.AckWait, rewritten.AckWait)
	}
}

// T5 / T5a. A durable occupying the deterministic name with a configuration this broker
// would never have built is refused — and left strictly alone, because it may be serving
// other members and a shape we do not recognise is not one to normalize on their behalf.
func TestMQTTSharedGroupNonCanonicalDurableRefusesTheResume(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		break_ func(cfg *ConsumerConfig)
	}{
		{"AckNone", func(cfg *ConsumerConfig) { cfg.AckPolicy = AckNone; cfg.MaxAckPending = 0 }},
		{"MaxDeliver", func(cfg *ConsumerConfig) { cfg.MaxDeliver = 1 }},
		{"InactiveThreshold", func(cfg *ConsumerConfig) { cfg.InactiveThreshold = time.Minute }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := testSharedSetup(t, "noncanon"+strings.ToLower(tc.name))
			id := "noncanon" + tc.name
			ca, _ := b.member(t, id, 300)
			b.bind(t, id)
			durable := b.groupDurable(t)
			testSharedResidentSession(t, b, id, durable)

			live, err := b.jsa.lookupConsumer(mqttStreamName, durable)
			if err != nil || live == nil || live.Config == nil {
				t.Fatalf("reading the group durable: %v", err)
			}
			ca.Close()

			// Recreate under the same name in a shape the join path would never produce.
			if _, err := b.jsa.deleteConsumer(mqttStreamName, durable, false); err != nil {
				t.Fatalf("deleting before re-occupying: %v", err)
			}
			occupant := *live.Config
			occupant.Name = _EMPTY_
			tc.break_(&occupant)
			if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{
				Stream: mqttStreamName, Config: occupant,
			}); err != nil {
				t.Fatalf("occupying the name with a %s consumer: %v", tc.name, err)
			}
			occupied, err := b.jsa.lookupConsumer(mqttStreamName, durable)
			if err != nil || occupied == nil {
				t.Fatalf("re-reading the occupant: %v", err)
			}

			c, _, reason, props := testSharedResidentReconnect(t, b, id, 300)
			defer c.Close()
			if reason != mqttReasonImplementationSpecificError {
				t.Fatalf("resume onto a %s group durable got CONNACK 0x%x, want a refusal", tc.name, reason)
			}
			if props != nil && strings.Contains(props.reasonString, "does not exist") {
				t.Fatalf("a PRESENT but non-canonical durable was reported as absent: %q", props.reasonString)
			}

			// Left alone: neither normalized nor deleted.
			still, err := b.jsa.lookupConsumer(mqttStreamName, durable)
			if err != nil || still == nil || still.Config == nil {
				t.Fatalf("the refusal destroyed the occupying consumer: info=%v err=%v", still, err)
			}
			if !still.Created.Equal(occupied.Created) {
				t.Fatal("the refusal replaced the occupying consumer instead of leaving it")
			}
		})
	}
}

// T4. A lookup that establishes nothing is NOT an absence. It refuses too — but with a
// different diagnostic, and without touching the session's local state, because acting on
// an unproven absence is how one broker's transient error becomes a group-wide change.
func TestMQTTSharedGroupIndeterminateLookupRefusesAndKeepsState(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "indet")
	ca, _ := b.member(t, "indetA", 300)
	b.bind(t, "indetA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "indetA", durable)
	ca.Close()

	sess := testMQTTGetClient(t, b.s, "indetA").mqtt.sess
	sess.mu.Lock()
	chargeBefore := sess.sharedCharged[durable]
	tmaxBefore := sess.tmaxack
	sess.mu.Unlock()

	restore := testSetSharedGroupLookupFail(b.asm, durable)
	c, _, reason, props := testSharedResidentReconnect(t, b, "indetA", 300)
	restore()
	defer c.Close()

	if reason != mqttReasonImplementationSpecificError {
		t.Fatalf("an indeterminate group-durable lookup got CONNACK 0x%x, want a refusal", reason)
	}
	if props == nil || !strings.Contains(props.reasonString, "could not be verified") {
		t.Fatalf("an indeterminate lookup must not be reported as an absence: %q", props.reasonString)
	}

	sess.mu.Lock()
	chargeAfter, ok := sess.sharedCharged[durable]
	tmaxAfter := sess.tmaxack
	sess.mu.Unlock()
	if !ok || chargeAfter != chargeBefore || tmaxAfter != tmaxBefore {
		t.Fatalf("an indeterminate refusal discarded local state: charge %d->%d (present=%v), tmaxack %d->%d. "+
			"Nothing was established, so nothing may be given up",
			chargeBefore, chargeAfter, ok, tmaxBefore, tmaxAfter)
	}
}

// testSetSharedGroupLookupFail makes the resume's batched verification treat one durable's
// lookup as having established nothing, and returns the undo.
//
// Absence and indeterminacy are the two outcomes this verification must never confuse, and
// only absence can be staged by deleting a consumer. Without a seam for the other, the
// fixture would have to pre-seed a verdict, which tests the fixture rather than the
// classification.
func testSetSharedGroupLookupFail(as *mqttAccountSessionManager, durable string) func() {
	as.mu.Lock()
	as.testSharedGroupLookupFail = func(d string) error {
		if d == durable {
			return errSharedGroupIndeterminate
		}
		return nil
	}
	as.mu.Unlock()
	return func() {
		as.mu.Lock()
		as.testSharedGroupLookupFail = nil
		as.mu.Unlock()
	}
}

// T6. A multi-level shared filter owns TWO group durables — its own and the level-up
// companion mqttNeedSubForLevelUp derives for '#' semantics — and each is an independent
// consumer under its own sid.
//
// Verifying only the main half would leave a dead companion restored and delivering
// nothing, which is this finding in miniature. Both directions are checked, because a
// collection keyed by durable rather than by sid, or one deriving only half the pair,
// would pass one direction and fail the other.
func TestMQTTSharedGroupLevelUpCompanionIsRepairedToo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		levelUp bool
	}{
		{"the level-up companion", true},
		{"the main durable", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := testMQTTDefaultOptionsV5()
			o.MQTT.ExperimentalSharedQoS2 = false
			o.MQTT.AckWait = 400 * time.Millisecond
			s := testMQTTRunServer(t, o)
			t.Cleanup(func() { testMQTTShutdownServer(s) })

			id := "levelup-" + strings.ReplaceAll(tc.name, " ", "-")
			group, filter := "lug", "levelup/topic/>"
			c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
				clientID:   id,
				cleanStart: false,
				props:      mqttV5ConnPropsSessionExpiry(300),
			}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/" + group + "/" + filter, opts: 1}})
			if len(codes) != 1 || codes[0] != 1 {
				t.Fatalf("SUBACK %v", codes)
			}
			testMQTTFlush(t, c, nil, r)

			cl := testMQTTGetClient(t, s, id)
			jsa := cl.mqtt.sess.jsa

			subj, err := mqttTopicToNATSPubSubject([]byte(filter))
			if err != nil {
				t.Fatalf("subject conversion: %v", err)
			}
			durs := mqttSharedDurablesFor("$share/"+group+"/"+string(subj), testSharedMembership("$share/"+group+"/"+string(subj), 1))
			if len(durs) != 2 {
				t.Fatalf("a multi-level shared filter must own two durables, got %v", durs)
			}
			target := durs[0]
			if tc.levelUp {
				target = durs[1]
			}

			for _, d := range durs {
				if ci, lerr := jsa.lookupConsumer(mqttStreamName, d); lerr != nil || ci == nil {
					t.Fatalf("durable %q was never created: %v", d, lerr)
				}
			}
			c.Close()

			if _, derr := jsa.deleteConsumer(mqttStreamName, target, false); derr != nil {
				t.Fatalf("deleting %q: %v", target, derr)
			}

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
				clientID:   id,
				cleanStart: false,
				props:      mqttV5ConnPropsSessionExpiry(300),
			}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			_, reason, _ := testMQTTReadConnAckV5(t, r2)
			if reason != mqttConnAckRCConnectionAccepted {
				t.Fatalf("deleting %s left the resume REFUSED (0x%x): F-077 repairs an "+
					"authoritatively absent group durable", tc.name, reason)
			}
			// The companion is verified — and now repaired — ON ITS OWN ACCOUNT. Asserting
			// that the SPECIFIC deleted durable came back is what keeps "the other half of
			// the pair answered for it" from passing: a build that classified only the main
			// half would leave the companion absent and this lookup would fail.
			if ci, lerr := jsa.lookupConsumer(mqttStreamName, target); lerr != nil || ci == nil {
				t.Fatalf("deleting %s left it unrepaired: the other half of the pair answered "+
					"for it, so a dead consumer was restored: %v", tc.name, lerr)
			}
			// And the half that was never deleted must still be there, so the repair did not
			// disturb it.
			for _, d := range durs {
				if ci, lerr := jsa.lookupConsumer(mqttStreamName, d); lerr != nil || ci == nil {
					t.Fatalf("durable %q is missing after the repair: %v", d, lerr)
				}
			}
		})
	}
}

// The post-pass must ADOPT its second reading, not merely refuse on it.
//
// This is the case the first implementation got wrong and a holistic review caught: the
// post-pass handed its verdicts to the refusal helper, which reads only the error, so a
// successful second reading was discarded and `sess.cons` kept the pre-pass snapshot. Every
// document describing this mechanism claims the second reading is the one adopted, so the
// omission made those claims false while every existing test still passed.
//
// The window is only reachable through a seam: another member moves a legitimately variable
// field BETWEEN the two classifications, which is the post-pass's whole reason for existing.
func TestMQTTSharedGroupPostPassAdoptsTheSecondReading(t *testing.T) {
	t.Parallel()
	b := testSharedSetup(t, "postpass")
	ca, _ := b.member(t, "postpassA", 300)
	b.bind(t, "postpassA")
	durable := b.groupDurable(t)
	testSharedResidentSession(t, b, "postpassA", durable)

	first, err := b.jsa.lookupConsumer(mqttStreamName, durable)
	if err != nil || first == nil || first.Config == nil {
		t.Fatalf("reading the group durable: %v", err)
	}
	ca.Close()

	// The value the PRE-pass will observe.
	preAckWait := first.Config.AckWait + 3*time.Second
	pre := *first.Config
	pre.AckWait = preAckWait
	if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName, Config: pre}); err != nil {
		t.Fatalf("staging the pre-pass value: %v", err)
	}

	// ...and the different value the POST-pass will observe, installed in between.
	postAckWait := preAckWait + 5*time.Second
	b.asm.mu.Lock()
	b.asm.testSharedGroupBetweenPasses = func() {
		post := pre
		post.AckWait = postAckWait
		if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName, Config: post}); err != nil {
			t.Errorf("moving the durable between passes: %v", err)
		}
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testSharedGroupBetweenPasses = nil
		b.asm.mu.Unlock()
	})

	c, _, reason, _ := testSharedResidentReconnect(t, b, "postpassA", 300)
	defer c.Close()
	if reason != mqttReasonSuccess {
		t.Fatalf("a canonical durable moved only in a variable field must still resume: 0x%x", reason)
	}

	sess := testMQTTGetClient(t, b.s, "postpassA").mqtt.sess
	sess.mu.Lock()
	var adopted *ConsumerConfig
	for _, cc := range sess.cons {
		if cc != nil && cc.Durable == durable {
			adopted = cc
		}
	}
	sess.mu.Unlock()
	if adopted == nil {
		t.Fatal("no cons entry for the group durable after the resume")
	}
	if adopted.AckWait == preAckWait {
		t.Fatalf("the session kept the PRE-pass reading (ack_wait %v): the post-pass discarded its "+
			"own second reading, so the CONNECT finished holding state the account no longer agrees "+
			"with — and every document saying the second reading is adopted is false", adopted.AckWait)
	}
	if adopted.AckWait != postAckWait {
		t.Fatalf("adopted ack_wait %v, want the post-pass value %v", adopted.AckWait, postAckWait)
	}
}

// The server-normalization oracle, measured against a GENUINE R3 CLUSTER.
//
// The standalone counterpart in shared_group_verify_test.go runs against a single embedded
// JetStream. A holistic review pointed out why that is not enough, and the risk is a
// production one rather than a coverage nicety: clustered JetStream has its own
// consumer-creation and defaulting path, while sharedGroupConfigUsable is strict
// whole-struct equality. A field the cluster defaults differently — or at all — would make
// EVERY healthy resident shared-session resume refuse, fail-closed, on every clustered
// deployment, and the standalone suite would stay green throughout.
//
// So this asserts the same two things the standalone case does, against a replicated
// stream: that the set of fields the server moved is exactly the dispositioned server-set
// rows, and that each row's expected VALUE is what the cluster actually produced.
func TestMQTTSharedGroupServerNormalizationReplicated(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	const cid = "sharednorm-r3"
	mc, mr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer mc.Close()
	testMQTTReadConnAckV5(t, mr)

	cl := testMQTTGetClient(t, b.s, cid)
	jsa := cl.mqtt.sess.jsa
	c.waitStreamLeader(t, "$G", mqttStreamName)

	// FIXTURE PRECONDITION, not decoration: a clustered normalization test that silently
	// ran against a single-replica stream would prove exactly what the standalone case
	// already proves, and would go green while the cluster-only path it exists for stayed
	// unexercised. Assert the stream really is replicated before measuring anything.
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := jsa.lookupStream(mqttStreamName)
		if err != nil || si == nil {
			return fmt.Errorf("stream info: %v", err)
		}
		if si.Config.Replicas != 3 {
			return fmt.Errorf("Config.Replicas=%d, want 3: this is not a replicated stream, so the "+
				"clustered consumer-creation path is not being exercised", si.Config.Replicas)
		}
		return nil
	})

	// Replicas is deliberately NOT set in the request: the group durable inherits the
	// stream's replication. If the cluster fills it in, that is precisely the kind of
	// cluster-only normalization this test exists to catch.
	const group, subject = "rg", "rtopic"
	req := buildSharedGroupConsumerConfig(group, subject, 1, 30*time.Second, 1024, false)
	if _, err := jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName, Config: req,
	}); err != nil {
		t.Fatalf("creating the group durable on the cluster: %v", err)
	}

	_, _, durable := mqttSharedNames(group, subject, 1)
	var ci *ConsumerInfo
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		var err error
		ci, err = jsa.lookupConsumer(mqttStreamName, durable)
		if err != nil || ci == nil || ci.Config == nil {
			return fmt.Errorf("group durable not readable yet: %v", err)
		}
		return nil
	})

	live := reflect.ValueOf(*ci.Config)
	sent := reflect.ValueOf(req)
	typ := live.Type()
	var moved []string
	for i := 0; i < typ.NumField(); i++ {
		if !reflect.DeepEqual(live.Field(i).Interface(), sent.Field(i).Interface()) {
			moved = append(moved, typ.Field(i).Name)
		}
	}
	for _, name := range moved {
		rule, ok := sharedGroupConfigRules[name]
		if !ok || (rule.kind != sharedGroupServerNormalized && rule.kind != sharedGroupServerOwned) {
			t.Errorf("the CLUSTER moved ConsumerConfig.%s (request %v -> live %v), which the "+
				"standalone oracle never saw. Strict whole-struct equality means this refuses every "+
				"healthy resident resume on a clustered deployment",
				name, sent.FieldByName(name).Interface(), live.FieldByName(name).Interface())
		}
	}

	// And the expected values hold against what the cluster produced, which is the half
	// that stops a server-set row becoming a free pass.
	if err := sharedGroupMetadataIsServerOnly(ci.Config.Metadata); err != nil {
		t.Errorf("a cluster-created group durable carries non-server metadata: %v", err)
	}
	want := sharedGroupExpectedConfig(group, subject, 1, ci.Config)
	if !reflect.DeepEqual(*ci.Config, want) {
		t.Errorf("a group durable created on a genuine R3 cluster does not match its own expected "+
			"configuration, so every resident resume against a cluster would refuse.\n got: %+v\nwant: %+v",
			*ci.Config, want)
	}
}

// A refusing PRE-PASS ends the CONNECT there: no restoration, and no second pass.
//
// The fourth and final cut of the batching seam. classifySharedGroups was made to stop at
// the first terminal verdict, but its CALLER still ran processSubs unconditionally and then
// invoked a second classification — so an already-refused CONNECT went on installing
// subscriptions, entering create paths for non-resident filters, and paying another lookup.
// The stop-at-terminal invariant has to hold at the level it actually governs.
//
// Only a CONNECT-level fixture can see this: the standalone classifier tests observe one
// call to one function, and the defect is that the function is called twice by something
// else. So this counts every lookup across the whole connect path.
func TestMQTTSharedGroupRefusingPrePassSkipsRestoration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		arm     func(b *testSharedBroker, durable string)
		wantMsg string
	}{
		{
			name: "transport failure",
			arm: func(b *testSharedBroker, durable string) {
				b.asm.mu.Lock()
				b.asm.testSharedGroupLookupErr = func(int) error { return fmt.Errorf("injected") }
				b.asm.mu.Unlock()
			},
			wantMsg: "could not be verified",
		},
		{
			name: "response-level refusal",
			arm: func(b *testSharedBroker, durable string) {
				b.asm.mu.Lock()
				b.asm.testSharedGroupLookupFail = func(d string) error {
					if d == durable {
						return errSharedGroupIndeterminate
					}
					return nil
				}
				b.asm.mu.Unlock()
			},
			wantMsg: "could not be verified",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := testSharedSetup(t, "prepass"+strings.ReplaceAll(tc.name, " ", ""))
			id := "prepass-" + strings.ReplaceAll(tc.name, " ", "-")
			ca, _ := b.member(t, id, 300)
			b.bind(t, id)
			durable := b.groupDurable(t)
			testSharedResidentSession(t, b, id, durable)
			ca.Close()

			var lookups int
			b.asm.mu.Lock()
			b.asm.testSharedGroupLookupBatch = func(int) { lookups++ }
			b.asm.mu.Unlock()
			tc.arm(b, durable)
			t.Cleanup(func() {
				b.asm.mu.Lock()
				b.asm.testSharedGroupLookupBatch = nil
				b.asm.testSharedGroupLookupErr = nil
				b.asm.testSharedGroupLookupFail = nil
				b.asm.mu.Unlock()
			})

			c, _, reason, props := testSharedResidentReconnect(t, b, id, 300)
			defer c.Close()
			if reason != mqttReasonImplementationSpecificError {
				t.Fatalf("a refusing pre-pass got CONNACK 0x%x, want a refusal", reason)
			}
			if props == nil || !strings.Contains(props.reasonString, tc.wantMsg) {
				t.Fatalf("refusal reason %+v does not report %q", props, tc.wantMsg)
			}
			// THE POINT: exactly one classification pass ran. Two means the post-pass was
			// invoked on a CONNECT whose outcome was already decided.
			if lookups != 1 {
				t.Fatalf("%d lookups across the CONNECT, want exactly 1: the pre-pass had already "+
					"refused, so restoration and the post-pass were work whose result could not "+
					"change the outcome", lookups)
			}
		})
	}
}
