//go:build mqtt_cluster_reference

// Copyright 2026 The NATS Authors
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

// Upstream cluster-harness tests kept as an unbuilt reference; they require
// the nats-server clustered test harness (cluster, createJetStreamCluster*),
// which this repository does not have. See CONTRIBUTING.md, "Test suites and build tags".

package mqtt

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// testMQTTGetClusterTemplateV5 is the standard MQTT cluster config template with
// MQTT 5.0 enabled.
func testMQTTGetClusterTemplateV5(t testing.TB) string {
	t.Helper()
	tmpl := strings.Replace(testMQTTGetClusterTemplaceNoLeaf(),
		"mqtt {\n\t\tlisten: 127.0.0.1:-1\n\t}",
		"mqtt {\n\t\tlisten: 127.0.0.1:-1\n\t\tmax_protocol_version: 5\n\t}", 1)
	if !strings.Contains(tmpl, "max_protocol_version: 5") {
		t.Fatal("failed to enable MQTT v5 in the cluster template")
	}
	return tmpl
}

func TestMQTTv5SharedSubscriptionCluster(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplateV5(t), "MQTT", 2)
	defer cl.shutdown()

	o0, o1 := cl.opts[0], cl.opts[1]

	// Member A on server 0, member B on server 1, same group.
	ca, ra := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "clA", cleanStart: true}, o0.MQTT.Host, o0.MQTT.Port, 5)
	defer ca.Close()
	testMQTTReadConnAckV5(t, ra)
	testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	cb, rb := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "clB", cleanStart: true}, o1.MQTT.Host, o1.MQTT.Port, 5)
	defer cb.Close()
	testMQTTReadConnAckV5(t, rb)
	testMQTTSubV5(t, cb, rb, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})

	// Publisher on server 0.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "clpub", cleanStart: true}, o0.MQTT.Host, o0.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)

	// QoS1-published messages: exactly one delivery per message across servers.
	const n = 8
	got := map[string]int{}
	for i := 0; i < n; i++ {
		payload := []byte(fmt.Sprintf("c%d", i))
		testMQTTPublishV5(t, cp, 1, uint16(i+1), "foo", payload)
		testMQTTReadPubAck(t, rp, uint16(i+1))
		topic, pl, _, pi, ok := testMQTTTryReadPublishV5(ra, 500*time.Millisecond)
		if ok {
			testMQTTSendPIPacket(mqttPacketPubAck, t, ca, pi)
		} else if topic, pl, _, pi, ok = testMQTTTryReadPublishV5(rb, time.Second); ok {
			testMQTTSendPIPacket(mqttPacketPubAck, t, cb, pi)
		} else {
			t.Fatalf("message %d not delivered to any member in the cluster", i)
		}
		if topic != "foo" {
			t.Fatalf("Expected topic foo, got %q", topic)
		}
		got[string(pl)]++
	}
	if len(got) != n {
		t.Fatalf("Expected %d distinct messages exactly once, got %v", n, got)
	}
	testMQTTExpectNothing(t, ra)
	testMQTTExpectNothing(t, rb)

	// QoS0-published: also exactly once, via the cluster-wide raw queue group.
	testMQTTPublishV5(t, cp, 0, 0, "foo", []byte("q0"))
	_, pl0, _, _, okA := testMQTTTryReadPublishV5(ra, 500*time.Millisecond)
	if !okA {
		if _, pl0, _, _, okA = testMQTTTryReadPublishV5(rb, time.Second); !okA {
			t.Fatal("QoS0 message not delivered to any member")
		}
	}
	if string(pl0) != "q0" {
		t.Fatalf("Expected q0, got %q", pl0)
	}
	testMQTTExpectNothing(t, ra)
	testMQTTExpectNothing(t, rb)

	// Cross-server last-holder: A (server 0) leaves; the durable survives via B
	// (server 1). Then B leaves and the durable is deleted cluster-wide.
	_, _, durable := mqttSharedNames("g", "foo", 1)
	consumerExists := func() bool {
		for _, s := range cl.servers {
			if mset, err := s.GlobalAccount().lookupStream(mqttStreamName); err == nil && mset != nil {
				if mset.lookupConsumer(durable) != nil {
					return true
				}
			}
		}
		return false
	}
	testMQTTUnsubV5(t, ca, ra, 1, []string{"$share/g/foo"})
	if !consumerExists() {
		t.Fatal("shared durable must survive while a member on another server holds it")
	}
	testMQTTPublishV5(t, cp, 1, 9, "foo", []byte("last"))
	testMQTTReadPubAck(t, rp, 9)
	topic, pl, _, pi, ok := testMQTTTryReadPublishV5(rb, 2*time.Second)
	if !ok || topic != "foo" || string(pl) != "last" {
		t.Fatalf("expected B (other server) to receive; ok=%v topic=%q", ok, topic)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, cb, pi)

	testMQTTUnsubV5(t, cb, rb, 1, []string{"$share/g/foo"})
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if consumerExists() {
			return fmt.Errorf("shared durable still present after last member left")
		}
		return nil
	})
}

func TestMQTTv5WillDelayCluster(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplateV5(t), "MQTT", 2)
	defer cl.shutdown()

	srvA, optsA := cl.servers[0], cl.opts[0]
	optsB := cl.opts[1]

	t.Run("fires and is delivered across servers", func(t *testing.T) {
		// Subscriber on server B.
		cs, rs := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "wdcsub1", cleanStart: true},
			optsB.MQTT.Host, optsB.MQTT.Port, 5)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/topic", opts: 1}})

		// Will client on server A, QoS1 (JetStream-backed, robust across the
		// cluster), short delay.
		will := &mqttWill{topic: []byte("will/topic"), message: []byte("bye"), qos: 1}
		cw, rw := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "wdcwill1", will: will,
			willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(30)},
			optsA.MQTT.Host, optsA.MQTT.Port, 5)
		testMQTTReadConnAckV5(t, rw)
		cw.Close()

		// After the delay, the will fires on A and is delivered to the subscriber on B.
		testMQTTReadPublishV5(t, rs, "will/topic", []byte("bye"))
	})

	t.Run("reconnect to another server cancels the will", func(t *testing.T) {
		// Prober/subscriber on server A so we can inspect server A's pending wills.
		cprobe, rprobe := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "wdcprobe", cleanStart: true},
			optsA.MQTT.Host, optsA.MQTT.Port, 5)
		defer cprobe.Close()
		testMQTTReadConnAckV5(t, rprobe)
		testMQTTSubV5(t, cprobe, rprobe, 1, []mqttV5SubFilter{{topic: "will/topic2", opts: 0}})

		// Will client on server A with a long delay so the timer cannot fire during
		// the test: any removal from server A's pending wills must be a cancellation.
		will := &mqttWill{topic: []byte("will/topic2"), message: []byte("bye"), qos: 0}
		cw, rw := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "wdcwill2", will: will,
			willProps: mqttV5WillDelayProps(30), props: mqttV5ConnPropsSessionExpiry(60)},
			optsA.MQTT.Host, optsA.MQTT.Port, 5)
		testMQTTReadConnAckV5(t, rw)
		cw.Close()

		// Scheduled on server A.
		checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
			if n := testMQTTNumPendingWills(t, srvA, "wdcprobe"); n != 1 {
				return fmt.Errorf("expected 1 pending will on server A, got %d", n)
			}
			return nil
		})

		// Reconnect the same client ID on server B before the 30s delay: a new
		// connection to the session, so server A must cancel its pending will via
		// the session-persist callback.
		cw2, rw2 := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "wdcwill2", cleanStart: true},
			optsB.MQTT.Host, optsB.MQTT.Port, 5)
		defer cw2.Close()
		testMQTTReadConnAckV5(t, rw2)

		// Server A cancels its pending will well before the 30s delay could elapse,
		// so the removal is a cancellation (not a firing).
		checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
			if n := testMQTTNumPendingWills(t, srvA, "wdcprobe"); n != 0 {
				return fmt.Errorf("expected server A pending will to be cancelled, got %d", n)
			}
			return nil
		})
		testMQTTExpectNothing(t, rprobe)
	})
}
func TestMQTTv5MaxProtocolVersionConfigParse(t *testing.T) {
	for _, test := range []struct {
		name    string
		val     string
		want    byte
		wantErr bool
	}{
		{"cap to 3.1.1", "4", 4, false},
		{"enable v5", "5", 5, false},
		{"default (MQTT 5.0)", "0", 0, false},
		{"invalid", "3", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(`
				listen: "127.0.0.1:-1"
				jetstream: enabled
				mqtt {
					listen: "127.0.0.1:-1"
					max_protocol_version: %s
				}
			`, test.val)))
			o, err := ProcessConfigFile(conf)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected a config error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected config error: %v", err)
			}
			if o.MQTT.MaxProtocolVersion != test.want {
				t.Fatalf("Expected MaxProtocolVersion=%d, got %d", test.want, o.MQTT.MaxProtocolVersion)
			}
		})
	}
}

func TestMQTTv5WillDelayPermViolation(t *testing.T) {
	template := `
		port: -1
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		authorization {
			mqtt_perms = {
				publish = ["will.allowed"]
				subscribe = ["will.allowed", "will.denied"]
			}
			users = [
				{user: mqtt, password: pass, permissions: $mqtt_perms}
				{user: admin, password: pass}
			]
		}
		mqtt {
			port: -1
			max_protocol_version: 5
		}
	`
	tdir := t.TempDir()
	conf := createConfFile(t, fmt.Appendf(nil, template, tdir))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	// Subscriber allowed to receive both will topics.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wpsub", cleanStart: true, user: "mqtt", pass: "pass"},
		o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/allowed", opts: 0}, {topic: "will/denied", opts: 0}})

	// A delayed Will on a DENIED topic must be dropped when the timer fires.
	willDenied := &mqttWill{topic: []byte("will/denied"), message: []byte("nope"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wpdenied", user: "mqtt", pass: "pass",
		will: willDenied, willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()
	// Scheduled...
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if n := testMQTTNumPendingWills(t, s, "wpsub"); n != 1 {
			return fmt.Errorf("expected 1 pending will, got %d", n)
		}
		return nil
	})
	// A denied Will must never be persisted with the session record: a restart
	// cannot re-check permissions, so a restored Will would bypass them. The
	// (would-be) record write races this check, so absence must hold for a
	// settle period, not just at one instant.
	nc, js := jsClientConnect(t, s, nats.UserInfo("admin", "pass"))
	sawRecord := false
	for deadline := time.Now().Add(750 * time.Millisecond); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		m, err := js.GetLastMsg(mqttSessStreamName, mqttSessStreamSubjectPrefix+getHash("wpdenied"))
		if err != nil {
			continue
		}
		sawRecord = true
		if bytes.Contains(m.Data, []byte(`"will"`)) {
			t.Fatal("denied will was persisted with the session record")
		}
	}
	if !sawRecord {
		t.Fatal("could not read the session record to verify the will was not persisted")
	}
	nc.Close()
	// ...then fired (removed from pending) and suppressed by publish permissions.
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testMQTTNumPendingWills(t, s, "wpsub"); n != 0 {
			return fmt.Errorf("expected pending will to have fired, got %d", n)
		}
		return nil
	})
	testMQTTExpectNothing(t, rs)

	// A delayed Will on an ALLOWED topic is delivered normally.
	willOK := &mqttWill{topic: []byte("will/allowed"), message: []byte("ok"), qos: 0}
	cw2, rw2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wpok", user: "mqtt", pass: "pass",
		will: willOK, willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw2)
	cw2.Close()
	testMQTTReadPublishV5(t, rs, "will/allowed", []byte("ok"))
}

// The following tests use Account.lookupStream / Account.AddMapping /
// stream.lookupConsumer / stream.addConsumer, upstream's UNEXPORTED internal
// JetStream stream API (package server). Even though testMQTTRunServer pairs
// each shim Server with a real embedded nats-server, these methods are
// unexported in the server package and therefore inaccessible from this
// package regardless of how the value is obtained — there is no thin mapping
// possible, only the public nats.go JetStream client (which the shim's own
// production code uses). See CONTRIBUTING.md.
func TestMQTTv5SharedSubscriptionLastHolder(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	_, _, durable := mqttSharedNames("g", "foo", 1)
	consumerExists := func() bool {
		mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
		if err != nil {
			t.Fatalf("lookup stream: %v", err)
		}
		return mset.lookupConsumer(durable) != nil
	}

	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "lhA", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer ca.Close()
	testMQTTReadConnAckV5(t, ra)
	testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "lhB", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cb.Close()
	testMQTTReadConnAckV5(t, rb)
	testMQTTSubV5(t, cb, rb, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})

	if !consumerExists() {
		t.Fatal("expected shared consumer to exist after subscribe")
	}

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "lhpub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)

	// A unsubscribes; the group (and its durable) is still alive via B.
	testMQTTUnsubV5(t, ca, ra, 1, []string{"$share/g/foo"})
	if !consumerExists() {
		t.Fatal("shared consumer must survive while B still holds it")
	}
	testMQTTPublishV5(t, cp, 1, 1, "foo", []byte("one"))
	testMQTTReadPubAck(t, rp, 1)
	topic, pl, _, pi, ok := testMQTTTryReadPublishV5(rb, time.Second)
	if !ok || topic != "foo" || string(pl) != "one" {
		t.Fatalf("expected B to still receive; got ok=%v topic=%q", ok, topic)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, cb, pi)
	testMQTTExpectNothing(t, ra)

	// B unsubscribes; the last holder is gone, so the shared durable is deleted.
	testMQTTUnsubV5(t, cb, rb, 1, []string{"$share/g/foo"})
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if consumerExists() {
			return fmt.Errorf("shared consumer still present after last holder left")
		}
		return nil
	})
}

func TestMQTTv5SharedSubscriptionSessionEndRelease(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	_, _, durable := mqttSharedNames("g", "foo", 1)
	consumerExists := func() bool {
		mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
		if err != nil {
			t.Fatalf("lookup stream: %v", err)
		}
		return mset.lookupConsumer(durable) != nil
	}

	ci := &mqttV5ConnInfo{clientID: "seShare", props: mqttV5ConnPropsSessionExpiry(300)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	if !consumerExists() {
		t.Fatal("expected shared consumer after subscribe")
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()
	// The session persists (expiry 300), so the durable must remain.
	if !consumerExists() {
		t.Fatal("shared consumer must persist across a disconnect with a live session")
	}

	// Reconnect with clean start: the old session is discarded (clear()), which
	// must release the shared durable as the last holder.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "seShare", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if consumerExists() {
			return fmt.Errorf("shared consumer still present after session end")
		}
		return nil
	})
}

func TestMQTTv5SharedSubscriptionUnsubBatch(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	consumerExists := func(durable string) bool {
		t.Helper()
		mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
		if err != nil {
			t.Fatalf("lookup stream: %v", err)
		}
		return mset.lookupConsumer(durable) != nil
	}

	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ubA", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer ca.Close()
	testMQTTReadConnAckV5(t, ra)
	testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{
		{topic: "$share/g1/foo", opts: 1},
		{topic: "$share/g2/baz", opts: 1},
		{topic: "$share/g3/foo/#", opts: 1},
	})
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ubB", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cb.Close()
	testMQTTReadConnAckV5(t, rb)
	testMQTTSubV5(t, cb, rb, 1, []mqttV5SubFilter{{topic: "$share/g1/foo", opts: 1}})

	_, _, durG1 := mqttSharedNames("g1", "foo", 1)
	_, _, durG2 := mqttSharedNames("g2", "baz", 1)
	dursG3 := mqttSharedDurablesFor("$share/g3/foo.>", testSharedMembership("$share/g3/foo.>", 1))
	if len(dursG3) != 2 {
		t.Fatalf("expected 2 durables for the '#' filter, got %v", dursG3)
	}
	for _, dur := range append([]string{durG1, durG2}, dursG3...) {
		if !consumerExists(dur) {
			t.Fatalf("expected durable %q after subscribe", dur)
		}
	}

	// A leaves all three groups in one packet.
	codes := testMQTTUnsubV5(t, ca, ra, 1, []string{"$share/g1/foo", "$share/g2/baz", "$share/g3/foo/#"})
	if len(codes) != 3 || codes[0] != 0 || codes[1] != 0 || codes[2] != 0 {
		t.Fatalf("Expected UNSUBACK [0 0 0], got %v", codes)
	}
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		for _, dur := range append([]string{durG2}, dursG3...) {
			if consumerExists(dur) {
				return fmt.Errorf("durable %q still present after last holder left", dur)
			}
		}
		return nil
	})
	if !consumerExists(durG1) {
		t.Fatal("g1 durable must survive while B still holds it")
	}
}

func TestMQTTv5SharedSubscriptionFailedJoinRollback(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	defer setMQTTTestSessSaveHook(nil)()

	_, _, durable := mqttSharedNames("g", "foo", 1)
	consumerExists := func() bool {
		mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
		if err != nil {
			t.Fatalf("lookup stream: %v", err)
		}
		return mset.lookupConsumer(durable) != nil
	}

	// Member A joins successfully.
	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fjA", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer ca.Close()
	testMQTTReadConnAckV5(t, ra)
	testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	if !consumerExists() {
		t.Fatal("expected shared consumer after A's join")
	}

	// Member B's early membership persist fails: per-filter SUBACK failure.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fjB", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cb.Close()
	testMQTTReadConnAckV5(t, rb)
	setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess.id == "fjB" {
			return fmt.Errorf("injected save failure")
		}
		return nil
	})
	codes := testMQTTSubV5(t, cb, rb, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	setMQTTTestSessSaveHook(nil)
	if len(codes) != 1 || codes[0] != mqttSubAckFailure {
		t.Fatalf("Expected SUBACK [0x%x], got %v", mqttSubAckFailure, codes)
	}

	// A's group is unaffected: the durable survives and A still receives.
	if !consumerExists() {
		t.Fatal("existing group must survive another member's failed join")
	}
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fjpub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, "foo", []byte("alive"))
	testMQTTReadPubAck(t, rp, 1)
	topic, pl, _, pi, ok := testMQTTTryReadPublishV5(ra, time.Second)
	if !ok || topic != "foo" || string(pl) != "alive" {
		t.Fatalf("expected A to receive; ok=%v topic=%q", ok, topic)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, ca, pi)
	// B, whose join failed, receives nothing.
	testMQTTExpectNothing(t, rb)

	// A leaves: last holder, durable deleted (B never became a member).
	testMQTTUnsubV5(t, ca, ra, 1, []string{"$share/g/foo"})
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if consumerExists() {
			return fmt.Errorf("durable still present after last real holder left")
		}
		return nil
	})
}

func TestMQTTv5SubscribeFinalSaveRollback(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	defer setMQTTTestSessSaveHook(nil)()

	// Count consumers on the messages stream whose filter matches the given NATS
	// subject (subject-scoped so subtests do not interfere).
	consumersFor := func(subject string) int {
		mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
		if err != nil {
			t.Fatalf("lookup stream: %v", err)
		}
		var n int
		mset.mu.RLock()
		for _, cons := range mset.consumers {
			cons.mu.RLock()
			fs := cons.cfg.FilterSubject
			cons.mu.RUnlock()
			if fs == mqttStreamSubjectPrefix+subject {
				n++
			}
		}
		mset.mu.RUnlock()
		return n
	}
	_, _, sharedDur := mqttSharedNames("g", "foo", 1)
	sharedExists := func() bool {
		mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
		if err != nil {
			t.Fatalf("lookup stream: %v", err)
		}
		return mset.lookupConsumer(sharedDur) != nil
	}

	// Fail exactly the n-th save of the given session (later saves — including
	// the rollback's own confirm-save — succeed).
	failNthSave := func(id string, n int) {
		var count int
		setMQTTTestSessSaveHook(func(sess *mqttSession) error {
			if sess.id != id {
				return nil
			}
			count++
			if count == n {
				return fmt.Errorf("injected save failure")
			}
			return nil
		})
	}

	t.Run("mixed shared and plain", func(t *testing.T) {
		ci := &mqttV5ConnInfo{clientID: "fsr1", props: mqttV5ConnPropsSessionExpiry(300)}
		c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		// Save #1 = early shared join; save #2 = final packet update.
		failNthSave("fsr1", 2)
		vh := newMQTTWriter(0)
		vh.WriteUint16(1)
		vh.WriteVarInt(0)
		vh.WriteBytes([]byte("$share/g/foo"))
		vh.WriteByte(1)
		vh.WriteBytes([]byte("plain"))
		vh.WriteByte(1)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
			t.Fatalf("Error writing SUBSCRIBE: %v", err)
		}
		// No SUBACK: the connection closes.
		var buf [16]byte
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if n, err := c.Read(buf[:]); err == nil && n > 0 && buf[0]&mqttPacketMask == mqttPacketSubAck {
			t.Fatalf("Expected no SUBACK, got one")
		}
		c.Close()
		setMQTTTestSessSaveHook(nil)

		// Resume: neither filter is present, and no consumers remain.
		c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTReadConnAckV5(t, r2)
		cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fsrpub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cp.Close()
		testMQTTReadConnAckV5(t, rp)
		testMQTTPublishV5(t, cp, 0, 0, "foo", []byte("x"))
		testMQTTPublishV5(t, cp, 0, 0, "plain", []byte("y"))
		testMQTTExpectNothing(t, r2)
		checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
			if sharedExists() {
				return fmt.Errorf("shared durable must not survive the failed packet")
			}
			if n := consumersFor("plain"); n != 0 {
				return fmt.Errorf("expected no plain consumers, got %d", n)
			}
			return nil
		})
		testMQTTDisconnect(t, c2, nil)
	})

	t.Run("QoS upgrade rolls back", func(t *testing.T) {
		ci := &mqttV5ConnInfo{clientID: "fsr2", props: mqttV5ConnPropsSessionExpiry(300)}
		c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "up/x", opts: 0}})
		if n := consumersFor("up.x"); n != 0 {
			t.Fatalf("QoS0 sub must have no consumer, got %d", n)
		}
		// Fail the (only) save of the upgrade packet.
		failNthSave("fsr2", 1)
		vh := newMQTTWriter(0)
		vh.WriteUint16(2)
		vh.WriteVarInt(0)
		vh.WriteBytes([]byte("up/x"))
		vh.WriteByte(1)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
			t.Fatalf("Error writing SUBSCRIBE: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		c.Close()
		setMQTTTestSessSaveHook(nil)
		// The upgrade's consumer must have been torn down.
		checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
			if n := consumersFor("up.x"); n != 0 {
				return fmt.Errorf("expected no consumer on up.x after rollback, got %d", n)
			}
			return nil
		})
		// Resume: subscription is still there at QoS0.
		c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
			t.Fatal("expected session present")
		}
		cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fsrpub2", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cp.Close()
		testMQTTReadConnAckV5(t, rp)
		testMQTTPublishV5(t, cp, 1, 1, "up/x", []byte("z"))
		testMQTTReadPubAck(t, rp, 1)
		if qos, _ := testMQTTReadPublishV5(t, r2, "up/x", []byte("z")); qos != 0 {
			t.Fatalf("expected delivery at rolled-back QoS0, got QoS%d", qos)
		}
		// The persisted record must not have carried the failed upgrade's consumer:
		// no consumer may exist (or have been recreated) after the resume.
		if n := consumersFor("up.x"); n != 0 {
			t.Fatalf("expected no consumer on up.x after resume, got %d", n)
		}
		testMQTTDisconnect(t, c2, nil)
	})

	t.Run("QoS downgrade keeps durable", func(t *testing.T) {
		ci := &mqttV5ConnInfo{clientID: "fsr3", props: mqttV5ConnPropsSessionExpiry(300)}
		c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "down/x", opts: 1}})
		if n := consumersFor("down.x"); n != 1 {
			t.Fatalf("expected 1 consumer on down.x, got %d", n)
		}
		failNthSave("fsr3", 1)
		vh := newMQTTWriter(0)
		vh.WriteUint16(2)
		vh.WriteVarInt(0)
		vh.WriteBytes([]byte("down/x"))
		vh.WriteByte(0)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
			t.Fatalf("Error writing SUBSCRIBE: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		c.Close()
		setMQTTTestSessSaveHook(nil)
		// The pre-existing durable must have survived the failed downgrade.
		if n := consumersFor("down.x"); n != 1 {
			t.Fatalf("expected the pre-existing consumer to survive, got %d", n)
		}
		// Resume: QoS1 delivery still works.
		c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
			t.Fatal("expected session present")
		}
		cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fsrpub3", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cp.Close()
		testMQTTReadConnAckV5(t, rp)
		testMQTTPublishV5(t, cp, 1, 1, "down/x", []byte("w"))
		testMQTTReadPubAck(t, rp, 1)
		topic, pl, qos, pi, ok := testMQTTTryReadPublishV5(r2, 2*time.Second)
		if !ok || topic != "down/x" || string(pl) != "w" || qos != 1 {
			t.Fatalf("expected QoS1 delivery after failed downgrade; ok=%v topic=%q qos=%d", ok, topic, qos)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
		// The persisted record must have kept the pre-existing consumer config:
		// exactly one consumer after the resume (no orphan + fresh duplicate).
		if n := consumersFor("down.x"); n != 1 {
			t.Fatalf("expected exactly 1 consumer on down.x after resume, got %d", n)
		}
		testMQTTDisconnect(t, c2, nil)
	})
}

func TestMQTTv5SharedSubscriptionStreamConsumerLimits(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// First connect initializes the account's MQTT streams.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "climshare", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	// Impose a consumer inactivity limit on the live messages stream.
	mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
	if err != nil {
		t.Fatalf("lookup stream: %v", err)
	}
	cfg := mset.config()
	cfg.ConsumerLimits.InactiveThreshold = 5 * time.Minute
	if err := mset.update(&cfg); err != nil {
		t.Fatalf("stream update: %v", err)
	}

	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	if len(codes) != 1 || codes[0] != mqttSubAckFailure {
		t.Fatalf("Expected SUBACK [0x%x], got %v", mqttSubAckFailure, codes)
	}
	_, _, durable := mqttSharedNames("g", "foo", 1)
	if mset.lookupConsumer(durable) != nil {
		t.Fatal("no shared durable must have been created")
	}
	// A plain QoS1 subscription is unaffected by the shared guard.
	codes = testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{{topic: "foo", opts: 1}})
	if len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("Expected plain SUBACK [1], got %v", codes)
	}
}

func TestMQTTv5NoLocalWithSubjectMapping(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Mapping is on NATS subjects; use single-level topics so the MQTT topic and
	// NATS subject coincide (nlfoo -> nlbar).
	if err := s.GlobalAccount().AddMapping("nlfoo", "nlbar"); err != nil {
		t.Fatalf("AddMapping: %v", err)
	}

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "nlmap", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	// Subscribe to the mapped destination with No Local, publish to the source
	// (rewritten to the destination). The client must not receive its own message.
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "nlbar", opts: 0x01 | 0x04}})
	testMQTTPubV5Props(t, c, r, 1, false, 1, "nlfoo", []byte("mapped"), nil)
	testMQTTExpectNothing(t, r)

	// A different connection publishing to the source still reaches it.
	other, ro := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "nlmap-other", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer other.Close()
	testMQTTReadConnAckV5(t, ro)
	testMQTTPubV5Props(t, other, ro, 1, false, 1, "nlfoo", []byte("external"), nil)
	testMQTTReadPubV5Props(t, c, r, "nlbar", []byte("external"))
}
