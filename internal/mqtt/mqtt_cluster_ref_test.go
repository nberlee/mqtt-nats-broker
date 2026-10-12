//go:build mqttport_clustertests

// Copyright 2020-2026 The NATS Authors
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
// which is not ported. See docs/PORTING.md.

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

func testMQTTGetClusterTemplaceNoLeaf() string {
	return strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", "", 1)
}

var jsClusterTemplWithLeafAndMQTT = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	mqtt {
		listen: 127.0.0.1:-1
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

func TestMQTTCluster(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	for _, topTest := range []struct {
		name    string
		restart bool
	}{
		{"first_start", true},
		{"restart", false},
	} {
		t.Run(topTest.name, func(t *testing.T) {
			for _, test := range []struct {
				name   string
				subQos byte
			}{
				{"qos_0", 0},
				{"qos_1", 1},
			} {
				t.Run(test.name, func(t *testing.T) {
					clientID := nuid.Next()

					o := cl.opts[0]
					mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
					defer mc.Close()
					testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

					testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: test.subQos}}, []byte{test.subQos})
					testMQTTFlush(t, mc, nil, r)

					check := func(mc net.Conn, r *mqttReader, o *Options, s *Server) {
						t.Helper()

						nc := natsConnect(t, s.ClientURL())
						defer nc.Close()

						natsPub(t, nc, "foo.bar", []byte("fromNats"))
						testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("fromNats"))

						mpc, pr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
						defer mpc.Close()
						testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

						testMQTTPublish(t, mpc, pr, 0, false, false, "foo/baz", 0, []byte("mqtt_qos0"))
						testMQTTCheckPubMsg(t, mc, r, "foo/baz", 0, []byte("mqtt_qos0"))

						testMQTTPublish(t, mpc, pr, 1, false, false, "foo/bat", 1, []byte("mqtt_qos1"))
						expectedQoS := byte(0)
						if test.subQos == 1 {
							expectedQoS = mqttPubQos1
						}
						testMQTTCheckPubMsg(t, mc, r, "foo/bat", expectedQoS, []byte("mqtt_qos1"))
						testMQTTDisconnect(t, mpc, nil)
					}
					check(mc, r, cl.opts[0], cl.servers[0])
					check(mc, r, cl.opts[1], cl.servers[1])

					// Start the same subscription from the other server. It should disconnect
					// the one connected in the first server.
					o = cl.opts[1]

					mc2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
					defer mc2.Close()
					testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

					// Expect first connection to be closed.
					testMQTTExpectDisconnect(t, mc)

					// Now re-run the checks
					check(mc2, r2, cl.opts[0], cl.servers[0])
					check(mc2, r2, cl.opts[1], cl.servers[1])

					// Disconnect our sub and restart with clean session then disconnect again to clear the state.
					testMQTTDisconnect(t, mc2, nil)
					mc2.Close()
					mc2, r2 = testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
					defer mc2.Close()
					testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
					testMQTTFlush(t, mc2, nil, r2)
					testMQTTDisconnect(t, mc2, nil)

					// Remove the session from the flappers so we can restart the test
					// without failure and have to wait for 1sec before being able to reconnect.
					s := cl.servers[0]
					sm := &s.mqtt.sessmgr
					sm.mu.Lock()
					asm := sm.sessions[globalAccountName]
					sm.mu.Unlock()
					if asm != nil {
						asm.mu.Lock()
						delete(asm.flappers, clientID)
						asm.mu.Unlock()
					}
				})
			}
			if !t.Failed() && topTest.restart {
				cl.stopAll()
				cl.restartAll()

				streams := []string{mqttStreamName, mqttRetainedMsgsStreamName, mqttSessStreamName}
				for _, sn := range streams {
					cl.waitOnStreamLeader(globalAccountName, sn)
				}

				mset, err := cl.randomServer().GlobalAccount().lookupStream(mqttRetainedMsgsStreamName)
				if err != nil {
					t.Fatalf("Expected to find a stream for %q", mqttRetainedMsgsStreamName)
				}

				rmConsumerNames := []string{}
				mset.mu.RLock()
				for _, o := range mset.consumers {
					rmConsumerNames = append(rmConsumerNames, o.name)
				}
				mset.mu.RUnlock()

				for _, consumerName := range rmConsumerNames {
					cl.waitOnConsumerLeader(globalAccountName, mqttRetainedMsgsStreamName, consumerName)
				}
			}
		})
	}
}

func TestMQTTClusterConnectDisconnectClean(t *testing.T) {
	// The purpose of this test was to illustrate and verify
	// https://github.com/nats-io/nats-server/pull/4734. Due to its timing
	// sensitivity it has never been 100% reliable, prone to flaking on an
	// occasional MQTT Connect failure (API timeout?). Skip for now, to reduce
	// the flaky noise.
	t.Skip()

	nServers := 3
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", nServers)
	defer cl.shutdown()

	clientID := nuid.Next()

	// test runs a connect/disconnect against a random server in the cluster, as
	// specified.
	N := 100
	for n := 0; n < N; n++ {
		testMQTTConnectDisconnect(t, cl.opts[rand.Intn(nServers)], clientID, true, false)
	}
}

func TestMQTTClusterConnectDisconnectPersist(t *testing.T) {
	// The purpose of this test was to illustrate and verify
	// https://github.com/nats-io/nats-server/pull/4734. Due to its timing
	// sensitivity it has never been 100% reliable, prone to flaking on an
	// occasional MQTT Connect failure (API timeout?). Skip for now, to reduce
	// the flaky noise.
	t.Skip()

	nServers := 3
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", nServers)
	defer cl.shutdown()

	clientID := nuid.Next()

	// test runs a connect/disconnect against a random server in the cluster, as
	// specified.
	N := 20
	for n := 0; n < N; n++ {
		// First clean sessions on all servers
		for i := 0; i < nServers; i++ {
			testMQTTConnectDisconnect(t, cl.opts[i], clientID, true, false)
		}

		testMQTTConnectDisconnect(t, cl.opts[0], clientID, false, false)
		testMQTTConnectDisconnect(t, cl.opts[1], clientID, false, true)
		testMQTTConnectDisconnect(t, cl.opts[2], clientID, false, true)
		testMQTTConnectDisconnect(t, cl.opts[0], clientID, false, true)
	}
}

func TestMQTTClusterCanCreateSessionWithOnServerDown(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 3)
	defer cl.shutdown()
	o := cl.opts[0]

	mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	mc.Close()

	// Shutdown one of the server.
	sd := cl.servers[1].StoreDir()
	defer os.RemoveAll(strings.TrimSuffix(sd, JetStreamStoreDir))
	cl.servers[1].Shutdown()

	// Make sure there is a meta leader
	cl.waitOnPeerCount(2)
	cl.waitOnLeader()

	// Now try to create a new session. Since we use a single stream now for all sessions,
	// this should succeed.
	o = cl.opts[2]
	// We may still get failures because of some JS APIs may timeout while things
	// settle, so try again for a certain amount of times.
	mc, rc = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
}

func TestMQTTClusterPlacement(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c := sc.randomCluster()
	lnc := c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafAndMQTT, "SPOKE", 3, 22111)
	defer lnc.shutdown()

	sc.waitOnPeerCount(9)
	sc.waitOnLeader()

	for i := 0; i < 10; i++ {
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, lnc.opts[i%3].MQTT.Host, lnc.opts[i%3].MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	}

	// Now check that MQTT assets have been created in the LEAF node's side, not the Hub.
	nc := natsConnect(t, lnc.servers[0].ClientURL())
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unable to get JetStream: %v", err)
	}
	count := 0
	for si := range js.StreamsInfo() {
		if si.Cluster == nil || si.Cluster.Name != "SPOKE" {
			t.Fatalf("Expected asset %q to be placed on spoke cluster, was placed on %+v", si.Config.Name, si.Cluster)
		}
		for _, repl := range si.Cluster.Replicas {
			if !strings.HasPrefix(repl.Name, "SPOKE-") {
				t.Fatalf("Replica on the wrong cluster: %+v", repl)
			}
		}
		if si.State.Consumers > 0 {
			for ci := range js.ConsumersInfo(si.Config.Name) {
				if ci.Cluster == nil || ci.Cluster.Name != "SPOKE" {
					t.Fatalf("Expected asset %q to be placed on spoke cluster, was placed on %+v", ci.Name, si.Cluster)
				}
				for _, repl := range ci.Cluster.Replicas {
					if !strings.HasPrefix(repl.Name, "SPOKE-") {
						t.Fatalf("Replica on the wrong cluster: %+v", repl)
					}
				}
			}
		}
		count++
	}
	if count == 0 {
		t.Fatal("No stream found!")
	}
}

func TestMQTTClusterReplicasCount(t *testing.T) {
	for _, test := range []struct {
		size     int
		replicas int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{5, 3},
	} {
		t.Run(fmt.Sprintf("size %v", test.size), func(t *testing.T) {
			var s *Server
			var o *Options
			if test.size == 1 {
				o = testMQTTDefaultOptions()
				s = testMQTTRunServer(t, o)
				defer testMQTTShutdownServer(s)
			} else {
				cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", test.size)
				defer cl.shutdown()
				o = cl.opts[0]
				s = cl.randomServer()
			}

			mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
			defer mc.Close()
			testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
			testMQTTFlush(t, mc, nil, rc)

			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()

			// Check the replicas of all MQTT streams
			js, err := nc.JetStream()
			if err != nil {
				t.Fatalf("Error getting js: %v", err)
			}
			for _, sname := range []string{
				mqttStreamName,
				mqttRetainedMsgsStreamName,
				mqttSessStreamName,
			} {
				t.Run(sname, func(t *testing.T) {
					si, err := js.StreamInfo(sname)
					if err != nil {
						t.Fatalf("Error getting stream info: %v", err)
					}
					if si.Config.Replicas != test.replicas {
						t.Fatalf("Expected %v replicas, got %v", test.replicas, si.Config.Replicas)
					}
				})
			}
		})
	}
}

func TestMQTTClusterRetainedMsg(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	srv1Opts := cl.opts[0]
	srv2Opts := cl.opts[1]

	// Connect subscription on server 1.
	mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	// Create a publisher from server 2.
	mp, rp := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Send retained message.
	testMQTTPublish(t, mp, rp, 1, false, true, "foo/bar", 1, []byte("retained"))
	// Check it is received.
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte("retained"))

	// Start a new subscription on server 1 and make sure we receive the retained message
	mc2, rc2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, mc2, rc2, "foo/bar", mqttPubQos1|mqttPubFlagRetain, []byte("retained"))
	testMQTTDisconnect(t, mc2, nil)
	mc2.Close()

	// Send an empty retained message which should remove it from storage, but still be delivered.
	testMQTTPublish(t, mp, rp, 1, false, true, "foo/bar", 1, []byte(""))
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte(""))

	// Now shutdown the consumer connection
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Reconnect to server where the retained message was published (server 2)
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	// The retained message should not be delivered.
	testMQTTExpectNothing(t, rc)
	// Now disconnect and reconnect back to first server
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Now reconnect to the server 1, which is not where the messages were published, and check
	// that we don't receive the message.
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rc)
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Will now test network deletes

	// Create a subscription on server 1 and server 2
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_one", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	mc2, rc2 = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_two", cleanSess: false}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, rc2)

	// Publish 1 retained message (producer is connected to server 2)
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte("msg1"))

	// Make sure messages are received by both
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte("msg1"))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte("msg1"))

	// Now send an empty retained message that should delete it. For the one on server 1,
	// this will be a network delete.
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte(""))
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte(""))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte(""))

	// Now send a new retained message
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte("msg2"))

	// Again, verify that they all receive it.
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte("msg2"))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte("msg2"))

	// But now, restart the consumer that was in the server that processed the
	// original network delete.
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_one", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	// Expect the message to be delivered as retained
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1|mqttPubFlagRetain, []byte("msg2"))
}

func TestMQTTConsumerReplicasOverride(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 5
			consumer_replicas: 1
			consumer_memory_storage: true
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 5)
	defer cl.shutdown()

	connectAndCheck := func(subject string, restarted bool) {
		t.Helper()

		o := cl.opts[0]
		mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, restarted)
		testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: subject, qos: 1}}, []byte{1})

		nc, js := jsClientConnect(t, cl.servers[2])
		defer nc.Close()

		for ci := range js.ConsumersInfo(mqttStreamName) {
			if ci.Config.FilterSubject == mqttStreamSubjectPrefix+subject {
				if rf := len(ci.Cluster.Replicas) + 1; rf != 5 {
					t.Fatalf("Expected consumer to be R5, got: %d", rf)
				}
			}
		}
	}
	connectAndCheck("foo", false)

	cl.stopAll()
	for _, o := range cl.opts {
		o.MQTT.ConsumerReplicas = 2
		o.MQTT.ConsumerMemoryStorage = false
	}
	cl.restartAllSamePorts()
	cl.waitOnStreamLeader(globalAccountName, mqttStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttRetainedMsgsStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttSessStreamName)

	connectAndCheck("bar", true)
}

func TestMQTTStreamReplicasOverride(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 3
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 3)
	defer cl.shutdown()

	connectAndCheck := func(restarted bool) {
		t.Helper()

		o := cl.opts[0]
		mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, restarted)

		nc, js := jsClientConnect(t, cl.servers[2])
		defer nc.Close()

		streams := []string{mqttStreamName, mqttRetainedMsgsStreamName, mqttSessStreamName}
		for _, sn := range streams {
			si, err := js.StreamInfo(sn)
			require_NoError(t, err)
			if n := len(si.Cluster.Replicas); n != 2 {
				t.Fatalf("Expected stream %q to have 2 replicas, got %v", sn, n)
			}
		}
	}
	connectAndCheck(false)

	cl.stopAll()
	for _, o := range cl.opts {
		o.MQTT.StreamReplicas = 2
	}
	cl.restartAllSamePorts()
	cl.waitOnStreamLeader(globalAccountName, mqttStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttRetainedMsgsStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttSessStreamName)

	l := &captureWarnLogger{warn: make(chan string, 10)}
	cl.servers[0].SetLogger(l, false, false)

	connectAndCheck(true)

	select {
	case w := <-l.warn:
		if !strings.Contains(w, "current is 3 but configuration is 2") {
			t.Fatalf("Unexpected warning: %q", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Should have warned against replicas mismatch")
	}
}

func TestMQTTStreamReplicasInsufficientResources(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 5
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 3)
	defer cl.shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	for _, s := range cl.servers {
		s.SetLogger(l, false, false)
	}

	o := cl.opts[1]
	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected to fail, did not")
	}

	select {
	case e := <-l.errCh:
		if !strings.Contains(e, fmt.Sprintf("%d", NewJSClusterNoPeersError(errors.New("")).ErrCode)) {
			t.Fatalf("Expected error regarding no peers error, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error regarding replicas count")
	}
}

func TestMQTTSessionMovingDomains(t *testing.T) {
	tmpl := strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", `leafnodes { listen: 127.0.0.1:-1 }`, 1)
	tmpl = strings.Replace(tmpl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()
	c.waitOnLeader()

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: SPOKE, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "SPOKE", 3, 22111)
	defer lnc.shutdown()
	lnc.waitOnPeerCount(3)

	connectSubAndDisconnect := func(host string, port int, present bool) {
		t.Helper()
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, host, port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, present)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)
		testMQTTDisconnect(t, mc, nil)
	}

	// Create a session on the HUB. Make sure we don't use "clean" session so that
	// it is not removed when the client connection closes.
	for i := 0; i < 7; i++ {
		var present bool
		if i > 0 {
			present = true
		}
		connectSubAndDisconnect(c.opts[0].MQTT.Host, c.opts[0].MQTT.Port, present)
	}

	// Now move to the SPOKE cluster, this is a brand new session there, so should not be present.
	connectSubAndDisconnect(lnc.opts[1].MQTT.Host, lnc.opts[1].MQTT.Port, false)

	// Move back to HUB cluster. Make it interesting by connecting to a different
	// server in that cluster. This should work, and present flag should be true.
	connectSubAndDisconnect(c.opts[2].MQTT.Host, c.opts[2].MQTT.Port, true)
}

func TestMQTTSessionsDifferentDomains(t *testing.T) {
	tmpl := strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", `leafnodes { listen: 127.0.0.1:-1 }`, 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()
	c.waitOnLeader()

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: LEAF1, store_dir:", 1)
	lnc1 := c.createLeafNodesWithTemplateAndStartPort(tmpl, "LEAF1", 2, 22111)
	defer lnc1.shutdown()
	lnc1.waitOnPeerCount(2)

	l := &remoteConnSameClientIDLogger{warn: make(chan string, 10)}
	lnc1.servers[0].SetLogger(l, false, false)

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: LEAF2, store_dir:", 1)
	lnc2 := c.createLeafNodesWithTemplateAndStartPort(tmpl, "LEAF2", 2, 23111)
	defer lnc2.shutdown()
	lnc2.waitOnPeerCount(2)

	o := &(lnc1.opts[0].MQTT)
	mc1, rc1 := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.Host, o.Port, 5)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rc1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc1, rc1, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc1, nil, rc1)

	o = &(lnc2.opts[0].MQTT)
	connectSubAndDisconnect := func(host string, port int, present bool) {
		t.Helper()
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, host, port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, present)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)
		testMQTTDisconnect(t, mc, nil)
	}

	for i := 0; i < 2; i++ {
		connectSubAndDisconnect(o.Host, o.Port, i > 0)
	}

	select {
	case w := <-l.warn:
		t.Fatalf("Got a warning: %v", w)
	case <-time.After(500 * time.Millisecond):
		// OK
	}
}

func TestMQTTSubPropagation(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	o := cl.opts[0]
	s2 := cl.servers[1]
	nc := natsConnect(t, s2.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Because in MQTT foo/# means foo.> but also foo, check that this is propagated
	checkSubInterest(t, s2, globalAccountName, "foo", time.Second)

	// Publish on foo.bar, foo./ and foo and we should receive them
	natsPub(t, nc, "foo.bar", []byte("hello"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("hello"))

	natsPub(t, nc, "foo./", []byte("from"))
	testMQTTCheckPubMsg(t, mc, r, "foo/", 0, []byte("from"))

	natsPub(t, nc, "foo", []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("NATS"))
}

func TestMQTTConfig(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		mqtt {
			port: -1
			tls {
				cert_file: "./configs/certs/server.pem"
				key_file: "./configs/certs/key.pem"
			}
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)
	if o.MQTT.TLSConfig == nil {
		t.Fatal("expected TLS config to be set")
	}
}

func TestMQTTConfigReload(t *testing.T) {
	template := `
		jetstream: true
		server_name: mqtt
		mqtt {
			port: -1
			ack_wait: %s
			max_ack_pending: %s
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, `"5s"`, `10000`)))

	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	if val := o.MQTT.AckWait; val != 5*time.Second {
		t.Fatalf("Invalid ackwait: %v", val)
	}
	if val := o.MQTT.MaxAckPending; val != 10000 {
		t.Fatalf("Invalid ackwait: %v", val)
	}

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"250ms"`, `1`)))
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	start := time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	if dur := time.Since(start); dur > 500*time.Millisecond {
		t.Fatalf("AckWait not applied? dur=%v", dur)
	}
	c.Close()
	cp.Close()
	testMQTTShutdownServer(s)

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"30s"`, `1`)))
	s, o = RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	cisub.cleanSess = true
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub = &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	testMQTTExpectNothing(t, r)

	// Increase the max ack pending
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"30s"`, `10`)))
	// Reload now
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Reload will have effect only on new subscriptions.
	// Create a new subscription, and we should not be able to get the 2 messages.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})

	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg3"))
	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg4"))

	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1, []byte("msg3"))
	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1, []byte("msg4"))
}

func TestMQTTConnectAndDisconnectEvent(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		accounts {
			MQTT {
				jetstream: enabled
				users: [{user: "mqtt", password: "pwd"}]
			}
			SYS {
				users: [{user: "sys", password: "pwd"}]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		system_account: "SYS"
	`, t.TempDir())))
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("sys", "pwd"))
	defer nc.Close()

	accConn := natsSubSync(t, nc, fmt.Sprintf(connectEventSubj, "MQTT"))
	accDisc := natsSubSync(t, nc, fmt.Sprintf(disconnectEventSubj, "MQTT"))
	accAuth := natsSubSync(t, nc, fmt.Sprintf(authErrorEventSubj, s.ID()))
	natsFlush(t, nc)

	checkConnEvent := func(data []byte, expected string) {
		t.Helper()
		var ce ConnectEventMsg
		json.Unmarshal(data, &ce)
		if ce.Client.MQTTClient != expected {
			t.Fatalf("Expected client ID %q, got this connect event: %+v", expected, ce)
		}
	}
	checkDiscEvent := func(data []byte, expected string) {
		t.Helper()
		var de DisconnectEventMsg
		json.Unmarshal(data, &de)
		if de.Client.MQTTClient != expected {
			t.Fatalf("Expected client ID %q, got this disconnect event: %+v", expected, de)
		}
	}

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	cm := natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn1")

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	cm = natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn2")

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)

	cm = natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn3")

	testMQTTDisconnect(t, c3, nil)
	c3.Close()
	cm = natsNexMsg(t, accDisc, time.Second)
	checkDiscEvent(cm.Data, "conn3")

	// Now try a bad auth
	c4, r4 := testMQTTConnect(t, &mqttConnInfo{clientID: "conn4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c4.Close()
	testMQTTCheckConnAck(t, r4, mqttConnAckRCNotAuthorized, false)
	// This will generate an auth error, which is a disconnect event
	cm = natsNexMsg(t, accAuth, time.Second)
	checkDiscEvent(cm.Data, "conn4")

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz", nil)
		if c.Conns == nil || len(c.Conns) != 3 {
			t.Fatalf("Expected 3 connections in array, got %v", len(c.Conns))
		}

		// Check that client ID is present
		for _, conn := range c.Conns {
			if conn.Type == clientTypeStringMap[MQTT] && conn.MQTTClient == _EMPTY_ {
				t.Fatalf("Expected a client ID to be set, got %+v", conn)
			}
		}

		// Check that we can select based on client ID:
		c = pollConnz(t, s, mode, url+"connz?mqtt_client=conn2", &ConnzOptions{MQTTClient: "conn2"})
		if c.Conns == nil || len(c.Conns) != 1 {
			t.Fatalf("Expected 1 connection in array, got %v", len(c.Conns))
		}
		if c.Conns[0].MQTTClient != "conn2" {
			t.Fatalf("Unexpected client ID: %+v", c.Conns[0])
		}

		// Check that we have the closed ones
		c = pollConnz(t, s, mode, url+"connz?state=closed", &ConnzOptions{State: ConnClosed})
		if c.Conns == nil || len(c.Conns) != 2 {
			t.Fatalf("Expected 2 connections in array, got %v", len(c.Conns))
		}
		for _, conn := range c.Conns {
			if conn.MQTTClient == _EMPTY_ {
				t.Fatalf("Expected a client ID, got %+v", conn)
			}
		}

		// Check that we can select with client ID for closed state
		c = pollConnz(t, s, mode, url+"connz?state=closed&mqtt_client=conn3", &ConnzOptions{State: ConnClosed, MQTTClient: "conn3"})
		if c.Conns == nil || len(c.Conns) != 1 {
			t.Fatalf("Expected 1 connection in array, got %v", len(c.Conns))
		}
		if c.Conns[0].MQTTClient != "conn3" {
			t.Fatalf("Unexpected client ID: %+v", c.Conns[0])
		}
		// Check that we can select with client ID for closed state (but in this case not found)
		c = pollConnz(t, s, mode, url+"connz?state=closed&mqtt_client=conn5", &ConnzOptions{State: ConnClosed, MQTTClient: "conn5"})
		if len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connection in array, got %v", len(c.Conns))
		}
	}

	reply := nc.NewRespInbox()
	replySub := natsSubSync(t, nc, reply)

	// Test system events now
	for _, test := range []struct {
		opt any
		cid string
	}{
		{&ConnzOptions{MQTTClient: "conn1"}, "conn1"},
		{&ConnzOptions{MQTTClient: "conn3", State: ConnClosed}, "conn3"},
		{&ConnzOptions{MQTTClient: "conn4", State: ConnClosed}, "conn4"},
		{&ConnzOptions{MQTTClient: "conn5"}, _EMPTY_},
		{json.RawMessage(`{"mqtt_client":"conn1"}`), "conn1"},
		{json.RawMessage(fmt.Sprintf(`{"mqtt_client":"conn3", "state":%v}`, ConnClosed)), "conn3"},
		{json.RawMessage(fmt.Sprintf(`{"mqtt_client":"conn4", "state":%v}`, ConnClosed)), "conn4"},
		{json.RawMessage(`{"mqtt_client":"conn5"}`), _EMPTY_},
	} {
		t.Run("sys connz", func(t *testing.T) {
			b, _ := json.Marshal(test.opt)

			// set a header to make sure request parsing knows to ignore them
			nc.PublishMsg(&nats.Msg{
				Subject: fmt.Sprintf("%s.CONNZ", serverStatsPingReqSubj),
				Reply:   reply,
				Data:    b,
			})

			msg := natsNexMsg(t, replySub, time.Second)
			var response ServerAPIResponse
			if err := json.Unmarshal(msg.Data, &response); err != nil {
				t.Fatalf("Error unmarshalling response json: %v", err)
			}
			tmp, _ := json.Marshal(response.Data)
			cz := &Connz{}
			if err := json.Unmarshal(tmp, cz); err != nil {
				t.Fatalf("Error unmarshalling connz: %v", err)
			}
			if test.cid == _EMPTY_ {
				if len(cz.Conns) != 0 {
					t.Fatalf("Expected no connections, got %v", len(cz.Conns))
				}
				return
			}
			if len(cz.Conns) != 1 {
				t.Fatalf("Expected single connection, got %v", len(cz.Conns))
			}
			conn := cz.Conns[0]
			if conn.MQTTClient != test.cid {
				t.Fatalf("Expected client ID %q, got %q", test.cid, conn.MQTTClient)
			}
		})
	}
}

func TestMQTTConsumerMemStorageReload(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		mqtt {
			port: -1
			consumer_memory_storage: %s
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, tdir, "false")))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, tdir, "true"))

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
	if err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	}
	var cons *consumer
	mset.mu.RLock()
	for _, c := range mset.consumers {
		cons = c
		break
	}
	mset.mu.RUnlock()
	cons.mu.RLock()
	st := cons.store.Type()
	cons.mu.RUnlock()
	if st != MemoryStorage {
		t.Fatalf("Expected storage %v, got %v", MemoryStorage, st)
	}
}

func TestMQTTCrossAccountRetain(t *testing.T) {
	for _, test := range []struct {
		name            string
		importTransform string
		sourceDest      string
		bDest           string
		getLastMsgSubj  string
	}{
		{"without transform", "", "", "foo/x", "$MQTT.rmsgs.foo.x"},
		{"with transform", `, to: "foobar.>"`, "$MQTT.rmsgs.foobar.>", "foobar/x", "$MQTT.rmsgs.foobar.x"},
	} {
		t.Run(test.name, func(t *testing.T) {
			td := t.TempDir()
			dir := filepath.Join(td, "js")
			conf := createConfFile(t, fmt.Appendf(nil, `
				server_name: server
				listen: "127.0.0.1:-1"
				jetstream {
					domain: "MYDOMAIN"
					store_dir: "%s"
				}
				mqtt {
					listen: "127.0.0.1:-1"
				}
				accounts: {
					A: {
						jetstream: true
						users: [ { user:a, password:x }]
						exports: [
							{ stream: "foo.>" }
							{ service: "$JS.API.>", response_type: stream }
							{ stream: "a2b.>" }
						]
					}
					B: {
						jetstream: true
						users: [ { user:b, password:x }]
						imports: [
							{ stream: { account: A, subject: "foo.>" }%s }
							{ service: { account: A, subject: "$JS.API.>"}, to: "A.$JS.API.>" }
							{ stream: { account: A, subject: "a2b.>" } }
						]
					}
				}
			`, dir, test.importTransform))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			// Connect a user on "B" to create the MQTT assets.
			c, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, user: "b", pass: "x"}, "127.0.0.1", o.MQTT.Port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTDisconnect(t, c, nil)
			c.Close()

			pubRetained := func(user, dest, msg string) {
				t.Helper()
				c, r := testMQTTConnect(t, &mqttConnInfo{
					cleanSess: true,
					user:      user,
					pass:      "x",
				}, "127.0.0.1", o.MQTT.Port)
				defer c.Close()
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				testMQTTPublish(t, c, r, 0, false, true, dest, 0, []byte(msg))
				testMQTTFlush(t, c, nil, r)
			}

			// Publish a retained message from account "A".
			retainInAMsg := "Retain in A"
			pubRetained("a", "foo/x", retainInAMsg)

			// Now we are going to do something unusual, which is to update
			// the MQTT retain stream in "B" to source from "A".
			nc, js := jsClientConnect(t, s, nats.UserInfo("b", "x"))
			defer nc.Close()

			si, err := js.StreamInfo(mqttRetainedMsgsStreamName)
			require_NoError(t, err)
			src := &nats.StreamSource{
				Name: mqttRetainedMsgsStreamName,
				External: &nats.ExternalStream{
					APIPrefix:     "A.$JS.API",
					DeliverPrefix: "a2b",
				},
				SubjectTransforms: []nats.SubjectTransformConfig{
					{
						Source:      mqttRetainedMsgsStreamSubject + "foo.>",
						Destination: test.sourceDest,
					},
				},
			}
			si.Config.Sources = []*nats.StreamSource{src}
			_, err = js.UpdateStream(&si.Config)
			require_NoError(t, err)

			// Now wait to make sure that the "B" retained messages stream
			// contains the message with body "Retain in A"
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				msg, err := js.GetLastMsg(mqttRetainedMsgsStreamName, test.getLastMsgSubj)
				if err != nil {
					return err
				}
				if !bytes.Contains(msg.Data, []byte(retainInAMsg)) {
					return fmt.Errorf("Message is not from A: %q", msg.Data)
				}
				return nil
			})

			getRetained := func(user, dest, msg string) {
				t.Helper()
				c, r := testMQTTConnect(t, &mqttConnInfo{
					cleanSess: true,
					user:      user,
					pass:      "x",
				}, "127.0.0.1", o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: dest, qos: 0}}, []byte{0})
				if msg == _EMPTY_ {
					testMQTTExpectNothing(t, r)
				} else {
					testMQTTCheckPubMsg(t, c, r, dest, mqttPubFlagRetain, []byte(msg))
				}
			}

			// The retained message in account A should of course be "Retain in A"
			getRetained("a", "foo/x", retainInAMsg)
			// But because of the sourcing, the retained message in "B" should be
			// the retained message from the "A" account.
			getRetained("b", test.bDest, retainInAMsg)

			// Now publish a retained message from "B" account and make sure that
			// it is correctly replacing "Retain in A".
			retainInBMsg := "Retain in B"
			pubRetained("b", test.bDest, retainInBMsg)
			// Check that we can receive it.
			getRetained("b", test.bDest, retainInBMsg)
			// And "A" still has the "Retain in A" message.
			getRetained("a", "foo/x", retainInAMsg)

			// Publish from "A" a new message:
			retainInAMsg = "Retain in A2"
			pubRetained("a", "foo/x", retainInAMsg)
			// Make sure that this retained appears on "A" and "B".
			getRetained("a", "foo/x", retainInAMsg)
			getRetained("b", test.bDest, retainInAMsg)

			// Now publish an empty body retained message from "A". This
			// should remove the retained message from both "A" and "B".
			pubRetained("a", "foo/x", _EMPTY_)

			// We will check that the message gets removed from the stream.
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				_, err := js.GetLastMsg(mqttRetainedMsgsStreamName, test.getLastMsgSubj)
				if err == nats.ErrMsgNotFound {
					return nil
				}
				return fmt.Errorf("Message still present or unexpected error %v", err)
			})
			// The helper will use "expect nothing" if the given string is empty.
			getRetained("a", "foo/x", _EMPTY_)
			getRetained("b", test.bDest, _EMPTY_)
		})
	}
}

// mqttRefRetainedNATSMsg builds the NATS message for a retained record in the
// only format this broker reads: the Nmqtt-R* header block that
// mqttEncodeRetainedMessage writes. Reference-only helper.
func mqttRefRetainedNATSMsg(rm *mqttRetainedMsg) *nats.Msg {
	hdr := nats.Header{}
	hdr.Set(mqttNatsRetainedMessageTopic, rm.Topic)
	hdr.Set(mqttNatsRetainedMessageFlags, strconv.FormatUint(uint64(rm.Flags), 16))
	if rm.Origin != _EMPTY_ {
		hdr.Set(mqttNatsRetainedMessageOrigin, rm.Origin)
	}
	if rm.Source != _EMPTY_ {
		hdr.Set(mqttNatsRetainedMessageSource, rm.Source)
	}
	return &nats.Msg{
		Subject: mqttRetainedMsgsStreamSubject + rm.Subject,
		Header:  hdr,
		Data:    rm.Msg,
	}
}

func TestMQTTDecodeRetainedMessage(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		mqtt {
			listen: 127.0.0.1:-1
			consumer_inactive_threshold: %q
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, tdir, "0.2s")))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	// Connect and publish a retained message, this will be in the "newer" form,
	// with the metadata in the header.
	mc, r := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc, r, 0, false, true, "foo/1", 0, []byte("msg1"))
	mc.Close()

	// Store a second retained message directly into the stream. Upstream wrote a
	// JSON-encoded body here to exercise the legacy reader; this broker accepts
	// only the header format (F-056 removed the JSON fallback), so the reference
	// injects the current format instead. Reference-only: this file is build
	// tagged mqttport_clustertests and never runs — see docs/PORTING.md.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	rm := mqttRetainedMsg{
		Origin:  "test",
		Subject: "foo.2",
		Topic:   "foo/2",
		Flags:   mqttPubFlagRetain,
		Msg:     []byte("msg2"),
	}
	_, err := js.PublishMsg(mqttRefRetainedNATSMsg(&rm))
	if err != nil {
		t.Fatalf("Error publishing retained message to JS directly: %v", err)
	}

	// Restart the server to see that it picks up both retained messages on restart.
	s.Shutdown()
	s = RunServer(o)
	defer testMQTTShutdownServer(s)

	// Connect again, subscribe, and check that we get both messages.
	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/+", qos: 0}}, []byte{0})
	for i := 0; i < 2; i++ {
		b, pl := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
		}
		_, _, topic, _ := testMQTTGetPubMsgExEx(t, mc, r, mqttPubFlagRetain, pl, "", nil)
		if string(topic) != "foo/1" && string(topic) != "foo/2" {
			t.Fatalf("Expected foo/1 or foo/2, got %q", topic)
		}
	}
	testMQTTExpectNothing(t, r)
	mc.Close()

	// Clear both retained messages.
	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc, r, 0, false, true, "foo/1", 0, []byte{})
	testMQTTPublish(t, mc, r, 0, false, true, "foo/2", 0, []byte{})
	testMQTTFlush(t, mc, nil, r)
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Connect again, subscribe, and check that we get nothing.
	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/+", qos: 0}}, []byte{0})
	testMQTTExpectNothing(t, r)
}

func TestMQTTImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		accounts {
			org {
				jetstream: enabled
				users: [{user: org, password: pwd}]
				imports = [{stream: {account: "device", subject: "foo"}, prefix: "org"}]
			}
			device {
				users: [{user: device, password: pwd}]
				exports = [{stream: "foo"}]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		no_auth_user: device
	`, t.TempDir())))
	defer os.Remove(conf)

	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	mc1, rc1 := testMQTTConnect(t, &mqttConnInfo{clientID: "sub1", user: "org", pass: "pwd", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rc1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc1, rc1, []*mqttFilter{{filter: "org/foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc1, nil, rc1)

	mc2, rc2 := testMQTTConnect(t, &mqttConnInfo{clientID: "sub2", user: "org", pass: "pwd", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "org/foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, rc2)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("msg"))

	// Verify message is received on receiver side.
	testMQTTCheckPubMsg(t, mc1, rc1, "org/foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc2, rc2, "org/foo", 0, []byte("msg"))
}

func TestMQTTJSApiMapping(t *testing.T) {
	td := t.TempDir()
	hubDir := filepath.Join(td, "hub")
	hubConf := createConfFile(t, fmt.Appendf(nil, `
        server_name: hub
        listen: "127.0.0.1:-1"
        jetstream {
            domain: "HUB"
            store_dir: "%s"
        }
        mqtt {
            listen: "127.0.0.1:-1"
        }
        leafnodes {
            listen: "127.0.0.1:-1"
            isolate: true
        }
        accounts: {
            SYS: { users: [ { user:s, password:x } ] }
            HUB: {
                jetstream: true
                users: [
                    {
                        user:h
                        password:x
                        permissions: {
                            subscribe: {allow: ["foo", "bar", "_INBOX.>"], deny: ["baz"] }
                        }
                    }
                ]
            }
        }
        system_account: SYS
    `, hubDir))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafDir := filepath.Join(td, "leaf")
	leafTmpl := `
        server_name: leaf
        listen: "127.0.0.1:-1"
        jetstream {
            domain: "LEAF"
            store_dir: "%s"
        }
        mqtt {
            listen: "127.0.0.1:-1"
            js_api_timeout: "500ms"
        }
        leafnodes {
            remotes [
                { urls: [ nats-leaf://s:x@127.0.0.1:%d ], account: SYS }
                { urls: [ nats-leaf://h:x@127.0.0.1:%d ], account: HUB }
            ]
        }
        accounts: {
            SYS: { users: [ { user:s, password:x } ] }
            HUB: {
                jetstream: false
                users: [ { user:h, password:x } ]
                mappings: {
                    "$JS.API.>" : "$JS.HUB.API.>"
                    %s
                }
            }
            LEAF: {
                jetstream: true
                users: [ { user:l, password:x } ]
            }
        }
        system_account: SYS
    `
	leafConf := createConfFile(t, fmt.Appendf(nil, leafTmpl, leafDir, ohub.LeafNode.Port, ohub.LeafNode.Port, ""))
	leaf, oleaf := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, leaf, 2)

	testRetained := func(user string, port int, msg string) {
		t.Helper()
		// Create a producer
		c, r := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			user:      user,
			pass:      "x",
		}, "127.0.0.1", port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, c, r, 0, false, true, "foo", 0, []byte(msg))
		testMQTTFlush(t, c, nil, r)
		testMQTTDisconnect(t, c, nil)
		c.Close()

		// Create a consumer
		c, r = testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			user:      user,
			pass:      "x",
		}, "127.0.0.1", port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
		testMQTTCheckPubMsg(t, c, r, "foo", mqttPubFlagRetain, []byte(msg))
	}

	// Connect to leaf
	testRetained("l", oleaf.MQTT.Port, "hi leaf")

	// Now same test in the hub
	testRetained("h", ohub.MQTT.Port, "hi hub")

	// Now test the hub account in the leaf node. We expect it to fail
	// because we don't have the mapping for the sessions to be persisted
	// in the hub.
	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{
		cleanSess: true,
		user:      "h",
		pass:      "x",
	}, oleaf.MQTT.Host, oleaf.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected failure to connect, but did connect")
	}

	// Config reload the leaf server to add the missing mapping.
	reloadUpdateConfig(t, leaf, leafConf, fmt.Sprintf(leafTmpl,
		leafDir, ohub.LeafNode.Port, ohub.LeafNode.Port, `"$MQTT.sess.LEAF.>" : "$MQTT.sess.HUB.>"`))

	c, r := testMQTTConnect(t, &mqttConnInfo{
		cleanSess: true,
		user:      "h",
		pass:      "x",
	}, "127.0.0.1", oleaf.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubFlagRetain, []byte("hi hub"))
}

func TestMQTTJWTWithAllowedConnectionTypes(t *testing.T) {
	o := testMQTTDefaultOptions()
	// Create System Account
	syskp, _ := nkeys.CreateAccount()
	syspub, _ := syskp.PublicKey()
	sysAc := jwt.NewAccountClaims(syspub)
	sysjwt, err := sysAc.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create memory resolver and store system account
	mr := &MemAccResolver{}
	mr.Store(syspub, sysjwt)
	if err != nil {
		t.Fatalf("Error saving system account JWT to memory resolver: %v", err)
	}
	// Add system account and memory resolver to server options
	o.SystemAccount = syspub
	o.AccountResolver = mr
	setupAddTrusted(o)

	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name            string
		connectionTypes []string
		rc              byte
	}{
		{"not allowed", []string{jwt.ConnectionTypeStandard}, mqttConnAckRCNotAuthorized},
		{"allowed", []string{jwt.ConnectionTypeStandard, strings.ToLower(jwt.ConnectionTypeMqtt)}, mqttConnAckRCConnectionAccepted},
		{"allowed with unknown", []string{jwt.ConnectionTypeMqtt, "SomeNewType"}, mqttConnAckRCConnectionAccepted},
		{"not allowed with unknown", []string{"SomeNewType"}, mqttConnAckRCNotAuthorized},
	} {
		t.Run(test.name, func(t *testing.T) {

			nuc := newJWTTestUserClaims()
			nuc.AllowedConnectionTypes = test.connectionTypes
			nuc.BearerToken = true

			okp, _ := nkeys.FromSeed(oSeed)

			akp, _ := nkeys.CreateAccount()
			apub, _ := akp.PublicKey()
			nac := jwt.NewAccountClaims(apub)
			// Enable Jetstream on account with lax limitations
			nac.Limits.JetStreamLimits.Consumer = -1
			nac.Limits.JetStreamLimits.Streams = -1
			nac.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024
			nac.Limits.JetStreamLimits.DiskStorage = 1024 * 1024
			ajwt, err := nac.Encode(okp)
			if err != nil {
				t.Fatalf("Error generating account JWT: %v", err)
			}

			nkp, _ := nkeys.CreateUser()
			pub, _ := nkp.PublicKey()
			nuc.Subject = pub
			jwt, err := nuc.Encode(akp)
			if err != nil {
				t.Fatalf("Error generating user JWT: %v", err)
			}

			addAccountToMemResolver(s, apub, ajwt)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "ignore_use_token",
				pass:      jwt,
			}

			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTJetStreamRepublishAndQoS0Subscribers(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Setup stream with republish on it.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		RePublish: &nats.RePublish{
			Source:      "foo",
			Destination: "mqtt.foo",
		},
	})
	require_NoError(t, err)

	// Create QoS0 subscriber to catch re-publishes.
	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "mqtt/foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	msg := []byte("HELLO WORLD")
	_, err = js.Publish("foo", msg)
	require_NoError(t, err)

	testMQTTCheckPubMsg(t, mc, r, "mqtt/foo", 0, msg)
	testMQTTExpectNothing(t, r)
}

func TestMQTTMappingsQoS0(t *testing.T) {
	td := t.TempDir()
	dir := filepath.Join(td, "js")
	conf := createConfFile(t, fmt.Appendf(nil, `
		server_name: server
		jetstream {
			domain: "A"
			store_dir: "%s"
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		accounts: {
			A: {
				jetstream: true
				users: [ { user:a, password:x }]
				exports: [ { stream: "foo.>" }]
				mappings: { "baz.>" : "bazz.>" }
			}
			B: {
				jetstream: true
				users: [ { user:b, password:x }]
				imports: [ { stream: { account: A, subject: "foo.>" }, to: "bar.>" } ]
			}
			C: {
				jetstream: true
				users: [ { user:c, password:x }]
				imports: [ { stream: { account: A, subject: "foo.>" }, to: "baz.>" } ]
			}
			D: {
				jetstream: true
				users: [ { user:d, password:x }]
				# Same imports than for account C
				imports: [ { stream: { account: A, subject: "foo.>" }, to: "baz.>" } ]
			}
		}
	`, dir))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	// First, we check for mapping inside the same account A.
	aSubConn, aSubReader := testMQTTConnect(t, &mqttConnInfo{
		cleanSess: true,
		clientID:  "sub",
		user:      "a",
		pass:      "x",
	}, "127.0.0.1", o.MQTT.Port)
	defer aSubConn.Close()
	testMQTTCheckConnAck(t, aSubReader, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, aSubConn, aSubReader, []*mqttFilter{{filter: "#", qos: 0}}, []byte{0})
	testMQTTFlush(t, aSubConn, nil, aSubReader)

	aPubConn, aPubReader := testMQTTConnect(t, &mqttConnInfo{
		cleanSess: true,
		clientID:  "pub",
		user:      "a",
		pass:      "x",
	}, "127.0.0.1", o.MQTT.Port)
	defer aPubConn.Close()
	testMQTTCheckConnAck(t, aPubReader, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, aPubConn, aPubReader, 0, false, true, "baz/x", 0, []byte("msg1"))
	// Because of mapping, the subscription should receive the message on "bazz/x"
	testMQTTCheckPubMsg(t, aSubConn, aSubReader, "bazz/x", 0, []byte("msg1"))

	// Helper to create a connection and sub for a given user on given topic
	createSub := func(user, topic string) (net.Conn, *mqttReader) {
		t.Helper()
		c, r := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			clientID:  nuid.Next(),
			user:      user,
			pass:      "x",
		}, "127.0.0.1", o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
		testMQTTFlush(t, c, nil, r)
		return c, r
	}
	bConn1, bReader1 := createSub("b", "bar/#")
	defer bConn1.Close()
	bConn2, bReader2 := createSub("b", "bar/+")
	defer bConn2.Close()
	cConn1, cReader1 := createSub("c", "baz/#")
	defer cConn1.Close()
	cConn2, cReader2 := createSub("c", "baz/+")
	defer cConn2.Close()
	dConn1, dReader1 := createSub("d", "baz/#")
	defer dConn1.Close()
	dConn2, dReader2 := createSub("d", "baz/+")
	defer dConn2.Close()

	// Now from "A" publisher, publishes on "foo/x", and with mapping across
	// accounts, we should get the expected results. We send 2 messages in
	// a row and verify that we get those, and no more than that.
	testMQTTPublish(t, aPubConn, aPubReader, 0, false, true, "foo/x", 0, []byte("msg2"))
	testMQTTPublish(t, aPubConn, aPubReader, 0, false, true, "foo/x", 0, []byte("msg3"))

	// "B" consumers should receive on "bar/x"
	conns := []net.Conn{bConn1, bConn2}
	readers := []*mqttReader{bReader1, bReader2}
	for i := range len(conns) {
		testMQTTCheckPubMsg(t, conns[i], readers[i], "bar/x", 0, []byte("msg2"))
		testMQTTCheckPubMsg(t, conns[i], readers[i], "bar/x", 0, []byte("msg3"))
		testMQTTExpectNothing(t, readers[i])
	}
	// For "C" and "D" consumers, it should be "baz/x"
	conns = []net.Conn{cConn1, cConn2, dConn1, dConn2}
	readers = []*mqttReader{cReader1, cReader2, dReader1, dReader2}
	for i := range len(conns) {
		testMQTTCheckPubMsg(t, conns[i], readers[i], "baz/x", 0, []byte("msg2"))
		testMQTTCheckPubMsg(t, conns[i], readers[i], "baz/x", 0, []byte("msg3"))
		testMQTTExpectNothing(t, readers[i])
	}
}

func TestMQTTMaxPayloadDoesNotCapPublishTopic(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: test_mqtt_max_payload_topic
		port: -1
		max_payload: 1024
		mqtt { listen: "127.0.0.1:-1" }
		jetstream: { domain: "TEST", max_mem_store: 8MB, max_file_store: 8MB, store_dir: "`+t.TempDir()+`" }
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "cid", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSendPublishPacket(t, mc, 0, false, false, strings.Repeat("a", 1100), 0, nil)
	testMQTTFlush(t, mc, nil, r)
}

func TestMQTTMaxPayloadEnforced(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: test_mqtt_max_payload
		port: -1
		max_payload: 1024
		mqtt { listen: "127.0.0.1:-1" }
		jetstream: { domain: "TEST", max_mem_store: 8MB, max_file_store: 8MB, store_dir: "`+t.TempDir()+`" }
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	mp := o.MQTT.Port
	host := o.MQTT.Host
	mc, _ := testMQTTConnect(t, &mqttConnInfo{clientID: "cid", cleanSess: true}, host, mp)
	defer mc.Close()

	oversized := bytes.Repeat([]byte{'A'}, 1500)
	testMQTTSendPublishPacket(t, mc, 0, false, false, "foo", 0, oversized)

	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTParseOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		content  string
		checkOpt func(*MQTTOpts) error
		err      string
	}{
		// Negative tests
		{"bad type", "mqtt: []", nil, "to be a map"},
		{"bad listen", "mqtt: { listen: [] }", nil, "port or host:port"},
		{"bad port", `mqtt: { port: "abc" }`, nil, "not int64"},
		{"bad host", `mqtt: { host: 123 }`, nil, "not string"},
		{"bad tls", `mqtt: { tls: 123 }`, nil, "not map[string]interface {}"},
		{"unknown field", `mqtt: { this_does_not_exist: 123 }`, nil, "unknown"},
		{"ack wait", `mqtt: {ack_wait: abc}`, nil, "invalid duration"},
		{"max ack pending", `mqtt: {max_ack_pending: abc}`, nil, "not int64"},
		{"max ack pending too high", `mqtt: {max_ack_pending: 12345678}`, nil, "invalid value"},
		{"js_api_timeout bad duration", `mqtt: {js_api_timeout: abc}`, nil, "invalid duration"},
		{"topic alias maximum bad type", `mqtt: {topic_alias_maximum: abc}`, nil, "not int64"},
		{"topic alias maximum too high", `mqtt: {topic_alias_maximum: 123456}`, nil, "invalid value"},
		{"keep alive maximum bad type", `mqtt: {keep_alive_maximum: abc}`, nil, "not int64"},
		{"keep alive maximum too high", `mqtt: {keep_alive_maximum: 123456}`, nil, "invalid value"},
		{"keep alive maximum negative", `mqtt: {keep_alive_maximum: -1}`, nil, "invalid value"},
		{"receive maximum bad type", `mqtt: {receive_maximum: abc}`, nil, "not int64"},
		{"receive maximum too high", `mqtt: {receive_maximum: 123456}`, nil, "invalid value"},
		{"receive maximum negative", `mqtt: {receive_maximum: -1}`, nil, "invalid value"},
		// Positive tests
		{"tls gen fails", `
			mqtt {
				tls {
					cert_file: "./configs/certs/server.pem"
				}
			}`, nil, "missing 'key_file'"},
		{"listen port only", `mqtt { listen: 1234 }`, func(o *MQTTOpts) error {
			if o.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", o.Port)
			}
			return nil
		}, ""},
		{"listen host and port", `mqtt { listen: "localhost:1234" }`, func(o *MQTTOpts) error {
			if o.Host != "localhost" || o.Port != 1234 {
				return fmt.Errorf("expected localhost:1234, got %v:%v", o.Host, o.Port)
			}
			return nil
		}, ""},
		{"host", `mqtt { host: "localhost" }`, func(o *MQTTOpts) error {
			if o.Host != "localhost" {
				return fmt.Errorf("expected localhost, got %v", o.Host)
			}
			return nil
		}, ""},
		{"port", `mqtt { port: 1234 }`, func(o *MQTTOpts) error {
			if o.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", o.Port)
			}
			return nil
		}, ""},
		{"tls config",
			`
			mqtt {
				tls {
					cert_file: "./configs/certs/server.pem"
					key_file: "./configs/certs/key.pem"
				}
			}
			`, func(o *MQTTOpts) error {
				if o.TLSConfig == nil {
					return fmt.Errorf("TLSConfig should have been set")
				}
				return nil
			}, ""},
		{"no auth user",
			`
			mqtt {
				no_auth_user: "noauthuser"
			}
			`, func(o *MQTTOpts) error {
				if o.NoAuthUser != "noauthuser" {
					return fmt.Errorf("Invalid NoAuthUser value: %q", o.NoAuthUser)
				}
				return nil
			}, ""},
		{"auth block",
			`
			mqtt {
				authorization {
					user: "mqttuser"
					password: "pwd"
					token: "token"
					timeout: 2.0
				}
			}
			`, func(o *MQTTOpts) error {
				if o.Username != "mqttuser" || o.Password != "pwd" || o.Token != "token" || o.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth block: %+v", o)
				}
				return nil
			}, ""},
		{"auth timeout as int",
			`
			mqtt {
				authorization {
					timeout: 2
				}
			}
			`, func(o *MQTTOpts) error {
				if o.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth timeout: %v", o.AuthTimeout)
				}
				return nil
			}, ""},
		{"ack wait",
			`
			mqtt {
				ack_wait: "10s"
			}
			`, func(o *MQTTOpts) error {
				if o.AckWait != 10*time.Second {
					return fmt.Errorf("Invalid ack wait: %v", o.AckWait)
				}
				return nil
			}, ""},
		{"max ack pending",
			`
			mqtt {
				max_ack_pending: 123
			}
			`, func(o *MQTTOpts) error {
				if o.MaxAckPending != 123 {
					return fmt.Errorf("Invalid max ack pending: %v", o.MaxAckPending)
				}
				return nil
			}, ""},
		{"topic alias maximum",
			`
			mqtt {
				topic_alias_maximum: 123
			}
			`, func(o *MQTTOpts) error {
				if o.TopicAliasMaximum != 123 {
					return fmt.Errorf("Invalid topic alias maximum: %v", o.TopicAliasMaximum)
				}
				return nil
			}, ""},
		{"topic alias maximum zero disables",
			`
			mqtt {
				topic_alias_maximum: 0
			}
			`, func(o *MQTTOpts) error {
				if o.TopicAliasMaximum != -1 {
					return fmt.Errorf("expected 0 to be stored as -1 (disabled), got %v", o.TopicAliasMaximum)
				}
				return nil
			}, ""},
		{"receive maximum",
			`
			mqtt {
				receive_maximum: 123
			}
			`, func(o *MQTTOpts) error {
				if o.ReceiveMaximum != 123 {
					return fmt.Errorf("Invalid receive maximum: %v", o.ReceiveMaximum)
				}
				return nil
			}, ""},
		{"receive maximum zero disables",
			`
			mqtt {
				receive_maximum: 0
			}
			`, func(o *MQTTOpts) error {
				if o.ReceiveMaximum != -1 {
					return fmt.Errorf("expected 0 to be stored as -1 (disabled), got %v", o.ReceiveMaximum)
				}
				return nil
			}, ""},
		{"keep alive maximum",
			`
			mqtt {
				keep_alive_maximum: 30
			}
			`, func(o *MQTTOpts) error {
				if o.KeepAliveMaximum != 30 {
					return fmt.Errorf("Invalid keep alive maximum: %v", o.KeepAliveMaximum)
				}
				return nil
			}, ""},
		{"keep alive maximum zero stays zero",
			`
			mqtt {
				keep_alive_maximum: 0
			}
			`, func(o *MQTTOpts) error {
				// Unlike topic_alias_maximum there is no 0 => -1 remap.
				if o.KeepAliveMaximum != 0 {
					return fmt.Errorf("expected 0 to stay 0 (no override), got %v", o.KeepAliveMaximum)
				}
				return nil
			}, ""},
		{"reject_qos2_publish",
			`
			mqtt {
				reject_qos2_publish: true
			}
			`, func(o *MQTTOpts) error {
				if !o.rejectQoS2Pub {
					return fmt.Errorf("Invalid: expected rejectQoS2Pub to be set")
				}
				return nil
			}, ""},
		{"downgrade_qos2_subscribe",
			`
			mqtt {
				downgrade_qos2_subscribe: true
			}
			`, func(o *MQTTOpts) error {
				if !o.downgradeQoS2Sub {
					return fmt.Errorf("Invalid: expected downgradeQoS2Sub to be set")
				}
				return nil
			}, ""},
		{"js_api_timeout",
			`
			mqtt {
				js_api_timeout: "60s"
			}
			`, func(o *MQTTOpts) error {
				if o.JSAPITimeout != 60*time.Second {
					return fmt.Errorf("Invalid JS API timeout: %v", o.JSAPITimeout)
				}
				return nil
			}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.content))
			o, err := ProcessConfigFile(conf)
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("For content: %q, expected error about %q, got %v", test.content, test.err, err)
				}
				return
			} else if err != nil {
				t.Fatalf("Unexpected error for content %q: %v", test.content, err)
			}
			if err := test.checkOpt(&o.MQTT); err != nil {
				t.Fatalf("Incorrect option for content %q: %v", test.content, err.Error())
			}
		})
	}
}

func TestMQTTPartial(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name string
		ws   bool
	}{
		{"standard", false},
		{"websocket", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ci := &mqttConnInfo{cleanSess: true, ws: test.ws}
			host, port := o.MQTT.Host, o.MQTT.Port
			if test.ws {
				host, port = o.Websocket.Host, o.Websocket.Port
			}
			c, r := testMQTTConnect(t, ci, host, port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			c = &chunkWriteConn{Conn: c}

			cp, rp := testMQTTConnect(t, ci, host, port)
			defer cp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			cp = &chunkWriteConn{Conn: cp}

			subj := nuid.Next()
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: subj, qos: 1}}, []byte{1})
			testMQTTFlush(t, c, nil, r)
			for i := 0; i < 10; i++ {
				testMQTTPublish(t, cp, rp, 1, false, false, subj, 1, []byte("msg"))
				testMQTTCheckPubMsg(t, c, r, subj, mqttPubQos1, []byte("msg"))
			}
		})
	}
}

func TestMQTTRetainedMessageWithDelSubjectIsNotRestored(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		mqtt {
			listen: 127.0.0.1:-1
			consumer_inactive_threshold: %q
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	conf := createConfFile(t, fmt.Appendf(nil, tmpl, tdir, "0.2s"))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	// Publish one normal retained message through MQTT.
	mc, r := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc, r, 0, false, true, "foo/ok", 0, []byte("ok"))
	mc.Close()

	// Store a retained message directly into JetStream using a subject that
	// contains DEL. That subject can no longer be indexed after restore. It is
	// written in the current header format so that the DEL check is what rejects
	// it — upstream injected a JSON body, and this broker no longer reads that
	// format at all (F-056), which would make the record fail earlier for an
	// unrelated reason.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	rm := mqttRetainedMsg{
		Origin:  "test",
		Subject: "foo.\x7fbad",
		Topic:   "foo/\x7fbad",
		Flags:   mqttPubFlagRetain,
		Msg:     []byte("bad"),
	}
	_, err := js.PublishMsg(mqttRefRetainedNATSMsg(&rm))
	require_NoError(t, err)

	// Restart the server so retained-message recovery runs through
	// processRetainedMsg().
	s.Shutdown()
	s = RunServer(o)
	defer testMQTTShutdownServer(s)

	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/+", qos: 0}}, []byte{0})
	testMQTTCheckPubMsg(t, mc, r, "foo/ok", mqttPubFlagRetain, []byte("ok"))
	testMQTTExpectNothing(t, r)
}

func TestMQTTServerNameRequired(t *testing.T) {
	conf := createConfFile(t, []byte(`
		cluster {
			port: -1
		}
		mqtt {
			port: -1
		}
	`))
	o, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTServerNameMustBeSet.Error() {
		t.Fatalf("Expected error about requiring server name to be set, got %v", err)
	}

	conf = createConfFile(t, []byte(`
		mqtt {
			port: -1
		}
	`))
	o, err = ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTStandaloneNeedsJetStream.Error() {
		t.Fatalf(`Expected errMQTTStandaloneNeedsJetStream ("next in line"), got %v`, err)
	}
}

func TestMQTTStandaloneRequiresJetStream(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: mqtt
		mqtt {
			port: -1
			tls {
				cert_file: "./configs/certs/server.pem"
				key_file: "./configs/certs/key.pem"
			}
		}
	`))
	o, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTStandaloneNeedsJetStream.Error() {
		t.Fatalf("Expected error about requiring JetStream in standalone mode, got %v", err)
	}
}

func TestMQTTStreamReplicasConfigReload(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		mqtt {
			port: -1
			stream_replicas: %v
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, tdir, 3)))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected to fail, did not")
	}

	select {
	case e := <-l.errCh:
		if !strings.Contains(e, NewJSStreamReplicasNotSupportedError().Description) {
			t.Fatalf("Expected error regarding replicas, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error regarding replicas count")
	}

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, tdir, 1))

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
}

func TestMQTTSubjectMapping(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		mappings = {
			foo0: bar0
			foo1: bar1
		}

		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, qos := range []byte{0, 1} {
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()

			bar := fmt.Sprintf("bar%v", qos)
			foo := fmt.Sprintf("foo%v", qos)

			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: bar, qos: qos}}, []byte{qos})
			testMQTTFlush(t, mc, nil, r)

			mcp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

			natsPub(t, nc, foo, []byte("msg0"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg0"))

			testMQTTPublish(t, mcp, rp, 0, false, false, foo, 0, []byte("msg1"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg1"))

			testMQTTPublish(t, mcp, rp, 0, false, true, foo, 0, []byte("msg1_retained"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg1_retained"))

			testMQTTPublish(t, mcp, rp, 1, false, false, foo, 1, []byte("msg2"))
			// For the receiving side, the expected QoS is based on the subscription's QoS,
			// not the publisher.
			expected := byte(0)
			if qos == 1 {
				expected = mqttPubQos1
			}
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("msg2"))

			testMQTTPublish(t, mcp, rp, 1, false, true, foo, 1, []byte("msg2_retained"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("msg2_retained"))

			testMQTTDisconnect(t, mcp, nil)
			mcp.Close()

			// Try the with the "will" with QoS0 first
			mcp, rp = testMQTTConnect(t, &mqttConnInfo{
				clientID:  "pub",
				cleanSess: true,
				will:      &mqttWill{topic: []byte(foo), qos: 0, message: []byte("willmsg1")}},
				o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			testMQTTFlush(t, mcp, nil, rp)

			// Close the connection without proper disconnect for will to be sent
			mcp.Close()
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("willmsg1"))

			// Try the with the "will" with QoS1 now
			mcp, rp = testMQTTConnect(t, &mqttConnInfo{
				clientID:  "pub",
				cleanSess: true,
				will:      &mqttWill{topic: []byte(foo), qos: 1, message: []byte("willmsg2")}},
				o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			testMQTTFlush(t, mcp, nil, rp)

			// Close the connection without proper disconnect for will to be sent
			mcp.Close()
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("willmsg2"))

			si, err := js.StreamInfo("$MQTT_msgs")
			require_NoError(t, err)
			if qos == 0 {
				require_True(t, si.State.Msgs == 0)
				require_True(t, si.State.NumSubjects == 0)
			} else {
				// Number of QoS1 messages: 1 regular, 1 retained, 1 from will.
				require_True(t, si.State.Msgs == 3)
				require_True(t, si.State.NumSubjects == 1)
			}
			testMQTTDisconnect(t, mc, nil)
		})
	}
}

func TestMQTTSubjectMappingWithImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		accounts {
			A {
				users = [{user: "a", password: "pwd"}]
				mappings = {
					bar: foo
				}
				exports = [{service: "bar"}]
				jetstream: enabled
			}
			B {
				users = [{user: "b", password: "pwd"}]
				mappings = {
					foo: bar
				}
				imports = [{service: {account: "A", subject: "bar"}}]
				jetstream: enabled
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}

		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, c1, nil, r1)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c3, r3, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, c3, nil, r3)

	c4, r4 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c4.Close()
	testMQTTCheckConnAck(t, r4, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c4, r4, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, c4, nil, r4)

	bc, br := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b0", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc.Close()
	testMQTTCheckConnAck(t, br, mqttConnAckRCConnectionAccepted, false)

	bc1, br1 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc1.Close()
	testMQTTCheckConnAck(t, br1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc1, br1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, bc1, nil, br1)

	bc2, br2 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc2.Close()
	testMQTTCheckConnAck(t, br2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc2, br2, []*mqttFilter{{filter: "b", qos: 1}}, []byte{1})
	testMQTTFlush(t, bc2, nil, br2)

	bc3, br3 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc3.Close()
	testMQTTCheckConnAck(t, br3, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc3, br3, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, bc3, nil, br3)

	bc4, br4 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc4.Close()
	testMQTTCheckConnAck(t, br4, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc4, br4, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, bc4, nil, br4)

	nc := natsConnect(t, fmt.Sprintf("nats://b:pwd@%s:%d", o.Host, o.Port))
	defer nc.Close()

	natsPub(t, nc, "foo", []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", 0, []byte("msg0"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", 0, []byte("msg0"))

	testMQTTPublish(t, bc, br, 0, false, false, "foo", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", 0, []byte("msg1"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", 0, []byte("msg1"))

	testMQTTPublish(t, bc, br, 1, false, false, "foo", 1, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", mqttPubQos1, []byte("msg2"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", mqttPubQos1, []byte("msg2"))

	// Connection nc is for account B
	check := func(nc *nats.Conn, subj string) {
		t.Helper()
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: ">"})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "$MQTT_msgs"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		require_True(t, si.State.Msgs == 1)
		require_True(t, si.State.NumSubjects == 1)
		require_True(t, si.State.Subjects[subj] == 1)
	}
	// Currently, nc is for account B
	check(nc, "$MQTT.msgs.bar")

	nc.Close()
	nc = natsConnect(t, fmt.Sprintf("nats://a:pwd@%s:%d", o.Host, o.Port))
	defer nc.Close()
	check(nc, "$MQTT.msgs.foo")
}

func TestMQTTSubjectWildcardStart(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mc1, r1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	mc2, r2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	mc3, r3 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)

	// These will fail already with the bug due to messages already being queue up before the subAck.
	testMQTTSub(t, 1, mc1, r1, []*mqttFilter{{filter: "*", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc1, nil, r1)

	testMQTTSub(t, 1, mc2, r2, []*mqttFilter{{filter: "#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, r2)

	testMQTTSub(t, 1, mc3, r3, []*mqttFilter{{filter: "*/foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, r2)

	// Just as a barrier
	natsFlush(t, nc)

	// Now publish

	// NATS Publish
	msg := []byte("HELLO WORLD")
	natsPub(t, nc, "foo", msg)

	// Check messages received
	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 0, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	// Anything that starts with $ is reserved against wildcard subjects like above.
	natsPub(t, nc, "$JS.foo", msg)
	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	// Now do MQTT QoS-0
	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, msg)

	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 0, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 0, false, false, "$JS/foo", 1, msg)

	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	// Now do MQTT QoS-1
	msg = []byte("HELLO WORLD - RETAINED")
	testMQTTPublish(t, mc, r, 1, false, false, "$JS/foo", 4, msg)

	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 1, false, false, "foo", 2, msg)

	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 2, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 1, false, false, "foo/foo", 3, msg)

	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo/foo", 2, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTCheckPubMsg(t, mc3, r3, "foo/foo", 2, msg)
	testMQTTExpectNothing(t, r3)

	// Make sure we did not retain the messages prefixed with $.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo(mqttStreamName)
	require_NoError(t, err)

	require_True(t, si.State.Msgs == 0)
}

func TestMQTTWillRetainPermViolation(t *testing.T) {
	template := `
		port: -1
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		authorization {
			mqtt_perms = {
				publish = ["%s"]
				subscribe = ["foo", "bar"]
			}
			users = [
				{user: mqtt, password: pass, permissions: $mqtt_perms}
			]
		}
		mqtt {
			port: -1
		}
	`
	tdir := t.TempDir()
	conf := createConfFile(t, fmt.Appendf(nil, template, tdir, "foo"))

	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		cleanSess: true,
		user:      "mqtt",
		pass:      "pass",
	}

	// We create first a connection with the Will topic that the publisher
	// is allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("foo"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, which will cause the Will to be sent with retain flag.
	mc.Close()

	// Wait for the server to process the connection close, which will
	// cause the "will" message to be published (and retained).
	checkClientsCount(t, s, 0)

	// Create a subscription on the Will subject and we should receive it.
	ci.will = nil
	mcs, rs := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "foo", []byte("bye"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
	}
	if qos := mqttGetQoS(pflags); qos != 1 {
		t.Fatalf("expected qos to be 1, got %v", qos)
	}
	testMQTTDisconnect(t, mcs, nil)
	mcs.Close()

	// Now create another connection with a Will that client is not allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("bar"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, to cause Will to be produced, but in that case should not be stored
	// since user not allowed to publish on "bar".
	mc.Close()

	// Wait for the server to process the connection close, which will
	// cause the "will" message to be published (and retained).
	checkClientsCount(t, s, 0)

	// Create sub on "bar" which user is allowed to subscribe to.
	ci.will = nil
	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	// No Will should be published since it should not have been stored in the first place.
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)
	mcs.Close()

	// Now remove permission to publish on "foo" and check that a new subscription
	// on "foo" is now not getting the will message because the original user no
	// longer has permission to do so.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, tdir, "baz"))

	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)
}

// The following tests exercise multi-account authorization (Options.Accounts,
// User.Account) that the standalone single-account broker does not implement
// (see docs/PORTING.md); User.Account is *server.Account (the User type is
// aliased from nats-server), which is not assignable from this package's
// single-account Account type, so these also do not type-check as written.
func TestMQTTRequiresJSEnabled(t *testing.T) {
	o := testMQTTDefaultOptions()
	acc := NewAccount("mqtt")
	o.Accounts = []*Account{acc}
	o.Users = []*User{{Username: "mqtt", Account: acc}}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c.Close()

	proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true, user: "mqtt"})
	if _, err := testMQTTWrite(c, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	if _, err := testMQTTRead(c); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

func TestMQTTNoAuthUser(t *testing.T) {
	for _, test := range []struct {
		name         string
		override     bool
		useAuth      bool
		expectedUser string
		expectedAcc  string
	}{
		{"no override, no user provided", false, false, "noauth", "normal"},
		{"no override, user povided", false, true, "user", "normal"},
		{"override, no user provided", true, false, "mqttnoauth", "mqtt"},
		{"override, user provided", true, true, "mqttuser", "mqtt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			normalAcc := NewAccount("normal")
			mqttAcc := NewAccount("mqtt")
			o.Accounts = []*Account{normalAcc, mqttAcc}
			o.Users = []*User{
				{Username: "noauth", Password: "pwd", Account: normalAcc},
				{Username: "user", Password: "pwd", Account: normalAcc},
				{Username: "mqttnoauth", Password: "pwd", Account: mqttAcc},
				{Username: "mqttuser", Password: "pwd", Account: mqttAcc},
			}
			o.NoAuthUser = "noauth"
			if test.override {
				o.MQTT.NoAuthUser = "mqttnoauth"
			}
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			testMQTTEnableJSForAccount(t, s, "normal")
			testMQTTEnableJSForAccount(t, s, "mqtt")

			ci := &mqttConnInfo{clientID: "mqtt", cleanSess: true}
			if test.useAuth {
				ci.user = test.expectedUser
				ci.pass = "pwd"
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			c := testMQTTGetClient(t, s, "mqtt")
			c.mu.Lock()
			uname := c.opts.Username
			aname := c.acc.GetName()
			c.mu.Unlock()
			if uname != test.expectedUser {
				t.Fatalf("Expected selected user to be %q, got %q", test.expectedUser, uname)
			}
			if aname != test.expectedAcc {
				t.Fatalf("Expected selected account to be %q, got %q", test.expectedAcc, aname)
			}
		})
	}
}

func testMQTTEnableJSForAccount(t *testing.T, s *Server, accName string) {
	t.Helper()
	acc, err := s.LookupAccount(accName)
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	limits := map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxConsumers: -1,
			MaxStreams:   -1,
			MaxMemory:    1024 * 1024,
			MaxStore:     1024 * 1024,
		},
	}
	if err := acc.EnableJetStream(limits, nil); err != nil {
		t.Fatalf("Error enabling JS: %v", err)
	}
}

// TestMQTTDontSetPinger reads c.ping.tmr, an internal field of upstream's
// client struct that the shim's client type does not have (the shim has no
// NATS-core ping-timer concept distinct from MQTT keep-alive).
func TestMQTTDontSetPinger(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.PingInterval = 15 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "mqtt", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	c := testMQTTGetClient(t, s, "mqtt")
	c.mu.Lock()
	timerSet := c.ping.tmr != nil
	c.mu.Unlock()
	if timerSet {
		t.Fatalf("Ping timer should not be set for MQTT clients")
	}

	// Wait a bit and expect nothing (and connection should still be valid)
	testMQTTExpectNothing(t, r)
	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, []byte("msg"))
}

// TestMQTTSubQoS2Restart inspects acc.sl (Account.sl), upstream's internal
// in-process sublist, to count routed subscriptions. The shim's Account has no
// such field: subscriptions are real nats.go subscriptions over the wire, not
// an in-process routing table. See docs/PORTING.md.
func TestMQTTSubQoS2Restart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	pubTopic := "foo/bar"

	publish := func(msg string) {
		t.Helper()
		mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer mcp.Close()
		testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, mcp, mpr, 2, false, false, pubTopic, 1, []byte(msg))
		testMQTTFlush(t, mcp, nil, mpr)
	}

	createSub := func(present bool) (net.Conn, *mqttReader) {
		t.Helper()
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, present)
		if !present {
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/bar", qos: 2}}, []byte{2})
			testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo/+", qos: 2}}, []byte{2})
			testMQTTFlush(t, c, nil, r)
		}
		return c, r
	}

	c, r := createSub(false)
	defer c.Close()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	checkSub := func(count int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			for c := range js.Consumers(mqttOutStreamName) {
				acc := s.GlobalAccount()
				res := acc.sl.Match(c.Config.DeliverSubject)
				if n := len(res.psubs); n != count {
					return fmt.Errorf("Expected %v subscription on %q, got %v",
						count, c.Config.DeliverSubject, n)
				} else {
					// We got the JS consumer and verified that it has the
					// expected number of subscription, so we are done.
					return nil
				}
			}
			// No JS consumer was found.
			return fmt.Errorf("Did not find a JS consumer for %q", mqttOutStreamName)
		})
	}
	checkSub(1)

	publish("msg1")

	consume := func(msg string) {
		t.Helper()
		var pis []uint16
		for range 2 {
			b, pl := testMQTTReadPacket(t, r)
			if pt := b & mqttPacketMask; pt != mqttPacketPub {
				t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
			}
			flags, pi, _, _ := testMQTTGetPubMsgExEx(t, c, r, b, pl, pubTopic, []byte(msg))
			require_Equal(t, flags, byte(mqttPubQoS2))
			pis = append(pis, pi)
		}
		for _, pi := range pis {
			testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
			testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
		}
		testMQTTExpectNothing(t, r)
	}

	consume("msg1")
	testMQTTDisconnect(t, c, nil)
	c.Close()
	checkSub(0)

	publish("msg2")

	c, r = createSub(true)
	defer c.Close()
	checkSub(1)
	consume("msg2")

	publish("msg3")
	consume("msg3")
}

// TestMQTTPublishSubjectLeafNodeParserFailures drives upstream's internal
// leafnode client parser directly (dummyClient, LEAF, leaf, client.parse),
// none of which exist in the shim's client type (a different struct built for
// the standalone broker, with no leafnode concept). See docs/PORTING.md.
func TestMQTTPublishSubjectLeafNodeParserFailures(t *testing.T) {
	const (
		hdrLen           = 25
		leafControlLimit = MAX_CONTROL_LINE_SIZE * 16
	)

	hdr := []byte("NATS/1.0\r\nNmqtt-Pub:0\r\n\r\n")
	require_Equal(t, len(hdr), hdrLen)

	convertMQTTTopic := func(t *testing.T, topic []byte) []byte {
		t.Helper()
		require_NoError(t, mqttValidateTopic(topic, "topic"))
		subject, err := mqttTopicToNATSPubSubject(topic)
		require_NoError(t, err)
		return subject
	}

	parseLeafHMSG := func(subject []byte) error {
		c := dummyClient()
		c.kind = LEAF
		c.leaf = &leaf{}
		c.flags.set(connectReceived)
		proto := fmt.Appendf(nil, "HMSG %s %d %d\r\n", subject, hdrLen, hdrLen)
		proto = append(proto, hdr...)
		proto = append(proto, _CRLF_...)
		return c.parse(proto)
	}

	leafHMSGArgLen := func(subject []byte) int {
		return len(subject) + len(" 25 25")
	}

	makeRepeatedLevelTopic := func(levels int) []byte {
		topic := bytes.Repeat([]byte("a."), levels)
		return topic[:len(topic)-1]
	}

	makeSlashExpansionTopic := func(t *testing.T) []byte {
		t.Helper()
		var topic []byte
		for {
			if len(topic) > 0 {
				topic = append(topic, '/', '/')
			}
			topic = append(topic, 'a')
			subject := convertMQTTTopic(t, topic)
			if len(subject) > leafControlLimit-len(" 25 25") {
				return topic
			}
		}
	}

	for _, test := range []struct {
		name          string
		topic         []byte
		wantErr       bool
		wantExpansion bool
		wantArgLen    int
	}{
		{
			name:       "converted subject at leaf control line limit is accepted",
			topic:      bytes.Repeat([]byte{'a'}, leafControlLimit-len(" 25 25")),
			wantErr:    false,
			wantArgLen: leafControlLimit,
		},
		{
			name:       "literal topic exceeds leaf control line limit",
			topic:      bytes.Repeat([]byte{'a'}, leafControlLimit-len(" 25 25")+1),
			wantErr:    true,
			wantArgLen: leafControlLimit + 1,
		},
		{
			name:          "dot expansion can make a shorter topic exceed leaf control line limit",
			topic:         makeRepeatedLevelTopic(21845),
			wantErr:       true,
			wantExpansion: true,
		},
		{
			name:          "slash escaping can make a shorter topic exceed leaf control line limit",
			topic:         makeSlashExpansionTopic(t),
			wantErr:       true,
			wantExpansion: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			subject := convertMQTTTopic(t, test.topic)
			if test.wantArgLen > 0 {
				require_Equal(t, leafHMSGArgLen(subject), test.wantArgLen)
			}
			if test.wantExpansion {
				require_True(t, len(test.topic) <= len(subject))
			}
			if err := parseLeafHMSG(subject); test.wantErr {
				require_Error(t, err, ErrMaxControlLine)
			} else {
				require_NoError(t, err)
			}
		})
	}

	for _, test := range []struct {
		name  string
		topic []byte
	}{
		{name: "space", topic: []byte("foo bar")},
		{name: "tab", topic: []byte("foo\tbar")},
		{name: "carriage return", topic: []byte("foo\rbar")},
		{name: "line feed", topic: []byte("foo\nbar")},
		{name: "carriage return line feed", topic: []byte("foo\r\nbar")},
		{name: "null", topic: []byte("foo\x00bar")},
		{name: "invalid utf8", topic: []byte{0xff}},
		{name: "single level wildcard", topic: []byte("foo/+")},
		{name: "multi level wildcard", topic: []byte("foo/#")},
	} {
		t.Run("rejected before leaf forwarding/"+test.name, func(t *testing.T) {
			if err := mqttValidateTopic(test.topic, "topic"); err == nil {
				_, err = mqttTopicToNATSPubSubject(test.topic)
				require_Error(t, err)
			}
		})
	}
}

// TestMQTTSubRetainedRace and its two helpers poll s.lookupAccount(...).
// lookupStream(...).state(), upstream's UNEXPORTED internal JetStream stream
// API (package server), inaccessible from this package. See docs/PORTING.md.
func testMQTTNewSubRetainedRace(t *testing.T, s *Server, o *Options, subTopic, pubTopic string, QOS byte) {
	expectedFlags := (QOS << 1) | mqttPubFlagRetain
	payload := []byte("testmsg")

	pubID := nuid.Next()
	pubc, pubr := testMQTTConnectRetry(t, &mqttConnInfo{clientID: pubID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 3)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	defer testMQTTDisconnectEx(t, pubc, nil, true)
	defer pubc.Close()
	testMQTTPublish(t, pubc, pubr, QOS, false, true, pubTopic, 1, payload)

	// Wait for retained messages stream to be populated.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		acc, err := s.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		mset, err := acc.lookupStream(mqttRetainedMsgsStreamName)
		if err != nil {
			return err
		}
		if mset.state().Msgs != 1 {
			return errors.New("retained message not populated yet")
		}
		return nil
	})

	subID := nuid.Next()
	subc, subr := testMQTTConnect(t, &mqttConnInfo{clientID: subID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, subc, subr, []*mqttFilter{{filter: subTopic, qos: QOS}}, []byte{QOS})

	testMQTTCheckPubMsg(t, subc, subr, pubTopic, expectedFlags, payload)
	if QOS == 2 {
		testMQTTCheckPubAck(t, subr, mqttPacketPubRel)
		testMQTTSendPIPacket(mqttPacketPubComp, t, subc, 1)
	}

	testMQTTDisconnectEx(t, subc, nil, true)
	subc.Close()
}

func testMQTTNewSubWithExistingTopLevelRetainedRace(t *testing.T, s *Server, o *Options, subTopic, pubTopic string, QOS byte) {
	expectedFlags := (QOS << 1) | mqttPubFlagRetain
	payload := []byte("testmsg")

	pubID := nuid.Next()
	pubc, pubr := testMQTTConnectRetry(t, &mqttConnInfo{clientID: pubID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 3)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	defer testMQTTDisconnectEx(t, pubc, nil, true)
	defer pubc.Close()

	subID := nuid.Next()
	subc, subr := testMQTTConnect(t, &mqttConnInfo{clientID: subID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)

	// Subscribe to `#` first.
	testMQTTSub(t, 1, subc, subr, []*mqttFilter{{filter: `#`, qos: 0}}, []byte{0})
	testMQTTExpectNothing(t, subr)

	// Publish the retained message with QOS2, see that the `#` subscriber gets it.
	testMQTTPublish(t, pubc, pubr, 2, false, true, pubTopic, 1, payload)
	testMQTTCheckPubMsg(t, subc, subr, pubTopic, 0, payload)
	testMQTTExpectNothing(t, subr)

	// Wait for retained messages stream to be populated.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		acc, err := s.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		mset, err := acc.lookupStream(mqttRetainedMsgsStreamName)
		if err != nil {
			return err
		}
		if mset.state().Msgs != 1 {
			return errors.New("retained message not populated yet")
		}
		return nil
	})

	// Now subscribe to the topic we want to test. We should get the retained
	// message there.
	testMQTTSub(t, 1, subc, subr, []*mqttFilter{{filter: subTopic, qos: QOS}}, []byte{QOS})
	testMQTTCheckPubMsg(t, subc, subr, pubTopic, expectedFlags, payload)
	if QOS == 2 {
		testMQTTCheckPubAck(t, subr, mqttPacketPubRel)
		testMQTTSendPIPacket(mqttPacketPubComp, t, subc, 1)
	}

	testMQTTDisconnectEx(t, subc, nil, true)
	subc.Close()
}

func TestMQTTSubRetainedRace(t *testing.T) {
	o := testMQTTDefaultOptions()

	useCases := []struct {
		name string
		f    func(t *testing.T, s *Server, o *Options, subTopic, pubTopic string, QOS byte)
	}{
		{"new top level", testMQTTNewSubRetainedRace},
		{"existing top level", testMQTTNewSubWithExistingTopLevelRetainedRace},
	}
	pubTopic := "/bar"
	subTopics := []string{"#", "/bar", "/+", "/#"}
	QOS := []byte{0, 1, 2}
	for _, tc := range useCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, subTopic := range subTopics {
				t.Run(subTopic, func(t *testing.T) {
					for _, qos := range QOS {
						t.Run(fmt.Sprintf("QOS%d", qos), func(t *testing.T) {
							s := testMQTTRunServer(t, o)
							defer testMQTTShutdownServer(s)

							tc.f(t, s, o, subTopic, pubTopic, qos)
						})
					}
				})
			}
		})
	}
}

// TestMQTTSubWithNATSStream uses Account.addStream (upstream's unexported
// internal JetStream API). TestMQTTPublishRetainPermViolation uses
// Server.ReloadOptions (unported config-reload machinery). See docs/PORTING.md.
func TestMQTTSubWithNATSStream(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	mcp, rp := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	testMQTTFlush(t, mcp, nil, rp)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sc := &StreamConfig{
		Name:      "test",
		Storage:   FileStorage,
		Retention: InterestPolicy,
		Subjects:  []string{"foo.>"},
	}
	mset, err := s.GlobalAccount().addStream(sc)
	if err != nil {
		t.Fatalf("Unable to create stream: %v", err)
	}

	sub := natsSubSync(t, nc, "bar")
	cc := &ConsumerConfig{
		Durable:        "dur",
		AckPolicy:      AckExplicit,
		DeliverSubject: "bar",
	}
	if _, err := mset.addConsumer(cc); err != nil {
		t.Fatalf("Unable to add consumer: %v", err)
	}

	// Now send message from NATS
	resp, err := nc.Request("foo.bar", []byte("nats"), time.Second)
	if err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	ar := &ApiResponse{}
	if err := json.Unmarshal(resp.Data, ar); err != nil || ar.Error != nil {
		t.Fatalf("Unexpected response: err=%v resp=%+v", err, ar.Error)
	}

	// Check that message is received by both
	checkRecv := func(content string, flags byte) {
		t.Helper()
		if msg := natsNexMsg(t, sub, time.Second); string(msg.Data) != content {
			t.Fatalf("Expected %q, got %q", content, msg.Data)
		}
		testMQTTCheckPubMsg(t, mc, r, "foo/bar", flags, []byte(content))
	}
	checkRecv("nats", 0)

	// Send from MQTT as a QoS0
	testMQTTPublish(t, mcp, rp, 0, false, false, "foo/bar", 0, []byte("qos0"))
	checkRecv("qos0", 0)

	// Send from MQTT as a QoS1
	testMQTTPublish(t, mcp, rp, 1, false, false, "foo/bar", 1, []byte("qos1"))
	checkRecv("qos1", mqttPubQos1)
}

func TestMQTTPublishRetainPermViolation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "mqtt1",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo"}},
				Subscribe: &SubjectPermission{Allow: []string{"bar"}},
			},
		},
		{
			Username: "mqtt2",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo", "bar"}},
				Subscribe: &SubjectPermission{Allow: []string{">"}},
			},
		},
		{
			Username: "mqtt3",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.bar", "baz", "barbaz"}},
				Subscribe: &SubjectPermission{Allow: []string{">"}},
			},
		},
		{
			Username: "mqtt4",
			Password: "pass",
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var asm *mqttAccountSessionManager

	pubRetained := func(user, subject string) {
		t.Helper()
		mc, rs := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			clientID:  "pub",
			user:      user,
			pass:      "pass",
		}, o.MQTT.Host, o.MQTT.Port)
		defer mc.Close()
		testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, mc, rs, 0, false, true, subject, 0, []byte("retained"))
		testMQTTFlush(t, mc, nil, rs)
		if asm == nil {
			asm = testMQTTGetAccountSessionManager(t, s, "pub")
		}
		testMQTTDisconnect(t, mc, nil)
	}
	consumeRetained := func(user, subject string, expected bool) {
		t.Helper()
		mc, rs := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			user:      user,
			pass:      "pass",
		}, o.MQTT.Host, o.MQTT.Port)
		defer mc.Close()
		testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, mc, rs, []*mqttFilter{{filter: subject, qos: 0}}, []byte{0})
		if expected {
			testMQTTCheckPubMsg(t, mc, rs, subject, mqttPubFlagRetain, []byte("retained"))
		} else {
			testMQTTExpectNothing(t, rs)
		}
		testMQTTDisconnect(t, mc, nil)
	}

	// With user "mqtt", publish a retained message on "bar".
	// Since this user has no permission, the server should not have stored it.
	pubRetained("mqtt1", "bar")

	// Verify that we can't get it with a new subscription.
	consumeRetained("mqtt1", "bar", false)

	// Use the user "mqtt2" that has permissions to publish on foo and bar.
	// Publish on "foo" and check retained message can be received.
	pubRetained("mqtt2", "foo")
	consumeRetained("mqtt2", "foo", true)

	// For user "mqtt3", we will publish on "foo/bar" and check retained
	// message is properly received.
	pubRetained("mqtt3", "foo/bar")
	consumeRetained("mqtt3", "foo/bar", true)

	// Simulate a message that would have been produced in a different server
	// on subject "barbaz". We will use user "mqtt4" that has no pub permissions
	// since we need to send to low-level "$MQTT.rmsgs.barbaz" subject...
	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("mqtt4", "pass"))
	defer nc.Close()
	msg := nats.NewMsg("$MQTT.rmsgs.barbaz")
	msg.Header.Set(mqttNatsRetainedMessageOrigin, "SomeOtherServer")
	msg.Header.Set(mqttNatsRetainedMessageTopic, "barbaz")
	msg.Header.Set(mqttNatsRetainedMessageFlags, "1")
	msg.Data = []byte("retained")
	nc.PublishMsg(msg)
	natsFlush(t, nc)

	// Wait a bit to make sure it is processed.
	time.Sleep(250 * time.Millisecond)

	// Then check that it can be received
	consumeRetained("mqtt3", "barbaz", true)

	// Same with user "mqtt4" that does not have permissions defined, which
	// means allowed to pub/sub on everything.
	pubRetained("mqtt4", "bat")
	consumeRetained("mqtt4", "bat", true)

	// Do a config reload and make sure that the server does not panic
	// and we can still get the retained messages.
	no := *o
	// Remove the "bar" publish permission from "mqtt2"
	no.Users[1].Permissions.Publish = &SubjectPermission{Allow: []string{"foo"}}
	// And the "foo.bar" and "barbaz" publish permissions from "mqtt3"
	no.Users[2].Permissions.Publish = &SubjectPermission{Allow: []string{"baz"}}
	err := s.ReloadOptions(&no)
	require_NoError(t, err)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		asm.mu.RLock()
		defer asm.mu.RUnlock()
		if ref, ok := asm.retmsgs.Find([]byte("foo.bar")); ok && !ref.deleted {
			return errors.New("foo.bar subject still in map")
		}
		return nil
	})

	// Still message on "bar" should not exist
	consumeRetained("mqtt1", "bar", false)
	// This one should still be able to be received
	consumeRetained("mqtt2", "foo", true)
	// Retained message on "foo.bar" should have been removed.
	consumeRetained("mqtt3", "foo/bar", false)
	// However, message on "barbaz" should have been left alone since it
	// was produced on a different server.
	consumeRetained("mqtt3", "barbaz", true)
	// And finally, this user that had no permission should still be able
	// to get the retained message on "bat".
	consumeRetained("mqtt4", "bat", true)
}
