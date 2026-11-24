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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// A SUBSCRIBE filter refused while JetStream did not answer closes the connection (issue #187).
// Every reach witness drops the REAL request on its route with testDropRequest — the store that
// cannot answer — and proves through the matcher's own count that the request was dropped before
// it asserts anything about the outcome.

// testUnansweredJSAPITimeout keeps a dropped request's wait short. It is an option, never written
// into a running manager.
const testUnansweredJSAPITimeout = time.Second

func testUnansweredOptions(v5 bool) *Options {
	o := testMQTTDefaultOptions()
	if v5 {
		o = testMQTTDefaultOptionsV5()
	}
	o.MQTT.JSAPITimeout = testUnansweredJSAPITimeout
	return o
}

// testArmUnansweredDrop makes the account's assistant swallow every request match accepts, and
// returns how many it swallowed.
func testArmUnansweredDrop(t *testing.T, asm *mqttAccountSessionManager, match func(subject string, body []byte) bool) *atomic.Int32 {
	t.Helper()
	var hits atomic.Int32
	drop := func(subject string, body []byte) bool {
		if match(subject, body) {
			hits.Add(1)
			return true
		}
		return false
	}
	asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { asm.jsa.testDropRequest.Store(nil) })
	return &hits
}

// testMatchConsumerCreate matches the durable create on stream whose filter subject is exactly
// filterSubject (the stream's subject prefix included).
func testMatchConsumerCreate(stream, filterSubject string, durable *atomic.Value) func(string, []byte) bool {
	prefix := fmt.Sprintf(JSApiDurableCreateT, stream, _EMPTY_)
	return func(subject string, body []byte) bool {
		if !strings.HasPrefix(subject, prefix) {
			return false
		}
		var req CreateConsumerRequest
		if json.Unmarshal(body, &req) != nil || req.Config.FilterSubject != filterSubject {
			return false
		}
		if durable != nil {
			durable.Store(req.Config.Durable)
		}
		return true
	}
}

// testMatchRecordRead matches a last-message read of recordSubject on stream.
func testMatchRecordRead(stream, recordSubject string) func(string, []byte) bool {
	api := fmt.Sprintf(JSApiMsgGetT, stream)
	return func(subject string, body []byte) bool {
		if subject != api {
			return false
		}
		var req JSApiMsgGetRequest
		return json.Unmarshal(body, &req) == nil && req.LastFor == recordSubject
	}
}

// testUnansweredWitnessConsumer is a consumer name only a witness request asks for.
func testUnansweredWitnessConsumer() string { return "issue187-witness-" + nuid.Next() }

// testMatchWitnessLookup matches the info request for a witness consumer name.
func testMatchWitnessLookup(subject string, _ []byte) bool {
	return strings.Contains(subject, "issue187-witness-")
}

// testIssueUnansweredRequest makes one request on the account's assistant go unanswered, as
// another session's would. The caller must have armed a drop matching testMatchWitnessLookup.
func testIssueUnansweredRequest(t *testing.T, jsa *mqttJSA) {
	t.Helper()
	if _, err := jsa.lookupConsumer(mqttStreamName, testUnansweredWitnessConsumer()); !errors.Is(err, errMQTTJSARequestTimeout) {
		t.Errorf("the witness request was answered (%v): the drop is not armed", err)
	}
}

// testMQTTLoadRecordSubs reads the session record's memberships.
func testMQTTLoadRecordSubs(t *testing.T, asm *mqttAccountSessionManager, clientID string) map[string]byte {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(clientID))
	if err != nil {
		t.Fatalf("loading the session record of %q: %v", clientID, err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the session record of %q: %v", clientID, err)
	}
	return ps.Subs
}

// testExpectClosedWithoutSubAck reads what the server sends after a SUBSCRIBE that must close:
// nothing for 3.1.1, a DISCONNECT with reason for v5, then the end of the connection.
func testExpectClosedWithoutSubAck(t *testing.T, c net.Conn, r *mqttReader, v5 bool, reason byte) {
	t.Helper()
	if v5 {
		if got, _ := testMQTTReadDisconnectV5(t, r); got != reason {
			t.Fatalf("DISCONNECT reason 0x%02x, want 0x%02x", got, reason)
		}
	}
	testMQTTExpectDisconnect(t, c)
}

// testArmSourcePreCreate installs the source consumer's pre-create seam for the duration of the
// test.
func testArmSourcePreCreate(t *testing.T, asm *mqttAccountSessionManager, h func(sess *mqttSession, durable string) error) {
	t.Helper()
	asm.mu.Lock()
	asm.testSourcePreCreate = h
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testSourcePreCreate = nil
		asm.mu.Unlock()
	})
}

// testArmSubFilterFail installs the live SUBSCRIBE exit seam for the duration of the test.
func testArmSubFilterFail(t *testing.T, asm *mqttAccountSessionManager, h func(exit, sid string) error) {
	t.Helper()
	asm.mu.Lock()
	asm.testSubFilterFail = h
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testSubFilterFail = nil
		asm.mu.Unlock()
	})
}

// testDefiniteCreateRefusal is a JetStream answer, not a silence: a consumer name already in use
// with another configuration.
func testDefiniteCreateRefusal() error {
	return &ApiError{Code: 400, ErrCode: 10148, Description: "consumer already exists"}
}

// TestSubscribeConsumerCreateUnansweredClosesWithoutSubAck: a packet whose second filter's
// consumer create goes unanswered is not SUBACKed; the connection closes (v5: DISCONNECT 0x89)
// after the packet's final save, so the healthy filter is in the record and resumes, the failed
// one is not, and the failed create's claimed name is retired.
func TestSubscribeConsumerCreateUnansweredClosesWithoutSubAck(t *testing.T) {
	for _, v5 := range []bool{false, true} {
		t.Run(map[bool]string{false: "3.1.1", true: "5.0"}[v5], func(t *testing.T) {
			o := testUnansweredOptions(v5)
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			clientID := fmt.Sprintf("unanswered-create-%v", v5)

			// A retained message on the healthy filter: the closed packet must not replay it.
			if v5 {
				pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
				testMQTTPublish(t, pc, pr, 0, false, true, "ok/topic", 0, []byte("retained"))
				testMQTTFlush(t, pc, nil, pr)
				pc.Close()
			}

			var c net.Conn
			var r *mqttReader
			if v5 {
				c, r = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true,
					props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}, o.MQTT.Host, o.MQTT.Port)
				testMQTTReadConnAckV5(t, r)
			} else {
				c, r = testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			}
			defer c.Close()
			asm := testMQTTGetAccountSessionManager(t, s, clientID)
			sess := testMQTTResidentSession(t, s, clientID)

			var durable atomic.Value
			hits := testArmUnansweredDrop(t, asm, testMatchConsumerCreate(mqttStreamName, mqttStreamSubjectPrefix+"bad.topic", &durable))
			if v5 {
				testMQTTSubV5NoSubAck(t, c, 1, 0, []mqttV5SubFilter{{topic: "ok/topic", opts: 1}, {topic: "bad/topic", opts: 1}})
			} else {
				testMQTTSendSubOnly(t, 1, c, []*mqttFilter{{filter: "ok/topic", qos: 1}, {filter: "bad/topic", qos: 1}})
			}
			testExpectClosedWithoutSubAck(t, c, r, v5, mqttReasonServerBusy)
			asm.jsa.testDropRequest.Store(nil)

			if hits.Load() == 0 {
				t.Fatal("fixture: the failing filter's consumer create was never dropped")
			}
			// The record keys a filter by its NATS subject form.
			subs := testMQTTLoadRecordSubs(t, asm, clientID)
			if _, ok := subs["ok.topic"]; !ok {
				t.Fatalf("the healthy filter is not in the record %v: the close skipped the packet's final save", subs)
			}
			if _, ok := subs["bad.topic"]; ok {
				t.Fatalf("the failed filter is in the record %v", subs)
			}
			name, _ := durable.Load().(string)
			checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
				own, _, _ := sess.jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name)
				if own == nil || own.State != mqttNameOwnRetired {
					return fmt.Errorf("the failed create's claimed name %q is not retired: %+v", name, own)
				}
				return nil
			})

			if v5 {
				return
			}
			// The healthy filter resumes and delivers.
			c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
			pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer pc.Close()
			testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, pc, pr, 1, false, false, "ok/topic", 1, []byte("after"))
			testMQTTCheckPubMsg(t, c2, r2, "ok/topic", mqttPubQos1, []byte("after"))
		})
	}
}

// testUnansweredClosesV5 connects a v5 client, arms match, sends filters, and expects the close
// with 0x89 after the matched request was dropped.
func testUnansweredClosesV5(t *testing.T, clientID string, filters []mqttV5SubFilter, match func(asm *mqttAccountSessionManager, sess *mqttSession) func(string, []byte) bool) {
	t.Helper()
	o := testUnansweredOptions(true)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true,
		props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	sess := testMQTTResidentSession(t, s, clientID)
	hits := testArmUnansweredDrop(t, asm, match(asm, sess))
	testMQTTSubV5NoSubAck(t, c, 1, 0, filters)
	testExpectClosedWithoutSubAck(t, c, r, true, mqttReasonServerBusy)
	if hits.Load() == 0 {
		t.Fatal("fixture: the route's request was never dropped")
	}
}

// TestSubscribeCompanionCreateUnansweredCloses: only the level-up companion's create of a
// multi-level filter goes unanswered.
func TestSubscribeCompanionCreateUnansweredCloses(t *testing.T) {
	var main atomic.Int32
	testUnansweredClosesV5(t, "unanswered-companion", []mqttV5SubFilter{{topic: "a/#", opts: 1}},
		func(*mqttAccountSessionManager, *mqttSession) func(string, []byte) bool {
			companion := testMatchConsumerCreate(mqttStreamName, mqttStreamSubjectPrefix+"a", nil)
			mainCreate := testMatchConsumerCreate(mqttStreamName, mqttStreamSubjectPrefix+"a.>", nil)
			return func(subject string, body []byte) bool {
				if mainCreate(subject, body) {
					main.Add(1)
				}
				return companion(subject, body)
			}
		})
	if main.Load() == 0 {
		t.Fatal("fixture: the main create was never issued, so the companion was not the one refused")
	}
}

// TestSubscribePubRelConsumerUnansweredCloses: the first QoS 2 filter of a session provisions the
// PUBREL consumer, and its create goes unanswered.
func TestSubscribePubRelConsumerUnansweredCloses(t *testing.T) {
	testUnansweredClosesV5(t, "unanswered-pubrel", []mqttV5SubFilter{{topic: "q2/topic", opts: 2}},
		func(*mqttAccountSessionManager, *mqttSession) func(string, []byte) bool {
			prefix := fmt.Sprintf(JSApiDurableCreateT, mqttOutStreamName, _EMPTY_)
			return func(subject string, _ []byte) bool { return strings.HasPrefix(subject, prefix) }
		})
}

// TestSubscribeSourceClaimUnansweredCloses: the source consumer's name claim reads the outbound
// authority and, through its inventory boundary, the generation closure; either read going
// unanswered through the claim's whole retry budget refuses the filter while JetStream is silent.
func TestSubscribeSourceClaimUnansweredCloses(t *testing.T) {
	t.Run("authority", func(t *testing.T) {
		testUnansweredClosesV5(t, "unanswered-claim-auth", []mqttV5SubFilter{{topic: "claim/topic", opts: 1}},
			func(_ *mqttAccountSessionManager, sess *mqttSession) func(string, []byte) bool {
				return testMatchRecordRead(mqttSessStreamName, mqttOutboundAuthoritySubject(getHash(sess.id)))
			})
	})
	t.Run("generation closure", func(t *testing.T) {
		testUnansweredClosesV5(t, "unanswered-claim-closure", []mqttV5SubFilter{{topic: "claim/topic", opts: 1}},
			func(_ *mqttAccountSessionManager, sess *mqttSession) func(string, []byte) bool {
				sess.mu.Lock()
				gen := sess.gen
				sess.mu.Unlock()
				return testMatchRecordRead(mqttSessStreamName, mqttGenClosureSubject(getHash(sess.id), gen))
			})
	})
}

// TestSharedSubscribeUnansweredCloses: a fresh shared membership whose group consumer create, or
// whose acquisition's ledger read, goes unanswered.
func TestSharedSubscribeUnansweredCloses(t *testing.T) {
	t.Run("group consumer create", func(t *testing.T) {
		testUnansweredClosesV5(t, "unanswered-shared-create", []mqttV5SubFilter{{topic: "$share/g/shared/topic", opts: 1}},
			func(*mqttAccountSessionManager, *mqttSession) func(string, []byte) bool {
				return testMatchConsumerCreate(mqttStreamName, mqttStreamSubjectPrefix+"shared.topic", nil)
			})
	})
	t.Run("acquisition ledger read", func(t *testing.T) {
		testUnansweredClosesV5(t, "unanswered-shared-ledger", []mqttV5SubFilter{{topic: "$share/g/shared/topic", opts: 1}},
			func(_ *mqttAccountSessionManager, sess *mqttSession) func(string, []byte) bool {
				sess.mu.Lock()
				gen := sess.gen
				sess.mu.Unlock()
				return testMatchRecordRead(mqttSessStreamName, mqttSharedLedgerSubject(getHash(sess.id), gen))
			})
	})
}

// TestSubscribeRawFlushUnansweredCloses: a core-NATS flush with no PONG on the raw subscription,
// which no assistant observes, is read from the refusal's error.
func TestSubscribeRawFlushUnansweredCloses(t *testing.T) {
	for _, exit := range []string{"raw", "raw-fwc"} {
		t.Run(exit, func(t *testing.T) {
			testFlushRefusalCloses(t, "unanswered-"+exit, exit)
		})
	}
}

// TestSubscribeDeliveryFlushUnansweredCloses: the flush processSub makes when it installs a
// JetStream consumer's delivery subscription reaches the consumer arms unchanged, with the
// assistant's sequence clean.
func TestSubscribeDeliveryFlushUnansweredCloses(t *testing.T) {
	for _, exit := range []string{"consumer", "consumer-fwc"} {
		t.Run(exit, func(t *testing.T) {
			testFlushRefusalCloses(t, "unanswered-"+exit, exit)
		})
	}
}

func testFlushRefusalCloses(t *testing.T, clientID, exit string) {
	t.Helper()
	o := testUnansweredOptions(true)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	mark := asm.jsa.unansweredMark()
	var fired atomic.Bool
	testArmSubFilterFail(t, asm, func(e, _ string) error {
		if e != exit {
			return nil
		}
		fired.Store(true)
		return fmt.Errorf("subscribe: %w", nats.ErrTimeout)
	})
	testMQTTSubV5NoSubAck(t, c, 1, 0, []mqttV5SubFilter{{topic: "f/#", opts: 1}})
	testExpectClosedWithoutSubAck(t, c, r, true, mqttReasonServerBusy)
	if !fired.Load() {
		t.Fatalf("fixture: the %s exit was never reached", exit)
	}
	if asm.jsa.unansweredSince(mark) {
		t.Fatal("fixture: the assistant's sequence moved, so this does not witness the flush fallback")
	}
}

// testWindowPacket connects a v5 client and returns what a window test needs.
func testWindowPacket(t *testing.T, clientID string) (*Server, net.Conn, *mqttReader, *mqttAccountSessionManager, *atomic.Int32, func()) {
	t.Helper()
	o := testUnansweredOptions(true)
	s := testMQTTRunServer(t, o)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	hits := testArmUnansweredDrop(t, asm, testMatchWitnessLookup)
	return s, c, r, asm, hits, func() { c.Close(); testMQTTShutdownServer(s) }
}

// TestDefiniteRefusalWithACleanWindowKeepsTheConnection: a refusal JetStream decided is still a
// per-filter 0x80, and the connection serves on.
func TestDefiniteRefusalWithACleanWindowKeepsTheConnection(t *testing.T) {
	_, c, r, asm, _, done := testWindowPacket(t, "unanswered-definite")
	defer done()
	testArmSourcePreCreate(t, asm, func(*mqttSession, string) error { return testDefiniteCreateRefusal() })
	if got := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "definite/topic", opts: 1}}); len(got) != 1 || got[0] != mqttSubAckFailure {
		t.Fatalf("SUBACK %v, want [0x80]", got)
	}
	testMQTTFlush(t, c, nil, r)
}

// TestDefiniteRefusalOverlappingAnUnansweredRequestCloses pins the stated over-attribution: the
// sequence is the account's, so a definite refusal whose window saw another request go unanswered
// closes rather than answering 0x80.
func TestDefiniteRefusalOverlappingAnUnansweredRequestCloses(t *testing.T) {
	_, c, r, asm, hits, done := testWindowPacket(t, "unanswered-overlap")
	defer done()
	testArmSourcePreCreate(t, asm, func(sess *mqttSession, _ string) error {
		testIssueUnansweredRequest(t, sess.jsa)
		return testDefiniteCreateRefusal()
	})
	testMQTTSubV5NoSubAck(t, c, 1, 0, []mqttV5SubFilter{{topic: "overlap/topic", opts: 1}})
	testExpectClosedWithoutSubAck(t, c, r, true, mqttReasonServerBusy)
	if hits.Load() == 0 {
		t.Fatal("fixture: the overlapping request was never dropped")
	}
}

// TestSuccessfulFilterIgnoresItsWindow: a filter that succeeded is granted whatever its window saw.
func TestSuccessfulFilterIgnoresItsWindow(t *testing.T) {
	_, c, r, asm, hits, done := testWindowPacket(t, "unanswered-success")
	defer done()
	testArmSourcePreCreate(t, asm, func(sess *mqttSession, _ string) error {
		testIssueUnansweredRequest(t, sess.jsa)
		return nil
	})
	if got := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "success/topic", opts: 1}}); len(got) != 1 || got[0] != 1 {
		t.Fatalf("SUBACK %v, want [1]", got)
	}
	testMQTTFlush(t, c, nil, r)
	if hits.Load() == 0 {
		t.Fatal("fixture: the overlapping request was never dropped")
	}
}

// TestAnEarlierFiltersWindowDoesNotClassifyALaterRefusal: each filter's window is its own.
func TestAnEarlierFiltersWindowDoesNotClassifyALaterRefusal(t *testing.T) {
	_, c, r, asm, hits, done := testWindowPacket(t, "unanswered-earlier")
	defer done()
	var calls atomic.Int32
	testArmSourcePreCreate(t, asm, func(sess *mqttSession, _ string) error {
		if calls.Add(1) == 1 {
			testIssueUnansweredRequest(t, sess.jsa)
			return nil
		}
		return testDefiniteCreateRefusal()
	})
	got := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "first/topic", opts: 1}, {topic: "second/topic", opts: 1}})
	if len(got) != 2 || got[0] != 1 || got[1] != mqttSubAckFailure {
		t.Fatalf("SUBACK %v, want [1 0x80]", got)
	}
	testMQTTFlush(t, c, nil, r)
	if hits.Load() == 0 {
		t.Fatal("fixture: the first filter's request was never dropped")
	}
}

// TestRetainedPreloadObservationDoesNotClassifyARefusal: the retained preload runs between the two
// passes; a read of it that goes unanswered is outside every second-pass window.
func TestRetainedPreloadObservationDoesNotClassifyARefusal(t *testing.T) {
	o := testUnansweredOptions(true)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "unanswered-preload-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 0, false, true, "preload/topic", 0, []byte("retained"))
	testMQTTFlush(t, pc, nil, pr)
	pc.Close()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unanswered-preload", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, "unanswered-preload")
	api := fmt.Sprintf(JSApiMsgGetT, mqttRetainedMsgsStreamName)
	hits := testArmUnansweredDrop(t, asm, func(subject string, _ []byte) bool { return subject == api })
	testArmSourcePreCreate(t, asm, func(*mqttSession, string) error { return testDefiniteCreateRefusal() })
	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "preload/topic", opts: 1}})
	if len(codes) != 1 || codes[0] != mqttSubAckFailure {
		t.Fatalf("SUBACK %v, want [0x80]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	if hits.Load() == 0 {
		t.Fatal("fixture: no retained read was dropped during the preload")
	}
}

// TestCompensationAfterARefusalIsOutsideItsWindow: the refusing site's own compensation runs after
// the comparison, so a compensation that goes unanswered does not turn a refusal the store had
// decided into a close.
func TestCompensationAfterARefusalIsOutsideItsWindow(t *testing.T) {
	s, c, r, asm, hits, done := testWindowPacket(t, "unanswered-compensation")
	defer done()
	testArmSubFilterFail(t, asm, func(exit, _ string) error {
		if exit == "consumer" {
			return errors.New("test: a definite refusal after the consumer was created")
		}
		return nil
	})
	sess := testMQTTResidentSession(t, s, "unanswered-compensation")
	var compensated atomic.Bool
	h := func(site, _ string) {
		if site != "failed-filter" {
			return
		}
		compensated.Store(true)
		testIssueUnansweredRequest(t, sess.jsa)
	}
	sess.testSourceDisposalSite.Store(&h)
	t.Cleanup(func() { sess.testSourceDisposalSite.Store(nil) })
	if got := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "comp/topic", opts: 1}}); len(got) != 1 || got[0] != mqttSubAckFailure {
		t.Fatalf("SUBACK %v, want [0x80]", got)
	}
	testMQTTFlush(t, c, nil, r)
	if !compensated.Load() || hits.Load() == 0 {
		t.Fatal("fixture: the refusal's compensation never issued its unanswered request")
	}
}

// TestSharedMembershipSaveRefusalOverlappingAnUnansweredRequest: a shared membership save that the
// server definitely CAS-rejects while a request goes unanswered in its window is classified. The
// packet then closes on its own final-save rejection, so the classification is read at the
// processSubs boundary rather than from the socket.
func TestSharedMembershipSaveRefusalOverlappingAnUnansweredRequest(t *testing.T) {
	o := testUnansweredOptions(true)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const clientID = "unanswered-membership"
	const key = "$share/gm/member/topic"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true,
		props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	sess := testMQTTResidentSession(t, s, clientID)
	hash := string(getHash(clientID))
	hits := testArmUnansweredDrop(t, asm, testMatchWitnessLookup)

	var saves atomic.Int32
	var displaced atomic.Bool
	restoreSave := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess || saves.Add(1) != 1 {
			return nil
		}
		// The membership save: a request goes unanswered in its window, then the record is
		// appended unconditionally so the server rejects the save's CAS.
		testIssueUnansweredRequest(t, hs.jsa)
		smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, hash)
		if err != nil {
			t.Errorf("reading the record to displace it: %v", err)
			return nil
		}
		if _, err := asm.jsa.storeSessionMsg(asm.domainTk, hash, 0, smsg.Data); err != nil {
			t.Errorf("displacing the record: %v", err)
			return nil
		}
		displaced.Store(true)
		return nil
	})
	defer restoreSave()

	var mu sync.Mutex
	var seen *mqttUnansweredRefusal
	var observed bool
	h := func(facts mqttSubPacketFacts, _ error) {
		mu.Lock()
		defer mu.Unlock()
		seen, observed = facts.jsUnanswered, true
	}
	mqttTestSubPacketFactsHookV.Store(&h)
	defer mqttTestSubPacketFactsHookV.Store(nil)

	testMQTTSubV5NoSubAck(t, c, 1, 0, []mqttV5SubFilter{{topic: key, opts: 1}})
	testMQTTReadDisconnectV5(t, r)
	testMQTTExpectDisconnect(t, c)
	if !displaced.Load() || hits.Load() == 0 {
		t.Fatal("fixture: the membership save was not both overlapped and CAS-rejected")
	}
	mu.Lock()
	defer mu.Unlock()
	if !observed {
		t.Fatal("fixture: the packet never returned from processSubs")
	}
	// The broker keys a filter by its NATS subject form.
	if seen == nil || seen.filter != "member.topic" {
		t.Fatalf("the CAS-rejected membership save overlapping an unanswered request was not classified: %+v", seen)
	}
}

// TestSubscribeSuccessTailRunsBeforeTheClose: a QoS 0 re-SUBSCRIBE in the closing packet still has
// its deferred consumer teardown executed, because the close is attached after the success tail.
func TestSubscribeSuccessTailRunsBeforeTheClose(t *testing.T) {
	o := testUnansweredOptions(true)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const clientID = "unanswered-tail"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true,
		props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	if got := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "down/topic", opts: 1}}); len(got) != 1 || got[0] != 1 {
		t.Fatalf("SUBACK %v, want [1]", got)
	}
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	var tornDown atomic.Bool
	restore := setMQTTTestDeferredDeleteHook(func(sid, _ string) {
		if sid == "down.topic" {
			tornDown.Store(true)
		}
	})
	defer restore()
	hits := testArmUnansweredDrop(t, asm, testMatchConsumerCreate(mqttStreamName, mqttStreamSubjectPrefix+"bad.topic", nil))
	testMQTTSubV5NoSubAck(t, c, 2, 0, []mqttV5SubFilter{{topic: "down/topic", opts: 0}, {topic: "bad/topic", opts: 1}})
	testExpectClosedWithoutSubAck(t, c, r, true, mqttReasonServerBusy)
	if hits.Load() == 0 {
		t.Fatal("fixture: the failing create was never dropped")
	}
	if !tornDown.Load() {
		t.Fatal("the downgraded consumer's deferred teardown never ran: the close skipped the success tail")
	}
}
