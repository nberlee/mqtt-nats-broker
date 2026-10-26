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

//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// F-105 7f over real connections: the resume repair's predecessor decision, and fork repair's
// fence. Not parallel: they arm package-global seams.

// testSuppressSettlement keeps the resumed CONNECT's settlement job from running, so what a test
// observes is the route under test and not the settlement's backstop.
func testSuppressSettlement(t *testing.T) {
	t.Helper()
	h := func(string, string) bool { return false }
	mqttTestTenureSettleStartV.Store(&h)
	t.Cleanup(func() { mqttTestTenureSettleStartV.Store(nil) })
}

// testOwnerOf reads the resource a source consumer name's ownership names.
func testOwnerOf(t *testing.T, sess *mqttSession, name string) (*mqttNameOwnership, *mqttOutboundResource) {
	t.Helper()
	jsa := sess.jsa
	own, _, err := jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name)
	if err != nil || own == nil {
		t.Fatalf("the ownership of %q: %+v (%v)", name, own, err)
	}
	r, _, rerr := jsa.loadOutboundResource(sess.id, sess.asm.sessStreamEpoch, own.OwnerGen, own.OwnerInc)
	if rerr != nil || r == nil {
		t.Fatalf("the resource of %q: %+v (%v)", name, r, rerr)
	}
	return own, r
}

// testConsumerOf is the durable the session's subscription to key uses.
func testConsumerOf(sess *mqttSession, key string) string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if cc := sess.cons[key]; cc != nil {
		return cc.Durable
	}
	return _EMPTY_
}

// T9 (M7): the resume repair decides the durable it replaced `repair-replaced`, under the resumed
// connection's tenure, after its confirmed save; the settlement is held back so only the repair
// can have decided it.
func TestF105ResumeRepairDecidesItsPredecessor(t *testing.T) {
	testSuppressSettlement(t)
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "settle-t9", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "settle/t9", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	sess := testMQTTResidentSession(t, s, "settle-t9")
	old := testConsumerOf(sess, "settle.t9")
	if old == _EMPTY_ {
		t.Fatal("fixture: the subscription must have a consumer")
	}
	if _, err := sess.jsa.deleteConsumer(mqttStreamName, old, false); err != nil {
		t.Fatalf("deleting the durable: %v", err)
	}

	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	sess2 := testMQTTResidentSession(t, s, "settle-t9")
	if fresh := testConsumerOf(sess2, "settle.t9"); fresh == _EMPTY_ || fresh == old {
		t.Fatalf("the repair must have replaced the durable, got %q", fresh)
	}
	ent, ok := testMQTTGetClient(t, s, "settle-t9").outboundEntitlement()
	if !ok {
		t.Fatal("the resumed connection holds no entitlement")
	}
	checkFor(t, 15*time.Second, 25*time.Millisecond, func() error {
		_, res := testOwnerOf(t, sess2, old)
		if res.Disposal == nil {
			return fmt.Errorf("the predecessor is not decided yet")
		}
		if res.Disposal.Reason != mqttDisposalRepairReplaced || res.Disposal.Tenure != ent.Tenure {
			return fmt.Errorf("decided %q under %q, want %q under %q", res.Disposal.Reason,
				res.Disposal.Tenure, mqttDisposalRepairReplaced, ent.Tenure)
		}
		if res.State != mqttResourceCleanupComplete {
			return fmt.Errorf("the predecessor is decided but not yet retired: %d", res.State)
		}
		return nil
	})
	testMQTTPublish(t, c2, r2, 1, false, false, "settle/t9", 1, []byte("fresh"))
	testMQTTCheckPubMsg(t, c2, r2, "settle/t9", mqttPubQos1, []byte("fresh"))
}

// testForkWithConsumer leaves a client's session FORKED: our acknowledged record names the
// subscription to filter (and its claimed consumer), the surviving sibling does not. Returns the
// stale session object and our consumer's name.
func testForkWithConsumer(t *testing.T, s *Server, o *Options, ci *mqttConnInfo, filter, key string) (*mqttSession, string) {
	t.Helper()
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	asm := testMQTTGetGlobalASM(t, s)
	sess := testMQTTGetClient(t, s, ci.clientID).mqtt.sess
	ours := testConsumerOf(sess, key)
	if ours == _EMPTY_ {
		t.Fatal("fixture: the subscription must have a consumer")
	}
	idHash := sess.idHash
	asm.mu.Lock()
	asm.testSessPersistDrop = func(h string) bool { return h == idHash }
	asm.mu.Unlock()
	mqttTestSessStoreSuppressV.Store(true)
	_ = testSessSave(t, sess)
	mqttTestSessStoreSuppressV.Store(false)
	if !sess.isSeqUncertain() {
		t.Fatal("the suppressed save must leave the session unresolved")
	}
	testLandSiblingWithout(t, sess, key)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	return sess, ours
}

// testLandSiblingWithout lands a sibling of the session's acknowledged record that names neither
// the subscription key nor its consumer: the survivor this population needs, whose own tenure's
// settlement may decide a consumer its record does not name.
func testLandSiblingWithout(t *testing.T, sess *mqttSession, key string) {
	t.Helper()
	sess.mu.Lock()
	sibling := sess.buildPersistedSessionLocked()
	sibling.Gen = sess.gen
	sibling.PWID = sess.pwid
	sibling.WID = mqttNewRecordWID()
	sibling.Subs = map[string]byte{"forked/e2e/marker": 1}
	delete(sibling.Cons, key)
	domainTk, idHash, pwid := sess.domainTk, sess.idHash, sess.pwid
	sess.mu.Unlock()
	if pwid == _EMPTY_ {
		t.Fatal("fixture: the acknowledged record must have a parent")
	}
	body, err := json.Marshal(&sibling)
	if err != nil {
		t.Fatalf("marshal the sibling: %v", err)
	}
	if _, err := sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); err != nil {
		t.Fatalf("land the sibling record: %v", err)
	}
}

// testDecideAsTheSurvivor writes the decision the survivor's tenure would: our consumer, which its
// record does not name, disposed of under the entitlement serving now.
func testDecideAsTheSurvivor(t *testing.T, sess *mqttSession, name string) error {
	t.Helper()
	own, _ := testOwnerOf(t, sess, name)
	auth, _, err := sess.jsa.servingAuthority(sess.id, sess.asm.sessStreamEpoch)
	if err != nil {
		return err
	}
	_, derr := sess.jsa.opRequestDisposal(&mqttRequestDisposalRequest{ID: sess.id,
		Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Gen: own.OwnerGen, Inc: own.OwnerInc,
		Reason: mqttDisposalUnsubscribed, Entitlement: mqttOutboundEntitlement{Gen: auth.Gen, Tenure: auth.Tenure}})
	return derr
}

// testRequireForkRepairedWithoutX asserts the reconnect kept the membership, did NOT reinstate the
// decided consumer, and serves the filter through a fresh one.
func testRequireForkRepairedWithoutX(t *testing.T, s *Server, ci *mqttConnInfo, c2 interface{}, key, x string) *mqttSession {
	t.Helper()
	sess2 := testMQTTGetClient(t, s, ci.clientID).mqtt.sess
	sess2.mu.Lock()
	ps := sess2.buildPersistedSessionLocked()
	sess2.mu.Unlock()
	if _, ok := ps.Subs[key]; !ok {
		t.Fatalf("the membership must be re-asserted: %v", ps.Subs)
	}
	if cc := ps.Cons[key]; cc == nil || cc.Durable == x {
		t.Fatalf("the decided consumer %q must not be reinstated; the record names %+v", x, cc)
	}
	return sess2
}

// T16a (M13a): a consumer of ours that another tenure decided is not reinstated by fork repair;
// the membership keeps its grant and a fresh consumer serves it.
func TestF105ForkRepairDoesNotReinstateADecidedConsumer(t *testing.T) {
	testSuppressSettlement(t)
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	ci := &mqttConnInfo{clientID: "settle-t16a", cleanSess: false}
	sess, x := testForkWithConsumer(t, s, o, ci, "settle/t16a", "settle.t16a")
	if err := testDecideAsTheSurvivor(t, sess, x); err != nil {
		t.Fatalf("the survivor's decision: %v", err)
	}
	var mu sync.Mutex
	seen := map[string][]bool{}
	h := func(name string, serving bool, err error) {
		mu.Lock()
		seen[name] = append(seen[name], serving)
		mu.Unlock()
	}
	mqttTestForkPreflightClassifiedV.Store(&h)
	defer mqttTestForkPreflightClassifiedV.Store(nil)

	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	mu.Lock()
	reads := seen[x]
	mu.Unlock()
	if len(reads) == 0 || reads[len(reads)-1] {
		t.Fatalf("fork repair must have read %q as not serving, got %v", x, reads)
	}
	testRequireForkRepairedWithoutX(t, s, ci, c2, "settle.t16a", x)
	testMQTTPublish(t, c2, r2, 1, false, false, "settle/t16a", 1, []byte("fresh"))
	testMQTTCheckPubMsg(t, c2, r2, "settle/t16a", mqttPubQos1, []byte("fresh"))
}

// T16c (M13b, M13d): a disposal landing AFTER fork repair read the consumer as serving — between
// the accept drive's input load and the handoff preparation's own authority read — refuses the
// preparation, the CONNECT takes the full re-drive, fork repair reads again, and the same CONNECT
// succeeds without the decided consumer.
func TestF105ForkFenceRefusesADisposalAfterTheRead(t *testing.T) {
	testSuppressSettlement(t)
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	ci := &mqttConnInfo{clientID: "settle-t16c", cleanSess: false}
	sess, x := testForkWithConsumer(t, s, o, ci, "settle/t16c", "settle.t16c")

	var preflights atomic.Int32
	ph := func(name string, serving bool, err error) {
		if name == x {
			preflights.Add(1)
		}
	}
	mqttTestForkPreflightClassifiedV.Store(&ph)
	defer mqttTestForkPreflightClassifiedV.Store(nil)
	var injected atomic.Bool
	var injectErr atomic.Value
	hook := func(id string) {
		if id != ci.clientID || preflights.Load() == 0 || !injected.CompareAndSwap(false, true) {
			return
		}
		if err := testDecideAsTheSurvivor(t, sess, x); err != nil {
			injectErr.Store(err)
		}
	}
	mqttTestPrepareHandoffReadHookV.Store(&hook)
	defer mqttTestPrepareHandoffReadHookV.Store(nil)
	var redrives atomic.Int32
	rh := func(id string) {
		if id == ci.clientID {
			redrives.Add(1)
		}
	}
	mqttTestForkFenceRedriveV.Store(&rh)
	defer mqttTestForkFenceRedriveV.Store(nil)

	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	if v := injectErr.Load(); v != nil {
		t.Fatalf("injecting the disposal: %v", v)
	}
	if !injected.Load() {
		t.Fatal("fixture: the disposal must have been injected at the preparation's read")
	}
	if redrives.Load() != 1 {
		t.Fatalf("the fence must have taken exactly one full re-drive, got %d", redrives.Load())
	}
	if preflights.Load() < 2 {
		t.Fatalf("fork repair must have read %q again on the re-driven pass, got %d reads", x, preflights.Load())
	}
	testRequireForkRepairedWithoutX(t, s, ci, c2, "settle.t16c", x)
	testMQTTPublish(t, c2, r2, 1, false, false, "settle/t16c", 1, []byte("fresh"))
	testMQTTCheckPubMsg(t, c2, r2, "settle/t16c", mqttPubQos1, []byte("fresh"))
}
