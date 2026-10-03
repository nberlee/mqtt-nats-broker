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
	"fmt"
	"sync"
	"testing"
	"time"
)

// F-105 7d end to end: a clean-session client disconnects with an unacknowledged delivery; the
// teardown transfers its consumers to the generation's recovery root, and the recovery executor
// retires them through their resources (plan-7d T1, T2, T11).

// testDiscardFix is one torn-down session's retirement, observed from the durable records.
type testDiscardFix struct {
	s      *Server
	asm    *mqttAccountSessionManager
	id     string
	gen    string
	src    string // the source consumer's name
	pubRel string // the PUBREL consumer's name (QoS 2 only)
	nonce  string // the held obligation
}

// testDiscardTearDown connects a clean-session subscriber, delivers one message it does not finish
// acknowledging, and disconnects it. pubrec answers a QoS 2 PUBLISH with PUBREC (and never
// PUBCOMP), so the obligation stands past PUBREC holding both consumers.
func testDiscardTearDown(t *testing.T, id string, qos byte, pubrec bool) *testDiscardFix {
	t.Helper()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Hour // no redelivery races the teardown
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "discard/topic", qos: qos}}, []byte{qos})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, qos, false, false, "discard/topic", 1, []byte("held"))

	_, pi := testMQTTGetPubMsg(t, c, r, "discard/topic", []byte("held"))
	if pubrec {
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		if b, _ := testMQTTReadPacket(t, r); b&mqttPacketMask != mqttPacketPubRel {
			t.Fatalf("expected the PUBREL after PUBREC, got packet 0x%x", b)
		}
	}

	sess := testMQTTResidentSession(t, s, id)
	f := &testDiscardFix{s: s, asm: sess.asm, id: id}
	sess.mu.Lock()
	for _, cc := range sess.cons {
		f.src = cc.Durable
	}
	if sess.pubRelConsumer != nil {
		f.pubRel = sess.pubRelConsumer.Durable
	}
	f.gen = sess.gen
	sess.mu.Unlock()

	own := f.ownership(t, mqttOwnedStreamMsgs, f.src)
	if own == nil {
		t.Fatalf("fixture: source consumer %q has no ownership record", f.src)
	}
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		res := f.resource(t, own.OwnerInc)
		if res == nil || len(res.Deps) == 0 {
			return fmt.Errorf("the delivery has not been admitted against %q yet", f.src)
		}
		f.nonce = res.Deps[0].Nonce
		return nil
	})

	c.Close()
	// THE TEARDOWN RETURNS once the record is released.
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if _, err := f.asm.jsa.loadLastMsgFor(mqttSessStreamName,
			mqttSessStreamSubjectPrefix+f.asm.domainTk+getHash(id)); err == nil {
			return fmt.Errorf("the session record of %q still stands", id)
		}
		return nil
	})
	return f
}

func (f *testDiscardFix) epoch() string { return f.asm.sessStreamEpoch }

func (f *testDiscardFix) ownership(t *testing.T, stream mqttOwnedStream, name string) *mqttNameOwnership {
	t.Helper()
	own, _, err := f.asm.jsa.loadNameOwnershipPhysical(f.epoch(), stream, name)
	if err != nil {
		t.Fatalf("reading the ownership of %q: %v", name, err)
	}
	return own
}

func (f *testDiscardFix) resource(t *testing.T, inc string) *mqttOutboundResource {
	t.Helper()
	r, _, err := f.asm.jsa.loadOutboundResource(f.id, f.epoch(), f.gen, inc)
	if err != nil {
		t.Fatalf("reading resource %q: %v", inc, err)
	}
	return r
}

func (f *testDiscardFix) consumerExists(t *testing.T, stream, name string) bool {
	t.Helper()
	ci, err := f.asm.jsa.lookupConsumer(stream, name)
	if err != nil && isErrorOtherThan(err, JSConsumerNotFoundErr) {
		t.Fatalf("looking up %q: %v", name, err)
	}
	return ci != nil
}

// retired reports nil once one consumer's whole retirement stands: RETIRED name, the discard's
// decision, CleanupComplete, both duties terminal, the consumer gone.
func (f *testDiscardFix) retired(t *testing.T, stream mqttOwnedStream, js, name string) error {
	own := f.ownership(t, stream, name)
	if own == nil || own.State != mqttNameOwnRetired {
		return fmt.Errorf("name %q is not RETIRED: %+v", name, own)
	}
	r := f.resource(t, own.OwnerInc)
	if r == nil || r.State != mqttResourceCleanupComplete {
		return fmt.Errorf("resource of %q is not CleanupComplete: %+v", name, r)
	}
	if r.Disposal == nil || r.Disposal.Reason != mqttDisposalGenerationDiscarded || r.Disposal.Tenure != _EMPTY_ {
		return fmt.Errorf("resource of %q does not carry the discard's decision: %+v", name, r.Disposal)
	}
	for _, kind := range []mqttDutyKind{mqttDutyProvision, mqttDutyCleanup} {
		ord, err := f.asm.jsa.resourceDutyOrdinal(f.id, f.epoch(), f.gen, own.OwnerInc, kind)
		if err != nil {
			return err
		}
		d, _, derr := f.asm.jsa.loadDutyEntryAt(f.id, f.epoch(), f.gen, ord)
		if derr != nil || d == nil || d.State != mqttDutyTerminal {
			return fmt.Errorf("the %s duty of %q is not terminal: %+v (%v)", kind, name, d, derr)
		}
	}
	if f.consumerExists(t, js, name) {
		return fmt.Errorf("consumer %q still exists", name)
	}
	return nil
}

func (f *testDiscardFix) obligationDiscarded(t *testing.T) error {
	x, _, err := f.asm.jsa.loadOutboundExchange(f.id, f.epoch(), f.gen, f.nonce)
	if err != nil || x == nil {
		return fmt.Errorf("the obligation does not load: %v", err)
	}
	if x.Phase != mqttExchangeDiscarded {
		return fmt.Errorf("the obligation stands at phase %d, want Discarded", x.Phase)
	}
	return nil
}

func (f *testDiscardFix) certified(t *testing.T) error {
	cert, err := f.asm.jsa.loadGenCompletionCert(f.id, f.epoch(), f.gen)
	if err != nil || cert == nil {
		return fmt.Errorf("generation %q has not certified: %v", f.gen, err)
	}
	return nil
}

// testDiscardParkAll parks every recovery caller — the prompt drive and the periodic pass — so
// nothing but the teardown itself acts until the returned function releases them.
func testDiscardParkAll(t *testing.T) (release func()) {
	t.Helper()
	restoreInterval := setMQTTSessionExpirySweepInterval(time.Hour)
	var mu sync.Mutex
	parked := true
	restoreHook := setMQTTTestRecoveryKickHook(func(string, string) bool {
		mu.Lock()
		defer mu.Unlock()
		return !parked
	})
	var once sync.Once
	release = func() {
		once.Do(func() {
			mu.Lock()
			parked = false
			mu.Unlock()
		})
	}
	t.Cleanup(func() {
		release()
		restoreHook()
		restoreInterval()
	})
	return release
}

// testDiscardObserveDeletes asserts that every physical consumer delete the executor performs is of
// a resource whose deletion is AUTHORIZED at that moment.
func testDiscardObserveDeletes(t *testing.T, f **testDiscardFix) (deleted func() []string) {
	t.Helper()
	var mu sync.Mutex
	var names []string
	restore := setMQTTTestRecoveryConsumerDeleteHook(func(stream, name string) error {
		fx := *f
		if fx != nil {
			for _, os := range []mqttOwnedStream{mqttOwnedStreamMsgs, mqttOwnedStreamOut} {
				own, _, _ := fx.asm.jsa.loadNameOwnershipPhysical(fx.epoch(), os, name)
				if own == nil {
					continue
				}
				r, _, _ := fx.asm.jsa.loadOutboundResource(fx.id, fx.epoch(), fx.gen, own.OwnerInc)
				if r == nil || r.State != mqttResourceDeleteAuthorized || own.State != mqttNameOwnRetired {
					t.Errorf("consumer %q was deleted while its resource stood %+v and its name %v",
						name, r, own.State)
				}
			}
		}
		mu.Lock()
		names = append(names, name)
		mu.Unlock()
		var err error
		if fx != nil {
			_, err = fx.asm.jsa.deleteConsumer(stream, name, false)
		}
		return err
	})
	t.Cleanup(restore)
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), names...)
	}
}

// T1: the teardown TRANSFERS the source consumer (it still exists when the teardown returns, with
// every caller parked), and the executor then retires it through its resource.
func TestF105DiscardedGenerationRetiresItsSourceConsumer(t *testing.T) {
	var fx *testDiscardFix
	deletes := testDiscardObserveDeletes(t, &fx)
	release := testDiscardParkAll(t)
	fx = testDiscardTearDown(t, "discard-src", 1, false)
	f := fx

	if !f.consumerExists(t, mqttStreamName, f.src) {
		t.Fatalf("the teardown deleted source consumer %q itself instead of transferring it", f.src)
	}
	if own := f.ownership(t, mqttOwnedStreamMsgs, f.src); own == nil || own.State != mqttNameOwnLive {
		t.Fatalf("the teardown moved the name of %q: %+v", f.src, own)
	}

	release()
	f.asm.kickRecoveryRoot(f.id, f.epoch(), f.gen)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if err := f.obligationDiscarded(t); err != nil {
			return err
		}
		if err := f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, f.src); err != nil {
			return err
		}
		return f.certified(t)
	})
	if got := deletes(); len(got) == 0 {
		t.Fatal("the executor performed no observed delete")
	}
}

// T2: a QoS 2 obligation past PUBREC holds the PUBREL consumer too; both are retired.
func TestF105DiscardedGenerationRetiresItsPubRelConsumer(t *testing.T) {
	var fx *testDiscardFix
	testDiscardObserveDeletes(t, &fx)
	release := testDiscardParkAll(t)
	fx = testDiscardTearDown(t, "discard-rel", 2, true)
	f := fx
	if f.pubRel == _EMPTY_ {
		t.Fatal("fixture: the QoS 2 session has no PUBREL consumer")
	}
	if !f.consumerExists(t, mqttOutStreamName, f.pubRel) {
		t.Fatalf("the teardown deleted PUBREL consumer %q itself instead of transferring it", f.pubRel)
	}
	release()
	f.asm.kickRecoveryRoot(f.id, f.epoch(), f.gen)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if err := f.obligationDiscarded(t); err != nil {
			return err
		}
		if err := f.retired(t, mqttOwnedStreamOut, mqttOutStreamName, f.pubRel); err != nil {
			return err
		}
		if err := f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, f.src); err != nil {
			return err
		}
		return f.certified(t)
	})
}

// T11: with the periodic pass an hour away and no test kick, the teardown's own kick retires the
// consumers promptly.
func TestF105DiscardIsDrivenPromptly(t *testing.T) {
	restore := setMQTTSessionExpirySweepInterval(time.Hour)
	defer restore()
	f := testDiscardTearDown(t, "discard-prompt", 1, false)
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if err := f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, f.src); err != nil {
			return err
		}
		return f.certified(t)
	})
}

// The 7d test seams' setters, beside the tests that use them.

func setMQTTTestRecoveryConsumerDeleteHook(h func(stream, name string) error) func() {
	prev := mqttTestRecoveryConsumerDeleteHookV.Load()
	if h == nil {
		mqttTestRecoveryConsumerDeleteHookV.Store(nil)
	} else {
		mqttTestRecoveryConsumerDeleteHookV.Store(&h)
	}
	return func() { mqttTestRecoveryConsumerDeleteHookV.Store(prev) }
}

func setMQTTTestRecoveryKickHook(h func(id, gen string) bool) func() {
	prev := mqttTestRecoveryKickHookV.Load()
	if h == nil {
		mqttTestRecoveryKickHookV.Store(nil)
	} else {
		mqttTestRecoveryKickHookV.Store(&h)
	}
	return func() { mqttTestRecoveryKickHookV.Store(prev) }
}

func setMQTTTestRecoveryKickVisit(h func(id, gen string) bool) func() {
	prev := mqttTestRecoveryKickVisitV.Load()
	if h == nil {
		mqttTestRecoveryKickVisitV.Store(nil)
	} else {
		mqttTestRecoveryKickVisitV.Store(&h)
	}
	return func() { mqttTestRecoveryKickVisitV.Store(prev) }
}
