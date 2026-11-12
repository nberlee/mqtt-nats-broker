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
package mqtt

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// testInvalidateAuthority rewrites a client's authority unchanged at the sequence it stands at: the
// shape of a pre-admitted background attempt landing, which moves the sequence every CAS
// against the authority expects.
func testInvalidateAuthorityOf(t *testing.T, asm *mqttAccountSessionManager, clientID string) {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid {
		t.Errorf("invalidating the authority of %q: it reads %v", clientID, al.status)
		return
	}
	body, err := json.Marshal(al.rec)
	if err != nil {
		t.Error(err)
		return
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, body, _EMPTY_, 0, false)
	if _, err := asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(clientID)), hdr, msg); err != nil {
		t.Errorf("invalidating the authority of %q: %v", clientID, err)
	}
}

// A CONNECT's acceptance and bind each outlast every pre-admitted background writer (issue #133,
// consult 15): with k actors registered for the client, each holding one attempt admitted before
// the announcement, k authority rewrites land between the read and the send of the acceptance and
// k more of the bind, and the CONNECT is still accepted. The rewrites are contention, so they
// charge neither the per-CONNECT re-attempt allowance nor anything but the derived budget.
// Mutations: the loops budgeted on the old base of four; a contention recomposition charging
// stepAllow.
func TestAConnectOutlastsItsPreAdmittedWriters(t *testing.T) {
	const k = 6 // above the five session workers the base budget was cut against
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "fg-preadmitted"
	ci := &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(3600)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	c.Close()
	time.Sleep(150 * time.Millisecond) // the persistent session outlives its close

	for i := 0; i < k; i++ {
		deregister := asm.registerForegroundActor(id)
		defer deregister()
	}
	var accepts, binds atomic.Int32
	h := func(cid, stage string) {
		if cid != id {
			return
		}
		n := &accepts
		if stage == "bind" {
			n = &binds
		}
		if n.Add(1) <= k {
			testInvalidateAuthorityOf(t, asm, id)
		}
	}
	mqttAcceptTestReadToSendHookV.Store(&h)
	defer mqttAcceptTestReadToSendHookV.Store(nil)

	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if _, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("a CONNECT against %d pre-admitted writers per stage was answered %#x (%d acceptance and %d bind sends)",
			k, rc, accepts.Load(), binds.Load())
	}
	if accepts.Load() <= k || binds.Load() <= k {
		t.Fatalf("fixture: %d acceptance and %d bind sends, want more than %d each", accepts.Load(), binds.Load(), k)
	}
}

// Background Will-anchor removals yield to announced foreground work per attempt (consult 15): a
// backlog of owed removals sends nothing while the client's announcement stands, keeps every
// removal owed, and completes once it is released. Mutation: the removal loop's transition
// unadmitted (runAnchorTransition without the admit).
func TestOwedAnchorRemovalsYieldToAnnouncedForegroundWork(t *testing.T) {
	e, done := testNewEnv(t, "fg-anchor-backlog")
	defer done()
	e.bootstrap(t, "txn-1")
	var refs []*mqttWillAnchorRef
	for i := 0; i < 3; i++ {
		refs = append(refs, testArmWillAnchor(t, e.asm, e.id))
	}
	before := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	fg, _ := e.asm.announceForeground(e.id)
	for _, ref := range refs {
		e.asm.dropWillAnchor(e.id, ref)
	}
	time.Sleep(3 * mqttAnchorRemovalRetryMin)
	if after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); after.seq != before.seq {
		fg.release()
		t.Fatalf("an owed removal wrote the authority (%d -> %d) under an announcement", before.seq, after.seq)
	}
	e.asm.mu.Lock()
	owed := len(e.asm.anchorRemovals)
	e.asm.mu.Unlock()
	fg.release()
	if owed != len(refs) {
		t.Fatalf("%d removal(s) owed under the announcement, want all %d kept", owed, len(refs))
	}
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if n := len(testWillAnchorsOf(t, e.asm, e.id)); n != 0 {
			return fmt.Errorf("%d anchor(s) still stand after the announcement was released", n)
		}
		return nil
	})
}

// The lifecycle sweep yields to announced foreground work per attempt (consult 15): a release
// whose record is gone and whose authority half never landed is not recorded while the client's
// announcement stands, and is once it is released. Mutation: the sweep's release recording
// handed no admit.
func TestTheLifecycleSweepYieldsToAnnouncedForegroundWork(t *testing.T) {
	e, done := testNewEnv(t, "fg-sweep-release")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	if _, err := e.asm.jsa.commitAuthorityTransition(testClosure(e, auth, seq, "txn-close")); err != nil {
		t.Fatalf("claiming the teardown: %v", err)
	}
	_, ord, _ := mqttParseGenToken(auth.Gen)
	pending := func() bool {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		return al.rec.Life.Outstanding[mqttOutstandingIndex(al.rec.Life, ord)].Teardown.Pending
	}
	if err := e.asm.jsa.deleteMsg(mqttSessStreamName, testStandingRecordSeq(t, e.asm, e.id), false); err != nil {
		t.Fatalf("deleting the marked record: %v", err)
	}
	fg, _ := e.asm.announceForeground(e.id)
	e.asm.sweepLifecycles(e.asm.server())
	stillPending := pending()
	fg.release()
	if !stillPending {
		t.Fatal("the sweep recorded a release under the client's announcement")
	}
	e.asm.sweepLifecycles(e.asm.server())
	if pending() {
		t.Fatal("the sweep did not record the release once the announcement was released")
	}
}
