//go:build mqttport_fulltests

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
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// THE SEAL ENDS ADMISSION UNDER THE GENERATION (F-105 WP-F W2, row S2, the effect form).
//
// The owner's teardown of a serving generation commits the generation's discarding closure FIRST
// and only then seals its shared acquisition ledger: the admission fence lands before the join
// fence, so no interval exists in which the ledger is sealed and the authority can still admit.
// T7 (TestF105_8bClosureLandsBeforeTheSeal) witnesses the ORDER from the durable records; this
// witnesses its EFFECT. A shared admission that passed every readiness read before the teardown
// began is held there; the teardown runs through the production owner destroy and is held just
// after its seal; the admission is then let go, and it must not land.
//
// Mutation (S2): the licence-bypassing reorder - the owner destroy's close and seal swapped, and
// the closing licence's default admitting. Target: the admission does not land after the seal.
func TestF105AnAdmissionCannotLandAfterTheSeal(t *testing.T) {
	// NOT parallel: the shared pause seam is process-wide.
	r := newTestSharedAdmitRig(t, "w2-admit-after-seal")
	e := r.e
	dep := r.dep()
	admitPt, sealPt := "admit:after-readiness:"+r.acq.Acq, "closing:after-seal"
	gates := testInstallSharedGates(t, admitPt, sealPt)

	// The admission's batch passing every boundary check while the seal stands is the failure the
	// store would otherwise be left to catch; record it where it happens.
	jsa := &e.asm.jsa
	var presentedOverSeal atomic.Bool
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionAdmitExchange {
			return
		}
		if l := jsa.loadSharedLedger(e.sess.id, e.sess.sharedEpoch(), tr.Attempt.Gen); l.status == mqttOutboundValid &&
			l.rec.State == mqttSharedLedgerSealed {
			presentedOverSeal.Store(true)
		}
	}
	jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { jsa.testTransitionPresend.Store(nil) })

	// The session's accepted record is persisted, bound to the rig's connection, so the owner
	// destroy below is RECEIPTED and its closing licensed by the discarding closure it commits.
	sess := e.sess
	sess.mu.Lock()
	sess.c = e.c
	sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)

	pi, admitted := testAdmitInBackground(t, r, dep)
	gates.wait(t, admitPt)

	// The production owner destroy, the connection detached first as production does.
	sess.mu.Lock()
	gen := sess.gen
	sess.c = nil
	sess.mu.Unlock()
	epoch := sess.sharedEpoch()
	cleared := make(chan error, 1)
	go func() { cleared <- sess.clear(false) }()

	// ROUTE WITNESS: the closing reached its seal, and the seal stands. Whether the discarding
	// closure stood BEFORE the seal is the order T7 asserts; here it is asserted after the target,
	// so a closing that sealed first is caught by what its admission did, not by a fixture check.
	gates.wait(t, sealPt)
	if l := jsa.loadSharedLedger(sess.id, epoch, gen); l.status != mqttOutboundValid || l.rec.State != mqttSharedLedgerSealed {
		t.Fatalf("fixture: the ledger does not read sealed at the closing's after-seal point: %v %+v", l.status, l.rec)
	}

	gates.open(admitPt)
	landed := testRecv(t, "the parked admission", admitted)
	// TARGET.
	if landed {
		t.Fatal("an admission LANDED after its generation's shared ledger was sealed")
	}
	if presentedOverSeal.Load() {
		t.Fatal("an admission batch passed every boundary check while the generation's ledger stood sealed")
	}
	if !testF105ClosureDiscards(e.asm, sess.id, e.asm.sessStreamEpoch, gen) {
		t.Fatal("the closing sealed a generation no discarding closure ever closed")
	}
	if reg, smsg, err := e.asm.loadInflightRecord(mqttInflightSubject(sess.idHash, gen, pi)); err != nil || smsg != nil {
		t.Fatalf("a register stands for the refused admission: %+v (%v)", reg, err)
	}

	gates.open(sealPt)
	if err := testRecv(t, "the owner destroy", cleared); err != nil {
		t.Fatalf("the owner destroy: %v", err)
	}
}

// ONE TENURE WINS, AND IT IS THE LATER ACCEPTANCE'S (F-105 WP-F W2, row S1).
//
// Two MQTT frontends serve one JetStream. Broker A accepts a client's CONNECT, landing its accept
// save, and is delayed just before it installs its tenure on the generation's shared ledger. The
// same client then CONNECTs to broker B, which takes the session over and installs its own, later
// tenure. When A's delayed installation is finally sent it must lose: the ledger's non-regression
// rule refuses an older tenure, A's CONNECT is refused, and A never serves the client - the
// ledger keeps B's tenure.
//
// Mutation (S1): the multi-site installation that admits an older tenure (the composer's
// ordering check plus both validator sites). Target: A's CONNECT is refused and the ledger keeps
// B's tenure.
func TestF105_8bADelayedInstallLosesToThePeersTenure(t *testing.T) {
	// NOT parallel: the shared pause seam is process-wide.
	const cid = "w2-delayed-install"
	const installPt = "install:before-send"
	oA := testMQTTDefaultOptions()
	sA := testMQTTRunServer(t, oA)
	t.Cleanup(func() { testMQTTShutdownServer(sA) }) // owns the shared nats-server: last
	oB := testMQTTDefaultOptions()
	oB.StoreDir = oA.StoreDir
	sB := testMQTTRunServer(t, oB)
	t.Cleanup(func() { sB.Shutdown() })

	// Armed only now, with no other CONNECT in flight: the FIRST installation to arrive is A's.
	gates := testInstallSharedGates(t, installPt)
	t.Cleanup(func() { gates.open(installPt) }) // before either server's shutdown
	ci := &mqttConnInfo{clientID: cid, cleanSess: false}
	cA, err := net.Dial("tcp", net.JoinHostPort(oA.MQTT.Host, fmt.Sprintf("%d", oA.MQTT.Port)))
	if err != nil {
		t.Fatalf("dialing broker A: %v", err)
	}
	t.Cleanup(func() { cA.Close() })
	if _, err := testMQTTWrite(cA, mqttCreateConnectProto(ci)); err != nil {
		t.Fatalf("writing A's CONNECT: %v", err)
	}
	// ROUTE WITNESS: A's acceptance reached its installation and is held there.
	gates.wait(t, installPt)

	cB, rB := testMQTTConnect(t, ci, oB.MQTT.Host, oB.MQTT.Port)
	t.Cleanup(func() { cB.Close() })
	b, pl := testMQTTReadPacket(t, rB)
	if b&mqttPacketMask != mqttPacketConnectAck || pl != 2 {
		t.Fatalf("broker B answered packet 0x%x (len %d), want a CONNACK", b, pl)
	}
	if _, err := rB.readByte("connack flags"); err != nil {
		t.Fatalf("reading B's CONNACK: %v", err)
	}
	if rc, err := rB.readByte("connack return code"); err != nil || rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("broker B refused the takeover: rc %d (%v)", rc, err)
	}
	tenureB, _, entB, _ := testF105_8bTenureOf(t, sB, cid)

	gates.open(installPt)
	// TARGET: A's CONNECT is refused - never an accepted CONNACK - and its connection ends.
	rA := &mqttReader{reader: cA}
	if b, pl, ok := testMQTTReadPacketReady(rA, 10*time.Second); ok {
		if b&mqttPacketMask != mqttPacketConnectAck || pl != 2 {
			t.Fatalf("broker A sent packet 0x%x (len %d) to a CONNECT whose installation lost", b, pl)
		}
		_, _ = rA.readByte("connack flags")
		if rc, _ := rA.readByte("connack return code"); rc == mqttConnAckRCConnectionAccepted {
			t.Fatal("broker A ACCEPTED a CONNECT whose tenure installation lost to the peer's later tenure")
		}
	}
	testMQTTExpectDisconnect(t, cA)

	asmB := testMQTTGetAccountSessionManager(t, sB, cid)
	epoch := testF105AccountEpoch(asmB)
	l := asmB.jsa.loadSharedLedger(cid, epoch, entB.Gen)
	if l.status != mqttOutboundValid || l.rec.Tenure != tenureB {
		t.Fatalf("the generation's ledger must keep B's tenure %+v, it reads %v %+v", tenureB, l.status, l.rec)
	}
	// A never served: no session of this client is resident there.
	asmA := testMQTTGetGlobalASM(t, sA)
	asmA.mu.RLock()
	resident := asmA.sessions[cid]
	asmA.mu.RUnlock()
	if resident != nil {
		resident.mu.Lock()
		bound := resident.c
		resident.mu.Unlock()
		if bound != nil {
			t.Fatal("broker A holds a connection bound to the session whose tenure it lost")
		}
	}
	// And B still serves it.
	testMQTTSub(t, 1, cB, rB, []*mqttFilter{{filter: "w2/install", qos: 1}}, []byte{1})
}
