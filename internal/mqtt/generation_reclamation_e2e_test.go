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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// T-empty-led (issue #133): a non-shared QoS 0 probe's close seals its generation's inventory
// ledger at Hi 0 and writes no shared ledger; with nothing allocated, its recovery root retires
// on the sealed empty ledger, and then the generation retires. Mutation M28: the inventory
// ledger sealed only for a generation that acquired (the probe's root never retires, so its
// generation is never reclaimed).
func TestAProbesCloseSealsAnEmptyLedgerAndItsGenerationRetires(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	id := fmt.Sprintf("empty-led-%d", time.Now().UnixNano())
	idHash := getHash(id)
	epoch := asm.sessStreamEpoch

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "probe/" + id, opts: 0}}); len(rc) != 1 || rc[0] != 0 {
		t.Fatalf("SUBSCRIBE: %v", rc)
	}
	gen := testSessionGenOf(t, s, id)
	// The certificate and the sealed ledgers are inspected before the reclamation purges them.
	asm.testHoldGenerationRetirement.Store(true)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !testRecordGone(asm, id) {
			return errors.New("the probe's session record still stands")
		}
		return nil
	})

	led, _, lerr := asm.jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil || led == nil || led.State != mqttGenLedgerSealed || led.SealedHi != 0 {
		t.Fatalf("the probe's close left its inventory ledger %+v (%v), want sealed at Hi 0", led, lerr)
	}
	if l := asm.jsa.loadSharedLedger(id, epoch, gen); l.status != mqttOutboundAbsent {
		t.Fatalf("a probe that never acquired left a shared ledger reading %v", l.status)
	}

	// THE ROOT RETIRES ON THE SEALED EMPTY LEDGER: the recovery visit certifies it.
	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		asm.sweepOutboundRecovery(s, nil)
		if cert, err := asm.jsa.loadGenCompletionCert(id, epoch, gen); err != nil || cert == nil {
			return fmt.Errorf("generation %q is not certified (%v)", gen, err)
		}
		return nil
	})
	if st, _ := asm.genStanding(id, gen); st != mqttGenOutstanding {
		t.Fatalf("fixture: the held generation %q reads %v, want outstanding", gen, st)
	}

	// AND THEN THE GENERATION RETIRES, and the client leaves nothing behind.
	asm.testHoldGenerationRetirement.Store(false)
	testDriveRetirement(t, s, asm, func() error {
		if st, _ := asm.genStanding(id, gen); st != mqttGenRetired {
			return fmt.Errorf("generation %q reads %v", gen, st)
		}
		if n, names := testClientRecordCount(t, asm, idHash); n != 0 {
			return fmt.Errorf("%d record(s) remain: %s", n, strings.Join(names, ", "))
		}
		return nil
	})
}

// T-unfinished-predecessor (issue #133): a Clean Start over a predecessor whose recovery has not
// finished (a QoS 1 delivery it never acknowledged) is not fenced by it: the successor is
// accepted at once, and the acceptance batch itself enrols the predecessor's generation
// outstanding and open, uncertified. Only afterwards does recovery finish the predecessor and
// its retirement remove the entry, the successor serving throughout. Mutation M8: the
// predecessor enrolled at its completion certificate instead of at the acceptance (no entry
// stands at the CONNACK).
func TestACleanStartEnrolsItsUnfinishedPredecessorAtAcceptance(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "unfinished-predecessor"
	const topic = "unfinished/predecessor"
	epoch := asm.sessStreamEpoch

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(3600)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}}); len(rc) != 1 || rc[0] != 1 {
		t.Fatalf("SUBSCRIBE: %v", rc)
	}
	g1 := testSessionGenOf(t, s, id)
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, topic, 1, []byte("never-acknowledged"))
	// The delivery is allocated in the predecessor's inventory and never acknowledged.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if led, _, err := asm.jsa.loadGenLedger(id, epoch, g1); err != nil || led == nil || led.Hi < 1 {
			return fmt.Errorf("the delivery is not allocated in %q yet: %+v (%v)", g1, led, err)
		}
		return nil
	})
	c.Close()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if _, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the Clean Start over an unfinished predecessor was refused: %d", rc)
	}
	g2 := testSessionGenOf(t, s, id)
	if g2 == g1 {
		t.Fatal("fixture: the Clean Start did not install a successor generation")
	}
	if cert, err := asm.jsa.loadGenCompletionCert(id, epoch, g1); err != nil || cert != nil {
		t.Fatalf("fixture: the predecessor %q was certified (%v) before its successor's acceptance could "+
			"be observed, so its recovery was not unfinished", g1, err)
	}
	entry, auth := testOutstandingEntry(t, asm, id, g1)
	if entry == nil || entry.Phase != mqttOutstandingOpen {
		t.Fatalf("the acceptance over an unfinished predecessor did not enrol %q outstanding and open: %+v",
			g1, auth.Life.Outstanding)
	}
	if auth.State != mqttAuthorityServing || auth.Gen != g2 {
		t.Fatalf("the successor does not serve beside its enrolled predecessor: %+v", auth)
	}

	testDriveRetirement(t, s, asm, func() error {
		if st, _ := asm.genStanding(id, g1); st != mqttGenRetired {
			return fmt.Errorf("the predecessor %q reads %v", g1, st)
		}
		return nil
	})
	if al := asm.jsa.loadOutboundAuthority(id, epoch); al.status != mqttOutboundValid ||
		al.rec.State != mqttAuthorityServing || al.rec.Gen != g2 || len(al.rec.Life.Outstanding) != 0 {
		t.Fatalf("after the predecessor retired, the authority reads %v %+v, want %q serving alone", al.status, al.rec, g2)
	}
}

// T-qos2-release-order, the second schedule (issue #133): the release WINS its claim, and only
// then does a discard make the generation noncurrent. The classification, which runs after the
// claim, observes it: 0x92, nothing published onward, the hold released. (The first schedule,
// the discard winning the claim, is TestQoS2ReleaseDiscardedMidClaimDeliversNothing; an open
// outstanding generation classified discarded is TestPubRelRefusesUnderAMatchingRetirement.)
// Mutation M37: Proceed from winning the claim alone (the classification skipped once claimed).
func TestAPubRelClaimedBeforeItsGenerationEndsIsRefused(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "pubrel-claimed-then-discarded")
	pi := uint16(51)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("claimed-then-discarded"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	gen, _ := testPubRelSessionFacts(t, b)

	var planted atomic.Bool
	b.asm.mu.Lock()
	b.asm.testQoS2ClaimDone = func(subject, _ string, status mqttQoS2ClaimStatus) {
		if subject == holdSubject && status == mqttQoS2ClaimClaimed && planted.CompareAndSwap(false, true) {
			testRetireGeneration(t, b.asm, b.pubID, gen)
		}
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQoS2ClaimDone = nil
		b.asm.mu.Unlock()
	})

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x for a release whose generation ended after its claim",
			reason, mqttReasonPacketIDNotFound)
	}
	if !planted.Load() {
		t.Fatal("the release never won its claim, so the schedule this test exists for was not exercised")
	}
	testQoS2ExpectNoDelivery(t, b)
	if rec, _, err := b.asm.loadQoS2Record(holdSubject); err != nil || rec != nil {
		t.Fatalf("the hold survived a refused release: %+v (%v)", rec, err)
	}
}

// T-qos2-release-order, the third schedule (issue #133): the classification answers Proceed and a
// discard completes after it, before the onward publication. The release publishes: the
// classification is the last decision, and a publication after a discard that landed past it is
// the late publication CONFORMANCE permits, not a refusal reclamation adds. Pins the carry-over
// (no mutation: a second classification after the verdict would fail it).
func TestAPubRelPastItsClassificationPublishesDespiteALaterDiscard(t *testing.T) {
	// NOT t.Parallel(): setMQTTTestQoS2RetireSampledHook is a process-global atomic.
	b := testQoS2Setup(t, "pubrel-proceed-then-discarded")
	pi := uint16(52)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("proceed-then-discarded"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	gen, _ := testPubRelSessionFacts(t, b)

	var planted atomic.Bool
	restore := setMQTTTestQoS2RetireSampledHook(func(v mqttQoS2RetireVerdict) {
		if v == mqttQoS2RetireProceed && planted.CompareAndSwap(false, true) {
			testRetireGeneration(t, b.asm, b.pubID, gen)
		}
	})
	defer restore()

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x00: a discard landing past the classification takes nothing back", reason)
	}
	if !planted.Load() {
		t.Fatal("the classification never answered Proceed, so the schedule this test exists for was not exercised")
	}
	b.consumeOne(t, "proceed-then-discarded")
	if st, _ := b.asm.genStanding(b.pubID, gen); st == mqttGenCurrent {
		t.Fatalf("fixture: the discard did not make generation %q noncurrent", gen)
	}
}

// T-candidate-cleanup, the standing-target arm (issue #133): the rebind of a published candidate
// LANDS, its acknowledgement is lost, and the generation retires before the rebind resolves; the
// purge takes the rebind's outcome but not the register, which still names the candidate. The
// rebind reads RETIRED, and the cleanup re-reads that register and spares the record it names:
// the canonical-register check is what decides, not the completed retirement. Mutation M31b: the
// canonical-register check dropped from the candidate cleanup (the canonical record is deleted).
func TestARetiredRebindSparesTheCandidateItsRegisterNames(t *testing.T) {
	t.Parallel()
	e, _, subject, _, canon := testPubRelReadyWorld(t, "remat-retired-standing")
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	var landed atomic.Bool
	testArmTransitionSeam(t, &e.asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind != mqttTransitionRebindCanonical || landed.Load() {
			return send()
		}
		if _, err := send(); err != nil {
			t.Errorf("fixture: the rebind did not land: %v", err)
			return 0, err
		}
		landed.Store(true)
		// The generation retires: its records (the rebind's outcome among them) are purged and
		// its entry removed. The register is not one of them.
		testRetireGeneration(t, e.asm, e.sess.id, gen)
		for _, f := range mqttGenerationPurgeFilters(getHash(e.sess.id), gen) {
			if err := e.asm.jsa.purgeStream(mqttSessStreamName, f, 0, false); err != nil {
				t.Errorf("purging %q: %v", f, err)
			}
		}
		_, ord, _ := mqttParseGenToken(gen)
		testRewriteAuthority(t, e, func(a *mqttOutboundAuthority) {
			if i := mqttOutstandingIndex(a.Life, ord); i >= 0 {
				a.Life.Outstanding = append(a.Life.Outstanding[:i], a.Life.Outstanding[i+1:]...)
			}
		})
		return 0, errors.New("test: the rebind's acknowledgement was lost")
	})
	if got := e.sess.rematerializeOnce(e.c, testRemat(t, e, subject, mqttLicenceOf(e.c))); got != mqttRematerializeDone {
		t.Fatalf("a rebind whose generation retired returned %d, want done", got)
	}
	if !landed.Load() {
		t.Fatal("the rebind never reached its seam, so the schedule this test exists for was not exercised")
	}
	slot := testSlotAt(t, e, subject)
	if slot == nil || slot.PubRelStreamSeq == canon {
		t.Fatalf("fixture: the landed rebind does not name its candidate: %+v", slot)
	}
	if rec, err := e.asm.jsa.loadMsg(mqttOutStreamName, slot.PubRelStreamSeq); err != nil || rec == nil {
		t.Fatalf("the cleanup deleted the candidate the standing register names: %v", err)
	}
	if n := testPubRelRecords(t, e); n != 1 {
		t.Fatalf("exactly the candidate the register names must stand, got %d records", n)
	}
}
