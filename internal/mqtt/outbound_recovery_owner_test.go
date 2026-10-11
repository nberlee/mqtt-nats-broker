//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestConsult71AbsentRecoveryOwners(t *testing.T) {
	for _, kind := range []string{"opaque", "missing-slot", "marker", "demoted-publish", "demoted-pubrel", "pubrel-placeholder", "slot-placeholder"} {
		t.Run(kind, func(t *testing.T) {
			e := testNewInflightEnv(t, "c71-absent")
			e.s.closeWillDispatch()
			p := mqttRecoveryOpaquePending(17)
			if kind == "missing-slot" {
				p = mqttRecoveryOpaquePending(0)
			}
			if kind == "marker" {
				p = &mqttPending{regSeq: 17, regNonce: "old", jsDur: "source", sseq: 3, markerOpaque: true}
			}
			if kind == "demoted-publish" || kind == "demoted-pubrel" {
				p = &mqttPending{regSeq: 17, regNonce: "old", jsDur: "source", sseq: 3}
				testDemoteInPlace(p)
			}
			e.sess.mu.Lock()
			e.sess.c = e.c
			e.sess.pendingPublish = map[uint16]*mqttPending{7: p}
			if kind == "demoted-pubrel" {
				e.sess.pendingPublish[7] = mqttRecoveryOpaquePending(17)
				e.sess.pendingPubRel = map[uint16]*mqttPending{7: p}
			}
			if kind == "pubrel-placeholder" || kind == "slot-placeholder" {
				e.sess.pendingPublish = nil
				e.sess.pendingPubRel = map[uint16]*mqttPending{7: {
					jsDur: "source", sseq: 3, regNonce: "old", regRecoveryPlaceholder: true,
				}}
				if kind == "slot-placeholder" {
					r := &mqttInflightRecord{PI: 7, Nonce: "old", Lin: "lineage",
						Dur: "source", Seq: 3, QoS: 2, State: mqttInflightPubRelIntended}
					e.sess.pendingPubRel[7] = mqttRecoveredPending(e.sess.idHash, e.sess.gen, 7,
						mqttRecoveredInflightReg{rec: r, seq: 17})
				}
			}
			e.sess.cpending = map[string]map[uint64]uint16{
				"source": {3: 7, 4: 8}, "transport": {9: 7},
			}
			e.sess.mu.Unlock()
			staged, err := e.sess.recoverInflightRegisters(e.c)
			if err != nil || len(staged) != 1 {
				t.Fatalf("absence was not staged: %v, %d", err, len(staged))
			}
			e.sess.mu.Lock()
			held := e.sess.pendingPublish[7] != nil || e.sess.pendingPubRel[7] != nil
			e.sess.mu.Unlock()
			if !held {
				t.Fatal("released before successful CONNECT")
			}
			e.sess.commitStagedReleases(e.c, staged)
			e.sess.mu.Lock()
			defer e.sess.mu.Unlock()
			if e.sess.pendingPublish[7] != nil || e.sess.pendingPubRel[7] != nil ||
				e.sess.cpending["source"][3] != 0 || e.sess.cpending["transport"][9] != 0 ||
				e.sess.cpending["source"][4] != 8 ||
				e.sess.sourceRestrictedLocked("source", 3) || e.sess.durableRestrictedLocked("source") {
				t.Fatal("retired recovery owner or mapping survived")
			}
			if !e.sess.sendReservedLocked(7) {
				t.Fatal("removal lost the exclusion before its independent absence proof")
			}
		})
	}
}

func TestConsult71RecoveryReleaseFence(t *testing.T) {
	for _, change := range []string{"replacement", "new-walk", "generation", "incomplete"} {
		t.Run(change, func(t *testing.T) {
			e := testNewInflightEnv(t, "c71-fence")
			e.s.closeWillDispatch()
			e.sess.mu.Lock()
			e.sess.c = e.c
			e.sess.pendingPublish = map[uint16]*mqttPending{7: mqttRecoveryOpaquePending(0)}
			e.sess.mu.Unlock()
			staged, err := e.sess.recoverInflightRegisters(e.c)
			if err != nil || len(staged) != 1 {
				t.Fatalf("stage: %v, %d", err, len(staged))
			}
			e.sess.mu.Lock()
			switch change {
			case "replacement":
				e.sess.pendingPublish[7] = mqttRecoveryOpaquePending(0)
			case "new-walk":
				e.sess.inflightWalks++
			case "generation":
				e.sess.gen = mqttNewSessGen()
			case "incomplete":
				e.sess.piSpaceIncomplete = true
			}
			want := e.sess.pendingPublish[7]
			e.sess.mu.Unlock()
			e.sess.commitStagedReleases(e.c, staged)
			e.sess.mu.Lock()
			defer e.sess.mu.Unlock()
			if e.sess.pendingPublish[7] != want {
				t.Fatal("stale CONNECT released a successor's reservation")
			}
		})
	}
}

func TestConsult71DemotedOrdinaryResume(t *testing.T) {
	e := testNewInflightEnv(t, "c71-demoted")
	e.s.closeWillDispatch()
	dur := e.sess.idHash + "_ordinary"
	seq := e.publishSource(t, dur)
	pi := e.track(t, dur, seq, 1)
	e.sess.mu.Lock()
	old := e.sess.pendingPublish[pi]
	testDemoteInPlace(old)
	e.sess.mu.Unlock()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	p := e.sess.pendingPublish[pi]
	if p == nil || p == old || p.regForeign || p.regRecoveryDemoted ||
		p.regNonce != old.regNonce || e.sess.cpending[dur][seq] != pi {
		t.Fatal("healthy ordinary classification left the demotion standing")
	}
}

func TestConsult71TerminalRecoveryHandoff(t *testing.T) {
	for _, marker := range []bool{false, true} {
		e := testNewInflightEnv(t, "c71-terminal")
		e.s.closeWillDispatch()
		// A SLOT-ONLY source (the experimental affine class; shared was slot-only until 7a): the
		// terminal handoff asserted here is the slot-only route, which needs no exchange.
		dur := mqttQ2AffineConsumerDurablePrefix + "c71"
		seq := e.publishSource(t, dur)
		pi := e.track(t, dur, seq, 1)
		r, _ := e.loadRegister(t, pi)
		r.State = mqttInflightPublishTerminal
		body, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.asm.jsa.storeMsg(mqttInflightSubject(e.sess.idHash, e.sess.gen, pi), 0, body); err != nil {
			t.Fatal(err)
		}
		e.sess.mu.Lock()
		p := e.sess.pendingPublish[pi]
		if marker {
			p.markerOpaque = true
		} else {
			testDemoteInPlace(p)
		}
		e.sess.mu.Unlock()
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatal(err)
		}
		e.sess.mu.Lock()
		held := e.sess.pendingPublish[pi] != nil || e.sess.cpending[dur][seq] == pi
		e.sess.mu.Unlock()
		if held {
			t.Fatal("terminal successor left the old scheduling owner behind")
		}
	}
}

func TestConsult71QuarantineLivePubRel(t *testing.T) {
	e := testNewInflightEnv(t, "c71-live")
	e.s.closeWillDispatch()
	dur := e.sess.idHash + "_source"
	seq := e.publishSource(t, dur)
	pi := e.track(t, dur, seq, 2)
	r, regSeq := e.loadRegister(t, pi)
	rel := mqttRecoveredPending(e.sess.idHash, e.sess.gen, pi,
		mqttRecoveredInflightReg{rec: r, seq: regSeq.Sequence})
	rel.markerOpaque = false
	rel.jsDur, rel.sseq = "rel", 12
	rel.jsAckSubject = testInflightAckSubject("rel", 12, 1)
	testDemoteInPlace(rel)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.c.cid = 1
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	e.sess.pubRelConsumer = &ConsumerConfig{Durable: "rel"}
	e.sess.pendingPublish[pi] = mqttRecoveryOpaquePending(regSeq.Sequence)
	e.sess.pendingPubRel = map[uint16]*mqttPending{pi: rel}
	e.sess.trackAsPubRel(pi, testInflightAckSubject("rel", 12, 2))
	replaced := e.sess.pendingPubRel[pi] != rel
	_, admitted := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if replaced || admitted {
		t.Fatal("delivery mutated or admitted through a recovery reservation")
	}
	e.c.mqttProcessPubComp(pi)
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if e.sess.pendingPubRel[pi] != rel || e.sess.pendingPublish[pi] == nil {
		t.Fatal("PUBCOMP consumed only one half of the quarantine")
	}
}

func TestConsult71CancelledRecoveryHandoff(t *testing.T) {
	e := testNewInflightEnv(t, "c71-cancelled")
	e.s.closeWillDispatch()
	dur := e.sess.idHash + "_source"
	seq := e.publishSource(t, dur)
	pi := e.track(t, dur, seq, 1)
	r, oldSeq := e.loadRegister(t, pi)
	r.Cancelled, r.CancelOrigin = true, mqttCancelOriginOccupant // F-088: an occupant CAS
	body, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.asm.jsa.storeMsg(mqttInflightSubject(e.sess.idHash, e.sess.gen, pi), 0, body); err != nil {
		t.Fatal(err)
	}
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = mqttRecoveryOpaquePending(oldSeq.Sequence)
	e.sess.mu.Unlock()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if e.sess.pendingPublish[pi] != nil || e.sess.cpending[dur][seq] == pi {
		t.Fatal("cancelled register was dropped without its recovery reservation")
	}
}

func TestConsult71ScannedPubRelOwner(t *testing.T) {
	e := testNewInflightEnv(t, "c71-scanned")
	e.s.closeWillDispatch()
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName, Config: ConsumerConfig{
			Durable: "c71canonical", AckPolicy: AckExplicit,
			FilterSubject: e.sess.pubRelSubject,
		},
	}); err != nil {
		t.Fatal(err)
	}
	seq := testStoreCanonical(t, e, 7, mqttNewInflightNonce(), 3)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	e.sess.mu.Lock()
	p := e.sess.pendingPubRel[7]
	held := e.sess.recoveryPIHeldLocked(7)
	e.sess.mu.Unlock()
	if p == nil || !p.regRecoveryPlaceholder || held {
		t.Fatal("known recovered PUBREL lost provenance or became quarantined")
	}
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, seq, true); err != nil {
		t.Fatal(err)
	}
	staged, err := e.sess.recoverInflightRegisters(e.c)
	if err != nil || len(staged) != 1 {
		t.Fatalf("scanned owner did not retire: %v, %d", err, len(staged))
	}
	e.sess.commitStagedReleases(e.c, staged)
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if e.sess.pendingPubRel[7] != nil {
		t.Fatal("scanned placeholder outlived its durable owner")
	}
}

func TestConsult71DemotedSourceRestrictions(t *testing.T) {
	p := &mqttPending{regSeq: 9, regNonce: "resident", jsDur: "resident-source", sseq: 1}
	testDemoteInPlace(p)
	sess := &mqttSession{
		pendingPublish: map[uint16]*mqttPending{7: p},
		pendingPubRel: map[uint16]*mqttPending{7: {
			regForeign: true, regRecoveryDemoted: true,
		}},
	}
	if !sess.sourceRestrictedLocked("rejected-pair-source", 2) ||
		!sess.durableRestrictedLocked("rejected-pair-source") {
		t.Fatal("a resident's identity attributed an unvalidated recovery pair")
	}
	if _, eligible, known := sess.drainClaimEligibleLocked(7); eligible || known {
		t.Fatal("demoted PUBREL was classified as a known drain successor")
	}
}

func TestConsult73PubRelCallbackRecoveryGate(t *testing.T) {
	for _, held := range []bool{false, true} {
		name := "validated"
		if held {
			name = "held"
		}
		t.Run(name, func(t *testing.T) {
			e := testNewInflightEnv(t, "c73-callback")
			e.s.closeWillDispatch()
			testReadyForRealPubRelDelivery(t, e)
			seed := testSeedRealPubRelReady(t, e)
			// Exercise the real classifier before adding the optional C5 hold.
			e.sess.mu.Lock()
			testDemoteInPlace(e.sess.pendingPubRel[seed.pi])
			e.sess.mu.Unlock()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatal(err)
			}
			r, sm := e.loadRegister(t, seed.pi)
			sourceAck := testInflightAckSubject(testRealPubRelSrcDur, seed.sseq, 1)
			relAck := seed.reply(1)
			st := &drainSeq{
				claimed: true, intent: true, pi: seed.pi, regSeq: sm.Sequence,
				ownerNonce: seed.nonce, phase: drainPubRelUncertain, ack: sourceAck,
			}
			d := &mqttDrain{
				cc: &ConsumerConfig{Durable: testRealPubRelSrcDur,
					DeliverSubject: "f105.real.source.delivery",
					FilterSubject:  mqttStreamSubjectPrefix + "f092.source"},
				qos: 2,
				// Keep the consumer alive; this test is about one claim's discharge.
				seqs: map[uint64]*drainSeq{seed.sseq: st, seed.sseq + 1: {claimed: true}},
			}
			e.sess.mu.Lock()
			rel := e.sess.pendingPubRel[seed.pi]
			validated := rel != nil && !e.sess.recoveryPIHeldLocked(seed.pi)
			e.sess.drains = map[string]*mqttDrain{"c73": d}
			e.sess.drainByDur = map[string]*mqttDrain{testRealPubRelSrcDur: d}
			var pub *mqttPending
			if held && rel != nil {
				pub = mqttRecoveryOpaquePending(sm.Sequence)
				e.sess.pendingPublish[seed.pi] = pub
				testDemoteInPlace(rel)
			}
			beforeSessionSeq := e.sess.seq
			e.sess.mu.Unlock()
			if !validated {
				t.Fatal("classifier did not produce an actionable recovery owner")
			}
			if got := e.sess.awaitTransmissionGates(e.c); got != mqttTransmissionOpen {
				t.Fatalf("transmission gates are not open: %v", got)
			}
			wire := testRow2Wire(e)
			obs, err := e.s.nrc.SubscribeSync("$JS.ACK.>")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = obs.Unsubscribe() })
			if err := e.s.nrc.Flush(); err != nil {
				t.Fatal(err)
			}
			// A marker through the SAME send queue bounds the observation, including
			// fire-and-forget source/transport acks; a connection Flush alone cannot.
			readAcks := func() (source, transport int) {
				e.sess.jsa.sendMsg(sourceAck, []byte("c73-barrier"))
				for {
					m, err := obs.NextMsg(testRow2Timeout)
					if err != nil {
						t.Fatal(err)
					}
					if m.Subject == sourceAck && string(m.Data) == "c73-barrier" {
						return
					}
					if m.Subject == sourceAck {
						source++
					}
					if m.Subject == relAck {
						transport++
					}
				}
			}
			readAcks() // discard acknowledgements queued by fixture setup
			var stores, forwarded atomic.Int64
			restoreStore := setMQTTTestStoreMsgHook(func(string, int, []byte) error {
				stores.Add(1)
				return nil
			})
			t.Cleanup(restoreStore)
			restoreForward := setMQTTTestPubRelPreForwardHook(func() { forwarded.Add(1) })
			t.Cleanup(restoreForward)
			testDeliverPubRelAs(t, e, seed.pi, relAck, seed.nonce)
			sourceAcks, transportAcks := readAcks()
			restoreForward()
			restoreStore()

			e.sess.mu.Lock()
			claimed, uncertain, phase := st.claimed, st.unclaimUnconfirmed, st.phase
			sameOwners := e.sess.pendingPublish[seed.pi] == pub && e.sess.pendingPubRel[seed.pi] == rel
			sessionSeq := e.sess.seq
			e.sess.mu.Unlock()
			if held {
				after, afterMsg := e.loadRegister(t, seed.pi)
				if !claimed || uncertain || phase != drainPubRelUncertain || !sameOwners ||
					sessionSeq != beforeSessionSeq || stores.Load() != 0 ||
					afterMsg.Sequence != sm.Sequence || !reflect.DeepEqual(after, r) ||
					sourceAcks != 0 || transportAcks != 0 || forwarded.Load() != 0 || len(wire.written) != 0 {
					t.Fatalf("held callback progressed: claim=%v uncertain=%v phase=%v owners=%v stores=%d source=%d transport=%d forward=%d wire=%x",
						claimed, uncertain, phase, sameOwners, stores.Load(), sourceAcks, transportAcks, forwarded.Load(), wire.written)
				}
			} else {
				if claimed || uncertain || phase != drainPubRelConfirmed ||
					sessionSeq == beforeSessionSeq || stores.Load() == 0 || sourceAcks != 1 ||
					transportAcks != 0 || forwarded.Load() != 1 ||
					!bytes.Equal(wire.written, testRow2PubRelFrame(seed.pi)) {
					t.Fatalf("validated callback did not discharge and forward: claim=%v uncertain=%v stores=%d source=%d transport=%d forward=%d wire=%x",
						claimed, uncertain, stores.Load(), sourceAcks, transportAcks, forwarded.Load(), wire.written)
				}
				testRow2Eventually(t, testRow2Timeout, "PUBREL action drain", func() bool {
					cur, _, err := e.asm.loadInflightRecord(seed.subject)
					return err == nil && cur != nil && cur.ActionToken == _EMPTY_
				})
			}
		})
	}
}

func TestConsult73PubCompSecondRecoveryGate(t *testing.T) {
	for _, held := range []bool{false, true} {
		name := "validated"
		if held {
			name = "held"
		}
		t.Run(name, func(t *testing.T) {
			e := testNewInflightEnv(t, "c73-pubcomp2")
			e.s.closeWillDispatch()
			testReadyForRealPubRelDelivery(t, e)
			seed := testSeedRealPubRelReady(t, e)
			wire := testRow2Wire(e)
			testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
			if !bytes.Equal(wire.written, testRow2PubRelFrame(seed.pi)) {
				t.Fatal("fixture did not deliver the canonical PUBREL")
			}
			testRow2Eventually(t, testRow2Timeout, "PUBREL action drain", func() bool {
				cur, _, err := e.asm.loadInflightRecord(seed.subject)
				return err == nil && cur != nil && cur.ActionToken == _EMPTY_
			})
			before, sm := e.loadRegister(t, seed.pi)
			obs := testF105ObservePubComp(t, e.sess)
			var terminalizations atomic.Int64
			testF105SetAckAdvance(t, e.sess, func(req *mqttAdvanceExchangeAckRequest,
				write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				if testF105IsPubCompAdvance(req, seed.lin) {
					terminalizations.Add(1)
				}
				return write()
			})
			var pub, rel *mqttPending
			var calls int
			var identityOK bool
			// The synchronous hook suspends PUBCOMP in its unlocked discharge
			// interval. No goroutine or callback referring to t can escape the test.
			hook := func(sess *mqttSession, pi uint16) {
				if sess != e.sess || pi != seed.pi {
					return
				}
				calls++
				sess.mu.Lock()
				defer sess.mu.Unlock()
				rel = sess.pendingPubRel[pi]
				identityOK = sess.c == e.c && rel != nil && !sess.recoveryPIHeldLocked(pi)
				if !identityOK {
					return
				}
				ack, dur, seq := rel.jsAckSubject, rel.jsDur, rel.sseq
				identityOK = ack == seed.reply(1) && dur == seed.pubRel && seq == seed.canon
				if held {
					pub = mqttRecoveryOpaquePending(sm.Sequence)
					sess.pendingPublish[pi] = pub
					testDemoteInPlace(rel)
				}
				identityOK = identityOK && sess.c == e.c && rel.jsAckSubject == ack &&
					rel.jsDur == dur && rel.sseq == seq
			}
			prev := mqttTestPubCompDischargeHookV.Load()
			mqttTestPubCompDischargeHookV.Store(&hook)
			restore := mqttTestRestoreOnce(func() { mqttTestPubCompDischargeHookV.Store(prev) })
			t.Cleanup(restore)
			e.c.mqttProcessPubComp(seed.pi)
			restore()
			if calls != 1 || !identityOK {
				t.Fatalf("did not cross first gate with unchanged transport: calls=%d identity=%v", calls, identityOK)
			}
			acks, workerAcks, enrolled, _ := obs.counts()
			e.sess.mu.Lock()
			sameOwners := e.sess.pendingPublish[seed.pi] == pub && e.sess.pendingPubRel[seed.pi] == rel
			released := e.sess.pendingPublish[seed.pi] == nil && e.sess.pendingPubRel[seed.pi] == nil
			e.sess.mu.Unlock()
			if held {
				after, afterMsg := e.loadRegister(t, seed.pi)
				x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, seed.lin)
				if !sameOwners || afterMsg.Sequence != sm.Sequence || !reflect.DeepEqual(after, before) ||
					err != nil || x == nil || x.Phase != mqttExchangePubRecObserved ||
					terminalizations.Load() != 0 || acks != 0 || enrolled != 0 {
					t.Fatalf("second gate consumed quarantine: owners=%v terminalizations=%d acks=%d enrolled=%d exchange=%+v err=%v",
						sameOwners, terminalizations.Load(), acks, enrolled, x, err)
				}
			} else if !released || terminalizations.Load() != 1 || acks != 1 || workerAcks != 0 || enrolled != 0 {
				t.Fatalf("positive PUBCOMP did not finish directly: released=%v terminalizations=%d acks=%d worker=%d enrolled=%d",
					released, terminalizations.Load(), acks, workerAcks, enrolled)
			}
		})
	}
}
