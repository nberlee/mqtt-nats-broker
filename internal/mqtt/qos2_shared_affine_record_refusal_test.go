//go:build mqtt_experimental_shared_qos2

// Copyright 2025 The NATS Authors
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
	"strings"
	"testing"

	"github.com/nats-io/nuid"
)

// A persisted qos-2 shared membership IMPLIES an affine generation. No writer in
// this tree can separate them — provisioning happens in the SUBSCRIBE's first
// filter loop, before the membership is persisted, and buildPersistedSessionLocked
// writes SharedSubs, Q2Aff and Q2AffGen in one record — so a record that does
// separate them predates the affine path and is REFUSED rather than accommodated.
//
// The refusal is not bookkeeping. Q2AffGen is what a contender compares a claim
// against to conclude the winner is gone, and an EMPTY one mismatches every claim:
// read instead of refused, such a record turns a malformed shape into proof of
// absence and discards an assignment owed to a delivery already started, which is
// the redistribution [MQTT-4.8.2-5] forbids.
//
// testG7Cases carries that shape and the shapes the rule must NOT over-refuse, in
// one table shared by every enforcement point, so the refusal and the exemptions
// cannot drift apart between them.
func testG7SharedKey(t *testing.T, group, topic string) string {
	t.Helper()
	subj, err := mqttTopicToNATSPubSubject([]byte(topic))
	if err != nil {
		t.Fatalf("subject conversion for %q: %v", topic, err)
	}
	key := (&mqttFilter{shared: true, group: group, filter: string(subj)}).sessionKey()
	if _, _, ok := mqttParseSharedKey(key); !ok {
		t.Fatalf("the shared key %q does not parse back, so the fixture proves nothing", key)
	}
	return key
}

// testG7Cases is the acceptance matrix. `wantErr` is the refusal message the record
// must produce IN FULL, or empty when the record must be ACCEPTED; every enforcement
// point is driven over the same rows. It is per-row and compared exactly because the
// rules refuse for different reasons, and reporting a neighbouring rule's reason
// would send an operator after the wrong defect.
func testG7Cases(t *testing.T) []struct {
	name    string
	wantErr string
	build   func() *mqttPersistedSession
} {
	t.Helper()
	key2 := testG7SharedKey(t, "g", "g7/topic")
	return []struct {
		name    string
		wantErr string
		build   func() *mqttPersistedSession
	}{
		{
			// THE REFUSED SHAPE.
			name:    "shared QoS 2 membership with no affine generation",
			wantErr: "record carries a shared QoS 2 subscription with no affine generation",
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{SharedSubs: map[string]mqttSharedMembership{key2: testSharedMembership(key2, 2)}}
			},
		},
		{
			// A qos-2 entry alongside legal ones still refuses: the rule is over ANY
			// qos-2 entry, not over the map being uniformly qos 2.
			name:    "one qos-2 membership among qos-0 and qos-1 ones",
			wantErr: "record carries a shared QoS 2 subscription with no affine generation",
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{SharedSubs: map[string]mqttSharedMembership{
					testG7SharedKey(t, "g0", "g7/zero"): testSharedMembership(testG7SharedKey(t, "g0", "g7/zero"), 0),
					testG7SharedKey(t, "g1", "g7/one"):  testSharedMembership(testG7SharedKey(t, "g1", "g7/one"), 1),
					key2:                                testSharedMembership(key2, 2),
				}}
			},
		},
		{
			// OVER-REFUSAL GUARD. Shared QoS 0 provisions no affine path, so its
			// record legitimately carries no generation. Mutation M-D (apply the rule
			// to every SharedSubs entry regardless of QoS) breaks exactly here.
			name: "shared QoS 0 membership with no affine generation",
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{SharedSubs: map[string]mqttSharedMembership{testG7SharedKey(t, "g0", "g7/zero"): testSharedMembership(testG7SharedKey(t, "g0", "g7/zero"), 0)}}
			},
		},
		{
			// The other half of M-D: shared QoS 1 redistributes freely and has no
			// affine path either.
			name: "shared QoS 1 membership with no affine generation",
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{SharedSubs: map[string]mqttSharedMembership{testG7SharedKey(t, "g1", "g7/one"): testSharedMembership(testG7SharedKey(t, "g1", "g7/one"), 1)}}
			},
		},
		{
			// OVER-REFUSAL GUARD. A generation with no consumer config is the shape
			// the `minted` gate is built around — the un-minted resume — and the
			// validator permits it today. Whether any writer can ORIGINATE it is a
			// pre-existing question about the affine design that this change does not
			// settle and deliberately does not decide: refusing it here would be a new
			// refusal of a shape whose non-existence is unproven, in a validator the
			// sweep and teardown adoption also read. These rows pin only that the G7
			// rule does not widen onto it. Mutation M-E (extend the refusal to it)
			// breaks here.
			name: "affine generation with no consumer config and a qos-2 membership",
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{
					SharedSubs: map[string]mqttSharedMembership{key2: testSharedMembership(key2, 2)},
					Q2AffGen:   mqttNewQ2AffGen(),
				}
			},
		},
		{
			// The same shape with no membership at all: the resume that owes a
			// delivery already started to a generation it did not mint.
			name: "affine generation with neither a consumer config nor a membership",
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{Q2AffGen: mqttNewQ2AffGen()}
			},
		},
		{
			// THE BYPASS. processSubs clamps a filter above QoS 2 down to 2 while
			// restoring, and the resume feeds SharedSubs values in verbatim — so this
			// record becomes a live shared QoS 2 membership. A rule matching only the
			// literal 2 would let it through into every destructive reader the refusal
			// exists to stop. No writer here can produce it: the value is persisted
			// after processSubs has already bounded it.
			name:    "shared membership with an out-of-range QoS 3",
			wantErr: `record carries a shared subscription "$share/g/g7.topic" with an invalid QoS 3`,
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{SharedSubs: map[string]mqttSharedMembership{key2: testSharedMembership(key2, 3)}}
			},
		},
		{
			// The top of the byte range, and with an affine generation present: the
			// record is malformed whether or not its affine pair happens to validate.
			name:    "shared membership with QoS 255 and a valid affine generation",
			wantErr: `record carries a shared subscription "$share/g/g7.topic" with an invalid QoS 255`,
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{
					SharedSubs: map[string]mqttSharedMembership{key2: testSharedMembership(key2, 255)},
					Q2AffGen:   mqttNewQ2AffGen(),
				}
			},
		},
		{
			// No shared subscriptions at all: the ordinary record.
			name: "no shared memberships",
			build: func() *mqttPersistedSession {
				return &mqttPersistedSession{Subs: map[string]byte{"g7.plain": 2}}
			},
		},
	}
}

// TestQ2AffRecordAffinePairRefusesAnOrphanedSharedQoS2Membership drives the rule
// itself, which is where the reference walk and teardown adoption both read it.
//
// Mutation M-B: drop the rule from mqttValidateRecordAffinePair and the refused
// rows below are accepted — after which the sweep's re-reads treat the shape as a
// valid record for a different generation, which is proof of absence.
func TestQ2AffRecordAffinePairRefusesAnOrphanedSharedQoS2Membership(t *testing.T) {
	t.Parallel()
	const cid = "g7-validator"
	idHash := string(getHash(cid))
	for _, tc := range testG7Cases(t) {
		t.Run(tc.name, func(t *testing.T) {
			ps := tc.build()
			ps.ID = cid
			ps.Gen = mqttNewSessGen()
			err := mqttValidateRecordAffinePair(ps, idHash)
			if tc.wantErr != _EMPTY_ {
				if err == nil {
					t.Fatal("an uninterpretable record was accepted; a contender would then read " +
						"its empty affine generation as proof its winner is gone")
				}
				// The diagnostic has to name THIS defect: a neighbouring rule's reason
				// would send an operator after the wrong one.
				if err.Error() != tc.wantErr {
					t.Fatalf("refusal names the wrong defect:\n got: %v\nwant: %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("a legal record was refused, which would strand a live session: %v", err)
			}
		})
	}
}

// TestQ2AffMaterializeOnBehalfRefusesAnUninterpretableWinnerRecord is the fourth
// enforcement point, and the one the shape is dangerous at — in BOTH directions,
// which is why the validation sits ahead of the generation comparison rather than
// inside one of its arms.
//
// On a MISMATCH the function reads `ps.Q2AffGen != claim.Gen` as proof the winner is
// gone and then discards the claim, confirms the source ack and reaps. The G7 shape
// carries an EMPTY affine generation, which mismatches every claim, so without
// validation a malformed record becomes proof of absence and an assignment owed to a
// started delivery is irreversibly discarded.
//
// On a MATCH it materializes the winner's copy and acknowledges the group source.
// That acknowledgement is irreversible, and the copy is owed to a session that can
// never resume — the same record is refused at CONNECT and its affine reconciliation
// is suspended in the sweep — so the delivery would be stranded. A record can match
// a claim's generation and still be malformed: the QoS-255 row below carries a valid
// affine generation.
//
// The contract on refusal is the function's own: anything indeterminate leaves the
// claim untouched and acknowledges nothing.
//
// Mutation M-G: remove the validation entirely — both rows act.
// Mutation M-K: move it back inside the mismatch arm — the matching row acts.
func TestQ2AffMaterializeOnBehalfRefusesAnUninterpretableWinnerRecord(t *testing.T) {
	for _, tc := range []struct {
		name string
		// record is built with the claim's own generation available, so a row can
		// choose to MATCH it or not.
		record func(claimGen string) *mqttPersistedSession
	}{
		{
			// Mismatch: an empty affine generation differs from every claim.
			name: "shared QoS 2 membership with no affine generation",
			record: func(string) *mqttPersistedSession {
				return &mqttPersistedSession{
					ID: "g7-winner", Gen: mqttNewSessGen(),
					SharedSubs: map[string]mqttSharedMembership{testG7SharedKey(t, "g", "g7/onbehalf"): testSharedMembership(testG7SharedKey(t, "g", "g7/onbehalf"), 2)},
				}
			},
		},
		{
			// MATCH: the generation is the claim's own, so the comparison sends this
			// record down the materialize-and-ack arm. It is still uninterpretable.
			name: "matching affine generation with an out-of-range shared QoS",
			record: func(claimGen string) *mqttPersistedSession {
				return &mqttPersistedSession{
					ID: "g7-winner", Gen: mqttNewSessGen(), Q2AffGen: claimGen,
					SharedSubs: map[string]mqttSharedMembership{testG7SharedKey(t, "g", "g7/onbehalf"): testSharedMembership(testG7SharedKey(t, "g", "g7/onbehalf"), 255)},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewOrphanEnv(t, "g7-onbehalf-"+strings.ReplaceAll(tc.name, " ", "-"))
			idHash, gen := getHash("g7-winner"), mqttNewQ2AffGen()
			groupDur := mqttSharedConsumerDurablePrefix + "G7ONBEHALF"
			subject := mqttQ2AffClaimSubject(groupDur, 77)
			claim := testValidQ2AffClaim(idHash, gen, groupDur, 77)
			if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, claim); err != nil {
				t.Fatalf("store claim: %v", err)
			}
			// The winner's affine consumer must really exist, with the identity the
			// claim records. Without it the MATCHING row returns false at the consumer
			// lookup instead of at the validation, and passes against a validator that
			// only guards the mismatch arm — which is exactly what it is here to catch.
			if dur := testCreateAffineConsumer(t, e, idHash, gen); dur != claim.AffDurable {
				t.Fatalf("fixture consumer %q is not the one the claim names (%q)", dur, claim.AffDurable)
			}

			rec := tc.record(gen)
			e.asm.mu.Lock()
			e.asm.testSessionLoad = func(string) (*mqttPersistedSession, uint64, error) {
				return rec, 1, nil
			}
			e.asm.mu.Unlock()
			t.Cleanup(func() {
				e.asm.mu.Lock()
				e.asm.testSessionLoad = nil
				e.asm.mu.Unlock()
			})
			// The fixture must really be the arm it claims, or a row proves the other
			// one twice.
			if (rec.Q2AffGen == claim.Gen) != strings.HasPrefix(tc.name, "matching") {
				t.Fatalf("fixture is on the wrong arm: record gen %q vs claim gen %q", rec.Q2AffGen, claim.Gen)
			}

			if e.asm.materializeQ2AffOnBehalf(e.sess, e.s, claim, subject, 1, &mqttQ2AffineEnvelope{}, &mqttQ2AffMoveInput{}) {
				t.Fatal("an uninterpretable winner record was acted on")
			}
			cur, _, err := e.asm.loadQ2AffClaim(subject)
			if err != nil {
				t.Fatalf("load claim: %v", err)
			}
			if cur == nil {
				t.Fatal("the claim was reaped on the strength of a record the broker cannot interpret")
			}
			if cur.Discarded {
				t.Fatalf("an assignment possibly owed to a started delivery was discarded: %+v", cur)
			}
			if cur.SourceAcked {
				t.Fatalf("the group source was acknowledged for a delivery that cannot be completed: %+v", cur)
			}
			if cur.AffineDone {
				t.Fatalf("the assignment was marked delivered off an uninterpretable record: %+v", cur)
			}
			// Phase too: materializing a copy advances it, and that is the step the
			// matching arm would otherwise take before acknowledging the source.
			if cur.Phase != mqttQ2AffClaimed {
				t.Fatalf("the claim advanced past %q off an uninterpretable record: %+v", mqttQ2AffClaimed, cur)
			}
		})
	}
}

// TestOrphanSweepAffinePairFailureSkipsOnlyTheAffinePhases pins the SCOPE of a
// reference-walk refusal, which is the whole reason the walk reports it separately
// from a fatal one.
//
// A record whose affine PAIR does not validate contributes no affine reference,
// while the session it describes may own an affine consumer and outstanding claims
// — so reconcileOrphanQ2AffClaims is suppressed. That ONE pass, and not "every affine
// resource": the retired affine register arm is outside this gate, and affine consumer
// deletion no longer exists (F-036 course E). Its IDENTITY is sound, though:
// the inbound, in-flight and PUBREL references it contributes are complete, so
// those classes must keep being reclaimed. Returning early instead would let one
// such record disable every class account-wide, and PERMANENTLY, since nothing
// rewrites the record and expiry refuses it too — the failure the affine claim
// inventory below already avoids for exactly this reason.
//
// Both halves are asserted on every row, because each alone passes against a
// different broken implementation: the retained CLAIM alone also passes if the pass
// aborts wholesale, and the reclaimed hold alone also passes if the affine skip is
// dropped.
//
// The affine half is asserted on a CLAIM TRANSITION rather than on a consumer. Since
// F-036 course E this gate governs no consumer decision at all — the candidate-reclamation
// route is gone — so a surviving-consumer assertion would pass by default against any
// implementation, including one with the guard removed. What affineUsable still gates is
// reconcileOrphanQ2AffClaims.
//
// Mutation: return early on an affine-pair failure instead of skipping — the hold
// survives. Mutation: force affineUsable true — the bystander claim is discarded on
// evidence no record could supply.
func TestOrphanSweepAffinePairFailureSkipsOnlyTheAffinePhases(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record func() *mqttPersistedSession
	}{
		{"shared QoS 2 membership with no affine generation", func() *mqttPersistedSession {
			return &mqttPersistedSession{
				ID: "affbad", Gen: mqttNewSessGen(),
				SharedSubs: map[string]mqttSharedMembership{"$share/g/refused.topic": testSharedMembership("$share/g/refused.topic", 2)},
			}
		}},
		{"malformed affine generation", func() *mqttPersistedSession {
			return &mqttPersistedSession{ID: "affbad", Gen: mqttNewSessGen(), Q2AffGen: "short"}
		}},
		{"affine consumer config with no affine generation", func() *mqttPersistedSession {
			return &mqttPersistedSession{
				ID: "affbad", Gen: mqttNewSessGen(),
				Q2Aff: &ConsumerConfig{Durable: "$MQTT_Q2AFF_whatever"},
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewOrphanEnv(t, "affpair-"+strings.ReplaceAll(tc.name, " ", "-"))
			e.setGraces(t, 0, 0)

			// An inbound hold and an affine CLAIM, both orphaned: no record and no
			// local session references either once the env forgets its session.
			//
			// The claim must stay INSPECTABLE, like every other claim-transition fixture:
			// a LIVE group durable still holding a real source message, so the two-sided
			// reap predicate refuses. Without that, forcing the gate open could discard
			// AND immediately reap, and the assertion below would kill the mutation on
			// claim ABSENCE without ever proving the forbidden Discarded state.
			holdSubject := e.hold(t, 1, nuid.Next(), "payload")
			bystanderGroup := mqttSharedConsumerDurablePrefix + "AFFPAIR"
			topic := "affpair." + strings.ReplaceAll(tc.name, " ", "-")
			if _, derr := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
				Stream: mqttStreamName,
				Config: ConsumerConfig{
					Durable: bystanderGroup, FilterSubject: mqttStreamSubjectPrefix + topic,
					DeliverSubject: mqttSharedSubDeliverPrefix + "affpair", DeliverGroup: "g",
					AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 64,
				},
			}); derr != nil {
				t.Fatalf("create the bystander's group durable: %v", derr)
			}
			resp, serr := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+topic, 0, []byte("source"))
			if serr != nil {
				t.Fatalf("store the bystander's source message: %v", serr)
			}
			bystander := mqttQ2AffClaimSubject(bystanderGroup, resp.Sequence)
			if _, cerr := e.asm.storeQ2AffClaimCAS(bystander, 0,
				testValidQ2AffClaim(getHash("affine-bystander"), mqttNewQ2AffGen(),
					bystanderGroup, resp.Sequence)); cerr != nil {
				t.Fatalf("store the bystander claim: %v", cerr)
			}
			e.forget(t)
			testStoreRawSession(t, e, "affbad", tc.record())

			// Not fatal, and reported.
			refs, affBad, err := e.asm.buildSessionRefs()
			if err != nil {
				t.Fatalf("an affine-pair failure aborted the whole walk: %v", err)
			}
			if refs == nil || affBad == nil {
				t.Fatalf("the walk did not report the affine half indeterminate: refs=%v affBad=%v", refs, affBad)
			}

			// Three passes: the inbound half needs its quarantine pass and an affine
			// claim is grace-OBSERVED before a transition, so one pass settles neither.
			for i := 0; i < 3; i++ {
				e.asm.sweepOrphanedQoS2State(e.s)
			}

			if e.holdPresent(t, holdSubject) {
				t.Fatal("an orphaned inbound hold stopped being reclaimed because a DIFFERENT record's " +
					"affine pair could not be interpreted; that record says nothing about this resource class")
			}
			got, _, gerr := e.asm.loadQ2AffClaim(bystander)
			if gerr != nil {
				t.Fatalf("load the bystander claim: %v", gerr)
			}
			if got == nil {
				t.Fatal("the bystander claim was reaped, so this fixture cannot distinguish the " +
					"forbidden Discarded transition from a discard-then-reap")
			}
			if got.Discarded {
				t.Fatalf("an affine claim was transitioned while no record could say which affine "+
					"generations are still referenced: %+v", got)
			}
		})
	}
}
