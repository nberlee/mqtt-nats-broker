package mqtt

import (
	"errors"
	"sync/atomic"
	"testing"
)

// The shared last-holder scan authorizes an IRREVERSIBLE delete of a group consumer whose
// backlog is interest-retained, so it may only report "no other holder" out of a record set
// it read in full. This file pins the refusal arms and, just as importantly, the arms that
// must NOT refuse: a scan answering "held" for everything would satisfy every refusal
// assertion here while never releasing a group durable again.
//
// The end-to-end half — that a client's UNSUBSCRIBE really rides this answer — is
// TestMQTTSharedLastHolderRefusalKeepsTheGroupDurable, which needs a real MQTT client and so
// lives behind the mqttport_fulltests tag.

// testSharedHolderKey is the session key a member holding "$share/<group>/<subject>" carries
// in its record. Built from the production prefix, so a rename cannot leave the fixture
// asserting against a key nothing writes.
func testSharedHolderKey(group, subject string) string {
	return string(mqttSharedSubPrefix) + group + "/" + subject
}

// testPlantSessionRecord stores raw bytes on another session's $MQTT_sess subject, derived
// from a client-ID hash exactly as production does, so the record is one the scan's filter
// really walks.
func testPlantSessionRecord(t *testing.T, asm *mqttAccountSessionManager, ownerID string, data []byte) {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + asm.domainTk + getHash(ownerID)
	if _, err := asm.jsa.storeMsg(subject, 0, data); err != nil {
		t.Fatalf("planting the session record of %q: %v", ownerID, err)
	}
}

// testPlantSessionRecordJSON is testPlantSessionRecord for a record that decodes.
func testPlantSessionRecordJSON(t *testing.T, asm *mqttAccountSessionManager, ownerID string, ps *mqttPersistedSession) {
	t.Helper()
	testPlantSessionRecord(t, asm, ownerID, testMustJSON(t, ps))
}

// testSharedConsRecord is a record of the one shape this broker never writes: a shared group
// consumer configuration in Cons. buildPersistedSessionLocked skips every entry whose
// DeliverGroup is non-empty, because SharedSubs is the sole authority — so this record
// decodes perfectly and its SharedSubs, which is all the scan reads, hides the holder.
func testSharedConsRecord(clientID, group, subject string) *mqttPersistedSession {
	_, deliver, durable := mqttSharedNames(group, subject)
	return &mqttPersistedSession{
		ID: clientID,
		Cons: map[string]*ConsumerConfig{
			testSharedHolderKey(group, subject): {
				Durable:        durable,
				DeliverSubject: deliver,
				DeliverGroup:   group,
			},
		},
	}
}

// TestMQTTSharedLastHolderScanFailsClosed pins the scan's whole-map answer across the
// records it can and cannot interpret.
//
// Each case gets its OWN environment, therefore its own stream. Sharing one would let a
// record planted by an earlier case still trigger the refusal during a later one, and the
// mutation that deletes the unsupported-shape arm would then survive on the malformed
// case's leftovers.
//
// C1-C3 are permanent CONTROL arms, not decoration. "Nothing planted" alone does not reject
// an implementation that treats every FOREIGN record as uninterpretable, and neither case
// discriminates a scan that simply answers held for everything: C2 supplies a well-formed
// foreign record that must NOT refuse, and C3 a well-formed foreign holder of exactly one of
// the two durables, so the answer is proved to be per-durable rather than all-or-nothing.
func TestMQTTSharedLastHolderScanFailsClosed(t *testing.T) {
	const clientID = "holderscan"
	const groupA, groupB = "ga", "gb"
	const subjectA, subjectB = "fooa", "foob"

	for _, tc := range []struct {
		name string
		// plant runs against the live account session manager; nil plants nothing.
		plant func(t *testing.T, e *testOrphanEnv)
		// wantA and wantB are the whole expected answer. Asserting the map rather than one
		// entry is what kills a refusal marking only the first still-pending durable.
		wantA, wantB bool
	}{
		{"C1 no foreign record", nil, false, false},
		{"C2 well-formed foreign non-holder", func(t *testing.T, e *testOrphanEnv) {
			testPlantSessionRecordJSON(t, e.asm, "bystander", &mqttPersistedSession{
				ID:         "bystander",
				SharedSubs: map[string]byte{testSharedHolderKey("othergroup", "othersubject"): 1},
			})
		}, false, false},
		{"C3 well-formed foreign selective holder", func(t *testing.T, e *testOrphanEnv) {
			testPlantSessionRecordJSON(t, e.asm, "memberofa", &mqttPersistedSession{
				ID:         "memberofa",
				SharedSubs: map[string]byte{testSharedHolderKey(groupA, subjectA): 1},
			})
		}, true, false},
		{"C4 undecodable record", func(t *testing.T, e *testOrphanEnv) {
			testPlantSessionRecord(t, e.asm, "corrupt", []byte("{not json"))
		}, true, true},
		{"C5 foreign record carrying a shared group consumer in Cons", func(t *testing.T, e *testOrphanEnv) {
			testPlantSessionRecordJSON(t, e.asm, "foreignshape",
				testSharedConsRecord("foreignshape", "somegroup", "somesubject"))
		}, true, true},
		{"C6 same-ID record carrying a shared group consumer in Cons", func(t *testing.T, e *testOrphanEnv) {
			// ps.ID is the LIVE session's client ID on a subject that is not its own, so the
			// record reaches the own-record skip. That is what makes this case pin the
			// ORDERING: with the shape check placed below that skip the record is skipped
			// and the scan answers "not held", which is the destructive answer.
			testPlantSessionRecordJSON(t, e.asm, "impostorsubject",
				testSharedConsRecord(clientID, "somegroup", "somesubject"))
		}, true, true},
		{"C7 indeterminate walk", func(t *testing.T, e *testOrphanEnv) {
			// The one refusal arm that predates the other two and, until F-082, was pinned
			// by NOTHING at either layer: the three sibling fixtures using this seam drive
			// the PUBREL barrier scan and the in-flight walks, not this one. A record cannot
			// express it — the walk has to fail — so the case arms the load seam instead of
			// planting bytes, and asserts the seam actually fired, because a hook that
			// silently never matched would leave this arm passing for the reason C1 passes.
			var fired atomic.Bool
			restore := setMQTTTestLoadNextMsgHook(func(stream, subject string, _ uint64) error {
				if stream != mqttSessStreamName {
					return nil
				}
				fired.Store(true)
				return errors.New("test: session record walk unavailable")
			})
			t.Cleanup(func() {
				restore()
				if !fired.Load() {
					t.Error("the $MQTT_sess walk seam never fired, so this case never reached " +
						"the indeterminate arm")
				}
			})
		}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewOrphanEnv(t, clientID)
			_, _, durA := mqttSharedNames(groupA, subjectA)
			_, _, durB := mqttSharedNames(groupB, subjectB)

			if tc.plant != nil {
				tc.plant(t, e)
			}

			held := e.sess.sharedDurablesHeldByOthers([]string{durA, durB})
			if len(held) != 2 {
				t.Fatalf("the scan answered for %d durables, want 2: %v", len(held), held)
			}
			if held[durA] != tc.wantA || held[durB] != tc.wantB {
				t.Fatalf("the scan answered {A:%v B:%v}, want {A:%v B:%v}",
					held[durA], held[durB], tc.wantA, tc.wantB)
			}
		})
	}
}
