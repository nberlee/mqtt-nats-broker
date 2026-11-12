// The session stream's subject set and the outbox decision shapes (issue #133: the acceptance
// facts these sat beside were replaced by Will anchors).
//
// The claimant-side half — what a delayed Will does with these facts — lives in
// will_accept_fence_test.go.

package mqtt

import (
	"fmt"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// testPlantAcceptFact writes one well-formed acceptance fact directly, bypassing
// the accept batch. Used where the test needs a fact at a chosen TIME or a chosen
// stream position rather than whatever a real CONNECT would produce.
// testSessStreamSubjects is the exact subject set $MQTT_sess must carry: the
// session records, the teardown receipts, the in-flight and outbound records (the
// authorities and lifecycle registry among them), and the shared-group lifecycle records.
func testSessStreamSubjects() []string {
	return []string{
		mqttSessStreamSubjectPrefix + ">",
		mqttInflightStreamSubjectPrefix + ">",
		mqttSharedLifecycleStreamSubjectPrefix + ">",
	}
}

// TestWillDecisionLegalMatrix pins the shapes a record may carry, and every shape it
// may not. publishing/cancel is the row that matters most: a cancel is licensed
// straight to cancelled, so a publishing record carrying one could only be a
// decision the entry branch would try to EXECUTE.
func TestWillDecisionLegalMatrix(t *testing.T) {
	t.Parallel()
	cut := uint64(5)
	for _, tc := range []struct {
		state mqttWillState
		dec   mqttWillDecision
		cutof *uint64
		legal bool
	}{
		{mqttWillPending, mqttWillDecisionUnset, nil, true},
		{mqttWillPending, mqttWillDecisionPublish, &cut, false},
		{mqttWillPending, mqttWillDecisionCancel, &cut, false},
		{mqttWillPublishing, mqttWillDecisionUnset, nil, true},
		{mqttWillPublishing, mqttWillDecisionPublish, &cut, true},
		{mqttWillPublishing, mqttWillDecisionCancel, &cut, false},
		{mqttWillCancelled, mqttWillDecisionCancel, &cut, true},
		{mqttWillCancelled, mqttWillDecisionPublish, &cut, false},
		{mqttWillCancelled, mqttWillDecisionUnset, nil, false},
		{mqttWillDone, mqttWillDecisionPublish, &cut, true},
		{mqttWillDone, mqttWillDecisionCancel, &cut, false},
		{mqttWillDone, mqttWillDecisionUnset, nil, false},
		{mqttWillPublishing, mqttWillDecisionPublish, nil, false},
		{mqttWillPublishing, mqttWillDecisionUnset, &cut, false},
		{mqttWillPending, "nonsense", &cut, false},
	} {
		t.Run(fmt.Sprintf("%s/%s/cutoff=%v", tc.state, tc.dec, tc.cutof != nil), func(t *testing.T) {
			err := validateWillDecisionShape(tc.state, tc.dec, tc.cutof)
			if tc.legal && err != nil {
				t.Fatalf("this shape is legal: %v", err)
			}
			if !tc.legal && err == nil {
				t.Fatal("this shape must be refused")
			}
		})
	}
}

// TestValidateSessStreamRequiresBatchesAndEverySubjectSpace is T10.
func TestValidateSessStreamRequiresBatchesAndEverySubjectSpace(t *testing.T) {
	t.Parallel()
	const domainTk = _EMPTY_
	base := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:               mqttSessStreamName,
			Subjects:           testSessStreamSubjects(),
			Storage:            FileStorage,
			Retention:          LimitsPolicy,
			Discard:            DiscardOld,
			MaxMsgsPer:         1,
			Replicas:           3,
			AllowAtomicPublish: true,
			Metadata:           map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
		}}
	}
	if err := validateSessStream(base(), domainTk, 3); err != nil {
		t.Fatalf("the baseline configuration must validate: %v", err)
	}

	for _, tc := range []struct {
		name    string
		poison  func(*StreamConfig)
		errWant string
	}{
		{"no atomic publish", func(c *StreamConfig) { c.AllowAtomicPublish = false }, "allow_atomic"},
		{"only the record subject", func(c *StreamConfig) {
			c.Subjects = []string{mqttSessStreamSubjectPrefix + domainTk + ">"}
		}, "subjects"},
		{"only the in-flight subject", func(c *StreamConfig) {
			c.Subjects = []string{mqttInflightStreamSubjectPrefix + ">"}
		}, "subjects"},
		{"an extra subject", func(c *StreamConfig) {
			c.Subjects = append(c.Subjects, "$MQTT.other.>")
		}, "subjects"},
		// The duplicate list is the RIGHT LENGTH, because the size check runs first: a
		// longer list would be refused for its size and this case would stop testing the
		// repeat it exists for. One subject space is therefore left unowned, which is the
		// hazard the repeat check is about.
		{"the record subject twice", func(c *StreamConfig) {
			c.Subjects = []string{
				mqttSessStreamSubjectPrefix + domainTk + ">",
				mqttSessStreamSubjectPrefix + domainTk + ">",
				mqttInflightStreamSubjectPrefix + ">",
			}
		}, "repeat"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := base()
			tc.poison(&si.Config)
			err := validateSessStream(si, domainTk, 3)
			if err == nil {
				t.Fatal("the validator must refuse this configuration")
			}
			if !strings.Contains(err.Error(), tc.errWant) {
				t.Fatalf("the refusal must name %q, got: %v", tc.errWant, err)
			}
		})
	}

	// Order independence is the one relaxation, and it is deliberate: JetStream does
	// not promise to return the configured order.
	si := base()
	si.Config.Subjects = []string{
		mqttSharedLifecycleStreamSubjectPrefix + ">",
		mqttInflightStreamSubjectPrefix + ">",
		mqttSessStreamSubjectPrefix + domainTk + ">",
	}
	if err := validateSessStream(si, domainTk, 3); err != nil {
		t.Fatalf("the subject set is order-independent: %v", err)
	}
}
