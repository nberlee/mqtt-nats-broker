package mqtt

import (
	"errors"
	"strings"
	"testing"
)

// testDisplaceableFacts is a marked record whose retirement is durably owned on every count.
func testDisplaceableFacts() mqttMarkedRecordFacts {
	return mqttMarkedRecordFacts{
		gen:             "g1",
		markerSeq:       10,
		receipt:         mqttOutboundValid,
		watermark:       &mqttGenRetirement{Gen: "g1", MarkerSeq: 10},
		watermarkStatus: mqttRetirementValid,
		releaseFacts:    mqttAdoptedReleaseFacts{Closed: &mqttGenClosure{Gen: "g1", Discard: true}},
	}
}

// Each of the five facts is necessary: falsifying any one of them alone, or leaving it
// undecided, keeps the inline adoption.
func TestMQTTMarkedRecordDisplaceablePredicate(t *testing.T) {
	if ok, why := mqttMarkedRecordDisplaceable(testDisplaceableFacts()); !ok {
		t.Fatalf("a durably owned retirement was refused: %s", why)
	}
	// A later retirement's watermark, at a higher marker sequence, still names the claim.
	later := testDisplaceableFacts()
	later.watermark = &mqttGenRetirement{Gen: "g1", MarkerSeq: 11}
	if ok, why := mqttMarkedRecordDisplaceable(later); !ok {
		t.Fatalf("a watermark past the marker's sequence was refused: %s", why)
	}

	cases := []struct {
		name   string
		mutate func(*mqttMarkedRecordFacts)
		reason string
	}{
		{"foreign targets", func(f *mqttMarkedRecordFacts) { f.targetsErr = errors.New("foreign") }, "not this session's"},
		{"receipt absent", func(f *mqttMarkedRecordFacts) { f.receipt = mqttOutboundAbsent }, "teardown receipt"},
		{"receipt indeterminate", func(f *mqttMarkedRecordFacts) { f.receipt = mqttOutboundIndeterminate }, "teardown receipt"},
		{"watermark absent", func(f *mqttMarkedRecordFacts) {
			f.watermark, f.watermarkStatus = nil, mqttRetirementAbsent
		}, "watermark reads"},
		{"watermark indeterminate", func(f *mqttMarkedRecordFacts) {
			f.watermark, f.watermarkStatus = nil, mqttRetirementIndeterminate
		}, "watermark reads"},
		{"watermark unresolved beside a decoded value", func(f *mqttMarkedRecordFacts) {
			f.watermarkStatus = mqttRetirementIndeterminate
		}, "watermark reads"},
		{"watermark of another generation", func(f *mqttMarkedRecordFacts) {
			f.watermark = &mqttGenRetirement{Gen: "g0", MarkerSeq: 10}
		}, "names generation"},
		{"watermark below the marker", func(f *mqttMarkedRecordFacts) {
			f.watermark = &mqttGenRetirement{Gen: "g1", MarkerSeq: 9}
		}, "names generation"},
		{"release facts unreadable", func(f *mqttMarkedRecordFacts) { f.releaseFactsErr = errors.New("unreadable") }, "could not be read"},
		{"no closure", func(f *mqttMarkedRecordFacts) { f.releaseFacts.Closed = nil }, "discarding closure"},
		{"preserving closure", func(f *mqttMarkedRecordFacts) {
			f.releaseFacts.Closed = &mqttGenClosure{Gen: "g1", Discard: false}
		}, "discarding closure"},
		{"release gate refuses", func(f *mqttMarkedRecordFacts) { f.releaseErr = errors.New("unsealed") }, "release gate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := testDisplaceableFacts()
			tc.mutate(&f)
			ok, why := mqttMarkedRecordDisplaceable(f)
			if ok {
				t.Fatal("displaceable, want the inline adoption")
			}
			if !strings.Contains(why, tc.reason) {
				t.Fatalf("refused for %q, want a reason naming %q", why, tc.reason)
			}
		})
	}
}
