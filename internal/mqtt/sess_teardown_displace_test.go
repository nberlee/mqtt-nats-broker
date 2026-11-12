package mqtt

import (
	"errors"
	"strings"
	"testing"
)

// testDisplaceableFacts is a marked record whose retirement is durably owned on every count.
func testDisplaceableFacts() mqttMarkedRecordFacts {
	return mqttMarkedRecordFacts{
		gen:          "g1",
		markerSeq:    10,
		auth:         mqttOutboundValid,
		pendingEntry: true,
		releaseFacts: mqttAdoptedReleaseFacts{Closed: &mqttGenClosure{Gen: "g1"}},
	}
}

// Each of the four facts is necessary: falsifying any one of them alone, or leaving it
// undecided, keeps the inline adoption.
func TestMQTTMarkedRecordDisplaceablePredicate(t *testing.T) {
	if ok, why := mqttMarkedRecordDisplaceable(testDisplaceableFacts()); !ok {
		t.Fatalf("a durably owned retirement was refused: %s", why)
	}

	cases := []struct {
		name   string
		mutate func(*mqttMarkedRecordFacts)
		reason string
	}{
		{"foreign targets", func(f *mqttMarkedRecordFacts) { f.targetsErr = errors.New("foreign") }, "not this session's"},
		{"authority absent", func(f *mqttMarkedRecordFacts) { f.auth = mqttOutboundAbsent }, "authority reads"},
		{"authority indeterminate", func(f *mqttMarkedRecordFacts) { f.auth = mqttOutboundIndeterminate }, "authority reads"},
		{"no pending release of the marker", func(f *mqttMarkedRecordFacts) { f.pendingEntry = false }, "pending release"},
		{"release facts unreadable", func(f *mqttMarkedRecordFacts) { f.releaseFactsErr = errors.New("unreadable") }, "could not be read"},
		{"no closure", func(f *mqttMarkedRecordFacts) { f.releaseFacts.Closed = nil }, "no closure"},
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
