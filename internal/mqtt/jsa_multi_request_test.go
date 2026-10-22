package mqtt

import (
	"testing"
	"time"
)

// A round with nothing in it returns at once: zero replies never satisfy the multi request's wait,
// so a caller with nothing to read would otherwise wait out the whole request timeout.
func TestJSAMultiRequestEmptyReturnsImmediately(t *testing.T) {
	e := testNewInflightEnv(t, "client-empty-multi")
	jsa := &e.asm.jsa
	start := time.Now()
	if r, err := jsa.loadLastMsgForMulti(mqttInflightStreamName, nil); err != nil || len(r) != 0 {
		t.Fatalf("an empty subject round: %d results, %v", len(r), err)
	}
	if r, err := jsa.loadMsgsMulti(mqttStreamName, nil); err != nil || len(r) != 0 {
		t.Fatalf("an empty sequence round: %d results, %v", len(r), err)
	}
	if infos, ok, err := jsa.lookupConsumersMulti(mqttStreamName, nil); err != nil || len(infos) != 0 || len(ok) != 0 {
		t.Fatalf("an empty consumer round: %d results, %v", len(infos), err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("three empty rounds took %v: an empty round waited for replies that never come", d)
	}
	// Argument validation still precedes the fast return.
	if _, err := jsa.newRequestExMultiKinds([]string{mqttJSAMsgLoad}, nil, nil, nil); err == nil {
		t.Fatal("a round whose kinds and messages disagree in length must be refused")
	}
}
