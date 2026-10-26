//go:build mqtt_fulltests

package mqtt

import (
	"sync/atomic"
	"testing"
)

// The floor crossing a chunk's exchanges AFTER its duties were read and BEFORE they are read: the
// duties name exchanges the floor then retires and deletes, so the pass reads them absent, and an
// absence is judged against a fresh ledger read, which shows the floor past the naming duty, so
// the pass restarts from the new floor instead of reporting the retired positions as uncertain. The injection is the caller-side inflight-load seam, on the traversal's own goroutine:
// it forwards the duty read first and only then advances the floor.
func TestTraversalRestartsWhenFloorCrossesAFailedExchange(t *testing.T) {
	e, done := testF112Env(t, "floor-cross")
	defer done()
	jsa := &e.asm.jsa
	for i := 0; i < 3; i++ {
		testF112EndAndResolve(t, e, testF112Admit(t, e, 10))
	}
	keep := testF112Admit(t, e, 2)

	o := testObserveJSA(t, jsa)
	m := o.mark()
	var calls atomic.Int32
	var advFrom, advTo int // the fixture's own floor advance, excluded from the traversal's count
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		resps, err := e.asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
		if calls.Add(1) == 1 {
			advFrom = o.mark()
			led, _ := testF112Ledger(t, e)
			testF112AdvanceTo(t, e, led.Hi)
			advTo = o.mark()
		}
		return resps, err
	}
	e.asm.mu.Unlock()
	census, err := mqttTraverseGenerationInventory(jsa, e.asm, e.id, e.epoch, testF105Gen, nil)
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = nil
	e.asm.mu.Unlock()
	if err != nil || !census.complete || len(census.uncertain) != 0 || len(census.live) != 1 || census.live[0].Nonce != keep {
		t.Fatalf("exchanges retired between the duty read and the exchange reads were not absorbed by a restart: "+
			"err %v uncertain %v live %d", err, census.uncertain, len(census.live))
	}
	if calls.Load() < 2 {
		t.Fatalf("the traversal read its duties once (%d): the crossed exchanges must restart it", calls.Load())
	}
	// THE RESTART'S COST is bounded by the passes it takes: the first pass read one exchange, found
	// it absent, and one ledger read judged it crossed, which restarts at once; the second pass read
	// the surviving exchange. Nothing is re-read within a pass.
	idHash := getHash(e.id)
	xprefix := mqttF105SubjectPrefix() + mqttExchangeClassTk + "." + idHash + "." + testF105Gen + "."
	traversal := func(match func(string) bool) int {
		return o.sends(m, match) - (o.sends(advFrom, match) - o.sends(advTo, match))
	}
	if n := traversal(testSubjectHasPrefix(xprefix)); n != 1+1 {
		t.Fatalf("the restarting traversal read %d exchanges, want 2: one in the crossed pass, one after", n)
	}
	if n := traversal(testSubjectIs(mqttGenLedgerSubject(idHash, testF105Gen))); n != 3 {
		t.Fatalf("the restarting traversal read the ledger %d times, want 3: each pass, and the crossing", n)
	}
}
