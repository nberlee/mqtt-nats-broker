//go:build mqtt_fulltests

package mqtt

import (
	"sync"
	"testing"
	"time"
)

// The inbound QoS 2 hold walk's width follows who waits on it: a clean close's continuation, once
// it has released the client-ID gate, walks at mqttQoS2DiscardWalkWidthReleased; every teardown
// that keeps the gate to its end walks at mqttQoS2DiscardWalkWidth. Each test parks every hold's
// claim store, so the number in flight is exactly the walk's width: fewer and the walk is narrower
// than it should be, more and it was never bounded there.

// testWalkWidthPark parks the claim store of every hold in holds until released, counting how
// many are parked at once.
type testWalkWidthPark struct {
	mu      sync.Mutex
	parked  int
	reached chan struct{}
	want    int
	release chan struct{}
	once    sync.Once
}

func testParkHoldClaims(t *testing.T, asm *mqttAccountSessionManager, holds []string, want int) *testWalkWidthPark {
	t.Helper()
	claims := make(map[string]bool, len(holds))
	for _, h := range holds {
		claims[mqttQoS2ClaimSubjectFor(h)] = true
	}
	p := &testWalkWidthPark{reached: make(chan struct{}), want: want, release: make(chan struct{})}
	asm.mu.Lock()
	asm.testQoS2ClaimStore = func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
		if claims[subject] {
			p.mu.Lock()
			p.parked++
			if p.parked == p.want {
				close(p.reached)
			}
			p.mu.Unlock()
			<-p.release
		}
		return asm.jsa.storeQoS2RelClaimCAS(subject, expectSeq, c)
	}
	asm.mu.Unlock()
	t.Cleanup(p.unpark)
	return p
}

func (p *testWalkWidthPark) unpark() { p.once.Do(func() { close(p.release) }) }

// expectWidth waits for exactly want claim stores to be parked, then holds them for a quiet
// interval in which no further one may be admitted, then releases them.
func (p *testWalkWidthPark) expectWidth(t *testing.T, what string) {
	t.Helper()
	select {
	case <-p.reached:
	case <-time.After(testWalkWatchdog):
		p.mu.Lock()
		n := p.parked
		p.mu.Unlock()
		t.Fatalf("%s: %d hold arbitrations in flight, want %d: the walk is narrower than its width", what, n, p.want)
	}
	time.Sleep(testWalkQuiet)
	p.mu.Lock()
	n := p.parked
	p.mu.Unlock()
	if n != p.want {
		t.Fatalf("%s: %d hold arbitrations in flight while %d were parked, want exactly %d", what, n, p.want, p.want)
	}
	p.unpark()
}

func testAwaitHoldsGone(t *testing.T, asm *mqttAccountSessionManager, holds []string, what string) {
	t.Helper()
	deadline := time.Now().Add(testWalkWatchdog)
	for testHoldsPresent(t, asm, holds) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d of %d holds still present after the walk was released", what, testHoldsPresent(t, asm, holds), len(holds))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A clean close's continuation walks at the released width once the gate is open.
func TestCleanCloseContinuationWalksAtTheReleasedWidth(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "released-walk-width"

	pred := testConnectCleanPredecessor(t, s, o, id, mqttQoS2DiscardWalkWidthReleased+4, nil, nil)
	park := testParkHoldClaims(t, asm, pred.holds, mqttQoS2DiscardWalkWidthReleased)
	pred.c.Close()
	park.expectWidth(t, "the released continuation")
	testAwaitHoldsGone(t, asm, pred.holds, "the released continuation")
}

// Session expiry discards through the plain destroy: it closes a serving authority with a receipt
// but never releases a gate early, so its walk keeps the full width.
func TestExpiryTeardownWalksAtTheFullWidth(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "expiry-walk-width"

	pred := testConnectCleanPredecessor(t, s, o, id, mqttQoS2DiscardWalkWidth+1, nil, mqttV5ConnPropsSessionExpiry(1))
	park := testParkHoldClaims(t, asm, pred.holds, mqttQoS2DiscardWalkWidth)
	pred.c.Close()
	park.expectWidth(t, "the expiry teardown")
	testAwaitHoldsGone(t, asm, pred.holds, "the expiry teardown")
}
