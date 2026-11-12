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
	"sync/atomic"
	"testing"
	"time"
)

// An owed anchor removal is RETAINED (plan 5.3): a refused CONNECT, a discarded or immediate
// Will, or an enqueue that published instead owes its anchor's removal, and a removal that
// fails is retried until it lands, rather than given up after one bounded attempt. A removal
// given up leaves an ARMED or OWED anchor that pins its lifecycle with nothing to complete it.
func TestAnOwedAnchorRemovalIsRetriedUntilItLands(t *testing.T) {
	_, asm, _ := testDelayedRetryEnv(t)
	const clientID = "removal-retried"
	ref := testAcceptWithWillAnchor(t, asm, clientID)

	const failures = 3
	var calls atomic.Int64
	asm.mu.Lock()
	asm.testAnchorTransitionErr = func(_ string, op mqttAnchorOp) error {
		if op == mqttAnchorRemove && calls.Add(1) <= failures {
			return errors.New("injected anchor-transition failure")
		}
		return nil
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testAnchorTransitionErr = nil
		asm.mu.Unlock()
	})

	asm.dropWillAnchor(clientID, ref)
	for deadline := time.Now().Add(10 * time.Second); len(testWillAnchorsOf(t, asm, clientID)) != 0; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the anchor still stands after %d removal attempts: an owed removal was given up", calls.Load())
		}
	}
	if n := calls.Load(); n <= failures {
		t.Fatalf("the anchor was removed after %d attempts, within the %d injected failures", n, failures)
	}
}
