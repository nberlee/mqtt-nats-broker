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
	"sync"
	"testing"
	"time"
)

// TestSubGateTerminalTransitionIsOneCAS pins the difference from the restore gate's shape.
//
// The restore gate stores `opened` and only then wins its `resolved` CAS, which is safe there
// because its two terminal transitions are exclusive by construction. This gate has more
// terminal paths, so the state and the close must move under ONE compare-and-swap: otherwise
// a losing opener can flip a verdict that was already withheld, and — worse — a second close
// panics the broker.
//
// M11's mutation is "replace the single terminal CAS with a plain store before the close".
// Both halves below kill it: the plain store lets Open overwrite Withheld, and it closes
// twice.
func TestSubGateTerminalTransitionIsOneCAS(t *testing.T) {
	t.Run("the first terminal state wins and the second is inert", func(t *testing.T) {
		g := &mqttSubGate{done: make(chan struct{})}
		g.resolve(mqttSubGateWithheld)
		// A late opener must NOT be able to turn a withheld verdict into permission
		// to transmit.
		g.resolve(mqttSubGateOpen)
		if got := mqttSubGateState(g.state.Load()); got != mqttSubGateWithheld {
			t.Fatalf("a losing terminal transition overwrote the verdict: got %v, want withheld", got)
		}
	})

	t.Run("resolving twice closes exactly once", func(t *testing.T) {
		g := &mqttSubGate{done: make(chan struct{})}
		g.resolve(mqttSubGateOpen)
		// A double close panics the process; the CAS is the only thing preventing it,
		// and the defer in mqttProcessSubs really can run after an explicit resolve.
		g.resolve(mqttSubGateOpen)
		select {
		case <-g.done:
		default:
			t.Fatal("the winning transition did not close the channel")
		}
	})

	t.Run("a nil gate resolves without panicking", func(t *testing.T) {
		// mqttProcessSubs installs the resolve defer only for a packet carrying a
		// shared filter, but the defer must be safe regardless.
		var g *mqttSubGate
		g.resolve(mqttSubGateOpen)
	})
}

// TestSubGateIsScopedToItsConnection pins D2: an abandoned verdict must not cross into
// another connection.
//
// Copying abandonRestoreGate's leave-installed rule would be unsound here. The restore gate
// can leave one installed because EVERY accepted CONNECT unconditionally replaces it; a
// packet gate is re-armed only by a later effectful shared SUBSCRIBE, so a withheld verdict
// left behind would block a resident session's traffic indefinitely — and a later CONNECT
// re-arms only the restore gate, so deliveries would pass the new verdict and then be
// blocked by the stale one.
//
// M6's mutation scopes the gate to the session instead of the connection.
func TestSubGateIsScopedToItsConnection(t *testing.T) {
	sess := &mqttSession{}
	// Distinct connection IDs: the gate's owner is a scalar c.cid, so two zero-valued
	// clients would BE the same connection as far as it is concerned.
	first := &client{}
	first.cid = 1
	second := &client{}
	second.cid = 2

	g := sess.armSubGate(first)
	g.resolve(mqttSubGateWithheld)

	if sess.subGateFor(first) == nil {
		t.Fatal("the arming connection must still see its own gate")
	}
	if got := sess.subGateFor(second); got != nil {
		t.Fatal("a withheld verdict from another connection is visible to this one: " +
			"a resident session would carry it into the next CONNECT and block traffic " +
			"the new connection was never refused")
	}
	if sess.subGateBlocksQoS0(second) {
		t.Fatal("another connection's verdict is dropping this connection's shared QoS 0")
	}
	if !sess.subGateBlocksQoS0(first) {
		t.Fatal("the arming connection's own withheld verdict must drop its shared QoS 0")
	}
}

// TestSubGateGenerationsAreMonotonic pins the second half of the scope.
//
// The connection alone is not enough: two packets on the SAME connection each get a gate,
// and a waiter that watched the first must not treat the second as already satisfied. The
// generation is what awaitTransmissionGates re-reads on every pass.
func TestSubGateGenerationsAreMonotonic(t *testing.T) {
	sess := &mqttSession{}
	c := &client{}
	c.cid = 7

	first := sess.armSubGate(c)
	second := sess.armSubGate(c)

	if second.gen <= first.gen {
		t.Fatalf("generations must advance per packet: first=%d second=%d", first.gen, second.gen)
	}
	if sess.subGateFor(c) != second {
		t.Fatal("arming must publish the newest gate")
	}
	// Resolving the FIRST instance must not resolve the second: a delayed defer from
	// generation N would otherwise open generation N+1's gate.
	first.resolve(mqttSubGateOpen)
	if got := mqttSubGateState(second.state.Load()); got != mqttSubGatePending {
		t.Fatalf("a stale defer resolved the current generation: got %v, want pending", got)
	}
}

// TestAwaitTransmissionGatesRequiresBoth proves the combined wait is a conjunction, not a
// choice, and that each gate's terminal verdict dominates.
func TestAwaitTransmissionGatesRequiresBoth(t *testing.T) {
	t.Run("both open", func(t *testing.T) {
		sess := &mqttSession{}
		c := &client{}
		sess.armRestoreGate()
		sess.openRestoreGate()
		sess.armSubGate(c).resolve(mqttSubGateOpen)
		if got := sess.awaitTransmissionGates(c); got != mqttTransmissionOpen {
			t.Fatalf("got %v, want open", got)
		}
	})

	t.Run("the operation gate withholds even with the restore gate open", func(t *testing.T) {
		sess := &mqttSession{}
		c := &client{}
		sess.armRestoreGate()
		sess.openRestoreGate()
		sess.armSubGate(c).resolve(mqttSubGateWithheld)
		// This is the whole point of F-078: on a LIVE connection the restore gate is
		// already open, so a delivery that consulted only it would transmit under
		// interest whose ownership was never proved.
		if got := sess.awaitTransmissionGates(c); got != mqttTransmissionWithheld {
			t.Fatalf("got %v, want withheld", got)
		}
	})

	t.Run("the restore gate withholds even with no operation gate", func(t *testing.T) {
		sess := &mqttSession{}
		c := &client{}
		sess.armRestoreGate()
		sess.abandonRestoreGate()
		if got := sess.awaitTransmissionGates(c); got != mqttTransmissionWithheld {
			t.Fatalf("got %v, want withheld", got)
		}
	})

	t.Run("no gate at all is open", func(t *testing.T) {
		sess := &mqttSession{}
		if got := sess.awaitTransmissionGates(&client{}); got != mqttTransmissionOpen {
			t.Fatalf("got %v, want open", got)
		}
	})
}

// TestAwaitTransmissionGatesMeetsALaterGeneration is D4's old-pointer case, and it is the
// half that IS closed.
//
// A callback captures generation N, waits, and N opens. Before it can act, the readLoop arms
// generation N+1. A wait that loaded one pointer and never re-read it would return Open here
// and transmit under interest the new packet has not yet proved. The wait must observe the
// CURRENT generation.
//
// WHY THIS NEEDS A SEAM, and why the obvious version of this test was FLAKY rather than
// wrong. Publishing N+1 after resolving N races the waiter: a CORRECT waiter may wake, reload
// while N is still current and open, and return Open — which is not a defect, because the
// guarantee is about a gate published before the waiter's final READ, not before its send.
// Publishing N+1 first has the opposite problem: a single-load waiter reads N+1 straight away
// and passes. Only pausing the waiter between its wake and its reload makes the property
// measurable, and the earlier form of this test failed about one run in eight under -count.
//
// The other half — a callback descheduled AFTER its final check and before its send — is the
// documented residual in docs/CONFORMANCE.md and is deliberately not asserted as blocked.
func TestAwaitTransmissionGatesMeetsALaterGeneration(t *testing.T) {
	sess := &mqttSession{}
	c := &client{}

	g1 := sess.armSubGate(c)

	// The waiter announces that it has read the gates and is about to block, then holds
	// there. One-shot: a second entry would deadlock on a channel nobody closes again.
	woke := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer setMQTTTestGateAboutToWait(func(s *mqttSession) {
		if s != sess {
			return
		}
		var entered bool
		once.Do(func() { entered = true })
		if !entered {
			return
		}
		close(woke)
		<-release
	})()

	got := make(chan mqttTransmissionVerdict, 1)
	go func() { got <- sess.awaitTransmissionGates(c) }()

	// Wait until the waiter is held, so everything below happens while it cannot observe
	// anything. It has already captured N at this point, which is what makes the reload the
	// only way to reach the right answer.
	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter never reached its blocking point, so nothing below is " +
			"measuring the reload")
	}

	// N opens and N+1 is published, both entirely inside the paused window. On release the
	// wait returns from an already-closed channel and re-reads: a waiter that reloads sees
	// N+1 withheld, one that captured N sees its own gate open.
	g1.resolve(mqttSubGateOpen)
	sess.armSubGate(c).resolve(mqttSubGateWithheld)
	close(release)

	select {
	case v := <-got:
		if v != mqttTransmissionWithheld {
			t.Fatalf("the wait returned %v while a later generation was withheld: it "+
				"captured one gate pointer and never re-read the current one (D4)", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait neither returned nor observed the later generation")
	}
}
