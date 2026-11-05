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

//go:build mqtt_fulltests

package mqtt

// Session-resident state names a connection or a subscription by identity and never keeps
// it (docs/ARCHITECTURE.md, "Resource lifetime and shutdown release"): the drain-at-detach
// decisions must come out the same after the named objects are gone, and finished retained
// work must leave nothing behind on the driver. See
// https://github.com/nberlee/mqtt-nats-broker/issues/174.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"runtime"
	"testing"
	"time"
	"weak"
)

// awaitWeakCollected requires every pointer to become unreachable within a bound. The bound is
// generous because work a closing connection still owes (a QoS 2 teardown, say) legitimately
// holds it until that work ends; a retainer holds it forever.
func awaitWeakCollected(t *testing.T, what string, alive func() int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		runtime.GC()
		runtime.GC()
		n := alive()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d %s still reachable", n, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// An origin decides the same after its connection and subscription are collected: the drain
// state it reads is the subscription's own cell, shared rather than copied, so an origin taken
// before the drain was issued sees the issue.
func TestLateDecisionAfterOriginCollected(t *testing.T) {
	t.Run("issued before capture", func(t *testing.T) {
		u := newTestDrainUnitEnv(t)
		origin := mqttOriginOf(u.predSub, u.pred)
		cw, sw := weak.Make(u.pred), weak.Make(u.predSub)
		u.pred, u.predSub = nil, nil
		awaitWeakCollected(t, "origin objects", func() int {
			n := 0
			if cw.Value() != nil {
				n++
			}
			if sw.Value() != nil {
				n++
			}
			return n
		})
		if got := u.decide(7, origin); got != mqttWithheldNAKOwed {
			t.Fatalf("decision after the origin was collected = %v, want NAK-owed", got)
		}
	})
	t.Run("issued after capture", func(t *testing.T) {
		u := newTestDrainUnitEnv(t)
		cell := u.predSub.mqtt.drain
		u.sess.mu.Lock()
		cell.claimed, cell.issued = true, false
		u.sess.mu.Unlock()
		origin := mqttOriginOf(u.predSub, u.pred)
		u.sess.mu.Lock()
		cell.issued = true
		u.sess.mu.Unlock()
		if got := u.decide(7, origin); got != mqttWithheldNAKOwed {
			t.Fatalf("an origin captured before the drain was issued decided %v, want NAK-owed", got)
		}
	})
	t.Run("subscription without MQTT metadata", func(t *testing.T) {
		u := newTestDrainUnitEnv(t)
		bare := &subscription{client: u.pred}
		if got := u.decide(7, mqttOriginOf(bare, u.pred)); got != mqttWithheldRecorded {
			t.Fatalf("a subscription with no drain cell decided %v, want recorded", got)
		}
		u.sess.mu.Lock()
		_, tagged := u.sess.deferredLate[mqttDeferredKey{u.dur, 7}]
		u.sess.mu.Unlock()
		if !tagged {
			t.Fatal("a late offer from a subscription with no drain cell was not tagged")
		}
	})
}

// A late tag names its origin subscription and the cutoff's connection without keeping them:
// with the tag still standing, both are collected. The tag itself stays until the next cutoff
// or the incarnation's reset, as an unread tag always did.
func TestLateMarkerKeepsNoConnection(t *testing.T) {
	u := newTestDrainUnitEnv(t)
	// A connection of the test's own as the bound one whose resume pass set the cutoff, so
	// nothing but the session could keep it.
	cutoffConn := &client{}
	u.sess.mu.Lock()
	u.sess.c = cutoffConn
	u.sess.setResumeCutoffLocked(cutoffConn)
	u.sess.mu.Unlock()
	notDraining := &subscription{client: u.pred, mqtt: &mqttSub{qos: 1, jsDur: u.dur, drain: &mqttDetachDrain{}}}
	if got := u.decide(7, mqttOriginOf(notDraining, u.pred)); got != mqttWithheldRecorded {
		t.Fatalf("fixture: a late offer from a not-yet-draining origin decided %v", got)
	}
	cw, sw, bound := weak.Make(u.pred), weak.Make(notDraining), weak.Make(cutoffConn)
	u.pred, u.predSub = nil, nil
	// Detach the cutoff's connection, so only the tag and the cutoff could keep it.
	u.sess.mu.Lock()
	u.sess.c = nil
	u.sess.mu.Unlock()
	awaitWeakCollected(t, "late-tag objects", func() int {
		n := 0
		for _, p := range []bool{cw.Value() != nil, sw.Value() != nil, bound.Value() != nil} {
			if p {
				n++
			}
		}
		return n
	})
	u.sess.mu.Lock()
	_, tagged := u.sess.deferredLate[mqttDeferredKey{u.dur, 7}]
	u.sess.mu.Unlock()
	if !tagged {
		t.Fatal("the late tag is gone: the collection check proved nothing")
	}
}

// The retained driver's finished work leaves no pointer in a slice's backing array, and an idle
// replay worker forgets its connection.
func TestRetainedDriverReleasesVacatedSlots(t *testing.T) {
	t.Run("replay queue", func(t *testing.T) {
		d := &mqttRetainedDriver{conn: &client{}}
		d.queue = []*mqttRetainedReplay{{}, {}, {}}
		backing := d.queue
		if _, _, ok := d.popReplayLocked(); !ok {
			t.Fatal("pop found nothing")
		}
		if backing[0] != nil {
			t.Fatal("the popped replay object is still in the queue's backing array")
		}
		d.popReplayLocked()
		d.popReplayLocked()
		if _, _, ok := d.popReplayLocked(); ok {
			t.Fatal("pop of an empty queue found work")
		}
		if d.conn != nil || d.queue != nil {
			t.Fatal("the idle replay worker kept its connection or its queue")
		}
	})
	t.Run("abandonments", func(t *testing.T) {
		d := &mqttRetainedDriver{}
		a, b, c := &mqttRetainedAbandonment{}, &mqttRetainedAbandonment{}, &mqttRetainedAbandonment{}
		d.abandons = []*mqttRetainedAbandonment{a, b, c}
		backing := d.abandons
		d.removeAbandonLocked(b)
		if len(d.abandons) != 2 || d.abandons[0] != a || d.abandons[1] != c || backing[2] != nil {
			t.Fatalf("removal left %v, vacated slot %v", d.abandons, backing[2])
		}
		d.removeAbandonLocked(a)
		d.removeAbandonLocked(c)
		if d.abandons != nil {
			t.Fatal("an emptied abandonment list kept its backing array")
		}
	})
	t.Run("owed transmissions", func(t *testing.T) {
		d := &mqttRetainedDriver{}
		a, b, c := &mqttRetainedOwed{}, &mqttRetainedOwed{}, &mqttRetainedOwed{}
		d.owed = []*mqttRetainedOwed{a, b, c}
		backing := d.owed
		d.removeOwedLocked(b)
		if len(d.owed) != 2 || d.owed[0] != a || d.owed[1] != c || backing[2] != nil {
			t.Fatalf("removal left %v, vacated slot %v", d.owed, backing[2])
		}
		d.removeOwedLocked(a)
		d.removeOwedLocked(c)
		if d.owed != nil {
			t.Fatal("an emptied owed list kept its backing array")
		}
	})
}

// The types that outlive a connection on a resident session hold no *client or *subscription,
// at any depth of their field types, except the session's bound connection itself. A field that
// only identifies one is a weak.Pointer; state that must outlive a subscription is a value that
// does not point back to it (mqttDetachDrain).
func TestSessionStateHoldsNoConnectionReference(t *testing.T) {
	t.Parallel()
	guarded := map[string]bool{
		"mqttSession": true, "mqttPending": true, "mqttOfferOrigin": true,
		"mqttAllocationFence": true, "mqttLateMarker": true, "mqttDetachDrain": true,
	}
	allowed := map[string]bool{"mqttSession.c": true}
	fset := token.NewFileSet()
	found := map[string]bool{}
	for _, name := range censusProductionFiles(t) {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || !guarded[ts.Name.Name] {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			found[ts.Name.Name] = true
			for _, field := range st.Fields.List {
				holds := false
				ast.Inspect(field.Type, func(n ast.Node) bool {
					if se, ok := n.(*ast.StarExpr); ok {
						if id, ok := se.X.(*ast.Ident); ok && (id.Name == "client" || id.Name == "subscription") {
							holds = true
						}
					}
					return true
				})
				if !holds {
					continue
				}
				for _, nm := range field.Names {
					if !allowed[ts.Name.Name+"."+nm.Name] {
						t.Errorf("%s: %s.%s keeps a connection or subscription; name it with a weak.Pointer",
							fset.Position(nm.Pos()), ts.Name.Name, nm.Name)
					}
				}
			}
			return false
		})
	}
	for name := range guarded {
		if !found[name] {
			t.Errorf("type %s not found: the guard is not reading the code it guards", name)
		}
	}
}
