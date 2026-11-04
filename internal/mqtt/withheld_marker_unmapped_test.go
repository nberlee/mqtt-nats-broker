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

// A withheld marker (sess.deferred) names an UNMAPPED source (issue #162). A sequence cpending
// maps already has an exchange, which owns its re-offer; a marker beside it was NAK'd as
// withheld work, spending the prompt-release slot a genuinely withheld delivery was owed.

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

func testWithheldMarkerEnv(t *testing.T) (*testInflightEnv, string) {
	t.Helper()
	e := testNewInflightEnv(t, "client-withheld-"+nuid.Next())
	return e, e.sess.idHash + "_" + nuid.Next()
}

// testReferenceDurableLocked makes dur a durable the session's record references — an
// ordinary consumer with its membership — so the marker readers act on its markers
// (recordReferencedDurablesLocked). sess.mu held.
func testReferenceDurableLocked(sess *mqttSession, dur string) {
	filter := "withheld." + dur
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	sess.cons[filter] = &ConsumerConfig{Durable: dur}
	sess.subs[filter] = 1
}

func testHasWithheldMarker(sess *mqttSession, dur string, sseq uint64) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	_, ok := sess.deferred[mqttDeferredKey{dur, sseq}]
	return ok
}

// The gate's writer refuses a re-offer of a sequence that already has an exchange, and still
// records one that has none.
func TestWithheldMarkerIsNeverRecordedBesideAMappedExchange(t *testing.T) {
	e, dur := testWithheldMarkerEnv(t)
	e.track(t, dur, 5, 1)
	sub := &subscription{client: e.c, mqtt: &mqttSub{qos: 1, jsDur: dur}}

	var decided []bool
	h := func(d string, sseq uint64, recorded bool) {
		if d == dur && sseq == 5 {
			decided = append(decided, recorded)
		}
	}
	mqttTestWithheldDecidedV.Store(&h)
	t.Cleanup(func() { mqttTestWithheldDecidedV.Store(nil) })

	e.sess.mu.Lock()
	if pi, _ := e.sess.cpending.lookup(dur, 5); pi == 0 {
		e.sess.mu.Unlock()
		t.Fatal("the tracked exchange is not mapped, so this fixture proves nothing")
	}
	e.sess.recordWithheldDeliveryLocked(e.c, sub, testInflightAckSubject(dur, 5, 2))
	e.sess.recordWithheldDeliveryLocked(e.c, sub, testInflightAckSubject(dur, 6, 1))
	e.sess.mu.Unlock()

	if len(decided) != 1 || decided[0] {
		t.Fatalf("the mapped re-offer's decision = %v, want exactly one refusal", decided)
	}
	if testHasWithheldMarker(e.sess, dur, 5) {
		t.Fatal("a withheld marker was recorded beside a mapped exchange")
	}
	if !testHasWithheldMarker(e.sess, dur, 6) {
		t.Fatal("an unmapped withheld offer recorded no marker")
	}
}

// A marker recorded while its sequence was unmapped is consumed by the mapping that gives the
// sequence an exchange (recovery's reservation, the PUBREC rollback), and no other is touched.
func TestMappingASourceConsumesItsWithheldMarker(t *testing.T) {
	e, dur := testWithheldMarkerEnv(t)
	e.sess.mu.Lock()
	e.sess.recordWithheldAttemptLocked(dur, 5, testInflightAckSubject(dur, 5, 1), mqttOfferOrigin{})
	e.sess.recordWithheldAttemptLocked(dur, 6, testInflightAckSubject(dur, 6, 1), mqttOfferOrigin{})
	e.sess.mapSourceLocked(dur, 5, 77)
	e.sess.mu.Unlock()
	if testHasWithheldMarker(e.sess, dur, 5) {
		t.Fatal("the mapping left its sequence's withheld marker standing")
	}
	if !testHasWithheldMarker(e.sess, dur, 6) {
		t.Fatal("the mapping consumed another sequence's marker")
	}
}

// The consequence: a slot freed at the cap NAKs the delivery that was withheld for want of it,
// not an exchange whose re-offer a gate happened to withhold. Before issue #162 the exchange's
// marker (the lower sequence) took the slot and the withheld delivery waited out AckWait.
func TestAFreedSlotNAKsTheWithheldNewDeliveryNotAStartedExchange(t *testing.T) {
	e, dur := testWithheldMarkerEnv(t)
	e.track(t, dur, 5, 1)
	sub := &subscription{client: e.c, mqtt: &mqttSub{qos: 1, jsDur: dur}}
	held := testInflightAckSubject(dur, 6, 1)

	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	testReferenceDurableLocked(e.sess, dur)
	e.sess.maxp = 1
	e.sess.recordWithheldDeliveryLocked(e.c, sub, testInflightAckSubject(dur, 5, 2))
	if pi, _ := e.sess.trackPublish(dur, held, mqttOfferOrigin{}); pi != 0 {
		t.Fatalf("sequence 6 was allocated (pi %d) at a full in-flight cap", pi)
	}
	if _, ok := e.sess.deferred[mqttDeferredKey{dur, 6}]; !ok {
		t.Fatal("the cap withheld sequence 6 without recording it, so this fixture proves nothing")
	}
	e.sess.maxp = 2 // one slot frees
	got := e.sess.deferredNaksLocked()
	if len(got) != 1 || got[0] != held {
		t.Fatalf("the freed slot NAK'd %q, want only the withheld delivery %q", got, held)
	}
}

// Cancelling an in-flight registration unmaps its sequence AND leaves the marker a rolled-back
// staging needs to NAK it forward: the unmapping comes first, or the writer refuses it.
func TestACancelledRegistrationKeepsItsWithheldMarker(t *testing.T) {
	e, dur := testWithheldMarkerEnv(t)
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5, 1), mqttOfferOrigin{})
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	if out := e.sess.cancelPendingRegistrationsLocked(dur, mqttLicenceOf(e.c)); len(out) != 1 {
		t.Fatalf("cancelled %d registrations, want 1, so this fixture proves nothing", len(out))
	}
	if p, _ := e.sess.cpending.lookup(dur, 5); p != 0 {
		t.Fatal("the cancelled registration's sequence is still mapped")
	}
	if _, ok := e.sess.deferred[mqttDeferredKey{dur, 5}]; !ok {
		t.Fatal("the cancelled registration left no withheld marker")
	}
}

// Allocating the last withheld sequence retires the latched custody demand: the marker is
// consumed BEFORE the allocator reports success, which is where the latch is tested.
func TestAllocatingTheLastWithheldSequenceRetiresTheDemand(t *testing.T) {
	e, dur := testWithheldMarkerEnv(t)
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	e.sess.maxp = 0
	reply := testInflightAckSubject(dur, 5, 1)
	if pi, _ := e.sess.trackPublish(dur, reply, mqttOfferOrigin{}); pi != 0 {
		t.Fatalf("allocated pi %d at a zero cap", pi)
	}
	if len(e.sess.deferred) != 1 {
		t.Fatalf("%d markers after the withholding, want 1", len(e.sess.deferred))
	}
	e.sess.custodyDemand = true
	e.sess.maxp = 100
	if pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5, 2), mqttOfferOrigin{}); pi == 0 || !isNew {
		t.Fatalf("the re-offer was not allocated: pi=%d isNew=%v", pi, isNew)
	}
	if e.sess.custodyDemand {
		t.Fatal("allocating the last withheld sequence left the custody demand latched")
	}
}

// ONE writer of sess.deferred and ONE writer of cpending, so the rule that a marker names an
// unmapped source is enforced where both maps change rather than restated at every site.
func TestWithheldMarkersAndSourceMappingsHaveOneWriterEach(t *testing.T) {
	t.Parallel()
	want := map[string]string{
		"deferred": "mqttSession.recordWithheldAttemptLocked",
		"cpending": "mqttSession.mapSourceLocked",
	}
	found := map[string]map[string]int{"deferred": {}, "cpending": {}}
	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, _ := decl.(*ast.FuncDecl)
			if fn == nil || fn.Body == nil {
				continue
			}
			name := testQualifiedFuncName(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if ix, ok := lhs.(*ast.IndexExpr); ok {
							if sel, ok := ix.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "deferred" {
								found["deferred"][name]++
							}
						}
					}
				case *ast.CallExpr:
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "put" {
						if recv, ok := sel.X.(*ast.SelectorExpr); ok && recv.Sel.Name == "cpending" {
							found["cpending"][name]++
						}
					}
				}
				return true
			})
		}
	}
	for m, owner := range want {
		if found[m][owner] == 0 {
			t.Errorf("%s: the one writer %s writes nothing, so this check guards nothing", m, owner)
		}
		for fn, n := range found[m] {
			if fn != owner {
				t.Errorf("%s is written directly by %s (%d sites); route it through %s", m, fn, n, owner)
			}
		}
	}
}

// The resume pass snapshots the deferred set before releasing the lock for its stream lookups,
// and a delivery callback can allocate a marked sequence in that gap. The NAKs it sends come from
// the markers still standing at its final lock: the sequence that now has an exchange is not
// NAK'd from the deferred allowance, and the one still withheld is.
func TestResumeNAKsOnlyTheWithheldSequencesStillUnmapped(t *testing.T) {
	// NOT parallel: installs a process-global restore hook.
	e, dur := testWithheldMarkerEnv(t)
	allocated, stillHeld := testInflightAckSubject(dur, 5, 1), testInflightAckSubject(dur, 6, 1)
	e.sess.mu.Lock()
	testReferenceDurableLocked(e.sess, dur)
	e.sess.maxp = 2
	e.sess.recordWithheldAttemptLocked(dur, 5, allocated, mqttOfferOrigin{})
	e.sess.recordWithheldAttemptLocked(dur, 6, stillHeld, mqttOfferOrigin{})
	e.sess.mu.Unlock()

	acks, err := e.s.nrc.SubscribeSync("$JS.ACK." + mqttStreamName + "." + dur + ".>")
	if err != nil {
		t.Fatalf("subscribing to the durable's ack subjects: %v", err)
	}
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var staged bool
	restore := setMQTTTestRestorePause(func(at string) {
		if at != "reap-scanned" || staged {
			return
		}
		staged = true
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		if pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 5, 2), mqttOfferOrigin{}); pi == 0 || !isNew {
			t.Errorf("fixture: sequence 5 was not allocated in the gap: pi=%d isNew=%v", pi, isNew)
		}
	})
	defer restore()

	e.sess.resendPendingRedelivery(e.c, nil, nil)
	if !staged {
		t.Fatal("fixture: the reap-scanned boundary was never reached, so nothing was interleaved")
	}

	var got []string
	for {
		m, err := acks.NextMsg(2 * time.Second)
		if err != nil {
			break
		}
		got = append(got, m.Subject)
	}
	if len(got) != 1 || got[0] != stillHeld {
		t.Fatalf("the resume NAK'd %q, want only the still-withheld %q", got, stillHeld)
	}
}
