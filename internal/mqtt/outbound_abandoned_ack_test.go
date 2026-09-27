// Copyright 2026 The mqtt-nats-bridge Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testUnitCAbandonedBinding(t *testing.T, f *testAbandonFix) *mqttAbandonedAckBinding {
	t.Helper()
	f.e.sess.mu.Lock()
	defer f.e.sess.mu.Unlock()
	b := f.e.sess.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}]
	if b == nil {
		t.Fatal("no ACK-only binding after abandonment")
	}
	return b
}

// M-C4: installation after removal loses the observable handoff invariant.
// M-C12: nonce/lineage/PI/QoS/acquisition mismatch must fail before a downstream CAS.
func TestUnitCAbandonedAckBindingHandoff(t *testing.T) {
	f := testAbandonAdmitted(t, "c2-binding", 1)
	sess := f.e.sess
	seen := false
	sess.mu.Lock()
	sess.testAbandonedAckInstalled = func() {
		seen = true
		if sess.pendingPublish[f.pi] == nil ||
			sess.cpending[testRealPubRelSrcDur][f.sseq] != f.pi ||
			sess.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}] == nil {
			t.Error("binding must be installed while both scheduling links still stand")
		}
	}
	sess.mu.Unlock()
	if f.deny() != mqttAbandonDone || !seen {
		t.Fatal("abandonment did not reach the install-before-removal boundary")
	}
	b := testUnitCAbandonedBinding(t, f)
	rec, _ := f.register(t)
	for _, change := range []func(*mqttInflightRecord){
		func(r *mqttInflightRecord) { r.Nonce = mqttNewInflightNonce() },
		func(r *mqttInflightRecord) { r.Lin = mqttNewInflightNonce() },
		func(r *mqttInflightRecord) { r.QoS = 2 },
		func(r *mqttInflightRecord) { r.PI++ },
		func(r *mqttInflightRecord) { r.Seq++ },
	} {
		other := *rec
		change(&other)
		if b.matchesRecord(&other) {
			t.Fatalf("accepted mismatched allocation: %+v", other)
		}
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.pendingPublish[f.pi] != nil || sess.cpending[b.dur][b.seq] != 0 {
		t.Fatal("abandonment retained actionable scheduling links")
	}
	for _, change := range []func(*mqttAbandonedAckBinding){
		func(v *mqttAbandonedAckBinding) { v.nonce = mqttNewInflightNonce() },
		func(v *mqttAbandonedAckBinding) { v.lin = mqttNewInflightNonce() },
		func(v *mqttAbandonedAckBinding) { v.qos = 2 },
		func(v *mqttAbandonedAckBinding) { v.name.pi++ },
		func(v *mqttAbandonedAckBinding) { v.acq++ },
		func(v *mqttAbandonedAckBinding) { v.epoch += "-other" },
		func(v *mqttAbandonedAckBinding) { v.sess = &mqttSession{} },
		func(v *mqttAbandonedAckBinding) { v.name.gen += "-other" },
		func(v *mqttAbandonedAckBinding) { v.name.subject += ".other" },
		func(v *mqttAbandonedAckBinding) { v.dur += "-other" },
		func(v *mqttAbandonedAckBinding) { v.seq++ },
	} {
		other := *b
		change(&other)
		if sess.removeAbandonedAckLocked(&other) {
			t.Fatal("a different identity removed the binding")
		}
	}
	before := sess.receiveMaxOccupancyLocked()
	sess.retrackPublishLocked(f.pi, b.pending())
	if sess.pendingPublish[f.pi] != nil || sess.cpending[b.dur][b.seq] != 0 ||
		sess.receiveMaxOccupancyLocked() != before {
		t.Fatal("pre-intent rollback restored PUBLISH or added a charge")
	}
	if !sess.removeAbandonedAckLocked(b) || sess.removeAbandonedAckLocked(b) {
		t.Fatal("exact identity removal was not a single compare-delete")
	}
}

// M-C4 reaches the real handler with no PUBLISH entry; M-C6/M-C7 require the actual
// abandoned-origin atomic arrow. These are substrate checks, not full T-C3..T-C8.
func TestUnitCAbandonedAckHandler(t *testing.T) {
	for _, tc := range []struct {
		name   string
		qos    byte
		pubrec bool
		reason byte
		phase  mqttExchangePhase
	}{
		{"puback", 1, false, mqttReasonSuccess, mqttExchangeTerminal},
		{"failed-pubrec", 2, true, 0x80, mqttExchangeRejected},
		{"pubrec", 2, true, mqttReasonSuccess, mqttExchangePubRecObserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testAbandonAdmitted(t, "c2-handler-"+tc.name, tc.qos)
			if f.deny() != mqttAbandonDone {
				t.Fatal("deny did not land")
			}
			b := testUnitCAbandonedBinding(t, f)
			var advanced atomic.Int32
			f.e.asm.mu.Lock()
			f.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest,
				write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				if req.To == tc.phase && req.Register.State == mqttInflightDeliveryAbandoned &&
					b.matchesRecord(req.Register) {
					advanced.Add(1)
				}
				return write()
			}
			f.e.asm.testInflightSourceAck = func(reply string) error {
				if !mqttSourceAckReplyBinds(reply, mqttStreamName, b.dur, b.seq) {
					t.Error("terminal settlement used a foreign reply")
				}
				return errors.New("source confirmation unavailable")
			}
			f.e.asm.mu.Unlock()
			if err := f.e.c.mqttProcessPublishReceived(f.pi, tc.pubrec, tc.reason); err != nil {
				t.Fatal(err)
			}
			if advanced.Load() == 0 || f.exchange(t).Phase != tc.phase {
				t.Fatal("bound packet did not advance the exact abandoned exchange")
			}
			if tc.phase != mqttExchangePubRecObserved {
				testCustodyEventually(t, "exact ended binding removed", func() bool {
					f.e.sess.mu.Lock()
					defer f.e.sess.mu.Unlock()
					return f.e.sess.abandonedAcks[b.index()] == nil
				})
			}
			if b.pending().jsAckSubject != _EMPTY_ {
				t.Fatal("protocol carrier exposed a source ACK handle")
			}
		})
	}
}

// M-C12 is measured at the binding decision and at handler entry, without a
// downstream transition guard masking acceptance of the wrong allocation.
func TestUnitCAbandonedAckRefusesReuse(t *testing.T) {
	f := testAbandonAdmitted(t, "c2-reuse", 1)
	if f.deny() != mqttAbandonDone {
		t.Fatal("deny did not land")
	}
	b := testUnitCAbandonedBinding(t, f)
	if err := f.e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	if f.exchange(t).Phase != mqttExchangeDeliveryAbandoned {
		t.Fatal("QoS-mismatched ACK advanced the exchange")
	}
	f.e.sess.mu.Lock()
	occupant := &mqttPending{regNonce: mqttNewInflightNonce(), lin: mqttNewInflightNonce(), regForeign: true}
	f.e.sess.pendingPublish[f.pi] = occupant
	got, _ := f.e.sess.resolveAbandonedAckLocked(f.pi, mqttAckArrival{Packet: mqttPacketPubAck})
	f.e.sess.mu.Unlock()
	if got != nil {
		t.Fatal("ACK-only index overrode a reused PI")
	}
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatal(err)
	}
	if f.exchange(t).Phase != mqttExchangeDeliveryAbandoned {
		t.Fatal("reuse advanced the abandoned exchange")
	}
	f.e.sess.mu.Lock()
	if f.e.sess.pendingPublish[f.pi] != occupant {
		t.Error("handler removed the reused occupant")
	}
	delete(f.e.sess.pendingPublish, f.pi)
	f.e.sess.mu.Unlock()
	rec, seq := f.register(t)
	var advances atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testAckAdvance = func(*mqttAdvanceExchangeAckRequest, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		advances.Add(1)
		return mqttOpOutcome{}, errors.New("unexpected advance")
	}
	f.e.asm.mu.Unlock()
	for _, change := range []func(*mqttInflightRecord){
		func(r *mqttInflightRecord) { r.Nonce = mqttNewInflightNonce() },
		func(r *mqttInflightRecord) { r.Lin = mqttNewInflightNonce() },
		func(r *mqttInflightRecord) { r.QoS = 2 },
		func(r *mqttInflightRecord) { r.PI++ },
	} {
		other := *rec
		change(&other)
		if b.matchesRecord(&other) {
			t.Fatal("binding accepted a forged occupant")
		}
		f.e.asm.mu.Lock()
		f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
			return &other, &StoredMsg{Sequence: seq}, nil
		}
		f.e.asm.mu.Unlock()
		if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
			t.Fatal(err)
		}
		if advances.Load() != 0 {
			t.Fatal("bound handler attempted to advance a mismatched occupant")
		}
	}
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = nil
	f.e.asm.mu.Unlock()
}

// M-C6: terminal release must enter the acknowledged-terminal arrow from abandoned.
// M-C7: direct intent must enter the successful-PUBREC arrow from abandoned.
func TestUnitCAbandonedAckDirectArrows(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		f := testAbandonAdmitted(t, "c2-direct-"+string(rune('0'+qos)), qos)
		if f.deny() != mqttAbandonDone {
			t.Fatal("deny did not land")
		}
		ack := testUnitCAbandonedBinding(t, f).pending()
		if qos == 1 {
			if !f.e.sess.attemptTerminalRegisterRelease(f.e.c, f.pi, ack.slot, true) ||
				f.exchange(t).Phase != mqttExchangeTerminal {
				t.Fatal("terminal release treated abandonment as no work")
			}
		} else if f.e.sess.intendPubRelSlot(f.e.c, ack) != mqttPubRelIntentConfirmed ||
			f.exchange(t).Phase != mqttExchangePubRecObserved {
			t.Fatal("abandoned origin did not record intent")
		}
	}
}

// M-C8 retries a positively unanswered abandoned origin. M-C44 retains the exact
// resolver on refusal; M-C46 retains it after a failed shutdown-final attempt.
func TestUnitCIntentRefusalAndFinalSettlement(t *testing.T) {
	f := testAbandonAdmitted(t, "c2-owed", 2)
	if f.deny() != mqttAbandonDone {
		t.Fatal("deny did not land")
	}
	ack := testUnitCAbandonedBinding(t, f).pending()
	f.e.s.closeWillDispatch() // Inject admission refusal without stopping JetStream.
	var failing atomic.Bool
	var attempts atomic.Int32
	failing.Store(true)
	f.e.asm.mu.Lock()
	f.e.asm.testPubRecAdvanceWait = func(time.Duration) {}
	f.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest,
		write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		attempts.Add(1)
		if failing.Load() {
			return mqttOpOutcome{}, errors.New("positively pre-intent failure")
		}
		return write()
	}
	f.e.asm.mu.Unlock()
	f.e.sess.scheduleIntentResolution(nil, f.pi, ack)
	f.e.sess.scheduleIntentResolution(nil, f.pi, ack)
	f.e.s.owedIntentsMu.Lock()
	if len(f.e.s.owedIntents) != 1 {
		f.e.s.owedIntentsMu.Unlock()
		t.Fatal("admission refusal lost or duplicated the resolver")
	}
	var owner *mqttIntentResolution
	for _, r := range f.e.s.owedIntents {
		owner = r
	}
	f.e.s.owedIntentsMu.Unlock()
	f.e.s.finishOwedIntents()
	f.e.s.owedIntentsMu.Lock()
	retained := f.e.s.owedIntents[owner.key] == owner && !owner.running
	f.e.s.owedIntentsMu.Unlock()
	if !retained || attempts.Load() == 0 || f.exchange(t).Phase != mqttExchangeDeliveryAbandoned {
		t.Fatal("failed final attempt lost its exact owner or failed to retry the abandoned origin")
	}
	// Unreadability is not a terminal classification either.
	base := testRealBaseLoad(f.e)
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, errors.New("unreadable")
	}
	f.e.asm.mu.Unlock()
	f.e.s.finishOwedIntents()
	f.e.s.owedIntentsMu.Lock()
	retained = f.e.s.owedIntents[owner.key] == owner
	f.e.s.owedIntentsMu.Unlock()
	if !retained {
		t.Fatal("unreadable final attempt detached its owner")
	}
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = base
	f.e.asm.mu.Unlock()
	failing.Store(false)
	f.e.s.finishOwedIntents()
	if f.exchange(t).Phase != mqttExchangePubRecObserved {
		t.Fatal("shutdown did not durably retain successful PUBREC without a second ACK")
	}
	rec, _ := f.register(t)
	if rec == nil || rec.State != mqttInflightPubRelReady {
		t.Fatal("shutdown did not hand durable intent to canonical materialization")
	}
	f.e.s.owedIntentsMu.Lock()
	_, retained = f.e.s.owedIntents[owner.key]
	f.e.s.owedIntentsMu.Unlock()
	if retained {
		t.Fatal("completed materialization did not remove the exact resolver")
	}
}

// M-C45: test the shutdown wiring separately from the callable final-pass contract.
// The final pass must precede the close of account infrastructure.
func TestUnitCShutdownSettlesIntentResolvers(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "shim_public.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var intent, terminal, teardown token.Pos
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "runShutdown" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "finishOwedIntents":
					intent = call.Pos()
				case "finishOwedTerminals":
					terminal = call.Pos()
				}
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "close" {
				teardown = call.Pos()
			}
			return true
		})
	}
	if intent == 0 || terminal <= intent || teardown <= terminal {
		t.Fatal("shutdown lacks intent settlement before terminal cleanup and JS teardown")
	}
}

// M-C8's classification entrance, independently of retry dispatch. A successor or
// terminal record must not be mistaken for an unanswered abandoned origin.
func TestUnitCAbandonedIntentClassification(t *testing.T) {
	f := testAbandonAdmitted(t, "c2-classify", 2)
	if f.deny() != mqttAbandonDone {
		t.Fatal("deny did not land")
	}
	b := testUnitCAbandonedBinding(t, f)
	rec, seq := f.register(t)
	f.e.s.closeWillDispatch()
	for _, tc := range []struct {
		name   string
		change func(*mqttInflightRecord)
		want   mqttPubRelIntentOutcome
	}{
		{"unanswered", func(*mqttInflightRecord) {}, mqttPubRelIntentAbandoned},
		{"intended", func(r *mqttInflightRecord) { r.State = mqttInflightPubRelIntended }, mqttPubRelIntentConfirmed},
		{"terminal", func(r *mqttInflightRecord) { r.State = mqttInflightPublishTerminal }, mqttPubRelIntentEnded},
		{"successor", func(r *mqttInflightRecord) { r.Nonce = mqttNewInflightNonce() }, mqttPubRelIntentOther},
		{"acquisition", func(r *mqttInflightRecord) { r.Acq++ }, mqttPubRelIntentOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cur := *rec
			tc.change(&cur)
			f.e.asm.mu.Lock()
			f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return &cur, &StoredMsg{Sequence: seq}, nil
			}
			f.e.asm.mu.Unlock()
			if got := f.e.sess.resolvePubRelIntent(nil, b.pending()); got != tc.want {
				t.Fatalf("classification = %v, want %v", got, tc.want)
			}
		})
	}
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = nil
	f.e.asm.mu.Unlock()
}

// M-C46: finishing an old instance must neither remove a replacement nor discharge
// an unresolved final attempt. No backend guard can mask this ownership comparison.
func TestUnitCIntentResolverExactCompletion(t *testing.T) {
	s := &Server{}
	key := mqttIntentResolutionKey{sess: &mqttSession{}, nonce: "allocation"}
	old := &mqttIntentResolution{key: key, srv: s}
	next := &mqttIntentResolution{key: key, srv: s}
	s.owedIntents = map[mqttIntentResolutionKey]*mqttIntentResolution{key: next}
	old.finish(true)
	if s.owedIntents[key] != next {
		t.Fatal("stale completion removed the replacement resolver")
	}
	next.finish(false)
	if s.owedIntents[key] != next {
		t.Fatal("failed final attempt removed the live resolver")
	}
	next.finish(true)
	if len(s.owedIntents) != 0 {
		t.Fatal("exact completed resolver was retained")
	}
}

// The write boundary lets an abandoned register move along an acknowledgement arrow ONLY under the
// acknowledgement's own kind: the same member shape under any other kind is the durable disposal
// record being rewritten, and is refused by exactly that rule (not by some later check that a
// different batch might not trip).
//
// Mutation M-Cbnd: the exception keyed on the member's shape alone -> these batches pass the
// abandoned-register rule and are refused elsewhere, or land.
func TestUnitCAbandonedRegisterAdvancesOnlyUnderTheAckKind(t *testing.T) {
	for _, k := range []mqttTransitionKind{mqttTransitionAdmitExchange, mqttTransitionAcquireAction} {
		t.Run(string(k), func(t *testing.T) {
			f := testAbandonAdmitted(t, "c2-bnd-"+string(k), 1)
			if f.deny() != mqttAbandonDone {
				t.Fatal("fixture: the deny did not land")
			}
			jsa := &f.e.asm.jsa
			auth, authSeq, err := jsa.requireAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
			if err != nil {
				t.Fatal(err)
			}
			reg, regSeq := f.register(t)
			x, xSeq, _ := jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.lin)
			tr, terr := mqttAdvanceExchangeAckTransition(auth, authSeq, &mqttAdvanceExchangeAckRequest{
				ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "forged",
				Gen: f.gen, Nonce: f.lin, To: mqttExchangeTerminal, Acq: reg.Acq, Register: reg,
				RegisterSeq: regSeq}, x, xSeq)
			if terr != nil {
				t.Fatalf("fixture: composing the acknowledgement arrow: %v", terr)
			}
			// Only the register member (the acknowledgement-shaped rewrite), under a foreign kind.
			tr.Extra = tr.Extra[len(tr.Extra)-1:]
			tr.Attempt.Kind = k
			_, cerr := jsa.commitAuthorityTransition(tr)
			if cerr == nil || !strings.Contains(cerr.Error(), "rewrites the abandoned register") {
				t.Fatalf("a %s batch rewriting the abandoned register was answered %v", k, cerr)
			}
			if cur, _ := f.register(t); cur.State != mqttInflightDeliveryAbandoned {
				t.Fatalf("the register moved to %q", cur.State)
			}
		})
	}
}

// A generation discard that REUSES the session object (a clean-start handoff) leaves no ACK-only
// binding of the discarded generation behind: nothing could route an acknowledgement to it, and
// nothing observes its exchange ending after the discard.
//
// Mutation M-Cwip2: resetForReuseLocked keeps abandonedAcks -> the binding survives the re-mint.
func TestUnitCAbandonedAckBindingsGoWithTheGeneration(t *testing.T) {
	f := testAbandonAdmitted(t, "c2-reuse", 1)
	if f.deny() != mqttAbandonDone {
		t.Fatal("fixture: the deny did not land")
	}
	sess := f.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if len(sess.abandonedAcks) == 0 {
		t.Fatal("fixture: no ACK-only binding was installed, so nothing is tested")
	}
	sess.resetForReuseLocked(mqttRetirementPermit{tr: sess.leases}, mqttNewInflightNonce(), 0)
	if n := len(sess.abandonedAcks); n != 0 {
		t.Fatalf("%d ACK-only binding(s) of the discarded generation survived its re-mint", n)
	}
}

// M-C55-binding-other: retain an unreachable ACK binding after positive displacement.
// M-C55-binding-unknown: drop the binding on an unresolved classification.
// M-C55-binding-absent: discharge an absent slot beside an unended exchange.
// M-C55-intent-final: drop a refused/failed resolver before its exact handoff.
func TestUnitCAbandonedAckOwnerLifecycle(t *testing.T) {
	for _, route := range []string{"immediate", "resolver"} {
		for _, exit := range []string{"foreign", "absent", "unreadable", "failed attempt", "epoch", "connection close", "generation discard"} {
			t.Run(route+"/"+exit, func(t *testing.T) {
				f := testAbandonAdmitted(t, "binding-lifecycle", 2)
				if f.deny() != mqttAbandonDone {
					t.Fatal("fixture: abandonment did not land")
				}
				b := testUnitCAbandonedBinding(t, f)
				ack := b.pending()
				f.e.s.closeWillDispatch()
				cur, seq := f.register(t)
				f.e.asm.mu.Lock()
				switch exit {
				case "foreign":
					other := *cur
					other.Nonce = mqttNewInflightNonce()
					f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
						return &other, &StoredMsg{Sequence: seq, Subject: f.subject}, nil
					}
				case "absent":
					f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
						return nil, nil, nil
					}
				case "unreadable":
					f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
						return nil, &StoredMsg{Sequence: seq, Subject: f.subject}, nil
					}
				case "failed attempt", "connection close":
					f.e.asm.testPubRecAdvanceWait = func(time.Duration) {}
					f.e.asm.testAckAdvance = func(*mqttAdvanceExchangeAckRequest, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
						return mqttOpOutcome{}, errors.New("pre-intent refusal")
					}
				case "epoch":
					old := *b
					old.epoch = "another-store-epoch"
					ack = old.pending()
				}
				f.e.asm.mu.Unlock()
				if exit == "connection close" {
					f.e.sess.mu.Lock()
					f.e.sess.c = nil
					f.e.sess.mu.Unlock()
				}
				if route == "resolver" || exit == "generation discard" {
					f.e.sess.scheduleIntentResolution(nil, f.pi, ack)
					f.e.s.owedIntentsMu.Lock()
					n := len(f.e.s.owedIntents)
					f.e.s.owedIntentsMu.Unlock()
					if n != 1 {
						t.Fatal("admission refusal lost the resolver")
					}
				}
				if exit == "generation discard" {
					f.e.sess.mu.Lock()
					f.e.sess.resetForReuseLocked(mqttRetirementPermit{tr: f.e.sess.leases}, mqttNewSessGen(), 0)
					f.e.sess.mu.Unlock()
				}
				if route == "resolver" || exit == "generation discard" {
					f.e.s.finishOwedIntents()
				} else {
					out := f.e.sess.intendPubRelSlot(nil, ack)
					if out == mqttPubRelIntentUnresolved || out == mqttPubRelIntentAbandoned {
						f.e.sess.scheduleIntentResolution(nil, f.pi, ack)
					}
				}
				wantRetained := exit != "foreign" && exit != "generation discard"
				f.e.sess.mu.Lock()
				retained := f.e.sess.abandonedAcks[b.index()] == b
				f.e.sess.mu.Unlock()
				f.e.s.owedIntentsMu.Lock()
				owed := len(f.e.s.owedIntents)
				f.e.s.owedIntentsMu.Unlock()
				if retained != wantRetained || (owed != 0) != wantRetained {
					t.Fatalf("retained=%v owed=%d, want active ownership=%v", retained, owed, wantRetained)
				}
				if !wantRetained {
					return
				}
				// Failed final passes must be repeatable, not a one-shot retention.
				f.e.s.finishOwedIntents()
				f.e.s.owedIntentsMu.Lock()
				after := len(f.e.s.owedIntents)
				f.e.s.owedIntentsMu.Unlock()
				if after != 1 {
					t.Fatal("failed final pass lost ownership")
				}
				// A proven generation discard consumes the retained resolver without
				// reinterpreting old packet evidence under a different generation.
				f.e.sess.mu.Lock()
				f.e.sess.resetForReuseLocked(mqttRetirementPermit{tr: f.e.sess.leases}, mqttNewSessGen(), 0)
				f.e.sess.mu.Unlock()
				f.e.s.finishOwedIntents()
				f.e.s.owedIntentsMu.Lock()
				left := len(f.e.s.owedIntents)
				f.e.s.owedIntentsMu.Unlock()
				if left != 0 {
					t.Fatal("discarded generation retained its resolver")
				}
			})
		}
	}
}

// M-C55-intent-admission: return done for PubRelIntended even though the
// materializer's admission was refused during a non-final resolver attempt.
func TestUnitCIntentResolverKeepsUnmaterializedHandoff(t *testing.T) {
	f := testAbandonAdmitted(t, "intent-handoff-owner", 2)
	if f.deny() != mqttAbandonDone {
		t.Fatal("fixture: abandonment did not land")
	}
	ack := testUnitCAbandonedBinding(t, f).pending()
	f.e.s.closeWillDispatch()
	f.e.sess.scheduleIntentResolution(nil, f.pi, ack)
	f.e.s.owedIntentsMu.Lock()
	var owner *mqttIntentResolution
	for _, r := range f.e.s.owedIntents {
		owner = r
	}
	f.e.s.owedIntentsMu.Unlock()
	if owner == nil {
		t.Fatal("no refused-admission owner")
	}
	f.e.asm.mu.Lock()
	f.e.asm.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
		return nil, 0, false
	}
	f.e.asm.mu.Unlock()
	done := owner.attempt(false) // A timer admitted before admission closed.
	owner.finish(done)
	f.e.s.owedIntentsMu.Lock()
	retained := f.e.s.owedIntents[owner.key] == owner
	f.e.s.owedIntentsMu.Unlock()
	r, _ := f.register(t)
	if done || !retained || r.State != mqttInflightPubRelIntended {
		t.Fatal("unmaterialized PUBREC lost its live resolver")
	}
	f.e.asm.mu.Lock()
	f.e.asm.testInflightPubRelScan = nil
	f.e.asm.mu.Unlock()
	f.e.s.finishOwedIntents()
	f.e.s.owedIntentsMu.Lock()
	left := len(f.e.s.owedIntents)
	f.e.s.owedIntentsMu.Unlock()
	r, _ = f.register(t)
	if left != 0 || r.State != mqttInflightPubRelReady {
		t.Fatal("healthy final pass failed the canonical PUBREL handoff")
	}
}

// M-C55-source-only: discharge an unanswered ACK binding on source completion alone.
func TestUnitCSourceCompletionPreservesUnansweredBinding(t *testing.T) {
	f := testAbandonAdmitted(t, "binding-source-only", 1)
	if f.deny() != mqttAbandonDone {
		t.Fatal("fixture: abandonment did not land")
	}
	b := testUnitCAbandonedBinding(t, f)
	ref := b.pending().slot
	f.e.sess.mu.Lock()
	f.e.sess.holdTerminalSourceLocked(ref)
	f.e.sess.mu.Unlock()
	f.e.asm.mu.Lock()
	f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	f.e.asm.mu.Unlock()
	if !f.e.sess.finishTerminalSource(ref, false) {
		t.Fatal("completed source was not discharged")
	}
	f.e.sess.mu.Lock()
	retained := f.e.sess.abandonedAcks[b.index()] == b
	f.e.sess.mu.Unlock()
	if !retained {
		t.Fatal("source completion consumed the unanswered MQTT binding")
	}
	if r, _ := f.register(t); r.State != mqttInflightDeliveryAbandoned {
		t.Fatal("source completion changed the unanswered register")
	}
}
