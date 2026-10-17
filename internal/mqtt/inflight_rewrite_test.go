// Copyright 2024 The NATS Authors
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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// F-087: every retargeted CAS on $MQTT_inflight rebuilds from the record it just read.
//
// The tests below are organized by what a rebuild PRESERVES — the fact a concurrent
// writer proved between the caller's build and its re-target — because that is the
// property, and a per-site assertion would go stale the moment a sixth writer appears.

// testRegisterAt reads the register on pi and fails the test if it is not there.
func testRegisterAt(t *testing.T, e *testInflightEnv, pi uint16) (*mqttInflightRecord, *StoredMsg) {
	t.Helper()
	rec, smsg := e.loadRegister(t, pi)
	if rec == nil || smsg == nil {
		t.Fatalf("the register on packet identifier %d must exist", pi)
	}
	return rec, smsg
}

// testMarkRegister rewrites the register on pi with mutate applied, at its exact
// sequence — a concurrent writer, from outside the operation under test.
func testMarkRegister(t *testing.T, e *testInflightEnv, pi uint16, mutate func(*mqttInflightRecord)) uint64 {
	t.Helper()
	rec, smsg := testRegisterAt(t, e, pi)
	next := *rec
	mutate(&next)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	seq, err := e.asm.jsa.storeInflightCAS(subject, smsg.Sequence, &next)
	if err != nil {
		t.Fatalf("concurrent rewrite of the register on %d: %v", pi, err)
	}
	return seq
}

func TestMQTTInflightRetireRetargetPreservesConcurrentCancellation(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-retire-rebuild")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	rec, smsg := testRegisterAt(t, e, pi)

	// A foreign claim value, planted so a refresh of the arbitration fields is visible.
	foreignClaim := mqttNewInflightNonce()
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) { r.Claim = foreignClaim })

	// The first retirement CAS is aimed at the stale sequence, so it rejects; the
	// re-read then finds a record carrying BOTH the foreign claim and a cancellation.
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) {
		r.Cancelled, r.CancelOrigin = true, mqttCancelOriginOccupant // F-088: provenance is written
	})

	if _, ok := e.sess.markInflightRegisterRetired(e.c, pi, dur, sseq, smsg.Sequence,
		rec.Nonce, _EMPTY_, _EMPTY_); !ok {
		t.Fatal("the retirement must resolve through its re-target")
	}
	got, _ := testRegisterAt(t, e, pi)
	if !got.Retired {
		t.Fatal("the retirement did not land")
	}
	if !got.Cancelled {
		t.Fatal("the re-targeted retirement ERASED a concurrent cancellation")
	}
	if got.Claim != foreignClaim {
		t.Fatalf("the retirement refreshed the arbitration fields, which belong to the contest: claim=%q want %q",
			got.Claim, foreignClaim)
	}
}

// Both cancellation writers own Cancelled and nothing else. A re-target that
// re-proposed the sparse first-CAS body would erase a Retired — the catastrophic
// direction, because recovery then treats a completed exchange as deliverable.
func TestMQTTInflightCancelRetargetPreservesConcurrentRetirement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mark func(e *testInflightEnv, pi uint16, dur string, sseq, regSeq uint64, nonce string) error
	}{
		{"markInflightRegisterCancelled", func(e *testInflightEnv, pi uint16, dur string, sseq, regSeq uint64, nonce string) error {
			_, merr := e.sess.markInflightRegisterCancelled(e.c, pi, dur, sseq, regSeq, nonce, mqttTestLicence(e.sess, nil))
			return merr
		}},
		{"markInflightRegisterCancelledPending", func(e *testInflightEnv, pi uint16, dur string, sseq, regSeq uint64, nonce string) error {
			return e.sess.markInflightRegisterCancelledPending(e.c, mqttRegCancellation{
				pi: pi, nonce: nonce, durable: dur, sseq: sseq, reply: testInflightAckSubject(dur, sseq, 1),
			}, mqttTestLicence(e.sess, nil))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-cancel-rebuild-"+tc.name)
			dur := e.sess.idHash + "_plain"
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 2)
			rec, smsg := testRegisterAt(t, e, pi)

			// The retirement lands between the caller's build and its re-target: the
			// first CAS (create-only, or at the stale sequence) rejects, and the re-read
			// finds a Retired record.
			testMarkRegister(t, e, pi, func(r *mqttInflightRecord) { r.Retired = true })

			if err := tc.mark(e, pi, dur, sseq, smsg.Sequence, rec.Nonce); err != nil {
				t.Fatalf("the cancellation must resolve through its re-target: %v", err)
			}
			got, _ := testRegisterAt(t, e, pi)
			if !got.Cancelled {
				t.Fatal("the cancellation did not land")
			}
			if !got.Retired {
				t.Fatal("the re-targeted cancellation ERASED a concurrent retirement: a completed exchange would be re-sent")
			}
		})
	}
}

// Retired dominates Cancelled at every reader. Recovery is the one that decides whether
// an identifier is freed, so it is the one worth stating twice: a record carrying both
// routes to the suppression, and its identifier is NOT handed to another message.
func TestMQTTInflightRetiredDominatesCancelledAtRecovery(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-precedence")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) {
		r.Retired, r.Cancelled, r.CancelOrigin = true, true, mqttCancelOriginOccupant // F-088: provenance
	})
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist session: %v", err)
	}

	_, sessB, cB := e.peer(t, "node2", "client-precedence")
	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	sessB.mu.Lock()
	entry := sessB.pendingPublish[pi]
	rel := sessB.pendingPubRel[pi]
	rs := sessB.srcAckOwed[mqttDeferredKey{dur, sseq}]
	sessB.mu.Unlock()
	if entry != nil {
		t.Fatalf("a Retired-and-Cancelled register must not be installed as deliverable: %+v", entry)
	}
	if rs == nil {
		t.Fatal("a Retired-and-Cancelled register must reinstate its suppression rather than free the identifier")
	}
	if rel == nil || rel.jsAckSubject != _EMPTY_ {
		t.Fatalf("a Retired-and-Cancelled register must keep a non-actionable identifier reservation: %+v", rel)
	}
}

func TestMQTTInflightVerdictReapForgetsOnlyOnProof(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-verdict-property")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	vrec, smsg := testRegisterAt(t, e, pi)
	nonce := e.nonceOf(pi)

	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	e.sess.mu.Unlock()

	// A delete that FAILS keeps the register and the verdict: the next scan retries the
	// whole sequence against a verdict that is already durable.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return fmt.Errorf("test: the delete could not be proven") }
	e.asm.mu.Unlock()
	if e.sess.reapRegisterWithVerdict(smsg.Sequence, nonce, false, vrec, smsg.Subject, nil) {
		t.Fatal("an unproven delete must not report the register gone")
	}
	e.sess.mu.Lock()
	_, stillHeld := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if !stillHeld {
		t.Fatal("the verdict was forgotten against a delete that was never proven")
	}

	e.asm.mu.Lock()
	e.asm.testInflightDelete = nil
	e.asm.mu.Unlock()
	if !e.sess.reapRegisterWithVerdict(smsg.Sequence, nonce, false, vrec, smsg.Subject, nil) {
		t.Fatal("a proven delete must report the register gone")
	}
	e.sess.mu.Lock()
	_, held := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if held {
		t.Fatal("a verdict whose register was deleted WITH PROOF must be forgotten, or it accumulates for the session's life")
	}
	if rec, _ := e.loadRegister(t, pi); rec != nil {
		t.Fatal("the register must be gone")
	}
}

// The sequence a resume applies to a live entry is judged against the operation's whole
// LINEAGE, not against its base alone: an operation may legitimately pass through S2 on
// its way from S1 to S3.
func TestMQTTInflightLineageAwareSequenceApply(t *testing.T) {
	t.Parallel()
	rv := mqttRecoveredInflightReg{seq: 30, traversed: []uint64{10, 20, 30}}
	for _, seq := range []uint64{10, 20, 30} {
		if !rv.traversedContains(seq) {
			t.Fatalf("sequence %d is a link in this operation's chain and must be applied", seq)
		}
	}
	if rv.traversedContains(40) {
		t.Fatal("a sequence outside the chain belongs to a writer this operation never saw; its fact stands")
	}
	// The degenerate case, which is the ordinary one: a record nothing contested carries
	// a one-element chain, and the check is exactly "unchanged since the walk".
	plain := mqttRecoveredInflightReg{seq: 7, traversed: []uint64{7}}
	if !plain.traversedContains(7) || plain.traversedContains(6) {
		t.Fatal("an uncontested record must apply at its walk sequence and nowhere else")
	}
}

// The sweep RETAINS a verdict-bearing register it cannot durably settle, and deletes
// one it can — with proof, releasing the verdict against that proof. A retained
// candidate must also report no mutation, or a stable prefix of them consumes the
// per-tick budget and starves every actionable candidate behind it.
func TestMQTTInflightSweepRetainsAVerdictItCannotForget(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-sweep-retention")
	// AFFINE, because the disposal is: this sweep arm is reached only for a retired affine
	// register. An ordinary register is an admitted obligation, ended through its exchange
	// under an entitlement (F-105 7b), never by this licence-less disposal.
	dur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	rec, smsg := testRegisterAt(t, e, pi)
	nonce := e.nonceOf(pi)
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	e.sess.mu.Unlock()

	name := &mqttInflightName{
		idHash: e.sess.idHash, gen: e.sess.gen, pi: pi,
		subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, pi),
	}
	carrying := *rec
	carrying.Retired, carrying.Cancelled = true, true
	carrying.CancelOrigin = mqttCancelOriginOccupant // F-088: provenance is written

	// NO resident, identity-fenced session: nothing here can forget the verdict, so the
	// register — the verdict's only carrier — must survive.
	e.asm.mu.Lock()
	delete(e.asm.sessByHash, e.sess.idHash)
	e.asm.mu.Unlock()
	if e.asm.disposeSettledAffineRegister(name, &carrying, smsg.Sequence) {
		t.Fatal("a retained candidate must report no mutation, or it charges the sweep's budget for doing nothing")
	}
	if got, _ := e.loadRegister(t, pi); got == nil {
		t.Fatal("deleting the register leaves the verdict with nothing to identify it by, for the life of the generation")
	}

	// With the session resident and its generation matching, the same disposal deletes
	// with proof and releases the verdict.
	e.asm.mu.Lock()
	e.asm.sessByHash[e.sess.idHash] = e.sess
	e.asm.mu.Unlock()
	if !e.asm.disposeSettledAffineRegister(name, &carrying, smsg.Sequence) {
		t.Fatal("a settleable register must be disposed of, and the mutation charged")
	}
	if got, _ := e.loadRegister(t, pi); got != nil {
		t.Fatal("the register must be gone")
	}
	e.sess.mu.Lock()
	_, held := e.sess.cancelledRegNonces[nonce]
	e.sess.mu.Unlock()
	if held {
		t.Fatal("the verdict must be released against the proof of the deletion")
	}
}

// The send section holds the identifier for as long as the write does — including
// across a session discard, which resets the identifier counter to zero.
func TestMQTTInflightSendSectionHoldsTheIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-section")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok || token.token == _EMPTY_ {
		t.Fatal("a confirmed entry on this session's connection must be reservable")
	}

	// A second section for the same identifier is refused while the first is open.
	e.sess.mu.Lock()
	_, twice := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if twice {
		t.Fatal("two send sections for one identifier would put two PUBLISHes on the wire under it")
	}

	// And refused even when the ENTRY says nothing, which is the case the registry
	// exists for: a successful PUBREC replaces the PUBLISH entry while the write is
	// still out, so a re-installed entry on this identifier carries no section of its
	// own. Only the registry knows the identifier is still being written.
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq}
	_, afterReplace := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if afterReplace {
		t.Fatal("the REGISTRY, not the entry, is what holds an identifier whose write has not returned")
	}

	// The registry, not the entry, is what bumpPI consults — so the identifier survives
	// a discard that nils every map and resets the counter.
	e.sess.mu.Lock()
	e.sess.pendingPublish = nil
	e.sess.pendingPubRel = nil
	e.sess.cpending = nil
	e.sess.last_pi = 0
	e.sess.pendingPublish = make(map[uint16]*mqttPending)
	e.sess.pendingPubRel = make(map[uint16]*mqttPending)
	e.sess.cpending = make(map[string]map[uint64]uint16)
	minted := make(map[uint16]bool)
	for i := 0; i < int(pi)+2; i++ {
		next := e.sess.bumpPI()
		if next == 0 {
			break
		}
		minted[next] = true
		e.sess.pendingPublish[next] = &mqttPending{}
	}
	e.sess.mu.Unlock()
	if minted[pi] {
		t.Fatalf("packet identifier %d was re-minted while its write was still in flight", pi)
	}

	e.sess.endSend(e.c, pi, token.token)
	e.sess.mu.Lock()
	_, stillReserved := e.sess.sendsInFlight[pi]
	e.sess.mu.Unlock()
	if stillReserved {
		t.Fatal("endSend must release the registry entry, or the identifier is held for the session's life")
	}
}

// A terminal acknowledgement arriving mid-write PARKS its identifier-freeing half. The
// protocol answer is unaffected; the untrack and the register delete wait for the write.
func TestMQTTInflightTerminalAckParksWhileASendIsInFlight(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-park")
	dur := e.sess.idHash + "_plain"
	sseq, reply := e.publishOfferedSource(t, dur)
	pi := e.trackAdmitted(t, dur, sseq, 1, reply)

	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	parked := ok && e.sess.condemnSendLocked(pi, mqttSendSettleAckComplete)
	entry := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if !ok || !parked {
		t.Fatal("a removal path meeting an active send section must condemn the entry rather than delete it")
	}
	if entry == nil || entry.sendSettle.kind != mqttSendSettleAckComplete {
		t.Fatalf("the parked disposition must name the continuation it owes: %+v", entry)
	}

	// A SECOND remover does its own durable work but neither replaces the parked
	// disposition nor executes it early.
	e.sess.mu.Lock()
	e.sess.condemnSendLocked(pi, mqttSendSettlePurgeTeardown)
	kind := e.sess.pendingPublish[pi].sendSettle.kind
	resident := e.sess.pendingPublish[pi] != nil
	e.sess.mu.Unlock()
	if kind != mqttSendSettleAckComplete {
		t.Fatalf("the FIRST parked settle wins, got %v", kind)
	}
	if !resident {
		t.Fatal("the identifier must stay resident until the write settles, or bumpPI can re-mint it")
	}

	e.sess.endSend(e.c, pi, token.token)
	e.sess.mu.Lock()
	after := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if after != nil {
		t.Fatalf("endSend must complete the parked continuation: entry=%+v", after)
	}
	// The identifier is held until the acknowledgement's terminal cleanup is confirmed,
	// and that cleanup is HANDED OFF to the terminal-retry dispatcher rather than run on
	// the section's goroutine (handOffAcknowledgedPublish), so the hold's release and the
	// register delete are observed where they happen: when the worker's attempt confirms.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		e.sess.mu.Lock()
		_, stillReserved := e.sess.sendsInFlight[pi]
		e.sess.mu.Unlock()
		if !stillReserved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("endSend must complete the parked continuation: the identifier is still reserved")
		}
	}
	e.waitRegisterCount(t, 0)
}

// The real acknowledgement path parks, not just the primitive: a PUBACK arriving while
// this entry's transmission is still out leaves the identifier resident and its register
// standing until the write returns.
func TestMQTTInflightPubAckParksThroughTheAckPath(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-puback-park")
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	dur := e.sess.idHash + "_plain"
	sseq, reply := e.publishOfferedSource(t, dur)
	pi := e.trackAdmitted(t, dur, sseq, 1, reply)

	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the entry must be reservable")
	}

	if err := e.c.mqttProcessPubAck(pi); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	e.sess.mu.Lock()
	entry := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if entry == nil {
		t.Fatal("a PUBACK that freed the identifier mid-write lets bumpPI re-mint it under the blocked PUBLISH")
	}
	if entry.sendSettle.kind != mqttSendSettleAckComplete {
		t.Fatalf("the acknowledgement must park its freeing half: %+v", entry.sendSettle)
	}
	if rec, _ := e.loadRegister(t, pi); rec == nil {
		t.Fatal("the register delete belongs to the parked continuation, not to the acknowledgement")
	}

	e.sess.endSend(e.c, pi, token.token)
	e.sess.mu.Lock()
	after := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if after != nil {
		t.Fatal("endSend must complete the acknowledgement's freeing half")
	}
	// The deferred register delete is the terminal continuation's, handed off by endSend
	// rather than run on its goroutine (handOffAcknowledgedPublish), and any attempt that
	// cannot confirm is retried by design — so it is observed when it lands.
	e.waitRegisterCount(t, 0)
}

// Every identifier-bearing transmission is inside a send section — asserted
// STRUCTURALLY, over the package's own syntax tree, because the property is about the
// call sites rather than about any one execution.
//
// A behavioral test can only cover the sites it happens to drive, and this is exactly
// the class of defect that reappears at whichever site the last review did not name: a
// new delivery path that enqueues under an allocated identifier without reserving it
// re-opens the stale-send race in full.
//
// WHAT THIS PROVES, and what it does not. It binds every identifier-bearing call to
// mqttEnqueuePublishMsgTo and to the two PUBREL response helpers to a send section
// whose endSend is deferred BEFORE the call. It does NOT prove that some future code
// path could not frame a packet by hand, and it does not resolve an alias that crosses
// a function boundary. One identifier-bearing transmission is deliberately outside it:
// a RETAINED QoS 1/2 message allocates through trackPublishRetained, builds its frame
// directly and is flushed by a later queueOutbound with no section at all (F-091),
// which this guard cannot see because the retained path uses neither helper.
//
// The QoS 0 enqueues (a literal 0 identifier) allocate nothing and are excluded by the
// same rule that keeps retained sends outside the register invariants (R6). The skip is
// COUNTED and asserted nonzero: a selector that quietly matched nothing would otherwise
// look exactly like a clean pass.
func TestMQTTInflightEveryQoS12SendIsReserved(t *testing.T) {
	t.Parallel()
	type siteClass struct {
		what     string
		callee   string
		piArg    int    // index into call.Args of the packet identifier
		typeArg  int    // index of the packet-type argument, or -1 when the callee implies it
		begin    string // the reservation this class must open
		wantSkip bool   // whether a literal-zero identifier is a legitimate skip here
	}
	classes := []siteClass{
		// ADMISSION is the identifier-bearing landmark: mqttPreparePublishMsgTo(cc, pi, ...)
		// frames the PUBLISH, and the write that follows it (mqttEnqueuePreparedPublishMsgTo,
		// which carries the identifier inside the prepared frame) is bound to it below. The
		// QoS 0 entry point passes the literal zero and has no identifier parameter at all.
		{"a QoS 1/2 PUBLISH", "mqttPreparePublishMsgTo", 1, -1, "beginSendLocked", true},
		// The response helpers carry every acknowledgement type, so the packet type is
		// what selects a PUBREL. Only a PUBREL bears an identifier the SERVER still owns
		// — a PUBACK, PUBREC or PUBCOMP answers one the CLIENT owns and reserves nothing.
		{"a PUBREL", "mqttEnqueuePubResponse", 1, 0, "beginPubRelSendLocked", false},
		{"a PUBREL", "mqttEnqueuePubResponseReason", 1, 0, "beginPubRelSendLocked", false},
	}
	found := map[string]int{}
	skipped := 0

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
			// Collect the reservation landmarks of this function once, with positions:
			// ordering is the property, not mere presence. A defer placed AFTER the
			// enqueue leaves the window open on an early return or a recovered panic,
			// and a presence-only check passes it.
			beginPos := map[string]token.Pos{}
			var deferEndSend token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.CallExpr:
					if name := testCalleeName(v); name != _EMPTY_ {
						if _, seen := beginPos[name]; !seen {
							beginPos[name] = v.Pos()
						}
					}
				case *ast.DeferStmt:
					// The PUBLISH section arms `defer func() { endPublishSend(..., intent, wrote) }()`,
					// because what it records is only known after intent and the enqueue
					// return; the PUBREL section arms `defer endSend(...)` directly. Both are
					// the landmark.
					if testDefersSendEnd(v) && deferEndSend == token.NoPos {
						deferEndSend = v.Pos()
					}
				}
				return true
			})

			// Sites are collected ACROSS every class before any pairing decision. Per
			// class is not enough: one PUBLISH and one PUBREL in a single function
			// would each be alone in its own class and would then share the function's
			// one begin/defer landmark, which is exactly the second-unreserved-enqueue
			// evasion this pairing exists to close.
			type enqueueSite struct {
				call *ast.CallExpr
				cl   siteClass
			}
			var sites []enqueueSite
			for _, cl := range classes {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, _ := n.(*ast.CallExpr)
					if call == nil || testCalleeName(call) != cl.callee || len(call.Args) <= cl.piArg {
						return true
					}
					if cl.typeArg >= 0 {
						if len(call.Args) <= cl.typeArg ||
							!testResolvesToConst(fn.Body, call.Args[cl.typeArg], "mqttPacketPubRel") {
							return true
						}
					}
					if testIsLiteralZero(call.Args[cl.piArg]) {
						if !cl.wantSkip {
							t.Errorf("%s: %s enqueued under a literal zero identifier, which this guard "+
								"does not expect for this class", fset.Position(call.Pos()), cl.what)
						}
						skipped++
						return true
					}
					sites = append(sites, enqueueSite{call, cl})
					return true
				})
			}
			if len(sites) == 0 {
				continue
			}
			// One-to-one, and the landmarks are single: a function holding two
			// identifier-bearing enqueues of ANY class cannot have each bound to its own
			// preceding pair by a guard that records one begin per name and one defer.
			// Fail closed and say what to do rather than silently pairing them.
			if len(sites) > 1 {
				var names []string
				for _, st := range sites {
					names = append(names, fset.Position(st.call.Pos()).String())
				}
				t.Errorf("%s: %d identifier-bearing enqueues share one enclosing function (%s), so this "+
					"guard cannot bind each to its own send section: give the second its own section, "+
					"or extend this guard to pair them",
					fset.Position(sites[0].call.Pos()), len(sites), strings.Join(names, ", "))
				continue
			}
			site, cl := sites[0].call, sites[0].cl
			found[cl.what]++
			// The WRITE consumes the admitted frame and must follow the admission it came
			// from: a transmission positioned before its own admission — or before the
			// section's landmarks, which admission is bound to below — would frame an
			// identifier the reservation does not yet cover.
			if cl.callee == "mqttPreparePublishMsgTo" {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if call, _ := n.(*ast.CallExpr); call != nil && testCalleeName(call) == "mqttEnqueuePreparedPublishMsgTo" && call.Pos() < site.Pos() {
						t.Errorf("%s: the PUBLISH is transmitted before its admission at %s", fset.Position(call.Pos()), fset.Position(site.Pos()))
					}
					return true
				})
			}
			bp, hasBegin := beginPos[cl.begin]
			if !hasBegin || bp > site.Pos() {
				t.Errorf("%s: %s is enqueued outside a send section (%s before it): the identifier can be "+
					"freed and re-minted while the write is blocked",
					fset.Position(site.Pos()), cl.what, cl.begin)
			}
			if deferEndSend == token.NoPos || deferEndSend > site.Pos() {
				t.Errorf("%s: %s reserves the identifier without arming endSend with defer BEFORE the "+
					"enqueue: an early return or a recovered panic would hold it for the connection's life",
					fset.Position(site.Pos()), cl.what)
			}
		}
	}

	for _, what := range []string{"a QoS 1/2 PUBLISH", "a PUBREL"} {
		if found[what] == 0 {
			t.Errorf("no identifier-bearing %s enqueue was found, so this guard checks nothing for that class", what)
		}
	}
	if skipped == 0 {
		t.Error("no QoS 0 enqueue was skipped, so the literal-zero exclusion matched nothing and " +
			"the selector is probably wrong")
	}
	testFramingCloseSet(t)
}

// testFramingCloseSet is the other half of the guarantee above, and it exists because the
// pairing guard can only see the two enqueue helpers.
//
// A hand-built frame escapes it entirely, which is what F-091 was: the retained path
// framed its own PUBLISH with mqttMakePublishHeader and was flushed by a later
// queueOutbound with no section at all, and this guard's own comment used to record that
// as an accepted exclusion.
//
// So the claim is a CLOSE SET rather than another sample. Every production reference to
// the three framing entry points is matched, and the set of enclosing functions is
// compared for EQUALITY against the list below. A new caller fails; a caller that
// disappears fails too, which is what stops the list rotting into a description of code
// that has moved on.
//
// REFERENCE-based, not call-based. `f := mqttMakePublishHeader; f(...)`, a stored method
// value, or passing the constructor to a higher-order function all frame a PUBLISH while
// never appearing as a CallExpr.Fun, so anything that is not exactly the callee of a
// direct call is rejected outright.
//
// WritePublishHeader is a METHOD, and go/ast alone cannot prove a selector's receiver is a
// *mqttWriter. The name is therefore RESERVED package-wide: every selector with that name
// is matched whatever its receiver. That is broader than strictly necessary and it is the
// deliberate choice — every structural guard in this package is pure go/ast, and a
// go/types dependency for one identifier is disproportionate.
func testFramingCloseSet(t *testing.T) {
	t.Helper()
	const writeHeader = "WritePublishHeader"
	framers := map[string]struct{}{
		"mqttMakePublishHeader": {}, "mqttAliasedPublishFrame": {},
	}
	// Each entry names what reserves the identifier it frames. The comment is
	// documentation; the enforceable part is the membership.
	want := map[string]string{
		// Reserved by beginRetainedSendLocked, in this same function, before the append.
		"mqttAccountSessionManager.serializeRetainedMsgsForSub": "beginRetainedSendLocked",
		// DELEGATED: ADMISSION and TRANSMISSION are the two halves mqttEnqueuePublishMsgTo
		// was split into so a QoS 1/2 sender can record durable intent between them; their
		// identifier-bearing callers open beginSendLocked, which the pairing guard binds.
		"client.mqttPreparePublishMsgTo":         "the caller's beginSendLocked",
		"client.mqttEnqueuePreparedPublishMsgTo": "the caller's beginSendLocked",
		// A RETAINED copy's frame (F-105 7c): built for the verdict before the section opens and for
		// the write inside it — the caller, transmitRetained, opens beginSendLocked and its written
		// frame goes out through mqttEnqueuePreparedPublishMsgTo, which the pairing guard binds.
		"client.mqttPrepareRetainedPublish": "the caller's beginSendLocked (transmitRetained)",
		// The framing helpers' own bodies. mqttWriter.WritePublishHeader is deliberately
		// NOT here: it is the innermost writer and frames the header inline, so it
		// references none of the three names and never appears as a caller.
		"client.mqttAliasedPublishFrame": "n/a, framing helper",
		"mqttMakePublishHeader":          "n/a, framing constructor",
	}
	got := map[string]struct{}{}

	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		// Parents, because "is this reference exactly a call's callee" is a question about
		// the node ABOVE it and ast.Inspect only walks downward.
		parent := map[ast.Node]ast.Node{}
		// Over the WHOLE FILE, not only function bodies. A package-level alias —
		// `var framePublish = mqttMakePublishHeader` — is a GenDecl initializer, so a walk
		// restricted to FuncDecl bodies never sees it, and every call through it is then
		// invisible too. That was the second holistic round's finding: the guard's comment
		// claimed "every production reference" while the walk did not.
		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				return false
			}
			ast.Inspect(n, func(ch ast.Node) bool {
				if ch != nil && ch != n {
					if _, seen := parent[ch]; !seen {
						parent[ch] = n
					}
					return false
				}
				return true
			})
			return true
		})
		for _, decl := range f.Decls {
			// The enclosing name a reference is attributed to. A FuncDecl gives its own
			// qualified name; anything else is package scope, which is never an approved
			// member, so a framing reference there fails the membership check as well as
			// the direct-callee check.
			qualified := "package scope"
			var scope ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				if fn.Body == nil {
					continue
				}
				qualified = testQualifiedFuncName(fn)
				scope = fn.Body
			}
			ast.Inspect(scope, func(n ast.Node) bool {
				// A framing entry point reaches the AST in two shapes and BOTH have to be
				// matched at the outermost node, or the "is this the callee" test asks
				// about the wrong parent. `mqttMakePublishHeader(...)` is a bare Ident
				// whose parent is the CallExpr; `cc.mqttAliasedPublishFrame(...)` is
				// a SelectorExpr whose Sel is that Ident and whose PARENT is the CallExpr.
				// Matching the inner Ident of the second shape reported every ordinary
				// method call as an alias — which this guard did on its first run.
				var ref ast.Node
				switch v := n.(type) {
				case *ast.SelectorExpr:
					if _, ok := framers[v.Sel.Name]; ok || v.Sel.Name == writeHeader {
						ref = v
					}
				case *ast.Ident:
					if _, ok := framers[v.Name]; !ok {
						return true
					}
					if sel, isSel := parent[v].(*ast.SelectorExpr); isSel && sel.Sel == v {
						return true // counted at the SelectorExpr above
					}
					ref = v
				}
				if ref == nil {
					return true
				}
				got[qualified] = struct{}{}
				call, _ := parent[ref].(*ast.CallExpr)
				if call == nil || call.Fun != ref {
					t.Errorf("%s: %s takes a %s framing entry point as a VALUE rather than calling it "+
						"directly; an identifier-bearing PUBLISH framed through an alias escapes every "+
						"reservation guard in this package",
						fset.Position(ref.Pos()), qualified, "PUBLISH")
				}
				return true
			})
		}
	}

	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s frames an identifier-bearing PUBLISH and is not in the frozen close set: "+
				"say which reservation covers its identifier and add it here, or route it through "+
				"mqttEnqueuePublishMsgTo", name)
		}
	}
	for name, reservation := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("%s no longer frames an identifier-bearing PUBLISH (its reservation was %q), so this "+
				"close set describes code that has moved: remove the entry", name, reservation)
		}
	}
}

// A hold's LIVENESS is the only thing that reserves an identifier, and presence in the
// registry is not it: a hold whose write returned and whose acknowledgement debt is owed
// to a connection that is no longer ours reserves nothing. A reader that tested presence
// would strand a re-minted identifier, its register and a Receive Maximum slot for the
// session's life (F-089).
//
// So the registry is behind an ownership boundary: the predicate and the four helpers
// that MUTATE a hold may touch the field, and every other reader asks
// sendReservedLocked. This guard is about the BOUNDARY — that a new reservation reader
// cannot appear outside the API. Misuse INSIDE a mutation helper is the behavioural
// tests' job, which is why the tombstone case is killed by
// TestMQTTInflightAnInertHoldIsTakenOverNotRefused rather than here.
//
// Every reference is matched, not merely a direct index or range: `holds :=
// sess.sendsInFlight` followed by `holds[pi]` is the same defect one alias away.
func TestMQTTInflightSendHoldsAreReadThroughOneAPI(t *testing.T) {
	t.Parallel()
	// Qualified by RECEIVER, not by bare name: Go lets any other type define a method
	// called endSend or beginSendLocked, and a name-only allow-list would admit a
	// presence reader hidden in one of those.
	owners := map[string]struct{}{
		// The predicate. ONE entry, and the three that used to sit beside it —
		// pubCompHeldLocked, sendHoldLiveLocked, retainedHeldLocked — are gone because
		// they take a *mqttSendHold and never touch the map. They were listed for
		// participating in the predicate rather than for reaching the registry, which is
		// how an exemption list drifts from the access set into a description of a
		// subsystem. Part 3 of the property below is what found them.
		"mqttSession.sendReservedLocked": {},
		// The helpers that mutate a hold.
		"mqttSession.beginSendLocked": {}, "mqttSession.beginPubRelSendLocked": {},
		"mqttSession.endSendCore": {}, "mqttSession.consumePubRelAckLocked": {},
		// A retained frame's reservation: one opener and two release verbs, because
		// written completion is fenced by the session incarnation and abandonment is not
		// (F-091). endRetainedSends and abandonRetainedSends are deliberately ABSENT:
		// they only take the lock and delegate, so the map access stays in these three.
		"mqttSession.beginRetainedSendLocked":            {},
		"mqttSession.releaseRetainedSendAbandonedLocked": {},
		"mqttSession.dropRetainedSendLocked":             {},
		// The retained hold's HANDOVER to the send section that replaces it (F-105 7c): it ends
		// the snapshot's hold in the section's own critical section, never answering whether the
		// identifier is reserved.
		"mqttSession.handRetainedHoldToSectionLocked": {},
		// The bounded-memory sweep.
		"mqttSession.sweepDeadSendHoldsLocked": {},
		// The PUBREL action claim's two verbs — and note what is NOT here.
		//
		// This list once exempted mergeSendActionLocked, clearSendActionLocked and
		// publishSendIdentLocked, described as the verbs that install and clear the action
		// subterm and the reader of its PUBLISH token. NONE OF THE THREE EXISTED: they were
		// written from the plan rather than from the tree, and each was a standing exemption
		// waiting for whoever next used the name. Part 3 of the property below found the
		// third after the first two were deleted by hand — a hand-audited exemption list
		// containing three phantoms is the argument for having the check.
		//
		// So these two are here because the tree has them, and they are named after what
		// they actually do rather than after what a plan said they would:
		"mqttSession.installTentativeActionLocked": {},
		"mqttSession.retireActionLocked_":          {},
		// The EXACT live PUBLISH send token — the interlock that stops an action being
		// acquired over a PUBLISH still on the wire. It reads the hold's own pubIdent
		// rather than asking whether the identifier is reserved, which is a different
		// question and is sendReservedLocked's.
		//
		// ONE reader, not two: admission's refusal and the PUBREL redrive waiter's reread
		// both ask this predicate, so pubRelAdmissionContext is deliberately no longer on
		// this list. Two readers of the same fact is how they drift.
		"mqttSession.publishSectionLiveLocked": {},
		// The open PUBLISH section's OUTPUT, which only the hold names (pubOut beside pubIdent):
		// the client's acknowledgement marks it answered so the section does not then write
		// (F-115). It asks no reservation question; it addresses one transmission's output.
		"mqttSession.answerOpenPublishLocked": {},
		// The EXACT live PUBREL action token, for the same reason: delegating a PUBCOMP'd
		// slot's cleanup to a local drain needs the action holding the token finalize saw,
		// which a reservation (an exclusion, an older action) does not establish. It answers
		// which ACTION holds a token, never whether the identifier is reserved.
		// The PUBREL write entry's exact-holder membership: pointer identity, which a token
		// comparison cannot give (F-105 row 2, holistic review round 1).
		"mqttSession.registeredActionLocked":   {},
		"mqttSession.localActionHoldingLocked": {},
		// The owner of a live PUBCOMP debt for a PUBREL POSSIBLY EMITTED (F-116): the lineage
		// release's question, which discounts an open attempt that has not entered its write. It
		// is not "is this identifier reserved" — the open unit still reserves it.
		"mqttSession.pubCompEmittableOwnerLocked": {},
		// The open PUBREL attempt's write-entry fact, set behind the exact token (F-116).
		"mqttSession.markPubRelEnteredLocked": {},
		// The acknowledged exchange's IDENTIFIER hold (F-098, holistic review round 1, finding
		// 5): one opener, installed in the untrack's own critical section, and one nonce-guarded
		// release, called when the terminal cleanup is confirmed. Both MUTATE a hold; neither
		// answers "is this identifier reserved", which stays sendReservedLocked's.
		// The TRANSMISSION question (unit C3): is a PUBLISH write still out? Not a reservation
		// reader — a terminal identifier hold reserves without transmitting.
		"mqttSession.sendTransmissionOpenLocked":       {},
		"mqttSession.holdTerminalCleanupLocked":        {},
		"mqttSession.releaseTerminalCleanupHoldLocked": {}, // the terminal hold's owner (the unlocked wrapper only locks)
		// The IDENTIFIER EXCLUSION a removal site hands off (F-098, C-IV's consolidation):
		// an install, a lift bound to the instance it observed, an adoption for a recovered
		// exchange's own exclusion, and the instance read that pairs an out-of-lock absence
		// observation with the reservation it may clear. All four MUTATE or address a hold
		// by identifier; none of them answers "is this identifier reserved", which stays
		// sendReservedLocked's — the sixth liveness term reads a HOLD it was handed, exactly
		// as actionHeldLocked does, and is deliberately absent from this list.
		"mqttSession.excludeIdentifierLocked":        {},
		"mqttSession.liftIdentifierExclusionLocked":  {},
		"mqttSession.adoptIdentifierExclusionLocked": {},
		"mqttSession.slotExclusionInstLocked":        {},
		// The RECLAMATION pass, which ENUMERATES exclusions rather than asking about any
		// one identifier. "Is this identifier reserved" is still sendReservedLocked's and
		// is a different question from "which restrictions are left to re-test" — the pass
		// has no identifier in hand when it asks, which is precisely why it cannot go
		// through that API.
		//
		// TWO entries, and the second is the reason the first is only one: the re-enrolment
		// on restoration and the re-arm at the end of a pass are the SAME question, so they
		// share anyExclusionHeldLocked instead of walking the map twice.
		"mqttSession.anyExclusionHeldLocked":          {},
		"mqttSession.snapshotExclusionReclaimLocked_": {},
		// F-088 WP3 rule 6: the reclaim pass's fence-cursor write-back mutates the exclusion
		// object's SCHEDULING state; it answers no reservation question.
		"mqttSession.advanceExclusionFenceCursorLocked": {},
		// actionHeldLocked is deliberately ABSENT. It is the fourth liveness term and it
		// reads a HOLD it was handed, never the registry — the same reason
		// pubCompHeldLocked and retainedHeldLocked are absent. A term that took the map
		// itself would be answering "is this identifier reserved" in a second place, which
		// is exactly what sendReservedLocked exists to prevent.
	}
	// The property is BIDIRECTIONAL, in three parts, and only the first was ever checked:
	//
	//  1. every production access belongs to an allow-listed function;
	//  2. every allow-listed name matches a production declaration;
	//  3. every allow-listed function actually contains an access.
	//
	// Parts 2 and 3 are what make the list the EXACT current access set rather than a
	// wish-list. Part 2 caught three phantoms. Part 3 catches the slower failure: a
	// function that once reached the map, stopped, and left its exemption behind for a
	// later edit to walk into unreviewed.
	declared := map[string]bool{}
	accesses := map[string]int{}
	seen := 0
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
			qualified := testQualifiedFuncName(fn)
			declared[qualified] = true
			_, owner := owners[qualified]
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, _ := n.(*ast.SelectorExpr)
				if sel == nil || sel.Sel.Name != "sendsInFlight" {
					return true
				}
				seen++
				accesses[qualified]++
				if !owner {
					t.Errorf("%s: %s reaches into sendsInFlight directly; presence in that registry is not a "+
						"reservation, so a reservation reader must ask sendReservedLocked",
						fset.Position(sel.Pos()), qualified)
				}
				return true
			})
		}
	}
	if seen == 0 {
		t.Error("no sendsInFlight reference was found at all, so this guard checks nothing")
	}
	for name := range owners {
		switch {
		case !declared[name]:
			t.Errorf("the owner list exempts %q, which no production declaration matches: delete "+
				"the entry, or the next function to take that name inherits an unreviewed "+
				"exemption to touch sendsInFlight", name)
		case accesses[name] == 0:
			t.Errorf("the owner list exempts %q, which no longer touches sendsInFlight: delete the "+
				"entry, because an exemption nothing uses is one a later edit inherits without "+
				"review", name)
		}
	}
}

// testMQTTProductionFiles lists every non-test Go file in this package, so a guard
// cannot be evaded by putting the new site in a file the guard's list forgot.
func testMQTTProductionFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		out = append(out, name)
	}
	if len(out) == 0 {
		t.Fatal("no production files found, so every AST guard in this file checks nothing")
	}
	return out
}

// testQualifiedFuncName names a declaration as <receiver type>.<method>, or just the
// name for a plain function, so an allow-list cannot be satisfied by a same-named
// method on a different receiver.
func testQualifiedFuncName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	t := fn.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// testCalleeName reports the called function's own name, for both plain and method
// calls, and empty for anything else.
// testDefersSendEnd reports whether a defer statement ends a send section: a direct
// `defer endSend(...)`, or a deferred function literal whose body calls endPublishSend,
// endPubRelSend (F-116: the PUBREL end reads its emission flag at exit) or endSend.
func testDefersSendEnd(d *ast.DeferStmt) bool {
	switch testCalleeName(d.Call) {
	case "endSend", "endPublishSend", "endPubRelSend":
		return true
	}
	lit, _ := d.Call.Fun.(*ast.FuncLit)
	if lit == nil {
		return false
	}
	found := false
	ast.Inspect(lit.Body, func(n ast.Node) bool {
		if call, _ := n.(*ast.CallExpr); call != nil {
			switch testCalleeName(call) {
			case "endSend", "endPublishSend", "endPubRelSend":
				found = true
			}
		}
		return !found
	})
	return found
}

func testCalleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return _EMPTY_
}

// testResolvesToConst reports whether e names the constant want, either directly or
// through a simple local alias assigned from it inside body. The alias case is the point:
// `pt := mqttPacketPubRel; enqueue(pt, ...)` is the same site one indirection away, and a
// guard that matched only the bare identifier would miss it.
func testResolvesToConst(body *ast.BlockStmt, e ast.Expr, want string) bool {
	id, _ := e.(*ast.Ident)
	if id == nil {
		return false
	}
	if id.Name == want {
		return true
	}
	aliased := false
	ast.Inspect(body, func(n ast.Node) bool {
		as, _ := n.(*ast.AssignStmt)
		if as == nil {
			return true
		}
		for i, lhs := range as.Lhs {
			l, _ := lhs.(*ast.Ident)
			if l == nil || l.Name != id.Name || i >= len(as.Rhs) {
				continue
			}
			if r, _ := as.Rhs[i].(*ast.Ident); r != nil && r.Name == want {
				aliased = true
			}
		}
		return true
	})
	return aliased
}

// testIsLiteralZero reports whether e is the integer literal 0 — a QoS 0 enqueue, which
// allocates no identifier. An expression that merely evaluates to zero is deliberately
// NOT excluded: this guard reads syntax, and reading it as a skip would let a computed
// identifier slip past.
func testIsLiteralZero(e ast.Expr) bool {
	lit, _ := e.(*ast.BasicLit)
	return lit != nil && lit.Kind == token.INT && lit.Value == "0"
}

// A NAK is withheld from every entry a redelivery could not help, and each state is
// asserted on its own so deleting any one clause fails.
func TestMQTTInflightRedeliveryWithheldFromUnsendableEntries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		what  string
		entry *mqttPending
		want  bool
	}{
		{"a confirmed, idle entry", &mqttPending{kind: mqttPendingOrdinary, regSeq: 4}, true},
		{"an allocation whose register is not confirmed (R1)", &mqttPending{regPending: true}, false},
		{"a quarantined identifier", &mqttPending{kind: mqttPendingOrdinary, regSeq: 4, regForeign: true}, false},
		{"an entry whose transmission has not returned", &mqttPending{kind: mqttPendingOrdinary, regSeq: 4, sendNonce: "tok"}, false},
		{"a reservation that acts for nothing", &mqttPending{regSeq: 4}, false},
	} {
		if got := mqttPendingRedeliverable(tc.entry); got != tc.want {
			t.Errorf("%s: redeliverable=%v, want %v", tc.what, got, tc.want)
		}
	}
}

// A subscribe denial must not take a quarantined identifier: its register is another
// writer's, so freeing the entry would leave the durable custody unrepresented and let
// the source's next offer mint a second identifier.
func TestMQTTInflightDenialLeavesAQuarantineAlone(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-denial-quarantine")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	e.sess.pendingPublish[5] = &mqttPending{jsDur: dur, sseq: sseq, regSeq: 3, regForeign: true}
	e.sess.cpending = map[string]map[uint64]uint16{dur: {sseq: 5}}
	e.sess.mu.Unlock()

	e.sess.releaseTransmittedIdentifier(e.c, dur, testInflightAckSubject(dur, sseq, 1))

	e.sess.mu.Lock()
	held := e.sess.pendingPublish[5]
	mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if held == nil || !held.regForeign || mapped != 5 {
		t.Fatalf("a terminal denial must leave a quarantine standing: entry=%+v cpending=%d", held, mapped)
	}
}

// A same-delivery occupant that is RETIRED is adopted, never replaced: it proves this
// identifier's exchange for this very source is past PUBREC, so replacing it would erase
// the client's own PUBREC and re-send a completed payload — while abandoning the
// allocation would let the source's next offer take a second identifier.
func TestMQTTInflightTakeoverAdoptsARetiredOccupant(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-takeover-adopt")
	dur := e.sess.idHash + "_plain"
	sseq, reply := e.publishOfferedSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	// The occupant: same source, ANOTHER writer's nonce, and retired.
	occupantNonce := mqttNewInflightNonce()
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) {
		r.Retired, r.Nonce = true, occupantNonce
	})
	_, occupantMsg := testRegisterAt(t, e, pi)

	// Our own fresh allocation for the same source and identifier.
	n2 := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq, jsAckSubject: reply, regPending: true, regNonce: n2}
	e.sess.mu.Unlock()
	if e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, n2, _EMPTY_, _EMPTY_) {
		t.Fatal("an allocation meeting a retired occupant must not be cleared to send")
	}

	got, gotMsg := testRegisterAt(t, e, pi)
	if gotMsg.Sequence != occupantMsg.Sequence || got.Nonce != occupantNonce || !got.Retired {
		t.Fatalf("the retired occupant must be adopted, not rewritten: seq=%d nonce=%q retired=%v",
			gotMsg.Sequence, got.Nonce, got.Retired)
	}
	e.sess.mu.Lock()
	rs := e.sess.srcAckOwed[mqttDeferredKey{dur, sseq}]
	e.sess.mu.Unlock()
	if rs == nil || rs.nonce != occupantNonce || !rs.durable {
		t.Fatalf("the adoption must reinstate the OCCUPANT's suppression: %+v", rs)
	}

	// The point of installing the suppression rather than abandoning: the source's next
	// offer meets sourceRetiredLocked — which the delivery callback consults BEFORE
	// trackPublish — so it is acknowledged and discarded instead of allocating a second
	// identifier for a message the client has already completed.
	e.sess.mu.Lock()
	suppressed := e.sess.sourceRetiredLocked(dur, sseq)
	e.sess.mu.Unlock()
	if !suppressed {
		t.Fatal("the adopted retirement must answer the source's next offer, or it allocates a second identifier")
	}
}

// The adoption is GENERATION-FENCED: a discard that ran while the reconcile was outside
// the lock has minted a new incarnation, and a late install would write the retiring
// generation's suppression into the reused object.
func TestMQTTInflightTakeoverAdoptionIsGenerationFenced(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-takeover-fence")
	dur := e.sess.idHash + "_plain"
	sseq, reply := e.publishOfferedSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	occupantNonce := mqttNewInflightNonce()
	testMarkRegister(t, e, pi, func(r *mqttInflightRecord) {
		r.Retired, r.Nonce = true, occupantNonce
	})

	n2 := mqttNewInflightNonce()
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq, jsAckSubject: reply, regPending: true, regNonce: n2}
	e.sess.mu.Unlock()

	// The generation is re-minted while the store's CAS is in flight, which is exactly
	// the window a discard occupies.
	bumped := 0
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if bumped == 0 && r.Nonce == n2 {
			bumped++
			e.sess.mu.Lock()
			e.sess.gen = mqttNewInflightNonce()
			e.sess.mu.Unlock()
			return 0, fmt.Errorf("test: the store did not land")
		}
		return e.asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	e.asm.mu.Unlock()
	e.sess.storeInflightRegister(e.c, pi, 2, dur, sseq, n2, _EMPTY_, _EMPTY_)
	e.asm.mu.Lock()
	e.asm.testInflightStore = nil
	e.asm.mu.Unlock()
	if bumped != 1 {
		t.Fatalf("the re-mint window was never entered, bumped=%d", bumped)
	}

	e.sess.mu.Lock()
	rs := e.sess.srcAckOwed[mqttDeferredKey{dur, sseq}]
	e.sess.mu.Unlock()
	if rs != nil {
		t.Fatalf("the retiring generation's suppression was installed into the new incarnation: %+v", rs)
	}
}

// A session discard does not release an identifier whose write is still blocked. clear()
// nils every tracking map AND resets the counter to zero, so a reused session object
// would otherwise mint the same identifier for a new incarnation while the displaced
// connection's write is still in the socket.
func TestMQTTInflightSendSectionSurvivesTheDiscard(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-discard")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the entry must be reservable")
	}

	// A client is BOUND here, so the discard runs the production two-phase
	// retirement (detach before the re-mint) rather than a bare clear().
	if err := mqttTestRetireBound(t, e.asm, e.sess, false); err != nil {
		t.Fatalf("clear: %v", err)
	}

	e.sess.mu.Lock()
	e.sess.maxp = 100
	e.sess.pendingPublish = make(map[uint16]*mqttPending)
	minted := make(map[uint16]bool)
	for i := 0; i < int(pi)+2; i++ {
		next := e.sess.bumpPI()
		if next == 0 {
			break
		}
		minted[next] = true
		e.sess.pendingPublish[next] = &mqttPending{}
	}
	e.sess.mu.Unlock()
	if minted[pi] {
		t.Fatalf("the new incarnation minted packet identifier %d while the displaced connection's write was still out", pi)
	}

	e.sess.endSend(e.c, pi, token.token)
	e.sess.mu.Lock()
	_, stillReserved := e.sess.sendsInFlight[pi]
	e.sess.mu.Unlock()
	if stillReserved {
		t.Fatal("on nilled maps the registry removal IS the whole continuation, and it must still happen")
	}
}

// The verdict property is guarded STRUCTURALLY, over the source of every production file
// in this package: a register delete whose caller can name the exchange nonce AND can
// reach the verdict set must route through reapRegisterWithVerdict, so a NEW delete site
// fails the suite instead of silently rejoining the class that strands verdicts.
//
// The second clause is not decoration. The verdict set lives on a session, so a caller
// with none resident can consult nothing however well it knows the nonce — which is
// F-090, filed under docs/ROADMAP.md §P, and why one allow-list entry below reads as it
// does.
//
// Three separate reviews each found one more stranding delete site — the affine sweep,
// the store's stale-record reap, recovery's second retirement drop — which is what makes
// a list the wrong shape for this and an enforced boundary the right one. The allow-list
// below is that boundary made explicit: every entry is a site that CANNOT name a nonce
// (or IS the property's own implementation), and adding to it is a deliberate act a
// reviewer can see.
func TestMQTTInflightEveryNonceRecoverableDeleteRoutesThroughTheProperty(t *testing.T) {
	t.Parallel()
	// site -> why it is outside the property. Nonce-UNRECOVERABLE deletes only.
	//
	// THREE shapes are outside it. A site that CANNOT name a nonce. A site that
	// implements the same discipline with a STRONGER ladder of its own — confirmed
	// delete, else a tombstone on the register, else the nonce verdict — which is not a
	// bypass but the full version of what the helper does in one step. And a site that
	// can name the nonce but cannot REACH the verdict set, which lives on a session:
	// the orphan sweep with none resident, which is F-090.
	allowed := map[string]string{
		"reapRegisterWithVerdict":          "the property's own implementation",
		"deleteInflightRegister":           "the fire-and-forget transport primitive the property calls",
		"deleteInflightRegisterConfirmed":  "the confirmed transport primitive the property calls",
		"deleteInflightRegistersPipelined": "the pipelined transport primitive over sequences already collected",
		"voidRegistersOnStream":            "the teardown's own three-rung ladder: confirmed delete then tombstone then verdict, forgetting on each proven rung",
		"confirmInflightRegister":          "the cancelled-allocation settle, which runs that same three-rung ladder for one register",
		"reapIncarnationRegisters":         "the generation reap: the whole incarnation goes, verdicts with it",
		"deleteDiscardRecord": "a DISCARDED generation's census delete (F-105 7d): the same generation-wide " +
			"retirement as the reap, after the closure — no session of that generation serves, so no verdict " +
			"set exists to strand; it takes only a register whose lineage is the discarded obligation's nonce, " +
			"by its exact sequence",
		"reconcileOrphanInflight": "the account-wide sweep's candidate router, which holds no session to consult",
		"deleteRetiredPositionsUpTo": "removes a duty entry the F-112 floor RETIRED, never a register. A duty subject " +
			"carries the f105 class token, which no register parser can place and no cancellation verdict can " +
			"name; the entry is LOADED and decoded before the delete, by the exact sequence read",
		"deleteReclaimedExchange": "removes an exchange record the F-112 floor RECLAIMED, never a register. An " +
			"exchange subject carries the f105 class token, which no register parser can place and no " +
			"cancellation verdict can name; it is read first and deleted by the exact sequence read, and only " +
			"once the register subject no longer names its lineage (the reclamation predicate)",
		"deleteInflightMarker": "the UNSENT MARKER's exact-sequence delete (F-072). A marker carries no cancellation " +
			"verdict — a verdict names the REGISTER's nonce, and the register's own delete routes through the " +
			"property — so there is nothing here to strand; the sequence comes only from an observation the " +
			"marker boundary admitted",
		"deleteChunkInterval": "removes F-105 retained MATERIAL, never a register. A chunk subject carries the " +
			"f105 class token, which satisfies neither mqttValidIDHashToken nor mqttValidSessGen, so no " +
			"register parser can place it and no cancellation verdict can name it. The record is LOADED and " +
			"its materialization checked before the delete, so what is taken is identified rather than " +
			"addressed",
		"finishMaterialRemoval": "removes the F-105 retained MANIFEST, by the EXACT sequence its caller was " +
			"authorized for, and only once every chunk is CONFIRMED removed. Same class as the chunks: not a " +
			"register, so there is no verdict to strand",
		"disposeRetainedRel": "removes a RETAINED exchange's CANONICAL PUBREL record (`rel`, F-105 7c), never a " +
			"register. A rel subject carries the f105 class token, which satisfies neither mqttValidIDHashToken " +
			"nor mqttValidSessGen, so no register parser can place it and no cancellation verdict can name it. " +
			"It is LOADED and its identity (the register's rel_id, the identifier) checked before the delete, " +
			"by the exact sequence read, once its exchange is terminal at PUBCOMP. Same class as the chunks " +
			"and the manifest",
		"retireRecoveryRoot": "removes an F-105 RECOVERY ROOT, never a register, by the EXACT sequence it was " +
			"read at and only after its completion certificate is CONFIRMED to stand. A root subject carries " +
			"the f105 class token, which satisfies neither mqttValidIDHashToken nor mqttValidSessGen, so no " +
			"register parser can place it and no cancellation verdict can name it — there is nothing here to " +
			"strand. Same class as the retained material's chunk and manifest deletes",
		"disposeSettledAffineRegister": "routes through the property where an identity-fenced session is resident, " +
			"and RETAINS a register that CARRIES a cancellation where none is. A verdict living only in a " +
			"non-resident session's persisted CancelledRegs is invisible from there and is NOT covered: F-090",
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		checked++
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			ast.Inspect(fn.Body, func(inner ast.Node) bool {
				call, ok := inner.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "deleteInflightRegister", "deleteInflightRegisterConfirmed":
				case "deleteMsg":
					// Only the register stream's deletes are in this class.
					if len(call.Args) == 0 {
						return true
					}
					id, ok := call.Args[0].(*ast.Ident)
					if !ok || id.Name != "mqttInflightStreamName" {
						return true
					}
				default:
					return true
				}
				if _, ok := allowed[fn.Name.Name]; !ok {
					t.Errorf("%s: %s deletes a register directly. If it can name the exchange nonce it must "+
						"route through reapRegisterWithVerdict, or a cancellation verdict naming that register "+
						"is stranded for the life of the session; if it genuinely cannot, add it to the "+
						"allow-list with the reason", fset.Position(call.Pos()), fn.Name.Name)
				}
				return true
			})
			return false
		})
	}
	if checked < 5 {
		t.Fatalf("only %d production files were parsed; this guard would pass vacuously", checked)
	}
}

// beginSendLocked refuses every entry state that must not be transmitted.
func TestMQTTInflightBeginSendRefusesUnsendableEntries(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-send-refuse")
	dur := e.sess.idHash + "_plain"
	e.sess.mu.Lock()
	e.sess.c = e.c
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	defer e.sess.mu.Unlock()
	for _, tc := range []struct {
		what  string
		entry *mqttPending
	}{
		{"an allocation whose register is not confirmed (R1)", &mqttPending{jsDur: dur, sseq: 1, regPending: true}},
		{"a withdrawn allocation", &mqttPending{jsDur: dur, sseq: 1, regCancelled: true}},
		{"a quarantined identifier", &mqttPending{jsDur: dur, sseq: 1, regForeign: true}},
		{"an entry whose removal is already parked", &mqttPending{
			jsDur: dur, sseq: 1, sendSettle: mqttSendSettle{kind: mqttSendSettleAckComplete},
		}},
	} {
		e.sess.pendingPublish[9] = tc.entry
		if _, ok := e.sess.beginSendLocked(e.c, 9, dur, 1); ok {
			t.Errorf("a send section was granted over %s", tc.what)
		}
		delete(e.sess.sendsInFlight, 9)
	}
}

// ---------------------------------------------------------------------------
// F-089: a PUBREL transmission holds its identifier until its PUBCOMP
// ---------------------------------------------------------------------------

// testConnIdentity gives a fixture connection a REAL identity, minted the way an
// accepted connection's is. testNewInflightEnv builds its client with
// createInternalAccountClient, which never calls initClient, so its cid is zero — and
// an ownership assertion written against zero compares a default with a default and
// passes whatever the code does.
func testConnIdentity(t *testing.T, e *testInflightEnv, c *client) *client {
	t.Helper()
	c.cid = e.s.gcid.Add(1)
	if c.cid == 0 {
		t.Fatal("a fixture connection must have a nonzero identity, or every ownership assertion is vacuous")
	}
	// AND ITS OUTPUT IDENTITY, assigned beside the cid exactly as a real connection's is: every
	// ordinary acquisition names the connection that will emit it (F-105 row 2).
	if c.outBind == nil {
		c.outBind = mqttBindConnection(c.cid)
	}
	return c
}

// testInflightWire is a socket that accepts every byte, for a fixture whose PUBRELs must really
// be transmitted. testNewInflightEnv's client has none, so its authorized writes are all
// PREVENTED — and since F-116 a prevented PUBREL owes no PUBCOMP, so a fixture asserting the debt
// of a "transmitted" copy, or the released delivery that debt makes, must first transmit it.
type testInflightWire struct {
	net.Conn
	mu      sync.Mutex
	written []byte
}

func (w *testInflightWire) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written = append(w.written, b...)
	return len(b), nil
}
func (w *testInflightWire) Close() error                     { return nil }
func (w *testInflightWire) SetWriteDeadline(time.Time) error { return nil }

// testWireInflightEnv gives e's connection its identity and a socket, so its PUBRELs reach the
// wire and incur the PUBCOMP debt the client then answers.
func testWireInflightEnv(t *testing.T, e *testInflightEnv) *testInflightWire {
	t.Helper()
	if e.c.cid == 0 {
		testConnIdentity(t, e, e.c)
	}
	w := &testInflightWire{}
	e.c.mu.Lock()
	e.c.nc, e.c.rawConn = w, w
	e.c.mu.Unlock()
	return w
}

// testPubRelHold reads a hold's raw fields, which several assertions below need at an
// instant rather than through the liveness predicate.
func testPubRelHold(t *testing.T, sess *mqttSession, pi uint16) (token string, owed int, owner uint64, present bool) {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	h := sess.sendsInFlight[pi]
	if h == nil {
		return _EMPTY_, 0, 0, false
	}
	return h.token, h.pubCompOwed, h.owner, true
}

// testMintsPI reports whether bumpPI will hand out want, with the maps cleared so the
// registry is the only thing that can refuse it. This is the discard shape: clear()
// resets last_pi to zero, so a reused object walks the space from the bottom.
func testMintsPI(t *testing.T, sess *mqttSession, want uint16) bool {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	savedPub, savedRel, savedCp, savedLast := sess.pendingPublish, sess.pendingPubRel, sess.cpending, sess.last_pi
	sess.pendingPublish = make(map[uint16]*mqttPending)
	sess.pendingPubRel = make(map[uint16]*mqttPending)
	sess.cpending = make(map[string]map[uint64]uint16)
	sess.last_pi = 0
	defer func() {
		sess.pendingPublish, sess.pendingPubRel, sess.cpending, sess.last_pi = savedPub, savedRel, savedCp, savedLast
	}()
	for i := 0; i <= int(want)+1; i++ {
		next := sess.bumpPI()
		if next == 0 {
			break
		}
		if next == want {
			return true
		}
		sess.pendingPublish[next] = &mqttPending{}
	}
	return false
}

// A PUBREL transmission reserves its identifier, and the reservation OUTLIVES the
// write: [MQTT-2.3.1-4] frees a PUBREL's identifier when its PUBCOMP is processed, not
// when the bytes leave.
func TestMQTTInflightPubRelSendHoldsTheIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-pubrel-hold")
	const pi = uint16(7)

	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	token, ok := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !ok || token == _EMPTY_ {
		t.Fatal("a PUBREL transmission on this session's connection must be reservable")
	}

	// A second transmission under the same identifier is refused while the first write
	// is out: two PUBRELs in the socket under one identifier is what produces the stray
	// PUBCOMP this mechanism exists to prevent.
	e.sess.mu.Lock()
	_, twice := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if twice {
		t.Fatal("a second PUBREL section must be refused while the first write has not returned")
	}

	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was re-minted while its PUBREL write was still in flight", pi)
	}

	// The write returns. The identifier is STILL held, because the client has not
	// answered — this is the assertion that separates this design from a plain
	// write-return section.
	e.sess.endSend(e.c, pi, token)
	if tok, owed, _, present := testPubRelHold(t, e.sess, pi); !present || tok != _EMPTY_ || owed != 1 {
		t.Fatalf("after endSend the hold must survive with the debt intact: token=%q owed=%d present=%v", tok, owed, present)
	}
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was re-minted while its PUBCOMP was still owed", pi)
	}

	// And the PUBCOMP releases it.
	e.sess.mu.Lock()
	e.sess.consumePubRelAckLocked(e.c, pi)
	e.sess.mu.Unlock()
	if _, _, _, present := testPubRelHold(t, e.sess, pi); present {
		t.Fatal("the PUBCOMP must release the hold, or the identifier is held for the connection's life")
	}
	if !testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d must be reusable once its PUBCOMP was processed", pi)
	}
}

// Two transmitted copies owe two PUBCOMPs. The first releases nothing: the client is
// still going to answer the duplicate, and that answer must not land on a re-minted
// exchange.
func TestMQTTInflightPubCompFreesOnlyTheLastOwedPubRel(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-pubrel-two-copies")
	const pi = uint16(9)

	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	tok1, ok1 := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !ok1 {
		t.Fatal("the first copy must be reservable")
	}
	e.sess.endSend(e.c, pi, tok1)

	e.sess.mu.Lock()
	tok2, ok2 := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !ok2 {
		t.Fatal("a redelivery must be transmittable once the previous write has returned")
	}
	e.sess.endSend(e.c, pi, tok2)

	if _, owed, _, _ := testPubRelHold(t, e.sess, pi); owed != 2 {
		t.Fatalf("two transmitted copies owe two PUBCOMPs, got %d", owed)
	}

	e.sess.mu.Lock()
	e.sess.consumePubRelAckLocked(e.c, pi)
	e.sess.mu.Unlock()
	if _, owed, _, present := testPubRelHold(t, e.sess, pi); !present || owed != 1 {
		t.Fatalf("the first PUBCOMP must leave the duplicate's debt standing: owed=%d present=%v", owed, present)
	}
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was re-minted while a second PUBCOMP was still owed", pi)
	}

	e.sess.mu.Lock()
	e.sess.consumePubRelAckLocked(e.c, pi)
	e.sess.mu.Unlock()
	if _, _, _, present := testPubRelHold(t, e.sess, pi); present {
		t.Fatal("the last owed PUBCOMP must release the hold")
	}
}

// A ZERO connection identity is not a connection identity. Every accepted connection
// takes its cid from srv.gcid.Add(1), so zero means uninitialized — and two
// uninitialized connections would be indistinguishable, letting a foreign PUBCOMP
// consume this debt or letting a debt stay live across a rebind. The refusal keeps that
// case out of the predicate entirely, so no hold can ever carry a zero owner.
func TestMQTTInflightAZeroIdentityIsNotAConnectionIdentity(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-zero-identity")
	const pi = uint16(14)
	zero := &client{} // cid 0, as createInternalAccountClient leaves it

	e.sess.mu.Lock()
	e.sess.c = zero
	_, ok := e.sess.beginPubRelSendLocked(zero, pi)
	e.sess.mu.Unlock()
	if ok {
		t.Fatal("a connection with no identity must not be recorded as a debt's owner")
	}
	if _, _, _, present := testPubRelHold(t, e.sess, pi); present {
		t.Fatal("the refusal must leave no hold behind")
	}

	// And a hold that somehow carried a zero owner reserves nothing, even while the
	// session's own connection is equally identity-less: the predicate must not read
	// two defaults as a match.
	e.sess.mu.Lock()
	e.sess.sendsInFlight = map[uint16]*mqttSendHold{pi: {pubCompOwed: 1, owner: 0}}
	live := e.sess.sendReservedLocked(pi)
	e.sess.consumePubRelAckLocked(zero, pi)
	owed := e.sess.sendsInFlight[pi].pubCompOwed
	e.sess.mu.Unlock()
	if live {
		t.Fatal("a zero owner must never satisfy the liveness predicate: 0 == 0 is not ownership")
	}
	if owed != 1 {
		t.Fatalf("a zero-identity PUBCOMP must consume nothing, got owed=%d", owed)
	}
}

// A PUBCOMP that is not the hold owner's consumes nothing. It answers a transmission
// this connection never made, and spending the debt on it would release an identifier
// the owning connection's client may still hold.
func TestMQTTInflightAForeignPubCompConsumesNoDebt(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-foreign-pubcomp")
	const pi = uint16(4)
	other := testConnIdentity(t, e, &client{})

	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	tok, ok := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the transmission must be reservable")
	}
	e.sess.endSend(e.c, pi, tok)

	e.sess.mu.Lock()
	e.sess.consumePubRelAckLocked(other, pi)
	e.sess.mu.Unlock()

	_, owed, owner, present := testPubRelHold(t, e.sess, pi)
	if !present || owed != 1 || owner != e.c.cid {
		t.Fatalf("a foreign PUBCOMP must consume nothing: owed=%d ownerIsOurs=%v present=%v",
			owed, owner == e.c.cid, present)
	}
}

// A PUBREL may not be transmitted while a PUBLISH write under the same identifier is
// still out — the successful-PUBREC case, where the PUBREL is ready while the PUBLISH
// bytes have not left. And a live PUBREL debt likewise refuses a PUBLISH section.
func TestMQTTInflightPubRelSectionRefusesUnderALivePublishWrite(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-pubrel-vs-publish")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)

	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	pubTok, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the PUBLISH entry must be reservable")
	}
	e.sess.mu.Lock()
	_, relOK := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if relOK {
		t.Fatal("a PUBREL must not be written under an identifier whose PUBLISH write has not returned")
	}
	e.sess.endSend(e.c, pi, pubTok.token)

	// UNCHANGED by F-091, and deliberately so: F-091 leaves ALL PUBREL admission exactly as
	// it was. Three candidate refusals were tried there and all three were deleted (D4), so
	// what this test exercises is the pre-existing rule alone — an active write token refuses,
	// nothing else does. Correlating a stored PUBREL record against the entry that owns its
	// identifier is F-092's, not this item's.
	e.sess.mu.Lock()
	relTok, ok := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the PUBREL must be reservable once the PUBLISH write returned")
	}
	e.sess.endSend(e.c, pi, relTok)
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: sseq}
	_, pubAgain := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if pubAgain {
		t.Fatal("a PUBLISH must not be transmitted under an identifier whose PUBCOMP is still owed")
	}
}

// A debt owed to a connection the session no longer has can never be answered, so it
// must stop holding the identifier at once — and the next connection's retransmission
// must RESET it rather than add to it, or one PUBCOMP can never clear a count of two.
//
// This is the persistent-session shape: the close path nils sess.c and calls clear()
// only when the expiry is zero, so a surviving session reaches the next connection with
// the debt still recorded.
func TestMQTTInflightPersistentDisconnectReleasesADeadPubCompDebt(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-dead-debt")
	const pi = uint16(3)

	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	tok, ok := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the transmission must be reservable")
	}
	e.sess.endSend(e.c, pi, tok)

	// The connection goes, the session stays. No clear() runs on this path.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()
	if !testMintsPI(t, e.sess, pi) {
		t.Fatalf("a debt owed to a departed connection must not hold packet identifier %d", pi)
	}

	// The next connection retransmits the same PUBREL and takes the debt over.
	next := testConnIdentity(t, e, &client{})
	e.sess.mu.Lock()
	e.sess.c = next
	tok2, ok2 := e.sess.beginPubRelSendLocked(next, pi)
	e.sess.mu.Unlock()
	if !ok2 {
		t.Fatal("the resumed connection must be able to retransmit the PUBREL")
	}
	e.sess.endSend(next, pi, tok2)
	if _, owed, owner, _ := testPubRelHold(t, e.sess, pi); owed != 1 || owner != next.cid {
		t.Fatalf("the retransmission must RESET the dead count and take ownership, got owed=%d ownerIsNext=%v",
			owed, owner == next.cid)
	}

	// One PUBCOMP is all the client owes on this connection, and it must be enough.
	e.sess.mu.Lock()
	e.sess.consumePubRelAckLocked(next, pi)
	e.sess.mu.Unlock()
	if _, _, _, present := testPubRelHold(t, e.sess, pi); present {
		t.Fatal("one PUBCOMP must release an identifier the resumed connection transmitted once")
	}
}

// The REBIND retires a debt, not clear(). clear() does not touch sess.c, and on the
// clean-start path it runs while the displaced client is still bound — so a debt
// dropped there would free an identifier that client is still holding.
func TestMQTTInflightRebindNotClearIsWhatRetiresAPubCompDebt(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-rebind-retires")
	const pi = uint16(6)

	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	tok, ok := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("the transmission must be reservable")
	}
	e.sess.endSend(e.c, pi, tok)

	// A client is BOUND here, so the discard runs the production two-phase
	// retirement (detach before the re-mint) rather than a bare clear().
	if err := mqttTestRetireBound(t, e.asm, e.sess, true); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, owed, _, present := testPubRelHold(t, e.sess, pi); !present || owed != 1 {
		t.Fatalf("clear() must not retire a debt of the still-bound connection: owed=%d present=%v", owed, present)
	}
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was freed while the client that owes its PUBCOMP is still bound", pi)
	}

	// The rebind is the event. From here the old client's PUBCOMP can never be
	// processed, so its debt reserves nothing.
	e.sess.mu.Lock()
	e.sess.c = testConnIdentity(t, e, &client{})
	e.sess.mu.Unlock()
	if !testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d must be free once the connection that owed its PUBCOMP is gone", pi)
	}
}

// An inert hold is TAKEN OVER by a PUBLISH section, never refused. Refusing it strands
// the identifier: bumpPI hands it out, trackPublish installs the entry and its
// register, and nothing can ever transmit under it.
func TestMQTTInflightAnInertHoldIsTakenOverNotRefused(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-inert-takeover")
	dur := e.sess.idHash + "_plain"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)

	// A debt from a connection that is gone: present in the registry, reserving nothing.
	e.sess.mu.Lock()
	e.sess.sendsInFlight = map[uint16]*mqttSendHold{pi: {pubCompOwed: 1, owner: testConnIdentity(t, e, &client{}).cid}}
	e.sess.c = e.c
	e.sess.mu.Unlock()

	e.sess.mu.Lock()
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	e.sess.mu.Unlock()
	if !ok {
		t.Fatal("an inert hold must be taken over, not refused: refusing strands the identifier, " +
			"its register and a Receive Maximum slot for the session's life")
	}
	if tok, owed, _, _ := testPubRelHold(t, e.sess, pi); tok != token.token || owed != 0 {
		t.Fatalf("the takeover must replace the inert hold: token matches=%v owed=%d", tok == token.token, owed)
	}
	e.sess.endSend(e.c, pi, token.token)
	if _, _, _, present := testPubRelHold(t, e.sess, pi); present {
		t.Fatal("the taken-over hold must be released when its own write returns")
	}
}

// clear()'s sweep is hygiene: it drops holds that reserve nothing and leaves every one
// that does. A hold whose write has not returned survives the discard, which is what
// keeps a displaced connection's blocked write from colliding with a re-minted
// identifier.
func TestMQTTInflightClearSweepsOnlyDeadHolds(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-clear-sweep")

	gone := testConnIdentity(t, e, &client{}).cid
	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.sendsInFlight = map[uint16]*mqttSendHold{
		11: {pubCompOwed: 1, owner: gone},    // inert: a real identity that is not ours
		12: {token: "live-write"},            // write still out
		13: {pubCompOwed: 1, owner: e.c.cid}, // owed by us
	}
	e.sess.mu.Unlock()

	// A client is BOUND here, so the discard runs the production two-phase
	// retirement (detach before the re-mint) rather than a bare clear().
	if err := mqttTestRetireBound(t, e.asm, e.sess, true); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, _, _, present := testPubRelHold(t, e.sess, 11); present {
		t.Fatal("a hold reserving nothing must be swept")
	}
	if tok, _, _, present := testPubRelHold(t, e.sess, 12); !present || tok != "live-write" {
		t.Fatal("a hold whose write has not returned must survive the discard (F-087 WP5 class F2)")
	}
	if _, owed, _, present := testPubRelHold(t, e.sess, 13); !present || owed != 1 {
		t.Fatal("a debt of the still-bound connection must survive clear(): sess.c is untouched here")
	}
}

// testReadyForPubRelDelivery wires the session so the delivery callback and the PUBCOMP
// handler both run: a bound connection, a PUBREL consumer, and no gate withholding.
// testPersistAcceptedRecord gives the fixture the durable precondition PRODUCTION always
// has by the time a stored PUBREL can be delivered: an accepted session record at the head
// of $MQTT_sess, whose stream sequence and write identity are the ones this session object
// carries.
//
// It is not scaffolding for the assertion under test — it is the state an accepted CONNECT
// leaves behind. The accept save runs before recovery and long before any PUBREL consumer
// exists, so a delivery callback in production never runs against a session with no record.
// A fixture that skipped it was exercising a state the broker cannot be in, and the
// acceptance fence (which refuses to transmit unless the head record is this incarnation's
// own acknowledged write) would correctly refuse every transmission in it.
func testPersistAcceptedRecord(t *testing.T, e *testInflightEnv) {
	t.Helper()
	tok := e.sess.beginRecordTx()
	if err := e.sess.save(tok); err != nil {
		t.Fatalf("persisting the fixture's accepted session record: %v", err)
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	// Proving it here rather than trusting the save keeps a later change to the record
	// shape from silently turning every PUBREL fixture into a refusal test.
	if _, out := e.sess.proveAcceptedIncarnation(e.c); out != mqttAcceptanceCurrent {
		t.Fatalf("the fixture's session record is not provably current (%v); every slot-backed "+
			"PUBREL transmission would be fenced off and the tests would pass for the wrong reason", out)
	}
}

func testReadyForPubRelDelivery(t *testing.T, e *testInflightEnv) {
	t.Helper()
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.pubRelConsumer = &ConsumerConfig{Durable: "PUBREL_F089"}
	e.sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)

	// Terminal cleanup now requires the source side to be provably complete for every
	// past-PUBREC slot, not only for records whose Retired bit happened to land before a
	// crash. Give the ordinary callback fixtures that proof: SRC exists, but the seeded
	// source sequence does not. A test that needs an unfinished source stores one matching
	// message and rewrites its slot to the returned sequence.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			DeliverSubject: "f092.source.delivery",
			Durable:        "SRC",
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  mqttStreamSubjectPrefix + "f092.source",
		},
	}); err != nil {
		t.Fatalf("creating the source consumer: %v", err)
	}

	// AND THE DURABLE PRECONDITION ADMISSION NOW REQUIRES. A stored slot-backed PUBREL is
	// admitted because the slot that owns its identifier hands out a non-revocable action —
	// whether the slot is already past PUBREC (`pubrel_ready`) or still in `publish` (the
	// direct `publish -> pubrel_sending` arrow from a proven record) — not because the
	// callback was reached. (A live retained exchange has no slot, R6, and is admitted by
	// its in-memory owner instead.) So a fixture that drives the callback for a slot-backed
	// exchange must establish that slot, or it exercises the refusal path and nothing else
	// (feedback_fixture_must_reach_its_route).
	//
	// One `pubrel_ready` slot per identifier, keyed by subject, carrying the same exchange
	// nonce testDeliverPubRel puts in the record's owner header. The store is a map so the
	// action CAS can actually land: an immutable stub would make every acquisition fail and
	// the fixture would pass for the wrong reason.
	// The store assigns MONOTONIC sequences and the delete addresses BY SEQUENCE, exactly
	// as JetStream does. A fixture that returned a constant sequence and swallowed deletes
	// would make every freshness fence trivially satisfied and every terminal deletion a
	// silent no-op — so the slot would appear to leak and the test would blame the broker.
	e.asm.mu.Lock()
	slots := map[string]*mqttInflightRecord{}
	bySeq := map[uint64]string{}
	seqOf := map[string]uint64{}
	var next uint64
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		e.asm.mu.RLock()
		defer e.asm.mu.RUnlock()
		rec := slots[subject]
		if rec == nil {
			return nil, nil, nil
		}
		cp := *rec
		return &cp, &StoredMsg{Subject: subject, Sequence: seqOf[subject]}, nil
	}
	e.asm.testInflightStore = func(subject string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		e.asm.mu.Lock()
		defer e.asm.mu.Unlock()
		// A per-subject CAS, so a stale base loses rather than overwriting — which is what
		// makes the arm-before-CAS and redispatch paths testable at all.
		if cur, ok := seqOf[subject]; ok && expect != 0 && expect != cur {
			return 0, fmt.Errorf("test: in-flight CAS rejected for %q", subject)
		}
		next++
		cp := *r
		slots[subject] = &cp
		delete(bySeq, seqOf[subject])
		seqOf[subject] = next
		bySeq[next] = subject
		return next, nil
	}
	e.asm.testInflightDelete = func(seq uint64) error {
		e.asm.mu.Lock()
		defer e.asm.mu.Unlock()
		if subject, ok := bySeq[seq]; ok {
			delete(slots, subject)
			delete(seqOf, subject)
			delete(bySeq, seq)
		}
		return nil
	}
	e.asm.mu.Unlock()
	e.pubRelSlots = slots
}

// testDeliverPubRelAs delivers a stored PUBREL naming an EXPLICIT exchange nonce, which is
// what lets a test distinguish "this record belongs to the identifier's exchange" from
// "this record is stale" — the whole discriminator F-092 rests on.
func testDeliverPubRelAs(t *testing.T, e *testInflightEnv, pi uint16, reply, nonce string) {
	t.Helper()
	testDeliverPubRelCorrelated(t, e, pi, reply, nonce, "SRC", 41)
}

// testDeliverPubRelCorrelated delivers a stored PUBREL with an EXPLICIT source correlation.
//
// The correlation matters more than it looks: a record with an empty one takes a different
// route through admission, and F-092's holistic review round 1 found that every acceptance
// fixture used the empty form — so none of them ever reached the ORDINARY correlated path,
// which was at that moment bypassing the slot entirely. A fixture that never reaches the
// route it names proves nothing about it (feedback_fixture_must_reach_its_route).
func testDeliverPubRelCorrelated(t *testing.T, e *testInflightEnv, pi uint16, reply, nonce,
	srcDur string, srcSeq uint64) {
	t.Helper()
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, nonce, srcDur, srcSeq, _EMPTY_, _EMPTY_, _EMPTY_)
	e.c.pa.hdr = hdrLen
	defer func() { e.c.pa.hdr = 0 }()
	mqttDeliverPubRelCb(&subscription{client: e.c}, e.c, nil, e.sess.pubRelSubject, reply, msg)
}

// testCountPubRelWrites counts the PUBREL transmissions fn causes.
//
// Counted at the write block hook, which sits INSIDE the send section, so it observes an
// actual transmission rather than a decision to transmit. That distinction is what makes
// the F-092 assertions meaningful: a refusal must produce no bytes, and "no bytes" is only
// observable here.
func testCountPubRelWrites(t *testing.T, e *testInflightEnv, fn func()) int {
	t.Helper()
	n := 0
	restore := setMQTTTestPubRelWriteBlock(func() { n++ })
	defer restore()
	fn()
	return n
}

// testCanonicalPubRelSeq is the $MQTT_out sequence the seeded slot names as CANONICAL.
//
// Fixtures must deliver at this sequence, because admission treats a valid same-nonce
// record at any OTHER sequence as a duplicate — correctly: the slot names exactly one
// canonical record, and a duplicate is discharged and acknowledged rather than transmitted.
// A fixture that delivered at an arbitrary sequence therefore exercised duplicate disposal
// while claiming to test transmission.
const testCanonicalPubRelSeq = uint64(7)

// testSeedPubRelReadySlot installs the `pubrel_ready` slot for one identifier, so the
// delivery callback's admission can acquire an action on it.
func testSeedPubRelReadySlot(t *testing.T, e *testInflightEnv, pi uint16) {
	t.Helper()
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	// The session's OWN claim, not a literal: admission proves custody by comparing the
	// slot's Claim against this object's stable regClaim, so a fixture seeding anything
	// else exercises the refusal path and nothing else.
	claim := e.sess.regClaim
	e.sess.mu.Unlock()
	// NO asm.mu here: the store hook takes it itself, and holding it across the call
	// deadlocks the fixture.
	if e.pubRelSlots == nil {
		t.Fatal("testReadyForPubRelDelivery must run first: there is no slot store to seed")
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := freshRegisterLineage(&mqttInflightRecord{
		PI: pi, Dur: "SRC", Seq: 41, QoS: 2,
		Nonce: "xxxxxxxxxxxxxxxxxxxxxx",
		State: mqttInflightPubRelReady,
		// Custody is mandatory past PUBREC, and the canonical record identity with it.
		Claim: claim, ClaimSeq: 1,
		PubRelDurable: "PUBREL_F089", PubRelStreamSeq: testCanonicalPubRelSeq,
	})
	// Seeded THROUGH the store hook, so the slot gets a real sequence in the same space
	// deletes address. Writing straight into the map would leave it unaddressable and the
	// terminal deletion would silently no-op.
	if _, err := e.asm.testInflightStore(subject, 0, rec); err != nil {
		t.Fatalf("seeding the slot for packet identifier %d: %v", pi, err)
	}
}

// EXECUTION 1, through the production route, with the duplicate's write GENUINELY
// BLOCKED inside its own send section. A unit call on the primitive cannot see a wiring
// defect in this callback, and a synchronous call cannot see the interleaving at all:
// the whole finding is a PUBCOMP arriving on the read loop while a duplicate PUBREL is
// mid-transmission on the JetStream dispatcher.
func TestMQTTInflightPubCompDuringABlockedPubRelWrite(t *testing.T) {
	// Deliberately NOT t.Parallel(): setMQTTTestPubRelWriteBlock assigns a
	// process-global atomic, so a parallel sibling driving any PUBREL delivery would
	// block in this test's hook. TestParallelTestsTouchNoProcessGlobalState enforces
	// exactly that, and it is the reason this seam costs a serial test rather than
	// silently corrupting whatever runs beside it.
	e := testNewInflightEnv(t, "client-pubcomp-blocked")
	// ON THE REAL STORE (consult 26): an ordinary exchange's PUBCOMP is one authority batch.
	testReadyForRealPubRelDelivery(t, e)
	testWireInflightEnv(t, e) // its PUBRELs are transmitted, so their PUBCOMPs are owed (F-116)
	seed := testSeedRealPubRelReady(t, e)
	pi := seed.pi

	// Copy 1 goes out and its write returns; the client has not answered it yet.
	testDeliverPubRelAs(t, e, pi, seed.reply(1), seed.nonce)
	if _, owed, _, _ := testPubRelHold(t, e.sess, pi); owed != 1 {
		t.Fatalf("the delivery callback must reserve the identifier: owed=%d", owed)
	}

	// Copy 2 — the AckWait redelivery — goes through the SAME callback on another
	// goroutine and blocks inside its send section, exactly where the socket write is.
	blocked, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	restore := setMQTTTestPubRelWriteBlock(func() {
		close(blocked)
		<-release
	})
	// The seam is torn down LAST and the write is released FIRST, so a t.Fatal below
	// cannot leave the delivery goroutine parked in a hook that outlives this test —
	// a late call from a leaked goroutine would fail some other test's run, not this
	// one.
	defer restore()
	defer func() { unblock(); <-done }()
	go func() {
		defer close(done)
		testDeliverPubRelAs(t, e, pi, seed.reply(2), seed.nonce)
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the duplicate never reached its send section, so this fixture never blocked a write")
	}
	// The seam fires INSIDE the section, which is the premise every assertion below
	// rests on. Asserted rather than assumed: a hook that moved above the reservation
	// would leave this test green while proving nothing.
	if tok, _, _, _ := testPubRelHold(t, e.sess, pi); tok == _EMPTY_ {
		t.Fatal("the write must block INSIDE the send section, or the reservation is not under test")
	}

	// The PUBCOMP for copy 1 arrives on the read loop, through the real handler, while
	// the duplicate's write is still out.
	e.c.mqttProcessPubComp(pi)

	// (a) the exchange completed: the entry is gone and nothing was deferred.
	e.sess.mu.Lock()
	_, stillTracked := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if stillTracked {
		t.Fatal("the PUBCOMP must complete the delivered entry; the owner slot/action now carries any remaining drain")
	}
	// (b) the identifier is NOT available while the duplicate's bytes are out.
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was re-minted while a duplicate PUBREL write was still in flight", pi)
	}
	// (c) the raw debt is exactly 1 AT THIS INSTANT. This is what distinguishes a debt
	// incurred at the reservation from one incurred after the write returns: a late
	// increment reaches the same final state but leaves this window open.
	if _, owed, _, _ := testPubRelHold(t, e.sess, pi); owed != 1 {
		t.Fatalf("the duplicate's debt must already be recorded while its write is out, got owed=%d", owed)
	}

	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocked delivery never returned")
	}
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d must stay held until the duplicate's own PUBCOMP arrives", pi)
	}
	e.c.mqttProcessPubComp(pi)
	if !testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d must be free once every transmitted copy was answered", pi)
	}
}

// EXECUTION 2, through the production route. The duplicate's own PUBCOMP must not be
// able to land on a re-minted exchange: the identifier stays held until every
// transmitted copy has been answered.
func TestMQTTInflightStrayPubCompCannotTakeAReMintedIdentifier(t *testing.T) {
	t.Parallel()
	// ON THE REAL STORE (consult 26): an ordinary exchange's PUBCOMP is one authority batch.
	e := testNewInflightEnv(t, "client-stray-pubcomp")
	testReadyForRealPubRelDelivery(t, e)
	testWireInflightEnv(t, e) // its PUBRELs are transmitted, so their PUBCOMPs are owed (F-116)
	seed := testSeedRealPubRelReady(t, e)
	pi := seed.pi

	testDeliverPubRelAs(t, e, pi, seed.reply(1), seed.nonce)
	testDeliverPubRelAs(t, e, pi, seed.reply(2), seed.nonce)
	if _, owed, _, _ := testPubRelHold(t, e.sess, pi); owed != 2 {
		t.Fatalf("two transmitted copies owe two PUBCOMPs, got %d", owed)
	}

	e.c.mqttProcessPubComp(pi)
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was freed while the duplicate's PUBCOMP was still to come — "+
			"that stray answer would acknowledge away a new exchange's PUBREL record", pi)
	}

	// The stray arrives. Now, and only now, the identifier is genuinely free.
	e.c.mqttProcessPubComp(pi)
	if !testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d must be reusable once every transmitted copy was answered", pi)
	}
}

// A PUBCOMP whose RECORD may not be acknowledged still consumes the acknowledgement
// debt: the client answered, which is the event [MQTT-2.3.1-4] names, and whether the
// record may go is a different question. Deferring the consume below the withhold gates
// makes a withheld exchange double-count across its own redelivery and never release
// its identifier.
func TestMQTTInflightWithheldPubCompStillConsumesItsDebt(t *testing.T) {
	t.Parallel()
	// ON THE REAL STORE (F-105 row 2, consult 29): an ORDINARY exchange admitted through the
	// authority and answered by the real PUBREC handler, so the PUBREL delivery acquires its
	// action through the authority as production does.
	e := testNewInflightEnv(t, "client-withheld-pubcomp")
	testReadyForRealPubRelDelivery(t, e)
	testWireInflightEnv(t, e) // its PUBRELs are transmitted, so their PUBCOMPs are owed (F-116)
	seed := testSeedRealPubRelReady(t, e)
	pi := seed.pi
	dur := seed.pubRel
	deliver := func(n int, claim, copySubj string) {
		t.Helper()
		msg, hdrLen := mqttNewCorrelatedPubRel(pi, seed.nonce, testRealPubRelSrcDur, seed.sseq, _EMPTY_, claim, copySubj)
		e.c.pa.hdr = hdrLen
		defer func() { e.c.pa.hdr = 0 }()
		mqttDeliverPubRelCb(&subscription{client: e.c}, e.c, nil, e.sess.pubRelSubject, testInflightAckSubject(dur, seed.canon, n), msg)
	}

	// A carrier whose copy subject is not a shape this broker's generators produce is
	// REFUSED, so the PUBCOMP path may not acknowledge the record and withholds. Both
	// headers are present, because the builder omits the pair entirely when either is
	// empty — an absent reference owes nothing and would never reach the withhold.
	deliver(1, "$MQTT_q2affclaim.x", "not-a-copy-subject")
	if _, owed, _, _ := testPubRelHold(t, e.sess, pi); owed != 1 {
		t.Fatalf("the delivery must reserve the identifier: owed=%d", owed)
	}

	e.c.mqttProcessPubComp(pi)
	e.sess.mu.Lock()
	_, stillTracked := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if !stillTracked {
		t.Fatal("this fixture must reach the withhold: an acknowledged record proves nothing about it")
	}
	if _, owed, _, present := testPubRelHold(t, e.sess, pi); present && owed != 0 {
		t.Fatalf("a withheld PUBCOMP must still consume its debt, or the redelivery double-counts: owed=%d", owed)
	}

	// AckWait re-offers the record; the redelivery re-incurs exactly one debt.
	deliver(2, "$MQTT_q2affclaim.x", "not-a-copy-subject")
	if _, owed, _, _ := testPubRelHold(t, e.sess, pi); owed != 1 {
		t.Fatalf("the redelivery must leave exactly one PUBCOMP owed, got %d", owed)
	}
	e.c.mqttProcessPubComp(pi)
	// THE ACCOUNTING MUST STAY EXACT, and the identifier must stay EXCLUDED. This check
	// used to require the registry hold to disappear entirely; it is corrected rather than
	// loosened (F-098, consult D-4), and the debt half it also proved is kept verbatim.
	//
	// What is WITHDRAWN is precise: the registry hold's DISAPPEARANCE after debt
	// consumption. It was never an end-to-end reclamation guarantee — pendingPubRel[pi]
	// survives this withheld path at HEAD too and blocks allocation by itself — so nothing
	// this fixture ever demonstrated about reuse is given up.
	//
	// What is ADDED is the property the old shape could not express: the drain retired from
	// `pubrel_ready` without completing the exchange, so the slot is still this exchange's
	// with no action token and a peer may acquire it and transmit a PUBREL under this
	// identifier. The exclusion is what refuses that, and it charges no capacity.
	tok, owed, _, present := testPubRelHold(t, e.sess, pi)
	if !present || tok != _EMPTY_ || owed != 0 {
		t.Fatalf("the second withheld PUBCOMP must consume all debt while preserving slot exclusion: token=%q owed=%d present=%v",
			tok, owed, present)
	}
	e.sess.mu.Lock()
	relStill := e.sess.pendingPubRel[pi]
	h := e.sess.sendsInFlight[pi]
	var binds int
	var boundSubject string
	if h != nil && h.slotOwed != nil {
		binds = len(h.slotOwed.binds)
		if binds > 0 {
			boundSubject = h.slotOwed.binds[0].subject
		}
	}
	liveAction := h != nil && h.action != nil
	retained := h != nil && h.retainedGen != _EMPTY_
	terminal := h != nil && h.terminalOwed != _EMPTY_
	wantSubject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()
	if relStill == nil {
		t.Fatal("the withheld record must still be pending: the exclusion is about its unfinished exchange")
	}
	if binds == 0 || boundSubject != wantSubject {
		t.Fatalf("the hold must carry a slot exclusion for %q, got %d binds naming %q",
			wantSubject, binds, boundSubject)
	}
	if liveAction || retained || terminal {
		// Otherwise another live term could be what refuses allocation below, and this
		// fixture would prove nothing about the exclusion.
		t.Fatalf("no other liveness term may mask the exclusion: action=%v retained=%v terminal=%v",
			liveAction, retained, terminal)
	}
	// Through the production loader, on the real store.
	if rec, _, lerr := e.asm.loadInflightRecord(wantSubject); lerr != nil || rec == nil ||
		rec.State != mqttInflightPubRelReady || rec.ActionToken != _EMPTY_ {
		t.Fatalf("the durable slot must still be this exchange's quiescent pubrel_ready slot: rec=%+v err=%v", rec, lerr)
	}
	if testMintsPI(t, e.sess, pi) {
		t.Fatal("bumpPI must not re-mint an identifier whose exchange is unresolved")
	}
	// AND THE RETAINED ALLOCATOR, which consults no durable state at all and is therefore
	// the route the exclusion exists to refuse. Exercised in isolation, with the tracking
	// maps emptied and capacity available, so a refusal cannot be capacity's doing — and it
	// must hand out a DIFFERENT NONZERO identifier, since demanding only "not pi" would pass
	// on a zero return.
	e.sess.mu.Lock()
	savedPub, savedRel, savedCp, savedLast := e.sess.pendingPublish, e.sess.pendingPubRel, e.sess.cpending, e.sess.last_pi
	e.sess.pendingPublish = make(map[uint16]*mqttPending)
	e.sess.pendingPubRel = make(map[uint16]*mqttPending)
	e.sess.cpending = make(map[string]map[uint64]uint16)
	e.sess.last_pi = pi - 1
	// The EXCHANGE is unfinished (its PUBREL is still owed), so it keeps its Receive Maximum
	// charge (Spec5 [4.9]) — held since F-105 row 5 by the custody ledger, not by the maps
	// emptied above. What this fixture measures is the EXCLUSION's charge: everything the
	// ledger does not account for, which with the maps emptied is whatever the exclusion
	// adds. The ledger's own charge is asserted separately, as exactly this one exchange.
	ledgerCharges := e.sess.custody.charges()
	charged := e.sess.receiveMaxOccupancyLocked() - ledgerCharges
	got := e.sess.bumpPI()
	e.sess.pendingPublish, e.sess.pendingPubRel, e.sess.cpending, e.sess.last_pi = savedPub, savedRel, savedCp, savedLast
	e.sess.mu.Unlock()
	if got == 0 || got == pi {
		t.Fatalf("an excluded identifier must be skipped for a NEW allocation, not exhaust it: got %d", got)
	}
	if charged != 0 {
		t.Fatalf("the exclusion must charge no Receive Maximum capacity, got %d", charged)
	}
	if ledgerCharges != 1 {
		t.Fatalf("the unfinished exchange must hold exactly its own custody charge, got %d", ledgerCharges)
	}
}

// TestPubRelDeliveryWithholdsAForeignIdentifierForADrainedSource is round 14's finding, and
// it is the SECOND reader of the same question rather than a second bug.
//
// Recovery compares the record's packet identifier against the drain intent's, so a record
// naming the right source and the right exchange under a foreign identifier is refused
// there. But PUBREL delivery is asynchronous: the restore gate opens before the PUBREL
// consumer is installed and before recoverDrainPubRels runs, so the same record can arrive
// at the callback FIRST. With the identifier left out of the delivery-side gate, it
// confirmed the drain, persisted the removal of its source claim, acknowledged that source
// and forwarded a PUBREL under the wrong identifier — after which the client's original
// exchange had neither its PUBREL nor the evidence to rebuild one.
//
// Withheld rather than refused-and-acked, because the record may be the only copy of
// whatever it does prove and AckWait re-offers it.
func TestPubRelDeliveryWithholdsAForeignIdentifierForADrainedSource(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-pubrel-foreign-pi")
	testReadyForPubRelDelivery(t, e)
	const (
		dur        = "dur-foreign"
		sseq       = uint64(19)
		intentPI   = uint16(6)
		foreignPI  = uint16(9)
		ownerNonce = "xxxxxxxxxxxxxxxxxxxxxx"
	)

	// A drained source past PUBREC, with its intent naming the identifier the client is
	// waiting on and the exchange that owns it.
	d := &mqttDrain{
		cc:   &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + dur, AckPolicy: AckExplicit},
		qos:  2,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true, intent: true, pi: intentPI, ownerNonce: ownerNonce}},
	}
	e.sess.mu.Lock()
	e.sess.drains = map[string]*mqttDrain{"sid": d}
	e.sess.drainByDur = map[string]*mqttDrain{dur: d}
	e.sess.mu.Unlock()

	// The premise, asserted against the gate itself: this exchange's OWN identifier is
	// accepted. Without it the refusal below could be any unrelated rejection.
	if !e.sess.drainIntentOwnedBy(dur, sseq, ownerNonce, intentPI) {
		t.Fatal("the intent's own identifier and exchange must be accepted, or the refusal proves nothing")
	}
	// t.Error rather than t.Fatal: if the gate accepts a foreign identifier, the callback
	// assertions below are exactly what must be seen to fail, and stopping here would hide
	// the consequence the finding is about.
	if e.sess.drainIntentOwnedBy(dur, sseq, ownerNonce, foreignPI) {
		t.Error("a foreign identifier for a drained source past PUBREC must be refused")
	}

	// Now through the callback, which is the path that races recovery.
	//
	// The record's own JetStream reply subject is OBSERVED, because "withheld" is a claim
	// about the acknowledgement and nothing else here can see it: a mismatch branch that
	// acked and then returned would leave the drain untouched and reserve nothing, passing
	// every other assertion below while irreversibly deleting the record — and that record
	// may be the only evidence for whichever exchange or affine assignment it really does
	// belong to.
	reply := testInflightAckSubject(dur, sseq, 1)
	ackObs, serr := e.s.nrc.SubscribeSync(reply)
	if serr != nil {
		t.Fatalf("subscribe to the record's reply subject: %v", serr)
	}
	defer func() { _ = ackObs.Unsubscribe() }()
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush the observer subscription: %v", err)
	}

	msg, hdrLen := mqttNewCorrelatedPubRel(foreignPI, ownerNonce, dur, sseq, _EMPTY_, _EMPTY_, _EMPTY_)
	if msg == nil {
		t.Fatal("the builder refused a record this test needs")
	}
	e.c.pa.hdr = hdrLen
	mqttDeliverPubRelCb(&subscription{client: e.c}, e.c, nil, e.sess.pubRelSubject, reply, msg)
	e.c.pa.hdr = 0

	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush after the delivery: %v", err)
	}
	if am, aerr := ackObs.NextMsg(250 * time.Millisecond); aerr == nil {
		t.Errorf("the record was ACKNOWLEDGED (%q on %q): a withheld record must be left for "+
			"redelivery, because acknowledging destroys the only copy of whatever it does prove",
			am.Data, am.Subject)
	}

	e.sess.mu.Lock()
	st := d.seqs[sseq]
	claimed, intent, gotPI := st.claimed, st.intent, st.pi
	phase := st.phase
	_, reserved := e.sess.pendingPubRel[foreignPI]
	e.sess.mu.Unlock()

	if !claimed {
		t.Error("the source claim must SURVIVE: acknowledging it on another exchange's evidence " +
			"loses the message the client is still waiting for")
	}
	if !intent || gotPI != intentPI {
		t.Errorf("the intent must stand under its ORIGINAL identifier, got intent=%v pi=%d", intent, gotPI)
	}
	if phase == drainPubRelConfirmed {
		t.Error("a record for a foreign identifier must not confirm the drain")
	}
	if reserved {
		t.Errorf("the withheld record must reserve nothing: identifier %d is not this session's to hold",
			foreignPI)
	}
}

// TestPubRelWithholdsAgainstAClaimedDrainWithNoIntent is round 18's first HIGH finding, and
// the fail-open shape is worth stating precisely because the two cases look identical from
// inside the gate.
//
// drainIntentOwnedBy returned true for BOTH "this source is not drained at all" and "this
// drain claims the sequence but records no PUBREL intent". The second is not an absence of
// evidence, it is a CONTRADICTION: the drain says the exchange is still in its PUBLISH
// phase and a stored PUBREL says the client answered PUBREC, and both cannot be true of one
// exchange. Returning true let the callback confirm the drain immediately — removing the
// persisted claim, acknowledging the source and forwarding the PUBREL — with neither the
// owner nonce nor the identifier compared, because there was no intent to compare them
// against.
//
// A session-record fork produces exactly this pair: the surviving record keeps the
// pre-PUBREC drain state while a superseded writer's correlated PUBREL survives on
// $MQTT_out.
func TestPubRelWithholdsAgainstAClaimedDrainWithNoIntent(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-drain-no-intent")
	testReadyForPubRelDelivery(t, e)
	const (
		dur   = "dur-no-intent"
		sseq  = uint64(23)
		pi    = uint16(4)
		owner = "xxxxxxxxxxxxxxxxxxxxxx"
	)

	// A CLAIMED drained sequence with NO intent: still in its PUBLISH phase as far as the
	// record is concerned.
	d := &mqttDrain{
		cc:   &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + dur, AckPolicy: AckExplicit},
		qos:  2,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true}},
	}
	e.sess.mu.Lock()
	e.sess.drains = map[string]*mqttDrain{"sid": d}
	e.sess.drainByDur = map[string]*mqttDrain{dur: d}
	e.sess.mu.Unlock()

	// The premise, both ways, against the gate itself.
	if e.sess.drainIntentOwnedBy(dur, sseq, owner, pi) {
		t.Error("a CLAIMED drain with no PUBREL intent contradicts the record and must be refused")
	}
	if !e.sess.drainIntentOwnedBy(dur, sseq+1, owner, pi) {
		t.Error("a sequence this drain does not claim is not its business and must be permitted, " +
			"or the refusal above is indiscriminate rather than a decision")
	}

	// The record's own reply subject is observed: withheld means NOT acknowledged.
	reply := testInflightAckSubject(dur, sseq, 1)
	ackObs, serr := e.s.nrc.SubscribeSync(reply)
	if serr != nil {
		t.Fatalf("subscribe to the reply subject: %v", serr)
	}
	defer func() { _ = ackObs.Unsubscribe() }()
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	msg, hdrLen := mqttNewCorrelatedPubRel(pi, owner, dur, sseq, _EMPTY_, _EMPTY_, _EMPTY_)
	if msg == nil {
		t.Fatal("the builder refused a record this test needs")
	}
	e.c.pa.hdr = hdrLen
	mqttDeliverPubRelCb(&subscription{client: e.c}, e.c, nil, e.sess.pubRelSubject, reply, msg)
	e.c.pa.hdr = 0
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush after delivery: %v", err)
	}

	if am, aerr := ackObs.NextMsg(250 * time.Millisecond); aerr == nil {
		t.Errorf("the source was ACKNOWLEDGED (%q): a record contradicting the drain may not "+
			"consume it", am.Data)
	}
	e.sess.mu.Lock()
	st := d.seqs[sseq]
	claimed, phase := st.claimed, st.phase
	_, reserved := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if !claimed {
		t.Error("the source claim must SURVIVE: it is the only thing keeping the payload")
	}
	if phase == drainPubRelConfirmed {
		t.Error("a record contradicting the drain must not confirm it")
	}
	if reserved {
		t.Errorf("the withheld record must reserve nothing, yet identifier %d is held", pi)
	}
}

// A RETIRED shared QoS 2 register is a shape this broker no longer writes (7a: a supported shared
// delivery is QoS 1 beside its exchange, and is never retired by a PUBREC). It replaced, BY NAME, the
// two contest tests that pinned how the shared claim contest (deleted in F-105 7b) re-targeted over a
// concurrent retirement and refused to close a retired tombstone: that population cannot arise, so
// recovery refuses it outright — the register untouched, no custody write, its identifier reserved
// with nothing deliverable, and no source retirement (the suppression that acknowledges re-offers
// away) installed from it.
//
// Mutation M26 (sharedRecordUnsupported reports every record supported) NO LONGER FAILS this test,
// measured 2026-10-11: the fixture's register names an obligation this generation's inventory does
// not reach, so the 7b inventory gate refuses the same record and every assertion still holds.
func TestMQTTInflightUnsupportedSharedShapeIsRefusedBeforeTheContest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*mqttInflightRecord)
	}{
		{"retired", func(r *mqttInflightRecord) { r.Retired = true }},
		{"retired and cancelled", func(r *mqttInflightRecord) {
			r.Retired, r.Cancelled, r.CancelOrigin = true, true, mqttCancelOriginOccupant // F-088: provenance
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := "client-unsupported-shared-" + map[bool]string{true: "a", false: "b"}[tc.name == "retired"]
			e := testNewInflightEnv(t, id)
			_, _, dur := mqttSharedNames("g", "unsupported.shape", 1)
			sseq := e.publishSourceGroup(t, dur, "g", "unsupported.shape", "unsupported.shape")
			pi := e.trackUnsupportedSharedShape(t, dur, sseq, 2)
			testMarkRegister(t, e, pi, tc.mutate)
			if err := testSessSave(t, e.sess); err != nil {
				t.Fatalf("persist session: %v", err)
			}
			asmB, sessB, cB := e.peer(t, "node2", id)
			if err := testSessSave(t, sessB); err != nil {
				t.Fatalf("persist takeover: %v", err)
			}
			subject := mqttInflightSubject(sessB.idHash, sessB.gen, pi)
			before, bmsg, err := asmB.jsa.loadInflightRecord(subject)
			if err != nil || before == nil || bmsg == nil {
				t.Fatalf("fixture: the unsupported register must stand: %+v %v", before, err)
			}
			var writes int
			asmB.mu.Lock()
			asmB.testInflightStore = func(subj string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
				if subj == subject {
					writes++
				}
				return asmB.jsa.storeInflightCAS(subj, expectSeq, r)
			}
			asmB.mu.Unlock()
			if _, err := sessB.recoverInflightRegisters(cB); err != nil {
				t.Fatalf("takeover recovery: %v", err)
			}
			asmB.mu.Lock()
			asmB.testInflightStore = nil
			asmB.mu.Unlock()
			if writes != 0 {
				t.Fatalf("recovery issued %d custody write(s) over an unsupported shared shape", writes)
			}
			after, amsg, err := asmB.jsa.loadInflightRecord(subject)
			if err != nil || after == nil || amsg == nil || amsg.Sequence != bmsg.Sequence || after.Claim != before.Claim {
				t.Fatalf("the unsupported register was touched: before seq=%d claim=%q, after %+v seq=%v",
					bmsg.Sequence, before.Claim, after, amsg)
			}
			sessB.mu.Lock()
			ent := sessB.pendingPublish[pi]
			_, retired := sessB.srcAckOwed[mqttDeferredKey{dur, sseq}]
			_, mapped := sessB.cpending[dur][sseq]
			sessB.mu.Unlock()
			if ent == nil || !(ent.regForeign || ent.markerOpaque) {
				t.Fatalf("the unsupported record's identifier is not held as a non-deliverable reservation: %+v", ent)
			}
			if retired || mapped {
				t.Fatalf("an unsupported record installed source authority: retired=%v mapped=%v", retired, mapped)
			}
		})
	}
}
