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
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// Drain retirement's DISPOSAL half.
//
// Retirement used to release the sid and refund the charge BEFORE it unsubscribed and issued
// a noWait consumer delete that returns (nil, nil) unconditionally — so nothing established
// that the consumer went away, nothing excluded a delivery callback already dispatched, and
// four routes could hand the retired drain back to the same session object.
//
// These pin the four halves of the repair: the one critical section that drops the maps and
// closes the delivery barrier together, the terminal per-object flag at each insertion
// boundary, the three-way classified delete, and the landed-only charge refund.
//
// SCOPE, so a later reader does not overread them: the guarantee is about this drain OBJECT
// within this resident session object's lifetime. A fork sibling's record can rebuild the
// drain on a fresh object, which no per-object flag reaches; that residual is pre-existing
// and stated in docs/CONFORMANCE.md.

// drainDisposalLogCapture installs a capturing slog handler on the test server and returns a
// predicate over what was logged. It exists because "the indeterminate disposal is REPORTED"
// is half of what the classification promises — docs/CONFORMANCE.md tells an operator to
// look for it — and a branch that silently swallowed it would otherwise be indistinguishable
// from one that reported.
func drainDisposalLogCapture(t *testing.T, s *Server) func(substr string) bool {
	t.Helper()
	cap := &drainDisposalLogSink{}
	s.log.Store(slog.New(cap))
	t.Cleanup(func() { s.log.Store(nil) })
	return cap.sawContaining
}

type drainDisposalLogSink struct {
	mu    sync.Mutex
	lines []string
}

func (h *drainDisposalLogSink) Enabled(context.Context, slog.Level) bool { return true }

func (h *drainDisposalLogSink) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.lines = append(h.lines, r.Message)
	h.mu.Unlock()
	return nil
}

func (h *drainDisposalLogSink) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *drainDisposalLogSink) WithGroup(string) slog.Handler      { return h }

func (h *drainDisposalLogSink) sawContaining(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, l := range h.lines {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

// drainDisposalWaitFor polls fn until it stops returning an error, then fails. Local to this
// file because the shared checkFor lives behind the mqttport_fulltests build tag and these
// tests are part of the default suite.
func drainDisposalWaitFor(t *testing.T, total, step time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(total)
	var last error
	for time.Now().Before(deadline) {
		if last = fn(); last == nil {
			return
		}
		time.Sleep(step)
	}
	t.Fatalf("condition not met within %v: %v", total, last)
}

// bindDrainDeliveryClient wires the fixture's client the way a real CONNECT wires it, so the
// delivery callback can actually run: it reads cc.mqtt.sess at its entry and compares
// sess.c against the delivering subscription's client. drainTestSession builds a session
// object with no bound connection, which is all the record-level tests need.
func bindDrainDeliveryClient(t *testing.T, s *Server, c *client, sess *mqttSession) {
	t.Helper()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	c.mqtt = &mqtt{asm: asm, sess: sess}
	sess.mu.Lock()
	sess.c = c
	sess.mu.Unlock()
}

// storeQoS12Source puts a message on the messages stream carrying the MQTT publish header
// the broker writes for a QoS > 0 PUBLISH. Without that header the delivery callback returns
// at its entry — a bare payload is not a QoS-published message — so a fixture that omitted
// it would silently never reach the code under test.
func storeQoS12Source(t *testing.T, sess *mqttSession, filter string, qos byte, payload string) {
	t.Helper()
	hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s:%d\r\n\r\n", mqttNatsHeader, qos)
	full := append(append([]byte(nil), hdr...), payload...)
	if _, err := sess.jsa.storeMsg(mqttStreamSubjectPrefix+filter, len(hdr), full); err != nil {
		t.Fatalf("storeMsg: %v", err)
	}
}

// drainDisposalObserver records every delete the retired-drain seam sees, with the noWait
// argument it was really called with. Observing the argument is what makes a reversion to
// the fire-and-forget form measurable: an error-substituting seam alone cannot tell a
// request that waited from one that did not.
type drainDisposalObserver struct {
	mu     sync.Mutex
	calls  []drainDisposalCall
	prefix string // only durables with this prefix are this test's
	send   bool
	obsErr error
	over   bool
}

type drainDisposalCall struct {
	durable string
	noWait  bool
}

// hook only acts on durables this test owns. The seam is process-global and other tests in
// this package run in parallel, so an unfiltered handler would suppress or corrupt THEIR
// drain disposals — a cross-test failure indistinguishable from a flake.
func (o *drainDisposalObserver) hook(durable string, noWait bool) (bool, error, bool) {
	if o.prefix != "" && !strings.HasPrefix(durable, o.prefix) {
		return true, nil, false // not ours: issue the real request, unchanged
	}
	o.mu.Lock()
	o.calls = append(o.calls, drainDisposalCall{durable, noWait})
	send, obsErr, over := o.send, o.obsErr, o.over
	o.mu.Unlock()
	return send, obsErr, over
}

func (o *drainDisposalObserver) observed() []drainDisposalCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]drainDisposalCall(nil), o.calls...)
}

func (o *drainDisposalObserver) durables() []string {
	out := []string{}
	for _, c := range o.observed() {
		out = append(out, c.durable)
	}
	return out
}

// pendingForDurable reports the packet identifiers currently tracked for one durable. The
// question every negative-delivery assertion here asks is "did THIS durable transmit", and a
// bare len(sess.pendingPublish) cannot answer it — another durable's legitimate delivery
// would satisfy it, and a forbidden one would hide in it.
func pendingForDurable(sess *mqttSession, dur string) []uint16 {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	var out []uint16
	for pi, p := range sess.pendingPublish {
		if p != nil && p.jsDur == dur {
			out = append(out, pi)
		}
	}
	return out
}

// awaitDeliveryBarrier publishes on a LIVE durable and waits until that durable's own
// delivery is tracked. It is the ordering barrier the negative assertions need: the released
// callback under test was let go first, so once a delivery published AFTER it has been
// tracked, the earlier one has had its chance on the same session.
//
// This replaces "poll until pendingPublish is empty", which succeeds on the state BEFORE the
// callback runs and therefore proves nothing at all.
func awaitDeliveryBarrier(t *testing.T, sess *mqttSession, liveDur, liveFilter, payload string) {
	t.Helper()
	storeQoS12Source(t, sess, liveFilter, 1, payload)
	drainDisposalWaitFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if len(pendingForDurable(sess, liveDur)) == 0 {
			return fmt.Errorf("the live control delivery on %q has not been tracked yet", liveDur)
		}
		return nil
	})
}

// installLiveControlConsumer gives the session a plain, non-draining QoS 1 consumer with a
// real delivery subscription, so a delivery on it is proof the fixture can still deliver.
func installLiveControlConsumer(t *testing.T, sess *mqttSession, c *client, sid, dur, filter string) {
	t.Helper()
	cfg := realDrainConsumer(t, sess, dur, filter)
	sess.mu.Lock()
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	_, err := sess.processQOS12Sub(c, []byte(mqttSubPrefix+dur), []byte(mqttSubPrefix+dur), nil,
		false, 1, false, false, false, 0, dur, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()
	if err != nil {
		t.Fatalf("the live control subscription: %v", err)
	}
}

// emptyDrainWithARealConsumer is the fixture every disposal test needs: a drain that is
// DURABLY empty (so retirement is eligible), still owns its sid, and has an actual durable
// on the stream so a delete has something to name and a sweep something to reclaim.
//
// The past-PUBREC intent is retained deliberately: it is what makes the sid HELD, so a test
// that observes the sid free afterwards has observed a real release rather than a drain that
// never held anything.
func emptyDrainWithARealConsumer(t *testing.T, sess *mqttSession, sid, dur, filter string) *mqttDrain {
	t.Helper()
	cfg := realDrainConsumer(t, sess, dur, filter)
	d := addTestDrain(sess, sid, dur, filter, 2, map[uint64]*drainSeq{
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	sess.mu.Lock()
	d.cc = cfg
	// The disposal path's production precondition: an owned retirement transition has
	// already marked this object and its save carried the omission, so the record no longer
	// names the durable and the delete is licensed. Without it these fixtures stop at the
	// unmarked guard and every disposal assertion passes for the wrong reason — the
	// symmetric trap to the retirable() check below.
	d.removalMarked = true
	retirable := d.retirable()
	sess.mu.Unlock()
	if !retirable {
		t.Fatal("the fixture must be durably empty, or retirement is refused and every " +
			"assertion below passes for the wrong reason")
	}
	return d
}

func drainConsumerExists(t *testing.T, sess *mqttSession, dur string) bool {
	t.Helper()
	// consumerList, never len(sess.cons): the question is whether the consumer is on the
	// stream, which the session's own maps cannot answer.
	infos, err := sess.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	for _, ci := range infos {
		if ci != nil && ci.Name == dur {
			return true
		}
	}
	return false
}

// T2. TestRetiredDrainDisposalIsClassifiedNotAssumed
//
// The delete now WAITS for an answer and classifies it three ways. Two mutations live here
// and they are separate: reverting to noWait=true is caught by the observed argument, and
// collapsing the indeterminate branch into the definite ones is caught by the report.
func TestRetiredDrainDisposalIsClassifiedNotAssumed(t *testing.T) {
	s, c, sess, _ := drainTestSession(t, "drain-dispose-classified")
	sawLog := drainDisposalLogCapture(t, s)

	const dur, sid = "dur-dispose-classified", "sid"
	d := emptyDrainWithARealConsumer(t, sess, sid, dur, "foo")

	// An INDETERMINATE outcome: neither success nor a confirmed absence. Supplied through
	// the seam rather than by breaking the connection, because a broken connection is not
	// aimable at one request.
	obs := &drainDisposalObserver{prefix: "dur-", obsErr: fmt.Errorf("test: no response for the consumer delete"), over: true}
	restore := setMQTTTestDrainDeleteHook(obs.hook)
	defer restore()

	sess.finishDrainIfEmpty(c, d)

	calls := obs.observed()
	if len(calls) != 1 {
		t.Fatalf("expected exactly one disposal of %q, got %v", dur, calls)
	}
	if calls[0].durable != dur {
		t.Fatalf("the disposal named %q, want %q", calls[0].durable, dur)
	}
	if calls[0].noWait {
		t.Fatal("the disposal was issued with noWait=true: nothing then establishes that " +
			"the consumer went away, which is the whole defect this closes")
	}
	if !sawLog("is unconfirmed") {
		t.Fatal("an indeterminate disposal must be REPORTED: docs/CONFORMANCE.md tells an " +
			"operator to expect this line, and without it a surviving consumer is silent")
	}
	// The definite outcomes must NOT be reported, or the report carries no information.
	obs2 := &drainDisposalObserver{prefix: "dur-", over: true}
	restore2 := setMQTTTestDrainDeleteHook(obs2.hook)
	d2 := emptyDrainWithARealConsumer(t, sess, "sid2", dur+"-ok", "bar")
	sess.finishDrainIfEmpty(c, d2)
	restore2()
	if sawLog(dur + "-ok") {
		t.Fatal("a successful disposal must report nothing, otherwise the unconfirmed line " +
			"does not discriminate")
	}

	// The named reaper for the one outcome that has no verdict. The hook suppressed the
	// request, so the consumer is still there — assert that positively first, or the sweep
	// below proves nothing.
	if !drainConsumerExists(t, sess, dur) {
		t.Fatal("the fixture must leave the consumer in place, or the sweep has nothing to reclaim")
	}
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	asm.sweepOrphanedConsumersBefore(s, time.Now().Add(time.Minute))
	if drainConsumerExists(t, sess, dur) {
		t.Fatal("the orphaned-consumer sweep must reclaim a retired drain's durable: it is " +
			"named by no resident object and by no record, which is what makes it reclaimable")
	}
}

// T9. TestCompetingRetirementsDisposeOnce
//
// Two goroutines reach retirement for the same drain. The sid-ownership guard and the map
// drop are in one critical section, so exactly one of them can win — and only the winner may
// issue a delete, because a second delete would be issued by a caller that never owned the
// drain.
func TestCompetingRetirementsDisposeOnce(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-dispose-once")

	const dur, sid = "dur-dispose-once", "sid"
	d := emptyDrainWithARealConsumer(t, sess, sid, dur, "foo")

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restore := setMQTTTestDrainDeleteHook(obs.hook)
	defer restore()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			sess.finishDrainIfEmpty(c, d)
		}()
	}
	close(start)
	wg.Wait()

	if got := obs.durables(); len(got) != 1 {
		t.Fatalf("competing retirements must dispose exactly once, got %v", got)
	}
}

// T5. TestRestoreDrainLockedRefusesARetiredObject
//
// A DIRECT UNIT TEST OF A DEFENSIVE GUARD, and labelled as one so it is not mistaken for
// evidence that the route is live. No production schedule reaches restoreDrainLocked with a
// retired object today: the fork-repair and record-restore callers mint fresh objects
// through mqttDrainFromPersisted, and undoPlans restores only for a non-empty sid, which
// dropDrainLocked returns empty for an already-retired object.
//
// The guard is kept because this is the package's only re-insertion of an EXISTING drain
// object into both maps, and a terminal invariant belongs at that one door. The production
// fixture requirement is therefore explicitly relaxed here.
func TestRestoreDrainLockedRefusesARetiredObject(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-restore-refuses-retired")

	const dur, sid = "dur-restore-refuses", "sid"
	d := emptyDrainWithARealConsumer(t, sess, sid, dur, "foo")

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restore := setMQTTTestDrainDeleteHook(obs.hook)
	defer restore()

	// Retirement through the production path, which is what sets the flag.
	sess.finishDrainIfEmpty(c, d)
	sess.mu.Lock()
	retired := d.retired
	sess.mu.Unlock()
	if !retired {
		t.Fatal("retirement must mark the object, or there is nothing for the guard to see")
	}

	sess.mu.Lock()
	sess.restoreDrainLocked(sid, d)
	back, byDur := sess.drains[sid], sess.drainByDur[dur]
	sess.mu.Unlock()
	if back != nil || byDur != nil {
		t.Fatal("restoreDrainLocked must refuse a terminally retired object: re-inserting it " +
			"gives a delete-requested durable a live delivery route and re-holds its sid")
	}

	// Positive control: the same call for a NON-retired object still installs, so the guard
	// is a refusal of the retired case and not a disabled function.
	live := addTestDrain(sess, "sid-live", dur+"-live", "bar", 2, nil)
	sess.mu.Lock()
	delete(sess.drains, "sid-live")
	delete(sess.drainByDur, dur+"-live")
	sess.restoreDrainLocked("sid-live", live)
	okFwd, okRev := sess.drains["sid-live"], sess.drainByDur[dur+"-live"]
	sess.mu.Unlock()
	if okFwd != live || okRev != live {
		t.Fatal("restoreDrainLocked must still install a drain that has not been retired")
	}
}

// T4. TestRestoreDrainsMustNotReviveARetiredDrain
//
// The schedule that matters is retirement BETWEEN the todo capture and the per-item install,
// and the kill is a POSITIVE observation: with the recheck dropped, the resume installs the
// stale reverse-index entry and the stale delivery subscription. Asserting only their absence
// would pass whenever the install failed for any other reason, which is why the pre-install
// pause is the seam and the QoS 1/2 subscription-failure hook is not.
//
// The target must be UNSKIPPED. A skipped item takes its continue before the install and so
// installs nothing even with the recheck dropped, which would make the mutation unkillable.
func TestRestoreDrainsMustNotReviveARetiredDrain(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-restore-no-revive")

	const target, control = "dur-restore-target", "dur-restore-control"
	tgt := emptyDrainWithARealConsumer(t, sess, "sid-target", target, "foo")
	ctl := emptyDrainWithARealConsumer(t, sess, "sid-control", control, "bar")

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restoreDel := setMQTTTestDrainDeleteHook(obs.hook)
	defer restoreDel()

	// Retire the target from the pause, which runs with sess.mu released and before the
	// per-item lock. Inline rather than on a second goroutine: retirement takes sess.mu
	// itself, so this is both deterministic and free of a rendezvous.
	var retiredOnce bool
	restoreHook := setMQTTTestDrainRestorePreInstallHook(func(dur string) {
		if dur != target || retiredOnce {
			return
		}
		retiredOnce = true
		sess.finishDrainIfEmpty(c, tgt)
	})
	defer restoreHook()

	sess.restoreDrains(c, nil)
	restoreHook()

	if !retiredOnce {
		t.Fatal("the pause never fired for the target, so this test never reached its schedule")
	}

	sess.mu.Lock()
	tgtRev := sess.drainByDur[target]
	ctlRev := sess.drainByDur[control]
	sess.mu.Unlock()
	c.mu.Lock()
	tgtSub := c.subs[mqttSubPrefix+target]
	ctlSub := c.subs[mqttSubPrefix+control]
	c.mu.Unlock()

	if tgtRev != nil || tgtSub != nil {
		t.Fatalf("the resume revived a drain retired since the todo capture (index=%v sub=%v): "+
			"that hands a delete-requested durable a live delivery route, and its sid is "+
			"re-held with no reaper able to clear it", tgtRev != nil, tgtSub != nil)
	}
	// The positive control is what proves the install was otherwise WORKING, which is what
	// makes the absence above attributable to the recheck.
	if ctlRev == nil || ctlSub == nil {
		t.Fatalf("the control drain must be restored with both its index entry and its "+
			"delivery subscription (index=%v sub=%v); without that the target's absence "+
			"proves nothing", ctlRev != nil, ctlSub != nil)
	}

	// The reverse ordering must be left alone: a drain retired AFTER the resume installed
	// for it retires normally, and nothing above may have broken that.
	sess.finishDrainIfEmpty(c, ctl)
	sess.mu.Lock()
	ctlGone, ctlHold := sess.drains["sid-control"], sess.drainSidHoldLocked("sid-control")
	sess.mu.Unlock()
	if ctlGone != nil || ctlHold != mqttDrainNotHeld {
		t.Fatalf("a drain restored and then retired must still retire and free its sid "+
			"(drain=%v hold=%v)", ctlGone != nil, ctlHold)
	}
}

// T3. TestSubscribeRollbackMustNotResurrectARetiredDrain
//
// A SUBSCRIBE snapshots the drain maps SHALLOWLY, so a rollback used to restore a drain
// retired during the packet — with its sid held for the life of the session, because
// st.intent is never cleared and the durable is gone, so no reaper can release it.
//
// The ordering is the point, and it is what makes M12 killable here: the retirement and its
// INLINE disposal both complete before the rollback runs, so the skip depends on d.retired
// alone. The rollback restores FROM THE SNAPSHOT, which still holds the object, so nothing
// the maps could also say is available to mask a cleared flag.
func TestSubscribeRollbackMustNotResurrectARetiredDrain(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-rollback-no-resurrect")

	const dur, sid = "dur-rollback-retired", "sid-retired"
	const keepDur, keepSid = "dur-rollback-keep", "sid-keep"
	d := emptyDrainWithARealConsumer(t, sess, sid, dur, "foo")
	keep := emptyDrainWithARealConsumer(t, sess, keepSid, keepDur, "bar")

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restoreDel := setMQTTTestDrainDeleteHook(obs.hook)
	defer restoreDel()

	// The packet's snapshot, taken while BOTH drains are live.
	snap := sess.snapshotSubs()
	if snap.drains[sid] != d || snap.drainByDur[dur] != d {
		t.Fatal("the snapshot must carry the drain, or the resurrection route is not exercised")
	}

	// The concurrent finalizer: retirement AND its inline disposal, both complete. This is
	// an ordinary caller holding no transaction, so the inline disposal is the production
	// route rather than a schedule the design forbids.
	sess.finishDrainIfEmpty(c, d)
	if got := obs.durables(); len(got) != 1 || got[0] != dur {
		t.Fatalf("the disposal must have completed before the rollback, got %v", got)
	}

	tok := sess.beginRecordTx()
	consSnap := map[string]*ConsumerConfig{}
	sess.rollbackSubscribePacket(c, tok, nil, consSnap, snap, true)

	sess.mu.Lock()
	revived, revivedRev := sess.drains[sid], sess.drainByDur[dur]
	hold := sess.drainSidHoldLocked(sid)
	kept, keptRev := sess.drains[keepSid], sess.drainByDur[keepDur]
	sess.mu.Unlock()

	if revived != nil || revivedRev != nil {
		t.Fatalf("the rollback resurrected a retired drain (fwd=%v rev=%v): its durable has "+
			"been delete-requested, and its sid is then held for the life of the session "+
			"because st.intent is never cleared", revived != nil, revivedRev != nil)
	}
	if hold != mqttDrainNotHeld {
		t.Fatalf("the retired drain's sid must stay free after a rollback, got %v", hold)
	}
	// The positive control: the rollback still restores what it is FOR. A per-entry restore
	// that dropped everything would satisfy the assertions above and lose a live drain.
	if kept != keep || keptRev != keep {
		t.Fatalf("the rollback must restore a drain that was NOT retired (fwd=%v rev=%v)",
			kept == keep, keptRev == keep)
	}
	_ = keep
}

// T11. TestRetirementIsTerminalAfterDisposal
//
// Terminality is observed AFTER the disposal returns, on a path whose outcome depends on
// d.retired and on nothing else. T4 cannot serve this: its own independent
// sess.drains[sid] == d check rejects the removed object whether or not the flag survives,
// so it would pass against a build that cleared the flag once the delete came back.
func TestRetirementIsTerminalAfterDisposal(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-terminal-after-disposal")

	const dur, sid = "dur-terminal", "sid"
	d := emptyDrainWithARealConsumer(t, sess, sid, dur, "foo")

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restoreDel := setMQTTTestDrainDeleteHook(obs.hook)
	defer restoreDel()

	snap := sess.snapshotSubs()
	sess.finishDrainIfEmpty(c, d)
	if got := obs.durables(); len(got) != 1 {
		t.Fatalf("the disposal must have returned before terminality is observed, got %v", got)
	}

	sess.mu.Lock()
	stillRetired := d.retired
	sess.mu.Unlock()
	if !stillRetired {
		t.Fatal("the retirement flag must survive the disposal: clearing it once the delete " +
			"came back is the refuted design in which a late delete destroys a consumer that " +
			"has been returned to service")
	}

	tok := sess.beginRecordTx()
	sess.rollbackSubscribePacket(c, tok, nil, map[string]*ConsumerConfig{}, snap, true)
	sess.mu.Lock()
	back := sess.drains[sid]
	sess.mu.Unlock()
	if back != nil {
		t.Fatal("a rollback after the disposal must still refuse the retired object")
	}
}

// T7. TestRetirementDisposalRunsAfterTheRecordTransactionIsReleased
//
// A retained continuation runs under the uncertain-write resolver's OPEN transaction. It may
// not block on a JetStream request there: no save on the session can proceed while the fence
// is held, so a synchronous delete inside the continuation stalls every other writer for up
// to the JS API timeout.
//
// Driven through the REAL route, which is what makes it cover all three continuation call
// sites: an ambiguous reconciliation save registers retainOnLanded, and the resolution runs
// the continuation with the fence open. A version that called finishDrainIfEmptyInTx
// directly proved nothing about the call sites — reverting any of them to the inline form
// left it passing — and a review round caught that.
//
// The oracle is sampled INSIDE the delete seam: at the moment a disposal is issued, the
// record fence must be closed AND another writer must be able to open it.
func TestRetirementDisposalRunsAfterTheRecordTransactionIsReleased(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-dispose-after-release")

	// A REAL consumer with a CLAIMED sequence the stream no longer has: recovery prunes the
	// claim (p.unclaim) rather than dropping the drain (p.drop), so the drain empties and
	// the continuation hands it to retirement. A consumer-ABSENT drain is dropped instead
	// and never reaches retirement at all, which is why that shape cannot drive this test.
	const dur, sid, filter = "dur-dispose-after-release", "sid", "afterrelease.topic"
	cfg := realDrainConsumer(t, sess, dur, filter)
	d := addTestDrain(sess, sid, dur, filter, 2, map[uint64]*drainSeq{
		41: {claimed: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	sess.mu.Lock()
	d.cc = cfg
	sess.mu.Unlock()

	type sample struct {
		durable   string
		fenceOpen bool
		writerOK  bool
	}
	var (
		mu      sync.Mutex
		samples []sample
	)
	restoreDel := setMQTTTestDrainDeleteHook(func(durable string, _ bool) (bool, error, bool) {
		if !strings.HasPrefix(durable, "dur-") {
			return true, nil, false
		}
		sess.mu.Lock()
		open := sess.recordTxOpen
		sess.mu.Unlock()
		// A SECOND writer must be able to acquire the fence while this disposal is
		// outstanding. That is the cost the queue exists to avoid, and asserting the flag
		// alone would not measure it.
		writerOK := false
		if tok, ok := sess.tryBeginRecordTx(); ok {
			writerOK = true
			_ = sess.endRecordTx(tok)
		}
		mu.Lock()
		samples = append(samples, sample{durable, open, writerOK})
		mu.Unlock()
		return false, nil, true
	})
	defer restoreDel()

	// A LOST RESPONSE makes the reconciliation save ambiguous, so the teardown is deferred
	// to the retained continuation instead of running in the immediate arm.
	var ambiguousHits int
	restoreStore := setMQTTTestSessStoreResultHook(
		func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if hs != sess {
				return resp, err
			}
			if err == nil {
				ambiguousHits++
				return nil, fmt.Errorf("test: session record response lost")
			}
			return resp, err
		})
	sess.recoverDrains(c, nil)
	restoreStore()

	if ambiguousHits == 0 {
		t.Fatal("the reconciliation save was never made ambiguous, so no retained " +
			"continuation was registered and this test exercised the ordinary inline route")
	}

	mu.Lock()
	got := append([]sample(nil), samples...)
	mu.Unlock()
	if len(got) == 0 {
		t.Fatal("the retained continuation never disposed of the retired drain")
	}
	for _, sm := range got {
		if sm.fenceOpen {
			t.Fatalf("the disposal of %q was issued while a record transaction was OPEN: it "+
				"blocks for up to the JS API timeout, and no save on the session can proceed "+
				"for its whole duration", sm.durable)
		}
		if !sm.writerOK {
			t.Fatalf("another writer could not open the record fence while the disposal of "+
				"%q was outstanding, which is exactly the stall the queue exists to avoid",
				sm.durable)
		}
	}
	sess.mu.Lock()
	leftover := len(sess.pendingDrainDisposals)
	sess.mu.Unlock()
	if leftover != 0 {
		t.Fatalf("the queue must be emptied by the release that owned it, got %d", leftover)
	}
}

// T8. TestQueuedDisposalIsDrainedByItsOwnTransactionsRelease
//
// The TWO-TRANSACTION schedule, which is what discriminates a queue DETACHED before the
// unlock from one merely read after it. A releaser that reads the queue after unlocking can
// pick up a disposal a later transaction enqueued in that window — issuing another
// transaction's work, and, symmetrically, leaving that transaction with nothing to issue at
// its own release.
//
// A test that only re-takes the queue after the unlock passes either way, because nothing
// has been stranded.
func TestQueuedDisposalIsDrainedByItsOwnTransactionsRelease(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-dispose-two-tx")

	const durA, sidA = "dur-dispose-tx-a", "sid-a"
	const durB, sidB = "dur-dispose-tx-b", "sid-b"
	dA := emptyDrainWithARealConsumer(t, sess, sidA, durA, "foo")
	dB := emptyDrainWithARealConsumer(t, sess, sidB, durB, "bar")

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restoreDel := setMQTTTestDrainDeleteHook(obs.hook)
	defer restoreDel()

	tokA := sess.beginRecordTx()
	sess.finishDrainIfEmptyInTx(c, dA)

	// B opens the fence and enqueues while A is held between its unlock and its disposals.
	//
	// bDone is closed by B and only READ here, so the token it publishes is safe to use
	// after A resumes without a second channel receive to lose it to. bErr carries B's
	// failure to the assertions rather than to a t.Fatalf on B's own goroutine, which would
	// panic the whole test binary if it landed after this test returned.
	var (
		tokB recordTx
		bErr error
	)
	bDone := make(chan struct{})
	held := make(chan struct{})
	restoreSeam := setMQTTTestRecordTxPostReleaseHook(func(hs *mqttSession) {
		if hs != sess {
			return
		}
		select {
		case <-held:
			return // only the first release, A's, holds here
		default:
		}
		close(held)
		go func() {
			defer close(bDone)
			tok, ok := sess.tryBeginRecordTx()
			if !ok {
				bErr = fmt.Errorf("B could not open the fence after A released it")
				return
			}
			tokB = tok
			sess.finishDrainIfEmptyInTx(c, dB)
		}()
		select {
		case <-bDone:
			// B has queued its durable and still holds its own transaction. Resume A.
		case <-time.After(10 * time.Second):
			bErr = fmt.Errorf("B never enqueued")
		}
	})
	defer restoreSeam()

	// A's release: the seam above runs inside it, then A issues its own batch.
	if err := sess.endRecordTx(tokA); err != nil {
		t.Fatalf("endRecordTx(A): %v", err)
	}
	restoreSeam()

	select {
	case <-held:
	default:
		t.Fatal("the post-release seam never fired, so this test never reached its schedule")
	}
	<-bDone
	if bErr != nil {
		t.Fatalf("the two-transaction schedule was not reached: %v", bErr)
	}

	if got := obs.durables(); len(got) != 1 || got[0] != durA {
		t.Fatalf("A's release must issue A's batch ONLY, got %v: a releaser that reads the "+
			"queue after unlocking takes the disposal B enqueued in that window", got)
	}
	sess.mu.Lock()
	stillQueued := append([]string(nil), sess.pendingDrainDisposals...)
	sess.mu.Unlock()
	if len(stillQueued) != 1 || stillQueued[0] != durB {
		t.Fatalf("B's disposal must still be queued after A's release, got %v", stillQueued)
	}

	if err := sess.endRecordTx(tokB); err != nil {
		t.Fatalf("endRecordTx(B): %v", err)
	}
	if got := obs.durables(); len(got) != 2 || got[1] != durB {
		t.Fatalf("B's own release must issue B's batch, got %v", got)
	}
}

// T6. TestReconciliationDropRefundsOnlyOnALandedSave
//
// Reconciliation's consumer-absent arm removes a drain through p.drop without ever reaching
// retirement, so retirement's own refund cannot cover it. That is a LIVE leak: such a drain
// is charged by stageDrain, survives into a resume, and sess.tmaxack is otherwise reset only
// by clear(), so the session's consumer budget shrinks until SUBSCRIBEs are refused.
//
// The refund is on a PROVEN save, not at the drop. Refunding at the drop releases a charge
// for a removal that never landed, and the definitely-failed arm restores the object still
// charged — so there would be nothing to re-charge.
func TestReconciliationDropRefundsOnlyOnALandedSave(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failSave   bool
		ambiguous  bool
		wantRefund bool
	}{
		// The immediate landed arm, the RETAINED landed continuation, and the definitely
		// failed arm. All three are required: without the ambiguous one, refunding in the
		// immediate arm alone is indistinguishable from refunding in both — and an
		// ambiguous write that later proves landed is a removal that DID land, so its
		// charge is owed exactly as much as the immediate case's.
		{"landed", false, false, true},
		{"maybe landed then resolved", false, true, true},
		{"definitely failed", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, c, sess, _ := drainTestSession(t, "drain-refund-"+strings.ReplaceAll(tc.name, " ", "-"))

			// An UNRELATED charged consumer, so the expected tmaxack is never the zero
			// default — a check whose expected value is the default passes against broken
			// code.
			const otherDur = "dur-refund-other"
			sess.mu.Lock()
			if sess.consCharged == nil {
				sess.consCharged = make(map[string]int)
			}
			sess.consCharged[otherDur] = 7
			sess.tmaxack = 7
			sess.mu.Unlock()

			// No real consumer: reconciliation takes its consumer-absent arm, which is the
			// route with the missing refund.
			const dur, sid = "dur-refund-drop", "sid"
			d := addTestDrain(sess, sid, dur, "foo", 2, map[uint64]*drainSeq{
				41: {claimed: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
			})
			sess.mu.Lock()
			d.charged = true
			d.cc.MaxAckPending = 11
			sess.consCharged[dur] = 11
			sess.tmaxack += 11
			sess.mu.Unlock()

			if tc.failSave {
				// SCOPED TO THIS SESSION. These hooks are process-global and other tests
				// in this package run in parallel, so an unfiltered handler fails their
				// saves too — which is a cross-test failure that looks exactly like a
				// flake in whichever test happened to be saving.
				restore := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
					if hs != sess {
						return nil
					}
					return fmt.Errorf("test: session record unavailable")
				})
				defer restore()
			}

			if tc.ambiguous {
				// A LOST RESPONSE, not a failure: the record is stored but the answer never
				// arrives, so classifySave returns saveMaybeLanded and the discharge is
				// deferred to the RETAINED continuation. recoverDrains then resolves the
				// ambiguity itself — it runs during CONNECT and says so — and the readback
				// finds the record present, so the continuation runs on this same call.
				//
				// That is why the assertion is "the ambiguous route was taken AND the
				// refund happened", not a mid-flight observation: the resolution is
				// synchronous here, and there is no instant between the two to sample.
				var ambiguousHits int
				// SCOPED TO THIS SESSION, for the reason above: unfiltered, this converts
				// EVERY successful record store in the process into a lost response.
				restore := setMQTTTestSessStoreResultHook(
					func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
						if hs != sess {
							return resp, err
						}
						if err == nil {
							ambiguousHits++
							return nil, fmt.Errorf("test: session record response lost")
						}
						return resp, err
					})
				sess.recoverDrains(c, nil)
				restore()
				if ambiguousHits == 0 {
					t.Fatal("the reconciliation save was never made ambiguous, so this arm " +
						"exercised the immediate landed path and cannot discriminate a " +
						"refund that only happens there")
				}
			} else {
				sess.recoverDrains(c, nil)
			}

			sess.mu.Lock()
			charged, present := sess.consCharged[dur]
			tmaxack := sess.tmaxack
			otherStill, otherPresent := sess.consCharged[otherDur]
			sess.mu.Unlock()

			if !otherPresent || otherStill != 7 {
				t.Fatalf("the unrelated charge must be untouched, got %d present=%v",
					otherStill, otherPresent)
			}
			if tc.wantRefund {
				if present {
					t.Fatalf("a landed removal must refund the drain's charge, still %d", charged)
				}
				if tmaxack != 7 {
					t.Fatalf("tmaxack must fall back to the unrelated charge alone, got %d", tmaxack)
				}
			} else {
				if !present || charged != 11 {
					t.Fatalf("a definitely failed save restores the drain STILL CHARGED, so "+
						"nothing may be refunded; got %d present=%v", charged, present)
				}
				if tmaxack != 18 {
					t.Fatalf("tmaxack must be unchanged when the removal did not land, got %d", tmaxack)
				}
			}
		})
	}
}

// drainDeliveryPark parks the delivery callback of one deliver subject before it takes
// sess.mu — the position a callback already dispatched by the client library occupies while
// retirement runs — and reports when it has arrived and when it has run to completion.
//
// It never calls t.Fatalf: a helper goroutine's late failure panics the whole test binary,
// so everything it observes is reported back through channels and asserted on the test's own
// goroutine.
type drainDeliveryPark struct {
	inbox    string
	arrived  chan struct{}
	release  chan struct{}
	finished chan struct{}
	once     sync.Once
	doneOnce sync.Once
}

func newDrainDeliveryPark(inbox string) *drainDeliveryPark {
	return &drainDeliveryPark{
		inbox:    inbox,
		arrived:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
}

// hook parks the FIRST callback that arrives on this inbox and lets every other delivery
// through untouched.
func (p *drainDeliveryPark) hook(sub *subscription) {
	if sub == nil || string(sub.subject) != p.inbox {
		return
	}
	var first bool
	p.once.Do(func() { first = true })
	if !first {
		return
	}
	close(p.arrived)
	<-p.release
}

// done fires from the callback's own deferred completion seam, so it is the moment the
// parked callback RETURNED. Nothing else here is allowed to stand in for that: a delivery on
// another durable proves nothing, because each durable has its own NATS subscription and its
// own dispatch goroutine, so the other one can be tracked while this callback is merely
// runnable. A review round established exactly that.
func (p *drainDeliveryPark) done(sub *subscription) {
	if sub == nil || string(sub.subject) != p.inbox {
		return
	}
	p.doneOnce.Do(func() { close(p.finished) })
}

// awaitCompletion blocks until the parked callback has returned.
func (p *drainDeliveryPark) awaitCompletion(t *testing.T) {
	t.Helper()
	select {
	case <-p.finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the parked delivery callback never returned, so nothing below observes " +
			"what it decided")
	}
}

// T1. TestRetiredDrainMustNotTransmitAPastPubRecSource
//
// A callback dispatched before retirement, paused before it takes sess.mu, and released
// after retirement has completed. processUnsub does not reach it — the *subscription it
// already holds stays valid — so without the delivery barrier it observes closed==false and
// an absent drain, falls through the drain switch, and reaches ordinary QoS 1/2 PUBLISH
// processing: a duplicate PUBLISH of an exchange the client has already answered, under a
// FRESH packet identifier.
//
// The oracle is the packet-identifier allocation, because that is what the fall-through
// does and what a withheld delivery does not.
func TestRetiredDrainMustNotTransmitAPastPubRecSource(t *testing.T) {
	s, c, sess, _ := drainTestSession(t, "drain-barrier-no-transmit")

	bindDrainDeliveryClient(t, s, c, sess)

	const dur, sid, filter = "dur-barrier", "sid", "barrier.topic"
	d := emptyDrainWithARealConsumer(t, sess, sid, dur, filter)
	inbox := mqttSubPrefix + dur

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restoreDel := setMQTTTestDrainDeleteHook(obs.hook)
	defer restoreDel()

	// The real delivery subscription, installed the way a resume installs it.
	sess.restoreDrains(c, nil)
	c.mu.Lock()
	sub := c.subs[inbox]
	c.mu.Unlock()
	if sub == nil {
		t.Fatal("the fixture needs a real delivery subscription, or no callback can be parked")
	}
	sess.mu.Lock()
	preClosed := sub.mqtt != nil && sub.mqtt.closed
	sess.mu.Unlock()
	if preClosed {
		t.Fatal("the subscription must start OPEN, or the barrier assertion is vacuous")
	}

	// The live control, installed BEFORE the schedule so it is ready to serve as the
	// ordering barrier the moment the parked callback is released.
	const liveDur, liveFilter = "dur-barrier-live", "barrier.live"
	installLiveControlConsumer(t, sess, c, "sid-live", liveDur, liveFilter)

	park := newDrainDeliveryPark(inbox)
	restorePark := setMQTTTestDeliveryPreLockHook(park.hook)
	defer restorePark()
	restoreDone := setMQTTTestDeliveryDoneHook(park.done)
	defer restoreDone()

	// A real delivery on the drained filter, through the real consumer.
	storeQoS12Source(t, sess, filter, 2, "past-pubrec")
	select {
	case <-park.arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery callback arrived, so this test never reached its schedule")
	}

	// Retirement runs to completion while the callback is parked.
	sess.finishDrainIfEmpty(c, d)
	sess.mu.Lock()
	closedNow := sub.mqtt != nil && sub.mqtt.closed
	drainGone := sess.drainByDur[dur] == nil
	sess.mu.Unlock()
	// drainGone is a genuine PREMISE: it must hold in every build, mutated or not, or the
	// fall-through is not reachable and the assertion below is vacuous.
	if !drainGone {
		t.Fatal("retirement must drop the reverse index, or the fall-through is not reachable")
	}
	// closedNow is deliberately NOT asserted on its own. It was, and a review round showed
	// why that was wrong: M1 then failed HERE, on the flag, and the transmission oracle
	// below was never exercised at all. It is carried into that oracle's failure message as
	// diagnostic context instead, so the assertion that discriminates is the one about what
	// the callback actually did.

	// Release it, and WAIT FOR IT TO RETURN before asserting it took nothing. This is the
	// only honest ordering: the completion seam fires from the callback's own deferred exit,
	// so there is no timeout and no proxy observation involved.
	close(park.release)
	park.awaitCompletion(t)

	// THE ASSERTION, keyed on the RETIRED durable rather than on a total count.
	if got := pendingForDurable(sess, dur); len(got) != 0 {
		t.Fatalf("a callback released after retirement TRANSMITTED: packet identifier(s) %v "+
			"allocated for the retired durable %q. That is a duplicate PUBLISH of an exchange "+
			"the client has already answered, under a FRESH identifier. "+
			"(delivery barrier closed=%v at the end of retirement)", got, dur, closedNow)
	}
	// The positive control, identified by its OWN durable so "something was delivered"
	// cannot be satisfied by the very delivery this test forbids. It proves the fixture can
	// still deliver; it is NOT the completion proof, which is awaitCompletion above.
	awaitDeliveryBarrier(t, sess, liveDur, liveFilter, "control")
	if got := pendingForDurable(sess, liveDur); len(got) == 0 {
		t.Fatalf("the live control durable %q delivered nothing, so the absence above is a "+
			"fact about the fixture rather than about the retired drain", liveDur)
	}
}

// T1b. TestRetiredDrainBarrierMustBeAtomicWithTheMapDrop
//
// What a barrier proves is not that it EXISTS but that it is atomic with the map drop, and
// one mutation cannot measure both halves. This is the schedule for the second half: the
// callback is released from retirement's own post-unlock seam, which is the earliest instant
// at which anything outside the critical section can observe the session — so a barrier
// applied any later than the map drop is a barrier the callback gets past.
//
// T1 cannot supply this: it releases the callback after retirement has entirely returned, by
// which time a late barrier has been applied too.
func TestRetiredDrainBarrierMustBeAtomicWithTheMapDrop(t *testing.T) {
	s, c, sess, _ := drainTestSession(t, "drain-barrier-atomic")

	bindDrainDeliveryClient(t, s, c, sess)

	const dur, sid, filter = "dur-barrier-atomic", "sid", "atomic.topic"
	d := emptyDrainWithARealConsumer(t, sess, sid, dur, filter)
	inbox := mqttSubPrefix + dur

	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restoreDel := setMQTTTestDrainDeleteHook(obs.hook)
	defer restoreDel()

	sess.restoreDrains(c, nil)
	c.mu.Lock()
	sub := c.subs[inbox]
	c.mu.Unlock()
	if sub == nil {
		t.Fatal("the fixture needs a real delivery subscription")
	}

	const liveDur, liveFilter = "dur-atomic-live", "atomic.live"
	installLiveControlConsumer(t, sess, c, "sid-live", liveDur, liveFilter)

	park := newDrainDeliveryPark(inbox)
	restorePark := setMQTTTestDeliveryPreLockHook(park.hook)
	defer restorePark()
	restoreDone := setMQTTTestDeliveryDoneHook(park.done)
	defer restoreDone()

	storeQoS12Source(t, sess, filter, 2, "past-pubrec")
	select {
	case <-park.arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery callback arrived, so this test never reached its schedule")
	}

	// Release the parked callback from INSIDE retirement, at the first moment sess.mu is
	// free, and let it get all the way to its exit before retirement continues. Whatever a
	// build does after this point cannot help it.
	var observedClosed, observedDrain bool
	sawSeam := make(chan struct{})
	restoreSeam := setMQTTTestDrainRetirementUnlockedHook(func(string) {
		close(sawSeam)
		// PREMISE, not the assertion: what the session looks like at the earliest instant
		// anything outside the critical section can observe it.
		sess.mu.Lock()
		observedClosed = sub.mqtt != nil && sub.mqtt.closed
		observedDrain = sess.drainByDur[dur] != nil
		sess.mu.Unlock()
		// Release the callback HERE, and WAIT FOR IT TO RETURN before retirement continues.
		// Waiting inside the seam is what makes this the atomicity test: a build that
		// applies the barrier anywhere after this point cannot apply it before the callback
		// has already decided. Returning from the seam first would let such a closure land
		// in time and the mutation would survive.
		close(park.release)
		park.awaitCompletion(t)
	})
	defer restoreSeam()

	sess.finishDrainIfEmpty(c, d)
	restoreSeam()

	select {
	case <-sawSeam:
	default:
		t.Fatal("the post-unlock seam never fired, so this test never reached its schedule")
	}
	// PREMISE: the map drop must already have happened at this seam, in every build.
	if observedDrain {
		t.Fatal("the map drop must already have happened at the post-unlock seam")
	}
	// observedClosed is diagnostic only, for the reason given in T1: asserted on its own it
	// takes the kill and the transmission oracle below never runs.

	if got := pendingForDurable(sess, dur); len(got) != 0 {
		t.Fatalf("the callback released between the map drop and retirement's return "+
			"TRANSMITTED: packet identifier(s) %v allocated for the retired durable %q. The "+
			"barrier is therefore not atomic with the map drop — a build that applies it "+
			"later leaves exactly this window. (barrier closed=%v at the seam)",
			got, dur, observedClosed)
	}
	// The positive control, and ONLY a control: the completion proof is awaitCompletion
	// inside the seam above, not this delivery.
	awaitDeliveryBarrier(t, sess, liveDur, liveFilter, "control")
	if got := pendingForDurable(sess, liveDur); len(got) == 0 {
		t.Fatalf("the live control durable %q delivered nothing, so the absence above is a "+
			"fact about the fixture rather than about the barrier", liveDur)
	}
}

// T10. TestCrashInsideDrainDisposalLeavesOrphanCleanupOnly
//
// A crash anywhere inside disposal leaves ONE durable state: the record names no drain,
// because the save that cleared the last claim removed the entry (I3), and the consumer may
// survive. That is orphan-sweep work and nothing else.
//
// **No write happens after the simulated crash**, which is the whole point and which an
// earlier version got wrong: it retired the drain and then explicitly saved, which
// guaranteed an empty restored record whatever the last-claim save had done. Here the record
// is written by the RECONCILIATION save — production code, clearing the last claim — and
// nothing writes after it. The reconstruction reads exactly the bytes that boundary stored.
//
// This PROVES CLEANUP ONLY. It cannot detect a broken callback barrier, because a restart
// destroys the callbacks; T1 and T1b are what cover that and this does not stand in for them.
func TestCrashInsideDrainDisposalLeavesOrphanCleanupOnly(t *testing.T) {
	s, c, sess, _ := drainTestSession(t, "drain-crash-in-disposal")

	// A real consumer and a CLAIMED sequence the stream no longer has, so reconciliation
	// prunes the claim and the save that lands is the one that omits the now-empty drain.
	const dur, sid, filter = "dur-crash-disposal", "sid", "crash.topic"
	cfg := realDrainConsumer(t, sess, dur, filter)
	d := addTestDrain(sess, sid, dur, filter, 2, map[uint64]*drainSeq{
		41: {claimed: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	sess.mu.Lock()
	d.cc = cfg
	sess.mu.Unlock()

	// THE CRASH, modelled as the delete request never being issued — the same durable
	// outcome as a crash before it, during it, or with its reply lost.
	obs := &drainDisposalObserver{prefix: "dur-", over: true}
	restoreDel := setMQTTTestDrainDeleteHook(obs.hook)
	defer restoreDel()

	sess.recoverDrains(c, nil)

	if got := obs.durables(); len(got) != 1 || got[0] != dur {
		t.Fatalf("reconciliation must have emptied and retired the drain, got disposals %v", got)
	}
	if !drainConsumerExists(t, sess, dur) {
		t.Fatal("the suppressed delete must leave the durable alive, or there is no crash residue")
	}

	// The restart: a fresh session object built from the record the reconciliation save
	// stored. NOTHING has written since.
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	restored, _, err := asm.createOrRestoreSession("drain-crash-in-disposal", s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	restored.mu.Lock()
	restoredDrains := len(restored.drains)
	restored.mu.Unlock()
	if restoredDrains != 0 {
		t.Fatalf("the record must name no drain for a retired sid, got %d restored: an empty "+
			"drain is omitted from the record by the very save that cleared its last claim (I3), "+
			"which is what makes the durable unreferenced BEFORE it is deleted",
			restoredDrains)
	}

	// So the survivor is orphan-cleanup work, and the sweep is what reclaims it.
	asm.sweepOrphanedConsumersBefore(s, time.Now().Add(time.Minute))
	if drainConsumerExists(t, sess, dur) {
		t.Fatal("the orphaned-consumer sweep must reclaim the durable a crashed disposal left: " +
			"it is named by no resident object and by no session record")
	}
}
