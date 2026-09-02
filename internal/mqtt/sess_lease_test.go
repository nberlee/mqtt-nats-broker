package mqtt

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The incarnation completion lease (F-036 unit 1). These are the mechanism's own
// tests: the ledger's semantics, the permit that makes property P mechanical, and the
// structural guards that keep the acquisition boundaries boundaries rather than lists.
//
// Each assertion is POSITIVE about the thing that would silently regress. "The client
// received nothing" also passes when nothing was ever sent; "the owner count is 1 and
// child creation refuses" does not.

func testTrackerCounts(t *testing.T, tr *mqttIncarnationTracker) (state uint8, owners, promoted int) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.state, tr.owners, tr.promoted
}

// A released lease can never mint an owner again. This is what makes "transfer
// ownership before you release it" a mechanical fact: once the drain proof exists —
// the count at zero — no capability able to raise it survives. A copyable lease value
// would break exactly this (plan round 3, finding 1).
func TestMQTTLeaseReleaseRevokesTheCapability(t *testing.T) {
	t.Parallel()
	tr := newIncarnationTracker()
	l, ok := tr.acquire()
	if !ok {
		t.Fatal("an open tracker must admit")
	}
	if _, owners, _ := testTrackerCounts(t, tr); owners != 1 {
		t.Fatalf("owners must be 1 while the lease is held, got %d", owners)
	}
	l.release()
	if _, owners, _ := testTrackerCounts(t, tr); owners != 0 {
		t.Fatalf("owners must be 0 after release, got %d", owners)
	}
	// Idempotent: a second release must not drive the count negative, which would
	// make a later drain unreachable.
	l.release()
	if _, owners, _ := testTrackerCounts(t, tr); owners != 0 {
		t.Fatalf("a repeated release must not change the count, got %d", owners)
	}
}

// Promotion revokes too, and its arithmetic is what lets a self-retiring caller drain:
// the predicate is owners-promoted == 0, so a sole promoted owner reads (1,1) and the
// drain completes rather than waiting on the caller performing it (plan round 5,
// finding 1).
func TestMQTTLeasePromotionDrainsAndRevokes(t *testing.T) {
	t.Parallel()
	tr := newIncarnationTracker()
	self, ok := tr.acquire()
	if !ok {
		t.Fatal("an open tracker must admit")
	}
	tr.close()
	// Not drained yet: the owner is still counted as an owner.
	select {
	case <-tr.drained:
		t.Fatal("the drain must not complete while an unpromoted owner is held")
	default:
	}
	tr.promote(self)
	if _, owners, promoted := testTrackerCounts(t, tr); owners != 1 || promoted != 1 {
		t.Fatalf("promotion keeps owners and adds promoted debt, got owners=%d promoted=%d",
			owners, promoted)
	}
	select {
	case <-tr.drained:
	default:
		t.Fatal("a sole promoted owner must satisfy the drain predicate")
	}
	// And the promoted lease's own deferred release is harmless.
	self.release()
	if _, owners, promoted := testTrackerCounts(t, tr); owners != 1 || promoted != 1 {
		t.Fatalf("the promoted lease's release must be a no-op, got owners=%d promoted=%d",
			owners, promoted)
	}
}

// Every close cycle gets FRESH channels. A retry that observed a previous cycle's
// closed drain channel would proceed with a real owner outstanding, which is the
// mechanism's central failure (plan round 1, finding 1).
func TestMQTTTrackerReopenAllocatesAFreshCycle(t *testing.T) {
	t.Parallel()
	tr := newIncarnationTracker()
	tr.close()
	firstDrained, firstDecided := tr.drained, tr.decided
	select {
	case <-firstDrained:
	default:
		t.Fatal("a tracker with no owners must drain at once")
	}
	tr.reopen()
	if state, _, _ := testTrackerCounts(t, tr); state != trackerOpen {
		t.Fatalf("reopen must re-admit, state=%d", state)
	}
	if tr.drained == firstDrained || tr.decided == firstDecided {
		t.Fatal("reopen must allocate fresh cycle channels, or a later transition " +
			"observes this cycle's completed drain")
	}
	select {
	case <-firstDecided:
	default:
		t.Fatal("the previous cycle's waiters must be woken")
	}
	if !tr.wasReopened(firstDecided) {
		t.Fatal("the previous cycle must report REOPENED, so a parked route retries " +
			"rather than refusing")
	}
	// A second cycle must wait on its own channel.
	l, _ := tr.acquire()
	tr.close()
	select {
	case <-tr.drained:
		t.Fatal("the new cycle must not read as drained while an owner is held")
	default:
	}
	l.release()
	select {
	case <-tr.drained:
	default:
		t.Fatal("the new cycle must drain when its owner releases")
	}
}

// A closing or retired tracker refuses ordinary acquisition. This is the admission
// half of the invariant: a displaced route cannot be admitted once its incarnation is
// being retired.
func TestMQTTTrackerRefusesWhileClosingAndRetired(t *testing.T) {
	t.Parallel()
	tr := newIncarnationTracker()
	tr.close()
	if _, ok := tr.acquire(); ok {
		t.Fatal("a closing tracker must refuse admission")
	}
	tr.resolveRetired()
	if _, ok := tr.acquire(); ok {
		t.Fatal("a retired tracker must refuse admission")
	}
	if !tr.wasReopened(make(chan struct{})) {
		// A stale cycle's channel reports "reopened" so its waiters retry against the
		// current state rather than concluding retirement from another cycle.
		t.Log("a foreign channel is treated as a stale cycle, which is the safe answer")
	}
}

// The retirement-pending mark and the drain inspection are ONE critical section: if the
// last owner releases between a caller's ceiling and the install, the installer itself
// schedules the finisher, because no later release exists to do it (plan round 4,
// finding 2). Without that arm the obligation is stranded.
func TestMQTTTrackerPendingInstallOnAnAlreadyDrainedCycle(t *testing.T) {
	t.Parallel()
	tr := newIncarnationTracker()
	l, _ := tr.acquire()
	tr.close()
	// The ceiling fires here in production; meanwhile the last owner releases.
	l.release()
	drained, armed := tr.armRetirementPending(func() {})
	if !armed {
		t.Fatal("a closing tracker must accept the obligation")
	}
	// The channel handed back is THIS cycle's, and it is already closed: the worker
	// the caller admits eagerly therefore runs immediately rather than waiting for a
	// release that will never come, which is what stranded the obligation before.
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("arming must hand back the cycle's own drained channel, already closed " +
			"when the last owner released before the arm; otherwise the eagerly " +
			"admitted worker waits forever and the parked discard is never completed")
	}
	// The obligation stays claimable exactly once: the scheduled finisher and a CONNECT
	// that finds the mark arbitrate through the claim.
	if _, ok := tr.takeRetirementPending(); !ok {
		t.Fatal("the obligation must still be claimable by whoever wins the gate")
	}
	if _, ok := tr.takeRetirementPending(); ok {
		t.Fatal("the obligation must be claimable only once")
	}
}

// The permit is what makes property P mechanical. It refuses while a client is bound —
// which is the exact violation F-036's clean-start arm committed for years — and while
// any owner remains.
func TestMQTTRetirementPermitEnforcesPropertyP(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-permit")
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	e.sess.leases.close()

	e.sess.mu.Lock()
	_, err := e.sess.retireIncarnationLocked()
	e.sess.mu.Unlock()
	if err == nil {
		t.Fatal("the permit must refuse while a client is bound: that is property P, " +
			"and re-minting there is what lost an acknowledged QoS 2 message")
	}

	// Detached but with an owner outstanding: still refused, and with the distinct
	// not-drained reason.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()
	l, ok := e.sess.leases.acquire()
	if ok {
		t.Fatal("a closing tracker must not admit; the owner has to predate the close")
	}
	_ = l
	e.sess.leases.reopen()
	held, ok := e.sess.leases.acquire()
	if !ok {
		t.Fatal("a reopened tracker must admit")
	}
	e.sess.leases.close()
	e.sess.mu.Lock()
	_, err = e.sess.retireIncarnationLocked()
	e.sess.mu.Unlock()
	if !errors.Is(err, errMQTTTrackerNotDrained) {
		t.Fatalf("the permit must refuse while an owner remains, got %v", err)
	}
	held.release()
	e.sess.mu.Lock()
	_, err = e.sess.retireIncarnationLocked()
	e.sess.mu.Unlock()
	if err != nil {
		t.Fatalf("the permit must be granted once unbound and drained, got %v", err)
	}
}

// A retirement gives the FRESH incarnation a fresh tracker, and the displaced
// connection keeps naming the retired one. That is the discrimination the whole design
// rests on: after the re-mint a displaced packet acquires its own retired tracker and is
// refused, rather than acquiring the incarnation it must not serve.
func TestMQTTRetirementInstallsAFreshTrackerAndRefusesTheDisplaced(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-fresh-tracker")
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	e.c.mqtt = &mqtt{sess: e.sess, tracker: e.sess.leases}
	displaced := e.c.mqtt.tracker

	if err := mqttTestRetireBound(t, e.asm, e.sess, true); err != nil {
		t.Fatalf("retire: %v", err)
	}

	e.sess.mu.Lock()
	fresh := e.sess.leases
	bound := e.sess.c
	e.sess.mu.Unlock()
	if bound != nil {
		t.Fatal("the retirement must leave the object unbound")
	}
	if fresh == displaced {
		t.Fatal("the fresh incarnation must have its own tracker, or a late release " +
			"decrements the incarnation it does not belong to")
	}
	if state, _, _ := testTrackerCounts(t, displaced); state != trackerRetired {
		t.Fatalf("the displaced tracker must be RETIRED, state=%d", state)
	}
	if state, _, _ := testTrackerCounts(t, fresh); state != trackerOpen {
		t.Fatalf("the fresh tracker must admit, state=%d", state)
	}
	// The displaced connection's own acquisition — what its next packet does — refuses.
	if l, ok := e.c.acquirePacketLease(); ok {
		l.release()
		t.Fatal("a packet from the displaced connection must be refused after the " +
			"re-mint; acquiring here is the silent-loss path")
	}
	// While a route admitted by the fresh incarnation proceeds.
	l, ok := fresh.acquire()
	if !ok {
		t.Fatal("the fresh incarnation must admit its own routes")
	}
	l.release()
}

// STRUCTURAL GUARDS. These are what keep the acquisition boundaries boundaries: a new
// call site of any of them must fail here rather than silently escape the mechanism.
func TestMQTTLeaseBoundariesAreStructural(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	names, gerr := filepath.Glob("*.go")
	if gerr != nil {
		t.Fatalf("globbing the package: %v", gerr)
	}
	files := map[string]*ast.File{}
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		files[name] = f
	}
	if len(files) == 0 {
		t.Fatal("no non-test sources parsed; this guard cannot check what it claims to")
	}

	counts := map[string]int{}
	for name, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, _ := n.(*ast.CallExpr)
			if call == nil {
				return true
			}
			sel, _ := call.Fun.(*ast.SelectorExpr)
			if sel == nil {
				return true
			}
			switch sel.Sel.Name {
			case "setIncarnationLocked", "retireIncarnationLocked", "leasedHandler",
				"acquirePacketLease", "promote":
				counts[sel.Sel.Name]++
				_ = name
			}
			return true
		})
	}

	// setIncarnationLocked is the ONLY writer of sess.leases and the only re-mint, and
	// it requires a permit. Two callers: the constructor (an object nothing else can
	// reach) and clear()'s destructive half.
	if got := counts["setIncarnationLocked"]; got != 2 {
		t.Errorf("setIncarnationLocked must have exactly 2 callers (the constructor and "+
			"clear()'s destructive half); found %d. A third would be a re-mint outside "+
			"the permit, which is property P's only enforcement point", got)
	}
	// The permit's mint sites: clear()'s composition, the bound retirement, and the
	// pending-discard finisher.
	if got := counts["retireIncarnationLocked"]; got != 3 {
		t.Errorf("retireIncarnationLocked must be called at exactly 3 sites (clear(), "+
			"the bound retirement, the pending-discard finisher); found %d", got)
	}
	// The delivery boundary: sess.processSub's wrapper plus the one PUBREL registration
	// made outside it. A third would be a session delivery callback registered without
	// completion ownership.
	if got := counts["leasedHandler"]; got != 2 {
		t.Errorf("leasedHandler must wrap at exactly 2 sites (sess.processSub, and the "+
			"PUBREL delivery registered outside it); found %d. A session delivery "+
			"callback registered without it escapes the boundary entirely", got)
	}
	// One packet-dispatch acquisition, at the single pre-dispatch point.
	if got := counts["acquirePacketLease"]; got != 1 {
		t.Errorf("acquirePacketLease must be called at exactly 1 site (the single "+
			"pre-dispatch point in mqttParse); found %d", got)
	}
	// One promotion: the expiry fire, which is the only caller that retires the
	// incarnation it is itself holding.
	if got := counts["promote"]; got != 1 {
		t.Errorf("promote must be called at exactly 1 site (the expiry fire, the one "+
			"self-retiring owner); found %d", got)
	}
}

// The queued-transport classification (scope consult 3). What keeps those effects safe
// is a property of their TARGET, so the guards are over the producers and the target
// shapes, not over an ownership parameter the design deliberately does not have.
func TestMQTTQueuedEffectProducersAreCensused(t *testing.T) {
	t.Parallel()
	src := testReadPackageSources(t)

	// The five sendq.push producers. A sixth would be a queued transport class nothing
	// has classified.
	if got := strings.Count(src, "sendq.push("); got != 5 {
		t.Errorf("expected exactly 5 sendq.push producers (the JS API fan-out, the "+
			"atomic batch, sendMsg, the no-wait purge and the no-wait deleteMsg); found "+
			"%d. A new one needs a target classification in ARCHITECTURE.md", got)
	}
	// sess.deleteConsumer's no-wait delete is safe because the durable is
	// per-consumer unique AND non-shared; both halves are load-bearing.
	if !strings.Contains(src, "// Shared group consumers are NOT deleted here") &&
		!strings.Contains(src, "refundConsumerChargeLocked") {
		t.Error("sess.deleteConsumer's shared-consumer restriction is what makes its " +
			"no-wait delete identity-safe; the guard could not find it")
	}
	// The plain durable's uniqueness, which is what a successor cannot recreate.
	if !strings.Contains(src, "nuid.Next()") {
		t.Error("the plain durable's per-consumer uniqueness is the identity-safety " +
			"argument for the no-wait deletes; the guard could not find its mint")
	}
}

func testReadPackageSources(t *testing.T) string {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("globbing the package: %v", err)
	}
	var b strings.Builder
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, rerr := os.ReadFile(name)
		if rerr != nil {
			t.Fatalf("reading %s: %v", name, rerr)
		}
		b.Write(data)
	}
	if b.Len() == 0 {
		t.Fatal("no sources read; this guard cannot check what it claims to")
	}
	return b.String()
}

// A disposition after the retirement must consume the CAPSULE. Rediscovering the
// displaced client through sess.c finds nil — the detach already ran — and silently
// drops its close, its notification, its Will, its disconnect stamp and its expiry,
// which is exactly the way the fourth attempt's early detach failed (its finding 4).
func TestMQTTDisposeCapturedUsesTheCapsuleNotTheSession(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-capsule")
	displaced := e.c
	action := &mqttDeferredDisconnect{disconnectedAt: 1234, expiryInterval: 0}

	// The state a retirement leaves behind: detached, with the identity in hand.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.deferredDisconnect = nil
	e.sess.mu.Unlock()
	capsule := &mqttRetiredBinding{
		client: displaced,
		action: action,
		fence:  &mqttWillFence{gen: "retiring-gen", seq: 7},
	}

	d := e.asm.disposeCaptured(e.sess, capsule, true)
	if !d.applied {
		t.Fatal("a captured disposition is past its identity check by construction")
	}
	if d.stale != displaced {
		t.Fatalf("the disposition must name the CAPSULE's client (%p), got %p — reading "+
			"sess.c here yields nil and the displaced connection is never closed, "+
			"notified or its Will discharged", displaced, d.stale)
	}
	if d.action != action {
		t.Fatal("the disposition must carry the capsule's parked action: it can hold the " +
			"only copy of the Will, with the deadline it was parked with")
	}
	if d.fence == nil || d.fence.gen != "retiring-gen" {
		t.Fatal("the disposition must carry the RETIRING incarnation's Will fence")
	}
	// A successor is held in hand, which is the only thing that licenses discarding a
	// live Will (F-084).
	if d.will != mqttWillActionDiscard {
		t.Fatalf("a licensed capture with a live connection discards the Will, got %v", d.will)
	}
	// And unlicensed, with no connection, the parked action is DISCHARGED rather than
	// dropped.
	capsule2 := &mqttRetiredBinding{action: action}
	if d2 := e.asm.disposeCaptured(e.sess, capsule2, false); d2.will != mqttWillActionDischargeDeferred {
		t.Fatalf("a parked action with no connection must be discharged, got %v", d2.will)
	}
}

// The delivery boundary's refusal must not itself write into the fresh incarnation.
// recordWithheldDeliveryLocked deliberately admits sess.c == nil and RECREATES
// sess.deferred, so taking it on a RETIRED tracker is precisely the stale in-memory
// write this whole mechanism exists to stop (plan round 6, finding 1).
func TestMQTTLeasedHandlerRefusalDoesNotWriteTheFreshIncarnation(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-refusal")
	sub := &subscription{
		client: e.c,
		mqtt:   &mqttSub{jsDur: e.sess.idHash + "_plain", qos: 1},
	}
	reply := "$JS.ACK." + mqttStreamName + "." + sub.mqtt.jsDur + ".1.9.9.0.0"

	called := 0
	h := e.sess.leasedHandler(func(*subscription, *client, *Account, string, string, []byte) {
		called++
	}, true)

	// CLOSING: the delivery is refused, and the withheld record IS owed — it keeps the
	// eventual redelivery a first transmission for DUP purposes, and it lands on the
	// retiring object, which the destroy then nils.
	e.sess.leases.close()
	h(sub, e.c, nil, "topic", reply, nil)
	if called != 0 {
		t.Fatal("a refused delivery must not run its callback")
	}
	e.sess.mu.Lock()
	recorded := len(e.sess.deferred)
	e.sess.deferred = nil
	e.sess.mu.Unlock()
	if recorded != 1 {
		t.Fatalf("a CLOSING refusal owes the withheld-delivery record, got %d entries", recorded)
	}

	// RETIRED: nothing may be written. The object beyond this point is the FRESH
	// incarnation's.
	e.sess.leases.resolveRetired()
	h(sub, e.c, nil, "topic", reply, nil)
	if called != 0 {
		t.Fatal("a refused delivery must not run its callback")
	}
	e.sess.mu.Lock()
	afterRetired := len(e.sess.deferred)
	e.sess.mu.Unlock()
	if afterRetired != 0 {
		t.Fatalf("a RETIRED refusal must record NOTHING: recordWithheldDeliveryLocked "+
			"recreates sess.deferred on the reused object, which is the stale write the "+
			"lease exists to prevent; got %d entries", afterRetired)
	}
}

// THE CENTRAL CLAIM: the retirement WAITS for an admitted route. Everything else in
// this mechanism is bookkeeping around this one property — an already-admitted route
// of the displaced incarnation completes before the generation is re-minted, so it can
// never derive a subject, answer a packet, or mutate a map under the fresh incarnation.
// That is the property F-036's clean-start arm violated, and losing an acknowledged
// QoS 2 message was the observable cost.
func TestMQTTRetirementWaitsForAnAdmittedRoute(t *testing.T) {
	e := testNewInflightEnv(t, "lease-drain-waits")
	prev := mqttIncarnationDrainTimeout
	mqttIncarnationDrainTimeout = 3 * time.Second
	t.Cleanup(func() { mqttIncarnationDrainTimeout = prev })

	e.sess.mu.Lock()
	e.sess.c = e.c
	gen := e.sess.gen
	e.sess.mu.Unlock()

	// An admitted route, parked mid-flight: exactly what a displaced connection's
	// in-flight PUBREL is when a clean-start CONNECT arrives.
	admitted, ok := e.sess.leases.acquire()
	if !ok {
		t.Fatal("an open incarnation must admit")
	}

	done := make(chan error, 1)
	go func() { done <- mqttTestRetireBound(t, e.asm, e.sess, true) }()

	select {
	case err := <-done:
		t.Fatalf("the retirement must WAIT for the admitted route; it returned %v with "+
			"an owner still outstanding, which is the re-mint-under-a-live-route that "+
			"loses an acknowledged QoS 2 message", err)
	case <-time.After(300 * time.Millisecond):
		// Still waiting, which is the point.
	}
	// The generation has NOT moved while the route is in flight.
	e.sess.mu.Lock()
	stillGen := e.sess.gen
	stillBound := e.sess.c
	e.sess.mu.Unlock()
	if stillGen != gen {
		t.Fatal("the generation must not be re-minted while an admitted route is in flight")
	}
	if stillBound != e.c {
		t.Fatal("the incumbent must stay bound until the drain completes")
	}

	admitted.release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the retirement must complete once the route released, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the retirement must complete once its last owner released")
	}
	e.sess.mu.Lock()
	newGen := e.sess.gen
	e.sess.mu.Unlock()
	if newGen == gen {
		t.Fatal("the retirement must re-mint once drained")
	}
}

// The ceiling REFUSES rather than disposing under a live owner: the incumbent keeps its
// binding, its Will, its socket and its expiry, admission reopens so its parked routes
// resume, and the contender is turned away. This is the disposition the fourth attempt
// could not supply, and it is why a wedged owner degrades availability instead of
// destroying state.
func TestMQTTRetirementCeilingRefusesAndPreservesTheIncumbent(t *testing.T) {
	e := testNewInflightEnv(t, "lease-ceiling")
	prev := mqttIncarnationDrainTimeout
	mqttIncarnationDrainTimeout = 250 * time.Millisecond
	t.Cleanup(func() { mqttIncarnationDrainTimeout = prev })

	e.sess.mu.Lock()
	e.sess.c = e.c
	gen := e.sess.gen
	e.sess.mu.Unlock()

	wedged, ok := e.sess.leases.acquire()
	if !ok {
		t.Fatal("an open incarnation must admit")
	}
	defer wedged.release()

	err := mqttTestRetireBound(t, e.asm, e.sess, true)
	if !errors.Is(err, errMQTTClearDrainTimeout) {
		t.Fatalf("a wedged owner must produce the drain-ceiling refusal, got %v", err)
	}
	e.sess.mu.Lock()
	boundAfter, genAfter := e.sess.c, e.sess.gen
	tr := e.sess.leases
	e.sess.mu.Unlock()
	if boundAfter != e.c {
		t.Fatal("the ceiling must leave the INCUMBENT bound: detaching it here would " +
			"strand its close, notification, Will, disconnect stamp and expiry with no " +
			"successor established (F-084)")
	}
	if genAfter != gen {
		t.Fatal("the ceiling must not re-mint: that is disposal under a live owner")
	}
	if state, _, _ := testTrackerCounts(t, tr); state != trackerOpen {
		t.Fatalf("the ceiling must REOPEN admission so the incumbent's parked routes "+
			"resume serving it, state=%d", state)
	}
	// And the incumbent can be served again immediately.
	l, ok := tr.acquire()
	if !ok {
		t.Fatal("a reopened incarnation must admit the incumbent's next packet")
	}
	l.release()
}

// The UNBOUND callers drain too. Five of the six discard paths run on an unbound or
// transient object, and the plan's first shape — a non-waiting "discard permit" for
// them — was refuted: an outstanding delivery continuation can still recreate a map
// clear() nilled, or open a record transaction and save, AFTER the reset. So every
// caller waits, and this is the test that says so for the ordinary clear()
// composition rather than for the bound retirement.
func TestMQTTClearDrainsAnUnboundIncarnation(t *testing.T) {
	e := testNewInflightEnv(t, "lease-unbound-drain")
	prev := mqttIncarnationDrainTimeout
	mqttIncarnationDrainTimeout = 250 * time.Millisecond
	t.Cleanup(func() { mqttIncarnationDrainTimeout = prev })

	e.sess.mu.Lock()
	e.sess.c = nil
	gen := e.sess.gen
	e.sess.mu.Unlock()

	// A continuation of this incarnation, still in flight: an off-read-loop register
	// settle, a delivery callback past its own ownership check.
	outstanding, ok := e.sess.leases.acquire()
	if !ok {
		t.Fatal("an open incarnation must admit")
	}

	if err := e.sess.clear(true); !errors.Is(err, errMQTTClearDrainTimeout) {
		t.Fatalf("clear() must refuse rather than destroy while a continuation of this "+
			"incarnation is in flight, got %v", err)
	}
	e.sess.mu.Lock()
	genAfter := e.sess.gen
	e.sess.mu.Unlock()
	if genAfter != gen {
		t.Fatal("a refused discard must not have re-minted: the continuation would then " +
			"be writing into the fresh incarnation, which is what the drain prevents")
	}

	// Released, and the same call now completes.
	outstanding.release()
	if err := e.sess.clear(true); err != nil && !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("clear() must complete once the incarnation drained, got %v", err)
	}
	e.sess.mu.Lock()
	genFinal := e.sess.gen
	e.sess.mu.Unlock()
	if genFinal == gen {
		t.Fatal("the completed discard must re-mint")
	}
}

// The drain trigger must survive firing while the PARKING CALLER still holds the
// client-ID gate. The close path holds that gate across its own discard, so the common
// case is exactly this one: the ceiling parks the obligation, the last owner releases,
// and the scheduled finisher meets a gate held by the very path that parked it. A
// finisher that gave up there would strand a clean-session discard [MQTT-3.1.2-6] that
// no sweep reaches — the expiry sweep skips expiry-zero records — until some later
// CONNECT for this client ID happened to arrive.
func TestMQTTPendingDiscardFinisherRetriesThroughAHeldGate(t *testing.T) {
	e := testNewInflightEnv(t, "lease-pending-gate")
	prevItvl := mqttPendingDiscardRetryItvl
	mqttPendingDiscardRetryItvl = 5 * time.Millisecond
	t.Cleanup(func() { mqttPendingDiscardRetryItvl = prevItvl })

	// Park the obligation the way the ceiling does, on a drained closing tracker.
	e.sess.mu.Lock()
	e.sess.c = nil
	tr := e.sess.leases
	e.sess.mu.Unlock()
	tr.close()

	// The gate is held by the parking caller, exactly as mqttHandleClosedClient holds
	// it across its own clearInner.
	if err := e.asm.lockSession(e.sess, nil); err != nil {
		t.Fatalf("the fixture must hold the gate to reach the schedule under test: %v", err)
	}

	finished := make(chan struct{})
	e.s.armPendingDiscardWorker(tr, func() {
		// What the production finisher does, with the retry loop under test.
		e.s.pendingDiscardFinisher(e.asm, e.sess, tr)()
		close(finished)
	})

	select {
	case <-finished:
		t.Fatal("the finisher must not conclude while the gate is held: giving up there " +
			"is what strands the clean-session discard")
	case <-time.After(150 * time.Millisecond):
		// Still retrying, which is the point.
	}
	if !e.sess.retirementPendingArmed() {
		t.Fatal("the obligation must stay parked while the finisher cannot claim it")
	}

	// The parking caller finishes and releases the gate; the finisher then claims.
	e.asm.unlockSession(e.sess)
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the finisher must claim the obligation once the gate is released")
	}
	if e.sess.retirementPendingArmed() {
		t.Fatal("the obligation must be consumed by the finisher that claimed it")
	}
}

// The expiry fire samples the tracker UNDER sess.mu, because setIncarnationLocked
// replaces that pointer there: an unlocked read races a concurrent re-mint, and the
// -race detector only sees it on the interleaving where a CONNECT retires between the
// session resolution and the acquisition. It also must not leak its lease on any exit
// after acquiring it — a leaked owner leaves the incarnation permanently undrainable,
// so every later retirement of that session would refuse.
func TestMQTTExpiryFireDoesNotLeakItsLease(t *testing.T) {
	e := testNewInflightEnv(t, "lease-expiry-exit")

	// The rebound exit: a client is bound by the time the fire takes the gate, so the
	// reconnect owns the session and the fire returns.
	e.sess.mu.Lock()
	e.sess.c = e.c
	tr := e.sess.leases
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.sessByHash[e.sess.idHash] = e.sess
	e.asm.pendingExpiries[e.sess.idHash] = mqttPendingExpiry{
		timer: time.NewTimer(time.Hour), token: 1,
	}
	e.asm.mu.Unlock()

	e.asm.expireSessionFired(e.s, e.sess.idHash, 1)

	if _, owners, promoted := testTrackerCounts(t, tr); owners-promoted != 0 {
		t.Fatalf("the expiry fire must not leak its lease on the rebound exit: "+
			"owners=%d promoted=%d. A leaked owner makes this incarnation "+
			"permanently undrainable, so every later retirement refuses", owners, promoted)
	}
	// And the incarnation is still retirable, which is what the leak would destroy.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()
	if err := e.sess.clear(true); err != nil && !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("the incarnation must still be retirable after the expiry fire "+
			"returned, got %v", err)
	}
}

// A stale expiry callback must not stomp a FRESH incarnation's deadline. After the
// callback consumed its own timer token it can be descheduled across a clean-start
// re-mint and a new disconnect; resuming then, a bare re-arm would REPLACE the new
// session's legitimate expiry with a one-second retry, and the retry would expire a
// session that had just connected. The re-arm is therefore conditional on this fire's
// view still being current — same resident object, same tracker, no timer installed.
func TestMQTTStaleExpiryFireDoesNotStompAFreshDeadline(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-stale-expiry")

	e.sess.mu.Lock()
	e.sess.c = nil
	captured := e.sess.leases
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.sessByHash[e.sess.idHash] = e.sess
	e.asm.mu.Unlock()

	// The re-mint happens under the stale callback: its captured tracker is retired
	// and the object now carries a fresh one.
	if err := e.sess.clear(true); err != nil && !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("the fixture's re-mint must run: %v", err)
	}
	// The fresh incarnation's own legitimate deadline, as a later close would arm it.
	fresh := time.NewTimer(time.Hour)
	e.asm.mu.Lock()
	e.asm.pendingExpiries[e.sess.idHash] = mqttPendingExpiry{timer: fresh, token: 99}
	e.asm.mu.Unlock()

	// The stale fire resumes and finds its captured tracker retired.
	e.asm.rearmExpiryIfViewCurrent(e.s, e.sess.idHash, e.sess, captured)

	e.asm.mu.Lock()
	entry := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if entry.timer != fresh || entry.token != 99 {
		t.Fatal("a stale fire must not replace the fresh incarnation's deadline: doing " +
			"so arms a one-second expiry on a session that just connected, and the " +
			"retry then destroys its subscriptions and queued QoS state")
	}
}

// Retirement CANCELS the bounded transmission wait. That wait is capped at the
// phase-gate maximum — up to 30 seconds, well past the incarnation drain ceiling — so
// without this a delivery admitted just before a clean-start takeover would hold its
// lease long enough to make the takeover time out and refuse, again and again, for a
// reason that has nothing to do with the contender. The answer is the one an armed
// gate already gives: WITHHELD, which the caller records without acking.
func TestMQTTClosingCancelsTheTransmissionWait(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-closing-wait")
	e.c.mqtt = &mqtt{sess: e.sess, tracker: e.sess.leases}

	// An armed restore gate that nobody will open: without the closing arm this waits
	// out the whole cap.
	rg := &mqttRestoreGate{done: make(chan struct{})}
	e.sess.restoreGate.Store(rg)

	done := make(chan mqttTransmissionVerdict, 1)
	go func() { done <- e.sess.awaitTransmissionGates(e.c) }()

	select {
	case v := <-done:
		t.Fatalf("the wait must not resolve while the gate is armed and the incarnation "+
			"is open, got %v", v)
	case <-time.After(150 * time.Millisecond):
	}

	e.sess.leases.close()
	select {
	case v := <-done:
		if v != mqttTransmissionWithheld {
			t.Fatalf("a retiring incarnation must WITHHOLD the delivery — recorded, not "+
				"acked, so its redelivery is still a first transmission — got %v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing the incarnation must cancel the bounded transmission wait, or " +
			"a clean-start takeover refuses until an unrelated 30s cap expires")
	}
}

// A parked discard's claim is checked against the PAIR {session object, its tracker},
// never the client ID alone. While a finisher waits, an uncertainty resolver can evict
// its object as foreign and a CONNECT can install a REPLACEMENT under the same client
// ID; completing the discard against the stale object would then delete the
// replacement's map entries by ID and leave its live connection bound to a session
// nothing can resolve.
func TestMQTTPendingDiscardRefusesAStaleObject(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-pending-stale")

	e.sess.mu.Lock()
	e.sess.c = nil
	stale := e.sess
	captured := stale.leases
	e.sess.mu.Unlock()
	captured.close()
	if _, armed := captured.armRetirementPending(func() {}); !armed {
		t.Fatal("a closing tracker must accept the obligation")
	}

	// The replacement: same client ID, a different object, resident.
	replacement, _, err := e.asm.createOrRestoreSession(stale.id, e.s.getOpts())
	if err != nil {
		t.Fatalf("the fixture must be able to install a replacement: %v", err)
	}
	e.asm.mu.Lock()
	e.asm.sessions[stale.id] = replacement
	e.asm.sessByHash[stale.idHash] = replacement
	e.asm.mu.Unlock()

	// The delayed finisher now runs against the STALE object.
	e.s.finishPendingDiscard(e.asm, stale, captured)

	e.asm.mu.Lock()
	stillSessions := e.asm.sessions[stale.id]
	stillByHash := e.asm.sessByHash[stale.idHash]
	e.asm.mu.Unlock()
	if stillSessions != replacement || stillByHash != replacement {
		t.Fatal("a stale finisher must not evict the REPLACEMENT session: removing by " +
			"client ID here leaves a live connection bound to a session no lookup finds")
	}
}

// A REFUSED worker admission must still complete the discard, synchronously, rather
// than leaving the obligation in memory to die with the process. It cannot be
// recovered from the record: a clean session's record carries expiry interval zero and
// the expiry sweep skips exactly those, so nothing would ever reload it and its
// referenced consumers would keep the orphan sweep away from them indefinitely.
func TestMQTTPendingDiscardWorkerCompletesWhenAdmissionIsRefused(t *testing.T) {
	e := testNewInflightEnv(t, "lease-admission-refused")
	prev := mqttIncarnationDrainTimeout
	mqttIncarnationDrainTimeout = 2 * time.Second
	t.Cleanup(func() { mqttIncarnationDrainTimeout = prev })

	// Shutdown has closed worker admission.
	e.s.closeWillDispatch()

	e.sess.mu.Lock()
	e.sess.c = nil
	tr := e.sess.leases
	e.sess.mu.Unlock()
	tr.close()

	done := make(chan struct{})
	e.s.armPendingDiscardWorker(tr, func() { close(done) })

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a refused admission must complete the discard on the arming goroutine: " +
			"leaving it in memory loses a discard [MQTT-3.1.2-6] requires, and an " +
			"expiry-zero record is not a recovery anchor")
	}
}

// A re-parked refusal must be reported as such, because a JOINING CONNECT that treats
// it as "the discard is done" restores over an object that is still resident and still
// carries both the obligation and the unclassified write that refused it. For a
// session whose disconnect required the discard, that CONNECT could then report
// Session Present and resume state the client asked to have destroyed.
func TestMQTTPendingDiscardReparkedIsDistinctFromCompleted(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-pending-repark")

	e.sess.mu.Lock()
	e.sess.c = nil
	tr := e.sess.leases
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.sessions[e.sess.id] = e.sess
	e.asm.sessByHash[e.sess.idHash] = e.sess
	e.asm.mu.Unlock()
	tr.close()
	if _, armed := tr.armRetirementPending(func() {}); !armed {
		t.Fatal("a closing tracker must accept the obligation")
	}

	// The write the last owner armed before releasing: prepareClear must refuse on it.
	e.sess.mu.Lock()
	e.sess.seqUncertain = true
	e.sess.mu.Unlock()

	if got := e.s.finishPendingDiscard(e.asm, e.sess, tr); got != mqttPendingDiscardReparked {
		t.Fatalf("an unresolved write must produce the REPARKED outcome, so a joining "+
			"CONNECT rejects instead of restoring over the obligation; got %v", got)
	}
	// The object is still the client ID's session, with its obligation still owed.
	e.asm.mu.Lock()
	stillResident := e.asm.sessions[e.sess.id] == e.sess &&
		e.asm.sessByHash[e.sess.idHash] == e.sess
	e.asm.mu.Unlock()
	if !stillResident {
		t.Fatal("a re-parked discard must leave its object resident: it carries the " +
			"unclassified write, and only this object can classify it")
	}
	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect != nil
	e.sess.mu.Unlock()
	if !parked {
		t.Fatal("the refusal must hand the obligation to the uncertainty machinery, " +
			"whose resolution re-drives the discard")
	}
}

// A fired expiry callback must not destroy a LATER incarnation of the deadline. It can
// be delayed after passing its token check while the client reconnects and disconnects
// again; the reconnect resumes this same object, so neither the binding nor the record
// sequence tells the newer deadline from the one this fire was armed for. Only re-reading
// the deadline under the gate does, and without it the callback deletes a session whose
// lifetime has not elapsed, taking its subscriptions and queued QoS state with it.
func TestMQTTDelayedExpiryFireRespectsALaterDeadline(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lease-expiry-later-deadline")

	// The state a reconnect-and-redisconnect leaves: unbound, with a deadline that has
	// NOT elapsed.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.expiryInterval = 3600
	e.sess.disconnectedAt = time.Now().Unix()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.sessions[e.sess.id] = e.sess
	e.asm.sessByHash[e.sess.idHash] = e.sess
	e.asm.pendingExpiries[e.sess.idHash] = mqttPendingExpiry{
		timer: time.NewTimer(time.Hour), token: 7,
	}
	e.asm.mu.Unlock()

	// The delayed callback resumes, presenting the token it validated before the
	// reconnect.
	e.asm.expireSessionFired(e.s, e.sess.idHash, 7)

	e.asm.mu.Lock()
	stillResident := e.asm.sessions[e.sess.id] == e.sess
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	genAfter := e.sess.gen
	e.sess.mu.Unlock()
	if !stillResident {
		t.Fatal("a delayed fire must not evict a session whose deadline has not " +
			"elapsed: that destroys a live client's subscriptions and queued QoS state")
	}
	if genAfter != gen {
		t.Fatal("a delayed fire must not discard the session: the deadline it was armed " +
			"for belongs to an earlier disconnect, and the current one has not passed")
	}
}
