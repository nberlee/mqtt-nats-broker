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
	"sync"
	"time"
)

// Retained replay, executed by the session's driver (F-105 7c).
//
// SUBSCRIBE takes a SNAPSHOT of each matched retained message under its locks — the material, the
// effective QoS, the allocated identifier and its nonce, the subscription identifier — and STAGES it
// on the subscription. After SUBACK the staged items are TRANSFERRED to the subscription's replay
// object and the driver executes them in order: a QoS 0 item is one actionless frame; a QoS 1/2
// item is materialized (reserve, chunks, seal), admitted against its READY copy, acquired and
// written through the authorized boundary — the bytes are the snapshot's, already in memory and
// equal to what the seal verified, so nothing reads payload material after the acquisition.
//
// THE GATE. Live traffic on the RAW subscription (the QoS 0 callback) must not overtake the
// retained replay, which is the guarantee prm's flush-first gave and the only one it gave (live
// QoS 1/2 rides the JetStream inbox and could already overtake). The replay object carries a gate
// that the QoS 0 callback waits on, locks released and cancelled by the connection's closing
// signal; it closes when both item lists are empty, and that decision is made under c.mu together
// with every append, transfer, replacement and detach, so none can slip between "observed empty"
// and "gate closed".
//
// ONE OBJECT PER SUBSCRIPTION LINEAGE. A same-sid replacement inherits the object (its executing
// items and its waiting callbacks) and stages its own snapshot after them; a departure (the sid
// deleted) marks it departed. Everything here is guarded by the owning connection's c.mu.

// mqttRetainedReplay is one subscription lineage's outstanding retained replay.
type mqttRetainedReplay struct {
	// exec are the items published to the driver, in order; staged items live on the
	// subscription's prm until its packet's transfer moves them here.
	exec []mqttRetainedFrame
	// sub is the subscription currently holding this object.
	sub *subscription
	// gate is closed exactly once, when the object retires.
	gate chan struct{}
	// queued says the driver has this object in its queue.
	queued bool
	// departed: the sid left (not replaced); remaining items take departure dispositions.
	departed bool
	// depart is closed exactly once, at the departure: a live QoS 0 delivery waiting on the gate
	// of a subscription that left is DROPPED then, not left parked until the driver retires the
	// object (a same-session takeover keeps the incarnation, so its closing signal never fires).
	depart chan struct{}
	// retired: the gate is closed and the object unlinked; nothing further happens to it.
	retired bool
	// handing: a same-sid replacement detached the object and has not attached it to the
	// replacement yet (two c.mu sections). The driver never retires an object in hand, or a
	// replacement could inherit a closed gate with its packet's staged items still to come.
	handing bool
}

func newMQTTRetainedReplay(sub *subscription) *mqttRetainedReplay {
	return &mqttRetainedReplay{sub: sub, gate: make(chan struct{}), depart: make(chan struct{})}
}

// retireLocked closes the gate and unlinks the object. Connection lock held on entry.
// departLocked records the departure of the object's subscription, once.
//
// Connection lock held on entry.
func (r *mqttRetainedReplay) departLocked() {
	if r.departed {
		return
	}
	r.departed = true
	close(r.depart)
}

func (r *mqttRetainedReplay) retireLocked() {
	if r.retired {
		return
	}
	r.retired = true
	close(r.gate)
	if r.sub != nil && r.sub.mqtt != nil && r.sub.mqtt.replay == r {
		r.sub.mqtt.replay = nil
	}
}

// stageRetainedFrameLocked stages one snapshot on sub, creating its replay object (and gate) on
// first use. The gate exists before sess.processSub returns, so before any live callback — which
// takes sess.subsMu for read — can observe the subscription.
//
// Connection lock held on entry.
func stageRetainedFrameLocked(sub *subscription, f mqttRetainedFrame) {
	if sub.mqtt.replay == nil || sub.mqtt.replay.retired {
		sub.mqtt.replay = newMQTTRetainedReplay(sub)
	}
	sub.mqtt.prm = append(sub.mqtt.prm, f)
}

// mqttRetainedDriver is one session's retained work, in TWO workers so neither stalls the other
// (plan §3.7 fairness). The REPLAY worker executes the replay objects in SUBSCRIBE order, one item
// at a time — exactly one active materialization per session, so a blocked item holds every later
// one and per-subscription order is kept. The CONTINUATION worker owns what must progress whatever
// the replay is doing: the abandonments until each stands in memory (outbound_retained_abandon.go)
// and the owed transmissions (a PUBREL, a restored PUBLISH) that could not be sent. Reclamation is
// the reclaimer's own worker (outbound_retained_reclaim.go).
type mqttRetainedDriver struct {
	mu      sync.Mutex
	queue   []*mqttRetainedReplay
	conn    *client
	running bool // the replay worker
	// The continuation worker's work, its liveness, and the kick that ends its backoff wait early
	// when new work arrives.
	abandons    []*mqttRetainedAbandonment
	owed        []*mqttRetainedOwed
	contRunning bool
	kick        chan struct{}
}

// startContinuationLocked marks the continuation worker running and reports whether the caller must
// start its goroutine, waking a waiting one otherwise.
//
// d.mu held.
func (d *mqttRetainedDriver) startContinuationLocked() bool {
	if d.kick == nil {
		d.kick = make(chan struct{}, 1)
	}
	if d.contRunning {
		select {
		case d.kick <- struct{}{}:
		default:
		}
		return false
	}
	d.contRunning = true
	return true
}

// THE OWNERSHIP PROPERTY (F-105 7c, consult 106). Every retained obligation this connection has
// created, observed or possibly landed stays OWNED, its captured identity kept, until (i) its own
// durable settlement is confirmed, (ii) its entitlement has positively passed — the session serves
// another connection, whose resume restores it from the records — or (iii) its connection is
// EFFECTIVELY closed, the records intact for the reconnect's resume. A path that can show none of
// the three hands the obligation here; nothing else is a legitimate return. A refusal, a missing
// local entry, an unreadable record or an exhausted retry proves none of the three.

// mqttRetainedOwedKind is the obligation an owner carries.
type mqttRetainedOwedKind uint8

const (
	// mqttRetainedOwedPublish: a restored retained PUBLISH that could not be sent.
	mqttRetainedOwedPublish mqttRetainedOwedKind = iota + 1
	// mqttRetainedOwedPubRel: a retained PUBREL that could not be sent.
	mqttRetainedOwedPubRel
	// mqttRetainedOwedWithdraw: an admitted, proven-unsent exchange whose withdrawal has not been
	// confirmed (outbound_retained_withdraw.go).
	mqttRetainedOwedWithdraw
	// mqttRetainedOwedInstall: an abandoned exchange's confirmed PUBREC whose PUBREL entry could
	// not be installed.
	mqttRetainedOwedInstall
	// mqttRetainedOwedClose: an obligation nothing on this connection can settle; owned until the
	// connection is closed.
	mqttRetainedOwedClose
)

func (k mqttRetainedOwedKind) String() string {
	switch k {
	case mqttRetainedOwedPublish:
		return "PUBLISH"
	case mqttRetainedOwedPubRel:
		return "PUBREL"
	case mqttRetainedOwedWithdraw:
		return "withdrawal"
	case mqttRetainedOwedInstall:
		return "PUBREL installation"
	}
	return "exchange"
}

// mqttRetainedOwed is one retained obligation the continuation worker owns, named by its
// ALLOCATION (identifier and nonce) and its connection: a retained exchange has no source
// redelivery to retry it.
type mqttRetainedOwed struct {
	c     *client
	pi    uint16
	nonce string
	kind  mqttRetainedOwedKind
	// snap is the captured entry: the ACK-only snapshot an INSTALL owner installs from, or the
	// withdrawn allocation a WITHDRAW owner deletes the register of.
	snap    *mqttPending
	attempt int
	nextAt  time.Time
	// closing: the connection's close was requested; the owner stays until it is effective.
	closing bool
	// what names the obligation in the close's log line.
	what string
}

// mqttRetainedOwedAttempts bounds the re-attempts of one owed obligation. Exhausted while the
// connection still serves, the connection is CLOSED: the exchange stays owed on its records, and
// the client's reconnect replays it (docs/CONFORMANCE.md).
const mqttRetainedOwedAttempts = 8

// mqttRetainedClosingPoll spaces a closing owner's checks for the close to take effect.
const mqttRetainedClosingPoll = 20 * time.Millisecond

// oweRetained hands one obligation to the continuation worker, once per allocation and kind.
func (sess *mqttSession) oweRetained(o *mqttRetainedOwed) {
	d := &sess.retainedDriver
	d.mu.Lock()
	for _, cur := range d.owed {
		if cur.pi == o.pi && cur.c == o.c && cur.nonce == o.nonce && cur.kind == o.kind {
			d.mu.Unlock()
			return
		}
	}
	if o.closing {
		o.nextAt = time.Now()
	} else {
		o.nextAt = time.Now().Add(mqttRetainedStepBackoff(0))
	}
	d.owed = append(d.owed, o)
	start := d.startContinuationLocked()
	d.mu.Unlock()
	if o.closing {
		sess.requestRetainedClose(o)
	}
	if start {
		go sess.runRetainedContinuations()
	}
}

// ownRetainedOwed hands one owed transmission to the continuation worker.
func (sess *mqttSession) ownRetainedOwed(c *client, pi uint16, nonce string, kind mqttRetainedOwedKind) {
	sess.oweRetained(&mqttRetainedOwed{c: c, pi: pi, nonce: nonce, kind: kind})
}

// oweRetainedClose closes c over an obligation nothing on it can settle, and owns the obligation
// until the close is effective — a requested close is not yet exit (iii).
func (sess *mqttSession) oweRetainedClose(c *client, pi uint16, nonce, what string) {
	sess.oweRetained(&mqttRetainedOwed{c: c, pi: pi, nonce: nonce, kind: mqttRetainedOwedClose,
		closing: true, what: what})
}

// stepRetainedOwed makes one attempt of an owed obligation and reports whether its owner is done:
// settled, the session serving another connection, or this connection closed. An exhausted owner
// requests the close and waits for it to take effect.
func (sess *mqttSession) stepRetainedOwed(o *mqttRetainedOwed) bool {
	sess.mu.Lock()
	current := sess.c == o.c && !sess.retainedQuarantinedLocked(o.nonce)
	sess.mu.Unlock()
	if !current || o.c.isClosed() {
		return true // another connection's, or quarantined: nothing here may act for it
	}
	if o.closing {
		return false
	}
	settled := false
	switch o.kind {
	case mqttRetainedOwedPublish:
		settled = sess.replayRestoredRetainedPublishOnce(o.c, o.pi, o.nonce)
	case mqttRetainedOwedPubRel:
		settled = sess.emitRetainedPubRelOnce(o.c, o.pi, o.nonce, sess.admitBackgroundAuthority)
	case mqttRetainedOwedWithdraw:
		settled = sess.withdrawRetainedOnce(o.c, o.snap, true)
	case mqttRetainedOwedInstall:
		installed, owned := sess.installAbandonedRetainedPubRel(o.c, o.pi, o.snap)
		if installed {
			// The entry stands: what remains is its PUBREL, owned by this same owner.
			o.kind, o.attempt = mqttRetainedOwedPubRel, 0
			settled = sess.emitRetainedPubRelOnce(o.c, o.pi, o.nonce, sess.admitBackgroundAuthority)
		} else {
			settled = !owned
		}
	}
	if settled {
		return true
	}
	if o.attempt+1 >= mqttRetainedOwedAttempts {
		o.closing = true
		sess.requestRetainedClose(o)
	}
	return false
}

// requestRetainedClose asks for the close of a connection whose owed obligation could not be
// settled: the exchange stays owed on its records, and the reconnect's resume replays it.
func (sess *mqttSession) requestRetainedClose(o *mqttRetainedOwed) {
	what := o.what
	if what == _EMPTY_ {
		what = o.kind.String()
	}
	o.c.Warnf("MQTT: closing the connection of client %q: its owed retained %s %d could not be settled; "+
		"the reconnect replays it", sess.id, what, o.pi)
	go o.c.closeConnection(WriteError)
}

// ownRetainedAbandonment hands one abandonment decision to the driver. A decision already owned
// for the same allocation is not owned twice.
func (sess *mqttSession) ownRetainedAbandonment(a *mqttRetainedAbandonment) {
	d := &sess.retainedDriver
	d.mu.Lock()
	for _, cur := range d.abandons {
		if cur.ev.Gen == a.ev.Gen && cur.ev.PI == a.ev.PI && cur.ev.Nonce == a.ev.Nonce {
			d.mu.Unlock()
			return
		}
	}
	d.abandons = append(d.abandons, a)
	start := d.startContinuationLocked()
	d.mu.Unlock()
	if start {
		go sess.runRetainedContinuations()
	}
}

// transferRetainedReplays publishes each new subscription's staged items to its replay object and
// queues the objects on the driver. Called after SUBACK, on the readLoop; it returns at once.
//
// Connection lock NOT held on entry.
func (c *client) transferRetainedReplays(subs []*subscription) {
	var wake []*mqttRetainedReplay
	c.mu.Lock()
	for _, sub := range subs {
		if sub.mqtt == nil || sub.mqtt.replay == nil {
			continue
		}
		r := sub.mqtt.replay
		if r.retired {
			r = newMQTTRetainedReplay(sub)
			sub.mqtt.replay = r
		}
		r.exec = append(r.exec, sub.mqtt.prm...)
		sub.mqtt.prm = nil
		if len(r.exec) == 0 {
			r.retireLocked()
			continue
		}
		if !r.queued {
			r.queued = true
			wake = append(wake, r)
		}
	}
	c.mu.Unlock()
	if len(wake) == 0 || c.mqtt == nil || c.mqtt.sess == nil {
		return
	}
	c.mqtt.sess.wakeRetainedDriver(c, wake)
}

// wakeRetainedDriver queues objects and starts the driver if it is idle.
func (sess *mqttSession) wakeRetainedDriver(c *client, objs []*mqttRetainedReplay) {
	d := &sess.retainedDriver
	d.mu.Lock()
	d.queue = append(d.queue, objs...)
	d.conn = c
	start := !d.running
	d.running = true
	d.mu.Unlock()
	if start {
		go sess.runRetainedDriver()
	}
}

// runRetainedDriver is the REPLAY worker: it executes queued objects in order until none remains.
func (sess *mqttSession) runRetainedDriver() {
	d := &sess.retainedDriver
	for {
		d.mu.Lock()
		if len(d.queue) == 0 {
			d.running = false
			d.mu.Unlock()
			return
		}
		r, c := d.queue[0], d.conn
		d.queue = d.queue[1:]
		d.mu.Unlock()
		sess.replayObject(c, r)
	}
}

// runRetainedContinuations is the CONTINUATION worker: one step of whichever owned abandonment or
// owed transmission is due, each backing off on its own, until none remains. It never waits on the
// replay worker.
func (sess *mqttSession) runRetainedContinuations() {
	d := &sess.retainedDriver
	for {
		d.mu.Lock()
		now := time.Now()
		var due *mqttRetainedAbandonment
		var dueOwed *mqttRetainedOwed
		wait := time.Duration(-1)
		for _, a := range d.abandons {
			if !a.nextAt.After(now) {
				due = a
				break
			}
			if w := a.nextAt.Sub(now); wait < 0 || w < wait {
				wait = w
			}
		}
		for _, o := range d.owed {
			if !o.nextAt.After(now) {
				dueOwed = o
				break
			}
			if w := o.nextAt.Sub(now); wait < 0 || w < wait {
				wait = w
			}
		}
		if due == nil && dueOwed == nil {
			if wait < 0 {
				d.contRunning = false
				d.mu.Unlock()
				return
			}
			kick := d.kick
			d.mu.Unlock()
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-kick:
				t.Stop()
			}
			continue
		}
		d.mu.Unlock()
		if due != nil {
			var closeOf *client
			step := sess.stepRetainedAbandonment(due)
			d.mu.Lock()
			if step == mqttRetainedAbandonRetry {
				due.nextAt = time.Now().Add(mqttRetainedStepBackoff(due.attempt))
				due.attempt++
				// BOUNDED ON THIS CONNECTION: a decision that cannot stand holds the entry's charge
				// and identifier, so past the bound its connection is closed — once — and the
				// decision is still stepped (it is durable-state driven); the resume decides again.
				if due.attempt >= mqttRetainedOwedAttempts && !due.closeRequested && due.c != nil {
					due.closeRequested = true
					closeOf = due.c
				}
			} else {
				for i, a := range d.abandons {
					if a == due {
						d.abandons = append(d.abandons[:i], d.abandons[i+1:]...)
						break
					}
				}
			}
			d.mu.Unlock()
			if closeOf != nil {
				sess.mu.Lock()
				current := sess.c == closeOf
				sess.mu.Unlock()
				if current && !closeOf.isClosed() {
					sess.requestRetainedClose(&mqttRetainedOwed{c: closeOf, pi: due.ev.PI, what: "abandonment"})
				}
			}
		}
		if dueOwed != nil {
			done := sess.stepRetainedOwed(dueOwed)
			d.mu.Lock()
			if done {
				for i, o := range d.owed {
					if o == dueOwed {
						d.owed = append(d.owed[:i], d.owed[i+1:]...)
						break
					}
				}
			} else if dueOwed.closing {
				dueOwed.nextAt = time.Now().Add(mqttRetainedClosingPoll)
			} else {
				dueOwed.attempt++
				dueOwed.nextAt = time.Now().Add(mqttRetainedStepBackoff(dueOwed.attempt))
			}
			d.mu.Unlock()
		}
	}
}

// replayObject executes one object's items in order and retires it when both lists are empty.
func (sess *mqttSession) replayObject(c *client, r *mqttRetainedReplay) {
	for {
		c.mu.Lock()
		if r.departed {
			items := r.exec
			r.exec = nil
			r.queued = false
			r.retireLocked()
			c.mu.Unlock()
			sess.disposeDepartedRetained(c, items)
			return
		}
		if len(r.exec) == 0 {
			// Staged items still on the holder are this object's too: the holder's packet has
			// not transferred yet, and its transfer will queue the object again. An object in a
			// replacement's hand is the same: its attach, then its packet's transfer, decide it.
			if r.handing || (r.sub != nil && r.sub.mqtt != nil && r.sub.mqtt.replay == r && len(r.sub.mqtt.prm) > 0) {
				r.queued = false
				c.mu.Unlock()
				return
			}
			r.queued = false
			r.retireLocked()
			c.mu.Unlock()
			return
		}
		f := r.exec[0]
		r.exec = r.exec[1:]
		c.mu.Unlock()
		sess.replayItem(c, r, f)
	}
}

// retainedReplayDeparted reports whether the object's lineage departed or the connection closes.
func (c *client) retainedReplayDeparted(r *mqttRetainedReplay) bool {
	c.mu.Lock()
	departed := r.departed
	c.mu.Unlock()
	return departed || c.isClosed()
}

// disposeDepartedRetained releases the allocations of items that will never be written.
func (sess *mqttSession) disposeDepartedRetained(c *client, items []mqttRetainedFrame) {
	sess.abandonRetainedSends(c, mqttRetainedFrameRefs(items))
}

// replayItem executes one item.
func (sess *mqttSession) replayItem(c *client, r *mqttRetainedReplay, f mqttRetainedFrame) {
	if f.qos == 0 || f.mat == nil {
		if c.retainedReplayDeparted(r) {
			return
		}
		frame := append(append([]byte{}, f.header...), f.msg...)
		if err := c.writeActionlessFrame(frame); err != nil {
			c.Warnf("MQTT: retained QoS 0 replay on %q refused by the actionless writer: %v", f.mat.topicOr(), err)
		}
		return
	}
	sess.replayDurableItem(c, r, f)
}

// topicOr is a nil-tolerant topic for logging.
func (m *mqttRetainedMaterial) topicOr() string {
	if m == nil {
		return _EMPTY_
	}
	return m.Topic
}

// mqttRetainedStepAttempts bounds one step's foreground retries before the driver backs off.
const mqttRetainedStepAttempts = 8

// mqttRetainedStepBackoff is the capped retry delay between attempts of one step.
func mqttRetainedStepBackoff(attempt int) time.Duration {
	d := 50 * time.Millisecond << uint(attempt)
	if d > 5*time.Second || d <= 0 {
		d = 5 * time.Second
	}
	return d
}

// awaitRetainedReplayGate parks a live QoS 0 delivery behind its subscription's retained replay.
// Returns false when the subscription departs first (an UNSUBSCRIBE, its connection's close, a
// takeover) or the incarnation starts retiring: the delivery is dropped, as QoS 0 permits. The
// incarnation's closing signal is closed before an incarnation drain waits, so a retirement is
// never held by a waiting callback; the departure releases the waiters a same-session takeover
// would otherwise leave parked behind the displaced connection's replay.
//
// No lock held on entry.
func (c *client) awaitRetainedReplayGate(sub *subscription) bool {
	c.mu.Lock()
	var gate, depart chan struct{}
	if sub != nil && sub.mqtt != nil && sub.mqtt.replay != nil && !sub.mqtt.replay.retired {
		gate, depart = sub.mqtt.replay.gate, sub.mqtt.replay.depart
	}
	c.mu.Unlock()
	if gate == nil {
		return true
	}
	var closing <-chan struct{}
	if c.mqtt != nil && c.mqtt.tracker != nil {
		closing = c.mqtt.tracker.closingSignal()
	}
	select {
	case <-gate:
		return true
	case <-depart:
		return false
	case <-closing:
		return false
	}
}
