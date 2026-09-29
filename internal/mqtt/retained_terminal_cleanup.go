package mqtt

import "time"

// A RETAINED TERMINAL SLOT IS OWNED UNTIL ITS CLEANUP COMPLETES (WP-A round 1, consult 81).
//
// mqttTerminalRetainedReoffer keeps a completed exchange's `pubrel_terminal` register because
// its source may still be re-offered, and hands the identifier's exclusion on. Something must
// then finish that slot once the source settles, with no client packet and no resume to prompt
// it. That owner used to be a 30-attempt poll whose documented backstop was "the next resume":
// a source settled after the poll gave up — by the disposal continuation, a retired-source
// redelivery or recovery — left the register and the exclusion standing on healthy storage.
//
// The obligation is now one registered owner per exact slot identity, enrolled in the same
// critical section as the exclusion, retried with capped backoff and no lifetime cutoff, and
// woken by every confirmed source settlement before the settler drops its own ownership. An
// attempt count paces the retries; it never retires the obligation. Process loss rebuilds
// from durable truth: recovery re-derives the retained slot and re-enrolls it.

// mqttRetainedCleanupMaxDelay caps the owner's backoff. The first retry waits
// mqttActionReconcileDelay; each unsettled attempt doubles the wait up to this.
const mqttRetainedCleanupMaxDelay = 30 * time.Second

// retainedCleanupDelayLocked is the owner's retry pacing for its attempt'th retry.
//
// Session lock held on entry.
func (sess *mqttSession) retainedCleanupDelayLocked(attempt int) time.Duration {
	if sess.testRetainedCleanupDelay != nil {
		return sess.testRetainedCleanupDelay(attempt)
	}
	d := mqttActionReconcileDelay
	for i := 0; i < attempt && d < mqttRetainedCleanupMaxDelay; i++ {
		d *= 2
	}
	if d > mqttRetainedCleanupMaxDelay {
		d = mqttRetainedCleanupMaxDelay
	}
	return d
}

type mqttRetainedCleanup struct {
	ref     mqttInflightSlotRef
	epoch   string
	c       *client
	srv     *Server
	attempt int
	timer   *time.Timer
	running bool
	woken   bool // a settlement arrived during a running attempt: rerun at once
}

// enrollRetainedTerminalCleanupLocked installs the cleanup owner of a retained terminal slot,
// or finds the one already installed. The caller holds sess.mu and installs the identifier's
// exclusion in the SAME critical section, so no settlement can observe the exclusion without
// its owner. Returns the owner to schedule outside the lock, nil when nothing new was
// installed or the slot cannot be addressed.
//
// Session lock held on entry.
func (sess *mqttSession) enrollRetainedTerminalCleanupLocked(c *client, ref mqttInflightSlotRef) *mqttRetainedCleanup {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ || ref.name.pi == 0 ||
		ref.name.gen != sess.gen || sess.asm == nil {
		return nil
	}
	// The finalizer's record disposal needs the client, exactly as the bounded poll this
	// replaces did; a caller without one has no route to finish the slot here.
	srv := sess.serverFor(c)
	if c == nil || srv == nil {
		return nil
	}
	key := mqttTerminalSourceKeyOf(ref)
	if w := sess.retainedCleanups[key]; w != nil && w.epoch == sess.asm.sessStreamEpoch {
		return nil // Ownership already present.
	}
	if sess.retainedCleanups == nil {
		sess.retainedCleanups = make(map[mqttTerminalSourceKey]*mqttRetainedCleanup)
	}
	w := &mqttRetainedCleanup{ref: ref, epoch: sess.asm.sessStreamEpoch, c: c, srv: srv}
	sess.retainedCleanups[key] = w
	return w
}

// wakeRetainedTerminalCleanupLocked is what every confirmed source settlement owes the retained
// terminal slot of the same allocation, BEFORE it drops its own ownership of that source: the
// owner re-attempts now instead of at its backoff. With no owner installed nothing is lost —
// a retained arm that enrolls later schedules a fresh attempt, and this settlement happened
// before it, so that attempt observes the settled source.
//
// Session lock held on entry.
func (sess *mqttSession) wakeRetainedTerminalCleanupLocked(nonce, dur string, sseq uint64) {
	if nonce == _EMPTY_ {
		return
	}
	for key, w := range sess.retainedCleanups {
		if key.nonce != nonce || key.dur != dur || key.sseq != sseq {
			continue
		}
		if w.running {
			w.woken = true
			continue
		}
		sess.scheduleRetainedCleanupLocked(w, 0)
	}
}

// scheduleRetainedCleanupLocked (re)arms the owner's next attempt. Worker admission is taken
// when the attempt FIRES, so a stopped or superseded timer holds no shutdown admission.
//
// Session lock held on entry.
func (sess *mqttSession) scheduleRetainedCleanupLocked(w *mqttRetainedCleanup, delay time.Duration) {
	if w.timer != nil && !w.timer.Stop() {
		// Already fired: that callback finds this owner current and runs the attempt now,
		// which is at least as early as anything this call could ask for.
		return
	}
	w.timer = time.AfterFunc(delay, func() { sess.runRetainedCleanup(w) })
}

// runRetainedCleanup is one attempt of the owner.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runRetainedCleanup(w *mqttRetainedCleanup) {
	key := mqttTerminalSourceKeyOf(w.ref)
	sess.mu.Lock()
	// This callback IS the armed timer's (a timer is replaced only after a successful Stop), so
	// the owner has none armed from here until it schedules the next attempt.
	w.timer = nil
	if sess.retainedCleanups[key] != w || w.running {
		sess.mu.Unlock()
		return
	}
	if w.ref.name.gen != sess.gen || sess.asm == nil || sess.asm.sessStreamEpoch != w.epoch {
		// A retired generation or another store epoch: recovery of that successor owns
		// whatever durable truth remains.
		delete(sess.retainedCleanups, key)
		sess.mu.Unlock()
		return
	}
	if !w.srv.admitWillWorker() {
		// Shutdown. The owner is handed to the server's final pass rather than dropped
		// (lock order sess.mu -> owedRetainedMu); what that pass cannot finish stays durable
		// for the next resume to re-enroll.
		w.srv.owedRetainedMu.Lock()
		if w.srv.owedRetained == nil {
			w.srv.owedRetained = make(map[*mqttRetainedCleanup]*mqttSession)
		}
		w.srv.owedRetained[w] = sess
		w.srv.owedRetainedMu.Unlock()
		sess.mu.Unlock()
		return
	}
	w.running, w.woken = true, false
	c := w.c
	sess.mu.Unlock()
	defer w.srv.willWorkerWG.Done()

	done := sess.attemptRetainedCleanup(c, w.ref)

	sess.mu.Lock()
	defer sess.mu.Unlock()
	w.running = false
	if sess.retainedCleanups[key] != w {
		return
	}
	if done {
		delete(sess.retainedCleanups, key)
		return
	}
	if w.woken {
		sess.scheduleRetainedCleanupLocked(w, 0)
		return
	}
	w.attempt++
	sess.scheduleRetainedCleanupLocked(w, sess.retainedCleanupDelayLocked(w.attempt))
}

// attemptRetainedCleanup reports whether the owner's obligation is over.
//
// A matching register that is not at `pubrel_terminal` is not a retained terminal slot: the
// client has not completed that exchange, source settlement cannot manufacture its PUBCOMP,
// and the route that later retains it enrolls its own owner. Every other shape goes through
// the terminal finalizer and its gates, under the register's FRESH sequence — the retained
// reference's sequence may predate a rewrite of the same allocation.
//
// Session lock NOT held on entry.
func (sess *mqttSession) attemptRetainedCleanup(c *client, ref mqttInflightSlotRef) bool {
	cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
	switch {
	case err != nil, smsg != nil && cur == nil:
		return false // Indeterminate: neither an absence nor a licence.
	case smsg != nil && ref.matchesRecord(cur):
		if cur.State != mqttInflightPubRelTerminal {
			return true
		}
		ref.seq = smsg.Sequence
		ref.state = cur.State
	}
	out, _ := sess.finalizeTerminalSlotObserved(c, ref)
	return out == mqttTerminalDone
}

// finishOwedRetainedCleanups gives every owner refused worker admission one last attempt while
// JetStream still answers. A failed attempt invents no success: the retained slot stays
// durable for the next resume.
func (s *Server) finishOwedRetainedCleanups() {
	s.owedRetainedMu.Lock()
	owed := s.owedRetained
	s.owedRetained = nil
	s.owedRetainedMu.Unlock()
	for w, sess := range owed {
		key := mqttTerminalSourceKeyOf(w.ref)
		sess.mu.Lock()
		current := sess.retainedCleanups[key] == w && !w.running &&
			w.ref.name.gen == sess.gen && sess.asm != nil && sess.asm.sessStreamEpoch == w.epoch
		if current {
			w.running = true
		}
		sess.mu.Unlock()
		if !current {
			continue
		}
		done := sess.attemptRetainedCleanup(w.c, w.ref)
		sess.mu.Lock()
		w.running = false
		if done && sess.retainedCleanups[key] == w {
			delete(sess.retainedCleanups, key)
		}
		sess.mu.Unlock()
	}
}
