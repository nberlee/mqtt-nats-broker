package mqtt

import (
	"errors"
	"testing"
	"time"
)

func (te *testTerminalEnv) waitSourceOwnerGone(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		te.sess.mu.Lock()
		held := te.sess.sourceRetiredLocked(te.dur, te.sseq)
		owners := len(te.sess.terminalSources)
		retries := len(te.sess.terminalRetries)
		h := te.sess.sendsInFlight[te.pi]
		terminal := h != nil && h.terminalOwed != ""
		te.sess.mu.Unlock()
		if !held && owners == 0 && retries == 0 && !terminal {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("orphan after completion: held=%v owners=%d retries=%d terminal=%v",
				held, owners, retries, terminal)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Called only by sequential tests: the hook is process-wide. Reaching this hook
// proves that the actual callback passed BOTH suppression entrances; it declines
// before allocation so the probe cannot create a competing exchange.
func testTerminalOfferProgress(t *testing.T, te *testTerminalEnv, reply string, want bool) {
	t.Helper()
	reached := false
	restore := setMQTTTestInflightPreSendHook(func(dur, offered string) bool {
		if dur == te.dur && offered == reply {
			reached = true
			return false
		}
		return true
	})
	defer restore()
	hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
	pc := &client{kind: JETSTREAM}
	pc.pa.hdr = len(hdr)
	sub := &subscription{client: te.c, mqtt: &mqttSub{qos: 1, jsDur: te.dur}}
	mqttDeliverMsgCbQoS12(sub, pc, nil, mqttStreamSubjectPrefix+"foo.bar", reply,
		append(hdr, []byte("source-offer")...))
	if reached != want {
		t.Fatalf("source offer reached ordinary delivery=%v, want %v", reached, want)
	}
}

func TestUnitCTerminalOwnerLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name, mutation string
		retry, offer   bool
	}{
		{"success", "M-C55-success: omit exact owner discharge", false, false},
		{"successor", "M-C55-survivor: retain suppression on doneOverSurvivor", false, true},
		{"foreign", "M-C55-foreign: omit explicit foreign handoff", false, true},
		{"no ACK authority", "M-C55-noack: consume another ACK owner's restriction", true, false},
		{"absent live", "M-C55-absent: report done without source completion", true, false},
		{"absent unknown", "M-C55-unknown: treat Unknown as settled", true, false},
		{"absent complete", "M-C55-complete: retain a positively completed owner", false, false},
		{"unreadable", "M-C55-unreadable: equate unreadable with absence", true, false},
		{"read failure", "M-C55-read: consume failed read", true, false},
		{"advance failure", "M-C55-advance: drop failed advance ownership", true, false},
		{"delete failure", "M-C55-delete: drop cleanup after source confirmation", true, false},
		{"source unknown", "M-C55-source: drop unsettled terminal ownership", true, false},
		{"live action", "M-C55-action: consume cleanup under a live token", true, false},
		{"closed connection", "M-C55-close: tie continuation to sess.c", true, false},
		{"epoch change", "M-C55-epoch: use a source owner in another epoch", true, false},
		{"clear()", "M-C55-clear-call: drop ownership outside generation teardown", false, false},
		{"clear abort", "M-C55-abort: discard maps on aborted clear", true, false},
		{"clear reset", "M-C55-clear: retain maps in destroy's reset", false, false},
		{"clean-start reuse", "M-C55-reuse: retain maps in handoff's reset", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.mutation)
			te := testNewTerminalEnv(t, "owner-lifecycle")
			te.s.closeWillDispatch() // Every retry must reach the admission-refusal owner.
			var resumeRetries func()
			if tc.name == "clear reset" || tc.name == "clean-start reuse" {
				// Keep real retry tokens installed until the reset is observed.
				resumeRetries = te.sess.deferTerminalRetries()
				defer func() {
					if resumeRetries != nil {
						resumeRetries()
					}
				}()
			}
			cur, msg, err := te.asm.jsa.loadInflightRecord(te.subject)
			if err != nil || cur == nil || msg == nil {
				t.Fatalf("fixture: %v", err)
			}
			ref := *mqttSlotRefFromRecord(te.subject, msg.Sequence, cur)
			ref.terminalEpoch = te.asm.sessStreamEpoch
			te.sess.mu.Lock()
			te.sess.untrackPublishEntry(te.pi)
			var w *mqttTerminalSource
			if tc.name != "no ACK authority" {
				w = te.sess.holdTerminalSourceLocked(ref)
			}
			te.sess.mu.Unlock()
			if w == nil && tc.name != "no ACK authority" {
				t.Fatal("fixture did not install a restriction")
			}
			acknowledged := true
			noACKRead := false
			switch tc.name {
			case "successor", "foreign", "live action":
				next := *cur
				if tc.name == "foreign" {
					next.Nonce = mqttNewInflightNonce()
				} else if tc.name == "successor" {
					next.Acq++
				} else {
					next.ActionToken = mqttNewInflightNonce()
					next.ActionClaim, next.ActionClaimSeq = next.Claim, next.ClaimSeq
				}
				if _, err := te.asm.jsa.storeInflightCAS(te.subject, msg.Sequence, &next); err != nil {
					t.Fatal(err)
				}
			case "no ACK authority":
				acknowledged = false
				te.asm.mu.Lock()
				te.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
					// The ACK handler installs its restriction after the entry
					// snapshot, while this recovery-only attempt reads the slot.
					noACKRead = true
					te.sess.mu.Lock()
					if w == nil {
						w = te.sess.holdTerminalSourceLocked(ref)
					}
					te.sess.mu.Unlock()
					return te.asm.jsa.loadInflightRecord(subject)
				}
				te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
					return mqttInflightSourceLive
				}
				te.asm.mu.Unlock()
			case "clear()", "clear reset", "clean-start reuse":
				// Both production discard entrances use this exact boundary; failed
				// clear never reaches it. Also leave a server-side old-generation owner.
				te.s.noteOwedTerminal(te.sess, te.pi, ref, true)
				te.sess.armTerminalRetry(ref, true)
				if tc.name == "clear()" {
					te.sess.mu.Lock()
					te.sess.c = nil // Production detaches before minting the discard permit.
					te.sess.mu.Unlock()
					if err := te.sess.clear(false); err != nil {
						t.Fatal(err)
					}
					break
				}
				base := uint64(0)
				if tc.name == "clean-start reuse" {
					// A clean-start HANDOFF displaces the predecessor record in place, so its base
					// is that record's sequence and never zero; zero is destroy()'s shape. Without a
					// non-zero base this leg would exercise the other entrance and prove nothing
					// about the handoff's reset.
					base = te.sess.seq
					if base == 0 {
						base = 1
					}
				}
				te.sess.mu.Lock()
				ownersBefore := len(te.sess.terminalSources)
				retriesBefore := len(te.sess.terminalRetries)
				te.sess.resetForReuseLocked(mqttRetirementPermit{tr: te.sess.leases}, mqttNewSessGen(), base)
				ownersAfter := len(te.sess.terminalSources)
				retriesAfter := len(te.sess.terminalRetries)
				te.sess.mu.Unlock()
				if ownersBefore != 1 || retriesBefore != 2 {
					t.Fatalf("reset fixture: owners=%d retries=%d", ownersBefore, retriesBefore)
				}
				// No stale attempt or shutdown pass may repair a defective reset
				// before we check disposal of the retired incarnation's maps.
				if ownersAfter != 0 || retriesAfter != 0 {
					t.Fatalf("reset retained retired metadata: owners=%d retries=%d", ownersAfter, retriesAfter)
				}
				resumeRetries()
				resumeRetries = nil
			case "clear abort":
				prepared, err := te.sess.prepareClear()
				if err != nil {
					t.Fatal(err)
				}
				prepared.abort()
			case "epoch change":
				te.sess.mu.Lock()
				w.epoch = "another-store-epoch"
				te.sess.mu.Unlock()
			}
			if tc.name == "advance failure" || tc.name == "closed connection" || tc.name == "clear abort" {
				te.advance = func(*mqttInflightRecord, func() (uint64, error)) (uint64, error) {
					return 0, errors.New("terminal advance refused")
				}
			}
			if tc.name == "closed connection" {
				te.sess.mu.Lock()
				te.sess.c = nil
				te.sess.mu.Unlock()
			}
			te.asm.mu.Lock()
			switch tc.name {
			case "absent live", "absent unknown", "absent complete":
				te.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return nil, nil, nil
				}
				state := mqttInflightSourceLive
				if tc.name == "absent unknown" {
					state = mqttInflightSourceUnknown
				} else if tc.name == "absent complete" {
					state = mqttInflightSourceComplete
				}
				te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return state }
			case "unreadable":
				// Complete isolates the read guard: an erroneous absence must not
				// be masked by the independent Live-source completion guard.
				te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
					return mqttInflightSourceComplete
				}
				te.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return nil, &StoredMsg{Sequence: msg.Sequence, Subject: te.subject}, nil
				}
			case "read failure", "live action":
				te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
					return mqttInflightSourceComplete
				}
				if tc.name == "live action" {
					break
				}
				te.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return nil, nil, errors.New("read failed")
				}
			case "delete failure":
				te.asm.testInflightDelete = func(uint64) error { return errors.New("delete failed") }
			case "source unknown":
				te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
					return mqttInflightSourceUnknown
				}
			}
			te.asm.mu.Unlock()
			done := te.sess.attemptTerminalRegisterRelease(nil, te.pi, ref, acknowledged)
			if tc.name == "no ACK authority" && (!noACKRead || w == nil) {
				t.Fatal("no-ACK attempt did not reach the load with an installed ACK owner")
			}
			if done == tc.retry {
				t.Fatalf("done=%v, want retry=%v", done, tc.retry)
			}
			if tc.name == "epoch change" {
				// The hold's own epoch check would mask a missing attempt fence
				// unless we also reject a durable phase advance in the wrong epoch.
				if after := te.slot(); after == nil || after.State != mqttInflightPublish {
					t.Fatal("epoch-mismatched owner advanced the protocol")
				}
				testTerminalOfferProgress(t, te, cur.SourceAckReply, true)
			}
			if !done {
				te.sess.scheduleTerminalRegisterRelease(nil, te.pi, ref, acknowledged)
			}
			// M-C55-final: failed final attempts must remain reachable, including
			// during the attempt, and must not lose ACK authority.
			te.s.finishOwedTerminals()
			te.s.owedTerminalsMu.Lock()
			owed := len(te.s.owedTerminals)
			te.s.owedTerminalsMu.Unlock()
			te.sess.mu.Lock()
			owners := len(te.sess.terminalSources)
			retries := len(te.sess.terminalRetries)
			busy := w.busy
			te.sess.mu.Unlock()
			if tc.retry {
				if owed != 1 || owners != 1 || retries != 0 || busy {
					t.Fatalf("unowned/refused cleanup: owed=%d owners=%d timers=%d busy=%v",
						owed, owners, retries, busy)
				}
				if tc.name != "closed connection" && tc.name != "epoch change" {
					testTerminalOfferProgress(t, te, cur.SourceAckReply, false)
				}
			} else {
				te.waitSourceOwnerGone(t)
				if owed != 0 {
					t.Fatalf("completed cleanup left %d owed entries", owed)
				}
				if tc.offer {
					testTerminalOfferProgress(t, te, cur.SourceAckReply, true)
				}
				if tc.name == "successor" || tc.name == "foreign" {
					if held, _ := te.identifierHeld(); !held {
						t.Fatal("successor identifier exclusion was released")
					}
					if after := te.slot(); after == nil || after.State != mqttInflightPublish {
						t.Fatal("successor was terminalized/deleted")
					}
				}
				return
			}
			// Storage becomes healthy; no traffic or second MQTT ACK drives completion.
			te.advance = nil
			te.asm.mu.Lock()
			te.asm.testInflightLoad = nil
			te.asm.testInflightDelete = nil
			te.asm.testInflightSourceState = nil
			te.asm.mu.Unlock()
			te.sess.mu.Lock()
			w.epoch = te.asm.sessStreamEpoch
			te.sess.mu.Unlock()
			if tc.name == "live action" {
				r, m, err := te.asm.jsa.loadInflightRecord(te.subject)
				if err != nil || r == nil || m == nil {
					t.Fatal("missing live action fixture")
				}
				r.ActionToken, r.ActionClaim, r.ActionClaimSeq = "", "", 0
				if _, err := te.asm.jsa.storeInflightCAS(te.subject, m.Sequence, r); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "no ACK authority" {
				// The recovery-only attempt cannot invent an ACK. Supply the real
				// acknowledgement to its separate owner, then retire the old retry.
				te.sess.completeAcknowledgedPublish(nil, te.pi, &mqttPending{slot: ref})
			}
			te.s.finishOwedTerminals()
			te.waitSourceOwnerGone(t)
			te.s.owedTerminalsMu.Lock()
			left := len(te.s.owedTerminals)
			te.s.owedTerminalsMu.Unlock()
			if left != 0 {
				t.Fatal("healthy final pass retained its owed entry")
			}
		})
	}
}

// M-C55-retry-key: merge acquisition or ACK authority in the retry key.
// M-C55-instance: let an old completion/disarm remove a newer owner.
func TestUnitCTerminalRetryIdentityAndHandoff(t *testing.T) {
	te := testNewTerminalEnv(t, "owner-retry-identity")
	defer te.sess.deferTerminalRetries()() // Inspect owners without racing their new automatic timers.
	cur, msg, _ := te.asm.jsa.loadInflightRecord(te.subject)
	ref := *mqttSlotRefFromRecord(te.subject, msg.Sequence, cur)
	ref.terminalEpoch = te.asm.sessStreamEpoch
	old := te.sess.armTerminalRetry(ref, false)
	ack := te.sess.armTerminalRetry(ref, true)
	next := ref
	next.acq++
	newer := te.sess.armTerminalRetry(next, true)
	if old == nil || ack == nil || newer == nil || te.sess.armTerminalRetry(next, true) != nil {
		t.Fatal("retry identity lost ACK authority or acquisition, or failed to deduplicate")
	}
	te.sess.mu.Lock()
	w := te.sess.holdTerminalSourceLocked(ref)
	if te.sess.holdTerminalSourceLocked(next) != w {
		t.Fatal("acquisition forked source suppression")
	}
	te.sess.mu.Unlock()
	if !te.sess.finishTerminalSource(ref, true, false) {
		t.Fatal("old continuation did not hand off to the newer one")
	}
	te.sess.disarmTerminalRetry(old)
	te.sess.disarmTerminalRetry(ack)
	te.sess.mu.Lock()
	held := te.sess.terminalSources[mqttTerminalSourceKeyOf(next)] == w &&
		te.sess.terminalRetries[newer.key] == newer && te.sess.sendReservedLocked(te.pi)
	te.sess.mu.Unlock()
	if !held {
		t.Fatal("older completion removed the successor's owner/hold")
	}
	te.sess.disarmTerminalRetry(newer)
	replacement := te.sess.armTerminalRetry(next, true)
	te.sess.disarmTerminalRetry(newer)
	te.sess.mu.Lock()
	same := te.sess.terminalRetries[replacement.key] == replacement
	te.sess.mu.Unlock()
	if !same {
		t.Fatal("stale disarm removed its replacement")
	}
	te.sess.disarmTerminalRetry(replacement)
	if !te.sess.finishTerminalSource(next, true, false) {
		t.Fatal("exact owner failed to discharge")
	}
}

// M-C55-terminal-handoff: let a survivor observation erase an already established
// terminal source restriction. M-C55-busy: leave busy latched after a stale proof.
func TestUnitCTerminalSourceExitRequiresSettlement(t *testing.T) {
	te := testNewTerminalEnv(t, "owner-terminal-exit")
	defer te.sess.deferTerminalRetries()() // Inspect owners without racing their new automatic timers.
	cur, msg, _ := te.asm.jsa.loadInflightRecord(te.subject)
	terminal := *cur
	seq := testAdvanceTerminalPair(t, te.testInflightEnv, te.subject, msg.Sequence, &terminal)
	ref := *mqttSlotRefFromRecord(te.subject, seq, &terminal)
	te.sess.mu.Lock()
	w := te.sess.holdTerminalSourceLocked(ref)
	w.record = terminal
	te.sess.mu.Unlock()
	for _, state := range []mqttInflightSourceState{mqttInflightSourceLive, mqttInflightSourceUnknown} {
		te.asm.mu.Lock()
		te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return state }
		te.asm.mu.Unlock()
		if te.sess.finishTerminalSource(ref, true, false) {
			t.Fatal("a handoff dropped an established terminal source")
		}
	}
	te.asm.mu.Lock()
	te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceLive
	}
	te.asm.testInflightSourceAck = func(string) error {
		te.sess.mu.Lock()
		w.epoch = "different-epoch"
		te.sess.mu.Unlock()
		return nil
	}
	te.asm.mu.Unlock()
	if te.sess.settleTerminalSource(te.subject, seq, &terminal) {
		t.Fatal("an epoch-stale confirmation was accepted")
	}
	te.sess.mu.Lock()
	busy := w.busy
	w.epoch = te.asm.sessStreamEpoch
	te.sess.mu.Unlock()
	if busy {
		t.Fatal("stale attempt stranded its single-flight claim")
	}
	te.asm.mu.Lock()
	te.asm.testInflightSourceAck = func(string) error { return nil }
	te.asm.mu.Unlock()
	if !te.sess.settleTerminalSource(te.subject, seq, &terminal) ||
		!te.sess.finishTerminalSource(ref, false, false) {
		t.Fatal("the restored owner could not complete after a stale attempt")
	}
}

// M-C55-snapshot: detach the owed map before a final attempt.
// M-C55-owed-instance: delete a replacement entry by key alone.
func TestUnitCTerminalFinalPassKeepsExactOwnership(t *testing.T) {
	te := testNewTerminalEnv(t, "owner-final-instance")
	te.s.closeWillDispatch()
	cur, msg, _ := te.asm.jsa.loadInflightRecord(te.subject)
	ref := *mqttSlotRefFromRecord(te.subject, msg.Sequence, cur)
	te.sess.mu.Lock()
	te.sess.holdTerminalSourceLocked(ref)
	te.sess.mu.Unlock()
	te.s.noteOwedTerminal(te.sess, te.pi, ref, true)
	var original, replacement *mqttOwedTerminal
	te.s.owedTerminalsMu.Lock()
	for _, o := range te.s.owedTerminals {
		original = o
	}
	te.s.owedTerminalsMu.Unlock()
	te.asm.mu.Lock()
	te.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if replacement == nil {
			te.s.owedTerminalsMu.Lock()
			for _, o := range te.s.owedTerminals {
				if o != original {
					t.Error("final pass changed its owner before classifying")
				}
				replacement = o
			}
			te.s.owedTerminalsMu.Unlock()
			if replacement == nil {
				t.Error("final pass detached ownership during its attempt")
			}
			te.s.noteOwedTerminal(te.sess, te.pi, ref, true)
			te.s.owedTerminalsMu.Lock()
			for _, o := range te.s.owedTerminals {
				replacement = o
			}
			te.s.owedTerminalsMu.Unlock()
		}
		return te.asm.jsa.loadInflightRecord(subject)
	}
	te.asm.mu.Unlock()
	te.s.finishOwedTerminals()
	te.s.owedTerminalsMu.Lock()
	retained := len(te.s.owedTerminals) == 1
	for _, o := range te.s.owedTerminals {
		retained = retained && o == replacement && o != original
	}
	te.s.owedTerminalsMu.Unlock()
	if !retained {
		t.Fatal("the old final pass consumed a replacement owed owner")
	}
	te.asm.mu.Lock()
	te.asm.testInflightLoad = nil
	te.asm.mu.Unlock()
	te.s.finishOwedTerminals()
	te.s.owedTerminalsMu.Lock()
	left := len(te.s.owedTerminals)
	te.s.owedTerminalsMu.Unlock()
	if left != 0 {
		t.Fatal("replacement could not settle its already-completed obligation")
	}
}

// The absent-source retry must consume a validated offer without recreating a
// register or waiting for another client acknowledgement.
func TestUnitCAbsentSourceUsesValidatedOffer(t *testing.T) {
	te := testNewTerminalEnv(t, "absent-source-offer")
	te.s.closeWillDispatch()
	cur, msg, err := te.asm.jsa.loadInflightRecord(te.subject)
	if err != nil || cur == nil || msg == nil {
		t.Fatalf("fixture: %v", err)
	}
	ref := *mqttSlotRefFromRecord(te.subject, msg.Sequence, cur)
	te.sess.mu.Lock()
	te.sess.untrackPublishEntry(te.pi)
	w := te.sess.holdTerminalSourceLocked(ref)
	te.sess.mu.Unlock()
	te.asm.mu.Lock()
	te.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, nil
	}
	te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceLive
	}
	acks := 0
	te.asm.testInflightSourceAck = func(reply string) error {
		if reply != cur.SourceAckReply {
			t.Errorf("source confirmation used an unbound reply: %q", reply)
		}
		acks++
		return nil
	}
	te.asm.mu.Unlock()
	te.sess.completeAcknowledgedPublish(nil, te.pi, &mqttPending{slot: ref})
	if held, _ := te.identifierHeld(); held {
		t.Fatal("source-only work retained the absent slot's identifier")
	}
	te.s.finishOwedTerminals()
	te.s.owedTerminalsMu.Lock()
	owed := len(te.s.owedTerminals)
	te.s.owedTerminalsMu.Unlock()
	te.sess.mu.Lock()
	retained := te.sess.terminalSources[mqttTerminalSourceKeyOf(ref)] == w
	te.sess.mu.Unlock()
	if !retained || owed != 1 || acks != 0 {
		t.Fatalf("no-reply source lost ownership: retained=%v owed=%d acks=%d", retained, owed, acks)
	}
	testTerminalOfferProgress(t, te, cur.SourceAckReply, false)
	te.s.finishOwedTerminals()
	te.waitSourceOwnerGone(t)
	te.s.owedTerminalsMu.Lock()
	left := len(te.s.owedTerminals)
	te.s.owedTerminalsMu.Unlock()
	if left != 0 || acks != 1 {
		t.Fatalf("validated offer did not settle its owner: owed=%d acks=%d", left, acks)
	}
}
