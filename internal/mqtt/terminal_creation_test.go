package mqtt

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func testTerminalCreationPair(t *testing.T) (*testTerminalEnv, mqttInflightSlotRef) {
	t.Helper()
	te := testNewTerminalEnv(t, "terminal-creation")
	r, msg, err := te.asm.jsa.loadInflightRecord(te.subject)
	if err != nil || r == nil || msg == nil {
		t.Fatalf("load fixture: %v", err)
	}
	seq := testAdvanceTerminalPair(t, te.testInflightEnv, te.subject, msg.Sequence, r)
	te.forgetInMemory()
	ref := *mqttSlotRefFromRecord(te.subject, seq, r)
	ref.terminalEpoch = te.asm.sessStreamEpoch
	return te, ref
}

// Check at the creation boundary, before any caller can schedule cleanup for S.
func TestUnitCTerminalCreationOwnsContinuation(t *testing.T) {
	for _, refused := range []bool{false, true} {
		name := "timer"
		if refused {
			name = "refused"
		}
		t.Run(name, func(t *testing.T) {
			te, ref := testTerminalCreationPair(t)
			if refused {
				te.s.closeWillDispatch()
			}
			te.sess.mu.Lock()
			w := te.sess.holdTerminalSourceLocked(ref)
			r := te.sess.terminalRetries[mqttTerminalRetryKey{mqttTerminalSourceKeyOf(ref), ref.acq, false, ref.terminalEpoch}]
			armed := r != nil && r.scheduled
			te.sess.mu.Unlock()
			te.s.owedTerminalsMu.Lock()
			owed := len(te.s.owedTerminals)
			te.s.owedTerminalsMu.Unlock()
			if w == nil || (!refused && !armed) || (refused && owed != 1) {
				t.Fatalf("creation escaped without continuation: source=%v armed=%v owed=%d", w != nil, armed, owed)
			}
			if refused {
				te.s.finishOwedTerminals()
			}
			te.waitSourceOwnerGone(t)
			if te.slot() != nil {
				t.Fatal("creation's continuation did not delete the terminal register")
			}
			te.s.owedTerminalsMu.Lock()
			left := len(te.s.owedTerminals)
			te.s.owedTerminalsMu.Unlock()
			if left != 0 {
				t.Fatal("final attempt replaced its own owed instance")
			}
		})
	}
}

// No subsequent recovery or delivery drives either leg. The marker failure is
// reached only after terminal settlement has failed; the live leg must wake
// itself, and the refused leg must remain reachable by the shutdown final pass.
func TestUnitCTerminalCreationSurvivesRecoveryExit(t *testing.T) {
	for _, refused := range []bool{false, true} {
		name := "timer"
		if refused {
			name = "refused"
		}
		t.Run(name, func(t *testing.T) {
			te, ref := testTerminalCreationPair(t)
			if refused {
				te.s.closeWillDispatch()
			}
			var failing atomic.Bool
			var confirmations atomic.Int64
			failing.Store(true)
			markerFailed := false
			te.asm.mu.Lock()
			te.asm.testInflightSourceAck = func(reply string) error {
				confirmations.Add(1)
				if failing.Load() {
					return errors.New("source confirmation unavailable")
				}
				return te.asm.jsa.confirmAck(reply)
			}
			te.asm.testInflightEnumerate = func(filter string) (map[string]uint64, error) {
				if filter != mqttInflightMarkerWalkFilter(te.sess.idHash, te.sess.gen) {
					subjects, _, err := te.asm.jsa.streamSubjects(mqttInflightStreamName, filter)
					return subjects, err
				}
				markerFailed = true
				before := confirmations.Load()
				te.sess.mu.Lock()
				w := te.sess.terminalSources[mqttTerminalSourceKeyOf(ref)]
				r := te.sess.terminalRetries[mqttTerminalRetryKey{mqttTerminalSourceKeyOf(ref), ref.acq, false, ref.terminalEpoch}]
				deferred := r != nil && !r.scheduled && te.sess.terminalRecoveryDepth != 0
				te.sess.mu.Unlock()
				if before == 0 || w == nil || !deferred {
					t.Error("failed settlement did not atomically retain a deferred owner")
				}
				// A shutdown pass must also respect the recovery gate.
				te.s.finishOwedTerminals()
				time.Sleep(2 * mqttActionReconcileDelay)
				if confirmations.Load() != before {
					t.Error("terminal cleanup dispatched inside the recovery walk")
				}
				return nil, errors.New("later marker enumeration unavailable")
			}
			te.asm.mu.Unlock()
			if _, err := te.sess.recoverInflightRegistersOnce(te.c); err != nil {
				t.Fatalf("recovery: %v", err)
			}
			if !markerFailed || confirmations.Load() == 0 {
				t.Fatal("fixture did not reach both failures")
			}
			if refused {
				te.s.finishOwedTerminals() // Still failing: this must retain the same T.
				te.s.owedTerminalsMu.Lock()
				owed := len(te.s.owedTerminals)
				te.s.owedTerminalsMu.Unlock()
				if owed != 1 {
					t.Fatalf("admission refusal lost cleanup: owed=%d", owed)
				}
			}
			te.asm.mu.Lock()
			te.asm.testInflightEnumerate = nil
			te.asm.mu.Unlock()
			failing.Store(false)
			if refused {
				te.s.finishOwedTerminals()
			}
			te.waitSourceOwnerGone(t)
			if te.slot() != nil {
				t.Fatal("restored storage did not complete terminal cleanup")
			}
			te.s.owedTerminalsMu.Lock()
			left := len(te.s.owedTerminals)
			te.s.owedTerminalsMu.Unlock()
			if left != 0 {
				t.Fatal("completed cleanup retained an owed replacement")
			}
		})
	}
}

// Source installation during a live ACK must not invent authority for its
// acquisition. Even positive source completion cannot consume the ACK's hold.
func TestUnitCTerminalCreationWaitsForACKOwner(t *testing.T) {
	te := testNewTerminalEnv(t, "terminal-creation-ack")
	te.s.closeWillDispatch()
	r, msg, err := te.asm.jsa.loadInflightRecord(te.subject)
	if err != nil || r == nil || msg == nil {
		t.Fatalf("load fixture: %v", err)
	}
	ref := *mqttSlotRefFromRecord(te.subject, msg.Sequence, r)
	te.asm.mu.Lock()
	te.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	te.asm.mu.Unlock()
	te.sess.mu.Lock()
	w := te.sess.holdTerminalSourceLocked(ref)
	te.sess.mu.Unlock()
	te.s.finishOwedTerminals()
	te.sess.mu.Lock()
	retained := w != nil && te.sess.terminalSources[mqttTerminalSourceKeyOf(ref)] == w &&
		te.sess.sendReservedLocked(te.pi)
	te.sess.mu.Unlock()
	te.s.owedTerminalsMu.Lock()
	owed := len(te.s.owedTerminals)
	te.s.owedTerminalsMu.Unlock()
	if !retained || owed != 1 {
		t.Fatal("source continuation consumed an unresolved ACK owner's restriction")
	}
	te.asm.mu.Lock()
	te.asm.testInflightSourceState = nil
	te.asm.mu.Unlock()
	te.sess.completeAcknowledgedPublish(nil, te.pi, &mqttPending{slot: ref})
	te.s.finishOwedTerminals()
	te.waitSourceOwnerGone(t)
}
