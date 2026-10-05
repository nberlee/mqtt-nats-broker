package mqtt

import "testing"

// mqttTestRetireBound discards a session that still has a client BOUND, by the same
// two-phase retirement mqttProcessConnect's clean-start arm performs: close
// admission, drain, capture, prepare, detach, mint, destroy (F-036 unit 1).
//
// It exists because sess.clear() on a bound object is no longer a legal sequence —
// property P forbids re-minting an incarnation while a client is attached, and the
// permit refuses it — so a fixture that binds a client to reach a code path (an
// ownership check, a send section) must retire the way production does. What the
// tests assert about the discard is unchanged; only how they drive it is.
func mqttTestRetireBound(t *testing.T, asm *mqttAccountSessionManager, sess *mqttSession, noWait bool) error {
	t.Helper()
	capsule, prepared, err := asm.retireBoundIncarnation(sess)
	if err != nil {
		return err
	}
	// Every destroy now takes the production receipt rule (F-105 8b): a SERVING authority at the
	// retiring generation is closed discarding, anything else follows the closing licence.
	return prepared.destroy(capsule.permit, noWait)
}

// testInstallFixtureTenure gives a fixture session that never went through a CONNECT the
// tenure an acceptance would have, and INSTALLS it on the generation's ledger — the production
// shape since F-105 8b, where only an installation creates a ledger and a join never does. A
// session that adopted an acceptance fact mints exactly as the CONNECT does; one that did not
// gets a well-formed synthetic tenure.
func testInstallFixtureTenure(t *testing.T, sess *mqttSession) mqttSharedTenure {
	t.Helper()
	sess.mu.Lock()
	tenure, ref, ent := sess.sharedTenure, sess.acceptFact, sess.boundEnt.Tenure
	sess.mu.Unlock()
	if tenure.isZero() {
		if ref.Subj != "" && ent != "" {
			if err := sess.mintAcceptanceTenure(sess.jsa, ent); err != nil {
				t.Fatalf("minting the fixture's tenure: %v", err)
			}
		} else {
			sess.mu.Lock()
			sess.sharedTenure = mqttSharedTenure{Seq: 1, ID: mqttNewInflightNonce()}
			sess.mu.Unlock()
		}
	}
	sess.mu.Lock()
	id, gen, epoch, tenure := sess.id, sess.gen, sess.sharedEpoch(), sess.sharedTenure
	sess.mu.Unlock()
	if _, err := sess.jsa.installSharedTenure(id, epoch, gen, tenure); err != nil {
		t.Fatalf("installing the fixture's tenure on generation %q: %v", gen, err)
	}
	return tenure
}
