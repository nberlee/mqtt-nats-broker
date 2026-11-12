package mqtt

import "testing"

// mqttTestRetireBound discards a session that still has a client BOUND, by the same
// two-phase retirement mqttProcessConnect's clean-start arm performs: close
// admission, drain, capture, prepare, detach, mint, destroy (issue #40 unit 1).
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
	// Every destroy now takes the production receipt rule (issue #107 8b): a SERVING authority at the
	// retiring generation is closed discarding, anything else follows the closing licence.
	return prepared.destroy(capsule.permit, noWait)
}

// testInstallFixtureTenure gives a fixture session that never went through a CONNECT the
// tenure an acceptance would have, and INSTALLS it on the generation's ledger — the production
// shape since issue #107 8b, where only an installation creates a ledger and a join never does. A
// session that adopted an acceptance fact mints exactly as the CONNECT does; one that did not
// gets a well-formed synthetic tenure.
func testInstallFixtureTenure(t *testing.T, sess *mqttSession) mqttSharedTenure {
	t.Helper()
	sess.mu.Lock()
	tenure, ent := sess.sharedTenure, sess.boundEnt.Tenure
	sess.mu.Unlock()
	// A SESSION WITH NO AUTHORITY gets the rig's, in a lifecycle of its own (issue #133): every
	// ledger step is fenced by the authority of the generation it names.
	if al := sess.jsa.loadOutboundAuthority(sess.id, sess.sharedEpoch()); al.status == mqttOutboundAbsent && sess.asm != nil {
		testInstallRigAuthority(t, sess.asm, sess)
	}
	if tenure.isZero() {
		al := sess.jsa.loadOutboundAuthority(sess.id, sess.sharedEpoch())
		if ent != "" && al.status == mqttOutboundValid && al.rec != nil && al.rec.Life != nil &&
			al.rec.Life.Accept.Tenure == ent {
			if err := sess.mintAcceptanceTenure(sess.jsa, ent, al.rec.Life.Accept.Nonce); err != nil {
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
	// The tenure is the lifecycle's current acceptance, as the CONNECT that minted it made it.
	testAcceptSharedTenure(t, sess.jsa, id, epoch, tenure)
	if _, err := sess.jsa.installSharedTenure(id, epoch, gen, tenure); err != nil {
		t.Fatalf("installing the fixture's tenure on generation %q: %v", gen, err)
	}
	return tenure
}
