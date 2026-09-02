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
	return prepared.destroy(capsule.permit, noWait)
}
