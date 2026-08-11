package mqtt

import (
	"testing"
)

// TestPubRelStoreFailureRollsBack is test 7: a failed PUBREL store must leave the
// session exactly as it was before the PUBREC.
//
// Regression: mqttProcessPublishReceived advances the in-memory state BEFORE the
// store — it has to, because mqttDeliverPubRelCb calls trackAsPubRel itself, so
// storing first would race a real ack subject against our empty placeholder and
// lose it. When the store then failed, the identifier was wedged in pendingPubRel
// forever (nothing will ever fill its ack subject) and the delivery's PUBLISH
// tracking was gone, so it could neither be redelivered nor acknowledged.
func TestPubRelStoreFailureRollsBack(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{
		id:             "rollback",
		pendingPublish: map[uint16]*mqttPending{},
		pendingPubRel:  map[uint16]*mqttPending{},
		cpending:       map[string]map[uint64]uint16{},
		// trackAsPubRel no-ops without one, so the PUBREC transition needs it.
		pubRelConsumer: &ConsumerConfig{Durable: "PUBREL_DUR"},
	}
	const pi = uint16(7)
	entry := &mqttPending{jsDur: "DUR", sseq: 42, jsAckSubject: "$JS.ACK.x"}
	sess.pendingPublish[pi] = entry
	sess.cpending["DUR"] = map[uint64]uint16{42: pi}

	// What the PUBREC path does before the store.
	sess.mu.Lock()
	sess.trackAsPubRel(pi, _EMPTY_)
	removed := sess.untrackPublishEntry(pi)
	sess.mu.Unlock()
	if removed != entry {
		t.Fatalf("untrackPublishEntry returned %v, want the removed entry", removed)
	}

	// The store fails and no delivery was observed, so the placeholder is still
	// empty: roll back.
	sess.mu.Lock()
	ack, ok := sess.pendingPubRel[pi]
	if !ok || ack.jsAckSubject != _EMPTY_ {
		t.Fatalf("expected an empty PUBREL placeholder, got %v (present=%v)", ack, ok)
	}
	delete(sess.pendingPubRel, pi)
	sess.retrackPublishLocked(pi, removed)
	sess.mu.Unlock()

	if _, wedged := sess.pendingPubRel[pi]; wedged {
		t.Error("the packet identifier is still wedged in pendingPubRel")
	}
	got, ok := sess.pendingPublish[pi]
	if !ok {
		t.Fatal("the PUBLISH tracking was not reinstated, so the delivery can neither be redelivered nor acked")
	}
	if got != entry {
		t.Errorf("pendingPublish[%d] = %v, want the original entry", pi, got)
	}
	if sess.cpending["DUR"][42] != pi {
		t.Error("the (durable, sequence) -> identifier mapping was not reinstated")
	}
}

// TestPubRelRollbackIsConditional is the other half of test 7/8: an AMBIGUOUS
// store — one that landed and whose PUBREL was already DELIVERED, while the caller
// saw an error — must NOT be rolled back.
//
// The callback's trackAsPubRel(pi, reply) fills jsAckSubject, and that filled
// entry is the proof. Rolling back over it would delete a delivered PUBREL's
// tracking (leaking the identifier and leaving nothing for PUBCOMP to ack) and
// reinstate stale PUBLISH tracking for an exchange that has already moved on.
func TestPubRelRollbackIsConditional(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{
		id:             "ambiguous",
		pendingPublish: map[uint16]*mqttPending{},
		pendingPubRel:  map[uint16]*mqttPending{},
		cpending:       map[string]map[uint64]uint16{},
		pubRelConsumer: &ConsumerConfig{Durable: "PUBREL_DUR"},
	}
	const pi = uint16(9)
	entry := &mqttPending{jsDur: "DUR", sseq: 11, jsAckSubject: "$JS.ACK.src"}
	sess.pendingPublish[pi] = entry
	sess.cpending["DUR"] = map[uint64]uint16{11: pi}

	sess.mu.Lock()
	sess.trackAsPubRel(pi, _EMPTY_)
	removed := sess.untrackPublishEntry(pi)
	// The store LANDED and its PUBREL was delivered; the callback filled the entry
	// with a real ack subject before the caller's error came back.
	sess.trackAsPubRel(pi, "$JS.ACK.$MQTT_out.PUBREL_DUR.1.5.5.0.0")
	sess.mu.Unlock()

	// The conditional rollback: only an EMPTY placeholder may be undone.
	sess.mu.Lock()
	if ack, ok := sess.pendingPubRel[pi]; ok && ack != nil && ack.jsAckSubject == _EMPTY_ {
		delete(sess.pendingPubRel, pi)
		sess.retrackPublishLocked(pi, removed)
	}
	sess.mu.Unlock()

	ack, ok := sess.pendingPubRel[pi]
	if !ok {
		t.Fatal("the delivered PUBREL's tracking was rolled back; its identifier leaks and PUBCOMP can never ack it")
	}
	if ack.jsAckSubject == _EMPTY_ {
		t.Error("the delivered PUBREL lost its ack subject")
	}
	if _, back := sess.pendingPublish[pi]; back {
		t.Error("stale PUBLISH tracking was reinstated for an exchange already past PUBREC")
	}
}

// TestConsumerChargeAccounting is test 10: tmaxack must reflect what this process
// actually charged, so a resume-then-teardown cycle cannot drive it negative.
//
// Regression: deleteConsumer subtracted the config's MaxAckPending
// unconditionally, but a consumer RESTORED from the session record is never
// charged (only the create path charges). Tearing one down therefore refunded a
// charge that was never made, inflating the account's remaining budget.
func TestConsumerChargeAccounting(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "charge"}

	// A consumer this process created.
	sess.mu.Lock()
	sess.chargeConsumerLocked("created", 100)
	sess.mu.Unlock()
	if sess.tmaxack != 100 {
		t.Fatalf("tmaxack = %v, want 100 after a create", sess.tmaxack)
	}

	// A consumer restored from the record: no charge, so no refund either.
	sess.mu.Lock()
	sess.refundConsumerChargeLocked("restored")
	sess.mu.Unlock()
	if sess.tmaxack != 100 {
		t.Errorf("tmaxack = %v, want 100: a restored consumer was refunded a charge it never made", sess.tmaxack)
	}

	// The created one refunds exactly once.
	sess.mu.Lock()
	sess.refundConsumerChargeLocked("created")
	sess.refundConsumerChargeLocked("created")
	sess.mu.Unlock()
	if sess.tmaxack != 0 {
		t.Errorf("tmaxack = %v, want 0 after refunding the charge exactly once", sess.tmaxack)
	}
	if sess.tmaxack < 0 {
		t.Error("tmaxack went negative")
	}
}
