//go:build mqttport_fulltests

// Replicated coverage for the "session record already deleted" classification in
// mqttSession.clear. On a cluster the record can vanish underneath the broker
// (another replica's cleanup, a concurrent expiry), and JetStream then answers a
// delete for that sequence with a GENERIC "message delete failed" (10057) whose
// only record of the actual reason is ErrStoreMsgNotFound flattened into
// ApiError.Description. Accepting it is what keeps session teardown from failing
// on an already-completed delete.
//
// This needs a real cluster: it is the clustered code path that produces the
// error, and mqttSession.clear already tolerates the other already-deleted
// answer (JSSequenceNotFoundErrF, 10043), so a test that merely deleted the
// record and watched clear() succeed could pass without touching the 10057
// handling at all.

package mqtt

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestMQTTSessionDeleteAlreadyDeletedReplicated proves, on a genuine R3 cluster,
// that a delete of an already-removed session record comes back as 10057 caused
// by ErrStoreMsgNotFound, that isJSErrCausedBy recognises it, and that session
// teardown accepts it. Always-run: no failure injection or elections, so it is
// fast.
func TestMQTTSessionDeleteAlreadyDeletedReplicated(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	js, err := b.nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}

	const cid = "sessdel-r3"
	mc, mr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer mc.Close()
	testMQTTReadConnAckV5(t, mr)

	cl := testMQTTGetClient(t, b.s, cid)
	sess := cl.mqtt.sess
	if sess == nil {
		t.Fatal("no session for the connected client")
	}
	jsa := sess.jsa

	// Fixture precondition: this record must be the stream head, so that deleting
	// it leaves the stale sequence BELOW FirstSeq. That is what makes the store
	// report the sequence as absent (ErrStoreMsgNotFound -> 10057); an interior
	// tombstone would instead take the "removed nothing" branch and answer 10043,
	// which is the pre-existing path and would not exercise the change.
	var seq uint64
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		sess.mu.Lock()
		seq = sess.seq
		sess.mu.Unlock()
		if seq == 0 {
			return fmt.Errorf("session record not persisted yet")
		}
		si, err := js.StreamInfo(mqttSessStreamName)
		if err != nil {
			return fmt.Errorf("stream info: %w", err)
		}
		if si.Config.Replicas != 3 {
			return fmt.Errorf("Config.Replicas=%d, want 3", si.Config.Replicas)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Msgs=%d, want exactly 1 session record", si.State.Msgs)
		}
		if si.State.FirstSeq != seq {
			return fmt.Errorf("FirstSeq=%d, want the session record at %d", si.State.FirstSeq, seq)
		}
		return nil
	})

	// Remove the record out from under the broker, then wait until the stale
	// sequence really is below the head on the replicated view.
	if err := js.DeleteMsg(mqttSessStreamName, seq); err != nil {
		t.Fatalf("external delete of session record at %v: %v", seq, err)
	}
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttSessStreamName)
		if err != nil {
			return fmt.Errorf("stream info: %w", err)
		}
		if si.State.FirstSeq <= seq {
			return fmt.Errorf("FirstSeq=%d, want > %d (stale sequence still at or above the head)",
				si.State.FirstSeq, seq)
		}
		return nil
	})

	// The classified error itself, from the clustered delete path.
	err = jsa.deleteMsg(mqttSessStreamName, seq, true)
	if err == nil {
		t.Fatal("Expected an error deleting the already-removed session record")
	}
	ae := jsAPIError(err)
	if ae == nil {
		t.Fatalf("Expected a JetStream ApiError, got %T: %v", err, err)
	}
	wantCode := ApiErrors[JSStreamMsgDeleteFailedF].ErrCode
	if ae.ErrCode != wantCode {
		t.Fatalf("Expected ErrCode %d (message delete failed), got %d with description %q. "+
			"ErrCode %d means the fixture did not put the stale sequence below the stream head",
			wantCode, ae.ErrCode, ae.Description, ApiErrors[JSSequenceNotFoundErrF].ErrCode)
	}
	if !isJSErrCausedBy(err, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
		t.Fatalf("isJSErrCausedBy did not classify %v (description %q) as caused by %v",
			err, ae.Description, ErrStoreMsgNotFound)
	}

	// And session teardown must accept it. clear() is driven directly, under the
	// session-manager lock exactly as the production callers hold it, because
	// sess.seq has to still be stale when it runs: disconnecting first would
	// re-persist the record (the disconnect save stamps DisconnectedAt) and a
	// clean-start CONNECT reloads the record, finds it gone and resets sess.seq to
	// 0 — in both cases clear() never issues the delete under test and the
	// assertion would be vacuous.
	sess.mu.Lock()
	stillStale := sess.seq == seq
	sess.mu.Unlock()
	if !stillStale {
		t.Fatalf("Session record was re-persisted (seq moved off %v); the teardown "+
			"assertion below would not exercise the stale delete", seq)
	}
	asm := cl.mqtt.asm
	if err := asm.lockSession(sess, cl); err != nil {
		t.Fatalf("unable to lock session: %v", err)
	}
	err = sess.clear(false)
	asm.unlockSession(sess)
	// ACCEPTANCE is the assertion, and since F-029 it has two shapes rather than one.
	// claimTeardown has always reported an absent record as SUPERSEDED — "the record is
	// gone, so somebody else already released this session" — and clear() used to
	// flatten that to nil. It now returns errMQTTClearSuperseded, which says the same
	// thing with a type: the base is not current, nothing was deleted on its behalf, and
	// there is no further record work. All seven callers treat it as complete: four are
	// terminal, the two CONNECT restore paths re-read and start fresh, and the CONNECT
	// rollback logs it and lets its own authoritative re-read classify the survivor.
	//
	// So the check is narrowed rather than relaxed: anything OTHER than those two
	// outcomes is still a rejection of the already-deleted record.
	if err != nil && !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("Session teardown rejected the already-deleted record: %v", err)
	}
	// And it stayed deleted: a teardown that "accepted" the absence by recreating the
	// record would satisfy the check above and be exactly wrong.
	if _, lerr := jsa.loadSessionMsg(sess.domainTk, sess.idHash); lerr == nil ||
		isErrorOtherThan(lerr, JSNoMessageFoundErr) {
		t.Fatalf("the record must still be absent after the teardown, got %v", lerr)
	}
}
