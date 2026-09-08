package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

var (
	errDrainTestSaveFailed = errors.New("test: session save failed")
	errDrainTestNotYet     = errors.New("test: not yet")
)

// drainCheckFor polls until fn returns nil, or fails. Named apart from the
// fulltests harness's checkFor so both can exist in the same package.
func drainCheckFor(t testing.TB, totalWait, sleepDur time.Duration, fn func() error) {
	t.Helper()
	var err error
	for end := time.Now().Add(totalWait); time.Now().Before(end); {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	t.Fatalf("timed out after %v: %v", totalWait, err)
}

// drainTestSession builds a real session backed by an embedded server, so the
// record writes and JetStream lookups below are the production ones.
func drainTestSession(t *testing.T, clientID string) (*Server, *client, *mqttSession, *nats.Conn) {
	t.Helper()
	_, nc := runNATS(t)
	s := &Server{nrc: nc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: "node1"}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	t.Cleanup(func() { close(s.quitCh) })

	c := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	sess, _, err := asm.createOrRestoreSession(clientID, s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	return s, c, sess, nc
}

// addTestDrain registers a drain directly, which is what a session restored from a
// record holds before any recovery step runs.
func addTestDrain(sess *mqttSession, sid, durable, filter string, qos byte, seqs map[uint64]*drainSeq) *mqttDrain {
	d := &mqttDrain{
		cc: &ConsumerConfig{
			Durable:        durable,
			DeliverSubject: mqttSubPrefix + durable,
			FilterSubject:  mqttStreamSubjectPrefix + filter,
			AckPolicy:      AckExplicit,
		},
		qos:  qos,
		seqs: seqs,
	}
	sess.mu.Lock()
	if sess.drains == nil {
		sess.drains = make(map[string]*mqttDrain)
	}
	if sess.drainByDur == nil {
		sess.drainByDur = make(map[string]*mqttDrain)
	}
	sess.drains[sid] = d
	sess.drainByDur[durable] = d
	sess.mu.Unlock()
	return d
}

func drainSeqState(sess *mqttSession, d *mqttDrain, seq uint64) drainSeq {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if st := d.seqs[seq]; st != nil {
		return *st
	}
	return drainSeq{}
}

func drainRegistered(sess *mqttSession, durable string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.drainByDur[durable] != nil
}

// TestDrainRecoveryKeepsIntentBearingClaim pins the rule that separates the two
// phases: past PUBREC a claim no longer depends on its source message at all.
//
// What the client is owed there is a PUBREL, and the persisted intent is the ONLY
// record of that obligation — so reconciling against consumer state must never
// prune it, however conclusively the source consumer is gone. A sequence still in
// the PUBLISH phase is the opposite case: with its consumer gone the message can
// neither be delivered nor acked, so it is reported as LOSS and dropped.
//
// Regression: recoverDrains dropped the whole drain whenever its consumer had
// disappeared, discarding the intent with it. The client kept its packet
// identifier pending and the exchange could never complete — a silently abandoned
// QoS 2 message rather than a reported loss.
func TestDrainRecoveryKeepsIntentBearingClaim(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-intent")

	// Neither durable exists as a consumer, so both lookups say "gone".
	owing := addTestDrain(sess, "sid-owing", "dur-owing", "foo", 2, map[uint64]*drainSeq{
		7: {claimed: true, intent: true, pi: 42},
	})
	plain := addTestDrain(sess, "sid-plain", "dur-plain", "bar", 1, map[uint64]*drainSeq{
		9: {claimed: true},
	})

	sess.recoverDrains(c, nil)

	if !drainRegistered(sess, "dur-owing") {
		t.Fatal("a drain owing a PUBREL must survive its consumer's disappearance: the client is owed the second phase, not the payload")
	}
	if st := drainSeqState(sess, owing, 7); !st.claimed || !st.intent || st.pi != 42 {
		t.Fatalf("the intent-bearing claim must be untouched, got %+v", st)
	}
	if drainRegistered(sess, "dur-plain") {
		t.Fatal("a drain with only PUBLISH-phase claims must be dropped once its consumer is gone")
	}
	if st := drainSeqState(sess, plain, 9); st.claimed {
		t.Fatal("a PUBLISH-phase claim whose consumer is gone is lost, and must not stay claimed")
	}
}

// TestDrainRecoverySkipsUnresolvedDurables pins that reconciliation stays away
// from a drain the past-PUBREC barrier could not resolve. Its PUBREL state is
// unknown, so nothing about it may be pruned — the next resume retries.
func TestDrainRecoverySkipsUnresolvedDurables(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-skip")

	d := addTestDrain(sess, "sid", "dur-unresolved", "foo", 2, map[uint64]*drainSeq{
		3: {claimed: true},
	})

	sess.recoverDrains(c, map[string]bool{"dur-unresolved": true})

	if !drainRegistered(sess, "dur-unresolved") {
		t.Fatal("a barrier-unresolved drain must not be dropped by reconciliation")
	}
	if st := drainSeqState(sess, d, 3); !st.claimed {
		t.Fatal("a barrier-unresolved drain's claims must be left alone")
	}
}

// TestDrainRecoveryUndoesAFailedSave pins that a reconciliation which does not
// become durable does not linger in memory as if it had.
//
// Regression: recoverDrains mutated first and saved afterwards with no rollback,
// so a failed save left the session believing claims were released and drains
// dropped while the record still carried them.
func TestDrainRecoveryUndoesAFailedSave(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "drain-rollback")

	d := addTestDrain(sess, "sid", "dur-gone", "foo", 1, map[uint64]*drainSeq{
		5: {claimed: true},
	})

	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	sess.recoverDrains(c, nil)
	restore()

	if !drainRegistered(sess, "dur-gone") {
		t.Fatal("a drop that was not persisted must be undone: the record still carries the drain")
	}
	if st := drainSeqState(sess, d, 5); !st.claimed {
		t.Fatal("a claim released by a save that failed must be restored")
	}

	// And with the save working, the same reconciliation applies.
	sess.recoverDrains(c, nil)
	if drainRegistered(sess, "dur-gone") {
		t.Fatal("expected the drain to be dropped once the reconciliation persists")
	}
}

// TestDrainRecoveryOpensTheFenceBeforeMutating pins I8 for this path: the record
// transaction must be open before the first mutation, or another writer can
// observe — and persist — a reconciliation this one may still roll back.
//
// Regression: recoverDrains mutated under sess.mu and only then opened the fence.
func TestDrainRecoveryOpensTheFenceBeforeMutating(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-fence")

	d := addTestDrain(sess, "sid", "dur-gone", "foo", 1, map[uint64]*drainSeq{
		5: {claimed: true},
	})

	// Hold the fence as another writer would.
	held := sess.beginRecordTx()

	done := make(chan struct{})
	go func() {
		defer close(done)
		sess.recoverDrains(c, nil)
	}()

	// While the fence is held, the reconciliation must be invisible. It may take
	// its JetStream lookups, but it may not touch the drain.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if st := drainSeqState(sess, d, 5); !st.claimed {
			t.Fatal("the claim was released while another transaction held the fence: the mutation happened outside the transaction")
		}
		if !drainRegistered(sess, "dur-gone") {
			t.Fatal("the drain was dropped while another transaction held the fence")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := sess.endRecordTx(held); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recoverDrains did not complete after the fence was released")
	}
	if drainRegistered(sess, "dur-gone") {
		t.Fatal("expected the reconciliation to apply once it could open a transaction")
	}
}

// TestDrainPubRelIntentOpensTheFenceBeforeMutating pins the same rule for the
// intent write, which is the linearization point of the QoS 2 second phase — the
// one mutation whose premature visibility would let another writer persist an
// intent this connection may still roll back.
//
// It also pins the BLOCKING fence form: the intent is written from the read loop,
// where refusing on contention would turn a busy fence into a dropped connection.
func TestDrainPubRelIntentOpensTheFenceBeforeMutating(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-intent-fence")

	d := addTestDrain(sess, "sid", "dur-intent", "foo", 2, map[uint64]*drainSeq{
		11: {claimed: true, pi: 5},
	})

	held := sess.beginRecordTx()

	var (
		mu       sync.Mutex
		definite bool
		err      error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, dfn, e := sess.recordPubRelIntent(c, "dur-intent", 11, 5, "xxxxxxxxxxxxxxxxxxxxxx")
		mu.Lock()
		definite, err = dfn, e
		mu.Unlock()
	}()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if st := drainSeqState(sess, d, 11); st.intent {
			t.Fatal("the intent became visible while another transaction held the fence")
		}
		select {
		case <-done:
			t.Fatal("recordPubRelIntent returned while the fence was held: it must wait, not refuse (a refusal drops the connection)")
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}

	if terr := sess.endRecordTx(held); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recordPubRelIntent did not complete after the fence was released")
	}

	mu.Lock()
	gotDefinite, gotErr := definite, err
	mu.Unlock()
	if gotErr != nil || gotDefinite {
		t.Fatalf("the intent write should have succeeded once the fence was free, got definite=%v err=%v", gotDefinite, gotErr)
	}
	if st := drainSeqState(sess, d, 11); !st.intent || st.pi != 5 || st.phase != drainPubRelUncertain {
		t.Fatalf("expected a recorded intent in the uncertain phase, got %+v", st)
	}
}

// TestDrainRecoveryPrunesAMessageAbsentAboveFirstSeq pins the exact-message
// lookup. Per-message expiry deletes individual sequences, leaving holes ABOVE the
// stream's first sequence, so "below FirstSeq" is not the only way a claimed
// message can be gone. A claim for a message that no longer exists can neither be
// delivered nor acked, and would otherwise hold its drain open forever.
func TestDrainRecoveryPrunesAMessageAbsentAboveFirstSeq(t *testing.T) {
	t.Parallel()
	_, c, sess, nc := drainTestSession(t, "drain-hole")

	const filter = "holefilter"
	durable := "dur-hole"
	inbox := mqttSubPrefix + durable

	// A subscriber that never acks, so the consumer keeps its deliveries pending.
	if _, err := nc.Subscribe(inbox, func(*nats.Msg) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := sess.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable:        durable,
			DeliverSubject: inbox,
			FilterSubject:  mqttStreamSubjectPrefix + filter,
			AckPolicy:      AckExplicit,
			AckWait:        time.Minute,
			MaxAckPending:  1024,
		},
	}); err != nil {
		t.Fatalf("createDurableConsumer: %v", err)
	}

	var seqs []uint64
	for i := 0; i < 2; i++ {
		resp, err := sess.jsa.storeMsg(mqttStreamSubjectPrefix+filter, 0, []byte("m"))
		if err != nil {
			t.Fatalf("storeMsg: %v", err)
		}
		seqs = append(seqs, resp.Sequence)
	}
	drainCheckFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ci, err := sess.jsa.lookupConsumer(mqttStreamName, durable)
		if err != nil {
			return err
		}
		if ci.NumAckPending != 2 {
			return errDrainTestNotYet
		}
		return nil
	})

	// Punch a hole at the HIGHER sequence, so the lower one keeps FirstSeq below it
	// and only an exact lookup can tell that it is gone.
	if err := sess.jsa.deleteMsg(mqttStreamName, seqs[1], true); err != nil {
		t.Fatalf("deleteMsg: %v", err)
	}
	si, err := sess.jsa.lookupStream(mqttStreamName)
	if err != nil {
		t.Fatalf("lookupStream: %v", err)
	}
	if si.State.FirstSeq > seqs[1] {
		t.Fatalf("this test needs the hole ABOVE the first sequence, got first=%v hole=%v", si.State.FirstSeq, seqs[1])
	}

	d := addTestDrain(sess, "sid", durable, filter, 1, map[uint64]*drainSeq{
		seqs[0]: {claimed: true},
		seqs[1]: {claimed: true},
	})

	sess.recoverDrains(c, nil)

	if st := drainSeqState(sess, d, seqs[0]); !st.claimed {
		t.Fatal("a claim whose message is still in the stream must be kept")
	}
	if st := drainSeqState(sess, d, seqs[1]); st.claimed {
		t.Fatalf("a claim whose message was deleted above FirstSeq must be pruned by the exact lookup (first=%v)", si.State.FirstSeq)
	}
}

// TestDrainClearsTheConsumerInactivityTimer pins that a committed drain sheds the
// per-consumer deletion timer an ordinary subscription is created with.
//
// A drain exists precisely to outlive a disconnect, so an inactivity timer would
// reap the durable while the session is offline and take its started deliveries
// with it. Unlike the stream-level limit — which no consumer config can escape, and
// which makes the broker refuse to drain at all — this one is escapable, so it is
// cleared.
func TestDrainClearsTheConsumerInactivityTimer(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-timer")

	const filter = "timerfilter"
	durable := "dur-timer"
	cfg := ConsumerConfig{
		Durable:           durable,
		DeliverSubject:    mqttSubPrefix + durable,
		FilterSubject:     mqttStreamSubjectPrefix + filter,
		AckPolicy:         AckExplicit,
		DeliverPolicy:     DeliverNew,
		AckWait:           time.Minute,
		MaxAckPending:     1024,
		InactiveThreshold: 5 * time.Minute,
	}
	if _, err := sess.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName, Config: cfg}); err != nil {
		t.Fatalf("createDurableConsumer: %v", err)
	}

	d := addTestDrain(sess, "sid", durable, filter, 1, map[uint64]*drainSeq{3: {claimed: true}})
	sess.mu.Lock()
	d.cc = &cfg
	d.staged = true
	timed, _ := sess.commitDrains()
	sess.mu.Unlock()

	if len(timed) != 1 {
		t.Fatalf("commitDrains must report the drains still carrying an inactivity timer, got %d", len(timed))
	}
	sess.clearDrainInactivityTimers(c, timed)

	ci, err := sess.jsa.lookupConsumer(mqttStreamName, durable)
	if err != nil {
		t.Fatalf("lookupConsumer: %v", err)
	}
	if ci.Config.InactiveThreshold != 0 {
		t.Fatalf("a drained durable must not keep a deletion timer, got %v: it would be reaped with its started deliveries while the session is offline", ci.Config.InactiveThreshold)
	}
	sess.mu.Lock()
	got := d.cc.InactiveThreshold
	sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("the cleared threshold must be reflected in the drain the record is built from, got %v", got)
	}
}

// TestSessionNeedsPubRelDeliveryForAStoredPubRel pins that the PUBREL delivery
// path is re-established on resume for a session that carries stored PUBRELs even
// when it holds no drain at all.
//
// pendingPubRel is memory-only, so after a restart the resume path's
// hasPendingPubRel guard is false and nothing else drives
// ensurePubRelConsumerSubscription. Gating it on "has a drain" left such a session
// unable to deliver a PUBREL it had already stored — the client waits on a PUBCOMP
// exchange that nothing will ever advance.
func TestSessionNeedsPubRelDeliveryForAStoredPubRel(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "pubrel-only")

	if sess.needsPubRelDelivery() {
		t.Fatal("a session with neither a PUBREL consumer nor a QoS 2 drain needs nothing")
	}

	sess.mu.Lock()
	sess.pubRelConsumer = &ConsumerConfig{Durable: "pubrel-dur"}
	sess.mu.Unlock()
	if !sess.needsPubRelDelivery() {
		t.Fatal("a persisted PUBREL consumer must drive PUBREL delivery on resume, drain or no drain")
	}

	sess.mu.Lock()
	sess.pubRelConsumer = nil
	sess.mu.Unlock()
	addTestDrain(sess, "sid1", "dur-q1", "foo", 1, nil)
	if sess.needsPubRelDelivery() {
		t.Fatal("a QoS 1 drain owes no PUBREL")
	}
	addTestDrain(sess, "sid2", "dur-q2", "bar", 2, nil)
	if !sess.needsPubRelDelivery() {
		t.Fatal("a QoS 2 drain may have to have its PUBREL recreated, so the delivery path must be ready first")
	}
	if durs := sess.qos2DrainDurables(); len(durs) != 1 || durs[0] != "dur-q2" {
		t.Fatalf("the withheld set must be exactly the QoS 2 drains, got %v", durs)
	}
}

// TestDrainAmbiguousSaveKeepsTheAttemptedState pins the rule that separates a
// definite failure from an ambiguous one.
//
// save() records an ambiguous failure together with a DIGEST of the bytes it tried
// to store, and the readback later recognises those bytes and adopts the record's
// sequence. That is only sound while memory still holds them. A caller that rolls
// its mutation back on every error therefore leaves the session describing state
// the record may not have — and the next save overwrites what did land.
//
// Regression: every drain caller restored unconditionally, so an ambiguous write
// could resurrect a claim the record had already released, or drop a drain the
// record still carried.
func TestDrainAmbiguousSaveKeepsTheAttemptedState(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "ambiguous-save")

	d := addTestDrain(sess, "sid", "dur-gone", "foo", 1, map[uint64]*drainSeq{
		5: {claimed: true},
	})

	// A DEFINITE failure: nothing was stored, so the reconciliation is undone.
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	sess.recoverDrains(c, nil)
	restore()
	if !drainRegistered(sess, "dur-gone") || !drainSeqState(sess, d, 5).claimed {
		t.Fatal("a definite failure must be rolled back")
	}

	// An AMBIGUOUS failure: the write may have landed, so the reconciliation stays.
	restoreAmb := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	// The readback is made unavailable too, so the reconciliation is observable
	// before it is resolved: this path now resolves an ambiguous write immediately
	// (see resolveUncertainNow), and a landed-response-lost write would otherwise
	// settle before the assertion runs.
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	sess.recoverDrains(c, nil)
	restoreRead()
	restoreAmb()

	if !sess.isSeqUncertain() {
		t.Fatal("an ambiguous record write must leave the session unresolved")
	}
	if drainRegistered(sess, "dur-gone") {
		t.Fatal("an ambiguous write must KEEP the drop: the record may already carry it, and restoring here makes the next save overwrite what landed")
	}
	if drainSeqState(sess, d, 5).claimed {
		t.Fatal("an ambiguous write must KEEP the released claim")
	}
}

// TestDrainDefiniteRollbackHappensInsideTheTransaction pins that the undo of a
// definite failure is not visible to another writer.
//
// Rolling back after releasing the fence leaves a window in which another writer
// can acquire it and persist exactly the temporary state this transaction is
// undoing — which is the same hazard the fence exists to prevent, reintroduced at
// the end of the transaction instead of the start.
func TestDrainDefiniteRollbackHappensInsideTheTransaction(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "rollback-window")

	d := addTestDrain(sess, "sid", "dur-window", "foo", 1, map[uint64]*drainSeq{
		5: {claimed: true, ack: "$JS.ACK.fake"},
	})

	// Observed at the instant the fence is released, with the session lock held:
	// this is exactly what the next holder of the fence will see, so the assertion
	// is about ordering rather than about which goroutine the scheduler favours.
	var claimedAtRelease, observed atomic.Bool
	restoreRelease := setMQTTTestRecordTxReleaseHook(func(s *mqttSession) {
		if s != sess {
			return
		}
		// sess.mu is held by the releasing frame, so this reads the same memory the
		// next writer would, without taking the lock again.
		if st := d.seqs[5]; st != nil && observed.CompareAndSwap(false, true) {
			claimedAtRelease.Store(st.claimed)
		}
	})
	defer restoreRelease()

	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	sess.finalizeDrainDelivery(c, "dur-window", 5, "$JS.ACK.fake")
	restore()
	restoreRelease()

	if !observed.Load() {
		t.Fatal("the transaction was never released, so this test proves nothing")
	}
	if !drainSeqState(sess, d, 5).claimed {
		t.Fatal("a definite failure must restore the claim")
	}
	if !claimedAtRelease.Load() {
		t.Fatal("the claim was still removed when the transaction was released: the rollback happens after the fence is free, " +
			"so another writer can acquire it and persist the state this transaction is undoing")
	}
}

// TestSubscribeAmbiguousSaveKeepsThePacketState pins the packet-level form of the
// same rule: a SUBSCRIBE whose final record write MAY have landed must not be
// rolled back.
//
// rollbackSubscribePacket restores the pre-packet membership and consumer maps and
// then re-persists them. If the failed write actually landed, that replaces the
// state the record may already hold. Which way memory has to be reconciled is not
// knowable until the readback classifies the write, so committing to "it did not
// land" at failure time is a guess — and if it did land, the subscriptions the stored
// record references are torn down underneath it.
func TestSubscribeAmbiguousSaveKeepsThePacketState(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "sub-ambiguous")

	// A membership the packet is about to add.
	filters := []*mqttFilter{{filter: "amb/topic", qos: 0}}

	restore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	tok := sess.beginRecordTx()
	err := sess.update(tok, filters, true)
	outcome := sess.classifySave(false, err)
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	restore()

	if err == nil {
		t.Fatal("this test needs the record write to fail")
	}
	if outcome != saveMaybeLanded {
		t.Fatalf("a lost response must classify as possibly-landed, got %v", outcome)
	}
	if !sess.isSeqUncertain() {
		t.Fatal("an ambiguous write must leave the session unresolved")
	}

	// The attempted membership must still be there: it is what the record may hold,
	// and what the readback will be asked to recognise.
	sess.mu.Lock()
	_, present := sess.subs["amb/topic"]
	sess.mu.Unlock()
	if !present {
		t.Fatal("the attempted membership was reverted after an ambiguous write: the record may already hold it, and only the readback can decide how memory reconciles")
	}
	_ = c
}

// TestSharedSubRollbackClassifiesItsOwnSave pins that rollbackSharedSub — itself a
// mutate-before-save caller — honours the outcome of its COMPENSATING write.
//
// It undoes the early membership persistence of a shared filter whose subscribe
// then failed, and does so through update(), which mutates the membership maps and
// only then persists them. So the same rule applies to it as to every other such
// caller, in both directions:
//
//   - DEFINITE failure: nothing was stored, so the record still holds the
//     previously persisted membership. The compensating mutation must be undone, or
//     memory and record disagree and a later save writes the wrong one.
//   - AMBIGUOUS failure: the record may already carry the rolled-back state, and
//     the readback is what decides how memory reconciles. The rollback must be KEPT
//     until it does.
//
// Both branches are covered: removing a newly-added membership, and restoring a
// re-SUBSCRIBE to its previous QoS.
func TestSharedSubRollbackClassifiesItsOwnSave(t *testing.T) {
	definiteFail := func() func() {
		return setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	}
	ambiguousFail := func() func() {
		return setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if err == nil {
				return nil, errors.New("test: session record response lost")
			}
			return resp, err
		})
	}

	t.Run("new membership, definite failure keeps it", func(t *testing.T) {
		_, c, sess, _ := drainTestSession(t, "rbshared-new-def")
		f := &mqttFilter{shared: true, group: "g1", filter: "rb/topic", qos: 1}
		sid := f.sessionKey()

		// The state the early persist left behind: the membership is present.
		sess.mu.Lock()
		sess.sharedSubs = map[string]byte{sid: 1}
		sess.mu.Unlock()

		restore := definiteFail()
		tok := sess.beginRecordTx()
		_, _, err := sess.rollbackSharedSub(c, tok, f, true, false, 0, false, 0)
		if terr := sess.endRecordTx(tok); terr != nil {
			t.Fatalf("endRecordTx: %v", terr)
		}
		restore()
		if err == nil {
			t.Fatal("this test needs the compensating write to fail")
		}

		sess.mu.Lock()
		_, present := sess.sharedSubs[sid]
		sess.mu.Unlock()
		if !present {
			t.Fatal("the compensating removal was left applied although its save definitely failed: " +
				"the record still holds the membership, so a later save would persist the wrong state")
		}
	})

	t.Run("new membership, ambiguous failure keeps the removal", func(t *testing.T) {
		_, c, sess, _ := drainTestSession(t, "rbshared-new-amb")
		f := &mqttFilter{shared: true, group: "g1", filter: "rb/topic", qos: 1}
		sid := f.sessionKey()

		sess.mu.Lock()
		sess.sharedSubs = map[string]byte{sid: 1}
		sess.mu.Unlock()

		restore := ambiguousFail()
		tok := sess.beginRecordTx()
		_, _, err := sess.rollbackSharedSub(c, tok, f, true, false, 0, false, 0)
		if terr := sess.endRecordTx(tok); terr != nil {
			t.Fatalf("endRecordTx: %v", terr)
		}
		restore()
		if err == nil {
			t.Fatal("this test needs the compensating write to fail")
		}
		if !sess.isSeqUncertain() {
			t.Fatal("an ambiguous write must leave the session unresolved")
		}

		sess.mu.Lock()
		_, present := sess.sharedSubs[sid]
		sess.mu.Unlock()
		if present {
			t.Fatal("the compensating removal was undone although the write may have landed: " +
				"the record may already omit it, and only the readback can decide how memory reconciles")
		}
	})

	t.Run("re-SUBSCRIBE, definite failure keeps the attempted QoS", func(t *testing.T) {
		testRequireExperimentalSharedQoS2(t)

		_, c, sess, _ := drainTestSession(t, "rbshared-resub-def")
		f := &mqttFilter{shared: true, group: "g1", filter: "rb/topic", qos: 2}
		sid := f.sessionKey()

		// The attempted (upgraded) QoS is what the early persist stored.
		sess.mu.Lock()
		sess.sharedSubs = map[string]byte{sid: 2}
		sess.mu.Unlock()

		restore := definiteFail()
		tok := sess.beginRecordTx()
		// priorExisted: the filter was subscribed at QoS 1 before this packet.
		_, _, err := sess.rollbackSharedSub(c, tok, f, true, true, 1, false, 0)
		if terr := sess.endRecordTx(tok); terr != nil {
			t.Fatalf("endRecordTx: %v", terr)
		}
		restore()
		if err == nil {
			t.Fatal("this test needs the compensating write to fail")
		}

		sess.mu.Lock()
		got := sess.sharedSubs[sid]
		sess.mu.Unlock()
		if got != 2 {
			t.Fatalf("shared QoS = %v, want the attempted 2: the restore to the prior QoS was left applied "+
				"although its save definitely failed, so memory disagrees with the record", got)
		}
	})
}

// TestDrainAmbiguousCompletionIsSettledForward is plan test 5(b) for the completion
// path: an ambiguous removal is settled by RE-ATTEMPTING it, and only then acked.
//
// This is the case that makes "keep the mutation when it may have landed"
// insufficient on its own. The claim is released in memory while the record may
// still hold it, and nothing else writes while the session is unresolved — so
// everything IRREVERSIBLE waits for proof: a redelivery must not be acked away and a
// correlated PUBREL must not be forwarded, or the client can PUBCOMP an exchange the
// record still says is owed (I1, I10). What supplies that proof is the re-attempt,
// not a read: a record found at the base sequence is silent about a proposal that may
// still commit, and undoing there is what let a late landing be read as foreign.
func TestDrainAmbiguousCompletionIsSettledForward(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "amb-notland")

	d := addTestDrain(sess, "sid", "dur-notland", "foo", 2, map[uint64]*drainSeq{
		5: {claimed: true, ack: "$JS.ACK.fake"},
	})
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the drain: %v", err)
	}

	// The completion's write is SUPPRESSED before it is issued, and reported as an
	// ambiguous failure: the record keeps the claim, but this broker cannot tell.
	// Suppress the write so it genuinely does not land, and make the READBACK
	// unavailable too, so the unresolved window stays open long enough to assert
	// what is barred inside it. Without the second half the path now resolves
	// immediately (see resolveUncertainNow) and the window cannot be observed.
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	mqttTestSessStoreSuppressV.Store(true)
	sess.finalizeDrainDelivery(c, "dur-notland", 5, "$JS.ACK.fake")
	mqttTestSessStoreSuppressV.Store(false)

	if !sess.isSeqUncertain() {
		t.Fatal("an ambiguous completion whose readback is unavailable must leave the session unresolved")
	}
	st := drainSeqState(sess, d, 5)
	if st.claimed {
		t.Fatal("an ambiguous completion keeps the removal: the record may already carry it")
	}
	if !st.unclaimUnconfirmed {
		t.Fatal("an unconfirmed removal must be marked as such, or an ack/forward will treat it as durable")
	}

	// While unconfirmed, a redelivery must NOT be acked: the removal may not exist
	// in any record, and acking would destroy the message.
	sess.finalizeDrainDelivery(c, "dur-notland", 5, "$JS.ACK.fake2")
	if drainSeqState(sess, d, 5).claimed {
		t.Fatal("the redelivery must not have re-claimed the sequence")
	}
	// A correlated PUBREL must not be forwarded either.
	if _, ok := sess.confirmDrainPubRel("dur-notland", 5); !ok {
		t.Fatal("an unconfirmed removal must read as still-owed to the PUBREL callback, " +
			"or the client can PUBCOMP an exchange the record still claims")
	}

	// Readback available again: the record is still at the base sequence, which
	// settles nothing — so the resolver re-attempts the completion's own bytes.
	restoreRead()
	res, err := sess.resolveUncertainWrite()
	if err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}
	if sess.isSeqUncertain() {
		t.Fatal("the session must be resolved")
	}

	st = drainSeqState(sess, d, 5)
	if st.claimed {
		t.Fatal("the re-attempt made the removal durable, so the claim must stay released")
	}
	if st.unclaimUnconfirmed {
		t.Fatal("the guard must come off once the removal is PROVEN durable: that is what authorizes the ack")
	}
	// The decisive observable: the record the re-attempt stored no longer claims the
	// sequence. It was the drain's last claim, so the landed continuation tore the
	// drain down as well — exactly what a landed original would have done. Under the
	// old at-base arm nothing was stored at all and the record still owed the
	// delivery.
	ps := testLoadDrainRecord(t, sess)
	if pd := ps.Drains["sid"]; pd != nil {
		for _, ds := range pd.Started {
			if ds.Seq == 5 {
				t.Fatalf("the settled record still claims sequence 5: the removal was never made durable, got %+v", pd.Started)
			}
		}
	}
	if drainRegistered(sess, "dur-notland") {
		t.Fatal("the drain claimed nothing once the removal was PROVEN durable, so it must have been torn down")
	}
}

// testLoadDrainRecord returns the session's STORED record, for a drain assertion that
// must read what is durable rather than what memory says.
func testLoadDrainRecord(t *testing.T, sess *mqttSession) *mqttPersistedSession {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil || smsg == nil {
		t.Fatalf("loading the persisted session record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("unmarshal the persisted session record: %v", err)
	}
	return ps
}

// TestSharedSubRollbackRefusedAtEntry covers rollbackSharedSub's already-uncertain
// case: an EARLIER ambiguous write in the same packet means its own compensating
// save is refused at entry.
//
// That refusal is a DEFINITE failure — nothing was stored — so the compensating
// mutation must be undone. Undoing it is also what leaves the earlier attempt's
// state in place, which is the state that attempt's readback will reconcile
// against. Reading the refusal as "may have landed" would keep a rollback that
// never reached the stream.
func TestSharedSubRollbackRefusedAtEntry(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "rbshared-refused")
	f := &mqttFilter{shared: true, group: "g1", filter: "rb/topic", qos: 1}
	sid := f.sessionKey()

	sess.mu.Lock()
	sess.sharedSubs = map[string]byte{sid: 1}
	// An earlier write in this packet already left the session unresolved.
	testMarkUncertainLocked(sess, sess.seq, []byte(`{"earlier":"attempt"}`))
	sess.mu.Unlock()

	tok := sess.beginRecordTx()
	_, _, err := sess.rollbackSharedSub(c, tok, f, true, false, 0, false, 0)
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if err == nil {
		t.Fatal("a save must be refused while an earlier write is unresolved")
	}

	sess.mu.Lock()
	_, present := sess.sharedSubs[sid]
	sess.mu.Unlock()
	if !present {
		t.Fatal("a compensating write refused at entry stored nothing, so its mutation must be undone: " +
			"leaving it applied discards the state the earlier ambiguous write's readback reconciles against")
	}
}

// TestPersistedRecordOmitsAnEmptyDrain pins I3 at the record level: an entry exists
// in the record if and only if its durable must survive.
//
// finishDrainIfEmpty deletes the durable with no further write, on the strength of
// the save that cleared the last claim having also removed the entry. If an empty
// drain were persisted instead, that save would leave the record naming a consumer
// which is then deleted — and a crash in between strands a reference to a durable
// that no longer exists, so a resume restores a drain nothing can deliver for.
func TestPersistedRecordOmitsAnEmptyDrain(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "empty-drain")

	d := addTestDrain(sess, "sid", "dur-empty", "foo", 1, map[uint64]*drainSeq{
		7: {claimed: true},
	})

	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the claimed drain: %v", err)
	}
	if n := testPersistedDrainCount(t, sess); n != 1 {
		t.Fatalf("a drain with a claimed sequence must be persisted, got %d", n)
	}

	// The last claim goes: this is the save finishDrainIfEmpty relies on.
	sess.mu.Lock()
	d.seqs[7].claimed = false
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the emptied drain: %v", err)
	}
	if n := testPersistedDrainCount(t, sess); n != 0 {
		t.Fatalf("a drain claiming nothing must be omitted from the record, got %d: "+
			"its durable is deleted with no further write, so the record would name a consumer that no longer exists", n)
	}
}

// testPersistedDrainCount reports how many drains the STORED record carries.
func testPersistedDrainCount(t *testing.T, sess *mqttSession) int {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil || smsg == nil {
		t.Fatalf("loading the persisted session record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the persisted session record: %v", err)
	}
	return len(ps.Drains)
}

// TestDrainAmbiguousLandedCompletionDischargesTheAck is the other half of the
// reconciliation: the ambiguous completion DID land.
//
// Keeping the removal is then correct, but the ack and the teardown were withheld
// only because durability could not be proven — so once the readback proves it, they
// are owed. Registering just the undo leaves a landed removal permanently unacked:
// the message sits in the consumer until AckWait re-offers it forever, and the drain
// is never torn down, so its durable and its record entry outlive the obligation.
func TestDrainAmbiguousLandedCompletionDischargesTheAck(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "amb-landed")

	d := addTestDrain(sess, "sid", "dur-landed", "foo", 1, map[uint64]*drainSeq{
		9: {claimed: true, ack: "$JS.ACK.landed"},
	})
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the drain: %v", err)
	}

	// The write LANDS; only the response is lost, and the readback is held off so
	// the unconfirmed window is observable.
	var suppressing atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressing.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	suppressing.set(true)
	restoreStore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	sess.finalizeDrainDelivery(c, "dur-landed", 9, "$JS.ACK.landed")
	restoreStore()

	if !sess.isSeqUncertain() {
		t.Fatal("an ambiguous completion whose readback is unavailable must leave the session unresolved")
	}
	if st := drainSeqState(sess, d, 9); st.claimed || !st.unclaimUnconfirmed {
		t.Fatalf("expected a kept-but-unconfirmed removal, got %+v", st)
	}

	// Resolve: the stored record is byte-identical to what was attempted, so the
	// write landed.
	suppressing.set(false)
	restoreRead()
	res, err := sess.resolveUncertainWrite()
	if err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}

	st := drainSeqState(sess, d, 9)
	if st.claimed {
		t.Fatal("a landed removal must stay removed")
	}
	if st.unclaimUnconfirmed {
		t.Fatal("the readback proved the removal durable, so the marker must be cleared: " +
			"left set, the source is never acked and the drain is never torn down")
	}
	if drainRegistered(sess, "dur-landed") {
		t.Fatal("the drain claimed nothing once the removal was confirmed, so it must have been torn down")
	}
}

// TestDrainAmbiguousIntentIsSettledForward covers the PUBREL intent, which is the
// linearization point of the whole QoS 2 second phase.
//
// An intent kept in memory that no record authorizes is worse than a lost one: it
// suppresses the payload the record still says is owed, and makes recovery recreate
// a PUBREL for an exchange the client never durably PUBREC'd (I11). The intent is
// kept anyway — resending the payload would be wrong if the write landed — and what
// makes memory and record agree is the resolver RE-ATTEMPTING those bytes, so the
// record ends up authorizing exactly the intent memory holds. Reading the base
// sequence as "it died" and rolling the intent back is what could leave a late
// landing describing an exchange this broker had already forgotten.
func TestDrainAmbiguousIntentIsSettledForward(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "amb-intent-notland")

	d := addTestDrain(sess, "sid", "dur-intent", "foo", 2, map[uint64]*drainSeq{
		4: {claimed: true},
	})
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the drain: %v", err)
	}

	var suppressing atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressing.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	suppressing.set(true)
	mqttTestSessStoreSuppressV.Store(true)
	_, definite, err := sess.recordPubRelIntent(c, "dur-intent", 4, 77, "xxxxxxxxxxxxxxxxxxxxxx")
	mqttTestSessStoreSuppressV.Store(false)
	if err == nil {
		t.Fatal("this test needs the intent write to fail")
	}
	if definite {
		t.Fatal("a suppressed store is ambiguous, not definite")
	}

	// Kept while unresolved: resending the payload would be wrong if it landed.
	if st := drainSeqState(sess, d, 4); !st.intent || st.pi != 77 {
		t.Fatalf("an ambiguous intent must be kept while unresolved, got %+v", st)
	}

	suppressing.set(false)
	restoreRead()
	if _, err := sess.resolveUncertainWrite(); err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}

	st := drainSeqState(sess, d, 4)
	if !st.intent || st.pi != 77 || st.phase != drainPubRelUncertain {
		t.Fatalf("the intent must stand once the resolver settles the write, got %+v", st)
	}
	if !st.claimed {
		t.Fatal("the source claim must still be held: the exchange is past PUBREC, not complete")
	}
	// And the record now AUTHORIZES it, which is the whole point of the intent: it is
	// a positive durable fact, never an inference from memory.
	ps := testLoadDrainRecord(t, sess)
	pd := ps.Drains["sid"]
	if pd == nil {
		t.Fatalf("the settled record must still carry the drain, got %+v", ps.Drains)
	}
	var found bool
	for _, ds := range pd.Started {
		if ds.Seq == 4 {
			found = true
			if !ds.PubRel || ds.PI != 77 {
				t.Fatalf("the settled record must carry the intent under its identifier, got %+v", ds)
			}
		}
	}
	if !found {
		t.Fatalf("the settled record must still claim sequence 4, got %+v", pd.Started)
	}
}

// atomicOnceBool is a mutex-free flag for hooks that run on other goroutines.
type atomicOnceBool struct{ v atomic.Bool }

func (a *atomicOnceBool) set(b bool) { a.v.Store(b) }
func (a *atomicOnceBool) get() bool  { return a.v.Load() }

// TestSubscribeAmbiguousIsSettledForward is the whole-packet arm of the settlement
// contract: a SUBSCRIBE whose final save is AMBIGUOUS keeps its state, and the
// resolver makes exactly those bytes durable.
//
// The packet used to be rolled back whenever a readback found the record still at the
// base sequence — which is not evidence that the write died, only that it has not
// committed YET. Undoing there is what let a late-committing proposal be read later
// as not-ours. So the state stands, with or without a SUBACK: the
// client was told nothing and must reconcile on reconnect, and this is exactly the
// outcome a landed original with a lost response would have produced.
//
// Mutation M1: revert the at-base arm to clearing the uncertainty and the packet's
// filter is neither in memory nor in the record.
func TestSubscribeAmbiguousIsSettledForward(t *testing.T) {
	_, _, sess, _ := drainTestSession(t, "sub-notland")

	// A membership the packet adds, persisted so there is a base sequence.
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	sess.mu.Lock()
	sess.subs = map[string]byte{"already.there": 1}
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the pre-packet membership: %v", err)
	}

	sess.mu.Lock()
	baseSeq := sess.seq
	sess.mu.Unlock()

	// A consumer the packet created. It is referenced by the membership the packet is
	// about to persist, so settling the write forward is precisely what keeps it
	// legitimate rather than orphaned.
	packetDur := "pkt_" + nuid.Next()
	if _, err := sess.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable:        packetDur,
			DeliverSubject: mqttSubPrefix + packetDur,
			FilterSubject:  mqttStreamSubjectPrefix + "packet.filter",
			AckPolicy:      AckExplicit,
			AckWait:        time.Minute,
			MaxAckPending:  1024,
		},
	}); err != nil {
		t.Fatalf("creating the packet's consumer: %v", err)
	}
	sess.mu.Lock()
	sess.subs["packet.filter"] = 1
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons["packet.filter"] = &ConsumerConfig{Durable: packetDur, DeliverSubject: mqttSubPrefix + packetDur}
	sess.mu.Unlock()

	// The packet's write neither lands nor resolves, so the retained state is
	// observable while the session is unresolved.
	var suppressing atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressing.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	suppressing.set(true)
	mqttTestSessStoreSuppressV.Store(true)
	tok := sess.beginRecordTx()
	err := sess.save(tok)
	outcome := sess.classifySave(false, err)
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	mqttTestSessStoreSuppressV.Store(false)
	if outcome != saveMaybeLanded {
		t.Fatalf("expected an ambiguous outcome, got %v", outcome)
	}

	sess.mu.Lock()
	_, kept := sess.subs["packet.filter"]
	sess.mu.Unlock()
	if !kept {
		t.Fatal("the packet's membership must be kept while the write is unresolved")
	}

	// The record is still at the base sequence — which settles nothing, so the
	// resolver re-attempts the packet's own bytes.
	suppressing.set(false)
	restoreRead()
	res, rerr := sess.resolveUncertainWrite()
	if rerr != nil {
		t.Fatalf("resolveUncertainWrite: %v", rerr)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours: the re-attempt arbitrates the ambiguity", res)
	}

	sess.mu.Lock()
	_, stillThere := sess.subs["packet.filter"]
	_, prior := sess.subs["already.there"]
	seqAfter := sess.seq
	sess.mu.Unlock()
	if !stillThere || !prior {
		t.Fatal("the packet's state is kept: the resolver settles the write forward rather than undoing it")
	}
	if sess.isSeqUncertain() {
		t.Fatal("a landed re-attempt must leave the session resolved and writable")
	}
	if seqAfter <= baseSeq {
		t.Fatalf("the re-attempt must advance the record (sequence %v, base %v)", seqAfter, baseSeq)
	}

	// The decisive observable: the RECORD carries the packet's filter. Under the old
	// arm it carried neither the filter nor any write at all.
	stored := testPersistedSubs(t, sess)
	if _, durable := stored["packet.filter"]; !durable {
		t.Fatal("the settled record must carry the packet's filter: the ambiguity is resolved by making those exact bytes durable")
	}
	if _, keptPrior := stored["already.there"]; !keptPrior {
		t.Fatal("the settled record must still carry the pre-packet membership")
	}

	// And the consumer the packet created is still there, referenced by that record.
	if ci, lerr := sess.jsa.lookupConsumer(mqttStreamName, packetDur); lerr != nil || ci == nil {
		t.Fatalf("the packet's consumer must survive with the membership that names it: %v", lerr)
	}
}

// testPersistedSubs returns the subscription map from the STORED session record.
func testPersistedSubs(t *testing.T, sess *mqttSession) map[string]byte {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil || smsg == nil {
		t.Fatalf("loading the persisted session record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the persisted session record: %v", err)
	}
	return ps.Subs
}

// TestResolveUncertainNowDetachesAForeignSession is finding 5: an immediate
// resolution that finds a FOREIGN record must hand the session to the
// ownership-aware path, not merely report the verdict.
//
// The stale client stays bound otherwise, and that is the harmful part: it still
// holds c.mqtt.sess, so it goes on publishing, subscribing and acking against a
// session whose record is no longer ours to write, and its own close path would act on
// that state too.
func TestResolveUncertainNowDetachesAForeignSession(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "foreign-now")

	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	// The session must be RESIDENT, or the eviction assertion below is vacuous:
	// drainTestSession creates the session but does not register it.
	asm := sess.asm
	if asm == nil {
		t.Fatal("the session must belong to an account manager")
	}
	asm.mu.Lock()
	if asm.sessions == nil {
		asm.sessions = make(map[string]*mqttSession)
	}
	asm.sessions[sess.id] = sess
	if asm.sessByHash == nil {
		asm.sessByHash = make(map[string]*mqttSession)
	}
	asm.sessByHash[sess.idHash] = sess
	asm.mu.Unlock()
	if testMQTTResidentSessionOrNil(asm, sess.id) != sess {
		t.Fatal("precondition: the session must be resident before the resolver runs")
	}

	sess.mu.Lock()
	sess.c = c
	baseSeq := sess.seq
	// An attempted write whose digest will NOT match whatever is stored.
	testMarkUncertainLocked(sess, baseSeq, []byte(`{"attempted":"ours"}`))
	sess.mu.Unlock()

	// Somebody else advances the record with different content.
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	if _, err := sess.jsa.storeMsg(subject, 0, []byte(`{"owner":"someone-else"}`)); err != nil {
		t.Fatalf("foreign write: %v", err)
	}

	if res := sess.resolveUncertainNow(c); res != mqttUncertainForeign {
		t.Fatalf("resolution = %v, want foreign", res)
	}

	sess.mu.Lock()
	bound := sess.c
	uncertain := sess.seqUncertain
	sess.mu.Unlock()
	if bound != nil {
		t.Fatal("a foreign verdict must DETACH the stale client: left bound, the periodic resolver skips this session " +
			"and the close path can act on state this broker may no longer write")
	}
	if uncertain {
		t.Fatal("a foreign verdict resolves the session's uncertainty: the record is settled and " +
			"is not ours, which is decided without identifying whoever wrote it")
	}
	if resident := testMQTTResidentSessionOrNil(sess.asm, sess.id); resident == sess {
		t.Fatal("a foreign verdict must evict the session so a later CONNECT rebuilds from the authoritative record")
	}
	// Detaching alone is not enough: the client still holds c.mqtt.sess and would go
	// on publishing, subscribing and acking against an evicted session while another
	// broker is authoritative. The connection has to be closed.
	drainCheckFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if !c.isClosed() {
			return errors.New("the stale connection is still open")
		}
		return nil
	})
}

// testMQTTResidentSessionOrNil returns the resident session for id, or nil.
func testMQTTResidentSessionOrNil(asm *mqttAccountSessionManager, id string) *mqttSession {
	if asm == nil {
		return nil
	}
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	return asm.sessions[id]
}

// TestUncertainRetrySkipsAnOpenRecordTransaction is finding 1: the periodic
// resolver must not settle an ambiguity whose writer is still mid-transaction.
//
// save() publishes seqUncertain BEFORE it returns, so between that and the writer's
// classifySave/retainOnUncertain there is a window in which the session looks
// resolvable but is not. Resolving inside it is silently destructive twice over: the
// writer then reads its own ambiguous error as DEFINITE (isSeqUncertain is false
// again) and rolls back a mutation that may have landed, and any continuation it
// registers afterwards lands in a list nobody will ever consume.
//
// The record transaction is exactly the interval over which mark-classify-register
// is atomic, so the resolver waits for it.
func TestUncertainRetrySkipsAnOpenRecordTransaction(t *testing.T) {
	t.Parallel()
	s, c, sess, _ := drainTestSession(t, "retry-fence")
	asm := sess.asm
	if asm == nil {
		t.Fatal("the session must belong to an account manager")
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	// The pass only walks RESIDENT sessions.
	asm.mu.Lock()
	if asm.sessions == nil {
		asm.sessions = make(map[string]*mqttSession)
	}
	asm.sessions[sess.id] = sess
	asm.mu.Unlock()

	// Simulate a writer that is inside its transaction and has just had save() mark
	// the session uncertain — the exact mid-flight state.
	tok := sess.beginRecordTx()
	sess.mu.Lock()
	testMarkUncertainLocked(sess, sess.seq, []byte(`{"attempted":"mid-flight"}`))
	sess.mu.Unlock()

	// The periodic pass must leave it alone.
	asm.reconcileResidentRecordState(s)

	if !sess.isSeqUncertain() {
		t.Fatal("the resolver settled an ambiguity while its writer held the record transaction: " +
			"the writer will now read its own ambiguous error as definite and roll back a mutation that may have landed")
	}

	// Once the writer is done, the pass resolves it normally.
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	asm.reconcileResidentRecordState(s)
	if sess.isSeqUncertain() {
		t.Fatal("with no transaction open the resolver must settle the session")
	}
	_ = c
}

// TestDrainRemovalIsGuardedBeforeTheSave is finding 2: the guard must be installed
// WITH the mutation, not after the save returns.
//
// A delivery or duplicate-PUBREL callback can run at any moment during the save. If
// the guard is only applied afterwards, such a callback observes claimed=false with
// no guard and treats the removal as durable — acking the message, or forwarding a
// PUBREL — while the save is still in flight and may not land at all.
func TestDrainRemovalIsGuardedBeforeTheSave(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "guard-order")

	d := addTestDrain(sess, "sid", "dur-guard", "foo", 2, map[uint64]*drainSeq{
		3: {claimed: true, ack: "$JS.ACK.guard"},
	})
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the drain: %v", err)
	}

	// Observed from INSIDE the save, which is exactly when a concurrent callback
	// would look: the removal must already be guarded there.
	var sawClaimed, sawGuard, observed atomicOnceBool
	restore := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
		if sp == sess && !observed.get() {
			observed.set(true)
			sp.mu.Lock()
			if st := d.seqs[3]; st != nil {
				sawClaimed.set(st.claimed)
				sawGuard.set(st.unclaimUnconfirmed)
			}
			sp.mu.Unlock()
		}
		return nil
	})
	sess.finalizeDrainDelivery(c, "dur-guard", 3, "$JS.ACK.guard")
	restore()

	if !observed.get() {
		t.Fatal("the save was never reached, so this test proves nothing")
	}
	if sawClaimed.get() {
		t.Fatal("the claim should already be released while the save is in flight")
	}
	if !sawGuard.get() {
		t.Fatal("the removal was UNGUARDED while its save was in flight: a delivery arriving then reads it as durable " +
			"and acks the message, or forwards a PUBREL, for a write that may never land")
	}

	// A confirmed save takes the guard off again.
	if st := drainSeqState(sess, d, 3); st.claimed || st.unclaimUnconfirmed {
		t.Fatalf("a confirmed removal must be unclaimed and unguarded, got %+v", st)
	}
}

// TestResolutionIsOneRecordTransaction pins the protocol the resolver now follows:
// deciding, clearing the uncertainty and running the retained continuations are ONE
// record-state transition, with the fence held throughout.
//
// Each of the three parts fails differently if they are not atomic. Observing "no
// transaction is open" and then resolving is a check-then-act, because an
// off-read-loop finalizer takes the fence in between and its save publishes
// seqUncertain before returning. Clearing the uncertainty and only then running the
// continuations lets another writer interleave with the state they reconcile. And a
// continuation that opens a transaction of its own would deadlock against the
// resolver holding it — which is why continuations receive the resolver's token.
// The at-base arm settles by RE-ATTEMPTING the attempt's bytes, which is itself a
// write under that same fence.
func TestResolutionIsOneRecordTransaction(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "resolve-atomic")
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	sess.mu.Lock()
	baseSeq := sess.seq
	testMarkUncertainLocked(sess, baseSeq, []byte(`{"attempted":"never-landed"}`))
	sess.mu.Unlock()

	var (
		ran        atomicOnceBool
		fenceHeld  atomicOnceBool
		tokenWorks atomicOnceBool
	)
	sess.retainOnLanded(func(tok recordTx) {
		ran.set(true)
		// The fence must be HELD while this runs: no other writer may open one, so the
		// state this continuation reconciles cannot be observed or persisted by anyone
		// else between the decision and the reconciliation.
		if _, ok := sess.tryBeginRecordTx(); !ok {
			fenceHeld.set(true)
		} else {
			t.Error("another writer could open the record fence during resolution: " +
				"the reconciliation is not atomic with the decision that authorized it")
		}
		// And the token it was handed must actually authorize a write, which is what
		// lets a continuation persist without opening (and deadlocking on) its own fence.
		if err := sess.save(tok); err == nil {
			tokenWorks.set(true)
		} else {
			t.Errorf("the continuation's token did not authorize a write: %v", err)
		}
	})

	res, err := sess.resolveUncertainWrite()
	if err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}
	if !ran.get() {
		t.Fatal("the landed continuation never ran")
	}
	if !fenceHeld.get() {
		t.Fatal("the fence was not held across the reconciliation")
	}
	if !tokenWorks.get() {
		t.Fatal("the continuation's token was not usable for a write")
	}

	// The fence is released afterwards, and the session is resolved.
	if tok, ok := sess.tryBeginRecordTx(); !ok {
		t.Fatal("the resolver must release the fence when it finishes")
	} else if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if sess.isSeqUncertain() {
		t.Fatal("the session must be resolved")
	}

	// Resolution is single-flight per attempt: a second pass is a no-op, so it can
	// neither re-run continuations nor mistake a record its own predecessor advanced
	// for a foreign one.
	if res2, err2 := sess.resolveUncertainWrite(); err2 != nil || res2 != mqttUncertainOurs {
		t.Fatalf("a second resolution must be a no-op, got %v err=%v", res2, err2)
	}
}

// TestResolutionRefusesWhileAnotherTransactionIsOpen pins that the resolver never
// waits for the fence, and never resolves without it.
//
// A caller may already hold a transaction on its own goroutine — the SUBSCRIBE
// packet does, and resolves before returning — so blocking would deadlock the
// readLoop against itself. Refusing is safe because an unresolved session can make
// no progress anyway: the periodic pass and the redelivery clocks retry.
func TestResolutionRefusesWhileAnotherTransactionIsOpen(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "resolve-busy")
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	sess.mu.Lock()
	testMarkUncertainLocked(sess, sess.seq, []byte(`{"attempted":"busy"}`))
	sess.mu.Unlock()

	held := sess.beginRecordTx()
	done := make(chan mqttUncertainResolution, 1)
	go func() {
		res, _ := sess.resolveUncertainWrite()
		done <- res
	}()
	select {
	case res := <-done:
		if res != mqttUncertainUnresolved {
			t.Fatalf("resolution = %v, want unresolved: it must refuse rather than resolve without the fence", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the resolver BLOCKED on the fence: a caller that already holds one on this goroutine would deadlock")
	}
	if !sess.isSeqUncertain() {
		t.Fatal("a refused resolution must leave the session unresolved")
	}
	if terr := sess.endRecordTx(held); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	// And it succeeds once the fence is free.
	if res, err := sess.resolveUncertainWrite(); err != nil || res != mqttUncertainOurs {
		t.Fatalf("resolution after release = %v err=%v", res, err)
	}
}

// TestForeignDispositionIsOwnershipGated pins that foreign disposition cannot
// damage a replacement session installed by a competing CONNECT.
//
// A foreign verdict means the record is not ours, so this object must go. (Not that
// another owner holds it — nothing here establishes one; F-084.)
// But eviction by client ID is not safe on its own: a CONNECT can have already
// evicted this object and installed a restored replacement under the same ID, and an
// unconditional delete then removes THAT — leaving a live connection with a session
// no map knows about. Identity is the only safe predicate, and the ownership gate is
// what keeps the decision and the removal from straddling such a CONNECT.
func TestForeignDispositionIsOwnershipGated(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "foreign-gate")
	asm := sess.asm
	if asm == nil {
		t.Fatal("the session must belong to an account manager")
	}

	// A REPLACEMENT is resident under the same client ID, as a CONNECT that already
	// rebuilt from the authoritative record would have left things.
	replacement, _, err := asm.createOrRestoreSession("foreign-gate", (&Server{opts: &Options{}}).getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if replacement == sess {
		t.Fatal("this test needs a distinct replacement session object")
	}
	asm.mu.Lock()
	if asm.sessions == nil {
		asm.sessions = make(map[string]*mqttSession)
	}
	if asm.sessByHash == nil {
		asm.sessByHash = make(map[string]*mqttSession)
	}
	asm.sessions[replacement.id] = replacement
	asm.sessByHash[replacement.idHash] = replacement
	asm.mu.Unlock()

	// The stale object disposes of itself.
	sess.mu.Lock()
	sess.c = c
	sess.mu.Unlock()
	sess.disposeForeignSession(c, mqttDisposalTakenOver)

	// The replacement must be untouched: the stale disposition may only remove
	// itself.
	if got := testMQTTResidentSessionOrNil(asm, "foreign-gate"); got != replacement {
		t.Fatalf("the replacement session was evicted by a stale foreign disposition (resident=%p want=%p): "+
			"removal by client ID deletes whatever is there, so a CONNECT that already installed a replacement loses it",
			got, replacement)
	}
	asm.mu.RLock()
	byHash := asm.sessByHash[replacement.idHash]
	asm.mu.RUnlock()
	if byHash != replacement {
		t.Fatal("the replacement's by-hash entry was removed by a stale foreign disposition")
	}

	// And the stale connection is closed, whichever resolver found it foreign.
	drainCheckFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if !c.isClosed() {
			return errors.New("the stale connection is still open")
		}
		return nil
	})
}

// TestPeriodicForeignVerdictClosesTheBoundClient pins that the PERIODIC resolver
// disposes of a foreign session exactly as the immediate one does.
//
// That pass now examines bound sessions, so a foreign verdict there leaves a live
// connection whose c.mqtt.sess points at an evicted object — free to publish and ack
// against a session whose record this broker may no longer write — unless it is closed. Both resolvers must
// go through one disposition path; that they used to differ is exactly the bug.
func TestPeriodicForeignVerdictClosesTheBoundClient(t *testing.T) {
	t.Parallel()
	s, c, sess, _ := drainTestSession(t, "foreign-periodic")
	asm := sess.asm
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	asm.mu.Lock()
	if asm.sessions == nil {
		asm.sessions = make(map[string]*mqttSession)
	}
	asm.sessions[sess.id] = sess
	asm.mu.Unlock()

	sess.mu.Lock()
	sess.c = c
	testMarkUncertainLocked(sess, sess.seq, []byte(`{"attempted":"ours"}`))
	sess.mu.Unlock()

	// A competing write advances the record with different content. It makes the record NOT
	// OURS, which is all the resolver reads from it — no successor is demonstrated (F-084).
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	if _, err := sess.jsa.storeMsg(subject, 0, []byte(`{"owner":"someone-else"}`)); err != nil {
		t.Fatalf("foreign write: %v", err)
	}

	asm.reconcileResidentRecordState(s)

	sess.mu.Lock()
	bound := sess.c
	sess.mu.Unlock()
	if bound != nil {
		t.Fatal("the periodic resolver must detach a foreign session's client")
	}
	drainCheckFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if !c.isClosed() {
			return errors.New("the bound connection is still open")
		}
		return nil
	})
}

// TestForeignDispositionRefusesWhileTheGateIsHeld is the deterministic form of the
// ownership-gate requirement: no concurrency needed, just a held gate.
//
// A held gate means a CONNECT is in progress. Disposing anyway races it directly, so
// the disposition must do NOTHING — not detach, not evict, not discard the deferred
// close, not cancel the expiry timer, not close the client.
func TestForeignDispositionRefusesWhileTheGateIsHeld(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "gate-held")
	asm := sess.asm
	if asm == nil {
		t.Fatal("the session must belong to an account manager")
	}
	asm.mu.Lock()
	if asm.sessions == nil {
		asm.sessions = make(map[string]*mqttSession)
	}
	asm.sessions[sess.id] = sess
	if asm.pendingExpiries == nil {
		asm.pendingExpiries = make(map[string]mqttPendingExpiry)
	}
	asm.pendingExpiries[sess.idHash] = mqttPendingExpiry{timer: time.NewTimer(time.Hour), token: 1}
	asm.mu.Unlock()

	// deferDisconnect detaches the client itself, so the binding is (re)established
	// after it: this test needs both a recorded obligation AND a bound client.
	if !sess.deferDisconnect(&mqttDeferredDisconnect{}) {
		t.Fatal("could not record a deferred disconnect action")
	}
	sess.mu.Lock()
	sess.c = c
	sess.mu.Unlock()

	// A CONNECT holds the gate.
	asm.mu.Lock()
	if asm.sessLocked == nil {
		asm.sessLocked = make(map[string]struct{})
	}
	asm.sessLocked[sess.id] = struct{}{}
	asm.mu.Unlock()

	sess.disposeForeignSession(c, mqttDisposalTakenOver)

	sess.mu.Lock()
	bound := sess.c
	sess.mu.Unlock()
	if bound != c {
		t.Fatal("a disposition that could not take the ownership gate detached the client anyway, racing the CONNECT that holds it")
	}
	if testMQTTResidentSessionOrNil(asm, sess.id) != sess {
		t.Fatal("a gate-refused disposition evicted the session anyway")
	}
	asm.mu.RLock()
	_, timerLives := asm.pendingExpiries[sess.idHash]
	asm.mu.RUnlock()
	if !timerLives {
		t.Fatal("a gate-refused disposition cancelled the expiry timer anyway")
	}
	if c.isClosed() {
		t.Fatal("a gate-refused disposition closed the connection anyway")
	}
	sess.mu.Lock()
	stillDeferred := sess.deferredDisconnect != nil
	sess.mu.Unlock()
	if !stillDeferred {
		t.Fatal("a gate-refused disposition discarded the deferred close anyway: " +
			"that obligation belongs to whoever holds the gate")
	}
}

// TestForeignDispositionSparesARenboundClient is the deterministic form of the
// client-identity requirement.
//
// A disposition decided for one connection must not tear down a DIFFERENT one that
// has since taken the session over — and it must refuse before touching anything,
// or it destroys the current session's deferred obligation on its way to deciding
// it had no business there.
func TestForeignDispositionSparesARenboundClient(t *testing.T) {
	t.Parallel()
	_, oldClient, sess, _ := drainTestSession(t, "rebound")
	asm := sess.asm
	newClient := (&Server{opts: &Options{}, quitCh: make(chan struct{})}).createInternalAccountClient()

	asm.mu.Lock()
	if asm.sessions == nil {
		asm.sessions = make(map[string]*mqttSession)
	}
	asm.sessions[sess.id] = sess
	if asm.pendingExpiries == nil {
		asm.pendingExpiries = make(map[string]mqttPendingExpiry)
	}
	asm.pendingExpiries[sess.idHash] = mqttPendingExpiry{timer: time.NewTimer(time.Hour), token: 1}
	asm.mu.Unlock()

	// deferDisconnect detaches the client, so record the obligation first and then
	// bind the NEW client — a different one from the one being disposed of.
	if !sess.deferDisconnect(&mqttDeferredDisconnect{}) {
		t.Fatal("could not record a deferred disconnect action")
	}
	sess.mu.Lock()
	sess.c = newClient
	sess.mu.Unlock()

	sess.disposeForeignSession(oldClient, mqttDisposalTakenOver)

	sess.mu.Lock()
	bound := sess.c
	stillDeferred := sess.deferredDisconnect != nil
	sess.mu.Unlock()
	if bound != newClient {
		t.Fatal("a disposition decided for the OLD connection detached the one that had since taken the session over")
	}
	if newClient.isClosed() {
		t.Fatal("a disposition decided for the OLD connection closed the new one")
	}
	if testMQTTResidentSessionOrNil(asm, sess.id) != sess {
		t.Fatal("a disposition decided for the OLD connection evicted a session the new one owns")
	}
	asm.mu.RLock()
	_, timerLives := asm.pendingExpiries[sess.idHash]
	asm.mu.RUnlock()
	if !timerLives {
		t.Fatal("a disposition decided for the OLD connection cancelled the REPLACEMENT session's expiry timer")
	}
	if !stillDeferred {
		t.Fatal("the refusal came AFTER discarding the deferred close: the current session's obligation " +
			"was destroyed by a caller that then decided it had no business here")
	}
}

// TestStaleDispositionSparesAReplacementExpiryTimer pins that the expiry timer, which
// is keyed by CLIENT HASH rather than by session object, is only cancelled when the
// identity-checked removal actually applies.
//
// Cancelling it first — before knowing whether this object is still resident —
// silently disarms a replacement session's finite-expiry deadline. The fallback
// sweep re-arms it on its next pass, so the state is not immortal; what is lost is
// the deadline guarantee, and the session lingers past the interval it was promised.
func TestStaleDispositionSparesAReplacementExpiryTimer(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "expiry-replacement")
	asm := sess.asm

	replacement, _, err := asm.createOrRestoreSession("expiry-replacement", (&Server{opts: &Options{}}).getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if replacement == sess {
		t.Fatal("this test needs a distinct replacement session object")
	}
	asm.mu.Lock()
	if asm.sessions == nil {
		asm.sessions = make(map[string]*mqttSession)
	}
	asm.sessions[replacement.id] = replacement
	if asm.pendingExpiries == nil {
		asm.pendingExpiries = make(map[string]mqttPendingExpiry)
	}
	// The REPLACEMENT's deadline, keyed by the hash the stale object shares.
	asm.pendingExpiries[replacement.idHash] = mqttPendingExpiry{timer: time.NewTimer(time.Hour), token: 1}
	asm.mu.Unlock()

	sess.mu.Lock()
	sess.c = c
	sess.mu.Unlock()
	sess.disposeForeignSession(c, mqttDisposalTakenOver)

	asm.mu.RLock()
	_, timerLives := asm.pendingExpiries[replacement.idHash]
	asm.mu.RUnlock()
	if !timerLives {
		t.Fatal("a stale disposition cancelled the REPLACEMENT session's expiry timer: the timer is keyed by client hash, " +
			"so it must only be cancelled when the identity-checked removal actually applies — otherwise that session loses its " +
			"expiry deadline and lingers until the periodic sweep re-arms it")
	}
	if testMQTTResidentSessionOrNil(asm, sess.id) != replacement {
		t.Fatal("the replacement must remain resident")
	}
}

// TestDrainRecoveryRefusesAPubRelForAnotherIdentifier is round 13's HIGH finding, and the
// mechanism is worth stating precisely because the record involved looks almost right.
//
// The drain intent persists the packet identifier the client is waiting on. Recovery
// indexed the stored PUBRELs by SOURCE and confirmed the intent on source plus exchange
// owner — the identifier was parsed by the scan and then discarded. So a record naming the
// right source and the right exchange under a DIFFERENT identifier confirmed the intent as
// durable proof, after which the claim was finalized and the source could be acknowledged
// while the client's original exchange had no PUBREL at all and no evidence left to
// recreate one from. A zero identifier is the same defect with a worse ending: nothing can
// be transmitted under it.
//
// The refusal is "treat it as NO record", which is what makes it safe rather than merely
// strict: the exchange recreates its own PUBREL under its own identifier and identity, and
// the stray record is left for the operator or generation cleanup.
func TestDrainRecoveryRefusesAPubRelForAnotherIdentifier(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		recordPI uint16
	}{
		{"another exchange's identifier", 9},
		{"no identifier at all", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, c, sess, _ := drainTestSession(t, "drain-pi-"+tc.name)
			const (
				dur        = "dur-pi"
				sseq       = uint64(11)
				intentPI   = uint16(5)
				ownerNonce = "xxxxxxxxxxxxxxxxxxxxxx"
			)
			d := addTestDrain(sess, "sid", dur, "foo", 2, map[uint64]*drainSeq{
				sseq: {claimed: true, intent: true, pi: intentPI, ownerNonce: ownerNonce},
			})
			// $MQTT_out is interest-retained, so a store with no consumer keeps nothing
			// and every assertion below would pass vacuously against an empty stream.
			if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
				t.Fatalf("ensurePubRelConsumerSubscription: %v", err)
			}

			// A record for the right source and the right EXCHANGE — the owner nonce is the
			// intent's own — but under a different identifier. Every other precondition
			// recovery checks is satisfied, so only the identifier can refuse it.
			msgId := mqttPubRelMsgId(sess.idHash, dur, sseq, 0)
			msg, hdrLen := mqttNewCorrelatedPubRel(tc.recordPI, ownerNonce, dur, sseq, msgId, _EMPTY_, _EMPTY_)
			if tc.recordPI == 0 {
				// The builder refuses a zero identifier, which is itself the right
				// behaviour; the record has to be planted to test the READER.
				msg, hdrLen = mqttNewCorrelatedPubRel(1, ownerNonce, dur, sseq, msgId, _EMPTY_, _EMPTY_)
				msg = bytes.Replace(msg, []byte(mqttNatsPubRelHeader+":1\r\n"),
					[]byte(mqttNatsPubRelHeader+":0\r\n"), 1)
			}
			if msg == nil {
				t.Fatal("the correlated PUBREL builder refused a record this test needs")
			}
			if _, err := sess.jsa.storeMsg(sess.pubRelSubject, hdrLen, msg); err != nil {
				t.Fatalf("plant the stray PUBREL: %v", err)
			}

			// The premise: the scan DOES see it, so the refusal below is a decision about
			// the record and not an artefact of it being invisible.
			records, refused, ok := sess.scanPubRelRecords(sess.pubRelSubject)
			if !ok {
				t.Fatal("the scan must succeed")
			}
			if len(records) != 1 {
				t.Fatalf("the scan must RETURN the planted record either way, got %+v", records)
			}
			if tc.recordPI == 0 {
				// Returned, but carrying its refusal: the scan reports what is there and
				// each caller decides. A reader that would act on it skips it.
				if refused != 1 || records[0].shapeErr == nil {
					t.Fatalf("a record with no identifier must carry a decode refusal, refused=%d rec=%+v",
						refused, records[0])
				}
			} else {
				if refused != 0 || records[0].shapeErr != nil || records[0].pi != tc.recordPI {
					t.Fatalf("a legal record under a foreign identifier must decode CLEANLY — the "+
						"refusal below is about the identifier not matching the intent, not about "+
						"the shape — refused=%d rec=%+v", refused, records[0])
				}
			}

			skip := sess.recoverDrainPubRels(c)
			if skip[dur] {
				t.Fatalf("the drain must not be skipped: the scan succeeded")
			}

			// The intent stands under its ORIGINAL identifier. The claim is released here
			// and that is correct rather than a gap: it is released only BECAUSE the
			// refusal recreated this exchange's own record below, which is the positive
			// durable fact the release rests on. Under the defect there was no such
			// record and the claim was released anyway.
			st := drainSeqState(sess, d, sseq)
			if !st.intent || st.pi != intentPI {
				t.Fatalf("the intent must stand under its ORIGINAL identifier, got %+v", st)
			}
			if st.phase != drainPubRelConfirmed {
				t.Fatalf("the exchange's own record exists, so its phase must be confirmed, got %+v", st)
			}

			// THE assertion: the exchange owns a PUBREL under ITS identifier. The refusal
			// recreates rather than merely declining, and this is what fails when the
			// identifier comparison is removed — the stray record confirms the intent and
			// nothing is ever written for the identifier the client is waiting on.
			after, _, ok := sess.scanPubRelRecords(sess.pubRelSubject)
			if !ok {
				t.Fatal("the second scan must succeed")
			}
			var mine int
			for _, r := range after {
				if r.shapeErr == nil && r.pi == intentPI && r.srcDur == dur && r.srcSeq == sseq &&
					r.owner.valid() && r.owner.Nonce == ownerNonce {
					mine++
				}
			}
			if mine != 1 {
				t.Fatalf("the exchange must own exactly one PUBREL under identifier %d, got %d of %+v",
					intentPI, mine, after)
			}
		})
	}
}

// TestPubRelScanReturnsAnUnsupportedRecordForTheWalksThatMustSeeIt pins the disposition
// split, which a first version of the one-decoder change got wrong.
//
// Dropping unsupported records inside the scan looked like the tidy answer: every reader
// that would ACT on one has to skip it anyway. It is wrong, and the caller it breaks is a
// destructive one. secureQ2AffPubRelProofs walks the stored PUBRELs of a generation about
// to be torn down, precisely to notice affine PUBREC proofs that are about to be destroyed
// with the consumer that retains them; mqttQ2AffProofWalk asks the same question for a
// loss report. A record the decoder refuses can still carry a perfectly good affine
// reference, and dropping it would have made the teardown destroy that proof unread while
// reporting nothing.
//
// So the scan returns EVERY record with its verdict attached, and each caller applies its
// own disposition. This asserts the half that no acting caller would notice.
func TestPubRelScanReturnsAnUnsupportedRecordForTheWalksThatMustSeeIt(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-scan-all")
	if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
		t.Fatalf("ensurePubRelConsumerSubscription: %v", err)
	}

	// An unsupported shape — no exchange owner — that nonetheless carries a VALID affine
	// reference. The builder refuses to write one, which is the point: this is a record
	// from elsewhere, and the walks still have to account for it.
	claim := mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcdef", 42)
	copySubject := mqttQ2AffCopySubject("abcd1234", testQ2AffFixtureNonce,
		strings.Repeat("a", mqttQ2AffClaimHashLen), testQ2AffFixtureNonce)
	if !mqttValidQ2AffClaimSubject(claim) || !mqttValidQ2AffCopySubject(copySubject) {
		t.Fatalf("the fixture's own subjects are not valid: %q %q", claim, copySubject)
	}
	const owner = "xxxxxxxxxxxxxxxxxxxxxx"
	msg, hdrLen := mqttNewCorrelatedPubRel(4, owner, "dur-scan", 7, _EMPTY_, claim, copySubject)
	if msg == nil {
		t.Fatal("the builder refused a record this test needs")
	}
	// The owner header is REMOVED after the build, because the builder will not write a
	// record without one — which is the property that makes this a record from elsewhere.
	ownerHdr := []byte(mqttPubRelOwnerHeader + ":" + owner + _CRLF_)
	if !bytes.Contains(msg, ownerHdr) {
		t.Fatalf("the built record does not carry the owner header this test removes")
	}
	msg = bytes.Replace(msg, ownerHdr, nil, 1)
	if _, err := sess.jsa.storeMsg(sess.pubRelSubject, hdrLen-len(ownerHdr), msg); err != nil {
		t.Fatalf("plant the owner-less PUBREL: %v", err)
	}

	records, refused, ok := sess.scanPubRelRecords(sess.pubRelSubject)
	if !ok {
		t.Fatal("the scan must succeed")
	}
	if len(records) != 1 {
		t.Fatalf("the scan must RETURN the record: the teardown walk decides its fate, not the "+
			"decoder, got %+v", records)
	}
	if refused != 1 || records[0].shapeErr == nil {
		t.Fatalf("the record must carry its refusal so an ACTING caller skips it, refused=%d rec=%+v",
			refused, records[0])
	}
	if records[0].affRef.State != mqttPubRelAffineRefValid {
		t.Fatalf("the affine reference the teardown walk exists to notice must be readable on the "+
			"returned record, got %+v", records[0].affRef)
	}
}

// TestStoredPubRelHeadersAreExactAndSingular is round 17's second HIGH finding, and both
// halves of it were reproduced before being fixed.
//
// The underlying header parsers are deliberately lenient for their own callers, and this
// boundary was trusting them. mqttParsePubRelNATSHeader discards strconv's error, so
// `Nmqtt-PubRel:65536` came back as 65535 — a plausible identifier that is NOT the one the
// record names, under which the PUBREL would then be transmitted. And every one of these
// headers takes the first occurrence with no count, so a record carrying two identifiers or
// two source-correlation pairs was accepted with the reader silently picking one.
//
// Both are shapes this broker never writes, so both are refused rather than resolved by an
// arbitrary rule — the same answer a duplicate JSON member gets on the register.
func TestStoredPubRelHeadersAreExactAndSingular(t *testing.T) {
	t.Parallel()
	const owner = "xxxxxxxxxxxxxxxxxxxxxx"
	base := "NATS/1.0" + _CRLF_ + mqttPubRelOwnerHeader + ":" + owner + _CRLF_
	pi := func(v string) string { return mqttNatsPubRelHeader + ":" + v + _CRLF_ }
	src := func(dur, seq string) string {
		return mqttPubRelSrcDurHeader + ":" + dur + _CRLF_ + mqttPubRelSrcSeqHeader + ":" + seq + _CRLF_
	}

	// The controls FIRST, so no refusal below can be vacuous.
	for _, tc := range []struct {
		name    string
		hdr     string
		wantPI  uint16
		wantDur string
		wantSeq uint64
	}{
		{"an uncorrelated record", base + pi("7") + _CRLF_, 7, _EMPTY_, 0},
		{"a correlated record", base + pi("7") + src("durA", "11") + _CRLF_, 7, "durA", 11},
		{"the largest identifier", base + pi("65535") + _CRLF_, 65535, _EMPTY_, 0},
	} {
		got, err := mqttDecodeStoredPubRel([]byte(tc.hdr))
		if err != nil {
			t.Errorf("%s must decode: %v", tc.name, err)
			continue
		}
		if got.pi != tc.wantPI || got.srcDur != tc.wantDur || got.srcSeq != tc.wantSeq {
			t.Errorf("%s decoded to pi=%d src=%q/%d", tc.name, got.pi, got.srcDur, got.srcSeq)
		}
	}

	for _, tc := range []struct{ name, hdr string }{
		// 65536 is the case that matters most: it SATURATES to 65535 rather than failing,
		// so a permissive decode yields a clean, ordinary-looking identifier.
		{"an identifier one past the 16-bit range", base + pi("65536") + _CRLF_},
		{"an identifier far out of range", base + pi("999999") + _CRLF_},
		{"an identifier that is not a number", base + pi("abc") + _CRLF_},
		{"an identifier with a sign", base + pi("-1") + _CRLF_},
		{"an identifier with trailing space", base + pi("7 ") + _CRLF_},
		{"two identifier headers", base + pi("9") + pi("7") + _CRLF_},
		{"two identifier headers agreeing", base + pi("7") + pi("7") + _CRLF_},
		{"two source-correlation pairs", base + pi("7") + src("durA", "11") + src("durB", "22") + _CRLF_},
		{"two source durables", base + pi("7") + mqttPubRelSrcDurHeader + ":durA" + _CRLF_ +
			mqttPubRelSrcDurHeader + ":durB" + _CRLF_ + mqttPubRelSrcSeqHeader + ":11" + _CRLF_ + _CRLF_},
		{"two source sequences", base + pi("7") + mqttPubRelSrcDurHeader + ":durA" + _CRLF_ +
			mqttPubRelSrcSeqHeader + ":11" + _CRLF_ + mqttPubRelSrcSeqHeader + ":22" + _CRLF_ + _CRLF_},
	} {
		if got, err := mqttDecodeStoredPubRel([]byte(tc.hdr)); err == nil {
			t.Errorf("%s must be refused, decoded to %+v", tc.name, got)
		}
	}
}

// TestDrainIntentPastPubRecWithNoIdentifierIsQuarantined is round 17's first HIGH finding.
//
// `owesPubRel` was `PubRel && PI != 0`, which folded "past PUBREC with no identifier" into
// "not past PUBREC". A record explicitly saying the exchange had passed PUBREC was
// therefore restored as an ORDINARY CLAIMED SOURCE: not quarantined, and invisible to
// PUBREL recovery, so source interest could be restored for it and the payload of an
// exchange the client has already answered redelivered.
//
// The phase is `PubRel` alone. Whether the intent can be ACTED ON is a separate question,
// and the answer to it is the quarantine — never an adjustment to the phase.
func TestDrainIntentPastPubRecWithNoIdentifierIsQuarantined(t *testing.T) {
	t.Parallel()
	const sid = "1"
	const dur = "$MQTT_plain_no_pi"
	for _, tc := range []struct {
		name string
		seq  mqttPersistedDrainSeq
	}{
		{"no identifier", mqttPersistedDrainSeq{Seq: 41, PubRel: true, PI: 0,
			OwnerNonce: "xxxxxxxxxxxxxxxxxxxxxx"}},
		{"no owner nonce", mqttPersistedDrainSeq{Seq: 41, PubRel: true, PI: 7}},
		{"neither", mqttPersistedDrainSeq{Seq: 41, PubRel: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := mqttDrainFromPersisted(&mqttPersistedDrain{
				Cons: &ConsumerConfig{Durable: dur}, QoS: 2,
				Started: []mqttPersistedDrainSeq{tc.seq},
			})
			if d == nil {
				t.Fatal("the drain must be RESTORED: dropping it accepts the session without a " +
					"drain whose consumer and pending exchange are still stranded")
			}
			st := d.seqs[41]
			if st == nil {
				t.Fatal("the sequence must be restored")
			}
			// THE phase fact, and the assertion the defect failed: the record says past
			// PUBREC, so the restored state must say past PUBREC.
			if !st.intent {
				t.Error("a record saying PubRel is PAST PUBREC: reading it as a PUBLISH-phase " +
					"delivery permits redelivering a payload the client has already answered")
			}
			if !st.ownerUnusable {
				t.Error("an intent nothing can attribute must be QUARANTINED")
			}
			if !st.claimed {
				t.Error("the source claim must stand: acknowledging it would lose the message")
			}
			// And the quarantine is visible through the property every subscription path
			// asks, which is what keeps the drain from being adopted or staged over.
			sess := &mqttSession{drains: map[string]*mqttDrain{sid: d}}
			sess.mu.Lock()
			hold := sess.drainSidHoldLocked(sid)
			sess.mu.Unlock()
			// The REASON matters, not just that it is held: F-094 widened the predicate so
			// every past-PUBREC intent holds its sid, and an unattributable one must still
			// report the stronger condition — it is the one that needs an operator, and the
			// refusal message says so.
			if hold != mqttDrainHeldUnusableIdentity {
				t.Errorf("the sid must report an unusable identity, got %v: otherwise a "+
					"re-SUBSCRIBE can take the drain away, or the operator is told the wrong "+
					"thing about why it cannot", hold)
			}
		})
	}
}

// TestUnattributableDrainIntentSkipsSourceInterest is the other half of that finding: the
// quarantine has to reach the SKIP SET, or recoverDrains restores source interest for the
// drain and JetStream re-offers the payload.
//
// recoverDrainPubRels builds its claims from intents it can act on, and an unattributable
// one has neither an identifier to compare a record against nor an identity to recreate one
// under — so it cannot be a claim. Being absent from the claims is not enough: absent means
// "nothing to skip", which is exactly the reading that restores interest.
func TestUnattributableDrainIntentSkipsSourceInterest(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-skip-unattributable")
	const dur = "dur-unattributable"
	// The owner nonce is deliberately VALID and only the identifier is missing. With an
	// unusable nonce as well, a recreate attempt would fail on the nonce and set the skip
	// flag anyway — so the test would pass with the branch removed, which is exactly what a
	// mutation showed. This fixture leaves the recreate no reason to fail except the branch.
	d := addTestDrain(sess, "sid", dur, "foo", 2, map[uint64]*drainSeq{
		41: {claimed: true, intent: true, pi: 0, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx",
			ownerUnusable: true, regAmbiguous: true},
	})
	if d == nil {
		t.Fatal("the fixture must install a drain")
	}
	// $MQTT_out is interest-retained, so a store lands only with a consumer — and this
	// test's second assertion is that NOTHING is stored, which would hold vacuously
	// without one.
	if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
		t.Fatalf("ensurePubRelConsumerSubscription: %v", err)
	}

	skip := sess.recoverDrainPubRels(c)
	if !skip[dur] {
		t.Fatalf("the drain's durable must be SKIPPED: without it recoverDrains restores source "+
			"interest and the payload of an exchange already past PUBREC is re-offered, got %v", skip)
	}
	// And NOTHING was written for it. This is the assertion that separates "skipped
	// deliberately" from "skipped because a recreate attempt failed": an intent nobody can
	// attribute must not reach a store at all, because the record it would write names an
	// identifier the client is not waiting on.
	if records, refused, ok := sess.scanPubRelRecords(sess.pubRelSubject); !ok {
		t.Fatal("the scan must succeed")
	} else if len(records) != 0 || refused != 0 {
		t.Fatalf("an unattributable intent must not be recreated: %d record(s), %d refused",
			len(records), refused)
	}
	// The claim and the intent are untouched: nothing was finalized, and nothing was
	// acknowledged.
	st := drainSeqState(sess, d, 41)
	if !st.claimed || !st.intent || !st.ownerUnusable {
		t.Fatalf("the quarantined intent must be left exactly as it was, got %+v", st)
	}
}

// TestAffineProofWalkRefusesAnUnreadableRecord is round 21's finding at the route level.
//
// The record is the one the disposition split exists for: an OWNERLESS PUBREL — a shape
// this broker refuses to write — carrying a perfectly well-formed affine reference. The
// scan RETURNS it, deliberately, so this walk and the teardown can account for what is
// about to be destroyed. Accounting for it is not the same as acting on it, and the walk
// used to read its reference: an unattributable record could resolve an assignment, record
// PubRecProven and AffineDone, and reap the claim — suppressing or retracting a real loss.
//
// INCOMPLETE rather than ignored is the assertion. The record may be the very proof the
// walk was looking for, so its unreadability is exactly the state in which no loss may be
// inferred either way.
func TestAffineProofWalkRefusesAnUnreadableRecord(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "aff-proof-unreadable")
	if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
		t.Fatalf("ensurePubRelConsumerSubscription: %v", err)
	}

	claim := mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcdef", 42)
	copySubject := mqttQ2AffCopySubject("abcd1234", testQ2AffFixtureNonce,
		strings.Repeat("a", mqttQ2AffClaimHashLen), testQ2AffFixtureNonce)
	if !mqttValidQ2AffClaimSubject(claim) || !mqttValidQ2AffCopySubject(copySubject) {
		t.Fatalf("the fixture's own subjects are not valid: %q %q", claim, copySubject)
	}
	const owner = "xxxxxxxxxxxxxxxxxxxxxx"
	msg, hdrLen := mqttNewCorrelatedPubRel(4, owner, "dur-aff", 7, _EMPTY_, claim, copySubject)
	if msg == nil {
		t.Fatal("the builder refused a record this test needs")
	}
	// The owner header is REMOVED after the build, because the builder will not write a
	// record without one — which is what makes this a record from elsewhere.
	ownerHdr := []byte(mqttPubRelOwnerHeader + ":" + owner + _CRLF_)
	if !bytes.Contains(msg, ownerHdr) {
		t.Fatal("the built record does not carry the owner header this test removes")
	}
	msg = bytes.Replace(msg, ownerHdr, nil, 1)
	if _, err := sess.jsa.storeMsg(sess.pubRelSubject, hdrLen-len(ownerHdr), msg); err != nil {
		t.Fatalf("plant the owner-less PUBREL: %v", err)
	}

	// The premise: the scan RETURNS it, refused, with its affine reference READABLE. If it
	// were dropped or its reference unparsed, the walk could not act on it either way and
	// this test would prove nothing.
	records, refused, ok := sess.scanPubRelRecords(sess.pubRelSubject)
	if !ok || len(records) != 1 || refused != 1 {
		t.Fatalf("the scan must return exactly one refused record, got ok=%v n=%d refused=%d",
			ok, len(records), refused)
	}
	if records[0].shapeErr == nil {
		t.Fatal("the record must carry a decode refusal")
	}
	if !records[0].affRef.valid() {
		t.Fatalf("the affine reference must be READABLE on the refused record, or the walk has "+
			"nothing to be tempted by: %+v", records[0].affRef)
	}

	// THE assertion: incomplete, not complete.
	if got := sess.reconcileQ2AffPubRelProofs(c); got != mqttQ2AffProofIncomplete {
		t.Fatalf("an unreadable record must leave the affine evidence INCOMPLETE, got %v: reading "+
			"its reference lets an unattributable record prove a delivery and reap the assignment "+
			"claim", got)
	}
}
