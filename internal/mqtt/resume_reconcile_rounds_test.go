package mqtt

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// testDrainWithClaims is a drain whose consumer has n deliveries pending and unacknowledged, every
// one claimed and still in the stream above its first sequence and the consumer's ack floor: the
// claims recoverDrains can settle only by reading each source by sequence.
func testDrainWithClaims(t *testing.T, sess *mqttSession, nc *nats.Conn, sid string, n int) (*mqttDrain, string, []uint64) {
	t.Helper()
	durable := "dur-" + sid + "-" + nuid.Next()
	filter := "filter" + sid
	inbox := mqttSubPrefix + durable
	if _, err := nc.Subscribe(inbox, func(*nats.Msg) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := sess.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable: durable, DeliverSubject: inbox, FilterSubject: mqttStreamSubjectPrefix + filter,
			AckPolicy: AckExplicit, AckWait: time.Minute, MaxAckPending: 1024,
		},
	}); err != nil {
		t.Fatalf("createDurableConsumer: %v", err)
	}
	seqs := make(map[uint64]*drainSeq, n)
	var list []uint64
	for i := 0; i < n; i++ {
		resp, err := sess.jsa.storeMsg(mqttStreamSubjectPrefix+filter, 0, []byte("m"))
		if err != nil {
			t.Fatalf("storeMsg: %v", err)
		}
		seqs[resp.Sequence] = &drainSeq{claimed: true}
		list = append(list, resp.Sequence)
	}
	drainCheckFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ci, err := sess.jsa.lookupConsumer(mqttStreamName, durable)
		if err != nil {
			return err
		}
		if ci.NumAckPending != n {
			return errDrainTestNotYet
		}
		return nil
	})
	return addTestDrain(sess, sid, durable, filter, 1, seqs), durable, list
}

func testConsumerInfoSubject(durable string) string {
	return JSApiPrefix + ".CONSUMER.INFO." + mqttStreamName + "." + durable
}

func testSeqTargets(seqs ...[]uint64) func(string) bool {
	want := map[string]bool{}
	for _, l := range seqs {
		for _, s := range l {
			want["seq:"+strconv.FormatUint(s, 10)] = true
		}
	}
	return func(s string) bool { return want[s] }
}

// A drain recovery reads every drain's consumer in one round, then every source its claims
// need in a second.
func TestRecoverDrainsPipelinesLookupsAndLoads(t *testing.T) {
	_, c, sess, nc := drainTestSession(t, "drain-rounds")
	_, durA, seqsA := testDrainWithClaims(t, sess, nc, "a", 2)
	_, durB, seqsB := testDrainWithClaims(t, sess, nc, "b", 2)
	o := testObserveJSA(t, &sess.asm.jsa)
	isLookup := func(s string) bool { return s == testConsumerInfoSubject(durA) || s == testConsumerInfoSubject(durB) }
	isLoad := testSeqTargets(seqsA, seqsB)

	h1 := o.hold(isLookup)
	m := o.mark()
	done := make(chan struct{})
	go func() {
		sess.recoverDrains(c, nil)
		close(done)
	}()
	if !o.waitSends(m, 2, isLookup, 2*time.Second) {
		t.Fatalf("with the first lookup's reply held, %d of the 2 drains' consumers were read: serial", o.sends(m, isLookup))
	}
	h2 := o.hold(isLoad)
	o.release(h1, nil)
	if !o.waitSends(m, 4, isLoad, 2*time.Second) {
		t.Fatalf("with the first load's reply held, %d of the 4 claimed sources were read: serial", o.sends(m, isLoad))
	}
	o.release(h2, nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain recovery did not complete after its rounds were released")
	}
}

// A drain whose consumer lookup settled nothing keeps every claim, and only that drain: another
// drain whose consumer is authoritatively gone is still reconciled.
func TestRecoverDrainsUnsettledLookupSkipsOnlyThatDrain(t *testing.T) {
	_, c, sess, nc := drainTestSession(t, "drain-unsettled")
	dA, durA, seqsA := testDrainWithClaims(t, sess, nc, "a", 2)
	dB, durB, seqsB := testDrainWithClaims(t, sess, nc, "b", 2)
	if _, err := sess.jsa.deleteConsumer(mqttStreamName, durB, false); err != nil {
		t.Fatalf("deleting drain B's consumer: %v", err)
	}
	drop := func(subject string, _ []byte) bool { return subject == testConsumerInfoSubject(durA) }
	sess.asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { sess.asm.jsa.testDropRequest.Store(nil) })

	sess.recoverDrains(c, nil)

	for _, s := range seqsA {
		if st := drainSeqState(sess, dA, s); !st.claimed {
			t.Fatalf("drain A's claim %d was pruned on a consumer lookup that never answered", s)
		}
	}
	for _, s := range seqsB {
		if st := drainSeqState(sess, dB, s); st.claimed {
			t.Fatalf("drain B's claim %d survived its consumer's authoritative absence", s)
		}
	}
}

// A consumer lookup answered with neither information nor an error establishes nothing: the drain
// keeps its claims. (One lookup alone used to read that answer as the consumer gone.)
func TestRecoverDrainsNilConsumerInfoIsIndeterminate(t *testing.T) {
	_, c, sess, nc := drainTestSession(t, "drain-nil-info")
	dA, durA, seqsA := testDrainWithClaims(t, sess, nc, "a", 2)
	o := testObserveJSA(t, &sess.asm.jsa)
	h := o.hold(testSubjectIs(testConsumerInfoSubject(durA)))
	m := o.mark()
	done := make(chan struct{})
	go func() {
		sess.recoverDrains(c, nil)
		close(done)
	}()
	if !o.waitSends(m, 1, testSubjectIs(testConsumerInfoSubject(durA)), 2*time.Second) {
		t.Fatal("the drain's consumer was never looked up")
	}
	// Give the reply time to arrive and be parked, then hand it back carrying nothing.
	time.Sleep(100 * time.Millisecond)
	o.release(h, func(r *mqttJSAResponse) { r.value = &JSApiConsumerInfoResponse{} })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain recovery did not complete")
	}
	for _, s := range seqsA {
		if st := drainSeqState(sess, dA, s); !st.claimed {
			t.Fatalf("claim %d was pruned on a consumer lookup that answered nothing", s)
		}
	}
	if cir := (&JSApiConsumerInfoResponse{}); true {
		if ci, ok := mqttClassifyConsumerInfoResponse(cir); ci != nil || ok {
			t.Fatalf("an answer with no information and no error classifies (%v, %v), want unsettled", ci, ok)
		}
	}
}

// testPendingFixture is a session with three pending QoS 1 deliveries, as a same-broker resume
// finds them.
func testPendingFixture(t *testing.T) (*testInflightEnv, string, []uint16, map[mqttPendingRef]struct{}) {
	t.Helper()
	e := testNewInflightEnv(t, "client-redeliver-"+nuid.Next())
	dur := e.sess.idHash + "_" + nuid.Next()
	var pis []uint16
	for _, seq := range []uint64{910, 911, 912} {
		pis = append(pis, e.track(t, dur, seq, 1))
	}
	e.sess.mu.Lock()
	snapshot := mqttPendingRefs(e.sess.pendingPublish)
	e.sess.mu.Unlock()
	return e, dur, pis, snapshot
}

// A resume with nothing pending does not read the source stream: no pending publish needs its
// first sequence. The reconcile's boundary hook still fires.
func TestRedeliverySkipsFirstSeqLookupWhenNothingNeedsIt(t *testing.T) {
	// NOT parallel: installs a process-global restore hook.
	e := testNewInflightEnv(t, "client-redeliver-none")
	o := testObserveJSA(t, &e.asm.jsa)
	var scanned bool
	restore := setMQTTTestRestorePause(func(at string) {
		if at == "reap-scanned" {
			scanned = true
		}
	})
	defer restore()
	m := o.mark()
	e.sess.resendPendingRedelivery(e.c, nil, nil)
	if n := o.sends(m, testSubjectIs(JSApiPrefix+".STREAM.INFO."+mqttStreamName)); n != 0 {
		t.Fatalf("a resume with no pending publish read the source stream %d times", n)
	}
	if !scanned {
		t.Fatal("the reconcile's reap-scanned boundary did not fire")
	}
}

// The pending publishes' sources are read in one round.
func TestRedeliveryPipelinesSourceLoads(t *testing.T) {
	e, _, _, snapshot := testPendingFixture(t)
	o := testObserveJSA(t, &e.asm.jsa)
	isLoad := testSeqTargets([]uint64{910, 911, 912})
	h := o.hold(isLoad)
	m := o.mark()
	done := make(chan struct{})
	go func() {
		e.sess.resendPendingRedelivery(e.c, snapshot, nil)
		close(done)
	}()
	if !o.waitSends(m, 3, isLoad, 2*time.Second) {
		t.Fatalf("with the first source load held, %d of the 3 were sent: serial", o.sends(m, isLoad))
	}
	o.release(h, nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reconcile did not complete after its round was released")
	}
}

// A source read that never answered is not an absence: the delivery is kept, under its identifier
// and its (durable, sequence), where a confirmed absence would have released it. (Whether it is
// re-offered at once depends on the replay allowance, which this rig does not grant.)
func TestRedeliveryIndeterminateLoadKeepsItsDelivery(t *testing.T) {
	e, dur, pis, snapshot := testPendingFixture(t)
	drop := func(subject string, body []byte) bool {
		var req JSApiMsgGetRequest
		return json.Unmarshal(body, &req) == nil && req.LastFor == _EMPTY_ && req.Seq == 911
	}
	e.asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { e.asm.jsa.testDropRequest.Store(nil) })
	e.sess.resendPendingRedelivery(e.c, snapshot, nil)

	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pis[1]]
	released := e.sess.pendingPublish[pis[0]] == nil && e.sess.pendingPublish[pis[2]] == nil
	e.sess.mu.Unlock()
	if ack == nil || ack.sseq != 911 || ack.jsDur != dur {
		t.Fatalf("the delivery whose source read went unanswered was not kept under identifier %d: %+v", pis[1], ack)
	}
	if !released {
		t.Fatal("fixture: the two deliveries whose sources read ABSENT must be released, or keeping the third proves nothing")
	}
}
