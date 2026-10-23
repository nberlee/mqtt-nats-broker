package mqtt

import (
	"encoding/json"
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
// keeps its claims. (It used to read that answer as the consumer gone.)
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
	// Wait until the reply is PARKED, not merely sent: a reply that arrived after the release
	// would bypass the transform, and the live consumer's real answer keeps the claims too.
	if !h.waitParked(1, 2*time.Second) {
		t.Fatal("the drain's consumer reply was never parked")
	}
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
