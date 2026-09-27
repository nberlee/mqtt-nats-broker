package mqtt

import (
	"testing"
	"time"
)

// A syntactically valid ACK subject is not a delivered JetStream message. Source
// confirmation tests must receive a real offer and persist that offer's reply.
func (e *testInflightEnv) publishOfferedSource(t *testing.T, dur string) (uint64, string) {
	t.Helper()
	sseq := e.publishSource(t, dur)
	ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, dur)
	if err != nil || ci == nil {
		t.Fatalf("load source consumer: %v", err)
	}
	sub, err := e.s.nrc.SubscribeSync(ci.Config.DeliverSubject)
	if err != nil {
		t.Fatalf("subscribe to source delivery: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush source interest: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("receive source offer: %v", err)
	}
	if !mqttSourceAckReplyBinds(msg.Reply, mqttStreamName, dur, sseq) {
		t.Fatalf("source offer %q does not bind %s/%d", msg.Reply, dur, sseq)
	}
	return sseq, msg.Reply
}

// Seed the crash cut after an MQTT ACK, before source settlement/deletion. A raw
// slot rewrite leaves the admitted exchange at Publish and is not that crash cut.
// Return the register's sequence, not the authority sequence from the ACK batch.
func testAdvanceTerminalPair(t *testing.T, e *testInflightEnv, subject string,
	seq uint64, r *mqttInflightRecord) uint64 {

	t.Helper()
	name, err := mqttParseInflightSubject(subject)
	if err != nil {
		t.Fatal(err)
	}
	to := mqttExchangeTerminal
	if r.QoS == 2 {
		to = mqttExchangeRejected
	}
	if _, err := e.sess.advanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Gen: name.gen,
		Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
		Nonce: r.Lin, To: to, Acq: r.Acq, Register: r, RegisterSeq: seq,
	}); err != nil {
		t.Fatalf("advance terminal exchange/register pair: %v", err)
	}
	cur, msg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || msg == nil {
		t.Fatalf("read terminal register: rec=%+v err=%v", cur, err)
	}
	ref := mqttSlotRefFromRecord(subject, msg.Sequence, cur)
	if ref == nil || !e.sess.terminalSourcePair(*ref, cur) ||
		cur.Nonce != r.Nonce || cur.Lin != r.Lin || cur.Acq != r.Acq ||
		cur.SourceAckReply != r.SourceAckReply {
		t.Fatalf("fixture did not establish the exact terminal pair: %+v", cur)
	}
	*r = *cur
	return msg.Sequence
}
