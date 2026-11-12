// Permanent per-CONNECT acceptance facts.
//
// An accepted CONNECT records one durable, dated fact of its own acceptance on a
// subject nothing else ever writes: $MQTT.accept.<domainTk><cidHash>.<epoch>.<acceptID>.
// The fact and the session record are committed in ONE JetStream atomic batch on
// $MQTT_sess, so there is no interval in which a session has been accepted and the
// evidence of that acceptance does not yet exist — the split that defeated every
// watermark design before this one.
//
// What the fact replaces is the single-slot delayed-Will cancellation watermark.
// A slot is regressible: a sibling write erases the acceptance an earlier CONNECT
// recorded, and an erased acceptance can never be recovered because nothing else
// ever knew about it. A fact's subject carries its own acceptID, so a fact is
// never overwritten by anything, and the set of acceptances is therefore
// monotone. That is the whole of issue #35.
//
// The fact is DATED (At, sampled by the accepting broker when it composes the
// attempt) because a delayed Will must be suppressed only by an acceptance that
// happened before the Will became due: MQTT 5.0 says a reconnect during the Will
// Delay Interval suppresses the Will ([MQTT-3.1.3-9]), and equally that the Will
// MUST be published once the delay has elapsed ([MQTT-3.1.2-8]). A watermark
// carrying no date could only answer the first.
//
// Ordering inside the batch is load-bearing: the fact is staged FIRST and the
// session record carries the commit. The batch's PubAck therefore reports the
// session record's sequence (the reply rides the commit entry), and the fact sorts
// strictly BELOW the session record of the connection that wrote it — so a
// connection can never suppress its own Will. A fact's sequence is never derived
// arithmetically from the ack; it is read back from the stream.
//
// Facts are permanent and never reaped: retiring one needs proof that no delayed
// Will it could still suppress remains, which nothing here has. See
// docs/CONFORMANCE.md for the storage residual that follows.

package mqtt

import "crypto/sha256"

// The accepted-CONNECT store observation seam and the session record subject, kept from the
// acceptance-fact file when the facts were replaced by Will anchors (issue #133).

// mqttAcceptStorePhase names the moment at which the accepted-CONNECT store
// observation seam fires.
//
// There are two, and the distinction is not decoration: only the second one has an
// acknowledgement, so only it can name the sequence the session is about to adopt,
// and only the first one precedes the write it describes. A single-moment seam could
// express neither half of "these exact bodies went out, and this exact sequence came
// back naming the record among them".
//
// The type is open on purpose. A test may declare a further phase and fire it from a
// deliberately broken store path, which is how the counterfactual this seam exists to
// exclude — an independently acknowledged fact write followed by a suppressed record
// write — is reachable at all. Production fires only the two below.
type mqttAcceptStorePhase uint8

const (
	// mqttAcceptStoreIssuing: the batch is composed and nothing has been sent.
	mqttAcceptStoreIssuing mqttAcceptStorePhase = iota
	// mqttAcceptStoreAcked: the commit's acknowledgement has been validated as this
	// batch's, so Seq names the SESSION RECORD.
	mqttAcceptStoreAcked
)

// mqttAcceptStoreObs is what the seam carries. Digests rather than bodies, because
// what an observer has to settle is identity — that the thing on the subject is what
// THIS attempt wrote — and a body would invite comparing something else.
type mqttAcceptStoreObs struct {
	Phase     mqttAcceptStorePhase
	RecSubj   string
	RecDigest [sha256.Size]byte
	// RecptSubj and RecptDigest name the COMMITTED authority, the member before the record:
	// an accepted CONNECT commits its acceptance and its record as ONE atomic batch.
	RecptSubj   string
	RecptDigest [sha256.Size]byte
	BatchID     string
	// Seq is the acknowledged SESSION RECORD sequence, and is zero before the ack.
	Seq uint64
}

// mqttSessSubject returns the per-client session-record subject.
func mqttSessSubject(domainTk, cidHash string) string {
	return mqttSessStreamSubjectPrefix + domainTk + cidHash
}

// mqttStopSignalled reports whether any of the stop signals has fired.
func mqttStopSignalled(stop []<-chan struct{}) bool {
	for _, ch := range stop {
		select {
		case <-ch:
			return true
		default:
		}
	}
	return false
}
