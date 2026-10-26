//go:build mqtt_fulltests

// The broker-level sentinel for the accepted-CONNECT atomic-batch write path, over a
// genuine 3-node embedded JetStream cluster and the production accept path.
//
// It is an INTEGRATION SENTINEL, not sole proof: the deterministic dependency canary
// for the server's apply-layer contract -- a committed batch prefix with no commit
// entry stores nothing -- lives in test/depproof, inside an isolated nats-server
// checkout. What this test adds is that the BROKER's accepted CONNECT really goes
// through that contract: one batch, both messages, and at the moment the commit is
// acknowledged the acceptance fact and the session record are both on the stream with
// the attempt's own bytes.
//
// The observation is a MOMENT, not an invariant. The session record's subject is a
// MaxMsgsPer:1 register, so a later legitimate save -- a repair save, a lifetime grant,
// a SUBSCRIBE -- replaces what the batch committed, and "a fact always has its original
// record" is simply false over time. What is true, and what mqttAcceptFactLanded's
// stated meaning needs, is that the record committed WITH the fact. That is a claim
// about the store boundary, so it is asserted at the store boundary: the seam fires
// inside storeAcceptBatch, before the caller adopts the sequence and before anything
// else can write the subject. The accept transaction the CONNECT holds is what keeps
// the window closed to a local successor.
//
// The paired mutation is m-accept-split-write (test/depproof/run.sh --broker): the one
// batch becomes two independent conditional writes, and the second is suppressed
// through the counterfactual phase below while the caller is still handed an
// acknowledgement it cannot tell from a real commit. That must make the assertion here
// fail with a torn pair.

package mqtt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/nats-io/nuid"
)

// acceptBatchTornMarker names the ONE failure this sentinel is evidence about: the
// acceptance fact durable without the session record its batch was supposed to carry.
// The harness scores the mutation row on this marker rather than on any failure, so a
// kill that came from a broken cluster or a refused CONNECT is not counted.
const acceptBatchTornMarker = "ACCEPT-BATCH-TORN"

// mqttAcceptStoreSplitRecordPending is the counterfactual phase: the acceptance fact
// has been INDEPENDENTLY acknowledged and the session record has not been written yet.
//
// Production never fires it, and cannot: storeAcceptBatch issues one atomic batch, so
// no such moment exists between the two messages. It is declared here, next to the
// assertion it exists to break, because the mutation needs an interception point
// rather than a notification -- an observer returning an error at this phase is what
// suppresses the record write. Declaring it in production would be declaring a moment
// the broker does not have.
const mqttAcceptStoreSplitRecordPending mqttAcceptStorePhase = 200

// TestMQTTAcceptBatchReplicatedIndivisible: a FIRST-EVER session's accepted CONNECT
// commits its acceptance fact and its session record together, over a real R3
// $MQTT_sess, and the acknowledgement it returns names the record.
//
// Named to be picked up by the `^TestMQTTAcceptBatchReplicated` filter.
func TestMQTTAcceptBatchReplicatedIndivisible(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	asm := testMQTTGetGlobalASM(t, b.s)
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	// The observer runs on the broker's CONNECT goroutine, and an atomic restore does
	// not wait for a call already past the load. Without the gate a late t.Errorf
	// panics the whole test binary rather than failing this test. Registered BEFORE
	// the restore, so cleanup order (LIFO) uninstalls the observer first and only then
	// waits out whoever is inside it.
	gate := newTestHookGate(t)

	var mu sync.Mutex
	var issued, acked, suppressed int
	var issuedObs mqttAcceptStoreObs

	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		if !gate.enter() {
			return nil
		}
		defer gate.leave()

		switch obs.Phase {
		case mqttAcceptStoreIssuing:
			mu.Lock()
			issued++
			issuedObs = obs
			mu.Unlock()
			return nil

		case mqttAcceptStoreSplitRecordPending:
			// Reachable only from a deliberately torn store path. Returning an error
			// is the suppression: the record write never happens, and the mutation
			// still hands the caller a success.
			mu.Lock()
			suppressed++
			mu.Unlock()
			return fmt.Errorf("the session record write is suppressed by m-accept-split-write")

		case mqttAcceptStoreAcked:
			mu.Lock()
			acked++
			want := issuedObs
			mu.Unlock()

			// The acknowledgement has to be about the batch that was issued, or
			// everything below is a readback of some other attempt's write.
			if obs.BatchID != want.BatchID || obs.FactDigest != want.FactDigest ||
				obs.RecDigest != want.RecDigest {
				t.Errorf("the acknowledged batch %q is not the batch that was issued (%q)",
					obs.BatchID, want.BatchID)
				return nil
			}

			// The fact, by CONTENT: its subject is unique to this attempt, so its own
			// bytes being there is the batch having committed.
			if _, rb := asm.jsa.readAcceptFactSeq(obs.FactSubj, obs.FactDigest); rb != mqttAcceptFactLanded {
				t.Errorf("%s: the acceptance fact on %q does not read back as this attempt's "+
					"(readback %d) although its batch was acknowledged at sequence %d",
					acceptBatchTornMarker, obs.FactSubj, rb, obs.Seq)
			}

			// And the session record, at the sequence the acknowledgement named --
			// which is the claim the caller acts on when it adopts resp.Sequence.
			smsg, lerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, obs.RecSubj)
			switch {
			case lerr != nil || smsg == nil:
				t.Errorf("%s: the acceptance batch was acknowledged at sequence %d, but no session "+
					"record is on %q: the fact committed without it (%v)",
					acceptBatchTornMarker, obs.Seq, obs.RecSubj, lerr)
			case smsg.Sequence != obs.Seq:
				t.Errorf("%s: the acceptance batch was acknowledged at sequence %d, but the session "+
					"record on %q is at %d: the acknowledged sequence does not name the record",
					acceptBatchTornMarker, obs.Seq, obs.RecSubj, smsg.Sequence)
			case mqttRecordDigest(smsg.Data) != obs.RecDigest:
				t.Errorf("%s: the session record at the acknowledged sequence %d on %q is not the "+
					"body this attempt sent", acceptBatchTornMarker, obs.Seq, obs.RecSubj)
			}

			// And the COMMITTED ownership receipt, the batch's THIRD member: it commits
			// with the fact and the record or not at all. That third member is what makes
			// the accepted CONNECT's metadata batch indivisible rather than a fact and a
			// record that happened to land beside a receipt -- the property F-105's cutover
			// rests on, because a receipt without its record would be a permanent claim to
			// an ownership the record never took.
			switch obs.RecptSubj {
			case "":
				t.Errorf("%s: the acknowledged accept batch carried no ownership receipt "+
					"member: the accept path is not the metadata-commit path", acceptBatchTornMarker)
			default:
				rmsg, rerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, obs.RecptSubj)
				switch {
				case rerr != nil || rmsg == nil:
					t.Errorf("%s: the accept batch was acknowledged at sequence %d, but no "+
						"COMMITTED receipt is on %q: the record committed without it (%v)",
						acceptBatchTornMarker, obs.Seq, obs.RecptSubj, rerr)
				case mqttRecordDigest(rmsg.Data) != obs.RecptDigest:
					t.Errorf("%s: the receipt on %q is not the body this attempt sent",
						acceptBatchTornMarker, obs.RecptSubj)
				}
			}
			return nil
		}
		return nil
	})
	t.Cleanup(restore)

	// A FIRST-EVER client id, and that is load-bearing rather than hygiene: on a
	// resumed session the previous record is still on the subject, so a suppressed
	// record write would leave a STALE record there instead of none -- the assertions
	// above would then be about a digest, and the mutation could survive a readback
	// that happened to find the old record at the acknowledged sequence.
	cid := "acceptbatch-" + nuid.Next()
	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// The seam having fired is part of what this test asserts. Without it a broker
	// that never took the accept-batch path at all -- or a hook that was restored
	// before the CONNECT reached it -- would pass every assertion above vacuously.
	mu.Lock()
	gotIssued, gotAcked, gotSuppressed := issued, acked, suppressed
	mu.Unlock()
	if gotIssued != 1 || gotAcked != 1 {
		t.Fatalf("the accepted CONNECT must have issued exactly one acceptance batch and had "+
			"exactly one acknowledged, got issued=%d acked=%d", gotIssued, gotAcked)
	}
	if gotSuppressed != 0 {
		t.Errorf("%s: the production accept path fired the split-write phase %d time(s); it has "+
			"no such moment", acceptBatchTornMarker, gotSuppressed)
	}

	testMQTTDisconnect(t, mc, nil)
}

// MUTATION ANCHOR: test/depproof/run.sh inserts the m-accept-split-write helper here
// and redirects storeAcceptBatch to it. The helper is written into this file rather
// than kept in the harness script so the counterfactual is read next to the assertion
// it has to break, and it is absent from a clean tree, so nothing unused is carried in
// production or in the ordinary test build.
