//go:build mqttport_fulltests

// The wire half of the reclamation tests.
//
// It lives behind the full-suite tag because that is where the real broker, the real client
// connection and the real delivery helpers are. The in-memory fixtures beside it can show
// what the pass DECIDES; only these can show what the client actually receives, and the
// licence this change rests on is a claim about the client's socket.

package mqtt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// THE POST-ABSENCE CREATE, ON THE WIRE, which is the schedule the design's licence rests on.
//
// A DISPLACED writer creates on a subject the current owner just read as empty, with no
// local reassertion, and the pass then lifts and returns the identifier to the allocator.
// The licence says that is safe because neither post-absence creator describes an
// outstanding client exchange: the tombstone is `Cancelled` by construction, and a fresh
// allocation is untransmitted at create time, with `beginSendLocked` requiring local pending
// state and connection ownership before anything reaches the wire.
//
// THE ASSERTION IS ON THE CLIENT'S SOCKET, not on broker state, because that is where the
// harm would be: one packet identifier carrying two exchanges the client has not completed.
//
// The exclusion is installed on an identifier with NO PRIOR EXCHANGE. An earlier revision
// stranded a completed exchange's own identifier instead, and spent three attempts racing
// that exchange's terminal cleanup, which re-created the register under the fixture. The
// schedule under test needs neither a prior exchange nor that race: what it needs is an
// excluded identifier, a real record appearing after an honest empty read, and a real
// delivery contending for it.
func TestMQTTExclusionReclaimWireSafetyUnderADisplacedWriterCreate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cancelled bool
		delivers  bool
		why       string
	}{
		{
			name:      "a displaced tombstone create",
			cancelled: true,
			delivers:  true,
			why: "a cancelled record describes a delivery that was NEVER transmitted, so the allocation may " +
				"replace the occupant and the exchange goes out normally",
		},
		{
			name:      "a displaced fresh allocation create",
			cancelled: false,
			delivers:  false,
			why: "a live displaced record is durably fenced by the create-only CAS, so the allocation is " +
				"refused and the delivery is WITHHELD for a later offer rather than sent under a contested " +
				"identifier",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: "excl-wire-sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
			defer sub.Close()
			testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "excl/wire", qos: 1}}, []byte{1})
			testMQTTFlush(t, sub, nil, subr)

			pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "excl-wire-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer pub.Close()
			testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)

			sess := testMQTTGetClient(t, s, "excl-wire-sub").mqtt.sess
			as := sess.asm

			// An identifier with no exchange behind it, so the subject is genuinely empty
			// and nothing else is operating on it.
			const strandedPI = uint16(4242)
			subject := func() string {
				sess.mu.Lock()
				defer sess.mu.Unlock()
				return sess.inflightSubjectLocked(strandedPI)
			}()
			if subject == _EMPTY_ {
				t.Fatal("fixture error: the session could not name a slot subject")
			}
			if _, smsg, lerr := as.loadInflightRecord(subject); lerr != nil || smsg != nil {
				t.Fatalf("fixture error: the subject is not empty: smsg=%v err=%v", smsg, lerr)
			}

			// THE DISCLOSED CLASS: the register is gone, its absence was never proven, and
			// the exclusion stands with nothing left to lift it.
			sess.mu.Lock()
			sess.excludeIdentifierLocked(strandedPI, subject, mqttNewInflightNonce())
			sess.mu.Unlock()

			// THE DISPLACED CREATE IS A REAL WRITE, through the production create-only CAS
			// boundary — the same expectSeq-0 write markInflightRegisterCancelledPending and
			// the allocation pair use — landing from INSIDE the pass's read: after the
			// subject was observed empty and before the lift. A fabricated loader answer
			// would prove nothing, because the question is whether a record that actually
			// PERSISTS can reach the client.
			var once sync.Once
			var reads, created int32
			as.mu.Lock()
			as.testInflightLoad = func(loaded string) (*mqttInflightRecord, *StoredMsg, error) {
				if loaded != subject {
					return nil, nil, nil
				}
				atomic.AddInt32(&reads, 1)
				once.Do(func() {
					displaced := freshRegisterLineage(&mqttInflightRecord{
						State: mqttInflightPublish, PI: strandedPI, Dur: "$MQTT_displaced",
						Seq: 77, QoS: 1, Nonce: mqttNewInflightNonce(), Cancelled: tc.cancelled,
					})
					if _, err := as.storeInflightCAS(subject, 0, displaced); err != nil {
						t.Errorf("the displaced create did not land: %v", err)
						return
					}
					atomic.AddInt32(&created, 1)
				})
				// The honest absence the lift acts on: this read observed the subject
				// BEFORE the displaced write above landed.
				return nil, nil, nil
			}
			as.mu.Unlock()

			sess.runExclusionReclaim(mqttExclusionReclaimDelay)

			as.mu.Lock()
			as.testInflightLoad = nil
			as.mu.Unlock()

			// THE FIXTURE MUST REACH ITS ROUTE. Without these the test would pass just as
			// happily against a pass that never ran, which is the vacuity this replaced.
			if atomic.LoadInt32(&reads) == 0 {
				t.Fatal("fixture error: the pass never read the bound subject")
			}
			if atomic.LoadInt32(&created) == 0 {
				t.Fatal("fixture error: the displaced writer created nothing, so no post-absence record was raced")
			}
			if cur, smsg, lerr := as.loadInflightRecord(subject); lerr != nil || smsg == nil || cur == nil {
				t.Fatalf("the displaced record is not on the subject: cur=%v smsg=%v err=%v", cur, smsg, lerr)
			}
			held := func() bool {
				sess.mu.Lock()
				defer sess.mu.Unlock()
				return slotExclusionHeldLocked(sess.sendsInFlight[strandedPI])
			}()
			if held {
				t.Fatal("fixture error: the honest absence did not lift, so the displaced create was never raced")
			}

			// AIM THE ALLOCATOR AT THE RECLAIMED IDENTIFIER, so the next delivery contends
			// for the very subject the displaced writer now occupies.
			//
			// Whether it WINS that identifier is deliberately not asserted: over a cancelled
			// tombstone the allocation may replace the occupant, while over a live displaced
			// record the create-only CAS refuses and the allocator moves on. Both are safe,
			// and the difference is the durable fence showing through. What the licence
			// claims — and all it claims — is that the client is never left holding one
			// identifier for two exchanges.
			sess.mu.Lock()
			sess.last_pi = strandedPI - 1
			sess.mu.Unlock()

			// THE CONTENDED ALLOCATION MUST ACTUALLY BE ATTEMPTED. Silence on the socket is
			// not evidence on its own — it passes just as well if the delivery never got as
			// far as the store — so the withheld arm waits for a real write attempt against
			// the contested subject before concluding anything from the quiet.
			attempted := make(chan struct{})
			var attemptOnce sync.Once
			if !tc.delivers {
				// OBSERVE, do not fabricate: the seam records that the write was attempted
				// against the contested subject and then performs the REAL store, so the
				// refusal below is the production create-only CAS refusing, not the fixture
				// standing in for it. Installed only on this arm, because the delivering arm
				// needs the untouched pair write.
				as.mu.Lock()
				as.testInflightStore = func(sub string, expect uint64, r *mqttInflightRecord) (uint64, error) {
					if sub == subject {
						attemptOnce.Do(func() { close(attempted) })
					}
					return as.jsa.storeInflightCAS(sub, expect, r)
				}
				as.mu.Unlock()
				defer func() {
					as.mu.Lock()
					as.testInflightStore = nil
					as.mu.Unlock()
				}()
			}

			testMQTTPublish(t, pub, pubr, 1, false, false, "excl/wire", 2, []byte("second"))
			if tc.delivers {
				secondPI := testMQTTCheckPubMsg(t, sub, subr, "excl/wire", mqttPubQos1, []byte("second"))
				if secondPI == 0 {
					t.Fatalf("the delivery never reached the client: %s", tc.why)
				}
				// ON THE RECLAIMED IDENTIFIER: accepting any identifier would let the test
				// pass without the contended allocation ever happening.
				if secondPI != strandedPI {
					t.Fatalf("the delivery took identifier %d, not the reclaimed %d, so the contested subject "+
						"was never allocated against: %s", secondPI, strandedPI, tc.why)
				}
			} else {
				select {
				case <-attempted:
				case <-time.After(5 * time.Second):
					t.Fatalf("no allocation was attempted against the contested subject, so the silence below "+
						"would prove nothing: %s", tc.why)
				}
			}
			// EITHER WAY, NOTHING MORE. One exchange or none, never two under one
			// identifier — that is the entire claim the licence makes, and the arm that
			// delivers nothing is the durable fence refusing a contested identifier rather
			// than a failure: AckWait re-offers it.
			testMQTTExpectNothing(t, subr)
		})
	}
}

// THE REGISTERLESS RETAINED ROUTE, which is the reuse path with NO durable fence.
//
// A retained send allocates a packet identifier through the send registry alone: it writes
// no register, so nothing CASes against whatever occupies the slot subject. Every other
// reuse route is fenced by the create-only CAS, and this one is not — which makes it the
// route where a false lift would show first, and therefore the one the licence most needs
// to be true of.
//
// It is true here for the same reason: the displaced record does not describe an outstanding
// client exchange, so an identifier reclaimed over it may be handed to a retained send
// without the client ever holding one identifier for two exchanges.
func TestMQTTExclusionReclaimWireSafetyOnTheRegisterlessRetainedRoute(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "excl-ret-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, true, "excl/ret", 1, []byte("kept"))
	testMQTTFlush(t, pub, nil, pubr)

	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: "excl-ret-sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)

	sess := testMQTTGetClient(t, s, "excl-ret-sub").mqtt.sess
	as := sess.asm

	const strandedPI = uint16(4242)
	subject := func() string {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.inflightSubjectLocked(strandedPI)
	}()
	if subject == _EMPTY_ {
		t.Fatal("fixture error: the session could not name a slot subject")
	}

	sess.mu.Lock()
	sess.excludeIdentifierLocked(strandedPI, subject, mqttNewInflightNonce())
	sess.mu.Unlock()

	// A LIVE displaced record — not a tombstone — because that is the shape the durable
	// fence would refuse for a register-backed send. A retained send never consults it, so
	// this is precisely the unfenced case.
	var once sync.Once
	var reads, created int32
	as.mu.Lock()
	as.testInflightLoad = func(loaded string) (*mqttInflightRecord, *StoredMsg, error) {
		if loaded != subject {
			return nil, nil, nil
		}
		atomic.AddInt32(&reads, 1)
		once.Do(func() {
			displaced := freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: strandedPI, Dur: "$MQTT_displaced",
				Seq: 77, QoS: 1, Nonce: mqttNewInflightNonce(),
			})
			if _, err := as.storeInflightCAS(subject, 0, displaced); err != nil {
				t.Errorf("the displaced create did not land: %v", err)
				return
			}
			atomic.AddInt32(&created, 1)
		})
		return nil, nil, nil
	}
	as.mu.Unlock()

	sess.runExclusionReclaim(mqttExclusionReclaimDelay)

	as.mu.Lock()
	as.testInflightLoad = nil
	as.mu.Unlock()

	if atomic.LoadInt32(&reads) == 0 || atomic.LoadInt32(&created) == 0 {
		t.Fatalf("fixture error: the pass did not race a displaced create (reads=%d created=%d)",
			atomic.LoadInt32(&reads), atomic.LoadInt32(&created))
	}
	if func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return slotExclusionHeldLocked(sess.sendsInFlight[strandedPI])
	}() {
		t.Fatal("fixture error: the honest absence did not lift, so the reclaimed identifier was never reused")
	}

	// Aim the allocator at the reclaimed identifier, then trigger the RETAINED send.
	sess.mu.Lock()
	sess.last_pi = strandedPI - 1
	sess.mu.Unlock()

	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "excl/ret", qos: 1}}, []byte{1})
	retainedPI := testMQTTCheckPubMsg(t, sub, subr, "excl/ret", mqttPubQos1|mqttPubFlagRetain, []byte("kept"))
	if retainedPI == 0 {
		t.Fatal("the retained delivery never reached the client")
	}
	if retainedPI != strandedPI {
		t.Fatalf("the retained send took identifier %d, not the reclaimed %d, so the unfenced route was never "+
			"exercised over the displaced record", retainedPI, strandedPI)
	}
	// One exchange on that identifier, never two.
	testMQTTExpectNothing(t, subr)
}
