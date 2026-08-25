//go:build mqttport_fulltests

package mqtt

import (
	"fmt"
	"testing"
	"time"
)

// TestMQTTSharedLastHolderRefusalKeepsTheGroupDurable drives the last-holder refusal through
// a real production route rather than the helper alone: a client UNSUBSCRIBEs from the group
// it is the only member of, which is the caller a client can actually reach. The unit half
// is TestMQTTSharedLastHolderScanFailsClosed, in the default tree.
//
// A3 is a NEGATIVE assertion and would hold if the release route never ran at all, so A1 and
// A2 are permanent controls that prove it does run. A2 in particular stops A3 passing on
// "any planted record blocks the delete": its record is well formed and names no holder, so
// the durable must still be deleted.
//
// Every arm asserts the SUBACK reason, the durable's presence both before the plant and
// immediately before the UNSUBSCRIBE, and the UNSUBACK reason. Without those an arm that
// observed a FAILED subscription would see an absent durable and pass for the wrong reason.
func TestMQTTSharedLastHolderRefusalKeepsTheGroupDurable(t *testing.T) {
	const clientID = "lastholder"
	const group, subject = "grp", "foo"
	filter := "$share/" + group + "/" + subject

	for _, tc := range []struct {
		name  string
		plant func(t *testing.T, asm *mqttAccountSessionManager)
		// wantDeleted is what the last-holder release must do with the record in place.
		wantDeleted bool
	}{
		{"A1 control, no foreign record", nil, true},
		{"A2 control, well-formed foreign non-holder", func(t *testing.T, asm *mqttAccountSessionManager) {
			testPlantSessionRecordJSON(t, asm, "bystander", &mqttPersistedSession{
				ID:         "bystander",
				SharedSubs: map[string]byte{testSharedHolderKey("othergroup", "othersubject"): 1},
			})
		}, true},
		{"A3 refusal, undecodable record", func(t *testing.T, asm *mqttAccountSessionManager) {
			testPlantSessionRecord(t, asm, "corrupt", []byte("{not json"))
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true},
				o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			testMQTTReadConnAckV5(t, r)

			codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}})
			if len(codes) != 1 || codes[0] != mqttReasonGrantedQoS1 {
				t.Fatalf("SUBACK %v, want a single granted QoS 1 (0x%02x)", codes, mqttReasonGrantedQoS1)
			}

			asm := testMQTTGetAccountSessionManager(t, s, clientID)
			_, _, dur := mqttSharedNames(group, subject)
			if !testMQTTConsumerExists(t, s, clientID, dur) {
				t.Fatalf("the group durable %q does not exist after a granted shared subscribe", dur)
			}

			if tc.plant != nil {
				tc.plant(t, asm)
			}
			// Re-checked AFTER the plant: a store that somehow disturbed the durable would
			// otherwise be indistinguishable from the release under test.
			if !testMQTTConsumerExists(t, s, clientID, dur) {
				t.Fatalf("the group durable %q vanished before the UNSUBSCRIBE", dur)
			}

			ucodes := testMQTTUnsubV5(t, c, r, 2, []string{filter})
			if len(ucodes) != 1 || ucodes[0] != 0 {
				t.Fatalf("UNSUBACK %v, want a single success (0x00): a refusal withholds a "+
					"deletion, it does not fail the packet", ucodes)
			}

			if tc.wantDeleted {
				checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
					if testMQTTConsumerExists(t, s, clientID, dur) {
						return fmt.Errorf("the group durable %q is still present after its last holder left", dur)
					}
					return nil
				})
				return
			}
			// The release is attempted before the UNSUBACK, so the durable is already spared
			// by the time the packet is read; the window guards against a delayed delete
			// reporting a pass that a moment later would not hold.
			deadline := time.Now().Add(500 * time.Millisecond)
			for {
				if !testMQTTConsumerExists(t, s, clientID, dur) {
					t.Fatalf("the group durable %q was deleted although the scan could not "+
						"read the record set", dur)
				}
				if time.Now().After(deadline) {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}
