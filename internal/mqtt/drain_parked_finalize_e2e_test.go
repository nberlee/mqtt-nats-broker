//go:build mqtt_fulltests

// Copyright 2025 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"sync/atomic"
	"testing"
	"time"
)

// A DRAINED SOURCE'S FINALIZE WAITS FOR ITS SECTION (F-098). The client can PUBACK a drained
// redelivery while the section that wrote it is still open. The register delete is parked on
// that section already; the drain's finalize releases the same register, so it is parked too.
// Run early, it would first PERSIST a release request for a register the writer still owns —
// a durable obligation known to be doomed, which the live-action veto then refuses.
//
// Deliberately NOT parallel: the send-window hook is process-global.
// Mutation M108: drop the parked arm, so the finalize runs at the PUBACK.
func TestMQTTDrainFinalizeIsParkedOnTheOpenSection(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "parked", "parked/topic", 1)
	defer conn.c.Close()
	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "parked/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// Hold the drain's REDELIVERY section open after its write.
	held := make(chan struct{}, 1)
	release := make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	restore := setMQTTTestMarkerSendWindowHook(func(hs *mqttSession, hpi uint16, window string) bool {
		if hs == sess && hpi == pi && window == mqttMarkerSendWrote && armed.CompareAndSwap(true, false) {
			held <- struct{}{}
			<-release
		}
		return true
	})
	t.Cleanup(restore)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	flags, gotPI, _, _ := testMQTTReadPubPacket(t, conn.r)
	if gotPI != pi || flags&mqttPubFlagDup == 0 {
		t.Fatalf("want the drain's DUP redelivery under %d, got pi=%d flags=%x", pi, gotPI, flags)
	}
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the redelivery's section never reached the post-write window")
	}

	// The client answers while the section is still open.
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	time.Sleep(300 * time.Millisecond)
	sess.mu.Lock()
	var rel *mqttPersistedDrainRelease
	claimed := false
	for _, d := range sess.drains {
		for _, st := range d.seqs {
			if st != nil && st.pi == pi {
				rel, claimed = st.rel, st.claimed
			}
		}
	}
	sess.mu.Unlock()
	if rel != nil {
		t.Fatalf("a release request was persisted for a register the open section still owns: %+v", rel)
	}
	if !claimed {
		t.Fatal("the claim must stand until the section's settlement finalizes it")
	}

	close(release)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return errDrainTestStillDraining
		}
		return nil
	})
}
