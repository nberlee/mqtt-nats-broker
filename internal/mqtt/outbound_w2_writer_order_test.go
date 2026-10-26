//go:build mqtt_fulltests

// Copyright 2026 The mqtt-nats-broker Authors
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
	"fmt"
	"testing"
	"time"
)

// The writer's half of the output gate (F-105 WP-F W2, R1 third).
//
// Certification seals the gate and then crosses wmu, and that pairing only means "no write is in
// progress" if every writer reads the gate UNDER wmu. A writer that read it before taking the lock
// could pass the check, queue on a lock a sealer is about to cross, and write after the seal.
// These fixtures put a writer exactly there - past every check that precedes the lock, queued on
// it - and only then seal. The seams are process-wide, so nothing here runs in parallel.

// testWriterPreLockSeam installs the pre-lock seam for one client: each writer on c reports that it
// has reached the lock, then waits for proceed (closed by the caller, or nil to run straight on).
func testWriterPreLockSeam(t *testing.T, c *client, proceed <-chan struct{}) <-chan struct{} {
	t.Helper()
	reached := make(chan struct{}, 4)
	h := func(w *client) {
		if w != c {
			return
		}
		reached <- struct{}{}
		if proceed != nil {
			<-proceed
		}
	}
	mqttTestWriterPreLockHookV.Store(&h)
	t.Cleanup(func() { mqttTestWriterPreLockHookV.Store(nil) })
	return reached
}

func testAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never happened", what)
	}
}

// TestF105AWriterQueuedOnTheLockHonoursALaterSeal pins that a seal applied while a writer is
// queued on wmu reaches that writer: the gate is read once the lock is held, never before it.
func TestF105AWriterQueuedOnTheLockHonoursALaterSeal(t *testing.T) {
	t.Run("writeAuthorizedFrame", func(t *testing.T) {
		c, tc, bind := testFrameClient(t, 0)
		reached := testWriterPreLockSeam(t, c, nil)

		c.wmu.Lock()
		type outcome struct {
			res mqttOutputResult
			err error
		}
		done := make(chan outcome, 1)
		go func() {
			res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: testAdmit(c, testFrameAction(bind, 1)),
				Action: testFrameAction(bind, 1), Binding: bind,
				Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
			})
			done <- outcome{res, err}
		}()
		// The writer has passed every check that precedes the lock and is about to block on it.
		testAwait(t, reached, "the frame writer reaching the write lock")
		sealOutput(bind)
		c.wmu.Unlock()

		var got outcome
		select {
		case got = <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("the queued frame writer never returned")
		}
		// THE SEAL CAME AFTER THE WRITER'S PRE-LOCK CHECKS AND BEFORE ITS LOCK, so it is the
		// under-lock read that must refuse. Prevented, not failed: nothing was emitted.
		if got.res != mqttOutputPrevented || got.err != nil {
			t.Fatalf("a writer queued on wmu across a seal gave %v %v, want prevented with no error",
				got.res, got.err)
		}
		if len(tc.written) != 0 {
			t.Fatalf("a writer queued on wmu across a seal put %q on the wire", string(tc.written))
		}
	})

	t.Run("writeSocket", func(t *testing.T) {
		c, tc, bind := testFrameClient(t, 0)
		reached := testWriterPreLockSeam(t, c, nil)

		c.wmu.Lock()
		done := make(chan error, 1)
		go func() { done <- c.writeSocket([]byte("must not be written"), time.Time{}) }()
		testAwait(t, reached, "the socket writer reaching the write lock")
		sealOutput(bind)
		c.wmu.Unlock()

		select {
		case err := <-done:
			// The actionless writer's refusal shape: the write simply does not happen.
			if err != nil {
				t.Fatalf("a socket writer queued on wmu across a seal reported %v rather than "+
					"not writing", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("the queued socket writer never returned")
		}
		if len(tc.written) != 0 {
			t.Fatalf("a socket writer queued on wmu across a seal put %q on the wire",
				string(tc.written))
		}
	})
}

// TestF105ATornWriterSealsBeforeAnyWriterCanFollow pins that a frame torn mid-write seals the
// gate BEFORE it releases wmu. The release is the first moment a queued writer can take the lock;
// a seal that came after it would let that writer begin a second frame on a transport already
// carrying half of one.
func TestF105ATornWriterSealsBeforeAnyWriterCanFollow(t *testing.T) {
	for _, follower := range []string{"writeAuthorizedFrame", "writeSocket"} {
		t.Run(follower, func(t *testing.T) {
			// The transport accepts the 3-byte header and refuses the payload: a torn frame.
			c, tc, bind := testFrameClient(t, 3)
			proceed := make(chan struct{})
			followerDone := make(chan string, 1)
			var reached <-chan struct{}

			// While the torn writer holds wmu (inside its first transport write), queue the
			// follower on the lock: it reports from the pre-lock seam, past every pre-lock
			// check, and waits there until the torn writer has released wmu.
			first := true
			tc.onWrite = func() {
				if !first {
					return
				}
				first = false
				reached = testWriterPreLockSeam(t, c, proceed)
				go func() {
					if follower == "writeSocket" {
						if err := c.writeSocket([]byte("FOLLOWER"), time.Time{}); err != nil {
							followerDone <- "error: " + err.Error()
							return
						}
						followerDone <- "nil"
						return
					}
					res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: testAdmit(c, testFrameAction(bind, 2)),
						Action: testFrameAction(bind, 2), Binding: bind,
						Parts: [][]byte{[]byte("FOLLOWER")},
					})
					if res == mqttOutputPrevented && err == nil {
						followerDone <- "prevented"
						return
					}
					followerDone <- fmt.Sprintf("not prevented: %v %v", res, err)
				}()
				testAwait(t, reached, "the follower reaching the write lock")
			}

			// In the interval between the torn writer's release of wmu and anything after it,
			// let the follower run to completion. The transport is made writable again first,
			// so a follower that wrote would show its bytes rather than fail on the torn socket.
			followerRan := false
			var followerOutcome string
			unlocked := func(w *client) {
				if w != c || followerRan {
					return
				}
				followerRan = true
				tc.failAt = 0
				close(proceed)
				select {
				case followerOutcome = <-followerDone:
				case <-time.After(5 * time.Second):
					followerOutcome = "never returned"
				}
			}
			mqttTestTornWriterUnlockedHookV.Store(&unlocked)
			t.Cleanup(func() { mqttTestTornWriterUnlockedHookV.Store(nil) })

			res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: testAdmit(c, testFrameAction(bind, 1)),
				Action: testFrameAction(bind, 1), Binding: bind,
				Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
			})
			if res != mqttOutputFailed || err == nil {
				t.Fatalf("the torn frame gave %v %v, want failed with an error", res, err)
			}
			if !followerRan {
				t.Fatalf("the torn writer never reached its unlock seam with a follower queued")
			}
			want := "prevented"
			if follower == "writeSocket" {
				want = "nil"
			}
			// THE FOLLOWER TOOK wmu AFTER THE TORN WRITER RELEASED IT, so the seal must already
			// have been in place: it was prevented and the wire carries only the torn header.
			if followerOutcome != want {
				t.Fatalf("a writer following a torn frame gave %q, want %q", followerOutcome, want)
			}
			if string(tc.written) != "HDR" {
				t.Fatalf("a writer following a torn frame reached the wire: %q, want only the "+
					"torn header", string(tc.written))
			}
		})
	}
}
