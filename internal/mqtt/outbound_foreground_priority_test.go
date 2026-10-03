// Copyright 2026 The mqtt-nats-bridge Authors
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
	"testing"
	"time"
)

// TestF105ForegroundAuthorityPriorityRelease (consult 105): every exit of a foreground scope
// releases it exactly once, overlapping scopes keep background waiting until the LAST release, and
// shutdown wakes a waiter without granting it an attempt.
func TestF105ForegroundAuthorityPriorityRelease(t *testing.T) {
	newSess := func() (*mqttSession, chan struct{}) {
		quit := make(chan struct{})
		return &mqttSession{jsa: &mqttJSA{c: &client{srv: &Server{quitCh: quit}}}}, quit
	}
	admitted := func(sess *mqttSession) chan bool {
		out := make(chan bool, 1)
		go func() { out <- sess.admitBackgroundAuthority() }()
		return out
	}
	expect := func(t *testing.T, ch chan bool, want bool, within time.Duration) {
		t.Helper()
		select {
		case got := <-ch:
			if got != want {
				t.Fatalf("admission answered %v, want %v", got, want)
			}
		case <-time.After(within):
			t.Fatalf("admission did not answer within %v", within)
		}
	}
	waiting := func(t *testing.T, ch chan bool) {
		t.Helper()
		select {
		case got := <-ch:
			t.Fatalf("admission answered %v while a foreground scope stood", got)
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Run("release is idempotent and returns to idle", func(t *testing.T) {
		sess, _ := newSess()
		release := sess.announceForegroundAuthority()
		release()
		release()
		if sess.foreground.n != 0 || sess.foreground.idle != nil {
			t.Fatalf("after a double release: n=%d idle=%v", sess.foreground.n, sess.foreground.idle)
		}
		expect(t, admitted(sess), true, time.Second)
	})
	t.Run("overlapping scopes", func(t *testing.T) {
		sess, _ := newSess()
		r1 := sess.announceForegroundAuthority()
		r2 := sess.announceForegroundAuthority()
		ch := admitted(sess)
		waiting(t, ch)
		r1()
		waiting(t, ch)
		r2()
		expect(t, ch, true, time.Second)
	})
	t.Run("shutdown wakes a waiter without a grant", func(t *testing.T) {
		sess, quit := newSess()
		granted := false
		h := func() { granted = true }
		sess.testBackgroundAdmit.Store(&h)
		release := sess.announceForegroundAuthority()
		defer release()
		ch := admitted(sess)
		waiting(t, ch)
		close(quit)
		expect(t, ch, false, time.Second)
		if granted {
			t.Fatal("a waiter woken by shutdown was granted an attempt")
		}
	})
	t.Run("the claim's success and early error release", func(t *testing.T) {
		e := testNewInflightEnv(t, "fg-early")
		if _, err := e.sess.claimSourceConsumerName(testServingEntitlement(t, e.sess), "fg.ok", e.sess.idHash+"_fgok"); err != nil {
			t.Fatalf("the fixture's claim was expected to land: %v", err)
		}
		if e.sess.foreground.n != 0 {
			t.Fatalf("the claim's success left %d announcements standing", e.sess.foreground.n)
		}
		// A connection with no accepted entitlement: the claim refuses before it writes anything.
		orphan := &mqttSession{id: "fg-no-authority", idHash: getHash("fg-no-authority"), asm: e.sess.asm, jsa: e.sess.jsa}
		if _, err := orphan.claimSourceConsumerName(mqttOutboundEntitlement{}, "fg.early", "dur"); err == nil {
			t.Fatal("a claim with no entitlement was expected to be refused")
		}
		if orphan.foreground.n != 0 {
			t.Fatalf("the claim's early error left %d announcements standing", orphan.foreground.n)
		}
	})
}
