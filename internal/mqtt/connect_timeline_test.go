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
	"errors"
	"testing"
	"time"
)

// testTimelineClock is a clock a test advances by hand.
type testTimelineClock struct{ t time.Time }

func (c *testTimelineClock) now() time.Time { return c.t }
func (c *testTimelineClock) advance(ms int) { c.t = c.t.Add(time.Duration(ms) * time.Millisecond) }

func TestConnectTimelineAttributesEachStep(t *testing.T) {
	clk := &testTimelineClock{t: time.Unix(1000, 0)}
	tl := newMQTTConnectTimeline(clk.now)
	clk.advance(1)
	tl.begin("authorize")
	clk.advance(2)
	tl.begin("session")
	clk.advance(3)
	tl.begin("ownership")
	clk.advance(40)
	tl.begin("session") // a re-drive
	clk.advance(5)
	tl.begin("ownership")
	clk.advance(200)
	tl.connAck()
	clk.advance(7)
	tl.cleanup("accept-tx")
	clk.advance(1)

	line, slow := tl.report(`a "quoted" id`, nil, 250*time.Millisecond)
	want := `CONNECT from client "a \"quoted\" id": success CONNACK after 251.0ms; admit 1.0ms, authorize 2.0ms, ` +
		`session 3.0ms, ownership 40.0ms, session#2 5.0ms, ownership#2 200.0ms, after-connack 7.0ms, cleanup:accept-tx 1.0ms`
	if line != want {
		t.Fatalf("report =\n %s\nwant\n %s", line, want)
	}
	if !slow {
		t.Fatal("251 ms to CONNACK at a 250 ms threshold was not reported slow")
	}
}

func TestConnectTimelineThresholdIsOnTheConnAck(t *testing.T) {
	for _, c := range []struct {
		toConnAck, after int
		slow             bool
	}{
		{249, 0, false},
		{250, 0, true},
		{100, 500, false}, // a slow tail after a prompt CONNACK is not a slow CONNECT
	} {
		clk := &testTimelineClock{t: time.Unix(1000, 0)}
		tl := newMQTTConnectTimeline(clk.now)
		clk.advance(c.toConnAck)
		tl.connAck()
		clk.advance(c.after)
		if _, slow := tl.report("id", nil, 250*time.Millisecond); slow != c.slow {
			t.Errorf("CONNACK after %d ms then %d ms: slow = %v, want %v", c.toConnAck, c.after, slow, c.slow)
		}
	}
}

func TestConnectTimelineNamesTheFailedStepAndItsCleanup(t *testing.T) {
	clk := &testTimelineClock{t: time.Unix(1000, 0)}
	tl := newMQTTConnectTimeline(clk.now)
	tl.begin("authorize")
	clk.advance(2)
	tl.begin("ownership")
	clk.advance(10)
	tl.cleanup("abandon") // the body returned inside ownership
	clk.advance(300)
	tl.cleanup("certify")
	clk.advance(4)
	tl.begin("resume") // ignored: the body has returned

	line, slow := tl.report("refused", errors.New(`budget "spent"`), 250*time.Millisecond)
	want := `CONNECT from client "refused": no success CONNACK, returned after 316.0ms; admit 0.0ms, authorize 2.0ms, ` +
		`ownership 10.0ms (failed here), cleanup:abandon 300.0ms, cleanup:certify 4.0ms; error: "budget \"spent\""`
	if line != want {
		t.Fatalf("report =\n %s\nwant\n %s", line, want)
	}
	if !slow {
		t.Fatal("a refusal 316 ms after entry was not reported slow")
	}
}

func TestConnectTimelineReturnWithoutCleanupMarksTheLastStep(t *testing.T) {
	clk := &testTimelineClock{t: time.Unix(1000, 0)}
	tl := newMQTTConnectTimeline(clk.now)
	tl.begin("authorize")
	clk.advance(3)
	line, _ := tl.report("id", errors.New("refused"), 250*time.Millisecond)
	want := `CONNECT from client "id": no success CONNACK, returned after 3.0ms; admit 0.0ms, authorize 3.0ms (failed here); error: "refused"`
	if line != want {
		t.Fatalf("report =\n %s\nwant\n %s", line, want)
	}
}

func TestConnectTimelineBeginUnlessOpenKeepsOneSpan(t *testing.T) {
	clk := &testTimelineClock{t: time.Unix(1000, 0)}
	tl := newMQTTConnectTimeline(clk.now)
	tl.begin("resume")
	clk.advance(2)
	tl.begin("finish") // started early, by a resume
	clk.advance(5)
	tl.beginUnlessOpen("finish") // the later boundary: the step continues
	clk.advance(3)
	tl.connAck()
	line, _ := tl.report("id", nil, time.Second)
	want := `CONNECT from client "id": success CONNACK after 10.0ms; admit 0.0ms, resume 2.0ms, finish 8.0ms, after-connack 0.0ms`
	if line != want {
		t.Fatalf("report =\n %s\nwant\n %s", line, want)
	}
}
