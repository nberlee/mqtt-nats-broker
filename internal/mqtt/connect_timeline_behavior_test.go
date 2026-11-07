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
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A CONNECT's timeline names the step it spent its time in. These tests delay one step through
// an existing seam and read the line the reporter renders at the function's return (the report
// hook), so they wait for the whole CONNECT, cleanup included, not for the CONNACK.

type testConnectReport struct {
	line string
	slow bool
}

// testCaptureConnectReport returns the reports of every CONNECT from clientID. The hook is
// process-wide; other tests' CONNECTs are filtered out by client ID.
func testCaptureConnectReport(t *testing.T, clientID string) <-chan testConnectReport {
	t.Helper()
	got := make(chan testConnectReport, 8)
	h := func(cid, line string, slow bool) {
		if cid == clientID {
			got <- testConnectReport{line, slow}
		}
	}
	prev := mqttTestConnectReportHookV.Load()
	mqttTestConnectReportHookV.Store(&h)
	t.Cleanup(func() { mqttTestConnectReportHookV.Store(prev) })
	return got
}

func testAwaitConnectReport(t *testing.T, got <-chan testConnectReport) testConnectReport {
	t.Helper()
	select {
	case r := <-got:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("the CONNECT's timeline was never reported")
		return testConnectReport{}
	}
}

var testConnectSpanRE = regexp.MustCompile(`(?:; |, )([a-z#:0-9-]+) ([0-9.]+)ms`)

// testConnectSpans parses a report into its span names, in order, and their durations.
func testConnectSpans(line string) ([]string, map[string]time.Duration) {
	var names []string
	durs := map[string]time.Duration{}
	for _, m := range testConnectSpanRE.FindAllStringSubmatch(line, -1) {
		ms, _ := strconv.ParseFloat(m[2], 64)
		names = append(names, m[1])
		durs[m[1]] = time.Duration(ms * float64(time.Millisecond))
	}
	return names, durs
}

// The steps of an accepted CONNECT that resumes nothing, and the cleanup after it.
var testFreshConnectSpans = []string{"admit", "authorize", "account", "gate", "session", "ownership",
	"tenure", "bind", "resume", "finish", "after-connack", "cleanup:accept-tx", "cleanup:will",
	"cleanup:gate-release", "cleanup:conn-tail", "cleanup:certify", "cleanup:abandon"}

func TestSlowConnectNamesTheOwnershipStep(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "timeline-ownership"
	const pause = 300 * time.Millisecond
	got := testCaptureConnectReport(t, id)

	var paused bool
	asm.mu.Lock()
	asm.testOwnershipStepEntry = func(mqttConnectOwnershipAction) error {
		if !paused {
			paused = true
			time.Sleep(pause)
		}
		return nil
	}
	asm.mu.Unlock()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNECT answered %#x", rc)
	}
	rep := testAwaitConnectReport(t, got)
	if !rep.slow {
		t.Fatalf("a CONNECT paused %v in ownership was not reported slow: %s", pause, rep.line)
	}
	names, durs := testConnectSpans(rep.line)
	if strings.Join(names, ",") != strings.Join(testFreshConnectSpans, ",") {
		t.Fatalf("spans = %v\nwant    %v\nline: %s", names, testFreshConnectSpans, rep.line)
	}
	if durs["ownership"] < pause {
		t.Fatalf("ownership = %v, want at least the %v pause inside it: %s", durs["ownership"], pause, rep.line)
	}
	if !strings.HasPrefix(rep.line, `CONNECT from client "`+id+`": success CONNACK after `) {
		t.Fatalf("report does not lead with the success CONNACK: %s", rep.line)
	}
}

func TestSlowConnectNamesTheTenureStep(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	const id = "timeline-tenure"
	const pause = 300 * time.Millisecond

	// The predecessor of the handoff case: a persistent session, closed, so the clean-start
	// CONNECT below displaces it through the handoff.
	c0, r0 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r0)
	_, _ = c0.Write([]byte{mqttPacketDisconnect, 0})
	c0.Close()
	time.Sleep(200 * time.Millisecond)

	got := testCaptureConnectReport(t, id)
	pauseOnce := make(chan struct{}, 1)
	pauseOnce <- struct{}{}
	h := func(point string) {
		if point == "resume:after-install" {
			select {
			case <-pauseOnce:
				time.Sleep(pause)
			default:
			}
		}
	}
	prev := mqttTestSharedPauseHookV.Load()
	mqttTestSharedPauseHookV.Store(&h)
	t.Cleanup(func() { mqttTestSharedPauseHookV.Store(prev) })

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNECT answered %#x", rc)
	}
	rep := testAwaitConnectReport(t, got)
	names, durs := testConnectSpans(rep.line)
	if durs["tenure"] < pause {
		t.Fatalf("tenure = %v, want at least the %v pause inside it: %s", durs["tenure"], pause, rep.line)
	}
	for _, other := range []string{"ownership", "predecessor", "bind"} {
		if durs[other] >= pause {
			t.Fatalf("the tenure pause was charged to %s too (%v): %s", other, durs[other], rep.line)
		}
	}
	has := map[string]bool{}
	for _, n := range names {
		has[n] = true
	}
	if !has["predecessor"] {
		t.Fatalf("a clean-start handoff over a persistent predecessor shows no predecessor step: %s", rep.line)
	}
}

func TestSlowRefusedConnectNamesTheFailedStepAndItsCleanup(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	asm := testMQTTGetGlobalASM(t, s)
	const id = "timeline-refused"
	const pause = 300 * time.Millisecond
	got := testCaptureConnectReport(t, id)

	refused := errors.New("test: the ownership step is refused")
	asm.mu.Lock()
	asm.testOwnershipStepEntry = func(mqttConnectOwnershipAction) error { return refused }
	asm.testCertified = func(c *client, _ *mqttCloseCert) {
		if c.mqtt.cid == id {
			time.Sleep(pause)
		}
	}
	asm.mu.Unlock()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	// The refusal may answer with a failure CONNACK or close without one. Either way the report
	// below decides acceptance: it says whether a success CONNACK was written.
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, _ = r.reader.Read(make([]byte, 1))
	rep := testAwaitConnectReport(t, got)
	if !rep.slow {
		t.Fatalf("a refusal whose certification took %v was not reported slow: %s", pause, rep.line)
	}
	if !strings.Contains(rep.line, "no success CONNACK") {
		t.Fatalf("a refused CONNECT is reported as acknowledged: %s", rep.line)
	}
	_, durs := testConnectSpans(rep.line)
	if durs["cleanup:certify"] < pause {
		t.Fatalf("cleanup:certify = %v, want at least the %v pause inside it: %s", durs["cleanup:certify"], pause, rep.line)
	}
	if !strings.Contains(rep.line, "(failed here)") || !regexp.MustCompile(`ownership [0-9.]+ms \(failed here\)`).MatchString(rep.line) {
		t.Fatalf("the ownership step is not named as the one the CONNECT failed in: %s", rep.line)
	}
	if durs["ownership"] >= pause {
		t.Fatalf("the certification's delay was charged to ownership (%v): %s", durs["ownership"], rep.line)
	}
	if !strings.Contains(rep.line, refused.Error()) {
		t.Fatalf("the report does not carry the returned error: %s", rep.line)
	}
}

// A resumed session with a subscription runs the phase-subscription sweep as deferred cleanup.
func TestResumedConnectNamesItsPhaseSubscriptionCleanup(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	const id = "timeline-resume"

	c0, r0 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r0)
	testMQTTSubV5(t, c0, r0, 1, []mqttV5SubFilter{{topic: "timeline/topic", opts: 1}})
	_, _ = c0.Write([]byte{mqttPacketDisconnect, 0})
	c0.Close()
	time.Sleep(200 * time.Millisecond)

	got := testCaptureConnectReport(t, id)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: false, props: mqttV5ConnPropsSessionExpiry(60)},
		o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("resume answered rc=%#x present=%v", rc, present)
	}
	rep := testAwaitConnectReport(t, got)
	names, _ := testConnectSpans(rep.line)
	if !strings.Contains(","+strings.Join(names, ",")+",", ",cleanup:phase-subs,") {
		t.Fatalf("a resumed session with a subscription shows no phase-subscription cleanup: %s", rep.line)
	}
}

// The checks and writes after the subscriptions are restored — here a shared group's membership
// ownership CAS — are the finish step, not the resume.
func TestSlowSharedResumeNamesItsFinishStep(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	const id = "timeline-shared-finish"
	const pause = 300 * time.Millisecond

	c0, r0 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r0)
	testMQTTSubV5(t, c0, r0, 1, []mqttV5SubFilter{{topic: "$share/g/timeline/shared", opts: 1}})
	_, _ = c0.Write([]byte{mqttPacketDisconnect, 0})
	c0.Close()
	time.Sleep(200 * time.Millisecond)

	got := testCaptureConnectReport(t, id)
	pauseOnce := make(chan struct{}, 1)
	pauseOnce <- struct{}{}
	h := func(boundary string) {
		if boundary == "after-ownership-cas" {
			select {
			case <-pauseOnce:
				time.Sleep(pause)
			default:
			}
		}
	}
	prev := mqttTestSharedArbitratePauseV.Load()
	mqttTestSharedArbitratePauseV.Store(&h)
	t.Cleanup(func() { mqttTestSharedArbitratePauseV.Store(prev) })

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: false, props: mqttV5ConnPropsSessionExpiry(60)},
		o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("resume answered rc=%#x present=%v", rc, present)
	}
	rep := testAwaitConnectReport(t, got)
	names, durs := testConnectSpans(rep.line)
	if durs["finish"] < pause {
		t.Fatalf("finish = %v, want at least the %v pause after the ownership CAS: %s", durs["finish"], pause, rep.line)
	}
	if durs["resume"] >= pause {
		t.Fatalf("the post-restore pause was charged to resume (%v): %s", durs["resume"], rep.line)
	}
	n := 0
	for _, name := range names {
		if strings.HasPrefix(name, "finish") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d finish spans, want one: %s", n, rep.line)
	}
}
