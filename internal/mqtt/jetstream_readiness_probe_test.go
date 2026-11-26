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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The readiness probe (jetstream_readiness_probe.go) succeeds only on two answers, and names the
// check that was not answered.

func testReadinessProbeServer(t *testing.T) (*Server, *natsserver.Server) {
	t.Helper()
	ns := runJetStreamServerHA(t)
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = 500 * time.Millisecond })
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, ns
}

func testRequireProbeFailure(t *testing.T, err error, check ReadinessCheck) {
	t.Helper()
	var pe *ReadinessProbeError
	if !errors.As(err, &pe) {
		t.Fatalf("probe = %v, want a failed %s", err, check)
	}
	if pe.Check != check {
		t.Fatalf("probe failed on %q (%v), want %q", pe.Check, pe.Err, check)
	}
}

func TestProbeJetStreamSucceedsOnAServingAccount(t *testing.T) {
	s, _ := testReadinessProbeServer(t)
	if err := s.ProbeJetStream(); err != nil {
		t.Fatalf("probe of a serving account = %v", err)
	}
}

func TestProbeJetStreamFailsBeforeTheAccountIsInitialized(t *testing.T) {
	ns := runJetStreamServerHA(t)
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
	testRequireProbeFailure(t, s.ProbeJetStream(), ReadinessCheckAccountInfo)
}

// TestProbeJetStreamFailsWhenTheSessionStreamIsMissing: a definite refusal is not an answer.
func TestProbeJetStreamFailsWhenTheSessionStreamIsMissing(t *testing.T) {
	s, ns := testReadinessProbeServer(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := js.DeleteStream(context.Background(), mqttSessStreamName); err != nil {
		t.Fatal(err)
	}
	testRequireProbeFailure(t, s.ProbeJetStream(), ReadinessCheckSessionStreamInfo)
}

// TestProbeJetStreamFailsOnADefiniteAccountInfoRefusal: an account without JetStream answers
// account info with a refusal that is not "unavailable"; it still fails the probe, and it does not
// count as a request JetStream left unanswered.
func TestProbeJetStreamFailsOnADefiniteAccountInfoRefusal(t *testing.T) {
	s, ns := testReadinessProbeServer(t)
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatal(err)
	}
	if err := ns.GlobalAccount().DisableJetStream(); err != nil {
		t.Fatal(err)
	}
	mark := asm.jsa.unansweredMark()
	err = s.ProbeJetStream()
	testRequireProbeFailure(t, err, ReadinessCheckAccountInfo)
	if errors.Is(err, errMQTTJSARequestTimeout) {
		t.Fatalf("the refusal was a timeout, not an answer: %v", err)
	}
	if asm.jsa.unansweredSince(mark) {
		t.Errorf("a definite refusal advanced the unanswered sequence: %v", err)
	}
}

// TestProbeJetStreamFailsOnAnUnansweredRequest: each of the two requests, left unanswered, fails
// the probe on its own check.
func TestProbeJetStreamFailsOnAnUnansweredRequest(t *testing.T) {
	s, _ := testReadinessProbeServer(t)
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		subject string
		check   ReadinessCheck
	}{
		{JSApiAccountInfo, ReadinessCheckAccountInfo},
		{fmt.Sprintf(JSApiStreamInfoT, mqttSessStreamName), ReadinessCheckSessionStreamInfo},
	} {
		t.Run(string(tc.check), func(t *testing.T) {
			drop := func(subject string, _ []byte) bool { return subject == tc.subject }
			asm.jsa.testDropRequest.Store(&drop)
			defer asm.jsa.testDropRequest.Store(nil)
			mark := asm.jsa.unansweredMark()
			err := s.ProbeJetStream()
			testRequireProbeFailure(t, err, tc.check)
			if !errors.Is(err, errMQTTJSARequestTimeout) || !strings.Contains(err.Error(), tc.subject) {
				t.Errorf("probe = %v, want the timeout of %s", err, tc.subject)
			}
			if !asm.jsa.unansweredSince(mark) {
				t.Error("an unanswered probe request did not advance the unanswered sequence")
			}
		})
	}
}

// TestReadinessNeedsTheStreamLeadersAnswer: a member of a leaderless group answers the session
// stream's info from its own state, with no leader named; that is not an answer the probe takes.
func TestReadinessNeedsTheStreamLeadersAnswer(t *testing.T) {
	for _, tc := range []struct {
		name string
		si   *StreamInfo
		ok   bool
	}{
		{"a stream whose leader is named", &StreamInfo{Cluster: &natsserver.ClusterInfo{Leader: "node-1"}}, true},
		{"a peer of a leaderless stream", &StreamInfo{Cluster: &natsserver.ClusterInfo{}}, false},
		{"an answer without a cluster block", &StreamInfo{}, false},
		{"no stream info", nil, false},
	} {
		if err := mqttReadinessLeaderAnswered(tc.si); (err == nil) != tc.ok {
			t.Errorf("%s: %v, want ok=%t", tc.name, err, tc.ok)
		}
	}
}

// TestProbeJetStreamRefusesALeaderlessSessionStreamInfo: the probe itself, given the session
// stream's real info rewritten as a leaderless group member's answer, fails on that check.
func TestProbeJetStreamRefusesALeaderlessSessionStreamInfo(t *testing.T) {
	s, _ := testReadinessProbeServer(t)
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatal(err)
	}
	// Background work may ask for the same info, so the probe's own reply cannot be told apart:
	// while the probe runs, every session stream info reply is rewritten, as it arrives and before
	// it is delivered, as a leaderless group member's answer.
	sessInfo := fmt.Sprintf(JSApiStreamInfoT, mqttSessStreamName)
	var mu sync.Mutex
	sent := map[string]string{}
	rewrite := func(ev mqttJSARequestEvent) {
		mu.Lock()
		defer mu.Unlock()
		switch ev.Kind {
		case mqttJSARequestSent:
			sent[ev.Reply] = ev.Subject
		case mqttJSAReplyArrived:
			if sent[ev.Reply] != sessInfo {
				return
			}
			if r, ok := ev.Value.(*JSApiStreamInfoResponse); ok && r.StreamInfo != nil {
				r.Cluster = &natsserver.ClusterInfo{Name: "c"}
			}
		}
	}
	asm.jsa.testObserveRequest.Store(&rewrite)
	defer asm.jsa.testObserveRequest.Store(nil)
	err = s.ProbeJetStream()
	testRequireProbeFailure(t, err, ReadinessCheckSessionStreamInfo)
	if !errors.Is(err, errMQTTReadinessNoStreamLeader) {
		t.Errorf("probe = %v, want the leaderless refusal", err)
	}
}
