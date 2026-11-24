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
	"testing"
	"time"
)

// The account assistant's unanswered sequence advances at every origin of a request JetStream did
// not answer, exactly once per observation, and at nothing else (sub_jetstream_unanswered.go).

func testUnansweredAssistant(t *testing.T) *mqttAccountSessionManager {
	t.Helper()
	ns := runJetStreamServerHA(t)
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = 500 * time.Millisecond })
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatal(err)
	}
	return asm
}

// testDeliverJSAPIReply hands processJSAPIReplies one reply of kind for a fresh registered waiter,
// with hdr bytes of msg as its header block, and returns what the waiter received.
func testDeliverJSAPIReply(t *testing.T, asm *mqttAccountSessionManager, kind string, msg []byte, hdr int) *mqttJSAResponse {
	t.Helper()
	reply := asm.jsa.rplyr + kind + "." + fmt.Sprint(time.Now().UnixNano())
	ch := make(chan *mqttJSAResponse, 1)
	asm.jsa.replies.Store(reply, ch)
	pc := &client{}
	pc.pa.hdr = hdr
	asm.processJSAPIReplies(nil, pc, nil, reply, _EMPTY_, msg)
	select {
	case r := <-ch:
		return r
	default:
		t.Fatalf("no reply was delivered for %q", reply)
		return nil
	}
}

func TestUnansweredObservationOrigins(t *testing.T) {
	asm := testUnansweredAssistant(t)
	jsa := &asm.jsa
	advanced := func(name string, want bool, f func()) {
		t.Helper()
		mark := jsa.unansweredMark()
		f()
		if got := jsa.unansweredMark() - mark; (got == 1) != want || got > 1 {
			t.Errorf("%s: the sequence advanced by %d, want %v", name, got, map[bool]int{false: 0, true: 1}[want])
		}
	}

	advanced("a dropped request's timeout", true, func() {
		drop := func(subject string, _ []byte) bool { return strings.HasSuffix(subject, ".issue187-nobody") }
		jsa.testDropRequest.Store(&drop)
		defer jsa.testDropRequest.Store(nil)
		if _, err := jsa.lookupConsumer(mqttStreamName, "issue187-nobody"); !errors.Is(err, errMQTTJSARequestTimeout) {
			t.Errorf("the dropped request was answered: %v", err)
		}
	})
	advanced("a wait cut by its view's ownership deadline", false, func() {
		drop := func(subject string, _ []byte) bool { return strings.HasSuffix(subject, ".issue187-nobody") }
		jsa.testDropRequest.Store(&drop)
		defer jsa.testDropRequest.Store(nil)
		view := jsa.withDeadline(time.Now().Add(100 * time.Millisecond))
		if _, err := view.lookupConsumer(mqttStreamName, "issue187-nobody"); !errors.Is(err, errMQTTOwnershipDeadline) {
			t.Errorf("the wait was not cut by the deadline: %v", err)
		}
	})
	advanced("a double-ack confirmation that never arrives", true, func() {
		if err := jsa.prepareAckConfirm("issue187.nobody").wait(); err == nil {
			t.Error("an unsent confirmation was confirmed")
		}
	})
	noResponders := []byte("NATS/1.0 503\r\nStatus: 503\r\n\r\n")
	advanced("a no-responders status", true, func() {
		testDeliverJSAPIReply(t, asm, mqttJSAConsumerLookup, noResponders, len(noResponders))
	})
	advanced("a no-responders status on a double-ack confirmation", true, func() {
		testDeliverJSAPIReply(t, asm, mqttJSAAckConfirm, noResponders, len(noResponders))
	})
	for _, ae := range []ApiError{{Code: 503, ErrCode: 10008}, {Code: 500, ErrCode: 10118}, {Code: 500, ErrCode: 10119}} {
		body := fmt.Appendf(nil, `{"type":"io.nats.jetstream.api.v1.consumer_info_response","error":{"code":%d,"err_code":%d,"description":"offline"}}`, ae.Code, ae.ErrCode)
		advanced(fmt.Sprintf("a decoded unavailable answer %d", ae.ErrCode), true, func() {
			testDeliverJSAPIReply(t, asm, mqttJSAConsumerLookup, body, 0)
		})
	}
	advanced("a decoded definite answer", false, func() {
		body := []byte(`{"type":"io.nats.jetstream.api.v1.consumer_info_response","error":{"code":404,"err_code":10014,"description":"consumer not found"}}`)
		testDeliverJSAPIReply(t, asm, mqttJSAConsumerLookup, body, 0)
	})
	advanced("an answered request", false, func() {
		if _, err := jsa.lookupStream(mqttStreamName); err != nil {
			t.Errorf("the stream lookup failed: %v", err)
		}
	})
	advanced("a request the service connection refused to publish for its size", true, func() {
		if _, err := jsa.storeMsg(mqttSessStreamSubjectPrefix+"issue187-oversized", 0, make([]byte, 2<<20)); err == nil {
			t.Error("an oversized store was accepted")
		}
	})
}

func TestJetStreamAPIUnavailableClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		ae   *ApiError
		want bool
	}{
		{"no responders", &ApiError{Code: 503}, true},
		{"a 503 carrying a JetStream code", &ApiError{Code: 503, ErrCode: 10039}, false},
		{"cluster unavailable", &ApiError{Code: 503, ErrCode: 10008}, true},
		{"stream offline", &ApiError{Code: 500, ErrCode: 10118}, true},
		{"consumer offline", &ApiError{Code: 500, ErrCode: 10119}, true},
		{"stream offline with a reason", &ApiError{Code: 500, ErrCode: 10194}, false},
		{"consumer offline with a reason", &ApiError{Code: 500, ErrCode: 10195}, false},
		{"consumer already exists", &ApiError{Code: 400, ErrCode: 10148}, false},
		{"another transport status", &ApiError{Code: 408}, false},
	} {
		if got := mqttJSAPIUnavailable(tc.ae); got != tc.want {
			t.Errorf("%s: unavailable = %v, want %v", tc.name, got, tc.want)
		}
	}
}
