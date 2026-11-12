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
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// TestMQTTStartupRefusesTheRetiredMetadataStreams covers the preflight's outcome matrix
// for $MQTT_inflight and $MQTT_shared, whose subject spaces now live on $MQTT_sess.
//
// The layout that still carries either is an unsupported persisted shape: refused, never
// read or migrated. Each name is decided on the same (info, error) pair matrix as the
// retired origin register, and the check creates nothing on any arm.
func TestMQTTStartupRefusesTheRetiredMetadataStreams(t *testing.T) {
	t.Parallel()

	notFound := ApiErrors[JSStreamNotFoundErr]
	other := errors.New("no responders")
	present := func(name string) func() (*StreamInfo, error) {
		return func() (*StreamInfo, error) { return &StreamInfo{Config: StreamConfig{Name: name}}, nil }
	}
	absent := func() (*StreamInfo, error) { return nil, notFound }

	for _, tc := range []struct {
		name    string
		replies []func() (*StreamInfo, error)
		refuse  bool
		mention []string
		lookups int
	}{
		{"all absent", []func() (*StreamInfo, error){absent, absent, absent}, false, nil, 3},
		{"in-flight present", []func() (*StreamInfo, error){present(mqttRetiredInflightStreamName), absent, absent},
			true, []string{mqttRetiredInflightStreamName}, 3},
		{"shared present", []func() (*StreamInfo, error){absent, present(mqttRetiredSharedStreamName), absent},
			true, []string{mqttRetiredSharedStreamName}, 3},
		{"teardown present", []func() (*StreamInfo, error){absent, absent, present(mqttRetiredTeardownStreamName)},
			true, []string{mqttRetiredTeardownStreamName}, 3},
		{"both present", []func() (*StreamInfo, error){present(mqttRetiredInflightStreamName), present(mqttRetiredSharedStreamName), absent},
			true, []string{mqttRetiredInflightStreamName, mqttRetiredSharedStreamName}, 3},
		// Info alongside a not-found error is decided by the half that says it EXISTS.
		{"in-flight present with a not-found error", []func() (*StreamInfo, error){
			func() (*StreamInfo, error) {
				return &StreamInfo{Config: StreamConfig{Name: mqttRetiredInflightStreamName}}, notFound
			}, absent, absent,
		}, true, []string{mqttRetiredInflightStreamName}, 3},
		// An undecided lookup fails startup at once: absence is not the default.
		{"in-flight undecided: no info, no error", []func() (*StreamInfo, error){
			func() (*StreamInfo, error) { return nil, nil },
		}, true, []string{mqttRetiredInflightStreamName}, 1},
		{"shared undecided: some other error", []func() (*StreamInfo, error){
			absent, func() (*StreamInfo, error) { return nil, other },
		}, true, []string{mqttRetiredSharedStreamName}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stub := &stubStreamAPI{lookupResults: tc.replies}
			err := mqttRefuseRetiredMetadataStreams(stub)
			if tc.refuse != (err != nil) {
				t.Fatalf("refuse=%v, got %v", tc.refuse, err)
			}
			for _, name := range tc.mention {
				if !strings.Contains(err.Error(), name) {
					t.Fatalf("the refusal must name %s, got %v", name, err)
				}
			}
			if stub.creates != 0 {
				t.Fatalf("the preflight must create nothing, got %d create(s)", stub.creates)
			}
			if stub.lookups != tc.lookups {
				t.Fatalf("the preflight looked up %d time(s), want %d", stub.lookups, tc.lookups)
			}
		})
	}
}

// TestMQTTStartupRefusesALayoutHoldingARetiredMetadataStream is the preflight's
// integration half: a live JetStream holding the retired streams, and the assertion that
// account-manager initialization fails with the retired-name diagnostic before anything is
// created.
//
// The diagnostic is what discriminates. Without the preflight an old layout still fails to
// initialize, but for another reason: the session-stream create is rejected by the
// server's subject-overlap check against a leftover, or an old session stream fails its
// subject-set validator. Neither names the stream to delete, so asserting failure alone
// would pass against a broker that never calls the preflight.
func TestMQTTStartupRefusesALayoutHoldingARetiredMetadataStream(t *testing.T) {
	t.Parallel()

	inflight := jetstream.StreamConfig{
		Name:     mqttRetiredInflightStreamName,
		Subjects: []string{mqttInflightStreamSubjectPrefix + ">"},
		Storage:  jetstream.FileStorage,
	}
	shared := jetstream.StreamConfig{
		Name:     mqttRetiredSharedStreamName,
		Subjects: []string{mqttSharedLifecycleStreamSubjectPrefix + ">"},
		Storage:  jetstream.FileStorage,
	}
	// The session stream as it was before the merge: four subject spaces.
	oldSess := jetstream.StreamConfig{
		Name: mqttSessStreamName,
		Subjects: []string{
			mqttSessStreamSubjectPrefix + ">",
			"$MQTT.accept.>", // the retired acceptance-fact space
			"$MQTT.trcpt.>",  // the retired teardown-receipt space
		},
		Storage: jetstream.FileStorage,
	}

	for _, tc := range []struct {
		name    string
		plant   []jetstream.StreamConfig
		retired []string
	}{
		{"a fresh account with a leftover in-flight stream", []jetstream.StreamConfig{inflight},
			[]string{mqttRetiredInflightStreamName}},
		{"a fresh account with a leftover shared stream", []jetstream.StreamConfig{shared},
			[]string{mqttRetiredSharedStreamName}},
		{"the layout before the merge", []jetstream.StreamConfig{oldSess, inflight, shared},
			[]string{mqttRetiredInflightStreamName, mqttRetiredSharedStreamName}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, c, nc := newBringUpServer(t)
			js, ctx := newJS(t, nc)
			for _, cfg := range tc.plant {
				if _, err := js.CreateStream(ctx, cfg); err != nil {
					t.Fatalf("planting %s: %v", cfg.Name, err)
				}
			}
			_, err := s.getOrCreateMQTTAccountSessionManager(c)
			if err == nil {
				t.Fatal("an account holding a retired metadata stream must not initialize")
			}
			for _, name := range tc.retired {
				if !strings.Contains(err.Error(), "still holds") || !strings.Contains(err.Error(), name) {
					t.Fatalf("the failure must be the retired-stream diagnostic naming %s, got %v", name, err)
				}
			}
			// Refused BEFORE provisioning: the first stream the loop would create, and
			// one later in the table, must not exist unless planted.
			planted := map[string]bool{}
			for _, cfg := range tc.plant {
				planted[cfg.Name] = true
			}
			for _, name := range []string{mqttSessStreamName, mqttWillQStreamName, mqttRetainedMsgsStreamName} {
				if planted[name] {
					continue
				}
				if _, serr := js.Stream(ctx, name); serr == nil {
					t.Fatalf("the refusal must precede the provisioning loop, but %s was created", name)
				}
			}
		})
	}
}
