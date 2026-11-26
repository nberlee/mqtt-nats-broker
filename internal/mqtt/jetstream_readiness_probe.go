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
	"fmt"
	"time"
)

// The readiness probe (issue #188): whether JetStream answers the requests a serving broker
// depends on, asked through the account's own assistant so it takes the broker's route (its
// reply prefix, its domain, its timeout) rather than a second client's.
//
// Two requests, because no one request covers both: account info is answered only by the meta
// leader (consumer and stream creates need one), and the session stream's info only by that
// stream's leader (every session persist and load needs one). Only an answer counts: a timeout,
// no responders, a leaderless or offline asset and a definite refusal (a missing session stream,
// an account without JetStream) all fail the probe alike. So does a session stream info that does
// not name the stream's leader: a member of a LEADERLESS group answers it successfully from its
// own state (jsStreamInfoRequest, isGroupLeaderless), with no leader named, while no write can
// commit. What the answers do NOT prove — write latency, consumer placement, other streams'
// leaders — is stated in docs/CONFORMANCE.md.

// ReadinessCheck names which of the probe's checks failed, for the operator's record.
type ReadinessCheck string

const (
	ReadinessCheckAccountInfo       ReadinessCheck = "account info"
	ReadinessCheckSessionStreamInfo ReadinessCheck = "session stream info"
)

// ReadinessProbeError is a failed readiness probe: the check that failed and why.
type ReadinessProbeError struct {
	Check ReadinessCheck
	Err   error
}

func (e *ReadinessProbeError) Error() string { return fmt.Sprintf("%s: %v", e.Check, e.Err) }
func (e *ReadinessProbeError) Unwrap() error { return e.Err }

// errMQTTReadinessNoManager is a probe before the account's session manager exists: Start has
// not succeeded, so there is no assistant to ask.
var errMQTTReadinessNoManager = errors.New("the account session manager is not initialized")

// ProbeJetStream asks JetStream, through the account's assistant, for the account's info and
// then for the session stream's info. nil means both were answered; otherwise the error is a
// *ReadinessProbeError naming the first check that was not. Each request waits at most the
// assistant's request timeout, and returns at once once the server shuts down.
func (s *Server) ProbeJetStream() error {
	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	asm := sm.sessions[s.globalAccount().GetName()]
	sm.mu.RUnlock()
	if asm == nil {
		return &ReadinessProbeError{ReadinessCheckAccountInfo, errMQTTReadinessNoManager}
	}
	if err := asm.jsa.accountInfo(); err != nil {
		return &ReadinessProbeError{ReadinessCheckAccountInfo, err}
	}
	si, err := asm.jsa.lookupStream(mqttSessStreamName)
	if err == nil {
		err = mqttReadinessLeaderAnswered(si)
	}
	if err != nil {
		return &ReadinessProbeError{ReadinessCheckSessionStreamInfo, err}
	}
	return nil
}

// errMQTTReadinessNoStreamLeader is a session stream info answered by a peer of a group with no
// leader: that peer's local state, not a stream that can commit.
var errMQTTReadinessNoStreamLeader = errors.New("answered without naming the stream's leader")

// mqttReadinessLeaderAnswered requires a stream info to name the stream's leader. Every answer
// carries one: a single server's stream names that server (jetStream.clusterInfo with no raft
// group), a clustered stream its elected leader, and only a leaderless group member's answer
// names none.
func mqttReadinessLeaderAnswered(si *StreamInfo) error {
	if si == nil || si.Cluster == nil || si.Cluster.Leader == _EMPTY_ {
		return errMQTTReadinessNoStreamLeader
	}
	return nil
}

// JetStreamRequestTimeout is how long one of ProbeJetStream's requests waits for its reply: the
// account assistant's request timeout.
func (s *Server) JetStreamRequestTimeout() time.Duration {
	return mqttJSAPITimeoutOf(s.getOpts())
}

// mqttJSAPITimeoutOf is the account assistant's request timeout under opts.
func mqttJSAPITimeoutOf(opts *Options) time.Duration {
	if opts.MQTT.JSAPITimeout == 0 {
		return mqttDefaultJSAPITimeout
	}
	return opts.MQTT.JSAPITimeout
}
