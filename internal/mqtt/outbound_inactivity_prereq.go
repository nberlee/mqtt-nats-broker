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
	"fmt"
	"time"
)

// The inactivity prerequisite for F-105 source/PUBREL consumers (cutover, consult 4, §5.1).
//
// F-105's invariant is that a source ($MQTT_msgs) or PUBREL ($MQTT_out) consumer a live
// obligation depends on is deleted by NOBODY — JetStream's autonomous inactivity timer
// included. Deleting such a consumer drops its interest-retained records, and those records ARE
// the obligation's completion evidence; a timer that reaps it is the original F-105 defect
// reached without any broker acting.
//
// The pinned server SUBSTITUTES a stream-level ConsumerLimits.InactiveThreshold for a consumer
// that requests zero, so "the consumer asked for no threshold" does NOT establish that it has
// none: the EFFECTIVE threshold is the consumer's own where it set one, and the stream's limit
// where it did not. Safe therefore means BOTH are zero.
//
// This is the same disqualification drainingAllowed already applies to drains — but the
// response differs, and the difference is the point. drainingAllowed FALLS BACK to an immediate
// teardown when a limit is present; F-105 has NO fallback. A source/PUBREL dependency that
// cannot establish preservation is REFUSED. "Cannot verify" is treated as unsafe on the same
// ground drainingAllowed treats it so: an unread limit is not a proven-absent one, and admitting
// a dependency against an unknown limit would rest preservation on an absence.
//
// The decision is split into a PURE core (mqttEffectiveInactivitySafe) and a thin store wrapper
// (consumerInactivitySafe), so the rule is tested without an embedded JetStream and the wrapper
// is only the lookup drainingAllowed already models.

// mqttInactivityUnsafe explains why preservation of a depended-on consumer cannot be
// established, so the F-105 admission path may name the reason it refuses.
type mqttInactivityUnsafe struct{ Why string }

func (u mqttInactivityUnsafe) Error() string { return u.Why }

// mqttEffectiveInactivitySafe is the pure rule: preservation is established only when the stream
// limit is verified present-and-zero AND the consumer requests no threshold of its own.
//
//   - streamVerified is false when the stream's limit could not be read; unsafe, because an
//     unread limit is not a proven-absent one.
//   - streamLimit is the stream's ConsumerLimits.InactiveThreshold; a nonzero one is substituted
//     for a zero-requesting consumer, so it is disqualifying.
//   - consumerReq is the consumer's own requested InactiveThreshold; a nonzero one is reapable
//     whatever the stream says.
func mqttEffectiveInactivitySafe(streamLimit, consumerReq time.Duration, streamVerified bool) error {
	if !streamVerified {
		return mqttInactivityUnsafe{Why: "the stream's consumer inactivity limit could not be verified; " +
			"an unread limit is not a proven-absent one, so a depended-on consumer's survival is not established"}
	}
	if consumerReq != 0 {
		return mqttInactivityUnsafe{Why: fmt.Sprintf("the consumer requests an inactivity threshold of %s, "+
			"so it is autonomously reapable and may not carry an F-105 dependency", consumerReq)}
	}
	if streamLimit != 0 {
		return mqttInactivityUnsafe{Why: fmt.Sprintf("the stream imposes a consumer inactivity limit of %s, "+
			"which the server substitutes for a consumer that requests none, so a depended-on consumer could be "+
			"autonomously reaped with its completion evidence", streamLimit)}
	}
	return nil
}

// streamInactivitySafe reports whether streamName imposes no consumer inactivity limit, so a
// consumer that itself sets none has an effective threshold of zero. A refusal, never a
// fallback: the caller REFUSES to admit a dependency when this returns an error.
func (jsa *mqttJSA) streamInactivitySafe(streamName string) error {
	return jsa.consumerInactivitySafe(streamName, nil)
}

// consumerInactivitySafe reports whether a consumer created with cfg on streamName would have an
// effective inactivity threshold of zero. cfg may be nil to ask only about the stream. Both
// halves matter: the stream limit is substituted only when cfg requests zero, and a cfg that
// sets its own threshold is reapable regardless of the stream.
func (jsa *mqttJSA) consumerInactivitySafe(streamName string, cfg *ConsumerConfig) error {
	si, err := jsa.lookupStream(streamName)
	if err != nil || si == nil {
		// Mirror drainingAllowed's unverifiable case, but refuse rather than fall back.
		if uerr := mqttEffectiveInactivitySafe(0, 0, false); uerr != nil {
			return mqttInactivityUnsafe{Why: fmt.Sprintf("stream %q: %v (%v)", streamName, uerr, err)}
		}
	}
	var consumerReq time.Duration
	if cfg != nil {
		consumerReq = cfg.InactiveThreshold
	}
	if serr := mqttEffectiveInactivitySafe(si.Config.ConsumerLimits.InactiveThreshold, consumerReq, true); serr != nil {
		return mqttInactivityUnsafe{Why: fmt.Sprintf("stream %q: %v", streamName, serr)}
	}
	return nil
}

// mqttClearConsumerInactivity zeroes a source/PUBREL consumer config's requested inactivity
// threshold, so the consumer opts out of any per-consumer timer. It is the safe-config half of
// the prerequisite; the stream half (no ConsumerLimits.InactiveThreshold) is a stream
// prerequisite checked by streamInactivitySafe before a dependency is admitted.
//
// Every source and PUBREL create (processJSConsumer, ensurePubRelConsumerSubscription, the resume
// repair) calls it: an enrolled consumer's lifetime is its resource's, not a timer's, so the
// broker requests an inactivity threshold for none of them. Admission verifies the stream
// limit and the consumer's effective threshold (readyDependencyInc); the supported-configuration
// boundary (docs/CONFORMANCE.md) states an administrator must not impose one on a depended-on
// consumer throughout its lifetime.
func mqttClearConsumerInactivity(cfg *ConsumerConfig) {
	if cfg != nil {
		cfg.InactiveThreshold = 0
	}
}
