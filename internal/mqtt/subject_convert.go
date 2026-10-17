// Copyright 2020-2026 The NATS Authors
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

// Conversion between MQTT topic names and filters and NATS subjects.

package mqtt

import (
	"fmt"
)

// Converts an MQTT Topic Name to a NATS Subject (used by PUBLISH)
// See mqttToNATSSubjectConversion() for details.
func mqttTopicToNATSPubSubject(mt []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(mt, false)
}

// Converts an MQTT Topic Filter to a NATS Subject (used by SUBSCRIBE)
// See mqttToNATSSubjectConversion() for details.
func mqttFilterToNATSSubject(filter []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(filter, true)
}

// Converts an MQTT Topic Name or Filter to a NATS Subject.
// In MQTT:
// - a Topic Name does not have wildcard (PUBLISH uses only topic names).
// - a Topic Filter can include wildcards (SUBSCRIBE uses those).
// - '+' and '#' are wildcard characters (single and multiple levels respectively)
// - '/' is the topic level separator.
//
// Conversion that occurs:
//   - '/' is replaced with '/.' if it is the first character in mt
//   - '/' is replaced with './' if the last or next character in mt is '/'
//     For instance, foo//bar would become foo./.bar
//   - '/' is replaced with '.' for all other conditions (foo/bar -> foo.bar)
//   - '.' is replaced with '//'.
//   - ' ' cause an error to be returned.
//
// If there is no need to convert anything (say "foo" remains "foo"), then
// the no memory is allocated and the returned slice is the original `mt`.
func mqttToNATSSubjectConversion(mt []byte, wcOk bool) ([]byte, error) {
	var cp bool
	var j int
	res := mt

	makeCopy := func(i int) {
		cp = true
		res = make([]byte, 0, len(mt)+10)
		if i > 0 {
			res = append(res, mt[:i]...)
		}
	}

	end := len(mt) - 1
	for i := 0; i < len(mt); i++ {
		switch mt[i] {
		case mqttTopicLevelSep:
			if i == 0 || res[j-1] == btsep {
				if !cp {
					makeCopy(0)
				}
				res = append(res, mqttTopicLevelSep, btsep)
				j++
			} else if i == end || mt[i+1] == mqttTopicLevelSep {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep, mqttTopicLevelSep)
				j++
			} else {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep)
			}
		case ' ', '\t', '\n', '\r', '\f':
			// We cannot support whitespace in the MQTT topic/filter — these
			// characters would also corrupt the NATS wire protocol when the
			// subject is forwarded to other connection types (e.g. leaf
			// nodes) where the resulting control line could be split.
			return nil, errMQTTUnsupportedCharacters
		case 0x7f:
			// SubjectTree uses DEL as an internal pivot marker, so retained
			// subjects containing it cannot be indexed safely, including
			// legacy retained messages recovered from the retained-message
			// stream.
			return nil, errMQTTUnsupportedCharacters
		case btsep:
			if !cp {
				makeCopy(i)
			}
			res = append(res, mqttTopicLevelSep, mqttTopicLevelSep)
			j++
		case mqttSingleLevelWC, mqttMultiLevelWC:
			if !wcOk {
				// Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1]
				// The wildcard characters can be used in Topic Filters, but MUST NOT be used within a Topic Name
				return nil, fmt.Errorf("%w: wildcards not allowed in publish's topic: %q", errMQTTMalformedPacket, mt)
			}
			// Spec [MQTT-4.7.1-3]: '#' must be on its own or follow a level
			// separator, and in either case must be the LAST character of the
			// filter (no further levels after it). Checked here — rather than
			// relying on the embedded nats-server's internal subject validator
			// the way upstream implicitly does — because in the standalone
			// broker a malformed subject reaches nats.go's Subscribe() over a
			// real connection (nats.go's own client-side check does not catch
			// this), and letting it through disrupts that shared connection's
			// other in-flight requests rather than failing this one filter.
			if mt[i] == mqttMultiLevelWC && (i != end || (i > 0 && mt[i-1] != mqttTopicLevelSep)) {
				return nil, fmt.Errorf("%w: multi-level wildcard must be the last character in filter: %q", errMQTTMalformedPacket, mt)
			}
			if !cp {
				makeCopy(i)
			}
			if mt[i] == mqttSingleLevelWC {
				res = append(res, pwc)
			} else {
				res = append(res, fwc)
			}
		default:
			if cp {
				res = append(res, mt[i])
			}
		}
		j++
	}
	if cp && res[j-1] == btsep {
		res = append(res, mqttTopicLevelSep)
		j++
	}
	return res[:j], nil
}

// Converts a NATS subject to MQTT topic. This is for publish
// messages only, so there is no checking for wildcards.
// Rules are reversed of mqttToNATSSubjectConversion.
func natsSubjectStrToMQTTTopic(subject string) []byte {
	return natsSubjectToMQTTTopic(stringToBytes(subject))
}

func natsSubjectToMQTTTopic(subject []byte) []byte {
	topic := make([]byte, len(subject))
	end := len(subject) - 1
	var j int
	for i := 0; i < len(subject); i++ {
		switch subject[i] {
		case mqttTopicLevelSep:
			if i < end {
				switch c := subject[i+1]; c {
				case btsep, mqttTopicLevelSep:
					if c == btsep {
						topic[j] = mqttTopicLevelSep
					} else {
						topic[j] = btsep
					}
					j++
					i++
				default:
				}
			}
		case btsep:
			topic[j] = mqttTopicLevelSep
			j++
		default:
			topic[j] = subject[i]
			j++
		}
	}
	return topic[:j]
}

// Returns true if the subject has more than 1 token and ends with ".>"
func mqttNeedSubForLevelUp(subject string) bool {
	if len(subject) < 3 {
		return false
	}
	end := len(subject)
	if subject[end-2] == '.' && subject[end-1] == fwc {
		return true
	}
	return false
}
