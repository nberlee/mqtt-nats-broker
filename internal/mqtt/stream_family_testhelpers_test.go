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

import "strings"

// $MQTT_sess carries three record families, so a test hook that means one of them selects
// it by SUBJECT: the stream name alone also matches the other two.

// testInflightSpace reports a read or write of the in-flight and outbound records, the
// $MQTT.inflight. space of the session stream.
func testInflightSpace(stream, subject string) bool {
	return stream == mqttSessStreamName && strings.HasPrefix(subject, mqttInflightStreamSubjectPrefix)
}

// testSessionFamily reports a read or write of the session-record family: the session
// records, the acceptance facts, the handoff outcomes and the teardown receipts.
func testSessionFamily(stream, subject string) bool {
	return stream == mqttSessStreamName && subject != _EMPTY_ &&
		!strings.HasPrefix(subject, mqttInflightStreamSubjectPrefix) &&
		!strings.HasPrefix(subject, mqttSharedLifecycleStreamSubjectPrefix)
}

// testSubjectAtSeq names the subject stored at seq, for a hook that is handed a sequence only
// (a delete) and must select one family. Read before the delete it intercepts, so the record is
// still there; a failed read names nothing, which no family predicate matches.
func testSubjectAtSeq(jsa *mqttJSA, stream string, seq uint64) string {
	smsg, err := jsa.loadMsg(stream, seq)
	if err != nil || smsg == nil {
		return _EMPTY_
	}
	return smsg.Subject
}
