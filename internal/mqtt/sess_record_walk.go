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
	"sort"
)

// errMQTTSessWalkStop, returned by a session-record visitor, ends the walk successfully: the
// visitor has what it needs (the orphan consumer sweep, once no candidate is left).
var errMQTTSessWalkStop = errors.New("session record walk stopped by its visitor")

// walkSessionRecords visits every session record in the account, for the sweeps that need
// all of them.
//
// WHY NOT A SEQUENCE WALK. Session records are a sparse class on $MQTT_sess: the stream also
// holds one permanent acceptance fact, handoff outcome and teardown receipt per CONNECT
// (issue #133), so a few hundred records sit among hundreds of thousands of subjects. A
// next_by_subj request on the wildcard filter that starts past a match makes the store visit
// every block up to the next one, and a block whose per-block subject state has expired (two
// minutes idle) is read whole to rebuild it. One long-lived record early in the stream made
// each sweep tick's first walk read the whole stream from disk, under the stream's read lock,
// stalling its writers (issue #179).
//
// THE WALK. The subject listing (stream info with a subjects filter) is answered from the
// store's subject index without reading a block, and a literal subject's last message names
// its own block. So:
//
//  1. List the session subjects in ONE page. The server builds the stream state before the
//     subject map, so the page's last sequence L is a watermark taken ahead of the listing:
//     every record at or below L that still exists when the map is computed is listed.
//  2. Load each listed subject's last message and visit it. "No message" means the record
//     was deleted since the listing and it is skipped, the same answer the sequence walk acted
//     on (a failed store read answers the same way: issue #110's upstream exposure, not
//     compensated here).
//  3. Walk by sequence from L+1 to the end, without deduplicating against step 2: every
//     record created or rewritten after the watermark is visited there, at each version the
//     cursor passes, as the sequence walk would. It never traverses the historical prefix;
//     what it does traverse (the block holding L and whatever was written since) keeps the
//     cold-block exposure.
//
// A listing that does not fit one page (more than JSMaxSubjectDetails records) is not a
// census: each page is a separate computation, and a delete between pages shifts the offsets
// past a record that never left. The walk then runs the sequence walk from the start, with
// its whole-stream cost. An invalid page is an error and nothing is visited.
//
// visit returning errMQTTSessWalkStop ends the walk with nil; any other error ends it and is
// returned unchanged. A walk error is what a failed sequence-walk read was before: the
// caller's census is incomplete.
func (as *mqttAccountSessionManager) walkSessionRecords(visit func(*StoredMsg) error) error {
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	page, err := as.jsa.streamSubjectsCompletePage(mqttSessStreamName, filter, 0)
	if err != nil {
		return err
	}
	_, whole, ok := mqttSubjectPageAdvance(page, filter, 0)
	if !ok {
		return fmt.Errorf("the session subject listing could not be vouched for (total %d, limit %d, %d subjects)",
			page.Total, page.Limit, len(page.Subjects))
	}
	from := uint64(1)
	if whole {
		subjects := make([]string, 0, len(page.Subjects))
		for subj := range page.Subjects {
			subjects = append(subjects, subj)
		}
		sort.Strings(subjects)
		for _, subj := range subjects {
			smsg, err := as.jsa.loadLastMsgFor(mqttSessStreamName, subj)
			if err != nil {
				if isErrorOtherThan(err, JSNoMessageFoundErr) {
					return err
				}
				continue
			}
			if err := visit(smsg); err != nil {
				return mqttSessWalkVisitResult(err)
			}
		}
		from = page.LastSeq + 1
	}
	for seq := from; ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return err
			}
			return nil
		}
		seq = smsg.Sequence + 1
		if err := visit(smsg); err != nil {
			return mqttSessWalkVisitResult(err)
		}
	}
}

// mqttSessWalkVisitResult maps a visitor's error to the walk's: a stop is success.
func mqttSessWalkVisitResult(err error) error {
	if errors.Is(err, errMQTTSessWalkStop) {
		return nil
	}
	return err
}
