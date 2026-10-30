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
	"strings"
	"sync"
)

// THE WRITE BOUNDARY READS WHAT IT RE-READS IN ONE PIPELINED REQUEST.
//
// The store comparisons of commitAuthorityTransition re-read, member by member, the records a
// transition overwrites and the exchanges its actions are acquired for. Each of those subjects is
// known from the transition alone before the first comparison runs, and each read was already a
// read taken after the transition was composed and before it is submitted. So they are requested
// together, pipelined, at the start of that window, and the comparisons, unchanged, are answered
// from a view of the assistant.
//
// ONE ANSWER PER SUBJECT. The view answers only the FIRST read of a prefetched subject; every later
// read of it goes to the store. A comparison that re-reads to observe progress (the inventory
// traversal's re-read of the ledger after it finds a position crossed) therefore still observes it.
// The writes are fenced at submission by their own expectations exactly as before.

// mqttPrefetchedReads holds one transition's prefetched responses, each given out once.
type mqttPrefetchedReads struct {
	mu   sync.Mutex
	resp map[string]*JSApiMsgGetResponse
}

func mqttPrefetchKey(stream, subject string) string { return stream + "\x00" + subject }

// take returns the prefetched response for subject and forgets it, so the subject's next read
// reaches the store.
func (p *mqttPrefetchedReads) take(stream, subject string) (*JSApiMsgGetResponse, bool) {
	if p == nil {
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := mqttPrefetchKey(stream, subject)
	r, ok := p.resp[key]
	if ok {
		delete(p.resp, key)
	}
	return r, ok
}

// mqttBoundaryPrefetchSet is the subjects a transition's comparisons will read before any other
// read decides them: every member carried at a nonzero expectation (the comparison re-reads what
// the member overwrites) and the exchange each action member is acquired for. Deduplicated, in
// member order, $MQTT_inflight subjects only.
func mqttBoundaryPrefetchSet(id string, extra []mqttTransitionMember) []string {
	var set []string
	seen := map[string]bool{}
	add := func(subj string) {
		if !seen[subj] {
			seen[subj] = true
			set = append(set, subj)
		}
	}
	for _, e := range extra {
		if !mqttIsInflightStreamSubject(e.subj) {
			continue
		}
		if e.expect != 0 {
			add(e.subj)
		}
		if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttActionClassTk {
			if toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], "."); len(toks) == 5 {
				add(mqttExchangeSubject(getHash(id), toks[2], toks[3]))
			}
		}
	}
	return set
}

// mqttIsInflightStreamSubject reports whether subject is one of $MQTT_inflight's: an F-105 record,
// a register or an unsent marker.
func mqttIsInflightStreamSubject(subject string) bool {
	if mqttIsF105Subject(subject) {
		return true
	}
	if _, err := mqttParseInflightSubject(subject); err == nil {
		return true
	}
	_, err := mqttParseInflightMarkerSubject(subject)
	return err == nil
}

// withBoundaryPrefetch returns a view of jsa that answers the first read of each subject of the
// transition's prefetch set from one pipelined request. Every subject with a response is answered,
// whatever error the request also returned (a partial batch arrives together with a timeout); a
// subject without one is read from the store. A set of fewer than two subjects, or a request that
// produced no response at all, returns jsa itself.
func (jsa *mqttJSA) withBoundaryPrefetch(id string, extra []mqttTransitionMember) *mqttJSA {
	set := mqttBoundaryPrefetchSet(id, extra)
	if len(set) < 2 {
		return jsa
	}
	resps, _ := jsa.loadLastMsgForMulti(mqttInflightStreamName, set)
	p := &mqttPrefetchedReads{resp: make(map[string]*JSApiMsgGetResponse, len(set))}
	for i, r := range resps {
		if i < len(set) && r != nil {
			p.resp[mqttPrefetchKey(mqttInflightStreamName, set[i])] = r
		}
	}
	if len(p.resp) == 0 {
		return jsa
	}
	v := *jsa
	v.prefetched = p
	return &v
}
