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
	"encoding/json"
	"fmt"
	"sort"
	"testing"
)

// testPlantRetiringGroup stores a group record whose incarnation 1 is retiring and whose
// consumer never existed, the shape a leaver that crashed inside L2 leaves for the sweep.
func testPlantRetiringGroup(t *testing.T, e *testOrphanEnv, qkey string) {
	t.Helper()
	body, err := json.Marshal(&mqttSharedGroupRecord{Epoch: e.asm.sessStreamEpoch, Group: qkey,
		Retiring: []uint64{1}, Next: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.asm.jsa.storeMsg(mqttSharedGroupSubject(qkey), 0, body); err != nil {
		t.Fatalf("planting the group record of %q: %v", qkey, err)
	}
}

// testGroupRetiring reports whether qkey's group record still lists incarnation 1 retiring.
func testGroupRetiring(t *testing.T, e *testOrphanEnv, qkey string) bool {
	t.Helper()
	g := e.asm.jsa.loadSharedGroup(e.asm.sessStreamEpoch, qkey)
	if g.status != mqttOutboundValid {
		t.Fatalf("the group record of %q reads %v (%v)", qkey, g.status, g.err)
	}
	return g.rec.isRetiring(1)
}

// testScriptGroupPages answers the sweep's group-record listing from subjects in pages of
// limit, failing page `brokenAt` (when non-negative) by answering it short.
func testScriptGroupPages(e *testOrphanEnv, subjects []string, limit, brokenAt int) {
	sort.Strings(subjects)
	obs := func(filter string, offset int) (mqttSubjectPage, error) {
		if filter != mqttSharedGroupWalkFilter() {
			return mqttSubjectPage{}, fmt.Errorf("unexpected listing of %q", filter)
		}
		end := min(offset+limit, len(subjects))
		page := mqttSubjectPage{Subjects: map[string]uint64{}, Total: len(subjects), Limit: limit, Offset: offset}
		for _, subj := range subjects[offset:end] {
			page.Subjects[subj] = 1
		}
		if brokenAt >= 0 && offset/limit == brokenAt {
			// A short page: it does not hold what its own envelope says it should.
			for subj := range page.Subjects {
				delete(page.Subjects, subj)
				break
			}
		}
		return page, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&obs)
}

// TestSharedGroupSweepFollowsEveryListingPage pins that the retirement sweep reaches a
// retiring group record listed on a LATER page of the group-record listing, now that it
// lists the records by subject instead of walking the session stream by sequence.
//
// The pages are of MATCHING group subjects: pagination applies to the subjects under the
// filter, so unrelated session or in-flight records would never consume a page slot.
func TestSharedGroupSweepFollowsEveryListingPage(t *testing.T) {
	e := testNewOrphanEnv(t, "sweep-pages")
	var subjects []string
	var target string
	for i := 0; i < 5; i++ {
		qkey := mqttSharedGroupKey("g", fmt.Sprintf("t/%d", i))
		testPlantRetiringGroup(t, e, qkey)
		subjects = append(subjects, mqttSharedGroupSubject(qkey))
	}
	sort.Strings(subjects)
	target = subjects[len(subjects)-1]
	testScriptGroupPages(e, subjects, 2, -1)

	e.asm.sweepSharedGroupRetirements(e.s)

	qkey := target[len(mqttSharedLifecycleStreamSubjectPrefix+mqttSharedGroupClassTk+"."):]
	if testGroupRetiring(t, e, qkey) {
		t.Fatalf("the retiring group on the last page (%s) was not retired: the sweep stopped "+
			"before the listing's end", target)
	}
}

// TestSharedGroupSweepActsOnNothingFromAnIncompleteListing pins that a listing which cannot
// be vouched for defers the WHOLE pass: a retiring group that the incomplete listing DID
// return is not acted on either.
//
// Mutation MA4, treating an incomplete listing as complete, acts on that returned group and
// retires it; a schedule whose only retiring group fell in the missing part would let the
// mutant pass, so the retiring group here is on the page that was answered.
func TestSharedGroupSweepActsOnNothingFromAnIncompleteListing(t *testing.T) {
	e := testNewOrphanEnv(t, "sweep-incomplete")
	var subjects []string
	for i := 0; i < 4; i++ {
		qkey := mqttSharedGroupKey("g", fmt.Sprintf("t/%d", i))
		testPlantRetiringGroup(t, e, qkey)
		subjects = append(subjects, mqttSharedGroupSubject(qkey))
	}
	// Page 0 is answered whole; page 1 is short, so the listing is incomplete.
	testScriptGroupPages(e, subjects, 2, 1)

	e.asm.sweepSharedGroupRetirements(e.s)

	sort.Strings(subjects)
	for _, subj := range subjects {
		qkey := subj[len(mqttSharedLifecycleStreamSubjectPrefix+mqttSharedGroupClassTk+"."):]
		if !testGroupRetiring(t, e, qkey) {
			t.Fatalf("the group %s was retired from an incomplete listing", subj)
		}
	}
}

// TestSharedGroupSweepRetiresFromTheLiveListing is the control on the real server: no
// scripted pages, the group records planted among unrelated records on the same stream,
// and every retiring entry cleared by one pass.
func TestSharedGroupSweepRetiresFromTheLiveListing(t *testing.T) {
	e := testNewOrphanEnv(t, "sweep-live")
	var qkeys []string
	for i := 0; i < 3; i++ {
		qkey := mqttSharedGroupKey("g", fmt.Sprintf("t/%d", i))
		testPlantRetiringGroup(t, e, qkey)
		qkeys = append(qkeys, qkey)
		// An unrelated in-flight-space record between the group records.
		if _, err := e.asm.jsa.storeMsg(fmt.Sprintf("%sunrelated.%d", mqttInflightStreamSubjectPrefix, i), 0, []byte("{}")); err != nil {
			t.Fatalf("planting an unrelated record: %v", err)
		}
	}
	e.asm.sweepSharedGroupRetirements(e.s)
	for _, qkey := range qkeys {
		if testGroupRetiring(t, e, qkey) {
			t.Fatalf("the retiring group %q was not retired by the live listing", qkey)
		}
	}
}
