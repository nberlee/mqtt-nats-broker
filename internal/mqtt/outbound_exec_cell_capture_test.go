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
	"sync"
	"testing"
)

// Fixture-scoped capture of execution cells (F-105 integrated gate, round 1). Untagged so the shared
// inflight fixture can register its client.

// testExecCells records the execution cells created for the connections a fixture REGISTERED
// (testTrackExecCells), for that test only, so an acquisition whose holder the session never hands
// back (an adopted, reconciled or refused one) can still be read. A test reads the intended holder's
// own cell wherever it can reach the holder; this is for the holders it cannot.
var testExecCells sync.Map // *client -> *testExecCellList, registered clients only

type testExecCellList struct {
	mu    sync.Mutex
	cells []*mqttExecCell
	// admit holds ONE execution cell per acquisition for testAdmit, as production holds one per
	// acquisition's holder: every request a test presents for the same acquisition on the same
	// connection reaches the same cell, whichever action pointer it carries.
	admit map[mqttActionKey]*mqttExecCell
}

func init() {
	h := func(x *mqttExecCell) {
		v, ok := testExecCells.Load(x.conn)
		if !ok {
			return // not a registered connection: nothing is retained for it
		}
		l := v.(*testExecCellList)
		l.mu.Lock()
		l.cells = append(l.cells, x)
		l.mu.Unlock()
	}
	mqttTestExecCellCreatedHookV.Store(&h)
}

// testTrackExecCells registers c's execution cells for capture until t ends, when they and c's
// testAdmit cells are dropped.
func testTrackExecCells(t *testing.T, c *client) {
	t.Helper()
	l := &testExecCellList{}
	testExecCells.Store(c, l)
	t.Cleanup(func() {
		testExecCells.Delete(c)
		l.mu.Lock()
		l.cells, l.admit = nil, nil
		l.mu.Unlock()
	})
}
