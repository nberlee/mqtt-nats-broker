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

//go:build mqttport_fulltests

package mqtt

import (
	"strings"
	"testing"
)

// The consume-once fact lives on the acquisition's holder (F-105 integrated gate, round 1): these
// pin the refusals that moved off the connection with it.

// An acquisition recorded ABANDONED is refused by its own execution cell. The section's output is
// left OPEN and the gate unsealed, so the holder still admits the request and nothing but the cell
// stands between it and the wire.
//
// Mutation: M2 (abandon reports success without recording).
func TestF105AnAbandonedAcquisitionIsRefusedByItsCell(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "exec-cell-abandoned", 2)
	s.acquire(t)
	acq := s.tok.out.acq
	s.tok.out.abandonProven(acq)
	if s.tok.out.closed.Load() || !s.e.c.outBind.mayWrite() {
		t.Fatal("the fixture must leave the output open and the gate unsealed, or the cell is not what refuses")
	}
	if st, ok := s.state(acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the abandoned acquisition reads %v (present=%v) on its cell, want ABANDONED", st, ok)
	}
	if !s.tok.out.enterWrite(s.tok.out.action, s.e.c) {
		t.Fatal("the holder must still admit the request, or the cell is not what refuses")
	}
	prep := s.prepare(t, 2)
	res, err := s.e.c.writeAuthorizedFrame(mqttFrameRequest{Action: s.tok.out.action, Binding: s.e.c.outBind,
		Holder: s.tok.out, Parts: [][]byte{prep.header, prep.msg}})
	if res != mqttOutputPrevented || err == nil || !strings.Contains(err.Error(), "has already executed") {
		t.Fatalf("an abandoned acquisition's request gave %v %v, want refused by its cell", res, err)
	}
	if len(s.tc.written) != 0 {
		t.Fatalf("an abandoned acquisition put %q on the wire", s.tc.written)
	}
}

// A PUBLISH section's output admits only the connection it was acquired for. Asked at the holder
// itself, because the writer checks the cell's connection again behind it.
//
// Mutation: M6 (enterWrite drops the connection comparison).
func TestF105APublishOutputAdmitsOnlyItsOwnConnection(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "exec-cell-conn", 2)
	s.acquire(t)
	other := &client{}
	if s.tok.out.enterWrite(s.tok.out.action, other) {
		t.Fatal("a section's output admitted a request on a connection it was not acquired for")
	}
	if !s.tok.out.enterWrite(s.tok.out.action, s.e.c) {
		t.Fatal("the positive control: the section's own connection must be admitted")
	}
}
