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
	"fmt"
	"strings"
	"time"
)

// mqttSlowConnectThreshold is how long a CONNECT may take, to its success CONNACK (or to its
// return, when it wrote none), before its timeline is logged. PROVISIONAL and diagnostic: below
// the 0.3 s CONNACK window the Paho interop suite gives a client, and above the ≈100 ms a
// clean-start CONNECT measured on a store that syncs every write, so the line names the steps of
// a CONNECT that comes near a client's patience. It is a diagnostic text line, not part of the
// operational interface (docs/ARCHITECTURE.md, "A slow CONNECT names its steps").
const mqttSlowConnectThreshold = 250 * time.Millisecond

// mqttConnectSpan is one step of a CONNECT: the work between its begin and the next one.
type mqttConnectSpan struct {
	name       string
	begin, end time.Time
	cleanup    bool // a deferred cleanup, run after the body returned
	exited     bool // the body span the function returned from
}

// mqttConnectTimeline records where a CONNECT spent its time, as an ordered list of spans each
// begun at the ENTRY of its work, so nothing between two steps is charged to the wrong one. A step
// begun again (a re-drive) is numbered. Deferred cleanup is its own span: the first cleanup call
// marks where the body returned, so a slow certification or Will is named as such rather than
// charged to the step the body returned from.
type mqttConnectTimeline struct {
	now       func() time.Time
	start     time.Time
	spans     []mqttConnectSpan
	connAckAt time.Time
	bodyDone  bool
	count     map[string]int
}

func newMQTTConnectTimeline(now func() time.Time) *mqttConnectTimeline {
	tl := &mqttConnectTimeline{now: now, count: map[string]int{}}
	tl.start = now()
	tl.open("admit", false)
	return tl
}

func (tl *mqttConnectTimeline) closeOpen(at time.Time) {
	if n := len(tl.spans); n > 0 && tl.spans[n-1].end.IsZero() {
		tl.spans[n-1].end = at
	}
}

func (tl *mqttConnectTimeline) open(name string, cleanup bool) {
	at := tl.now()
	tl.closeOpen(at)
	tl.count[name]++
	if n := tl.count[name]; n > 1 {
		name = fmt.Sprintf("%s#%d", name, n)
	}
	tl.spans = append(tl.spans, mqttConnectSpan{name: name, begin: at, cleanup: cleanup})
}

// begin ends the open step and opens name. A begin after the body returned is ignored: the body
// has no steps left.
func (tl *mqttConnectTimeline) begin(name string) {
	if tl.bodyDone {
		return
	}
	tl.open(name, false)
}

// beginUnlessOpen is begin, except that a step already open under name continues: two boundaries
// that can each start the same step (a resume starts it earlier than a fresh CONNECT) make one span.
func (tl *mqttConnectTimeline) beginUnlessOpen(name string) {
	if n := len(tl.spans); n > 0 && tl.spans[n-1].end.IsZero() && tl.spans[n-1].name == name {
		return
	}
	tl.begin(name)
}

// connAck stamps the instant the success CONNACK was written and opens the work after it.
func (tl *mqttConnectTimeline) connAck() {
	tl.begin("after-connack")
	tl.connAckAt = tl.spans[len(tl.spans)-1].begin
}

// cleanup opens a deferred cleanup step. The first one marks the body span that was open as the
// one the function returned from.
func (tl *mqttConnectTimeline) cleanup(name string) {
	if !tl.bodyDone {
		tl.bodyDone = true
		if n := len(tl.spans); n > 0 && tl.spans[n-1].end.IsZero() {
			tl.spans[n-1].exited = true
		}
	}
	tl.open("cleanup:"+name, true)
}

// report ends the open step and renders the timeline: whether a success CONNACK was written,
// how long it took (or how long until return, when none was), every step in order, and the
// returned error. slow reports whether that time reached threshold.
func (tl *mqttConnectTimeline) report(clientID string, connErr error, threshold time.Duration) (line string, slow bool) {
	end := tl.now()
	tl.closeOpen(end)
	if !tl.bodyDone {
		// No deferred cleanup ran: the body's last span is the one it returned from.
		if n := len(tl.spans); n > 0 {
			tl.spans[n-1].exited = true
		}
	}
	var b strings.Builder
	var took time.Duration
	if !tl.connAckAt.IsZero() {
		took = tl.connAckAt.Sub(tl.start)
		fmt.Fprintf(&b, "CONNECT from client %q: success CONNACK after %s;", clientID, mqttSpanDuration(took))
	} else {
		took = end.Sub(tl.start)
		fmt.Fprintf(&b, "CONNECT from client %q: no success CONNACK, returned after %s;", clientID, mqttSpanDuration(took))
	}
	for i, sp := range tl.spans {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, " %s %s", sp.name, mqttSpanDuration(sp.end.Sub(sp.begin)))
		if sp.exited && connErr != nil {
			b.WriteString(" (failed here)")
		}
	}
	if connErr != nil {
		fmt.Fprintf(&b, "; error: %q", connErr.Error())
	}
	return b.String(), took >= threshold
}

func mqttSpanDuration(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

// mqttReportConnectTimeline logs one CONNECT's timeline when it was slow. Called once, last, at
// mqttProcessConnect's return.
func mqttReportConnectTimeline(c *client, tl *mqttConnectTimeline, connErr error) {
	threshold := mqttSlowConnectThreshold
	if p := mqttTestSlowConnectThresholdV.Load(); p != nil {
		threshold = *p
	}
	c.mu.Lock()
	cid := c.mqtt.cid
	c.mu.Unlock()
	line, slow := tl.report(cid, connErr, threshold)
	if slow {
		c.Warnf("MQTT: slow %s", line)
	}
	if h := mqttTestConnectReportHookV.Load(); h != nil {
		(*h)(cid, line, slow)
	}
}
