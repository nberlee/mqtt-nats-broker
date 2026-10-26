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

//go:build mqtt_fulltests

package mqtt

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The structural half of the output boundary (F-105, row 2; plan §7.1).
//
// §7.1's property is about what REACHES THE WRITER, and no amount of validation inside the two
// boundary functions establishes it: a caller that never touches them writes to the socket
// unchecked. A review put it plainly — "every transport write through the constrained boundary
// with lower raw paths and their aliases structurally closed".
//
// Go cannot close them within a package, so the closure is this check. It enumerates every call
// to a raw transport primitive in the package's non-test sources and refuses any that is neither
// a BOUNDARY (the two functions that are allowed to write) nor a RECORDED unconverted emitter.
//
// # Why the recorded list is failures rather than exceptions
//
// The same review ruled that a complete structural check CANNOT pass against the existing
// bypassing emitters, and that the answer is to build the check and RECORD ITS EXPECTED
// FAILURES rather than introduce temporary caller exceptions. The difference is not cosmetic: an
// exception says a site is allowed, and this list says a site is OWED. It shrinks as the output
// conversion proceeds and it may never grow — a new entry is a new bypass, and this test refuses
// one whether or not anybody remembered to look.
//
// A stale entry fails too. A site that was converted and left in the list would hide the next
// bypass that happens to land in the same function.
//
// # What this check cannot see, stated so it is not mistaken for a proof
//
// It is syntactic. A socket reached through a type assertion (`c.nc.(*tls.Conn)`), through an
// embedded field, or through an interface value stored elsewhere is invisible to it, and so is
// anything in another package. What it does establish is that no NEW call to a named primitive,
// and no new handing-out of the socket, appears without somebody recording it.

// mqttOutputSite is one recorded site: how many times the primitive appears there, and why.
//
// The COUNT is what a review found missing. Keyed on the site alone, a SECOND raw write added
// inside a function that already has a permitted one is invisible, and a function with a
// permitted write is exactly where an extra one does the most damage.
type mqttOutputSite struct {
	Times int
	Why   string
}

// mqttRawOutputPrimitives are the calls that can put bytes on a connection.
//
// Both halves matter. The queueing family is the ALIAS chain — `enqueueProto` is one line
// calling `queueOutbound`, which writes through `writeSocket` — and a check naming only
// the top of it would be satisfied by calling one link lower. `nc.Write` and `rawConn.Write` are
// the socket itself, which is what the aliases eventually reach.
var mqttRawOutputPrimitives = map[string]struct{}{
	"enqueueProto": {}, "queueOutbound": {},
	"nc.Write": {}, "rawConn.Write": {},
	// AND THE TWO LOWER WRITERS. A review found the hole: `c.writeSocket(frame, when)` reaches
	// `nc.Write` inside a function this table already accounts for, so the scan recorded
	// nothing at the CALL site and the frame went out having consumed no action. The same
	// applies to the authorized boundary's own part writer. Neither needed any of the
	// type-assertion or embedding limits this check already admits to — they were simply
	// missing from the list.
	"writeSocket": {}, "writeFramePartLocked": {},
}

// mqttOutputBoundarySites are the calls that ARE the boundary, plus the primitives' own
// definitions.
//
// Deliberately keyed by function, not by file: a file-level permission would let anything added
// beside a boundary function inherit its licence, and "direct output from an otherwise permitted
// emitter" is one of the bypasses this check was required to catch.
var mqttOutputBoundarySites = map[string]mqttOutputSite{
	"outbound_frame_write.go|writeFramePartLocked|nc.Write":             {1, "the AUTHORIZED boundary's only write"},
	"outbound_frame_write.go|writeAuthorizedFrame|writeFramePartLocked": {1, "the boundary's own part loop"},
	"outbound_actionless.go|writeActionlessFrameBy|writeSocket":         {1, "the ACTIONLESS boundary's only write"},
	"shim_engine.go|enqueueProto|queueOutbound":                         {1, "the alias chain's own definition"},
	"shim_engine.go|queueOutbound|writeSocket":                          {1, "the alias chain reaching the transport"},
	"shim_engine.go|writeSocket|nc.Write":                               {1, "the transport implementation the chain reaches"},
}

// mqttOutputBypassesOwed is the RECORDED FAILURE SET: every emitter that still reaches the
// transport without passing a boundary.
//
// Since WP-F step 1 only the two MIXED emitters remain (mqttAlphaMixedSites): every
// actionless-only emitter writes through writeActionlessFrameBy. What keeps the two here is
// chapter P's slot-only QoS 1/2, and the value says what each site carries.
var mqttOutputBypassesOwed = map[string]mqttOutputSite{
	"protocol_output.go|mqttEnqueueProtoLocked|enqueueProto":           {1, "the generic locked emitter — MIXED: actionless acknowledgements, PINGRESP, SUBACK v5 and UNSUBACK, plus the slot-only PUBREL (experimental affine, chapter P); an ordinary or retained PUBREL goes through writeAuthorizedFrame"},
	"publish_deliver.go|mqttEnqueuePreparedPublishMsgTo|queueOutbound": {2, "PUBLISH delivery's raw branch — MIXED: QoS 0, plus slot-only QoS 1/2 (experimental affine, chapter P); every other QoS 1/2 frame requires its acquisition and goes through writeAuthorizedFrame (bindSection)"},
}

// mqttSocketPlumbingSites are the places the socket is named for something other than emitting
// an MQTT frame.
//
// A separate table from the bypasses because they are a different kind of thing: a bypass is an
// EMITTER the conversion owes, and these are the transport's own plumbing — the read side, the
// TLS wrap, the WebSocket handshake and wrap, and teardown. Recording them rather than excluding the pattern is what keeps a new
// one visible: `io.Copy(c.nc, buf)` added tomorrow lands in this check, not in silence.
var mqttSocketPlumbingSites = map[string]mqttOutputSite{
	"conn_lifecycle.go|createMQTTClient|nc (handed out)":  {3, "io.ReadFull on the connection: the READ side; the WebSocket opening handshake, which writes only its HTTP answer before any MQTT frame exists; and the WebSocket adapter wrapping the socket, through which every later MQTT frame still leaves by writeAuthorizedFrame or writeActionlessFrame, its own writes being Pong and Close control frames"},
	"shim_engine.go|doTLSServerHandshake|nc (handed out)": {1, "tls.Server wraps the socket"},
	"shim_engine.go|readLoop|nc (aliased)":                {1, "the read loop's own local"},
}

// TestF105EveryTransportWriteIsAccountedFor is the structural check itself.
func TestF105EveryTransportWriteIsAccountedFor(t *testing.T) {
	found := mqttScanRawOutputSites(t)

	recorded := map[string]mqttOutputSite{}
	for _, tbl := range []map[string]mqttOutputSite{mqttOutputBoundarySites,
		mqttOutputBypassesOwed, mqttSocketPlumbingSites} {
		for site, rec := range tbl {
			recorded[site] = rec
		}
	}

	var unaccounted []string
	for site, times := range found {
		rec, ok := recorded[site]
		if !ok {
			unaccounted = append(unaccounted, site)
			continue
		}
		// THE COUNT IS PART OF THE RECORD. A second raw write inside a function that already
		// has a permitted one is exactly where an extra one does the most damage, and keyed on
		// the site alone it would be invisible.
		if times != rec.Times {
			unaccounted = append(unaccounted,
				fmt.Sprintf("%s (recorded %d, found %d)", site, rec.Times, times))
		}
	}
	sort.Strings(unaccounted)
	if len(unaccounted) != 0 {
		t.Fatalf("these transport writes are neither a boundary nor a recorded bypass:\n  %s\n"+
			"A new raw write is a new way for an obligation-carrying frame to reach the socket "+
			"without consuming its action. Route it through writeAuthorizedFrame or "+
			"writeActionlessFrame, or record it in mqttOutputBypassesOwed with what it emits.",
			strings.Join(unaccounted, "\n  "))
	}

	// A STALE ENTRY IS A FAILURE TOO. A converted site left in the list would hide the next
	// bypass that lands in the same function, and the list's whole purpose is to shrink.
	var gone []string
	for site := range recorded {
		if _, ok := found[site]; !ok {
			gone = append(gone, site)
		}
	}
	sort.Strings(gone)
	if len(gone) != 0 {
		t.Fatalf("these recorded sites no longer exist:\n  %s\nRemove them: a list that "+
			"outlives what it describes stops describing anything.", strings.Join(gone, "\n  "))
	}

	// THE SET'S CONTENT IS TestF105OutputBypassSetIsTheTwoMixedSites's to assert; this test
	// asks only that every raw write is recorded, accurately.
	t.Logf("output boundary: %d boundary site(s), %d bypass(es) still owed",
		len(mqttOutputBoundarySites), len(mqttOutputBypassesOwed))
}

// mqttAlphaMixedSites is the ALPHA requirement on the recorded failure set (F-105 WP-F step 1,
// consult 125): exactly the two MIXED emitters, each with its count.
//
// A SEPARATE literal on purpose. Compared against itself, or by key alone, the bypass table passes
// a new raw write that arrives with its own matching entry (B2) and a second raw write inside a
// mixed site that arrives with its count bumped — and a mixed site is exactly where an extra write
// does the most damage, because it already carries traffic nobody re-validates.
//
// The cutover plan's EMPTY set is not met and is not claimed: what is left on these two sites is
// supported QoS 0 and actionless acknowledgements beside chapter P's slot-only QoS 1/2.
// TestF105SupportedObligationsNeverLeaveThroughAMixedSite is the runtime half that a count cannot
// supply: it shows supported QoS 1/2 traffic does not pass through them.
var mqttAlphaMixedSites = map[string]int{
	"protocol_output.go|mqttEnqueueProtoLocked|enqueueProto":           1,
	"publish_deliver.go|mqttEnqueuePreparedPublishMsgTo|queueOutbound": 2,
}

// TestF105OutputBypassSetIsTheTwoMixedSites asserts the recorded failure set EQUALS the alpha set,
// by key and by count.
func TestF105OutputBypassSetIsTheTwoMixedSites(t *testing.T) {
	var diff []string
	for site, rec := range mqttOutputBypassesOwed {
		want, ok := mqttAlphaMixedSites[site]
		switch {
		case !ok:
			diff = append(diff, fmt.Sprintf("%s is recorded as a bypass but is not a mixed site", site))
		case rec.Times != want:
			diff = append(diff, fmt.Sprintf("%s is recorded with %d raw write(s), the alpha set allows %d",
				site, rec.Times, want))
		}
	}
	for site := range mqttAlphaMixedSites {
		if _, ok := mqttOutputBypassesOwed[site]; !ok {
			diff = append(diff, fmt.Sprintf("%s is a mixed site the bypass table no longer records", site))
		}
	}
	sort.Strings(diff)
	if len(diff) != 0 {
		t.Fatalf("the output bypass set is not the two mixed sites:\n  %s\nAn actionless emitter "+
			"writes through writeActionlessFrameBy; an obligation-carrying one through "+
			"writeAuthorizedFrame. Neither belongs in the bypass table.", strings.Join(diff, "\n  "))
	}
}

// mqttSocketSelector reports the socket field an expression names, or empty.
func mqttSocketSelector(e ast.Expr) string {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return _EMPTY_
	}
	if sel.Sel.Name == "nc" || sel.Sel.Name == "rawConn" {
		return sel.Sel.Name
	}
	return _EMPTY_
}

// mqttScanRawOutputSites parses the package's own sources and returns every raw output call,
// keyed file|function|primitive.
//
// Parsing rather than grepping, because a grep cannot say which FUNCTION a line is in — and the
// bypass a review named specifically was "direct output from an otherwise permitted emitter",
// which is exactly a line in the wrong function of a permitted file.
func mqttScanRawOutputSites(t *testing.T) map[string]int {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	fset := token.NewFileSet()
	found := map[string]int{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		var fn string
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncDecl:
				fn = node.Name.Name
			case *ast.SelectorExpr:
				// EVERY SELECTOR, not only the ones being called. `f := c.enqueueProto` names
				// the primitive without calling it there, and the call through `f` is
				// invisible to a check that only looks at CallExpr.
				key := node.Sel.Name
				recv := _EMPTY_
				switch x := node.X.(type) {
				case *ast.Ident:
					recv = x.Name
				case *ast.SelectorExpr:
					recv = x.Sel.Name
				}
				if key == "Write" {
					key = recv + ".Write"
				}
				if _, ok := mqttRawOutputPrimitives[key]; ok {
					found[fmt.Sprintf("%s|%s|%s", name, fn, key)]++
					return true
				}
			case *ast.CallExpr:
				// THE SOCKET HANDED TO SOMEBODY ELSE. `io.Copy(c.nc, r)`,
				// `fmt.Fprintf(c.nc, ...)` and `bufio.NewWriter(c.nc)` all reach the
				// transport without a `.Write` this scan would otherwise see. Only ARGUMENT
				// position counts: `c.nc.Close()` and `c.nc != nil` are not writes, and a
				// check that flagged them would be noise nobody reads.
				for _, arg := range node.Args {
					if sock := mqttSocketSelector(arg); sock != _EMPTY_ {
						found[fmt.Sprintf("%s|%s|%s (handed out)", name, fn, sock)]++
					}
				}
			case *ast.ValueSpec:
				// A DECLARATION IS AN ASSIGNMENT. `var conn = c.nc` moves the socket exactly
				// as `conn := c.nc` does, and a check that watched only one of the two forms
				// was a check a new emitter could step around by spelling it differently.
				for _, v := range node.Values {
					if sock := mqttSocketSelector(v); sock != _EMPTY_ {
						found[fmt.Sprintf("%s|%s|%s (aliased)", name, fn, sock)]++
					}
				}
			case *ast.AssignStmt:
				// AND THE SOCKET ALIASED. `conn := c.nc` moves it somewhere this scan cannot
				// follow, so the alias is recorded where it is made.
				for _, rhs := range node.Rhs {
					if sock := mqttSocketSelector(rhs); sock != _EMPTY_ {
						found[fmt.Sprintf("%s|%s|%s (aliased)", name, fn, sock)]++
					}
				}
			}
			return true
		})
	}
	if len(found) == 0 {
		// A SCAN THAT FINDS NOTHING IS A BROKEN SCAN, not a closed boundary. Without this the
		// check passes hardest exactly when it has stopped working.
		t.Fatalf("the scan found no transport writes at all, so it is not reaching the sources")
	}
	return found
}
