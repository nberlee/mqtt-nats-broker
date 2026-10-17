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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// mqttPositiveAckTokens are the names through which a positive JetStream acknowledgement can be
// emitted: the fire-and-forget primitive, the positive-ack payload, and the confirmed request kind.
// A selector OR a bare identifier counts, so a method value (`f := jsa.sendAck`) is seen too.
var mqttPositiveAckTokens = map[string]struct{}{
	"sendAck": {}, "confirmAck": {}, "AckAck": {}, "mqttJSAAckConfirm": {},
}

// mqttPositiveAckSites is EVERY production site allowed to name a positive-ack token, and why. The
// gate is the only emitter; the rest name the token without emitting on a source reply.
var mqttPositiveAckSites = map[string]string{
	"outbound_source_ack_gate.go|submitSourceAckLocked|sendAck":       "THE GATE: the fire-and-forget submission, after the HandBack check",
	"outbound_source_ack_gate.go|enqueue|AckAck":                      "THE GATE: the confirmed submission's payload, enqueued under the check",
	"outbound_source_ack_gate.go|prepareAckConfirm|mqttJSAAckConfirm": "THE GATE: the confirmed submission's reply kind",
	"jsa.go|sendAck|sendAck":                                          "the primitive's own declaration",
	"jsa.go|processJSAPIReplies|mqttJSAAckConfirm":                    "the reply demultiplexer: routes a confirmation, emits nothing",
	"mqtt.go||mqttJSAAckConfirm":                                      "the request-kind constant's declaration",
	"shim_natsalias.go||AckAck":                                       "the payload alias's declaration",
}

func mqttScanPositiveAckSites(t *testing.T) map[string]int {
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
		for _, decl := range f.Decls {
			fn := _EMPTY_
			if fd, ok := decl.(*ast.FuncDecl); ok {
				fn = fd.Name.Name
				if _, ok := mqttPositiveAckTokens[fn]; ok {
					found[fmt.Sprintf("%s|%s|%s", name, fn, fn)]++
				}
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.SelectorExpr:
					if _, ok := mqttPositiveAckTokens[node.Sel.Name]; ok {
						found[fmt.Sprintf("%s|%s|%s", name, fn, node.Sel.Name)]++
					}
					// Do not also count the selector's Sel identifier below.
					ast.Inspect(node.X, func(m ast.Node) bool {
						if id, ok := m.(*ast.Ident); ok {
							if _, hit := mqttPositiveAckTokens[id.Name]; hit {
								found[fmt.Sprintf("%s|%s|%s", name, fn, id.Name)]++
							}
						}
						return true
					})
					return false
				case *ast.Ident:
					if _, ok := mqttPositiveAckTokens[node.Name]; ok {
						if fd, isFn := decl.(*ast.FuncDecl); isFn && fd.Name == node {
							return true // counted as the declaration above
						}
						found[fmt.Sprintf("%s|%s|%s", name, fn, node.Name)]++
					}
				}
				return true
			})
		}
	}
	return found
}

// TestEverySourceAckIsGated pins the positive source-ACK gate's REACH (7a, consult 83): no
// production code names a positive-ack primitive, payload or request kind outside the gate and the
// recorded declarations. A new `jsa.sendAck(...)`, a method value of it, or an equivalent raw
// publication of the positive-ack payload fails here, and so does a stale exemption.
func TestEverySourceAckIsGated(t *testing.T) {
	found := mqttScanPositiveAckSites(t)
	var unaccounted []string
	for site, n := range found {
		if _, ok := mqttPositiveAckSites[site]; !ok {
			unaccounted = append(unaccounted, fmt.Sprintf("%s (x%d)", site, n))
		}
	}
	sort.Strings(unaccounted)
	if len(unaccounted) != 0 {
		t.Fatalf("these sites name a positive-ack primitive outside the gate:\n  %s\n"+
			"Submit through sess.submitSourceAck / submitSourceAckLocked, which refuses a positive "+
			"acknowledgement of a handed-back shared source.", strings.Join(unaccounted, "\n  "))
	}
	var gone []string
	for site := range mqttPositiveAckSites {
		if _, ok := found[site]; !ok {
			gone = append(gone, site)
		}
	}
	sort.Strings(gone)
	if len(gone) != 0 {
		t.Fatalf("these recorded sites no longer exist:\n  %s", strings.Join(gone, "\n  "))
	}
}

// TestNoSupportedRouteCallsStoreInflightRegister (T15): the pre-F-105 writer's only production
// caller is the experimental affine route (chapter P's to convert).
func TestNoSupportedRouteCallsStoreInflightRegister(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var callers []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatal(perr)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fd, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "storeInflightRegister" {
					callers = append(callers, name+"|"+fd.Name.Name)
				}
				return true
			})
		}
	}
	sort.Strings(callers)
	if want := []string{"qos2_shared_affine.go|mqttDeliverQ2AffineCb"}; strings.Join(callers, ",") != strings.Join(want, ",") {
		// The affine caller's enclosing function name is pinned too: a supported route that grew a
		// call inside the affine file under another function would otherwise pass.
		t.Fatalf("storeInflightRegister callers = %v, want only the experimental affine route %v", callers, want)
	}
}

// T13: the selection is deterministic, excludes pending verdicts, and does not depend on order.
func TestSharedDeliveryAcquisitionSelection(t *testing.T) {
	a := mqttSharedAcqRef{Group: "G", Inc: 1, Acq: "aaa"}
	b := mqttSharedAcqRef{Group: "G", Inc: 1, Acq: "bbb"}
	for _, tc := range []struct {
		name    string
		cands   []mqttSharedAcqRef
		pending map[string]struct{}
		want    mqttSharedAcqRef
		ok      bool
	}{
		{"none", nil, nil, mqttSharedAcqRef{}, false},
		{"one", []mqttSharedAcqRef{b}, nil, b, true},
		{"main and up, minimum first", []mqttSharedAcqRef{a, b}, nil, a, true},
		{"main and up, minimum last", []mqttSharedAcqRef{b, a}, nil, a, true},
		{"the minimum has a pending verdict", []mqttSharedAcqRef{a, b}, map[string]struct{}{"aaa": {}}, b, true},
		{"every candidate is releasing", []mqttSharedAcqRef{a, b}, map[string]struct{}{"aaa": {}, "bbb": {}}, mqttSharedAcqRef{}, false},
	} {
		got, ok := mqttSelectSharedAcq(tc.cands, tc.pending)
		if ok != tc.ok || got != tc.want {
			t.Errorf("%s: mqttSelectSharedAcq = %+v, %v; want %+v, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

// T13 (session half): sharedAcqForDeliveryLocked derives each membership's group key the way the
// join does — Main from the filter's subject, Up from the shortened subject — and a pending release
// verdict on the smaller nonce selects the other.
func TestSharedAcqForDeliveryFollowsTheMembership(t *testing.T) {
	const key = "$share/g/foo.>"
	mainQ, upQ := mqttSharedGroupKey("g", "foo.>"), mqttSharedGroupKey("g", "foo")
	sess := &mqttSession{sharedSubs: map[string]mqttSharedMembership{
		key: {QoS: 1, Main: mqttSharedAcquisition{Inc: 3, Acq: "mmm"}, Up: &mqttSharedAcquisition{Inc: 5, Acq: "uuu"}},
	}}
	_, _, mainDur := mqttSharedNames("g", "foo.>", 3)
	_, _, upDur := mqttSharedNames("g", "foo", 5)
	if got, ok := sess.sharedAcqForDeliveryLocked(mainDur); !ok || got != (mqttSharedAcqRef{Group: mainQ, Inc: 3, Acq: "mmm"}) {
		t.Fatalf("main durable selects %+v, %v", got, ok)
	}
	if got, ok := sess.sharedAcqForDeliveryLocked(upDur); !ok || got != (mqttSharedAcqRef{Group: upQ, Inc: 5, Acq: "uuu"}) {
		t.Fatalf("level-up durable selects %+v, %v", got, ok)
	}
	_, _, staleDur := mqttSharedNames("g", "foo.>", 2)
	if got, ok := sess.sharedAcqForDeliveryLocked(staleDur); ok {
		t.Fatalf("another incarnation selected %+v", got)
	}
	sess.acqReleases = map[string]mqttSharedAcqRef{"mmm": {Group: mainQ, Inc: 3, Acq: "mmm"}}
	if got, ok := sess.sharedAcqForDeliveryLocked(mainDur); ok {
		t.Fatalf("an acquisition with a pending release verdict was selected: %+v", got)
	}
}

func testValidSharedAcq() mqttSharedAcqRef {
	return mqttSharedAcqRef{Group: mqttSharedGroupKey("g", "foo"), Inc: 4, Acq: mqttNewInflightNonce()}
}

func testSharedPairExchange(ref *mqttSharedAcqRef) *mqttOutboundExchange {
	return &mqttOutboundExchange{
		ID: "c", Epoch: "e", Gen: mqttNewSessGen(), Nonce: mqttNewInflightNonce(), PI: 7, QoS: 1,
		Phase: mqttExchangePublish, AdmitTxn: mqttNewInflightNonce(), Tenure: mqttNewInflightNonce(),
		SharedAcq: ref,
	}
}

// T12a: the decoder refuses a malformed reference and accepts an absent one; it does not decide
// whether one is REQUIRED (that is the pair rule's).
func TestExchangeDecoderRefusesAMalformedSharedAcq(t *testing.T) {
	good := testValidSharedAcq()
	decode := func(x *mqttOutboundExchange) error {
		data, err := json.Marshal(x)
		if err != nil {
			t.Fatal(err)
		}
		_, derr := mqttDecodeOutboundExchange(data, x.ID, x.Epoch, x.Gen, x.Nonce)
		return derr
	}
	if err := decode(testSharedPairExchange(nil)); err != nil {
		t.Fatalf("an exchange with no shared acquisition was refused: %v", err)
	}
	if err := decode(testSharedPairExchange(&good)); err != nil {
		t.Fatalf("a well-formed shared acquisition was refused: %v", err)
	}
	for name, bad := range map[string]mqttSharedAcqRef{
		"zero incarnation": {Group: good.Group, Inc: 0, Acq: good.Acq},
		"malformed group":  {Group: "short", Inc: 4, Acq: good.Acq},
		"malformed nonce":  {Group: good.Group, Inc: 4, Acq: "x"},
	} {
		bad := bad
		if err := decode(testSharedPairExchange(&bad)); err == nil {
			t.Errorf("%s: the decoder accepted %+v", name, bad)
		}
	}
	x := testSharedPairExchange(&good)
	x.QoS, x.Phase = 2, mqttExchangePublish
	if err := decode(x); err == nil {
		t.Error("a QoS 2 exchange depending on a shared acquisition was accepted")
	}
	// An extra member inside the reference is a shape this broker does not write.
	data, _ := json.Marshal(testSharedPairExchange(&good))
	data = []byte(strings.Replace(string(data), `"acq":"`, `"extra":1,"acq":"`, 1))
	if _, err := mqttDecodeOutboundExchange(data, "c", "e", "", ""); err == nil {
		t.Error("a reference carrying an unknown member was accepted")
	}
}

// T12b: THE pair rule.
func TestSharedRegisterExchangePairRequiresTheExactAcq(t *testing.T) {
	ref := testValidSharedAcq()
	sharedReg := &mqttInflightRecord{Dur: mqttSharedDurableOf(ref), Seq: 9, PI: 7, QoS: 1}
	plainReg := &mqttInflightRecord{Dur: "abc_def", Seq: 9, PI: 7, QoS: 1}
	other := ref
	other.Inc = 5
	for _, tc := range []struct {
		name string
		reg  *mqttInflightRecord
		ref  *mqttSharedAcqRef
		ok   bool
	}{
		{"shared register, its exact acquisition", sharedReg, &ref, true},
		{"shared register, no acquisition", sharedReg, nil, false},
		{"shared register, another incarnation", sharedReg, &other, false},
		{"plain register, no acquisition", plainReg, nil, true},
		{"plain register, an acquisition", plainReg, &ref, false},
	} {
		err := mqttCheckSharedAcqPair(tc.reg, testSharedPairExchange(tc.ref))
		if (err == nil) != tc.ok {
			t.Errorf("%s: mqttCheckSharedAcqPair = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

// T14: every successor exchange carries the reference unchanged — the advance copies the stored
// record and changes only the phase.
func TestExchangeSuccessorsCarrySharedAcq(t *testing.T) {
	ref := testValidSharedAcq()
	x := testSharedPairExchange(&ref)
	next := *x
	next.Phase = mqttExchangeTerminal
	if !mqttSharedAcqRefEqual(next.SharedAcq, &ref) {
		t.Fatal("a phase successor dropped the shared acquisition")
	}
	superseded := *x
	superseded.Phase = mqttExchangeSupersededUnsent
	if !mqttSharedAcqRefEqual(superseded.SharedAcq, &ref) {
		t.Fatal("a superseded predecessor dropped the shared acquisition")
	}
	// Equality compares it by value: two reads of one record are two allocations.
	cp := ref
	y := *x
	y.SharedAcq = &cp
	if !mqttExchangeEqual(x, &y) {
		t.Fatal("equal references compared unequal")
	}
	cp.Inc++
	if mqttExchangeEqual(x, &y) {
		t.Fatal("a different reference compared equal")
	}
}

// T17 (unit): the gate's scope classification — only a $MQTT_msgs reply on a shared durable is in
// the predicate's scope, and an acknowledgement-shaped reply that does not parse is refused rather
// than treated as out of scope.
func TestSourceAckGateScope(t *testing.T) {
	ref := testValidSharedAcq()
	shared := testInflightAckSubject(mqttSharedDurableOf(ref), 12, 1)
	if dur, seq, in, bad := mqttSourceAckScope(shared); !in || bad || dur != mqttSharedDurableOf(ref) || seq != 12 {
		t.Fatalf("shared source reply classified as dur=%q seq=%d shared=%v malformed=%v", dur, seq, in, bad)
	}
	plain := testInflightAckSubject("abc_def", 12, 1)
	if _, _, in, bad := mqttSourceAckScope(plain); in || bad {
		t.Fatalf("ordinary source reply classified shared=%v malformed=%v", in, bad)
	}
	if _, _, in, bad := mqttSourceAckScope("$JS.ACK.broken"); in || !bad {
		t.Fatalf("a malformed acknowledgement subject classified shared=%v malformed=%v", in, bad)
	}
	if _, _, in, bad := mqttSourceAckScope("_INBOX.x"); in || bad {
		t.Fatalf("a non-acknowledgement subject classified shared=%v malformed=%v", in, bad)
	}
}

// T16 (unit): an authorization re-taken for a recovered disposal is judged under the delivery's own
// queue group — a shared durable's, derived from the durable — never under no queue, which skips
// every queue-scoped deny; an unparseable shared durable decides nothing.
//
// Mutation M20: the derivation returns no queue for a shared durable.
func TestSharedDisposalRecoveryHonoursAQueueScopedDeny(t *testing.T) {
	ref := testValidSharedAcq()
	q, ok := mqttDeliveryQueueOf(mqttSharedDurableOf(ref))
	if !ok || q != mqttSharedQueuePrefix+ref.Group {
		t.Fatalf("a shared durable's recovery queue = %q, %v; want %q", q, ok, mqttSharedQueuePrefix+ref.Group)
	}
	if q, ok := mqttDeliveryQueueOf("abc_def"); !ok || q != _EMPTY_ {
		t.Fatalf("an ordinary durable's recovery queue = %q, %v; want none", q, ok)
	}
	if _, ok := mqttDeliveryQueueOf(mqttSharedConsumerDurablePrefix + "garbage"); ok {
		t.Fatal("an unparseable shared durable was given a queue to be judged under")
	}
}
