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

// The drain census (F-105 row 2, consult 28).
//
// The PUBREL output boundary decides write entry under the session lock and then writes under wmu
// with no holder read, and that is sound for ONE reason: no first drain of a holder can run while
// its frame is being written. Every entry into the drain is the acquiring goroutine before it hands
// the holder off, the delivery callback's deferred drain after the synchronous write returned, or a
// reconcile/shutdown pass that only a drain which already ran can enrol.
//
// This records that set. A new or moved call site — or a method value taken so a call can happen
// somewhere this scan cannot follow — fails, and whoever adds it must say why it cannot run during
// a write. What it cannot prove, stated so it is not mistaken for more: an unchanged site count is
// not an unchanged ORDERING. Moving the callback's deferred drain ahead of the write keeps every
// count; the token-retention test is what catches that.
var mqttDrainEntrySites = map[string]mqttOutputSite{
	"pubrel_action.go|mqttAcquirePubRelAction|startActionDrain":                {1, "the acquiring goroutine's unwind defer, before hand-off"},
	"pubrel_action.go|acquireActionFromPublish|startActionDrain":               {1, "the acquiring goroutine's unwind defer, before hand-off"},
	"pubrel_action.go|resolveUnacquiredAction|startActionDrain":                {3, "the acquiring goroutine resolving its own failed acquisition"},
	"pubrel_action.go|startActionDrain|scheduleActionReconcile":                {3, "a drain that already ran enrols its own retry"},
	"pubrel_action.go|scheduleActionReconcile|noteOwedAction":                  {1, "a drain that already ran hands itself to shutdown"},
	"pubrel_action.go|scheduleActionReconcile|startActionDrain":                {1, "the reconcile timer, enrolled only by a drain"},
	"mqtt.go|mqttDeliverPubRelCb|startActionDrain":                             {1, "the delivery callback's deferred drain, after the synchronous write returns"},
	"outbound_retained_replay_emit.go|emitRetainedPubRelOnce|startActionDrain": {1, "a retained PUBREL's emission (F-105 7c): the same deferred drain, after the same synchronous write (mqttWriteAuthorizedPubRel) returns"},
	"shim_public.go|finishOwedActions|startActionDrain":                        {1, "the shutdown pass over drains a drain enrolled"},
	"shim_public.go|runShutdown|finishOwedActions":                             {1, "the shutdown sequence, after the send barriers drained"},
}

func TestF105DrainEntriesCannotRunDuringAWrite(t *testing.T) {
	names := map[string]struct{}{"startActionDrain": {}, "scheduleActionReconcile": {},
		"noteOwedAction": {}, "finishOwedActions": {}}
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
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				// EVERY SELECTOR, called or not: a method value (`f := sess.startActionDrain`)
				// moves the call somewhere this scan cannot follow, so it is recorded where taken.
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if _, want := names[sel.Sel.Name]; want {
						found[fmt.Sprintf("%s|%s|%s", name, fd.Name.Name, sel.Sel.Name)]++
					}
				}
				return true
			})
		}
	}
	if len(found) == 0 {
		t.Fatalf("the scan found no drain entries at all, so it is not reaching the sources")
	}
	var bad []string
	for site, n := range found {
		rec, ok := mqttDrainEntrySites[site]
		if !ok || rec.Times != n {
			bad = append(bad, fmt.Sprintf("%s (recorded %d, found %d)", site, rec.Times, n))
		}
	}
	for site := range mqttDrainEntrySites {
		if _, ok := found[site]; !ok {
			bad = append(bad, site+" (recorded, no longer present)")
		}
	}
	sort.Strings(bad)
	if len(bad) != 0 {
		t.Fatalf("the drain entry census changed:\n  %s\nA drain that can run while a holder's frame is "+
			"being written would let the durable token go while bytes may still be going out. Record the "+
			"site with why it cannot, or move it.", strings.Join(bad, "\n  "))
	}
}
