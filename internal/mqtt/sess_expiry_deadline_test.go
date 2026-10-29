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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"
)

// A session's disconnect is stamped in whole seconds, so the instant its Session Expiry Interval
// has elapsed is only known to within a second; it is dated conservatively, never early.

// TestSessionExpiryDeadlineIsNeverEarly: for a disconnect at any fraction of a second, the deadline
// derived from its truncated stamp is never before the true disconnect plus the interval and at
// most one second after it; a session is not overdue a hair before its interval has really
// elapsed, and is overdue once the deadline is reached.
func TestSessionExpiryDeadlineIsNeverEarly(t *testing.T) {
	for _, interval := range []uint32{1, 5, 60} {
		for f := 0; f < 100; f++ {
			disc := time.Unix(1000, int64(f)*int64(10*time.Millisecond))
			stamp := disc.Unix()
			truth := disc.Add(time.Duration(interval) * time.Second)
			deadline := mqttSessionExpiryDeadline(stamp, interval)
			if deadline.Before(truth) {
				t.Fatalf("interval %d, disconnect %v: deadline %v precedes the true expiry %v", interval, disc, deadline, truth)
			}
			if deadline.Sub(truth) > time.Second {
				t.Fatalf("interval %d, disconnect %v: deadline %v is more than a second after %v", interval, disc, deadline, truth)
			}
			if mqttSessionOverdue(stamp, interval, truth.Add(-10*time.Millisecond)) {
				t.Fatalf("interval %d, disconnect %v: overdue 10 ms before its interval elapsed", interval, disc)
			}
			if !mqttSessionOverdue(stamp, interval, deadline) {
				t.Fatalf("interval %d, disconnect %v: not overdue at its own deadline %v", interval, disc, deadline)
			}
			if !mqttSessionOverdue(stamp, interval, truth.Add(time.Second)) {
				t.Fatalf("interval %d, disconnect %v: not overdue a second after its interval elapsed", interval, disc)
			}
		}
	}
	for _, c := range []struct {
		stamp    int64
		interval uint32
	}{{0, 5}, {1000, 0}, {1000, mqttSessionNeverExpire}} {
		if mqttSessionOverdue(c.stamp, c.interval, time.Unix(1<<40, 0)) {
			t.Fatalf("stamp %d interval %d: a connected, zero-interval or never-expiring session is never overdue here",
				c.stamp, c.interval)
		}
	}
}

// TestSessionExpiryHasOneDefinition: no production function but mqttSessionExpiryDeadline adds a
// session's interval to its disconnect stamp, and each decision and timer that dates an expiry from
// a stamp asks the helpers.
func TestSessionExpiryHasOneDefinition(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"sess_expiry.go|reconcileRemoteSessionExpiry":  false,
		"sess_expiry.go|expireSessionFired":            false,
		"sess_expiry.go|loadExpiringSessionRecord":     false,
		"sess_expiry.go|sweepSessionExpiries":          false,
		"connect_process.go|mqttProcessConnect":        false,
		"sess_manager.go|createOrRestoreSessionWithin": false,
		"sess_uncertain.go|expiryDeadline":             false,
		"conn_lifecycle.go|mqttHandleClosedClient":     false,
	}
	stampy := func(e ast.Expr) bool {
		found := false
		ast.Inspect(e, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && strings.EqualFold(id.Name, "disconnectedAt") {
				found = true
			}
			if sel, ok := n.(*ast.SelectorExpr); ok && strings.EqualFold(sel.Sel.Name, "disconnectedAt") {
				found = true
			}
			return !found
		})
		return found
	}
	fset := token.NewFileSet()
	for _, de := range entries {
		name := de.Name()
		if de.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatal(perr)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.BinaryExpr:
					if v.Op == token.ADD && (stampy(v.X) || stampy(v.Y)) && fn.Name.Name != "mqttSessionExpiryDeadline" {
						t.Errorf("%s: %s adds to a disconnect stamp; date an expiry through mqttSessionExpiryDeadline",
							fset.Position(v.Pos()), fn.Name.Name)
					}
				case *ast.CallExpr:
					if c := testCalleeName(v); c == "mqttSessionExpiryDeadline" || c == "mqttSessionOverdue" {
						if _, ok := want[name+"|"+fn.Name.Name]; ok {
							want[name+"|"+fn.Name.Name] = true
						}
					}
				}
				return true
			})
		}
	}
	for site, asked := range want {
		if !asked {
			t.Errorf("%s dates a session's expiry without asking mqttSessionExpiryDeadline or mqttSessionOverdue", site)
		}
	}
}
