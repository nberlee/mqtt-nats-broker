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
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"
	"testing"
)

// TestMQTTInboundFailureRunsOnJoinedGoroutines keeps structural the premise the reader's handed-
// over drain rests on (mqttDrainHandedOffClose): the failure position runs only on the inbound
// pipe's own goroutines, every one counted in its wait group, so once mqttInboundClose has joined
// them the handoff obligation is final. A failure position reachable from the readLoop, from an
// untracked goroutine or from a callback would let the reader read "not owed" and close under a
// DISCONNECT still being written.
//
// The pipe's methods are identified by type, not by name (drainPlan.drop and wsConn.fail are other
// methods). The package is type-checked from source with its imports stubbed out: only the
// selectors on the package's own types need resolving.
func TestMQTTInboundFailureRunsOnJoinedGoroutines(t *testing.T) {
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
	conf := types.Config{Importer: stubImporter{}, Error: func(error) {}}
	_, _ = conf.Check("mqtt", fset, files, info)

	// The allowed callers of each protected method, by enclosing top-level function.
	allowed := map[string]map[string]bool{
		"fail":         {"emit": true},
		"emit":         {"issuer": true, "markReady": true},
		"markReady":    {"issueOrdered": true, "runHold": true, "runRelease": true, "drop": true},
		"drop":         {"runHold": true, "runRelease": true},
		"issueOrdered": {"issuer": true},
	}
	// The pipe's tracked launches: a go statement of one of these methods, or the completion
	// closure inside issueOrdered.
	launched := map[string]bool{"issuer": true, "runHold": true, "runRelease": true}

	pipeMethod := func(sel *ast.SelectorExpr) string {
		s := info.Selections[sel]
		if s == nil {
			return ""
		}
		fn, ok := s.Obj().(*types.Func)
		if !ok {
			return ""
		}
		recv := fn.Type().(*types.Signature).Recv()
		if recv == nil {
			return ""
		}
		ptr, ok := recv.Type().(*types.Pointer)
		if !ok {
			return ""
		}
		named, ok := ptr.Elem().(*types.Named)
		if !ok || named.Obj().Name() != "mqttInboundPipe" {
			return ""
		}
		return fn.Name()
	}

	var calls, launches int
	for _, f := range files {
		file := fset.Position(f.Pos()).Filename
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			owner := fd.Name.Name
			var stack []ast.Node
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				parent := ast.Node(nil)
				if len(stack) > 0 {
					parent = stack[len(stack)-1]
				}
				stack = append(stack, n)
				pos := fset.Position(n.Pos())
				switch n := n.(type) {
				case *ast.GoStmt:
					tracked := false
					if sel, ok := n.Call.Fun.(*ast.SelectorExpr); ok && launched[pipeMethod(sel)] {
						tracked = true
					}
					if lit, ok := n.Call.Fun.(*ast.FuncLit); ok && owner == "issueOrdered" {
						tracked = true
						if !mqttDefersWGDone(lit.Body) {
							t.Errorf("%s: the completion closure does not defer p.wg.Done()", pos)
						}
					}
					if file == "inbound_pipeline.go" && !tracked {
						t.Errorf("%s: an untracked goroutine in the inbound pipeline", pos)
					}
					if tracked {
						launches++
						if !mqttPrecededByWGAdd(stack) {
							t.Errorf("%s: a pipe goroutine launched without p.wg.Add(1) just before it", pos)
						}
					}
				case *ast.SelectorExpr:
					m := pipeMethod(n)
					if allowed[m] == nil {
						return true
					}
					call, ok := parent.(*ast.CallExpr)
					if !ok || call.Fun != n {
						t.Errorf("%s: %s used as a value, which can run it outside the pipe's goroutines", pos, m)
						return true
					}
					calls++
					if !allowed[m][owner] {
						t.Errorf("%s: %s called from %s, outside its allowed callers", pos, m, owner)
					}
					// Inside a closure: only the completion closure of issueOrdered, which is a
					// tracked launch.
					for _, a := range stack[:len(stack)-1] {
						if _, ok := a.(*ast.FuncLit); ok && owner != "issueOrdered" {
							t.Errorf("%s: %s called inside a closure", pos, m)
						}
					}
				}
				return true
			})
			if launched[owner] && pipeMethodDecl(fd) && !mqttDefersWGDone(fd.Body) {
				t.Errorf("%s: %s does not defer p.wg.Done()", fset.Position(fd.Pos()), owner)
			}
		}
	}
	// A type-check that resolved nothing would pass every rule vacuously.
	if calls < 16 || launches != 4 {
		t.Fatalf("the scan resolved %d protected calls and %d tracked launches; want at least 16 and exactly 4", calls, launches)
	}
}

type stubImporter struct{}

func (stubImporter) Import(path string) (*types.Package, error) {
	return nil, errors.New("stubbed: " + path)
}

func pipeMethodDecl(fd *ast.FuncDecl) bool {
	if fd.Recv == nil || len(fd.Recv.List) != 1 {
		return false
	}
	star, ok := fd.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	id, ok := star.X.(*ast.Ident)
	return ok && id.Name == "mqttInboundPipe"
}

// mqttIsWGCall reports whether s is the statement p.wg.<method>(args...).
func mqttIsWGCall(s ast.Stmt, method string) bool {
	var call *ast.CallExpr
	switch s := s.(type) {
	case *ast.ExprStmt:
		call, _ = s.X.(*ast.CallExpr)
	case *ast.DeferStmt:
		call = s.Call
	}
	if call == nil {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != method {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "wg"
}

// mqttPrecededByWGAdd reports whether the go statement on top of stack immediately follows
// p.wg.Add(1) in its block or case clause.
func mqttPrecededByWGAdd(stack []ast.Node) bool {
	g := stack[len(stack)-1]
	for i := len(stack) - 2; i >= 0; i-- {
		var list []ast.Stmt
		switch b := stack[i].(type) {
		case *ast.BlockStmt:
			list = b.List
		case *ast.CaseClause:
			list = b.Body
		default:
			continue
		}
		for j, s := range list {
			if s == g {
				return j > 0 && mqttIsWGCall(list[j-1], "Add")
			}
		}
		return false
	}
	return false
}

// mqttDefersWGDone reports whether body starts with defer p.wg.Done(): DEFERRED, so the count
// covers everything the goroutine does, and a bare Done would let it outlive the join.
func mqttDefersWGDone(body *ast.BlockStmt) bool {
	if len(body.List) == 0 {
		return false
	}
	_, deferred := body.List[0].(*ast.DeferStmt)
	return deferred && mqttIsWGCall(body.List[0], "Done")
}
