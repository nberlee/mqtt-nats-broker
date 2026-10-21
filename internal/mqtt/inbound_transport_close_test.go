package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMQTTInboundTransportClosesGoThroughOneHelper keeps the inbound pipeline's abort property
// structural: an accepted client's socket is closed only by closeTransport, which also fires the
// connection's abort latch. A close that bypassed it would leave a readLoop blocked in a drain
// or the subs barrier with its socket already gone, and shutdown's clientWG wait behind it.
//
// It finds every Close call whose receiver is c.rawConn, or a local variable assigned from an
// expression naming rawConn (the alias the service-account invalidation once used), and
// requires it to be inside closeTransport. Setup-time closes of the accepted net.Conn before a
// client exists use other names and are not affected.
func TestMQTTInboundTransportClosesGoThroughOneHelper(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var found int
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			aliases := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for i, rhs := range n.Rhs {
						if i < len(n.Lhs) && mentionsRawConn(rhs) {
							if id, ok := n.Lhs[i].(*ast.Ident); ok {
								aliases[id.Name] = true
							}
						}
					}
				case *ast.CallExpr:
					sel, ok := n.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "Close" {
						return true
					}
					recv := mentionsRawConn(sel.X)
					if id, ok := sel.X.(*ast.Ident); ok && aliases[id.Name] {
						recv = true
					}
					if !recv {
						return true
					}
					found++
					if fn.Name.Name != "closeTransport" {
						t.Errorf("%s: %s closes the client socket directly; route it through closeTransport so the inbound abort latch fires",
							fset.Position(n.Pos()), fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	// The census must have seen the one legitimate close, or it is scanning nothing.
	if found != 1 {
		t.Fatalf("found %d socket closes; want exactly the one inside closeTransport", found)
	}
}

func mentionsRawConn(e ast.Expr) bool {
	var hit bool
	ast.Inspect(e, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "rawConn" {
			hit = true
		}
		return !hit
	})
	return hit
}
