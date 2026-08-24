// The census behind TestSharedGroupCreateCensus.
//
// It answers one question about the production sources: which functions call
// createDurableConsumer, and which of those construct a ConsumerConfig carrying a
// DeliverGroup inline. That is the class F-077 arbitrates, and a create outside
// processSharedJSConsumer would be unarbitrated.
//
// WHAT THIS CANNOT SEE, stated here rather than left for a reader to assume. The scan
// follows a request built as a composite literal at the call site or assigned to a local in
// the same function. A config passed in as a parameter, built by a helper, or copied out of
// a map escapes it. So a failure means the class grew; a pass does NOT mean the class is
// closed (feedback_quantitative_claims_drift).
package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// testCreateDurableConsumerCallers maps each production function that calls
// createDurableConsumer to whether that function also constructs a ConsumerConfig with a
// non-empty DeliverGroup field inline.
func testCreateDurableConsumerCallers() (map[string]bool, error) {
	entries, err := os.ReadDir(".")
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if perr != nil {
			return nil, perr
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var calls, group bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.SelectorExpr:
					if v.Sel != nil && v.Sel.Name == "createDurableConsumer" {
						calls = true
					}
					// A config built by the canonical group-config constructor is a group
					// config, and that is how processSharedJSConsumer builds one: the
					// DeliverGroup field is named inside buildSharedGroupConsumerConfig, not
					// at the create site. Following the constructor is what keeps the guard
					// pointed at the code it claims to watch.
					if v.Sel != nil && v.Sel.Name == "buildSharedGroupConsumerConfig" {
						group = true
					}
				case *ast.Ident:
					if v.Name == "buildSharedGroupConsumerConfig" {
						group = true
					}
				case *ast.KeyValueExpr:
					// Or a ConsumerConfig literal that names DeliverGroup directly. The
					// value is not evaluated: naming the field is what puts the create in
					// the class.
					if k, kok := v.Key.(*ast.Ident); kok && k.Name == "DeliverGroup" {
						group = true
					}
				}
				return true
			})
			if !calls {
				continue
			}
			// Definition of createDurableConsumer itself is not a caller of it.
			if fn.Name != nil && fn.Name.Name == "createDurableConsumer" {
				continue
			}
			if fn.Name != nil {
				out[fn.Name.Name] = out[fn.Name.Name] || group
			}
		}
	}
	return out, nil
}
