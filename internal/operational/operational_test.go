package operational

import (
	"bytes"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The operational interface is pinned by testdata/interface.txt, rendered from the
// definition; the tests compare bytes and never write it. `make
// operational-interface-fixture` regenerates it with the change it records
// (docs/ARCHITECTURE.md, "The operational interface").
const interfaceFixture = "testdata/interface.txt"

func TestOperationalInterfaceIsPinned(t *testing.T) {
	want, err := os.ReadFile(interfaceFixture)
	if err != nil {
		t.Fatal(err)
	}
	if got := Render(); got != string(want) {
		t.Fatalf("the operational interface differs from %s; a change to it regenerates the pin "+
			"(make operational-interface-fixture) and updates the docs in the same change.\ngot:\n%s", interfaceFixture, got)
	}
}

func TestWriteOperationalInterfaceFixture(t *testing.T) {
	if os.Getenv("MQTTNB_WRITE_OPERATIONAL_INTERFACE_FIXTURE") != "1" {
		t.Skip("set MQTTNB_WRITE_OPERATIONAL_INTERFACE_FIXTURE=1 (make operational-interface-fixture) to regenerate " + interfaceFixture)
	}
	if err := os.WriteFile(interfaceFixture, []byte(Render()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestOperationsDocumentStatesTheOperationalInterface: docs/OPERATIONS.md carries each table
// the definition renders, verbatim, under its heading, so a cell edited on either side fails.
func TestOperationsDocumentStatesTheOperationalInterface(t *testing.T) {
	data, err := os.ReadFile("../../docs/OPERATIONS.md")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	for heading, table := range DocTables() {
		start := -1
		for i, l := range lines {
			if l == heading {
				if start >= 0 {
					t.Fatalf("%q appears twice", heading)
				}
				start = i
			}
		}
		if start < 0 {
			t.Errorf("docs/OPERATIONS.md has no %q heading", heading)
			continue
		}
		i := start + 1
		for i < len(lines) && !strings.HasPrefix(lines[i], "|") {
			if strings.HasPrefix(lines[i], "#") {
				break
			}
			i++
		}
		j := i
		for j < len(lines) && strings.HasPrefix(lines[j], "|") {
			j++
		}
		if got := strings.Join(lines[i:j], "\n") + "\n"; got != table {
			t.Errorf("the table under %q is not the definition's; want:\n%s\ngot:\n%s", heading, table, got)
		}
	}
}

type stringer string

func (s stringer) String() string { return string(s) }

// TestRecordLogEncodesEachFieldByItsKind: each field carries its own value, under its own
// key, in the JSON type of its kind, whatever Go type the producer held.
func TestRecordLogEncodesEachFieldByItsKind(t *testing.T) {
	r := Record{Msg: "m", Level: slog.LevelWarn, Fields: []Field{
		{"s", String}, {"a", String}, {"i", Integer}, {"u", Integer}, {"b", Bool},
		{"d", Duration}, {"t", Time}, {"x", Text}, {"e", Text},
	}}
	at := time.Date(2026, 11, 8, 12, 0, 0, 5, time.UTC)
	var buf bytes.Buffer
	r.Log(slog.New(slog.NewJSONHandler(&buf, nil)),
		Str("one"), Addr(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1883}), Int(-3), Uint(4), Flag(true),
		Dur(1500*time.Millisecond), At(at), Reason("why"), Err(errors.New("boom")))
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"s": "one", "a": "127.0.0.1:1883", "i": float64(-3), "u": float64(4), "b": true,
		"d": float64(1500 * time.Millisecond), "t": "2026-11-08T12:00:00.000000005Z", "x": "why", "e": "boom",
		"level": "WARN", "msg": "m",
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %#v, want %#v", k, m[k], v)
		}
	}
	if len(m) != len(want)+1 { // and time
		t.Errorf("keys %v, want exactly %d", m, len(want)+1)
	}

	buf.Reset()
	Record{Msg: "m", Level: slog.LevelInfo, Fields: []Field{{"d", Duration}, {"n", Integer}}}.
		Log(slog.New(slog.NewJSONHandler(&buf, nil)), Str("1s"))
	m = nil
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if m["d"] != BadValue || m["n"] != Missing {
		t.Errorf("misused fields d=%v n=%v, want %s and %s", m["d"], m["n"], BadValue, Missing)
	}
}

// constructorKinds is the kind each Value constructor produces, independent of the
// constructors themselves, for the census.
var constructorKinds = map[string]Kind{
	"Str": String, "Addr": String, "Int": Integer, "Uint": Integer, "Flag": Bool,
	"Dur": Duration, "At": Time, "Reason": Text, "Err": Text,
}

// TestEveryConstructorProducesItsKind keeps constructorKinds honest.
func TestEveryConstructorProducesItsKind(t *testing.T) {
	for name, v := range map[string]Value{
		"Str": Str(""), "Addr": Addr(stringer("")), "Int": Int(0), "Uint": Uint(0), "Flag": Flag(false),
		"Dur": Dur(0), "At": At(time.Time{}), "Reason": Reason(""), "Err": Err(nil),
	} {
		if v.kind != constructorKinds[name] {
			t.Errorf("%s produces %s, the census expects %s", name, v.kind, constructorKinds[name])
		}
	}
}

// producers is every frozen record's producers: the functions whose bodies log it, one entry
// per call site. A producer moved, added or reverted to a literal changes this table.
var producers = map[string][]string{
	"BrokerStarting":                           {"cmd/mqtt-nats-broker.runContext"},
	"HealthServerListening":                    {"cmd/mqtt-nats-broker.startHTTP"},
	"HealthServerError":                        {"cmd/mqtt-nats-broker.startHTTP"},
	"ProfilerListening":                        {"cmd/mqtt-nats-broker.startPprof"},
	"ProfilerBindFailed":                       {"cmd/mqtt-nats-broker.startPprof"},
	"ProfilerError":                            {"cmd/mqtt-nats-broker.startPprof"},
	"EmbeddedNATSReady":                        {"cmd/mqtt-nats-broker.startEmbeddedNATS"},
	"WaitingForJetStream":                      {"internal/backend.waitJetStream"},
	"ConnectedToNATS":                          {"internal/backend.Connect"},
	"MQTTInitializationRetry":                  {"internal/mqtt.(*Server).initializeWithRetries"},
	"StartupStopped":                           {"cmd/mqtt-nats-broker.startupOutcome"},
	"MQTTListenerStarted":                      {"cmd/mqtt-nats-broker.serveMQTT"},
	"WebSocketListenerStarted":                 {"cmd/mqtt-nats-broker.serveMQTT"},
	"ShutdownSignalReceived":                   {"cmd/mqtt-nats-broker.runContext"},
	"DrainDelayWaiting":                        {"cmd/mqtt-nats-broker.runContext"},
	"ServiceConnectionError":                   {"internal/backend.Connect"},
	"ServiceConnectionUnidentified":            {"internal/mqtt.(*Server).armServiceIdentityHandlers"},
	"ServiceConnectionDroppedWhileIdentifying": {"internal/mqtt.(*Server).armServiceIdentityHandlers"},
	"ServiceAccountChanged":                    {"internal/mqtt.(*Server).armServiceIdentityHandlers"},
	"ConnectRefusedAccountUnknown":             {"internal/mqtt.(*Server).authorizeClient"},
	"ConnectRefused":                           {"internal/mqtt.(*Server).authorizeClient", "internal/mqtt.(*Server).authorizeClient"},
	"TLSReloaded":                              {"internal/tlsreload.(*Source).check"},
	"TLSReloadRefused":                         {"internal/tlsreload.(*Source).refuse"},
	"TLSCertificateExpired":                    {"internal/tlsreload.(*Source).warnIfActiveExpired"},
	"ServiceConnectionCloseTimedOut":           {"internal/backend.(*Backend).Close"},
	"ServiceConnectionClosed":                  {"internal/backend.(*Backend).Close"},
}

// convertedTexts are the messages the frozen records replaced; a producer reverted to one
// would still log, so its literal is refused outright.
var convertedTexts = []string{
	"MQTT: refusing CONNECT for client",
	"MQTT: initialization attempt",
	"MQTT: the service connection reconnected into account",
	"MQTT: the service connection reconnected but could not be identified, so",
	"MQTT: the service connection dropped again",
}

// definedRecords maps each frozen record's Go name to its definition, read from this
// package's source: the name a producer writes is the name the census resolves.
func definedRecords(t *testing.T) map[string]*Record {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "operational.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	byMsg := map[string]*Record{}
	for _, r := range Records {
		byMsg[r.Msg] = r
	}
	out := map[string]*Record{}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, v := range vs.Values {
				cl, ok := v.(*ast.CompositeLit)
				if !ok || len(cl.Elts) == 0 {
					continue
				}
				if id, ok := cl.Type.(*ast.Ident); !ok || id.Name != "Record" {
					continue
				}
				lit, ok := cl.Elts[0].(*ast.BasicLit)
				if !ok {
					t.Fatalf("%s's message is not a literal", vs.Names[i].Name)
				}
				msg, _ := strconv.Unquote(lit.Value)
				r, ok := byMsg[msg]
				if !ok {
					t.Errorf("record %s is defined but not in Records", vs.Names[i].Name)
					continue
				}
				out[vs.Names[i].Name] = r
			}
		}
	}
	if len(out) != len(Records) {
		t.Fatalf("%d records defined, %d in Records", len(out), len(Records))
	}
	return out
}

// productionFiles is every non-test Go file of the broker outside this package.
func productionFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	for _, root := range []string{"../../cmd/mqtt-nats-broker", "../../internal"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "testdata" || path == "../../internal/operational") {
				return filepath.SkipDir
			}
			if !d.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(files) < 50 {
		t.Fatalf("only %d production files found: the walk is wrong, and the census would pass vacuously", len(files))
	}
	return files
}

func funcName(dir string, fd *ast.FuncDecl) string {
	name := fd.Name.Name
	if fd.Recv != nil && len(fd.Recv.List) == 1 {
		typ := fd.Recv.List[0].Type
		star := ""
		if s, ok := typ.(*ast.StarExpr); ok {
			star, typ = "*", s.X
		}
		if id, ok := typ.(*ast.Ident); ok {
			name = "(" + star + id.Name + ")." + name
		}
	}
	return dir + "." + name
}

// TestEveryFrozenRecordHasItsNamedProducers: every frozen record is logged through its
// definition, by exactly its named producers, with one value per field built by the
// constructor of that field's kind; no producer writes a frozen message, or a message a
// frozen record replaced, past the definition.
func TestEveryFrozenRecordHasItsNamedProducers(t *testing.T) {
	defined := definedRecords(t)
	got := map[string][]string{}
	for _, path := range productionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dir := strings.TrimPrefix(filepath.ToSlash(filepath.Dir(path)), "../../")
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				s, _ := strconv.Unquote(lit.Value)
				for _, r := range Records {
					if strings.HasPrefix(s, r.Msg) {
						t.Errorf("%s: the literal %q writes the frozen message %q past its definition", fset.Position(lit.Pos()), s, r.Msg)
					}
				}
				for _, c := range convertedTexts {
					if strings.HasPrefix(s, c) {
						t.Errorf("%s: the literal %q is the text a frozen record replaced", fset.Position(lit.Pos()), s)
					}
				}
			}
			return true
		})
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			producer := funcName(dir, fd)
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Log" {
					return true
				}
				rec, ok := sel.X.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if pkg, ok := rec.X.(*ast.Ident); !ok || pkg.Name != "operational" {
					return true
				}
				pos := fset.Position(call.Pos())
				r, ok := defined[rec.Sel.Name]
				if !ok {
					t.Errorf("%s: operational.%s is not a frozen record", pos, rec.Sel.Name)
					return true
				}
				got[rec.Sel.Name] = append(got[rec.Sel.Name], producer)
				values := call.Args[1:]
				if len(values) != len(r.Fields) {
					t.Errorf("%s: %s logged with %d values, it has %d fields", pos, rec.Sel.Name, len(values), len(r.Fields))
					return true
				}
				for i, v := range values {
					c, ok := v.(*ast.CallExpr)
					var ctor string
					if ok {
						if cs, ok := c.Fun.(*ast.SelectorExpr); ok {
							if p, ok := cs.X.(*ast.Ident); ok && p.Name == "operational" {
								ctor = cs.Sel.Name
							}
						}
					}
					kind, known := constructorKinds[ctor]
					if !known || kind != r.Fields[i].Kind {
						t.Errorf("%s: %s's field %s (%s) is given %q, not a constructor of its kind",
							pos, rec.Sel.Name, r.Fields[i].Key, r.Fields[i].Kind, ctor)
					}
				}
				return true
			})
		}
	}
	for name := range defined {
		want := append([]string(nil), producers[name]...)
		have := append([]string(nil), got[name]...)
		sort.Strings(want)
		sort.Strings(have)
		if strings.Join(want, "|") != strings.Join(have, "|") {
			t.Errorf("%s is produced by %v, want %v", name, have, want)
		}
	}
	for name := range producers {
		if _, ok := defined[name]; !ok {
			t.Errorf("the producer table names %s, which is not a frozen record", name)
		}
	}
}
