package tlsreload

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"
)

// jsonRecords decodes every record a JSON handler wrote whose msg is msg.
func jsonRecords(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("not a JSON record: %q", sc.Text())
		}
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func requireShape(t *testing.T, m map[string]any, level string, fields map[string]any) {
	t.Helper()
	if m["level"] != level {
		t.Errorf("%v: level %v, want %s", m["msg"], m["level"], level)
	}
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	want := []string{"time", "level", "msg"}
	for k, v := range fields {
		want = append(want, k)
		if got, ok := m[k]; ok && got != v {
			t.Errorf("%v: %s = %#v, want %#v", m["msg"], k, got, v)
		}
	}
	sort.Strings(keys)
	sort.Strings(want)
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Errorf("%v: keys %v, want %v", m["msg"], keys, want)
	}
}

// TestTLSRecordsCarryTheirFrozenFields: the reload, refusal and expiry records are the
// operational interface's (docs/OPERATIONS.md, "Records"): their messages, levels, keys
// and values, the time a string and the generation a number.
func TestTLSRecordsCarryTheirFrozenFields(t *testing.T) {
	a, b, other := newGeneration(t), newGeneration(t), newGeneration(t)
	var buf bytes.Buffer
	proj := newProjection(t, a.contents())
	clk := newClock(time.Now())
	src := newSource(proj.files(false), slog.New(slog.NewJSONHandler(&buf, nil)))
	src.now = clk.now
	if err := src.load(); err != nil {
		t.Fatal(err)
	}

	proj.swap(b.contents())
	if out := src.Check(); out != Applied {
		t.Fatalf("Check = %v, want applied", out)
	}
	reloaded := jsonRecords(t, &buf, "MQTT TLS configuration reloaded")
	if len(reloaded) != 1 {
		t.Fatalf("%d reload records, want 1", len(reloaded))
	}
	notAfter := src.cur.Load().notAfter.Format(time.RFC3339Nano)
	requireShape(t, reloaded[0], "INFO", map[string]any{"subject": reloaded[0]["subject"], "serial": b.serial,
		"not_after": notAfter, "generation": float64(2)})
	if s, _ := reloaded[0]["subject"].(string); s == "" {
		t.Errorf("subject %v, want the leaf's subject as a string", reloaded[0]["subject"])
	}

	proj.swap(contents{cert: a.cert, key: other.key})
	src.Check()
	src.Check()
	refused := jsonRecords(t, &buf, "MQTT TLS reload refused; keeping the active configuration")
	if len(refused) != 1 {
		t.Fatalf("%d refusal records for one standing refusal, want 1", len(refused))
	}
	requireShape(t, refused[0], "ERROR", map[string]any{"err": refused[0]["err"]})
	if s, _ := refused[0]["err"].(string); s == "" {
		t.Errorf("err %v, want the refusal as text", refused[0]["err"])
	}

	clk.set(src.cur.Load().notAfter.Add(time.Hour))
	src.Check()
	src.Check()
	expired := jsonRecords(t, &buf, "MQTT TLS certificate has expired and no valid replacement is on disk")
	if len(expired) != 1 {
		t.Fatalf("%d expiry records, want 1", len(expired))
	}
	requireShape(t, expired[0], "WARN", map[string]any{"subject": reloaded[0]["subject"], "serial": b.serial,
		"not_after": notAfter})
}
