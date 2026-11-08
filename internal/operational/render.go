package operational

import (
	"fmt"
	"strings"
)

// Render is the pinned form of the interface, testdata/interface.txt: every probe answer
// in every combination of its inputs, the exit statuses, the envelope, the field kinds and
// every frozen record.
func Render() string {
	var b strings.Builder
	b.WriteString("# The operational interface (docs/ARCHITECTURE.md, \"The operational interface\").\n")
	b.WriteString("# Regenerate with `make operational-interface-fixture`; never edit by hand.\n\n")
	status, body := Healthz()
	fmt.Fprintf(&b, "healthz %s %d %q\n", HealthzPath, status, body)
	for _, r := range []Readiness{Starting, Serving} {
		for _, stopped := range []bool{false, true} {
			for _, connected := range []bool{false, true} {
				status, body := Readyz(r, stopped, connected)
				fmt.Fprintf(&b, "readyz %s state=%s stopped=%t connected=%t %d %q\n",
					ReadyzPath, readinessName(r), stopped, connected, status, body)
			}
		}
	}
	b.WriteString("\n")
	for _, e := range exitRules {
		fmt.Fprintf(&b, "exit %d %q\n", e.status, e.meaning)
	}
	fmt.Fprintf(&b, "exit fatal-prefix %q\n\n", FatalPrefix)
	for _, e := range envelopeRules {
		fmt.Fprintf(&b, "envelope %s %q\n", e.key, e.meaning)
	}
	b.WriteString("\n")
	for k := range kindNames {
		fmt.Fprintf(&b, "kind %s %q\n", Kind(k), kindEncodings[k])
	}
	b.WriteString("\n")
	for _, r := range Records {
		fmt.Fprintf(&b, "record %q level=%s\n", r.Msg, r.Level)
		for _, f := range r.Fields {
			fmt.Fprintf(&b, "  field %s %s\n", f.Key, f.Kind)
		}
		fmt.Fprintf(&b, "  when %q\n", r.When)
	}
	return b.String()
}

func readinessName(r Readiness) string {
	if r == Serving {
		return "serving"
	}
	return "starting"
}

// The docs/OPERATIONS.md headings each rendered table sits under.
const (
	ReadinessHeading = "### Readiness answers"
	ExitHeading      = "## Exit status"
	EnvelopeHeading  = "### Record envelope"
	KindsHeading     = "### Field kinds"
	RecordsHeading   = "### Records"
)

// DocTables is docs/OPERATIONS.md's statement of the interface: each table, keyed by the
// heading it follows, exactly as the document must carry it.
func DocTables() map[string]string {
	var ready strings.Builder
	ready.WriteString("| `/readyz` answers | When |\n|---|---|\n")
	for _, r := range readyzRules {
		fmt.Fprintf(&ready, "| `%d %s` | %s |\n", r.status, r.body, r.when)
	}
	var exit strings.Builder
	exit.WriteString("| Status | Meaning |\n|---|---|\n")
	for _, e := range exitRules {
		fmt.Fprintf(&exit, "| %d | %s |\n", e.status, e.meaning)
	}
	var env strings.Builder
	env.WriteString("| Key | Value |\n|---|---|\n")
	for _, e := range envelopeRules {
		fmt.Fprintf(&env, "| `%s` | %s |\n", e.key, e.meaning)
	}
	var kinds strings.Builder
	kinds.WriteString("| Kind | Encoded as |\n|---|---|\n")
	for k := range kindNames {
		fmt.Fprintf(&kinds, "| %s | %s |\n", Kind(k), kindEncodings[k])
	}
	var recs strings.Builder
	recs.WriteString("| `msg` | `level` | Fields | When |\n|---|---|---|---|\n")
	for _, r := range Records {
		fs := make([]string, 0, len(r.Fields))
		for _, f := range r.Fields {
			fs = append(fs, fmt.Sprintf("`%s` (%s)", f.Key, f.Kind))
		}
		fl := strings.Join(fs, ", ")
		if fl == "" {
			fl = "none"
		}
		fmt.Fprintf(&recs, "| `%s` | `%s` | %s | %s |\n", r.Msg, r.Level, fl, r.When)
	}
	return map[string]string{
		ReadinessHeading: ready.String(),
		ExitHeading:      exit.String(),
		EnvelopeHeading:  env.String(),
		KindsHeading:     kinds.String(),
		RecordsHeading:   recs.String(),
	}
}
