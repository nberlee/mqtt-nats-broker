// Agreement between the dossier's NUMERIC sections and the engine, by exact rendering.
//
// Four holistic rounds (3, 7, 8, 9) each found a hole in the rule that DISCOVERED which
// figures to check, never in a figure itself: matching anywhere in the document, a whole table
// no check mentioned, a bold token followed by prose, a duplicate row, a denominator nobody
// owned, an unbolded count. Consult 4 ruled the mechanism wrong rather than its implementation
// — "formatting-based discovery cannot support exhaustive figure coverage" — and replaced it
// with this: the numeric sections are RENDERED from measurements and reviewed constants, and
// compared byte-for-byte against the committed document between declared markers. Inside a
// block every character is checked, so a denominator, an unbolded count, a row order and a
// stray space are all covered by construction.
//
// THE TEST NEVER WRITES THE DOSSIER. It renders, compares, and on a mismatch fails and prints
// the expected block for a person to paste and review. A test that rewrote the document would
// be the regenerated baseline consult 2 forbade: the check has to stay one-directional, or the
// document stops being evidence and becomes an echo.
//
// What this does NOT prove, because byte agreement is agreement and not truth: that the
// measurements are right, that the reviewed constants are right, or that the attribution
// reconciles. Those keep their own independent checks.
package mqtt

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// prDossierBlocks is the FIXED specification of which blocks must exist, in this order. Blocks
// are discovered from this list and not from whatever markers the document happens to carry,
// so deleting a marker pair fails rather than silently shrinking the checked region.
var prDossierBlocks = []string{
	"hand-reviewed-tier",
	"computed-closure",
	"measured-deficit",
	"classification-worklist",
	"per-cause-attribution",
	"conversion-withdrawals",
}

func prBlockOpen(name string) string  { return "<!-- checked-block: " + name + " -->" }
func prBlockClose(name string) string { return "<!-- end checked-block: " + name + " -->" }

// The round-6 finding, as REVIEWED HISTORY. Rendering these from current measurements would
// let a future run silently rewrite what round 6 observed — the account would always describe
// today's numbers while claiming to describe that round's. They change only when the history
// is re-reviewed.
const (
	prHistRound6Published = 42  // what the row published: the declared-node population
	prHistRound6Sum       = 155 // what the four rows therefore summed to
	prHistRound6Members   = 147 // the discovered members they were read against
)

// prWithdrawalNotes is the REVIEWED half of the withdrawal table: why a record cannot be
// attributed to one read. A renderer may combine it with the counts beside it, but it may
// never infer or update these, and changing one is an evidence review rather than a re-render.
//
// Those counts come from `prNodeReadOccs`, which is an IMMUTABLE SNAPSHOT of the reads at the
// reviewed revision, not a live measurement — its length is a derived snapshot count. That is
// sound here only because an independent check compares the snapshot against the live
// occurrences, so a read appearing or disappearing fails there rather than silently changing
// this table.
var prWithdrawalNotes = map[string]struct{ Reads, Why string }{
	"outbound_transition.go:(mqttJSA).checkResourceOverwrites": {
		Reads: " (two inside function literals)",
		Why:   "the record names no read, and its reads have different consequences",
	},
	"outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations": {
		Reads: "",
		Why:   "the record names no read, so neither absent arm is the one discharged",
	},
}

// prRenderDossierBlocks renders every checked block. Row order is fixed and map-derived content
// is sorted, because a renderer whose output depends on map iteration would fail at random.
func prRenderDossierBlocks(t *testing.T, sc *censusScan, d prDeficit) map[string]string {
	t.Helper()
	edges, expand, link, discharge := 0, 0, 0, 0
	for _, n := range prCensusNodes {
		for _, e := range n.Edges {
			edges++
			switch e.Disp {
			case edgeExpand:
				expand++
			case edgeLink:
				link++
			default:
				discharge++
			}
		}
	}
	rows := func(pairs ...[2]string) string {
		var b strings.Builder
		b.WriteString("| | |\n|---|---|\n")
		for _, p := range pairs {
			fmt.Fprintf(&b, "| %s | %s |\n", p[0], p[1])
		}
		return b.String()
	}
	n := func(v int) string { return fmt.Sprintf("**%d**", v) }

	out := map[string]string{}

	out["hand-reviewed-tier"] = rows(
		[2]string{"direct per-subject primitive callers (declared nodes)", n(len(prCensusNodes))},
		[2]string{"accounted candidate boundary edges", fmt.Sprintf("**%d** — %d expand, %d link, %d discharge", edges, expand, link, discharge)},
		[2]string{"excluded sites with a per-site F-108 ruling", n(len(prCensusExclusions))},
		[2]string{"named explanations", n(len(prCensusExplanations))},
		[2]string{"retained former-frontier entries", n(len(prFormerFrontier))},
		[2]string{"recorded conversion withdrawals", n(len(prConversionWithdrawn))},
	)

	out["computed-closure"] = rows(
		[2]string{"admitted origins (seed inventory)", n(d.Origins)},
		[2]string{"reached (function, arrival) states", n(d.States)},
		[2]string{"reached triples", n(d.Triples)},
		[2]string{"conservatively reached functions", fmt.Sprintf("**%d** of %d", d.ReachedFns, len(sc.Funcs))},
		[2]string{"reviewed discharges", n(d.Discharged)},
		[2]string{"reviewed propagations", n(d.Propagated)},
	)

	out["measured-deficit"] = rows(
		[2]string{"default-only (edge, origin, arrival) triples", n(d.DefaultPairs)},
		[2]string{"functions carrying at least one", n(d.DefaultFns)},
		[2]string{"unresolved callee obligations", n(d.UnresolvedCallee)},
		[2]string{"possible-only dispatch obligations", n(d.UnresolvedDispatch)},
		[2]string{"unresolved reverse-call obligations", n(d.UnresolvedReverse)},
		[2]string{"unresolved state obligations", n(d.UnresolvedState)},
	)

	// The paragraph is inside the block because it makes numeric claims. Prose is not a reason
	// to leave a figure half-owned, which is why the sum and the difference are rendered too.
	declaredOnly := len(prCensusNodes) - d.DeclaredInWorklist
	out["classification-worklist"] = fmt.Sprintf(`The four rows below are a PARTITION of the discovered members. Note the first: of the **%d**
declared nodes, **%d** are also discovered mechanically, and the other %d are not — they call
no 404 predicate and reference no absence sentinel, so nothing proposes them and they are
accounted only as declared nodes. Publishing %d in this table made the rows sum to %d
against %d members, which holistic round 6 caught.

`, len(prCensusNodes), d.DeclaredInWorklist, declaredOnly,
		prHistRound6Published, prHistRound6Sum, prHistRound6Members) +
		rows(
			[2]string{"discovered members", n(d.WorklistMembers)},
			[2]string{"already declared nodes, also discovered here", n(d.DeclaredInWorklist)},
			[2]string{"dispositioned `interprets`", n(d.Interprets)},
			[2]string{"dispositioned `consumes`", n(d.Consumes)},
			[2]string{"dispositioned `not-an-interpretation`", n(d.NotInterpretations)},
		)

	signed := func(v int) string { return fmt.Sprintf("**%+d**", v) }
	out["per-cause-attribution"] = "| change | default-only triples | unresolved callee | possible-only dispatch |\n" +
		"|---|---|---|---|\n" +
		fmt.Sprintf("| withdrawing all dispatch certainty | %s | %s | %s |\n",
			signed(prAttrWithdrawalTriples), signed(prAttrWithdrawalCallee), signed(prAttrWithdrawalDispatch)) +
		fmt.Sprintf("| the bare-call method filter | %s | %s | %s |\n",
			signed(prAttrFilterTriples), signed(prAttrFilterCallee), signed(prAttrFilterDispatch)) +
		fmt.Sprintf("| net, as published above | %s | %s | %s |\n",
			n(d.DefaultPairs), n(d.UnresolvedCallee), n(d.UnresolvedDispatch)) +
		fmt.Sprintf(`
Withdrawing certainty ADDED triples here, because a formerly proved call now propagates to
every name match. That +%d is an EMPIRICAL result on this package, not a necessary
consequence of the change: how much extra propagation a withdrawn certainty buys depends on
how many name matches the formerly proved calls had. The decrease comes entirely from the
filter, which removes candidate edges that a bare call could never have taken; the %d calls
it empties move from dispatch obligations to callee obligations rather than disappearing.
`, prAttrWithdrawalTriples, prAttrFilterCallee)

	var withdrawn []string
	for fn := range prConversionWithdrawn {
		withdrawn = append(withdrawn, fn)
	}
	sort.Strings(withdrawn)
	var wb strings.Builder
	wb.WriteString("| withdrawn node | reads | why the record cannot be attributed |\n|---|---|---|\n")
	for _, fn := range withdrawn {
		note, ok := prWithdrawalNotes[fn]
		if !ok {
			t.Errorf("%s is a recorded conversion withdrawal with no reviewed reason. The reason "+
				"is evidence and is never inferred, so the renderer cannot supply one", fn)
			continue
		}
		fmt.Fprintf(&wb, "| `%s` | %d%s | %s |\n", prDocIdentity(fn), len(prNodeReadOccs[fn]), note.Reads, note.Why)
	}
	out["conversion-withdrawals"] = wb.String()
	return out
}

// prDossierBounds is THE boundary parser. Both the byte comparison and the scope guard read
// it, because two parsers of one boundary is the shape this run has now corrected four times —
// and here it would be worse than untidy: a guard that reconstructs boundaries its own way can
// exempt a region the comparison never covered. A marker must occupy a COMPLETE line, so text
// beside one cannot fall into the gap between the two readings.
//
// It fails CLOSED. Blocks are discovered from the fixed prDossierBlocks list, never from the
// markers the document happens to carry, and a missing, duplicated, reversed, out-of-order,
// nested or overlapping marker is an error rather than a quietly smaller checked region.
type prDossierBound struct {
	Open, Close int // line indices of the marker lines themselves
}

func prDossierBounds(t *testing.T, lines []string) (map[string]prDossierBound, bool) {
	t.Helper()
	out := map[string]prDossierBound{}
	ok := true
	prevClose := -1
	for _, name := range prDossierBlocks {
		open, close := prBlockOpen(name), prBlockClose(name)
		var opens, closes []int
		for i, l := range lines {
			switch strings.TrimSpace(l) {
			case open:
				opens = append(opens, i)
			case close:
				closes = append(closes, i)
			default:
				// A marker sharing its line with anything else is not a boundary. Accepting
				// one would put the rest of that line outside the compared region while the
				// scope guard treated the whole line as covered.
				if strings.Contains(l, open) || strings.Contains(l, close) {
					t.Errorf("line %d carries a marker for block %q together with other text: "+
						"%q. A marker must occupy a complete line, or the text beside it is "+
						"neither compared nor guarded", i+1, name, l)
					ok = false
				}
			}
		}
		if len(opens) != 1 || len(closes) != 1 {
			t.Errorf("the dossier carries %d opening and %d closing markers for block %q, want "+
				"exactly one of each. A boundary that is missing or duplicated must fail rather "+
				"than leave a region unchecked", len(opens), len(closes), name)
			ok = false
			continue
		}
		if closes[0] < opens[0] {
			t.Errorf("block %q closes (line %d) before it opens (line %d)", name, closes[0]+1, opens[0]+1)
			ok = false
			continue
		}
		if opens[0] <= prevClose {
			t.Errorf("block %q opens at line %d, before the previous block closed at line %d. "+
				"Blocks appear once each, in the order prDossierBlocks declares, and never "+
				"nest or overlap", name, opens[0]+1, prevClose+1)
			ok = false
			continue
		}
		prevClose = closes[0]
		out[name] = prDossierBound{Open: opens[0], Close: closes[0]}
	}
	return out, ok
}

// TestPointReadCensusDossierBlocksRender is the agreement itself. Comparison is byte-for-byte
// with no normalisation: trailing whitespace fails, a changed delimiter fails, a reordered row
// fails. Normalising before comparing would hide exactly the edits this is here to catch.
func TestPointReadCensusDossierBlocksRender(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "findings", "point-read-absence-census.md"))
	if err != nil {
		t.Fatalf("reading the dossier: %v", err)
	}
	doc := string(body)
	if strings.Contains(doc, "\r") {
		t.Fatal("the dossier contains a carriage return; the comparison assumes LF line endings")
	}
	lines := strings.Split(doc, "\n")
	bounds, _ := prDossierBounds(t, lines)
	sc, _ := prEngineOnPackage(t)
	want := prRenderDossierBlocks(t, sc, prMeasureDeficit(t))

	for _, name := range prDossierBlocks {
		w, ok := want[name]
		if !ok {
			t.Errorf("no renderer produces block %q, which prDossierBlocks requires", name)
			continue
		}
		b, ok := bounds[name]
		if !ok {
			continue // prDossierBounds has already reported why
		}
		got := strings.Join(lines[b.Open+1:b.Close], "\n") + "\n"
		if got != w {
			t.Errorf("the dossier's %q block does not match what the engine renders.\n"+
				"--- committed ---\n%s\n--- expected (paste this between the markers) ---\n%s\n"+
				"Every character inside a block is checked, so a denominator, an unbolded "+
				"count, a row order or a stray space all count as a mismatch. This test never "+
				"writes the document: a person pastes the block and reviews the diff.",
				name, got, w)
		}
	}
}

// prUncheckedTables is the ALLOWLIST of tables that may stand outside a checked block,
// identified by their header row. Each is qualitative: it names sites, routes, axes,
// constructs or statuses, and publishes no measured quantity. A table outside a block whose
// header is not here FAILS, which is what makes "a new numeric table must be given an owner" a
// rule rather than a hope — the previous guard only looked for BOLD numbers, so an unbolded
// table, a figure in the first column, and `total **999**` all walked past it.
var prUncheckedTables = map[string]string{
	"| Site | |": "the named interpreting sites, matched by identity elsewhere",
	"| round | the construct that defeated the proof |": "the history of the withdrawn dispatch proof",
	"| Route | Treatment |":                             "the four under-approximation routes",
	"| Axis | Values | Applies to |":                    "the axis vocabulary",
	"| Property | Status |":                             "what is enforced and what is owed",
}

// THE OWNERSHIP CONTRACT, ruled by consult 5. Every line of the dossier is in exactly one of
// three classes, none of them a default:
//
//  1. INSIDE A CHECKED BLOCK — byte-compared against what the engine renders. Authoritative.
//  2. AN EXACT OWNED LINE — excluded from screening because a named check enforces that line
//     VERBATIM. The unit is the whole line: a substring exemption let round 12 smuggle a
//     figure onto a declared line. Naming an owner is not enough, so
//     TestPointReadCensusOwnedLinesAreEnforced proves each named check really does enforce it.
//  3. THE UNCHECKED REMAINDER — everything else, explicitly OUTSIDE authoritative figure
//     verification. It is SCREENED for bold figures and pipe rows, and screening is all it is.
//
// What a clean run therefore means, stated because overstating it is what six review rounds
// kept catching: "no violation of the specified screening rules was detected." It does NOT
// mean every published figure is owned. An undetected class-3 figure is a DOCUMENTED
// limitation of this contract; a figure the screening rules do cover and miss is a defect.
// The dossier's status table carries the same limitation, and so must anything downstream.
var prOwnedLines = map[string]string{
	"this increment started from (**90784015** triples, **4925** unresolved callee, **4895** possible-only": "TestPointReadCensusDossierAgrees requires this exact prefix, built from the attribution constants",
}

// prScreeningExemptions are lines the SCREEN misreads — they are class 3, unchecked, and
// claimed to be nothing else. They are listed separately from prOwnedLines precisely so that
// "the screen skips it" is never confused with "something checks it".
var prScreeningExemptions = map[string]string{
	"  own plan misreading `unsub_drain.go:3287`: a non-404 returns `ok=false`, but a **404": "a PROTOCOL STATUS CODE inside a bolded phrase that spans a line break, not a measurement",
}

// prStripContainers removes the markdown containers a line can sit inside — blockquote
// markers, list bullets, ordered-list numbers — so that recognising a table does not depend on
// the container it is nested in. Rounds 10 and 11 were both container-blindness: a delimiter
// row judged by its first cell, then `> | Metric | Count |`, which renders as a table and
// passes markdownlint. Hand-rolled markdown recognition leaks, so the figure rule below does
// not depend on this at all — this only keeps the structural table rule honest.
func prStripContainers(line string) string {
	l := strings.TrimSpace(line)
	for {
		switch {
		case strings.HasPrefix(l, ">"):
			l = strings.TrimSpace(l[1:])
		case strings.HasPrefix(l, "- "), strings.HasPrefix(l, "* "), strings.HasPrefix(l, "+ "):
			l = strings.TrimSpace(l[2:])
		default:
			if i := strings.Index(l, ". "); i > 0 && i <= 3 && strings.Trim(l[:i], "0123456789") == "" {
				l = strings.TrimSpace(l[i+2:])
				continue
			}
			return l
		}
	}
}

// The guarantee is exhaustive agreement WITHIN the declared blocks. Byte comparison cannot
// discover a numeric claim published outside them, so this is the scope guard consult 4 asked
// for — and it is STRUCTURAL rather than formatting-based, because the formatting-based
// version was itself bypassable three times. Every table either lives in a block or is named
// here as qualitative, and no cell of an allowlisted table may publish a bold figure, in any
// column, anywhere in the cell.
//
// PROSE is outside this guarantee, deliberately and explicitly: the dossier's status table
// says so, and quantities appearing in prose are declared uncovered rather than described as
// checked.
func TestPointReadCensusScreensLinesOutsideBlocks(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "findings", "point-read-absence-census.md"))
	if err != nil {
		t.Fatalf("reading the dossier: %v", err)
	}
	lines := strings.Split(string(body), "\n")
	bounds, _ := prDossierBounds(t, lines)
	inside := map[int]bool{}
	for _, b := range bounds {
		for i := b.Open; i <= b.Close; i++ {
			inside[i] = true
		}
	}

	// A table is a contiguous run of pipe lines, SEPARATOR INCLUDED: `|---|---|` is part of
	// the table, and treating it as a break made every row after it look like a new table's
	// header. The header is the run's first line.
	// FORMAT-AGNOSTIC, and the rule that actually holds the line: no bold figure may appear
	// anywhere outside a block, in any container, unless its line is declared. This does not
	// ask what a table is, which is the question that leaked in rounds 10 and 11.
	for i, line := range lines {
		if inside[i] {
			continue
		}
		// EXACT lines, never substrings: round 12 rewrapped a paragraph so a declared
		// fragment carried a new figure beside it, and the whole line inherited the exemption.
		if _, owned := prOwnedLines[line]; owned {
			continue
		}
		if _, exempt := prScreeningExemptions[line]; exempt {
			continue
		}
		for _, tok := range strings.Split(line, "**") {
			if tok == "" || !prIsBoldNum("**"+tok+"**") {
				continue
			}
			t.Errorf("line %d carries the figure **%s** outside every checked block: %q. A "+
				"figure is published wherever it is written — in a table, a blockquote, a list "+
				"or a sentence — so it belongs in a block (class 1), or its EXACT line is "+
				"declared in prOwnedLines with the check that enforces it (class 2), or in "+
				"prScreeningExemptions if the screen is misreading it (class 3, unchecked)",
				i+1, tok, strings.TrimSpace(line))
			break
		}
	}

	var header string
	inTable := false
	for i, line := range lines {
		trimmed := prStripContainers(line)
		if !strings.HasPrefix(trimmed, "|") {
			inTable = false
			continue
		}
		if !inTable {
			header = trimmed
		}
		inTable = true
		if inside[i] {
			continue
		}
		// Every pipe row is inspected for a figure, INCLUDING one that looks like a delimiter.
		// Skipping delimiters first is what let `| --- | **999** |` publish an unchecked number
		// in an allowlisted table: the row is a delimiter by its first cell and a figure by its
		// second, and only one of those was being read.
		cells := prPipeCells(trimmed)
		_, isRow := prDossierCells(trimmed)
		if _, allowed := prUncheckedTables[header]; !allowed && isRow {
			t.Errorf("line %d belongs to a table outside every checked block whose header is "+
				"%q. Either a block owns it, or it is named in prUncheckedTables as carrying "+
				"no measured quantity — an unowned table is how an unchecked figure gets "+
				"published", i+1, header)
			continue
		}
		for col, c := range cells {
			for _, tok := range strings.Split(c, "**") {
				if tok == "" || !prIsBoldNum("**"+tok+"**") {
					continue
				}
				t.Errorf("line %d publishes **%s** in column %d of an allowlisted QUALITATIVE "+
					"table (%q). A table carrying a measured quantity belongs in a checked "+
					"block", i+1, tok, col, header)
				break
			}
		}
	}
}

// prPipeCells splits any pipe row into cells WITHOUT judging whether it is a delimiter. The
// scope guard needs every cell of every row, because a row can be a delimiter in one cell and
// a published figure in the next.
func prPipeCells(line string) []string {
	l := strings.TrimSpace(line)
	if !strings.HasPrefix(l, "|") {
		return nil
	}
	var cells []string
	for _, c := range strings.Split(strings.Trim(l, "|"), "|") {
		cells = append(cells, strings.TrimSpace(c))
	}
	return cells
}

// TestPointReadCensusOwnedLinesAreEnforced is the half of class 2 that makes it a class rather
// than a hole: an owned line is excluded from screening because a named check enforces it
// VERBATIM, so this proves the named check exists and really does require that text. Consult
// 5: "A named check must actually enforce that line; naming an owner alone is insufficient."
func TestPointReadCensusOwnedLinesAreEnforced(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "findings", "point-read-absence-census.md"))
	if err != nil {
		t.Fatalf("reading the dossier: %v", err)
	}
	lines := strings.Split(string(body), "\n")

	// The owned SET must equal the set of lines an owner actually enforces — today exactly
	// one, the attribution baseline, whose owner is TestPointReadCensusDossierAgrees and whose
	// text is derived from the same constants that check requires. Equality is the point: with
	// a membership test instead, any existing line could be added with an invented owner name
	// and would be exempted from screening while nothing checked it.
	supported := map[string]string{
		prAttributionBaselineLine(): "TestPointReadCensusDossierAgrees requires this exact line, built from the attribution constants",
	}
	for owned := range prOwnedLines {
		if _, ok := supported[owned]; !ok {
			t.Errorf("prOwnedLines declares %q, and no owner in this package enforces it. An "+
				"owned line is exempt from screening BECAUSE something checks it verbatim, so "+
				"an unsupported declaration is an unchecked figure with a note attached", owned)
		}
	}
	for want, owner := range supported {
		if _, ok := prOwnedLines[want]; !ok {
			t.Errorf("%s enforces the line %q, which prOwnedLines does not declare. The two "+
				"must agree, or the screen inspects a line its owner already checks, or worse "+
				"exempts one nobody does", owner, want)
		}
	}

	present := func(s string) bool {
		for _, l := range lines {
			if l == s {
				return true
			}
		}
		return false
	}
	for owned, why := range prOwnedLines {
		if !present(owned) {
			t.Errorf("prOwnedLines declares %q, owned by %q, but the dossier has no line "+
				"exactly equal to it. A declaration for a line that no longer exists silently "+
				"loosens the screen", owned, why)
		}
	}
	for exempt, why := range prScreeningExemptions {
		if !present(exempt) {
			t.Errorf("prScreeningExemptions declares %q (%q), but the dossier has no line "+
				"exactly equal to it — a stale exemption is an exemption nobody reviewed",
				exempt, why)
		}
		if _, dup := prOwnedLines[exempt]; dup {
			t.Errorf("%q is declared BOTH owned and screening-exempt. The two lists mean "+
				"opposite things — one says a check enforces the line, the other says the "+
				"screen misreads an unchecked one — and a line in both is unreviewable", exempt)
		}
	}
}
