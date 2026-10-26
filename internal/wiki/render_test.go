package wiki

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const testSHA = "0123456789abcdef0123456789abcdef01234567"

// tree writes files under a temporary root and returns a Config over them; every
// file is tracked.
func tree(t *testing.T, files map[string]string) Config {
	t.Helper()
	root := t.TempDir()
	var tracked []string
	for name, body := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		tracked = append(tracked, name)
	}
	sort.Strings(tracked)
	return Config{Root: root, Repo: "o/r", SHA: testSHA, Tracked: tracked}
}

func manifest(entries ...string) string {
	return "# Wiki\n\nIntro.\n\n## Pages\n\n" + strings.Join(entries, "\n") + "\n"
}

func mustRender(t *testing.T, cfg Config) Output {
	t.Helper()
	out, err := Render(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func requirePage(t *testing.T, out Output, name string) string {
	t.Helper()
	b, ok := out[name]
	if !ok {
		t.Fatalf("no page %s; have %v", name, out.sortedNames())
	}
	return string(b)
}

func requireContains(t *testing.T, page, body, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Errorf("%s does not contain %q:\n%s", page, want, body)
	}
}

func requireError(t *testing.T, cfg Config, want string) {
	t.Helper()
	_, err := Render(cfg)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Render error = %v, want one containing %q", err, want)
	}
}

// big is a paragraph of n bytes.
func big(n int) string { return strings.Repeat("x", n-1) + "\n" }

func TestFittingDocumentIsOnePage(t *testing.T) {
	cfg := tree(t, map[string]string{
		"docs/WIKI.md":  manifest("- [Guide](GUIDE.md)"),
		"docs/GUIDE.md": "# Guide\n\n## One\n\ntext\n\n## Two\n\ntext\n",
	})
	out := mustRender(t, cfg)
	body := requirePage(t, out, "Guide.md")
	requireContains(t, "Guide.md", body, "## Two")
	if _, ok := out["Guide-One.md"]; ok {
		t.Error("a document within budget was split")
	}
}

func TestOversizedDocumentSplitsAtH2AndDescends(t *testing.T) {
	cfg := tree(t, map[string]string{
		"docs/WIKI.md": manifest("- [Guide](GUIDE.md)"),
		"docs/GUIDE.md": "# Guide\n\nIntro.\n\n## Small\n\ntext\n\n" +
			"## Large\n\nlarge intro\n\n### Part A\n\n" + big(600) + "\n### Part B\n\n" + big(600),
	})
	cfg.Budget = 1000
	out := mustRender(t, cfg)
	requireContains(t, "Guide.md", requirePage(t, out, "Guide.md"), "- [Small](Guide-Small)\n- [Large](Guide-Large)\n")
	requireContains(t, "Guide-Small.md", requirePage(t, out, "Guide-Small.md"), "## Small\n\ntext\n")
	large := requirePage(t, out, "Guide-Large.md")
	requireContains(t, "Guide-Large.md", large, "- [Part A](Guide-Part-A)\n- [Part B](Guide-Part-B)\n")
	if strings.Contains(large, "xxxx") {
		t.Error("the split H2's page carries its H3 sections")
	}
	requireContains(t, "Guide-Part-B.md", requirePage(t, out, "Guide-Part-B.md"), "### Part B\n")
}

func TestIndivisibleOversizedSectionIsAnError(t *testing.T) {
	cfg := tree(t, map[string]string{
		"docs/WIKI.md":  manifest("- [Guide](GUIDE.md)"),
		"docs/GUIDE.md": "# Guide\n\n## Huge\n\n" + big(1200) + "\n## Small\n\ntext\n",
	})
	cfg.Budget = 1000
	requireError(t, cfg, `docs/GUIDE.md: section "Huge" renders to`)
}

// A page that fits as source but not once its links are rewritten is split: the
// budget is measured on the final page.
func TestBudgetIsMeasuredAfterLinkExpansion(t *testing.T) {
	links := strings.Repeat("[n](../NOTICE) ", 6) // each grows by the pinned URL, 66 bytes
	cfg := tree(t, map[string]string{
		"NOTICE":        "notice\n",
		"docs/WIKI.md":  manifest("- [Guide](GUIDE.md)"),
		"docs/GUIDE.md": "# Guide\n\n## A\n\n" + links + "\n\n## B\n\n" + links + "\n",
	})
	cfg.Budget = 700
	if src := len("# Guide\n\n## A\n\n" + links + "\n\n## B\n\n" + links + "\n"); src >= cfg.Budget/2 {
		t.Fatalf("fixture: source is %d bytes, not well under the budget", src)
	}
	out := mustRender(t, cfg)
	if _, ok := out["Guide-A.md"]; !ok {
		t.Fatalf("a document under budget as source but over it rendered was not split; pages %v", out.sortedNames())
	}
}

func TestOversizedParentIntroductionIsAnError(t *testing.T) {
	cfg := tree(t, map[string]string{
		"docs/WIKI.md":  manifest("- [Guide](GUIDE.md)"),
		"docs/GUIDE.md": "# Guide\n\n## Parent\n\n" + big(1200) + "\n### Child\n\ntext\n",
	})
	cfg.Budget = 1000
	requireError(t, cfg, `section "Parent" renders to`)
}

// Two split pages with the same heading text each render it as #same: anchors are
// counted per page, and a link to the second occurrence follows it there.
func TestDuplicateHeadingAnchorsArePerPage(t *testing.T) {
	cfg := tree(t, map[string]string{
		"docs/WIKI.md": manifest("- [Guide](GUIDE.md)", "- [Other](OTHER.md)"),
		"docs/GUIDE.md": "# Guide\n\n## First\n\n### Same\n\n" + big(600) +
			"\n## Second\n\n### Same\n\n" + big(600),
		"docs/OTHER.md": "# Other\n\nSee [the second](GUIDE.md#same-1) and [the first](GUIDE.md#same).\n",
	})
	cfg.Budget = 1000
	out := mustRender(t, cfg)
	other := requirePage(t, out, "Other.md")
	requireContains(t, "Other.md", other, "[the second](Guide-Second#same)")
	requireContains(t, "Other.md", other, "[the first](Guide-First#same)")
}

func TestLinksAreRewrittenForTheWiki(t *testing.T) {
	cfg := tree(t, map[string]string{
		"NOTICE":       "notice\n",
		"test/x/a.txt": "a\n",
		"docs/WIKI.md": manifest("- [Guide](GUIDE.md)", "- [Deploy](deploy/kubernetes/README.md)",
			"- [Chart](../charts/c/README.md)"),
		"docs/GUIDE.md": "# Guide\n\n## Setup\n\n" +
			"[notice](../NOTICE) [dir](../test/x) [deploy](deploy/kubernetes/README.md#before-applying) " +
			"[self](#setup) [abs](/NOTICE) [ext](https://example.com/a.md) ![img](../NOTICE \"title\")\n\n" +
			"A [wrapped\nlink](deploy/kubernetes/README.md) and `[code](NOWHERE.md)`.\n\n" +
			"```text\n[fenced](NOWHERE.md)\n```\n\n" +
			"| a | b |\n|---|---|\n| [cell](CHART.md) | x |\n",
		"docs/CHART.md":                    "# Not in the manifest\n",
		"docs/deploy/kubernetes/README.md": "# Deploy\n\n## Before applying\n\n[chart](../../../charts/c/README.md)\n",
		"charts/c/README.md":               "# Chart\n\n[back](../../docs/deploy/kubernetes/README.md#before-applying)\n",
	})
	out := mustRender(t, cfg)
	g := requirePage(t, out, "Guide.md")
	blob := "https://github.com/o/r/blob/" + testSHA + "/"
	for _, want := range []string{
		"[notice](" + blob + "NOTICE)",
		"[dir](https://github.com/o/r/tree/" + testSHA + "/test/x)",
		"[deploy](Deploy#before-applying)",
		"[self](Guide#setup)",
		"[abs](" + blob + "NOTICE)",
		"[ext](https://example.com/a.md)",
		"![img](" + blob + "NOTICE \"title\")",
		"[wrapped\nlink](Deploy)",
		"`[code](NOWHERE.md)`",
		"[fenced](NOWHERE.md)",
		"| [cell](" + blob + "docs/CHART.md) | x |",
	} {
		requireContains(t, "Guide.md", g, want)
	}
	requireContains(t, "Deploy.md", requirePage(t, out, "Deploy.md"), "[chart](Chart)")
	requireContains(t, "Chart.md", requirePage(t, out, "Chart.md"), "[back](Deploy#before-applying)")
}

func TestBrokenLinksAreErrors(t *testing.T) {
	for _, c := range []struct{ link, want string }{
		{"[x](MISSING.md)", `link "MISSING.md": no such file in the repository`},
		{"[x](OTHER.md#nowhere)", "docs/OTHER.md has no heading with the anchor #nowhere"},
		{"[x](#nowhere)", "docs/GUIDE.md has no heading with the anchor #nowhere"},
		{"[x](../../outside)", "points outside the repository"},
		// The renderer names the source line to fix; Verify, reading the output, could only
		// name the wiki page.
		{"[x](notes/n.md#renamed)", `docs/GUIDE.md:3: link "notes/n.md#renamed": docs/notes/n.md has no heading with the anchor #renamed`},
		// A target outside the published subset has no certified anchors: its heading
		// inside a comment is one the parse would see and GitHub would not render.
		{"[x](notes/hidden.md#ghost)", `docs/GUIDE.md:3: link "notes/hidden.md#ghost": its anchors cannot be certified: docs/notes/hidden.md:3: raw HTML is not supported`},
		{"[x](notes/refs.md#real)", `docs/GUIDE.md:3: link "notes/refs.md#real": its anchors cannot be certified: docs/notes/refs.md: reference-style link and footnote definitions are not supported`},
		{"[x](../NOTICE#top)", `docs/GUIDE.md:3: link "../NOTICE#top": a link to NOTICE, which is not Markdown, carries no anchor`},
		{"[x](../NOTICE#L1)", `docs/GUIDE.md:3: link "../NOTICE#L1": a link to NOTICE, which is not Markdown, carries no anchor`},
		{"[x](notes#real)", `docs/GUIDE.md:3: link "notes#real": a directory has no anchors`},
	} {
		t.Run(c.link, func(t *testing.T) {
			cfg := tree(t, map[string]string{
				"docs/WIKI.md":         manifest("- [Guide](GUIDE.md)", "- [Other](OTHER.md)"),
				"docs/GUIDE.md":        "# Guide\n\n" + c.link + "\n",
				"docs/OTHER.md":        "# Other\n",
				"docs/notes/n.md":      "# Notes\n\n## Real\n",
				"docs/notes/hidden.md": "# Hidden\n\n<!--\n## Ghost\n-->\n",
				"docs/notes/refs.md":   "# Refs\n\n## Real\n\n[r]: https://example.com\n",
				"NOTICE":               "notice\n",
			})
			requireError(t, cfg, c.want)
		})
	}
}

func TestManifestIsRefusedWhenMalformed(t *testing.T) {
	for _, c := range []struct{ entries, want string }{
		{"- Guide", "a Pages entry must be exactly one link"},
		{"- [Guide](GUIDE.md) and more", "a Pages entry must be exactly one link"},
		{"- [Gone](GONE.md)", `"GONE.md" is not a tracked Markdown file`},
		{"- [Guide](GUIDE.md)\n- [Again](GUIDE.md)", "docs/GUIDE.md is listed twice"},
	} {
		t.Run(c.entries, func(t *testing.T) {
			cfg := tree(t, map[string]string{
				"docs/WIKI.md":  manifest(c.entries),
				"docs/GUIDE.md": "# Guide\n",
			})
			requireError(t, cfg, c.want)
		})
	}
}

// TestUnsupportedMarkdownIsRefused: outside code, a link comes only from an inline link
// or image or an autolink, and a heading is plain text and code spans. Everything else
// GitHub would turn into a link or an anchor is refused, wherever it sits.
func TestUnsupportedMarkdownIsRefused(t *testing.T) {
	for _, c := range []struct{ name, body, want string }{
		{"reference definition", "[x]: https://example.com\n", "reference-style link and footnote definitions are not supported ([x]: ...)"},
		{"reference definition in a blockquote", "> [x]: https://example.com\n", "reference-style link and footnote definitions are not supported ([x]: ...)"},
		{"reference definition in a list item", "- item\n\n  [x]: https://example.com\n", "reference-style link and footnote definitions are not supported ([x]: ...)"},
		{"multiline reference definition", "[x]:\n  https://example.com\n", "reference-style link and footnote definitions are not supported ([x]: ...)"},
		{"footnote", "Text[^1].\n\n[^1]: a note\n", "docs/GUIDE.md:3: footnotes are not supported"},
		{"HTML block", "<div>\n<a href=\"MISSING.md\">x</a>\n</div>\n", "docs/GUIDE.md:3: raw HTML is not supported"},
		{"inline HTML link", "See <a href=\"#missing\">here</a>.\n", "docs/GUIDE.md:3: raw HTML is not supported"},
		{"HTML comment", "<!--\n## Ghost\n-->\n", "docs/GUIDE.md:3: raw HTML is not supported"},
		{"wiki link", "See [[Other Page]].\n", "docs/GUIDE.md:3: the wiki's [[...]] syntax is not supported"},
		{"autolink into this repository", "<https://github.com/o/r/blob/main/NOTICE>\n", "autolink https://github.com/o/r/blob/main/NOTICE points into this repository"},
		{"bare URL into this repository", "See https://github.com/O/R/blob/main/NOTICE today.\n", "autolink https://github.com/O/R/blob/main/NOTICE points into this repository"},
		{"entity in a destination", "[x](https://github&#46;com/o/r/blob/main/NOTICE)\n", "uses an escape or entity; write it literally"},
		{"escape in a destination", "[x](OTHER\\_x.md)\n", "uses an escape or entity; write it literally"},
		{"heading with emphasis", "## A *bold* claim\n", "a heading may hold only text and code spans, not Emphasis"},
		{"heading with a link", "## See [A `code`](https://example.com)\n", "a heading may hold only text and code spans, not Link"},
		{"heading with an entity", "## A &amp; B\n", `heading "A &amp; B" uses an escape or entity`},
		{"heading with a non-ASCII letter", "## \u03a3igma\n", `outside the decorations a heading may carry`},
		{"heading in a blockquote", "> ## Quoted\n", `the headings the rewriter found differ from the document's at #2: rewriter "(none)", document "2 Quoted"`},
		{"setext heading", "Title\n=====\n", `the headings the rewriter found differ from the document's at #2: rewriter "(none)", document "1 Title"`},
		// A backtick the scanner would pair across lines, around a block that ends the
		// paragraph: the scanner hides the link GitHub renders. The heading form is refused
		// on its own (a link in a heading); the list form only because the two readings
		// disagree.
		{"code span boundary across a heading", "unmatched `\n# [bad](MISSING.md)\nclosing `\n", "a heading may hold only text and code spans, not Link"},
		{"code span boundary across a list", "unmatched `\n- [bad](MISSING.md)\nclosing `\n", `the link destinations the rewriter found differ from the document's at #1: rewriter "(none)", document "MISSING.md"`},
		{"unterminated fence", "```go\nunterminated\n", "unterminated ``` fence"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := tree(t, map[string]string{
				"docs/WIKI.md":  manifest("- [Guide](GUIDE.md)"),
				"docs/GUIDE.md": "# Guide\n\n" + c.body,
				"NOTICE":        "notice\n",
			})
			requireError(t, cfg, c.want)
		})
	}
}

// An image inside a link's label is a link destination too, and is rewritten.
func TestNestedImageIsRewritten(t *testing.T) {
	cfg := tree(t, map[string]string{
		"NOTICE":        "notice\n",
		"docs/WIKI.md":  manifest("- [Guide](GUIDE.md)", "- [Other](OTHER.md)"),
		"docs/GUIDE.md": "# Guide\n\n[![badge](../NOTICE)](OTHER.md#part)\n",
		"docs/OTHER.md": "# Other\n\n## Part\n",
	})
	out := mustRender(t, cfg)
	requireContains(t, "Guide.md", requirePage(t, out, "Guide.md"),
		"[![badge](https://github.com/o/r/blob/"+testSHA+"/NOTICE)](Other#part)")
}

func TestPublicationRecordOnlyForAReleaseTag(t *testing.T) {
	files := map[string]string{"docs/WIKI.md": manifest("- [Guide](GUIDE.md)"), "docs/GUIDE.md": "# Guide\n"}
	out := mustRender(t, tree(t, files))
	if _, ok := out[PublicationFile]; ok {
		t.Error("a render without a tag carries a publication record")
	}
	cfg := tree(t, files)
	cfg.Tag = "v0.1.0-alpha.1"
	out = mustRender(t, cfg)
	if got, want := string(out[PublicationFile]), "tag v0.1.0-alpha.1\ncommit "+testSHA+"\n"; got != want {
		t.Errorf("record = %q, want %q", got, want)
	}
	requireContains(t, "_Footer.md", requirePage(t, out, "_Footer.md"), "release `v0.1.0-alpha.1`")
	cfg.Tag = "v1.0"
	requireError(t, cfg, `"v1.0" is not a release tag`)
}

// TestRepositoryDocsRenderAsWiki renders this repository's documentation, so a doc
// change that breaks a link or an anchor, or grows a section past the page budget,
// fails the default test run.
func TestRepositoryDocsRenderAsWiki(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out, err := Render(Config{Root: root, Repo: "nberlee/mqtt-nats-broker", SHA: testSHA})
	if err != nil {
		t.Fatal(err)
	}
	sidebar := requirePage(t, out, "_Sidebar.md")
	for _, name := range out.sortedNames() {
		page, ok := strings.CutSuffix(name, ".md")
		if !ok || strings.HasPrefix(page, "_") || page == "Home" {
			continue
		}
		if !strings.Contains(sidebar, "]("+page+")") {
			t.Errorf("_Sidebar.md does not list %s", page)
		}
	}
	for _, page := range []string{"Architecture.md", "Conformance.md", "Authentication-and-authorization.md", "Provenance.md"} {
		requirePage(t, out, page)
	}
	if _, ok := out["Conformance-Known-deviations.md"]; !ok {
		t.Error("Conformance's Known deviations section is over budget today and was not split")
	}
}
