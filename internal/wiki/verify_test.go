package wiki

import (
	"strings"
	"testing"
)

// Verify reads output as the wiki serves it, so these hand it pages directly rather
// than through the renderer whose mistakes it exists to catch.
func TestVerifyRefusesWhatTheWikiCannotServe(t *testing.T) {
	pinned := "https://github.com/o/r/blob/" + testSHA + "/"
	root := tree(t, map[string]string{"NOTICE": "line one\nline two\n", "docs/notes.md": "# Notes\n\n## Real heading\n",
		"docs/hidden.md": "# Hidden\n\n<!--\n## Ghost\n-->\n"}).Root
	tracked := []string{"NOTICE", "docs/notes.md", "docs/hidden.md"}
	good := Output{
		"Home.md": []byte("# Home\n\n## Start\n\n[a](A#part) [self](#start) [n](" + pinned + "NOTICE) " +
			"[h](" + pinned + "docs/notes.md#real-heading) [i](https://github.com/o/r/issues/1) " +
			"<https://example.com/a>\n"),
		"A.md":       []byte("# A\n\n## Part\n"),
		"_Footer.md": []byte("footer\n"),
	}
	if err := Verify(good, root, "o/r", testSHA, tracked, 1000); err != nil {
		t.Fatalf("a servable wiki was refused: %v", err)
	}
	for _, c := range []struct{ link, want string }{
		{"[a](A.md#part)", "a wiki link names a page, not a file path"},
		{"[a](docs/A)", "a wiki link names a page, not a file path"},
		{"[a](Missing)", `no page "Missing"`},
		{"[a](_Footer)", `no page "_Footer"`},
		{"[a](A#nowhere)", `page "A" renders no anchor #nowhere`},
		{"[a](#nowhere)", `page "Home" renders no anchor #nowhere`},
		{"[n](https://github.com/o/r/blob/main/NOTICE)", `pinned to "main", not the rendered commit`},
		{"[n](" + pinned + "GONE)", `no tracked file "GONE"`},
		{"[n](https://github.com/o/r/tree/" + testSHA + "/nodir)", `no tracked directory "nodir"`},
		{"[h](" + pinned + "docs/notes.md#renamed-heading)", "docs/notes.md has no heading with the anchor #renamed-heading"},
		{"[n](" + pinned + "NOTICE#top)", "a link to NOTICE, which is not Markdown, carries no anchor"},
		{"[n](" + pinned + "NOTICE#L1)", "a link to NOTICE, which is not Markdown, carries no anchor"},
		{"[h](" + pinned + "docs/hidden.md#ghost)", "its anchors cannot be certified: docs/hidden.md:3: raw HTML is not supported"},
		{"[n](https://GitHub.com/O/R/blob/main/NOTICE)", `pinned to "main", not the rendered commit`},
		{"<https://github.com/o/r/blob/main/NOTICE>", `pinned to "main", not the rendered commit`},
		{"https://www.github.com/o/r/tree/main/docs", `pinned to "main", not the rendered commit`},
		{"[r](https://raw.githubusercontent.com/o/r/" + testSHA + "/NOTICE)", "a raw view of a repository file is not a supported link"},
		{"[b](https://github.com/o/r/blame/" + testSHA + "/NOTICE)", "a blame view of a repository file is not a supported link"},
		{"<a href=\"Missing\">x</a>", "raw HTML is not supported"},
		{"[[Missing]]", "the wiki's [[...]] syntax is not supported"},
		{"[d](https://github.com/o/r/tree/" + testSHA + "/docs#x)", "a directory has no anchors"},
	} {
		t.Run(c.link, func(t *testing.T) {
			out := Output{"Home.md": []byte("# Home\n\n" + c.link + "\n"), "A.md": good["A.md"], "_Footer.md": good["_Footer.md"]}
			err := Verify(out, root, "o/r", testSHA, tracked, 1000)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Verify = %v, want an error containing %q", err, c.want)
			}
		})
	}
	t.Run("over budget", func(t *testing.T) {
		err := Verify(Output{"Home.md": []byte("# Home\n" + big(2000))}, root, "o/r", testSHA, nil, 1000)
		if err == nil || !strings.Contains(err.Error(), "Home.md: 2007 bytes, over the 1000-byte page budget") {
			t.Fatalf("Verify = %v", err)
		}
	})
}
