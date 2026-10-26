package wiki

import (
	"strconv"
	"strings"
	"unicode"
)

// GitHub gives every rendered heading an anchor. Its rule is the one github-slugger
// v2.0.0 reproduces (commit 3461c43, script/generate-regex.js), and that package's
// fixtures were captured from GitHub's own renderer, so they are this file's oracle
// (testdata/github-slugger-fixtures.json).
//
// Two differences from upstream are known, and the documentation this package
// renders meets neither: upstream strips by Unicode 13.0.0 general categories while
// Go's tables are newer, so a code point assigned after 13.0 is kept here where
// upstream (as unassigned, Cn) drops it; and JavaScript lower-cases a word-final
// capital sigma to final sigma, which this does not.

// slugStripped reports whether github-slugger removes r: the general categories No,
// Pe, Pf, Pi, Ps, Po, Pd, S*, Cc, Co, Cf, Cn and Z*, except Alphabetic code points,
// the ASCII hyphen and the ASCII space.
func slugStripped(r rune) bool {
	if r == '-' || r == ' ' {
		return false
	}
	if unicode.In(r, unicode.L, unicode.Nl, unicode.Other_Alphabetic) {
		return false // Alphabetic = L + Nl + Other_Alphabetic
	}
	if unicode.In(r, unicode.No, unicode.Pe, unicode.Pf, unicode.Pi, unicode.Ps, unicode.Po,
		unicode.Pd, unicode.S, unicode.Cc, unicode.Co, unicode.Cf, unicode.Z) {
		return true
	}
	// Cn, unassigned: no category table claims it.
	return !unicode.In(r, unicode.L, unicode.M, unicode.N, unicode.P, unicode.S, unicode.C, unicode.Z)
}

// slugOf is the anchor GitHub derives from a heading's rendered text, before
// deduplication.
func slugOf(text string) string {
	var b strings.Builder
	// JavaScript's toLowerCase applies Unicode's unconditional special casing, of which
	// the dotted capital I is the one lower-case entry: it becomes i + combining dot.
	lower := strings.ToLower(strings.ReplaceAll(text, "\u0130", "i\u0307"))
	for _, r := range lower {
		switch {
		case slugStripped(r):
		case r == ' ':
			b.WriteByte('-')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// slugger hands out the anchors of one rendered page in heading order. A candidate
// any earlier heading already produced, naturally or by suffix, is bumped with the
// counter of the slug it started from until it is unused: "foo", "foo", "foo-1"
// become foo, foo-1, foo-1-1.
type slugger struct {
	occurrences map[string]int
}

func newSlugger() *slugger { return &slugger{occurrences: map[string]int{}} }

func (s *slugger) slug(text string) string {
	original := slugOf(text)
	result := original
	for {
		if _, taken := s.occurrences[result]; !taken {
			break
		}
		s.occurrences[original]++
		result = original + "-" + strconv.Itoa(s.occurrences[original])
	}
	s.occurrences[result] = 0
	return result
}
