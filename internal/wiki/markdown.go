package wiki

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// The scanner locates what the wiki rewrites: ATX headings, fenced code, code spans,
// and inline link and image destinations, by byte offset. It is not the authority on
// what a document holds; the GFM parse in admit.go is, and every source must read the
// same through both (render.go), so a construct the scanner misreads fails the render
// instead of being rewritten wrongly.

// mdDoc is one Markdown source split into lines, with the fenced lines marked.
type mdDoc struct {
	path   string
	src    string
	starts []int  // byte offset of each line
	fenced []bool // the line is a fence delimiter or inside a fence
}

type heading struct {
	line  int
	level int
	text  string // rendered text, what the anchor is derived from
}

// mdLink is an inline link or image; [start,end) is its destination in the source.
type mdLink struct {
	start, end int
	dest       string
}

var (
	atxHeading = regexp.MustCompile(`^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$`)
	fenceOpen  = regexp.MustCompile("^[ \t]*(`{3,}|~{3,})")
)

// parseMarkdown splits src into lines and marks its fences.
func parseMarkdown(path, src string) (*mdDoc, error) {
	d := &mdDoc{path: path, src: src}
	for i := 0; i < len(src); {
		d.starts = append(d.starts, i)
		j := strings.IndexByte(src[i:], '\n')
		if j < 0 {
			break
		}
		i += j + 1
	}
	d.fenced = make([]bool, len(d.starts))
	fence := ""
	for i := range d.starts {
		line := d.line(i)
		if fence != "" {
			d.fenced[i] = true
			t := strings.TrimRight(strings.TrimLeft(line, " \t"), " \t")
			if strings.Trim(t, fence[:1]) == "" && len(t) >= len(fence) {
				fence = ""
			}
			continue
		}
		if m := fenceOpen.FindStringSubmatch(line); m != nil && (m[1][0] == '~' || !strings.Contains(line[len(m[0]):], "`")) {
			fence = m[1]
			d.fenced[i] = true
		}
	}
	if fence != "" {
		return nil, fmt.Errorf("%s: unterminated %s fence", path, fence)
	}
	return d, nil
}

// repoAnchors reads the anchors GitHub renders for Markdown files in the repository,
// so a link into a file the wiki does not publish is checked like one into a page.
// Only an admitted file's anchors are certified: one outside the published subset
// could hold a heading the parse sees and GitHub does not render as one, or the
// reverse.
type repoAnchors struct {
	root  string
	cache map[string]map[string]bool
}

// check reports whether frag is an anchor GitHub renders for the tracked file p. Only
// a Markdown file's headings are anchors here; a link to any other file or to a
// directory carries none.
func (a *repoAnchors) check(p, frag string) error {
	if !strings.HasSuffix(p, ".md") {
		return fmt.Errorf("a link to %s, which is not Markdown, carries no anchor", p)
	}
	set, ok := a.cache[p]
	if !ok {
		b, err := os.ReadFile(filepath.Join(a.root, filepath.FromSlash(p)))
		if err != nil {
			return err
		}
		ad, err := admit(p, b)
		if err != nil {
			return fmt.Errorf("its anchors cannot be certified: %w", err)
		}
		set = ad.anchors()
		if a.cache == nil {
			a.cache = map[string]map[string]bool{}
		}
		a.cache[p] = set
	}
	if !set[frag] {
		return fmt.Errorf("%s has no heading with the anchor #%s", p, frag)
	}
	return nil
}

func (d *mdDoc) line(i int) string {
	end := len(d.src)
	if i+1 < len(d.starts) {
		end = d.starts[i+1]
	}
	return strings.TrimRight(d.src[d.starts[i]:end], "\r\n")
}

func (d *mdDoc) lineOf(offset int) int {
	lo, hi := 0, len(d.starts)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if d.starts[mid] <= offset {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

func (d *mdDoc) headings() []heading {
	var hs []heading
	for i := range d.starts {
		if d.fenced[i] {
			continue
		}
		if m := atxHeading.FindStringSubmatch(d.line(i)); m != nil {
			hs = append(hs, heading{line: i, level: len(m[1]), text: renderInline(m[2])})
		}
	}
	return hs
}

// links finds every inline link and image destination outside fences and code spans.
// A paragraph boundary (a blank line, a fence) ends any construct still open.
//
// A link's label is scanned too, so an image inside it is found; the scan then resumes
// after the outer link's destination.
func (d *mdDoc) links() []mdLink {
	var out []mdLink
	type resume struct{ at, next int }
	var labels []resume
	src := d.src
	for i := 0; i < len(src); {
		if n := len(labels); n > 0 && i >= labels[n-1].at {
			i = labels[n-1].next
			labels = labels[:n-1]
			continue
		}
		if d.fenced[d.lineOf(i)] {
			i = d.nextLineStart(i)
			continue
		}
		switch src[i] {
		case '\\':
			i += 2
		case '`':
			i = d.skipCodeSpan(i)
		case '[':
			if l, labelEnd, next, ok := d.parseLink(i); ok {
				out = append(out, l)
				labels = append(labels, resume{at: labelEnd, next: next})
				i++
			} else {
				i++
			}
		default:
			i++
		}
	}
	// An image in a label sits before its link's destination.
	slices.SortFunc(out, func(a, b mdLink) int { return a.start - b.start })
	return out
}

func (d *mdDoc) nextLineStart(i int) int {
	if j := strings.IndexByte(d.src[i:], '\n'); j >= 0 {
		return i + j + 1
	}
	return len(d.src)
}

// blockEnd is the offset where the paragraph holding i ends: a blank line or a fence.
func (d *mdDoc) blockEnd(i int) int {
	for l := d.lineOf(i) + 1; l < len(d.starts); l++ {
		if d.fenced[l] || strings.TrimSpace(d.line(l)) == "" {
			return d.starts[l]
		}
	}
	return len(d.src)
}

// skipCodeSpan returns the offset after the code span opening at i, or after the
// backtick run when no run of the same length closes it in the paragraph.
func (d *mdDoc) skipCodeSpan(i int) int {
	n := runLen(d.src, i, '`')
	end := d.blockEnd(i)
	for j := i + n; j < end; {
		if d.src[j] != '`' {
			j++
			continue
		}
		m := runLen(d.src, j, '`')
		if m == n {
			return j + m
		}
		j += m
	}
	return i + n
}

func runLen(s string, i int, c byte) int {
	n := 0
	for i+n < len(s) && s[i+n] == c {
		n++
	}
	return n
}

// parseLink parses "[text](dest "title")" at i. Link text may nest brackets and
// contain code spans, and may wrap across lines within its paragraph.
// It returns the offset of the label's closing bracket and the offset after the link.
func (d *mdDoc) parseLink(i int) (mdLink, int, int, bool) {
	src, end := d.src, d.blockEnd(i)
	depth := 0
	j := i
	for ; j < end; j++ {
		switch src[j] {
		case '\\':
			j++
		case '`':
			j = d.skipCodeSpan(j) - 1
		case '[':
			depth++
		case ']':
			depth--
		}
		if depth == 0 {
			break
		}
	}
	if depth != 0 || j+1 >= end || src[j+1] != '(' {
		return mdLink{}, 0, 0, false
	}
	k := skipSpace(src, j+2, end)
	var start, stop int
	if k < end && src[k] == '<' {
		start = k + 1
		e := strings.IndexAny(src[start:end], ">\n")
		if e < 0 || src[start+e] != '>' {
			return mdLink{}, 0, 0, false
		}
		stop = start + e
		k = stop + 1
	} else {
		start = k
		parens := 0
	scan:
		for ; k < end; k++ {
			switch src[k] {
			case '\\':
				k++
			case '(':
				parens++
			case ')':
				if parens == 0 {
					break scan
				}
				parens--
			case ' ', '\t', '\n':
				break scan
			}
		}
		stop = k
	}
	k = skipSpace(src, k, end)
	if k < end && (src[k] == '"' || src[k] == '\'' || src[k] == '(') {
		closer := src[k]
		if closer == '(' {
			closer = ')'
		}
		e := strings.IndexByte(src[k+1:end], closer)
		if e < 0 {
			return mdLink{}, 0, 0, false
		}
		k = skipSpace(src, k+1+e+1, end)
	}
	if k >= end || src[k] != ')' {
		return mdLink{}, 0, 0, false
	}
	return mdLink{start: start, end: stop, dest: src[start:stop]}, j, k + 1, true
}

func skipSpace(s string, i, end int) int {
	for i < end && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n') {
		i++
	}
	return i
}

var (
	inlineLink = regexp.MustCompile(`!?\[([^\]]*)\]\([^)]*\)`)
	inlineHTML = regexp.MustCompile(`<[A-Za-z/][^>]*>`)
)

// renderInline is the text a heading renders as: code spans keep their content
// without the backticks, links and images keep their text, emphasis markers and
// inline HTML are dropped, and backslash escapes resolve.
func renderInline(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '`' {
			j := strings.IndexByte(s[i:], '`')
			if j < 0 {
				j = len(s) - i
			}
			b.WriteString(renderProse(s[i : i+j]))
			i += j
			continue
		}
		n := runLen(s, i, '`')
		closeAt := -1
		for j := i + n; j < len(s); {
			if s[j] != '`' {
				j++
				continue
			}
			m := runLen(s, j, '`')
			if m == n {
				closeAt = j
				break
			}
			j += m
		}
		if closeAt < 0 {
			b.WriteString(s[i : i+n])
			i += n
			continue
		}
		code := s[i+n : closeAt]
		if len(code) > 2 && code[0] == ' ' && code[len(code)-1] == ' ' && strings.Trim(code, " ") != "" {
			code = code[1 : len(code)-1]
		}
		b.WriteString(code)
		i = closeAt + n
	}
	return b.String()
}

func renderProse(s string) string {
	s = inlineLink.ReplaceAllString(s, "$1")
	s = inlineHTML.ReplaceAllString(s, "")
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' && i+1 < len(s) && strings.IndexByte("\\`*_{}[]()#+-.!|<>~", s[i+1]) >= 0:
			i++
			b.WriteByte(s[i])
		case c == '*':
		case c == '_' && emphasisUnderscore(s, i):
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// emphasisUnderscore: an underscore delimits emphasis only at a word boundary, so
// snake_case stays intact.
func emphasisUnderscore(s string, i int) bool {
	word := func(j int) bool {
		if j < 0 || j >= len(s) {
			return false
		}
		c := s[j]
		return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
	}
	j, k := i-1, i+1
	for j >= 0 && s[j] == '_' {
		j--
	}
	for k < len(s) && s[k] == '_' {
		k++
	}
	return !word(j) || !word(k)
}
