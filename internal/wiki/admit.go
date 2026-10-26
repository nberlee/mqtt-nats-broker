package wiki

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

// The wiki publishes a subset of GitHub Flavored Markdown, and a CommonMark/GFM parse
// is the authority on it: it decides what a document's links and headings are, so the
// subset is judged by what GitHub will render rather than by a pattern.
//
// Outside code, links come only from inline links and images, and from autolinks
// (angle-bracketed, or bare URLs and email addresses as GFM extends them). Everything
// else that GitHub turns into a link is refused, at any depth: link reference
// definitions (and so every reference-style use), footnotes, raw HTML of any form, and
// the wiki's own [[...]] syntax. A heading is plain text and code spans only, with an
// ASCII anchor: its non-ASCII characters are limited to decorations github-slugger
// strips, so its anchor does not depend on Unicode tables or casing rules.

var gfm = goldmark.New(goldmark.WithExtensions(extension.GFM))

// headingDecorations are the non-ASCII characters a heading may carry, each one
// github-slugger strips: §, em dash, rightwards arrow, white heavy check mark. An
// author restriction, measured on the documentation of 2026-10-26.
const headingDecorations = "§—→✅"

var entityLike = regexp.MustCompile(`&(#[0-9]+|#[xX][0-9a-fA-F]+|[A-Za-z][A-Za-z0-9]*);`)

type admittedHeading struct {
	level int
	text  string
}

// admittedDoc is what a document links to and the headings it renders, as the parse
// reads them.
type admittedDoc struct {
	dests     []string // inline link and image destinations, raw
	autolinks []string // autolink URLs as rendered
	headings  []admittedHeading
}

// admit parses src and refuses anything outside the published subset.
func admit(path string, src []byte) (*admittedDoc, error) {
	pc := parser.NewContext()
	doc := gfm.Parser().Parse(text.NewReader(src), parser.WithContext(pc))
	if refs := pc.References(); len(refs) > 0 {
		return nil, fmt.Errorf("%s: reference-style link and footnote definitions are not supported ([%s]: ...)", path, refs[0].Label())
	}
	a := &admittedDoc{}
	var err error
	where := func(n ast.Node) string {
		for p := n; p != nil; p = p.Parent() {
			if p.Type() == ast.TypeBlock && p.Lines().Len() > 0 {
				return fmt.Sprintf("%s:%d", path, bytes.Count(src[:p.Lines().At(0).Start], []byte("\n"))+1)
			}
		}
		return path
	}
	walkErr := ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch n := n.(type) {
		case *ast.HTMLBlock, *ast.RawHTML:
			return ast.WalkStop, fmt.Errorf("%s: raw HTML is not supported", where(n))
		case *ast.Link:
			if err = literalDestination(n.Destination); err != nil {
				return ast.WalkStop, fmt.Errorf("%s: %w", where(n), err)
			}
			a.dests = append(a.dests, string(n.Destination))
		case *ast.Image:
			if err = literalDestination(n.Destination); err != nil {
				return ast.WalkStop, fmt.Errorf("%s: %w", where(n), err)
			}
			a.dests = append(a.dests, string(n.Destination))
		case *ast.AutoLink:
			u := string(n.URL(src))
			if n.AutoLinkType == ast.AutoLinkEmail && !strings.HasPrefix(strings.ToLower(u), "mailto:") {
				u = "mailto:" + u
			}
			a.autolinks = append(a.autolinks, u)
		case *ast.Heading:
			h, herr := admitHeading(n, src)
			if herr != nil {
				return ast.WalkStop, fmt.Errorf("%s: %w", where(n), herr)
			}
			a.headings = append(a.headings, h)
		case *ast.Paragraph, *ast.TextBlock:
			t := plainText(n, src)
			if strings.Contains(t, "[[") {
				return ast.WalkStop, fmt.Errorf("%s: the wiki's [[...]] syntax is not supported", where(n))
			}
			// GitHub renders footnotes in a repository and not in a wiki, so a footnote
			// would link one place and not the other.
			if strings.Contains(t, "[^") {
				return ast.WalkStop, fmt.Errorf("%s: footnotes are not supported", where(n))
			}
		}
		return ast.WalkContinue, nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return a, nil
}

// literalDestination refuses a destination whose rendered form differs from its source
// text, so the destination checked is the one GitHub links.
func literalDestination(d []byte) error {
	if bytes.ContainsRune(d, '\\') || entityLike.Match(d) {
		return fmt.Errorf("link destination %q uses an escape or entity; write it literally", d)
	}
	return nil
}

func admitHeading(n *ast.Heading, src []byte) (admittedHeading, error) {
	var b strings.Builder
	for c := n.FirstChild(); c != nil; c = c.NextSibling() {
		switch c := c.(type) {
		case *ast.Text:
			b.Write(c.Segment.Value(src))
			if c.SoftLineBreak() || c.HardLineBreak() {
				b.WriteByte(' ')
			}
		case *ast.CodeSpan:
			for t := c.FirstChild(); t != nil; t = t.NextSibling() {
				if tx, ok := t.(*ast.Text); ok {
					b.Write(tx.Segment.Value(src))
				}
			}
		default:
			return admittedHeading{}, fmt.Errorf("a heading may hold only text and code spans, not %s", c.Kind())
		}
	}
	text := b.String()
	if strings.ContainsRune(text, '\\') || entityLike.MatchString(text) {
		return admittedHeading{}, fmt.Errorf("heading %q uses an escape or entity", text)
	}
	for _, r := range text {
		if r >= utf8.RuneSelf && !strings.ContainsRune(headingDecorations, r) {
			return admittedHeading{}, fmt.Errorf("heading %q holds %q, outside the decorations a heading may carry (%s)", text, r, headingDecorations)
		}
	}
	return admittedHeading{level: n.Level, text: text}, nil
}

// plainText is the text of n's inline content outside code spans.
func plainText(n ast.Node, src []byte) string {
	var b strings.Builder
	_ = ast.Walk(n, func(c ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch c := c.(type) {
		case *ast.CodeSpan:
			return ast.WalkSkipChildren, nil
		case *ast.Text:
			b.Write(c.Segment.Value(src))
		}
		return ast.WalkContinue, nil
	})
	return b.String()
}

// anchors are the anchors GitHub renders for an admitted document's headings, counted
// over the whole document.
func (a *admittedDoc) anchors() map[string]bool {
	set := map[string]bool{}
	sl := newSlugger()
	for _, h := range a.headings {
		set[sl.slug(h.text)] = true
	}
	return set
}
