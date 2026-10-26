package wiki

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// ManifestPath is the wiki's source of truth: its "## Pages" list names, in order,
// every document the wiki publishes and the title each is published under, and the
// rest of it is the wiki's Home page.
const ManifestPath = "docs/WIKI.md"

// DefaultBudget is the largest page this package writes, in bytes of final output
// (after link rewriting and generated navigation). GitHub documents no per-page
// limit; a third-party measurement puts it at 484,044 characters, and this stays
// below that with margin.
const DefaultBudget = 400_000

// Config says what to render. SHA pins every link to a repository file; Tag, when
// set, names the release the output is for and is what publication is guarded by.
type Config struct {
	Root   string // repository checkout
	Repo   string // owner/name on GitHub
	SHA    string // commit the links point at
	Tag    string // release tag; empty renders an unpublishable local preview
	Budget int    // 0 means DefaultBudget
	// Tracked lists the repository's files, slash-separated and relative to Root; nil
	// reads them with git ls-files. A link to anything else is an error.
	Tracked []string
}

// Output is the rendered wiki: file name -> content.
type Output map[string][]byte

// source is one manifest entry and its parsed document.
type source struct {
	title string
	path  string // slash-separated, relative to Root
	doc   *mdDoc
	hs    []heading
	// srcSlug -> heading index: what a link into this document means.
	srcSlugs map[string]int
	root     *node
}

// node is a heading's section: its heading line through the line before the next
// heading of the same or a shallower level. The document's root node has no heading.
type node struct {
	h        int // index into source.hs, -1 for the root
	start    int // first line
	end      int // line after the last
	children []*node
	split    bool
	page     *page
}

type page struct {
	name   string // page name = file name without .md
	src    *source
	node   *node
	ranges [][2]int // source line ranges the page shows
	// heading index -> anchor on this page
	anchors map[int]string
}

// Render builds the wiki from the manifest and checks the result (Verify).
func Render(cfg Config) (Output, error) {
	if cfg.Budget == 0 {
		cfg.Budget = DefaultBudget
	}
	if cfg.Tag != "" {
		if _, err := parseReleaseTag(cfg.Tag); err != nil {
			return nil, err
		}
	}
	if !shaPattern.MatchString(cfg.SHA) {
		return nil, fmt.Errorf("commit %q is not a full hexadecimal object name", cfg.SHA)
	}
	tracked := cfg.Tracked
	if tracked == nil {
		var err error
		if tracked, err = gitTracked(cfg.Root); err != nil {
			return nil, err
		}
	}
	r := &renderer{cfg: cfg, files: map[string]bool{}, dirs: map[string]bool{}, anchors: repoAnchors{root: cfg.Root}}
	for _, f := range tracked {
		r.files[f] = true
		for d := path.Dir(f); d != "."; d = path.Dir(d) {
			r.dirs[d] = true
		}
	}
	if err := r.readManifest(); err != nil {
		return nil, err
	}
	out, err := r.paginate()
	if err != nil {
		return nil, err
	}
	if err := Verify(out, cfg.Root, cfg.Repo, cfg.SHA, tracked, cfg.Budget); err != nil {
		return nil, err
	}
	return out, nil
}

var shaPattern = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

func gitTracked(root string) ([]string, error) {
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard").Output()
	if err != nil {
		return nil, fmt.Errorf("listing the repository's files: %w", err)
	}
	return strings.Split(strings.TrimRight(string(out), "\x00"), "\x00"), nil
}

type renderer struct {
	cfg     Config
	anchors repoAnchors
	files   map[string]bool
	dirs    map[string]bool
	home    *source
	sources []*source
	byPath  map[string]*source
	pages   []*page
}

var manifestItem = regexp.MustCompile(`^- \[([^\]]+)\]\(([^)\s]+)\)$`)

func (r *renderer) readManifest() error {
	home, err := r.load("Home", ManifestPath)
	if err != nil {
		return err
	}
	r.home = home
	r.byPath = map[string]*source{ManifestPath: home}
	in := false
	for i := range home.doc.starts {
		line := home.doc.line(i)
		if m := atxHeading.FindStringSubmatch(line); m != nil && !home.doc.fenced[i] {
			if in && len(m[1]) <= 2 {
				break
			}
			in = len(m[1]) == 2 && renderInline(m[2]) == "Pages"
			continue
		}
		if !in || strings.TrimSpace(line) == "" {
			continue
		}
		m := manifestItem.FindStringSubmatch(line)
		if m == nil {
			return fmt.Errorf("%s:%d: a Pages entry must be exactly one link, \"- [Title](path)\": %q", ManifestPath, i+1, line)
		}
		p, frag, err := r.resolve(ManifestPath, m[2])
		if err != nil || frag != "" || !strings.HasSuffix(p, ".md") || !r.files[p] {
			return fmt.Errorf("%s:%d: %q is not a tracked Markdown file", ManifestPath, i+1, m[2])
		}
		if r.byPath[p] != nil {
			return fmt.Errorf("%s:%d: %s is listed twice", ManifestPath, i+1, p)
		}
		s, err := r.load(m[1], p)
		if err != nil {
			return err
		}
		r.sources = append(r.sources, s)
		r.byPath[p] = s
	}
	if len(r.sources) == 0 {
		return fmt.Errorf("%s: no \"## Pages\" list", ManifestPath)
	}
	return nil
}

func (r *renderer) load(title, p string) (*source, error) {
	b, err := os.ReadFile(filepath.Join(r.cfg.Root, filepath.FromSlash(p)))
	if err != nil {
		return nil, err
	}
	d, err := parseMarkdown(p, string(b))
	if err != nil {
		return nil, err
	}
	s := &source{title: title, path: p, doc: d, hs: d.headings(), srcSlugs: map[string]int{}}
	if err := r.agree(s, b); err != nil {
		return nil, err
	}
	sl := newSlugger()
	for i, h := range s.hs {
		s.srcSlugs[sl.slug(h.text)] = i
	}
	s.root = buildTree(s, len(d.starts))
	return s, nil
}

// agree admits the source and requires the scanner, which rewrites it, to read it as
// the GFM parse does: the same link destinations and the same headings, in order. A
// construct either one reads differently (a code span the scanner bounds wrongly, a
// heading in a container, a setext heading) fails here rather than being published.
func (r *renderer) agree(s *source, b []byte) error {
	ad, err := admit(s.path, b)
	if err != nil {
		return err
	}
	// Compared as sorted lists: the parse lists a link before the image in its label,
	// the scanner in source order.
	var scanned []string
	for _, l := range s.doc.links() {
		scanned = append(scanned, l.dest)
	}
	slices.Sort(scanned)
	parsed := slices.Sorted(slices.Values(ad.dests))
	if !slices.Equal(scanned, parsed) {
		return fmt.Errorf("%s: the link destinations the rewriter found differ from the document's %s",
			s.path, firstDifference(scanned, parsed))
	}
	var hs []string
	for _, h := range s.hs {
		hs = append(hs, fmt.Sprintf("%d %s", h.level, h.text))
	}
	var want []string
	for _, h := range ad.headings {
		want = append(want, fmt.Sprintf("%d %s", h.level, h.text))
	}
	if !slices.Equal(hs, want) {
		return fmt.Errorf("%s: the headings the rewriter found differ from the document's %s", s.path, firstDifference(hs, want))
	}
	for _, u := range ad.autolinks {
		if repoURL(u, r.cfg.Repo) != nil {
			return fmt.Errorf("%s: autolink %s points into this repository; write an inline link to the file so it is pinned", s.path, u)
		}
	}
	return nil
}

// firstDifference describes where two sequences first part.
func firstDifference(got, want []string) string {
	for i := 0; i < len(got) || i < len(want); i++ {
		g, w := "(none)", "(none)"
		if i < len(got) {
			g = got[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if g != w {
			return fmt.Sprintf("at #%d: rewriter %q, document %q", i+1, g, w)
		}
	}
	return ""
}

func buildTree(s *source, lines int) *node {
	root := &node{h: -1, start: 0, end: lines}
	stack := []*node{root}
	level := func(n *node) int {
		if n.h < 0 {
			return 0
		}
		return s.hs[n.h].level
	}
	for i, h := range s.hs {
		for level(stack[len(stack)-1]) >= h.level {
			stack[len(stack)-1].end = h.line
			stack = stack[:len(stack)-1]
		}
		n := &node{h: i, start: h.line, end: lines}
		parent := stack[len(stack)-1]
		parent.children = append(parent.children, n)
		stack = append(stack, n)
	}
	// A document whose only top-level heading is its title, with nothing before it,
	// is that heading's section.
	if len(root.children) == 1 && strings.TrimSpace(s.doc.src[:s.doc.starts[root.children[0].start]]) == "" {
		return root.children[0]
	}
	return root
}

// paginate assigns every section to a page. A section is split into its children
// only when its page would exceed the budget; the size is the final page's, so
// paging repeats until no page is over (each round only adds splits).
func (r *renderer) paginate() (Output, error) {
	for {
		r.assign()
		pages, err := r.renderPages()
		if err != nil {
			return nil, err
		}
		grew := false
		for _, p := range r.pages {
			if size := len(pages[p.name+".md"]); size > r.cfg.Budget {
				if p.src == r.home || p.node.split || len(p.node.children) == 0 {
					return nil, fmt.Errorf("%s: section %q renders to %d bytes, over the %d-byte page budget, and has no subsection to split it at",
						p.src.path, r.sectionTitle(p.src, p.node), size, r.cfg.Budget)
				}
				p.node.split = true
				grew = true
			}
		}
		if !grew {
			return pages, nil
		}
	}
}

func (r *renderer) sectionTitle(s *source, n *node) string {
	if n.h < 0 {
		return s.title
	}
	return s.hs[n.h].text
}

// assign names a page for every unsplit section and every split section's
// introduction, and maps each heading to its page and its anchor there.
func (r *renderer) assign() {
	r.pages = nil
	used := map[string]bool{"Home": true, "_Sidebar": true, "_Footer": true}
	unique := func(name string) string {
		base := pageFileName(name)
		n := base
		for i := 2; used[strings.ToLower(n)]; i++ {
			n = fmt.Sprintf("%s-%d", base, i)
		}
		used[strings.ToLower(n)] = true
		return n
	}
	homePage := &page{name: "Home", src: r.home, node: r.home.root, ranges: [][2]int{{0, len(r.home.doc.starts)}}}
	r.home.root.page = homePage
	r.anchorPage(homePage)
	r.pages = append(r.pages, homePage)
	for _, s := range r.sources {
		var walk func(n *node, name string)
		walk = func(n *node, name string) {
			p := &page{name: unique(name), src: s, node: n}
			n.page = p
			if !n.split {
				p.ranges = [][2]int{{n.start, n.end}}
				r.anchorPage(p)
				r.pages = append(r.pages, p)
				return
			}
			p.ranges = [][2]int{{n.start, n.children[0].start}}
			r.anchorPage(p)
			r.pages = append(r.pages, p)
			for _, c := range n.children {
				walk(c, s.title+": "+s.hs[c.h].text)
			}
		}
		walk(s.root, s.title)
	}
}

func pageFileName(name string) string {
	var b strings.Builder
	dash := false
	for _, c := range name {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
		switch {
		case ok:
			b.WriteRune(c)
			dash = false
		case (c == ' ' || c == '-') && !dash && b.Len() > 0:
			b.WriteByte('-')
			dash = true
		}
	}
	return strings.TrimRight(b.String(), "-")
}

// anchorPage gives each heading the page shows the anchor the wiki renders for it:
// a slug counted over that page's headings alone.
func (r *renderer) anchorPage(p *page) {
	p.anchors = map[int]string{}
	sl := newSlugger()
	for i, h := range p.src.hs {
		for _, rg := range p.ranges {
			if h.line >= rg[0] && h.line < rg[1] {
				p.anchors[i] = sl.slug(h.text)
			}
		}
	}
}

// pageOf is the page holding heading h of s.
func (r *renderer) pageOf(s *source, h int) *page {
	for _, p := range r.pages {
		if p.src != s {
			continue
		}
		if _, ok := p.anchors[h]; ok {
			return p
		}
	}
	return nil
}

func (r *renderer) renderPages() (Output, error) {
	out := Output{}
	rewritten := map[*source][]string{}
	for _, s := range append([]*source{r.home}, r.sources...) {
		text, err := r.rewriteLinks(s)
		if err != nil {
			return nil, err
		}
		rewritten[s] = strings.SplitAfter(text, "\n")
	}
	for _, p := range r.pages {
		lines := rewritten[p.src]
		var b strings.Builder
		for _, rg := range p.ranges {
			for i := rg[0]; i < rg[1] && i < len(lines); i++ {
				b.WriteString(lines[i])
			}
		}
		if p.node.split {
			b.WriteString(r.nav(p.node))
		}
		out[p.name+".md"] = []byte(b.String())
	}
	out["_Sidebar.md"] = []byte(r.sidebar())
	out["_Footer.md"] = []byte(r.footer())
	if r.cfg.Tag != "" {
		out[PublicationFile] = []byte(publication{tag: r.cfg.Tag, sha: r.cfg.SHA}.String())
	}
	return out, nil
}

// nav lists a split section's subsection pages under its introduction.
func (r *renderer) nav(n *node) string {
	var b strings.Builder
	b.WriteString("\nSections:\n\n")
	for _, c := range n.children {
		fmt.Fprintf(&b, "- [%s](%s)\n", escapeLinkText(c.page.src.hs[c.h].text), c.page.name)
	}
	return b.String()
}

func (r *renderer) sidebar() string {
	var b strings.Builder
	b.WriteString("- [Home](Home)\n")
	for _, s := range r.sources {
		var walk func(n *node, depth int, title string)
		walk = func(n *node, depth int, title string) {
			fmt.Fprintf(&b, "%s- [%s](%s)\n", strings.Repeat("  ", depth), escapeLinkText(title), n.page.name)
			if n.split {
				for _, c := range n.children {
					walk(c, depth+1, s.hs[c.h].text)
				}
			}
		}
		walk(s.root, 0, s.title)
	}
	return b.String()
}

func (r *renderer) footer() string {
	what := "commit `" + r.cfg.SHA[:12] + "`"
	if r.cfg.Tag != "" {
		what = "release `" + r.cfg.Tag + "` (commit `" + r.cfg.SHA[:12] + "`)"
	}
	return fmt.Sprintf("Generated from [`docs/`](https://github.com/%s/tree/%s/docs) at %s. "+
		"Edits made in the wiki are overwritten when the next release is published.\n", r.cfg.Repo, r.cfg.SHA, what)
}

func escapeLinkText(s string) string {
	return strings.NewReplacer(`\`, `\\`, `[`, `\[`, `]`, `\]`).Replace(s)
}

var uriScheme = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*:`)

// rewriteLinks returns s's text with every link destination rewritten for the wiki.
func (r *renderer) rewriteLinks(s *source) (string, error) {
	var b strings.Builder
	last := 0
	for _, l := range s.doc.links() {
		target, err := r.target(s, l.dest)
		if err != nil {
			return "", fmt.Errorf("%s:%d: link %q: %w", s.path, s.doc.lineOf(l.start)+1, l.dest, err)
		}
		b.WriteString(s.doc.src[last:l.start])
		b.WriteString(target)
		last = l.end
	}
	b.WriteString(s.doc.src[last:])
	return b.String(), nil
}

// target is what a link from s to dest becomes on the wiki.
func (r *renderer) target(s *source, dest string) (string, error) {
	if uriScheme.MatchString(dest) {
		return dest, nil
	}
	p, frag, err := r.resolve(s.path, dest)
	if err != nil {
		return "", err
	}
	if t := r.byPath[p]; t != nil {
		if frag == "" {
			return t.root.page.name, nil
		}
		h, ok := t.srcSlugs[frag]
		if !ok {
			return "", fmt.Errorf("%s has no heading with the anchor #%s", p, frag)
		}
		pg := r.pageOf(t, h)
		return pg.name + "#" + pg.anchors[h], nil
	}
	kind := "blob"
	switch {
	case r.files[p]:
	case r.dirs[p] || p == ".":
		kind = "tree"
	default:
		return "", errors.New("no such file in the repository")
	}
	u := fmt.Sprintf("https://github.com/%s/%s/%s/%s", r.cfg.Repo, kind, r.cfg.SHA, (&url.URL{Path: p}).EscapedPath())
	if frag != "" {
		if kind == "tree" {
			return "", errors.New("a directory has no anchors")
		}
		if err := r.anchors.check(p, frag); err != nil {
			return "", err
		}
		u += "#" + frag
	}
	return u, nil
}

// resolve turns a relative destination in the file at from into a path relative to
// the repository root and a fragment. A destination naming no path is from itself.
func (r *renderer) resolve(from, dest string) (string, string, error) {
	raw, frag, _ := strings.Cut(dest, "#")
	p, err := url.PathUnescape(raw)
	if err != nil {
		return "", "", err
	}
	if p == "" {
		return from, frag, nil
	}
	if strings.HasPrefix(p, "/") {
		p = path.Clean(strings.TrimPrefix(p, "/"))
	} else {
		p = path.Join(path.Dir(from), p)
	}
	if p == ".." || strings.HasPrefix(p, "../") {
		return "", "", errors.New("points outside the repository")
	}
	p = strings.TrimSuffix(p, "/")
	return p, frag, nil
}

// sortedNames is out's file names in order, for deterministic iteration.
func (o Output) sortedNames() []string {
	names := make([]string, 0, len(o))
	for n := range o {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
