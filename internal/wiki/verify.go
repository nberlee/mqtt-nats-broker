package wiki

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
)

// Verify checks rendered output as the wiki will serve it, independently of how it was
// produced: it parses every page with the GFM parser (admit.go), not the renderer's
// scanner, and so judges the links and headings GitHub will render. Every page is
// within budget and inside the published subset; every wiki link names a page that
// exists, with no .md and no directory, and an anchor that page renders; every link to
// a file or directory of this repository is pinned to sha, names a tracked path, and
// carries only an anchor that file renders, read from the checkout at root.
func Verify(out Output, root, repo, sha string, tracked []string, budget int) error {
	files, dirs := map[string]bool{}, map[string]bool{".": true}
	for _, f := range tracked {
		files[f] = true
		for d := path.Dir(f); d != "."; d = path.Dir(d) {
			dirs[d] = true
		}
	}
	anchors := &repoAnchors{root: root}
	docs := map[string]*admittedDoc{}
	pageAnchors := map[string]map[string]bool{}
	var errs []error
	for _, name := range out.sortedNames() {
		if len(out[name]) > budget {
			errs = append(errs, fmt.Errorf("%s: %d bytes, over the %d-byte page budget", name, len(out[name]), budget))
		}
		page, ok := strings.CutSuffix(name, ".md")
		if !ok {
			continue
		}
		ad, err := admit(name, out[name])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		docs[page] = ad
		pageAnchors[page] = ad.anchors()
	}
	for _, page := range sortedKeys(docs) {
		ad := docs[page]
		for _, dest := range append(append([]string(nil), ad.dests...), ad.autolinks...) {
			where := fmt.Sprintf("%s.md: link %q", page, dest)
			if ref := repoURL(dest, repo); ref != nil {
				if err := ref.check(sha, files, dirs, anchors); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", where, err))
				}
				continue
			}
			if uriScheme.MatchString(dest) {
				continue
			}
			target, frag, _ := strings.Cut(dest, "#")
			switch {
			case strings.HasSuffix(target, ".md") || strings.Contains(target, "/"):
				errs = append(errs, fmt.Errorf("%s: a wiki link names a page, not a file path", where))
				continue
			case target == "":
				target = page
			}
			a, ok := pageAnchors[target]
			if !ok || strings.HasPrefix(target, "_") {
				errs = append(errs, fmt.Errorf("%s: no page %q", where, target))
				continue
			}
			if frag != "" && !a[frag] {
				errs = append(errs, fmt.Errorf("%s: page %q renders no anchor #%s", where, target, frag))
			}
		}
	}
	return errors.Join(errs...)
}

// repoRef is a URL into this repository on GitHub.
type repoRef struct {
	kind string // blob, tree, raw, ... or "" for the repository itself
	ref  string
	path string
	frag string
}

// repoURL classifies u: a reference into repo (owner/name) on github.com, compared the
// way GitHub resolves it, without regard to case in the host, owner or name; or nil.
func repoURL(u, repo string) *repoRef {
	pu, err := url.Parse(u)
	if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") {
		return nil
	}
	host := strings.ToLower(pu.Hostname())
	segs := strings.Split(strings.Trim(pu.Path, "/"), "/")
	owner, name, _ := strings.Cut(repo, "/")
	if len(segs) < 2 || !strings.EqualFold(segs[0], owner) || !strings.EqualFold(segs[1], name) {
		return nil
	}
	switch host {
	case "github.com", "www.github.com":
		r := &repoRef{frag: pu.Fragment}
		if len(segs) > 2 {
			r.kind = segs[2]
		}
		if len(segs) > 3 {
			r.ref = segs[3]
		}
		if len(segs) > 4 {
			r.path = strings.Join(segs[4:], "/")
		}
		return r
	case "raw.githubusercontent.com":
		return &repoRef{kind: "raw"}
	}
	return nil
}

// check holds a repository link to what the wiki may publish: a file or directory is
// linked as a blob or tree pinned to the rendered commit; other file views are refused;
// issues, pull requests, Actions and the repository page name no file and pass.
func (r *repoRef) check(sha string, files, dirs map[string]bool, anchors *repoAnchors) error {
	switch r.kind {
	case "blob", "tree":
	case "raw", "blame", "edit", "commits", "history":
		return fmt.Errorf("a %s view of a repository file is not a supported link; link the file as a pinned blob", r.kind)
	default:
		return nil
	}
	if r.ref != sha {
		return fmt.Errorf("a repository link is pinned to %q, not the rendered commit", r.ref)
	}
	if (r.kind == "blob" && !files[r.path]) || (r.kind == "tree" && !dirs[r.path]) {
		return fmt.Errorf("no tracked %s %q", map[string]string{"blob": "file", "tree": "directory"}[r.kind], r.path)
	}
	switch {
	case r.frag == "":
	case r.kind == "tree":
		return errors.New("a directory has no anchors")
	default:
		return anchors.check(r.path, r.frag)
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	o := Output{}
	for k := range m {
		o[k] = nil
	}
	return o.sortedNames()
}
