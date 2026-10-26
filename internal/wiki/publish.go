package wiki

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// PublicationFile records which release the wiki shows. It is not a page, so the
// wiki's web editor neither lists nor changes it, and a web edit leaves it as the
// last publication wrote it; it is what the publication guard reads.
const PublicationFile = ".wiki-publication"

type publication struct {
	tag string
	sha string
}

func (p publication) String() string { return "tag " + p.tag + "\ncommit " + p.sha + "\n" }

func parsePublication(b []byte) (publication, error) {
	lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(lines) != 2 {
		return publication{}, fmt.Errorf("%s: want two lines, tag and commit", PublicationFile)
	}
	tag, ok1 := strings.CutPrefix(lines[0], "tag ")
	sha, ok2 := strings.CutPrefix(lines[1], "commit ")
	if !ok1 || !ok2 || !shaPattern.MatchString(sha) {
		return publication{}, fmt.Errorf("%s: malformed: %q", PublicationFile, b)
	}
	if _, err := parseReleaseTag(tag); err != nil {
		return publication{}, fmt.Errorf("%s: %w", PublicationFile, err)
	}
	return publication{tag: tag, sha: sha}, nil
}

// Outcome is what a publication did.
type Outcome string

const (
	Published  Outcome = "published"  // a commit with the output was pushed
	Unchanged  Outcome = "unchanged"  // the wiki already holds exactly the output
	Superseded Outcome = "superseded" // the wiki shows a newer release; nothing pushed
)

// Publisher pushes a rendered output into a clone of the wiki repository.
type Publisher struct {
	Wiki        string // the clone, on the branch the wiki serves, with an origin remote
	Out         string // the rendered output, carrying its PublicationFile
	MaxAttempts int    // 0 means 5
	// beforePush runs after the commit and before each push: the seam the tests use to
	// land a competing publication or a web edit in the remote.
	beforePush func()
}

// Publish publishes p.Out unless the wiki already shows a newer release. The
// candidate's identity is the output's own record, so the release guarded and the
// content pushed cannot disagree. Every attempt re-reads the wiki's record from the
// remote's current state: a rejected push means the wiki moved, and whatever moved
// it - a newer release, an older one, a moved tag, a web edit - is judged afresh.
func (p Publisher) Publish() (Outcome, error) {
	rec, err := os.ReadFile(filepath.Join(p.Out, PublicationFile))
	if err != nil {
		return "", fmt.Errorf("the output carries no %s (rendered without a release tag?): %w", PublicationFile, err)
	}
	cand, err := parsePublication(rec)
	if err != nil {
		return "", fmt.Errorf("the output's record: %w", err)
	}
	candTag, _ := parseReleaseTag(cand.tag)
	branch, err := p.git("symbolic-ref", "--short", "HEAD")
	if err != nil {
		return "", err
	}
	attempts := p.MaxAttempts
	if attempts == 0 {
		attempts = 5
	}
	var pushErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if _, err := p.git("fetch", "origin", branch); err != nil {
				return "", err
			}
			if _, err := p.git("reset", "--hard", "origin/"+branch); err != nil {
				return "", err
			}
		}
		if b, err := os.ReadFile(filepath.Join(p.Wiki, PublicationFile)); err == nil {
			pub, err := parsePublication(b)
			if err != nil {
				return "", fmt.Errorf("the wiki's record: %w", err)
			}
			pubTag, _ := parseReleaseTag(pub.tag)
			switch c := candTag.compare(pubTag); {
			case c < 0:
				return Superseded, nil
			case c == 0 && pub.sha != cand.sha:
				return "", fmt.Errorf("the wiki shows %s at commit %s; refusing to publish it again at %s (a moved tag)", pub.tag, pub.sha, cand.sha)
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		if err := p.replaceTree(); err != nil {
			return "", err
		}
		if _, err := p.git("add", "-A"); err != nil {
			return "", err
		}
		if _, err := p.git("diff", "--cached", "--quiet"); err == nil {
			return Unchanged, nil
		}
		if _, err := p.git("-c", "user.name=github-actions[bot]",
			"-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
			"commit", "--quiet", "-m", fmt.Sprintf("Publish %s (%s)", cand.tag, cand.sha)); err != nil {
			return "", err
		}
		if p.beforePush != nil {
			p.beforePush()
		}
		if _, pushErr = p.git("push", "--quiet", "origin", "HEAD:"+branch); pushErr == nil {
			return Published, nil
		}
	}
	return "", fmt.Errorf("the wiki moved under every one of %d attempts: %w", attempts, pushErr)
}

// replaceTree makes the clone's working tree exactly the output: every file but .git
// is removed first, so a page the output no longer has is deleted.
func (p Publisher) replaceTree() error {
	entries, err := os.ReadDir(p.Wiki)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == ".git" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(p.Wiki, e.Name())); err != nil {
			return err
		}
	}
	return filepath.WalkDir(p.Out, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(p.Out, path)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dst := filepath.Join(p.Wiki, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o644)
	})
}

func (p Publisher) git(args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", p.Wiki}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}
