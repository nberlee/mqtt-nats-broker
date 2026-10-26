package wiki

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The wiki repository is stood in for by a local bare repository whose first commit is
// what GitHub's "create the first page" leaves, plus leftovers a publication must
// remove: an obsolete page, a nested file and a dotfile.

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func gitT(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "init.defaultBranch=master"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// newWiki returns a bare repository holding the web UI's first page and leftovers.
func newWiki(t *testing.T) string {
	t.Helper()
	bare := t.TempDir()
	gitT(t, bare, "init", "--quiet", "--bare")
	seed := t.TempDir()
	gitT(t, seed, "init", "--quiet")
	writeFiles(t, seed, map[string]string{"Home.md": "placeholder\n", "Old.md": "old\n", "dir/x.md": "x\n", ".hidden": "h\n"})
	gitT(t, seed, "add", "-A")
	gitT(t, seed, "commit", "--quiet", "-m", "Initial Home page")
	gitT(t, seed, "push", "--quiet", bare, "HEAD:master")
	return bare
}

func cloneWiki(t *testing.T, bare string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wiki")
	gitT(t, filepath.Dir(dir), "clone", "--quiet", bare, dir)
	return dir
}

// output is a rendered output for tag at sha, with one page saying which.
func output(t *testing.T, tag, sha string) string {
	t.Helper()
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"Home.md":       "# Home\n\nrelease " + tag + " at " + sha + "\n",
		"_Sidebar.md":   "- [Home](Home)\n",
		PublicationFile: publication{tag: tag, sha: sha}.String(),
	})
	return dir
}

func publishTo(t *testing.T, bare, tag, sha string) Outcome {
	t.Helper()
	o, err := Publisher{Wiki: cloneWiki(t, bare), Out: output(t, tag, sha)}.Publish()
	if err != nil {
		t.Fatalf("publishing %s at %s: %v", tag, sha, err)
	}
	return o
}

var webEdits int

// webEdit lands a commit the way the wiki's web editor would: a page changed, the
// publication record untouched.
func webEdit(t *testing.T, bare string) {
	t.Helper()
	webEdits++
	c := cloneWiki(t, bare)
	writeFiles(t, c, map[string]string{"Home.md": fmt.Sprintf("edited in the browser (%d)\n", webEdits)})
	gitT(t, c, "commit", "--quiet", "-am", "Web edit")
	gitT(t, c, "push", "--quiet", "origin", "HEAD:master")
}

func remote(t *testing.T, bare, name string) string {
	t.Helper()
	out, err := exec.Command("git", "--git-dir", bare, "show", "master:"+name).Output()
	if err != nil {
		return "<absent>"
	}
	return string(out)
}

func remoteHead(t *testing.T, bare string) string {
	t.Helper()
	return gitT(t, bare, "rev-parse", "master")
}

func remoteFiles(t *testing.T, bare string) []string {
	t.Helper()
	return strings.Fields(gitT(t, bare, "ls-tree", "-r", "--name-only", "master"))
}

func TestPublishFirstPublicationReplacesEverything(t *testing.T) {
	bare := newWiki(t)
	if o := publishTo(t, bare, "v0.1.0", shaA); o != Published {
		t.Fatalf("outcome %s", o)
	}
	if got, want := strings.Join(remoteFiles(t, bare), " "), PublicationFile+" Home.md _Sidebar.md"; got != want {
		t.Errorf("wiki files = %q, want %q (leftovers removed)", got, want)
	}
	if got := remote(t, bare, PublicationFile); got != "tag v0.1.0\ncommit "+shaA+"\n" {
		t.Errorf("record = %q", got)
	}
}

func TestPublishRerunIsUnchanged(t *testing.T) {
	bare := newWiki(t)
	publishTo(t, bare, "v0.1.0", shaA)
	head := remoteHead(t, bare)
	if o := publishTo(t, bare, "v0.1.0", shaA); o != Unchanged {
		t.Fatalf("outcome %s, want %s", o, Unchanged)
	}
	if remoteHead(t, bare) != head {
		t.Error("an identical re-run added a commit")
	}
}

func refused(t *testing.T, bare, tag, sha, want string) {
	t.Helper()
	head := remoteHead(t, bare)
	_, err := Publisher{Wiki: cloneWiki(t, bare), Out: output(t, tag, sha)}.Publish()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Publish = %v, want an error containing %q", err, want)
	}
	if remoteHead(t, bare) != head {
		t.Error("a refused publication changed the wiki")
	}
}

func TestPublishRefusesAMovedTag(t *testing.T) {
	bare := newWiki(t)
	publishTo(t, bare, "v0.1.0", shaA)
	refused(t, bare, "v0.1.0", shaB, "(a moved tag)")
}

func TestPublishOlderReleaseIsSuperseded(t *testing.T) {
	bare := newWiki(t)
	publishTo(t, bare, "v0.2.0", shaB)
	head := remoteHead(t, bare)
	if o := publishTo(t, bare, "v0.2.0-rc.1", shaA); o != Superseded {
		t.Fatalf("outcome %s, want %s", o, Superseded)
	}
	if remoteHead(t, bare) != head {
		t.Error("an older release changed the wiki")
	}
	if o := publishTo(t, bare, "v0.3.0-alpha.1", shaA); o != Published {
		t.Fatalf("a newer release: outcome %s", o)
	}
}

// A web edit adds commits without touching the record, so it does not let an older
// release back in.
func TestPublishWebEditDoesNotLetAnOlderReleaseIn(t *testing.T) {
	bare := newWiki(t)
	publishTo(t, bare, "v0.2.0", shaB)
	webEdit(t, bare)
	head := remoteHead(t, bare)
	if o := publishTo(t, bare, "v0.1.0", shaA); o != Superseded {
		t.Fatalf("outcome %s, want %s", o, Superseded)
	}
	if remoteHead(t, bare) != head {
		t.Error("an older release overwrote the wiki after a web edit")
	}
}

func TestPublishRefusesMalformedRecords(t *testing.T) {
	t.Run("in the wiki", func(t *testing.T) {
		bare := newWiki(t)
		c := cloneWiki(t, bare)
		writeFiles(t, c, map[string]string{PublicationFile: "tag latest\ncommit " + shaA + "\n"})
		gitT(t, c, "add", "-A")
		gitT(t, c, "commit", "--quiet", "-m", "bad record")
		gitT(t, c, "push", "--quiet", "origin", "HEAD:master")
		refused(t, bare, "v0.1.0", shaA, `the wiki's record: .wiki-publication: "latest" is not a release tag`)
	})
	t.Run("output without a record", func(t *testing.T) {
		bare := newWiki(t)
		out := output(t, "v0.1.0", shaA)
		if err := os.Remove(filepath.Join(out, PublicationFile)); err != nil {
			t.Fatal(err)
		}
		head := remoteHead(t, bare)
		_, err := Publisher{Wiki: cloneWiki(t, bare), Out: out}.Publish()
		if err == nil || !strings.Contains(err.Error(), "rendered without a release tag?") {
			t.Fatalf("Publish = %v", err)
		}
		if remoteHead(t, bare) != head {
			t.Error("an unpublishable output changed the wiki")
		}
	})
	t.Run("output with a malformed record", func(t *testing.T) {
		bare := newWiki(t)
		out := output(t, "v0.1.0", shaA)
		writeFiles(t, out, map[string]string{PublicationFile: "tag v0.1.0\n"})
		_, err := Publisher{Wiki: cloneWiki(t, bare), Out: out}.Publish()
		if err == nil || !strings.Contains(err.Error(), "the output's record") {
			t.Fatalf("Publish = %v", err)
		}
	})
}

// raced publishes tag at sha while land runs once between that publication's guard and
// its first push, so the push is rejected and the retry must judge what landed.
func raced(t *testing.T, bare, tag, sha string, land func()) (Outcome, error) {
	t.Helper()
	fired := false
	p := Publisher{Wiki: cloneWiki(t, bare), Out: output(t, tag, sha), beforePush: func() {
		if !fired {
			fired = true
			land()
		}
	}}
	return p.Publish()
}

func TestPublishRetryRejudgesWhatLanded(t *testing.T) {
	t.Run("a web edit lands first", func(t *testing.T) {
		bare := newWiki(t)
		publishTo(t, bare, "v0.1.0", shaA)
		o, err := raced(t, bare, "v0.2.0", shaB, func() { webEdit(t, bare) })
		if err != nil || o != Published {
			t.Fatalf("Publish = %s, %v", o, err)
		}
		if got := remote(t, bare, "Home.md"); !strings.Contains(got, "release v0.2.0") {
			t.Errorf("Home.md = %q, want the publication over the edit", got)
		}
	})
	t.Run("a newer publication lands first", func(t *testing.T) {
		bare := newWiki(t)
		var landed string
		o, err := raced(t, bare, "v0.1.0", shaA, func() {
			publishTo(t, bare, "v0.2.0", shaB)
			landed = remoteHead(t, bare)
		})
		if err != nil || o != Superseded {
			t.Fatalf("Publish = %s, %v; want %s", o, err, Superseded)
		}
		if remoteHead(t, bare) != landed || remote(t, bare, PublicationFile) != "tag v0.2.0\ncommit "+shaB+"\n" {
			t.Error("the older publication changed the wiki after the newer one landed")
		}
	})
	t.Run("an older publication lands first", func(t *testing.T) {
		bare := newWiki(t)
		o, err := raced(t, bare, "v0.2.0", shaB, func() { publishTo(t, bare, "v0.1.0", shaA) })
		if err != nil || o != Published {
			t.Fatalf("Publish = %s, %v", o, err)
		}
		if got := remote(t, bare, PublicationFile); got != "tag v0.2.0\ncommit "+shaB+"\n" {
			t.Errorf("record = %q, want the newer release's", got)
		}
	})
	t.Run("the same tag at another commit lands first", func(t *testing.T) {
		bare := newWiki(t)
		var landed string
		_, err := raced(t, bare, "v0.1.0", shaA, func() {
			publishTo(t, bare, "v0.1.0", shaB)
			landed = remoteHead(t, bare)
		})
		if err == nil || !strings.Contains(err.Error(), "(a moved tag)") {
			t.Fatalf("Publish = %v, want the moved-tag refusal", err)
		}
		if remoteHead(t, bare) != landed {
			t.Error("the refused publication changed the wiki")
		}
	})
	t.Run("the wiki moves under every attempt", func(t *testing.T) {
		bare := newWiki(t)
		p := Publisher{Wiki: cloneWiki(t, bare), Out: output(t, "v0.1.0", shaA), MaxAttempts: 3,
			beforePush: func() { webEdit(t, bare) }}
		_, err := p.Publish()
		if err == nil || !strings.Contains(err.Error(), "the wiki moved under every one of 3 attempts") {
			t.Fatalf("Publish = %v", err)
		}
	})
}
