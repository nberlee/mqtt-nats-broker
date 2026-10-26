// Command wikigen renders the GitHub Wiki from the documentation under docs/ and
// publishes a rendered release into a clone of the wiki repository
// (docs/ARCHITECTURE.md, "The wiki is generated from docs/").
//
//	wikigen render -out DIR [-tag vX.Y.Z] [-sha COMMIT] [-repo OWNER/NAME] [-root DIR]
//	wikigen publish -wiki CLONE -out DIR
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/nberlee/mqtt-nats-broker/internal/wiki"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "wikigen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: wikigen render|publish [flags]")
	}
	switch args[0] {
	case "render":
		return render(args[1:])
	case "publish":
		return publish(args[1:])
	}
	return fmt.Errorf("unknown subcommand %q", args[0])
}

func render(args []string) error {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	root := fs.String("root", ".", "repository checkout")
	out := fs.String("out", "", "output directory: absent or empty, and outside the checkout (required)")
	repo := fs.String("repo", "nberlee/mqtt-nats-broker", "GitHub repository the links point into")
	sha := fs.String("sha", "", "commit the links are pinned to (default: the checkout's HEAD)")
	tag := fs.String("tag", "", "release tag the output is for; without one the output cannot be published")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("render: -out is required")
	}
	if err := checkOutDir(*root, *out); err != nil {
		return err
	}
	if *sha == "" {
		b, err := exec.Command("git", "-C", *root, "rev-parse", "HEAD").Output()
		if err != nil {
			return fmt.Errorf("render: reading HEAD: %w", err)
		}
		*sha = strings.TrimSpace(string(b))
	}
	o, err := wiki.Render(wiki.Config{Root: *root, Repo: *repo, SHA: *sha, Tag: *tag})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	for name, b := range o {
		if err := os.WriteFile(filepath.Join(*out, name), b, 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("wikigen: rendered %d files into %s\n", len(o), *out)
	return nil
}

// checkOutDir refuses an output directory inside the checkout, where markdownlint and
// git would see generated pages, and one that already holds files, which would be
// published as if rendered.
func checkOutDir(root, out string) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absOut, err := filepath.Abs(out)
	if err != nil {
		return err
	}
	if rel, err := filepath.Rel(absRoot, absOut); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("render: -out %s is inside the checkout %s", out, root)
	}
	entries, err := os.ReadDir(out)
	if err == nil && len(entries) > 0 {
		return fmt.Errorf("render: -out %s is not empty", out)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func publish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	clone := fs.String("wiki", "", "clone of the wiki repository, on its served branch (required)")
	out := fs.String("out", "", "output of wikigen render with -tag (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *clone == "" || *out == "" {
		return errors.New("publish: -wiki and -out are required")
	}
	outcome, err := wiki.Publisher{Wiki: *clone, Out: *out}.Publish()
	if err != nil {
		return err
	}
	fmt.Println("wikigen:", outcome)
	return nil
}
