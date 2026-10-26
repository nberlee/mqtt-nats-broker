package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderRefusesAnOutputItCouldMisplace(t *testing.T) {
	root := t.TempDir()
	full := t.TempDir()
	if err := os.WriteFile(filepath.Join(full, "stale.md"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ out, want string }{
		{filepath.Join(root, "wiki"), "is inside the checkout"},
		{root, "is inside the checkout"},
		{full, "is not empty"},
	} {
		err := checkOutDir(root, c.out)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("checkOutDir(%s) = %v, want %q", c.out, err, c.want)
		}
	}
	if err := checkOutDir(root, filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Errorf("an absent directory outside the checkout was refused: %v", err)
	}
	if err := checkOutDir(root, root+"-sibling"); err != nil {
		t.Errorf("a sibling whose name extends the checkout's was refused: %v", err)
	}
}
