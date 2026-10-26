package wiki

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// releaseTag is a release tag as the Makefile's release-version target admits it,
// read for SemVer 2.0.0 precedence.
type releaseTag struct {
	tag     string
	core    [3]int
	prerelL []string
}

// The release-version rule (Makefile): v + strict SemVer without build metadata,
// numbers of at most nine digits, at most 47 characters, ending in a letter or digit,
// not ending in -embedded.
var releaseTagPattern = func() *regexp.Regexp {
	num := `(0|[1-9][0-9]{0,8})`
	pre := `(0|[1-9][0-9]{0,8}|[0-9]*[A-Za-z-][0-9A-Za-z-]*)`
	return regexp.MustCompile(`^v` + num + `\.` + num + `\.` + num + `(-` + pre + `(\.` + pre + `)*)?$`)
}()

func parseReleaseTag(tag string) (releaseTag, error) {
	if !releaseTagPattern.MatchString(tag) || len(tag) > 47 || strings.HasSuffix(tag, "-embedded") ||
		!strings.ContainsAny(tag[len(tag)-1:], "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
		return releaseTag{}, fmt.Errorf("%q is not a release tag", tag)
	}
	t := releaseTag{tag: tag}
	core, pre, _ := strings.Cut(tag[1:], "-")
	for i, f := range strings.Split(core, ".") {
		t.core[i], _ = strconv.Atoi(f)
	}
	if pre != "" {
		t.prerelL = strings.Split(pre, ".")
	}
	return t, nil
}

// compare orders a and b by SemVer 2.0.0 §11 precedence: -1, 0 or 1.
func (a releaseTag) compare(b releaseTag) int {
	for i := range a.core {
		if c := cmpInt(a.core[i], b.core[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(a.prerelL) == 0 && len(b.prerelL) == 0:
		return 0
	case len(a.prerelL) == 0:
		return 1 // a release follows its prereleases
	case len(b.prerelL) == 0:
		return -1
	}
	for i := 0; i < len(a.prerelL) && i < len(b.prerelL); i++ {
		x, y := a.prerelL[i], b.prerelL[i]
		xn, xerr := strconv.Atoi(x)
		yn, yerr := strconv.Atoi(y)
		var c int
		switch {
		case xerr == nil && yerr == nil:
			c = cmpInt(xn, yn)
		case xerr == nil:
			c = -1 // numeric identifiers precede alphanumeric ones
		case yerr == nil:
			c = 1
		default:
			c = strings.Compare(x, y)
		}
		if c != 0 {
			return c
		}
	}
	return cmpInt(len(a.prerelL), len(b.prerelL))
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
