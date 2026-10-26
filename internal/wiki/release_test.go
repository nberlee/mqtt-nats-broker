package wiki

import "testing"

// Each pair is strictly ascending by SemVer 2.0.0 §11; both directions are checked.
func TestReleasePrecedence(t *testing.T) {
	for _, c := range [][2]string{
		{"v1.0.0-alpha.1", "v1.0.0"},
		{"v1.0.0-alpha.2", "v1.0.0-alpha.10"},
		{"v1.0.0-alpha.1", "v1.0.0-alpha.beta"},
		{"v1.0.0-1", "v1.0.0-alpha"},
		{"v1.0.0-alpha", "v1.0.0-alpha.1"},
		{"v0.9.9", "v0.10.0"},
		{"v1.0.0", "v2.0.0-alpha.1"},
	} {
		lo, err := parseReleaseTag(c[0])
		if err != nil {
			t.Fatal(err)
		}
		hi, err := parseReleaseTag(c[1])
		if err != nil {
			t.Fatal(err)
		}
		if lo.compare(hi) != -1 || hi.compare(lo) != 1 {
			t.Errorf("%s vs %s: compare = %d, reverse = %d; want -1 and 1", c[0], c[1], lo.compare(hi), hi.compare(lo))
		}
		if lo.compare(lo) != 0 {
			t.Errorf("%s does not equal itself", c[0])
		}
	}
}

func TestReleaseTagRuleMatchesReleaseVersion(t *testing.T) {
	for _, tag := range []string{"v0.1.0", "v0.1.0-alpha.1", "v1.2.3-rc.0", "v999999999.0.0"} {
		if _, err := parseReleaseTag(tag); err != nil {
			t.Errorf("%s refused: %v", tag, err)
		}
	}
	for _, tag := range []string{
		"0.1.0", "v1.0", "v01.0.0", "v1.0.0-01", "v1.0.0+build", "v1000000000.0.0",
		"v1.0.0-", "v1.0.0-embedded", "v1.0.0-a.", "v1.0.0-" + "a1234567890123456789012345678901234567890",
	} {
		if _, err := parseReleaseTag(tag); err == nil {
			t.Errorf("%s admitted", tag)
		}
	}
}
