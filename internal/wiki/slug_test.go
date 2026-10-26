package wiki

import (
	"encoding/json"
	"os"
	"testing"
)

// TestSlugMatchesGithubSlugger runs github-slugger's own fixtures, captured from
// GitHub's renderer, through one slugger in file order: the fixtures include
// repetitions whose expected anchors depend on the earlier ones.
func TestSlugMatchesGithubSlugger(t *testing.T) {
	raw, err := os.ReadFile("testdata/github-slugger-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Name     string `json:"name"`
		Input    string `json:"input"`
		Expected string `json:"expected"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) < 70 {
		t.Fatalf("only %d fixtures read; the pinned file has 78", len(fixtures))
	}
	s := newSlugger()
	for _, f := range fixtures {
		if f.Name == "Unassigned" {
			// Code points unassigned in Unicode 13.0.0 that Go's newer tables assign
			// (slug.go). Still slugged, so the occurrence counts stay in step.
			s.slug(f.Input)
			continue
		}
		if got := s.slug(f.Input); got != f.Expected {
			t.Errorf("%s: slug(%q) = %q, want %q", f.Name, f.Input, got, f.Expected)
		}
	}
}

func TestSlugCollisions(t *testing.T) {
	s := newSlugger()
	for _, c := range []struct{ in, want string }{
		{"foo", "foo"},
		{"foo", "foo-1"},
		{"foo-1", "foo-1-1"},
		{"foo", "foo-2"},
		// The bump's own candidate is taken: "bar-1" exists before the second "bar".
		{"bar-1", "bar-1"},
		{"bar", "bar"},
		{"bar", "bar-2"},
	} {
		if got := s.slug(c.in); got != c.want {
			t.Errorf("slug(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSlugOfRealHeadings(t *testing.T) {
	for _, c := range []struct{ text, want string }{
		// The rendered text of "## Reserved `$MQTT.>` namespace".
		{"Reserved $MQTT.> namespace", "reserved-mqtt-namespace"},
		{"Eclipse Paho interoperability suite ✅ runs; 26/27 pass, 1 baselined",
			"eclipse-paho-interoperability-suite--runs-2627-pass-1-baselined"},
		{"The outbound obligation authority (F-105, integrated and accepted)",
			"the-outbound-obligation-authority-f-105-integrated-and-accepted"},
		{"été", "été"}, // combining marks are kept
	} {
		if got := slugOf(c.text); got != c.want {
			t.Errorf("slugOf(%q) = %q, want %q", c.text, got, c.want)
		}
	}
}
