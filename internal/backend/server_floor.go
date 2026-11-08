package backend

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/nats-io/nats-server/v2/server"
)

// The NATS dependency floor is the linked nats-server module itself: the pin and the floor
// name the same release (docs/ARCHITECTURE.md, "Compatibility boundary"), so there is no
// second value to drift from go.mod. A pin bump raises the floor with it.
//
// What startup can check is narrower than what the floor requires. The floor is a
// prerequisite on EVERY JetStream peer; one client connection sees two of them: the server
// it dialed (its INFO version and header capability) and the server that answers
// $JS.API.INFO, which in a cluster is the meta leader (its JetStream API level). Other
// peers, and a later reconnect or leader change, stay the operator's prerequisite.

// semVer is the SemVer 2.0.0 grammar (semver.org's reference expression, the one nats-server
// validates its own VERSION with). Anything it does not match cannot be shown to meet the
// floor, so it is refused rather than ordered.
var semVer = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)` +
	`(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?` +
	`(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)

// parsedVersion is a version's precedence: its core and its prerelease identifiers. Build
// metadata carries no precedence and is dropped.
type parsedVersion struct {
	core [3]int
	pre  []string
}

func parseVersion(v string) (parsedVersion, error) {
	m := semVer.FindStringSubmatch(strings.TrimPrefix(v, "v"))
	if m == nil {
		return parsedVersion{}, fmt.Errorf("%q is not a semantic version", v)
	}
	var p parsedVersion
	for i := range p.core {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return parsedVersion{}, fmt.Errorf("%q is not a semantic version: %w", v, err)
		}
		p.core[i] = n
	}
	if m[4] != "" {
		p.pre = strings.Split(m[4], ".")
	}
	return p, nil
}

// comparePrerelease orders two prerelease identifier lists by SemVer precedence: numeric
// identifiers numerically and below alphanumeric ones, alphanumeric ones in ASCII order (so
// "RC.1" sorts below "preview.1", the order Go's module selection uses too), and a list
// that is a prefix of a longer one below it. An empty list is a release, above every
// prerelease of the same core.
func comparePrerelease(a, b []string) int {
	switch {
	case len(a) == 0 && len(b) == 0:
		return 0
	case len(a) == 0:
		return 1
	case len(b) == 0:
		return -1
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		aNum, bNum := isNumericIdentifier(a[i]), isNumericIdentifier(b[i])
		switch {
		case aNum && bNum:
			// The grammar forbids leading zeros, so the longer digit string is the larger
			// number, at any length.
			if c := cmpInt(len(a[i]), len(b[i])); c != 0 {
				return c
			}
			if c := strings.Compare(a[i], b[i]); c != 0 {
				return c
			}
		case aNum:
			return -1
		case bNum:
			return 1
		default:
			if c := strings.Compare(a[i], b[i]); c != 0 {
				return c
			}
		}
	}
	return cmpInt(len(a), len(b))
}

func isNumericIdentifier(id string) bool {
	return strings.Trim(id, "0123456789") == ""
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

// versionAtLeast reports whether version has at least floor's precedence. A version or
// floor that is not a semantic version is an error, never an ordering.
func versionAtLeast(version, floor string) (bool, error) {
	v, err := parseVersion(version)
	if err != nil {
		return false, err
	}
	f, err := parseVersion(floor)
	if err != nil {
		return false, err
	}
	for i := range v.core {
		if v.core[i] != f.core[i] {
			return v.core[i] > f.core[i], nil
		}
	}
	return comparePrerelease(v.pre, f.pre) >= 0, nil
}

// connectedServerMeetsFloor checks what the service connection's INFO says about the server
// it dialed: its version against the floor, then its header support. The broker's batches,
// its Nmqtt-* envelopes and every Nats-TTL ride headers, and a server configured with
// no_header_support advertises their absence while the headerless startup probe still
// succeeds.
func connectedServerMeetsFloor(version string, headers bool) error {
	ok, err := versionAtLeast(version, server.VERSION)
	if err != nil {
		return fmt.Errorf("nats-server version: %w; every JetStream peer must run nats-server %s or newer",
			err, server.VERSION)
	}
	if !ok {
		return fmt.Errorf("nats-server %s is below this broker's floor %s; every JetStream peer must run nats-server %s or newer",
			version, server.VERSION, server.VERSION)
	}
	if !headers {
		return fmt.Errorf("nats-server %s has header support disabled (no_header_support); the broker requires it on every peer",
			version)
	}
	return nil
}

// jetStreamAPILevelMeetsFloor checks the JetStream API level the startup probe was answered
// with. The answering server is the meta leader in a cluster, so this reaches a peer the
// version check may not; it cannot replace that check, because one API level spans a whole
// release line.
func jetStreamAPILevelMeetsFloor(level int) error {
	if level < server.JSApiLevel {
		return fmt.Errorf("JetStream API level %d (reported by the server answering $JS.API.INFO, the meta leader in a cluster) "+
			"is below this broker's floor %d; every JetStream peer must run nats-server %s or newer",
			level, server.JSApiLevel, server.VERSION)
	}
	return nil
}
