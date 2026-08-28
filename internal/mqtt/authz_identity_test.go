package mqtt

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// The matcher decides every authorization outcome in F-079 and F-081, and it is
// the one part with no broker in the loop, so it is tested here rather than
// inferred from an end-to-end fixture. Each case names the rule it encodes; a
// row whose expected value is the type's zero value is called out, because such
// a row passes against a matcher that does nothing.

func TestPermFilterValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		in    string
		valid bool
	}{
		{"plain", "foo.bar", true},
		{"single wildcard", "foo.*.bar", true},
		{"trailing full wildcard", "foo.>", true},
		{"bare full wildcard", ">", true},
		{"leading dollar is an ordinary token", "$sparkplug.certificates.>", true},
		// Each of these would silently match NOTHING if it reached the matcher,
		// which is a fail-OPEN for a deny entry. They must be refused instead.
		{"empty", "", false},
		{"empty leading token", ".foo", false},
		{"empty interior token", "foo..bar", false},
		{"empty trailing token", "foo.", false},
		{"full wildcard not last", "foo.>.bar", false},
		{"embedded space", "foo bar", false},
		{"embedded tab", "foo\tbar", false},
	} {
		if got := mqttIsValidSubjectFilter(tc.in); got != tc.valid {
			t.Errorf("%s: mqttIsValidSubjectFilter(%q) = %v, want %v", tc.name, tc.in, got, tc.valid)
		}
	}
}

func TestPublishPermissionsAllows(t *testing.T) {
	t.Parallel()
	compile := func(t *testing.T, allow, deny []string) *mqttSubjectPermissions {
		t.Helper()
		p, err := mqttCompilePermissions(&SubjectPermission{Allow: allow, Deny: deny}, false)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return p
	}

	// A nil permission set is what the server sends for an identity with no ACLs
	// configured, and it means UNRESTRICTED. The opposite reading would refuse
	// every publish on a default deployment, so this row is load-bearing.
	var unrestricted *mqttSubjectPermissions
	if !unrestricted.allows("anything.at.all") {
		t.Error("a nil permission set must be unrestricted, not denied")
	}

	t.Run("deny overrides a broader allow", func(t *testing.T) {
		p := compile(t, []string{">"}, []string{"denied.>"})
		if p.allows("denied.q1") {
			t.Error("denied.q1 matched a deny and must not be allowed")
		}
		if !p.allows("allowed.q1") {
			t.Error("allowed.q1 matches no deny and must be allowed")
		}
	})

	t.Run("an empty allow list is unrestricted, an empty deny list denies nothing", func(t *testing.T) {
		p := compile(t, nil, []string{"denied.>"})
		if p.allows("denied.x") {
			t.Error("the deny still applies when no allow list is configured")
		}
		if !p.allows("other.x") {
			t.Error("no allow list means every non-denied subject is allowed")
		}
	})

	t.Run("* matches exactly one token", func(t *testing.T) {
		p := compile(t, []string{"a.*.c"}, nil)
		if !p.allows("a.b.c") {
			t.Error("a.b.c must match a.*.c")
		}
		for _, s := range []string{"a.c", "a.b.x.c", "a.b"} {
			if p.allows(s) {
				t.Errorf("%q must not match a.*.c", s)
			}
		}
	})

	t.Run("> matches one or more trailing tokens but not zero", func(t *testing.T) {
		p := compile(t, []string{"a.>"}, nil)
		if !p.allows("a.b") || !p.allows("a.b.c.d") {
			t.Error("a.> must match one and many trailing tokens")
		}
		if p.allows("a") {
			t.Error("a.> must not match the bare prefix")
		}
	})

	t.Run("a $-leading MQTT-derived subject is an ordinary token", func(t *testing.T) {
		p := compile(t, []string{">"}, []string{"$sparkplug.certificates.>"})
		if p.allows("$sparkplug.certificates.ns.grp.NBIRTH.node") {
			t.Error("the derived certificate subject is denied and must not be allowed")
		}
		if !p.allows("spBv1_0.ns.grp.NBIRTH.node") {
			t.Error("the original birth subject is not denied")
		}
	})

	t.Run("a malformed entry refuses the whole identity", func(t *testing.T) {
		for _, bad := range [][]string{{"foo..bar"}, {"a.>.b"}, {""}} {
			if _, err := mqttCompilePermissions(&SubjectPermission{Deny: bad}, false); err == nil {
				t.Errorf("deny %q compiled; a malformed deny that matches nothing is a fail-open", bad)
			}
		}
	})

	t.Run("a publish entry is never queue-split", func(t *testing.T) {
		// Publish permissions are not queue-scoped, so a space is part of the
		// subject and therefore malformed - not a queue suffix to be trimmed off.
		if _, err := mqttCompilePermissions(&SubjectPermission{Deny: []string{"foo bar"}}, false); err == nil {
			t.Error("a publish deny containing a space must be refused, not split")
		}
	})
}

func TestSubscribeDeniesSub(t *testing.T) {
	t.Parallel()
	compile := func(t *testing.T, deny []string) *mqttSubjectPermissions {
		t.Helper()
		p, err := mqttCompilePermissions(&SubjectPermission{Allow: []string{">"}, Deny: deny}, true)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return p
	}

	// Only the DENY side is evaluated at delivery: NATS already enforced the
	// allow side when it accepted the subscription, so re-checking it here would
	// withhold deliveries NATS admitted.
	var none *mqttSubjectPermissions
	if none.deniesSub("foo.secret", "") {
		t.Error("a nil permission set denies no delivery")
	}

	t.Run("a subject-only deny applies to every queue and to none", func(t *testing.T) {
		p := compile(t, []string{"foo.secret"})
		if !p.deniesSub("foo.secret", "") {
			t.Error("a plain subscription on a denied subject must be denied")
		}
		if !p.deniesSub("foo.secret", "$mqttshare.abc") {
			t.Error("a queued subscription on a denied subject must be denied too")
		}
		if p.deniesSub("foo.public", "$mqttshare.abc") {
			t.Error("an undenied subject must be delivered")
		}
	})

	t.Run("a queue-scoped deny says nothing about a plain subscription", func(t *testing.T) {
		p := compile(t, []string{"foo.secret q1"})
		if p.deniesSub("foo.secret", "") {
			t.Error("a queue-scoped deny must not deny a queueless subscription")
		}
		if !p.deniesSub("foo.secret", "q1") {
			t.Error("the named queue is denied")
		}
		if p.deniesSub("foo.secret", "q2") {
			t.Error("a different queue is not denied")
		}
	})

	t.Run("a queue named literally * or > is compared literally first", func(t *testing.T) {
		p := compile(t, []string{"foo.secret *"})
		if !p.deniesSub("foo.secret", "*") {
			t.Error("the literal queue name * must match its own deny")
		}
		// And the wildcard reading still applies to an ordinary name, which is
		// nats-server's behaviour rather than ours to choose.
		if !p.deniesSub("foo.secret", "anything") {
			t.Error("* as a pattern matches a one-token queue name")
		}
	})

	t.Run("the deny subject is matched with wildcards", func(t *testing.T) {
		p := compile(t, []string{"foo.*.secret"})
		if !p.deniesSub("foo.a.secret", "") {
			t.Error("foo.a.secret must match foo.*.secret")
		}
		if p.deniesSub("foo.a.b.secret", "") {
			t.Error("foo.a.b.secret must not match foo.*.secret")
		}
	})
}

// TestSharedQueueFromGroupDurable pins the affine guard's queue provenance.
//
// The affine subscription is created with a NIL queue, so by delivery time
// sub.queue no longer names the shared group the copy came from; the guard
// recovers it from the claim's group durable instead. That recovery is only sound
// because mqttSharedNames derives queue, deliver subject and durable from ONE
// hash, so it is a prefix swap rather than a re-derivation -- and if that ever
// stops being true, this is what says so.
func TestSharedQueueFromGroupDurable(t *testing.T) {
	t.Parallel()

	// Round-trip against the real deriver rather than a hand-written pair: a
	// hardcoded expectation would keep passing if the naming scheme changed.
	wantQueue, _, durable := mqttSharedNames("grp", "foo.bar")
	got, ok := mqttSharedQueueFromGroupDurable(durable)
	if !ok {
		t.Fatalf("a durable this broker generated (%q) must yield its queue", durable)
	}
	if got != wantQueue {
		t.Fatalf("recovered queue %q, want %q", got, wantQueue)
	}

	// Anything this broker did not generate is UNDERIVABLE, and the caller must
	// treat that as "unknown", never as "allowed" -- it withholds the copy without
	// acknowledging it. A permissive default here would silently downgrade a
	// queue-scoped deny to a subject-only check on the one route that cannot fall
	// back to sub.queue.
	for _, bad := range []string{
		"",
		"$MQTT_SHARED_",                   // prefix only, no hash
		"$MQTT_SHARED_tooshort",           // wrong hash length
		"$MQTT_SHARED_0123456789abcdefff", // too long
		"$MQTT_SHARED_0123456789abcde!",   // not a hash character
		"$MQTT_Q2AFF_0123456789abcdef",    // a different class of durable
		"0123456789abcdef",                // no prefix at all
	} {
		if q, ok := mqttSharedQueueFromGroupDurable(bad); ok {
			t.Errorf("durable %q must be underivable, got queue %q", bad, q)
		}
	}
}

// TestSnapshotUsabilityRequiresBothFactsIndependently pins each term of
// snapshotUsableLocked separately.
//
// Neither term can be shown by the end-to-end fixtures, and that is precisely why
// this exists: with derived liveness, closing the connection makes the snapshot
// unusable whichever term you delete, so a test that closes it cannot tell them
// apart. A review round caught two ledger mutations claiming otherwise. Here the
// two facts are set independently.
func TestSnapshotUsabilityRequiresBothFactsIndependently(t *testing.T) {
	t.Parallel()

	// A connected NATS connection with no server behind it is not obtainable, so
	// liveness is exercised through a connection this test really does close.
	srv := runNATSAuthz(t)
	live, err := nats.Connect(srv, nats.UserInfo("alice", "pw"), nats.MaxReconnects(0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer live.Close()

	c := &client{srv: &Server{dialClient: func(string, string, string) (*nats.Conn, error) { return nil, nil }}}
	c.nrc = live

	c.authzValid = true
	if !c.snapshotUsableLocked() {
		t.Fatal("a valid snapshot on a LIVE connection must be usable; without this the two " +
			"negative cases below are satisfied by a helper that always says no")
	}
	c.authzValid = false
	if c.snapshotUsableLocked() {
		t.Fatal("an invalidated snapshot must be unusable even while its connection is live: " +
			"this is the term the disconnect handler sets")
	}
	c.authzValid = true
	live.Close()
	if c.snapshotUsableLocked() {
		t.Fatal("a snapshot whose connection is gone must be unusable even with the flag still " +
			"set: this is the term that makes the install ordering irrelevant")
	}
}

// runNATSAuthz starts a NATS server with an `alice` this package's authz tests
// can dial, and returns its client URL.
func runNATSAuthz(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true,
		Users: []*natsserver.User{{Username: "alice", Password: "pw"}},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}
