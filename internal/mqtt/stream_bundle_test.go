// Shared fixtures for the broker-owned stream BUNDLE, and the unit tests for the
// preflight that enforces it.
//
// Untagged on purpose: the ten streams are one recreation unit (F-066), so
// every fixture that hand-builds broker-owned streams now needs these helpers,
// and several of those fixtures live outside the mqttport_fulltests build.

package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// jsRawCreateStream creates a stream through the raw JS API with the server's
// own StreamConfig struct, so tests can set fields (AllowMsgTTL, Metadata,
// DenyDelete) independently of the nats.go client's config coverage.
func jsRawCreateStream(t *testing.T, nc *nats.Conn, cfg *StreamConfig) {
	t.Helper()
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal stream config: %v", err)
	}
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), b, 5*time.Second)
	if err != nil {
		t.Fatalf("create stream %s: %v", cfg.Name, err)
	}
	var resp JSApiStreamCreateResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("create stream %s: %+v", cfg.Name, resp.Error)
	}
}

// jsRawDeleteStream deletes a stream through the raw JS API.
func jsRawDeleteStream(t *testing.T, nc *nats.Conn, name string) {
	t.Helper()
	if _, err := nc.Request(fmt.Sprintf(JSApiStreamDeleteT, name), nil, 5*time.Second); err != nil {
		t.Fatalf("delete stream %s: %v", name, err)
	}
}

// ---------------------------------------------------------------------------
// Bundle fixtures
//
// The ten broker-owned streams are ONE recreation unit and startup REFUSES a
// proper subset before any validator runs (mqttPreflightBrokerStreamBundle), so
// a fixture that hand-creates one nonconforming stream no longer reaches the
// validator it means to exercise — it is refused by the count rule instead.
// Every such fixture is therefore built from a COMPLETE bundle, which is the
// shape a production account is in whenever a broker is serving it — not the
// only shape it can ever be in, since an interrupted first provisioning leaves a
// partial one and that is exactly what the preflight refuses.
//
// Four narrow helpers rather than one general one, because the cases differ in
// ways a single helper would paper over: two of them must NOT be stamped with
// the common epoch, since an absent or malformed stamp is the defect under
// test, and one needs several members replaced at once.
// ---------------------------------------------------------------------------

// testBundleConfigs returns the ten configurations the broker itself would
// create, in provisioning order, already stamped with one epoch. Built from the
// production table so a stream added there appears here without being restated.
func testBundleConfigs(t *testing.T, domainTk string, replicas int) []*StreamConfig {
	t.Helper()
	as := &mqttAccountSessionManager{domainTk: domainTk}
	specs := as.brokerStreamSpecs(&Server{}, replicas, replicas, mqttDefaultMaxAckPending)
	if specs[0].post != nil {
		_ = specs[0].post(&StreamInfo{Config: *specs[0].cfg})
	}
	cfgs := make([]*StreamConfig, 0, len(specs))
	for _, spec := range specs {
		if spec.preCreate != nil {
			spec.preCreate()
		}
		cfgs = append(cfgs, spec.cfg)
	}
	return cfgs
}

// testCreateBundle creates all ten streams BY HAND under one epoch, applying
// per-stream overrides. It is the only helper that can express a bundle the
// broker would never create itself — TTL-less members, a poisoned member, or a
// member carrying a foreign epoch — and it returns the epoch they share.
func testCreateBundle(t *testing.T, nc *nats.Conn, overrides map[string]func(*StreamConfig)) string {
	t.Helper()
	cfgs := testBundleConfigs(t, _EMPTY_, 1)
	epoch := cfgs[0].Metadata[mqttSessEpochMetaKey]
	for _, cfg := range cfgs {
		if mut := overrides[cfg.Name]; mut != nil {
			mut(cfg)
		}
		jsRawCreateStream(t, nc, cfg)
	}
	return epoch
}

// testBundleThenPoisonEpoch is the deliberate opposite of the helper above: it
// replaces one member with the metadata given, and NEVER stamps it. Auto-stamping
// these would erase the very defect they exist to test.
func testBundleThenPoisonEpoch(t *testing.T, s *Server, c *client, nc *nats.Conn, name string, meta map[string]string) {
	t.Helper()
	if _, err := s.getOrCreateMQTTAccountSessionManager(c); err != nil {
		t.Fatalf("bringing the bundle up: %v", err)
	}
	cfg := (*StreamConfig)(nil)
	for _, bc := range testBundleConfigs(t, _EMPTY_, 1) {
		if bc.Name == name {
			cfg = bc
		}
	}
	if cfg == nil {
		t.Fatalf("%s is not a member of the broker-owned bundle", name)
	}
	cfg.Metadata = meta
	jsRawDeleteStream(t, nc, name)
	jsRawCreateStream(t, nc, cfg)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
}

// ---------------------------------------------------------------------------
// The bundle preflight
// ---------------------------------------------------------------------------

// testBundleLookupStub answers lookupStream from a scripted per-stream outcome
// and RECORDS the names it was asked for, in order.
//
// The recording is not decoration: the preflight's name set must come from the
// same spec table the provisioning loop iterates, and "derived by construction"
// is a claim about the code rather than a test of it. A preflight built from a
// hand-written list that drops or reorders a stream fails on the recording even
// when every presence verdict happens to come out right.
type testBundleLookupStub struct {
	// outcome per stream name; a name in neither map is ABSENT (nil, not-found).
	info map[string]*StreamInfo
	err  map[string]error
	// silent names return (nil, nil): a reply that decoded but carried NEITHER
	// stream information nor an error. It has to be its own field rather than
	// "err[name] == nil", because that is also how an unset entry reads, and the
	// stub would then synthesise a not-found for the very arm under test — the
	// fixture would pass while never reaching the code it names.
	silent map[string]bool
	seen   []string
}

func (st *testBundleLookupStub) lookupStream(name string) (*StreamInfo, error) {
	st.seen = append(st.seen, name)
	if st.silent[name] {
		return nil, nil
	}
	err := st.err[name]
	if err == nil {
		if _, ok := st.info[name]; !ok {
			err = ApiErrors[JSStreamNotFoundErr]
		}
	}
	return st.info[name], err
}

// testBundleSpecs is the production table, used by the preflight tests so a
// stream added there is covered here without being restated.
func testBundleSpecs() []brokerStreamSpec {
	as := &mqttAccountSessionManager{}
	return as.brokerStreamSpecs(&Server{}, 1, 1, mqttDefaultMaxAckPending)
}

func testPresentInfo(name string) *StreamInfo {
	return &StreamInfo{Config: StreamConfig{Name: name}}
}

// testBundleRefusalLists extracts the refusal's own "present: [...]" and
// "absent: [...]" segments.
//
// Parsing them rather than substring-matching the whole message is the point: the
// refusal also lists all ten names as the streams to delete, so "the error
// mentions $MQTT_out" is true whatever the vector says. A mutation that emptied
// both lists survived an assertion written that way.
func testBundleRefusalLists(t *testing.T, err error) (present, absent []string) {
	t.Helper()
	msg := err.Error()
	seg := func(label string) []string {
		i := strings.Index(msg, label+": [")
		if i < 0 {
			t.Fatalf("the refusal carries no %q list: %v", label, err)
		}
		i += len(label) + 3
		j := strings.Index(msg[i:], "]")
		if j < 0 {
			t.Fatalf("the refusal's %q list is unterminated: %v", label, err)
		}
		return strings.Fields(msg[i : i+j])
	}
	return seg("present"), seg("absent")
}

// The count rule, both admitting arms and every refusing one.
//
// "Every count from 1 to 8, in more than one membership shape" is the point: an
// implementation that refuses only some partial shapes — say only when
// $MQTT_sess is missing — passes a spot check and fails here.
func TestBrokerStreamBundlePreflightCountRule(t *testing.T) {
	t.Parallel()
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)

	t.Run("zero present admits", func(t *testing.T) {
		st := &testBundleLookupStub{}
		if err := mqttPreflightBrokerStreamBundle(st, specs); err != nil {
			t.Fatalf("an empty account is initial creation and must admit: %v", err)
		}
	})

	t.Run("ten present admits", func(t *testing.T) {
		st := &testBundleLookupStub{info: map[string]*StreamInfo{}}
		for _, n := range names {
			st.info[n] = testPresentInfo(n)
		}
		if err := mqttPreflightBrokerStreamBundle(st, specs); err != nil {
			t.Fatalf("a complete bundle must admit: %v", err)
		}
	})

	// Every partial count, in two different membership shapes: a PREFIX of the
	// provisioning order, and a scattered set that is not one.
	//
	// The scattered selector must be a PERMUTATION of the index space or it does not
	// produce the count it claims. An earlier version used i*3 mod 9, which is not
	// one — it takes only the values {0,3,6}, so counts 2, 4, 5 and 7 silently got
	// prefix coverage only, and the test asserted a two-shape guarantee it was not
	// delivering. i*(n-1) mod n is (-i) mod n, the reversal, which is a permutation for
	// EVERY n — so this stays a two-shape guarantee however many streams the bundle has.
	n := len(names)
	for k := 1; k < n; k++ {
		for _, shape := range []struct {
			label string
			pick  func(i int) bool
		}{
			{"prefix", func(i int) bool { return i < k }},
			{"scattered", func(i int) bool { return (i*(n-1))%n < k }},
		} {
			t.Run(fmt.Sprintf("%d present/%s refuses", k, shape.label), func(t *testing.T) {
				st := &testBundleLookupStub{info: map[string]*StreamInfo{}}
				var want []string
				for i, n := range names {
					if shape.pick(i) {
						st.info[n] = testPresentInfo(n)
						want = append(want, n)
					}
				}
				// With a permutation selector this cannot fire; it is kept as a
				// tripwire so a future selector that is NOT one fails loudly here
				// instead of silently halving the coverage.
				if len(st.info) != k {
					t.Fatalf("shape %s selected %d streams at k=%d: the selector must be a "+
						"permutation of the index space", shape.label, len(st.info), k)
				}
				err := mqttPreflightBrokerStreamBundle(st, specs)
				if err == nil {
					t.Fatalf("a proper subset (%d of %d) must be refused", len(st.info), len(names))
				}
				// The operator cannot act on "the bundle is inconsistent": the
				// refusal names what is there and what is not, IN THOSE TWO LISTS.
				gotPresent, gotAbsent := testBundleRefusalLists(t, err)
				var wantAbsent []string
				for _, n := range names {
					if _, ok := st.info[n]; !ok {
						wantAbsent = append(wantAbsent, n)
					}
				}
				if strings.Join(gotPresent, " ") != strings.Join(want, " ") {
					t.Fatalf("the refusal's present list is %v, want %v", gotPresent, want)
				}
				if strings.Join(gotAbsent, " ") != strings.Join(wantAbsent, " ") {
					t.Fatalf("the refusal's absent list is %v, want %v", gotAbsent, wantAbsent)
				}
			})
		}
	}

	// Position coverage from both sides: each stream alone, and each stream
	// missing from an otherwise complete bundle.
	for _, n := range names {
		t.Run("only "+n+" refuses", func(t *testing.T) {
			st := &testBundleLookupStub{info: map[string]*StreamInfo{n: testPresentInfo(n)}}
			if err := mqttPreflightBrokerStreamBundle(st, specs); err == nil {
				t.Fatalf("an account holding only %s must be refused", n)
			}
		})
		t.Run("all but "+n+" refuses", func(t *testing.T) {
			st := &testBundleLookupStub{info: map[string]*StreamInfo{}}
			for _, m := range names {
				if m != n {
					st.info[m] = testPresentInfo(m)
				}
			}
			if err := mqttPreflightBrokerStreamBundle(st, specs); err == nil {
				t.Fatalf("an account missing only %s must be refused", n)
			}
		})
	}
}

// The (si, err) PAIR matrix. Both undecided arms fail startup, at EVERY
// position, and neither is folded into absence — folding either one turns a
// transient lookup outage into "the account is empty", which admits and then
// silently reprovisions the streams this whole mechanism exists to protect.
func TestBrokerStreamBundlePreflightUndecidedFailsClosed(t *testing.T) {
	t.Parallel()
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)

	for _, arm := range []struct {
		label string
		// applied to the stub for the position under test
		poison func(st *testBundleLookupStub, n string)
	}{
		{"no info and no error", func(st *testBundleLookupStub, n string) {
			st.silent[n] = true // decoded, but carried neither
		}},
		{"no info and a non-not-found error", func(st *testBundleLookupStub, n string) {
			st.err[n] = fmt.Errorf("transient JetStream failure")
		}},
	} {
		for _, n := range names {
			t.Run(arm.label+" at "+n, func(t *testing.T) {
				// Everything else present, so the ONLY reason to refuse is the
				// undecided position: a count-rule refusal would prove nothing.
				st := &testBundleLookupStub{
					info:   map[string]*StreamInfo{},
					err:    map[string]error{},
					silent: map[string]bool{},
				}
				for _, m := range names {
					st.info[m] = testPresentInfo(m)
				}
				delete(st.info, n)
				arm.poison(st, n)
				err := mqttPreflightBrokerStreamBundle(st, specs)
				if err == nil {
					t.Fatalf("an undecided lookup for %s must fail startup, never read as absent", n)
				}
				if !strings.Contains(err.Error(), n) {
					t.Fatalf("the refusal must name the undecided stream %s: %v", n, err)
				}
				if strings.Contains(err.Error(), "of the 10 broker-owned streams") {
					t.Fatalf("%s was counted as ABSENT rather than undecided: %v", n, err)
				}
			})
		}
	}
}

// Positive evidence outranks a contradictory error, in BOTH error shapes. A
// response saying the stream exists and also erroring is self-contradictory, and
// the half saying it EXISTS is the half that must win — otherwise a bundle is
// read as partial and silently completed.
func TestBrokerStreamBundlePreflightPresentWinsOverError(t *testing.T) {
	t.Parallel()
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)

	for _, tc := range []struct {
		label string
		err   error
	}{
		{"not-found", ApiErrors[JSStreamNotFoundErr]},
		{"a generic error", fmt.Errorf("transient JetStream failure")},
	} {
		t.Run("info plus "+tc.label+" is PRESENT", func(t *testing.T) {
			st := &testBundleLookupStub{info: map[string]*StreamInfo{}, err: map[string]error{}}
			for _, n := range names {
				st.info[n] = testPresentInfo(n)
				st.err[n] = tc.err
			}
			if err := mqttPreflightBrokerStreamBundle(st, specs); err != nil {
				t.Fatalf("ten present-with-%s must admit: %v", tc.label, err)
			}
		})
	}
}

// The preflight asks for exactly the spec table's streams, in its order.
func TestBrokerStreamBundlePreflightUsesTheSpecTable(t *testing.T) {
	t.Parallel()
	specs := testBundleSpecs()
	st := &testBundleLookupStub{}
	if err := mqttPreflightBrokerStreamBundle(st, specs); err != nil {
		t.Fatalf("empty account: %v", err)
	}
	want := mqttBrokerStreamNames(specs)
	if len(st.seen) != len(want) {
		t.Fatalf("the preflight looked up %d streams (%v), want the table's %d (%v)",
			len(st.seen), st.seen, len(want), want)
	}
	for i := range want {
		if st.seen[i] != want[i] {
			t.Fatalf("lookup %d was %q, want %q: the preflight must walk the spec table, not a second list",
				i, st.seen[i], want[i])
		}
	}
}

// validateBundleEpochStamp's own contract, including the case that decides
// whether an ordering fault fails open: an unusable wantEpoch is a REFUSAL, not
// a wildcard that admits every stream.
func TestValidateBundleEpochStamp(t *testing.T) {
	t.Parallel()
	good := mqttGenSessEpoch()
	stamped := func(v string) *StreamConfig {
		return &StreamConfig{Metadata: map[string]string{mqttSessEpochMetaKey: v}}
	}
	if err := validateBundleEpochStamp(stamped(good), good); err != nil {
		t.Fatalf("a matching stamp must validate: %v", err)
	}
	cfg := stamped(good)
	cfg.Metadata["unrelated"] = "keepme"
	if err := validateBundleEpochStamp(cfg, good); err != nil {
		t.Fatalf("unrelated metadata keys must be accepted: %v", err)
	}
	for _, tc := range []struct {
		name string
		cfg  *StreamConfig
		want string
	}{
		{"absent metadata", &StreamConfig{}, good},
		{"empty stamp", stamped(_EMPTY_), good},
		{"malformed stamp", stamped("not-a-nuid"), good},
		{"foreign but well-formed stamp", stamped(mqttGenSessEpoch()), good},
		{"empty wantEpoch", stamped(good), _EMPTY_},
		{"malformed wantEpoch", stamped(good), "not-a-nuid"},
		{"empty wantEpoch against an empty stamp", stamped(_EMPTY_), _EMPTY_},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateBundleEpochStamp(tc.cfg, tc.want); err == nil {
				t.Fatalf("%s must be refused", tc.name)
			}
		})
	}
}

// The interleaving the preflight's non-atomic scan actually admits, produced
// deterministically rather than hoped for.
//
// The vector B observes here NEVER EXISTED DURABLY: B reads $MQTT_sess absent, a peer
// then provisions the whole bundle, and B's remaining nine lookups all report
// present. B therefore sees nine-of-ten, which is a state no operator created, and
// REFUSES. That refusal is the documented behaviour of the settled contract — the
// availability cost of a fail-closed count rule — and this test pins it as intended
// rather than accidental, together with the restart that converges it.
//
// Two brokers started concurrently and raced cannot reach this: the existing
// concurrent fixture gates both managers at the $MQTT_sess lookup-to-create boundary,
// so both complete their preflights against zero before either creates anything. The
// pause has to be inside the scan, which is what the seam provides.
func TestBrokerStreamBundlePreflightRefusesAnOverlappingScan(t *testing.T) {
	s1, c1, nc := newBringUpServer(t)

	// The peer provisions the bundle DURING B's scan, immediately after B has seen
	// the first stream absent. Fired once: the hook is also live for the peer's own
	// preflight, and re-entering would deadlock on the map it is populating.
	fired := false
	hook := func(name string) {
		if fired || name != mqttSessStreamName {
			return
		}
		fired = true
		for _, cfg := range testBundleConfigs(t, _EMPTY_, 1) {
			jsRawCreateStream(t, nc, cfg)
		}
	}
	h := func(name string) { hook(name) }
	mqttTestPreflightLookupHookV.Store(&h)
	t.Cleanup(func() { mqttTestPreflightLookupHookV.Store(nil) })

	_, err := s1.getOrCreateMQTTAccountSessionManager(c1)
	if err == nil {
		t.Fatal("a scan that straddled a peer's provisioning must REFUSE: it observed a " +
			"partial bundle, and admitting on a vector that never existed durably is the " +
			"fail-open this rule exists to prevent")
	}
	present, absent := testBundleRefusalLists(t, err)
	if len(absent) != 1 || absent[0] != mqttSessStreamName {
		t.Fatalf("the refusal's absent list is %v, want exactly [%s] — the one stream read "+
			"before the peer created it", absent, mqttSessStreamName)
	}
	if len(present) != len(mqttTestBrokerStreamNames)-1 {
		t.Fatalf("the refusal's present list is %v, want the other nine", present)
	}

	// And the documented recovery: the peer has completed the bundle, so a restart
	// converges. A fresh Server, because Start's error path is one-shot.
	mqttTestPreflightLookupHookV.Store(nil)
	s2, c2, _ := newBringUpServerOn(t, nc, "node-restart")
	asm, rerr := s2.getOrCreateMQTTAccountSessionManager(c2)
	if rerr != nil {
		t.Fatalf("a restart against the completed bundle must succeed: %v", rerr)
	}
	if !mqttValidSessEpoch(asm.sessStreamEpoch) {
		t.Fatalf("the restart adopted no usable recovery epoch: %q", asm.sessStreamEpoch)
	}
}
