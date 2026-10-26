// Shared fixtures for the broker-owned stream BUNDLE, and the unit tests for the
// preflight that enforces it.
//
// Untagged on purpose: the ten streams are one recreation unit (F-066), so
// every fixture that hand-builds broker-owned streams now needs these helpers,
// and several of those fixtures live outside the mqtt_fulltests build.

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
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
	if _, err := nc.Request(fmt.Sprintf(server.JSApiStreamDeleteT, name), nil, 5*time.Second); err != nil {
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
		if err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil); err != nil {
			t.Fatalf("an empty account is initial creation and must admit: %v", err)
		}
	})

	t.Run("ten present admits", func(t *testing.T) {
		st := &testBundleLookupStub{info: map[string]*StreamInfo{}}
		for _, n := range names {
			st.info[n] = testPresentInfo(n)
		}
		if err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil); err != nil {
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
				err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil)
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
			if err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil); err == nil {
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
			if err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil); err == nil {
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
				err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil)
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
			if err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil); err != nil {
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
	if err := mqttPreflightBrokerStreamBundle(st, specs, 0, nil); err != nil {
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

// The interleaving a concurrent first start produces, made deterministic: a peer
// provisions the whole bundle in the middle of B's scan, right after B has seen
// $MQTT_sess absent, so B's first scan returns nine-of-ten, a vector no one created.
//
// B must not admit on that vector, must not repair it, and must not need a restart: it
// rescans, read-only, and admits on a LATER scan that by itself returned all ten, then
// adopts the peer's bundle and recovery epoch. Brokers started together reach this
// schedule in practice (every pod of a clustered embedded deployment leaves its
// JetStream wait at the same moment); the concurrent fixture elsewhere excludes it by
// gating both managers at the $MQTT_sess lookup-to-create boundary, and the pause has
// to be inside the scan, which is what the seam provides.
func TestBrokerStreamBundlePreflightRescansAnOverlappingProvisioning(t *testing.T) {
	s1, c1, nc := newBringUpServer(t)
	s1.bundleSettleBudget = mqttBundleSettleBudget // the production budget; literal servers scan once

	// Every lookup's RESULT, by scan, as the preflight saw it.
	var mu sync.Mutex
	scans := map[int]map[string]bool{}
	var peerDone time.Time
	fired := false
	hook := func(scan int, name string, present bool) {
		mu.Lock()
		if scans[scan] == nil {
			scans[scan] = map[string]bool{}
		}
		scans[scan][name] = present
		fire := !fired && scan == 0 && name == mqttSessStreamName
		fired = fired || fire
		mu.Unlock()
		if fire {
			// The peer: provisions all ten, attributed here and not to B.
			for _, cfg := range testBundleConfigs(t, _EMPTY_, 1) {
				jsRawCreateStream(t, nc, cfg)
			}
			mu.Lock()
			peerDone = time.Now()
			mu.Unlock()
		}
	}
	mqttTestPreflightLookupHookV.Store(&hook)
	t.Cleanup(func() { mqttTestPreflightLookupHookV.Store(nil) })

	start := time.Now()
	asm, err := s1.getOrCreateMQTTAccountSessionManager(c1)
	if err != nil {
		t.Fatalf("the same start must admit once a rescan sees the peer's complete bundle, with no restart: %v", err)
	}
	if took := time.Since(start); took >= mqttBundleSettleBudget {
		t.Fatalf("admission took %s, the whole settle budget", took)
	}
	mqttTestPreflightLookupHookV.Store(nil)

	mu.Lock()
	defer mu.Unlock()
	names := mqttTestBrokerStreamNames
	// 1. The intended interleaving occurred: scan 0 returned $MQTT_sess absent and every
	// other member present.
	first := scans[0]
	if len(first) != len(names) || first[mqttSessStreamName] {
		t.Fatalf("scan 0 returned %v, want all %d looked up and %s absent", first, len(names), mqttSessStreamName)
	}
	for _, n := range names {
		if n != mqttSessStreamName && !first[n] {
			t.Fatalf("scan 0 returned %s absent, want the nine the peer had created present", n)
		}
	}
	// 2 and 3. That scan did not admit: a fresh scan ran, looking up every member again,
	// and the admitting (last) scan returned all ten present by itself.
	if len(scans) < 2 {
		t.Fatalf("%d scan(s) ran, want the nine-of-ten scan followed by at least one fresh scan", len(scans))
	}
	last := scans[len(scans)-1]
	if len(last) != len(names) {
		t.Fatalf("the admitting scan looked up %d members, want all %d", len(last), len(names))
	}
	for _, n := range names {
		if !last[n] {
			t.Fatalf("the admitting scan returned %s absent: admission came from a vector that was not complete", n)
		}
	}
	// 4 and 5. B adopted the peer's bundle rather than creating one: the $MQTT_sess it
	// validated against existed before B's admitting scan, and carries the epoch B adopted.
	if !mqttValidSessEpoch(asm.sessStreamEpoch) {
		t.Fatalf("no usable recovery epoch adopted: %q", asm.sessStreamEpoch)
	}
	si, err := asm.jsa.lookupStream(mqttSessStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookup %s: %v", mqttSessStreamName, err)
	}
	if got := si.Config.Metadata[mqttSessEpochMetaKey]; got != asm.sessStreamEpoch {
		t.Fatalf("%s carries epoch %q, B adopted %q", mqttSessStreamName, got, asm.sessStreamEpoch)
	}
	if si.Created.After(peerDone) {
		t.Fatalf("%s was created at %s, after the peer finished at %s: B provisioned it", mqttSessStreamName, si.Created, peerDone)
	}
}

// testScriptedBundleStub answers each scan from its own presence vector: scan i uses
// vectors[min(i, len-1)], advancing when the first spec-table member is looked up
// again. An error map applies to the scans from errFrom on.
type testScriptedBundleStub struct {
	first   string
	vectors []map[string]bool
	errFrom int
	err     map[string]error
	scan    int
	started bool
}

func (st *testScriptedBundleStub) lookupStream(name string) (*StreamInfo, error) {
	if name == st.first {
		if st.started {
			st.scan++
		}
		st.started = true
	}
	if st.errFrom > 0 && st.scan >= st.errFrom {
		if e, ok := st.err[name]; ok {
			return nil, e
		}
	}
	v := st.vectors[min(st.scan, len(st.vectors)-1)]
	if v[name] {
		return testPresentInfo(name), nil
	}
	return nil, ApiErrors[JSStreamNotFoundErr]
}

// testVector is a presence vector holding the members of names whose index pick says.
func testVector(names []string, pick func(i int) bool) map[string]bool {
	v := map[string]bool{}
	for i, n := range names {
		if pick(i) {
			v[n] = true
		}
	}
	return v
}

// The rescans that make a concurrent first start converge, and the refusals they must
// leave intact: a partial vector is never admitted, however long it is waited on, and
// presence is never accumulated across scans.
func TestBrokerStreamBundlePreflightRescans(t *testing.T) {
	t.Parallel()
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)
	all := testVector(names, func(int) bool { return true })
	none := map[string]bool{}
	const budget = 600 * time.Millisecond

	t.Run("a partial vector completed by a peer admits", func(t *testing.T) {
		st := &testScriptedBundleStub{first: names[0], vectors: []map[string]bool{
			testVector(names, func(i int) bool { return i != 0 }), all,
		}}
		if err := mqttPreflightBrokerStreamBundle(st, specs, budget, nil); err != nil {
			t.Fatalf("a rescan saw all ten and must admit: %v", err)
		}
		if st.scan < 1 {
			t.Fatal("admitted without a second scan")
		}
	})

	for k := 1; k < len(names); k++ {
		t.Run(fmt.Sprintf("%d present for the whole budget refuses", k), func(t *testing.T) {
			st := &testScriptedBundleStub{first: names[0], vectors: []map[string]bool{
				testVector(names, func(i int) bool { return i < k }),
			}}
			// A watchdog, so a preflight that ignores its budget fails here instead of
			// hanging the package: past the budget plus slack it is stopped and reported.
			quit := make(chan struct{})
			done := make(chan error, 1)
			start := time.Now()
			go func() { done <- mqttPreflightBrokerStreamBundle(st, specs, budget, quit) }()
			var err error
			select {
			case err = <-done:
			case <-time.After(budget + 5*time.Second):
				close(quit)
				<-done
				t.Fatalf("still rescanning %s after a %s budget", budget+5*time.Second, budget)
			}
			if err == nil {
				t.Fatalf("%d of %d present throughout must be refused", k, len(names))
			}
			if took := time.Since(start); took < budget {
				t.Fatalf("refused after %s, before the %s budget was spent", took, budget)
			}
			if st.scan < 1 {
				t.Fatal("refused without rescanning")
			}
			if present, absent := testBundleRefusalLists(t, err); len(present) != k || len(absent) != len(names)-k {
				t.Fatalf("refusal lists present %v absent %v, want %d and %d", present, absent, k, len(names)-k)
			}
		})
	}

	t.Run("a complete union that no scan saw refuses", func(t *testing.T) {
		half := len(names) / 2
		st := &testScriptedBundleStub{first: names[0], vectors: []map[string]bool{
			testVector(names, func(i int) bool { return i < half }),
			testVector(names, func(i int) bool { return i >= half }),
		}}
		if err := mqttPreflightBrokerStreamBundle(st, specs, budget, nil); err == nil {
			t.Fatal("each scan saw only part of the bundle; accumulating them admitted a vector that never existed")
		}
	})

	t.Run("streams vanishing after a partial scan refuses at once", func(t *testing.T) {
		st := &testScriptedBundleStub{first: names[0], vectors: []map[string]bool{
			testVector(names, func(i int) bool { return i < 3 }), none,
		}}
		start := time.Now()
		err := mqttPreflightBrokerStreamBundle(st, specs, budget, nil)
		if err == nil || !strings.Contains(err.Error(), "only a deletion produces") {
			t.Fatalf("partial then zero = %v, want the deletion refusal", err)
		}
		if time.Since(start) >= budget {
			t.Fatal("the deletion refusal waited out the budget")
		}
	})

	t.Run("an undecided lookup on a rescan fails closed", func(t *testing.T) {
		st := &testScriptedBundleStub{first: names[0], errFrom: 1, err: map[string]error{names[3]: errors.New("lookup outage")},
			vectors: []map[string]bool{testVector(names, func(i int) bool { return i < 3 })}}
		err := mqttPreflightBrokerStreamBundle(st, specs, budget, nil)
		if err == nil || !strings.Contains(err.Error(), "lookup outage") {
			t.Fatalf("an outage on a rescan = %v, want the undecided failure", err)
		}
	})

	t.Run("stopping during a scan starts no further lookup", func(t *testing.T) {
		quit := make(chan struct{})
		st := &testScriptedBundleStub{first: names[0], vectors: []map[string]bool{all}}
		var looked []string
		stop := &testStoppingLookup{inner: st, stopAfter: names[2], quit: quit, looked: &looked}
		err := mqttPreflightBrokerStreamBundle(stop, specs, time.Minute, quit)
		if err == nil || !strings.Contains(err.Error(), "stopped while waiting") {
			t.Fatalf("a stop during the scan = %v, want the stopped error", err)
		}
		if len(looked) != 3 {
			t.Fatalf("lookups %v, want none after the stop (the first three)", looked)
		}
	})

	t.Run("stopping ends the wait", func(t *testing.T) {
		st := &testScriptedBundleStub{first: names[0], vectors: []map[string]bool{
			testVector(names, func(i int) bool { return i < 3 }),
		}}
		quit := make(chan struct{})
		time.AfterFunc(100*time.Millisecond, func() { close(quit) })
		start := time.Now()
		err := mqttPreflightBrokerStreamBundle(st, specs, time.Minute, quit)
		if err == nil || !strings.Contains(err.Error(), "stopped while waiting") {
			t.Fatalf("stopping = %v, want the stopped error", err)
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("stopping did not end the wait")
		}
	})
}

// testStoppingLookup closes quit as the lookup of stopAfter returns, and records every
// lookup made.
type testStoppingLookup struct {
	inner     brokerStreamLookupAPI
	stopAfter string
	quit      chan struct{}
	looked    *[]string
}

func (l *testStoppingLookup) lookupStream(name string) (*StreamInfo, error) {
	*l.looked = append(*l.looked, name)
	si, err := l.inner.lookupStream(name)
	if name == l.stopAfter {
		close(l.quit)
	}
	return si, err
}
