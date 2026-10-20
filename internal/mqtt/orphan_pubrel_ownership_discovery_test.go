package mqtt

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// Coverage for how the orphan sweep DISCOVERS a PUBREL consumer to reclaim: from the name's
// RETIRED ownership record, read by the in-flight walk it already makes, and never from a
// $MQTT_out listing. The listing grew with every consumer on the stream and could not be
// paged soundly (its offsets move under another broker's creates and deletes); the ownership
// record is the arm's licence, so the set it can ever delete is exactly the set this walk
// finds.

// testJSAPIRequestCounter counts the JetStream API requests whose subject contains want,
// as the server sees them. A plain subscription on the API space observes the requests the
// broker's own connection publishes.
type testJSAPIRequestCounter struct {
	nc   *nats.Conn
	mu   sync.Mutex
	seen map[string]int
}

func testCountJSAPIRequests(t *testing.T, e *testOrphanEnv) *testJSAPIRequestCounter {
	t.Helper()
	nc, err := nats.Connect(e.s.nrc.ConnectedUrl())
	if err != nil {
		t.Fatalf("connect the API observer: %v", err)
	}
	t.Cleanup(nc.Close)
	c := &testJSAPIRequestCounter{nc: nc, seen: make(map[string]int)}
	if _, err := nc.Subscribe("$JS.API.>", func(m *nats.Msg) {
		c.mu.Lock()
		c.seen[m.Subject]++
		c.mu.Unlock()
	}); err != nil {
		t.Fatalf("subscribe the API observer: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush the API observer: %v", err)
	}
	return c
}

// count returns how many observed requests' subjects start with prefix, after a round trip
// through the server so every request published before the call has been delivered.
func (c *testJSAPIRequestCounter) count(t *testing.T, prefix string) int {
	t.Helper()
	if err := c.nc.Flush(); err != nil {
		t.Fatalf("flush the API observer: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for subj, k := range c.seen {
		if strings.HasPrefix(subj, prefix) {
			n += k
		}
	}
	return n
}

func (c *testJSAPIRequestCounter) reset() {
	c.mu.Lock()
	c.seen = make(map[string]int)
	c.mu.Unlock()
}

// setPubRelAllowance scopes the PUBREL allowance to this account manager.
func (e *testOrphanEnv) setPubRelAllowance(t *testing.T, n int) {
	t.Helper()
	e.asm.mu.Lock()
	e.asm.testPubRelAllowance = &n
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testPubRelAllowance = nil
		e.asm.mu.Unlock()
	})
}

// restartInflightWalkAtFirst makes the next in-flight walk start at the stream's first
// record, as a fresh manager seeded there would.
func (e *testOrphanEnv) restartInflightWalkAtFirst(t *testing.T) {
	t.Helper()
	e.asm.mu.Lock()
	e.asm.inflightSweepSeeded = false
	e.asm.testQoS2SweepSeed = func(firstSeq, _ uint64) uint64 { return firstSeq }
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQoS2SweepSeed = nil
		e.asm.mu.Unlock()
	})
}

func (e *testOrphanEnv) inflightCursor() uint64 {
	e.asm.mu.Lock()
	defer e.asm.mu.Unlock()
	return e.asm.inflightSweepCursor
}

// TestOrphanSweepPubRelDiscoveryTakesNoOutListing runs the PRODUCTION pass over a RETIRED
// name and asserts it reclaims the consumer while sending no CONSUMER.LIST or CONSUMER.NAMES
// request for $MQTT_out. The lookup it does send is the control that the observer sees the
// broker's API traffic at all.
func TestOrphanSweepPubRelDiscoveryTakesNoOutListing(t *testing.T) {
	e := testNewOrphanEnv(t, "pubrel-no-listing")
	durable, _, _ := e.retiredPubRelConsumer(t, "pubrel-no-listing-owner")
	api := testCountJSAPIRequests(t, e)

	e.asm.sweepOrphanedQoS2State(e.s)

	if e.consumerExists(t, durable) {
		t.Fatalf("the production pass did not reclaim the consumer under RETIRED name %q", durable)
	}
	if n := api.count(t, "$JS.API.CONSUMER.LIST."+mqttOutStreamName); n != 0 {
		t.Fatalf("the pass listed $MQTT_out %d time(s): PUBREL discovery must not enumerate the stream", n)
	}
	if n := api.count(t, "$JS.API.CONSUMER.NAMES."+mqttOutStreamName); n != 0 {
		t.Fatalf("the pass enumerated $MQTT_out names %d time(s)", n)
	}
	if n := api.count(t, "$JS.API.CONSUMER.INFO."+mqttOutStreamName+"."+durable); n == 0 {
		t.Fatal("fixture: the observer saw no lookup of the reclaimed consumer, so its zero listing count proves nothing")
	}
}

// TestOrphanSweepPubRelDiscoveryRefusesAnUnsupportedOwnership plants ownership records no
// supported writer emits, each RETIRED, and asserts the walk admits none of them. The
// assertion is on ADMISSION: the later config validation would refuse some of these names
// anyway, and must not be what hides an admitted bad record.
//
// Where the record names an existing, attributable PUBREL consumer (the epoch and the
// subject cases), the test also asserts that consumer survives a full pass, since admitting
// either record would delete it.
func TestOrphanSweepPubRelDiscoveryRefusesAnUnsupportedOwnership(t *testing.T) {
	type body = map[string]any
	retired := func(epoch, stream, name string) body {
		return body{
			"id": "pubrel-planted", "epoch": epoch, "stream": stream, "name": name,
			"state": int(mqttNameOwnRetired), "owner_gen": "planted-gen", "owner_inc": "planted-inc",
		}
	}
	for _, tc := range []struct {
		name string
		// plant returns the subject and body to store, and the consumer that must survive
		// ("" when the case names no existing consumer).
		plant func(t *testing.T, e *testOrphanEnv) (subject string, b body, survivor string)
	}{
		{"another epoch", func(t *testing.T, e *testOrphanEnv) (string, body, string) {
			d := testCreatePubRelConsumer(t, e, getHash("pubrel-planted-epoch"), mqttNewSessGen())
			return mqttNameOwnershipSubject(mqttOwnedStreamOut, d), retired("another-epoch", "out", d), d
		}},
		{"filed under another name's subject", func(t *testing.T, e *testOrphanEnv) (string, body, string) {
			d := testCreatePubRelConsumer(t, e, getHash("pubrel-planted-subject"), mqttNewSessGen())
			_, _, other := mqttPubRelNames(getHash("pubrel-planted-other"), mqttNewSessGen())
			return mqttNameOwnershipSubject(mqttOwnedStreamOut, other), retired(e.asm.sessStreamEpoch, "out", d), d
		}},
		{"a msgs-stream body on an out subject", func(t *testing.T, e *testOrphanEnv) (string, body, string) {
			d := testCreatePubRelConsumer(t, e, getHash("pubrel-planted-stream"), mqttNewSessGen())
			return mqttNameOwnershipSubject(mqttOwnedStreamOut, d), retired(e.asm.sessStreamEpoch, "msgs", d), d
		}},
		{"a malformed PUBREL name", func(t *testing.T, e *testOrphanEnv) (string, body, string) {
			n := mqttPubRelConsumerDurablePrefix + "not-a-hash_not-a-gen"
			return mqttNameOwnershipSubject(mqttOwnedStreamOut, n), retired(e.asm.sessStreamEpoch, "out", n), ""
		}},
		{"a bare name without the PUBREL prefix", func(t *testing.T, e *testOrphanEnv) (string, body, string) {
			_, _, d := mqttPubRelNames(getHash("pubrel-planted-bare"), mqttNewSessGen())
			n := strings.TrimPrefix(d, mqttPubRelConsumerDurablePrefix)
			return mqttNameOwnershipSubject(mqttOwnedStreamOut, n), retired(e.asm.sessStreamEpoch, "out", n), ""
		}},
		{"an unsupported member set", func(t *testing.T, e *testOrphanEnv) (string, body, string) {
			d := testCreatePubRelConsumer(t, e, getHash("pubrel-planted-members"), mqttNewSessGen())
			b := retired(e.asm.sessStreamEpoch, "out", d)
			b["extra"] = true
			return mqttNameOwnershipSubject(mqttOwnedStreamOut, d), b, d
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewOrphanEnv(t, "pubrel-planted")
			subject, b, survivor := tc.plant(t, e)
			data, err := json.Marshal(b)
			if err != nil {
				t.Fatalf("encoding the planted record: %v", err)
			}
			nc := e.s.nrc
			resp, err := nc.Request(subject, data, 5*time.Second)
			if err != nil || strings.Contains(string(resp.Data), `"error"`) {
				t.Fatalf("planting %s: %v %s", subject, err, func() string {
					if resp == nil {
						return ""
					}
					return string(resp.Data)
				}())
			}
			// The env holds no supported RETIRED record, so anything admitted is the planted one.
			e.restartInflightWalkAtFirst(t)
			if cands := e.sweepPubRelConsumers(t); len(cands) != 0 {
				t.Fatalf("the walk admitted an unsupported ownership record naming %v", testPubRelDurables(cands))
			}
			if survivor != _EMPTY_ && !e.consumerExists(t, survivor) {
				t.Fatalf("PUBREL consumer %q was deleted on an unsupported ownership record", survivor)
			}
		})
	}
}

// TestOrphanSweepPubRelDiscoveryStopsAtTheAllowance is the scheduling contract: each RETIRED
// name the walk admits costs the arm a round trip whatever its outcome, so the walk STOPS at
// the allowance, and its cursor resumes at the first name it did not hand on. A walk that ran
// on to its record budget and handed the arm only the first names would skip the rest of a
// static run of retired names on every rotation; one that admitted past the allowance would
// break the arm's per-pass bound.
//
// Three retired names in walk order: the first two long since reclaimed (their lookups are
// NotFound), the third still standing, and an in-flight register stored after all three.
// With an allowance of two, the first pass must take exactly the first two, leave the cursor
// just after the second's record, and look up no more than two consumers; the second pass
// must start at the third and reclaim it; and once nothing retired remains ahead, a pass
// walks the rest of the stream, discovers the register beyond the region, and wraps. The
// register is the other classes' stake: the stop slows their discovery inside an
// ownership-heavy region, and must never strand what lies past it.
func TestOrphanSweepPubRelDiscoveryStopsAtTheAllowance(t *testing.T) {
	e := testNewOrphanEnv(t, "pubrel-allowance")
	a, _, _ := e.retiredPubRelConsumer(t, "pubrel-allowance-a")
	b, _, _ := e.retiredPubRelConsumer(t, "pubrel-allowance-b")
	c, _, _ := e.retiredPubRelConsumer(t, "pubrel-allowance-c")
	for _, d := range []string{a, b} {
		if _, derr := e.asm.jsa.deleteConsumer(mqttOutStreamName, d, false); derr != nil {
			t.Fatalf("deleting %q: %v", d, derr)
		}
	}
	regSubject := mqttInflightSubject(getHash("pubrel-allowance-register"), mqttNewSessGen(), 9)
	if _, err := e.asm.jsa.storeInflightCAS(regSubject, 0, freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 9, Dur: getHash("pubrel-allowance-register") + "_" + nuid.Next(),
		Seq: 3, QoS: 1, Nonce: mqttNewInflightNonce(),
	})); err != nil {
		t.Fatalf("storing the register past the retired region: %v", err)
	}
	// One pass in production's order, returning the registers it discovered beside the names.
	pass := func() (regs []string, cands []*mqttPubRelCandidate) {
		t.Helper()
		found, _, cands, err := e.asm.collectInflightCandidates(e.s)
		if err != nil {
			t.Fatalf("collectInflightCandidates: %v", err)
		}
		e.asm.deleteOrphanPubRelConsumers(e.s, cands)
		for _, r := range found {
			regs = append(regs, r.name.subject)
		}
		return regs, cands
	}
	hasReg := func(regs []string) bool { return slices.Contains(regs, regSubject) }

	e.setPubRelAllowance(t, 2)
	e.restartInflightWalkAtFirst(t)
	api := testCountJSAPIRequests(t, e)
	lookups := func() int {
		return api.count(t, "$JS.API.CONSUMER.INFO."+mqttOutStreamName+"."+mqttPubRelConsumerDurablePrefix)
	}

	regs, first := pass()
	if hasReg(regs) {
		t.Fatal("the first pass reached the register past the retired region beyond its allowance")
	}
	if len(first) != 2 || first[0].durable != a || first[1].durable != b {
		t.Fatalf("the first pass must hand on exactly the first two retired names, got %v", testPubRelDurables(first))
	}
	if got, want := e.inflightCursor(), first[1].seq+1; got != want {
		t.Fatalf("the walk's cursor stands at %d, not just after the last name it handed on (%d)", got, want)
	}
	if n := lookups(); n > 2 {
		t.Fatalf("the first pass looked up %d PUBREL consumers against an allowance of 2", n)
	}
	if !e.consumerExists(t, c) {
		t.Fatal("the first pass reached the third retired name beyond its allowance")
	}

	// One name per pass from here, so each pass's stopping point is the name it took.
	e.setPubRelAllowance(t, 1)
	api.reset()
	regs, second := pass()
	if hasReg(regs) {
		t.Fatal("the second pass walked on past the name that spent its allowance")
	}
	if len(second) != 1 || second[0].durable != c {
		t.Fatalf("the second pass must start at the first name the first did not hand on (%q), got %v", c, testPubRelDurables(second))
	}
	if e.consumerExists(t, c) {
		t.Fatalf("the second pass did not reclaim %q", c)
	}

	// Nothing retired lies ahead of the third, so the third pass walks the rest of the
	// stream, wraps, and meets the first name again: the region did not stall the walk.
	regs, third := pass()
	if !hasReg(regs) {
		t.Fatal("the register past the retired region was never discovered: the stop stranded the other classes")
	}
	if len(third) != 1 || third[0].durable != a {
		t.Fatalf("after the retired region, the walk must reach the end, wrap and meet %q first; got %v", a, testPubRelDurables(third))
	}
}

// TestOrphanSweepPubRelDiscoveryMeetsARetirementBehindTheCursor: a name LIVE when the walk
// passed it and retired afterwards is rewritten at a later sequence, so the walk meets it
// again and reclaims its consumer. Nothing about the earlier LIVE observation is kept.
func TestOrphanSweepPubRelDiscoveryMeetsARetirementBehindTheCursor(t *testing.T) {
	e := testNewOrphanEnv(t, "pubrel-behind")
	o := e.liveOwnedPubRelConsumer(t, "pubrel-behind-owner")
	e.restartInflightWalkAtFirst(t)
	if testPubRelDiscovered(e.sweepPubRelConsumers(t), o.durable) {
		t.Fatalf("fixture: PUBREL name %q was admitted while LIVE", o.durable)
	}
	e.retireOwnedPubRel(t, "pubrel-behind-owner", o)
	for i := 0; i < 2 && e.consumerExists(t, o.durable); i++ {
		e.sweepPubRelConsumers(t)
	}
	if e.consumerExists(t, o.durable) {
		t.Fatalf("PUBREL name %q, retired after the walk passed its LIVE record, was never reclaimed", o.durable)
	}
}

func testPubRelDurables(cands []*mqttPubRelCandidate) []string {
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.durable)
	}
	return out
}
