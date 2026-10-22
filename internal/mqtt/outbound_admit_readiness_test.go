package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// testReadinessFixture is an admission's readiness world: a session with a source consumer (and
// for QoS 2 the canonical PUBREL consumer) standing and claimed by name, the connection accepted
// under the serving authority, and the names every readiness read targets.
type testReadinessFixture struct {
	e        *testInflightEnv
	o        *testJSAObserver
	dur      string
	pubRel   string
	epoch    string
	gen      string
	qos      byte
	srcInc   string
	relInc   string
	roundOne map[string]bool // round 1's targets
	roundTwo map[string]bool // round 2's targets
}

func testNewReadinessFixture(t *testing.T, qos byte) *testReadinessFixture {
	t.Helper()
	e := testNewInflightEnv(t, "client-readiness-"+nuid.Next())
	f := &testReadinessFixture{e: e, dur: e.sess.idHash + "_" + nuid.Next(), qos: qos}
	e.admissionPrerequisites(t, f.dur, qos)
	e.acceptUnderServingAuthority(t)
	ent, _ := e.c.outboundEntitlement()
	f.epoch, f.gen = e.asm.sessStreamEpoch, ent.Gen
	f.roundOne, f.roundTwo = map[string]bool{}, map[string]bool{}
	add := func(owned mqttOwnedStream, stream, dur string) string {
		own, _, err := e.sess.jsa.loadNameOwnershipPhysical(f.epoch, owned, dur)
		if err != nil || own == nil {
			t.Fatalf("fixture: the ownership of %q: %v", dur, err)
		}
		f.roundOne[mqttNameOwnershipSubject(owned, dur)] = true
		f.roundOne[JSApiPrefix+".STREAM.INFO."+stream] = true
		f.roundOne[JSApiPrefix+".CONSUMER.INFO."+stream+"."+dur] = true
		f.roundTwo[mqttResourceSubject(e.sess.idHash, f.gen, own.OwnerInc)] = true
		return own.OwnerInc
	}
	f.srcInc = add(mqttOwnedStreamMsgs, mqttStreamName, f.dur)
	if qos >= 2 {
		_, _, f.pubRel = mqttPubRelNames(e.sess.idHash, f.gen)
		f.relInc = add(mqttOwnedStreamOut, mqttOutStreamName, f.pubRel)
	}
	f.o = testObserveJSA(t, &e.asm.jsa)
	return f
}

func (f *testReadinessFixture) isReadinessRead(s string) bool {
	return f.roundOne[s] || f.roundTwo[s]
}

// assemble runs the admission's readiness and returns what it decided.
func (f *testReadinessFixture) assemble() ([]mqttAdmitDependency, bool) {
	return f.e.sess.assembleAdmitDeps(f.e.c, f.dur, f.qos, f.epoch, f.gen)
}

// The ownership a dependency's consumer name stands under is read ONCE per dependency: the record
// round 1 read is the one judged. Scoped to the readiness alone — the write boundary reads the
// ownership again (ownedLive), and that read is not this one.
func TestReadinessReadsOwnershipOnce(t *testing.T) {
	f := testNewReadinessFixture(t, 2)
	m := f.o.mark()
	if _, ok := f.assemble(); !ok {
		t.Fatal("the fixture's dependencies must be ready")
	}
	for _, own := range []string{mqttNameOwnershipSubject(mqttOwnedStreamMsgs, f.dur), mqttNameOwnershipSubject(mqttOwnedStreamOut, f.pubRel)} {
		if n := f.o.sends(m, testSubjectIs(own)); n != 1 {
			t.Errorf("the readiness read the ownership %q %d times, want once", own, n)
		}
	}
}

// The two rounds, made observable by holding their replies: every round-1 read of BOTH
// dependencies is sent before any of them is answered, then both resources together.
func TestReadinessIssuesTwoRounds(t *testing.T) {
	f := testNewReadinessFixture(t, 2)
	isOne := func(s string) bool { return f.roundOne[s] }
	isTwo := func(s string) bool { return f.roundTwo[s] }
	h1 := f.o.hold(isOne)
	m := f.o.mark()
	type out struct {
		deps []mqttAdmitDependency
		ok   bool
	}
	done := make(chan out, 1)
	go func() {
		deps, ok := f.assemble()
		done <- out{deps, ok}
	}()
	if !f.o.waitSends(m, 6, isOne, 2*time.Second) {
		t.Fatalf("round 1 sent %d of its 6 reads while their replies were held: the reads are serial",
			f.o.sends(m, isOne))
	}
	time.Sleep(50 * time.Millisecond)
	if n := f.o.sends(m, isTwo); n != 0 {
		t.Fatalf("a resource was read (%d) before round 1 was answered", n)
	}
	h2 := f.o.hold(isTwo) // replaces h1 as the armed hold; h1 still delivers what it parked
	f.o.release(h1, nil)
	if !f.o.waitSends(m, 2, isTwo, 2*time.Second) {
		t.Fatalf("round 2 sent %d of its 2 reads while their replies were held: the reads are serial",
			f.o.sends(m, isTwo))
	}
	f.o.release(h2, nil)
	select {
	case r := <-done:
		if !r.ok || len(r.deps) != 2 || r.deps[0].Inc != f.srcInc || r.deps[1].Inc != f.relInc {
			t.Fatalf("the readiness decided %+v (ok %v), want the source %q then the PUBREL %q", r.deps, r.ok, f.srcInc, f.relInc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the readiness did not complete after its replies were released")
	}
	if n := f.o.sends(m, f.isReadinessRead); n != 8 {
		t.Fatalf("the readiness made %d reads, want 8: two rounds of 6 and 2", n)
	}
}

// testValidReadinessDep is one dependency whose every read is valid: a test changes one thing.
func testValidReadinessDep(id, gen, inc string, owned mqttOwnedStream, stream, dur string) *mqttReadinessDep {
	kind := mqttResourceSourceConsumer
	if owned == mqttOwnedStreamOut {
		kind = mqttResourcePubRelConsumer
	}
	return &mqttReadinessDep{
		owned: owned, streamName: stream, durName: dur,
		own:     &mqttNameOwnership{ID: id, Stream: owned, Name: dur, State: mqttNameOwnLive, OwnerGen: gen, OwnerInc: inc},
		si:      &StreamInfo{Config: StreamConfig{Name: stream}},
		ci:      &ConsumerInfo{Config: &ConsumerConfig{Durable: dur}},
		resRead: true,
		res:     &mqttOutboundResource{Kind: kind, State: mqttResourceLive, Name: dur},
	}
}

// The judgment, over hand-built reads: the refusal each defect produces, and the ORDER that
// decides which refusal wins when two defects stand together — the serial order the five reads
// were judged in.
func TestReadinessJudgeTable(t *testing.T) {
	const id, gen = "client-judge", "gen-judge"
	src := func() *mqttReadinessDep {
		return testValidReadinessDep(id, gen, "inc-src", mqttOwnedStreamMsgs, mqttStreamName, "dur-src")
	}
	rel := func() *mqttReadinessDep {
		return testValidReadinessDep(id, gen, "inc-rel", mqttOwnedStreamOut, mqttOutStreamName, "dur-rel")
	}
	limited := func(d *mqttReadinessDep) {
		d.si.Config.ConsumerLimits.InactiveThreshold = time.Hour
	}
	for _, tc := range []struct {
		name string
		deps func() []*mqttReadinessDep
		want string // a refusal's distinguishing text; empty means ready
	}{
		{"valid", func() []*mqttReadinessDep { return []*mqttReadinessDep{src(), rel()} }, ""},
		{"ownership names another client", func() []*mqttReadinessDep {
			d := src()
			d.own.ID = "client-other"
			return []*mqttReadinessDep{d}
		}, `names client "client-other"`},
		{"resource names another consumer", func() []*mqttReadinessDep {
			d := src()
			d.res.Name = "dur-other"
			return []*mqttReadinessDep{d}
		}, "the resource names consumer"},
		{"resource is a consumer on the other stream", func() []*mqttReadinessDep {
			d := src()
			d.res.Kind = mqttResourcePubRelConsumer
			return []*mqttReadinessDep{d}
		}, "the resource names consumer"},
		{"resource unreadable beside a stream limit", func() []*mqttReadinessDep {
			d := src()
			d.res, d.resErr = nil, ApiErrors[JSNoMessageFoundErr]
			limited(d)
			return []*mqttReadinessDep{d}
		}, "no message found"},
		{"dependency 1's resource before dependency 2's ownership", func() []*mqttReadinessDep {
			a, b := src(), rel()
			a.res, a.resErr = nil, ApiErrors[JSNoMessageFoundErr]
			b.own, b.resRead, b.res = nil, false, nil
			return []*mqttReadinessDep{a, b}
		}, `"gen-judge"/"inc-src"`},
		{"ownership retired beside a stream limit", func() []*mqttReadinessDep {
			d := src()
			d.own.State = mqttNameOwnRetired
			limited(d)
			return []*mqttReadinessDep{d}
		}, "not LIVE"},
		{"stream limit", func() []*mqttReadinessDep {
			d := src()
			limited(d)
			return []*mqttReadinessDep{d}
		}, "inactivity limit"},
		{"consumer threshold", func() []*mqttReadinessDep {
			d := src()
			d.ci.Config.InactiveThreshold = time.Hour
			return []*mqttReadinessDep{d}
		}, "effective inactivity threshold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			incs, err := mqttJudgeReadinessAll(id, gen, tc.deps())
			if tc.want == _EMPTY_ {
				if err != nil || len(incs) != 2 || incs[0] != "inc-src" || incs[1] != "inc-rel" {
					t.Fatalf("valid reads judged %v, %v", incs, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("judged ready (%v), want a refusal naming %q", incs, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the refusal %q does not name %q: another check judged first", err, tc.want)
			}
		})
	}
	// The dependency-order case names dependency 1 in the right clause, not merely somewhere.
	a, b := src(), rel()
	a.res, a.resErr = nil, ApiErrors[JSNoMessageFoundErr]
	b.own, b.resRead, b.res = nil, false, nil
	if _, err := mqttJudgeReadinessAll(id, gen, []*mqttReadinessDep{a, b}); err == nil ||
		strings.Contains(err.Error(), "no name-ownership record stands") {
		t.Fatalf("dependency 2's absent ownership outranked dependency 1's resource: %v", err)
	}
}

// Round 2 reads the resource of every dependency whose ownership record was read, whatever else
// round 1 found: a stream limit on the first does not spare its resource read.
func TestReadinessRoundPlan(t *testing.T) {
	a := testValidReadinessDep("c", "g", "inc-a", mqttOwnedStreamMsgs, mqttStreamName, "dur-a")
	a.si.Config.ConsumerLimits.InactiveThreshold = time.Hour
	a.resRead, a.res = false, nil
	b := testValidReadinessDep("c", "g", "inc-b", mqttOwnedStreamOut, mqttOutStreamName, "dur-b")
	b.own, b.resRead, b.res = nil, false, nil
	c := testValidReadinessDep("c", "g", "inc-c", mqttOwnedStreamOut, mqttOutStreamName, "dur-c")
	c.own, c.ownErr, c.resRead, c.res = nil, ApiErrors[JSStreamNotFoundErr], false, nil
	second := mqttReadinessSecondRound([]*mqttReadinessDep{a, b, c})
	if len(second) != 1 || second[0] != a {
		t.Fatalf("round 2 reads %d resources, want exactly the first dependency's (the only one with an ownership record)", len(second))
	}
}

// What each refusal costs, on a real store: the readiness's reads per class, as the round rule
// states them (docs/ARCHITECTURE.md, "Admission readiness in two rounds"). A refused admission
// repeats on its AckWait re-offer, so each count is also the per-re-offer cost under a persistent
// fault.
func TestReadinessRefusalCosts(t *testing.T) {
	type mutateFn func(t *testing.T, f *testReadinessFixture)
	rewriteOwnership := func(change func(o *mqttNameOwnership)) mutateFn {
		return func(t *testing.T, f *testReadinessFixture) {
			jsa := &f.e.asm.jsa
			subj := mqttNameOwnershipSubject(mqttOwnedStreamMsgs, f.dur)
			o, _, err := jsa.loadNameOwnershipPhysical(f.epoch, mqttOwnedStreamMsgs, f.dur)
			if err != nil || o == nil {
				t.Fatalf("fixture: reading the ownership: %v", err)
			}
			change(o)
			b, _ := json.Marshal(o)
			if _, err := jsa.storeMsg(subj, -1, b); err != nil {
				t.Fatalf("fixture: rewriting the ownership: %v", err)
			}
		}
	}
	deleteSubject := func(subjOf func(f *testReadinessFixture) string) mutateFn {
		return func(t *testing.T, f *testReadinessFixture) {
			jsa := &f.e.asm.jsa
			smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subjOf(f))
			if err != nil {
				t.Fatalf("fixture: reading what to delete: %v", err)
			}
			if err := jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); err != nil {
				t.Fatalf("fixture: deleting: %v", err)
			}
		}
	}
	ownSubj := func(f *testReadinessFixture) string { return mqttNameOwnershipSubject(mqttOwnedStreamMsgs, f.dur) }
	resSubj := func(f *testReadinessFixture) string { return mqttResourceSubject(f.e.sess.idHash, f.gen, f.srcInc) }
	for _, tc := range []struct {
		name   string
		qos    byte
		mutate mutateFn
		ready  bool
		reads  int
	}{
		{"proceeding", 1, nil, true, 4},
		{"ownership absent", 1, deleteSubject(ownSubj), false, 3},
		{"ownership names another client", 1, rewriteOwnership(func(o *mqttNameOwnership) { o.ID = "client-other" }), false, 4},
		{"ownership retired", 1, rewriteOwnership(func(o *mqttNameOwnership) { o.State = mqttNameOwnRetired }), false, 4},
		{"resource absent", 1, deleteSubject(resSubj), false, 4},
		{"resource names another consumer", 1, func(t *testing.T, f *testReadinessFixture) {
			jsa := &f.e.asm.jsa
			r, _, err := jsa.loadOutboundResource(f.e.sess.id, f.epoch, f.gen, f.srcInc)
			if err != nil || r == nil {
				t.Fatalf("fixture: reading the resource: %v", err)
			}
			r.Name = f.e.sess.idHash + "_" + nuid.Next()
			b, _ := json.Marshal(r)
			if _, err := jsa.storeMsg(resSubj(f), -1, b); err != nil {
				t.Fatalf("fixture: rewriting the resource: %v", err)
			}
		}, false, 4},
		{"stream inactivity limit", 1, func(t *testing.T, f *testReadinessFixture) {
			testImposeStreamInactivityLimit(t, f.e.s.nrc, mqttStreamName, time.Hour)
		}, false, 4},
		{"consumer absent", 1, func(t *testing.T, f *testReadinessFixture) {
			if _, err := f.e.asm.jsa.deleteConsumer(mqttStreamName, f.dur, false); err != nil {
				t.Fatalf("fixture: deleting the consumer: %v", err)
			}
		}, false, 4},
		// Dependency 1 refused at its ownership, dependency 2 still read in both rounds: the
		// cross-dependency speculation, within the 6 + 2 the admitting path reads.
		{"QoS 2, the source's ownership absent", 2, deleteSubject(ownSubj), false, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testNewReadinessFixture(t, tc.qos)
			if tc.mutate != nil {
				tc.mutate(t, f)
			}
			m := f.o.mark()
			_, ok := f.assemble()
			if ok != tc.ready {
				t.Fatalf("the readiness decided ready=%v, want %v", ok, tc.ready)
			}
			if n := f.o.sends(m, f.isReadinessRead); n != tc.reads {
				t.Fatalf("the readiness made %d reads, want %d: %v", n, tc.reads, f.o.sentSubjects(m, f.isReadinessRead))
			}
		})
	}
}

// testImposeStreamInactivityLimit sets a stream's consumer inactivity limit the way an
// administrator would, out of band.
func testImposeStreamInactivityLimit(t *testing.T, nc *nats.Conn, stream string, d time.Duration) {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream context: %v", err)
	}
	si, err := js.StreamInfo(stream)
	if err != nil {
		t.Fatalf("reading %s: %v", stream, err)
	}
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = d
	if _, err := js.UpdateStream(&cfg); err != nil {
		t.Fatalf("imposing an inactivity limit on %s: %v", stream, err)
	}
}
