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
	targets  map[string]bool // every read the readiness may make, both dependencies
	relReads map[string]bool // the PUBREL dependency's reads alone
}

func testNewReadinessFixture(t *testing.T, qos byte) *testReadinessFixture {
	t.Helper()
	e := testNewInflightEnv(t, "client-readiness-"+nuid.Next())
	f := &testReadinessFixture{e: e, dur: e.sess.idHash + "_" + nuid.Next(), qos: qos}
	e.admissionPrerequisites(t, f.dur, qos)
	e.acceptUnderServingAuthority(t)
	ent, _ := e.c.outboundEntitlement()
	f.epoch, f.gen = e.asm.sessStreamEpoch, ent.Gen
	f.targets, f.relReads = map[string]bool{}, map[string]bool{}
	add := func(owned mqttOwnedStream, stream, dur string, into map[string]bool) string {
		own, _, err := e.sess.jsa.loadNameOwnershipPhysical(f.epoch, owned, dur)
		if err != nil || own == nil {
			t.Fatalf("fixture: the ownership of %q: %v", dur, err)
		}
		for _, s := range []string{
			mqttNameOwnershipSubject(owned, dur),
			JSApiPrefix + ".STREAM.INFO." + stream,
			JSApiPrefix + ".CONSUMER.INFO." + stream + "." + dur,
			mqttResourceSubject(e.sess.idHash, f.gen, own.OwnerInc),
		} {
			f.targets[s], into[s] = true, true
		}
		return own.OwnerInc
	}
	f.srcInc = add(mqttOwnedStreamMsgs, mqttStreamName, f.dur, map[string]bool{})
	if qos >= 2 {
		_, _, f.pubRel = mqttPubRelNames(e.sess.idHash, f.gen)
		f.relInc = add(mqttOwnedStreamOut, mqttOutStreamName, f.pubRel, f.relReads)
	}
	f.o = testObserveJSA(t, &e.asm.jsa)
	return f
}

func (f *testReadinessFixture) isReadinessRead(s string) bool { return f.targets[s] }

func (f *testReadinessFixture) isRelRead(s string) bool { return f.relReads[s] }

// assemble runs the admission's readiness and returns what it decided.
func (f *testReadinessFixture) assemble() ([]mqttAdmitDependency, bool) {
	return f.e.sess.assembleAdmitDeps(f.e.c, f.dur, f.qos, f.epoch, f.gen)
}

// The ownership a dependency's consumer name stands under is read ONCE per dependency: the record
// that resolved the incarnation is the one judged. Scoped to the readiness alone — the write boundary reads the
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

// The judgment against the ownership record already read, on a real store: the refusal each
// defect produces, and the ORDER that decides which refusal wins when two defects stand together.
// The equalities are the ones a second, client-checked read of the resource's own name supplied
// by construction; each has its own case.
func TestReadinessJudgeTable(t *testing.T) {
	limit := func(t *testing.T, f *testReadinessFixture) {
		testImposeStreamInactivityLimit(t, f.e.s.nrc, mqttStreamName, time.Hour)
	}
	rewriteResource := func(change func(r *mqttOutboundResource)) func(t *testing.T, f *testReadinessFixture) {
		return func(t *testing.T, f *testReadinessFixture) {
			testRewriteReadinessResource(t, f, change)
		}
	}
	for _, tc := range []struct {
		name   string
		store  func(t *testing.T, f *testReadinessFixture) // a defect written to the store
		own    func(o *mqttNameOwnership)                  // a defect in the record already read
		stream string                                      // the stream the caller names; empty is the right one
		want   string                                      // a refusal's distinguishing text; empty means ready
	}{
		{name: "valid"},
		{name: "ownership names another client", own: func(o *mqttNameOwnership) { o.ID = "client-other" },
			want: `names client "client-other"`},
		{name: "resource names another consumer",
			store: rewriteResource(func(r *mqttOutboundResource) { r.Name = "dur-other" }), want: "the resource names consumer"},
		{name: "resource is a consumer on the other stream",
			store: rewriteResource(func(r *mqttOutboundResource) { r.Kind = mqttResourcePubRelConsumer }), want: "the resource names consumer"},
		{name: "the caller names another stream", stream: mqttOutStreamName, want: "the owned stream"},
		{name: "resource absent beside a stream limit",
			store: func(t *testing.T, f *testReadinessFixture) {
				testDeleteReadinessSubject(t, f, mqttResourceSubject(f.e.sess.idHash, f.gen, f.srcInc))
				limit(t, f)
			}, want: "no message found"},
		{name: "ownership retired beside a stream limit", store: limit,
			own: func(o *mqttNameOwnership) { o.State = mqttNameOwnRetired }, want: "not LIVE"},
		{name: "stream limit", store: limit, want: "inactivity limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testNewReadinessFixture(t, 1)
			if tc.store != nil {
				tc.store(t, f)
			}
			own, _, err := f.e.sess.jsa.loadNameOwnershipPhysical(f.epoch, mqttOwnedStreamMsgs, f.dur)
			if err != nil || own == nil {
				t.Fatalf("fixture: the ownership: %v", err)
			}
			if tc.own != nil {
				tc.own(own)
			}
			stream := mqttStreamName
			if tc.stream != _EMPTY_ {
				stream = tc.stream
			}
			err = f.e.sess.readyForUse(mqttOwnedStreamMsgs, stream, f.dur, f.epoch, f.gen, own)
			if tc.want == _EMPTY_ {
				if err != nil {
					t.Fatalf("a valid dependency was refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("judged ready, want a refusal naming %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the refusal %q does not name %q: another check judged first", err, tc.want)
			}
		})
	}
}

// testRewriteReadinessResource rewrites the source dependency's resource record in place.
func testRewriteReadinessResource(t *testing.T, f *testReadinessFixture, change func(r *mqttOutboundResource)) {
	t.Helper()
	jsa := &f.e.asm.jsa
	r, _, err := jsa.loadOutboundResource(f.e.sess.id, f.epoch, f.gen, f.srcInc)
	if err != nil || r == nil {
		t.Fatalf("fixture: reading the resource: %v", err)
	}
	change(r)
	b, _ := json.Marshal(r)
	if _, err := jsa.storeMsg(mqttResourceSubject(f.e.sess.idHash, f.gen, f.srcInc), -1, b); err != nil {
		t.Fatalf("fixture: rewriting the resource: %v", err)
	}
}

// testDeleteReadinessSubject deletes a subject's last record from the inflight stream.
func testDeleteReadinessSubject(t *testing.T, f *testReadinessFixture, subj string) {
	t.Helper()
	jsa := &f.e.asm.jsa
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil {
		t.Fatalf("fixture: reading what to delete: %v", err)
	}
	if err := jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("fixture: deleting: %v", err)
	}
}

// What each refusal costs, on a real store: the readiness's reads per class. The reads are serial
// and stop at the first refusal, so no class reads more than the admitting path, and dependency 1's
// refusal reads nothing of dependency 2. A refused admission repeats on its AckWait re-offer, so
// each count is also the per-re-offer cost under a persistent fault.
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
		return func(t *testing.T, f *testReadinessFixture) { testDeleteReadinessSubject(t, f, subjOf(f)) }
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
		{"QoS 2 proceeding", 2, nil, true, 8},
		{"ownership absent", 1, deleteSubject(ownSubj), false, 1},
		{"ownership names another client", 1, rewriteOwnership(func(o *mqttNameOwnership) { o.ID = "client-other" }), false, 2},
		{"ownership retired", 1, rewriteOwnership(func(o *mqttNameOwnership) { o.State = mqttNameOwnRetired }), false, 2},
		{"resource absent", 1, deleteSubject(resSubj), false, 2},
		{"resource names another consumer", 1, func(t *testing.T, f *testReadinessFixture) {
			testRewriteReadinessResource(t, f, func(r *mqttOutboundResource) {
				r.Name = f.e.sess.idHash + "_" + nuid.Next()
				// A second ownership read would follow the new name: count it too.
				f.targets[mqttNameOwnershipSubject(mqttOwnedStreamMsgs, r.Name)] = true
			})
		}, false, 2},
		{"stream inactivity limit", 1, func(t *testing.T, f *testReadinessFixture) {
			testImposeStreamInactivityLimit(t, f.e.s.nrc, mqttStreamName, time.Hour)
		}, false, 3},
		{"consumer absent", 1, func(t *testing.T, f *testReadinessFixture) {
			if _, err := f.e.asm.jsa.deleteConsumer(mqttStreamName, f.dur, false); err != nil {
				t.Fatalf("fixture: deleting the consumer: %v", err)
			}
		}, false, 4},
		// Dependency 1 refused at its ownership: dependency 2 is never read.
		{"QoS 2, the source's ownership absent", 2, deleteSubject(ownSubj), false, 1},
		{"QoS 2, the source's resource absent", 2, deleteSubject(resSubj), false, 2},
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
			if !tc.ready && tc.qos >= 2 {
				if n := f.o.sends(m, f.isRelRead); n != 0 {
					t.Fatalf("dependency 1's refusal still read dependency 2 (%d reads): %v", n, f.o.sentSubjects(m, f.isRelRead))
				}
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
