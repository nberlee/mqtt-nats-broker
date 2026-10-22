package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testTraversalFixture is a generation with three outstanding QoS 1 deliveries, admitted as
// production admits them, its custody settled so nothing else reads its inventory meanwhile.
type testTraversalFixture struct {
	e          *testInflightEnv
	o          *testJSAObserver
	gen, epoch string
	exchanges  []string // the three exchange subjects, in admission order
	dutyCount  int
}

func testNewTraversalFixture(t *testing.T) *testTraversalFixture {
	t.Helper()
	e := testNewInflightEnv(t, "client-traversal-"+nuid.Next())
	dur := e.sess.idHash + "_" + nuid.Next()
	f := &testTraversalFixture{e: e, epoch: e.asm.sessStreamEpoch}
	for _, seq := range []uint64{10, 11, 12} {
		pi := e.trackAdmitted(t, dur, seq, 1)
		rec, _ := e.loadRegister(t, pi)
		if rec == nil {
			t.Fatalf("fixture: register %d did not land", pi)
		}
		ent, _ := e.c.outboundEntitlement()
		f.gen = ent.Gen
		f.exchanges = append(f.exchanges, mqttExchangeSubject(e.sess.idHash, f.gen, rec.Lin))
	}
	testWaitCustodySettled(t, e.sess, 5*time.Second)
	led, _, err := e.asm.jsa.loadGenLedger(e.sess.id, f.epoch, f.gen)
	if err != nil || led == nil {
		t.Fatalf("fixture: the ledger: %v", err)
	}
	f.dutyCount = int(led.Hi - led.Lo + 1)
	f.o = testObserveJSA(t, &e.asm.jsa)
	return f
}

func (f *testTraversalFixture) isExchange(s string) bool {
	for _, x := range f.exchanges {
		if s == x {
			return true
		}
	}
	return false
}

func (f *testTraversalFixture) isInventoryRead(s string) bool {
	idHash := f.e.sess.idHash
	return s == mqttGenLedgerSubject(idHash, f.gen) ||
		strings.HasPrefix(s, mqttF105SubjectPrefix()+mqttDutyClassTk+"."+idHash+"."+f.gen+".") || f.isExchange(s)
}

func (f *testTraversalFixture) traverse() (*mqttCustodyCensus, error) {
	return mqttTraverseGenerationInventory(&f.e.asm.jsa, f.e.asm, f.e.sess.id, f.epoch, f.gen, nil)
}

// A duty chunk's exchanges are read in one round: all three are sent before any is answered, and
// the traversal reads exactly what it read one exchange at a time.
func TestTraversalPipelinesExchangeReads(t *testing.T) {
	f := testNewTraversalFixture(t)
	h := f.o.hold(f.isExchange)
	m := f.o.mark()
	type out struct {
		c   *mqttCustodyCensus
		err error
	}
	done := make(chan out, 1)
	go func() {
		c, err := f.traverse()
		done <- out{c, err}
	}()
	if !f.o.waitSends(m, 3, f.isExchange, 2*time.Second) {
		t.Fatalf("the traversal sent %d of the chunk's 3 exchange reads while their replies were held: "+
			"the reads are serial", f.o.sends(m, f.isExchange))
	}
	f.o.release(h, nil)
	select {
	case r := <-done:
		if r.err != nil || !r.c.complete || len(r.c.uncertain) != 0 || len(r.c.live) != 3 {
			t.Fatalf("the traversal read err %v complete %v uncertain %v live %d, want three live exchanges",
				r.err, r.c != nil && r.c.complete, r.c.uncertain, len(r.c.live))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the traversal did not complete after its replies were released")
	}
	if n, want := f.o.sends(m, f.isInventoryRead), 1+f.dutyCount+3; n != want {
		t.Fatalf("the traversal made %d reads, want %d (the ledger, %d duties, 3 exchanges) in three rounds",
			n, want, f.dutyCount)
	}
}

// An exchange read that is never answered is a failed read, never an absence: its subject is
// uncertain, and an admission that would have to prove its identifier free over that inventory is
// refused.
func TestTraversalUnansweredExchangeIsUncertain(t *testing.T) {
	f := testNewTraversalFixture(t)
	dropped := f.exchanges[1]
	drop := func(subject string, body []byte) bool {
		var req JSApiMsgGetRequest
		return strings.HasPrefix(subject, JSApiPrefix+".STREAM.MSG.GET.") &&
			json.Unmarshal(body, &req) == nil && req.LastFor == dropped
	}
	f.e.asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { f.e.asm.jsa.testDropRequest.Store(nil) })

	c, err := f.traverse()
	if err != nil {
		t.Fatalf("the traversal: %v", err)
	}
	if len(c.uncertain) != 1 || c.uncertain[0] != dropped {
		t.Fatalf("the unanswered exchange is not the one uncertain position: uncertain %v", c.uncertain)
	}
	if len(c.live) != 2 {
		t.Fatalf("%d live exchanges, want the two that answered", len(c.live))
	}
	if !c.complete {
		t.Fatal("the pass must still complete: uncertainty is reported, not raised")
	}

	// Downstream: an admission of a fresh identifier on this generation is refused, because the
	// unreadable position may hold it.
	ent, _ := f.e.c.outboundEntitlement()
	dur := f.e.sess.idHash + "_" + nuid.Next()
	f.e.admissionPrerequisites(t, dur, 1)
	deps, ok := f.e.sess.assembleAdmitDeps(f.e.c, dur, 1, f.epoch, ent.Gen)
	if !ok {
		t.Fatal("fixture: the fresh dependency must be ready")
	}
	rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 999, Dur: dur, Seq: 99, QoS: 1, Nonce: mqttNewInflightNonce()})
	_, aerr := f.e.sess.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: f.e.sess.id, Epoch: f.epoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publish",
		Gen: ent.Gen, Tenure: ent.Tenure, Register: rec, Deps: deps,
	})
	if aerr == nil || !strings.Contains(aerr.Error(), "unreadable position") {
		t.Fatalf("an admission over an inventory with an unreadable exchange was not refused for it: %v", aerr)
	}
}

// A generation whose duties name no exchange issues no exchange read.
func TestTraversalNoExchangeDutyIssuesNoExchangeRead(t *testing.T) {
	e := testNewInflightEnv(t, "client-traversal-empty-"+nuid.Next())
	dur := e.sess.idHash + "_" + nuid.Next()
	e.admissionPrerequisites(t, dur, 1) // a source resource and its duty, no obligation
	e.acceptUnderServingAuthority(t)
	ent, _ := e.c.outboundEntitlement()
	testWaitCustodySettled(t, e.sess, 5*time.Second)
	o := testObserveJSA(t, &e.asm.jsa)
	m := o.mark()
	prefix := strings.TrimSuffix(mqttExchangeSubject(e.sess.idHash, ent.Gen, "x"), "x")
	c, err := mqttTraverseGenerationInventory(&e.asm.jsa, e.asm, e.sess.id, e.asm.sessStreamEpoch, ent.Gen, nil)
	if err != nil || !c.complete || len(c.live) != 0 {
		t.Fatalf("the traversal: err %v complete %v live %d", err, c != nil && c.complete, len(c.live))
	}
	if n := o.sends(m, testSubjectHasPrefix(prefix)); n != 0 {
		t.Fatalf("a generation with no exchange duty read %d exchanges", n)
	}
}
