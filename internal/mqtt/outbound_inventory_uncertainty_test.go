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
	gen, epoch string
	exchanges  []string // the three exchange subjects, in admission order
	pis        []uint16 // the identifiers they hold, in the same order
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
		f.pis = append(f.pis, pi)
	}
	testWaitCustodySettled(t, e.sess, 5*time.Second)
	return f
}

func (f *testTraversalFixture) traverse() (*mqttCustodyCensus, error) {
	return mqttTraverseGenerationInventory(&f.e.asm.jsa, f.e.asm, f.e.sess.id, f.epoch, f.gen, nil)
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

	// Downstream: an admission claiming the identifier the unreadable exchange's duty names is
	// refused, because that exchange may hold it. One claiming an identifier no duty names is not
	// refused for it: the duty binds its exchange to its identifier, so the exchange cannot hold
	// another (docs/ARCHITECTURE.md, "Identifier exclusivity reads the inventory").
	ent, _ := f.e.c.outboundEntitlement()
	dur := f.e.sess.idHash + "_" + nuid.Next()
	f.e.admissionPrerequisites(t, dur, 1)
	deps, ok := f.e.sess.assembleAdmitDeps(f.e.c, dur, 1, f.epoch, ent.Gen)
	if !ok {
		t.Fatal("fixture: the fresh dependency must be ready")
	}
	admit := func(pi uint16, seq uint64) error {
		rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: dur, Seq: seq, QoS: 1, Nonce: mqttNewInflightNonce()})
		_, aerr := f.e.sess.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
			ID: f.e.sess.id, Epoch: f.epoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publish",
			Gen: ent.Gen, Tenure: ent.Tenure, Register: rec, Deps: deps,
		})
		return aerr
	}
	if aerr := admit(f.pis[1], 98); aerr == nil || !strings.Contains(aerr.Error(), "unreadable position") {
		t.Fatalf("an admission claiming the identifier of an unreadable exchange was not refused for it: %v", aerr)
	}
	if aerr := admit(999, 99); aerr != nil && strings.Contains(aerr.Error(), "unreadable position") {
		t.Fatalf("an admission claiming an identifier no duty names was refused for an unreadable exchange: %v", aerr)
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
