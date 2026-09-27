//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func testUnitCTerminalSource(t *testing.T, qos byte, abandoned bool) (*testAbandonFix, *mqttInflightRecord, uint64) {
	t.Helper()
	f := testAbandonAdmitted(t, fmt.Sprintf("c4-%d-%v", qos, abandoned), qos)
	if abandoned && f.deny() != mqttAbandonDone {
		t.Fatal("abandonment did not land")
	}
	r, seq := f.register(t)
	reply := r.SourceAckReply
	to := mqttExchangeTerminal
	if qos == 2 {
		to = mqttExchangeRejected
	}
	if _, err := f.e.sess.advanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Gen: f.gen,
		Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
		Nonce: r.Lin, To: to, Acq: r.Acq, Register: r, RegisterSeq: seq,
	}); err != nil {
		t.Fatal(err)
	}
	r, seq = f.register(t)
	x := f.exchange(t)
	if r.State != mqttInflightPublishTerminal || r.SourceAckReply != reply ||
		(x.Abandoned != nil) != abandoned || (r.AbandonReason != "") != abandoned ||
		r.ActionToken != "" {
		t.Fatalf("not the required clean terminal pair: register=%+v exchange=%+v", r, x)
	}
	return f, r, seq
}

func TestUnitCOrdinarySourceReplyShape(t *testing.T) {
	base := freshRegisterLineage(&mqttInflightRecord{
		PI: 7, Dur: "SRC", Seq: 41, QoS: 1, State: mqttInflightPublish,
		Nonce: mqttNewInflightNonce(),
	})
	for _, reply := range []string{
		base.SourceAckReply,
		fmt.Sprintf("$JS.ACK.DOMAIN.ACCOUNT.%s.SRC.2.41.9.0.0", mqttStreamName),
	} {
		r := *base
		r.SourceAckReply = reply
		body, _ := json.Marshal(&r)
		if _, err := mqttDecodeInflightRecord(body); err != nil {
			t.Fatal(err)
		}
	}
	for name, reply := range map[string]string{
		"absent": "", "wrong stream": strings.Replace(base.SourceAckReply, mqttStreamName, "OTHER", 1),
		"wrong consumer": testInflightAckSubject("OTHER", 41, 1),
		"wrong sequence": testInflightAckSubject("SRC", 42, 1),
	} {
		t.Run(name, func(t *testing.T) {
			r := *base
			r.SourceAckReply = reply
			body, _ := json.Marshal(&r)
			if _, err := mqttDecodeInflightRecord(body); err == nil {
				t.Fatal("unsupported source reply accepted")
			}
		})
	}
	body, _ := json.Marshal(base)
	for _, suffix := range []string{
		`,"Source_ack_reply":"x"}`,
		`,"source_ack_reply":null}`,
	} {
		if _, err := mqttDecodeInflightRecord(append(body[:len(body)-1:len(body)-1], suffix...)); err == nil {
			t.Fatal("non-closed source reply schema")
		}
	}
	// Every ACK arrow compares the complete carried record, including the reply.
	for _, from := range []mqttInflightState{mqttInflightPublish, mqttInflightDeliveryAbandoned, mqttInflightPubRelReady} {
		for _, to := range []mqttExchangePhase{mqttExchangeTerminal, mqttExchangeRejected, mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain} {
			st, ok := mqttAckSlotArrow(from, to)
			if !ok {
				continue
			}
			cur := *base
			cur.State = from
			next := cur
			next.State = st
			if !mqttIsAckPhaseAdvance(&cur, &next) {
				t.Fatalf("carried reply refused on %s -> %s", from, st)
			}
			next.SourceAckReply = testInflightAckSubject(cur.Dur, cur.Seq, 2)
			if mqttIsAckPhaseAdvance(&cur, &next) {
				t.Fatal("ACK rewrite changed its durable source reply")
			}
		}
	}
}

// M-C56 and M-C23 invoke the actual deletion predicate, not a caller guard.
// Marker, release authority, identity, freshness, phase and action premises are
// independently asserted. The no-fact fixture contains no disposal or source owner.
func TestUnitCTerminalDeletionRequiresSourceCompletion(t *testing.T) {
	for _, abandoned := range []bool{false, true} {
		for _, qos := range []byte{1, 2} {
			t.Run(fmt.Sprintf("fact=%v/qos=%d", abandoned, qos), func(t *testing.T) {
				f, r, seq := testUnitCTerminalSource(t, qos, abandoned)
				sess := f.e.sess
				ref := mqttSlotRefFromRecord(f.subject, seq, r)
				if !abandoned && (len(sess.terminalSources) != 0 || len(sess.srcAckOwed) != 0) {
					t.Fatal("M-C56 fixture is masked by an installed source owner")
				}
				for _, state := range []mqttInflightSourceState{
					mqttInflightSourceLive, mqttInflightSourceUnknown,
					mqttInflightSourceComplete, mqttInflightSourceLost,
				} {
					f.e.asm.mu.Lock()
					f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return state }
					f.e.asm.mu.Unlock()
					rel := sess.mintSlotRelease(f.subject, r, seq)
					d := mqttSlotDeletePublishTerminal
					if mqttValidateInflightShape(r) != nil || ref.seq != seq || !ref.matchesRecord(r) ||
						!mqttSlotGuardsAdmitted(r, ref, d) || !rel.permits(f.subject, r, seq, d) {
						t.Fatal("another deletion premise masks the source contract")
					}
					want := state == mqttInflightSourceComplete || state == mqttInflightSourceLost
					if got := mqttSlotDestructionAdmitted(r, ref, d, f.subject, seq, rel); got != want {
						t.Fatalf("source=%v deletion=%v want=%v", state, got, want)
					}
				}
			})
		}
	}
}

// M-C24: submission and timeout mint no proof. A confirmed ACK can establish
// completion even before a subsequent source read reflects the acknowledgement.
func TestUnitCSourceCompletionRequiresConfirmation(t *testing.T) {
	f, r, seq := testUnitCTerminalSource(t, 1, false)
	state := mqttInflightSourceLive
	calls := 0
	ackErr := errors.New("confirmation timed out")
	f.e.asm.mu.Lock()
	f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return state }
	f.e.asm.testInflightSourceAck = func(reply string) error {
		calls++
		if reply != r.SourceAckReply {
			t.Fatal("completion used a different reply")
		}
		return ackErr
	}
	f.e.asm.mu.Unlock()
	if p := f.e.sess.completeSource(f.subject, seq, r, false); p.permits(f.subject, seq, r) || calls != 0 {
		t.Fatal("classification-only/submission path minted completion")
	}
	if p := f.e.sess.completeSource(f.subject, seq, r, true); p.permits(f.subject, seq, r) || calls != 1 {
		t.Fatal("timeout minted completion, or confirmation was not attempted")
	}
	state = mqttInflightSourceUnknown
	if p := f.e.sess.completeSource(f.subject, seq, r, true); p.permits(f.subject, seq, r) || calls != 1 {
		t.Fatal("Unknown minted completion")
	}
	state, ackErr = mqttInflightSourceLive, nil
	p := f.e.sess.completeSource(f.subject, seq, r, true)
	if !p.permits(f.subject, seq, r) {
		t.Fatal("confirmed completion was refused")
	}
	for _, change := range []func(*mqttInflightRecord){
		func(v *mqttInflightRecord) { v.Dur = "OTHER" },
		func(v *mqttInflightRecord) { v.Seq++ },
		func(v *mqttInflightRecord) { v.Nonce = mqttNewInflightNonce() },
		func(v *mqttInflightRecord) { v.Acq++ },
	} {
		other := *r
		change(&other)
		if p.permits(f.subject, seq, &other) {
			t.Fatal("completion transferred to another identity")
		}
	}
	if p.permits(f.subject, seq+1, r) {
		t.Fatal("completion transferred to another loaded version")
	}
}

// M-C57 substrate: no delivery reply is supplied to the stage. Its only usable
// handle is the register's persisted reply, distinct from the denial-attempt reply.
func TestUnitCTerminalStageUsesDurableReply(t *testing.T) {
	for _, abandoned := range []bool{false, true} {
		t.Run(fmt.Sprint(abandoned), func(t *testing.T) {
			f, r, seq := testUnitCTerminalSource(t, 1, abandoned)
			want := r.SourceAckReply
			if abandoned && want == f.exchange(t).Abandoned.Reply {
				t.Fatal("fixture does not distinguish register reply from fact reply")
			}
			var got string
			fail := true
			f.e.asm.mu.Lock()
			f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return mqttInflightSourceLive }
			f.e.asm.testInflightSourceAck = func(reply string) error {
				got = reply
				if fail {
					return errors.New("lost confirmation")
				}
				return nil
			}
			f.e.asm.mu.Unlock()
			sess := f.e.sess
			if sess.settleTerminalSource(f.subject, seq, r) || got != want {
				t.Fatalf("timeout settled or wrong durable reply: %q want %q", got, want)
			}
			sess.mu.Lock()
			held := sess.sourceRetiredLocked(r.Dur, r.Seq)
			sess.mu.Unlock()
			if !held {
				t.Fatal("lost confirmation released suppression")
			}
			if cur, _ := f.register(t); cur == nil {
				t.Fatal("source settlement deleted its carrier")
			}
			fail = false
			if !sess.settleTerminalSource(f.subject, seq, r) {
				t.Fatal("confirmed source did not settle")
			}
			rel := sess.mintSlotRelease(f.subject, r, seq)
			ref := mqttSlotRefFromRecord(f.subject, seq, r)
			if !mqttSlotDestructionAdmitted(r, ref, mqttSlotDeletePublishTerminal, f.subject, seq, rel) {
				t.Fatal("exact confirmed completion did not reach the deletion boundary")
			}
		})
	}
}

func TestUnitCSourceReplySurvivesReplacement(t *testing.T) {
	r, done := testF105NewReplEnv(t, "c4-reply-replacement")
	defer done()
	r.deleteMarker(t)
	adm := r.replacingFor(t, mqttReplaceRegisterOnly, "txn-c4-replace")
	adm.Register.SourceAckReply = testInflightAckSubject(r.occ.Dur, r.occ.Seq, 9)
	tr, err := mqttReplacePublishTransition(adm)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i := range tr.Extra {
		if tr.Extra[i].subj != r.regSubj {
			continue
		}
		found = true
		rec, err := mqttDecodeInflightRecord(tr.Extra[i].body)
		if err != nil || rec.SourceAckReply != r.occ.SourceAckReply {
			t.Fatal("composer did not carry the predecessor's reply")
		}
		carried := tr.Extra[i].body
		rec.SourceAckReply = adm.Register.SourceAckReply
		tr.Extra[i].body, _ = json.Marshal(rec)
		if _, err := r.asm.jsa.commitAuthorityTransition(tr); err == nil ||
			!strings.Contains(err.Error(), "source ACK reply") {
			t.Fatalf("boundary accepted a changed reply or refused elsewhere: %v", err)
		}
		tr.Extra[i].body = carried
	}
	if !found {
		t.Fatal("no replacing register member")
	}
	if _, err := r.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatal(err)
	}
}
