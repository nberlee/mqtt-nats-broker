//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"testing"
)

// testF105PrepareTokenBase readies the register an acquisition of act will install its token on,
// in the state the protocol steps a fixture skipped would have left it (F-105 row 2: an acquisition
// is the token AND the record, so a hand-built one needs the register too). It never writes through
// the authority boundary: the fixture is standing in for the admission, the drain of an earlier
// acquisition, or the PUBREC-and-rebind it did not run.
//
//   - no register at all (an exchange a fixture admitted without one): a PUBLISH register naming the
//     exchange's lineage, as opAdmitPublish would have written beside it;
//   - a live token (an earlier acquisition of this exchange): cleared, as that holder's drain would;
//   - a PUBREL acquisition over a slot still at `publish`: moved to `pubrel_ready` with a canonical
//     tuple, as the PUBREC advance and its rebind would;
//   - a counter behind act.Acq-1 (a fixture numbering acquisitions freely): set to act.Acq-1.
func testF105PrepareTokenBase(t *testing.T, e *testF105Env, gen string, act *mqttOutboundAction) (*mqttInflightRecord, uint64) {
	t.Helper()
	subj := mqttInflightSubject(getHash(e.id), gen, act.PI)
	cur, smsg, err := e.asm.jsa.loadInflightRecord(subj)
	if err != nil {
		t.Fatalf("reading the register on %q: %v", subj, err)
	}
	seq := uint64(0)
	if smsg != nil {
		seq = smsg.Sequence
	}
	if cur == nil {
		cur = freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: act.PI, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		})
		cur.Lin = act.Nonce
	}
	next := *cur
	next.ActionToken, next.ActionClaim, next.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	// Only where the PUBREC stands recorded on the exchange: a fixture asking for a PUBREL any
	// earlier is asking to be refused, and the register is left as it is for that refusal.
	// A live PUBREC leaves the slot at pubrel_intended; an earlier fixture path left it at publish.
	if act.Kind == mqttActionPubRel && (next.State == mqttInflightPublish || next.State == mqttInflightPubRelIntended) &&
		testF105PubRecRecorded(e, gen, act.Nonce) {
		next.State = mqttInflightPubRelReady
		next.PubRelDurable, next.PubRelStreamSeq = "$MQTT_PUBREL_fixture", 1
	}
	if act.Acq > 0 && next.Acq != act.Acq-1 {
		next.Acq = act.Acq - 1
	}
	if smsg == nil || next != *cur {
		nseq, serr := e.asm.jsa.storeInflightCAS(subj, seq, &next)
		if serr != nil {
			t.Fatalf("preparing the register on %q: %v", subj, serr)
		}
		return &next, nseq
	}
	return cur, seq
}

// testF105TokenMember is the register member production's composer puts beside an action record.
func testF105TokenMember(t *testing.T, e *testF105Env, gen string, act *mqttOutboundAction) mqttTransitionMember {
	t.Helper()
	cur, seq := testF105PrepareTokenBase(t, e, gen, act)
	next := *cur
	next.ActionToken = mqttNewInflightNonce()
	next.ActionClaim, next.ActionClaimSeq = cur.Claim, cur.ClaimSeq
	next.Acq = cur.Acq + 1
	body, err := json.Marshal(&next)
	if err != nil {
		t.Fatalf("marshalling the token member: %v", err)
	}
	return mqttTransitionMember{subj: mqttInflightSubject(getHash(e.id), gen, act.PI), expect: seq, body: body}
}

// testF105WithRegister fills an acquire-action request's register fields as production's callers do.
func testF105WithRegister(t *testing.T, e *testF105Env, pi uint16, r *mqttAcquireActionRequest) *mqttAcquireActionRequest {
	t.Helper()
	cur, seq := testF105PrepareTokenBase(t, e, r.Gen, &mqttOutboundAction{Nonce: r.Nonce, Acq: r.Acq, Kind: r.Kind, PI: pi})
	r.Register, r.RegisterSeq, r.Token = cur, seq, mqttNewInflightNonce()
	return r
}

func testF105PubRecRecorded(e *testF105Env, gen, nonce string) bool {
	x, _, err := e.asm.jsa.loadOutboundExchange(e.id, e.epoch, gen, nonce)
	return err == nil && x != nil && x.Phase == mqttExchangePubRecObserved
}

// testLinOf reads the lineage of the register a reference names.
func testLinOf(sess *mqttSession, ref *mqttInflightSlotRef) string {
	if sess.asm == nil || ref == nil {
		return _EMPTY_
	}
	if cur, _, err := sess.asm.loadInflightRecord(ref.name.subject); err == nil && cur != nil {
		return cur.Lin
	}
	return _EMPTY_
}

// mqttTestAdmitHolder is a holder that authorizes every execution, for tests of what the boundary
// does AFTER the holder: the gate, the binding, consumption and the three outcomes. The holder's own
// refusals are tested with the production holders. cell is the acquisition's execution cell.
type mqttTestAdmitHolder struct{ cell *mqttExecCell }

func (mqttTestAdmitHolder) enterWrite(*mqttOutboundAction, *client) bool { return true }
func (h mqttTestAdmitHolder) execCell() *mqttExecCell                    { return h.cell }

// testAdmit is an admitting holder carrying the acquisition's cell on c.
func testAdmit(c *client, a *mqttOutboundAction) mqttTestAdmitHolder {
	v, tracked := testExecCells.Load(c)
	if !tracked {
		return mqttTestAdmitHolder{cell: mqttNewExecCell(c)}
	}
	l := v.(*testExecCellList)
	k := mqttActionKeyOf(a)
	l.mu.Lock()
	cell := l.admit[k]
	l.mu.Unlock()
	if cell == nil {
		cell = mqttNewExecCell(c) // the creation hook takes l.mu, so this is made outside it
		l.mu.Lock()
		if l.admit == nil {
			l.admit = map[mqttActionKey]*mqttExecCell{}
		}
		if prev := l.admit[k]; prev != nil {
			cell = prev
		} else {
			l.admit[k] = cell
		}
		l.mu.Unlock()
	}
	return mqttTestAdmitHolder{cell: cell}
}
