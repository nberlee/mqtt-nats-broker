// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// The acceptance fixture every build of the tests can use (issue #133).

// testAcceptAndBind commits one acceptance of whatever shape the standing authority calls for,
// with a session record derived from the standing one (or a minimal one over none), and binds
// it. gen, when set, is the generation the fixture expects the lifecycle to allocate.
func testAcceptAndBind(t *testing.T, jsa *mqttJSA, domainTk, id, epoch, txn, gen, tenure string, clean bool,
	retire mqttClosingTargets) (*mqttAuthorityTransition, uint64) {

	t.Helper()
	bt, bseq, _ := testAcceptAndBindAs(t, jsa, domainTk, id, epoch, txn, gen, tenure, clean, retire, false)
	return bt, bseq
}

// testAcceptWithWillAnchor commits and binds a delay-capable acceptance of clientID, and returns
// the Will anchor it allocated: what a real CONNECT with a delayed Will leaves on its connection.
func testAcceptWithWillAnchor(t testing.TB, asm *mqttAccountSessionManager, clientID string) *mqttWillAnchorRef {
	t.Helper()
	txn := mqttNewInflightNonce()
	bt, _, anchor := testAcceptAndBindAs(t, &asm.jsa, asm.domainTk, clientID, asm.sessStreamEpoch, txn, _EMPTY_,
		mqttNewInflightNonce(), false, mqttClosingTargets{}, true)
	if anchor == nil {
		t.Fatalf("the delay-capable acceptance of %q allocated no Will anchor", clientID)
	}
	return &mqttWillAnchorRef{L: bt.Auth.Life.L, Ord: anchor.Ord}
}

func testAcceptAndBindAs(t testing.TB, jsa *mqttJSA, domainTk, id, epoch, txn, gen, tenure string, clean bool,
	retire mqttClosingTargets, willDelayCapable bool) (*mqttAuthorityTransition, uint64, *mqttWillAnchor) {

	t.Helper()
	al := jsa.loadOutboundAuthority(id, epoch)
	reg := jsa.loadLifecycleRegistryShard(epoch, mqttLifecycleRegistryShardFor(getHash(id)))
	recSubj := mqttSessSubject(domainTk, getHash(id))
	// Over a closed authority the record a reopen meets is gone or marked, and a marked one is
	// displaced.
	var displaced string
	recordExists := false
	if smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, recSubj); lerr == nil && smsg != nil {
		recordExists = true
		if al.status == mqttOutboundValid && al.rec != nil && al.rec.State == mqttAuthorityClosed {
			var standing mqttPersistedSession
			if json.Unmarshal(smsg.Data, &standing) == nil && standing.Teardown != nil {
				displaced = standing.Teardown.Nonce
			}
		}
	}
	ca, err := jsa.composeAcceptance(mqttAcceptRequest{
		ID: id, Epoch: epoch, Txn: txn, Tenure: tenure, Nonce: "intent-" + txn,
		At: time.Now().UnixNano(), CleanStart: clean, Retire: retire, WillDelayCapable: willDelayCapable,
		RecordExists: recordExists, DisplacedMarker: displaced,
	}, al, reg)
	if err != nil {
		t.Fatalf("composing the acceptance %q: %v", txn, err)
	}
	if gen != _EMPTY_ && ca.Gen != gen {
		t.Fatalf("the acceptance %q installs generation %q, but the fixture expects %q", txn, ca.Gen, gen)
	}
	var recSeq uint64
	ps := &mqttPersistedSession{ID: id, Origin: "o"}
	if smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, recSubj); lerr == nil && smsg != nil {
		recSeq = smsg.Sequence
		if derr := json.Unmarshal(smsg.Data, ps); derr != nil {
			ps = &mqttPersistedSession{ID: id, Origin: "o"}
		}
	}
	rec := *ps
	rec.Gen, rec.PWID, rec.WID, rec.Teardown = ca.Gen, ps.WID, mqttNewRecordWID(), nil
	body, _ := json.Marshal(&rec)
	ca.T.Record = &mqttTransitionMember{subj: recSubj, expect: recSeq, body: body}
	aseq, cerr := jsa.commitAuthorityTransition(ca.T)
	if cerr != nil {
		t.Fatalf("the acceptance %q was refused: %v", txn, cerr)
	}
	bt, berr := mqttComposeBind(ca.T.Auth, aseq, txn+"-fin", "intent-"+txn)
	if berr != nil {
		t.Fatal(berr)
	}
	bseq, ferr := jsa.commitAuthorityTransition(bt)
	if ferr != nil {
		t.Fatalf("binding the acceptance %q was refused: %v", txn, ferr)
	}
	return bt, bseq, ca.Anchor
}

// testAcceptSharedTenure makes tenure the lifecycle's current acceptance (Accept.Ord is the tenure's
// sequence less one, Accept.Tenure its ID): the shared ledger's installations and joins are fenced
// by the authority to exactly that tenure (shared_ledger_fence.go), so a fixture installing a tenure
// of its own choosing stages the acceptance that would have minted it.
func testAcceptSharedTenure(t testing.TB, jsa *mqttJSA, id, epoch string, tenure mqttSharedTenure) {
	t.Helper()
	for attempt := 0; attempt < 8; attempt++ {
		al := jsa.loadOutboundAuthority(id, epoch)
		if al.status != mqttOutboundValid || al.rec.Life == nil {
			t.Fatalf("fixture: the authority of %q reads %v", id, al.status)
		}
		if acc := al.rec.Life.Accept; acc.Ord+1 == tenure.Seq && acc.Tenure == tenure.ID {
			return
		}
		next := *al.rec
		l := *al.rec.Life
		l.Accept.Ord, l.Accept.Tenure = tenure.Seq-1, tenure.ID
		if l.NextAcceptOrd <= l.Accept.Ord {
			l.NextAcceptOrd = l.Accept.Ord + 1
		}
		if l.Pending != nil {
			p := *l.Pending
			p.AcceptOrd = l.Accept.Ord
			l.Pending = &p
		}
		next.Life, next.Rev, next.Txn = &l, al.rec.Rev+1, mqttNewInflightNonce()
		b, _ := json.Marshal(&next)
		msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
		if _, err := jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(id)), hdr, msg); err == nil {
			return
		}
	}
	t.Fatalf("fixture: could not stage tenure %+v as %q's acceptance", tenure, id)
}

// testPlantAnchorAcceptance records, on every Will anchor of the client's lifecycle, a later
// acceptance at time at: the evidence a remote broker's reconnect leaves (issue #133), written
// the way an acceptance batch lowers minAt and nothing else. A concurrent authority write moves
// the CAS, so the read and write are re-attempted.
func testPlantAnchorAcceptance(t testing.TB, asm *mqttAccountSessionManager, clientID string, at int64) {
	t.Helper()
	var last error
	for attempt := 0; attempt < 8; attempt++ {
		al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
		if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil || len(al.rec.Life.Anchors) == 0 {
			t.Fatalf("planting an acceptance for %q: its authority reads %v with no Will anchor", clientID, al.status)
		}
		next := *al.rec
		life := *al.rec.Life
		life.Anchors = append([]mqttWillAnchor(nil), life.Anchors...)
		for i := range life.Anchors {
			if life.Anchors[i].MinAt == 0 || at < life.Anchors[i].MinAt {
				life.Anchors[i].MinAt = at
			}
		}
		next.Life, next.Rev, next.Txn = &life, next.Rev+1, mqttNewInflightNonce()
		b, err := json.Marshal(&next)
		if err != nil {
			t.Fatalf("planting an acceptance for %q: %v", clientID, err)
		}
		msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
		if _, last = asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(clientID)), hdr, msg); last == nil {
			return
		}
	}
	t.Fatalf("planting an acceptance for %q: %v", clientID, last)
}

// testArmWillAnchor gives the client's CURRENT acceptance the Will anchor a delay-capable
// CONNECT's acceptance allocates, in place, and returns it: for a rig whose connection was
// accepted without one and then given a delayed Will. A fresh acceptance would install a new
// generation under the rig's fence.
func testArmWillAnchor(t testing.TB, asm *mqttAccountSessionManager, clientID string) *mqttWillAnchorRef {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
		t.Fatalf("arming a Will anchor for %q: its authority reads %v", clientID, al.status)
	}
	next := *al.rec
	life := *al.rec.Life
	own := mqttWillAnchor{Ord: life.NextAnchorOrd, AcceptOrd: life.Accept.Ord, State: mqttAnchorArmed}
	life.Anchors = append(append([]mqttWillAnchor(nil), life.Anchors...), own)
	life.NextAnchorOrd++
	next.Life, next.Rev, next.Txn = &life, next.Rev+1, mqttNewInflightNonce()
	b, err := json.Marshal(&next)
	if err != nil {
		t.Fatal(err)
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	if _, err := asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(clientID)), hdr, msg); err != nil {
		t.Fatalf("arming a Will anchor for %q: %v", clientID, err)
	}
	return &mqttWillAnchorRef{L: life.L, Ord: own.Ord}
}

// testWillAnchorsOf returns the Will anchors the client's authority carries.
func testWillAnchorsOf(t testing.TB, asm *mqttAccountSessionManager, clientID string) []mqttWillAnchor {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
		t.Fatalf("read the Will anchors of %q: its authority reads %v", clientID, al.status)
	}
	return al.rec.Life.Anchors
}

// testClaimTeardown claims sess's teardown through the claim batch, as the discard boundary
// does: the authority read the boundary would carry, and the retirement of the record's own
// generation.
func testClaimTeardown(t testing.TB, sess *mqttSession, tok recordTx, ps *mqttPersistedSession, seq uint64,
	retiringWID string) (uint64, string, mqttTeardownClaim, error) {

	t.Helper()
	al := sess.jsa.loadOutboundAuthority(sess.id, sess.asm.sessStreamEpoch)
	ret := mqttRetirementRequest{
		Gen: ps.Gen, SessWID: retiringWID, SessSeq: seq, Reason: mqttRetireSessionEnd,
	}
	return sess.claimTeardown(tok, ps, seq, retiringWID, al, ret)
}

// testCommitTeardownClaim discards the generation prev serves through the production claim
// batch, marking the standing record: what a session-end teardown commits. One attempt; the
// caller retries from a fresh read.
func testCommitTeardownClaim(asm *mqttAccountSessionManager, id string, prev *mqttOutboundAuthority, prevSeq uint64) error {
	return testCommitTeardownClaimWith(asm, id, prev, prevSeq, nil)
}

// testCommitTeardownClaimWith is testCommitTeardownClaim with the marker's content shaped by
// mutate before it is written (the marker preserves the record's content, and a fixture may need
// that content to say something, such as an elapsed lifetime).
func testCommitTeardownClaimWith(asm *mqttAccountSessionManager, id string, prev *mqttOutboundAuthority, prevSeq uint64,
	mutate func(*mqttPersistedSession)) error {

	t, err := testComposeTeardownClaim(asm, id, prev, prevSeq, mqttNewInflightNonce(),
		mqttRetirementRequest{Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn}, mutate)
	if err != nil {
		return err
	}
	_, err = asm.jsa.commitAuthorityTransition(t)
	return err
}

// testComposeTeardownClaim composes, and does not send, the production claim batch that discards
// the generation ret names over prev, marking the client's standing record: the one act that
// closes a generation (issue #133). ret carries the ownership the caller means; the record
// identity and the reason are filled in here, and the composer refuses an ownership prev does
// not hold.
func testComposeTeardownClaim(asm *mqttAccountSessionManager, id string, prev *mqttOutboundAuthority, prevSeq uint64,
	txn string, ret mqttRetirementRequest, mutate func(*mqttPersistedSession)) (*mqttAuthorityTransition, error) {

	recSubj := mqttSessSubject(asm.domainTk, getHash(id))
	smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, recSubj)
	if err != nil {
		return nil, err
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return nil, err
	}
	nonce := mqttNewInflightNonce()
	marked := *ps
	marked.WID, marked.PWID = mqttNewRecordWID(), ps.WID
	marked.Teardown = &mqttSessTeardown{Origin: "test", Nonce: nonce, StartedAt: time.Now().Unix()}
	if mutate != nil {
		mutate(&marked)
	}
	body, _ := json.Marshal(&marked)
	ret.SessWID, ret.SessSeq, ret.Reason = ps.WID, smsg.Sequence, mqttRetireSessionEnd
	return asm.jsa.composeTeardownClaim(mqttTeardownClaimRequest{
		ID: id, Epoch: asm.sessStreamEpoch, Txn: txn, Prev: prev, PrevSeq: prevSeq, Retire: ret,
		RecSubj: recSubj, RecSeq: smsg.Sequence, Marker: body, Nonce: nonce, MarkerWID: marked.WID,
	})
}

// testRetireGeneration makes gen read NONCURRENT for clientID (gen_standing.go), staging the
// durable fact its retirement leaves: a generation the authority serves is closed with the
// generation outstanding; one the lifecycle has not yet allocated is moved below the
// allocator, which reads retired; and one of a lifecycle the authority does not carry, or of a
// client with no authority, has its lifecycle recorded retired in the registry shard. The
// fixture refuses a generation that names no lifecycle: no writer produces one.
func testRetireGeneration(t testing.TB, asm *mqttAccountSessionManager, clientID, gen string) {
	t.Helper()
	testRetireGenerationAs(t, asm, clientID, gen, false)
}

func testRetireGenerationAs(t testing.TB, asm *mqttAccountSessionManager, clientID, gen string, later bool) {
	t.Helper()
	l, ord, ok := mqttParseGenToken(gen)
	if !ok {
		t.Fatalf("fixture: generation %q names no lifecycle", gen)
	}
	idHash := getHash(clientID)
	al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
	if al.status == mqttOutboundValid && al.rec != nil && al.rec.Life != nil && al.rec.Life.L == l {
		if s, _ := asm.genStanding(clientID, gen); s == mqttGenOutstanding || s == mqttGenRetired {
			return
		}
		next := *al.rec
		life := *al.rec.Life
		if al.rec.State == mqttAuthorityServing && al.rec.Gen == gen {
			life.Outstanding = append(append([]mqttOutstandingGen(nil), life.Outstanding...), mqttOutstandingGen{
				Ord: ord, Phase: mqttOutstandingOpen,
				Teardown: mqttOutstandingTeardown{Kind: mqttTeardownMarker, Nonce: mqttNewInflightNonce(), MarkerWID: mqttNewRecordWID()},
			})
			if later {
				laterGen, err := mqttGenToken(life.L, life.NextGenOrd)
				if err != nil {
					t.Fatal(err)
				}
				life.Outstanding = append(life.Outstanding, mqttOutstandingGen{
					Ord: life.NextGenOrd, Phase: mqttOutstandingOpen,
					Teardown: mqttOutstandingTeardown{Kind: mqttTeardownMarker, Nonce: mqttNewInflightNonce(), MarkerWID: mqttNewRecordWID()},
				})
				next.Gen = laterGen
				life.NextGenOrd++
			}
			life.Phase, life.GenAcqs, life.Pending = mqttLifecycleClosed, false, nil
			next.State = mqttAuthorityClosed
		} else if later {
			t.Fatalf("fixture: a later generation can only follow one the authority serves")
		} else if ord >= life.NextGenOrd {
			life.NextGenOrd = ord + 1
		}
		next.Life, next.Rev, next.Txn = &life, next.Rev+1, mqttNewInflightNonce()
		body, err := json.Marshal(&next)
		if err != nil {
			t.Fatal(err)
		}
		msg, hdr := mqttFrameBatchWrite(al.seq, body, _EMPTY_, 0, false)
		if _, err := asm.jsa.storeMsg(mqttOutboundAuthoritySubject(idHash), hdr, msg); err != nil {
			t.Fatalf("fixture: retiring generation %q of %q: %v", gen, clientID, err)
		}
	} else {
		shard := mqttLifecycleRegistryShardFor(idHash)
		rl := asm.jsa.loadLifecycleRegistryShard(asm.sessStreamEpoch, shard)
		if rl.status != mqttOutboundValid && rl.status != mqttOutboundAbsent {
			t.Fatalf("fixture: the registry shard of %q reads %v", clientID, rl.status)
		}
		reg := *rl.rec
		if reg.Active[idHash] == l {
			// The caller purged the lifecycle's authority: finish what a completed lifecycle
			// retirement leaves, the client out of the shard's membership. With any other
			// authority standing, the active lifecycle is not one this fixture may retire.
			if al.status != mqttOutboundAbsent {
				t.Fatalf("fixture: lifecycle %d of %q is active, so its generations are not retired", l, clientID)
			}
			active := make(map[string]uint64, len(reg.Active))
			for h, other := range reg.Active {
				if h != idHash {
					active[h] = other
				}
			}
			reg.Active = active
		}
		if reg.Next <= l {
			reg.Next = l + 1
		}
		body, err := json.Marshal(&reg)
		if err != nil {
			t.Fatal(err)
		}
		msg, hdr := mqttFrameBatchWrite(rl.seq, body, _EMPTY_, 0, false)
		if _, err := asm.jsa.storeMsg(mqttLifecycleRegistrySubject(shard), hdr, msg); err != nil {
			t.Fatalf("fixture: retiring lifecycle %d of %q: %v", l, clientID, err)
		}
	}
	if s, _ := asm.genStanding(clientID, gen); s != mqttGenOutstanding && s != mqttGenRetired {
		t.Fatalf("fixture: generation %q of %q still reads %d after its retirement was staged", gen, clientID, s)
	}
}

// testLifecycleGen is generation ord of lifecycle l, for a fixture that names a generation
// without running the acceptance that allocates it.
func testLifecycleGen(t testing.TB, l, ord uint64) string {
	t.Helper()
	gen, err := mqttGenToken(l, ord)
	if err != nil {
		t.Fatal(err)
	}
	return gen
}

// testDisplaceMarker commits another broker's displacing acceptance over the marked record of id
// (plan 5.1 shape 4): a fresh generation's record CASed at the marker's sequence, the marker's
// release discharged in the same batch. It returns the record's write identity. Errors are
// returned rather than reported: it may run on a goroutine that does not own the test.
func testDisplaceMarker(jsa *mqttJSA, domainTk, id, epoch string) (string, error) {
	recSubj := mqttSessSubject(domainTk, getHash(id))
	smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, recSubj)
	if err != nil {
		return _EMPTY_, err
	}
	marker := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, marker); err != nil || marker.Teardown == nil {
		return _EMPTY_, fmt.Errorf("the record of %q holds no marker", id)
	}
	txn := mqttNewInflightNonce()
	ca, err := jsa.composeAcceptance(mqttAcceptRequest{
		ID: id, Epoch: epoch, Txn: txn, Tenure: mqttNewInflightNonce(), Nonce: "intent-" + txn,
		At: time.Now().UnixNano(), CleanStart: true, RecordExists: true, DisplacedMarker: marker.Teardown.Nonce,
	}, jsa.loadOutboundAuthority(id, epoch), mqttRegistryLoad{})
	if err != nil {
		return _EMPTY_, err
	}
	rec := &mqttPersistedSession{Origin: "rival-frontend", ID: id, Gen: ca.Gen, WID: mqttNewRecordWID()}
	body, _ := json.Marshal(rec)
	ca.T.Record = &mqttTransitionMember{subj: recSubj, expect: smsg.Sequence, body: body}
	if _, err := jsa.commitAuthorityTransition(ca.T); err != nil {
		return _EMPTY_, err
	}
	return rec.WID, nil
}

// mqttWillQClientFilter matches every outbox record for one client hash. Production
// never needs it: suppression is decided from the Will's anchor. Tests use it to assert
// "nothing is enqueued for this client".
func mqttWillQClientFilter(domainTk, cidHash string) string {
	return mqttWillQSubjectPrefix + domainTk + cidHash + ".*"
}

// testAnchoredWillID commits a delay-capable acceptance of clientID and returns the outbox
// identity of the Will its anchor protects: the only identity a claimant arbitrates.
func testAnchoredWillID(t testing.TB, asm *mqttAccountSessionManager, clientID string) string {
	t.Helper()
	return testAcceptWithWillAnchor(t, asm, clientID).willID()
}
