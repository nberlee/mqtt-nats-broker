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
)

// Removing replay material (F-105, row 7; contract 6).
//
// # A classification of mine that contradicted itself
//
// This lane's audit called the cleanup contract "closed-ish" in one column, fail-open in the
// summary, and harmless in the explanation — on the reasoning that the undefended direction
// merely ACCUMULATES material. A review took that apart: the reasoning smuggles in a premise,
// that deletion was already authorized against every dependency.
//
//   - WITHOUT that premise, deleting can destroy replay material an obligation still needs.
//   - WITH it, deleting before the manifest is closed still lets a delayed writer resurrect
//     material after an apparently completed cleanup.
//
// "Neither case licenses an unguarded deleter." So a deleter must establish BOTH: the exact
// resource's deletion authorization, and the manifest's closure. Not one standing in for the
// other — they answer different questions. The authorization says no obligation depends on this
// material any more; the closure says nothing can write more of it.
//
// # Deletes are not batch members, so the announcement comes first
//
// A JetStream delete is its own call and cannot ride in the atomic batch, which decides the
// protocol: the manifest's PURGED count is advanced FIRST, durably, and the chunks it covers are
// removed afterwards. An interruption then leaves a state that says what was being removed, and
// a resume re-deletes the same interval — deleting an absent chunk is the one operation that is
// safe to repeat.
//
// The other order is what this lane refuses everywhere else: delete first and record after, and
// a crash leaves material that nothing knows to finish removing while the manifest still claims
// it is all there. `feedback_fence_before_mutation` is the same rule in its original setting.
//
// Because the manifest is CLOSED before any of this, announcing a removal early takes nothing
// from anybody: nothing may be admitted against a closed manifest, so no obligation can arrive
// between the announcement and the delete.
//
// # The manifest record is the LAST thing to go
//
// It is the identity of the material and the progress of its removal. Removing it while chunks
// remain would leave records whose owner, layout and count nothing can state — the shape a
// retirement worker cannot act on and cannot even describe.

// mqttPurgeBatchMax bounds how many chunks one announcement may cover.
//
// The same reasoning as the sweep bound: an announcement is one CAS, and the deletes that follow
// it are individual calls. A purge that announced a hundred-thousand-chunk interval would hold a
// cleanup in a single uninterruptible run, and a crash inside it would be indistinguishable from
// one at the start.
const mqttPurgeBatchMax = 64

// mqttMaterialDeletionAllowed reports whether this material may be removed at all.
//
// BOTH facts are required and neither is inferred from the other. A resource may be authorized
// for deletion while its manifest is still open — that is the window a delayed writer uses — and
// a manifest may be closed while its resource is still live, which is an abandoned
// materialization whose owner has other work.
func mqttMaterialDeletionAllowed(m *mqttRetainedManifest, r *mqttOutboundResource) error {
	if m == nil || r == nil {
		return fmt.Errorf("deleting replay material requires both the manifest and the resource " +
			"that owns its cleanup")
	}
	// THE EXACT RESOURCE, not one that happens to be deletable. An authorization is about a
	// named incarnation of a named generation; reading it as permission to delete other
	// material is the false-reachability shape this project has met before.
	if r.Inc != m.Inc || r.Gen != m.Gen {
		return fmt.Errorf("payload %q names cleanup owner %q of generation %q, and the "+
			"authorization offered is for %q of %q", m.Payload, m.Inc, m.Gen, r.Inc, r.Gen)
	}
	if !mqttResourceDeletable(r) {
		return fmt.Errorf("payload %q may not be removed: its cleanup owner %q stands in state "+
			"%d, and only an authorized deletion establishes that nothing depends on this "+
			"material any more", m.Payload, r.Inc, r.State)
	}
	if m.State != mqttManifestClosed {
		return fmt.Errorf("payload %q may not be removed: its manifest is in state %d rather "+
			"than closed, so a delayed writer can still land material after this cleanup "+
			"believes it finished", m.Payload, m.State)
	}
	return nil
}

// mqttNextPurgeInterval is the next interval of chunks to announce and then remove.
//
// Returns the inclusive ordinal bounds and whether there is anything left. Ordinal order rather
// than any order, so PURGED is a single number that means "everything up to here": a set of
// removed ordinals would be a second structure to keep durable, and one this record has no room
// for.
func mqttNextPurgeInterval(m *mqttRetainedManifest) (uint64, uint64, bool) {
	if m == nil || m.Announced >= m.Chunks {
		return 0, 0, false
	}
	from := m.Announced + 1
	to := m.Chunks
	if to-from+1 > mqttPurgeBatchMax {
		to = from + mqttPurgeBatchMax - 1
	}
	return from, to, true
}

// mqttPendingPurgeInterval is the interval an interrupted cleanup still owes.
//
// Everything ANNOUNCED and not CONFIRMED, which after a crash is whatever the announcement
// covered — including a chunk that was never reached. A resume re-deletes the whole of it
// rather than guessing where it stopped, because the store cannot say which deletes landed and
// an absent chunk is indistinguishable from one that was never there.
func mqttPendingPurgeInterval(m *mqttRetainedManifest) (uint64, uint64, bool) {
	if m == nil || m.Announced <= m.Purged {
		return 0, 0, false
	}
	return m.Purged + 1, m.Announced, true
}

// mqttMaterialFullyPurged reports whether every chunk has been announced removed.
//
// The condition for removing the manifest record itself, and the reason it is the last thing to
// go: while it stands, an interrupted cleanup can be resumed from it; once it is gone, any chunk
// still standing is a record whose layout and owner nothing can state.
func mqttMaterialFullyPurged(m *mqttRetainedManifest) bool {
	// CONFIRMED, not announced. The whole point of the two counters is that an announcement is
	// what a cleanup INTENDS and a confirmation is what it has done.
	return m != nil && m.State == mqttManifestClosed && m.Purged == m.Chunks
}

// mqttCheckPurgeStep validates one announcement against the manifest as it stands.
//
// It is the write-side half of the rules above, and it is where they become enforcement rather
// than a caller contract — the distinction this lane's audit exists to draw.
func mqttCheckPurgeStep(a *mqttTransitionAttempt, cur, next *mqttRetainedManifest) error {
	if next.Announced == cur.Announced && next.Purged == cur.Purged {
		return nil
	}
	// NEITHER COUNT EVER FALLS. An announcement is what a resumed cleanup replays and a
	// confirmation is what it may stop replaying; material already taken does not come back.
	if next.Announced < cur.Announced {
		return fmt.Errorf("transition %q lowers the announced count of payload %q from %d to %d",
			a.Txn, cur.Payload, cur.Announced, next.Announced)
	}
	if next.Purged < cur.Purged {
		return fmt.Errorf("transition %q lowers the confirmed count of payload %q from %d to %d",
			a.Txn, cur.Payload, cur.Purged, next.Purged)
	}
	// ONE STEP DOES ONE THING. A step that announced AND confirmed in one act would confirm
	// deletes that cannot have happened: the deletes run BETWEEN the two, outside this
	// transaction, which is the whole reason there are two counts.
	if next.Announced != cur.Announced && next.Purged != cur.Purged {
		return fmt.Errorf("transition %q both announces and confirms a removal of payload %q: "+
			"the deletes run between the two, so an act that did both would confirm work that "+
			"cannot have been done", a.Txn, cur.Payload)
	}
	// A CONFIRMATION CONFIRMS THE WHOLE ANNOUNCEMENT. Confirming part of one would leave the
	// rest indistinguishable from material a later pass must re-delete — which it is — while
	// claiming progress that no bounded interval describes.
	if next.Purged != cur.Purged && next.Purged != cur.Announced {
		return fmt.Errorf("transition %q confirms %d chunk(s) of payload %q removed against an "+
			"announcement of %d: a confirmation answers the whole announcement",
			a.Txn, next.Purged, cur.Payload, cur.Announced)
	}
	// THAT THE STORED MANIFEST WAS ALREADY CLOSED is NOT restated here. It is one half of
	// mqttMaterialDeletionAllowed, which the authorization check applies to this same stored
	// record — so closing and announcing in one step is refused there, by the function that
	// states closure and authorization together because they are the pair the review required.
	// A second statement of it here would agree with that one by construction, and the two
	// would drift the moment either moved.
	if next.Announced-cur.Announced > mqttPurgeBatchMax {
		return fmt.Errorf("transition %q announces %d chunk(s) of payload %q at once, over the "+
			"%d one step may cover: a cleanup that cannot be interrupted cannot be resumed",
			a.Txn, next.Announced-cur.Announced, cur.Payload, mqttPurgeBatchMax)
	}
	return nil
}

// checkPurgeConfirmed establishes that the chunks a confirmation claims to have removed are
// actually gone.
//
// A review found this to be the deleted witness's defect wearing a different hat: the validator
// checked the counters' arithmetic and the cleanup's authorization, and never that anything had
// been deleted. A caller could announce an interval, confirm it without deleting a byte, and —
// once the confirmed count reached the chunk count — have the manifest removed, leaving orphan
// material that nothing describes.
//
// So the confirmation reads the interval. A chunk of THIS materialization still standing is a
// confirmation of work that did not happen; an unresolved read is not an absence and refuses
// too. A chunk of ANOTHER materialization at the same address is not this cleanup's business
// and does not block it: ours is gone, which is what the confirmation is about.
//
// The interval is bounded by mqttPurgeBatchMax, so this is a bounded read — the same
// proportionality the seal's verification has.
func (jsa *mqttJSA) checkPurgeConfirmed(a *mqttTransitionAttempt, cur, next *mqttRetainedManifest) error {
	if next.Purged == cur.Purged {
		return nil
	}
	idHash := getHash(a.ID)
	for ord := cur.Purged + 1; ord <= next.Purged; ord++ {
		subject := mqttChunkSubject(idHash, cur.Gen, cur.Payload, ord)
		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return fmt.Errorf("transition %q confirms chunk %d of payload %q removed, and "+
					"this broker could not read that address: %w", a.Txn, ord, cur.Payload, lerr)
			}
			continue
		}
		c, derr := mqttDiscoverRetainedChunk(smsg.Data, ord)
		if derr != nil {
			return fmt.Errorf("transition %q confirms chunk %d of payload %q removed, and a "+
				"record this broker cannot interpret stands at that address: %w",
				a.Txn, ord, cur.Payload, derr)
		}
		if c.Inc == cur.Inc {
			return fmt.Errorf("transition %q confirms chunk %d of payload %q removed while it "+
				"is still standing: a confirmation is evidence of a delete, not a claim about "+
				"one", a.Txn, ord, cur.Payload)
		}
	}
	return nil
}

// checkPurgeIsAuthorized re-reads the cleanup owner a purge claims.
//
// The authorization is NOT taken from the batch's own members. A resource member in the same
// batch is what this transition WANTS to be true; the question here is whether it already IS,
// because the deletes that follow this announcement are irreversible and run outside it.
func (jsa *mqttJSA) checkPurgeIsAuthorized(a *mqttTransitionAttempt, cur, next *mqttRetainedManifest) error {
	if next.Announced == cur.Announced && next.Purged == cur.Purged {
		return nil
	}
	r, _, rerr := jsa.loadOutboundResource(a.ID, a.Epoch, cur.Gen, cur.Inc)
	if rerr != nil || r == nil {
		return fmt.Errorf("transition %q announces the removal of material of payload %q whose "+
			"cleanup owner %q could not be read (%v): an unresolved authorization is not an "+
			"authorization", a.Txn, cur.Payload, cur.Inc, rerr)
	}
	return mqttMaterialDeletionAllowed(cur, r)
}

// mqttPurgeCountStep says which count one CAS moves.
type mqttPurgeCountStep uint8

const (
	// mqttPurgeAnnounce raises Announced: these chunks are about to be removed.
	mqttPurgeAnnounce mqttPurgeCountStep = iota + 1
	// mqttPurgeConfirm raises Purged to what was announced: those chunks are gone.
	mqttPurgeConfirm
)

// mqttPurgeCountTransition assembles ONE of the two CASes a removal is made of.
//
// One at a time, deliberately. The deletes run BETWEEN them, outside any transaction, so a CAS
// that announced and confirmed together would be recording work that cannot have happened yet
// — which is the interleaving a review found: a single count advanced before the deletes left a
// manifest saying a crashed cleanup was finished.
//
// It carries the manifest and nothing else. The authorization it rests on is re-read by the
// boundary from the store, not asserted here — a transition that carried its own permission
// would be the caller contract this whole file replaces.
func mqttPurgeCountTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	m *mqttRetainedManifest, mSeq uint64, step mqttPurgeCountStep, to uint64) (*mqttAuthorityTransition, error) {

	if prev == nil {
		return nil, fmt.Errorf("stepping a purge requires the authority it runs under")
	}
	if m == nil || mSeq == 0 {
		return nil, fmt.Errorf("stepping a purge requires the manifest and the sequence it " +
			"was read at")
	}
	next := *m
	switch step {
	case mqttPurgeAnnounce:
		next.Announced = to
	case mqttPurgeConfirm:
		next.Purged = to
	default:
		return nil, fmt.Errorf("stepping a purge requires which count it moves")
	}
	body, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			// A DISPOSAL, deliberately: cleanup must run under a CLOSED authority, which is
			// exactly when old material is reclaimed. The kinds that WRITE material are
			// refused here for the mirror-image reason.
			Kind: mqttTransitionDisposeResource, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttManifestSubject(getHash(prev.ID), m.Gen, m.Payload),
			expect: mSeq, body: body,
		}},
	}, nil
}

// purgeMaterialStep removes one bounded interval of a payload's material.
//
// THE ONLY ROUTE THAT REMOVES MATERIAL, and the reason it exists rather than a rule written in
// a comment: `deleteMsg` is a bare primitive, so "delete nothing before the announcement lands"
// is not something the validator can enforce — the validator never sees a delete. Putting the
// order in ONE function is what makes it a mechanism, exactly as the two write boundaries are
// for output.
//
// The order is: gate, announce, delete, and never any other. A delete that ran first would
// leave, on a crash, material nothing knows to finish removing while the manifest still claims
// it is whole.
//
// Returns whether anything remains, so a caller drives it to completion without deciding how
// much one step may take.
func (as *mqttAccountSessionManager) purgeMaterialStep(id, epoch, txn string,
	m *mqttRetainedManifest, mSeq uint64) (bool, error) {

	if m == nil || mSeq == 0 {
		return false, fmt.Errorf("purging material requires the manifest and the sequence it " +
			"was read at")
	}
	r, _, rerr := as.jsa.loadOutboundResource(id, epoch, m.Gen, m.Inc)
	if rerr != nil || r == nil {
		return false, fmt.Errorf("purging payload %q: its cleanup owner %q could not be read "+
			"(%v), and an unresolved authorization is not an authorization", m.Payload, m.Inc, rerr)
	}
	if err := mqttMaterialDeletionAllowed(m, r); err != nil {
		return false, err
	}

	// AN OUTSTANDING ANNOUNCEMENT IS FINISHED FIRST, and finishing it means DELETING AGAIN.
	// After an interruption this broker cannot tell which of the announced deletes landed —
	// an absent chunk is indistinguishable from one that was never written — so the whole
	// announced interval is re-deleted and only then confirmed.
	if from, to, pending := mqttPendingPurgeInterval(m); pending {
		if err := as.deleteChunkInterval(id, m, from, to); err != nil {
			return true, err
		}
		if err := as.stepPurgeCount(id, epoch, txn, m, mSeq, mqttPurgeConfirm, to); err != nil {
			return true, err
		}
		// STILL MORE, always. Even a confirmation of the LAST interval leaves the record that
		// describes the payload standing, and removing it is a step of its own — the one that
		// must not run until a confirmation, not an announcement, says the material is gone.
		return true, nil
	}

	_, to, more := mqttNextPurgeInterval(m)
	if !more {
		// Every chunk is CONFIRMED removed, so what is left is the record that describes them.
		return false, as.finishMaterialRemoval(id, m, mSeq)
	}
	// NOTHING IS DELETED BY THIS CALL. The announcement lands, and the deletes it licenses are
	// the NEXT call's pending interval — which is also what a resumed cleanup finds, so the
	// crash path and the ordinary path are one path.
	if err := as.stepPurgeCount(id, epoch, txn, m, mSeq, mqttPurgeAnnounce, to); err != nil {
		return true, err
	}
	return true, nil
}

// stepPurgeCount commits one of the two CASes.
func (as *mqttAccountSessionManager) stepPurgeCount(id, epoch, txn string,
	m *mqttRetainedManifest, mSeq uint64, step mqttPurgeCountStep, to uint64) error {

	al := as.jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid {
		return fmt.Errorf("purging payload %q: the outbound authority does not load back "+
			"(status %v)", m.Payload, al.status)
	}
	tr, terr := mqttPurgeCountTransition(al.rec, al.seq, txn, "op-"+txn, m, mSeq, step, to)
	if terr != nil {
		return terr
	}
	_, cerr := as.jsa.commitAuthorityTransition(tr)
	return cerr
}

// deleteChunkInterval removes the chunks of one announced interval.
//
// Absence is not an error here and is not evidence of anything either: it is what a re-run of
// an announced interval expects to find, and the only thing it licenses is continuing.
func (as *mqttAccountSessionManager) deleteChunkInterval(id string, m *mqttRetainedManifest,
	from, to uint64) error {

	idHash := getHash(id)
	for ord := from; ord <= to; ord++ {
		subject := mqttChunkSubject(idHash, m.Gen, m.Payload, ord)
		smsg, lerr := as.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return fmt.Errorf("purging chunk %d of payload %q: %w", ord, m.Payload, lerr)
			}
			continue
		}
		// WHOSE CHUNK IS THIS? The load already happened — it is how the sequence is learned —
		// so checking the record costs nothing and closes the interleaving a review found: a
		// paused cleanup resuming after a REPLACEMENT materialization reserved the same payload
		// token would otherwise delete the newcomer's material by subject.
		c, derr := mqttDiscoverRetainedChunk(smsg.Data, ord)
		if derr != nil {
			return fmt.Errorf("purging chunk %d of payload %q: a record this broker cannot "+
				"interpret stands at its address, and a cleanup does not remove what it cannot "+
				"identify: %w", ord, m.Payload, derr)
		}
		if c.Inc != m.Inc {
			// A LATER MATERIALIZATION'S CHUNK. Ours is already gone, which is exactly what this
			// interval was re-deleting to establish.
			continue
		}
		if derr := as.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); derr != nil {
			return fmt.Errorf("purging chunk %d of payload %q: %w", ord, m.Payload, derr)
		}
	}
	return nil
}

// finishMaterialRemoval removes the record that DESCRIBES a payload, once none of it is left.
//
// The manifest goes last and goes by its EXACT SEQUENCE. A review found why that matters: a
// cleanup that reloads the subject and deletes whatever stands there can delete a DIFFERENT
// materialization — one that reserved the same payload token after the first was removed, with
// a live cleanup owner of its own and obligations already admitted against it. Deleting the
// sequence this cleanup was authorized for cannot do that: if the record moved, the delete
// finds nothing of this incarnation and the pass simply ends.
func (as *mqttAccountSessionManager) finishMaterialRemoval(id string, m *mqttRetainedManifest,
	mSeq uint64) error {

	if !mqttMaterialFullyPurged(m) {
		return fmt.Errorf("payload %q has %d of %d chunk(s) confirmed removed, so the record "+
			"that describes them is what a resumed cleanup needs", m.Payload, m.Purged, m.Chunks)
	}
	if mSeq == 0 {
		return fmt.Errorf("removing the manifest of payload %q requires the sequence it was "+
			"read at: a cleanup that deletes whatever occupies the address can destroy a "+
			"later materialization's record", m.Payload)
	}
	return as.jsa.deleteMsg(mqttInflightStreamName, mSeq, true)
}
