// Copyright 2026 The mqtt-nats-bridge Authors
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
	"errors"
	"fmt"
	"sort"
	"time"
)

// Restoring a retained exchange on resume (F-105 7c).
//
// A retained register is dispatched by the resume's walk BEFORE every ordinary-source route: the
// source cache, the source grouping, the drain grouping and the source mappings never see it. The
// inventory gate still applies (the exchange must be one the generation's inventory reached), and
// the exchange is then read FRESH, with its sequence — the census is built once outside the
// reclassify loop and keeps no sequence, so it decides membership only.
//
//   - publish: the copy must be READY, admitted to this exchange, and agree on payload and digest;
//     its chunks are read and VERIFIED NOW, before any acquisition, and the bytes are held on the
//     restored entry, so the post-CONNACK replay never reads material. The marker decides first
//     send (present) versus possibly sent (absent: DUP 1).
//   - pubrel_ready: the canonical PUBREL must stand at the register's subject with its identity.
//   - delivery_abandoned: the exchange must read DeliveryAbandoned with the RETAINED arm of its
//     fact, bound to this register's allocation, acquisition and copy. The ACK-only identity is
//     installed and nothing is replayed; no material is read, so none needs to survive.
//   - publish_terminal / pubrel_terminal: the exchange ended; its cleanup is continued.
//   - a cancellation tombstone over an ended exchange (a withdrawal whose delete did not land):
//     the register is consumed, as a terminal one is.
//
// A material failure is RECLASSIFIED before it withholds: missing or corrupt material is also what a
// legitimate reclamation looks like when the exchange ended concurrently on another broker. So the
// register and the exchange are re-read; if either MOVED the pass reclassifies and the next pass
// dispatches on the fresh phase; if both are unchanged the exchange still needs its PUBLISH and its
// material is genuinely unusable: withheld, deterministically.

// mqttRecoveredRetainedKind is what the walk decided for one retained register.
type mqttRecoveredRetainedKind uint8

const (
	mqttRecoveredRetainedPublish mqttRecoveredRetainedKind = iota + 1
	mqttRecoveredRetainedPubRel
	mqttRecoveredRetainedTerminal
	mqttRecoveredRetainedAbandoned
	// mqttRecoveredRetainedWithdrawn: OUR cancellation tombstone over an exchange that ended — a
	// withdrawal landed and its register's delete did not — consumed like a terminal register.
	mqttRecoveredRetainedWithdrawn
)

// mqttRecoveredRetained is one retained register the walk restores or continues.
type mqttRecoveredRetained struct {
	kind        mqttRecoveredRetainedKind
	subject     string
	seq         uint64
	rec         *mqttInflightRecord
	m           *mqttRetainedManifest
	data        []byte
	markerSeq   uint64
	transmitted bool
}

// mqttRetainedReplayMaterial is the verified material a restored retained PUBLISH replays from.
type mqttRetainedReplayMaterial struct {
	m    *mqttRetainedManifest
	data []byte
}

// classifyRecoveredRetained decides one retained register. A *mqttRetainedQuarantineError
// QUARANTINES it (identifier reserved, nothing acts) — only on a POSITIVE store answer, named by its
// reason, that the exchange cannot be restored from (an absent or contradicting record, material
// that does not verify): the one exception to the ownership property (consult 107). errInflightReclassify asks the
// caller to re-run the pass. A read the store did not ANSWER refuses the resume instead: a
// quarantine owns nothing, so a failed read withheld here would leave the connected client its
// PUBREL or PUBLISH owed with no owner; refused before CONNACK, the reconnect classifies again
// with the records intact (consult 106).
//
// Session lock NOT held on entry.
func (sess *mqttSession) classifyRecoveredRetained(as *mqttAccountSessionManager, gen, subject string, seq uint64,
	rec *mqttInflightRecord) (*mqttRecoveredRetained, error) {

	jsa := sess.jsa
	if jsa == nil || as == nil {
		return nil, fmt.Errorf("the retained in-flight register on %q has no store to be classified against", subject)
	}
	epoch := as.sessStreamEpoch
	x, xSeq, xerr := jsa.loadOutboundExchange(sess.id, epoch, gen, rec.Lin)
	if xerr != nil && !mqttExchangeReadAbsent(xerr) {
		return nil, mqttRetainedRecoveryUnreadable(subject, "its exchange", xerr)
	}
	if xerr != nil || x == nil || x.PI != rec.PI {
		// The register is re-read first: a register that moved is a reclassification, not evidence.
		cur, smsg, lerr := as.loadInflightRecord(subject)
		switch {
		case lerr != nil:
			return nil, mqttRetainedRecoveryUnreadable(subject, "its register, re-read", lerr)
		case smsg == nil || cur == nil || smsg.Sequence != seq:
			return nil, errMQTTRetainedReclassify
		}
		return nil, mqttQuarantine("its exchange is absent or names another identifier")
	}
	// moved re-reads the pair: nil when both stand unchanged, a RETAINED reclassification when
	// either moved, and the resume's refusal when a re-read went unanswered — a failed read is
	// not a move, and a pass that kept "re-running" on it would end in the walk's conservative
	// success with this exchange owned by nothing.
	moved := func() error {
		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil {
			return mqttRetainedRecoveryUnreadable(subject, "its register, re-read", lerr)
		}
		if smsg == nil || cur == nil || smsg.Sequence != seq {
			return errMQTTRetainedReclassify
		}
		_, again, aerr := jsa.loadOutboundExchange(sess.id, epoch, gen, rec.Lin)
		if aerr != nil && !mqttExchangeReadAbsent(aerr) {
			return mqttRetainedRecoveryUnreadable(subject, "its exchange, re-read", aerr)
		}
		if aerr != nil || again != xSeq {
			return errMQTTRetainedReclassify
		}
		return nil
	}
	// quarantine is the ONLY way this classification reaches the exception: the pair is re-read
	// first, so an answer a concurrent advance produced (a PUBCOMP on another broker removing the
	// canonical, an ending) is a reclassification, never evidence (consult 107).
	quarantine := func(reason string) error {
		if err := moved(); err != nil {
			return err
		}
		return mqttQuarantine(reason)
	}
	if h := sess.testRetainedClassify.Load(); h != nil {
		(*h)(subject)
	}
	// THE ONE CONSISTENCY DEFINITION decides the pair (retainedPairVerdict); only the material and
	// the marker of a PUBLISH it would replay are checked here beyond it.
	switch agrees, reason, verr := jsa.retainedPairVerdict(sess.id, epoch, gen, rec, x, mqttCanonicalRequired); {
	case verr != nil:
		return nil, mqttRetainedRecoveryUnreadable(subject, "its canonical PUBREL", verr)
	case !agrees:
		return nil, quarantine(reason)
	}
	rr := &mqttRecoveredRetained{subject: subject, seq: seq, rec: rec}
	switch rec.State {
	case mqttInflightPublishTerminal, mqttInflightPubRelTerminal:
		rr.kind = mqttRecoveredRetainedTerminal
		return rr, nil
	case mqttInflightPubRelReady:
		rr.kind = mqttRecoveredRetainedPubRel
		return rr, nil
	case mqttInflightDeliveryAbandoned:
		rr.kind = mqttRecoveredRetainedAbandoned
		return rr, nil
	}
	// PUBLISH.
	if mqttExchangePhaseEnded(x.Phase) {
		rr.kind = mqttRecoveredRetainedWithdrawn // our withdrawal's tombstone over the ended exchange
		return rr, nil
	}
	m, _, merr := jsa.loadRetainedManifest(sess.id, epoch, gen, rec.Payload)
	if errors.Is(merr, errMQTTStoreUnreadable) {
		return nil, mqttRetainedRecoveryUnreadable(subject, "its copy's manifest", merr)
	}
	var data []byte
	var verr error
	if merr == nil {
		if m.State != mqttManifestReady || m.Admitted != rec.Lin || m.AdmittedPI != rec.PI || m.Digest != rec.Digest ||
			m.QoS != rec.QoS {
			verr = errInflightReclassify
		} else {
			data, verr = jsa.verifyMaterial(m)
			if errors.Is(verr, errMQTTStoreUnreadable) {
				return nil, mqttRetainedRecoveryUnreadable(subject, "its copy's material", verr)
			}
		}
	}
	if merr != nil || verr != nil {
		return nil, quarantine("its copy is absent, contradicts the register, or does not verify")
	}
	// GOOD MATERIAL IS NOT A FRESH PHASE. An answer landing on another broker after this pass's
	// reads — a PUBREC that moved the exchange past PUBLISH, or an ending — leaves the copy intact
	// (nothing reclaims a copy its exchange still needs), so verification alone would restore a
	// PUBLISH the client already answered. The pair is re-read once more: moved, the pass re-runs
	// and dispatches on the fresh phase.
	if err := moved(); err != nil {
		return nil, err
	}
	load := as.loadInflightMarker(mqttMarkerCallerStore, subject)
	switch {
	case load.kind == mqttInflightResponsePresent && load.marker != nil && load.marker.Nonce == rec.Nonce:
		rr.markerSeq = load.seq
	case load.kind == mqttInflightResponseAbsent:
		rr.transmitted = true
	case load.kind == mqttInflightResponseIndeterminate:
		return nil, mqttRetainedRecoveryUnreadable(subject, "its marker", errMQTTStoreUnreadable)
	default:
		return nil, quarantine("its marker belongs to another allocation")
	}
	rr.kind, rr.m, rr.data = mqttRecoveredRetainedPublish, m, data
	return rr, nil
}

// installRecoveredRetainedLocked installs the restored retained entries, custody-gated exactly as
// an ordinary restoration (recoveryRestoreRefusedLocked), each adopting its own exclusion. It
// returns the subjects it could NOT give an owner: an exchange that reached installation is this
// generation's, live and classified restorable, so a refusal here — a conflicting resident or
// binding, a custody refusal, an unaddressable record — leaves it owned by nothing, and the resume
// refuses before CONNACK rather than accept that (recoverInflightRegisters, consult 106).
//
// Lock held on entry.
func (sess *mqttSession) installRecoveredRetainedLocked(census *mqttCustodyCensus, rrs []mqttRecoveredRetained) (unowned []string) {
	for i := range rrs {
		rr := &rrs[i]
		if rr.kind == mqttRecoveredRetainedTerminal || rr.kind == mqttRecoveredRetainedWithdrawn {
			continue // cleanup only: continueRecoveredRetainedTerminals owns it
		}
		if rr.kind == mqttRecoveredRetainedAbandoned {
			// CUSTODY BEFORE THE BINDING, on the same terms as every restored entry: a late PUBREC
			// installs this exchange's PUBREL entry, which custody must hold the lineage of, so an
			// inventory this resume could not decide seeds it provisionally. A complete census
			// that does not reach the exchange seeds nothing.
			sess.seedUndecidedRecoveredCustodyLocked(census, rr.rec)
			// The ACK-only identity, and a resident PUBLISH entry of this allocation removed: the
			// charge stays released, the identifier held by the ledger's abandoned exchange.
			if !sess.settleAbandonedDeliveryLocked(mqttDenyEvidence{Gen: sess.gen, Tenure: rr.rec.AbandonTenure,
				Subject: rr.subject, Nonce: rr.rec.Nonce, Lin: rr.rec.Lin, PI: rr.rec.PI,
				Reason: rr.rec.AbandonReason, Payload: rr.rec.Payload}, rr.rec) {
				unowned = append(unowned, rr.subject)
			}
			continue
		}
		slot := mqttSlotRefFromRecord(rr.subject, rr.seq, rr.rec)
		if slot == nil {
			unowned = append(unowned, rr.subject)
			continue
		}
		which := mqttPendingPublishMap
		if rr.kind == mqttRecoveredRetainedPubRel {
			which = mqttPendingPubRelMap
		}
		resident := sess.pendingMapLocked(which)[rr.rec.PI]
		if resident != nil && resident.acts() {
			if resident.regNonce != rr.rec.Nonce {
				unowned = append(unowned, rr.subject) // another exchange's entry holds the identifier
				continue
			}
			// A resident entry of this same exchange (a same-object resume) keeps its identity and
			// custody; a retained PUBLISH one gains the verified material and the marker's send
			// state, which is what its post-CONNACK replay emits.
			if rr.kind == mqttRecoveredRetainedPublish && resident.kind == mqttPendingRetained &&
				!sess.setPendingLocked(which, rr.rec.PI, mqttExpectExactly(resident), mqttWithRetainedReplay(resident, rr)) {
				unowned = append(unowned, rr.subject)
			}
			continue
		}
		restored := mqttRecoveredRetainedPending(sess.gen, rr, *slot)
		h := sess.testRecoveredRetainedRefuse.Load()
		if (h != nil && (*h)(rr.subject)) || sess.recoveryRestoreRefusedLocked(census, rr.rec, restored) ||
			!sess.setPendingLocked(which, rr.rec.PI, mqttExpectCaptured(resident), restored) {
			unowned = append(unowned, rr.subject)
			continue
		}
		sess.adoptIdentifierExclusionLocked(rr.rec.PI, rr.subject, rr.rec.Nonce)
	}
	return unowned
}

// continueRecoveredRetainedTerminals finishes the cleanup of retained exchanges that ended before
// the resume, and wakes the reclamation of an abandoned one's copy: a publish_terminal register is
// deleted at its sequence, a pubrel_terminal one through the terminal finalization (which removes
// its canonical PUBREL), and either wakes its copy's reclamation. Asynchronous: the resume does
// not wait for cleanup. A cleanup that could not complete is RETRIED, capped, while this session
// object serves the generation — never dropped on its first refusal.
func (sess *mqttSession) continueRecoveredRetainedTerminals(c *client, rrs []mqttRecoveredRetained) {
	var terms []mqttRecoveredRetained
	for _, rr := range rrs {
		if rr.kind == mqttRecoveredRetainedTerminal || rr.kind == mqttRecoveredRetainedAbandoned ||
			rr.kind == mqttRecoveredRetainedWithdrawn {
			terms = append(terms, rr)
		}
	}
	if len(terms) == 0 {
		return
	}
	go func() {
		for _, rr := range terms {
			ref := mqttSlotRefFromRecord(rr.subject, rr.seq, rr.rec)
			if ref == nil {
				continue
			}
			if rr.kind == mqttRecoveredRetainedAbandoned {
				// The abandonment fact releases the copy's material edge: reclaim it.
				sess.wakeRetainedReclaimForPayload(ref.name.gen, rr.rec.Payload)
				continue
			}
			for attempt := 0; ; attempt++ {
				done := false
				switch rr.rec.State {
				case mqttInflightPubRelTerminal:
					out, _ := sess.finalizeTerminalSlotObserved(c, *ref)
					done = out != mqttTerminalIndeterminate
				default:
					d := mqttSlotDeletePublishTerminal
					if rr.kind == mqttRecoveredRetainedWithdrawn {
						d = mqttSlotDeleteComplete
					}
					done = sess.reapRegisterWithVerdictGated(rr.seq, rr.rec.Nonce, true, ref, d, mqttDestroyLicence{})
					if done {
						sess.wakeRetainedReclaimForPayload(ref.name.gen, rr.rec.Payload)
					}
				}
				if done || !sess.servesGeneration(ref.name.gen) {
					break
				}
				time.Sleep(mqttRetainedStepBackoff(attempt))
			}
		}
	}()
}

// replayRecoveredRetained re-sends, after CONNACK, every restored retained exchange this connection
// owns: a PUBLISH from its held, verified bytes (DUP as the marker said), a PUBREL from its
// canonical. In identifier order; entries the custody gate refused were never installed.
//
// No lock held on entry.
func (sess *mqttSession) replayRecoveredRetained(c *client) {
	sess.mu.Lock()
	disc := sess.retainedDiscovery
	sess.retainedDiscovery = mqttRetainedDiscovery{}
	sess.mu.Unlock()
	if len(disc.incs) > 0 {
		go sess.discoverRetainedPayloads(disc.gen, disc.incs)
	}
	sess.mu.Lock()
	var pubs, rels []uint16
	for pi, p := range sess.pendingPublish {
		if p.kind == mqttPendingRetained && p.retainedReplay != nil {
			pubs = append(pubs, pi)
		}
	}
	for pi, p := range sess.pendingPubRel {
		if p.kind == mqttPendingRetained {
			rels = append(rels, pi)
		}
	}
	sess.mu.Unlock()
	sort.Slice(pubs, func(i, j int) bool { return pubs[i] < pubs[j] })
	sort.Slice(rels, func(i, j int) bool { return rels[i] < rels[j] })
	for _, pi := range rels {
		sess.emitRetainedPubRel(c, pi)
	}
	for _, pi := range pubs {
		sess.replayRestoredRetainedPublish(c, pi)
	}
}

// replayRestoredRetainedPublish transmits one restored retained PUBLISH through the send section.
func (sess *mqttSession) replayRestoredRetainedPublish(c *client, pi uint16) {
	sess.mu.Lock()
	p := sess.pendingPublish[pi]
	if p == nil || p.kind != mqttPendingRetained || p.retainedReplay == nil {
		sess.mu.Unlock()
		return
	}
	mat := p.retainedReplay
	qos, nonce := p.qos, p.regNonce
	sess.mu.Unlock()
	f := mqttRetainedFrame{pi: pi, nonce: nonce, qos: qos,
		mat: &mqttRetainedMaterial{Topic: mat.m.Topic, QoS: qos, Props: mat.m.Props, ExpiresAt: mat.m.ExpiresAt,
			SessionHash: mat.m.SessionHash, SubID: mat.m.SubID, Data: mat.data}}
	if !sess.transmitRetained(c, nil, f, mat.m, mqttLandedRegister{}, false) {
		sess.ownRetainedOwed(c, pi, nonce, mqttRetainedOwedPublish)
	}
}

// replayRestoredRetainedPublishOnce is the driver's re-attempt of a restored retained PUBLISH that
// could not be sent: one background section. It reports whether the PUBLISH settled; an entry no
// longer this allocation's is its own path's.
func (sess *mqttSession) replayRestoredRetainedPublishOnce(c *client, pi uint16, nonce string) bool {
	sess.mu.Lock()
	p := sess.pendingPublish[pi]
	if p == nil || p.kind != mqttPendingRetained || p.retainedReplay == nil || p.regNonce != nonce {
		sess.mu.Unlock()
		return true
	}
	mat := p.retainedReplay
	qos, nonce := p.qos, p.regNonce
	sess.mu.Unlock()
	f := mqttRetainedFrame{pi: pi, nonce: nonce, qos: qos,
		mat: &mqttRetainedMaterial{Topic: mat.m.Topic, QoS: qos, Props: mat.m.Props, ExpiresAt: mat.m.ExpiresAt,
			SessionHash: mat.m.SessionHash, SubID: mat.m.SubID, Data: mat.data}}
	return sess.transmitRetained(c, nil, f, mat.m, mqttLandedRegister{}, true)
}

// mqttRetainedDiscovery is one resume's resource census, for retained discovery after CONNACK.
type mqttRetainedDiscovery struct {
	gen  string
	incs []string
}

// discoverRetainedPayloads enrols every non-complete retained copy among incs with the reclaimer.
// Discovery decides nothing: the reclaimer's boundary arms decide whether a copy may close.
//
// No lock held on entry.
func (sess *mqttSession) discoverRetainedPayloads(gen string, incs []string) {
	if sess.asm == nil || sess.jsa == nil {
		return
	}
	epoch := sess.asm.sessStreamEpoch
	for _, inc := range incs {
		// An unreadable owner is NOT skipped: the reclaimer reads it again and retries.
		r, _, err := sess.jsa.loadOutboundResource(sess.id, epoch, gen, inc)
		if err == nil && (r == nil || r.Kind != mqttResourceRetainedPayload || r.State == mqttResourceCleanupComplete) {
			continue
		}
		sess.wakeRetainedReclaim(gen, inc)
	}
}

// mqttRetainedRecoveryUnreadable is the resume's refusal over a retained register a read could not
// classify.
func mqttRetainedRecoveryUnreadable(subject, what string, err error) error {
	return fmt.Errorf("the retained in-flight register on %q cannot be classified: %s could not be read: %w",
		subject, what, err)
}

// errMQTTRetainedReclassify is a retained classification's request to re-run the pass: the pair
// MOVED. It is a reclassification (errors.Is errInflightReclassify), marked retained so a walk
// that exhausts its passes on it refuses the resume rather than taking its conservative success,
// which would leave the exchange owned by nothing (consult 106).
var errMQTTRetainedReclassify = fmt.Errorf("%w: a retained exchange moved under classification", errInflightReclassify)

// mqttRetainedQuarantineError is the POSITIVE-STORE-ANSWER RECOVERY QUARANTINE (consult 107): the
// one exception to the retained ownership property. It is returned only where ANSWERED reads, after
// the applicable consistency checks, establish a named unusable or contradictory persisted
// condition; the walk then accepts the resume with this exchange unowned, its identifier reserved,
// and nothing — replay, completion, abandonment, cleanup — authorized by the quarantine. An
// unanswered read, an exhausted reclassification or a refused installation is never this.
type mqttRetainedQuarantineError struct{ reason string }

func (e *mqttRetainedQuarantineError) Error() string { return "quarantined: " + e.reason }

func mqttQuarantine(reason string) error { return &mqttRetainedQuarantineError{reason: reason} }

// mqttRetainedQuarantined is one exchange the resume quarantined, with its evidence.
type mqttRetainedQuarantined struct {
	subject, nonce, reason string
}

// retainedQuarantinedLocked reports whether the last applied resume quarantined the allocation
// nonce: a quarantine authorizes no replay, completion, abandonment or cleanup, so every surviving
// continuation that would act for it stands down (consult 107).
//
// Lock held on entry.
func (sess *mqttSession) retainedQuarantinedLocked(nonce string) bool {
	if nonce == _EMPTY_ {
		return false
	}
	for _, q := range sess.retainedQuarantined {
		if q.nonce == nonce {
			return true
		}
	}
	return false
}

// retainedCanonicalAgrees is THE canonical-PUBREL check for a retained register at pubrel_ready,
// shared by recovery's classification and the manifest close's boundary so the two can never
// disagree about it (consult 108, row 9): the canonical must stand, decode, and name this
// register's exchange and identifier. err reports a read the store did not answer; otherwise
// agrees, or the quarantine reason that it does not.
func (jsa *mqttJSA) retainedCanonicalAgrees(id, epoch, gen string, rec *mqttInflightRecord) (agrees bool, reason string, err error) {
	smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, rec.RelSubj)
	if !mqttStoreAnswered(lerr) {
		return false, _EMPTY_, lerr
	}
	if lerr != nil || smsg == nil {
		return false, "its canonical PUBREL is absent", nil
	}
	rel, derr := mqttDecodeRetainedRel(smsg.Data, id, epoch, gen, rec.Lin)
	if derr != nil || rel.Identity != rec.RelID || rel.PI != rec.PI {
		return false, "its canonical PUBREL does not decode or names another exchange", nil
	}
	return true, _EMPTY_, nil
}

// mqttRetainedCanonicalCheck says whether the pair's canonical PUBREL must already stand.
type mqttRetainedCanonicalCheck uint8

const (
	// mqttCanonicalRequired: a pubrel_ready register's canonical must stand and agree.
	mqttCanonicalRequired mqttRetainedCanonicalCheck = iota + 1
	// mqttCanonicalCreatedHere: the batch being judged creates it (a PUBREC advance), and validates it
	// as its own member.
	mqttCanonicalCreatedHere
)

// retainedPairVerdict is THE consistency definition of a retained exchange's records (consult 109):
// recovery's classification, the manifest close's boundary and the acknowledgement advance's
// boundary consume this one verdict and recreate none of it. It decides identity and identifier
// agreement, the legal pairs of register state and exchange phase, the abandonment fact's
// agreement — historical too, after a late PUBREC — and, where the phase has one, the canonical
// PUBREL. err reports a read the store did not answer; otherwise the pair agrees, or reason names
// the contradiction (the quarantine's evidence). The copy's material and the unsent marker are NOT
// in it: recovery checks those itself, for the PUBLISH it would replay.
func (jsa *mqttJSA) retainedPairVerdict(id, epoch, gen string, reg *mqttInflightRecord, x *mqttOutboundExchange,
	canon mqttRetainedCanonicalCheck) (agrees bool, reason string, err error) {

	if x == nil || x.PI != reg.PI || x.Nonce != reg.Lin {
		return false, "its exchange is absent or names another identifier", nil
	}
	f := x.Abandoned
	factAgrees := func(full bool) bool {
		if !f.retained() || f.Payload != reg.Payload || f.Alloc != reg.Nonce {
			return false
		}
		if !full {
			// PAST THE ABANDONMENT (a late PUBREC): the register's later acquisitions move its counter,
			// so the abandonment's own acquisition is not compared; its reason and tenure, where the
			// register still carries them, are.
			return reg.AbandonReason == _EMPTY_ || (f.Reason == reg.AbandonReason && f.Tenure == reg.AbandonTenure)
		}
		return f.Acq == reg.Acq && f.Reason == reg.AbandonReason && f.Tenure == reg.AbandonTenure
	}
	switch reg.State {
	case mqttInflightPublishTerminal, mqttInflightPubRelTerminal:
		return true, _EMPTY_, nil // ended; the cleanup that follows legitimately removes the canonical
	case mqttInflightPublish:
		if reg.Cancelled && !reg.Retired && mqttExchangePhaseEnded(x.Phase) {
			return true, _EMPTY_, nil // our withdrawal's tombstone over the ended exchange
		}
		if x.Abandoned != nil {
			return false, "its abandonment fact disagrees with its register or phase", nil
		}
		if x.Phase != mqttExchangePublish {
			return false, "its register is at PUBLISH while its exchange is not", nil
		}
		return true, _EMPTY_, nil
	case mqttInflightPubRelReady:
		if x.Phase != mqttExchangePubRecObserved {
			return false, "its register is at pubrel_ready while its exchange is not past PUBREC", nil
		}
		if x.Abandoned != nil && !factAgrees(false) {
			return false, "its abandonment fact disagrees with its register or phase", nil
		}
		if canon == mqttCanonicalRequired {
			return jsa.retainedCanonicalAgrees(id, epoch, gen, reg)
		}
		return true, _EMPTY_, nil
	case mqttInflightDeliveryAbandoned:
		if x.Phase != mqttExchangeDeliveryAbandoned || !factAgrees(true) {
			return false, "its abandonment fact disagrees with its register or phase", nil
		}
		return true, _EMPTY_, nil
	}
	return false, "its register stands in a state this broker does not restore", nil
}
