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

// The ordinary QoS 1/2 admission orchestrator (F-105 cutover, step 5).
//
// admitInflightPublish makes a freshly allocated identifier durable through the outbound
// obligation AUTHORITY — the register and its unsent marker, the client-facing exchange, an edge
// on every backing resource the obligation will need, and the authority's own revision, in ONE
// atomic batch (opAdmitPublish). It replaces the standalone register-and-marker write
// storeInflightRegister performs, whose failure F-105 exists to close: today the register can
// land on its own, so a source or PUBREL consumer can be deleted by a teardown that never knew an
// obligation depended on it, and the register survives to prove nothing.
//
// # This is an INTERMEDIATE that carries only the CREATE arm
//
// storeInflightRegister runs an ~11-arm classifier ladder (mqttClassifyInflightSlot) on a CAS
// contention: replacement of a proven-unsent occupant, adoption of its own lost write, reaping a
// cancellation or a completed slot, withdrawing a terminal slot, adopting a source retirement,
// quarantining a foreign claim. That ladder is ported onto this route before it replaces
// storeInflightRegister at the delivery callback. Until then this handles the common first-offer
// CREATE fully and DELIBERATELY REFUSES anything that does not land cleanly — an occupied subject
// or a lost-acknowledgement ambiguity is re-offered by AckWait, exactly the cost a regPending
// decline already accepts, and NEVER falls back to the old writer (Codex consult 11). It is not
// yet wired; storeInflightRegister remains the live route (and stays for the affine caller until
// 7a).
func (sess *mqttSession) admitInflightPublish(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim, q2AffCopy string) bool {
	// Preamble, identical to storeInflightRegister: an opportunistic void flush, then the entry
	// identity captured ONCE under the lock. The caller's nonce — read under the lock that
	// allocated the entry — is the identity every check keys on; the identifier alone cannot be,
	// because this goroutine can stall across a cancellation, a settle and a reuse.
	sess.persistVoidedRegs(c)
	sess.mu.Lock()
	idHash, gen, regClaim, regClaimSeq := sess.idHash, sess.gen, sess.regClaim, sess.seq
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	ack := sess.pendingPublish[pi]
	stillOurs := ack != nil && ack.regPending && !ack.regCancelled &&
		ack.jsDur == jsDur && ack.sseq == sseq && ack.regNonce == nonce
	sess.mu.Unlock()

	if sess.asm == nil || idHash == _EMPTY_ || gen == _EMPTY_ {
		// Unreachable for a real session; fail closed rather than admit under an identity
		// nothing records.
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}
	if !stillOurs {
		// The entry went away under us, or is no longer the allocation we were asked to admit.
		// Our cancelled reservation still has to be released, keyed by the nonce so a newer
		// allocation's identifier is not freed with its own write still out.
		sess.releaseCancelledReservation(pi, nonce)
		return false
	}
	if nonce == _EMPTY_ {
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}

	subject := mqttInflightSubject(idHash, gen, pi)
	rec := &mqttInflightRecord{
		PI: pi, Dur: jsDur, Seq: sseq, QoS: qos,
		State:      mqttInflightPublish,
		Nonce:      nonce,
		Lin:        nonce,
		Claim:      regClaim,
		ClaimSeq:   regClaimSeq,
		Q2AffClaim: q2AffClaim,
		Q2AffCopy:  q2AffCopy,
	}
	if verr := mqttValidateInflightShape(rec); verr != nil {
		c.Errorf("MQTT: refusing to admit an unsupported in-flight shape for client %q: %v", sess.id, verr)
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}

	// THE ENTITLEMENT THIS DELIVERY IS ADMITTED UNDER, captured now. opAdmitPublish re-compares
	// it with the standing authority: a delivery decided under one generation and delayed until
	// another serves is refused rather than admitted as the successor's work under a tenure that
	// never authorized it. gen == auth.Gen holds because the authority generation IS sess.gen
	// (the unified generation, driveAcceptToServingAuthority).
	auth, _, aerr := sess.servingAuthority()
	if aerr != nil {
		c.Warnf("MQTT: refusing to admit delivery %s/%d for client %q: no serving outbound authority: %v",
			jsDur, sseq, sess.id, aerr)
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}

	// THE §5.1 DEPENDENCIES the obligation needs from the moment it exists, each proven READY
	// for use before admission (no fallback): the source consumer, and for QoS 2 the canonical
	// PUBREL consumer. The admission increments each resource's edge in the SAME batch, so no
	// teardown can delete a consumer this obligation depends on without contending on the record
	// it shares.
	deps, dok := sess.assembleAdmitDeps(c, jsDur, rec.QoS, epoch, auth.Gen)
	if !dok {
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}

	outcome, err := sess.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publish",
		Gen: auth.Gen, Tenure: auth.Tenure, Register: rec, Deps: deps,
	})
	if err != nil {
		// DELIBERATELY-REFUSING INTERMEDIATE (consult 11): the contention/replacement/adopt/reap/
		// retire ladder is not yet ported here, so a subject that is occupied — or a lost-ack
		// ambiguity whose resolution the ported ladder will read from outcome.Handle — is refused
		// and re-offered by AckWait rather than resolved through a fallback writer.
		c.Warnf("MQTT: admission of delivery %s/%d for client %q did not land cleanly and is re-offered: %v",
			jsDur, sseq, sess.id, err)
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}

	// The register and marker landed in the admission batch; settle the local allocation state
	// against the landed sequence exactly as storeInflightRegister's create arm does — this
	// threads the landed QoS onto the entry and clears regPending, which the send section reads.
	return sess.settleLandedRegister(c, pi, jsDur, sseq, outcome.Seq, rec.Nonce, rec.Lin, rec.QoS, subject, false)
}

// assembleAdmitDeps resolves and READINESS-gates the resource dependencies an ordinary QoS 1/2
// PUBLISH admission takes: the source consumer in the source-replay role, and for QoS 2 the
// canonical PUBREL consumer in the canonical-pubrel role. Both are admitted in the same
// admission for a QoS 2 PUBLISH; the roles it acquires later (an output action, an ack handler,
// the PUBREC edge) are taken by those acts, not predicted here.
func (sess *mqttSession) assembleAdmitDeps(c *client, jsDur string, qos byte, epoch, gen string) ([]mqttAdmitDependency, bool) {
	srcInc, ok := sess.readyDependencyInc(c, mqttOwnedStreamMsgs, mqttStreamName, jsDur, epoch, gen)
	if !ok {
		return nil, false
	}
	deps := []mqttAdmitDependency{{Inc: srcInc, Role: mqttRoleSourceReplay}}
	if qos >= 2 {
		_, _, pubRelDur := mqttPubRelNames(sess.idHash, gen)
		relInc, rok := sess.readyDependencyInc(c, mqttOwnedStreamOut, mqttOutStreamName, pubRelDur, epoch, gen)
		if !rok {
			return nil, false
		}
		deps = append(deps, mqttAdmitDependency{Inc: relInc, Role: mqttRoleCanonicalPubRel})
	}
	return deps, true
}

// readyDependencyInc resolves a consumer name's owning resource incarnation from its
// name-ownership record — the delivery callback holds the durable NAME, not the incarnation — and
// proves the resource READY for use before it is admitted as a dependency. Readiness is the
// durable half (mqttSourcePubRelReadyForUse: the resource stands LIVE for a logical-use kind, its
// name-ownership stands LIVE and names this exact incarnation, and the stream carries no
// inactivity timer that could reap a depended-on consumer) PLUS a live check that the physical
// consumer actually exists. Any gap REFUSES the admission — never a fallback (Codex consult 11);
// AckWait re-offers, and once the consumer becomes usable the admission lands.
func (sess *mqttSession) readyDependencyInc(c *client, owned mqttOwnedStream, streamName, durName, epoch, gen string) (string, bool) {
	own, _, oerr := sess.jsa.loadNameOwnershipPhysical(epoch, owned, durName)
	if oerr != nil {
		c.Warnf("MQTT: refusing to admit against %q on %q for client %q: its name ownership could not be read: %v",
			durName, streamName, sess.id, oerr)
		return _EMPTY_, false
	}
	if own == nil {
		c.Warnf("MQTT: refusing to admit against %q on %q for client %q: no name-ownership record stands",
			durName, streamName, sess.id)
		return _EMPTY_, false
	}
	inc := own.OwnerInc
	if rerr := sess.jsa.mqttSourcePubRelReadyForUse(sess.id, epoch, gen, inc); rerr != nil {
		c.Warnf("MQTT: refusing to admit against %q/%q on %q for client %q: %v", gen, inc, streamName, sess.id, rerr)
		return _EMPTY_, false
	}
	ci, lerr := sess.jsa.lookupConsumer(streamName, durName)
	if lerr != nil || ci == nil {
		c.Warnf("MQTT: refusing to admit against %q on %q for client %q: its physical consumer is not usable: %v",
			durName, streamName, sess.id, lerr)
		return _EMPTY_, false
	}
	return inc, true
}
