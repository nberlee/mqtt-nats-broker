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

import "errors"

// errMQTTReplacementDepsNotReady refuses a pair replacement whose fresh obligation's dependencies
// cannot be shown ready for use. It is a non-nil write outcome, so the shared ladder re-classifies
// the occupied slot and, after the bounded retries, abandons and AckWait re-offers — never a
// fallback.
var errMQTTReplacementDepsNotReady = errors.New("the pair replacement's dependencies are not ready for use")

// The ordinary QoS 1/2 admission orchestrator (F-105 cutover, step 5).
//
// admitInflightPublish makes a freshly allocated identifier durable through the outbound
// obligation AUTHORITY — the register and its unsent marker, the client-facing exchange, an edge
// on every backing resource the obligation will need, and the authority's own revision, in ONE
// atomic batch (opAdmitPublish). It replaces the standalone register-and-marker write, whose
// failure F-105 exists to close: today the register can land on its own, so a source or PUBREL
// consumer can be deleted by a teardown that never knew an obligation depended on it, and the
// register survives to prove nothing.
//
// It runs the SAME slot ladder as storeInflightRegister (runInflightAdmission): the preamble,
// the classifier and its ~11 dispositions are shared and unchanged. The ONLY difference is the
// WRITE — here the authority-carrying opAdmitPublish (create) and the replacement composer, in
// place of the bare register/marker pair.
//
// # This is an INTERMEDIATE that carries only the CREATE write
//
// The replacement write (a same-delivery occupant this allocation may rewrite) is composed by
// outbound_replace_compose.go and is NOT yet wired here. Until it is, a create that does not land
// on an empty subject is DELIBERATELY REFUSED (Codex consult 11): the ladder re-classifies the
// occupied slot, this writer refuses the replacement, and after the bounded retries the
// allocation is abandoned and re-offered by AckWait — the same cost a regPending decline already
// accepts, and NEVER a fallback to the old writer. It is not yet wired at the delivery callback;
// storeInflightRegister remains the live route (and stays for the affine caller until 7a).
func (sess *mqttSession) admitInflightPublish(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim, q2AffCopy string) bool {
	// THE ENTITLEMENT AND THE §5.1 DEPENDENCIES, assembled ONCE before the ladder: they do not
	// change across its retry attempts, and readiness (gen = auth.Gen) needs the serving
	// authority. A delivery with no serving authority, or a dependency that cannot be proven
	// ready for use, is REFUSED here — never a fallback — and the ladder is not entered; AckWait
	// re-offers, and once the authority serves and the consumer is usable the admission lands.
	auth, _, aerr := sess.servingAuthority()
	if aerr != nil {
		c.Warnf("MQTT: refusing to admit delivery %s/%d for client %q: no serving outbound authority: %v",
			jsDur, sseq, sess.id, aerr)
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	deps, dok := sess.assembleAdmitDeps(c, jsDur, qos, epoch, auth.Gen)
	if !dok {
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}

	return sess.runInflightAdmission(c, pi, qos, jsDur, sseq, nonce, q2AffClaim, q2AffCopy,
		func(regExpect, markerExpect uint64, registerOnly bool, cur, rec *mqttInflightRecord, subject string) (uint64, error) {
			if regExpect == 0 && !registerOnly {
				// CREATE: the authority-carrying admission. gen == auth.Gen holds because the
				// authority generation IS sess.gen (the unified generation,
				// driveAcceptToServingAuthority); opAdmitPublish re-compares the captured
				// entitlement with the standing authority and refuses a delivery whose
				// generation has moved on.
				out, err := sess.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
					ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publish",
					Gen: auth.Gen, Tenure: auth.Tenure, Register: rec, Deps: deps,
				})
				return out.Seq, err
			}
			// REPLACEMENT of a same-delivery occupant this allocation may rewrite. The ladder has
			// already decided the mode (registerOnly) from the occupant's marker via
			// mqttDecideMarker and set the register's lineage to match; opReplacePublish composes
			// and commits it. Register-only CONTINUES a possibly-attempted occupant's obligation
			// (no fresh dependencies). A pair replacement admits a FRESH obligation over a
			// proven-unsent occupant, and its dependencies match the OCCUPANT's preserved QoS — an
			// incoming QoS 1 replacing a QoS 2 occupant still owes the release — so they are
			// assembled HERE against cur.QoS, not the incoming qos the create arm used.
			var repDeps []mqttAdmitDependency
			if !registerOnly {
				var dok bool
				repDeps, dok = sess.assembleAdmitDeps(c, jsDur, cur.QoS, epoch, auth.Gen)
				if !dok {
					// A dependency that cannot be shown ready refuses the replacement; the ladder
					// re-classifies and, after the bounded retries, AckWait re-offers.
					return 0, errMQTTReplacementDepsNotReady
				}
			}
			out, rerr := sess.jsa.opReplacePublish(&mqttReplacePublishRequest{
				ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-replace-publish",
				Gen: auth.Gen, Tenure: auth.Tenure, RegisterOnly: registerOnly,
				Occupant: cur, OccupantSeq: regExpect, MarkerSeq: markerExpect, Register: rec, Deps: repDeps,
			})
			return out.Seq, rerr
		})
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
