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
// Both writes are wired: the create (opAdmitPublish) and the same-delivery replacement
// (opReplacePublish, register-only or pair). Every supported QoS 1/2 delivery — ordinary and, since
// 7a, shared-group QoS 1 (admitSharedInflightPublish) — is admitted here. storeInflightRegister's
// sole remaining production caller is the experimental affine route, whose conversion is chapter P's.
func (sess *mqttSession) admitInflightPublish(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim, q2AffCopy string) bool {
	return sess.admitThroughAuthority(c, pi, qos, jsDur, sseq, nonce, q2AffClaim, q2AffCopy, nil)
}

// admitSharedInflightPublish is the SHARED-GROUP QoS 1 admission (F-105 cutover, unit 7a): the same
// ladder and the same authority-carrying write as an ordinary delivery, except that the obligation
// takes no RESOURCE edge — a group consumer is not a resource any generation owns — and depends on
// the exact acquisition the delivery callback selected (sharedAcqForDeliveryLocked), whose
// readiness opAdmitPublish checks after its authority read. Supported shared delivery is QoS 1;
// anything else is refused here, before a single write.
func (sess *mqttSession) admitSharedInflightPublish(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce string,
	shared *mqttAdmitShared) bool {

	if shared == nil || qos != 1 || !mqttIsSharedDurable(jsDur) {
		c.Warnf("MQTT: refusing to admit shared delivery %s/%d at QoS %d for client %q: supported shared "+
			"delivery is QoS 1 and depends on an acquisition", jsDur, sseq, qos, sess.id)
		sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
		return false
	}
	return sess.admitThroughAuthority(c, pi, qos, jsDur, sseq, nonce, _EMPTY_, _EMPTY_, shared)
}

// admitThroughAuthority is the one admission body both entrances share. shared is nil for an
// ordinary delivery.
func (sess *mqttSession) admitThroughAuthority(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim, q2AffCopy string,
	shared *mqttAdmitShared) bool {
	// THE CONNECTION'S OWN ENTITLEMENT AND THE §5.1 DEPENDENCIES, assembled ONCE before the
	// ladder: they do not change across its retry attempts. The entitlement is the one this
	// connection was ACCEPTED under (recorded by the CONNECT accept loop), never a read of whatever
	// authority serves now — a displaced connection whose delivery callback enters here after a
	// takeover would otherwise capture the SUCCESSOR's tenure and admit under it, and the
	// commit-time comparison with the standing authority would pass because it compares against
	// the value this site just read. Captured here and closed over by every attempt, so a retry
	// cannot recapture either. A delivery with no entitlement, or a dependency that cannot be
	// proven ready for use, is REFUSED here — never a fallback — and the ladder is not entered;
	// AckWait re-offers.
	// THE CUSTODY RESERVATION, for the whole ladder (F-105 row 5). In one critical section the
	// tracked allocation moves from the scheduling projection to an admission reservation in
	// the custody ledger, so the slot is never uncounted; and it stays reserved until this
	// function returns AND every attempt it submitted is positively settled. The ladder
	// retries without passing the delivery gate again, so a charge released between attempts
	// would let another delivery take the slot while a retry lands beside it. An allocation
	// already abandoned between tracking and here is not admitted at all.
	sess.mu.Lock()
	gen := sess.gen
	opened := sess.openAdmissionCustodyLocked(gen, pi, nonce)
	sess.mu.Unlock()
	if !opened {
		return false
	}
	defer sess.finishAdmissionCustody(gen, nonce)
	// A FOREGROUND AUTHORITY OPERATION for the whole ladder (outbound_foreground_priority.go): its
	// bounded attempts are what a burst of background authority writes — the retained workers' and
	// the F-112 floor unit's resolutions, advances and watermark updates — would otherwise exhaust,
	// abandoning a valid delivery for a re-offer.
	release := sess.announceForegroundAuthority()
	defer release()
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testAdmitReserved
		as.mu.RUnlock()
		if hook != nil {
			hook(sess, nonce)
		}
	}

	auth, eok := c.outboundEntitlement()
	if !eok {
		c.Warnf("MQTT: refusing to admit delivery %s/%d for client %q: the connection carries no accepted "+
			"outbound entitlement", jsDur, sseq, sess.id)
		sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
		return false
	}
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	// A shared obligation takes no resource edge; its dependency is the acquisition, checked by the
	// op inside each attempt.
	var deps []mqttAdmitDependency
	if shared == nil {
		var dok bool
		deps, dok = sess.assembleAdmitDeps(c, jsDur, qos, epoch, auth.Gen)
		if !dok {
			sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
			return false
		}
	}

	// THE FOREGROUND BUDGET, as for the send-section acquisition: announced for the whole ladder,
	// it loses at most one attempt to each admitted background writer, and one more than that can
	// land. Each attempt reports contended exactly when its operation reached commitOp (a frozen
	// handle beside an error) — the only failure a same-session commit can have caused.
	return sess.runInflightAdmission(c, pi, qos, jsDur, sseq, nonce, q2AffClaim, q2AffCopy, mqttForegroundAcquireAttempts,
		func(regExpect, markerExpect uint64, registerOnly bool, cur, rec *mqttInflightRecord, subject string) (uint64, bool, error) {
			if regExpect == 0 && !registerOnly {
				// CREATE: the authority-carrying admission. opAdmitPublish compares the captured
				// entitlement with the standing authority and refuses a delivery whose
				// connection was displaced or whose generation has moved on.
				lin := rec.Lin
				out, err := sess.admitWrite(false, func() (mqttOpOutcome, error) {
					return sess.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
						ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publish",
						Gen: auth.Gen, Tenure: auth.Tenure, Register: rec, Deps: deps, Shared: shared,
					})
				})
				// Recorded and settled BEFORE the ladder sees the outcome: a landed attempt's
				// exchange is charged, a never-landed one is dropped, an unresolved one stays
				// on the reservation for the reconciler.
				sess.recordAdmissionAttempt(gen, nonce, lin, out, err)
				if err != nil {
					return 0, out.Handle != nil, err
				}
				seq, rerr := sess.landedRegisterSeq(subject, rec.Nonce)
				return seq, false, rerr
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
			var repShared *mqttAdmitShared
			if !registerOnly && shared != nil {
				// A shared pair replacement admits a fresh QoS 1 obligation over a proven-unsent shared
				// occupant: the same dependency a create states, re-validated by the op.
				if cur.QoS != 1 {
					return 0, false, errMQTTReplacementDepsNotReady
				}
				repShared = shared
			} else if !registerOnly {
				var dok bool
				repDeps, dok = sess.assembleAdmitDeps(c, jsDur, cur.QoS, epoch, auth.Gen)
				if !dok {
					// A dependency that cannot be shown ready refuses the replacement; the ladder
					// re-classifies and, after the bounded retries, AckWait re-offers.
					return 0, false, errMQTTReplacementDepsNotReady
				}
			}
			// The lineage THIS attempt writes: the occupant's for a register-only replacement
			// (it continues that exchange), a fresh one for a pair replacement, as the ladder
			// froze it from mqttLineageForWrite. Custody charges exactly this value, and the
			// composer refuses a batch whose register would carry another. Copied now — the
			// ladder rewrites rec.Lin between attempts.
			lin := rec.Lin
			out, rerr := sess.admitWrite(registerOnly, func() (mqttOpOutcome, error) {
				return sess.jsa.opReplacePublish(&mqttReplacePublishRequest{
					ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-replace-publish",
					Gen: auth.Gen, Tenure: auth.Tenure, RegisterOnly: registerOnly,
					Occupant: cur, OccupantSeq: regExpect, MarkerSeq: markerExpect, Register: rec, Deps: repDeps,
					Shared: repShared,
				})
			})
			if landed := sess.recordAdmissionAttempt(gen, nonce, lin, out, rerr); landed && !registerOnly && cur != nil {
				// The pair replacement wrote SupersededUnsent for the occupant's exchange in the
				// same batch; its charge goes when that phase is observed.
				sess.hintCustody(gen, cur.Lin)
			}
			if rerr != nil {
				return 0, out.Handle != nil, rerr
			}
			seq, lerr := sess.landedRegisterSeq(subject, rec.Nonce)
			return seq, false, lerr
		})
}

// errMQTTAdmittedRegisterUnread reports that an admission batch LANDED but its register could not
// be read back as this allocation's LIVE register (unreadable, another occupant, or already ended). It is returned as a write failure on purpose: the ladder then
// re-reads the subject and classifies it, and finding its own nonce there is exactly the
// adopt-the-landed-write arm, which settles against the sequence that read returns.
var errMQTTAdmittedRegisterUnread = errors.New("the admission landed but its register could not be read back as this allocation's")

// landedRegisterSeq is the sequence the admitted REGISTER stands at, which is what the ladder's
// settlement records on the entry.
//
// It is NOT the operation's returned sequence. An authority batch acknowledges on its commit
// member — the authority — so that sequence names the authority record, not the register beside
// it; recorded as the register's, every later exact-sequence reference to this slot reads as a
// MOVED record, and the gated door's freshness fence then preserves a register it was asked to
// destroy while reporting the continuation unblocked. The register is read back by its nonce, the
// only fact that identifies this allocation on a reused subject.
//
// Session lock NOT held on entry.
func (sess *mqttSession) landedRegisterSeq(subject, nonce string) (uint64, error) {
	if sess.asm == nil {
		return 0, errMQTTAdmittedRegisterUnread
	}
	cur, smsg, err := sess.asm.loadInflightRecord(subject)
	if err != nil || smsg == nil || cur == nil || cur.Nonce != nonce {
		return 0, errMQTTAdmittedRegisterUnread
	}
	if cur.Cancelled {
		// OURS, BUT ALREADY ENDED: a withdrawal landed between the batch and this read-back. It
		// is not a settlement to confirm — confirming would install a live entry for an exchange
		// no acquisition may serve — so it is reported as a write the ladder must re-read, whose
		// adopt arm finds our own tombstone and abandons the allocation.
		return 0, errMQTTAdmittedRegisterUnread
	}
	return smsg.Sequence, nil
}

// admitWrite runs one admission attempt's authority write, honoring an instance-scoped test
// seam (testAdmitWrite) exactly as advanceExchangeAck honors testAckAdvance.
//
// Session lock NOT held on entry.
func (sess *mqttSession) admitWrite(registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testAdmitWrite
		as.mu.RUnlock()
		if hook != nil {
			return hook(sess, registerOnly, write)
		}
	}
	return write()
}
