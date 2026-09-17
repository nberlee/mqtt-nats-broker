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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Verifying retained replay material (F-105, row 7; contract 4).
//
// # The claim that was not authorized, and the design that did not deliver it
//
// This lane's audit said `Written == Chunks` is what a boundary can check and that whether the
// bytes are the bytes is checked at REPLAY. A review refused that as a SEMANTIC WEAKENING:
// replay-time checking can stop corrupt output, but READY is what an ADMISSION relies on, and an
// obligation admitted against material that was never whole has already been promised to a
// client.
//
// My first answer was a durable WITNESS record: a verifier read the material, wrote down what it
// found, and the seal was refused unless a witness stood for that exact manifest incarnation. A
// second review took it apart in one sentence — **the witness was supplied by the caller, and
// the validator only compared it with the manifest's own claims.** Reserve a manifest whose
// digest describes bytes A, write bytes B of equal length through the chunk fence, and submit
// READY beside a witness copying the manifest's own numbers: every check passed and nothing ever
// read B. A record that says "I verified" is not a verification.
//
// # So the SEAL VERIFIES, inside the transaction
//
// The validator reads the exchange's chunks and checks them against the manifest being sealed.
// There is nothing left to forge, because there is no evidence record: the evidence is the
// material, read at the moment the CAS fences it.
//
// The witness class is DELETED rather than kept beside this. Two mechanisms stating one fact is
// how they drift, and the one that could be fabricated is not the one to keep.
//
// # The cost, and why it is the right place to pay it
//
// The manifest header argued that a boundary "is not going to read a whole payload inside the
// write path, because that would make every ADMISSION pay for it". That argument is about
// admissions, and it still holds: an admission reads the manifest and nothing else. A SEAL
// happens ONCE per materialization, immediately after the material was written chunk by chunk,
// so reading it once more is a constant factor over having written it.
//
// # And replay verifies AGAIN, not instead
//
// Material can rot after a seal. `verifyMaterial` is the same function on both sides: the seal
// runs it inside the transaction, and a replay runs it before emitting a byte.

// mqttMaterialDigest is the digest a manifest and a verification both mean.
//
// Stated in ONE place so the seal side and the replay side cannot drift into two spellings of
// "the same bytes" — two functions computing a digest is two definitions of what the payload is.
func mqttMaterialDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// verifyMaterial reads one materialization's chunks and reports what is actually there.
//
// Every failure is a REFUSAL rather than a partial answer. A missing chunk, a chunk that will
// not decode, a total that disagrees with the manifest, a digest that does not match: each one
// means the payload this broker would replay is not the payload the client was promised, and
// there is no arm in which emitting some of it is better than emitting none.
func (jsa *mqttJSA) verifyMaterial(m *mqttRetainedManifest) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("verifying material requires the manifest describing it")
	}
	idHash := getHash(m.ID)
	buf := make([]byte, 0, m.Bytes)
	for ord := uint64(1); ord <= m.Chunks; ord++ {
		subject := mqttChunkSubject(idHash, m.Gen, m.Payload, ord)
		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if lerr != nil {
			// A CHUNK THAT CANNOT BE READ IS A CHUNK THAT IS NOT THERE, whether the store said
			// so or could not say. Neither licenses a replay of what is left.
			return nil, fmt.Errorf("chunk %d of payload %q could not be read: %w", ord, m.Payload, lerr)
		}
		// THE MATERIALIZATION IS PART OF THE CHECK. A chunk standing at this address that
		// belongs to a LATER materialization of the same payload token is not this manifest's
		// material, and reassembling it would verify somebody else's bytes against our digest.
		c, derr := mqttDecodeRetainedChunk(smsg.Data, ord, m.Inc)
		if derr != nil {
			return nil, fmt.Errorf("chunk %d of payload %q cannot be interpreted: %w", ord, m.Payload, derr)
		}
		buf = append(buf, c.Data...)
	}
	if uint64(len(buf)) != m.Bytes {
		return nil, fmt.Errorf("payload %q reassembles to %d byte(s) where its manifest claims %d: "+
			"the material is not the material this manifest describes", m.Payload, len(buf), m.Bytes)
	}
	if d := mqttMaterialDigest(buf); d != m.Digest {
		return nil, fmt.Errorf("payload %q reassembles to digest %q where its manifest claims %q",
			m.Payload, d, m.Digest)
	}
	return buf, nil
}

// checkSealVerifiesItsMaterial is the seal's half of the boundary.
//
// Reads the material of the manifest STANDING at the sequence this batch is fenced on. Not the
// proposed record's: a caller writes the proposal, and what must be verified is what is actually
// stored — the chunk fence has already made the two agree about layout, and the CAS is what
// makes this read the state the seal lands against.
func (jsa *mqttJSA) checkSealVerifiesItsMaterial(a *mqttTransitionAttempt, cur *mqttRetainedManifest) error {
	// SEALING IS THE MATERIALIZATION'S OWN ACT. Disposal may carry a manifest — closing one is
	// what a cleanup does first — but it is permitted under a CLOSED authority, and a seal is
	// the state an ADMISSION relies on. A generation that may no longer admit anything has
	// nothing to seal material for.
	if a.Kind != mqttTransitionMaterializePayload {
		return fmt.Errorf("transition %q of kind %q seals payload %q READY: sealing is a "+
			"materialization's own act under a serving authority", a.Txn, a.Kind, cur.Payload)
	}
	if _, err := jsa.verifyMaterial(cur); err != nil {
		return fmt.Errorf("transition %q seals payload %q READY, and its material does not "+
			"verify: READY is what an admission relies on, so an obligation admitted against "+
			"this payload would be promised material that is not there: %w", a.Txn, cur.Payload, err)
	}
	return nil
}
