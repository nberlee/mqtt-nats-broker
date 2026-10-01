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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Immutable retained replay material (F-105, row 7; design D4).
//
// # What retained QoS 1/2 loses
//
// Its register bypass. A retained send today builds its frames from the live retained slot, so
// the material an obligation replays is whatever that slot happens to hold later — which is not
// the material the client was promised. Replay material therefore becomes its own durable thing,
// with a payload identity whose lifetime is NOT shared with the retained topic slot.
//
// # A manifest plus create-only chunks, and the fence between them
//
// The manifest carries what is needed to reconstruct the PUBLISH — effective QoS, total byte
// length, chunk count, an integrity digest, and the provisioning duty that owns its cleanup.
// Connection-specific encodings such as topic aliases are rebuilt for the destination rather
// than stored, because they are facts about a connection and not about the payload.
//
// THE CHUNK FENCE IS AN ATOMIC MANIFEST-STATE CAS TOGETHER WITH THE CHUNK CREATION, IN ONE
// BATCH. Reading the manifest and then publishing a chunk does NOT implement it — that is the
// read-then-act shape this whole item exists to remove, and it would let a chunk land against a
// manifest that had been sealed or closed in between. The CAS is what serializes the writers,
// and the written count is what makes it a CAS with something to say.
//
// # Sealing, and what "verified" means here
//
// READY is reached only once every chunk has landed THROUGH THE FENCE — `Written == Chunks` is
// the durable trace of that, and it is what the boundary can check. Whether the bytes are the
// bytes is the CALLER's evidence, exactly as an obligation's terminal phase is the caller's: the
// boundary is not going to read a whole payload inside the write path, and pretending otherwise
// would make every admission pay for it.
//
// An exchange is admitted only against an exact READY manifest.
//
// # Cleanup seals FIRST, and that ordering is the point
//
// Closing the manifest precedes removing anything. A delayed chunk writer arriving afterwards
// finds a closed manifest and its batch fails — so it cannot recreate material that cleanup has
// already deleted. Deleting first and closing second would leave exactly that window.
//
// An unadmitted payload is reclaimable ONLY after its provisioning outcome is positively aborted
// or sealed against admission. Never on its apparent absence, and never because an admission
// merely failed to be observed.

// The record classes this file owns.
const (
	mqttManifestClassTk = "pay"   // one materialization's manifest
	mqttChunkClassTk    = "chunk" // one create-only slice of its payload
)

// mqttChunkMaxBytes is the largest slice a chunk may carry.
//
// 64 KiB, reduced by the caller to fit the server publish limit INCLUDING headers. The bound
// exists so a payload never becomes an unbounded atomic batch: material too large for one
// message is split, and material too large for the account's storage prevents ADMISSION rather
// than permitting an unregistered send.
const mqttChunkMaxBytes = 64 * 1024

func mqttManifestSubject(idHash, gen, payloadID string) string {
	return mqttF105SubjectPrefix() + mqttManifestClassTk + "." + idHash + "." + gen + "." + payloadID
}

func mqttChunkSubject(idHash, gen, payloadID string, ord uint64) string {
	return mqttF105SubjectPrefix() + mqttChunkClassTk + "." + idHash + "." + gen + "." +
		payloadID + "." + mqttFormatOrdinal(ord)
}

// mqttChunkWalkFilter enumerates one materialization's chunks, and only those.
func mqttChunkWalkFilter(idHash, gen, payloadID string) string {
	return mqttF105SubjectPrefix() + mqttChunkClassTk + "." + idHash + "." + gen + "." +
		payloadID + ".*"
}

func mqttIsManifestSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix()+mqttManifestClassTk+".")
}

func mqttIsChunkSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix()+mqttChunkClassTk+".")
}

// mqttManifestState is how far one materialization has got.
type mqttManifestState uint8

const (
	// mqttManifestProvisioning: chunks may be written, through the fence. NOT admissible — an
	// exchange admitted against a half-written payload would replay material that does not
	// exist yet.
	mqttManifestProvisioning mqttManifestState = 1
	// mqttManifestReady: every chunk landed through the fence and the caller verified them.
	// This is the only state an admission may name.
	mqttManifestReady mqttManifestState = 2
	// mqttManifestClosed: sealed against admission AND against further chunks, which is what
	// makes cleanup safe. Reached from either of the others — from PROVISIONING when a
	// materialization is abandoned, from READY when its cleanup begins.
	mqttManifestClosed mqttManifestState = 3
)

func mqttValidManifestState(s mqttManifestState) bool {
	return s == mqttManifestProvisioning || s == mqttManifestReady || s == mqttManifestClosed
}

// mqttRetainedManifest is one materialization of retained replay material.
type mqttRetainedManifest struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// Payload is this materialization's identity, minted per materialization and NEVER shared
	// with the retained topic slot. That separation is the whole point: the slot may be
	// rewritten or removed while an obligation still owes the material it was promised.
	Payload string            `json:"payload"`
	State   mqttManifestState `json:"state"`

	// Topic, QoS, Props, ExpiresAt and SessionHash are the MQTT material needed to rebuild the
	// PUBLISH. Connection-specific encodings are NOT here; a topic alias is a fact about a
	// connection and is rebuilt for the destination.
	//
	// Props and ExpiresAt exist because the retained SERIALIZER uses them today (`rm.Props` and
	// `rm.expires`), and the whole point of this material is that it outlives the retained slot:
	// once the slot changes or disappears, a manifest without them cannot reproduce a Response
	// Topic, Correlation Data or Content Type, and cannot honour the Message Expiry Interval the
	// publisher set. Storing the topic and the bytes alone would replay a DIFFERENT message.
	//
	// SessionHash is the authenticated No Local origin marker. No Local suppression is decided
	// against the subscriber at send time, so a replay months later still needs it — and it is
	// the authenticated marker rather than a forgeable header for the reason Spec5 [3.8.3.1]
	// requires.
	Topic string `json:"topic"`
	QoS   byte   `json:"qos"`
	Props []byte `json:"props,omitempty"`
	// ExpiresAt is the absolute deadline in Unix nanoseconds, or 0 when the message has no
	// Message Expiry Interval. Absolute rather than an interval: an interval would restart at
	// every materialization and a message would outlive the deadline its publisher set.
	ExpiresAt   int64  `json:"expires_at,omitempty"`
	SessionHash string `json:"session_hash,omitempty"`

	// Bytes is the payload's total length and Chunks how many slices carry it.
	Bytes  uint64 `json:"bytes"`
	Chunks uint64 `json:"chunks"`
	// Written is how many chunks have landed THROUGH THE FENCE. It is what makes the fence a
	// CAS with something to say, and what READY is checked against.
	Written uint64 `json:"written"`
	// Digest is the integrity digest over the whole payload. Carried so a reader can verify
	// what it reassembled; the boundary does not recompute it, because reading a whole payload
	// inside the write path would make every admission pay for it.
	Digest string `json:"digest"`

	// Announced and Purged are the two halves of a removal, and they are two because a
	// JetStream delete cannot ride in the atomic batch.
	//
	// Announced is how many chunks, in ordinal order from 1, a cleanup has said it is about to
	// remove. Purged is how many it has CONFIRMED removed. A resumed cleanup re-deletes
	// (Purged, Announced] — deleting an absent chunk is the one operation that is safe to
	// repeat — and only then confirms.
	//
	// One counter was not enough, and a review found the interleaving: with a single count
	// advanced BEFORE the deletes, a crash between the announcement and the first delete left
	// a manifest saying the work was done, so the resume found nothing to do and removed the
	// records describing chunks that are all still standing.
	Announced uint64 `json:"announced,omitempty"`
	Purged    uint64 `json:"purged,omitempty"`

	// Inc is the resource incarnation that owns this material's cleanup. The payload's resource
	// identity and cleanup ownership are reserved BEFORE any chunk is written, so a crash
	// mid-materialization leaves material that a retirement worker can still find and own.
	Inc string `json:"inc"`

	// SubID is the Subscription Identifier of the delivery this copy was made for (Spec5
	// [3.8.2.1.2]), or 0 when the subscription carried none. It is a fact about the DELIVERY, so a
	// replay injects it from here rather than from whichever subscription matches later — which
	// could carry a different identifier and change the message the client was promised.
	SubID uint32 `json:"sub_id,omitempty"`
	// Tenure is the PRODUCING entitlement's tenure, immutable from the reservation on. It is the
	// fence a reclamation of UNADMITTED material is decided against: closing such a manifest is
	// admitted only when a successor tenure serves, the generation is closed, or the closer is the
	// producer itself (checkRetainedManifestArbitration).
	Tenure string `json:"tenure"`
	// Admitted names the ONE exchange admitted against this copy, set once by the admission batch
	// READY -> READY. It is what makes admission and closure ARBITRATE on one record: both CAS the
	// manifest, so whichever lands first makes the other's CAS fail, and a close re-validates the
	// named exchange inside its own batch rather than trusting an earlier observation.
	Admitted string `json:"admitted,omitempty"`
	// AdmittedPI is the identifier the admitted exchange's register stands at, written WITH
	// Admitted by the same batch and immutable with it (a retained exchange's identifier never
	// moves after admission, consult 102). It is the close's independent address for that
	// register, so an exchange record whose PI contradicts it locates nothing (consult 109).
	AdmittedPI uint16 `json:"admitted_pi,omitempty"`
}

var mqttManifestMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "payload": {}, "state": {}, "topic": {}, "qos": {},
	"props": {}, "expires_at": {}, "session_hash": {},
	"bytes": {}, "chunks": {}, "written": {}, "digest": {}, "inc": {},
	"announced": {}, "purged": {}, "sub_id": {}, "tenure": {}, "admitted": {}, "admitted_pi": {},
}

var mqttManifestRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "payload": {}, "state": {}, "topic": {}, "qos": {},
	"bytes": {}, "chunks": {}, "written": {}, "digest": {}, "inc": {}, "tenure": {},
}

var mqttManifestOptional = map[string]struct{}{
	"props": {}, "expires_at": {}, "session_hash": {}, "announced": {}, "purged": {},
	"sub_id": {}, "admitted": {},
}

func mqttDecodeRetainedManifest(data []byte, wantID, wantEpoch, wantGen, wantPayload string) (*mqttRetainedManifest, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttManifestMembers, "a retained manifest"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttManifestRequired, "a retained manifest"); err != nil {
		return nil, err
	}
	if err := mqttJSONOptionalMembersNotNull(data, mqttManifestOptional, "a retained manifest"); err != nil {
		return nil, err
	}
	m := &mqttRetainedManifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	if err := mqttValidateRetainedManifest(m, wantID, wantEpoch, wantGen, wantPayload); err != nil {
		return nil, err
	}
	return m, nil
}

// mqttValidateRetainedManifest refuses a manifest this broker cannot act on.
func mqttValidateRetainedManifest(m *mqttRetainedManifest, wantID, wantEpoch, wantGen, wantPayload string) error {
	if m == nil {
		return fmt.Errorf("retained manifest is absent")
	}
	if m.ID != wantID || m.Epoch != wantEpoch || m.Gen != wantGen || m.Payload != wantPayload {
		return fmt.Errorf("retained manifest names %q/%q/%q/%q, not %q/%q/%q/%q",
			m.ID, m.Epoch, m.Gen, m.Payload, wantID, wantEpoch, wantGen, wantPayload)
	}
	if !mqttF105IdentTk(m.Payload) {
		return fmt.Errorf("retained manifest carries a malformed payload identity %q", m.Payload)
	}
	if !mqttF105IdentTk(m.Inc) {
		return fmt.Errorf("retained manifest carries a malformed cleanup incarnation %q", m.Inc)
	}
	if !mqttF105IdentTk(m.Tenure) {
		return fmt.Errorf("retained manifest carries a malformed producing tenure %q", m.Tenure)
	}
	// AN ADMISSION NAMES AN EXCHANGE, and only a payload that was whole could have taken one.
	if m.Admitted != _EMPTY_ {
		if !mqttValidInflightNonce(m.Admitted) {
			return fmt.Errorf("retained manifest names admitted exchange %q, which is not an exchange identity", m.Admitted)
		}
		if m.State == mqttManifestProvisioning {
			return fmt.Errorf("retained manifest is provisioning yet names admitted exchange %q: an "+
				"exchange is admitted only against a READY payload", m.Admitted)
		}
	}
	// THE ADMISSION ADDRESS EXISTS EXACTLY WHEN AN ADMISSION DOES: no fallback to the exchange's PI.
	if (m.Admitted != _EMPTY_) != (m.AdmittedPI != 0) {
		return fmt.Errorf("retained manifest names admitted exchange %q at identifier %d: the admission and its "+
			"address are written together", m.Admitted, m.AdmittedPI)
	}
	if !mqttValidManifestState(m.State) {
		return fmt.Errorf("retained manifest carries an unsupported state %d", m.State)
	}
	if m.Topic == _EMPTY_ {
		return fmt.Errorf("retained manifest carries no topic, so no PUBLISH can be rebuilt from it")
	}
	// RETAINED QoS 0 NEEDS NO MANIFEST. It has no outstanding acknowledgement obligation and no
	// exchange register, so material that outlives the slot is material nothing will replay.
	if m.QoS != 1 && m.QoS != 2 {
		return fmt.Errorf("retained manifest carries QoS %d: only 1 and 2 have an obligation to "+
			"replay material for", m.QoS)
	}
	if m.Digest == _EMPTY_ {
		return fmt.Errorf("retained manifest carries no integrity digest, so nothing reassembled " +
			"from it can be checked")
	}
	// A ZERO-LENGTH PAYLOAD IS LEGITIMATE and carries no chunks; anything else needs at least
	// one. The two are tied together so a manifest cannot claim bytes nothing stores.
	if (m.Bytes == 0) != (m.Chunks == 0) {
		return fmt.Errorf("retained manifest claims %d byte(s) across %d chunk(s): a payload with "+
			"bytes has chunks and one without has none", m.Bytes, m.Chunks)
	}
	if m.Announced > m.Chunks {
		return fmt.Errorf("retained manifest has %d chunk(s) announced for removal of %d: a "+
			"cleanup cannot remove material the payload never had", m.Announced, m.Chunks)
	}
	// CONFIRMED NEVER RUNS AHEAD OF ANNOUNCED. A confirmation is of an announcement, and one
	// that outran it would report chunks removed that nothing had said it was removing.
	if m.Purged > m.Announced {
		return fmt.Errorf("retained manifest confirms %d chunk(s) removed of %d announced: a "+
			"confirmation answers an announcement", m.Purged, m.Announced)
	}
	// MATERIAL IS REMOVED ONLY ONCE THE MANIFEST IS CLOSED, so a record that is not closed and
	// claims a removal describes a state this broker never writes.
	if m.Announced > 0 && m.State != mqttManifestClosed {
		return fmt.Errorf("retained manifest in state %d has %d chunk(s) announced for removal: "+
			"removal begins after closure, which is what stops a delayed writer replacing "+
			"material a cleanup has already taken", m.State, m.Announced)
	}
	// AND THE LAYOUT IS BOUNDED BY WHAT A CHUNK CAN HOLD. Without this a manifest can claim
	// any number of bytes across any number of chunks, and a reader that sizes a buffer from
	// the claim allocates whatever the record said — a description is not material.
	if m.Bytes > m.Chunks*mqttChunkMaxBytes {
		return fmt.Errorf("retained manifest claims %d byte(s) across %d chunk(s), more than the "+
			"%d a chunk may hold", m.Bytes, m.Chunks, mqttChunkMaxBytes)
	}
	// AND EVERY CHUNK CARRIES SOMETHING, which is the rule a chunk's own shape already states.
	// A manifest claiming more chunks than bytes describes slices that cannot exist.
	if m.Bytes < m.Chunks {
		return fmt.Errorf("retained manifest claims %d byte(s) across %d chunk(s): a chunk that "+
			"stores nothing is a slice of a payload that does not need it", m.Bytes, m.Chunks)
	}
	if m.Written > m.Chunks {
		return fmt.Errorf("retained manifest has %d chunk(s) written of %d: more material landed "+
			"than the manifest says exists", m.Written, m.Chunks)
	}
	// READY MEANS EVERY CHUNK LANDED THROUGH THE FENCE. Sealing with fewer would admit an
	// obligation against material that is not all there.
	if m.State == mqttManifestReady && m.Written != m.Chunks {
		return fmt.Errorf("retained manifest is READY with %d chunk(s) written of %d: sealing "+
			"says every chunk landed through the fence", m.Written, m.Chunks)
	}
	return nil
}

// mqttManifestAdmits reports whether an obligation may be admitted against this manifest.
//
// ONLY a READY one, and the function exists so that fact has one home. An admission against a
// PROVISIONING manifest replays material that is not all there; one against a CLOSED manifest
// replays material whose cleanup has already been sealed.
func mqttManifestAdmits(m *mqttRetainedManifest) bool {
	return m != nil && m.State == mqttManifestReady
}

// mqttRetainedChunk is one create-only slice of a payload.
type mqttRetainedChunk struct {
	// Ord is this chunk's position, from 1. Slices are reassembled in ordinal order, which is
	// why the ordinal is a subject token here for the same reason it is for a duty: the name
	// must not move when the record does.
	Ord uint64 `json:"ord"`
	// Inc is the MATERIALIZATION this slice belongs to — its manifest's cleanup owner.
	//
	// It is here because a subject can be reused and a record cannot be un-deleted. A review
	// found the interleaving: a cleanup pauses mid-interval, another finishes and removes
	// everything, a NEW materialization reserves the same payload token, and the paused worker
	// resumes and deletes the newcomer's chunks by subject. Every delete now loads the record
	// first — which it did anyway, to learn its sequence — and a chunk of a different
	// incarnation is not this cleanup's to take.
	Inc  string `json:"inc"`
	Data []byte `json:"data"`
}

var mqttChunkMembers = map[string]struct{}{"ord": {}, "inc": {}, "data": {}}

// mqttDecodeRetainedChunk reads a chunk that the caller knows belongs to a named
// materialization, and refuses one that does not.
func mqttDecodeRetainedChunk(data []byte, wantOrd uint64, wantInc string) (*mqttRetainedChunk, error) {
	if !mqttF105IdentTk(wantInc) {
		// AN EMPTY ARGUMENT IS NOT A PERMISSION. A blank token here used to mean "do not
		// check", which is the shape this lane refuses everywhere else: a caller that wants
		// whatever stands at an address says so by NAME, through the discovery decoder below.
		return nil, fmt.Errorf("reading chunk %d requires the materialization it must belong "+
			"to; a caller reading whatever stands at that address uses "+
			"mqttDiscoverRetainedChunk", wantOrd)
	}
	return mqttDecodeChunkBody(data, wantOrd, wantInc)
}

// mqttDiscoverRetainedChunk reads WHATEVER chunk stands at an address, for the two callers whose
// question is exactly that.
//
// A cleanup re-deleting an announced interval, and a confirmation establishing that its own
// chunks are gone, both meet records that may belong to a LATER materialization of the same
// payload token. Neither may refuse such a record — it answers their question — and neither may
// act on it. Named apart from the checking decoder so the permissive read is a deliberate call
// rather than a blank argument.
func mqttDiscoverRetainedChunk(data []byte, wantOrd uint64) (*mqttRetainedChunk, error) {
	return mqttDecodeChunkBody(data, wantOrd, _EMPTY_)
}

func mqttDecodeChunkBody(data []byte, wantOrd uint64, wantInc string) (*mqttRetainedChunk, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttChunkMembers, "a retained chunk"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttChunkMembers, "a retained chunk"); err != nil {
		return nil, err
	}
	c := &mqttRetainedChunk{}
	if err := json.Unmarshal(data, c); err != nil {
		return nil, err
	}
	if err := mqttValidateRetainedChunk(c, wantOrd, wantInc); err != nil {
		return nil, err
	}
	return c, nil
}

func mqttValidateRetainedChunk(c *mqttRetainedChunk, wantOrd uint64, wantInc string) error {
	if c == nil {
		return fmt.Errorf("retained chunk is absent")
	}
	if !mqttF105IdentTk(c.Inc) {
		return fmt.Errorf("retained chunk %d carries a malformed materialization %q", c.Ord, c.Inc)
	}
	// THE MATERIALIZATION IS CHECKED WHEREVER THE CALLER KNOWS IT. A caller that does not — a
	// reader discovering what stands at an address — passes empty and gets the record back to
	// decide about, which is the case a cleanup needs.
	if wantInc != _EMPTY_ && c.Inc != wantInc {
		return fmt.Errorf("retained chunk %d belongs to materialization %q, not %q",
			c.Ord, c.Inc, wantInc)
	}
	if c.Ord != wantOrd {
		return fmt.Errorf("retained chunk carries ordinal %d on the subject of %d", c.Ord, wantOrd)
	}
	if c.Ord == 0 {
		return fmt.Errorf("retained chunk carries ordinal zero, and chunks are numbered from 1")
	}
	if len(c.Data) == 0 {
		return fmt.Errorf("retained chunk %d carries no bytes: a chunk that stores nothing is a "+
			"slice of a payload that does not need it", c.Ord)
	}
	if len(c.Data) > mqttChunkMaxBytes {
		return fmt.Errorf("retained chunk %d carries %d bytes, over the %d a chunk may hold",
			c.Ord, len(c.Data), mqttChunkMaxBytes)
	}
	return nil
}

// mqttCheckManifestStep is the manifest's own state machine, applied to the STORED manifest and
// the proposed one.
//
// Identity never moves and the state runs FORWARD ONLY. The two things it is careful about:
//
//   - WRITTEN ONLY EVER RISES, and only by the chunks this batch actually carries. A manifest
//     that could step its own count would seal READY over material nobody wrote.
//   - CLOSED IS TERMINAL. Reopening it would let a delayed chunk writer recreate material whose
//     cleanup was already sealed, which is the exact window sealing-before-deleting closes.
func mqttCheckManifestStep(a *mqttTransitionAttempt, cur, next *mqttRetainedManifest, chunksHere uint64) error {
	if cur.Payload != next.Payload || cur.Inc != next.Inc {
		return fmt.Errorf("transition %q re-identifies manifest %q as %q/%q", a.Txn,
			cur.Payload, next.Payload, next.Inc)
	}
	if cur.Topic != next.Topic || cur.QoS != next.QoS || cur.Bytes != next.Bytes ||
		cur.Chunks != next.Chunks || cur.Digest != next.Digest ||
		cur.ExpiresAt != next.ExpiresAt || cur.SessionHash != next.SessionHash ||
		cur.SubID != next.SubID || cur.Tenure != next.Tenure ||
		!bytes.Equal(cur.Props, next.Props) {
		return fmt.Errorf("transition %q rewrites the immutable material of manifest %q: what a "+
			"payload IS was fixed when it was reserved, and an obligation may already have been "+
			"admitted against it", a.Txn, cur.Payload)
	}
	if cur.State == mqttManifestClosed && next.State != mqttManifestClosed {
		return fmt.Errorf("transition %q reopens closed manifest %q: closing precedes deletion "+
			"precisely so a delayed writer cannot recreate material that is already gone",
			a.Txn, cur.Payload)
	}
	if cur.State == mqttManifestReady && next.State == mqttManifestProvisioning {
		return fmt.Errorf("transition %q returns manifest %q from READY to provisioning: an "+
			"obligation may already have been admitted against it", a.Txn, cur.Payload)
	}
	// ADMITTED IS WRITTEN ONCE, by a READY -> READY step carrying no material, and never changes:
	// a second admission against one subscriber's copy, or one that moves, would let two
	// exchanges depend on material whose reclamation each would decide alone.
	if cur.Admitted != _EMPTY_ && (next.Admitted != cur.Admitted || next.AdmittedPI != cur.AdmittedPI) {
		return fmt.Errorf("transition %q rewrites manifest %q's admitted exchange %q at %d as %q at %d", a.Txn,
			cur.Payload, cur.Admitted, cur.AdmittedPI, next.Admitted, next.AdmittedPI)
	}
	if cur.Admitted == _EMPTY_ && next.Admitted != _EMPTY_ &&
		(cur.State != mqttManifestReady || next.State != mqttManifestReady || chunksHere != 0) {
		return fmt.Errorf("transition %q admits against manifest %q moving from state %d to %d: an "+
			"admission names a READY payload and changes nothing else", a.Txn, cur.Payload, cur.State, next.State)
	}
	if err := mqttCheckPurgeStep(a, cur, next); err != nil {
		return err
	}
	if next.Written < cur.Written {
		return fmt.Errorf("transition %q lowers manifest %q's written count from %d to %d",
			a.Txn, cur.Payload, cur.Written, next.Written)
	}
	// THE COUNT MOVES BY EXACTLY THE CHUNKS THIS BATCH CARRIES. That is what makes it a durable
	// trace of material landing through the fence rather than an assertion about material that
	// might be anywhere.
	if next.Written-cur.Written != chunksHere {
		return fmt.Errorf("transition %q steps manifest %q's written count by %d while carrying "+
			"%d chunk(s): the count is the trace of chunks landing through this fence",
			a.Txn, cur.Payload, next.Written-cur.Written, chunksHere)
	}
	// AND CHUNKS LAND ONLY WHILE PROVISIONING. A chunk beside a READY or CLOSED manifest is
	// material arriving after the payload was declared whole.
	if chunksHere > 0 && (cur.State != mqttManifestProvisioning || next.State != mqttManifestProvisioning) {
		return fmt.Errorf("transition %q carries %d chunk(s) for manifest %q moving from state "+
			"%d to %d: material lands only while the payload is still being provisioned",
			a.Txn, chunksHere, cur.Payload, cur.State, next.State)
	}
	return nil
}

// checkManifestMembers validates every manifest and chunk member of one batch.
//
// THE FENCE LIVES HERE, and it is stated as an obligation on the batch rather than a condition on
// its contents — the same correction the inventory rules needed. A chunk member in a batch with
// no manifest CAS is refused for that reason alone: the absence IS the violation, because a
// caller that simply omitted the manifest would otherwise escape every rule conditioned on it.
func (jsa *mqttJSA) checkManifestMembers(a *mqttTransitionAttempt, prev *mqttOutboundAuthority, extra []mqttTransitionMember) error {
	idHash := getHash(a.ID)

	var (
		manifest    *mqttRetainedManifest
		manifestSub string
		manifestExp uint64
		manifestGen string
		manifestPay string
		manifestInc string
	)
	for _, e := range extra {
		if !mqttIsManifestSubject(e.subj) {
			continue
		}
		gen, pay, perr := mqttParseManifestSubject(e.subj, idHash)
		if perr != nil {
			return fmt.Errorf("transition %q carries a manifest member on %q, which this broker "+
				"cannot place: %w", a.Txn, e.subj, perr)
		}
		if err := mqttMaterialMemberGeneration(a, e.subj, gen); err != nil {
			return err
		}
		next, derr := mqttDecodeRetainedManifest(e.body, a.ID, a.Epoch, gen, pay)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported retained manifest on %q: %w",
				a.Txn, e.subj, derr)
		}
		if manifest != nil {
			return fmt.Errorf("transition %q carries two manifests, %q and %q: a chunk fence is "+
				"about one materialization", a.Txn, manifestPay, pay)
		}
		manifest, manifestSub, manifestExp = next, e.subj, e.expect
		manifestGen, manifestPay, manifestInc = gen, pay, next.Inc
	}

	chunks := uint64(0)
	seen := map[uint64]struct{}{}
	for _, e := range extra {
		if !mqttIsChunkSubject(e.subj) {
			continue
		}
		gen, pay, ord, perr := mqttParseChunkSubject(e.subj, idHash)
		if perr != nil {
			return fmt.Errorf("transition %q carries a chunk member on %q, which this broker "+
				"cannot place: %w", a.Txn, e.subj, perr)
		}
		if err := mqttMaterialMemberGeneration(a, e.subj, gen); err != nil {
			return err
		}
		// CREATE-ONLY, ALWAYS. A chunk is immutable material; an expectation here would mean
		// this batch believed it was replacing bytes an obligation may already be replaying.
		if e.expect != 0 {
			return fmt.Errorf("transition %q rewrites chunk %d of payload %q at sequence %d: "+
				"replay material is immutable, and an obligation may already be reading it",
				a.Txn, ord, pay, e.expect)
		}
		// MATERIAL IS WRITTEN UNDER A SERVING AUTHORITY, and the kind is what says so. Disposal
		// carries manifests and chunks too — a cleanup closes the manifest before removing
		// anything — but it is permitted under a CLOSED authority because cleanup must outlive
		// closure. Writing bytes must not: material landing in a closed generation is material
		// nothing may admit against and nothing will finish.
		if a.Kind != mqttTransitionMaterializePayload && a.Kind != mqttTransitionProvisionResource {
			return fmt.Errorf("transition %q of kind %q carries chunk %d of payload %q: material "+
				"is written by a materialization under a serving authority", a.Txn, a.Kind, ord, pay)
		}
		if manifest == nil {
			return fmt.Errorf("transition %q carries chunk %d of payload %q with no manifest "+
				"member: the fence is the manifest CAS and the chunk creation in ONE batch, and "+
				"a chunk landing without it is material arriving against a manifest nobody "+
				"checked", a.Txn, ord, pay)
		}
		if gen != manifestGen || pay != manifestPay {
			return fmt.Errorf("transition %q carries a chunk of %q/%q beside the manifest of "+
				"%q/%q", a.Txn, gen, pay, manifestGen, manifestPay)
		}
		// AND ONLY NOW IS THE BODY READ, against the materialization the fence just established.
		// The identity a chunk must carry is the manifest's cleanup owner, so a body check
		// ahead of the fence would have no identity to check against and would have to accept
		// whatever the chunk named for itself.
		if _, derr := mqttDecodeRetainedChunk(e.body, ord, manifestInc); derr != nil {
			return fmt.Errorf("transition %q carries an unsupported retained chunk on %q: %w",
				a.Txn, e.subj, derr)
		}
		if _, dup := seen[ord]; dup {
			return fmt.Errorf("transition %q carries chunk %d of payload %q twice", a.Txn, ord, pay)
		}
		seen[ord] = struct{}{}
		if ord > manifest.Chunks {
			return fmt.Errorf("transition %q carries chunk %d of payload %q, which claims only "+
				"%d chunk(s)", a.Txn, ord, pay, manifest.Chunks)
		}
		chunks++
	}

	if manifest == nil {
		return nil
	}
	return jsa.checkManifestAgainstStore(a, prev, extra, manifest, manifestSub, manifestExp, manifestGen,
		manifestPay, chunks)
}

// checkManifestAgainstStore re-reads the standing manifest and applies its state machine.
func (jsa *mqttJSA) checkManifestAgainstStore(a *mqttTransitionAttempt, prev *mqttOutboundAuthority, extra []mqttTransitionMember,
	next *mqttRetainedManifest, subj string, expect uint64, gen, pay string, chunksHere uint64) error {

	smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	absent := (lerr != nil && !isErrorOtherThan(lerr, JSNoMessageFoundErr)) || (lerr == nil && smsg == nil)
	if lerr != nil && !absent {
		return fmt.Errorf("transition %q could not read the manifest of payload %q: %w", a.Txn, pay, lerr)
	}

	if absent {
		// A RESERVATION IS AN ACT OF MATERIALIZING, so only a materializing kind may perform
		// one. The chunk loop has said this since it was written — "material is written under a
		// serving authority, and the kind is what says so" — and the branch that brings a
		// payload into existence never got the same rule.
		//
		// DISPOSAL IS THE ONE THAT MATTERS. A cleanup legitimately carries manifests, because
		// closing one precedes deleting its material, and it is permitted under a CLOSED
		// authority because cleanup must outlive closure. It has no business CREATING a payload:
		// material reserved by a disposal is material admitted under an authority that may
		// already be closed, in an act whose whole purpose is removal. Round I-7's bypass was
		// exactly this batch — a disposal carrying its resource's seal beside a create-only
		// manifest — and the seal was only the half that was visible.
		if a.Kind != mqttTransitionMaterializePayload && a.Kind != mqttTransitionProvisionResource {
			return fmt.Errorf("transition %q of kind %q reserves payload %q: a materialization "+
				"brings material into existence under a serving authority, and a kind that does "+
				"not materialize does not get to create the material it may later remove",
				a.Txn, a.Kind, pay)
		}
		// A MATERIALIZATION'S FIRST ACT RESERVES IT, create-only, with nothing written and its
		// cleanup owner already named — so a crash mid-materialization leaves material a
		// retirement worker can still find and own.
		if expect != 0 {
			return fmt.Errorf("transition %q conditions the manifest of payload %q on sequence "+
				"%d, but nothing stands there", a.Txn, pay, expect)
		}
		if next.State != mqttManifestProvisioning {
			return fmt.Errorf("transition %q reserves payload %q directly in state %d: a "+
				"materialization begins provisioning and is sealed only once its material has "+
				"landed", a.Txn, pay, next.State)
		}
		if next.Written != 0 || chunksHere != 0 {
			return fmt.Errorf("transition %q reserves payload %q already carrying material: the "+
				"payload's identity and cleanup ownership are reserved BEFORE any chunk is "+
				"written", a.Txn, pay)
		}
		// THE PRODUCING TENURE IS THE BATCH'S OWN (F-105 7c). It is the fence every later
		// reclamation of unadmitted material decides against, so it is bound here to the serving
		// authority this reservation CASes rather than taken from the caller.
		if prev == nil || prev.State != mqttAuthorityServing || next.Tenure != prev.Tenure || next.Tenure != a.Tenure {
			return fmt.Errorf("transition %q reserves payload %q naming producing tenure %q, which is not the "+
				"serving tenure this batch is admitted under", a.Txn, pay, next.Tenure)
		}
		// AND THE CLEANUP OWNER IS ESTABLISHED, NOT MERELY NAMED.
		//
		// This is the gap round I-3 found, and it is the same shape as the roles one: a
		// well-formed Inc token proves nothing about a resource existing. Without this, a crash
		// after reservation leaves durable payload material whose owner does not exist — so the
		// duty sweep never discovers it, and because the generation may have neither ledger nor
		// closure the release gate treats it as vacuous too. Material with no owner and no
		// enrolment is material nothing will ever reclaim.
		//
		// The owner may be created BY THIS BATCH, in which case rule 3 has already required its
		// provisioning and cleanup duty entries, or it may already stand.
		return jsa.checkManifestOwnerExists(a, extra, gen, pay, next.Inc)
	}
	if expect == 0 {
		return fmt.Errorf("transition %q reserves payload %q create-only while a manifest "+
			"already stands at sequence %d", a.Txn, pay, smsg.Sequence)
	}
	if smsg.Sequence != expect {
		return fmt.Errorf("transition %q conditions the manifest of payload %q on sequence %d, "+
			"but it stands at %d: the fence was computed against a manifest that has moved",
			a.Txn, pay, expect, smsg.Sequence)
	}
	cur, derr := mqttDecodeRetainedManifest(smsg.Data, a.ID, a.Epoch, gen, pay)
	if derr != nil {
		return fmt.Errorf("transition %q fences against the manifest of payload %q, which this "+
			"broker cannot interpret: %w", a.Txn, pay, derr)
	}
	if err := mqttCheckManifestStep(a, cur, next, chunksHere); err != nil {
		return err
	}
	// AND THE COPY ARBITRATES admission, the retained PUBREC fence and closure (F-105 7c).
	if err := jsa.checkRetainedManifestArbitration(a, prev, extra, cur, next); err != nil {
		return err
	}
	// AND A REMOVAL IS AUTHORIZED AGAINST THE EXACT RESOURCE THAT OWNS THIS MATERIAL, read
	// from the store rather than from this batch's own members.
	if err := jsa.checkPurgeIsAuthorized(a, cur, next); err != nil {
		return err
	}
	// AND A CONFIRMATION IS EVIDENCE. The chunks it claims to have removed are read, because
	// the arithmetic of two counters says nothing about whether anything was deleted.
	if err := jsa.checkPurgeConfirmed(a, cur, next); err != nil {
		return err
	}
	// AND A SEAL VERIFIES THE MATERIAL IT IS SEALING, inside this transaction.
	//
	// `Written == Chunks` proves every chunk landed through the fence; it proves nothing about
	// the bytes. A review refused the argument that verification can therefore happen at REPLAY:
	// replay-time checking can stop corrupt output, but READY is what an ADMISSION relies on,
	// and an obligation admitted against material that was never whole has already been
	// promised to a client. A second review refused the record that replaced that argument, for
	// a sharper reason: a caller-supplied witness is a claim, not a verification.
	if next.State == mqttManifestReady && cur.State != mqttManifestReady {
		return jsa.checkSealVerifiesItsMaterial(a, cur)
	}
	return nil
}

// mqttParseManifestSubject reads a manifest subject's generation and payload identity.
func mqttParseManifestSubject(subject, wantIDHash string) (string, string, error) {
	rest, ok := strings.CutPrefix(subject, mqttF105SubjectPrefix()+mqttManifestClassTk+".")
	if !ok {
		return _EMPTY_, _EMPTY_, fmt.Errorf("subject is not a retained manifest")
	}
	toks := strings.Split(rest, ".")
	if len(toks) != 3 {
		return _EMPTY_, _EMPTY_, fmt.Errorf("manifest subject has %d tokens past its class (want 3)",
			len(toks))
	}
	if toks[0] != wantIDHash {
		return _EMPTY_, _EMPTY_, fmt.Errorf("manifest subject names client hash %q, not %q",
			toks[0], wantIDHash)
	}
	if !mqttValidSessGen(toks[1]) {
		return _EMPTY_, _EMPTY_, fmt.Errorf("malformed generation token %q", toks[1])
	}
	if !mqttF105IdentTk(toks[2]) {
		return _EMPTY_, _EMPTY_, fmt.Errorf("malformed payload token %q", toks[2])
	}
	return toks[1], toks[2], nil
}

// mqttParseChunkSubject reads a chunk subject's generation, payload identity and ordinal.
func mqttParseChunkSubject(subject, wantIDHash string) (string, string, uint64, error) {
	rest, ok := strings.CutPrefix(subject, mqttF105SubjectPrefix()+mqttChunkClassTk+".")
	if !ok {
		return _EMPTY_, _EMPTY_, 0, fmt.Errorf("subject is not a retained chunk")
	}
	toks := strings.Split(rest, ".")
	if len(toks) != 4 {
		return _EMPTY_, _EMPTY_, 0, fmt.Errorf("chunk subject has %d tokens past its class (want 4)",
			len(toks))
	}
	if toks[0] != wantIDHash {
		return _EMPTY_, _EMPTY_, 0, fmt.Errorf("chunk subject names client hash %q, not %q",
			toks[0], wantIDHash)
	}
	if !mqttValidSessGen(toks[1]) {
		return _EMPTY_, _EMPTY_, 0, fmt.Errorf("malformed generation token %q", toks[1])
	}
	if !mqttF105IdentTk(toks[2]) {
		return _EMPTY_, _EMPTY_, 0, fmt.Errorf("malformed payload token %q", toks[2])
	}
	ord, oerr := mqttParseOrdinal(toks[3])
	if oerr != nil {
		return _EMPTY_, _EMPTY_, 0, oerr
	}
	return toks[1], toks[2], ord, nil
}

// checkManifestOwnerExists requires a reserved payload's cleanup owner to be a real resource.
//
// Either this batch creates it — in which case the coverage rule has already required its
// provisioning and cleanup duty entries, so the material is discoverable the moment it exists —
// or it already stands, in which case those entries were required when it was created.
//
// An UNREADABLE resource is a refusal. A resource this broker cannot interpret is not one it may
// declare a valid owner, and reserving material against it would be reserving against nothing.
func (jsa *mqttJSA) checkManifestOwnerExists(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	gen, pay, inc string) error {

	idHash := getHash(a.ID)
	want := mqttResourceSubject(idHash, gen, inc)
	var r *mqttOutboundResource
	for _, e := range extra {
		if e.subj != want || e.expect != 0 {
			continue
		}
		here, derr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, gen, inc)
		if derr != nil {
			return fmt.Errorf("transition %q reserves payload %q against a cleanup owner it "+
				"creates in the same batch, which this broker cannot interpret: %w",
				a.Txn, pay, derr)
		}
		r = here
	}
	if r == nil {
		here, _, rerr := jsa.loadOutboundResource(a.ID, a.Epoch, gen, inc)
		if rerr != nil || here == nil {
			return fmt.Errorf("transition %q reserves payload %q naming cleanup owner %q, which "+
				"this batch does not create and which does not stand (%v): naming an owner does "+
				"not reserve one, and material whose owner never existed is material no duty "+
				"sweep will discover and nothing will ever reclaim", a.Txn, pay, inc, rerr)
		}
		r = here
	}
	// A CLOSED OWNER OWNS NOTHING FURTHER. Reserving new material against a resource whose
	// deletion is already authorized would attach it to a cleanup that has already been decided.
	if r.State != mqttResourceLive {
		return fmt.Errorf("transition %q reserves payload %q against cleanup owner %q in state "+
			"%d: a resource whose disposal is already authorized takes on no new material",
			a.Txn, pay, inc, r.State)
	}
	// AND THE OWNER IS THE PAYLOAD'S OWN, NAMED IN BOTH DIRECTIONS.
	//
	// The manifest names its owner and the resource names its material, and the two must be the
	// same materialization. The agreement is what makes the seal below answerable at all: a
	// resource whose Name did not address its manifest could be sealed while material stood
	// under it, because the boundary would be reading an address nothing occupies.
	if r.Kind != mqttResourceRetainedPayload {
		return fmt.Errorf("transition %q reserves payload %q against resource %q of kind %q: "+
			"replay material is owned by the payload resource provisioned for it",
			a.Txn, pay, inc, r.Kind)
	}
	if r.Name != pay {
		return fmt.Errorf("transition %q reserves payload %q against resource %q, whose material "+
			"is %q: a resource owns the materialization it was provisioned for, and material "+
			"filed under another name is material its owner's seal does not cover",
			a.Txn, pay, inc, r.Name)
	}
	// AND A SEALED OWNER MATERIALIZES NOTHING FURTHER — that is what the seal SAYS.
	//
	// Provisioning sealed is the positive fact a deletion authorization rests on. Reserving
	// material after it would make the seal false retroactively: the authorization was taken
	// while nothing could arrive, and material arriving afterwards belongs to a cleanup that has
	// already counted what it has to remove.
	if r.Prov == mqttProvisioningSealed {
		return fmt.Errorf("transition %q reserves payload %q against cleanup owner %q whose "+
			"provisioning is sealed: a seal is the positive fact that nothing further can "+
			"materialize the resource, and material landing after it is material the seal "+
			"already said would not exist", a.Txn, pay, inc)
	}
	return nil
}

// mqttMaterialMemberGeneration refuses a material member of a generation this transition does
// not speak for.
//
// CLEANUP IS THE EXEMPTION, and the only one. A disposal of G1 runs while G2 serves — that is
// the whole reason its kind exists — so a disposal names another generation's material by
// design. Every other kind writes material for the generation it is admitted under: a
// materialization that could name G1's payload would grow material in a generation that may
// already be closed, and an admission would then depend on it.
func mqttMaterialMemberGeneration(a *mqttTransitionAttempt, subject, gen string) error {
	if a.Kind == mqttTransitionDisposeResource || gen == a.Gen {
		return nil
	}
	// THE RETAINED PUBREC FENCE may name an earlier generation's copy for the advance's own
	// reason: a G1 exchange must reach its end while G2 serves. It carries the manifest unchanged
	// and no chunk (chunks are confined to the materializing kinds).
	if a.Kind == mqttTransitionAdvanceExchange && !mqttIsChunkSubject(subject) {
		return nil
	}
	return fmt.Errorf("transition %q carries material on %q, which names generation %q rather "+
		"than the %q it is admitted under", a.Txn, subject, gen, a.Gen)
}
