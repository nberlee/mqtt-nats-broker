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
	"strings"
)

// Physical-name ownership for source/PUBREL consumers (F-105 cutover, consult 4).
//
// # Why a name-keyed record, when resources are keyed by incarnation
//
// The outbound resource record is keyed by generation+incarnation on purpose
// (mqttResourceSubject): a retirement authorization must be about the exact thing it was
// computed for, so that a DELETE cannot destroy a successor that merely reuses a name. That
// keying answers "may THIS resource be deleted", and it is right for deletion authorization.
//
// It does not answer the other question the consumer lifecycle needs: "may this physical
// consumer NAME be used at all, right now, and by whom". JetStream's CONSUMER.CREATE and
// CONSUMER.DELETE both carry a stream and a consumer NAME, not an F-105 incarnation, and a
// CREATE is a queued request the broker can neither fence nor retract. So the broker cannot
// make a resource record the arbiter of the physical consumer's EXISTENCE. What it can
// arbitrate is USE: whether an obligation may depend on a name, and whether a delete of a
// name is authorized.
//
// This record is that USE arbiter. It is keyed by the physical name (within the owning
// client), NEVER by generation, so exactly one owner can hold a name LIVE and a retired name
// is spent for good:
//
//	absent -> LIVE(owner) -> RETIRED(owner)
//
// LIVE(I1) -> LIVE(I2) and RETIRED -> LIVE are both refused. A replacement uses a FRESH
// physical name, which the existing naming already guarantees: a source durable is
// "<idHash>_<nuid>" (a fresh nuid per subscribe) and a PUBREL durable carries the session
// generation (mqttPubRelNames), so a legitimate flow never asks to relive a retired name.
// This record makes that non-reuse DURABLE and ENFORCED against the pathological cases: a
// delayed/queued CREATE resurrecting a retired name, and an orphan sweep deleting a name an
// obligation still depends on.
//
// # Why permanent non-reuse is the whole safety argument
//
// Retirement wins first: admission that would depend on the name fails, and no replacement
// resource may claim it. Admission wins first: its dependency blocks retirement until the
// dependency discharges. A delayed CREATE landing after retirement produces a consumer no
// supported obligation can adopt (adoption is gated on a LIVE owner beyond the CREATE
// response). An already-issued old DELETE cannot destroy a successor's consumer, because no
// successor reuses the name. None of this requires fencing the physical CREATE.
//
// The record RETAINS its owner on retirement so cleanup can resolve its authorization, and
// it is never deleted while the account's recovery epoch stands, on the same ground the
// closure and outcome records give: there is no proof every delayed caller has stopped
// referring to it, and an expiry would rebuild the inference-from-absence the mechanism
// removes. A MISSING ownership record is therefore withheld, never read as permission.

// mqttNameOwnClassTk is the record class for a physical consumer name's ownership. Like every
// other F-105 class it fails mqttValidIDHashToken and mqttValidSessGen, so it can never parse
// as a register.
const mqttNameOwnClassTk = "own"

// mqttOwnedStream names which broker-owned stream a consumer name lives on. The physical
// namespace JetStream deletes on is (account/domain, stream, name); the broker is a plain
// client of one external JetStream with a single domain, so the stream plus the name is the
// discriminating part, and the domain is the deployment's one domain.
type mqttOwnedStream string

const (
	mqttOwnedStreamMsgs mqttOwnedStream = "msgs" // $MQTT_msgs, the source consumers
	mqttOwnedStreamOut  mqttOwnedStream = "out"  // $MQTT_out, the PUBREL consumers
)

func mqttValidOwnedStream(s mqttOwnedStream) bool {
	return s == mqttOwnedStreamMsgs || s == mqttOwnedStreamOut
}

// mqttStreamForOwned maps the ownership record's stream token to the JetStream stream name.
func mqttStreamForOwned(s mqttOwnedStream) (string, error) {
	switch s {
	case mqttOwnedStreamMsgs:
		return mqttStreamName, nil
	case mqttOwnedStreamOut:
		return mqttOutStreamName, nil
	}
	return _EMPTY_, fmt.Errorf("unsupported owned-consumer stream %q", s)
}

// mqttNameOwnState is whether a physical name may still be used.
//
// Numbered from 1 for the reason every other F-105 enum is: a missing or null member decodes
// to zero, and a zero naming a real state would make an unsupported record read as the most
// permissive one.
type mqttNameOwnState uint8

const (
	// mqttNameOwnLive: the name is owned by exactly one live resource and may be depended on.
	mqttNameOwnLive mqttNameOwnState = 1
	// mqttNameOwnRetired: the name is spent. It may never go LIVE again; its owner is retained
	// so cleanup can resolve authorization, and a physical consumer found under it is an orphan.
	mqttNameOwnRetired mqttNameOwnState = 2
)

func mqttValidNameOwnState(s mqttNameOwnState) bool {
	return s == mqttNameOwnLive || s == mqttNameOwnRetired
}

// mqttNameOwnership is one physical consumer name's ownership record.
type mqttNameOwnership struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	// Stream and Name are the physical namespace the name occupies. Name is carried in full
	// (the subject token is only a hash of it) so a reader validates the record is about the
	// name it looked up.
	Stream mqttOwnedStream  `json:"stream"`
	Name   string           `json:"name"`
	State  mqttNameOwnState `json:"state"`
	// OwnerGen and OwnerInc identify the resource incarnation that owns (or, once RETIRED,
	// last owned) the name. Retained through retirement so cleanup resolves its authorization.
	OwnerGen string `json:"owner_gen"`
	OwnerInc string `json:"owner_inc"`
}

var mqttNameOwnMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "stream": {}, "name": {}, "state": {}, "owner_gen": {}, "owner_inc": {},
}

var mqttNameOwnRequired = mqttNameOwnMembers

// mqttNameOwnershipSubject returns the durable subject of one physical name's ownership
// record. Keyed by the PHYSICAL NAMESPACE JetStream deletes on — the stream and the name
// HASH — and NOT by the owning client or the generation (Codex consult 12): a DELETE issued
// for (stream, name) carries no client and no generation, so the slot it must arbitrate
// against is the one keyed by exactly that physical pair. The client, generation and owner
// live in the BODY. The physical account/domain is supplied by the stream itself, which is
// per-backend, so two API routes to one backend address one slot without a domain token.
//
// getHash is short, so distinct names can collide onto one subject; every reader therefore
// validates the body's exact Stream and Name (loadNameOwnershipPhysical), turning a collision
// into a refusal rather than an aliased authorization.
func mqttNameOwnershipSubject(stream mqttOwnedStream, name string) string {
	return mqttF105SubjectPrefix() + mqttNameOwnClassTk + "." +
		string(stream) + "." + getHash(name)
}

// mqttDecodeNameOwnership decodes and fully validates one ownership body.
func mqttDecodeNameOwnership(data []byte, wantID, wantEpoch string) (*mqttNameOwnership, error) {
	o, err := mqttDecodeNameOwnershipStruct(data)
	if err != nil {
		return nil, err
	}
	if err := mqttValidateNameOwnership(o, wantID, wantEpoch); err != nil {
		return nil, err
	}
	return o, nil
}

// mqttDecodeNameOwnershipAnyClient decodes and validates an ownership body WITHOUT requiring a
// specific client — the client is a body attribute, and the sweeps that hold only a physical
// (stream, name) have no client to assert (Codex consult 12). It still refuses a body this
// broker does not support; the caller reads o.ID from the validated record.
func mqttDecodeNameOwnershipAnyClient(data []byte, wantEpoch string) (*mqttNameOwnership, error) {
	o, err := mqttDecodeNameOwnershipStruct(data)
	if err != nil {
		return nil, err
	}
	if err := mqttValidateNameOwnershipBody(o, wantEpoch); err != nil {
		return nil, err
	}
	return o, nil
}

// mqttDecodeNameOwnershipStruct enforces the exact-and-present member set and unmarshals; the
// semantic validation is the caller's (client-specific or client-agnostic).
func mqttDecodeNameOwnershipStruct(data []byte) (*mqttNameOwnership, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttNameOwnMembers, "a name-ownership record"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttNameOwnRequired, "a name-ownership record"); err != nil {
		return nil, err
	}
	o := &mqttNameOwnership{}
	if err := json.Unmarshal(data, o); err != nil {
		return nil, err
	}
	return o, nil
}

// mqttValidateNameOwnership refuses a body this broker does not support, and requires it to
// name the expected client.
func mqttValidateNameOwnership(o *mqttNameOwnership, wantID, wantEpoch string) error {
	if err := mqttValidateNameOwnershipBody(o, wantEpoch); err != nil {
		return err
	}
	if o.ID != wantID {
		return fmt.Errorf("name-ownership record names client %q, not %q", o.ID, wantID)
	}
	return nil
}

// mqttValidateNameOwnershipBody refuses a body this broker does not support, WITHOUT the
// client check — the client-agnostic half, shared by the strict and physical validators.
func mqttValidateNameOwnershipBody(o *mqttNameOwnership, wantEpoch string) error {
	if o == nil {
		return fmt.Errorf("name-ownership record is absent")
	}
	if o.ID == _EMPTY_ {
		return fmt.Errorf("name-ownership record names no client")
	}
	if o.Epoch != wantEpoch {
		return fmt.Errorf("name-ownership record carries recovery epoch %q, not this account's %q", o.Epoch, wantEpoch)
	}
	if !mqttValidOwnedStream(o.Stream) {
		return fmt.Errorf("name-ownership record carries unsupported stream %q", o.Stream)
	}
	if o.Name == _EMPTY_ {
		return fmt.Errorf("name-ownership record carries no consumer name")
	}
	if !mqttValidNameOwnState(o.State) {
		return fmt.Errorf("name-ownership record carries unsupported state %d", o.State)
	}
	if o.OwnerGen == _EMPTY_ || o.OwnerInc == _EMPTY_ {
		return fmt.Errorf("name-ownership record carries no owner (gen %q, inc %q)", o.OwnerGen, o.OwnerInc)
	}
	return nil
}

// mqttOwnedStreamForResourceKind maps a resource kind to the physical stream its consumer
// name lives on, and reports whether this kind is one whose name the ownership record governs.
// Only the source and PUBREL consumers are: the affine consumer's name carries its own
// generation and is the affine lifecycle's, and the payload resource is not a consumer.
func mqttOwnedStreamForResourceKind(kind mqttResourceKind) (mqttOwnedStream, bool) {
	switch kind {
	case mqttResourceSourceConsumer:
		return mqttOwnedStreamMsgs, true
	case mqttResourcePubRelConsumer:
		return mqttOwnedStreamOut, true
	}
	return _EMPTY_, false
}

// mqttIsNameOwnershipSubject reports whether a subject is an ownership record.
func mqttIsNameOwnershipSubject(subject string) bool {
	if !mqttIsF105Subject(subject) {
		return false
	}
	return strings.HasPrefix(subject[len(mqttF105SubjectPrefix()):], mqttNameOwnClassTk+".")
}

// loadNameOwnership reads a physical name's ownership record as it durably stands. ABSENT is
// returned as a nil record and a zero sequence rather than an error, because the first claim
// legitimately acts on it; a CORRUPT or INDETERMINATE read is an error and is never treated as
// absent, on the same ground every F-105 loader gives — an absence a failed read invented would
// let a spent name be reclaimed.
func (jsa *mqttJSA) loadNameOwnership(id, epoch string, stream mqttOwnedStream, name string) (*mqttNameOwnership, uint64, error) {
	o, seq, err := jsa.loadNameOwnershipPhysical(epoch, stream, name)
	if err != nil || o == nil {
		return o, seq, err
	}
	if o.ID != id {
		return nil, 0, fmt.Errorf("the ownership of consumer name %q on %q names client %q, not %q",
			name, stream, o.ID, id)
	}
	return o, seq, nil
}

// loadNameOwnershipPhysical reads a physical name's ownership record with NO client id — the
// client is a body attribute, not part of the key (Codex consult 12), so a sweep that holds
// only a physical (stream, name) resolves the owner from the validated body. Because getHash
// is short and distinct names can collide onto one subject, the body's Stream and Name must
// EXACTLY match the pair looked up, so a collision refuses rather than aliasing another
// consumer's authorization. ABSENT is a nil record + zero sequence (the first claim acts on
// it); a corrupt or INDETERMINATE read is an error, never absence.
func (jsa *mqttJSA) loadNameOwnershipPhysical(epoch string, stream mqttOwnedStream, name string) (*mqttNameOwnership, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttNameOwnershipSubject(stream, name))
	return mqttDecodeNameOwnershipPhysical(smsg, err, epoch, stream, name)
}

// mqttDecodeNameOwnershipPhysical judges one read of a physical name's ownership subject, however
// it was issued: alone (loadNameOwnershipPhysical) or in an admission's readiness round. ABSENT is
// a nil record and a zero sequence; a failed read, or an answer carrying no message, is an error.
func mqttDecodeNameOwnershipPhysical(smsg *StoredMsg, err error, epoch string, stream mqttOwnedStream, name string) (*mqttNameOwnership, uint64, error) {
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, fmt.Errorf("the ownership of consumer name %q on %q could not be read: %w", name, stream, err)
		}
		return nil, 0, nil
	}
	if smsg == nil {
		return nil, 0, fmt.Errorf("the ownership of consumer name %q on %q could not be read: "+
			"the server answered without a message", name, stream)
	}
	o, derr := mqttDecodeNameOwnershipAnyClient(smsg.Data, epoch)
	if derr != nil {
		return nil, 0, fmt.Errorf("the ownership of consumer name %q on %q is unsupported: %w", name, stream, derr)
	}
	if o.Stream != stream || o.Name != name {
		return nil, 0, fmt.Errorf("the ownership record on the subject for %q/%q instead names %q/%q: a "+
			"name-hash collision must refuse, never alias another consumer's authorization",
			stream, name, o.Stream, o.Name)
	}
	return o, smsg.Sequence, nil
}

// mqttNameOwnershipMember composes the ownership record's batch member for a transition: the
// record on its name-keyed subject, at the expectation its predecessor was read at (0 for a
// first claim). The transition legality is re-derived at the write boundary; this only frames it.
func mqttNameOwnershipMember(id, epoch string, stream mqttOwnedStream, name string,
	state mqttNameOwnState, ownerGen, ownerInc string, expect uint64) (mqttTransitionMember, error) {

	o := &mqttNameOwnership{
		ID: id, Epoch: epoch, Stream: stream, Name: name, State: state,
		OwnerGen: ownerGen, OwnerInc: ownerInc,
	}
	if verr := mqttValidateNameOwnership(o, id, epoch); verr != nil {
		return mqttTransitionMember{}, fmt.Errorf("composing an unsupported ownership member: %w", verr)
	}
	body, merr := json.Marshal(o)
	if merr != nil {
		return mqttTransitionMember{}, merr
	}
	return mqttTransitionMember{
		subj:   mqttNameOwnershipSubject(stream, name),
		expect: expect,
		body:   body,
	}, nil
}

// mqttNameOwnershipTransitionLegal reports whether moving a name's ownership from prev to next
// is one of the three legal transitions, given the exact owner. prev is nil for an absent
// record (the first claim).
//
//	absent            -> LIVE(owner)      a first claim
//	LIVE(owner)       -> RETIRED(owner)   the same owner retiring the name
//	LIVE(owner)       -> LIVE(owner)      idempotent re-claim by the same owner (no-op writer)
//
// Everything else is refused: LIVE(I1) -> LIVE(I2) (a second owner claiming a live name),
// RETIRED -> anything (a spent name never revives), and a retire whose owner is not the
// standing owner.
func mqttNameOwnershipTransitionLegal(prev, next *mqttNameOwnership) error {
	if next == nil {
		return fmt.Errorf("a name-ownership transition must install a record")
	}
	if !mqttValidNameOwnState(next.State) {
		return fmt.Errorf("a name-ownership transition installs unsupported state %d", next.State)
	}
	if prev == nil {
		if next.State != mqttNameOwnLive {
			return fmt.Errorf("a first claim of name %q must be LIVE, not state %d", next.Name, next.State)
		}
		return nil
	}
	if prev.Stream != next.Stream || prev.Name != next.Name {
		return fmt.Errorf("a name-ownership transition may not change the name it is about (%q/%q -> %q/%q)",
			prev.Stream, prev.Name, next.Stream, next.Name)
	}
	switch prev.State {
	case mqttNameOwnRetired:
		return fmt.Errorf("name %q is RETIRED and may never be reused", prev.Name)
	case mqttNameOwnLive:
		if prev.OwnerGen != next.OwnerGen || prev.OwnerInc != next.OwnerInc {
			return fmt.Errorf("name %q is owned by %q/%q and may not be claimed by %q/%q; a replacement uses a fresh name",
				prev.Name, prev.OwnerGen, prev.OwnerInc, next.OwnerGen, next.OwnerInc)
		}
		// Same owner: LIVE->LIVE (idempotent) or LIVE->RETIRED (this owner retiring it).
		return nil
	}
	return fmt.Errorf("name %q carries unsupported state %d", prev.Name, prev.State)
}
