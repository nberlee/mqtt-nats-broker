// Session incarnation generations.
//
// A session's JetStream resources are named after the client ID hash, which is
// stable across incarnations: a clean start, a session expiry and a
// takeover-discard all destroy one incarnation and let a later one reuse the very
// same hash. Deterministic hash-only names therefore have no identity, and a
// teardown issued by a retired incarnation can name — and delete — a resource a
// fresh incarnation just created. Re-reading a claim marker before each delete
// does not close that window: the read and the JetStream delete are not atomic,
// so a paused broker can verify its claim, let a peer finish the whole teardown
// and create a new session, and only then complete its delete.
//
// So the names carry an incarnation generation instead, exactly as the
// session-affine QoS 2 path already does with q2AffGen (see
// qos2_shared_affine.go): a late delete from a retired generation can only ever
// name a resource that no longer exists. That is harmless by construction, with
// no atomicity requirement anywhere. The generation is minted when a session is
// first created and re-minted by clear() (which every discard path runs,
// including the two that REUSE the session object as a fresh session); a plain
// reconnect or resume preserves it, because a resumed session must still find its
// own PUBRELs, QoS 2 holds and Will.
//
// EVERY session carries one. A record without a generation is rejected rather
// than accommodated (mqttSessGenFromRecord), because the bridge is pre-release and
// its `$MQTT_*` schemas carry no compatibility contract yet — see "Compatibility
// boundary" in docs/ARCHITECTURE.md, which says designs must not carry legacy-format
// machinery. That is not merely tidiness here: a hash-only session would have no
// identity to scope an arbitration with, so the inbound QoS 2 release claim could
// not be arbitrated for it (qos2_release_claim.go) and the discard would fall back
// to a blind hash-wide purge — which can destroy a held payload whose releaser
// claimed it and then died, turning a narrow ordering deviation into silent loss.
// A partially-arbitrated path is worse than no path.

package mqtt

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nuid"
)

// mqttSessGenLen is the length of a nuid.Next() identity, the form every
// incarnation generation takes.
const mqttSessGenLen = 22

// mqttNewSessGen mints a session incarnation generation.
func mqttNewSessGen() string { return nuid.Next() }

// mqttValidSessGen reports whether s is a well-formed incarnation generation:
// exactly mqttSessGenLen characters of the NUID base-62 alphabet. Same rule, and
// same reasoning, as mqttValidQ2AffGen — an out-of-alphabet body of the right
// length must not be admitted into the identity space, and empty must not collapse
// "no identity" into it — which is also why an absent generation is rejected rather
// than accepted as a distinct, unscoped mode.
func mqttValidSessGen(s string) bool {
	if len(s) != mqttSessGenLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// mqttSessGenTk returns the subject/durable token for a generation: the generation
// preceded by its separator.
//
// sep is "." inside a subject and "_" inside a durable name, both of which a
// generation's alphabet excludes, so the token can never be mistaken for part of
// the hash.
//
// gen is always a well-formed generation: it is either freshly minted or came
// through mqttSessGenFromRecord, which rejects an absent one. There is deliberately
// no empty-string branch — a name built from one would be a malformed subject
// (`…HASH..1`) or, worse, a filter that widens across incarnations.
func mqttSessGenTk(gen, sep string) string {
	return sep + gen
}

// mqttPubRelNames returns the three names of a session incarnation's PUBREL path:
// the $MQTT_out subject its PUBRELs are stored on, the NATS subject they are
// delivered on, and the durable that delivers them.
//
// The subject is generation-scoped along with the durable, and must be: $MQTT_out
// is interest-retained, so if two incarnations shared a subject while owning
// distinct durables, an undelivered PUBREL from the retired one would be retained
// by the fresh one's interest and delivered to it — resurrecting a completed
// client's exchange under a packet identifier the new session never issued.
func mqttPubRelNames(idHash, gen string) (subject, deliverSubject, durable string) {
	subject = mqttPubRelSubjectPrefix + idHash + mqttSessGenTk(gen, ".")
	deliverSubject = mqttPubRelDeliverySubjectPrefix + idHash + mqttSessGenTk(gen, ".")
	durable = mqttPubRelConsumerDurablePrefix + idHash + mqttSessGenTk(gen, "_")
	return subject, deliverSubject, durable
}

// mqttQoS2HoldSubject returns the $MQTT_qos2in subject holding an inbound QoS 2
// PUBLISH for one packet identifier of one session incarnation
// ("$MQTT.qos2.in.<idHash>.<gen>.<PI>").
func mqttQoS2HoldSubject(idHash, gen string, pi uint16) string {
	return mqttQoS2IncomingMsgsStreamSubjectPrefix + idHash + mqttSessGenTk(gen, ".") +
		"." + strconv.FormatUint(uint64(pi), 10)
}

// mqttQoS2ClaimSubjectToken is the last token of a release-claim subject, and
// mqttQoS2ClaimSubjectSuffix the token with its separator. A hold subject's last
// token is always a decimal packet identifier, so it can never be this token —
// which is what lets a filter over $MQTT.qos2.in.> tell the two record kinds apart
// by the token itself rather than by counting, which a wildcard depth change would
// silently invalidate.
const (
	mqttQoS2ClaimSubjectToken  = "rel"
	mqttQoS2ClaimSubjectSuffix = "." + mqttQoS2ClaimSubjectToken
)

// mqttQoS2ClaimSubject returns the $MQTT_qos2in subject carrying the RELEASE
// CLAIM for one packet identifier of one session incarnation: the hold subject
// plus a "rel" token ("$MQTT.qos2.in.<idHash>.<gen>.<PI>.rel").
//
// It is a sibling of the hold subject, not part of it: MaxMsgsPer:1 makes each a
// single-slot register of its own, and loadLastMsgFor on the hold subject is an
// exact-subject lookup that cannot see the claim. See qos2_release_claim.go.
func mqttQoS2ClaimSubject(idHash, gen string, pi uint16) string {
	return mqttQoS2ClaimSubjectFor(mqttQoS2HoldSubject(idHash, gen, pi))
}

// mqttQoS2ClaimSubjectFor returns the claim subject for a hold subject. The
// discard walk has the subject rather than the packet identifier, and must never
// have to parse one back out of it.
func mqttQoS2ClaimSubjectFor(holdSubject string) string {
	return holdSubject + mqttQoS2ClaimSubjectSuffix
}

// mqttQoS2IsClaimSubject reports whether subject is a release-claim subject
// rather than a hold subject.
func mqttQoS2IsClaimSubject(subject string) bool {
	return strings.HasSuffix(subject, mqttQoS2ClaimSubjectSuffix)
}

// mqttQoS2HoldWalkFilter returns the filter enumerating one incarnation's HOLD
// subjects, and only those: exactly one token past the generation, so a claim
// subject (two) cannot match.
//
// Excluding claims is not merely tidiness. The response is a subject list, and
// JetStream pages it above JSMaxSubjectDetails (100_000) entries; a packet
// identifier is 16 bits, so holds alone are bounded by 65_535 and one request
// always returns the whole set, while holds plus claims could in principle
// exceed the limit and need paging. The walk is also uninterested in a claim
// whose hold record is gone: qos2_release_claim.go gives those a different owner.
func mqttQoS2HoldWalkFilter(idHash, gen string) string {
	return mqttQoS2IncomingMsgsStreamSubjectPrefix + idHash + mqttSessGenTk(gen, ".") + ".*"
}

// mqttSessGenFromRecord validates a generation read from a persisted session
// record before anything derives a NAME from it.
//
// Everything downstream — the PUBREL durable, subject and delivery subject, the
// inbound QoS 2 hold subjects and their release claims, and the discard's
// enumeration FILTER — is built by concatenation. A stored value carrying "." or a
// wildcard would either produce an unusable name or widen that filter across other
// incarnations, reaching records this session has no claim on. The record is not a
// trusted input: it can be written by a future broker, or hand-edited.
//
// Absent and malformed both fail CLOSED. Absent is the shape a record written
// before incarnation generations existed has, and admitting it was the whole
// legacy path: such a session has no identity to scope an arbitration with, so its
// inbound QoS 2 release could not be arbitrated against a discard and the discard
// would fall back to a blind hash-wide purge — which destroys a held payload whose
// releaser claimed it and then died. That is silent loss, strictly worse than the
// ordering deviation the claim exists to fix, so the path is rejected rather than
// half-supported. The bridge is pre-release and its `$MQTT_*` schemas carry no
// compatibility contract (docs/ARCHITECTURE.md, "Compatibility boundary"): such a
// record means the development store predates this design and must be recreated.
func mqttSessGenFromRecord(gen string) (string, error) {
	if mqttValidSessGen(gen) {
		return gen, nil
	}
	if gen == _EMPTY_ {
		return _EMPTY_, fmt.Errorf("session record carries no incarnation generation: it predates " +
			"generation-scoped resource naming, and the $MQTT_* streams must be recreated")
	}
	return _EMPTY_, fmt.Errorf("session record carries a malformed incarnation generation %q", gen)
}
