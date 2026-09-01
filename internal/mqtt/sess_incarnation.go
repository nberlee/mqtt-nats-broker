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
// qos2_shared_affine.go): a late delete from a retired generation cannot name a
// resource a FRESH INCARNATION owns, because a fresh incarnation mints a new
// generation and therefore new names. That much is harmless by construction, with no
// atomicity requirement anywhere.
//
// It does NOT follow that a late delete names nothing live, and F-062 is what that
// over-reading cost. A plain resume PRESERVES the generation (see below), so a peer may
// still be serving the very incarnation a teardown is retiring, and a name-scoped
// deletion reaches whatever that peer has created under it since. Generation scoping
// protects the NEXT incarnation; it does not arbitrate against the current one. Where a
// late removal had to be made safe, the bound came from somewhere else entirely: on
// $MQTT_qos2in every hold purge carries the highest sequence its remover could prove it
// was entitled to destroy (F-062), which is a fact about the removal, not about the name.
// The generation is minted when a session is
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

// Session STREAM incarnation epochs.
//
// Distinct from the per-session generation above, and at a different granularity: a
// generation identifies one session's incarnation, while an epoch identifies the
// $MQTT_sess STORE's. It is stamped into the stream metadata at creation and adopted
// by every broker from the authoritative re-read, and it is what makes a stream
// SEQUENCE meaningful — a sequence is comparable only within the incarnation that
// assigned it, so every durable record that carries one carries the epoch beside it.
// Resetting the bundle mints a new epoch, which is exactly how records naming
// sequences of the old store are recognised as uncomparable rather than misread. Every
// broker-owned stream carries it, not just $MQTT_sess: the nine are one recreation
// unit (F-066), and recreating any one of them alone is refused at startup.
//
// It is an identity token: equality means "same incarnation, sequences comparable",
// inequality means "another incarnation". Nothing is ever ordered by it.

// mqttGenSessEpoch generates a fresh session-stream incarnation epoch.
func mqttGenSessEpoch() string {
	return mqttSessEpochNUIDPrefix + nuid.Next()
}

// mqttValidSessEpoch reports whether s is a well-formed epoch: exactly
// "nuid:<22 base-62 chars>". Empty or any other form is invalid (it collapses no
// value into the identity space and must never be trusted or compared). The body
// must be 22 characters drawn from the NUID base-62 alphabet [0-9A-Za-z] — length
// alone is insufficient, since a same-length body of out-of-alphabet bytes would
// otherwise be treated as a valid identity and (having a different value than the
// current epoch) drive a spurious cross-incarnation cancel instead of a defer.
func mqttValidSessEpoch(s string) bool {
	if !strings.HasPrefix(s, mqttSessEpochNUIDPrefix) {
		return false
	}
	body := s[len(mqttSessEpochNUIDPrefix):]
	if len(body) != 22 { // nuid.Next() is 22 chars
		return false
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

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

// mqttValidatePubRelNames checks a PUBREL consumer's config against the three names
// the given session generation must own. Mirror of mqttValidateQ2AffineNames, and used
// for the same reason: the durable NAME is what attributes a consumer to a session, so
// a config that disagrees with its own name cannot be attributed at all and must not
// be acted on. The orphan sweep applies it at discovery and again immediately before
// the delete.
func mqttValidatePubRelNames(cc *ConsumerConfig, idHash, gen string) error {
	if cc == nil {
		return fmt.Errorf("no consumer config")
	}
	if !mqttValidSessGen(gen) {
		return fmt.Errorf("session generation %q is not a valid identity", gen)
	}
	subject, deliver, durable := mqttPubRelNames(idHash, gen)
	if cc.Durable != durable {
		return fmt.Errorf("durable is %q (want %q)", cc.Durable, durable)
	}
	if cc.FilterSubject != subject {
		return fmt.Errorf("filter subject is %q (want %q)", cc.FilterSubject, subject)
	}
	if cc.DeliverSubject != deliver {
		return fmt.Errorf("deliver subject is %q (want %q)", cc.DeliverSubject, deliver)
	}
	return nil
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

// mqttInflightSubject returns the $MQTT_inflight subject carrying the durable
// IN-FLIGHT REGISTER for one outbound packet identifier of one session incarnation
// ("$MQTT.inflight.<idHash>.<gen>.<PI>").
//
// Keyed by the packet identifier, which is what makes the create-only CAS on it an
// allocation arbitration as well as a record: two brokers cannot both believe they
// own the same identifier. A 16-bit identifier also bounds one incarnation at 65_535
// subjects, so the recovery walk's filter always returns the whole set in one
// request — the same bound, for the same reason, as mqttQoS2HoldWalkFilter.
//
// The generation scopes it for the reason every other name here is scoped: a register
// left behind by a retired incarnation names a subject no FRESH incarnation uses, so a
// late write or delete from one cannot reach the next session's state.
//
// That is the whole of the invariant. It is NOT that the subject is unused: a resume
// preserves the generation, so a peer may still be serving the retiring incarnation and
// using these very subjects. F-034 paid that cost the same way F-062 paid it on
// $MQTT_qos2in — not by naming, but by BOUNDING every removal to what its remover could
// prove it was entitled to destroy. The teardown's reap enumerates, loads, checks each
// occupant's identity and deletes only at or below its own enumeration watermark; the
// drain's release addresses a register by the subject its packet identifier names and
// deletes only a matching occupant. What neither can arbitrate is a genuinely live owner
// of the same register, which is F-036's predicate. So the naming here is still not the
// safety argument — the bound is.
func mqttInflightSubject(idHash, gen string, pi uint16) string {
	return mqttInflightStreamSubjectPrefix + idHash + mqttSessGenTk(gen, ".") +
		"." + strconv.FormatUint(uint64(pi), 10)
}

// mqttInflightWalkFilter returns the filter enumerating one incarnation's in-flight
// registers, and only those: exactly one token past the generation. Used by the
// resume barrier and by the teardown's BOUNDED REAP — since F-034 there is no purge on
// this stream; the reap enumerates with this filter and then deletes at exact sequences.
func mqttInflightWalkFilter(idHash, gen string) string {
	return mqttInflightStreamSubjectPrefix + idHash + mqttSessGenTk(gen, ".") + ".*"
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
