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
// Migration: a record written before this existed carries no generation, which
// means "legacy hash-only names". Such a session keeps working on the old names
// and is re-minted the first time it is discarded; it stays exposed to the
// late-delete race until then. The rename also means a broker on the old naming
// and one on the new naming do not see each other's PUBRELs for the same session,
// so — like the affine protocol — this only fully holds once every broker in the
// cluster understands it.

package mqtt

import (
	"fmt"
	"strconv"

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
// length must not be admitted into the identity space, and empty must not
// collapse "legacy, no identity" into it.
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

// mqttSessGenTk returns the subject/durable token for a generation: the empty
// string for a legacy session (so its names are byte-identical to what an older
// broker wrote), otherwise the generation preceded by its separator.
//
// sep is "." inside a subject and "_" inside a durable name, both of which a
// generation's alphabet excludes, so the token can never be mistaken for part of
// the hash.
func mqttSessGenTk(gen, sep string) string {
	if gen == _EMPTY_ {
		return _EMPTY_
	}
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
// ("$MQTT.qos2.in.<idHash>.<gen>.<PI>"; legacy: "$MQTT.qos2.in.<idHash>.<PI>").
func mqttQoS2HoldSubject(idHash, gen string, pi uint16) string {
	return mqttQoS2IncomingMsgsStreamSubjectPrefix + idHash + mqttSessGenTk(gen, ".") +
		"." + strconv.FormatUint(uint64(pi), 10)
}

// mqttQoS2HoldPurgeFilters returns the subject filters a discarded session must
// purge from $MQTT_qos2in. They cover both record states — held PUBLISHes and
// terminal tombstones — which is what the move protocol needs: a discarded session
// must leave neither behind, or they collide by packet identifier with a later
// session for the same client ID.
//
// A generation-scoped session purges its OWN generation, plus the legacy
// single-token range "<idHash>.*". That second filter matters and is precisely
// scoped: a legacy record's subject is "<idHash>.<PI>", exactly one token past the
// hash, so the filter reaps records orphaned by a pre-upgrade incarnation (which
// today's hash-wide purge would have cleaned up) while being unable to match any
// generation-scoped subject, which always has two.
//
// A legacy session has no identity to scope with, so it keeps the old hash-wide
// filter and stays able to purge a newer generation's holds — the pre-existing
// exposure, and the reason such a session is only safe once it has been discarded
// and re-minted.
func mqttQoS2HoldPurgeFilters(idHash, gen string) []string {
	base := mqttQoS2IncomingMsgsStreamSubjectPrefix + idHash
	if gen == _EMPTY_ {
		return []string{base + ".>"}
	}
	return []string{base + ".*", base + "." + gen + ".>"}
}

// mqttSessGenFromRecord validates a generation read from a persisted session
// record before anything derives a NAME from it.
//
// Everything downstream — the PUBREL durable, subject and delivery subject, the
// inbound QoS 2 hold subjects and, critically, the purge FILTERS — is built by
// concatenation. A stored value carrying "." or a wildcard would either produce an
// unusable name or widen a purge filter across other incarnations, deleting
// records this session has no claim on. The record is not a trusted input: it can
// be written by an older broker, a future one, or hand-edited.
//
// Empty is the legacy form and stays legal (those sessions keep the hash-only
// names until they are next discarded). Anything else must be a well-formed
// generation, and a malformed one fails CLOSED rather than being coerced to the
// legacy form — silently widening a purge is the outcome this exists to prevent.
func mqttSessGenFromRecord(gen string) (string, error) {
	if gen == _EMPTY_ || mqttValidSessGen(gen) {
		return gen, nil
	}
	return _EMPTY_, fmt.Errorf("session record carries a malformed incarnation generation %q", gen)
}
