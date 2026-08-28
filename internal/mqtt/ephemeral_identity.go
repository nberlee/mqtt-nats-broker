// Per-server-run identity for this broker's per-account ephemeral consumers, and
// the two provisioning protocols built on it.
//
// The broker owns exactly two ephemeral push consumers per account: the retained
// observer on $MQTT_rmsgs and the session-record watch on $MQTT_sess. Both used to
// be named with a fresh nuid on every initialization ATTEMPT, and every attempt
// therefore minted a consumer no later attempt could name. A rollback delete whose
// response was lost left one behind that nothing could ever reap by name; its
// lifetime was bounded by InactiveThreshold and its CARDINALITY by nothing.
//
// Retries are real: Start opens the listener (startMQTT) BEFORE it initializes the
// account manager, so a CONNECT arriving in that window drives initialization
// itself, and a failed attempt is retried by the next CONNECT or by Start's own
// call — within one server run, under one identity.
//
// So the name and the delivery subject are DERIVED from a per-server-run token, and
// an attempt asks for the same consumer rather than minting another one. What that
// costs, and why the two purposes get different protocols, is the subject of the two
// halves below.
package mqtt

import (
	"fmt"
	"sync/atomic"

	"github.com/nats-io/nuid"
)

// The metadata keys stamped on both consumers. They are not decoration: the SERVER
// compares consumer metadata as part of the ActionCreate equality check, so the
// per-run token below is what makes a consumer minted by a DIFFERENT run unequal —
// and therefore refused rather than silently adopted.
//
// No broker code reads these back or branches on them. An operator and any future
// sweep are the human readers the platform audit asked for, and mqttEphemeralSchema
// is what a later shape change would bump: because a mismatch REFUSES, an
// unsupported shape is never adopted or rewritten.
const (
	mqttEphemeralOwnerKey   = "mqtt_nats_bridge_owner"
	mqttEphemeralPurposeKey = "mqtt_nats_bridge_purpose"
	mqttEphemeralRunKey     = "mqtt_nats_bridge_run"
	mqttEphemeralSchemaKey  = "mqtt_nats_bridge_schema"

	mqttEphemeralOwnerValue = "mqtt-nats-bridge"
	mqttEphemeralSchema     = "1"

	mqttEphemeralPurposeRetained = "retained-observer"
	mqttEphemeralPurposeWatch    = "session-record-watch"
)

// mqttEphemeralPurpose identifies one of the two per-broker ephemeral consumers. It
// is the key of every per-purpose decision in this file, and the reason none of them
// is a boolean: the two purposes deliberately do NOT share a protocol.
type mqttEphemeralPurpose int

const (
	mqttEphemeralRetainedObserver mqttEphemeralPurpose = iota
	mqttEphemeralSessRecordWatch
)

func (p mqttEphemeralPurpose) metadataValue() string {
	if p == mqttEphemeralRetainedObserver {
		return mqttEphemeralPurposeRetained
	}
	return mqttEphemeralPurposeWatch
}

// mqttTestEphemeralDrawV replaces the token draw itself, which is the only seam that
// can force two Servers to derive the SAME name while keeping INDEPENDENT owner
// nonces. Assigning the fields after minting cannot do it: a nonce derived from the
// id (the mutation this discriminates) would already have been derived from that
// server's own id, so the two nonces would still differ and the mutation would
// survive its test.
var mqttTestEphemeralDrawV atomic.Pointer[func(which string) (string, bool)]

// setMQTTTestEphemeralDraw arms the token-draw seam and returns its restore function.
func setMQTTTestEphemeralDraw(h func(which string) (string, bool)) func() {
	prev := mqttTestEphemeralDrawV.Load()
	mqttTestEphemeralDrawV.Store(&h)
	return func() { mqttTestEphemeralDrawV.Store(prev) }
}

func mqttDrawEphemeralToken(which string) string {
	if h := mqttTestEphemeralDrawV.Load(); h != nil {
		if v, ok := (*h)(which); ok {
			return v
		}
	}
	return nuid.Next()
}

// ephemeralIdentity returns this server run's two tokens, minting them on first use.
//
// ephemeralID NAMES the consumers: it derives both the consumer name and the
// delivery subject for each purpose, so an initialization retry asks for the same
// consumer. ephemeralOwner WITNESSES ownership, and it is an INDEPENDENT draw rather
// than a copy of ephemeralID for one reason: under ActionCreate the server compares
// the submitted config with the existing one, and every other field a colliding run
// would submit derives from the colliding token, so the nonce is the only field that
// still differs. It is what turns a name collision into a refusal instead of a
// silent adoption.
//
// Per SERVER, not per process: the test suite runs several brokers in one process,
// and a shared token would let one delete the other's consumers. Never the node
// name, which the caller of NewServer supplies (in production the hostname) and two
// brokers can be given the same one.
//
// Minted LAZILY here rather than in Start, because many fixtures build a Server
// literal and call getOrCreateMQTTAccountSessionManager without ever calling Start;
// tokens minted only in Start would be empty in exactly the tests that exercise this
// code. nuid.Next() cannot fail, so neither token has a fail-open path — an empty
// token must be impossible, which is why nothing here falls back to a shared or
// guessable value.
//
// What the nonce does NOT buy, stated where it is minted: it does not make a delete
// collision-safe (the observer's delete is unconditional by stream and name, and no
// create-time predicate can condition it), and it does not discriminate an EXACT
// copy of our config, which requires this account's credentials and is therefore
// outside the threat model. Name safety rests on this run's own nuid draw, as
// today's per-attempt delivery subjects already do.
func (s *Server) ephemeralIdentity() (id, owner string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mqtt.ephemeralID == _EMPTY_ {
		s.mqtt.ephemeralID = mqttDrawEphemeralToken("id")
	}
	if s.mqtt.ephemeralOwner == _EMPTY_ {
		s.mqtt.ephemeralOwner = mqttDrawEphemeralToken("owner")
	}
	return s.mqtt.ephemeralID, s.mqtt.ephemeralOwner
}

// The derived names and delivery subjects. Both subjects stay under mqttSubPrefix
// and on the service connection, so the recommended client ACL denying "$MQTT.>"
// stays correct and sweepOrphanedConsumers' "$MQTT.sub." candidacy rule keeps its
// meaning on the stream it actually walks ($MQTT_msgs, which neither of these
// touches). No account token is needed: consumer names are per stream per account
// and subjects are account-scoped, so one process serving two accounts uses the same
// two names in two disjoint spaces.
func mqttRetainedObserverName(id string) string {
	return mqttRetainedMsgsStreamName + "_obs_" + id
}

func mqttRetainedObserverDeliverSubject(id string) string {
	return mqttSubPrefix + id + ".rmsgs"
}

func mqttSessRecordWatchName(id string) string {
	return mqttSessStreamName + "_watch_" + id
}

func mqttSessRecordWatchDeliverSubject(id string) string {
	return mqttSubPrefix + id + ".sessw"
}

func mqttEphemeralConsumerMetadata(owner string, purpose mqttEphemeralPurpose) map[string]string {
	// Sent on EVERY create, because metadata is not sticky: a create-or-update that
	// omits custom keys WIPES them (measured; copyConsumerMetadata carries only the
	// server's own versioning keys).
	return map[string]string{
		mqttEphemeralOwnerKey:   mqttEphemeralOwnerValue,
		mqttEphemeralPurposeKey: purpose.metadataValue(),
		mqttEphemeralRunKey:     owner,
		mqttEphemeralSchemaKey:  mqttEphemeralSchema,
	}
}

// mqttEphemeralCreated records which purposes this initialization attempt
// CONFIRMABLY created, and it is the whole licence the rollback has.
//
// Nothing else licenses a rollback delete — not "the request was issued", not "the
// error was definite". No error-code partition can distinguish "our create landed"
// from "a foreign occupant is now at that name", because both server paths return
// structured errors BEFORE they look the existing consumer up: a foreign occupant
// plus a pre-validation error would otherwise be read as "may have landed" and the
// rollback would delete a live peer's consumer.
//
// What that gives up is stated rather than hidden: a create whose RESPONSE was lost
// leaves a consumer the rollback does not remove. The same run's next attempt
// reconciles it — the observer's confirmed delete removes it definitely, the watch's
// ActionCreate adopts it as config-identical — and failing that, InactiveThreshold.
// The cardinality guarantee never rested on the rollback.
type mqttEphemeralCreated map[mqttEphemeralPurpose]struct{}

func (c mqttEphemeralCreated) confirm(p mqttEphemeralPurpose) {
	if c != nil {
		c[p] = struct{}{}
	}
}

func (c mqttEphemeralCreated) has(p mqttEphemeralPurpose) bool {
	if c == nil {
		return false
	}
	_, ok := c[p]
	return ok
}

// mqttTestConsumerDeleteHookV decides the fate of one ephemeral-consumer delete
// BEFORE it is issued: whether the request is sent at all, and what the caller
// observes. The two are independent, and that is the point — "the delete did not
// land" and "the delete landed and its reply was lost" differ in exactly the state a
// result-only hook cannot reach, which is whether the consumer is still there.
//
// attempt is 1-based, so a test can make the first attempt indefinite and let the
// second one through.
var mqttTestConsumerDeleteHookV atomic.Pointer[func(stream, name string, attempt int) (send bool, observed error, override bool)]

// setMQTTTestConsumerDeleteHook arms the delete seam and returns its restore function.
func setMQTTTestConsumerDeleteHook(h func(stream, name string, attempt int) (send bool, observed error, override bool)) func() {
	prev := mqttTestConsumerDeleteHookV.Load()
	mqttTestConsumerDeleteHookV.Store(&h)
	return func() { mqttTestConsumerDeleteHookV.Store(prev) }
}

// mqttEphemeralDeleteAttempts is the whole budget of the PRE-CREATE
// absence-establishment phase: the original attempt plus one re-attempt.
//
// Two, because a second attempt CAN converge a possibly lost response into a
// definite absence, while nothing about either attempt explains WHY the first was
// indefinite — so a third only adds startup latency without changing the partition.
//
// The budget belongs to this phase and nowhere else. A rollback delete of a CONFIRMED
// create is not a third absence attempt; it is best-effort cleanup of a consumer
// known to exist.
const mqttEphemeralDeleteAttempts = 2

// deleteEphemeralConsumerDefinite establishes that a derived name is ABSENT, or
// refuses to say so.
//
// A METHOD rather than a free function, deliberately: the persisted-identity census
// scanner records selector calls, so a free function would hide every caller that
// interprets this outcome from the bijection that is supposed to catch a new one.
//
// Success and JSConsumerNotFoundErr are both definite: "already absent" is success,
// the same rule rollbackEphemeralConsumers states. Anything else — a timeout, a
// no-responder, an API error — is an AMBIGUITY, not a verdict, and is settled by
// re-attempting the operation rather than by reading anything back. When the budget
// is spent without a definite answer, this returns an error and its caller issues NO
// create: proceeding is what would adopt a consumer whose position is unknown.
func (jsa *mqttJSA) deleteEphemeralConsumerDefinite(stream, name string) error {
	var lastErr error
	for attempt := 1; attempt <= mqttEphemeralDeleteAttempts; attempt++ {
		send, observed, override := true, error(nil), false
		if h := mqttTestConsumerDeleteHookV.Load(); h != nil {
			send, observed, override = (*h)(stream, name, attempt)
		}
		err := error(nil)
		if send {
			_, err = jsa.deleteConsumer(stream, name, false)
		}
		if override {
			err = observed
		} else if !send {
			err = observed
			if err == nil {
				err = fmt.Errorf("mqtt test: delete of %q on %q was not sent", name, stream)
			}
		}
		if err == nil || !isErrorOtherThan(err, JSConsumerNotFoundErr) {
			// Definite: the consumer was removed, or it was already absent.
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("could not establish that consumer %q on %q is absent in %d attempts: %w",
		name, stream, mqttEphemeralDeleteAttempts, lastErr)
}

// deleteEphemeralConsumerBestEffort issues ONE delete of an ephemeral consumer and
// reports what the caller observed. It is the rollback's half of the pair: a single
// attempt, no re-attempt, no verdict required — the guarantee lives in
// deleteEphemeralConsumerDefinite, and this is only promptness.
//
// It shares the delete seam with the definite form deliberately. A seam that covered
// only the pre-create delete would leave the rollback's invisible to tests, and
// "the rollback deleted nothing" is exactly the property the confirmed-create licence
// has to be tested on.
func (jsa *mqttJSA) deleteEphemeralConsumerBestEffort(stream, name string) error {
	if h := mqttTestConsumerDeleteHookV.Load(); h != nil {
		send, observed, override := (*h)(stream, name, 1)
		if !send {
			if observed == nil {
				observed = fmt.Errorf("mqtt test: delete of %q on %q was not sent", name, stream)
			}
			return observed
		}
		_, err := jsa.deleteConsumer(stream, name, false)
		if override {
			return observed
		}
		return err
	}
	_, err := jsa.deleteConsumer(stream, name, false)
	return err
}

// createEphemeralConsumerOwned creates one of this run's ephemeral consumers with
// ActionCreate, and reports whether the create was CONFIRMED.
//
// ActionCreate rather than the zero value (create-or-update) because the zero value
// ADOPTS a consumer already present under the name, at its current position and
// without a word. With ActionCreate the server compares configs: our own consumer,
// config-identical, is adopted silently — correct, it IS ours — and anything
// non-identical is refused with JSConsumerAlreadyExists, which fails initialization
// loudly instead of producing a broker attached to a consumer it does not
// understand. An EXACT copy is indistinguishable from ours by construction; see
// ephemeralIdentity.
//
// The returned bool is the rollback's only licence, and it is true ONLY for a
// confirmed success — never for an error of any shape.
func (jsa *mqttJSA) createEphemeralConsumerOwned(ccr *CreateConsumerRequest) (bool, error) {
	ccr.Action = ActionCreate
	_, err := jsa.createEphemeralConsumer(ccr)
	if h := mqttTestConsumerCreateResultHookV.Load(); h != nil {
		err = (*h)(ccr, err)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
