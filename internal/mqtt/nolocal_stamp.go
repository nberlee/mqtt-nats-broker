package mqtt

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"sort"
	"strings"

	server "github.com/nats-io/nats-server/v2/server"
)

// No Local is decided when the server takes OWNERSHIP of a message, not when the message is
// later forwarded (F-117). [MQTT-4.5.0-1] adds an owned message to the Session State of the
// clients whose subscriptions match at that moment, and [MQTT-3.8.3-3] excludes the
// publisher from a No Local subscription; [MQTT-3.8.4-3] replaces a subscription with a NEW
// one and [MQTT-3.8.4-4] forbids losing messages by doing so. So a re-SUBSCRIBE that changes
// No Local governs messages owned after it and never re-judges one owned before it: an ON
// filter turned OFF must not resurrect a message it excluded, and an OFF filter turned ON
// must not destroy a message it admitted.
//
// The fact is therefore taken at ownership and carried by the message, as the No Local
// STAMP: one authenticated tag per filter of the PUBLISHING session that had No Local set
// and could receive the subject at that moment. Only the publisher's own subscriptions can be
// affected, and a connection's PUBLISH and SUBSCRIBE packets are processed in order on its
// read loop, so sampling the session's option set while processing the PUBLISH sees exactly
// the options of every SUBSCRIBE that preceded it. QoS 0 and 1 are owned in mqttProcessPub;
// QoS 2 at PUBREC, so its stamp is written into the hold and the release reuses it.
//
// The delivery callbacks decide from the stamp alone and never from the subscription's live
// option. Retained replay is a different fact — a fresh forwarding under the subscription
// being created — and keeps the plain origin marker with the current option.

// mqttNoLocalTag authenticates one claim: "the session idHash, owning this subject+payload,
// excluded itself on filter filterKey". An HMAC under the per-run origin key, which never
// leaves the server, so a client that can write the MQTT stream subjects cannot mint one for
// a victim; a replayed tag can only re-assert the victim's own exclusion for that same filter,
// subject and payload. idHash, filterKey and subject contain no NUL, so the separators delimit
// the fields unambiguously. Empty when there is no key or no session.
func mqttNoLocalTag(key []byte, idHash, filterKey, subject string, payload []byte) string {
	if len(key) == 0 || idHash == _EMPTY_ || filterKey == _EMPTY_ {
		return _EMPTY_
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(idHash))
	mac.Write([]byte{0})
	mac.Write([]byte(filterKey))
	mac.Write([]byte{0})
	mac.Write([]byte(subject))
	mac.Write([]byte{0})
	mac.Write(payload)
	return base64.RawStdEncoding.EncodeToString(mac.Sum(nil)[:16])
}

// mqttNoLocalFilterReaches reports whether a subscription to the session key filterKey can
// receive subject. A multi-level filter is installed as TWO subscriptions, the key itself
// ("foo.*.>") and a level-up companion on the key without its ".>" ("foo.*", see processSubs'
// fwc derivation), so the parent-level arm matches against that shortened FILTER rather than
// comparing strings: `foo/+/#` reaches `foo/x`. Over-reaching is harmless (only a receiving
// filter's callback ever looks for its tag); under-reaching would deliver a message the
// publisher excluded.
func mqttNoLocalFilterReaches(subject, filterKey string) bool {
	if server.SubjectMatchesFilter(subject, filterKey) {
		return true
	}
	if parent, ok := strings.CutSuffix(filterKey, ".>"); ok && parent != _EMPTY_ {
		return server.SubjectMatchesFilter(subject, parent)
	}
	return false
}

// mqttNoLocalStamp is the No Local stamp for a message this client is publishing on subject
// with payload, taken NOW, which the caller makes the ownership boundary: the tags of this
// session's No Local filters that reach subject, comma-separated (base64 has no comma), in
// key order so a re-computation is byte-identical. Empty when the client has no session (a Will
// is server-originated) or no No Local filter reaches subject; No Local is a v5 subscription
// option, so a session that never subscribed under v5 has none.
func (c *client) mqttNoLocalStamp(subject string, payload []byte) string {
	if c.mqtt == nil || c.mqtt.sess == nil || c.srv == nil {
		return _EMPTY_
	}
	sess := c.mqtt.sess
	sess.mu.Lock()
	var keys []string
	for k := range sess.noLocalSubs {
		if mqttNoLocalFilterReaches(subject, k) {
			keys = append(keys, k)
		}
	}
	idHash := sess.idHash
	sess.mu.Unlock()
	if len(keys) == 0 {
		return _EMPTY_
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		tag := mqttNoLocalTag(c.srv.mqtt.originKey, idHash, k, subject, payload)
		if tag == _EMPTY_ {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(tag)
	}
	return b.String()
}

// mqttNoLocalExcluded is THE No Local decision of both delivery callbacks: whether the
// subscription for filterKey was one of the publisher's own No Local filters when the server
// took ownership of this message. True only on an authentic tag for exactly this subscriber
// session, filter, subject and payload; an absent, foreign or unverifiable stamp delivers
// (No Local fails open, never dropping a legitimate message). A tag minted by another server
// run does not verify — the origin key is per run — which is the residual docs/CONFORMANCE.md
// states.
func mqttNoLocalExcluded(key []byte, idHash, filterKey, subject string, payload, stamp []byte) bool {
	if len(stamp) == 0 {
		return false
	}
	want := mqttNoLocalTag(key, idHash, filterKey, subject, payload)
	if want == _EMPTY_ {
		return false
	}
	for tag := range bytes.SplitSeq(stamp, []byte{','}) {
		if hmac.Equal(tag, []byte(want)) {
			return true
		}
	}
	return false
}

// mqttNoLocalFilterKey is the filterKey a SUBSCRIBE's filter stamps on every subscription it
// installs, including its level-up companion and its JetStream delivery subscriptions: the
// filter's session key, which is what sess.noLocalSubs and therefore the stamp's tags name.
// Empty for a shared subscription, on which No Local is a Protocol Error [MQTT-3.8.3-4].
func mqttNoLocalFilterKey(f *mqttFilter) string {
	if f.shared {
		return _EMPTY_
	}
	return f.sessionKey()
}

// mqttSidFilterKey is the session key of the filter a plain subscription id belongs to: the
// sid itself, or, for a level-up companion ("foo fwc"), its main filter's key ("foo.>"). It is
// how a drain, which is known by its sid, carries its filter's key onto the subscription a
// later re-SUBSCRIBE adopts.
func mqttSidFilterKey(sid string) string {
	if parent, ok := strings.CutSuffix(sid, mqttMultiLevelSidSuffix); ok {
		return parent + ".>"
	}
	return sid
}
